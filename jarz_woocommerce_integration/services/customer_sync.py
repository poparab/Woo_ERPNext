from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import re
from typing import Any, Dict, Optional, Tuple
import unicodedata

import frappe
from frappe.utils import get_datetime  # type: ignore[import]

from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
    WooCommerceSettings,
)
from jarz_woocommerce_integration.services.geo_passthrough import (
    GEO_SOURCE_CUSTOMER_PIN,
    GeoPin,
    apply_geo_pin as _apply_geo_pin,
    extract_address_pin as _extract_address_geo_pin,
    resolve_order_pins as _resolve_order_geo_pins,
)
from jarz_woocommerce_integration.utils.customer_woo_id import (
    customer_woo_id_is_claimed_by_other,
    find_customer_by_woo_id,
    get_legacy_customer_woo_id,
    get_customer_woo_id,
    normalize_woo_customer_id,
)
from jarz_woocommerce_integration.utils.http_client import WooClient


def _normalize_name(first: str | None, last: str | None, email: Optional[str] = None, order_id: Optional[int] = None) -> str:
    first = (first or "").strip()
    last = (last or "").strip()
    if first or last:
        return (first + " " + last).strip()
    if email:
        return email
    if order_id:
        return f"Woo Guest {order_id}"
    return "Woo Guest"


_field_exists_cache: dict[tuple[str, str], bool] = {}


def _field_exists(doctype: str, fieldname: str) -> bool:
    key = (doctype, fieldname)
    cached = _field_exists_cache.get(key)
    if cached is not None:
        return cached
    try:
        meta = frappe.get_meta(doctype)
        result = bool(meta and meta.get_field(fieldname))
    except Exception:
        result = False
    _field_exists_cache[key] = result
    return result


def _is_duplicate_key_error(exc: BaseException) -> bool:
    duplicate_error_types = tuple(
        error_type
        for error_type in (
            getattr(frappe, "DuplicateEntryError", None),
            getattr(frappe, "UniqueValidationError", None),
        )
        if isinstance(error_type, type)
    )
    if duplicate_error_types and isinstance(exc, duplicate_error_types):
        return True

    fragments = [str(exc)]
    for arg in getattr(exc, "args", ()) or ():
        if isinstance(arg, BaseException):
            if _is_duplicate_key_error(arg):
                return True
            fragments.append(str(arg))
        else:
            fragments.append(str(arg))

    normalized = " ".join(fragment for fragment in fragments if fragment).lower()
    return (
        "duplicate entry" in normalized
        or ("1062" in normalized and "duplicate" in normalized)
        or ("integrityerror" in normalized and "duplicate" in normalized)
        or (
            "unique" in normalized
            and ("constraint" in normalized or "validation" in normalized or "primary" in normalized)
        )
    )


def _normalize_phone(p: Optional[str]) -> Optional[str]:
    """Canonicalise a phone number to the local Egyptian form (``0XXXXXXXXXX``).

    The store writes the same number two ways — ``01111034268`` from the POS and
    checkout, ``+201111034268`` from WooCommerce accounts that stored the country
    code.  Digit-stripping alone left those as two different identities, so the
    phone step of :func:`_ensure_customer` was blind across the pair and minted a
    fresh Customer every time it crossed the boundary.  Collapsing the country
    code here makes the two spellings one key.

    Non-Egyptian numbers keep a leading ``+`` and are otherwise returned as-is;
    there is no attempt to guess a country for them.  Reads must still go through
    :func:`_phone_variants` because historical rows are stored un-canonicalised.
    """
    if not p:
        return None
    s = ''.join(ch for ch in str(p) if ch.isdigit() or ch == '+').strip()
    if not s:
        return None

    digits = s.lstrip('+')
    if not digits.isdigit():
        return s

    # 00 20 ... -> 20 ...   (international prefix written out)
    if digits.startswith('0020'):
        digits = digits[2:]
    # 20 1XXXXXXXXX (12 digits) -> 01XXXXXXXXX
    if digits.startswith('20') and len(digits) == 12:
        return '0' + digits[2:]
    # 0020 1XXXXXXXXX already folded above; guard the 200XXXXXXXXXX spelling too
    if digits.startswith('200') and len(digits) == 13:
        return digits[2:]
    return s


def _phone_variants(p: str | None) -> list[str]:
    """Every stored spelling of *p* that means the same number.

    ``mobile_no`` is matched with an exact ``=``/``IN`` comparison, and production
    holds the same subscriber as ``01111034268``, ``+201111034268`` and
    ``201111034268``.  A lookup that queries only the canonical form silently
    misses the other two, which is precisely how duplicate Customers were created.
    Returns canonical-first so callers that only want one value can take ``[0]``.
    """
    canonical = _normalize_phone(p)
    if not canonical:
        return []

    variants = [canonical]
    if canonical.startswith('0') and len(canonical) == 11 and canonical.isdigit():
        national = canonical[1:]
        for variant in (f'+20{national}', f'20{national}', f'0020{national}'):
            if variant not in variants:
                variants.append(variant)

    # The raw input may itself be a spelling we do not synthesise (e.g. spaces or
    # dashes already stripped by the caller). Keep it so an exact stored match
    # is never lost.
    raw = ''.join(ch for ch in str(p) if ch.isdigit() or ch == '+').strip()
    if raw and raw not in variants:
        variants.append(raw)
    return variants


def _pick_established_customer(candidates: list[str]) -> str | None:
    """Of several Customers sharing one identity, which should orders attach to?

    The one the business is already using: the record carrying the most recent
    submitted Sales Invoice.  This is not hypothetical tidiness — production has
    504 phone numbers held by more than one Customer, and for the largest family
    the *newer*, odd-looking ``<name>-<order_id>`` record is the real one while
    the clean-named record is an empty shell.  So neither "oldest" nor "newest"
    is the right tie-break; "wherever the orders already are" is, and it makes
    every future order converge on that same record instead of splitting the
    history further.

    Falls back to the oldest candidate when none has ever been invoiced, so the
    answer is always deterministic — an unordered ``get_value`` picking whichever
    row the index happened to yield first is what we are replacing.
    """
    candidates = [c for c in candidates if c]
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    placeholders = ", ".join(["%s"] * len(candidates))
    try:
        rows = frappe.db.sql(
            f"""
            SELECT `customer`
            FROM `tabSales Invoice`
            WHERE `docstatus` = 1 AND `customer` IN ({placeholders})
            GROUP BY `customer`
            ORDER BY MAX(`posting_date`) DESC, MAX(`creation`) DESC
            LIMIT 1
            """,
            tuple(candidates),
        )
        if rows and rows[0] and rows[0][0]:
            return rows[0][0]
    except Exception:
        frappe.logger("woo").warning(
            f"established_customer_lookup_failed candidates={candidates!r}"
        )

    try:
        oldest = frappe.db.get_values(
            "Customer",
            {"name": ["in", candidates]},
            "name",
            order_by="creation asc",
            pluck=True,
        )
        if oldest:
            return oldest[0]
    except Exception:
        pass
    return sorted(candidates)[0]


def _find_customer_by_phone(phone: str | None) -> str | None:
    """Resolve a Customer from any stored spelling of *phone*.

    Checks ``mobile_no`` first, then ``phone``, mirroring the original lookup
    order so an existing match keeps resolving to the same record.  Where the
    number resolves to more than one Customer — the duplicates this module is
    being fixed to stop creating — :func:`_pick_established_customer` decides,
    rather than whichever row the index happened to return.
    """
    variants = _phone_variants(phone)
    if not variants:
        return None

    for fieldname in ("mobile_no", "phone"):
        if fieldname == "phone" and not _field_exists("Customer", "phone"):
            continue
        matches = frappe.db.get_values(
            "Customer",
            {fieldname: ["in", variants]},
            "name",
            order_by="creation asc",
            pluck=True,
        ) or []
        if matches:
            return _pick_established_customer(list(matches))
    return None


def _candidate_conflicts_with_woo_customer(name: Optional[str], woo_customer_id: Optional[int | str]) -> bool:
    normalized_woo_customer_id = normalize_woo_customer_id(woo_customer_id)
    if not name or not normalized_woo_customer_id:
        return False

    existing_woo_customer_id = get_customer_woo_id(name) or get_legacy_customer_woo_id(name)
    return bool(existing_woo_customer_id and existing_woo_customer_id != normalized_woo_customer_id)


def _candidate_safe_for_guest(name: Optional[str]) -> bool:
    """Return False if the candidate Customer is already bound to a Woo identity.

    A customer with a woo_customer_id, legacy custom_woo_customer_id, or
    woo_username belongs to a real Woo account and must never be recycled for a
    guest order (woo_customer_id=0 / None).  Guest orders always create a fresh
    customer in that case.
    """
    if not name:
        return True
    try:
        existing_woo_id = get_customer_woo_id(name) or get_legacy_customer_woo_id(name)
        if existing_woo_id:
            return False
        if _field_exists("Customer", "woo_username"):
            existing_username = frappe.db.get_value("Customer", name, "woo_username")
            if existing_username:
                return False
    except Exception:
        pass
    return True


@contextmanager
def _suppress_woo_outbound():
    previous = getattr(frappe.flags, "ignore_woo_outbound", None)
    setattr(frappe.flags, "ignore_woo_outbound", True)
    try:
        yield
    finally:
        if previous is None:
            try:
                delattr(frappe.flags, "ignore_woo_outbound")
            except Exception:
                setattr(frappe.flags, "ignore_woo_outbound", False)
        else:
            setattr(frappe.flags, "ignore_woo_outbound", previous)


def _update_customer_identity(
    name: str,
    *,
    woo_customer_id: Optional[int | str],
    username: Optional[str],
    phone_norm: Optional[str],
    email: Optional[str],
    customer_cache: dict | None,
    display_name: Optional[str] = None,
    overwrite_existing: bool = False,
) -> None:
    normalized_woo_customer_id = normalize_woo_customer_id(woo_customer_id)
    normalized_display_name = (display_name or "").strip()

    try:
        updates: dict[str, Any] = {}
        if frappe.db.get_value("Customer", name, "disabled"):
            updates["disabled"] = 0
        current_customer_name = str(frappe.db.get_value("Customer", name, "customer_name") or "")
        if normalized_display_name and (overwrite_existing or not current_customer_name) and current_customer_name != normalized_display_name:
            updates["customer_name"] = normalized_display_name
        current_woo_customer_id = get_customer_woo_id(name)
        if normalized_woo_customer_id and _field_exists("Customer", "woo_customer_id") and (overwrite_existing or not current_woo_customer_id) and current_woo_customer_id != normalized_woo_customer_id:
            # One Woo account maps to one Customer. Stamping an id that another
            # Customer already holds is what made the field ambiguous across
            # hundreds of records and broke identity resolution at step zero.
            if customer_woo_id_is_claimed_by_other(normalized_woo_customer_id, name):
                frappe.logger("woo").warning(
                    f"skipped_woo_customer_id_write customer={name!r} id={normalized_woo_customer_id} "
                    f"already_claimed_by_another_customer"
                )
            else:
                updates["woo_customer_id"] = normalized_woo_customer_id
        current_username = str(frappe.db.get_value("Customer", name, "woo_username") or "") if _field_exists("Customer", "woo_username") else ""
        if username and _field_exists("Customer", "woo_username") and (overwrite_existing or not current_username) and current_username != username:
            updates["woo_username"] = username
        current_mobile = str(frappe.db.get_value("Customer", name, "mobile_no") or "")
        if phone_norm and (overwrite_existing or not current_mobile) and current_mobile != phone_norm:
            updates["mobile_no"] = phone_norm
        current_email = str(frappe.db.get_value("Customer", name, "email_id") or "")
        if email and (overwrite_existing or not current_email) and current_email != email:
            updates["email_id"] = email
        if updates:
            frappe.db.set_value("Customer", name, updates, update_modified=False)
    except Exception:
        pass


def _ensure_customer(email: Optional[str], first_name: str | None, last_name: str | None, order_id: Optional[int], *, username: Optional[str] = None, phone: Optional[str] = None, woo_customer_id: Optional[int] = None, customer_cache: dict | None = None) -> str:
    """Find or create a Customer, preferring phone identity after exact Woo ID.

    Priority:
    1) Customer.woo_customer_id == woo_customer_id
    2) Customer.mobile_no or Customer.phone == normalized(phone)
    3) Customer.woo_username (custom field) == username
    3) Customer.email_id == email
    4) Create a new ERP customer
    Automated sync does not reuse existing customers by display name.
    On create, set woo_username (if field exists), mobile_no, email_id.

    When *customer_cache* is provided (historical migration), resolved
    customers are stored there to skip redundant DB lookups.
    """
    phone_norm = _normalize_phone(phone)

    # Fast path: check in-memory cache first (historical migration)
    if customer_cache is not None:
        for cache_key in (
            f"woo_cid:{woo_customer_id}" if woo_customer_id else None,
            f"user:{username}" if username else None,
            f"phone:{phone_norm}" if phone_norm else None,
            f"email:{email}" if email else None,
        ):
            if cache_key and cache_key in customer_cache:
                cached_name = customer_cache[cache_key]
                if cache_key.startswith(("user:", "email:")) and (
                    _candidate_conflicts_with_woo_customer(cached_name, woo_customer_id)
                    or (not woo_customer_id and not _candidate_safe_for_guest(cached_name))
                ):
                    continue
                return cached_name

    # 0) woo_customer_id-based (most reliable, unique WooCommerce identifier)
    if woo_customer_id and _field_exists("Customer", "woo_customer_id"):
        name = find_customer_by_woo_id(woo_customer_id)
        if name:
            _update_customer_identity(
                name,
                woo_customer_id=woo_customer_id,
                username=username,
                phone_norm=phone_norm,
                email=email,
                customer_cache=customer_cache,
            )
            _cache_customer(customer_cache, name, woo_customer_id, username, phone_norm, email)
            return name

    # 1) phone-based
    if phone_norm:
        name = _find_customer_by_phone(phone_norm)
        # For guest orders, phone alone is not sufficient to reuse a Woo-bound customer;
        # require email to also match to confirm it is the same person.
        if name and not woo_customer_id and not _candidate_safe_for_guest(name):
            phone_email_match = bool(email and frappe.db.get_value("Customer", name, "email_id") == email)
            if not phone_email_match:
                frappe.logger("woo").warning(
                    f"woo_order={order_id} guest phone={phone_norm!r} matched Woo-bound "
                    f"customer {name!r}; email mismatch — creating new guest customer"
                )
                name = None
        if name:
            _update_customer_identity(
                name,
                woo_customer_id=woo_customer_id,
                username=username,
                phone_norm=phone_norm,
                email=email,
                customer_cache=customer_cache,
            )
            _cache_customer(customer_cache, name, woo_customer_id, username, phone_norm, email)
            return name

    # 2) username-based
    if username and _field_exists("Customer", "woo_username"):
        name = frappe.db.get_value("Customer", {"woo_username": username}, "name")
        if _candidate_conflicts_with_woo_customer(name, woo_customer_id) or (
            not woo_customer_id and not _candidate_safe_for_guest(name)
        ):
            if name:
                frappe.logger("woo").warning(
                    f"woo_order={order_id} guest username={username!r} matched Woo-bound "
                    f"customer {name!r} — creating new guest customer"
                )
            name = None
        if name:
            _update_customer_identity(
                name,
                woo_customer_id=woo_customer_id,
                username=username,
                phone_norm=phone_norm,
                email=email,
                customer_cache=customer_cache,
            )
            _cache_customer(customer_cache, name, woo_customer_id, username, phone_norm, email)
            return name

    # 3) email-based
    if email:
        name = frappe.db.get_value("Customer", {"email_id": email}, "name")
        if _candidate_conflicts_with_woo_customer(name, woo_customer_id) or (
            not woo_customer_id and not _candidate_safe_for_guest(name)
        ):
            if name:
                frappe.logger("woo").warning(
                    f"woo_order={order_id} guest email={email!r} matched Woo-bound "
                    f"customer {name!r} — creating new guest customer"
                )
            name = None
        if name:
            _update_customer_identity(
                name,
                woo_customer_id=woo_customer_id,
                username=username,
                phone_norm=phone_norm,
                email=email,
                customer_cache=customer_cache,
            )
            _cache_customer(customer_cache, name, woo_customer_id, username, phone_norm, email)
            return name

    # 4) automated display-name reuse is unsafe; only use the normalized name on create
    display_name = _normalize_name(first_name, last_name, email, order_id)

    # Create new — use a per-customer Redis lock to prevent parallel worker races.
    # Lock key is scoped to the most reliable identifier available.  Workers processing
    # different page ranges may hit the same customer simultaneously; without this lock
    # they would both fall through all lookup checks and insert duplicates.
    _lock_id = (
        f"woo_cid:{woo_customer_id}" if woo_customer_id
        else f"user:{username}" if username
        else f"phone:{phone_norm}" if phone_norm
        else f"email:{email}" if email
        else f"name:{display_name}"
    )
    _lock = None
    _lock_acquired = False
    try:
        from frappe.utils.background_jobs import get_redis_conn as _get_redis
        _r = _get_redis()
        _lock = _r.lock(f"woo-customer-lock:{_lock_id}", timeout=30, blocking_timeout=10)
        _lock_acquired = _lock.acquire(blocking=True)
    except Exception:
        _lock = None
        _lock_acquired = False  # Redis unavailable — _safe_insert_customer is the recovery safeguard

    try:
        if _lock_acquired:
            # Re-check under the lock: another worker may have created the customer
            # while we were waiting for it
            for cache_key in (
                f"woo_cid:{woo_customer_id}" if woo_customer_id else None,
                f"user:{username}" if username else None,
                f"phone:{phone_norm}" if phone_norm else None,
                f"email:{email}" if email else None,
            ):
                if cache_key and customer_cache is not None and cache_key in customer_cache:
                    cached_name = customer_cache[cache_key]
                    if cache_key.startswith(("user:", "email:")) and (
                        _candidate_conflicts_with_woo_customer(cached_name, woo_customer_id)
                        or (not woo_customer_id and not _candidate_safe_for_guest(cached_name))
                    ):
                        continue
                    return cached_name

            # Re-query DB under lock for the most reliable identifiers
            if woo_customer_id and _field_exists("Customer", "woo_customer_id"):
                _recheck = find_customer_by_woo_id(woo_customer_id)
                if _recheck:
                    _cache_customer(customer_cache, _recheck, woo_customer_id, username, phone_norm, email)
                    return _recheck
            if phone_norm:
                _recheck = _find_customer_by_phone(phone_norm)
                if _recheck and not woo_customer_id and not _candidate_safe_for_guest(_recheck):
                    phone_email_match = bool(email and frappe.db.get_value("Customer", _recheck, "email_id") == email)
                    if not phone_email_match:
                        _recheck = None
                if _recheck:
                    _update_customer_identity(
                        _recheck,
                        woo_customer_id=woo_customer_id,
                        username=username,
                        phone_norm=phone_norm,
                        email=email,
                        customer_cache=customer_cache,
                    )
                    _cache_customer(customer_cache, _recheck, woo_customer_id, username, phone_norm, email)
                    return _recheck
            if username and _field_exists("Customer", "woo_username"):
                _recheck = frappe.db.get_value("Customer", {"woo_username": username}, "name")
                if _candidate_conflicts_with_woo_customer(_recheck, woo_customer_id) or (
                    not woo_customer_id and not _candidate_safe_for_guest(_recheck)
                ):
                    _recheck = None
                if _recheck:
                    _update_customer_identity(
                        _recheck,
                        woo_customer_id=woo_customer_id,
                        username=username,
                        phone_norm=phone_norm,
                        email=email,
                        customer_cache=customer_cache,
                    )
                    _cache_customer(customer_cache, _recheck, woo_customer_id, username, phone_norm, email)
                    return _recheck
            if email:
                _recheck = frappe.db.get_value("Customer", {"email_id": email}, "name")
                if _candidate_conflicts_with_woo_customer(_recheck, woo_customer_id) or (
                    not woo_customer_id and not _candidate_safe_for_guest(_recheck)
                ):
                    _recheck = None
                if _recheck:
                    _update_customer_identity(
                        _recheck,
                        woo_customer_id=woo_customer_id,
                        username=username,
                        phone_norm=phone_norm,
                        email=email,
                        customer_cache=customer_cache,
                    )
                    _cache_customer(customer_cache, _recheck, woo_customer_id, username, phone_norm, email)
                    return _recheck

        # All rechecks exhausted under lock — safe to create
        fields = {
            "doctype": "Customer",
            "customer_name": display_name if display_name else (username or "Woo Customer"),
            "customer_type": "Individual",
            "disabled": 0,
        }
        if email:
            fields["email_id"] = email
        if phone_norm:
            fields["mobile_no"] = phone_norm
        if username and _field_exists("Customer", "woo_username"):
            fields["woo_username"] = username
        if woo_customer_id and _field_exists("Customer", "woo_customer_id"):
            fields["woo_customer_id"] = str(woo_customer_id)
        doc = frappe.get_doc(fields)
        doc.flags.ignore_woo_outbound = True
        with _suppress_woo_outbound():
            inserted_name = _safe_insert_customer(
                doc,
                woo_customer_id=woo_customer_id,
                username=username,
                phone_norm=phone_norm,
                email=email,
                order_id=order_id,
            )
        _cache_customer(customer_cache, inserted_name, woo_customer_id, username, phone_norm, email)
        return inserted_name

    finally:
        if _lock is not None and _lock_acquired:
            try:
                _lock.release()
            except Exception:
                pass


def _cache_customer(cache: dict | None, name: str, woo_cid, username, phone, email):
    """Store all known keys for a resolved customer into the in-memory cache."""
    if cache is None:
        return
    if woo_cid:
        cache[f"woo_cid:{woo_cid}"] = name
    if username:
        cache[f"user:{username}"] = name
    if phone:
        cache[f"phone:{phone}"] = name
    if email:
        cache[f"email:{email}"] = name


def _read_committed_row(
    doctype: str,
    name: str,
    fields: tuple[str, ...],
    *,
    for_update: bool = True,
) -> dict | None:
    """Read one row by primary key, bypassing this transaction's MVCC snapshot.

    MariaDB runs REPEATABLE READ by default and Frappe never overrides it, so a
    plain ``SELECT`` inside an open transaction is served from a snapshot taken at
    that transaction's first read — a snapshot which, by construction, predates
    the row a racing worker committed a moment ago.  The *unique index* is not
    snapshotted; it is what raised the duplicate-key error in the first place.

    That asymmetry is the whole bug: the recovery lookups re-read through the
    stale snapshot, found nothing, and the caller concluded it had hit a genuine
    name collision when it had actually hit the race the recovery exists for.

    A *locking* read is the fix — InnoDB always serves those from the latest
    committed version rather than from the snapshot.  This is a primary-key point
    read, so it takes one record lock and no gap lock, and a duplicate-key error
    already implies the racing transaction committed (InnoDB blocks the
    conflicting insert until it does), so it never actually waits.

    ``for_update`` picks the lock mode and matters: a caller that writes the row
    afterwards must take the exclusive lock up front, because two workers both
    holding a shared lock and then both upgrading to exclusive is a textbook
    deadlock.  Read-only callers pass ``False`` and take the lighter shared lock.
    """
    if not name:
        return None
    columns = ", ".join(f"`{field}`" for field in fields)
    lock_clause = "FOR UPDATE" if for_update else "LOCK IN SHARE MODE"
    try:
        rows = frappe.db.sql(
            f"SELECT {columns} FROM `tab{doctype}` WHERE `name` = %s LIMIT 1 {lock_clause}",
            (name,),
            as_dict=True,
        )
    except Exception:
        # Never let the recovery path itself abort the sync. Falling back to a
        # plain read keeps the old behaviour rather than losing the order.
        frappe.logger("woo").warning(
            f"locking_read_failed doctype={doctype} name={name!r}; falling back to snapshot read"
        )
        try:
            row = frappe.db.get_value(doctype, name, list(fields), as_dict=True)
        except Exception:
            return None
        return dict(row) if row else None
    return dict(rows[0]) if rows else None


def _customer_is_same_identity(
    row: dict,
    *,
    woo_customer_id: int | str | None,
    username: str | None,
    phone_norm: str | None,
    email: str | None,
) -> bool:
    """Is the Customer in *row* the same person the caller is trying to insert?

    Used to tell the two duplicate-key causes apart once the colliding row has
    actually been read.  A shared identifier means a racing worker got there
    first; no shared identifier means two different people happen to carry the
    same display name, which is the one case where suffixing is correct.

    Absence of evidence is deliberately *not* treated as a match: when neither
    side carries any identifier the two records are genuinely indistinguishable,
    and merging them on name alone would fuse unrelated customers.
    """
    existing_woo = normalize_woo_customer_id(row.get("woo_customer_id"))
    incoming_woo = normalize_woo_customer_id(woo_customer_id)
    if existing_woo and incoming_woo:
        return existing_woo == incoming_woo

    if phone_norm:
        for fieldname in ("mobile_no", "phone"):
            if _normalize_phone(row.get(fieldname)) == _normalize_phone(phone_norm):
                return True

    if username and str(row.get("woo_username") or "").strip() == str(username).strip():
        return True

    if email and str(row.get("email_id") or "").strip().lower() == str(email).strip().lower():
        return True

    return False


_CUSTOMER_IDENTITY_FIELDS = ("name", "woo_customer_id", "woo_username", "mobile_no", "email_id")


def _safe_insert_customer(
    doc,
    *,
    woo_customer_id: Optional[int | str],
    username: Optional[str],
    phone_norm: Optional[str],
    email: Optional[str],
    order_id: Optional[int],
) -> str:
    """Insert a Customer doc with duplicate-key race recovery.

    On a DuplicateEntryError the savepoint is rolled back and the full
    identifier-priority lookup chain is re-run to return the Customer already
    inserted by a concurrent worker.  Those lookups can be served from a stale
    snapshot, so a miss is not proof of a genuine collision: the colliding row is
    then re-read by primary key with :func:`_read_committed_row`, which sees the
    latest committed data.  Only when that row exists and belongs to a *different*
    person is the customer_name suffixed with the order_id and retried once.
    """
    sp = "woo_cust_ins"
    savepoint = getattr(frappe.db, "savepoint", None)
    release_savepoint = getattr(frappe.db, "release_savepoint", None)
    rollback = getattr(frappe.db, "rollback", None)
    supports_savepoints = callable(savepoint) and callable(release_savepoint) and callable(rollback)

    colliding_name = str(getattr(doc, "name", "") or "").strip()

    try:
        if supports_savepoints:
            savepoint(sp)
        doc.insert(ignore_permissions=True)
        if supports_savepoints:
            release_savepoint(sp)
        return doc.name
    except Exception as exc:
        if not _is_duplicate_key_error(exc):
            raise
        # Frappe names the Customer during insert(), so the key that collided is
        # readable off the doc even though the insert failed.
        colliding_name = str(getattr(doc, "name", "") or colliding_name or getattr(doc, "customer_name", "") or "").strip()
        if supports_savepoints:
            rollback(save_point=sp)
        frappe.logger("woo").info(
            f"recovered_from_race customer woo_cid={woo_customer_id} order={order_id}"
        )
        # Re-run priority lookup to find what the racing worker created.
        if woo_customer_id and _field_exists("Customer", "woo_customer_id"):
            found = find_customer_by_woo_id(woo_customer_id)
            if found:
                return found
        if phone_norm:
            found = _find_customer_by_phone(phone_norm)
            if found:
                return found
        if username and _field_exists("Customer", "woo_username"):
            found = frappe.db.get_value("Customer", {"woo_username": username}, "name")
            if found:
                return found
        if email:
            found = frappe.db.get_value("Customer", {"email_id": email}, "name")
            if found:
                return found

        # Every lookup above reads through this transaction's snapshot, so a miss
        # is not evidence that the racing row does not exist. Re-read the exact
        # key that collided with a locking read, which sees committed data.
        existing = _read_committed_row("Customer", colliding_name, _CUSTOMER_IDENTITY_FIELDS)
        if existing:
            if _customer_is_same_identity(
                existing,
                woo_customer_id=woo_customer_id,
                username=username,
                phone_norm=phone_norm,
                email=email,
            ):
                frappe.logger("woo").info(
                    f"recovered_race_via_committed_read customer='{existing.get('name')}' "
                    f"woo_cid={woo_customer_id} order={order_id}"
                )
                _update_customer_identity(
                    existing.get("name"),
                    woo_customer_id=woo_customer_id,
                    username=username,
                    phone_norm=phone_norm,
                    email=email,
                    customer_cache=None,
                )
                return existing.get("name")
            frappe.logger("woo").warning(
                f"customer_name_collision_distinct_identity name={colliding_name!r} "
                f"existing_woo_cid={existing.get('woo_customer_id')!r} incoming_woo_cid={woo_customer_id!r} "
                f"order={order_id}"
            )

        # Genuine non-race collision on the generated name — suffix once and retry.
        suffix = f"-{order_id}" if order_id else "-dup"
        doc.customer_name = f"{doc.customer_name}{suffix}"
        doc.name = None
        doc.insert(ignore_permissions=True)
        frappe.logger("woo").warning(
            f"customer_name_suffix_applied customer='{doc.name}' order={order_id}"
        )
        return doc.name


def _normalize_address_text(value: Any) -> str:
    return " ".join(str(value or "").replace(",", " ").split()).strip().lower()


def _coerce_source_address_lines(data: dict) -> tuple[str, str]:
    address_line1 = str(data.get("address_1") or "").strip()[:240]
    address_line2 = str(data.get("address_2") or "").strip()[:240]
    if address_line1:
        return address_line1, address_line2
    if address_line2:
        return address_line2, ""
    return "", ""


def _address_signature_parts(
    address_line1: Any,
    address_line2: Any,
    city: Any,
    state: Any,
    postcode: Any,
    country: Any,
) -> tuple[str, str, str, str, str, str]:
    return (
        _normalize_address_text(address_line1),
        _normalize_address_text(address_line2),
        _normalize_address_text(city),
        _normalize_address_text(state),
        _normalize_address_text(postcode),
        _normalize_address_text(country),
    )


def _source_address_signature(data: dict) -> tuple[str, str, str, str, str, str]:
    address_line1, address_line2 = _coerce_source_address_lines(data)
    country_value = _resolve_country(data.get("country"))
    if not country_value:
        try:
            country_value = frappe.defaults.get_global_default("country") or ""
        except Exception:
            country_value = str(data.get("country") or "")
    city_value = (data.get("city") or "").strip() or (data.get("state") or "").strip() or "Unknown"
    return _address_signature_parts(
        address_line1,
        address_line2,
        city_value,
        data.get("state"),
        data.get("postcode"),
        country_value,
    )


def _stored_address_signature(address_row: Dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    return _address_signature_parts(
        address_row.get("address_line1"),
        address_row.get("address_line2"),
        address_row.get("city"),
        address_row.get("state"),
        address_row.get("pincode"),
        address_row.get("country"),
    )


def _has_usable_source_address(data: dict) -> bool:
    address_line1, _address_line2 = _coerce_source_address_lines(data)
    return bool(address_line1)


def _same_source_address(left: dict, right: dict) -> bool:
    return _has_usable_source_address(left) and _has_usable_source_address(right) and _source_address_signature(left) == _source_address_signature(right)


def _find_existing_address_for_customer(customer: str, address_type: str, address_data: dict | str, address_cache: dict | None = None) -> Optional[str]:
    del address_type
    source_data = address_data if isinstance(address_data, dict) else {"address_1": address_data}
    signature = _source_address_signature(source_data)
    if not any(signature):
        return None

    # Check in-memory cache first (historical migration)
    if address_cache is not None:
        cache_key = (customer, signature)
        cached = address_cache.get(cache_key)
        if cached is not None:
            return cached

    try:
        result = frappe.db.sql(
            """
            SELECT a.name, a.address_line1, a.address_line2, a.city, a.state, a.pincode, a.country
            FROM `tabAddress` a
            JOIN `tabDynamic Link` dl ON dl.parent = a.name
            WHERE dl.link_doctype = 'Customer'
              AND dl.link_name = %s
              AND dl.parenttype = 'Address'
              AND IFNULL(a.disabled, 0) = 0
            """,
            (customer,),
            as_dict=True,
        )
        found = next(
            (
                row.get("name") if isinstance(row, dict) else row.name
                for row in result
                if _stored_address_signature(row) == signature
            ),
            None,
        )
        # Populate cache for future lookups
        if address_cache is not None and found:
            cache_key = (customer, signature)
            address_cache[cache_key] = found
        return found
    except Exception:
        return None


def _set_address_as_default(address_name: str, customer: str, address_type: str) -> None:
    """Set an address as the preferred/default for a customer using bulk SQL."""
    try:
        flag_field = "is_primary_address" if address_type == "Billing" else "is_shipping_address"

        # Unmark all same-type addresses for this customer in one UPDATE
        frappe.db.sql(
            f"""
            UPDATE `tabAddress` a
            JOIN `tabDynamic Link` dl ON dl.parent = a.name
            SET a.`{flag_field}` = 0
            WHERE dl.link_doctype = 'Customer'
              AND dl.link_name = %s
              AND dl.parenttype = 'Address'
              AND a.address_type = %s
              AND a.`{flag_field}` = 1
              AND a.name != %s
            """,
            (customer, address_type, address_name),
        )

        # Mark the target address
        frappe.db.sql(
            f"""
            UPDATE `tabAddress`
            SET `{flag_field}` = 1
            WHERE name = %s AND IFNULL(`{flag_field}`, 0) = 0
            """,
            (address_name,),
        )
    except Exception as e:
        frappe.logger().warning(f"Failed to set address {address_name} as default: {e}")


def _apply_customer_pin(address_name: Optional[str], pin: Optional[GeoPin]) -> bool:
    """Stamp a customer-supplied map pin on an Address (courier lane O1).

    Writes ``custom_*`` geo fields only -- never ``address_line1``,
    ``address_line2``, ``city``, ``state``, ``pincode``, ``country``, ``phone``,
    ``email_id``, ``address_type`` or ``is_shipping_address``.  Two reasons, both
    load-bearing:

    1. The first six of those *are* the address dedup signature
       (``_address_signature_parts``).  Touching one forks a duplicate Address
       for the customer on the next sync instead of matching the existing row.
    2. All ten are ``outbound_sync._CUSTOMER_ADDRESS_OUTBOUND_UPDATE_FIELDS``,
       the gate on the Woo outbound Address hooks.  A geo-only write therefore
       fans out nothing; bundling a text edit into the same save would push a
       customer + invoice sync to WooCommerce per address.

    Never raises -- a pin is a nice-to-have, an order is not.  The failure is
    logged rather than swallowed silently: a geo write that quietly stops
    happening looks exactly like "no customer ever sent a pin".
    """
    if not address_name or pin is None:
        return False
    try:
        return _apply_geo_pin(
            address_name,
            pin.latitude,
            pin.longitude,
            GEO_SOURCE_CUSTOMER_PIN,
        )
    except Exception as exc:  # noqa: BLE001
        frappe.logger("woo").warning(
            f"geo_pin_failed address={address_name} error={exc}"
        )
        return False


def _resolve_country(raw: str | None) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    # Exact match
    try:
        if frappe.db.exists("Country", raw):
            return raw
    except Exception:
        pass
    # ISO alpha-2 mapping commonly seen from Woo
    code_map = {
        "EG": "Egypt",
        "AE": "United Arab Emirates",
        "SA": "Saudi Arabia",
        "KW": "Kuwait",
        "QA": "Qatar",
        "OM": "Oman",
        "BH": "Bahrain",
        "JO": "Jordan",
        "LB": "Lebanon",
        "MA": "Morocco",
        "TN": "Tunisia",
        "DZ": "Algeria",
    }
    mapped = code_map.get(raw.upper())
    if mapped and frappe.db.exists("Country", mapped):
        return mapped
    # Title-case fallback for name variants
    titled = raw.title()
    try:
        if frappe.db.exists("Country", titled):
            return titled
    except Exception:
        pass
    # Site default country as last resort
    try:
        default = frappe.defaults.get_global_default("country")
        if default and frappe.db.exists("Country", default):
            return default
    except Exception:
        pass
    return None


_TERRITORY_LABEL_SEPARATOR_RE = re.compile(r"\s+-\s+")
_TERRITORY_LOOKUP_FIELDS = (
    "territory_name",
    "custom_woo_code",
    "woo_code",
    "custom_territory_name_ar",
)


def _normalize_territory_lookup_text(value: Any) -> str:
    text = unicodedata.normalize("NFC", str(value or "")).strip()
    text = re.sub(r"\s+", " ", text)
    return re.sub(r"\s*-\s*", " - ", text)


def _territory_lookup_candidates(value: Any) -> list[str]:
    normalized = _normalize_territory_lookup_text(value)
    candidates: list[str] = []
    for candidate in [normalized, *_TERRITORY_LABEL_SEPARATOR_RE.split(normalized, maxsplit=1)]:
        candidate = _normalize_territory_lookup_text(candidate)
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    return candidates


def _lookup_territory_by_field(fieldname: str, value: str) -> str | None:
    if not value:
        return None
    try:
        if fieldname == "name":
            if frappe.db.exists("Territory", value):
                return value
            return None

        if not _field_exists("Territory", fieldname):
            return None

        return (
            frappe.db.get_value("Territory", {fieldname: value, "is_group": 0}, "name")
            or frappe.db.get_value("Territory", {fieldname: value}, "name")
        )
    except Exception:
        return None


def _lookup_territory_from_db_candidates(candidates: list[str]) -> str | None:
    for candidate in candidates:
        territory = _lookup_territory_by_field("name", candidate)
        if territory:
            return territory

    for candidate in candidates:
        for fieldname in _TERRITORY_LOOKUP_FIELDS:
            territory = _lookup_territory_by_field(fieldname, candidate)
            if territory:
                return territory
    return None


def _lookup_territory_from_code_map(candidates: list[str]) -> str | None:
    from jarz_woocommerce_integration.services.territory_sync import CODE_TO_DISPLAY

    normalized_candidates = {candidate.lower(): candidate for candidate in candidates}
    for code, display in CODE_TO_DISPLAY.items():
        display_candidates = _territory_lookup_candidates(display)
        if any(candidate.lower() in normalized_candidates for candidate in display_candidates):
            try:
                if frappe.db.exists("Territory", code):
                    return code
            except Exception:
                continue
    return None


def _resolve_territory_from_state(state_value: str | None, territory_state_cache: dict | None = None) -> str | None:
    """Extract territory from WooCommerce state field (which contains delivery zone).
    
    WooCommerce stores the delivery zone in the 'state' field like "Dokki - الدقي" or "Nasr City - مدينه نصر".
    We need to match this against Territory codes using the territory_sync CODE_TO_DISPLAY mapping.
    
    Args:
        state_value: The state field from WooCommerce address (e.g., "Dokki - الدقي")
        territory_state_cache: Optional dict for caching state → territory lookups.
        
    Returns:
        Territory name (code) if found, None otherwise
    """
    candidates = _territory_lookup_candidates(state_value)
    if not candidates:
        return None

    cache_key = candidates[0]

    # Check in-memory cache first (historical migration)
    if territory_state_cache is not None and cache_key in territory_state_cache:
        return territory_state_cache[cache_key]

    result = _lookup_territory_from_db_candidates(candidates)

    if not result:
        result = _lookup_territory_from_code_map(candidates)

    if not result:
        # Last real-match fallback: case-insensitive scan across available Territory aliases.
        fields = ["name", "territory_name"]
        for fieldname in ("custom_woo_code", "woo_code", "custom_territory_name_ar"):
            if _field_exists("Territory", fieldname):
                fields.append(fieldname)
        territories = frappe.get_all(
            "Territory",
            filters={"is_group": 0},
            fields=fields,
        )
        candidate_lowers = {candidate.lower() for candidate in candidates}
        for terr in territories:
            for fieldname in fields:
                value = terr.get(fieldname) if isinstance(terr, dict) else getattr(terr, fieldname, None)
                if _normalize_territory_lookup_text(value).lower() in candidate_lowers:
                    result = terr.get("name") if isinstance(terr, dict) else terr.name
                    break
            if result:
                break

    # Populate cache for future lookups
    if territory_state_cache is not None:
        territory_state_cache[cache_key] = result
    
    return result


_ADDRESS_SIGNATURE_FIELDS = (
    "name",
    "address_line1",
    "address_line2",
    "city",
    "state",
    "pincode",
    "country",
)


def _address_is_linked_to_customer(address_name: str, customer: str) -> bool:
    """Does *address_name* carry a Dynamic Link to *customer*?

    Read with a locking read for the same reason as :func:`_read_committed_row`:
    the link row was written by the racing worker in the transaction that just
    committed, so a snapshot read would report the address as unlinked and send
    the caller down the suffix path it is trying to avoid.
    """
    if not address_name or not customer:
        return False
    try:
        rows = frappe.db.sql(
            """
            SELECT 1
            FROM `tabDynamic Link`
            WHERE `parenttype` = 'Address'
              AND `parent` = %s
              AND `link_doctype` = 'Customer'
              AND `link_name` = %s
            LIMIT 1
            LOCK IN SHARE MODE
            """,
            (address_name, customer),
        )
        return bool(rows)
    except Exception:
        frappe.logger("woo").warning(
            f"address_link_locking_read_failed address={address_name!r} customer={customer!r}"
        )
        return False


def _safe_insert_address(
    addr_doc,
    *,
    customer: str,
    data: dict,
    order_id: Optional[int],
) -> str:
    """Insert an Address doc with duplicate-key race recovery.

    On a DuplicateEntryError the savepoint is rolled back and
    _find_existing_address_for_customer is re-run to return the Address
    already inserted by a concurrent worker.  That lookup reads through the
    transaction snapshot, so a miss is re-checked against the colliding key with
    :func:`_read_committed_row` before concluding this is a genuine collision —
    the same stale-snapshot trap that produced suffixed duplicate Customers.
    Only then is the address_title suffixed once and retried.
    """
    sp = "woo_addr_ins"
    savepoint = getattr(frappe.db, "savepoint", None)
    release_savepoint = getattr(frappe.db, "release_savepoint", None)
    rollback = getattr(frappe.db, "rollback", None)
    supports_savepoints = callable(savepoint) and callable(release_savepoint) and callable(rollback)

    colliding_name = str(getattr(addr_doc, "name", "") or "").strip()

    try:
        if supports_savepoints:
            savepoint(sp)
        addr_doc.insert(ignore_permissions=True)
        if supports_savepoints:
            release_savepoint(sp)
        return addr_doc.name
    except Exception as exc:
        if not _is_duplicate_key_error(exc):
            raise
        colliding_name = str(getattr(addr_doc, "name", "") or colliding_name or "").strip()
        if supports_savepoints:
            rollback(save_point=sp)
        frappe.logger("woo").info(
            f"recovered_from_race address customer={customer} order={order_id}"
        )
        found = _find_existing_address_for_customer(customer, addr_doc.address_type, data)
        if found:
            return found

        # Snapshot-invisible race: re-read the exact key that collided. Read-only,
        # so the lighter shared lock is enough.
        existing = _read_committed_row(
            "Address", colliding_name, _ADDRESS_SIGNATURE_FIELDS, for_update=False
        )
        if existing and _has_usable_source_address(data):
            if _stored_address_signature(existing) == _source_address_signature(data) and (
                _address_is_linked_to_customer(colliding_name, customer)
            ):
                frappe.logger("woo").info(
                    f"recovered_race_via_committed_read address='{colliding_name}' "
                    f"customer={customer} order={order_id}"
                )
                return colliding_name
            frappe.logger("woo").warning(
                f"address_title_collision_distinct_address name={colliding_name!r} "
                f"customer={customer} order={order_id}"
            )

        # Genuine collision — suffix address_title once and retry.
        suffix = f"-{order_id}" if order_id else "-dup"
        addr_doc.address_title = f"{addr_doc.address_title}{suffix}"
        addr_doc.name = None
        addr_doc.insert(ignore_permissions=True)
        frappe.logger("woo").warning(
            f"address_title_suffix_applied address='{addr_doc.name}' order={order_id}"
        )
        return addr_doc.name


def _create_address(customer: str, address_type: str, data: dict, phone: str | None, email: str | None, order_id: int | None = None) -> str:
    country_val = _resolve_country(data.get("country"))
    city_val = (data.get("city") or "").strip() or (data.get("state") or "").strip() or "Unknown"
    # Truncate address fields to ERPNext's 240-char limit and accept line2-only source addresses.
    addr_line1, addr_line2 = _coerce_source_address_lines(data)
    addr = frappe.get_doc({
        "doctype": "Address",
        "address_title": customer,
        "address_type": address_type,
        "address_line1": addr_line1,
        "address_line2": addr_line2,
        "city": city_val,
        "state": data.get("state") or "",
        "pincode": data.get("postcode") or "",
        **({"country": country_val} if country_val else {}),
        "phone": phone or "",
        "email_id": email or "",
        "links": [
            {
                "link_doctype": "Customer",
                "link_name": customer,
            }
        ],
    })
    addr.flags.ignore_woo_outbound = True
    # Best-effort Redis lock to minimise duplicate insert attempts before
    # falling through to the savepoint-based recovery in _safe_insert_address.
    _alock = None
    _alock_acquired = False
    try:
        from frappe.utils.background_jobs import get_redis_conn as _get_redis
        _r = _get_redis()
        _alock = _r.lock(f"woo-address-lock:{customer}:{address_type}", timeout=30, blocking_timeout=10)
        _alock_acquired = _alock.acquire(blocking=True)
    except Exception:
        _alock = None
        _alock_acquired = False
    try:
        if _alock_acquired:
            # Re-check under lock: another worker may have already created this address.
            _recheck = _find_existing_address_for_customer(customer, address_type, data)
            if _recheck:
                return _recheck
        with _suppress_woo_outbound():
            return _safe_insert_address(addr, customer=customer, data=data, order_id=order_id)
    finally:
        if _alock is not None and _alock_acquired:
            try:
                _alock.release()
            except Exception:
                pass


def ensure_customer_with_addresses(order: dict, settings, customer_cache: dict | None = None, address_cache: dict | None = None, territory_state_cache: dict | None = None) -> Tuple[str, str | None, str | None]:
    """Create or get Customer and their Billing/Shipping addresses from Woo order.

    Requirements:
    - At least one of billing/shipping address_1 or address_2 must be non-empty.
    - Email must be present (validated by caller typically).

    Args:
        customer_cache: Optional dict for caching customer lookups across orders.
        address_cache: Optional dict for caching address lookups across orders.
        territory_state_cache: Optional dict for caching state → territory lookups.

    Returns: (customer_name, billing_address_name, shipping_address_name)
    Raises: ValueError if no usable address present.
    """
    billing = order.get("billing") or {}
    shipping = order.get("shipping") or {}
    email = billing.get("email") or (order.get("customer_email") if isinstance(order.get("customer_email"), str) else None)
    # Try username from order if present (rare). Most often not present on order payload.
    username = order.get("username") if isinstance(order.get("username"), str) else None
    # Prefer billing phone; else shipping
    phone = billing.get("phone") or shipping.get("phone")

    if not _has_usable_source_address(billing) and not _has_usable_source_address(shipping):
        # Explicitly enforce address presence
        raise ValueError("no_address")

    # Extract WooCommerce customer ID from order for idempotent lookups
    woo_customer_id = order.get("customer_id") if isinstance(order.get("customer_id"), int) and order.get("customer_id") > 0 else None
    order_id = order.get("id") if isinstance(order.get("id"), int) else None

    customer = _ensure_customer(email, billing.get("first_name"), billing.get("last_name"), order.get("id"), username=username, phone=phone, woo_customer_id=woo_customer_id, customer_cache=customer_cache)

    billing_addr_name = None
    shipping_addr_name = None

    # Check if billing and shipping are the same physical address
    same_address = _same_source_address(billing, shipping)

    # Courier lane O1 — customer-supplied map pin. Resolved once, up front, so
    # both address branches below stamp the same values. See
    # geo_passthrough.resolve_order_pins for the billing/shipping routing rule.
    billing_pin, shipping_pin = _resolve_order_geo_pins(order)

    # Ensure billing address if present
    if _has_usable_source_address(billing):
        existing = _find_existing_address_for_customer(customer, "Billing", billing, address_cache=address_cache)
        billing_addr_name = existing or _create_address(customer, "Billing", billing, billing.get("phone"), email, order_id)
        # Set as default billing address for this customer
        if billing_addr_name:
            _set_address_as_default(billing_addr_name, customer, "Billing")
            # BOTH branches of `existing or _create_address(...)` land here on
            # purpose: the expression short-circuits, so a create-only pin write
            # would silently never update a returning customer — which is most
            # customers.
            _apply_customer_pin(billing_addr_name, billing_pin)
            # Cache newly created address too
            if address_cache is not None and not existing:
                address_cache[(customer, _source_address_signature(billing))] = billing_addr_name

    if same_address and billing_addr_name:
        # Reuse billing address for shipping — same physical address
        shipping_addr_name = billing_addr_name
        _set_address_as_default(billing_addr_name, customer, "Shipping")
        # Same record as billing: equal rank is accepted, so the order-level pin
        # applied here is the one that ends up stored.
        _apply_customer_pin(shipping_addr_name, shipping_pin)
    elif _has_usable_source_address(shipping):
        # Different address — create/find shipping separately
        existing = _find_existing_address_for_customer(customer, "Shipping", shipping, address_cache=address_cache)
        shipping_addr_name = existing or _create_address(customer, "Shipping", shipping, billing.get("phone") or shipping.get("phone"), email, order_id)
        # Set as default shipping address for this customer
        if shipping_addr_name:
            _set_address_as_default(shipping_addr_name, customer, "Shipping")
            # Created *and* matched-existing both reach this line.
            _apply_customer_pin(shipping_addr_name, shipping_pin)
            # Cache newly created address too
            if address_cache is not None and not existing:
                address_cache[(customer, _source_address_signature(shipping))] = shipping_addr_name

    # Assign territory from shipping state (delivery zone)
    # Prefer shipping address, fallback to billing
    state_value = (shipping.get("state") or billing.get("state") or "").strip()
    if state_value:
        territory = _resolve_territory_from_state(state_value, territory_state_cache=territory_state_cache)
        if territory:
            try:
                # Update customer territory if not already set or different
                current_territory = frappe.db.get_value("Customer", customer, "territory")
                if current_territory != territory:
                    frappe.db.set_value("Customer", customer, "territory", territory, update_modified=False)
            except Exception as e:
                frappe.logger().warning(f"Could not set territory {territory} for customer {customer}: {e}")

    return customer, billing_addr_name, shipping_addr_name


def _format_datetime_for_woo(dt: datetime) -> str:
    dt_utc = dt.astimezone(timezone.utc)
    iso = dt_utc.replace(microsecond=0).isoformat()
    return iso.replace("+00:00", "Z")


def _extract_customer_created_ts(cust: Dict[str, Any]) -> datetime | None:
    for key in ("date_created_gmt", "date_created"):
        raw = cust.get(key)
        if not raw:
            continue
        try:
            dt_val = get_datetime(raw)
            if dt_val is None:
                continue
            if dt_val.tzinfo is None:
                dt_val = dt_val.replace(tzinfo=timezone.utc)
            return dt_val.astimezone(timezone.utc)
        except Exception:  # noqa: BLE001
            continue
    return None


def _extract_customer_modified_ts(cust: Dict[str, Any]) -> datetime | None:
    for key in ("date_modified_gmt", "date_modified", "date_created_gmt", "date_created"):
        raw = cust.get(key)
        if not raw:
            continue
        try:
            dt_val = get_datetime(raw)
            if dt_val is None:
                continue
            if dt_val.tzinfo is None:
                dt_val = dt_val.replace(tzinfo=timezone.utc)
            return dt_val.astimezone(timezone.utc)
        except Exception:  # noqa: BLE001
            continue
    return None


def _sync_customer_payload(cust: Dict[str, Any]) -> Dict[str, Any]:
    billing = cust.get("billing") or {}
    shipping = cust.get("shipping") or {}
    email = (
        billing.get("email")
        or shipping.get("email")
        or (cust.get("email") if isinstance(cust.get("email"), str) else None)
    )
    username = cust.get("username") if isinstance(cust.get("username"), str) else None
    first_name = billing.get("first_name") or shipping.get("first_name")
    last_name = billing.get("last_name") or shipping.get("last_name")
    phone = billing.get("phone") or shipping.get("phone")

    # Use WooCommerce customer ID for idempotent customer lookup
    woo_cust_id = cust.get("id") if isinstance(cust.get("id"), int) else None
    
    customer_name = _ensure_customer(
        email,
        first_name,
        last_name,
        None,  # order_id not applicable for direct customer sync
        username=username,
        phone=phone,
        woo_customer_id=woo_cust_id,
    )
    _update_customer_identity(
        customer_name,
        woo_customer_id=woo_cust_id,
        username=username,
        phone_norm=_normalize_phone(phone),
        email=email,
        customer_cache=None,
        display_name=_normalize_name(first_name, last_name, email, None),
        overwrite_existing=True,
    )

    def _upsert_address(kind: str, data: dict) -> Optional[str]:
        if not _has_usable_source_address(data):
            return None
        # Courier lane O1: a Woo customer record carries no order-level pin, but
        # its address lines can still hold a Maps link the customer typed.
        pin = _extract_address_geo_pin(data)
        existing = _find_existing_address_for_customer(customer_name, kind, data)
        if existing:
            # Set existing address as default
            _set_address_as_default(existing, customer_name, kind)
            _apply_customer_pin(existing, pin)
            return existing
        # Create new address
        new_addr = _create_address(customer_name, kind, data, data.get("phone"), email)
        if new_addr:
            # Set newly created address as default
            _set_address_as_default(new_addr, customer_name, kind)
            _apply_customer_pin(new_addr, pin)
        return new_addr

    same_address = _same_source_address(billing, shipping)

    billing_name = _upsert_address("Billing", billing)
    if same_address and billing_name:
        # Reuse billing address for shipping — same physical address
        shipping_name = billing_name
        _set_address_as_default(billing_name, customer_name, "Shipping")
    else:
        shipping_name = _upsert_address("Shipping", shipping)

    # Assign territory from shipping state (delivery zone)
    # Prefer shipping address, fallback to billing
    state_value = (shipping.get("state") or billing.get("state") or "").strip()
    if state_value:
        territory = _resolve_territory_from_state(state_value)
        if territory:
            try:
                # Update customer territory if not already set or different
                current_territory = frappe.db.get_value("Customer", customer_name, "territory")
                if current_territory != territory:
                    frappe.db.set_value("Customer", customer_name, "territory", territory, update_modified=False)
            except Exception as e:
                frappe.logger().warning(f"Could not set territory {territory} for customer {customer_name}: {e}")

    return {
        "customer": customer_name,
        "billing": billing_name,
        "shipping": shipping_name,
    }



def process_customer_record(payload: dict, settings, debug: bool = False, debug_samples=None) -> dict:
    try:
        result = _sync_customer_payload(payload)
        frappe.db.commit()
        return {'status': 'success', 'customer': result.get('customer'), 'billing_address': result.get('billing'), 'shipping_address': result.get('shipping')}
    except Exception as e:
        return {'status': 'error', 'error': str(e), 'customer_id': payload.get('id')}


def sync_recent_customers(per_page: int = 50, max_pages: int | None = 5) -> Dict[str, Any]:
    settings = WooCommerceSettings.get_settings()
    auto_enabled = bool(getattr(settings, "auto_create_customers", 0))
    if not auto_enabled:
        return {"skipped": True, "reason": "auto_create_customers_disabled"}

    base_url_raw = (getattr(settings, "base_url", "") or "").strip()
    consumer_key = (getattr(settings, "consumer_key", "") or "").strip()
    try:
        consumer_secret = settings.get_password("consumer_secret")
    except Exception:  # noqa: BLE001
        consumer_secret = None

    if not base_url_raw or not consumer_key or not consumer_secret:
        return {"skipped": True, "reason": "missing_credentials"}

    client = WooClient(
        base_url=base_url_raw.rstrip("/"),
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        api_version=settings.api_version or "v3",
    )

    since_raw = getattr(settings, "last_synced_customer_created", None)
    since_dt = get_datetime(since_raw) if since_raw else None
    if since_dt and since_dt.tzinfo is None:
        since_dt = since_dt.replace(tzinfo=timezone.utc)

    page = 1
    processed = 0
    successes = 0
    failures = 0
    latest_seen = since_dt
    results_sample: list[Dict[str, Any]] = []

    while True:
        params = {
            "per_page": per_page,
            "page": page,
            "orderby": "id",
            "order": "asc",
        }
        if since_dt:
            from datetime import timedelta

            lookback = since_dt - timedelta(seconds=1)
            iso_since = _format_datetime_for_woo(lookback)
            params["after"] = iso_since
            # modified_after is available on WooCommerce REST customers (v3+)
            params["modified_after"] = iso_since

        data = client.list_customers(params=params)
        if not data:
            break

        for cust in data:
            processed += 1
            try:
                summary = _sync_customer_payload(cust)
                successes += 1
                if len(results_sample) < 5:
                    results_sample.append(summary)
            except Exception as exc:  # noqa: BLE001
                failures += 1
                frappe.logger().error({
                    "event": "woo_customer_sync_error",
                    "customer_id": cust.get("id"),
                    "error": str(exc),
                    "traceback": frappe.get_traceback(),
                })
                frappe.db.rollback()
                continue

            modified_ts = _extract_customer_modified_ts(cust)
            if modified_ts and (latest_seen is None or modified_ts > latest_seen):
                latest_seen = modified_ts

        if len(data) < per_page:
            break
        page += 1
        if max_pages and page > max_pages:
            break

    if latest_seen and (since_dt is None or latest_seen > since_dt):
        try:
            settings.db_set("last_synced_customer_created", latest_seen)
        except Exception:  # noqa: BLE001
            frappe.logger().warning({
                "event": "woo_customer_sync_timestamp_update_failed",
                "timestamp": latest_seen.isoformat(),
            })

    try:
        frappe.db.commit()
    except Exception:  # noqa: BLE001
        frappe.logger().warning({"event": "woo_customer_sync_commit_failed"})

    return {
        "processed": processed,
        "successes": successes,
        "failures": failures,
        "latest_created": latest_seen.isoformat() if latest_seen else None,
        "since": since_dt.isoformat() if since_dt else None,
        "sample": results_sample,
    }


def sync_customers_cron():  # pragma: no cover
    try:
        result = sync_recent_customers()
        frappe.logger().info({
            "event": "woo_customer_sync",
            "result": result,
        })
    except Exception:  # noqa: BLE001
        frappe.logger().error({
            "event": "woo_customer_sync_error",
            "traceback": frappe.get_traceback(),
        })


def resync_all_customers_cli():  # pragma: no cover
    """CLI command to resync all WooCommerce customers (updates territories).
    
    This will fetch all customers from WooCommerce (up to 500) and update
    their territories based on their latest address information.
    
    Usage:
        bench --site <site> execute jarz_woocommerce_integration.services.customer_sync.resync_all_customers_cli
    """
    frappe.logger().info("Starting full customer resync...")
    result = sync_recent_customers(per_page=50, max_pages=10)
    frappe.logger().info(f"Customer resync complete: {result}")
    print(f"\n✅ Customer Resync Complete:")
    print(f"  Processed: {result.get('processed', 0)}")
    print(f"  Created: {result.get('successes', 0)}")
    print(f"  Errors: {result.get('failures', 0)}")
    return result

