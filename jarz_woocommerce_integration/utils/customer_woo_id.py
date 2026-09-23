from __future__ import annotations

from typing import Any

import frappe


_CUSTOMER_COLUMN_CACHE: dict[str, bool] = {}

# Woo accounts a Customer ALSO answers for, beyond its own woo_customer_id.
#
# One shop can reach ERPNext as two Customers, each bound to its own Woo account
# (two branches of one brand, each once registered on the site). When the two
# Customers are merged, the absorbed account's id has nowhere to live: the
# survivor keeps its own woo_customer_id, and the next order or profile event
# from the absorbed account would find nobody and mint the duplicate again.
# The absorbed id is kept here instead, stored as ",6540,7011," so a LIKE on
# ",<id>," can never match a longer id that merely contains it.
ALIAS_FIELD = "woo_customer_id_aliases"


def _customer_has_column(fieldname: str) -> bool:
    cached = _CUSTOMER_COLUMN_CACHE.get(fieldname)
    if cached is not None:
        return cached

    result = False
    try:
        result = bool(
            frappe.db.sql(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND table_name = 'tabCustomer'
                  AND column_name = %s
                LIMIT 1
                """,
                (fieldname,),
                as_dict=True,
            )
        )
    except Exception:
        try:
            meta = frappe.get_meta("Customer")
            result = bool(meta and meta.get_field(fieldname))
        except Exception:
            result = False

    # A "no" for the alias column is not cached: a long-lived worker that asked
    # before the migrate added it would otherwise ignore aliases until restart,
    # fall through to phone/email and could mint a customer holding the id.
    if result or fieldname != ALIAS_FIELD:
        _CUSTOMER_COLUMN_CACHE[fieldname] = result
    return result


def normalize_woo_customer_id(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        try:
            integer_value = int(value)
        except (TypeError, ValueError):
            return None
        return str(integer_value) if integer_value > 0 else None

    text = str(value).strip()
    if not text or not text.isdigit():
        return None

    integer_value = int(text)
    return str(integer_value) if integer_value > 0 else None


def get_customer_woo_id(customer: Any) -> str | None:
    if isinstance(customer, str):
        try:
            value = frappe.db.get_value("Customer", customer, "woo_customer_id")
        except Exception:
            value = None
    else:
        value = getattr(customer, "woo_customer_id", None)
        if value is None and getattr(customer, "name", None):
            try:
                value = frappe.db.get_value("Customer", customer.name, "woo_customer_id")
            except Exception:
                value = None
    return normalize_woo_customer_id(value)


def has_legacy_customer_woo_id() -> bool:
    return _customer_has_column("custom_woo_customer_id")


def get_legacy_customer_woo_id(customer: Any) -> str | None:
    if not has_legacy_customer_woo_id():
        return None

    if isinstance(customer, str):
        try:
            value = frappe.db.get_value("Customer", customer, "custom_woo_customer_id")
        except Exception:
            value = None
    else:
        value = getattr(customer, "custom_woo_customer_id", None)
        if value is None and getattr(customer, "name", None):
            try:
                value = frappe.db.get_value("Customer", customer.name, "custom_woo_customer_id")
            except Exception:
                value = None
    return normalize_woo_customer_id(value)


def has_unmigrated_legacy_customer_woo_id(customer: Any) -> bool:
    return not get_customer_woo_id(customer) and bool(get_legacy_customer_woo_id(customer))


def customer_woo_id_alias_column_exists() -> bool:
    return _customer_has_column(ALIAS_FIELD)


def parse_woo_id_aliases(value: Any) -> list[str]:
    """``",6540,7011,"`` -> ``["6540", "7011"]`` (normalised, de-duplicated)."""
    out: list[str] = []
    for part in str(value or "").replace(";", ",").split(","):
        normalized = normalize_woo_customer_id(part.strip())
        if normalized and normalized not in out:
            out.append(normalized)
    return out


def format_woo_id_aliases(ids: Any) -> str:
    cleaned = parse_woo_id_aliases(",".join(str(i) for i in (ids or [])))
    return f",{','.join(cleaned)}," if cleaned else ""


def get_customer_woo_id_aliases(customer_name: str) -> list[str]:
    if not customer_name or not customer_woo_id_alias_column_exists():
        return []
    try:
        value = frappe.db.get_value("Customer", customer_name, ALIAS_FIELD)
    except Exception:
        return []
    return parse_woo_id_aliases(value)


def _alias_holders(normalized: str, limit: int = 2) -> list[str]:
    if not normalized or not customer_woo_id_alias_column_exists():
        return []
    try:
        return frappe.db.get_values(
            "Customer",
            {ALIAS_FIELD: ["like", f"%,{normalized},%"]},
            "name",
            order_by="creation asc",
            limit=limit,
            pluck=True,
        ) or []
    except Exception:
        return []


def customer_holds_woo_id_as_alias(customer_name: str, woo_customer_id: Any) -> bool:
    """Did *customer_name* answer for *woo_customer_id* only through an alias?

    True means the Woo account is an absorbed one: it may route an order to this
    Customer, but it must never rewrite who this Customer is.
    """
    normalized = normalize_woo_customer_id(woo_customer_id)
    if not normalized or not customer_name:
        return False
    if get_customer_woo_id(customer_name) == normalized:
        return False
    return normalized in get_customer_woo_id_aliases(customer_name)


def record_woo_id_aliases(customer_name: str, ids: Any) -> list[str]:
    """Add *ids* to the Customer's aliases, never its own primary id.

    Returns the resulting alias list. A no-op (returns ``[]``) when the column is
    absent, so a site that has not migrated keeps today's behaviour.
    """
    if not customer_name or not customer_woo_id_alias_column_exists():
        return []
    primary = get_customer_woo_id(customer_name)
    current = get_customer_woo_id_aliases(customer_name)
    merged = list(current)
    for value in parse_woo_id_aliases(",".join(str(i) for i in (ids or []))):
        if value != primary and value not in merged:
            merged.append(value)
    if merged != current:
        frappe.db.set_value(
            "Customer", customer_name, ALIAS_FIELD, format_woo_id_aliases(merged),
            update_modified=False,
        )
    return merged


def find_customer_by_woo_id(woo_customer_id: Any) -> str | None:
    """Resolve the single Customer bound to *woo_customer_id*.

    Returns ``None`` when the id is claimed by more than one Customer.  That is
    not a defensive nicety: production has ids held by hundreds of unrelated
    customers, minted when the outbound push generated a colliding placeholder
    email and adopted whatever account WooCommerce matched it to.  This function
    is step zero of every customer resolution, and an unordered ``get_value``
    over a poisoned id returns an arbitrary stranger — silently attaching an
    order to the wrong person.

    Refusing to answer is the safe failure: the caller falls through to the phone
    lookup, which is reliable.  The ambiguity is logged so the affected ids stay
    visible rather than being papered over.
    """
    normalized = normalize_woo_customer_id(woo_customer_id)
    if not normalized or not _customer_has_column("woo_customer_id"):
        return None

    # frappe.db.get_values, not frappe.get_all: the query builder dereferences
    # frappe.db.TableMissingError, and this module's callers routinely patch
    # frappe.db with a plain namespace. Staying at the db layer also matches the
    # rest of this file.
    matches = frappe.db.get_values(
        "Customer",
        {"woo_customer_id": normalized},
        "name",
        order_by="creation asc",
        limit=2,
        pluck=True,
    ) or []
    if not matches:
        # A primary holder always wins; only when nobody holds the id as their
        # own does an absorbed-account alias answer for it.
        aliased = _alias_holders(normalized)
        if len(aliased) > 1:
            frappe.logger("woo").warning(
                f"ambiguous_woo_customer_id_alias id={normalized} "
                f"(e.g. {aliased[0]!r}, {aliased[1]!r}); falling back to phone identity"
            )
            return None
        return aliased[0] if aliased else None
    if len(matches) > 1:
        frappe.logger("woo").warning(
            f"ambiguous_woo_customer_id id={normalized} claimed_by_multiple_customers "
            f"(e.g. {matches[0]!r}, {matches[1]!r}); falling back to phone identity"
        )
        return None
    return matches[0]


def customer_woo_id_column_exists() -> bool:
    """Is ``Customer.woo_customer_id`` present on this site?

    Public so callers that must distinguish "no binding" from "no column" — the
    dedupe snapshot, which treats a lost binding as a merge failure — do not have
    to reach into the private cache helper.
    """
    return _customer_has_column("woo_customer_id")


def customer_woo_id_holders(
    woo_customer_id: Any,
    *,
    exclude: str | None = None,
    limit: int = 5,
) -> list[str]:
    """Which ERPNext Customers currently store *woo_customer_id*.

    The guards below only ever needed a yes/no, and answering only that is why
    diagnosing a refused binding took a production archaeology dig: the log said
    the id was claimed but never said by whom.  Returning the names costs the
    same query and turns the next occurrence into a report instead of an
    investigation.

    Returns ``[]`` — never raises — when the column is absent or the probe fails,
    so a diagnostic can never be the thing that breaks a sync.
    """
    normalized = normalize_woo_customer_id(woo_customer_id)
    if not normalized or not _customer_has_column("woo_customer_id"):
        return []
    try:
        holders = frappe.db.get_values(
            "Customer",
            {"woo_customer_id": normalized},
            "name",
            limit=limit,
            pluck=True,
        ) or []
    except Exception:
        return []
    # A Customer answering for the id through an alias holds it just as much:
    # stamping it on anyone else would split one Woo account across two records.
    for holder in _alias_holders(normalized, limit=limit):
        if holder not in holders:
            holders.append(holder)
    return [holder for holder in holders if holder and holder != exclude]


def customer_woo_id_is_claimed_by_other(woo_customer_id: Any, customer_name: str) -> bool:
    """Is *woo_customer_id* already stored on a Customer other than *customer_name*?

    Guards every write of the field.  A Woo account maps to exactly one ERPNext
    Customer; stamping a second one on it is what made ``find_customer_by_woo_id``
    ambiguous in the first place.
    """
    return bool(customer_woo_id_holders(woo_customer_id, exclude=customer_name, limit=2))


def set_customer_woo_id(
    customer_name: str,
    woo_customer_id: Any,
    *,
    clear_legacy: bool = False,
    update_modified: bool = False,
) -> str | None:
    normalized = normalize_woo_customer_id(woo_customer_id)
    if not normalized or not _customer_has_column("woo_customer_id"):
        return None

    updates: dict[str, Any] = {"woo_customer_id": normalized}
    if clear_legacy and has_legacy_customer_woo_id():
        updates["custom_woo_customer_id"] = 0
    frappe.db.set_value("Customer", customer_name, updates, update_modified=update_modified)
    return normalized