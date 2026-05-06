from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import frappe
from frappe.utils import get_datetime  # type: ignore[import]

from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
    WooCommerceSettings,
)
from jarz_woocommerce_integration.utils.customer_woo_id import (
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


def _normalize_phone(p: Optional[str]) -> Optional[str]:
    if not p:
        return None
    s = ''.join(ch for ch in str(p) if ch.isdigit() or ch == '+').strip()
    return s or None


def _candidate_conflicts_with_woo_customer(name: Optional[str], woo_customer_id: Optional[int | str]) -> bool:
    normalized_woo_customer_id = normalize_woo_customer_id(woo_customer_id)
    if not name or not normalized_woo_customer_id:
        return False

    existing_woo_customer_id = get_customer_woo_id(name) or get_legacy_customer_woo_id(name)
    return bool(existing_woo_customer_id and existing_woo_customer_id != normalized_woo_customer_id)


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
) -> None:
    normalized_woo_customer_id = normalize_woo_customer_id(woo_customer_id)

    try:
        if customer_cache is not None:
            updates: dict[str, Any] = {}
            if frappe.db.get_value("Customer", name, "disabled"):
                updates["disabled"] = 0
            if normalized_woo_customer_id and _field_exists("Customer", "woo_customer_id") and not get_customer_woo_id(name):
                updates["woo_customer_id"] = normalized_woo_customer_id
            if username and _field_exists("Customer", "woo_username") and not frappe.db.get_value("Customer", name, "woo_username"):
                updates["woo_username"] = username
            if phone_norm and not frappe.db.get_value("Customer", name, "mobile_no"):
                updates["mobile_no"] = phone_norm
            if email and not frappe.db.get_value("Customer", name, "email_id"):
                updates["email_id"] = email
            if updates:
                frappe.db.set_value("Customer", name, updates, update_modified=False)
            return

        cust = frappe.get_doc("Customer", name)
        changed = False
        if getattr(cust, "disabled", 0):
            cust.disabled = 0
            changed = True
        if normalized_woo_customer_id and _field_exists("Customer", "woo_customer_id") and not get_customer_woo_id(cust):
            cust.woo_customer_id = normalized_woo_customer_id
            changed = True
        if username and _field_exists("Customer", "woo_username") and not getattr(cust, "woo_username", None):
            cust.woo_username = username
            changed = True
        if phone_norm and not getattr(cust, "mobile_no", None):
            cust.mobile_no = phone_norm
            changed = True
        if email and not getattr(cust, "email_id", None):
            cust.email_id = email
            changed = True
        if changed:
            cust.flags.ignore_woo_outbound = True
            with _suppress_woo_outbound():
                cust.save(ignore_permissions=True)
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
                if cache_key.startswith(("user:", "email:")) and _candidate_conflicts_with_woo_customer(cached_name, woo_customer_id):
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
        name = frappe.db.get_value("Customer", {"mobile_no": phone_norm}, "name")
        if not name and _field_exists("Customer", "phone"):
            name = frappe.db.get_value("Customer", {"phone": phone_norm}, "name")
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
        if _candidate_conflicts_with_woo_customer(name, woo_customer_id):
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
        if _candidate_conflicts_with_woo_customer(name, woo_customer_id):
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
        _lock_acquired = True  # proceed without lock if Redis unavailable

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
                    if cache_key.startswith(("user:", "email:")) and _candidate_conflicts_with_woo_customer(cached_name, woo_customer_id):
                        continue
                    return cached_name

            # Re-query DB under lock for the most reliable identifiers
            if woo_customer_id and _field_exists("Customer", "woo_customer_id"):
                _recheck = find_customer_by_woo_id(woo_customer_id)
                if _recheck:
                    _cache_customer(customer_cache, _recheck, woo_customer_id, username, phone_norm, email)
                    return _recheck
            if phone_norm:
                _recheck = frappe.db.get_value("Customer", {"mobile_no": phone_norm}, "name")
                if not _recheck and _field_exists("Customer", "phone"):
                    _recheck = frappe.db.get_value("Customer", {"phone": phone_norm}, "name")
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
                if _candidate_conflicts_with_woo_customer(_recheck, woo_customer_id):
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
                if _candidate_conflicts_with_woo_customer(_recheck, woo_customer_id):
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
            doc.insert(ignore_permissions=True)
        _cache_customer(customer_cache, doc.name, woo_customer_id, username, phone_norm, email)
        return doc.name

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
    country_value = _resolve_country(data.get("country")) or str(data.get("country") or "")
    return _address_signature_parts(
        address_line1,
        address_line2,
        data.get("city"),
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
    if not state_value:
        return None
    
    state_value = state_value.strip()
    if not state_value:
        return None

    # Check in-memory cache first (historical migration)
    if territory_state_cache is not None and state_value in territory_state_cache:
        return territory_state_cache[state_value]
    
    # Import the mapping from territory_sync
    from jarz_woocommerce_integration.services.territory_sync import CODE_TO_DISPLAY
    
    # Create reverse mapping (display -> code)
    DISPLAY_TO_CODE = {v: k for k, v in CODE_TO_DISPLAY.items()}
    
    result = None

    # Try exact match in reverse mapping
    if state_value in DISPLAY_TO_CODE:
        territory_code = DISPLAY_TO_CODE[state_value]
        if frappe.db.exists("Territory", territory_code):
            result = territory_code
    
    if not result:
        # Try matching just the English part (before the hyphen)
        english_part = state_value.split(" - ")[0].strip() if " - " in state_value else state_value
        
        # Try finding by English part in the display values
        for code, display in CODE_TO_DISPLAY.items():
            display_english = display.split(" - ")[0].strip() if " - " in display else display
            if english_part.lower() == display_english.lower():
                if frappe.db.exists("Territory", code):
                    result = code
                    break
    
    if not result:
        # Try exact match against territory name directly (for territories not in CODE_TO_DISPLAY)
        if frappe.db.exists("Territory", {"territory_name": state_value, "is_group": 0}):
            result = frappe.db.get_value("Territory", {"territory_name": state_value, "is_group": 0}, "name")
    
    if not result:
        # Try case-insensitive search on all territories
        territories = frappe.get_all(
            "Territory",
            filters={"is_group": 0},
            fields=["name", "territory_name"]
        )
        
        state_lower = state_value.lower()
        for terr in territories:
            if terr.territory_name and terr.territory_name.lower() == state_lower:
                result = terr.name
                break

    if not result:
        # Final fallback: use global default territory if configured
        try:
            default_territory = frappe.defaults.get_global_default("territory")
            if default_territory and frappe.db.exists("Territory", default_territory):
                result = default_territory
        except Exception:
            pass

    # Populate cache for future lookups
    if territory_state_cache is not None:
        territory_state_cache[state_value] = result
    
    return result


def _create_address(customer: str, address_type: str, data: dict, phone: str | None, email: str | None) -> str:
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
    addr.insert(ignore_permissions=True)
    return addr.name


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
    
    customer = _ensure_customer(email, billing.get("first_name"), billing.get("last_name"), order.get("id"), username=username, phone=phone, woo_customer_id=woo_customer_id, customer_cache=customer_cache)

    billing_addr_name = None
    shipping_addr_name = None

    # Check if billing and shipping are the same physical address
    same_address = _same_source_address(billing, shipping)

    # Ensure billing address if present
    if _has_usable_source_address(billing):
        existing = _find_existing_address_for_customer(customer, "Billing", billing, address_cache=address_cache)
        billing_addr_name = existing or _create_address(customer, "Billing", billing, billing.get("phone"), email)
        # Set as default billing address for this customer
        if billing_addr_name:
            _set_address_as_default(billing_addr_name, customer, "Billing")
            # Cache newly created address too
            if address_cache is not None and not existing:
                address_cache[(customer, _source_address_signature(billing))] = billing_addr_name

    if same_address and billing_addr_name:
        # Reuse billing address for shipping — same physical address
        shipping_addr_name = billing_addr_name
        _set_address_as_default(billing_addr_name, customer, "Shipping")
    elif _has_usable_source_address(shipping):
        # Different address — create/find shipping separately
        existing = _find_existing_address_for_customer(customer, "Shipping", shipping, address_cache=address_cache)
        shipping_addr_name = existing or _create_address(customer, "Shipping", shipping, billing.get("phone") or shipping.get("phone"), email)
        # Set as default shipping address for this customer
        if shipping_addr_name:
            _set_address_as_default(shipping_addr_name, customer, "Shipping")
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

    def _upsert_address(kind: str, data: dict) -> Optional[str]:
        if not _has_usable_source_address(data):
            return None
        existing = _find_existing_address_for_customer(customer_name, kind, data)
        if existing:
            # Set existing address as default
            _set_address_as_default(existing, customer_name, kind)
            return existing
        # Create new address
        new_addr = _create_address(customer_name, kind, data, data.get("phone"), email)
        if new_addr:
            # Set newly created address as default
            _set_address_as_default(new_addr, customer_name, kind)
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

