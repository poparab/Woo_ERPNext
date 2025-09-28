from __future__ import annotations

from typing import Tuple, Optional
import frappe


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


def _field_exists(doctype: str, fieldname: str) -> bool:
    try:
        meta = frappe.get_meta(doctype)
        return bool(meta and meta.get_field(fieldname))
    except Exception:
        return False


def _normalize_phone(p: Optional[str]) -> Optional[str]:
    if not p:
        return None
    s = ''.join(ch for ch in str(p) if ch.isdigit() or ch == '+').strip()
    return s or None


def _ensure_customer(email: Optional[str], first_name: str | None, last_name: str | None, order_id: Optional[int], *, username: Optional[str] = None, phone: Optional[str] = None) -> str:
    """Find or create a Customer, preferring uniqueness by username, then phone, then email.

    Priority:
    1) Customer.woo_username (custom field) == username
    2) Customer.mobile_no or Customer.phone == normalized(phone)
    3) Customer.email_id == email
    4) Customer.customer_name == normalized name
    On create, set woo_username (if field exists), mobile_no, email_id.
    """
    phone_norm = _normalize_phone(phone)

    # 1) username-based
    if username and _field_exists("Customer", "woo_username"):
        name = frappe.db.get_value("Customer", {"woo_username": username}, "name")
        if name:
            # ensure phone/email filled if missing
            try:
                cust = frappe.get_doc("Customer", name)
                changed = False
                if phone_norm and not getattr(cust, "mobile_no", None):
                    cust.mobile_no = phone_norm
                    changed = True
                if email and not getattr(cust, "email_id", None):
                    cust.email_id = email
                    changed = True
                if changed:
                    cust.save(ignore_permissions=True)
            except Exception:
                pass
            return name

    # 2) phone-based
    if phone_norm:
        name = frappe.db.get_value("Customer", {"mobile_no": phone_norm}, "name")
        if not name and _field_exists("Customer", "phone"):
            name = frappe.db.get_value("Customer", {"phone": phone_norm}, "name")
        if name:
            # backfill username if field exists and not set
            if username and _field_exists("Customer", "woo_username"):
                try:
                    cur = frappe.get_doc("Customer", name)
                    if not getattr(cur, "woo_username", None):
                        cur.db_set("woo_username", username, commit=False)
                except Exception:
                    pass
            # backfill email if missing
            try:
                cur = frappe.get_doc("Customer", name)
                if email and not getattr(cur, "email_id", None):
                    cur.db_set("email_id", email, commit=False)
            except Exception:
                pass
            return name

    # 3) email-based
    if email:
        name = frappe.db.get_value("Customer", {"email_id": email}, "name")
        if name:
            # backfill username/phone if missing
            try:
                cur = frappe.get_doc("Customer", name)
                if username and _field_exists("Customer", "woo_username") and not getattr(cur, "woo_username", None):
                    cur.db_set("woo_username", username, commit=False)
                if phone_norm and not getattr(cur, "mobile_no", None):
                    cur.db_set("mobile_no", phone_norm, commit=False)
            except Exception:
                pass
            return name

    # 4) display name fallback
    display_name = _normalize_name(first_name, last_name, email, order_id)
    existing_by_name = frappe.db.get_value("Customer", {"customer_name": display_name}, "name")
    if existing_by_name:
        # backfill username/phone/email if missing
        try:
            cur = frappe.get_doc("Customer", existing_by_name)
            if username and _field_exists("Customer", "woo_username") and not getattr(cur, "woo_username", None):
                cur.db_set("woo_username", username, commit=False)
            if phone_norm and not getattr(cur, "mobile_no", None):
                cur.db_set("mobile_no", phone_norm, commit=False)
            if email and not getattr(cur, "email_id", None):
                cur.db_set("email_id", email, commit=False)
        except Exception:
            pass
        return existing_by_name

    # Create new
    fields = {
        "doctype": "Customer",
        "customer_name": display_name if display_name else (username or "Woo Customer"),
        "customer_type": "Individual",
    }
    if email:
        fields["email_id"] = email
    if phone_norm:
        fields["mobile_no"] = phone_norm
    if username and _field_exists("Customer", "woo_username"):
        fields["woo_username"] = username
    doc = frappe.get_doc(fields)
    doc.insert(ignore_permissions=True)
    return doc.name


def _find_existing_address_for_customer(customer: str, address_type: str, address_line1: str) -> Optional[str]:
    # Try to find an Address linked to the customer with same type and line1
    addresses = frappe.get_all(
        "Address",
        filters={
            "address_type": address_type,
            "address_line1": address_line1,
            "disabled": 0,
        },
        fields=["name"],
    )
    for a in addresses:
        links = frappe.get_all("Dynamic Link", filters={"parenttype": "Address", "parent": a.name, "link_doctype": "Customer", "link_name": customer}, fields=["name"])
        if links:
            return a.name
    return None


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


def _create_address(customer: str, address_type: str, data: dict, phone: str | None, email: str | None) -> str:
    country_val = _resolve_country(data.get("country"))
    city_val = (data.get("city") or "").strip() or (data.get("state") or "").strip() or "Unknown"
    addr = frappe.get_doc({
        "doctype": "Address",
        "address_title": customer,
        "address_type": address_type,
        "address_line1": data.get("address_1") or "",
        "address_line2": data.get("address_2") or "",
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


def ensure_customer_with_addresses(order: dict, settings) -> Tuple[str, str | None, str | None]:
    """Create or get Customer and their Billing/Shipping addresses from Woo order.

    Requirements:
    - At least one of billing.address_1 or shipping.address_1 must be non-empty.
    - Email must be present (validated by caller typically).

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

    billing_line1 = (billing.get("address_1") or "").strip()
    shipping_line1 = (shipping.get("address_1") or "").strip()
    if not billing_line1 and not shipping_line1:
        # Explicitly enforce address presence
        raise ValueError("no_address")

    customer = _ensure_customer(email, billing.get("first_name"), billing.get("last_name"), order.get("id"), username=username, phone=phone)

    billing_addr_name = None
    shipping_addr_name = None

    # Ensure billing address if present
    if billing_line1:
        existing = _find_existing_address_for_customer(customer, "Billing", billing_line1)
        billing_addr_name = existing or _create_address(customer, "Billing", billing, billing.get("phone"), email)

    # Ensure shipping address if present
    if shipping_line1:
        existing = _find_existing_address_for_customer(customer, "Shipping", shipping_line1)
        shipping_addr_name = existing or _create_address(customer, "Shipping", shipping, billing.get("phone") or shipping.get("phone"), email)

    return customer, billing_addr_name, shipping_addr_name

