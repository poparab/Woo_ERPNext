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


def _ensure_customer(email: Optional[str], first_name: str | None, last_name: str | None, order_id: Optional[int]) -> str:
    # Prefer match by email when provided
    if email:
        name = frappe.db.get_value("Customer", {"email_id": email}, "name")
        if name:
            return name
    # Else try by normalized display name to reduce duplicates
    display_name = _normalize_name(first_name, last_name, email, order_id)
    existing_by_name = frappe.db.get_value("Customer", {"customer_name": display_name}, "name")
    if existing_by_name:
        return existing_by_name
    doc = frappe.get_doc({
        "doctype": "Customer",
        "customer_name": display_name,
        "customer_type": "Individual",
        **({"email_id": email} if email else {}),
    })
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

    billing_line1 = (billing.get("address_1") or "").strip()
    shipping_line1 = (shipping.get("address_1") or "").strip()
    if not billing_line1 and not shipping_line1:
        # Explicitly enforce address presence
        raise ValueError("no_address")

    customer = _ensure_customer(email, billing.get("first_name"), billing.get("last_name"), order.get("id"))

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

