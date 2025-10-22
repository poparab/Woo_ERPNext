from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import frappe
from frappe.utils import get_datetime  # type: ignore[import]

from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
    WooCommerceSettings,
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

    customer_name = _ensure_customer(
        email,
        first_name,
        last_name,
        cust.get("id"),
        username=username,
        phone=phone,
    )

    def _upsert_address(kind: str, data: dict) -> Optional[str]:
        line1 = (data.get("address_1") or "").strip()
        if not line1:
            return None
        existing = _find_existing_address_for_customer(customer_name, kind, line1)
        if existing:
            return existing
        return _create_address(customer_name, kind, data, data.get("phone"), email)

    billing_name = _upsert_address("Billing", billing)
    shipping_name = _upsert_address("Shipping", shipping)

    return {
        "customer": customer_name,
        "billing": billing_name,
        "shipping": shipping_name,
    }


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
        }
        if since_dt:
            params["after"] = _format_datetime_for_woo(since_dt)

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

            created_ts = _extract_customer_created_ts(cust)
            if created_ts and (latest_seen is None or created_ts > latest_seen):
                latest_seen = created_ts

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
            "event": "woo_customer_sync_cron_error",
            "traceback": frappe.get_traceback(),
        })

