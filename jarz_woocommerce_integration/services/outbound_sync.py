from __future__ import annotations

from dataclasses import dataclass
import importlib
import re
from typing import Any, Dict, Optional

import frappe
try:
    _frappe_utils = importlib.import_module("frappe.utils")
except ImportError:  # pragma: no cover - allow type checkers without frappe
    _frappe_utils = importlib.import_module("frappe.utils.data")

cint = getattr(_frappe_utils, "cint")
flt = getattr(_frappe_utils, "flt")
now_datetime = getattr(_frappe_utils, "now_datetime")

from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
    WooCommerceSettings,
)
from jarz_woocommerce_integration.utils.http_client import WooAPIError, WooClient

LOGGER = frappe.logger("jarz_woocommerce.outbound")


def _normalize_outbound_status(status: str | None) -> str:
    """Normalize outbound status values to the allowed title-case options."""
    if not status:
        return ""
    normalized = str(status).strip().lower()
    mapping = {
        "pending": "Pending",
        "synced": "Synced",
        "error": "Error",
        "skipped": "Skipped",
    }
    return mapping.get(normalized, status)


class MissingWooProductError(Exception):
    """Raised when invoice items lack WooCommerce product mappings."""


@dataclass(slots=True)
class OutboundConfig:
    enable_customer_push: bool
    enable_order_push: bool
    payment_cod: str
    payment_instapay: str
    payment_wallet: str
    shipping_method_id: str
    shipping_method_title: str


def _get_settings() -> tuple[WooCommerceSettings, OutboundConfig]:
    settings = WooCommerceSettings.get_settings()
    cfg = OutboundConfig(
        enable_customer_push=bool(getattr(settings, "enable_outbound_customers", 0)),
        enable_order_push=bool(getattr(settings, "enable_outbound_orders", 0)),
        payment_cod=(getattr(settings, "payment_method_cod", None) or "cod").strip(),
        payment_instapay=(getattr(settings, "payment_method_instapay", None) or "instapay").strip(),
        payment_wallet=(getattr(settings, "payment_method_wallet", None) or "wallet").strip(),
        shipping_method_id=(getattr(settings, "default_shipping_method_id", None) or "flat_rate").strip(),
        shipping_method_title=(getattr(settings, "default_shipping_method_title", None) or "Shipping").strip(),
    )
    return settings, cfg


def _build_client(settings: WooCommerceSettings) -> WooClient:
    base_url = (getattr(settings, "base_url", "") or "").strip().rstrip("/")
    consumer_key = (getattr(settings, "consumer_key", "") or "").strip()
    consumer_secret = settings.get_consumer_secret()
    if not base_url or not consumer_key or not consumer_secret:
        raise ValueError("missing_credentials")
    return WooClient(
        base_url=base_url,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        api_version=settings.api_version or "v3",
    )


# ---------------------------------------------------------------------------
# Address helpers
# ---------------------------------------------------------------------------

def _split_contact_name(raw: str | None) -> tuple[str, str]:
    if not raw:
        return "", ""
    pieces = raw.strip().split(" ", 1)
    if not pieces:
        return "", ""
    if len(pieces) == 1:
        return pieces[0], ""
    return pieces[0], pieces[1]


def _get_address_payload(address_name: str | None, *, fallback_name: str, phone: str | None, email: str | None) -> dict:
    if not address_name:
        return {}
    fields = [
        "address_line1",
        "address_line2",
        "city",
        "state",
        "pincode",
        "country",
        "phone",
        "email_id",
    ]
    address = frappe.db.get_value("Address", address_name, fields, as_dict=True)
    if not address:
        return {}
    first, last = _split_contact_name(fallback_name)
    return {
        "first_name": first,
        "last_name": last,
        "company": fallback_name,
        "address_1": address.get("address_line1") or "",
        "address_2": address.get("address_line2") or "",
        "city": address.get("city") or "",
        "state": address.get("state") or "",
        "postcode": address.get("pincode") or "",
        "country": address.get("country") or "",
        "phone": address.get("phone") or phone or "",
        "email": address.get("email_id") or email or "",
    }


def _get_any_address_for_customer(customer_name: str) -> dict:
    """Fetch first available Address linked to a customer as a generic fallback."""
    try:
        link = frappe.get_all(
            "Dynamic Link",
            filters={"link_doctype": "Customer", "link_name": customer_name, "parenttype": "Address"},
            fields=["parent"],
            limit=1,
        )
        if not link:
            return {}
        addr = frappe.get_doc("Address", link[0].parent)
        return {
            "address_1": addr.address_line1 or "",
            "address_2": addr.address_line2 or "",
            "city": addr.city or "",
            "state": addr.state or "",
            "postcode": addr.pincode or "",
            "country": addr.country or "",
            "phone": addr.phone or "",
            "email": addr.email_id or "",
        }
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Customer outbound sync
# ---------------------------------------------------------------------------

def enqueue_customer_sync(customer: frappe.model.document.Document | str, method: str | None = None, *, reason: str = "event", force: bool = False) -> None:
    settings, cfg = _get_settings()
    if not cfg.enable_customer_push and not force:
        return
    if not isinstance(customer, str):
        reason = reason if reason != "event" else (method or "event")
        customer_name = customer.name
    else:
        customer_name = customer
    frappe.enqueue(
        "jarz_woocommerce_integration.services.outbound_sync.sync_customer",
        queue="short",
        timeout=300,
        now=force,
        customer_name=customer_name,
        reason=reason,
    )


def _mark_customer_status(customer_name: str, *, status: str, error: str | None = None) -> None:
    updates = {
        "woo_outbound_status": _normalize_outbound_status(status),
        "woo_outbound_synced_on": now_datetime(),
    }
    if error:
        updates["woo_outbound_error"] = error[:500]
    else:
        updates["woo_outbound_error"] = ""
    frappe.db.set_value("Customer", customer_name, updates, update_modified=False)


def _build_customer_payload(customer: frappe.model.document.Document, *, include_password: bool = False, include_username: bool = True) -> dict:
    # Mobile number is mandatory for WooCommerce
    phone_val = (getattr(customer, "mobile_no", "") or getattr(customer, "phone", "") or "").strip()
    if not phone_val:
        # Try pulling phone from linked addresses
        addr_candidates = [getattr(customer, "customer_primary_address", None), getattr(customer, "customer_shipping_address", None)]
        for addr_name in addr_candidates:
            if not addr_name:
                continue
            addr_phone = frappe.db.get_value("Address", addr_name, "phone")
            if addr_phone:
                phone_val = addr_phone.strip()
                break
    if not phone_val:
        fallback_addr = _get_any_address_for_customer(customer.name)
        phone_val = (fallback_addr.get("phone") or "").strip() if fallback_addr else ""
    if not phone_val:
        phone_val = "0000000000"
        LOGGER.warning({
            "event": "woo_outbound_customer_missing_phone_placeholder",
            "customer": customer.name,
            "message": "No phone found; using placeholder 0000000000",
        })
    
    email = (getattr(customer, "email_id", "") or "").strip()
    if not email:
        # Generate a default email for customers without email
        # WooCommerce requires email, so we create a placeholder using customer name
        sanitized_name = re.sub(r'[^a-zA-Z0-9]', '', customer.name.lower())
        if not sanitized_name:
            sanitized_name = 'customer'
        email = f"{sanitized_name}@orderjarz.local"
        LOGGER.info({
            "event": "woo_outbound_customer_default_email",
            "customer": customer.name,
            "generated_email": email,
        })

    first, last = _split_contact_name(customer.customer_name)
    billing = _get_address_payload(
        getattr(customer, "customer_primary_address", None),
        fallback_name=customer.customer_name,
        phone=phone_val,
        email=email,
    )
    shipping = _get_address_payload(
        getattr(customer, "customer_shipping_address", None),
        fallback_name=customer.customer_name,
        phone=phone_val,
        email=email,
    )
    payload = {
        "email": email,
        "first_name": first,
        "last_name": last,
        "billing": billing,
        "shipping": shipping,
    }
    
    # Only include username for NEW customer creation (WooCommerce doesn't allow editing username)
    if include_username:
        username_field = getattr(customer, "woo_username", None) or email
        if username_field:
            payload["username"] = username_field
    
    # Always include phone in billing and shipping
    payload.setdefault("billing", {})["phone"] = phone_val
    payload.setdefault("shipping", {})["phone"] = phone_val

    # Fallback: if both billing/shipping lack address lines, try any linked address
    billing_line1 = (payload.get("billing", {}).get("address_1") or "").strip()
    shipping_line1 = (payload.get("shipping", {}).get("address_1") or "").strip()
    if not billing_line1 and not shipping_line1:
        fallback_addr = _get_any_address_for_customer(customer.name)
        if fallback_addr:
            payload["billing"] = {**payload.get("billing", {}), **fallback_addr}
            payload["shipping"] = {**payload.get("shipping", {}), **fallback_addr}
            billing_line1 = fallback_addr.get("address_1", "").strip()
            shipping_line1 = fallback_addr.get("address_1", "").strip()

    if not billing_line1 and not shipping_line1:
        LOGGER.error({
            "event": "woo_outbound_customer_missing_address",
            "customer": customer.name,
            "message": "Customer has no billing or shipping address",
        })
        raise ValueError("Customer has no billing or shipping address for WooCommerce")
    
    # Only include password for NEW customer creation
    if include_password:
        # Derive a deterministic password from the customer's phone number
        password_seed = re.sub(r"[^0-9A-Za-z]", "", phone_val)
        if not password_seed:
            password_seed = "OrderJarz123"
        if len(password_seed) < 8:
            password_seed = (password_seed + "12345678")[:12]
        payload["password"] = password_seed
    
    return payload


def sync_customer(customer_name: str, *, reason: str | None = None, force: bool = False) -> dict:
    settings, cfg = _get_settings()
    if not cfg.enable_customer_push and not force:
        return {"skipped": True, "reason": "disabled"}

    try:
        customer = frappe.get_doc("Customer", customer_name)
    except frappe.DoesNotExistError:
        return {"skipped": True, "reason": "missing"}

    if getattr(customer.flags, "ignore_woo_outbound", False) or getattr(frappe.flags, "ignore_woo_outbound", False):
        return {"skipped": True, "reason": "inbound"}

    # Check if this is a new customer (no woo_customer_id)
    woo_id = getattr(customer, "woo_customer_id", None)
    is_new_customer = not woo_id
    
    try:
        payload = _build_customer_payload(customer, include_password=True, include_username=is_new_customer)
    except ValueError as exc:
        LOGGER.warning({
            "event": "woo_outbound_customer_skipped",
            "customer": customer_name,
            "reason": "invalid_payload",
            "detail": str(exc),
        })
        _mark_customer_status(customer_name, status="error", error=str(exc))
        return {"status": "error", "detail": str(exc)}

    try:
        client = _build_client(settings)
    except ValueError:
        LOGGER.warning({"event": "woo_outbound_customer_skipped", "customer": customer_name, "reason": "missing_credentials"})
        return {"skipped": True, "reason": "missing_credentials"}

    try:
        if woo_id:
            response = client.put(f"customers/{woo_id}", payload)
        else:
            response = client.post("customers", payload)
    except WooAPIError as exc:
        if woo_id and exc.status_code == 404:
            # create anew if stored id is stale - include password and username for recreation
            payload_with_password = _build_customer_payload(customer, include_password=True, include_username=True)
            response = client.post("customers", payload_with_password)
        elif not woo_id and exc.status_code == 400 and "already registered" in exc.message.lower():
            # Customer exists in WooCommerce but we don't have the ID - reconcile
            LOGGER.info({
                "event": "woo_outbound_customer_reconcile",
                "customer": customer_name,
                "detail": "Customer exists in WooCommerce, searching by email to reconcile",
            })
            try:
                # Search for customer by email
                email = payload.get("email", "")
                search_result = client.get("customers", params={"email": email, "per_page": 1})
                if search_result and len(search_result) > 0:
                    existing_woo_customer = search_result[0]
                    woo_customer_id = existing_woo_customer.get("id")
                    LOGGER.info({
                        "event": "woo_outbound_customer_found",
                        "customer": customer_name,
                        "woo_id": woo_customer_id,
                        "email": email,
                    })
                    # Store the ID and retry as UPDATE (no password, no username)
                    frappe.db.set_value("Customer", customer_name, "woo_customer_id", str(woo_customer_id), update_modified=False)
                    frappe.db.commit()
                    # Rebuild payload for UPDATE (no password, no username)
                    update_payload = _build_customer_payload(customer, include_password=True, include_username=False)
                    # Retry with UPDATE
                    response = client.put(f"customers/{woo_customer_id}", update_payload)
                else:
                    raise ValueError(f"Could not find WooCommerce customer with email {email}")
            except Exception as search_exc:
                LOGGER.error({
                    "event": "woo_outbound_customer_reconcile_failed",
                    "customer": customer_name,
                    "error": str(search_exc),
                })
                _mark_customer_status(customer_name, status="error", error=f"Reconciliation failed: {str(search_exc)}")
                return {"status": "error", "detail": f"Reconciliation failed: {str(search_exc)}"}
        else:
            LOGGER.error({
                "event": "woo_outbound_customer_error",
                "customer": customer_name,
                "reason": reason,
                "status_code": exc.status_code,
                "message": exc.message,
            })
            _mark_customer_status(customer_name, status="error", error=exc.message)
            return {"status": "error", "detail": exc.message}

    woo_customer_id = response.get("id") if isinstance(response, dict) else None
    if woo_customer_id:
        frappe.db.set_value(
            "Customer",
            customer_name,
            {
                "woo_customer_id": str(woo_customer_id),
                "woo_outbound_status": "Synced",
                "woo_outbound_error": "",
                "woo_outbound_synced_on": now_datetime(),
            },
            update_modified=False,
        )
    else:
        _mark_customer_status(customer_name, status="Synced")

    LOGGER.info({"event": "woo_outbound_customer_synced", "customer": customer_name, "woo_id": woo_customer_id, "reason": reason})
    return {"status": "ok", "woo_customer_id": woo_customer_id}


# ---------------------------------------------------------------------------
# Sales Invoice outbound sync
# ---------------------------------------------------------------------------

def enqueue_invoice_sync(invoice: frappe.model.document.Document | str, method: str | None = None, *, reason: str = "event", force: bool = False, cancel: bool = False) -> None:
    settings, cfg = _get_settings()
    if not cfg.enable_order_push and not force:
        return
    if not isinstance(invoice, str):
        reason = reason if reason != "event" else (method or "event")
        cancel = cancel or method == "on_cancel"
        invoice_name = invoice.name
    else:
        invoice_name = invoice
    frappe.enqueue(
        "jarz_woocommerce_integration.services.outbound_sync.sync_sales_invoice",
        queue="short",
        timeout=600,
        now=force,
        invoice_name=invoice_name,
        reason=reason,
        cancel=cancel,
    )


def _mark_invoice_status(invoice_name: str, *, status: str, error: str | None = None) -> None:
    updates = {
        "woo_outbound_status": _normalize_outbound_status(status),
        "woo_outbound_synced_on": now_datetime(),
    }
    if error:
        updates["woo_outbound_error"] = error[:500]
    else:
        updates["woo_outbound_error"] = ""
    frappe.db.set_value("Sales Invoice", invoice_name, updates, update_modified=False)


def _parse_product_identifier(raw: Any) -> tuple[Optional[int], Optional[int]]:
    if raw is None:
        return None, None
    if isinstance(raw, int):
        return raw, None
    text = str(raw).strip()
    if not text:
        return None, None
    if ":" in text:
        left, right = text.split(":", 1)
        left_id = cint(left)
        right_id = cint(right)
        return (left_id or None, right_id or None)
    if text.isdigit():
        return int(text), None
    return None, None


def _format_money(value: float | int, precision: int = 2) -> str:
    return f"{flt(value, precision):.{precision}f}"


def _collect_line_items(invoice: frappe.model.document.Document) -> tuple[list[dict], list[str]]:
    line_items: list[dict] = []
    missing_products: list[str] = []
    for item in invoice.items:
        if getattr(item, "is_bundle_parent", 0):
            continue
        qty = flt(item.qty)
        if qty <= 0:
            continue
        product_row = frappe.db.get_value("Item", item.item_code, ["woo_product_id", "item_name"], as_dict=True)
        product_identifier = (product_row or {}).get("woo_product_id")
        product_id, variation_id = _parse_product_identifier(product_identifier)
        if not product_identifier or (product_id is None and variation_id is None):
            missing_products.append(item.item_code)
            continue
        subtotal_base = item.price_list_rate or item.rate
        subtotal = subtotal_base * qty if subtotal_base else item.amount
        entry = {
            "quantity": int(qty),
            "subtotal": _format_money(subtotal),
            "total": _format_money(item.amount),
            "name": item.item_name or item.item_code,
            "meta_data": [
                {"key": "erpnext_item_code", "value": item.item_code},
            ],
        }
        if product_id:
            entry["product_id"] = product_id
        if variation_id:
            entry["variation_id"] = variation_id
        if getattr(item, "discount_percentage", None):
            entry["meta_data"].append({"key": "discount_percentage", "value": flt(item.discount_percentage)})
        line_items.append(entry)
    return line_items, missing_products


def _compute_shipping_total(invoice: frappe.model.document.Document) -> float:
    shipping_total = 0.0
    for tax in getattr(invoice, "taxes", []) or []:
        description = (getattr(tax, "description", "") or "").lower()
        account = (getattr(tax, "account_head", "") or "").lower()
        if getattr(tax, "charge_type", "") == "Actual" and (
            "ship" in description
            or "delivery" in description
            or "ship" in account
            or "delivery" in account
        ):
            shipping_total += flt(getattr(tax, "tax_amount", 0))
    if shipping_total > 0:
        return shipping_total
    # fallback: detect explicit items that look like shipping rows
    for item in invoice.items:
        name = (item.item_name or item.description or "").lower()
        if "shipping" in name or "delivery" in name:
            shipping_total += flt(item.amount)
    return shipping_total


def _map_payment_method(invoice: frappe.model.document.Document, cfg: OutboundConfig) -> tuple[str, str]:
    raw_method = (
        getattr(invoice, "custom_payment_method", None)
        or getattr(invoice, "mode_of_payment", None)
        or "Cash on Delivery"
    )
    raw_lower = str(raw_method).strip().lower()
    if "insta" in raw_lower:
        return cfg.payment_instapay or "instapay", str(raw_method or "Instapay")
    if "wallet" in raw_lower:
        return cfg.payment_wallet or "wallet", str(raw_method or "Wallet")
    return cfg.payment_cod or "cod", str(raw_method or "Cash on Delivery")


def _determine_status(invoice: frappe.model.document.Document, *, cancel: bool = False) -> str:
    state = (getattr(invoice, "sales_invoice_state", None) or getattr(invoice, "custom_sales_invoice_state", None) or "").strip().lower()

    if cancel:
        return "cancelled"
    if state == "cancelled":
        return "cancelled"
    if invoice.docstatus == 2:
        return "cancelled"
    if state == "delivered":
        return "completed"
    return "processing"


def _extract_item_code(entry: dict) -> Optional[str]:
    for meta in entry.get("meta_data", []) or []:
        if meta.get("key") == "erpnext_item_code":
            return meta.get("value")
    return None


def _attach_existing_line_ids(line_items: list[dict], existing_line_items: list[dict]) -> tuple[list[dict], list[dict]]:
    if not existing_line_items:
        return line_items, []

    by_meta: dict[str, list[dict]] = {}
    by_product: dict[tuple[Optional[int], Optional[int]], list[dict]] = {}
    for existing in existing_line_items:
        code = None
        for md in existing.get("meta_data", []) or []:
            if md.get("key") == "erpnext_item_code":
                code = md.get("value")
                break
        if code:
            by_meta.setdefault(code, []).append(existing)
        
        pid = existing.get("product_id")
        vid = existing.get("variation_id")
        if vid == 0:
            vid = None
        key = (pid, vid)
        by_product.setdefault(key, []).append(existing)

    mapped: list[dict] = []
    unmapped: list[dict] = []
    for entry in line_items:
        match: Optional[dict] = None
        code = _extract_item_code(entry)
        if code and by_meta.get(code):
            match = by_meta[code].pop(0)
        else:
            pid = entry.get("product_id")
            vid = entry.get("variation_id")
            if vid == 0:
                vid = None
            key = (pid, vid)
            bucket = by_product.get(key) or []
            if bucket:
                match = bucket.pop(0)

        if match and match.get("id"):
            entry["id"] = match["id"]
            mapped.append(entry)
        else:
            unmapped.append(entry)
    return mapped, unmapped


def _build_order_payload(
    invoice: frappe.model.document.Document,
    cfg: OutboundConfig,
    *,
    cancel: bool = False,
    existing_order: Optional[dict] = None,
) -> dict:
    line_items, missing_products = _collect_line_items(invoice)
    unmapped_line_items: list[dict] = []
    if existing_order:
        matched, unmapped_line_items = _attach_existing_line_ids(line_items, existing_order.get("line_items") or [])
        line_items = matched

    if not line_items:
        raise ValueError("No line items available for Woo order payload")

    shipping_total = _compute_shipping_total(invoice)
    customer_doc = frappe.get_doc("Customer", invoice.customer)
    customer_payload = _build_customer_payload(customer_doc)

    payment_method, payment_title = _map_payment_method(invoice, cfg)
    set_paid = flt(getattr(invoice, "outstanding_amount", 0)) <= 0.01
    woo_status = _determine_status(invoice, cancel=cancel)

    # Prefer invoice addresses for both billing and shipping (order address must populate both)
    default_email = (
        (customer_payload.get("billing") or {}).get("email")
        or (customer_payload.get("shipping") or {}).get("email")
        or (getattr(customer_doc, "email_id", "") or "").strip()
    )
    default_phone = (
        (customer_payload.get("billing") or {}).get("phone")
        or (customer_payload.get("shipping") or {}).get("phone")
        or (getattr(customer_doc, "mobile_no", "") or getattr(customer_doc, "phone", "") or "").strip()
    )

    billing_address = {}
    shipping_address = {}
    invoice_billing_address = getattr(invoice, "customer_address", None)
    invoice_shipping_address = getattr(invoice, "shipping_address_name", None)

    if invoice_billing_address:
        billing_address = _get_address_payload(
            invoice_billing_address,
            fallback_name=customer_doc.customer_name,
            phone=default_phone,
            email=default_email,
        )
    if invoice_shipping_address:
        shipping_address = _get_address_payload(
            invoice_shipping_address,
            fallback_name=customer_doc.customer_name,
            phone=default_phone,
            email=default_email,
        )

    # Fallback to customer addresses only if invoice addresses are missing
    if not billing_address or not billing_address.get("address_1"):
        billing_address = customer_payload.get("billing") or {}
    if not shipping_address or not shipping_address.get("address_1"):
        shipping_address = customer_payload.get("shipping") or customer_payload.get("billing") or {}

    # Ensure both billing and shipping are populated with the order address
    if billing_address.get("address_1") and not shipping_address.get("address_1"):
        shipping_address = dict(billing_address)
    elif shipping_address.get("address_1") and not billing_address.get("address_1"):
        billing_address = dict(shipping_address)

    if default_phone:
        billing_address.setdefault("phone", default_phone)
        shipping_address.setdefault("phone", default_phone)
    if default_email:
        billing_address.setdefault("email", default_email)
        shipping_address.setdefault("email", default_email)

    # Final guardrail: do not push orders without any address
    if not (billing_address.get("address_1") or shipping_address.get("address_1")):
        LOGGER.error({
            "event": "woo_outbound_order_missing_address",
            "invoice": invoice.name,
            "customer": invoice.customer,
            "message": "Cannot push order to WooCommerce without billing or shipping address",
        })
        raise ValueError("No billing or shipping address available for Woo order")

    payload: dict = {
        "status": woo_status,
        "currency": invoice.currency,
        "payment_method": payment_method,
        "payment_method_title": payment_title,
        "set_paid": bool(set_paid),
        "line_items": line_items,
        "billing": billing_address,
        "shipping": shipping_address,
        "meta_data": [
            {"key": "erpnext_sales_invoice", "value": invoice.name},
        ],
    }
    
    # Add delivery date and time using WooCommerce Order Delivery Date plugin fields
    delivery_date = getattr(invoice, "custom_delivery_date", None) or getattr(invoice, "delivery_date", None)
    delivery_time = getattr(invoice, "custom_delivery_time", None) or getattr(invoice, "delivery_time", None)
    
    if delivery_date:
        from datetime import datetime, time as dt_time
        import time
        
        # Format date for display (e.g., "Friday, November 22, 2025")
        if isinstance(delivery_date, str):
            delivery_date_obj = datetime.strptime(delivery_date, "%Y-%m-%d").date()
        else:
            delivery_date_obj = delivery_date
        
        formatted_date = delivery_date_obj.strftime("%A, %B %d, %Y")
        
        # Combine date and time for timestamp
        if delivery_time:
            # Parse time if it's a string
            if isinstance(delivery_time, str):
                try:
                    time_obj = datetime.strptime(delivery_time, "%H:%M:%S").time()
                except ValueError:
                    try:
                        time_obj = datetime.strptime(delivery_time, "%H:%M").time()
                    except ValueError:
                        time_obj = dt_time(12, 0)  # Default to noon
            else:
                time_obj = delivery_time
            
            delivery_datetime = datetime.combine(delivery_date_obj, time_obj)
            time_slot_label = delivery_time if isinstance(delivery_time, str) else time_obj.strftime("%H:%M")
        else:
            delivery_datetime = datetime.combine(delivery_date_obj, dt_time(12, 0))
            time_slot_label = ""
        
        # Convert to Unix timestamp
        timestamp = int(time.mktime(delivery_datetime.timetuple()))
        
        # Add WooCommerce Order Delivery Date plugin fields
        payload["meta_data"].extend([
            {"key": "_orddd_timestamp", "value": str(timestamp)},
            {"key": "_orddd_delivery_date", "value": formatted_date},
            {"key": "Delivery Date", "value": formatted_date},
        ])
        
        if time_slot_label:
            payload["meta_data"].extend([
                {"key": "_orddd_time_slot", "value": time_slot_label},
                {"key": "Time Slot", "value": time_slot_label},
            ])
    
    woo_customer_id = getattr(customer_doc, "woo_customer_id", None)
    if woo_customer_id:
        payload["customer_id"] = cint(woo_customer_id)

    if shipping_total or cfg.shipping_method_id:
        shipping_entry: dict = {
            "method_id": cfg.shipping_method_id or "flat_rate",
            "method_title": cfg.shipping_method_title or "Shipping",
            "total": _format_money(shipping_total if shipping_total else 0),
        }
        # Attach existing shipping-line ID so WooCommerce updates the line
        # in-place instead of appending a brand-new one on every PUT.
        if existing_order:
            existing_shipping = existing_order.get("shipping_lines") or []
            if existing_shipping:
                shipping_entry["id"] = existing_shipping[0].get("id")
        payload["shipping_lines"] = [shipping_entry]

    if missing_products:
        raise MissingWooProductError(
            "Missing WooCommerce product mapping for items: " + ", ".join(missing_products)
        )
    if existing_order and unmapped_line_items:
        codes = [(_extract_item_code(entry) or entry.get("name") or "unknown") for entry in unmapped_line_items]
        payload.setdefault("meta_data", []).append({"key": "unmapped_line_items", "value": ",".join(codes)})
        LOGGER.warning({
            "event": "woo_outbound_unmapped_line_items",
            "invoice": invoice.name,
            "unmatched": codes,
        })
    return payload


def sync_sales_invoice(invoice_name: str, *, reason: str | None = None, cancel: bool = False, force: bool = False) -> dict:
    settings, cfg = _get_settings()
    if not cfg.enable_order_push and not force:
        return {"skipped": True, "reason": "disabled"}

    try:
        invoice = frappe.get_doc("Sales Invoice", invoice_name)
    except frappe.DoesNotExistError:
        return {"skipped": True, "reason": "missing"}

    if getattr(invoice.flags, "ignore_woo_outbound", False) or getattr(frappe.flags, "ignore_woo_outbound", False):
        return {"skipped": True, "reason": "inbound"}

    # Permanent guard: invoices that originated from WooCommerce (inbound)
    # must never be pushed back.  Only inbound sync creates Order Map entries.
    _woo_id = invoice.get("woo_order_id")
    if _woo_id and frappe.db.exists("WooCommerce Order Map", {"woo_order_id": _woo_id}):
        return {"skipped": True, "reason": "inbound_order"}

    if invoice.docstatus == 0 and not cancel:
        return {"skipped": True, "reason": "draft"}

    if invoice.docstatus == 2 and not cancel and not invoice.get("woo_order_id"):
        return {"skipped": True, "reason": "cancelled_without_remote"}

    try:
        client = _build_client(settings)
    except ValueError:
        LOGGER.warning({"event": "woo_outbound_invoice_skipped", "invoice": invoice_name, "reason": "missing_credentials"})
        return {"skipped": True, "reason": "missing_credentials"}

    # Ensure customer exists on Woo first
    customer_doc = frappe.get_doc("Customer", invoice.customer)
    if not getattr(customer_doc, "woo_customer_id", None) and cfg.enable_customer_push:
        sync_customer(customer_doc.name, reason="order_dependency", force=True)
        customer_doc = frappe.get_doc("Customer", invoice.customer)

    woo_order_id = getattr(invoice, "woo_order_id", None)
    existing_order: Optional[Dict[str, Any]] = None
    if woo_order_id:
        try:
            existing_order = client.get(f"orders/{woo_order_id}")
        except WooAPIError as exc:
            if exc.status_code == 404:
                existing_order = None
                woo_order_id = None
            else:
                LOGGER.error({
                    "event": "woo_outbound_invoice_fetch_error",
                    "invoice": invoice_name,
                    "status_code": exc.status_code,
                    "message": exc.message,
                    "reason": reason,
                })
                _mark_invoice_status(invoice_name, status="error", error=exc.message)
                return {"status": "error", "detail": exc.message}

    try:
        payload = _build_order_payload(invoice, cfg, cancel=cancel, existing_order=existing_order)
    except MissingWooProductError as exc:
        LOGGER.warning({
            "event": "woo_outbound_missing_product_mapping",
            "invoice": invoice_name,
            "detail": str(exc),
            "reason": reason,
        })
        _mark_invoice_status(invoice_name, status="error", error=str(exc))
        return {"status": "error", "detail": str(exc)}

    response: Dict[str, Any]
    try:
        if woo_order_id:
            response = client.put(f"orders/{woo_order_id}", payload)
        else:
            response = client.post("orders", payload)
    except WooAPIError as exc:
        if woo_order_id and exc.status_code == 404:
            response = client.post("orders", payload)
        else:
            LOGGER.error({
                "event": "woo_outbound_invoice_error",
                "invoice": invoice_name,
                "status_code": exc.status_code,
                "message": exc.message,
                "reason": reason,
            })
            _mark_invoice_status(invoice_name, status="error", error=exc.message)
            return {"status": "error", "detail": exc.message}

    woo_id = response.get("id") if isinstance(response, dict) else None
    woo_number = response.get("number") if isinstance(response, dict) else None
    updates = {
        "woo_outbound_status": "Synced",
        "woo_outbound_error": "",
        "woo_outbound_synced_on": now_datetime(),
    }
    if woo_id and not getattr(invoice, "woo_order_id", None):
        updates["woo_order_id"] = woo_id
    if woo_number:
        updates["woo_order_number"] = woo_number
    frappe.db.set_value("Sales Invoice", invoice_name, updates, update_modified=False)

    LOGGER.info({
        "event": "woo_outbound_invoice_synced",
        "invoice": invoice_name,
        "woo_order_id": woo_id,
        "reason": reason,
    })
    return {"status": "ok", "woo_order_id": woo_id}


# ---------------------------------------------------------------------------
# Reconciliation / garbage collection
# ---------------------------------------------------------------------------

def reconcile_outbound_state(batch_limit: int = 100) -> dict:
    settings, cfg = _get_settings()
    summary = {"customers": 0, "invoices": 0}
    if cfg.enable_customer_push:
        missing_customers = frappe.get_all(
            "Customer",
            filters={
                "disabled": 0,
                "woo_customer_id": ("in", ("", None)),
            },
            fields=["name"],
            limit=batch_limit,
        )
        error_customers = frappe.get_all(
            "Customer",
            filters={"woo_outbound_status": ["in", ["Error", "error"]]},
            fields=["name"],
            limit=batch_limit,
        )
        for row in (*missing_customers, *error_customers):
            enqueue_customer_sync(row.name, reason="reconcile")
            summary["customers"] += 1
    if cfg.enable_order_push:
        missing_invoices = frappe.get_all(
            "Sales Invoice",
            filters={
                "docstatus": 1,
                "woo_order_id": ("in", ("", None)),
            },
            fields=["name"],
            limit=batch_limit,
        )
        error_invoices = frappe.get_all(
            "Sales Invoice",
            filters={"woo_outbound_status": ["in", ["Error", "error"]], "docstatus": ("!=", 2)},
            fields=["name"],
            limit=batch_limit,
        )
        for row in (*missing_invoices, *error_invoices):
            enqueue_invoice_sync(row.name, reason="reconcile")
            summary["invoices"] += 1
    if summary["customers"] or summary["invoices"]:
        LOGGER.info({"event": "woo_outbound_reconcile_enqueued", "summary": summary})
    return summary
