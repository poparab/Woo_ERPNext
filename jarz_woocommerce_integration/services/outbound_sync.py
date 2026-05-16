from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date as dt_date, datetime, time as dt_time, timedelta
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
from jarz_woocommerce_integration.utils.customer_woo_id import (
    get_customer_woo_id,
    get_legacy_customer_woo_id,
    has_unmigrated_legacy_customer_woo_id,
    set_customer_woo_id,
)
from jarz_woocommerce_integration.utils.http_client import WooAPIError, WooClient

LOGGER = frappe.logger("jarz_woocommerce.outbound")

_ACCEPTANCE_ONLY_FIELDS = frozenset({
    "custom_acceptance_status",
    "custom_accepted_by",
    "custom_accepted_on",
})

_CUSTOMER_OUTBOUND_UPDATE_FIELDS = frozenset({
    "customer_name",
    "email_id",
    "mobile_no",
    "phone",
    "customer_shipping_address",
    "territory",
})

_CUSTOMER_CORE_OUTBOUND_UPDATE_FIELDS = frozenset({
    "customer_name",
    "email_id",
    "mobile_no",
    "phone",
})

_CUSTOMER_SHIPPING_OUTBOUND_UPDATE_FIELDS = frozenset({
    "customer_shipping_address",
})

_CUSTOMER_TERRITORY_OUTBOUND_UPDATE_FIELDS = frozenset({
    "territory",
})

_CUSTOMER_ADDRESS_OUTBOUND_UPDATE_FIELDS = frozenset({
    "address_line1",
    "address_line2",
    "city",
    "state",
    "pincode",
    "country",
    "phone",
    "email_id",
    "address_type",
    "is_shipping_address",
})

_ORDER_SYNC_META_KEYS_TO_COMPARE = frozenset({
    "_orddd_timestamp",
    "_orddd_delivery_date",
    "Delivery Date",
    "_orddd_time_slot",
    "Time Slot",
    "unmapped_line_items",
})

_OUTBOUND_RELEVANT_UPDATE_FIELDS = frozenset({
    "custom_sales_invoice_state",
    "sales_invoice_state",
    "docstatus",
    "customer",
    "currency",
    "outstanding_amount",
    "custom_payment_method",
    "mode_of_payment",
    "customer_address",
    "shipping_address_name",
    "custom_delivery_date",
    "delivery_date",
    "custom_delivery_time_from",
    "custom_delivery_duration",
    "custom_delivery_time",
    "delivery_time",
    "woo_order_id",
})

_INVOICE_OUTBOUND_DELIVERY_FIELDS = frozenset({
    "custom_delivery_date",
    "delivery_date",
    "custom_delivery_time_from",
    "custom_delivery_duration",
    "custom_delivery_time",
    "delivery_time",
})

_APPROVED_INVOICE_OUTBOUND_STATUSES = frozenset({
    "out-for-delivery",
    "completed",
    "cancelled",
})


def _resolve_order_map_link_field() -> str:
    try:
        cols = frappe.db.get_table_columns("WooCommerce Order Map") or []
    except Exception:
        cols = []
    if "erpnext_sales_invoice" in cols:
        return "erpnext_sales_invoice"
    if "sales_invoice" in cols:
        return "sales_invoice"
    return "erpnext_sales_invoice"


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


def _normalize_woo_address_lines(address_line1: str | None, address_line2: str | None) -> tuple[str, str]:
    line1 = (address_line1 or "").strip()
    line2 = (address_line2 or "").strip()
    if not line1 and line2:
        return line2, ""
    return line1, line2


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
    line1, line2 = _normalize_woo_address_lines(address.get("address_line1"), address.get("address_line2"))
    first, last = _split_contact_name(fallback_name)
    return {
        "first_name": first,
        "last_name": last,
        "company": fallback_name,
        "address_1": line1,
        "address_2": line2,
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
        line1, line2 = _normalize_woo_address_lines(addr.address_line1, addr.address_line2)
        return {
            "address_1": line1,
            "address_2": line2,
            "city": addr.city or "",
            "state": addr.state or "",
            "postcode": addr.pincode or "",
            "country": addr.country or "",
            "phone": addr.phone or "",
            "email": addr.email_id or "",
        }
    except Exception:
        return {}


def _get_linked_customer_addresses(customer_name: str) -> list[dict[str, Any]]:
    try:
        links = frappe.get_all(
            "Dynamic Link",
            filters={"link_doctype": "Customer", "link_name": customer_name, "parenttype": "Address"},
            fields=["parent"],
            order_by="modified desc",
        )
    except Exception:
        return []

    address_names = []
    for link in links or []:
        address_name = _get_doc_value(link, "parent")
        if address_name and address_name not in address_names:
            address_names.append(address_name)

    if not address_names:
        return []

    try:
        rows = frappe.get_all(
            "Address",
            filters={"name": ["in", address_names]},
            fields=["name", "address_type", "is_primary_address", "is_shipping_address"],
            order_by="modified desc",
        )
    except Exception:
        return []

    rows_by_name = {
        _get_doc_value(row, "name"): row
        for row in rows or []
        if _get_doc_value(row, "name")
    }
    ordered_rows = []
    for address_name in address_names:
        row = rows_by_name.get(address_name)
        if row:
            ordered_rows.append(row)
    return ordered_rows


def _resolve_customer_billing_address_name(customer: frappe.model.document.Document) -> str | None:
    explicit = getattr(customer, "customer_primary_address", None)
    if explicit:
        return explicit

    linked_addresses = _get_linked_customer_addresses(customer.name)
    for row in linked_addresses:
        if cint(_get_doc_value(row, "is_primary_address", 0)):
            return _get_doc_value(row, "name")
    for row in linked_addresses:
        if str(_get_doc_value(row, "address_type", "") or "").strip().lower() == "billing":
            return _get_doc_value(row, "name")
    for row in linked_addresses:
        address_name = _get_doc_value(row, "name")
        if address_name:
            return address_name
    return None


def _resolve_customer_shipping_address_name(customer: frappe.model.document.Document) -> str | None:
    explicit = getattr(customer, "customer_shipping_address", None)
    if explicit:
        return explicit

    linked_addresses = _get_linked_customer_addresses(customer.name)
    for row in linked_addresses:
        if cint(_get_doc_value(row, "is_shipping_address", 0)):
            return _get_doc_value(row, "name")
    for row in linked_addresses:
        if str(_get_doc_value(row, "address_type", "") or "").strip().lower() == "shipping":
            return _get_doc_value(row, "name")
    return None


def _build_customer_metadata(customer: frappe.model.document.Document) -> list[dict[str, str]]:
    territory = str(getattr(customer, "territory", "") or "").strip()
    if not territory:
        return []
    return [{"key": "erpnext_territory", "value": territory}]


def _get_doc_value(document: Any, fieldname: str, default: Any = None) -> Any:
    if document is None:
        return default
    getter = getattr(document, "get", None)
    if callable(getter):
        try:
            return getter(fieldname, default)
        except TypeError:
            try:
                return getter(fieldname)
            except Exception:
                pass
        except Exception:
            pass
    return getattr(document, fieldname, default)


def _get_doc_before_save(document: Any) -> Any:
    return getattr(document, "get_doc_before_save", lambda: None)()


def _get_changed_fields(document: Any, fieldnames: frozenset[str]) -> set[str]:
    return {fieldname for fieldname in fieldnames if _safe_has_value_changed(document, fieldname)}


def _is_outbound_suppressed(document: Any | None = None) -> bool:
    if getattr(getattr(document, "flags", None), "ignore_woo_outbound", False):
        return True
    try:
        return bool(getattr(frappe.flags, "ignore_woo_outbound", False))
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Customer outbound sync
# ---------------------------------------------------------------------------

def _serialize_customer_sync_scope(scopes: set[str]) -> str | None:
    normalized = sorted(scope for scope in scopes if scope)
    if not normalized:
        return None
    return ",".join(normalized)


def _derive_customer_sync_scope(
    customer: frappe.model.document.Document,
    *,
    method: str | None = None,
    force: bool = False,
) -> str | None:
    if force or method != "on_update":
        return None

    changed_fields = _get_changed_fields(customer, _CUSTOMER_OUTBOUND_UPDATE_FIELDS)
    scopes: set[str] = set()
    if changed_fields & _CUSTOMER_CORE_OUTBOUND_UPDATE_FIELDS:
        scopes.add("core")
    if changed_fields & _CUSTOMER_SHIPPING_OUTBOUND_UPDATE_FIELDS:
        scopes.add("shipping")
    if changed_fields & _CUSTOMER_TERRITORY_OUTBOUND_UPDATE_FIELDS:
        scopes.add("territory")
    return _serialize_customer_sync_scope(scopes)


def _address_is_shipping_relevant(address: Any) -> bool:
    if not address:
        return False
    if cint(_get_doc_value(address, "is_shipping_address", 0)):
        return True
    return str(_get_doc_value(address, "address_type", "") or "").strip().lower() == "shipping"


def _get_linked_customer_names_from_address(address: Any) -> list[str]:
    customer_names: set[str] = set()
    for candidate in (address, _get_doc_before_save(address)):
        for link in getattr(candidate, "links", []) or []:
            if _get_doc_value(link, "link_doctype") == "Customer":
                link_name = _get_doc_value(link, "link_name")
                if link_name:
                    customer_names.add(link_name)

    if customer_names:
        return sorted(customer_names)

    address_name = _get_doc_value(address, "name")
    if not address_name:
        return []

    try:
        links = frappe.get_all(
            "Dynamic Link",
            filters={"parenttype": "Address", "parent": address_name, "link_doctype": "Customer"},
            fields=["link_name"],
        )
    except Exception:
        return []

    for link in links or []:
        link_name = _get_doc_value(link, "link_name")
        if link_name:
            customer_names.add(link_name)
    return sorted(customer_names)


def _should_enqueue_customer_address_event(
    address: frappe.model.document.Document,
    *,
    method: str | None = None,
    force: bool = False,
) -> bool:
    if _is_outbound_suppressed(address):
        return False
    if force:
        return True

    previous = _get_doc_before_save(address)
    shipping_relevant_now = _address_is_shipping_relevant(address)
    shipping_relevant_before = _address_is_shipping_relevant(previous)

    if method == "after_insert":
        return shipping_relevant_now

    if not (shipping_relevant_now or shipping_relevant_before):
        return False

    if method != "on_update":
        return True

    if shipping_relevant_now != shipping_relevant_before:
        return True

    if _get_changed_fields(address, _CUSTOMER_ADDRESS_OUTBOUND_UPDATE_FIELDS):
        return True

    LOGGER.info({
        "event": "woo_outbound_customer_address_update_skipped",
        "address": getattr(address, "name", None),
        "method": method,
    })
    return False


def enqueue_linked_customer_sync_for_address(
    address: frappe.model.document.Document,
    method: str | None = None,
    *,
    reason: str = "event",
    force: bool = False,
) -> None:
    settings, cfg = _get_settings()
    if not cfg.enable_customer_push and not force:
        return
    if not _should_enqueue_customer_address_event(address, method=method, force=force):
        return

    customer_names = _get_linked_customer_names_from_address(address)
    if not customer_names:
        return

    enqueue_reason = reason if reason != "event" else (method or "address_event")
    for customer_name in customer_names:
        frappe.enqueue(
            "jarz_woocommerce_integration.services.outbound_sync.sync_customer",
            queue="short",
            timeout=300,
            now=force,
            customer_name=customer_name,
            reason=enqueue_reason,
            scope="shipping",
        )


def enqueue_customer_sync(
    customer: frappe.model.document.Document | str,
    method: str | None = None,
    *,
    reason: str = "event",
    force: bool = False,
    scope: str | None = None,
) -> None:
    settings, cfg = _get_settings()
    if not cfg.enable_customer_push and not force:
        return
    if not isinstance(customer, str):
        reason = reason if reason != "event" else (method or "event")
        if not _should_enqueue_customer_event(customer, method=method, force=force):
            return
        scope = scope or _derive_customer_sync_scope(customer, method=method, force=force)
        customer_name = customer.name
    else:
        if _is_outbound_suppressed():
            return
        customer_name = customer
    frappe.enqueue(
        "jarz_woocommerce_integration.services.outbound_sync.sync_customer",
        queue="short",
        timeout=300,
        now=force,
        customer_name=customer_name,
        reason=reason,
        scope=scope,
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


def _normalize_customer_sync_scopes(scope: str | None) -> set[str]:
    if not scope:
        return {"full"}
    scopes = {part.strip().lower() for part in str(scope).split(",") if part.strip()}
    if not scopes or "full" in scopes:
        return {"full"}
    return scopes


def _build_customer_payload(
    customer: frappe.model.document.Document,
    *,
    include_password: bool = False,
    include_username: bool = True,
    scope: str | None = None,
) -> dict:
    # Mobile number is mandatory for WooCommerce
    phone_val = (getattr(customer, "mobile_no", "") or getattr(customer, "phone", "") or "").strip()
    if not phone_val:
        # Try pulling phone from linked addresses
        addr_candidates = [
            _resolve_customer_billing_address_name(customer),
            _resolve_customer_shipping_address_name(customer),
        ]
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

    billing_address_name = _resolve_customer_billing_address_name(customer)
    shipping_address_name = _resolve_customer_shipping_address_name(customer)
    first, last = _split_contact_name(customer.customer_name)
    billing = _get_address_payload(
        billing_address_name,
        fallback_name=customer.customer_name,
        phone=phone_val,
        email=email,
    )
    shipping = _get_address_payload(
        shipping_address_name,
        fallback_name=customer.customer_name,
        phone=phone_val,
        email=email,
    )
    metadata = _build_customer_metadata(customer)
    scopes = _normalize_customer_sync_scopes(scope)

    if scopes == {"full"}:
        payload = {
            "email": email,
            "first_name": first,
            "last_name": last,
            "billing": billing,
            "shipping": shipping,
        }

        if include_username:
            username_field = getattr(customer, "woo_username", None) or email
            if username_field:
                payload["username"] = username_field

        payload.setdefault("billing", {})["phone"] = phone_val
        payload.setdefault("shipping", {})["phone"] = phone_val
        if metadata:
            payload["meta_data"] = metadata

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

        if include_password:
            password_seed = re.sub(r"[^0-9A-Za-z]", "", phone_val)
            if not password_seed:
                password_seed = "OrderJarz123"
            if len(password_seed) < 8:
                password_seed = (password_seed + "12345678")[:12]
            payload["password"] = password_seed

        return payload

    payload: dict[str, Any] = {}
    if "core" in scopes:
        payload["email"] = email
        payload["first_name"] = first
        payload["last_name"] = last
        if phone_val:
            payload.setdefault("billing", {})["phone"] = phone_val
            payload.setdefault("shipping", {})["phone"] = phone_val

    if "shipping" in scopes:
        shipping_payload = dict(shipping)
        if phone_val:
            shipping_payload.setdefault("phone", phone_val)
        if email:
            shipping_payload.setdefault("email", email)
        payload["shipping"] = shipping_payload

    if "territory" in scopes and metadata:
        payload["meta_data"] = metadata

    return payload


def sync_customer(
    customer_name: str,
    *,
    reason: str | None = None,
    force: bool = False,
    scope: str | None = None,
) -> dict:
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
    woo_id = get_customer_woo_id(customer)
    if not woo_id and has_unmigrated_legacy_customer_woo_id(customer):
        legacy_woo_id = get_legacy_customer_woo_id(customer)
        detail = (
            f"Customer {customer_name} still has legacy Woo ID {legacy_woo_id}; "
            "run the customer Woo ID migration before outbound sync"
        )
        LOGGER.warning({
            "event": "woo_outbound_customer_legacy_id_blocked",
            "customer": customer_name,
            "legacy_woo_id": legacy_woo_id,
            "reason": reason,
        })
        _mark_customer_status(customer_name, status="error", error=detail)
        return {"status": "error", "detail": detail}
    is_new_customer = not woo_id
    effective_scope = None if is_new_customer else scope
    
    try:
        payload = _build_customer_payload(
            customer,
            include_password=is_new_customer,
            include_username=is_new_customer,
            scope=effective_scope,
        )
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
                    set_customer_woo_id(customer_name, woo_customer_id, update_modified=False)
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
        if not _should_enqueue_invoice_event(invoice, method=method, cancel=cancel, force=force):
            return
        invoice_name = invoice.name
    else:
        if _is_outbound_suppressed():
            return
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


def _is_truthy_flag(value: Any) -> bool:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"", "0", "false", "no", "none", "null"}:
            return False
        if normalized in {"1", "true", "yes"}:
            return True
    try:
        return bool(cint(value))
    except Exception:
        return bool(value)


def _get_registered_bundle_product_ids(invoice: frappe.model.document.Document) -> set[str]:
    item_codes = {
        str(getattr(item, "item_code", "") or "").strip()
        for item in getattr(invoice, "items", []) or []
        if getattr(item, "item_code", None)
    }
    if not item_codes:
        return set()
    try:
        item_rows = frappe.get_all(
            "Item",
            filters={"name": ("in", sorted(item_codes))},
            fields=["woo_product_id"],
        )
    except Exception:
        return set()

    woo_product_ids = {
        str(row.get("woo_product_id") or "").strip()
        for row in item_rows
        if row.get("woo_product_id")
    }
    if not woo_product_ids:
        return set()

    try:
        bundle_ids = frappe.get_all(
            "Woo Jarz Bundle",
            filters={"woo_bundle_id": ("in", sorted(woo_product_ids))},
            pluck="woo_bundle_id",
        )
    except Exception:
        return set()

    return {str(bundle_id).strip() for bundle_id in bundle_ids if bundle_id}


def _get_item_product_row(item_code: str, *, cache: dict[str, dict[str, Any]]) -> dict[str, Any]:
    key = str(item_code or "").strip()
    if not key:
        return {}
    if key not in cache:
        row = frappe.db.get_value("Item", key, ["woo_product_id", "item_name"], as_dict=True)
        cache[key] = dict(row or {})
    return cache[key]


def _get_bundle_link_key(item: frappe.model.document.Document) -> str:
    for fieldname in ("parent_bundle", "bundle_code"):
        value = str(getattr(item, fieldname, "") or "").strip()
        if value:
            return value
    return ""


def _is_explicit_bundle_parent_item(item: frappe.model.document.Document) -> bool:
    return _is_truthy_flag(getattr(item, "is_bundle_parent", 0))


def _is_explicit_bundle_child_item(item: frappe.model.document.Document) -> bool:
    return _is_truthy_flag(getattr(item, "is_bundle_child", 0)) or bool(str(getattr(item, "parent_bundle", "") or "").strip())


def _collect_explicit_bundle_parent_product_ids(
    invoice: frappe.model.document.Document,
    *,
    item_product_rows: dict[str, dict[str, Any]],
) -> dict[str, int]:
    parent_product_ids: dict[str, int] = {}
    for item in getattr(invoice, "items", []) or []:
        if not _is_explicit_bundle_parent_item(item):
            continue
        bundle_key = _get_bundle_link_key(item)
        item_code = str(getattr(item, "item_code", "") or "").strip()
        if not bundle_key or not item_code:
            continue
        product_identifier = _get_item_product_row(item_code, cache=item_product_rows).get("woo_product_id")
        product_id, variation_id = _parse_product_identifier(product_identifier)
        if variation_id is None and product_id is not None:
            parent_product_ids[bundle_key] = product_id
    return parent_product_ids


def _is_bundle_parent_item(
    item: frappe.model.document.Document,
    *,
    product_identifier: str | None,
    registered_bundle_product_ids: set[str],
) -> bool:
    if getattr(item, "is_bundle_parent", 0):
        return True

    product_id, variation_id = _parse_product_identifier(product_identifier)
    if variation_id is not None or product_id is None:
        return False

    if str(product_id) not in registered_bundle_product_ids:
        return False

    return (
        flt(getattr(item, "price_list_rate", 0)) > 0
        and flt(getattr(item, "rate", 0)) <= 0
        and flt(getattr(item, "amount", 0)) <= 0
        and flt(getattr(item, "discount_percentage", 0)) >= 100
    )


def _collect_line_items(invoice: frappe.model.document.Document) -> tuple[list[dict], list[str]]:
    line_items: list[dict] = []
    missing_products: list[str] = []
    registered_bundle_product_ids = _get_registered_bundle_product_ids(invoice)
    item_product_rows: dict[str, dict[str, Any]] = {}
    explicit_bundle_parent_product_ids = _collect_explicit_bundle_parent_product_ids(
        invoice,
        item_product_rows=item_product_rows,
    )

    for item in invoice.items:
        qty = flt(item.qty)
        if qty <= 0:
            continue
        product_row = _get_item_product_row(item.item_code, cache=item_product_rows)
        product_identifier = (product_row or {}).get("woo_product_id")
        product_id, variation_id = _parse_product_identifier(product_identifier)

        if not product_identifier or (product_id is None and variation_id is None):
            missing_products.append(item.item_code)
            continue

        if _is_explicit_bundle_parent_item(item):
            entry = {
                "quantity": int(qty),
                "subtotal": _format_money(0),
                "total": _format_money(0),
                "name": item.item_name or item.item_code,
                "meta_data": [
                    {"key": "erpnext_item_code", "value": item.item_code},
                ],
            }
            if product_id:
                entry["product_id"] = product_id
            if variation_id:
                entry["variation_id"] = variation_id
            line_items.append(entry)
            continue

        if _is_bundle_parent_item(
            item,
            product_identifier=product_identifier,
            registered_bundle_product_ids=registered_bundle_product_ids,
        ):
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

        if _is_explicit_bundle_child_item(item):
            parent_product_id = explicit_bundle_parent_product_ids.get(_get_bundle_link_key(item))
            if parent_product_id:
                entry["meta_data"].append({"key": "_woosb_parent_id", "value": str(parent_product_id)})

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


def _normalize_invoice_state(raw_state: Any) -> str:
    return re.sub(r"[\s_]+", "-", str(raw_state or "").strip().lower())


def _collect_invoice_states(invoice: frappe.model.document.Document) -> list[str]:
    states: list[str] = []
    seen: set[str] = set()
    for fieldname in ("custom_sales_invoice_state", "sales_invoice_state"):
        state = _normalize_invoice_state(getattr(invoice, fieldname, None))
        if state and state not in seen:
            seen.add(state)
            states.append(state)
    return states


def _determine_status(invoice: frappe.model.document.Document, *, cancel: bool = False) -> str:
    states = _collect_invoice_states(invoice)

    if cancel:
        return "cancelled"
    if invoice.docstatus == 2:
        return "cancelled"
    if any(state in {"cancelled", "canceled"} for state in states):
        return "cancelled"
    if any(state in {"delivered", "completed"} for state in states):
        return "completed"
    if any(state == "out-for-delivery" for state in states):
        return "out-for-delivery"
    return "processing"


def _safe_has_value_changed(invoice: frappe.model.document.Document, fieldname: str) -> bool:
    try:
        return bool(invoice.has_value_changed(fieldname))
    except Exception:
        previous = _get_doc_before_save(invoice)
        if not previous:
            return False
        return _get_doc_value(previous, fieldname) != _get_doc_value(invoice, fieldname)


def _has_any_value_changed(document: frappe.model.document.Document, fieldnames: frozenset[str]) -> bool:
    return any(_safe_has_value_changed(document, fieldname) for fieldname in fieldnames)


def _serialize_invoice_item_rows(invoice: frappe.model.document.Document) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for item in getattr(invoice, "items", []) or []:
        rows.append((
            str(_get_doc_value(item, "item_code", "") or "").strip(),
            flt(_get_doc_value(item, "qty", 0)),
            flt(_get_doc_value(item, "rate", 0)),
            flt(_get_doc_value(item, "amount", 0)),
            flt(_get_doc_value(item, "price_list_rate", 0)),
            flt(_get_doc_value(item, "discount_percentage", 0)),
            cint(_get_doc_value(item, "is_bundle_parent", 0)),
            cint(_get_doc_value(item, "is_bundle_child", 0)),
            str(_get_doc_value(item, "parent_bundle", "") or "").strip(),
            str(_get_doc_value(item, "bundle_code", "") or "").strip(),
        ))
    return sorted(rows)


def _invoice_items_changed(invoice: frappe.model.document.Document) -> bool:
    previous = _get_doc_before_save(invoice)
    if not previous:
        return False
    return _serialize_invoice_item_rows(previous) != _serialize_invoice_item_rows(invoice)


def _has_missing_or_stale_woo_order_mapping(invoice: frappe.model.document.Document) -> bool:
    woo_order_id = _get_doc_value(invoice, "woo_order_id")
    if not woo_order_id:
        return True
    try:
        return not bool(frappe.db.exists("WooCommerce Order Map", {"woo_order_id": woo_order_id}))
    except Exception:
        return False


def _has_approved_invoice_status_change(
    invoice: frappe.model.document.Document,
    *,
    cancel: bool = False,
) -> bool:
    previous = _get_doc_before_save(invoice)
    if not previous:
        return False

    current_status = _determine_status(invoice, cancel=cancel)
    previous_status = _determine_status(previous, cancel=False)
    return current_status in _APPROVED_INVOICE_OUTBOUND_STATUSES and current_status != previous_status


def _should_enqueue_customer_event(
    customer: frappe.model.document.Document,
    *,
    method: str | None = None,
    force: bool = False,
) -> bool:
    if _is_outbound_suppressed(customer):
        return False
    if force:
        return True
    if method != "on_update":
        return True
    if _has_any_value_changed(customer, _CUSTOMER_OUTBOUND_UPDATE_FIELDS):
        return True

    LOGGER.info({
        "event": "woo_outbound_customer_update_skipped",
        "customer": getattr(customer, "name", None),
        "method": method,
    })
    return False


def _should_enqueue_invoice_event(
    invoice: frappe.model.document.Document,
    *,
    method: str | None = None,
    cancel: bool = False,
    force: bool = False,
) -> bool:
    if _is_outbound_suppressed(invoice):
        return False
    if force:
        return True
    if cancel or method in {None, "on_submit", "on_cancel"}:
        return True
    if method != "on_update_after_submit":
        return True
    if _has_missing_or_stale_woo_order_mapping(invoice):
        return True
    if _should_skip_acceptance_only_update(invoice, method=method, cancel=cancel):
        return False
    if _has_approved_invoice_status_change(invoice, cancel=cancel):
        return True
    if _has_any_value_changed(invoice, _INVOICE_OUTBOUND_DELIVERY_FIELDS):
        return True
    if _invoice_items_changed(invoice):
        return True

    LOGGER.info({
        "event": "woo_outbound_invoice_update_skipped",
        "invoice": getattr(invoice, "name", None),
        "method": method,
    })
    return False


def _should_skip_acceptance_only_update(
    invoice: frappe.model.document.Document,
    *,
    method: str | None = None,
    cancel: bool = False,
) -> bool:
    if cancel or method != "on_update_after_submit":
        return False

    previous = _get_doc_before_save(invoice)
    if not previous:
        return False

    if not _has_any_value_changed(invoice, _ACCEPTANCE_ONLY_FIELDS):
        return False

    if _has_any_value_changed(invoice, _OUTBOUND_RELEVANT_UPDATE_FIELDS):
        return False

    if _invoice_items_changed(invoice):
        return False

    if _determine_status(previous, cancel=cancel) != _determine_status(invoice, cancel=cancel):
        return False

    LOGGER.info({
        "event": "woo_outbound_invoice_acceptance_only_skipped",
        "invoice": invoice.name,
        "method": method,
    })
    return True


def _recover_amended_invoice_woo_order_id(invoice: frappe.model.document.Document) -> Optional[int]:
    amended_from = str(getattr(invoice, "amended_from", "") or invoice.get("amended_from") or "").strip()
    if not amended_from:
        return None

    try:
        source_woo_order_id = frappe.db.get_value("Sales Invoice", amended_from, "woo_order_id")
        if source_woo_order_id:
            return cint(source_woo_order_id) or None
    except Exception:
        pass

    link_field = _resolve_order_map_link_field()
    try:
        map_row = frappe.db.get_value(
            "WooCommerce Order Map",
            {link_field: amended_from},
            ["woo_order_id"],
            as_dict=True,
        )
        recovered = (map_row or {}).get("woo_order_id") if isinstance(map_row, dict) else None
        return cint(recovered) or None
    except Exception:
        return None


def _relink_order_map_to_invoice(woo_order_id: int | str | None, invoice_name: str) -> None:
    if not woo_order_id or not invoice_name:
        return

    link_field = _resolve_order_map_link_field()
    try:
        map_name = frappe.db.get_value("WooCommerce Order Map", {"woo_order_id": woo_order_id}, "name")
    except Exception:
        map_name = None
    if not map_name:
        return

    try:
        frappe.db.set_value(
            "WooCommerce Order Map",
            map_name,
            {link_field: invoice_name},
            update_modified=False,
        )
    except Exception:
        LOGGER.warning({
            "event": "woo_outbound_order_map_relink_failed",
            "invoice": invoice_name,
            "woo_order_id": woo_order_id,
            "link_field": link_field,
        })


def _extract_item_code(entry: dict) -> Optional[str]:
    for meta in entry.get("meta_data", []) or []:
        if meta.get("key") == "erpnext_item_code":
            return meta.get("value")
    return None


def _extract_meta_value(entry: dict, key: str) -> Optional[str]:
    for meta in entry.get("meta_data", []) or []:
        if meta.get("key") == key:
            value = meta.get("value")
            if value in (None, ""):
                return None
            return str(value)
    return None


def _line_product_key(entry: dict) -> tuple[Optional[int], Optional[int]]:
    product_id = cint(entry.get("product_id") or 0) or None
    variation_id = cint(entry.get("variation_id") or 0) or None
    return product_id, variation_id


def _consume_matching_existing_line(
    entry: dict,
    remaining: list[dict],
    predicate,
) -> Optional[dict]:
    for index, candidate in enumerate(remaining):
        if predicate(candidate):
            return remaining.pop(index)
    return None


def _attach_existing_line_ids(line_items: list[dict], existing_line_items: list[dict]) -> tuple[list[dict], list[dict]]:
    if not existing_line_items:
        return line_items, []

    remaining: list[dict] = []
    for existing in existing_line_items:
        remaining.append({
            "entry": existing,
            "item_code": _extract_item_code(existing),
            "product_key": _line_product_key(existing),
            "bundle_parent_id": _extract_meta_value(existing, "_woosb_parent_id"),
            "quantity": flt(existing.get("quantity") or 0),
        })

    mapped: list[dict] = []
    unmapped: list[dict] = []
    for entry in line_items:
        match: Optional[dict] = None
        code = _extract_item_code(entry)
        if code:
            match = _consume_matching_existing_line(
                entry,
                remaining,
                lambda candidate: candidate.get("item_code") == code,
            )

        if not match:
            desired_product_key = _line_product_key(entry)
            match = _consume_matching_existing_line(
                entry,
                remaining,
                lambda candidate: candidate.get("product_key") == desired_product_key,
            )

        if not match:
            desired_bundle_parent_id = _extract_meta_value(entry, "_woosb_parent_id")
            desired_quantity = flt(entry.get("quantity") or 0)
            if desired_bundle_parent_id:
                match = _consume_matching_existing_line(
                    entry,
                    remaining,
                    lambda candidate: (
                        candidate.get("bundle_parent_id") == desired_bundle_parent_id
                        and candidate.get("quantity") == desired_quantity
                    ),
                )

        if match and match.get("entry", {}).get("id"):
            entry["id"] = match["entry"]["id"]
            mapped.append(entry)
        else:
            unmapped.append(entry)
    return mapped, unmapped


def _meta_entries_to_map(entries: list[dict] | None) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for entry in entries or []:
        key = str(entry.get("key") or "").strip()
        if key:
            result[key] = entry.get("value")
    return result


def _normalize_order_line_items(entries: list[dict] | None) -> list[tuple[Any, ...]]:
    normalized: list[tuple[Any, ...]] = []
    for entry in entries or []:
        variation_id = entry.get("variation_id")
        if variation_id == 0:
            variation_id = None
        metadata = tuple(sorted(
            (
                str(meta.get("key") or ""),
                str(meta.get("value") or ""),
            )
            for meta in entry.get("meta_data", []) or []
            if meta.get("key")
        ))
        normalized.append((
            cint(entry.get("id") or 0),
            cint(entry.get("product_id") or 0),
            cint(variation_id or 0),
            flt(entry.get("quantity") or 0),
            str(entry.get("subtotal") or ""),
            str(entry.get("total") or ""),
            metadata,
        ))
    return sorted(normalized)


def _normalize_order_shipping_lines(entries: list[dict] | None) -> list[tuple[str, str, str]]:
    normalized = []
    for entry in entries or []:
        normalized.append((
            str(entry.get("method_id") or ""),
            str(entry.get("method_title") or ""),
            str(entry.get("total") or ""),
        ))
    return sorted(normalized)


def _order_payload_requires_update(existing_order: dict, payload: dict) -> bool:
    current_status = str(existing_order.get("status") or "").strip().lower()
    desired_status = str(payload.get("status") or "").strip().lower()
    if current_status != desired_status:
        return True

    payload_customer_id = payload.get("customer_id")
    if payload_customer_id and str(existing_order.get("customer_id") or "") != str(payload_customer_id):
        return True

    desired_meta = {
        key: value
        for key, value in _meta_entries_to_map(payload.get("meta_data") or []).items()
        if key in _ORDER_SYNC_META_KEYS_TO_COMPARE
    }
    if desired_meta:
        existing_meta = _meta_entries_to_map(existing_order.get("meta_data") or [])
        for key, value in desired_meta.items():
            if str(existing_meta.get(key) or "") != str(value or ""):
                return True

    payload_line_items = payload.get("line_items") or []
    if payload_line_items:
        existing_line_items = existing_order.get("line_items") or []
        if _normalize_order_line_items(payload_line_items) != _normalize_order_line_items(existing_line_items):
            return True

    payload_shipping_lines = payload.get("shipping_lines") or []
    if payload_shipping_lines:
        existing_shipping_lines = existing_order.get("shipping_lines") or []
        if _normalize_order_shipping_lines(payload_shipping_lines) != _normalize_order_shipping_lines(existing_shipping_lines):
            return True

    return False


def _build_order_payload(
    invoice: frappe.model.document.Document,
    cfg: OutboundConfig,
    *,
    cancel: bool = False,
    existing_order: Optional[dict] = None,
) -> dict:
    line_items, missing_products = _collect_line_items(invoice)
    payload_line_items = list(line_items)
    unmapped_line_items: list[dict] = []
    if existing_order:
        matched, unmapped_line_items = _attach_existing_line_ids(line_items, existing_order.get("line_items") or [])
        payload_line_items = matched

    if not payload_line_items and not existing_order:
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
        "billing": billing_address,
        "shipping": shipping_address,
        "meta_data": [
            {"key": "erpnext_sales_invoice", "value": invoice.name},
        ],
    }

    if payload_line_items:
        payload["line_items"] = payload_line_items
    payload["meta_data"].extend(_build_delivery_metadata(invoice))
    
    woo_customer_id = get_customer_woo_id(customer_doc)
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

    if not payload.get("line_items"):
        LOGGER.info({
            "event": "woo_outbound_status_only_update",
            "invoice": invoice.name,
            "woo_order_id": getattr(invoice, "woo_order_id", None),
            "reason": "no_line_item_matches",
        })
    return payload


def _coerce_delivery_date(raw: Any) -> dt_date | None:
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, dt_date):
        return raw
    if isinstance(raw, str):
        value = raw.strip()
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None


def _coerce_delivery_time(raw: Any) -> dt_time | None:
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw.time().replace(microsecond=0)
    if isinstance(raw, dt_time):
        return raw.replace(microsecond=0)
    if isinstance(raw, str):
        value = raw.strip()
        for fmt in ("%H:%M:%S.%f", "%H:%M:%S", "%H:%M"):
            try:
                return datetime.strptime(value, fmt).time().replace(microsecond=0)
            except ValueError:
                continue
    return None


def _coerce_delivery_duration_seconds(raw: Any) -> int | None:
    if raw in (None, ""):
        return None
    if isinstance(raw, str):
        value = raw.strip()
        if not value:
            return None
        if ":" in value:
            parts = value.split(":")
            if len(parts) == 3:
                try:
                    hours, minutes, seconds = [int(float(part)) for part in parts]
                    return max(0, hours * 3600 + minutes * 60 + seconds)
                except ValueError:
                    return None
        try:
            return max(0, int(float(value)))
        except ValueError:
            return None
    try:
        return max(0, int(float(raw)))
    except (TypeError, ValueError):
        return None


def _build_delivery_metadata(invoice: frappe.model.document.Document) -> list[dict[str, str]]:
    delivery_date = _coerce_delivery_date(
        getattr(invoice, "custom_delivery_date", None) or getattr(invoice, "delivery_date", None)
    )
    if not delivery_date:
        return []

    formatted_date = delivery_date.strftime("%A, %B %d, %Y")
    midnight_timestamp = calendar.timegm(datetime.combine(delivery_date, dt_time(0, 0)).timetuple())
    metadata = [
        {"key": "_orddd_timestamp", "value": str(midnight_timestamp)},
        {"key": "_orddd_delivery_date", "value": formatted_date},
        {"key": "Delivery Date", "value": formatted_date},
    ]

    delivery_time_from = _coerce_delivery_time(getattr(invoice, "custom_delivery_time_from", None))
    delivery_duration_seconds = _coerce_delivery_duration_seconds(getattr(invoice, "custom_delivery_duration", None))

    time_slot_label = None
    if delivery_time_from and delivery_duration_seconds and delivery_duration_seconds > 0:
        end_datetime = datetime.combine(delivery_date, delivery_time_from) + timedelta(seconds=delivery_duration_seconds)
        time_slot_label = f"{delivery_time_from.strftime('%H:%M')} - {end_datetime.strftime('%H:%M')}"
    else:
        legacy_delivery_time = _coerce_delivery_time(
            getattr(invoice, "custom_delivery_time", None) or getattr(invoice, "delivery_time", None)
        )
        if legacy_delivery_time:
            time_slot_label = legacy_delivery_time.strftime("%H:%M")

    if time_slot_label:
        metadata.extend([
            {"key": "_orddd_time_slot", "value": time_slot_label},
            {"key": "Time Slot", "value": time_slot_label},
        ])

    return metadata


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

    # When the invoice was just created as the replacement half of a Woo-initiated
    # amendment, Woo already holds the authoritative state — suppress the outbound push.
    if getattr(invoice.flags, "skip_woo_outbound_after_amend", False):
        return {"skipped": True, "reason": "amendment_no_push"}

    _woo_id = invoice.get("woo_order_id") or _recover_amended_invoice_woo_order_id(invoice)
    order_map_exists = bool(_woo_id and frappe.db.exists("WooCommerce Order Map", {"woo_order_id": _woo_id}))

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
    if not get_customer_woo_id(customer_doc) and has_unmigrated_legacy_customer_woo_id(customer_doc):
        legacy_woo_id = get_legacy_customer_woo_id(customer_doc)
        detail = (
            f"Customer {customer_doc.name} still has legacy Woo ID {legacy_woo_id}; "
            "run the customer Woo ID migration before outbound order sync"
        )
        LOGGER.warning({
            "event": "woo_outbound_invoice_legacy_customer_id_blocked",
            "invoice": invoice_name,
            "customer": customer_doc.name,
            "legacy_woo_id": legacy_woo_id,
            "reason": reason,
        })
        _mark_invoice_status(invoice_name, status="error", error=detail)
        return {"status": "error", "detail": detail}

    if not get_customer_woo_id(customer_doc) and cfg.enable_customer_push:
        sync_customer(customer_doc.name, reason="order_dependency", force=True)
        customer_doc = frappe.get_doc("Customer", invoice.customer)

    original_woo_order_id = getattr(invoice, "woo_order_id", None)
    woo_order_id = original_woo_order_id or _recover_amended_invoice_woo_order_id(invoice)
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

    if order_map_exists and woo_order_id and existing_order and not _order_payload_requires_update(existing_order, payload):
        _mark_invoice_status(invoice_name, status="Synced")
        LOGGER.info({
            "event": "woo_outbound_invoice_already_in_sync",
            "invoice": invoice_name,
            "woo_order_id": woo_order_id,
            "status": payload.get("status"),
            "reason": reason,
        })
        return {"skipped": True, "reason": "already_in_sync", "woo_order_id": woo_order_id}

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

    # --- Verify WooCommerce accepted the desired status ------------------
    # Some WordPress plugins (e.g. delivery management) silently override the
    # status in the same PUT response.  If the response status doesn't match
    # what we sent, do not mark as Synced — flag as error so the reconcile
    # will retry.
    actual_woo_status = (response.get("status") or "") if isinstance(response, dict) else ""
    desired_woo_status = payload.get("status") or ""
    if actual_woo_status and desired_woo_status and actual_woo_status != desired_woo_status:
        LOGGER.warning({
            "event": "woo_outbound_status_mismatch",
            "invoice": invoice_name,
            "desired_status": desired_woo_status,
            "actual_status": actual_woo_status,
            "woo_order_id": woo_order_id,
            "reason": reason,
        })
        _mark_invoice_status(
            invoice_name,
            status="error",
            error=f"WooCommerce returned status={actual_woo_status!r}; expected {desired_woo_status!r}",
        )
        return {"status": "error", "detail": f"status_mismatch:{actual_woo_status}"}
    # ---------------------------------------------------------------------

    updates = {
        "woo_outbound_status": "Synced",
        "woo_outbound_error": "",
        "woo_outbound_synced_on": now_datetime(),
    }
    if woo_id and str(woo_id) != str(original_woo_order_id or ""):
        updates["woo_order_id"] = woo_id
    if woo_number:
        updates["woo_order_number"] = woo_number
    frappe.db.set_value("Sales Invoice", invoice_name, updates, update_modified=False)
    _relink_order_map_to_invoice(woo_id or woo_order_id, invoice_name)

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
