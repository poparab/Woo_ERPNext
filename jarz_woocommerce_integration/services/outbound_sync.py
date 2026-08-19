from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date as dt_date, datetime, time as dt_time, timedelta, timezone
import hashlib
import importlib
import json
import re
from typing import Any, Dict, Optional

import frappe
try:
    _frappe_utils = importlib.import_module("frappe.utils")
except ImportError:  # pragma: no cover - allow type checkers without frappe
    _frappe_utils = importlib.import_module("frappe.utils.data")

cint = getattr(_frappe_utils, "cint")
flt = getattr(_frappe_utils, "flt")
get_datetime = getattr(_frappe_utils, "get_datetime", None)
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
from jarz_woocommerce_integration.services import payment_map, tracking_link
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
    "_orddd_timeslot_timestamp",
    "Time Slot",
    # The customer tracking link. Compared so that minting a token on an
    # already-synced order still produces a PUT -- without this the order would
    # be dropped as "unchanged" and the customer would never get a Track button.
    # Safe for the historical backlog: the keys are only compared when the
    # *payload* carries them, and it only carries them for invoices that
    # actually have a token, so nothing old is marked dirty.
    *tracking_link.TRACKING_META_KEYS,
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
    # The promo engine writes the header discount after submit. Without these
    # the invoice reached the store at full price (F-01).
    "discount_amount",
    "apply_discount_on",
    # Minting the tracking token is often the *only* change on an
    # already-submitted invoice, and it happens after submit. Without these the
    # hook would classify it as "nothing outbound changed" and the store would
    # never receive the link.
    *tracking_link.INVOICE_TRACKING_FIELDS,
})

_INVOICE_OUTBOUND_DELIVERY_FIELDS = frozenset({
    "custom_delivery_date",
    "delivery_date",
    "custom_delivery_time_from",
    "custom_delivery_duration",
    "custom_delivery_time",
    "delivery_time",
})

_INVOICE_OUTBOUND_PAYLOAD_FIELDS = frozenset({
    "customer",
    "currency",
    "outstanding_amount",
    "custom_payment_method",
    "mode_of_payment",
    "customer_address",
    "shipping_address_name",
    "woo_order_id",
    # A header discount is part of the payload (it becomes a negative fee line),
    # so a change to it must re-push the order — F-01.
    "discount_amount",
    "apply_discount_on",
    *tracking_link.INVOICE_TRACKING_FIELDS,
})

_APPROVED_INVOICE_OUTBOUND_STATUSES = frozenset({
    "out-for-delivery",
    "completed",
    "cancelled",
    "refunded",
})

#: Line-item meta keys **we** own. Only these may take part in the
#: already-in-sync comparison: WooCommerce adds keys of its own to every line
#: (``_reduced_stock``, the variation attributes), so comparing the whole
#: ``meta_data`` tuple meant our payload could never equal the store's and every
#: order looked dirty forever — a PUT per sync, on every order (F-18).
_ORDER_LINE_META_KEYS_TO_COMPARE = frozenset({
    "erpnext_item_code",
    "discount_percentage",
    "_woosb_parent_id",
    "_woosb_ids",
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


def _note_outbound_disabled(switch: str) -> None:
	"""Make the master outbound kill-switch visible when it silently drops work.

	``enable_outbound_customers`` / ``enable_outbound_orders`` are checked before
	every outbox gate, and both call sites used to be a bare ``return``. Staging
	sat with them off from 2026-06-12 to 2026-08-01: zero outbound events, zero
	log lines, and a sync dashboard that showed every visible flag green.

	Rate-limited to one Error Log per switch per hour — this fires on every doc
	event, so it must not become the spam that gets ignored. Deliberately an
	Error Log rather than logger().warning(), which is below the servers'
	default log level and would never be seen.
	"""
	try:
		cache_key = f"woo_outbound_disabled::{switch}"
		if frappe.cache().get_value(cache_key):
			return
		frappe.cache().set_value(cache_key, "1", expires_in_sec=3600)
		frappe.log_error(
			title=f"WooCommerce: outbound push skipped ({switch} is off)",
			message=(
				f"WooCommerce Settings.{switch} is disabled, so outbound work is being "
				f"dropped before it reaches the outbox. No sync events are being created "
				f"for it and nothing is retried.\n\n"
				f"If this is not deliberate, re-enable it in WooCommerce Settings → "
				f"Outbound Sync. Note that re-enabling enable_outbound_orders lets the "
				f"hourly reconcile sweep push a backlog of submitted invoices to the "
				f"store as new orders — enable it deliberately, not by reflex."
			),
		)
	except Exception:  # noqa: BLE001
		pass


def _get_settings() -> tuple[WooCommerceSettings, OutboundConfig]:
    settings = WooCommerceSettings.get_settings()
    cfg = OutboundConfig(
        enable_customer_push=bool(getattr(settings, "enable_outbound_customers", 0)),
        enable_order_push=bool(getattr(settings, "enable_outbound_orders", 0)),
        payment_cod=(getattr(settings, "payment_method_cod", None) or "cod").strip(),
        payment_instapay=(getattr(settings, "payment_method_instapay", None) or "instapay").strip(),
        payment_wallet=(getattr(settings, "payment_method_wallet", None) or "wallet").strip(),
        # Deliberately NOT defaulted here. An unset value must stay empty so
        # `_build_shipping_line` can tell "the operator configured flat_rate"
        # from "nobody ever set this" and pick the native title accordingly.
        shipping_method_id=(getattr(settings, "default_shipping_method_id", None) or "").strip(),
        shipping_method_title=(getattr(settings, "default_shipping_method_title", None) or "").strip(),
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


_TERRITORY_ZONE_LOOKUP_FIELDS = ("custom_woo_code", "woo_code", "custom_territory_name_ar")


def _territory_has_column(fieldname: str) -> bool:
    try:
        return bool(frappe.db.has_column("Territory", fieldname))
    except Exception:
        return False


def _resolve_leaf_territory(value: Any) -> str | None:
    """Resolve a city/state label to a Territory, ignoring group territories.

    Delivery zones are always leaves, so requiring ``is_group = 0`` keeps a
    free-text WooCommerce city like "Cairo" from matching the governorate group
    and overwriting a perfectly good zone.
    """
    text = str(value or "").strip()
    if not text:
        return None

    from jarz_woocommerce_integration.services.customer_sync import _resolve_territory_from_state

    try:
        territory = _resolve_territory_from_state(text)
    except Exception:
        return None
    if not territory:
        return None

    try:
        if cint(frappe.db.get_value("Territory", territory, "is_group")):
            return None
    except Exception:
        return None
    return territory


def _territory_woo_zone_label(territory: str | None) -> str:
    """Return the delivery-zone string WooCommerce keeps in an address ``state``."""
    territory = str(territory or "").strip()
    if not territory:
        return ""

    from jarz_woocommerce_integration.services.territory_sync import CODE_TO_DISPLAY

    fields = ["territory_name"] + [
        fieldname for fieldname in _TERRITORY_ZONE_LOOKUP_FIELDS if _territory_has_column(fieldname)
    ]
    try:
        row = frappe.db.get_value("Territory", territory, fields, as_dict=True) or {}
    except Exception:
        row = {}

    for code in (row.get("custom_woo_code"), row.get("woo_code"), territory):
        display = CODE_TO_DISPLAY.get(str(code or "").strip())
        if display:
            return display

    english = str(row.get("territory_name") or "").strip()
    arabic = str(row.get("custom_territory_name_ar") or "").strip()
    if english and arabic and english != arabic:
        return f"{english} - {arabic}"
    return english or arabic or ""


def _is_payload_dry_run() -> bool:
    """Whether payload building must not touch the database.

    Set ``frappe.flags.woo_payload_dry_run = True`` to build an order payload
    for inspection (diffing our wire format against a native order, say) without
    the address realignment writing back. Off in every real code path.
    """
    try:
        return bool(getattr(frappe.flags, "woo_payload_dry_run", False))
    except Exception:  # noqa: BLE001
        return False


def _realign_address_state_with_territory(address_name: str | None, city: Any, state: Any) -> str:
    """Return the ``state`` to push, realigned with the address's territory.

    A territory change made in ERPNext is recorded by writing the Territory into
    ``Address.city``; ``Address.state`` keeps whatever delivery zone WooCommerce
    sent first. Pushing both verbatim left the store showing the new zone in
    ``city`` and the stale one in ``state`` — and the store bills shipping off
    ``state``.

    The realigned value is written back to the Address too: inbound order
    matching hashes city + state, so a stored value that disagrees with the
    store's would fork a duplicate Address on the next pull.

    ``realign_address_state`` runs the same logic from the Address doc event, so
    an address whose order is never pushed no longer keeps a ``state`` that
    disagrees with its ``city`` (F-22).
    """
    territory = _resolve_leaf_territory(city)
    if not territory:
        return str(state or "")

    label = _territory_woo_zone_label(territory)
    current = str(state or "")
    if not label or label == current:
        return current

    if address_name and not _is_payload_dry_run():
        try:
            frappe.db.set_value("Address", address_name, "state", label, update_modified=False)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning({
                "event": "woo_outbound_address_state_writeback_failed",
                "address": address_name,
                "territory": territory,
                "error": str(exc),
            })
    LOGGER.info({
        "event": "woo_outbound_address_state_realigned",
        "address": address_name,
        "territory": territory,
        "previous_state": current,
        "state": label,
    })
    return label


#: Address fields that can put ``state`` out of step with ``city``.
_ADDRESS_TERRITORY_FIELDS = frozenset({"city", "state"})


def realign_address_state(address: frappe.model.document.Document, method: str | None = None) -> None:
    """Doc-event entry point: keep ``Address.state`` in step with its territory.

    Realignment used to happen *only* as a side effect of building an outbound
    order payload, so an address belonging to an order that was never pushed
    kept a ``state`` that contradicted its ``city`` indefinitely — and inbound
    order matching hashes both, so the next pull forked a duplicate Address
    (F-22).

    A doc event rather than a scheduled sweep, deliberately: the two fields that
    can drift are written here and nowhere else, so the event fires exactly when
    drift becomes possible and never otherwise. A sweep would re-resolve every
    Address in the system on a timer to find the handful that moved.

    Three guards, all necessary:

    * ``ignore_woo_outbound`` — during an inbound sync the store's values are
      authoritative and rewriting them here would fight the puller;
    * a re-entrancy flag — the write-back is a ``db.set_value``, which does not
      re-fire doc events today, but this must not become a recursion if that
      ever changes;
    * a changed-fields check on ``on_update`` — every Address save would
      otherwise pay for a territory resolution it cannot need.

    Never raises: an address save must not fail because a delivery zone could
    not be resolved.
    """
    try:
        if _is_outbound_suppressed(address):
            return
        if getattr(frappe.flags, "woo_realigning_address_state", False):
            return
        if method == "on_update" and not _get_changed_fields(address, _ADDRESS_TERRITORY_FIELDS):
            return

        address_name = _get_doc_value(address, "name")
        city = _get_doc_value(address, "city")
        state = _get_doc_value(address, "state")
        if not address_name or not str(city or "").strip():
            return

        frappe.flags.woo_realigning_address_state = True
        try:
            realigned = _realign_address_state_with_territory(address_name, city, state)
        finally:
            frappe.flags.woo_realigning_address_state = False

        # Keep the in-memory document consistent with what was just written, so
        # anything reading the doc later in this request sees the same value.
        if realigned and realigned != str(state or ""):
            try:
                address.state = realigned
            except Exception:  # noqa: BLE001
                pass
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_address_state_realign_hook_failed",
            "address": _get_doc_value(address, "name"),
            "method": method,
            "error": str(exc),
        })


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
    state = _realign_address_state_with_territory(
        address_name,
        address.get("city"),
        address.get("state"),
    )
    return {
        "first_name": first,
        "last_name": last,
        "company": fallback_name,
        "address_1": line1,
        "address_2": line2,
        "city": address.get("city") or "",
        "state": state,
        "postcode": address.get("pincode") or "",
        "country": address.get("country") or "",
        "phone": address.get("phone") or phone or "",
        "email": address.get("email_id") or email or "",
    }


def _get_invoice_order_fallback_name(
    invoice: frappe.model.document.Document,
    customer: frappe.model.document.Document,
    *fieldnames: str,
) -> str:
    for fieldname in fieldnames:
        value = str(getattr(invoice, fieldname, "") or "").strip()
        if value:
            return value
    invoice_customer_name = str(getattr(invoice, "customer_name", "") or "").strip()
    if invoice_customer_name:
        return invoice_customer_name
    return str(getattr(customer, "customer_name", "") or "").strip()


def _apply_address_contact_name(address: dict, fallback_name: str) -> None:
    if not address or not fallback_name:
        return
    first, last = _split_contact_name(fallback_name)
    address["first_name"] = first
    address["last_name"] = last
    address["company"] = fallback_name


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
            "state": _realign_address_state_with_territory(addr.name, addr.city, addr.state),
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
        _note_outbound_disabled("enable_outbound_customers")
        return
    if not _should_enqueue_customer_address_event(address, method=method, force=force):
        return

    customer_names = _get_linked_customer_names_from_address(address)
    if not customer_names:
        return

    enqueue_reason = reason if reason != "event" else (method or "address_event")
    for customer_name in customer_names:
        from jarz_woocommerce_integration.services import sync_events

        if sync_events.should_use_customer_outbox(settings, reason=enqueue_reason):
            event = None
            try:
                event = sync_events.create_outbound_customer_event(
                    customer_name,
                    reason=enqueue_reason,
                    scope="shipping",
                    force=force,
                )
            except Exception as exc:  # noqa: BLE001
                if not sync_events.is_shadow_mode_enabled(settings):
                    raise
                sync_events.report_shadow_insert_failure(
                    "outbound_customer_address",
                    exc,
                    settings=settings,
                    context={
                        "customer_name": customer_name,
                        "reason": enqueue_reason,
                        "scope": "shipping",
                    },
                )
            if not sync_events.is_shadow_mode_enabled(settings):
                sync_events.enqueue_sync_event(event.name, after_commit=not force)
                continue

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
        _note_outbound_disabled("enable_outbound_customers")
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

    from jarz_woocommerce_integration.services import sync_events

    if sync_events.should_use_customer_outbox(settings, reason=reason):
        event = None
        try:
            event = sync_events.create_outbound_customer_event(
                customer_name,
                reason=reason,
                scope=scope,
                force=force,
            )
        except Exception as exc:  # noqa: BLE001
            if not sync_events.is_shadow_mode_enabled(settings):
                raise
            sync_events.report_shadow_insert_failure(
                "outbound_customer_enqueue",
                exc,
                settings=settings,
                context={
                    "customer_name": customer_name,
                    "reason": reason,
                    "scope": scope,
                },
            )
        if not sync_events.is_shadow_mode_enabled(settings):
            sync_events.enqueue_sync_event(event.name, after_commit=not force)
            return

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
        email = f"{sanitized_name}@placeholder.com"
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
        _note_outbound_disabled("enable_outbound_customers")
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
        _note_outbound_disabled("enable_outbound_orders")
        return
    if not isinstance(invoice, str):
        reason = reason if reason != "event" else (method or "event")
        cancel = cancel or method == "on_cancel"
        # A credit note is never pushed as an order, but it is not nothing
        # either: it changes what the customer's ORDER means. Routed to its own
        # handler (F-16) instead of being dropped on the floor.
        if _is_credit_note(invoice):
            _enqueue_invoice_return_sync(invoice, method=method, force=force)
            return
        if not _should_enqueue_invoice_event(invoice, method=method, cancel=cancel, force=force):
            return
        invoice_name = invoice.name
    else:
        if _is_outbound_suppressed():
            return
        invoice_name = invoice

    from jarz_woocommerce_integration.services import sync_events

    if sync_events.should_use_invoice_outbox(settings, reason=reason):
        event = None
        try:
            event = sync_events.create_outbound_invoice_event(
                invoice_name,
                reason=reason,
                cancel=cancel,
                force=force,
            )
        except Exception as exc:  # noqa: BLE001
            if not sync_events.is_shadow_mode_enabled(settings):
                raise
            sync_events.report_shadow_insert_failure(
                "outbound_invoice_enqueue",
                exc,
                settings=settings,
                context={
                    "invoice_name": invoice_name,
                    "reason": reason,
                    "cancel": cancel,
                },
            )
        if not sync_events.is_shadow_mode_enabled(settings):
            sync_events.enqueue_sync_event(event.name, after_commit=not force)
            return

    frappe.enqueue(
        "jarz_woocommerce_integration.services.outbound_sync.sync_sales_invoice",
        queue="short",
        timeout=600,
        now=force,
        invoice_name=invoice_name,
        reason=reason,
        cancel=cancel,
    )


def _is_credit_note(invoice: Any) -> bool:
    try:
        return bool(cint(_get_doc_value(invoice, "is_return", 0)))
    except Exception:  # noqa: BLE001
        return False


def _enqueue_invoice_return_sync(
    credit_note: frappe.model.document.Document,
    *,
    method: str | None = None,
    force: bool = False,
) -> None:
    """Queue the Woo-side reflection of a submitted (or cancelled) credit note.

    Credit notes are still never pushed *as orders* — see the guard in
    ``sync_sales_invoice``. What changed is that they no longer vanish: a return
    is the one event that can make the store's copy of an order wrong in the
    customer's favour, and it used to leave the order reading ``completed``
    forever (F-16).
    """
    # Global suppression only — deliberately NOT the credit note's own doc flag.
    #
    # jarz_pos's return builder sets `cn.flags.ignore_woo_outbound = True` (and
    # blanks woo_order_id/woo_order_number) to stop the credit note being pushed
    # AS AN ORDER: those ids are copyable custom fields, so an unguarded push
    # would re-open a completed order, or create a brand-new one once the id was
    # blank. That concern is real, but it is already answered structurally —
    # `_should_enqueue_invoice_event` refuses `is_return`, `sync_sales_invoice`
    # refuses it again, and `enqueue_invoice_sync` routes every credit note here
    # instead of down the order path. This handler never pushes the credit note.
    #
    # Honouring the doc flag here therefore disabled the whole F-16 reflection on
    # the one path that matters: a return posted through the POS workflow
    # returned at this line and the customer's order silently stayed "completed".
    # The Desk/`make_return_doc` path did not set the flag, which is why tests
    # that built credit notes that way saw it working.
    #
    # The GLOBAL flag is still honoured: it means "we are inside inbound sync,
    # touch nothing on the store", and that must keep winning.
    if _is_outbound_suppressed():
        return
    if method not in (None, "on_submit", "on_cancel") and not force:
        return

    credit_note_name = str(_get_doc_value(credit_note, "name", "") or "").strip()
    return_against = str(_get_doc_value(credit_note, "return_against", "") or "").strip()
    if not credit_note_name or not return_against:
        return

    frappe.enqueue(
        "jarz_woocommerce_integration.services.outbound_sync.sync_invoice_return",
        queue="short",
        timeout=600,
        enqueue_after_commit=not force,
        now=force,
        credit_note_name=credit_note_name,
    )


def _get_original_invoice_return_row(invoice_name: str) -> dict[str, Any]:
    """``grand_total`` / ``currency`` / ``custom_return_status`` for an invoice.

    ``custom_return_status`` belongs to jarz_pos, so the query is guarded: on a
    bench without that app the column does not exist and asking for it raises
    rather than returning a partial row. Falling back to the two stock fields
    keeps this app inert there instead of broken.
    """
    fields = ["grand_total", "currency", "custom_return_status"]
    try:
        row = frappe.db.get_value("Sales Invoice", invoice_name, fields, as_dict=True)
    except Exception:  # noqa: BLE001
        row = frappe.db.get_value(
            "Sales Invoice", invoice_name, ["grand_total", "currency"], as_dict=True
        )
    return dict(row or {})


def _return_is_full(
    return_status: Any,
    *,
    returned_total: float,
    original_total: float,
) -> bool:
    """Whether a return covers the whole order.

    The canonical ``custom_return_status`` decides when it says so. It is set
    across two saves, though, so a blank — or a value still reading
    ``Partially Returned`` while a second credit note lands — must not be taken
    as proof of the opposite. The credit-note sum stays underneath it as the
    accounting answer that cannot lag.
    """
    if str(return_status or "").strip().casefold() == _RETURN_STATUS_FULL:
        return True
    return original_total > 0 and returned_total >= original_total - _RETURN_TOLERANCE


def sync_invoice_return(credit_note_name: str) -> dict:
    """Reflect an ERPNext credit note onto the ORIGINAL WooCommerce order.

    Never pushes the credit note itself — it is a local accounting document, not
    an order. Instead:

    * a **full** return — ``custom_return_status`` on the original reads
      ``Fully Returned``, or the submitted credit notes add up to its grand
      total — drives the original order to ``refunded``; ``_determine_status``
      derives that on its own, so this simply re-syncs the original invoice;
    * a **partial** return leaves the status alone — the order really was
      delivered — and records what came back as a Woo order note, so the
      customer-facing history is honest without lying about the state.

    Cancelling a credit note runs the same path: ``_returned_amount`` drops back
    and the original leaves ``refunded`` by itself.
    """
    settings, cfg = _get_settings()
    if not cfg.enable_order_push:
        _note_outbound_disabled("enable_outbound_orders")
        return {"skipped": True, "reason": "disabled"}

    try:
        credit_note = frappe.get_doc("Sales Invoice", credit_note_name)
    except frappe.DoesNotExistError:
        return {"skipped": True, "reason": "missing"}

    if not _is_credit_note(credit_note):
        return {"skipped": True, "reason": "not_a_credit_note"}

    original_invoice = str(_get_doc_value(credit_note, "return_against", "") or "").strip()
    if not original_invoice:
        return {"skipped": True, "reason": "no_return_against"}

    woo_order_id = _resolve_note_woo_order_id(original_invoice)
    if not woo_order_id:
        # The original never reached the store, so there is no order to correct.
        return {"skipped": True, "reason": "no_woo_order", "invoice": original_invoice}

    original_row = _get_original_invoice_return_row(original_invoice)
    original_total = flt(original_row.get("grand_total") or 0)
    returned_total = _returned_amount(original_invoice)
    is_full = _return_is_full(
        original_row.get("custom_return_status"),
        returned_total=returned_total,
        original_total=original_total,
    )

    currency = str(original_row.get("currency") or "").strip()
    currency_suffix = f" {currency}" if currency else ""
    if returned_total <= _RETURN_TOLERANCE:
        # Every credit note against this order has been cancelled.
        body = (
            f"Jarz POS: the return against this order was reversed — nothing is "
            f"credited any more. Credit note: {credit_note_name}."
        )
    else:
        body = (
            f"Jarz POS: {'Full' if is_full else 'Partial'} return recorded against this "
            f"order ({returned_total:.2f}{currency_suffix} of "
            f"{original_total:.2f}{currency_suffix} returned). "
            f"Credit note: {credit_note_name}."
        )

    try:
        client = _build_client(settings)
    except ValueError:
        return {"skipped": True, "reason": "missing_credentials"}

    note_outcome = _post_woo_order_note(client, woo_order_id, body)
    return_status = str(original_row.get("custom_return_status") or "").strip()
    LOGGER.info({
        "event": "woo_outbound_return_noted",
        "credit_note": credit_note_name,
        "invoice": original_invoice,
        "woo_order_id": woo_order_id,
        "returned_total": returned_total,
        "original_total": original_total,
        "return_status": return_status or None,
        "full_return": is_full,
        "note": note_outcome,
    })

    # The canonical flag is written across two saves, so it can legitimately lag
    # the credit notes for a moment. A disagreement that persists means the two
    # have genuinely diverged, and the accounting is what we acted on.
    if return_status.casefold() == _RETURN_STATUS_PARTIAL and is_full:
        LOGGER.warning({
            "event": "woo_outbound_return_status_disagrees_with_credit_notes",
            "invoice": original_invoice,
            "return_status": return_status,
            "returned_total": returned_total,
            "original_total": original_total,
            "detail": (
                "custom_return_status still reads Partially Returned while the submitted "
                "credit notes cover the whole invoice; the order was pushed as refunded."
            ),
        })

    result = {
        "status": "ok",
        "woo_order_id": woo_order_id,
        "invoice": original_invoice,
        "full_return": is_full,
        "note": note_outcome,
    }

    # Always re-sync the original, not just on a full return. Crossing the
    # full/partial line in *either* direction changes the Woo status: a full
    # return makes it `refunded`, and cancelling that credit note has to take it
    # back off `refunded` again. `_order_payload_requires_update` drops the PUT
    # when nothing actually moved, so a partial return costs one GET.
    # Run inline: we are already in a background job and re-queueing adds a hop.
    result["order_sync"] = sync_sales_invoice(
        original_invoice,
        reason="credit_note_full" if is_full else "credit_note_partial",
    )
    return result


#: The POS keeps its order notes in a DocType owned by jarz_pos. This app never
#: imports that app — it listens for the doc event by name only, so the hook is
#: simply inert wherever jarz_pos is not installed.
_INVOICE_NOTE_DOCTYPE = "Jarz Invoice Note"


def _resolve_note_woo_order_id(invoice_name: str) -> str:
    woo_order_id = str(frappe.db.get_value("Sales Invoice", invoice_name, "woo_order_id") or "").strip()
    return "" if woo_order_id in ("", "0") else woo_order_id


def enqueue_invoice_note_sync(note: frappe.model.document.Document, method: str | None = None) -> None:
    """Push a POS order note out to the linked WooCommerce order."""
    settings, cfg = _get_settings()
    if not cfg.enable_order_push:
        _note_outbound_disabled("enable_outbound_orders")
        return
    if _is_outbound_suppressed(note):
        return

    note_text = str(getattr(note, "note", "") or "").strip()
    invoice_name = str(getattr(note, "sales_invoice", "") or "").strip()
    if not note_text or not invoice_name:
        return
    if not _resolve_note_woo_order_id(invoice_name):
        return

    frappe.enqueue(
        "jarz_woocommerce_integration.services.outbound_sync.sync_invoice_note",
        queue="short",
        timeout=300,
        enqueue_after_commit=True,
        note_name=note.name,
    )


def _post_woo_order_note(client: WooClient, woo_order_id: Any, body: str) -> str:
    """Post a private order note, once. Returns ``posted``/``already_posted``/an error.

    A queued job can be retried and WooCommerce has no natural key for a note,
    so an unguarded post shows the same message twice in the order screen.
    """
    body = str(body or "").strip()
    if not woo_order_id or not body:
        return "empty_note"

    try:
        for existing in client.get(f"orders/{woo_order_id}/notes") or []:
            if str(existing.get("note") or "").strip() == body:
                return "already_posted"
    except WooAPIError:
        pass  # a failed pre-check must not stop the note itself

    try:
        client.post(f"orders/{woo_order_id}/notes", {"note": body, "customer_note": False})
    except WooAPIError as exc:
        return exc.message or f"note_post_failed:{exc.status_code}"
    return "posted"


def _report_unpayable_transition(client: WooClient, woo_order_id: Any, invoice_name: str) -> None:
    """Record the one-way ``set_paid`` limitation on the order itself.

    See ``_detect_unpayable_transition``. Nothing here can un-pay the order, so
    the only honest thing to do is make the divergence visible to a human. The
    note text is fixed so the idempotency check in ``_post_woo_order_note``
    keeps it to one note per order rather than one per sync.
    """
    if not woo_order_id:
        return
    body = (
        "Jarz POS: this order is no longer settled in ERPNext (the invoice has an "
        "outstanding balance again), but WooCommerce cannot be moved back to "
        "unpaid over the API — set_paid only ever marks an order paid. The store "
        "will keep showing this order as paid until someone clears the payment in "
        f"wp-admin. ERPNext invoice: {invoice_name}."
    )
    outcome = _post_woo_order_note(client, woo_order_id, body)
    LOGGER.warning({
        "event": "woo_outbound_unpayable_transition",
        "invoice": invoice_name,
        "woo_order_id": woo_order_id,
        "note": outcome,
    })


#: Tolerance for the outbound total assertion: one piastre.
_TOTAL_ASSERT_TOLERANCE = 0.01


def _payload_total_mismatch(
    payload: dict | None,
    invoice: frappe.model.document.Document,
) -> str | None:
    """Error string when the payload's own arithmetic misses ``grand_total``.

    The counterpart to :func:`_check_response_total`, run BEFORE the request
    rather than after it. Same reasoning about why the sum must equal
    ``grand_total`` (no VAT rows on this site), and the same narrowing: only
    asserted when the payload actually carries ``line_items``, because a
    status-only update deliberately sends no money at all.

    Returns ``None`` when there is nothing to complain about — including when
    the payload is empty or unreadable, since a guard that cannot compute an
    answer must not block a push.
    """
    if not payload or not payload.get("line_items"):
        return None
    try:
        total = 0.0
        for group in ("line_items", "shipping_lines", "fee_lines"):
            for row in payload.get(group) or []:
                # A removal entry carries an id and no total; it moves no money.
                if row.get("total") in (None, ""):
                    continue
                total += flt(row.get("total"))
        grand_total = flt(getattr(invoice, "grand_total", 0) or 0)
    except Exception:  # noqa: BLE001
        return None

    line_count = len(payload.get("line_items") or [])
    tolerance = _TOTAL_ASSERT_TOLERANCE * (1 + line_count)
    if abs(total - grand_total) <= tolerance:
        return None
    return (
        f"Payload total {total:.2f} does not reach grand_total {grand_total:.2f} "
        f"(difference {grand_total - total:+.2f}). A line was most likely dropped "
        f"for want of a WooCommerce product mapping; the order was NOT pushed."
    )


def _check_response_total(
    response: Any,
    invoice: frappe.model.document.Document,
    *,
    invoice_name: str,
    payload: dict | None = None,
    reason: str | None = None,
) -> str | None:
    """Error string when the store's order total disagrees with ``grand_total``.

    This site has no VAT rows — the only tax row in use is the shipping-income
    ``Actual`` charge — so an order total is exactly
    ``sum(line totals) + shipping - header discount``, which is ``grand_total``.
    Any drift means a component did not survive the push (a dropped line, a
    discount that never became a fee line, a shipping row read wrong), and until
    now that was completely silent: the invoice was marked Synced regardless.

    **Only asserted when the payload actually carried the money.** A status-only
    update sends no ``line_items``, so the store's total is whatever it already
    held — and for the 10,651 Woo-origin orders that can legitimately differ from
    ``grand_total``, because inbound deliberately ignores Woo prices and rebuilds
    every line from the ERPNext price list. Asserting there would convert a
    working pipeline into a wall of false errors.

    The tolerance grows by one piastre per line: each line total is rounded to
    two decimals independently, so a long order can drift by a few cents without
    anything being wrong. A real failure is orders of magnitude larger — the
    live F-01 case is 640 EGP on a 30 EGP invoice.
    """
    if not isinstance(response, dict):
        return None
    line_items = (payload or {}).get("line_items") or []
    if not line_items:
        return None
    raw_total = response.get("total")
    if raw_total in (None, ""):
        return None

    store_total = flt(raw_total)
    invoice_total = flt(_get_doc_value(invoice, "grand_total", 0))
    difference = abs(store_total - invoice_total)
    tolerance = _TOTAL_ASSERT_TOLERANCE * (1 + len(line_items))
    if difference <= tolerance:
        return None

    LOGGER.error({
        "event": "woo_outbound_total_mismatch",
        "invoice": invoice_name,
        "woo_order_id": response.get("id"),
        "store_total": store_total,
        "invoice_grand_total": invoice_total,
        "difference": difference,
        "reason": reason,
    })
    return (
        f"WooCommerce order total {store_total:.2f} does not match invoice "
        f"grand_total {invoice_total:.2f} (difference {difference:.2f})"
    )


def sync_invoice_note(note_name: str) -> dict:
    """Post one POS order note to its Woo order as a private order note."""
    settings, cfg = _get_settings()
    if not cfg.enable_order_push:
        return {"status": "skipped", "reason": "outbound_disabled"}

    try:
        note = frappe.get_doc(_INVOICE_NOTE_DOCTYPE, note_name)
    except frappe.DoesNotExistError:
        return {"status": "skipped", "reason": "note_missing"}

    note_text = str(getattr(note, "note", "") or "").strip()
    invoice_name = str(getattr(note, "sales_invoice", "") or "").strip()
    if not note_text or not invoice_name:
        return {"status": "skipped", "reason": "empty_note"}

    woo_order_id = _resolve_note_woo_order_id(invoice_name)
    if not woo_order_id:
        return {"status": "skipped", "reason": "no_woo_order"}

    author = str(getattr(note, "added_by_full_name", "") or "").strip()
    body = f"{note_text}\n\n— {author} (Jarz POS)" if author else f"{note_text}\n\n— Jarz POS"

    try:
        client = _build_client(settings)
    except ValueError:
        return {"status": "skipped", "reason": "missing_credentials"}

    outcome = _post_woo_order_note(client, woo_order_id, body)
    if outcome == "already_posted":
        return {"status": "skipped", "reason": "already_posted", "woo_order_id": woo_order_id}
    if outcome != "posted":
        LOGGER.error({
            "event": "woo_outbound_note_error",
            "note": note_name,
            "invoice": invoice_name,
            "woo_order_id": woo_order_id,
            "detail": outcome,
        })
        return {"status": "error", "detail": outcome}

    LOGGER.info({
        "event": "woo_outbound_note_synced",
        "note": note_name,
        "invoice": invoice_name,
        "woo_order_id": woo_order_id,
    })
    return {"status": "synced", "woo_order_id": woo_order_id, "note": note_name}


def enqueue_linked_invoice_sync_for_payment_entry(payment_entry: frappe.model.document.Document, method: str | None = None) -> None:
    reason = f"payment_entry_{method}" if method else "payment_entry"
    seen_invoices: set[str] = set()
    for row in getattr(payment_entry, "references", []) or []:
        if str(getattr(row, "reference_doctype", "") or "") != "Sales Invoice":
            continue
        invoice_name = str(getattr(row, "reference_name", "") or "").strip()
        if not invoice_name or invoice_name in seen_invoices:
            continue
        seen_invoices.add(invoice_name)
        enqueue_invoice_sync(invoice_name, reason=reason)


#: Cap the fan-out from one Address edit. A delivery address belongs to a
#: handful of live orders at most; a number far above that means the address is
#: shared in a way this push was never meant to cover, so we log rather than
#: quietly truncate.
_ADDRESS_INVOICE_FANOUT_LIMIT = 20


def _invoice_is_woo_pushable_for_address(invoice_name: str) -> bool:
    """Whether an address correction is still worth pushing for this invoice."""
    try:
        state_field = None
        meta = frappe.get_meta("Sales Invoice")
        for candidate in ("custom_sales_invoice_state", "sales_invoice_state"):
            if meta.get_field(candidate):
                state_field = candidate
                break
        if not state_field:
            return True
        state = frappe.db.get_value("Sales Invoice", invoice_name, state_field)
    except Exception:  # noqa: BLE001
        return True
    return _normalize_invoice_state(state) not in {"cancelled", "canceled"}


def enqueue_linked_invoice_sync_for_address(
    address: frappe.model.document.Document,
    method: str | None = None,
    *,
    reason: str = "address_event",
    force: bool = False,
) -> None:
    """Push the Woo *orders* that use an Address when its text is edited.

    Editing an address in place — fixing a building number, a landmark, a phone
    — changes no field on the Sales Invoice, so ``on_update_after_submit`` never
    fires and the invoice's own outbound gate never sees anything. The Address
    hook that already existed only re-pushed the *customer profile*, so the
    store's copy of the order kept the address the rider had failed to find.

    Only re-pointing an invoice to a different Address record used to reach the
    order. This closes the far more common case: same record, corrected text.
    """
    settings, cfg = _get_settings()
    if not cfg.enable_order_push and not force:
        _note_outbound_disabled("enable_outbound_orders")
        return
    if _is_outbound_suppressed(address):
        return
    if not force and method == "on_update" and not _get_changed_fields(
        address, _CUSTOMER_ADDRESS_OUTBOUND_UPDATE_FIELDS
    ):
        return

    address_name = _get_doc_value(address, "name")
    if not address_name:
        return

    invoice_names: list[str] = []
    for fieldname in ("shipping_address_name", "customer_address"):
        try:
            rows = frappe.get_all(
                "Sales Invoice",
                filters={
                    fieldname: address_name,
                    "docstatus": 1,
                    "woo_order_id": ("not in", ("", None, 0)),
                },
                fields=["name"],
                limit=_ADDRESS_INVOICE_FANOUT_LIMIT + 1,
            )
        except Exception:  # noqa: BLE001
            continue
        for row in rows or []:
            name = _get_doc_value(row, "name")
            if name and name not in invoice_names:
                invoice_names.append(name)

    if not invoice_names:
        return

    if len(invoice_names) > _ADDRESS_INVOICE_FANOUT_LIMIT:
        LOGGER.warning({
            "event": "woo_outbound_address_fanout_capped",
            "address": address_name,
            "matched_invoices": len(invoice_names),
            "limit": _ADDRESS_INVOICE_FANOUT_LIMIT,
        })
        invoice_names = invoice_names[:_ADDRESS_INVOICE_FANOUT_LIMIT]

    for invoice_name in invoice_names:
        if not _invoice_is_woo_pushable_for_address(invoice_name):
            continue
        # Pass the NAME, not the doc: `_should_enqueue_invoice_event` only runs
        # for a document, and it compares Sales Invoice fields — none of which
        # moved here, because the change is one level down inside the Address
        # the payload reads. Passing a string skips that gate for us.
        #
        # Deliberately not force=True: force means `now=True`, which would run
        # the Woo HTTP call synchronously inside the address save. The redundant
        # PUT it would guard against is already suppressed by
        # `_order_payload_requires_update` once the store holds the new address.
        enqueue_invoice_sync(invoice_name, reason=reason)


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
    numeric = flt(value) if value not in (None, "") else 0.0
    return f"{numeric:.{precision}f}"


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
        try:
            row = frappe.db.get_value(
                "Item", key, ["woo_product_id", "woo_variation_id", "item_name"], as_dict=True
            )
        except Exception:  # noqa: BLE001
            # `woo_variation_id` is a Custom Field. On a bench where the fixture
            # has not been applied the query raises instead of returning a
            # partial row, so fall back to the columns that certainly exist —
            # one degraded lookup rather than a failed push.
            row = frappe.db.get_value("Item", key, ["woo_product_id", "item_name"], as_dict=True)
        cache[key] = dict(row or {})
    return cache[key]


def _resolve_item_product_identity(product_row: dict[str, Any] | None) -> tuple[Optional[int], Optional[int]]:
    """``(product_id, variation_id)`` for an Item, honouring *both* mapping fields.

    ``Item.woo_product_id`` holds either a bare product id or the composite
    ``"<product>:<variation>"``. Separately, ``Item.woo_variation_id`` carries
    the variation on its own — and outbound used to read only the first field,
    so an Item mapped that way resolved to nothing, raised
    ``MissingWooProductError`` and failed the **entire** invoice over one line
    (F-12). It also meant a variation Item reached the store as its parent
    product with no variation attached.

    Hardening rather than a live bug fix: zero variation-only Items exist today.
    """
    row = product_row or {}
    product_id, variation_id = _parse_product_identifier(row.get("woo_product_id"))
    if variation_id is None:
        candidate, _ = _parse_product_identifier(row.get("woo_variation_id"))
        if candidate:
            variation_id = candidate
    return product_id, variation_id


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
        product_row = _get_item_product_row(item_code, cache=item_product_rows)
        product_id, variation_id = _resolve_item_product_identity(product_row)
        if variation_id is None and product_id is not None:
            parent_product_ids[bundle_key] = product_id
    return parent_product_ids


def _invoice_uses_explicit_bundle_flags(invoice: frappe.model.document.Document) -> bool:
    """Whether this invoice carries the modern ``is_bundle_*`` markers at all."""
    for item in getattr(invoice, "items", []) or []:
        if _is_explicit_bundle_parent_item(item) or _is_explicit_bundle_child_item(item):
            return True
    return False


def _allow_bundle_parent_inference(invoice: frappe.model.document.Document) -> bool:
    """Whether the legacy "this row *looks* like a bundle parent" guess may run.

    The guess (registered bundle product id + price_list_rate > 0 + rate/amount
    <= 0 + discount >= 100) exists for invoices written before either builder
    stamped ``is_bundle_parent``. It is a *heuristic*, and it silently deletes
    the line it matches — so a genuine 100%-off giveaway of a bundle-registered
    product disappears from the customer's order (F-11).

    Both builders set the explicit flags today (``BundleProcessor`` stamps
    ``is_bundle_parent``/``parent_bundle``, and the POS cart does the same), so
    the guess is switched off entirely for any invoice that uses them: the flags
    are then authoritative and an unflagged 100%-off row is exactly what it
    looks like. It also cannot run on a single-line invoice, because a bundle
    parent without children on the same document is not a bundle.
    """
    if _invoice_uses_explicit_bundle_flags(invoice):
        return False
    return len(list(getattr(invoice, "items", []) or [])) > 1


def _is_bundle_parent_item(
    item: frappe.model.document.Document,
    *,
    product_id: Optional[int],
    variation_id: Optional[int],
    registered_bundle_product_ids: set[str],
    allow_inference: bool = True,
) -> bool:
    if _is_truthy_flag(getattr(item, "is_bundle_parent", 0)):
        return True

    if not allow_inference:
        return False

    if variation_id is not None or product_id is None:
        return False

    if str(product_id) not in registered_bundle_product_ids:
        return False

    price_list_rate = flt(_get_doc_value(item, "price_list_rate", 0))
    rate = flt(_get_doc_value(item, "rate", 0))
    amount = flt(_get_doc_value(item, "amount", 0))
    discount_percentage = flt(_get_doc_value(item, "discount_percentage", 0))

    return (
        price_list_rate > 0
        and rate <= 0
        and amount <= 0
        and discount_percentage >= 100
    )


def _woosb_selection_token(item_code: str) -> str:
    """The short WooSB-internal token that occupies ``_woosb_ids`` part 1.

    Native entries look like ``13777/anox/1/{"attribute_pa_size":"medium"}``.
    Nothing on either side reads part 1 — our parser takes parts 0 and 2 — but
    a native order always has one, so we emit one to stay indistinguishable.

    Derived from the item code rather than generated, deliberately: a fresh
    token on every push would make the meta value differ every time and
    re-create exactly the permanently-dirty comparator F-18 removes.
    """
    digest = hashlib.sha1(str(item_code or "").encode("utf-8")).hexdigest()
    return digest[:4]


def _woosb_selection_identifier(product_id: Optional[int], variation_id: Optional[int]) -> str:
    """Part 0 of a ``_woosb_ids`` entry: the variation id, else the product id.

    Matches native WooSB, which writes the variation id there for a variable
    product, and matches ``order_sync._resolve_bundle_selection_item_code``,
    which tries ``Item.woo_variation_id`` before ``Item.woo_product_id``. Using
    the variation id is therefore both the native shape *and* the only value
    that resolves back to one specific Item when several variations share a
    parent product id.
    """
    return str(variation_id or product_id or "")


def _build_woosb_ids_value(children: list[dict[str, Any]]) -> str:
    """Render the parent-side ``_woosb_ids`` selection string.

    Grammar, verbatim from a website-created order::

        13777/anox/1/{"attribute_pa_size":"medium"},13780/88zq/1/{...}

        part 0  child variation id (product id when the Item has no variation)
        part 1  short WooSB token
        part 2  quantity, TOTAL across the parent line (per-bundle x parent qty)
        part 3  attributes JSON

    Returns ``""`` when any child cannot be described honestly — a partial
    selection string is worse than none, because inbound treats ``_woosb_ids``
    as authoritative and would rebuild the bundle from an incomplete list.
    """
    entries: list[str] = []
    for child in children:
        identifier = _woosb_selection_identifier(child.get("product_id"), child.get("variation_id"))
        quantity = int(flt(child.get("qty") or 0))
        if not identifier or quantity <= 0:
            return ""
        attributes = child.get("attributes") or "{}"
        entries.append(f"{identifier}/{child.get('token') or ''}/{quantity}/{attributes}")
    return ",".join(entries)


def _collect_line_items(invoice: frappe.model.document.Document) -> tuple[list[dict], list[str]]:
    """Render the invoice's items in the shape a website-created order has.

    Bundle wire format (F-10), taken from a genuinely native order:

    * the **parent** line carries the whole bundle price and the ``_woosb_ids``
      selection string;
    * every **child** line is zero-rated and carries ``_woosb_parent_id``.

    ERPNext models a bundle the other way round — zero-rated parent, discounted
    children — and that internal representation is deliberately left alone. Only
    the wire format changes here, and the *sum* of the emitted line totals is
    identical either way, so the order total is unaffected.

    Child rows that share a (product, variation, parent) identity are merged
    into one line: ERPNext can legitimately produce the same item twice when a
    bundle asks for it from two different item groups, but a native order shows
    it once with the combined quantity.
    """
    line_items: list[dict] = []
    missing_products: list[str] = []
    registered_bundle_product_ids = _get_registered_bundle_product_ids(invoice)
    item_product_rows: dict[str, dict[str, Any]] = {}
    explicit_bundle_parent_product_ids = _collect_explicit_bundle_parent_product_ids(
        invoice,
        item_product_rows=item_product_rows,
    )
    allow_bundle_parent_inference = _allow_bundle_parent_inference(invoice)

    #: bundle link key -> the emitted parent entry, priced once every child is known
    parent_entries: dict[str, dict] = {}
    #: bundle link key -> child records in emission order (drives ``_woosb_ids``)
    parent_children: dict[str, list[dict[str, Any]]] = {}
    #: (bundle key, product, variation) -> the child record that owns that line
    child_slots: dict[tuple[str, str, str], dict[str, Any]] = {}

    for item in getattr(invoice, "items", []) or []:
        qty = flt(item.qty)
        if qty <= 0:
            continue
        price_list_rate = flt(_get_doc_value(item, "price_list_rate", 0))
        rate = flt(_get_doc_value(item, "rate", 0))
        amount = flt(_get_doc_value(item, "amount", 0))
        product_row = _get_item_product_row(item.item_code, cache=item_product_rows)
        product_id, variation_id = _resolve_item_product_identity(product_row)

        if product_id is None and variation_id is None:
            # No Woo product to point at. Only 14 of 26 Jarz Bundles carry a
            # `woo_product_id`, so an unmapped *bundle parent* is routine — and
            # it is worth nothing (100% discounted, amount 0), while its
            # children below carry the whole price and fall through to the
            # ordinary priced-line branch. Dropping it therefore FLATTENS the
            # bundle: the store loses the gift-box grouping but keeps every item
            # and every piastre. Raising instead is what stranded 11 production
            # invoices at `woo_outbound_status = error`, their status updates no
            # longer reaching the store at all.
            if _is_explicit_bundle_parent_item(item) and abs(amount) <= _TOTAL_ASSERT_TOLERANCE:
                LOGGER.warning({
                    "event": "woo_outbound_bundle_flattened_unmapped_parent",
                    "invoice": getattr(invoice, "name", None),
                    "item_code": item.item_code,
                    "bundle": _get_bundle_link_key(item),
                    "detail": (
                        "Bundle parent has no Item.woo_product_id, so the order is pushed "
                        "with its children as ordinary priced lines. Map the bundle to a "
                        "Woo product (and add a Woo Jarz Bundle record) to restore the "
                        "native bundle grouping in the store."
                    ),
                })
                continue
            # A priced line we cannot express. Dropped rather than failing the
            # whole order — but recorded, because the total assertion after the
            # push will now notice the money that went missing.
            missing_products.append(item.item_code)
            continue
        if product_id is None:
            # A variation with no parent product id still identifies a purchasable
            # thing; emit it and say so rather than failing the whole invoice.
            LOGGER.warning({
                "event": "woo_outbound_line_variation_only_mapping",
                "invoice": getattr(invoice, "name", None),
                "item_code": item.item_code,
                "variation_id": variation_id,
            })

        if _is_explicit_bundle_parent_item(item):
            entry = {
                "quantity": int(qty),
                # Priced after the loop, from the children's ERPNext amounts.
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
            bundle_key = _get_bundle_link_key(item)
            if bundle_key:
                parent_entries[bundle_key] = entry
                parent_children.setdefault(bundle_key, [])
            else:
                LOGGER.warning({
                    "event": "woo_outbound_bundle_parent_without_link_key",
                    "invoice": getattr(invoice, "name", None),
                    "item_code": item.item_code,
                })
            continue

        if _is_bundle_parent_item(
            item,
            product_id=product_id,
            variation_id=variation_id,
            registered_bundle_product_ids=registered_bundle_product_ids,
            allow_inference=allow_bundle_parent_inference,
        ):
            # Legacy guess, and it *deletes* the line — never silently.
            LOGGER.warning({
                "event": "woo_outbound_inferred_bundle_parent_dropped",
                "invoice": getattr(invoice, "name", None),
                "item_code": item.item_code,
                "product_id": product_id,
                "qty": qty,
                "price_list_rate": price_list_rate,
                "rate": rate,
                "amount": amount,
                "discount_percentage": flt(_get_doc_value(item, "discount_percentage", 0)),
                "detail": (
                    "Row inferred to be a WooSB bundle parent (registered bundle product, "
                    "100% discounted) and omitted from the Woo payload because the store "
                    "already carries the parent. If this was a genuine giveaway line, mark "
                    "the real bundle rows with is_bundle_parent/parent_bundle."
                ),
            })
            continue

        bundle_key = _get_bundle_link_key(item) if _is_explicit_bundle_child_item(item) else ""
        parent_product_id = explicit_bundle_parent_product_ids.get(bundle_key) if bundle_key else None

        if bundle_key and parent_product_id:
            slot_key = (bundle_key, str(product_id or ""), str(variation_id or ""))
            record = child_slots.get(slot_key)
            if record is not None:
                # Same product, same bundle: one native line, combined quantity.
                record["qty"] += qty
                record["amount"] += amount
                record["entry"]["quantity"] = int(record["qty"])
                LOGGER.info({
                    "event": "woo_outbound_bundle_child_merged",
                    "invoice": getattr(invoice, "name", None),
                    "item_code": item.item_code,
                    "bundle": bundle_key,
                    "merged_qty": record["qty"],
                })
                continue

            entry = {
                "quantity": int(qty),
                # Native children are free; the money is on the parent line.
                "subtotal": _format_money(0),
                "total": _format_money(0),
                "name": item.item_name or item.item_code,
                "meta_data": [
                    {"key": "erpnext_item_code", "value": item.item_code},
                    {"key": "_woosb_parent_id", "value": str(parent_product_id)},
                ],
            }
            if product_id:
                entry["product_id"] = product_id
            if variation_id:
                entry["variation_id"] = variation_id
            line_items.append(entry)
            record = {
                "entry": entry,
                "qty": qty,
                "amount": amount,
                "product_id": product_id,
                "variation_id": variation_id,
                "token": _woosb_selection_token(item.item_code),
                # We do not carry Woo variation attributes in ERPNext, and an
                # invented value would be a lie the store renders.
                "attributes": "{}",
            }
            child_slots[slot_key] = record
            parent_children.setdefault(bundle_key, []).append(record)
            continue

        if bundle_key and not parent_product_id:
            # Flagged as a child but its parent is not on this invoice (or the
            # parent Item has no Woo mapping). Keep the row priced: making it
            # free with no parent to carry the money would under-bill the order.
            LOGGER.warning({
                "event": "woo_outbound_bundle_child_without_parent",
                "invoice": getattr(invoice, "name", None),
                "item_code": item.item_code,
                "bundle": bundle_key,
            })

        subtotal_base = price_list_rate or rate
        subtotal = subtotal_base * qty if subtotal_base else amount
        entry = {
            "quantity": int(qty),
            "subtotal": _format_money(subtotal),
            "total": _format_money(amount),
            "name": item.item_name or item.item_code,
            "meta_data": [
                {"key": "erpnext_item_code", "value": item.item_code},
            ],
        }
        if product_id:
            entry["product_id"] = product_id
        if variation_id:
            entry["variation_id"] = variation_id
        discount_percentage = flt(_get_doc_value(item, "discount_percentage", 0))
        if discount_percentage:
            # Emitted as a normalised string so the value we send and the value
            # WooCommerce echoes back compare equal (see F-18).
            entry["meta_data"].append({
                "key": "discount_percentage",
                "value": _normalize_meta_compare_value(discount_percentage),
            })

        line_items.append(entry)

    for bundle_key, entry in parent_entries.items():
        children = parent_children.get(bundle_key) or []
        bundle_total = sum(flt(child.get("amount") or 0) for child in children)
        entry["subtotal"] = _format_money(bundle_total)
        entry["total"] = _format_money(bundle_total)

        woosb_ids = _build_woosb_ids_value(children)
        if woosb_ids:
            entry["meta_data"].append({"key": "_woosb_ids", "value": woosb_ids})
        else:
            LOGGER.warning({
                "event": "woo_outbound_bundle_selection_string_unavailable",
                "invoice": getattr(invoice, "name", None),
                "bundle": bundle_key,
                "children": len(children),
                "detail": (
                    "Parent line pushed without _woosb_ids; the store falls back to the "
                    "child lines' _woosb_parent_id, which our inbound parser also accepts."
                ),
            })

    return line_items, missing_products


def _resolve_shipping_income_account(company: str | None) -> str | None:
    """The account a shipping-income tax row is booked to, for this company.

    Mirrors ``order_sync._get_shipping_income_account``, which is what the
    inbound lane uses when it *writes* the row — so matching on it here is an
    exact identity test rather than a guess. Imported lazily: ``order_sync`` is
    a heavy module and outbound already reaches into it that way, which also
    keeps the import graph acyclic.
    """
    company = str(company or "").strip()
    if not company:
        return None
    try:
        from jarz_woocommerce_integration.services.order_sync import _get_shipping_income_account

        return _get_shipping_income_account(company)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_outbound_shipping_account_lookup_failed",
            "company": company,
            "error": str(exc),
        })
        return None


def _compute_shipping_total(invoice: frappe.model.document.Document) -> float:
    """Sum the invoice's shipping-income charge rows.

    Identity first, text second. The old test was ``"ship" in description or
    "delivery" in account``, which is why an item called "Delivery Box" was
    counted as freight, and why any tax row whose description happened to
    mention shipping inflated the number pushed to the store.

    The account head is the reliable signal: the shipping row is always the
    ``Actual`` charge booked to the company's shipping-income account (there is
    no VAT row on this site, so it is the only tax row in practice). The
    description match is kept purely as a fallback for rows booked elsewhere,
    and it says so in the log when it fires. The old "scan item *names* for the
    word shipping" fallback is gone — it could only ever double-count a real
    product.
    """
    shipping_account = _resolve_shipping_income_account(getattr(invoice, "company", None))
    normalized_account = str(shipping_account or "").strip().casefold()

    shipping_total = 0.0
    fallback_total = 0.0
    fallback_rows: list[str] = []

    for tax in getattr(invoice, "taxes", []) or []:
        if str(getattr(tax, "charge_type", "") or "") != "Actual":
            continue
        account = str(getattr(tax, "account_head", "") or "").strip()
        amount = flt(getattr(tax, "tax_amount", 0))

        if normalized_account and account.casefold() == normalized_account:
            shipping_total += amount
            continue

        description = str(getattr(tax, "description", "") or "").casefold()
        if "ship" in description or "delivery" in description:
            fallback_total += amount
            fallback_rows.append(f"{account}|{description[:60]}")

    if shipping_total:
        return shipping_total

    if fallback_total:
        LOGGER.warning({
            "event": "woo_outbound_shipping_matched_by_description",
            "invoice": getattr(invoice, "name", None),
            "expected_account": shipping_account,
            "rows": fallback_rows,
            "amount": fallback_total,
            "detail": (
                "No Actual charge row on the shipping-income account; fell back to a "
                "description match. Check the tax row's account_head."
            ),
        })
    return fallback_total


def _map_payment_method(invoice: frappe.model.document.Document, cfg: OutboundConfig) -> tuple[str, str]:
    """``(woo payment_method, payment_method_title)`` for this invoice.

    Delegates to ``services.payment_map``, the one table both lanes share.
    Previously three substring tests decided this, and ``Kashier Card`` matched
    none of them and shipped as ``cod`` while ``Kashier Wallet`` matched the
    wallet test and shipped as the mobile-wallet id (F-02).
    """
    raw_method = (
        getattr(invoice, "custom_payment_method", None)
        or getattr(invoice, "mode_of_payment", None)
        or ""
    )
    return payment_map.erpnext_to_woo(raw_method, cfg)


def _coerce_datetime_value(raw: Any) -> datetime | None:
    if not raw:
        return None
    if callable(get_datetime):
        try:
            value = get_datetime(raw)
            if isinstance(value, datetime):
                return value
            if isinstance(value, dt_date):
                return datetime.combine(value, dt_time.min)
        except Exception:
            pass
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, dt_date):
        return datetime.combine(raw, dt_time.min)
    if isinstance(raw, str):
        value = raw.strip()
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                parsed = datetime.strptime(value, fmt)
                if fmt == "%Y-%m-%d":
                    return datetime.combine(parsed.date(), dt_time.min)
                return parsed
            except ValueError:
                continue
    return None


def _build_paid_metadata(
    invoice: frappe.model.document.Document,
    *,
    payment_method: str,
    set_paid: bool,
    cfg: OutboundConfig,
) -> list[dict[str, str]]:
    if not set_paid:
        return []
    if str(payment_method or "").strip().lower() != str(cfg.payment_cod or "cod").strip().lower():
        return []

    paid_at = _coerce_datetime_value(getattr(invoice, "modified", None))
    if not paid_at:
        paid_at = _coerce_datetime_value(now_datetime())
    if not paid_at:
        return []

    paid_at_value = paid_at.replace(microsecond=0).isoformat(timespec="seconds")
    return [
        {"key": "_date_paid", "value": paid_at_value},
        {"key": "_date_paid_gmt", "value": paid_at_value},
    ]


#: Label for the order-level discount we push. WooCommerce has no order-level
#: discount primitive over REST other than a negative fee line, so that is what
#: an ERPNext header discount becomes. Inbound already reads it back the same
#: way (``order_sync._woo_noncoupon_discount_total`` sums negative fee lines).
_DISCOUNT_FEE_NAME = "Discount"


def _invoice_promo_codes(invoice: frappe.model.document.Document) -> list[str]:
    """Promo codes on the invoice, if the promo engine recorded any.

    ``custom_promo_codes`` is a JSON list written by the coupon passthrough. It
    is read defensively: a malformed value must never stop an order pushing.
    """
    raw = _get_doc_value(invoice, "custom_promo_codes", None)
    if not raw:
        return []
    if isinstance(raw, (list, tuple)):
        candidates = list(raw)
    else:
        try:
            parsed = json.loads(str(raw))
        except Exception:  # noqa: BLE001
            return []
        candidates = parsed if isinstance(parsed, (list, tuple)) else [parsed]
    return [str(code).strip() for code in candidates if str(code or "").strip()]


def _build_discount_fee_lines(
    invoice: frappe.model.document.Document,
    *,
    existing_order: Optional[dict] = None,
) -> list[dict]:
    """Render the invoice's header discount as WooCommerce fee lines.

    ``apply_discount_on`` / ``discount_amount`` are what the promo engine writes
    for a coupon or a dynamic-pricing discount. Nothing carried them outbound,
    so a discounted order reached the store at full price and the customer saw a
    total they had not agreed to (F-01).

    A negative ``fee_lines`` entry is WooCommerce's only order-level discount
    over REST — ``coupon_lines`` require a coupon that actually exists in the
    store, which an ERPNext-side promo does not. There is no VAT on this site,
    so ``total = sum(line totals) + shipping - discount`` exactly, which is what
    the post-push total assertion checks.

    An existing fee line is updated in place by id rather than appended, and a
    discount that has gone away is deleted (Woo removes a fee line when the PUT
    sends ``name: null``). Only fee lines we recognise as ours — negative total,
    or our own label — are ever touched: a fee somebody added in wp-admin is
    left alone.
    """
    discount = flt(_get_doc_value(invoice, "discount_amount", 0))
    existing_fee_lines = list((existing_order or {}).get("fee_lines") or [])
    ours = [
        line for line in existing_fee_lines
        if line and (
            flt(line.get("total") or 0) < 0
            or str(line.get("name") or "").strip().startswith(_DISCOUNT_FEE_NAME)
        )
    ]

    if discount <= 0:
        # Discount removed in ERPNext: retract it from the store too, otherwise
        # the customer keeps a reduction the invoice no longer grants.
        return [
            {"id": cint(line.get("id") or 0), "name": None}
            for line in ours
            if cint(line.get("id") or 0)
        ]

    codes = _invoice_promo_codes(invoice)
    name = f"{_DISCOUNT_FEE_NAME} ({', '.join(codes)})" if codes else _DISCOUNT_FEE_NAME

    entry: dict[str, Any] = {
        "name": name,
        "total": _format_money(-discount),
        # No VAT rows exist on this site; saying so explicitly stops WooCommerce
        # from trying to apportion tax across a negative fee.
        "tax_status": "none",
    }
    reused = next((line for line in ours if cint(line.get("id") or 0)), None)
    if reused is not None:
        entry["id"] = cint(reused.get("id"))

    removals = [
        {"id": cint(line.get("id") or 0), "name": None}
        for line in ours
        if cint(line.get("id") or 0) and (reused is None or line is not reused)
    ]
    return [entry, *removals]


#: What the production store calls its shipping lines, by census over 300
#: orders: ``flat_rate``/"Delivery" x195 when delivery is charged and
#: ``flat_rate``/"Free Delivery" x67 when it is waived (a ``free_shipping``
#: method id also appears x34, but it is the minority form and changing the
#: method id risks the store recalculating the line, so only the title moves).
#: The literal we used to send, "Shipping", appears on exactly two orders in the
#: whole store — both pushed from here, which is precisely the giveaway this
#: parity work exists to remove.
_SHIPPING_METHOD_PAID = ("flat_rate", "Delivery")
_SHIPPING_METHOD_FREE = ("flat_rate", "Free Delivery")


def _build_shipping_line(
    shipping_total: float,
    cfg: OutboundConfig,
    *,
    existing_order: Optional[dict] = None,
) -> dict:
    """One shipping line, titled the way the store titles its own.

    A configured ``default_shipping_method_id`` / ``default_shipping_method_title``
    still wins — this only replaces the fallback literals, and both settings are
    unset in production.
    """
    method_id, method_title = (
        _SHIPPING_METHOD_PAID if flt(shipping_total) > 0 else _SHIPPING_METHOD_FREE
    )
    entry: dict[str, Any] = {
        "method_id": cfg.shipping_method_id or method_id,
        "method_title": cfg.shipping_method_title or method_title,
        "total": _format_money(shipping_total if shipping_total else 0),
    }
    # Attach the existing shipping-line ID so WooCommerce updates the line in
    # place instead of appending a brand-new one on every PUT.
    if existing_order:
        existing_shipping = existing_order.get("shipping_lines") or []
        if existing_shipping:
            entry["id"] = existing_shipping[0].get("id")
    return entry


def _store_shows_order_paid(existing_order: Optional[dict]) -> bool:
    return bool(
        (existing_order or {}).get("date_paid")
        or (existing_order or {}).get("date_paid_gmt")
    )


def _detect_unpayable_transition(
    invoice: frappe.model.document.Document,
    existing_order: Optional[dict],
) -> bool:
    """Whether we are being asked to un-pay an order WooCommerce will not un-pay.

    **Known one-way limitation.** ``set_paid`` is a *transition*, not a state:
    sending ``true`` moves an unpaid order to paid, but sending ``false`` on an
    order that already has ``date_paid`` does nothing at all. WooCommerce has no
    REST verb for "this is no longer paid".

    So when an invoice's ``outstanding_amount`` goes back above zero — a payment
    reversed, a settlement corrected, a payment entry cancelled — the store keeps
    showing the order as paid and nothing in the response says otherwise. That
    used to be completely silent (F-23). It still cannot be fixed from here, but
    it is now logged and written to the order as a note so a human can reconcile
    it in wp-admin.
    """
    if not existing_order:
        return False
    if flt(_get_doc_value(invoice, "outstanding_amount", 0)) <= 0.01:
        return False
    return _store_shows_order_paid(existing_order)


def _normalize_invoice_state(raw_state: Any) -> str:
    return re.sub(r"[\s_]+", "-", str(raw_state or "").strip().lower())


# ---------------------------------------------------------------------------
# WooCommerce order statuses this app may emit
# ---------------------------------------------------------------------------
#: ``out-for-delivery`` is a *custom* status registered by the companion
#: WordPress plugin (``jarz_woo_plugin``) via ``register_post_status`` + the
#: ``wc_order_statuses`` filter. PUTting it to a store where that plugin is not
#: active is rejected/ignored by WooCommerce, and ``sync_sales_invoice`` already
#: catches that: it compares the status in the response against the one it sent
#: and marks the invoice Error on a mismatch rather than claiming Synced.
WOO_STATUS_PROCESSING = "processing"
WOO_STATUS_OUT_FOR_DELIVERY = "out-for-delivery"
WOO_STATUS_COMPLETED = "completed"
WOO_STATUS_CANCELLED = "cancelled"
#: Core WooCommerce status. Emitted when the order has been fully returned in
#: ERPNext — see ``_determine_status`` and ``sync_invoice_return``.
WOO_STATUS_REFUNDED = "refunded"

#: Registered by the plugin so a human can set it in wp-admin, but **never
#: emitted from here**. See ``_determine_status``.
WOO_STATUS_DELIVERY_FAILED = "delivery-failed"

#: Statuses the customer reads as "this order is over". Nothing about a failed
#: delivery attempt may produce one of these.
_TERMINAL_WOO_STATUSES = frozenset({
    WOO_STATUS_COMPLETED,
    WOO_STATUS_CANCELLED,
    WOO_STATUS_REFUNDED,
})

#: Invoice states that mean the goods came back. ``Returned`` is an option on
#: ``custom_sales_invoice_state`` (the terminal kanban column).
_RETURNED_INVOICE_STATES = frozenset({"returned", "refunded"})


def is_terminal_woo_status(status: Any) -> bool:
    """Whether a Woo status tells the customer the order has finished."""
    return str(status or "").strip().lower() in _TERMINAL_WOO_STATUSES


#: Sales Invoice fields that describe a *failed delivery attempt* (contract
#: section 2, fields 7-8). Order matters: (reason, attempt number).
#:
#: Deliberately absent from ``_OUTBOUND_RELEVANT_UPDATE_FIELDS``. A failed attempt
#: changes no field WooCommerce holds -- the status stays ``out-for-delivery`` --
#: so adding them there would fire a PUT per missed doorbell that rewrote the
#: order to exactly what it already said. The customer still learns about the
#: attempt: it is on the ERPNext tracking page their browser is polling.
_DELIVERY_FAILURE_FIELDS: tuple[str, str] = (
    "custom_delivery_failure_reason",
    "custom_delivery_attempt_no",
)


def _has_failed_delivery_attempt(invoice: frappe.model.document.Document) -> bool:
    """Whether at least one delivery attempt has failed on this invoice."""
    reason_field, attempt_field = _DELIVERY_FAILURE_FIELDS
    if str(_get_doc_value(invoice, reason_field, "") or "").strip():
        return True
    try:
        return cint(_get_doc_value(invoice, attempt_field, 0)) > 0
    except Exception:  # noqa: BLE001
        return False


#: A return within this much of the original grand total counts as a full one.
#: One piastre, matching the tolerance the outbound total assertion uses.
_RETURN_TOLERANCE = 0.01

#: The canonical return flag, written to the ORIGINAL Sales Invoice by the POS
#: return workflow (``custom_return_status``). Referenced by value only — the
#: field belongs to jarz_pos and this app never imports it, so on a bench where
#: that app is absent the value is simply blank and the fallbacks below decide.
_RETURN_STATUS_FULL = "fully returned"
_RETURN_STATUS_PARTIAL = "partially returned"


def _invoice_return_status(invoice: Any) -> str:
    """``custom_return_status`` on an invoice, normalised, or ``""``."""
    return str(_get_doc_value(invoice, "custom_return_status", "") or "").strip().casefold()


def _returned_amount(invoice_name: str) -> float:
    """Absolute value of every *submitted* credit note against an invoice.

    Standard ERPNext: a credit note is a Sales Invoice with ``is_return = 1``
    and ``return_against`` pointing at the original, carrying a negative
    ``grand_total``. No app-specific field is involved, so this reads the same
    whichever app created the return.
    """
    if not invoice_name:
        return 0.0
    try:
        rows = frappe.get_all(
            "Sales Invoice",
            filters={"return_against": invoice_name, "is_return": 1, "docstatus": 1},
            fields=["grand_total"],
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_outbound_return_lookup_failed",
            "invoice": invoice_name,
            "error": str(exc),
        })
        return 0.0
    return sum(abs(flt(_get_doc_value(row, "grand_total", 0))) for row in rows or [])


def _is_fully_returned(invoice: frappe.model.document.Document) -> bool:
    """Whether the customer's whole order has been returned.

    Three signals, in descending order of authority; any one is enough:

    1. ``custom_return_status == "Fully Returned"`` — the canonical,
       purpose-built flag the return workflow writes to the original invoice;
    2. ``custom_sales_invoice_state == "Returned"`` — the terminal kanban column
       a fully-returned order lands in;
    3. submitted credit notes against this invoice add up to its grand total.

    (1) is preferred because it says exactly this and nothing else. (3) is kept
    underneath it rather than replaced by it: the workflow sets the flag across
    two saves, so a blank value means "not known yet", not "not returned", and
    accounting is the thing that cannot lag. Consequently a row still reading
    ``Partially Returned`` is *not* short-circuited to False — if the credit
    notes have since reached the full amount, the order really is refunded.
    """
    if _invoice_return_status(invoice) == _RETURN_STATUS_FULL:
        return True

    if any(state in _RETURNED_INVOICE_STATES for state in _collect_invoice_states(invoice)):
        return True

    grand_total = flt(_get_doc_value(invoice, "grand_total", 0))
    if grand_total <= 0:
        return False

    invoice_name = str(_get_doc_value(invoice, "name", "") or "").strip()
    if not invoice_name:
        return False

    return _returned_amount(invoice_name) >= grand_total - _RETURN_TOLERANCE


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
    """Map ``custom_sales_invoice_state`` onto a WooCommerce order status.

    ``Out for Delivery`` -> ``out-for-delivery``, ``Delivered`` -> ``completed``,
    ``Cancelled`` (or docstatus 2) -> ``cancelled``, everything earlier in the
    pipeline -> ``processing``.

    **A failed delivery attempt is not a status.** COURIER_CONTRACTS.md section 1
    is explicit: no new invoice state was added for it, a failed stop leaves the
    invoice at ``Out for Delivery`` and records
    ``custom_delivery_failure_reason`` + ``custom_delivery_attempt_no`` instead.
    So the failure fields are read here only to be *ignored on purpose*: the
    order stays ``out-for-delivery``, never ``delivery-failed`` and never a
    terminal status. Egypt is a reschedule-heavy market -- the second attempt is
    usually the successful one, and telling a customer by email that their order
    failed, when a courier is coming back tomorrow, is a support call we create
    ourselves.

    ``Delivered`` is checked before the failure fields deliberately: a stale
    reason left behind by an earlier attempt must not un-complete a delivered
    order (the contract has it cleared on success, but this does not depend on
    that).

    A **fully returned** order maps to ``refunded`` (F-15), checked ahead of
    ``Delivered`` because a return happens *after* delivery: leaving the order
    at ``completed`` told the customer their returned order was still fulfilled.
    A partial return deliberately does not change the status — the order really
    was delivered — and is reported as a Woo order note instead (F-16).
    """
    states = _collect_invoice_states(invoice)

    if cancel:
        return WOO_STATUS_CANCELLED
    if invoice.docstatus == 2:
        return WOO_STATUS_CANCELLED
    if any(state in {"cancelled", "canceled"} for state in states):
        return WOO_STATUS_CANCELLED
    if _is_fully_returned(invoice):
        return WOO_STATUS_REFUNDED
    if any(state in {"delivered", "completed"} for state in states):
        return WOO_STATUS_COMPLETED
    if any(state == WOO_STATUS_OUT_FOR_DELIVERY for state in states):
        # Reached by both a clean first attempt and a failed one. Same status.
        if _has_failed_delivery_attempt(invoice):
            LOGGER.info({
                "event": "woo_outbound_failed_attempt_keeps_out_for_delivery",
                "invoice": getattr(invoice, "name", None),
                "attempt_no": _get_doc_value(invoice, "custom_delivery_attempt_no", 0),
                "failure_reason": _get_doc_value(invoice, "custom_delivery_failure_reason", None),
            })
        return WOO_STATUS_OUT_FOR_DELIVERY
    return WOO_STATUS_PROCESSING


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
    # Credit notes never sync outbound — see the guard in sync_sales_invoice.
    # Checked before `force` so a forced resync cannot push one either.
    try:
        if int(invoice.get("is_return") or 0):
            return False
    except Exception:
        pass
    if force:
        return True
    if cancel or method in {None, "on_submit", "on_cancel"}:
        return True
    if method != "on_update_after_submit":
        return True
    if _should_skip_acceptance_only_update(invoice, method=method, cancel=cancel):
        return False
    if _has_missing_or_stale_woo_order_mapping(invoice):
        return True
    if _has_approved_invoice_status_change(invoice, cancel=cancel):
        return True
    if _has_any_value_changed(invoice, _INVOICE_OUTBOUND_PAYLOAD_FIELDS):
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


def _find_replacement_invoice(invoice_name: str) -> Optional[str]:
    """The live invoice that amends ``invoice_name``, if one exists.

    ERPNext models an amendment as cancel-then-recreate, so at the moment the
    source is cancelled the replacement may be a draft (docstatus 0) that the
    operator is still editing, or already submitted (docstatus 1) if the
    amendment ran as one automated unit. Both mean the same thing for Woo: the
    order is not dead, it has moved to another invoice.
    """
    if not invoice_name:
        return None
    try:
        rows = frappe.get_all(
            "Sales Invoice",
            filters={"amended_from": invoice_name, "docstatus": ("in", (0, 1))},
            fields=["name"],
            order_by="creation desc",
            limit=1,
        )
    except Exception:  # noqa: BLE001
        return None
    return rows[0]["name"] if rows else None


def _handover_woo_order_to_replacement(
    *,
    woo_order_id: Any,
    source_invoice: str,
    replacement_invoice: str,
) -> None:
    """Move the Woo order's ERPNext ownership from a cancelled invoice to its amendment.

    Without this the Order Map keeps pointing at a cancelled invoice, and the
    inbound path treats the order as unmapped and builds a second invoice for it.
    """
    if not woo_order_id or not replacement_invoice:
        return

    try:
        current = frappe.db.get_value("Sales Invoice", replacement_invoice, "woo_order_id")
        if str(current or "") != str(woo_order_id):
            frappe.db.set_value(
                "Sales Invoice",
                replacement_invoice,
                "woo_order_id",
                cint(woo_order_id),
                update_modified=False,
            )
    except Exception:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_outbound_amendment_handover_id_failed",
            "source_invoice": source_invoice,
            "replacement_invoice": replacement_invoice,
            "woo_order_id": woo_order_id,
        })

    link_field = _resolve_order_map_link_field()
    try:
        map_name = frappe.db.get_value("WooCommerce Order Map", {"woo_order_id": woo_order_id}, "name")
        if map_name:
            frappe.db.set_value(
                "WooCommerce Order Map",
                map_name,
                {
                    # Clear the hash so the next inbound poll re-evaluates the
                    # order against the replacement rather than skipping it as
                    # "unchanged" and leaving the two systems silently apart.
                    link_field: replacement_invoice,
                    "hash": "",
                    "synced_on": now_datetime(),
                },
                update_modified=False,
            )
    except Exception:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_outbound_amendment_handover_map_failed",
            "source_invoice": source_invoice,
            "replacement_invoice": replacement_invoice,
            "woo_order_id": woo_order_id,
        })

    LOGGER.info({
        "event": "woo_outbound_amendment_handover",
        "source_invoice": source_invoice,
        "replacement_invoice": replacement_invoice,
        "woo_order_id": woo_order_id,
    })


def _response_has_contact_data(order: dict) -> bool:
    """Whether a Woo order response actually carries addressee information.

    Guard against overwriting good snapshot values with a stub: a response with
    empty ``billing``/``shipping`` would make the extractor invent
    ``"Woo Guest <id>"`` and we would write that over the real customer name.
    """
    for scope in ("billing", "shipping"):
        block = order.get(scope) or {}
        if any(
            str(block.get(key) or "").strip()
            for key in ("first_name", "last_name", "phone", "email", "address_1")
        ):
            return True
    return bool(str(order.get("customer_email") or "").strip())


def _extract_response_snapshots(
    response: Any,
    *,
    invoice_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """``(contact_snapshot, territory_snapshot)`` from a Woo order response.

    The response to a create/update **is** a Woo order dict, so the inbound
    extractors take it unchanged. POS-origin orders never travel the inbound
    path, so nothing had ever written these fields for them: every POS order
    read as "no contact snapshot", which made the contact-hash comparison
    useless and left the Order Map holding a seven-field subset (F-05/F-06).

    Never raises — a snapshot is a nice-to-have and must not fail a push that
    the store has already accepted.
    """
    if not isinstance(response, dict) or not _response_has_contact_data(response):
        return {}, {}
    try:
        from jarz_woocommerce_integration.services.order_sync import (
            _extract_order_contact_snapshot,
            _extract_order_territory_snapshot,
        )

        return (
            _extract_order_contact_snapshot(response) or {},
            _extract_order_territory_snapshot(response) or {},
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_outbound_snapshot_extract_failed",
            "invoice": invoice_name,
            "error": str(exc),
        })
        return {}, {}


def _contact_snapshot_invoice_values(snapshot: dict[str, Any]) -> dict[str, Any]:
    """The snapshot fields that exist as Sales Invoice columns.

    Deliberately excludes ``customer_name``: that belongs to the ERPNext
    Customer and must not be rewritten from a store response.
    """
    if not snapshot:
        return {}
    try:
        from jarz_woocommerce_integration.services.order_sync import ORDER_CONTACT_SNAPSHOT_FIELDS
    except Exception:  # noqa: BLE001
        return {}
    return {fieldname: (snapshot.get(fieldname) or "") for fieldname in ORDER_CONTACT_SNAPSHOT_FIELDS}


#: Never a realtime recipient: not a real session, and addressing it would put
#: order data in a room the public website shares.
_REALTIME_EXCLUDED_USERS = frozenset({"Guest"})


def _pos_profile_recipients(pos_profile: Any) -> list[str]:
    """The users assigned to a POS Profile.

    Replicated here rather than imported — this app never imports ``jarz_pos``
    — but there is nothing app-specific to share: the lookup reads only stock
    ERPNext DocTypes, ``POS Profile`` and its ``POS Profile User`` child table.

    ``disabled = 0`` is part of the filter deliberately. A closed branch's users
    can no longer load the orders the event refers to, so delivering it to them
    would only produce a board update they cannot act on.

    Returns ``[]`` rather than raising; the caller decides what an empty
    recipient list means.
    """
    profile = str(pos_profile or "").strip()
    if not profile:
        return []

    try:
        enabled_profiles = frappe.get_all(
            "POS Profile",
            filters={"name": ["in", [profile]], "disabled": 0},
            pluck="name",
        ) or []
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_realtime_pos_profile_lookup_failed",
            "pos_profile": profile,
            "error": str(exc),
        })
        return []

    if not enabled_profiles:
        return []

    try:
        rows = frappe.get_all(
            "POS Profile User",
            filters={"parent": ["in", enabled_profiles], "parenttype": "POS Profile"},
            fields=["user"],
        ) or []
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_realtime_pos_profile_user_lookup_failed",
            "pos_profile": profile,
            "error": str(exc),
        })
        return []

    recipients: list[str] = []
    for row in rows:
        user = str(_get_doc_value(row, "user", "") or "").strip()
        if not user or user in _REALTIME_EXCLUDED_USERS or user in recipients:
            continue
        recipients.append(user)
    return recipients


def _publish_woo_order_assigned(
    invoice_name: str,
    woo_order_id: Any,
    *,
    invoice: Any = None,
) -> None:
    """Tell the branch that owns this invoice that it just acquired a Woo order id.

    Addressed **per user, to the invoice's POS Profile**. There are two ways to
    get this wrong and this call site has now been on both:

    * ``user="*"`` resolves to ``get_user_room("*")`` — a room literally named
      ``user:*`` that no socket ever joins, so the event reached nobody (F-25);
    * omitting ``user`` falls through to the site-wide ``all`` room, which
      delivers to every System User on the site regardless of branch. That is a
      leak, not a fix: one branch's orders would appear on every other branch's
      board, which is exactly what the branch-scoping work exists to prevent.

    An empty recipient list is **dropped and logged**, never widened into a
    broadcast — the broadcast is the leak. Each publish is wrapped on its own so
    one bad recipient cannot silence the rest.
    """
    payload: dict[str, Any] = {
        "invoice": invoice_name,
        "invoice_id": invoice_name,
        "woo_order_id": woo_order_id,
        "event": "woo_order_assigned",
    }
    pos_profile = str(_get_doc_value(invoice, "pos_profile", "") or "").strip()
    if pos_profile:
        payload["pos_profile"] = pos_profile

    recipients = _pos_profile_recipients(pos_profile)
    if not recipients:
        LOGGER.warning({
            "event": "woo_order_assigned_realtime_no_recipients",
            "invoice": invoice_name,
            "woo_order_id": woo_order_id,
            "pos_profile": pos_profile or None,
            "detail": (
                "No enabled POS Profile user to address, so the board update was "
                "dropped. Deliberately not broadcast site-wide: that would show this "
                "branch's order to every other branch."
            ),
        })
        return

    for user in recipients:
        try:
            frappe.publish_realtime("kanban_update", payload, user=user)
        except Exception:  # noqa: BLE001
            LOGGER.warning({
                "event": "woo_order_assigned_realtime_failed",
                "invoice": invoice_name,
                "user": user,
            })


def _relink_order_map_to_invoice(
    woo_order_id: int | str | None,
    invoice_name: str,
    *,
    currency: str | None = None,
    total: float | None = None,
    status: str | None = None,
    payment_method: str | None = None,
    hash_value: str | None = None,
    order_response: Optional[dict] = None,
    contact_snapshot: Optional[dict] = None,
    territory_snapshot: Optional[dict] = None,
) -> None:
    if not woo_order_id or not invoice_name:
        return

    link_field = _resolve_order_map_link_field()
    map_updates = {
        link_field: invoice_name,
        "currency": currency or "",
        "total": flt(total or 0),
        "status": status or "",
        "payment_method": payment_method or "",
        "synced_on": now_datetime(),
    }
    if hash_value:
        map_updates["hash"] = hash_value

    # Write the FULL map row when the store handed us a real order back, not the
    # seven-field subset outbound used to leave behind. In particular
    # `woo_order_number` — the id every screen shows the customer — was blank on
    # every POS-origin map row (F-06).
    #
    # Gated on a non-empty snapshot: an empty one means the response carried no
    # addressee data, and writing it through would blank every snapshot column.
    if isinstance(order_response, dict) and contact_snapshot:
        try:
            from jarz_woocommerce_integration.services.order_sync import (
                _build_order_map_snapshot_values,
            )

            full_values = _build_order_map_snapshot_values(
                order_response,
                contact_snapshot or {},
                link_field=link_field,
                invoice_name=invoice_name,
                order_hash=hash_value,
                territory_snapshot=territory_snapshot or {},
            )
            # The response is authoritative for what the store holds, but fall
            # back to the caller's values wherever it is silent.
            for fieldname, fallback in (
                ("currency", currency or ""),
                ("status", status or ""),
                ("payment_method", payment_method or ""),
            ):
                if not full_values.get(fieldname):
                    full_values[fieldname] = fallback
            if full_values.get("total") in (None, ""):
                full_values["total"] = flt(total or 0)
            map_updates = full_values
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning({
                "event": "woo_outbound_order_map_snapshot_failed",
                "invoice": invoice_name,
                "woo_order_id": woo_order_id,
                "error": str(exc),
            })

    try:
        map_name = frappe.db.get_value("WooCommerce Order Map", {"woo_order_id": woo_order_id}, "name")
    except Exception:
        map_name = None

    try:
        if map_name:
            frappe.db.set_value(
                "WooCommerce Order Map",
                map_name,
                map_updates,
                update_modified=False,
            )
            return

        frappe.get_doc({
            "doctype": "WooCommerce Order Map",
            "woo_order_id": cint(woo_order_id),
            **map_updates,
        }).insert(ignore_permissions=True)
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


def _attach_existing_line_ids(
    line_items: list[dict],
    existing_line_items: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    """Split the desired lines against what the Woo order already holds.

    Returns ``(matched, added, orphaned)``:

    * ``matched``   — desired lines that map onto an existing Woo line; each
      carries that line's ``id`` so Woo updates it in place.
    * ``added``     — desired lines with no counterpart on the order. These are
      new items and MUST be sent without an ``id`` so Woo appends them. They
      used to be discarded here, which is why an item added in ERPNext never
      appeared on the store (Woo order 15808).
    * ``orphaned``  — existing Woo lines nothing desired maps onto, i.e. items
      removed in ERPNext. The caller decides which are safe to delete.
    """
    if not existing_line_items:
        return line_items, [], []

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
    orphaned = [row.get("entry") or {} for row in remaining]
    return mapped, unmapped, orphaned


def _preserve_native_bundle_selection(
    payload_line_items: list[dict],
    existing_order: dict | None,
) -> None:
    """Never overwrite a ``_woosb_ids`` the store wrote itself.

    Ours is *reconstructed* from ERPNext rows: the WooSB token becomes a hash of
    the item code and the attributes object is empty, because ERPNext does not
    carry Woo variation attributes. That is right for an order we created and
    wrong for one the website created — the store renders the bundle contents
    from this string, so replacing the plugin's own value with our approximation
    would degrade 10,000+ Woo-origin orders on their next status update.

    Mutates ``payload_line_items`` in place, dropping only our own key.
    """
    existing_by_id: dict[int, dict] = {}
    for entry in (existing_order or {}).get("line_items") or []:
        line_id = cint((entry or {}).get("id") or 0)
        if line_id:
            existing_by_id[line_id] = entry
    if not existing_by_id:
        return

    for entry in payload_line_items:
        line_id = cint(entry.get("id") or 0)
        existing = existing_by_id.get(line_id) if line_id else None
        if not existing or not _extract_meta_value(existing, "_woosb_ids"):
            continue
        meta = entry.get("meta_data") or []
        kept = [meta_entry for meta_entry in meta if str(meta_entry.get("key") or "") != "_woosb_ids"]
        if len(kept) != len(meta):
            entry["meta_data"] = kept
            LOGGER.info({
                "event": "woo_outbound_native_bundle_selection_preserved",
                "line_id": line_id,
                "item_code": _extract_item_code(entry),
            })


def _protected_existing_line_ids(
    invoice: frappe.model.document.Document,
    existing_order: dict,
    *,
    protected_item_codes: set[str] | None = None,
) -> set[int]:
    """Woo line ids an outbound sync must never delete.

    Two families are protected.

    A registered bundle's parent line can be absent from the ERPNext payload —
    the legacy ``_is_bundle_parent_item`` inference still skips such a row on
    flag-free invoices, and a bundle with no ``woo_product_id`` is flattened. To
    the orphan check it looks like a line the user removed, and deleting it
    would strip the bundle from the customer's order.

    An item whose Woo mapping is missing is likewise absent from the payload
    (see ``protected_item_codes``) without having been removed from the invoice.

    Two independent signals mark a parent, and we honour either one:
    the line is referenced by a sibling's ``_woosb_parent_id``, or its
    ``product_id`` is registered in Woo Jarz Bundle.
    """
    existing_lines = existing_order.get("line_items") or []
    if not existing_lines:
        return set()

    protected: set[int] = set()

    # A line whose ERPNext Item lost (or never had) a Woo mapping is absent from
    # the payload, so the orphan check sees it as "removed in ERPNext". It was
    # not: we simply cannot describe it. Deleting it would strip a real product
    # — and its money — off the customer's order.
    protected_item_codes = {code for code in (protected_item_codes or set()) if code}
    if protected_item_codes:
        for entry in existing_lines:
            line_id = cint(entry.get("id") or 0)
            if line_id and str(_extract_item_code(entry) or "") in protected_item_codes:
                protected.add(line_id)

    parent_product_ids: set[str] = set()
    for entry in existing_lines:
        raw = _extract_meta_value(entry, "_woosb_parent_id")
        if raw:
            parent_product_ids.add(str(raw).strip())
    parent_product_ids |= _get_registered_bundle_product_ids(invoice)

    if not parent_product_ids:
        return protected

    for entry in existing_lines:
        product_id = cint(entry.get("product_id") or 0)
        line_id = cint(entry.get("id") or 0)
        if product_id and line_id and str(product_id) in parent_product_ids:
            protected.add(line_id)
    return protected


def _build_line_item_removals(
    orphaned: list[dict],
    *,
    protected_ids: set[int],
) -> list[dict]:
    """Turn orphaned Woo lines into deletion instructions.

    WooCommerce removes a line item when the PUT sends it with an integer
    ``quantity`` of 0 (its check is a strict ``0 === $item['quantity']``, so the
    value must not be a string).
    """
    removals: list[dict] = []
    for entry in orphaned:
        line_id = cint((entry or {}).get("id") or 0)
        if not line_id or line_id in protected_ids:
            continue
        removals.append({"id": line_id, "quantity": 0})
    return removals


def _meta_entries_to_map(entries: list[dict] | None) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for entry in entries or []:
        key = str(entry.get("key") or "").strip()
        if key:
            result[key] = entry.get("value")
    return result


def _normalize_meta_compare_value(value: Any) -> str:
    """Compare meta values by *meaning*, not by repr.

    We send ``discount_percentage`` as a number and WooCommerce echoes it back
    as the string ``"50"``; ``str(50.0)`` is ``"50.0"``, so a naive comparison
    never matched and the order stayed permanently dirty.
    """
    text = str("" if value is None else value).strip()
    if not text:
        return ""
    try:
        numeric = float(text)
    except (TypeError, ValueError):
        return text
    if numeric == int(numeric):
        return str(int(numeric))
    return f"{numeric:g}"


def _payload_line_meta_keys(entries: list[dict] | None) -> frozenset[str]:
    """The owned meta keys our payload actually carries.

    Comparison is one-directional, exactly like the order-level meta check: the
    question is "does the store already say everything we are about to say?",
    not "are the two identical". A key we deliberately do not send — a native
    ``_woosb_ids`` we are preserving, say — must not make the order look dirty
    forever.
    """
    keys: set[str] = set()
    for entry in entries or []:
        for meta in entry.get("meta_data", []) or []:
            key = str(meta.get("key") or "")
            if key in _ORDER_LINE_META_KEYS_TO_COMPARE:
                keys.add(key)
    return frozenset(keys)


def _normalize_order_line_items(
    entries: list[dict] | None,
    *,
    meta_keys: frozenset[str] | None = None,
) -> list[tuple[Any, ...]]:
    compare_keys = _ORDER_LINE_META_KEYS_TO_COMPARE if meta_keys is None else meta_keys
    normalized: list[tuple[Any, ...]] = []
    for entry in entries or []:
        variation_id = entry.get("variation_id")
        if variation_id == 0:
            variation_id = None
        metadata = tuple(sorted(
            (
                str(meta.get("key") or ""),
                _normalize_meta_compare_value(meta.get("value")),
            )
            for meta in entry.get("meta_data", []) or []
            if str(meta.get("key") or "") in compare_keys
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


def _normalize_order_fee_lines(entries: list[dict] | None) -> list[tuple[str, str]]:
    """``(name, total)`` per fee line, money compared numerically.

    Woo answers with ``"-25.00"`` for a fee we sent as ``"-25.0"``; comparing
    the raw strings would mark the order dirty on every sync.
    """
    normalized = []
    for entry in entries or []:
        if not entry:
            continue
        normalized.append((
            str(entry.get("name") or "").strip(),
            _format_money(flt(entry.get("total") or 0)),
        ))
    return sorted(normalized)


_ORDER_ADDRESS_COMPARE_KEYS = ("address_1", "address_2", "city", "state", "postcode")


def _normalize_order_address(address: dict | None) -> tuple[str, ...]:
    address = address or {}
    return tuple(str(address.get(key) or "").strip().casefold() for key in _ORDER_ADDRESS_COMPARE_KEYS)


def _order_address_requires_update(existing: dict | None, desired: dict | None) -> bool:
    """Whether the store's copy of an order address still matches ours.

    ``country`` is deliberately not compared: WooCommerce answers with the ISO
    code ("EG") while ERPNext stores the country name ("Egypt"), so comparing it
    would mark every order dirty forever.
    """
    desired = desired or {}
    if not str(desired.get("address_1") or "").strip():
        return False
    return _normalize_order_address(desired) != _normalize_order_address(existing)


def _order_payload_requires_update(existing_order: dict, payload: dict) -> bool:
    current_status = str(existing_order.get("status") or "").strip().lower()
    desired_status = str(payload.get("status") or "").strip().lower()
    if current_status != desired_status:
        return True

    payload_payment_method = str(payload.get("payment_method") or "").strip().lower()
    if payload_payment_method:
        current_payment_method = str(existing_order.get("payment_method") or "").strip().lower()
        if current_payment_method != payload_payment_method:
            return True

    payload_payment_title = str(payload.get("payment_method_title") or "").strip().lower()
    if payload_payment_title:
        current_payment_title = str(existing_order.get("payment_method_title") or "").strip().lower()
        if current_payment_title != payload_payment_title:
            return True

    if bool(payload.get("set_paid")):
        current_paid = bool(existing_order.get("date_paid") or existing_order.get("date_paid_gmt"))
        if not current_paid:
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
        meta_keys = _payload_line_meta_keys(payload_line_items)
        if _normalize_order_line_items(payload_line_items, meta_keys=meta_keys) != _normalize_order_line_items(
            existing_line_items, meta_keys=meta_keys
        ):
            return True

    payload_shipping_lines = payload.get("shipping_lines") or []
    if payload_shipping_lines:
        existing_shipping_lines = existing_order.get("shipping_lines") or []
        if _normalize_order_shipping_lines(payload_shipping_lines) != _normalize_order_shipping_lines(existing_shipping_lines):
            return True

    # The header discount rides as a negative fee line. Without comparing it, an
    # order whose discount changed (or was applied for the first time) was
    # dropped as "already in sync" and the customer kept the old total — F-01.
    if "fee_lines" in payload:
        payload_fee_lines = [
            line for line in (payload.get("fee_lines") or [])
            if line and line.get("name") is not None
        ]
        existing_fee_lines = existing_order.get("fee_lines") or []
        if _normalize_order_fee_lines(payload_fee_lines) != _normalize_order_fee_lines(existing_fee_lines):
            return True

    # An address edit is often the only thing that changed — without this the
    # order was dropped as "unchanged" and the store kept the old delivery zone.
    for scope in ("billing", "shipping"):
        if _order_address_requires_update(existing_order.get(scope), payload.get(scope)):
            return True

    return False


def _build_order_payload(
    invoice: frappe.model.document.Document,
    cfg: OutboundConfig,
    *,
    cancel: bool = False,
    existing_order: Optional[dict] = None,
    is_create: Optional[bool] = None,
) -> dict:
    if is_create is None:
        is_create = existing_order is None
    line_items, missing_products = _collect_line_items(invoice)
    payload_line_items = list(line_items)
    added_line_items: list[dict] = []
    removed_line_items: list[dict] = []
    if existing_order:
        matched, added_line_items, orphaned = _attach_existing_line_ids(
            line_items, existing_order.get("line_items") or []
        )
        removed_line_items = _build_line_item_removals(
            orphaned,
            protected_ids=_protected_existing_line_ids(
                invoice,
                existing_order,
                protected_item_codes=set(missing_products),
            ),
        )
        # Order matters only for readability; Woo applies each entry by id.
        payload_line_items = matched + added_line_items + removed_line_items
        _preserve_native_bundle_selection(payload_line_items, existing_order)

    if not payload_line_items and not existing_order:
        raise ValueError("No line items available for Woo order payload")

    shipping_total = _compute_shipping_total(invoice)
    customer_doc = frappe.get_doc("Customer", invoice.customer)
    customer_payload = _build_customer_payload(customer_doc)

    payment_method, payment_title = _map_payment_method(invoice, cfg)
    set_paid = flt(getattr(invoice, "outstanding_amount", 0)) <= 0.01
    woo_status = _determine_status(invoice, cancel=cancel)
    billing_fallback_name = _get_invoice_order_fallback_name(
        invoice,
        customer_doc,
        "woo_billing_name",
        "woo_order_display_name",
    )
    shipping_fallback_name = _get_invoice_order_fallback_name(
        invoice,
        customer_doc,
        "woo_shipping_name",
        "woo_order_display_name",
    )
    snapshot_email = str(getattr(invoice, "woo_order_email", "") or "").strip()
    snapshot_phone = str(getattr(invoice, "woo_order_phone", "") or "").strip()

    # Prefer invoice addresses for both billing and shipping (order address must populate both)
    default_email = (
        snapshot_email
        or (customer_payload.get("billing") or {}).get("email")
        or (customer_payload.get("shipping") or {}).get("email")
        or (getattr(customer_doc, "email_id", "") or "").strip()
    )
    default_phone = (
        snapshot_phone
        or (customer_payload.get("billing") or {}).get("phone")
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
            fallback_name=billing_fallback_name,
            phone=default_phone,
            email=default_email,
        )
    if invoice_shipping_address:
        shipping_address = _get_address_payload(
            invoice_shipping_address,
            fallback_name=shipping_fallback_name,
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

    _apply_address_contact_name(billing_address, billing_fallback_name)
    _apply_address_contact_name(shipping_address, shipping_fallback_name)

    if snapshot_phone:
        billing_address["phone"] = snapshot_phone
        shipping_address["phone"] = snapshot_phone
    elif default_phone:
        billing_address.setdefault("phone", default_phone)
        shipping_address.setdefault("phone", default_phone)
    if snapshot_email:
        billing_address["email"] = snapshot_email
        shipping_address["email"] = snapshot_email
    elif default_email:
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

    fee_lines = _build_discount_fee_lines(invoice, existing_order=existing_order)
    if fee_lines:
        payload["fee_lines"] = fee_lines

    # The store stamps its own clock on a create, so a backfilled or
    # end-of-shift invoice showed up in the store dated whenever the job ran
    # rather than when the sale happened (F-19). Only sent on the create:
    # WooCommerce takes `date_created` on POST and ignores it on PUT, and
    # re-sending it would be noise in the already-in-sync comparison.
    if is_create:
        payload.update(_build_order_creation_dates(invoice))

    payload["meta_data"].extend(_build_delivery_metadata(invoice))
    payload["meta_data"].extend(_build_paid_metadata(invoice, payment_method=payment_method, set_paid=bool(set_paid), cfg=cfg))
    # The customer tracking link rides the ordinary order payload rather than a
    # push of its own: same outbox, same retries, same idempotency check, no
    # extra HTTP call and no second queue to forget about. WooCommerce only ever
    # *stores* these two keys -- all live tracking happens browser -> ERPNext.
    payload["meta_data"].extend(_build_tracking_metadata(invoice))

    woo_customer_id = get_customer_woo_id(customer_doc)
    if woo_customer_id:
        payload["customer_id"] = cint(woo_customer_id)

    payload["shipping_lines"] = [_build_shipping_line(shipping_total, cfg, existing_order=existing_order)]

    if missing_products:
        detail = "Missing WooCommerce product mapping for items: " + ", ".join(missing_products)
        if not payload_line_items:
            # Nothing at all resolved: an empty payload is the one case where
            # pushing is genuinely meaningless, so this stays a hard failure.
            raise MissingWooProductError(detail)
        # Otherwise degrade. An order that reaches the store missing one line is
        # recoverable and visible (the total assertion flags it); an order that
        # never reaches the store, or whose status silently stops updating, is
        # not. log_error rather than LOGGER.warning: warnings are below the
        # servers' log level and this must be seen.
        LOGGER.error({
            "event": "woo_outbound_line_dropped_unmapped",
            "invoice": invoice.name,
            "item_codes": missing_products,
        })
        try:
            if _is_payload_dry_run():
                raise RuntimeError("dry_run")  # keeps the builder write-free
            frappe.log_error(
                title=f"WooCommerce: unmapped items on {invoice.name}",
                message=(
                    f"{detail}\n\nThe order was pushed without those lines so its status "
                    f"keeps syncing, but the store's total will be short by their value. "
                    f"Set Item.woo_product_id (or woo_variation_id) for each of them."
                ),
            )
        except Exception:  # noqa: BLE001
            pass
    if existing_order and (added_line_items or removed_line_items):
        # Not a warning any more: these lines are now carried in the payload
        # rather than silently dropped. Logged so an unexpected churn of
        # add/remove on an order is still traceable after the fact.
        LOGGER.info({
            "event": "woo_outbound_line_items_changed",
            "invoice": invoice.name,
            "added": [
                (_extract_item_code(entry) or entry.get("name") or "unknown")
                for entry in added_line_items
            ],
            "removed_line_ids": [entry["id"] for entry in removed_line_items],
        })

    if not payload.get("line_items"):
        LOGGER.info({
            "event": "woo_outbound_status_only_update",
            "invoice": invoice.name,
            "woo_order_id": getattr(invoice, "woo_order_id", None),
            "reason": "no_line_item_matches",
        })
    return payload


def _first_present(*values: Any) -> Any:
    """First value that is actually set, rather than first *truthy* value.

    ``a or b`` is the wrong test for delivery fields: midnight arrives as
    ``timedelta(0)``/``time(0, 0)`` and a zero-length duration as ``0``, all of
    which are falsy but real. Only ``None`` and a blank string mean "unset".
    """
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


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
    # NOT `if not raw`. Midnight is a legitimate slot start, and both of its
    # natural representations -- `timedelta(0)` from a Frappe `Time` column and
    # `time(0, 0)` -- are falsy. Treating them as "unset" made this return None,
    # which drops the entire time-slot block from the outbound payload: the Woo
    # order got a delivery date and no slot at all.
    if raw is None:
        return None
    if isinstance(raw, str) and not raw.strip():
        return None
    if isinstance(raw, datetime):
        return raw.time().replace(microsecond=0)
    if isinstance(raw, dt_time):
        return raw.replace(microsecond=0)
    # Frappe hands back a `Time` column as a timedelta since midnight, not a
    # datetime.time. Without this branch every POS order reached Woo with a
    # delivery date but no time slot.
    if isinstance(raw, timedelta):
        seconds = int(raw.total_seconds()) % 86400
        return dt_time(seconds // 3600, (seconds % 3600) // 60, seconds % 60)
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


def _invoice_posting_datetime(invoice: frappe.model.document.Document) -> datetime | None:
    """The moment of sale: ``posting_date`` + ``posting_time``, site-local."""
    posting_date = _coerce_delivery_date(_get_doc_value(invoice, "posting_date", None))
    if not posting_date:
        return None
    posting_time = _coerce_delivery_time(_get_doc_value(invoice, "posting_time", None)) or dt_time.min
    return datetime.combine(posting_date, posting_time)


def _to_utc_naive(local_dt: datetime) -> datetime | None:
    """Convert a site-local naive datetime to a naive UTC one, or ``None``."""
    try:
        from zoneinfo import ZoneInfo

        from frappe.utils import get_system_timezone

        tz_name = get_system_timezone()
        if not tz_name:
            return None
        return (
            local_dt.replace(tzinfo=ZoneInfo(tz_name))
            .astimezone(timezone.utc)
            .replace(tzinfo=None)
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_outbound_utc_conversion_failed",
            "error": str(exc),
        })
        return None


def _build_order_creation_dates(invoice: frappe.model.document.Document) -> dict[str, str]:
    """``date_created`` / ``date_created_gmt`` from the invoice's posting moment.

    ``date_created`` is site-local and ``date_created_gmt`` is UTC — WooCommerce
    treats them as two views of the same instant, so sending the same string for
    both would shift the order by the site's UTC offset in half the store's
    screens. When the timezone cannot be resolved we send only the local value
    and let WooCommerce derive the GMT one, rather than send a wrong number.
    """
    local_dt = _invoice_posting_datetime(invoice)
    if not local_dt:
        return {}

    local_dt = local_dt.replace(microsecond=0)
    dates = {"date_created": local_dt.isoformat(timespec="seconds")}

    utc_dt = _to_utc_naive(local_dt)
    if utc_dt:
        dates["date_created_gmt"] = utc_dt.replace(microsecond=0).isoformat(timespec="seconds")
    return dates


def _build_delivery_metadata(invoice: frappe.model.document.Document) -> list[dict[str, str]]:
    delivery_date = _coerce_delivery_date(
        _first_present(
            getattr(invoice, "custom_delivery_date", None),
            getattr(invoice, "delivery_date", None),
        )
    )
    if not delivery_date:
        return []

    # Mirror the shape the ORDDD plugin itself writes on a Woo-native checkout,
    # e.g. "4 August, 2026" — not "%A, %B %d, %Y". The plugin reads these keys
    # back when it renders/edits the slot in the order screen.
    formatted_date = f"{delivery_date.day} {delivery_date.strftime('%B')}, {delivery_date.year}"
    midnight_timestamp = calendar.timegm(datetime.combine(delivery_date, dt_time(0, 0)).timetuple())
    metadata = [
        {"key": "_orddd_timestamp", "value": str(midnight_timestamp)},
        {"key": "_orddd_delivery_date", "value": formatted_date},
        {"key": "_orddd_delivery_date_label", "value": "Delivery Date"},
        {"key": "_orddd_delivery_date_meta_id", "value": "0"},
        {"key": "_orddd_delivery_schedule_id", "value": "0"},
        {"key": "_orddd_vendor_id", "value": [0]},
        {"key": "_total_delivery_charges", "value": "0"},
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
            _first_present(
                getattr(invoice, "custom_delivery_time", None),
                getattr(invoice, "delivery_time", None),
            )
        )
        if legacy_delivery_time:
            delivery_time_from = legacy_delivery_time
            time_slot_label = legacy_delivery_time.strftime("%H:%M")

    if time_slot_label:
        slot_start_timestamp = midnight_timestamp + (
            delivery_time_from.hour * 3600 + delivery_time_from.minute * 60 + delivery_time_from.second
        )
        metadata.extend([
            {"key": "_orddd_time_slot", "value": time_slot_label},
            {"key": "_orddd_time_slot_label", "value": "Time Slot"},
            {"key": "_orddd_time_slot_meta_id", "value": "0"},
            {"key": "_orddd_timeslot_timestamp", "value": str(slot_start_timestamp)},
            {"key": "Time Slot", "value": time_slot_label},
        ])

    return metadata


def _build_tracking_metadata(invoice: frappe.model.document.Document) -> list[dict[str, str]]:
    """``_jarz_tracking_url`` / ``_jarz_tracking_token`` for the order payload.

    Thin adapter over ``services/tracking_link`` so the outbound payload has one
    obvious seam to mock and the URL/token rules live in one module. Reads the
    Settings singleton again rather than threading it through
    ``_build_order_payload``; Frappe caches Single docs, and ``_get_settings`` is
    the seam every test in this app already patches.

    Never raises: ``tracking_link`` swallows its own failures, and a missing
    tracking link must never stop an order from reaching the store.
    """
    try:
        settings, _cfg = _get_settings()
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_tracking_settings_unavailable",
            "invoice": getattr(invoice, "name", None),
            "error": str(exc),
        })
        return []
    return tracking_link.build_tracking_metadata(invoice, settings)


def sync_sales_invoice(invoice_name: str, *, reason: str | None = None, cancel: bool = False, force: bool = False) -> dict:
    settings, cfg = _get_settings()
    if not cfg.enable_order_push and not force:
        _note_outbound_disabled("enable_outbound_orders")
        return {"skipped": True, "reason": "disabled"}

    try:
        invoice = frappe.get_doc("Sales Invoice", invoice_name)
    except frappe.DoesNotExistError:
        return {"skipped": True, "reason": "missing"}

    if getattr(invoice.flags, "ignore_woo_outbound", False) or getattr(frappe.flags, "ignore_woo_outbound", False):
        return {"skipped": True, "reason": "inbound"}

    # A credit note is a local accounting correction, never a Woo order. It
    # inherits `woo_order_id` from the invoice it reverses (the custom field is
    # copyable), so without this guard submitting one would push a status update
    # to the live store — re-opening a cancelled or completed order. And when the
    # id is blank it is worse: the flow below would create a brand-new Woo order.
    # Whether the customer-facing order should change on a return is decided by
    # the return workflow, not by this hook.
    if int(invoice.get("is_return") or 0):
        return {"skipped": True, "reason": "credit_note"}

    # When the invoice was just created as the replacement half of a Woo-initiated
    # amendment, Woo already holds the authoritative state — suppress the outbound push.
    if getattr(invoice.flags, "skip_woo_outbound_after_amend", False):
        return {"skipped": True, "reason": "amendment_no_push"}

    _woo_id = invoice.get("woo_order_id") or _recover_amended_invoice_woo_order_id(invoice)
    order_map_exists = bool(_woo_id and frappe.db.exists("WooCommerce Order Map", {"woo_order_id": _woo_id}))

    # An amendment is not a cancellation. ERPNext expresses "change a submitted
    # invoice" as cancel-then-recreate, and the cancel half fires this hook with
    # cancel=True — which would tell the store the customer's order is dead,
    # moments before the replacement pushes it back to processing. Whichever of
    # the two lands last wins, so the store's status was a coin flip.
    #
    # The POS amendment sidesteps this by setting flags.ignore_woo_outbound on
    # the source, but a plain Desk cancel+amend does not, and neither path moved
    # the Order Map onto the replacement. Resolve it here instead, at the point
    # where the replacement is actually observable: if one exists, hand the Woo
    # order over to it and stand down. If the invoice was genuinely just
    # cancelled, no replacement exists and the cancel proceeds as before.
    if cancel or invoice.docstatus == 2:
        replacement_invoice = _find_replacement_invoice(invoice_name)
        if replacement_invoice:
            _handover_woo_order_to_replacement(
                woo_order_id=_woo_id,
                source_invoice=invoice_name,
                replacement_invoice=replacement_invoice,
            )
            _mark_invoice_status(invoice_name, status="Skipped")
            return {
                "skipped": True,
                "reason": "amended_replacement_owns_order",
                "woo_order_id": _woo_id,
                "replacement_invoice": replacement_invoice,
            }

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
    recovered_amended_order_id = bool(woo_order_id and not original_woo_order_id)
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

    unpayable_transition = _detect_unpayable_transition(invoice, existing_order)

    try:
        payload = _build_order_payload(
            invoice,
            cfg,
            cancel=cancel,
            existing_order=existing_order,
            is_create=not woo_order_id,
        )
    except MissingWooProductError as exc:
        LOGGER.warning({
            "event": "woo_outbound_missing_product_mapping",
            "invoice": invoice_name,
            "detail": str(exc),
            "reason": reason,
        })
        _mark_invoice_status(invoice_name, status="error", error=str(exc))
        return {"status": "error", "detail": str(exc)}

    # --- Refuse to push a total we already know is wrong ---------------------
    # _check_response_total below catches drift AFTER the store has been written,
    # which is the right place for anything only the store can tell us. But a
    # payload whose own arithmetic does not reach grand_total is knowably wrong
    # BEFORE the request, and pushing it anyway would state a false amount on a
    # customer-facing order and only then flag it.
    #
    # This became reachable when unmapped lines started degrading per line
    # instead of failing the whole invoice: dropping a line the store has no
    # product for keeps the order pushable, but silently removes its money. Four
    # production invoices are in exactly that state (e.g. ACC-SINV-2026-15618,
    # payload 1500.00 against grand_total 2288.40). Better to stay Error and name
    # the gap than to publish a short order.
    prepush_mismatch = _payload_total_mismatch(payload, invoice)
    if prepush_mismatch:
        LOGGER.error({
            "event": "woo_outbound_payload_total_mismatch",
            "invoice": invoice_name,
            "detail": prepush_mismatch,
            "reason": reason,
        })
        _mark_invoice_status(invoice_name, status="error", error=prepush_mismatch)
        return {"status": "error", "detail": f"payload_total_mismatch:{prepush_mismatch}"}

    if (
        order_map_exists
        and woo_order_id
        and existing_order
        and not recovered_amended_order_id
        and not _order_payload_requires_update(existing_order, payload)
    ):
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
    desired_woo_status = payload.get("status") or ""

    # The store has accepted the order, so record what we know about it BEFORE
    # deciding whether we are happy with the answer. The verification guards
    # below mark the invoice Error, and if they ran first a freshly POSTed order
    # would have no `woo_order_id` recorded anywhere — the retry would create a
    # second order in the customer's account.
    updates = {
        "woo_outbound_status": "Synced",
        "woo_outbound_error": "",
        "woo_outbound_synced_on": now_datetime(),
    }
    if woo_id and str(woo_id) != str(original_woo_order_id or ""):
        updates["woo_order_id"] = woo_id
    if woo_number:
        updates["woo_order_number"] = woo_number

    contact_snapshot, territory_snapshot = _extract_response_snapshots(
        response, invoice_name=invoice_name
    )
    updates.update(_contact_snapshot_invoice_values(contact_snapshot))

    frappe.db.set_value("Sales Invoice", invoice_name, updates, update_modified=False)
    if "woo_order_id" in updates:
        _publish_woo_order_assigned(invoice_name, woo_id, invoice=invoice)

    response_status = response.get("status") if isinstance(response, dict) else None
    response_payment_method = response.get("payment_method") if isinstance(response, dict) else None
    response_hash = None
    if isinstance(response, dict):
        try:
            from jarz_woocommerce_integration.services.order_sync import _compute_order_hash

            response_hash = _compute_order_hash(response)
        except Exception:
            response_hash = None

    _relink_order_map_to_invoice(
        woo_id or woo_order_id,
        invoice_name,
        currency=getattr(invoice, "currency", None),
        total=flt(getattr(invoice, "grand_total", 0) or 0),
        status=response_status or desired_woo_status,
        payment_method=response_payment_method or payload.get("payment_method"),
        hash_value=response_hash,
        order_response=response if isinstance(response, dict) else None,
        contact_snapshot=contact_snapshot,
        territory_snapshot=territory_snapshot,
    )

    # `set_paid` only ever moves an order *to* paid. Say so out loud when the
    # invoice has gone back to outstanding — see `_detect_unpayable_transition`.
    if unpayable_transition:
        _report_unpayable_transition(client, woo_id or woo_order_id, invoice_name)

    # --- Verify WooCommerce accepted the desired status ------------------
    # Some WordPress plugins (e.g. delivery management) silently override the
    # status in the same PUT response.  If the response status doesn't match
    # what we sent, do not leave the invoice reading Synced — flag it as Error
    # so the reconcile sweep retries.
    actual_woo_status = (response.get("status") or "") if isinstance(response, dict) else ""
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

    # --- Verify the store charged what the invoice says ------------------
    # There are no VAT rows on this site: the only components of an order total
    # are the line totals, the shipping-income row and the header discount. So
    # the store's total must equal `grand_total` exactly, and any drift means
    # one of those three did not survive the push — silently, until now (F-03).
    #
    # Deliberately not raised: the order exists in the store, and an exception
    # here would have the outbox retry the POST and duplicate it.
    total_mismatch = _check_response_total(
        response, invoice, invoice_name=invoice_name, payload=payload, reason=reason
    )
    if total_mismatch:
        _mark_invoice_status(invoice_name, status="error", error=total_mismatch)
        return {"status": "error", "detail": f"total_mismatch:{total_mismatch}"}

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
