from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Tuple

from frappe.utils.background_jobs import get_redis_conn
from frappe.utils import get_datetime

import frappe

from jarz_woocommerce_integration.utils.http_client import WooClient
from jarz_woocommerce_integration.utils.custom_fields import ensure_custom_fields
from jarz_woocommerce_integration.services.customer_sync import ensure_customer_with_addresses


DEFAULT_LIVE_ORDER_OVERLAP_MINUTES = 15
DEFAULT_LIVE_ORDER_MAX_PAGES = 6
DEFAULT_LIVE_BOOTSTRAP_LOOKBACK_MINUTES = 240

DEFAULT_CANCELLED_ORDER_OVERLAP_MINUTES = 180
DEFAULT_CANCELLED_ORDER_MAX_PAGES = 12
DEFAULT_CANCELLED_BOOTSTRAP_LOOKBACK_MINUTES = 1440

DEFAULT_RECONCILE_LOOKBACK_MINUTES = 1440
DEFAULT_RECONCILE_MAX_PAGES = 20
PROCESSING_EQUIVALENT_WOO_STATUSES = (
    "processing",
    "pre-nasrcity",
    "pre-ismailia",
    "pre-hadayk",
    "pre-hadayek",
    "pre-dokki",
)
RECONCILE_TARGET_WOO_STATUSES = PROCESSING_EQUIVALENT_WOO_STATUSES + (
    "completed", "cancelled", "refunded", "failed"
)
# Value actually sent to Woo REST API — custom statuses are filtered client-side
RECONCILE_API_STATUS_FILTER = "any"
# Local-membership constant (not sent to Woo API). Used for set membership checks and tests.
RECONCILE_ORDER_STATUSES = ",".join(RECONCILE_TARGET_WOO_STATUSES)
KASHIER_AUTO_PAY_METHODS = {"Kashier Card", "Kashier Wallet"}
NON_PAYABLE_WOO_STATUSES = {"cancelled", "refunded", "failed"}

ORDER_SYNC_CURSOR_FIELDS = {
    "live": {
        "modified": "live_order_cursor_modified_gmt",
        "order_id": "live_order_cursor_order_id",
        "synced_on": "live_order_cursor_synced_on",
    },
    "cancelled": {
        "modified": "cancelled_order_cursor_modified_gmt",
        "order_id": "cancelled_order_cursor_order_id",
        "synced_on": "cancelled_order_cursor_synced_on",
    },
}


class MigrationCache:
    """In-memory lookup caches for historical migration.

    Pre-loads Items, Item Prices, Bundles, and Territory chains so that
    per-order processing can use dict lookups instead of DB queries.
    """

    def __init__(self, price_list: str | None = None, company: str | None = None):
        self.sku_to_item: dict[str, str] = {}          # item_code → item_code
        self.woo_pid_to_item: dict[str, str] = {}      # woo_product_id → item_code
        self.woo_vid_to_item: dict[str, str] = {}      # woo_variation_id → item_code
        self.item_prices: dict[tuple[str, str], float] = {}  # (price_list, item_code) → rate
        self.bundle_map: dict[str, str] = {}            # woo_bundle_id → bundle_code
        self.bundle_free_shipping: dict[str, bool] = {}  # bundle_code → free_shipping
        self.territory_chain: dict[str, dict] = {}      # territory → {pos_profile, warehouse, price_list}
        self.customer_cache: dict[str, str] = {}        # composite key → customer name
        self.order_map_set: set[int] = set()            # woo_order_ids already mapped
        self.item_groups: dict[str, str] = {}           # item_code → item_group
        self.company_accounts: dict[str, str | None] = {}  # account_key → account name
        self.link_field: str = "erpnext_sales_invoice"  # Order Map link field name
        self.address_cache: dict[tuple, str] = {}       # (customer, type, line1_hash) → address name
        self.territory_state_cache: dict[str, str | None] = {}  # state_value → territory name
        self._price_list = price_list
        self._company = company

    def load(self):
        """Pre-load all lookup tables from DB."""
        self._load_items()
        self._load_prices()
        self._load_bundles()
        self._load_territory_chain()
        self._load_existing_maps()
        self._load_company_accounts()
        self._load_link_field()

    def _load_items(self):
        rows = frappe.db.sql(
            "SELECT name, IFNULL(item_name, '') as item_name, "
            "IFNULL(woo_product_id, '') as woo_product_id, "
            "IFNULL(woo_variation_id, '') as woo_variation_id, "
            "IFNULL(disabled, 0) as disabled, "
            "IFNULL(item_group, '') as item_group "
            "FROM `tabItem`",
            as_dict=True,
        )
        self.item_name_to_item: dict[str, str] = {}  # lower(item_name) → item_code
        for r in rows:
            if r.item_group:
                self.item_groups[r.name] = r.item_group
            if not r.disabled:
                # Active items: available for SKU and product_id resolution
                self.sku_to_item[r.name] = r.name
                if r.woo_product_id:
                    self.woo_pid_to_item[str(r.woo_product_id).strip()] = r.name
                if r.woo_variation_id:
                    self.woo_vid_to_item[str(r.woo_variation_id).strip()] = r.name
            # All items (including disabled): available for name-based historical matching
            if r.item_name:
                self.item_name_to_item[r.item_name.strip().lower()] = r.name

    def _load_prices(self):
        rows = frappe.db.sql(
            "SELECT price_list, item_code, price_list_rate FROM `tabItem Price` "
            "WHERE selling = 1 AND IFNULL(price_list_rate, 0) > 0",
            as_dict=True,
        )
        for r in rows:
            self.item_prices[(r.price_list, r.item_code)] = float(r.price_list_rate)

    def _load_bundles(self):
        rows = frappe.db.sql(
            "SELECT name, woo_bundle_id, IFNULL(free_shipping, 0) AS free_shipping "
            "FROM `tabWoo Jarz Bundle`",
            as_dict=True,
        )
        for r in rows:
            self.bundle_free_shipping[r.name] = bool(r.free_shipping)
            if r.woo_bundle_id:
                self.bundle_map[str(r.woo_bundle_id).strip()] = r.name

    def _load_territory_chain(self):
        territories = frappe.db.sql(
            "SELECT name, IFNULL(pos_profile, '') as pos_profile, "
            "IFNULL(delivery_income, 0) as delivery_income "
            "FROM `tabTerritory`",
            as_dict=True,
        )
        pos_profiles = {}
        pp_rows = frappe.db.sql(
            "SELECT name, IFNULL(warehouse, '') as warehouse, "
            "IFNULL(selling_price_list, '') as price_list "
            "FROM `tabPOS Profile`",
            as_dict=True,
        )
        for p in pp_rows:
            pos_profiles[p.name] = {"warehouse": p.warehouse, "price_list": p.price_list}

        for t in territories:
            pp = t.pos_profile
            pp_data = pos_profiles.get(pp, {}) if pp else {}
            self.territory_chain[t.name] = {
                "pos_profile": pp or None,
                "warehouse": pp_data.get("warehouse") or None,
                "price_list": pp_data.get("price_list") or None,
                "delivery_income": float(t.delivery_income or 0),
            }

    def _load_existing_maps(self):
        rows = frappe.db.sql(
            "SELECT woo_order_id FROM `tabWooCommerce Order Map` "
            "WHERE IFNULL(erpnext_sales_invoice, '') != ''",
            as_dict=True,
        )
        for r in rows:
            if r.woo_order_id:
                self.order_map_set.add(int(r.woo_order_id))

    def _load_company_accounts(self):
        company = self._company
        if not company:
            company = frappe.defaults.get_global_default("company")
        if not company:
            return
        row = frappe.db.get_value(
            "Company", company,
            ["default_cash_account", "default_bank_account",
             "custom_kashier_account", "default_receivable_account"],
            as_dict=True,
        )
        if row:
            self.company_accounts["Cash"] = row.get("default_cash_account")
            self.company_accounts["Instapay"] = row.get("default_bank_account")
            self.company_accounts["Kashier Card"] = row.get("custom_kashier_account")
            self.company_accounts["Kashier Wallet"] = row.get("custom_kashier_account")
            self.company_accounts["default_receivable_account"] = row.get("default_receivable_account")
        # Mobile Wallet: from Mode of Payment Account
        mw = frappe.db.get_value(
            "Mode of Payment Account",
            {"parent": "Mobile Wallet", "company": company},
            "default_account",
        )
        self.company_accounts["Mobile Wallet"] = mw or row.get("default_bank_account") if row else mw

    def _load_link_field(self):
        try:
            cols = frappe.db.get_table_columns("WooCommerce Order Map") or []
            if "erpnext_sales_invoice" in cols:
                self.link_field = "erpnext_sales_invoice"
            elif "sales_invoice" in cols:
                self.link_field = "sales_invoice"
        except Exception:
            pass

    def resolve_item(self, sku: str, product_id, variation_id=None) -> str | None:
        """Resolve a Woo line item to an ERPNext item_code via cache.

        Resolution order: variation_id → sku → product_id.
        """
        if variation_id and int(variation_id) > 0:
            found = self.woo_vid_to_item.get(str(variation_id).strip())
            if found:
                return found
        if sku and sku in self.sku_to_item:
            return self.sku_to_item[sku]
        if product_id:
            return self.woo_pid_to_item.get(str(product_id).strip())
        return None

    def resolve_item_by_name(self, item_name: str) -> str | None:
        """Resolve a Woo line item to an ERPNext item_code by item name (case-insensitive)."""
        if not item_name:
            return None
        return self.item_name_to_item.get(item_name.strip().lower())

    def get_price(self, item_code: str, price_list: str | None = None) -> float | None:
        pl = price_list or self._price_list
        if pl:
            return self.item_prices.get((pl, item_code))
        return None

    def get_bundle_code(self, product_id) -> str | None:
        if product_id:
            return self.bundle_map.get(str(product_id).strip())
        return None

    def bundle_has_free_shipping(self, bundle_code: str | None) -> bool:
        if not bundle_code:
            return False
        return bool(self.bundle_free_shipping.get(bundle_code))

    def get_territory_data(self, territory_name: str | None) -> dict:
        if not territory_name:
            return {}
        return self.territory_chain.get(territory_name, {})


def _is_processing_equivalent_woo_status(woo_status: str | None) -> bool:
    return (woo_status or "").strip().lower() in PROCESSING_EQUIVALENT_WOO_STATUSES


def _map_status(woo_status: str | None, is_historical: bool = False) -> dict[str, Any]:
    """Map Woo status to ERPNext docstatus and custom state.
    
    Args:
        woo_status: WooCommerce order status
        is_historical: If True, creates paid invoices for completed orders (historical migration)
                      If False, creates unpaid submitted invoices (live orders)
    """
    s = (woo_status or "").lower()
    if s == "completed":
        if is_historical:
            # Historical: mark as paid (submitted + paid status)
            return {"docstatus": 1, "custom_state": "Completed", "is_paid": True}
        else:
            # Live: mark as submitted but unpaid
            return {"docstatus": 1, "custom_state": "Completed", "is_paid": False}
    if _is_processing_equivalent_woo_status(s):
        # Processing = payment received, not shipped yet. Submit but never mark as paid.
        return {"docstatus": 1, "custom_state": "Processing", "is_paid": False}
    if s == "out-for-delivery":
        return {"docstatus": 1, "custom_state": "Out for Delivery", "is_paid": False}
    if s in {"cancelled", "refunded", "failed"}:
        if is_historical:
            # Historical: keep as Draft to avoid GL entry pollution from submit+cancel cycle
            return {"docstatus": 0, "custom_state": "Cancelled", "is_paid": False}
        return {"docstatus": 2, "custom_state": "Cancelled", "is_paid": False}
    return {"docstatus": 0, "custom_state": "Draft", "is_paid": False}


def _map_payment_method(woo_payment_method: str | None, woo_payment_method_title: str | None = None) -> str | None:
    """Map WooCommerce payment method to ERPNext custom_payment_method.
    
    WooCommerce -> ERPNext mapping:
    - cod -> Cash
    - instapay -> Instapay
    - wallet -> Mobile Wallet
    - card -> Kashier Card
    - kashier_card -> Kashier Card
    - kashier -> title-aware fallback (wallet -> Kashier Wallet, else Kashier Card)
    - kashier_wallet -> Kashier Wallet
    """
    if not woo_payment_method:
        return None
    
    pm = woo_payment_method.lower().strip()
    title = (woo_payment_method_title or "").lower().strip()
    if pm == "cod":
        return "Cash"
    elif pm == "instapay":
        return "Instapay"
    elif pm == "wallet":
        return "Mobile Wallet"
    elif pm in ("card", "kashier_card"):
        return "Kashier Card"
    elif pm == "kashier":
        return "Kashier Wallet" if "wallet" in title else "Kashier Card"
    elif pm == "kashier_wallet":
        return "Kashier Wallet"
    else:
        return None


def _should_treat_inbound_order_as_paid(
    woo_status: str | None,
    custom_payment_method: str | None,
    status_map: dict[str, Any] | None = None,
    is_historical: bool = False,
) -> bool:
    """Return whether inbound processing should settle the invoice with a Payment Entry."""
    status = (woo_status or "").strip().lower()
    if status in NON_PAYABLE_WOO_STATUSES:
        return False
    if status_map and status_map.get("is_paid"):
        return True
    if is_historical:
        return False
    return (
        (custom_payment_method or "").strip() in KASHIER_AUTO_PAY_METHODS
        and (_is_processing_equivalent_woo_status(status) or status == "completed")
    )


def _add_payment_failure_comment(invoice_name: str, woo_order_id: Any) -> None:
    message = (
        f"Order {woo_order_id} cancelled because WooCommerce reported a payment failure."
    )

    existing = frappe.get_all(
        "Comment",
        filters={
            "reference_doctype": "Sales Invoice",
            "reference_name": invoice_name,
            "comment_type": "Comment",
            "content": ["like", "%payment failure%"],
        },
        limit=1,
    )
    if not existing:
        try:
            frappe.get_doc("Sales Invoice", invoice_name).add_comment("Comment", message)
        except Exception:
            frappe.log_error(
                frappe.get_traceback(),
                "Woo Payment Failure Comment Error",
            )


def _format_datetime_for_woo(dt_val: datetime) -> str:
    if dt_val.tzinfo is None:
        dt_val = dt_val.replace(tzinfo=timezone.utc)
    dt_utc = dt_val.astimezone(timezone.utc)
    return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _minutes_ago_for_woo(minutes: int) -> str:
    return _format_datetime_for_woo(datetime.now(timezone.utc) - timedelta(minutes=int(minutes)))


def _extract_order_modified_ts(order: dict[str, Any]) -> datetime | None:
    for key in ("date_modified_gmt", "date_modified", "date_created_gmt", "date_created"):
        raw = order.get(key)
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


def _extract_order_cursor(order: dict[str, Any]) -> tuple[datetime | None, int]:
    modified_at = _extract_order_modified_ts(order)
    try:
        order_id = int(order.get("id") or 0)
    except Exception:  # noqa: BLE001
        order_id = 0
    return modified_at, order_id


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return int(default)


def _get_setting_int(settings: Any, fieldname: str, default: int) -> int:
    return max(1, _safe_int(getattr(settings, fieldname, default) or default, default))


def _get_order_sync_cursor(settings: Any, cursor_name: str) -> tuple[datetime | None, int]:
    fields = ORDER_SYNC_CURSOR_FIELDS[cursor_name]
    raw_modified = getattr(settings, fields["modified"], None)
    modified_at = None
    if raw_modified:
        try:
            modified_at = get_datetime(raw_modified)
            if modified_at is not None and modified_at.tzinfo is None:
                modified_at = modified_at.replace(tzinfo=timezone.utc)
            if modified_at is not None:
                modified_at = modified_at.astimezone(timezone.utc)
        except Exception:  # noqa: BLE001
            modified_at = None
    order_id = _safe_int(getattr(settings, fields["order_id"], 0) or 0, 0)
    return modified_at, order_id


def _set_order_sync_cursor(
    settings: Any,
    cursor_name: str,
    modified_at: datetime | None,
    order_id: int,
    synced_on: datetime | None = None,
) -> None:
    fields = ORDER_SYNC_CURSOR_FIELDS[cursor_name]
    updates = {
        fields["order_id"]: int(order_id or 0),
        fields["synced_on"]: synced_on or frappe.utils.now_datetime(),
    }
    if modified_at is not None:
        updates[fields["modified"]] = _format_datetime_for_woo(modified_at)

    for fieldname, value in updates.items():
        frappe.db.set_single_value("WooCommerce Settings", fieldname, value)
        setattr(settings, fieldname, value)


def _update_order_sync_cursor_from_metrics(settings: Any, cursor_name: str, metrics: dict[str, Any]) -> None:
    latest_raw = metrics.get("latest_seen_modified_gmt")
    latest_dt = None
    if latest_raw:
        try:
            latest_dt = get_datetime(latest_raw)
            if latest_dt is not None and latest_dt.tzinfo is None:
                latest_dt = latest_dt.replace(tzinfo=timezone.utc)
            if latest_dt is not None:
                latest_dt = latest_dt.astimezone(timezone.utc)
        except Exception:  # noqa: BLE001
            latest_dt = None
    latest_order_id = _safe_int(metrics.get("latest_seen_order_id") or 0, 0)
    _set_order_sync_cursor(settings, cursor_name, latest_dt, latest_order_id)


def _serialize_sync_message(message: Any, limit: int = 1000) -> str:
    if message is None:
        return ""
    if isinstance(message, str):
        text = message
    else:
        try:
            text = json.dumps(message, ensure_ascii=False, default=str, sort_keys=True)
        except Exception:  # noqa: BLE001
            text = str(message)
    return text[:limit]


def create_sync_log_entry(
    operation: str,
    status: str,
    message: Any,
    *,
    woo_order_id: int | None = None,
    started_on: datetime | None = None,
):
    try:
        return frappe.get_doc(
            {
                "doctype": "WooCommerce Sync Log",
                "operation": operation,
                "woo_order_id": woo_order_id,
                "status": status,
                "message": _serialize_sync_message(message),
                "started_on": started_on or frappe.utils.now_datetime(),
            }
        ).insert(ignore_permissions=True)
    except Exception:  # noqa: BLE001
        return None


def finish_sync_log_entry(
    log_doc: Any,
    status: str,
    message: Any,
    *,
    traceback: str | None = None,
    started_on: datetime | None = None,
) -> None:
    if not log_doc:
        return
    ended_on = frappe.utils.now_datetime()
    duration = None
    if started_on is not None:
        duration = (ended_on - started_on).total_seconds()
    try:
        updates = {
            "status": status,
            "message": _serialize_sync_message(message),
            "ended_on": ended_on,
        }
        if duration is not None:
            updates["duration"] = duration
        if traceback:
            updates["traceback"] = traceback[:2000]
        log_doc.db_set(updates, commit=True)
    except Exception:  # noqa: BLE001
        pass


def _list_orders_window(
    client: WooClient,
    params: dict[str, Any],
    max_pages: int = 1,
) -> tuple[list[dict[str, Any]], int, int]:
    base_params = (params or {}).copy()
    per_page = max(1, min(int(base_params.get("per_page") or 20), 100))
    page = max(1, int(base_params.get("page") or 1))
    page_limit = max(1, int(max_pages or 1))
    base_params["per_page"] = per_page

    orders: list[dict[str, Any]] = []
    pages_fetched = 0
    total_pages = 0

    while pages_fetched < page_limit:
        current_params = base_params.copy()
        current_params["page"] = page
        page_orders, _, response_total_pages = client.list_orders_with_meta(params=current_params)
        pages_fetched += 1
        total_pages = max(total_pages, int(response_total_pages or 0))

        if not page_orders:
            break

        orders.extend(page_orders)

        if len(page_orders) < per_page:
            break
        if response_total_pages and page >= int(response_total_pages):
            break

        page += 1

    return orders, pages_fetched, total_pages


def _resolve_posting_date(order: dict, is_historical: bool) -> str:
    """Return the posting_date to use for a Sales Invoice.

    For historical imports, use the original WooCommerce order creation date.
    For live orders, use today.
    """
    if is_historical:
        raw = order.get("date_created") or order.get("date_completed") or ""
        if raw:
            # WooCommerce dates come as "2025-01-15T12:30:00" or "2025-01-15"
            date_part = str(raw).split("T")[0].strip()
            if date_part and len(date_part) >= 10:
                return date_part
    return frappe.utils.today()


def _compute_order_hash(order: dict) -> str:
    import hashlib
    import json as _json

    payload = {
        "id": order.get("id"),
        "total": order.get("total"),
        "currency": order.get("currency"),
        "status": order.get("status"),
        "line_count": len(order.get("line_items") or []),
        "updated": order.get("date_modified") or order.get("date_created"),
    }
    b = _json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def _get_line_meta_value(meta_data_list: list[dict] | None, target_key: str) -> Any | None:
    for md in (meta_data_list or []):
        key = (md.get("key") or md.get("display_key") or "").strip()
        if key == target_key:
            value = md.get("value")
            return value if value not in (None, "") else md.get("display_value")
    return None


def _split_woosb_ids(raw_value: Any) -> list[str]:
    if raw_value in (None, ""):
        return []

    entries: list[str] = []
    current: list[str] = []
    brace_depth = 0

    for ch in str(raw_value):
        if ch == "," and brace_depth == 0:
            entry = "".join(current).strip()
            if entry:
                entries.append(entry)
            current = []
            continue
        if ch == "{":
            brace_depth += 1
        elif ch == "}" and brace_depth > 0:
            brace_depth -= 1
        current.append(ch)

    entry = "".join(current).strip()
    if entry:
        entries.append(entry)
    return entries


def _resolve_bundle_selection_item_code(
    selection_identifier: Any,
    cache: "MigrationCache | None" = None,
) -> str | None:
    identifier = str(selection_identifier or "").strip()
    if not identifier:
        return None

    if cache:
        return cache.resolve_item("", identifier, variation_id=identifier)

    item_code = frappe.db.get_value("Item", {"woo_variation_id": identifier}, "name")
    if item_code:
        return item_code
    return frappe.db.get_value("Item", {"woo_product_id": identifier}, "name")


def _append_bundle_selection(
    selected_items: dict[str, list[dict]],
    item_group: str,
    item_code: str,
    quantity: int,
) -> None:
    group_list = selected_items.setdefault(item_group, [])
    for entry in group_list:
        if entry["item_code"] == item_code:
            entry["selected_qty"] += quantity
            return
    group_list.append({"item_code": item_code, "selected_qty": quantity})


def _build_bundle_selections_from_parent_meta(
    parent_line: dict,
    parent_qty: int,
    cache: "MigrationCache | None" = None,
) -> dict:
    raw_selection_value = _get_line_meta_value(parent_line.get("meta_data"), "_woosb_ids")
    if not raw_selection_value:
        return {}

    selected_items: dict[str, list[dict]] = {}
    parent_line_id = parent_line.get("id") or "unknown"

    for entry in _split_woosb_ids(raw_selection_value):
        parts = entry.split("/", 3)
        if len(parts) < 3:
            frappe.logger().warning(
                f"Bundle selection: invalid _woosb_ids entry '{entry}' on parent line {parent_line_id}"
            )
            return {}

        child_identifier = str(parts[0] or "").strip()
        try:
            child_qty = int(float(parts[2] or 0))
        except Exception:
            frappe.logger().warning(
                f"Bundle selection: invalid quantity in _woosb_ids entry '{entry}' on parent line {parent_line_id}"
            )
            return {}

        if not child_identifier or child_qty <= 0:
            frappe.logger().warning(
                f"Bundle selection: empty child identifier or quantity in _woosb_ids entry '{entry}' on parent line {parent_line_id}"
            )
            return {}

        per_bundle_qty = child_qty // max(1, parent_qty)
        if per_bundle_qty <= 0:
            per_bundle_qty = child_qty

        wc_item_code = _resolve_bundle_selection_item_code(child_identifier, cache=cache)
        if not wc_item_code:
            frappe.logger().warning(
                f"Bundle selection: cannot map _woosb_ids child={child_identifier} on parent line {parent_line_id}"
            )
            return {}

        wc_item_group = (
            cache.item_groups.get(wc_item_code) if cache else None
        ) or frappe.db.get_value("Item", wc_item_code, "item_group")
        if not wc_item_group:
            frappe.logger().warning(
                f"Bundle selection: item {wc_item_code} has no item_group on parent line {parent_line_id}"
            )
            return {}

        _append_bundle_selection(selected_items, wc_item_group, wc_item_code, per_bundle_qty)

    if selected_items:
        frappe.logger().info(
            f"Bundle selections built from parent _woosb_ids: "
            f"{{{', '.join(f'{g}: {len(v)} item(s)' for g, v in selected_items.items())}}}"
        )
    return selected_items


def _build_bundle_selections(
    line_items: list[dict],
    parent_product_id: int | str,
    parent_qty: int,
    cache: "MigrationCache | None" = None,
    parent_line: dict | None = None,
) -> dict:
    """Parse WooCommerce child line items to build *selected_items* for BundleProcessor.

    WooCommerce Smart Bundles (WOOSB) may send parent-specific ``_woosb_ids`` on
    the bundle parent. When present, that metadata is preferred because it is tied
    to a specific parent line instance. Otherwise, fall back to scanning child rows
    whose ``meta_data`` contains ``_woosb_parent_id == parent_product_id``.

    Returns
    -------
    dict
        ``{item_group_name: [{"item_code": ..., "selected_qty": ...}, ...]}``
        Empty dict when any child cannot be mapped (caller should fall back to
        default bundle expansion).
    """
    parent_meta_selections = (
        _build_bundle_selections_from_parent_meta(parent_line, parent_qty, cache=cache)
        if parent_line
        else {}
    )
    if parent_line and _get_line_meta_value(parent_line.get("meta_data"), "_woosb_ids"):
        return parent_meta_selections

    parent_pid_str = str(parent_product_id)

    # Collect Woo child lines belonging to this bundle parent
    woo_children: list[dict] = []
    for _li in line_items:
        for md in (_li.get("meta_data") or []):
            key = (md.get("key") or md.get("display_key") or "").strip()
            if key == "_woosb_parent_id":
                val = str(md.get("value") or md.get("display_value") or "").strip()
                if val == parent_pid_str:
                    woo_children.append(_li)
                break  # only one _woosb_parent_id per line item

    if not woo_children:
        return {}

    selected_items: dict[str, list[dict]] = {}

    for wc in woo_children:
        wc_sku = (wc.get("sku") or "").strip()
        wc_product_id = wc.get("product_id")
        wc_variation_id = wc.get("variation_id")
        wc_qty = int(float(wc.get("quantity") or 0))
        if wc_qty <= 0:
            continue

        # Resolve to ERPNext Item code
        wc_item_code = None
        if cache:
            wc_item_code = cache.resolve_item(wc_sku, wc_product_id, variation_id=wc_variation_id)
        else:
            # Variation lookup first
            if wc_variation_id and int(wc_variation_id) > 0:
                wc_item_code = frappe.db.get_value(
                    "Item", {"woo_variation_id": str(wc_variation_id)}, "name"
                )
            if not wc_item_code and wc_sku and frappe.db.exists("Item", wc_sku):
                wc_item_code = wc_sku
            if not wc_item_code and wc_product_id:
                wc_item_code = frappe.db.get_value(
                    "Item", {"woo_product_id": str(wc_product_id)}, "name"
                )

        if not wc_item_code:
            frappe.logger().warning(
                f"Bundle selection: cannot map child sku={wc_sku}, "
                f"product_id={wc_product_id} to ERPNext – falling back to defaults"
            )
            return {}  # partial mapping is unreliable → fall back

        # Per-bundle quantity (WOOSB sends total = per_bundle × parent_qty)
        per_bundle_qty = wc_qty // max(1, parent_qty)
        if per_bundle_qty <= 0:
            per_bundle_qty = wc_qty  # assume already per-bundle

        # Determine item group so BundleProcessor can match to the right bundle row
        wc_item_group = (cache.item_groups.get(wc_item_code) if cache else None) or frappe.db.get_value("Item", wc_item_code, "item_group")
        if not wc_item_group:
            frappe.logger().warning(
                f"Bundle selection: item {wc_item_code} has no item_group – falling back"
            )
            return {}

        # Aggregate into selected_items (same item may appear from multiple child lines)
        _append_bundle_selection(selected_items, wc_item_group, wc_item_code, per_bundle_qty)

    if selected_items:
        frappe.logger().info(
            f"Bundle selections built from WooCommerce children: "
            f"{{{', '.join(f'{g}: {len(v)} item(s)' for g, v in selected_items.items())}}}"
        )
    return selected_items


def _build_invoice_items(order: dict, price_list: str | None = None, cache: "MigrationCache | None" = None, is_historical: bool = False) -> Tuple[list[dict], list[dict], dict[str, Any]]:
    """Build Sales Invoice Item rows from Woo order line_items.

        Pricing policy:
        - Always use ERPNext Price List rates for standalone items.
        - WooCommerce prices/totals/discounts are NEVER used for item rates.
    - Prefer Woo Jarz Bundle expansion for bundles (uses internal pricing from Woo Jarz Bundle),
            even when Woo sends woosb parent/child lines; expand once from the parent and skip
            the related children to avoid duplication.

    Returns: (items, missing_items_info, bundle_context)
    missing contains entries for lines we could not map (no item code/sku).
    """
    items: list[dict] = []
    missing: list[dict] = []
    bundle_codes: set[str] = set()
    free_shipping_bundle_codes: set[str] = set()

    line_items = order.get("line_items") or []

    # 0) Pre-scan for woosb children and collect their parent IDs
    def _get_parent_id_from_meta(md_list: list[dict] | None) -> str | None:
        value = _get_line_meta_value(md_list, "_woosb_parent_id")
        if value in (None, ""):
            return None
        return str(value).strip()

    child_parent_ids: set[str] = set()
    for _li in line_items:
        _pid = _get_parent_id_from_meta(_li.get("meta_data"))
        if _pid:
            child_parent_ids.add(str(_pid))
    has_woosb_children = len(child_parent_ids) > 0
    handled_parents: set[str] = set()

    for li in line_items:
        sku = (li.get("sku") or "").strip()
        product_id = li.get("product_id")
        variation_id = li.get("variation_id")
        qty = float(li.get("quantity") or 0) or 0
        if qty <= 0:
            continue

        # 1) Prefer Woo Jarz Bundle expansion for bundle parents (even if woosb children exist)
        bundle_code = None
        if product_id:
            if cache:
                bundle_code = cache.get_bundle_code(product_id)
            else:
                try:
                    bundle_code = frappe.db.get_value("Woo Jarz Bundle", {"woo_bundle_id": str(product_id)}, "name")
                except Exception:
                    bundle_code = None
        if bundle_code and (str(product_id) in child_parent_ids or not has_woosb_children):
            try:
                # Import locally to avoid hard dependency at module import time
                from jarz_woocommerce_integration.services.bundle_processing import BundleProcessor  # type: ignore

                # --- Build selected_items from WooCommerce child line items ---
                has_explicit_bundle_selections = bool(
                    _get_line_meta_value(li.get("meta_data"), "_woosb_ids")
                ) or str(product_id) in child_parent_ids
                selected_items = _build_bundle_selections(
                    line_items,
                    product_id,
                    int(qty),
                    cache=cache,
                    parent_line=li,
                )

                if has_explicit_bundle_selections and not selected_items:
                    raise ValueError(
                        f"Bundle {bundle_code}: explicit Woo selections could not be reconstructed"
                    )

                bp = BundleProcessor(
                    bundle_code,
                    int(qty),
                    selected_items=selected_items or None,
                )
                bp.load_bundle()
                bundle_lines = bp.get_invoice_items()

                # Log what BundleProcessor returned
                frappe.logger().info(f"===== BUNDLE EXPANSION DEBUG =====")
                frappe.logger().info(f"Bundle Code: {bundle_code}, Product ID: {product_id}, Qty: {qty}")
                frappe.logger().info(f"Selected items passed: {bool(selected_items)}")
                frappe.logger().info(f"BundleProcessor returned {len(bundle_lines)} items:")
                for idx, bl in enumerate(bundle_lines):
                    item_code = bl.get("item_code", "UNKNOWN")
                    item_qty = bl.get("qty", 0)
                    item_rate = bl.get("rate", 0)
                    is_parent = bl.get("is_bundle_parent", False)
                    is_child = bl.get("is_bundle_child", False)
                    frappe.logger().info(f"  [{idx}] {item_code} x{item_qty} @ {item_rate} (parent={is_parent}, child={is_child})")
                frappe.logger().info(f"====================================")

                frappe.logger().info(f"Bundle {bundle_code} expanded into {len(bundle_lines)} line items for qty {qty}")
                bundle_codes.add(bundle_code)
                if _bundle_has_free_shipping(bundle_code, cache=cache):
                    free_shipping_bundle_codes.add(bundle_code)
                
                # CRITICAL: Use BundleProcessor items AS-IS with price_list_rate and discount_percentage
                # BundleProcessor follows the integration's bundle logic:
                # - Parent: 100% discount (rate becomes 0)
                # - Children: uniform discount_percentage so their total equals bundle_price
                # ERPNext will calculate the final rate from price_list_rate and discount_percentage
                
                allowed = {"item_code", "item_name", "description", "qty", "rate", "price_list_rate", 
                          "discount_percentage", "discount_amount", "is_bundle_child", "is_bundle_parent",
                          "bundle_code", "parent_bundle"}
                
                for bl in bundle_lines:
                    # Keep all fields that BundleProcessor returned - they are already correct
                    filtered = {k: v for k, v in bl.items() if k in allowed}
                    items.append(filtered)
                handled_parents.add(str(product_id))
                continue  # done with this Woo line
            except Exception:
                # If bundle expansion fails, report as missing to skip safely
                missing.append({"name": li.get("name"), "sku": sku, "product_id": product_id, "reason": "bundle_error"})
                continue

        # 1b) Unregistered WOOSB parent — flag as missing rather than creating flat items
        if not bundle_code and product_id and str(product_id) in child_parent_ids:
            frappe.logger().error(
                f"Unregistered WOOSB bundle parent: product_id={product_id}, "
                f"name={li.get('name')}. Register it in the Woo Jarz Bundle table."
            )
            missing.append({"name": li.get("name"), "sku": sku, "product_id": product_id, "reason": "unregistered_bundle"})
            handled_parents.add(str(product_id))
            continue

        # 2) If this is a woosb child for a parent we've already handled via Woo Jarz Bundle, skip it
        parent_id_in_meta = _get_parent_id_from_meta(li.get("meta_data"))
        if parent_id_in_meta and str(parent_id_in_meta) in handled_parents:
            continue

        # 3) Fall back to direct Item by variation_id, SKU, or woo_product_id
        item_code = None
        if cache:
            item_code = cache.resolve_item(sku, product_id, variation_id=variation_id)
        else:
            # Variation lookup first
            if variation_id and int(variation_id) > 0:
                item_code = frappe.db.get_value("Item", {"woo_variation_id": str(variation_id)}, "name")
            if not item_code and sku and frappe.db.exists("Item", sku):
                item_code = sku
            if not item_code and product_id:
                item_code = frappe.db.get_value("Item", {"woo_product_id": str(product_id)}, "name")

        # 3b) Name-based fallback when product_id is absent/zero (legacy orders where product_id
        #     was not stored in WooCommerce). Also applies in historical migration mode.
        if not item_code and (is_historical or not product_id or int(product_id or 0) == 0):
            woo_item_name = (li.get("name") or "").strip()
            if woo_item_name:
                if cache:
                    item_code = cache.resolve_item_by_name(woo_item_name)
                else:
                    item_code = frappe.db.get_value(
                        "Item", {"item_name": woo_item_name}, "name"
                    )
                if item_code:
                    frappe.logger().info(
                        f"Resolved product_id=0 item by name: '{woo_item_name}' → {item_code}"
                    )

        if not item_code:
            # If this is a woosb parent and has no mapped Item, don't fail the whole order.
            # We'll still apply parent's discount to children; parent line is skipped silently.
            if product_id is not None and str(product_id) in child_parent_ids:
                continue
            missing.append({"name": li.get("name"), "sku": sku, "product_id": product_id})
            continue

        # Pricing: always use ERPNext Price List rate.
        # Bundle items are already handled by BundleProcessor above (with their own
        # discount_percentage logic).  Standalone items always get ERPNext prices.
        erp_price = None
        if cache:
            erp_price = cache.get_price(item_code, price_list)
        else:
            try:
                if price_list:
                    erp_price = frappe.db.get_value("Item Price", {"item_code": item_code, "price_list": price_list}, "price_list_rate")
            except Exception:
                erp_price = None

        rate_value = float(erp_price or 0) if erp_price is not None else 0

        if rate_value == 0:
            frappe.logger().warning(f"Item {item_code} has zero rate (price_list={price_list}, sku={sku})")
        row = {
            "item_code": item_code,
            "qty": qty,
            "rate": rate_value,
        }
        if erp_price is not None:
            row["price_list_rate"] = float(erp_price)
        items.append(row)
    return (
        items,
        missing,
        {
            "bundle_codes": sorted(bundle_codes),
            "free_shipping_bundle_codes": sorted(free_shipping_bundle_codes),
            "has_free_shipping_bundle": bool(free_shipping_bundle_codes),
        },
    )


def _parse_delivery_parts(o: dict) -> tuple[str | None, str | None, int | None]:
    """Extract delivery date, start time, and duration (minutes) from Woo meta."""
    import re
    from datetime import datetime as dt

    delivery_date_str = None
    time_slot_str = None
    for md in (o.get("meta_data") or []):
        key = (md.get("key") or md.get("display_key") or "").strip()
        if key == "Delivery Date":
            delivery_date_str = (md.get("value") or md.get("display_value") or "").strip()
        elif key == "Time Slot":
            time_slot_str = (md.get("value") or md.get("display_value") or "").strip()

    date_part: str | None = None
    if delivery_date_str:
        for fmt in ("%d %B %Y", "%d %B, %Y", "%B %d %Y", "%B %d, %Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                date_part = dt.strptime(delivery_date_str.replace(",", ""), fmt).date().isoformat()
                break
            except Exception:
                continue

    time_from: str | None = None
    duration_minutes: int | None = None
    if time_slot_str:
        normalized = (
            time_slot_str.replace("—", "-").replace("–", "-")
            .replace(" to ", "-").replace("TO", "-").replace("To", "-")
        )
        normalized = re.sub(r"\s*\-\s*", "-", normalized.strip())
        m = re.match(r"^(\d{1,2}:\d{2}(?::\d{2})?)\-(\d{1,2}:\d{2}(?::\d{2})?)$", normalized)
        if not m:
            times = re.findall(r"\d{1,2}:\d{2}(?::\d{2})?", normalized)
            if len(times) >= 2:
                start_s, end_s = times[0], times[1]
            else:
                start_s = end_s = None
        else:
            start_s, end_s = m.group(1), m.group(2)

        if start_s and end_s:
            fmt1 = "%H:%M:%S" if start_s.count(":") == 2 else "%H:%M"
            fmt2 = "%H:%M:%S" if end_s.count(":") == 2 else "%H:%M"
            try:
                t1 = dt.strptime(start_s, fmt1)
                t2 = dt.strptime(end_s, fmt2)
                time_from = f"{t1.hour:02d}:{t1.minute:02d}:{(t1.second if t1.second else 0):02d}"
                delta_sec = (t2 - t1).total_seconds()
                if delta_sec < 0:
                    delta_sec += 24 * 3600
                duration_minutes = max(1, int(round(delta_sec / 60.0)))
            except Exception:
                time_from = None
                duration_minutes = None

    return date_part, time_from, duration_minutes


# Maps WooCommerce Order Attribution meta_data keys to ERPNext Sales Invoice fieldnames.
ATTRIBUTION_META_MAP: dict[str, str] = {
    "_wc_order_attribution_source_type": "woo_source_type",
    "_wc_order_attribution_utm_source": "woo_utm_source",
    "_wc_order_attribution_utm_medium": "woo_utm_medium",
    "_wc_order_attribution_utm_campaign": "woo_utm_campaign",
    "_wc_order_attribution_utm_content": "woo_utm_content",
    "_wc_order_attribution_utm_term": "woo_utm_term",
    "_wc_order_attribution_utm_id": "woo_utm_id",
    "_wc_order_attribution_referrer": "woo_referrer",
    "_wc_order_attribution_device_type": "woo_device_type",
    "_wc_order_attribution_session_entry": "woo_session_entry",
    "_wc_order_attribution_session_start_time": "woo_session_start",
    "_wc_order_attribution_session_pages": "woo_session_pages",
    "_wc_order_attribution_session_count": "woo_session_count",
    "_wc_order_attribution_user_agent": "woo_user_agent",
}

# First-touch fields to copy from Sales Invoice attribution → Customer record.
# Only the 6 business-critical fields — session analytics are per-order, not per-customer.
ATTRIBUTION_FIRST_TOUCH_FIELDS = {
    "woo_source_type": "woo_first_source_type",
    "woo_utm_source": "woo_first_utm_source",
    "woo_utm_medium": "woo_first_utm_medium",
    "woo_utm_campaign": "woo_first_utm_campaign",
    "woo_referrer": "woo_first_referrer",
    "woo_device_type": "woo_first_device_type",
}


def _extract_attribution(meta_data_list: "list[dict] | None") -> dict:
    """Extract WooCommerce Order Attribution fields from order-level meta_data.

    Returns a dict of {erpnext_fieldname: value} for every attribution key found.
    Only includes keys that have a non-empty value.
    Integer fields (session_pages, session_count) are cast to int where possible.
    HTML tags from display_value fallbacks are stripped.
    """
    _strip_html = re.compile(r"<[^>]+>")
    result: dict = {}
    for md in (meta_data_list or []):
        key = (md.get("key") or md.get("display_key") or "").strip()
        field = ATTRIBUTION_META_MAP.get(key)
        if not field:
            continue
        value = md.get("value")
        if value is None or str(value).strip() == "":
            # Fallback to display_value but strip HTML tags
            value = md.get("display_value") or ""
        value = _strip_html.sub("", str(value)).strip()
        if not value:
            continue
        # Cast integer fields
        if field in ("woo_session_pages", "woo_session_count"):
            try:
                value = int(float(value))
            except (ValueError, TypeError):
                continue
        # Truncate string values for Data fields (140 char limit)
        if isinstance(value, str) and len(value) > 140:
            value = value[:140]
        result[field] = value
    return result


def _get_shipping_income_account(company: str) -> str | None:
    """Return Freight and Forwarding Charges account for the company if it exists."""
    abbr = frappe.db.get_value("Company", company, "abbr") or ""
    # Prefer exact account name with company abbreviation suffix
    candidate_filters = []
    if abbr:
        candidate_filters.append({"name": f"Freight and Forwarding Charges - {abbr}"})
        candidate_filters.append({"account_name": f"Freight and Forwarding Charges - {abbr}"})
    candidate_filters.append({"account_name": "Freight and Forwarding Charges"})
    candidate_filters.append({"name": "Freight and Forwarding Charges"})

    for filters in candidate_filters:
        try:
            account = frappe.db.get_value("Account", {"company": company, **filters}, "name")
            if account:
                return account
        except Exception:
            continue
    return frappe.db.get_value("Company", company, "default_income_account")


def add_delivery_charges_to_taxes(inv, amount: float, delivery_description: str | None = None, account_head: str | None = None) -> None:
    """Append an 'Actual' charge row for delivery/shipping income on the invoice taxes table."""
    try:
        amt = float(amount) if amount is not None else 0.0
    except Exception:
        amt = 0.0
    if amt <= 0:
        return
    desc = delivery_description or "Shipping Income"
    if not account_head:
        account_head = _get_shipping_income_account(inv.company)
    # Try update existing matching row
    for t in inv.get("taxes", []) or []:
        if getattr(t, "charge_type", None) == "Actual" and (t.get("description") or getattr(t, "description", "") or "").startswith(desc):
            t.tax_amount = amt
            if account_head and not getattr(t, "account_head", None):
                t.account_head = account_head
            return
    # Else append
    row = {
        "charge_type": "Actual",
        "description": desc,
        "tax_amount": amt,
    }
    if account_head:
        row["account_head"] = account_head
    inv.append("taxes", row)


def _tax_row_value(row: Any, fieldname: str, default: Any = None) -> Any:
    if hasattr(row, "get"):
        try:
            return row.get(fieldname, default)
        except TypeError:
            value = row.get(fieldname)
            return default if value is None else value
    return getattr(row, fieldname, default)


def _bundle_has_free_shipping(bundle_code: str | None, cache: "MigrationCache | None" = None) -> bool:
    if not bundle_code:
        return False
    if cache:
        return cache.bundle_has_free_shipping(bundle_code)
    try:
        return bool(int(frappe.db.get_value("Woo Jarz Bundle", bundle_code, "free_shipping") or 0))
    except Exception:
        return False


def _row_value(row: Any, fieldname: str, default: Any = None) -> Any:
    if hasattr(row, "get"):
        try:
            return row.get(fieldname, default)
        except TypeError:
            value = row.get(fieldname)
            return default if value is None else value
    return getattr(row, fieldname, default)


def _get_invoice_item_qty(inv) -> float:
    total_qty = 0.0
    for item in inv.get("items", []) or []:
        try:
            total_qty += float(_row_value(item, "qty", 0) or 0)
        except Exception:
            continue
    return total_qty


def _get_invoice_merchandise_subtotal(inv) -> float:
    subtotal = 0.0
    for item in inv.get("items", []) or []:
        try:
            qty = float(_row_value(item, "qty", 0) or 0)
            price_list_rate = _row_value(item, "price_list_rate", None)
            if price_list_rate in (None, ""):
                price_list_rate = _row_value(item, "rate", 0) or 0
            discount_pct = float(_row_value(item, "discount_percentage", 0) or 0)
        except Exception:
            continue
        discount_pct = min(max(discount_pct, 0.0), 100.0)
        subtotal += float(price_list_rate or 0) * qty * (1 - discount_pct / 100.0)
    return round(subtotal, 2)


def _apply_delivery_promotion_audit(inv, decision: dict[str, Any]) -> None:
    if not decision.get("matched") or not decision.get("rule_name"):
        return

    marker = (
        f"[DELIVERY PROMO] {decision['rule_name']} | "
        f"merchandise_subtotal={float(decision.get('merchandise_subtotal', 0) or 0):.2f}"
    )
    existing = (getattr(inv, "remarks", "") or "").strip()
    if marker in existing:
        return
    inv.remarks = (existing + "\n" if existing else "") + marker


def _woo_order_has_free_shipping(order: dict[str, Any] | None) -> bool:
    if not isinstance(order, dict):
        return False

    try:
        shipping_total = float(order.get("shipping_total") or 0)
    except Exception:
        shipping_total = 0.0

    shipping_lines = order.get("shipping_lines") or []
    method_ids = {
        str(line.get("method_id") or "").strip().lower()
        for line in shipping_lines
        if isinstance(line, dict)
    }

    if shipping_total > 0:
        return False
    if "free_shipping" in method_ids:
        return True
    if not shipping_lines:
        return True

    try:
        return all(float(line.get("total") or 0) <= 0 for line in shipping_lines if isinstance(line, dict))
    except Exception:
        return False


def _resolve_delivery_promotion(
    inv,
    *,
    territory_name: str | None,
    customer_name: str | None = None,
    pos_profile_name: str | None = None,
    channel: str = "woo",
    is_pickup: bool = False,
) -> dict[str, Any]:
    decision = {
        "matched": False,
        "rule_name": None,
        "rule_type": None,
        "merchandise_subtotal": _get_invoice_merchandise_subtotal(inv),
        "item_qty": _get_invoice_item_qty(inv),
        "suppress_shipping_income": False,
        "suppress_legacy_delivery_charges": False,
    }

    if is_pickup:
        return decision

    if not frappe.db.exists("DocType", "Jarz Promotion Rule"):
        return decision

    customer_group = None
    if customer_name:
        try:
            customer_group = frappe.db.get_value("Customer", customer_name, "customer_group")
        except Exception:
            customer_group = None

    normalized_channel = (channel or "woo").strip().lower()
    if normalized_channel == "woocommerce":
        normalized_channel = "woo"

    rule_names = frappe.get_all(
        "Jarz Promotion Rule",
        filters={"enabled": 1, "promotion_scope": "Delivery"},
        pluck="name",
        order_by="priority asc, creation asc",
    )
    now_dt = frappe.utils.now_datetime()

    for rule_name in rule_names:
        rule = frappe.get_doc("Jarz Promotion Rule", rule_name)

        if getattr(rule, "active_from", None) and rule.active_from > now_dt:
            continue
        if getattr(rule, "active_to", None) and rule.active_to < now_dt:
            continue

        company = getattr(inv, "company", None)
        if getattr(rule, "company", None) and rule.company != company:
            continue
        if getattr(rule, "territory", None) and rule.territory != territory_name:
            continue
        if getattr(rule, "customer_group", None) and rule.customer_group != customer_group:
            continue
        if getattr(rule, "pos_profile", None) and rule.pos_profile != pos_profile_name:
            continue

        allowed_channels = {
            str(_row_value(row, "channel", "") or "").strip().lower()
            for row in (getattr(rule, "channels", None) or [])
            if str(_row_value(row, "channel", "") or "").strip()
        }
        if allowed_channels and normalized_channel not in allowed_channels:
            continue

        basis = getattr(rule, "threshold_basis", None) or "Merchandise Subtotal"
        if basis == "Item Quantity":
            metric_value = decision["item_qty"]
            minimum_value = float(getattr(rule, "minimum_item_qty", 0) or 0)
            maximum_value = None
        else:
            metric_value = decision["merchandise_subtotal"]
            minimum_value = float(getattr(rule, "minimum_threshold", 0) or 0)
            maximum_value = getattr(rule, "maximum_threshold", None)
            maximum_value = float(maximum_value or 0) if maximum_value not in (None, "") else None
            if maximum_value is not None and maximum_value <= 0:
                maximum_value = None

        if minimum_value and metric_value < minimum_value:
            continue
        if maximum_value is not None and metric_value > maximum_value:
            continue
        if (getattr(rule, "rule_type", "") or "") != "Free Delivery":
            continue

        decision["matched"] = True
        decision["rule_name"] = getattr(rule, "rule_name", None) or rule_name
        decision["rule_type"] = getattr(rule, "rule_type", None)
        decision["suppress_shipping_income"] = bool(getattr(rule, "apply_to_shipping_income", 0))
        decision["suppress_legacy_delivery_charges"] = bool(getattr(rule, "apply_to_legacy_delivery_charges", 0))
        return decision

    return decision


def _get_linked_payment_entries(invoice_name: str) -> list[str]:
    rows = frappe.get_all(
        "Payment Entry Reference",
        filters={
            "reference_doctype": "Sales Invoice",
            "reference_name": invoice_name,
        },
        pluck="parent",
    )
    return [str(row).strip() for row in rows if str(row).strip()]


def _payment_entries_are_simple_for_invoice(invoice_name: str, payment_entries: list[str]) -> bool:
    if not payment_entries:
        return True
    for payment_entry in payment_entries:
        refs = frappe.get_all(
            "Payment Entry Reference",
            filters={"parent": payment_entry},
            fields=["reference_doctype", "reference_name"],
        )
        if len(refs) != 1:
            return False
        ref = refs[0]
        if ref.get("reference_doctype") != "Sales Invoice" or ref.get("reference_name") != invoice_name:
            return False
    return True


def _invoice_has_delivery_note_link(invoice_name: str) -> bool:
    return bool(
        frappe.db.exists(
            "Delivery Note Item",
            {"against_sales_invoice": invoice_name},
        )
    )


def _list_current_month_free_delivery_repair_candidates(limit: int | None = None) -> list[dict[str, Any]]:
    limit_clause = ""
    params: list[Any] = []
    if limit and int(limit) > 0:
        limit_clause = " LIMIT %s"
        params.append(int(limit))

    return frappe.db.sql(
        """
        SELECT
            si.name,
            si.woo_order_id,
            si.customer,
            si.territory,
            si.pos_profile,
            si.posting_date,
            si.docstatus,
            IFNULL(si.amended_from, '') AS amended_from,
            IFNULL(si.is_return, 0) AS is_return
        FROM `tabSales Invoice` si
        WHERE COALESCE(si.woo_order_id, 0) > 0
          AND si.posting_date >= DATE_FORMAT(CURDATE(), '%%Y-%%m-01')
          AND si.posting_date < DATE_ADD(LAST_DAY(CURDATE()), INTERVAL 1 DAY)
          AND si.docstatus IN (0, 1)
          AND EXISTS (
              SELECT 1
              FROM `tabSales Taxes and Charges` stc
              WHERE stc.parent = si.name
                AND stc.parenttype = 'Sales Invoice'
                AND stc.description LIKE 'Shipping Income%%'
          )
        ORDER BY si.posting_date ASC, si.modified ASC
        """ + limit_clause,
        tuple(params),
        as_dict=True,
    )


def repair_current_month_free_delivery_imports(
    limit: int | None = None,
    *,
    dry_run: bool = True,
    require_woo_free_shipping: bool = True,
) -> dict[str, Any]:
    settings = frappe.get_single("WooCommerce Settings")
    ensure_custom_fields()
    client = WooClient(
        base_url=settings.base_url,
        consumer_key=settings.consumer_key,
        consumer_secret=settings.get_password("consumer_secret"),
    )

    candidates = _list_current_month_free_delivery_repair_candidates(limit=limit)
    summary: dict[str, Any] = {
        "current_month": frappe.utils.today()[:7],
        "requested": len(candidates),
        "evaluated": 0,
        "repaired": 0,
        "skipped": 0,
        "errors": 0,
        "dry_run": bool(dry_run),
        "require_woo_free_shipping": bool(require_woo_free_shipping),
        "results": [],
    }

    original_ignore_woo_outbound = getattr(frappe.flags, "ignore_woo_outbound", False)
    frappe.flags.ignore_woo_outbound = True

    try:
        for row in candidates:
            invoice_name = row.get("name")
            woo_order_id = row.get("woo_order_id")
            result: dict[str, Any] = {
                "invoice": invoice_name,
                "woo_order_id": woo_order_id,
                "status": "skipped",
            }
            summary["evaluated"] += 1
            summary["results"].append(result)

            try:
                inv = frappe.get_doc("Sales Invoice", invoice_name)
                if int(getattr(inv, "is_return", 0) or 0):
                    result["reason"] = "is_return"
                    summary["skipped"] += 1
                    continue
                if getattr(inv, "amended_from", None):
                    result["reason"] = "already_amended"
                    summary["skipped"] += 1
                    continue

                territory_name = getattr(inv, "territory", None) or row.get("territory")
                if not territory_name:
                    territory_name = frappe.db.get_value("Customer", inv.customer, "territory")

                promotion = _resolve_delivery_promotion(
                    inv,
                    territory_name=territory_name,
                    customer_name=inv.customer,
                    pos_profile_name=getattr(inv, "pos_profile", None) or row.get("pos_profile"),
                    channel="woo",
                )
                result["promotion"] = {
                    "matched": bool(promotion.get("matched")),
                    "rule_name": promotion.get("rule_name"),
                    "merchandise_subtotal": promotion.get("merchandise_subtotal"),
                }
                if not promotion.get("matched") or not promotion.get("suppress_shipping_income"):
                    result["reason"] = "promotion_not_matched"
                    summary["skipped"] += 1
                    continue

                order = client.get_order(woo_order_id)
                if not order:
                    result["reason"] = "woo_order_not_found"
                    summary["skipped"] += 1
                    continue
                woo_free_shipping = _woo_order_has_free_shipping(order)
                result["woo_shipping_total"] = order.get("shipping_total")
                result["woo_free_shipping"] = woo_free_shipping
                if require_woo_free_shipping and not woo_free_shipping:
                    result["reason"] = "woo_shipping_not_free"
                    summary["skipped"] += 1
                    continue

                payment_entries = _get_linked_payment_entries(inv.name)
                result["payment_entries"] = payment_entries
                if not _payment_entries_are_simple_for_invoice(inv.name, payment_entries):
                    result["reason"] = "complex_payment_entries"
                    summary["skipped"] += 1
                    continue
                if _invoice_has_delivery_note_link(inv.name):
                    result["reason"] = "delivery_note_linked"
                    summary["skipped"] += 1
                    continue

                if dry_run:
                    result["status"] = "dry_run"
                    result["reason"] = "eligible"
                    continue

                save_point = f"woo_free_delivery_repair_{str(woo_order_id).strip()}"
                frappe.db.savepoint(save_point)
                canceled_payment_entries: list[str] = []
                try:
                    for payment_entry_name in payment_entries:
                        payment_entry = frappe.get_doc("Payment Entry", payment_entry_name)
                        if payment_entry.docstatus == 1:
                            payment_entry.cancel()
                            canceled_payment_entries.append(payment_entry_name)

                    inv = frappe.get_doc("Sales Invoice", inv.name)
                    if inv.docstatus == 1:
                        inv.cancel()

                    replay = process_order_phase1(order, settings, allow_update=True)
                    if replay.get("status") not in {"created", "updated"}:
                        raise frappe.ValidationError(f"replay_failed: {replay}")

                    new_invoice_name = replay.get("invoice")
                    if not new_invoice_name:
                        raise frappe.ValidationError(f"replay_missing_invoice: {replay}")

                    new_invoice = frappe.get_doc("Sales Invoice", new_invoice_name)
                    shipping_rows_after = _get_delivery_charge_rows(new_invoice)
                    if shipping_rows_after:
                        raise frappe.ValidationError(
                            f"shipping_rows_still_present: {shipping_rows_after}"
                        )

                    frappe.db.release_savepoint(save_point)
                    frappe.db.commit()

                    result["status"] = "repaired"
                    result["reason"] = "replayed"
                    result["recreated_invoice"] = new_invoice_name
                    result["canceled_payment_entries"] = canceled_payment_entries
                    summary["repaired"] += 1
                except Exception:
                    frappe.db.rollback(save_point=save_point)
                    raise
            except Exception as exc:  # noqa: BLE001
                result["status"] = "error"
                result["reason"] = str(exc)
                summary["errors"] += 1
            else:
                if result["status"] == "skipped":
                    summary["skipped"] += 1
    finally:
        frappe.flags.ignore_woo_outbound = original_ignore_woo_outbound

    return summary


def _get_delivery_charge_rows(inv) -> list[dict[str, Any]]:
    rows = []
    for tax in inv.get("taxes", []) or []:
        description = str(_tax_row_value(tax, "description", "") or "")
        if description.startswith("Shipping Income"):
            try:
                amount = float(_tax_row_value(tax, "tax_amount", 0) or 0)
            except Exception:
                amount = 0.0
            rows.append(
                {
                    "description": description,
                    "tax_amount": amount,
                }
            )
    return rows


def _clear_delivery_charge_rows(inv) -> None:
    keep_rows = []
    for tax in inv.get("taxes", []) or []:
        description = str(_tax_row_value(tax, "description", "") or "")
        if not description.startswith("Shipping Income"):
            keep_rows.append(tax)
    inv.set("taxes", keep_rows)


def _resolve_delivery_charge_policy(
    territory_name: str | None,
    has_free_shipping_bundle: bool,
    cache: "MigrationCache | None" = None,
    *,
    inv=None,
    customer_name: str | None = None,
    pos_profile_name: str | None = None,
    channel: str = "woo",
) -> dict[str, Any]:
    if has_free_shipping_bundle:
        return {
            "amount": 0.0,
            "description": None,
            "reason": "free_shipping_bundle",
        }

    promotion = None
    if inv is not None:
        try:
            promotion = _resolve_delivery_promotion(
                inv,
                territory_name=territory_name,
                customer_name=customer_name,
                pos_profile_name=pos_profile_name,
                channel=channel,
            )
        except Exception:
            promotion = None

    if promotion and promotion.get("matched") and promotion.get("suppress_shipping_income"):
        return {
            "amount": 0.0,
            "description": None,
            "reason": "delivery_promotion",
            "promotion": promotion,
            "promotion_rule_name": promotion.get("rule_name"),
        }

    delivery_amt = 0.0
    if territory_name:
        if cache:
            territory_data = cache.get_territory_data(territory_name)
            delivery_amt = float(territory_data.get("delivery_income", 0) or 0)
        elif frappe.db.exists("Territory", territory_name):
            delivery_amt = float(
                frappe.db.get_value("Territory", territory_name, "delivery_income") or 0
            )

    if delivery_amt > 0:
        return {
            "amount": delivery_amt,
            "description": f"Shipping Income ({territory_name})",
            "reason": "territory_delivery_income",
        }

    return {
        "amount": 0.0,
        "description": None,
        "reason": "territory_missing_or_zero",
    }


def _apply_delivery_charge_policy(
    inv,
    territory_name: str | None,
    has_free_shipping_bundle: bool,
    cache: "MigrationCache | None" = None,
    *,
    customer_name: str | None = None,
    pos_profile_name: str | None = None,
    channel: str = "woo",
) -> dict[str, Any]:
    before_rows = _get_delivery_charge_rows(inv)
    decision = _resolve_delivery_charge_policy(
        territory_name,
        has_free_shipping_bundle=has_free_shipping_bundle,
        cache=cache,
        inv=inv,
        customer_name=customer_name,
        pos_profile_name=pos_profile_name,
        channel=channel,
    )

    _clear_delivery_charge_rows(inv)
    if decision["amount"] > 0:
        add_delivery_charges_to_taxes(
            inv,
            decision["amount"],
            delivery_description=decision["description"],
        )

    try:
        inv.calculate_taxes_and_totals()
    except Exception:
        pass

    promotion = decision.get("promotion")
    if promotion and promotion.get("matched"):
        _apply_delivery_promotion_audit(inv, promotion)

    after_rows = _get_delivery_charge_rows(inv)
    decision["changed"] = before_rows != after_rows
    decision["before_rows"] = before_rows
    decision["after_rows"] = after_rows

    try:
        frappe.logger().info(
            {
                "event": "woo_delivery_policy_applied",
                "invoice": getattr(inv, "name", None),
                "territory": territory_name,
                "has_free_shipping_bundle": has_free_shipping_bundle,
                "reason": decision["reason"],
                "amount": decision["amount"],
                "promotion_rule_name": decision.get("promotion_rule_name"),
                "changed": decision["changed"],
            }
        )
    except Exception:
        pass

    return decision


def _enqueue_delivery_charge_repost(inv) -> None:
    if getattr(inv, "docstatus", 0) != 1:
        return

    repost_doc = frappe.get_doc(
        {
            "doctype": "Repost Accounting Ledger",
            "company": inv.company,
            "delete_cancelled_entries": 1,
            "vouchers": [
                {
                    "voucher_type": inv.doctype,
                    "voucher_no": inv.name,
                }
            ],
        }
    )
    repost_doc.insert(ignore_permissions=True)
    repost_doc.submit()


def _check_and_repair_submitted_invoice_drift(inv, woo_id, territory_name=None, pos_profile=None, default_warehouse=None):
    """Check if a submitted Sales Invoice has item warehouses that don't match the resolved
    POS Profile's warehouse. If drift is detected: log it loudly, and optionally repair
    (controlled by WooCommerce Settings.auto_repair_drift flag, default OFF)."""
    if not pos_profile or not default_warehouse:
        return

    try:
        item_rows = frappe.get_all(
            "Sales Invoice Item",
            filters={"parent": inv.name},
            fields=["name", "warehouse", "item_code"],
        )
        drifted = [r for r in item_rows if r.get("warehouse") != default_warehouse]
        if not drifted:
            return

        # Drift detected — log it
        drift_detail = [{"row": r["name"], "item": r["item_code"], "has_wh": r["warehouse"], "want_wh": default_warehouse} for r in drifted]
        create_sync_log_entry(
            "DriftDetected",
            "Warning",
            frappe.as_json({
                "invoice": inv.name,
                "pos_profile": pos_profile,
                "target_warehouse": default_warehouse,
                "drifted_rows": len(drifted),
                "detail": drift_detail,
            }),
            woo_order_id=woo_id,
        )
        frappe.log_error(
            title=f"Woo: warehouse drift on submitted invoice {inv.name}",
            message=frappe.as_json({
                "woo_order_id": woo_id,
                "invoice": inv.name,
                "territory": territory_name,
                "pos_profile": pos_profile,
                "target_warehouse": default_warehouse,
                "drifted_rows": drift_detail,
            })
        )

        # Auto-repair if enabled and safe (no Delivery Notes)
        try:
            settings = frappe.get_single("WooCommerce Settings")
            auto_repair = getattr(settings, "auto_repair_drift", None)
        except Exception:
            auto_repair = None

        if not auto_repair:
            return

        # Safety: abort if any Delivery Note references this invoice
        if frappe.db.exists("Delivery Note Item", {"against_sales_invoice": inv.name}):
            frappe.log_error(
                title=f"Woo: drift auto-repair blocked — Delivery Note exists for {inv.name}",
                message=f"woo_order={woo_id}"
            )
            return

        # Repair all drifted rows
        for r in drifted:
            frappe.db.set_value("Sales Invoice Item", r["name"], "warehouse", default_warehouse, update_modified=False)
        frappe.db.set_value("Sales Invoice", inv.name, "set_warehouse", default_warehouse, update_modified=False)
        frappe.db.commit()
        create_sync_log_entry(
            "DriftRepaired",
            "Success",
            frappe.as_json({
                "invoice": inv.name,
                "repaired_rows": len(drifted),
                "warehouse": default_warehouse,
            }),
            woo_order_id=woo_id,
        )
    except Exception as e:
        try:
            frappe.log_error(title=f"Woo: drift check failed for {inv.name}", message=str(e))
        except Exception:
            pass


def _apply_invoice_pos_profile(inv, pos_profile: str | None, *, submitted: bool) -> None:
    if not pos_profile:
        return

    if submitted:
        inv.db_set("pos_profile", pos_profile, commit=False)
        try:
            inv.db_set("custom_kanban_profile", pos_profile, commit=False)
        except Exception:
            pass
        try:
            inv.db_set("is_pos", 1, commit=False)
        except Exception:
            pass
        return

    inv.pos_profile = pos_profile
    try:
        inv.custom_kanban_profile = pos_profile
    except Exception:
        pass


def _get_invoice_accounting_flags(invoice_name: str) -> tuple[bool, bool]:
    has_payment_ledger = bool(
        frappe.db.exists("Payment Ledger Entry", {"voucher_no": invoice_name, "delinked": 0})
    )
    has_gl_entries = bool(
        frappe.db.exists("GL Entry", {"voucher_no": invoice_name, "is_cancelled": 0})
    )
    return has_payment_ledger, has_gl_entries


def _ensure_submitted_invoice_accounting(inv) -> None:
    has_payment_ledger, has_gl_entries = _get_invoice_accounting_flags(inv.name)
    if has_payment_ledger and has_gl_entries:
        return

    try:
        inv.flags.ignore_permissions = True
        inv.make_gl_entries()
    except Exception:
        frappe.log_error(
            f"GL repair failed for {inv.name} after submit (gl={int(has_gl_entries)}, ple={int(has_payment_ledger)})",
            "GL Entry Repair Error",
        )

    has_payment_ledger, has_gl_entries = _get_invoice_accounting_flags(inv.name)
    if has_payment_ledger and has_gl_entries:
        return

    frappe.throw(
        f"Sales Invoice {inv.name} submit completed without required accounting entries "
        f"(gl={int(has_gl_entries)}, ple={int(has_payment_ledger)})."
    )


def _submit_invoice_with_accounting_guards(inv, pos_profile: str | None = None) -> None:
    inv.submit()
    _ensure_submitted_invoice_accounting(inv)
    _apply_invoice_pos_profile(inv, pos_profile, submitted=True)


def _resolve_paid_to_account(payment_method: str | None, company: str, cache: "MigrationCache | None" = None) -> str | None:
    """Return the paid-to account for a mapped Woo payment method."""
    pm = (payment_method or "").strip()
    if not pm:
        return None
    # Fast path: use pre-loaded accounts from cache
    if cache and cache.company_accounts:
        return cache.company_accounts.get(pm)
    if pm == "Cash":
        return frappe.db.get_value("Company", company, "default_cash_account")
    if pm == "Instapay":
        return frappe.db.get_value("Company", company, "default_bank_account")
    if pm == "Mobile Wallet":
        account = frappe.db.get_value(
            "Mode of Payment Account",
            {"parent": "Mobile Wallet", "company": company},
            "default_account",
        )
        return account or frappe.db.get_value("Company", company, "default_bank_account")
    if pm in ("Kashier Card", "Kashier Wallet"):
        return frappe.db.get_value("Company", company, "custom_kashier_account")
    return None


def _create_payment_entry(invoice_name: str, payment_method: str | None, posting_date: str | None = None, cache: "MigrationCache | None" = None) -> str | None:
    """Create a Payment Entry for the invoice's current outstanding amount."""
    try:
        frappe.set_user("Administrator")
        inv = frappe.get_doc("Sales Invoice", invoice_name)
        company = inv.company

        # Use the current outstanding amount so reruns top up partial payments
        # instead of duplicating the original Woo total.
        pay_amount = float(inv.outstanding_amount or inv.grand_total or 0)
        if pay_amount <= 0:
            return None

        paid_to = _resolve_paid_to_account(payment_method, company, cache=cache)
        if not paid_to:
            frappe.log_error(
                f"No paid-to account resolved for payment_method={payment_method}, company={company}",
                "Payment Entry Creation Failed",
            )
            return None

        if cache and cache.company_accounts:
            paid_from = cache.company_accounts.get("default_receivable_account")
        else:
            paid_from = frappe.db.get_value("Company", company, "default_receivable_account")
        if not paid_from:
            frappe.log_error(
                f"No default_receivable_account for company={company}",
                "Payment Entry Creation Failed",
            )
            return None

        pe_date = posting_date or frappe.utils.today()
        pe = frappe.get_doc({
            "doctype": "Payment Entry",
            "payment_type": "Receive",
            "posting_date": pe_date,
            "company": company,
            "party_type": "Customer",
            "party": inv.customer,
            "paid_from": paid_from,
            "paid_to": paid_to,
            "paid_amount": pay_amount,
            "received_amount": pay_amount,
            "reference_no": f"WOO-{invoice_name}",
            "reference_date": pe_date,
            "references": [{
                "reference_doctype": "Sales Invoice",
                "reference_name": invoice_name,
                "allocated_amount": pay_amount,
            }]
        })
        pe.insert(ignore_permissions=True)
        pe.submit()

        return pe.name
    except Exception as e:
        frappe.log_error(
            f"Failed to create payment entry for {invoice_name} (method={payment_method}): {str(e)}",
            "Payment Entry Creation Error",
        )
        return None


def _maybe_create_payment_entry_for_invoice(
    inv,
    order: dict[str, Any],
    status_map: dict[str, Any],
    custom_payment_method: str | None,
    woo_payment_method: str | None,
    is_historical: bool = False,
    cache: "MigrationCache | None" = None,
    skip_payment_entry: bool = False,
) -> None:
    if not inv or inv.docstatus != 1 or skip_payment_entry:
        return

    woo_status = order.get("status")
    woo_id = order.get("id")
    should_be_paid = _should_treat_inbound_order_as_paid(
        woo_status,
        custom_payment_method,
        status_map=status_map,
        is_historical=is_historical,
    )
    if not should_be_paid:
        return

    if not custom_payment_method:
        frappe.log_error(
            f"Paid Woo invoice {inv.name} has no mapped payment method (woo_method={woo_payment_method})",
            "Payment Entry Mapping Missing",
        )
        try:
            frappe.get_doc("Sales Invoice", inv.name).add_comment(
                "Comment",
                f"⚠ No mapped payment method for WooCommerce order {woo_id} (woo_method={woo_payment_method})",
            )
        except Exception:
            pass
        return

    already_paid = frappe.db.exists(
        "Payment Entry Reference",
        {"reference_doctype": "Sales Invoice", "reference_name": inv.name},
    )
    if already_paid:
        return

    try:
        pe_posting_date = _resolve_posting_date(order, is_historical)
        payment_entry = _create_payment_entry(
            inv.name,
            custom_payment_method,
            posting_date=pe_posting_date,
            cache=cache,
        )
        if payment_entry:
            frappe.logger().info(
                {
                    "event": "payment_entry_created",
                    "invoice": inv.name,
                    "payment_entry": payment_entry,
                    "method": custom_payment_method,
                }
            )
    except Exception as pe_error:
        frappe.log_error(
            f"Failed to create payment for {inv.name}: {str(pe_error)}",
            "Payment Entry Creation Error",
        )
        try:
            frappe.get_doc("Sales Invoice", inv.name).add_comment(
                "Comment",
                f"⚠ Payment Entry creation failed for WooCommerce order {woo_id}: {pe_error}",
            )
        except Exception:
            pass


def process_order_phase1(order: dict, settings, allow_update: bool = True, is_historical: bool = False, cache: "MigrationCache | None" = None, skip_payment_entry: bool = False) -> dict:
    """Process a single Woo order into a Sales Invoice.
    
    Args:
        order: WooCommerce order dict
        settings: WooCommerce Settings singleton
        allow_update: Whether to update existing invoices
        is_historical: True for historical migration (paid invoices), False for live orders (unpaid)
        cache: Optional MigrationCache for fast lookups during historical migration
        skip_payment_entry: When True, skip Payment Entry creation even for paid completed orders.
            Use with _run_full_historical_migration(defer_payment_entries=True) to defer PE creation
            to a separate batch pass via _batch_create_payment_entries().
    """
    woo_id = order.get("id")

    # Fast-skip via cache: if this order is already mapped to an invoice, skip immediately
    if cache and not allow_update and woo_id and int(woo_id) in cache.order_map_set:
        return {"status": "skipped", "reason": "already_mapped", "woo_order_id": woo_id}

    # Prevent duplicate work when webhook and poller race on the same order
    # Skip locks during historical migration (single worker, no races)
    lock = None
    db_lock_key = f"woo-order-{woo_id}"
    db_lock_acquired = False
    if not (is_historical and cache):
        try:
            lock = get_redis_conn().lock(f"woo-order-lock-{woo_id}", timeout=120, blocking_timeout=1)
            if not lock.acquire(blocking=False):
                return {"status": "skipped", "reason": "locked", "woo_order_id": woo_id}
        except Exception:
            lock = None

        # DB-level advisory lock as a second guard (handles multi-worker races)
        try:
            res = frappe.db.sql("SELECT GET_LOCK(%s, 2)", (db_lock_key,))
            db_lock_acquired = bool(res and res[0] and res[0][0] == 1)
            if not db_lock_acquired:
                return {"status": "skipped", "reason": "db_locked", "woo_order_id": woo_id}
        except Exception:
            db_lock_acquired = False

    # Determine mapping link field name based on actual DB schema
    if cache:
        LINK_FIELD = cache.link_field
    else:
        LINK_FIELD = "erpnext_sales_invoice"
        try:
            cols = frappe.db.get_table_columns("WooCommerce Order Map") or []
            if LINK_FIELD not in cols and "sales_invoice" in cols:
                LINK_FIELD = "sales_invoice"
        except Exception:
            pass

    # Get existing mapping (robust to column name differences)
    existing_map = None
    try:
        existing_map = frappe.db.get_value(
            "WooCommerce Order Map",
            {"woo_order_id": woo_id},
            ["name", LINK_FIELD, "hash", "status"],
            as_dict=True,
        )
    except Exception:
        # Final fallback: only fetch name
        try:
            nm = frappe.db.get_value("WooCommerce Order Map", {"woo_order_id": woo_id}, "name")
            if nm:
                existing_map = {"name": nm, LINK_FIELD: None, "hash": None, "status": None}
        except Exception:
            existing_map = None
    order_hash = _compute_order_hash(order)

    # Skip if order hasn't changed since last sync (hash match + valid invoice link
    # AND the linked Sales Invoice is already in the target state).
    # Exception: if the target state requires cancellation (docstatus=2) but the linked
    # SI is still active (docstatus 0 or 1), fall through so it gets cancelled.
    if existing_map and existing_map.get("hash") == order_hash and existing_map.get(LINK_FIELD):
        linked_docstatus = frappe.db.get_value("Sales Invoice", existing_map[LINK_FIELD], "docstatus")
        if linked_docstatus is not None and int(linked_docstatus) != 2:
            target_docstatus = _map_status(order.get("status")).get("docstatus", 0)
            if target_docstatus != 2:
                return {"status": "skipped", "reason": "unchanged", "woo_order_id": woo_id}
            # target is cancellation but SI is active — fall through to cancel it

    # Hard idempotency: if a live Sales Invoice already exists with this woo_order_id, use it.
    # Only consider submitted (docstatus=1) or draft (docstatus=0) invoices — never cancelled.
    # Previously this filter used ["!=", 2] which caused a new SI to be created after every
    # cancellation while the cancelled ghost persisted, generating duplicate SIs over time.
    linked_invoice_name = None
    duplicate_invoices = []
    try:
        si_list = frappe.get_all(
            "Sales Invoice",
            filters={"woo_order_id": woo_id, "docstatus": ["in", [0, 1]]},
            fields=["name", "creation"],
            order_by="creation desc",
            page_length=5,
        )
        if si_list:
            linked_invoice_name = si_list[0]["name"]
            if len(si_list) > 1:
                duplicate_invoices = [x["name"] for x in si_list[1:]]
    except Exception:
        pass

    # Reconcile mapping to found invoice if mapping is missing or points elsewhere.
    # Persist the recovered link so future re-runs and outbound guards see the
    # order as inbound even when the invoice existed before the map row.
    if linked_invoice_name:
        if not existing_map or not existing_map.get(LINK_FIELD):
            map_values = {
                "woo_order_id": woo_id,
                "woo_order_number": order.get("number"),
                LINK_FIELD: linked_invoice_name,
                "status": order.get("status"),
                "currency": order.get("currency"),
                "total": order.get("total"),
                "payment_method": order.get("payment_method"),
                "synced_on": frappe.utils.now_datetime(),
                "hash": order_hash,
            }
            try:
                if existing_map and existing_map.get("name"):
                    map_doc = frappe.get_doc("WooCommerce Order Map", existing_map["name"])
                    map_doc.update(map_values)
                    map_doc.save(ignore_permissions=True)
                else:
                    map_doc = frappe.get_doc({"doctype": "WooCommerce Order Map", **map_values})
                    map_doc.insert(ignore_permissions=True)
                existing_map = {
                    "name": map_doc.name,
                    LINK_FIELD: linked_invoice_name,
                    "hash": order_hash,
                    "status": order.get("status"),
                }
            except Exception:
                if not existing_map:
                    existing_map = {"name": None, LINK_FIELD: linked_invoice_name, "hash": order_hash, "status": order.get("status")}
                else:
                    existing_map[LINK_FIELD] = linked_invoice_name
                    existing_map["hash"] = order_hash
                    existing_map["status"] = order.get("status")
    if existing_map and not allow_update:
        # If map has a linked invoice, genuinely skip (already processed)
        if existing_map.get(LINK_FIELD):
            return {"status": "skipped", "reason": "already_mapped", "woo_order_id": woo_id}
        # Orphan map entry (processing/error without invoice) — clean up and retry
        map_status = (existing_map.get("status") or "").lower()
        if map_status in ("processing", "error", ""):
            try:
                frappe.delete_doc("WooCommerce Order Map", existing_map["name"], force=1, ignore_permissions=True)
                frappe.db.commit()
                existing_map = None
            except Exception:
                return {"status": "skipped", "reason": "cleanup_failed", "woo_order_id": woo_id}
        else:
            return {"status": "skipped", "reason": "already_mapped", "woo_order_id": woo_id}

    # Suppress outbound sync hooks while processing inbound WooCommerce data.
    # Must be set BEFORE customer operations to prevent Customer.on_update from
    # pushing data back to WooCommerce during migration.
    frappe.flags.ignore_woo_outbound = True

    # Ensure customer and at least one address before proceeding
    try:
        customer, billing_addr, shipping_addr = ensure_customer_with_addresses(
            order, settings,
            customer_cache=cache.customer_cache if cache else None,
            address_cache=cache.address_cache if cache else None,
            territory_state_cache=cache.territory_state_cache if cache else None,
        )
    except ValueError as ve:
        reason = str(ve)
        if reason == "no_address":
            return {"status": "skipped", "reason": "no_address", "woo_order_id": woo_id}
        return {"status": "skipped", "reason": f"customer_error:{reason}", "woo_order_id": woo_id}
    except Exception as ce:  # noqa
        return {"status": "skipped", "reason": f"customer_error:{ce}", "woo_order_id": woo_id}

    # Resolve Territory -> POS Profile -> warehouse.
    # Warehouse is ALWAYS derived from POS Profile.warehouse — never from settings.default_warehouse.
    territory_name = None
    pos_profile = None
    default_warehouse = None
    _territory_fallback_used = False
    try:
        territory_name = frappe.db.get_value("Customer", customer, "territory")
        if cache and territory_name:
            td = cache.get_territory_data(territory_name)
            pos_profile = td.get("pos_profile")
            # warehouse must come from POS Profile directly, not from territory cache
            if pos_profile and not td.get("warehouse"):
                default_warehouse = frappe.db.get_value("POS Profile", pos_profile, "warehouse")
            else:
                default_warehouse = td.get("warehouse")
        elif territory_name:
            pos_profile = frappe.db.get_value("Territory", territory_name, "pos_profile")
            if pos_profile:
                default_warehouse = frappe.db.get_value("POS Profile", pos_profile, "warehouse")
    except Exception:
        pass

    # Fallback: use Default POS Profile from settings when territory resolution produced nothing.
    # This is intentionally loud — fires a frappe.log_error and marks the map for self-heal.
    if not pos_profile:
        _territory_fallback_used = True
        try:
            fallback = getattr(settings, "default_pos_profile", None)
            if fallback and frappe.db.exists("POS Profile", fallback):
                pos_profile = fallback
                default_warehouse = frappe.db.get_value("POS Profile", pos_profile, "warehouse")
                frappe.log_error(
                    title="Woo: territory_unresolved — fallback pos_profile used",
                    message=frappe.as_json({
                        "woo_order_id": woo_id,
                        "customer": customer,
                        "territory_name": territory_name,
                        "billing_state": (order.get("billing") or {}).get("state"),
                        "shipping_state": (order.get("shipping") or {}).get("state"),
                        "fallback_pos_profile": pos_profile,
                        "fallback_warehouse": default_warehouse,
                    })
                )
        except Exception:
            pass

    # Final guard: if we still have no warehouse (POS Profile missing warehouse field),
    # derive it by name convention as last resort and log.
    if pos_profile and not default_warehouse:
        try:
            default_warehouse = frappe.db.get_value("POS Profile", pos_profile, "warehouse")
        except Exception:
            pass
        if not default_warehouse:
            frappe.log_error(
                title="Woo: POS Profile has no warehouse configured",
                message=f"woo_order={woo_id} pos_profile={pos_profile!r} — items may have NULL warehouse"
            )

    # Resolve ERPNext Price List
    price_list = None
    try:
        # Prefer POS Profile's price list; else company default selling price list
        if cache and territory_name:
            td = cache.get_territory_data(territory_name)
            price_list = td.get("price_list")
        elif pos_profile:
            price_list = frappe.db.get_value("POS Profile", pos_profile, "selling_price_list")
        if not price_list:
            default_company = getattr(settings, "default_company", None) or frappe.defaults.get_global_default("company")
            if default_company:
                price_list = frappe.db.get_value("Company", default_company, "default_selling_price_list")
    except Exception:
        price_list = None

    lines, missing, bundle_context = _build_invoice_items(
        order,
        price_list=price_list,
        cache=cache,
        is_historical=is_historical,
    )
    if missing:
        return {"status": "skipped", "reason": "unmapped_items", "details": missing, "woo_order_id": woo_id}
    if not lines:
        return {"status": "skipped", "reason": "no_lines", "woo_order_id": woo_id}

    # Check payment status for live orders
    woo_status = (order.get("status") or "").lower()
    if not is_historical:
        # For live orders, skip if pending payment
        if woo_status in {"pending", "on-hold"}:
            return {"status": "skipped", "reason": "pending_payment", "woo_order_id": woo_id, "woo_status": woo_status}
    
    # Map payment method
    woo_payment_method = order.get("payment_method")
    woo_payment_method_title = order.get("payment_method_title")
    custom_payment_method = _map_payment_method(woo_payment_method, woo_payment_method_title)
    
    status_map = _map_status(woo_status, is_historical=is_historical)

    # Create the processing map only after deterministic skip conditions pass.
    # This keeps no_address / no_lines / unmapped_items / pending_payment orders
    # from leaving behind orphan map rows with no linked invoice.
    if not existing_map:
        try:
            map_doc = frappe.get_doc({
                "doctype": "WooCommerce Order Map",
                "woo_order_id": woo_id,
                "woo_order_number": order.get("number"),
                "status": "processing",
                "synced_on": frappe.utils.now_datetime(),
            })
            map_doc.insert(ignore_permissions=True)
            existing_map = {"name": map_doc.name, LINK_FIELD: None, "hash": None, "status": "processing"}
        except Exception as map_err:
            # If a duplicate map already exists, avoid creating another invoice.
            try:
                existing_map = frappe.db.get_value(
                    "WooCommerce Order Map",
                    {"woo_order_id": woo_id},
                    ["name", LINK_FIELD, "hash", "status"],
                    as_dict=True,
                )
            except Exception:
                existing_map = None
            if existing_map and existing_map.get(LINK_FIELD):
                return {"status": "skipped", "reason": "already_mapped", "woo_order_id": woo_id}
            if existing_map and _is_processing_equivalent_woo_status(existing_map.get("status")):
                return {"status": "skipped", "reason": "processing", "woo_order_id": woo_id}
            raise map_err

    try:
        delivery_date_val, time_from_val, duration_val = _parse_delivery_parts(order)
        if not (delivery_date_val and time_from_val and (duration_val is not None)):
            delivery_date_val = None
            time_from_val = None
            duration_val = None

        attribution = _extract_attribution(order.get("meta_data"))

        candidate_name = None
        candidate_doc = None
        if allow_update:
            candidate_name = (existing_map or {}).get(LINK_FIELD) or linked_invoice_name
        if candidate_name:
            try:
                candidate_doc = frappe.get_doc("Sales Invoice", candidate_name)
                if candidate_doc.docstatus == 2 and status_map["docstatus"] != 2:
                    candidate_doc = None
                    if existing_map:
                        existing_map[LINK_FIELD] = None
                    linked_invoice_name = None
                else:
                    linked_invoice_name = candidate_doc.name
            except Exception:
                candidate_doc = None

        inv = None
        action = "created"

        if candidate_doc:
            inv = candidate_doc
            action = "updated"
            delivery_result = None
            # PROD-WOO-001: Once a Sales Invoice is submitted (docstatus=1), the inbound
            # sync must not mutate it in any way — no save, no db_set, no cancel.
            # ERPNext is the source of truth from submission onwards; all status/delivery
            # changes flow outbound to Woo via outbound_sync.py hooks.
            if inv.docstatus == 1:
                # Before skipping, check for warehouse drift and self-heal if possible.
                _check_and_repair_submitted_invoice_drift(
                    inv, woo_id, territory_name=territory_name, pos_profile=pos_profile,
                    default_warehouse=default_warehouse,
                )
                create_sync_log_entry(
                    "InboundSkip",
                    "Skipped",
                    f"submitted_frozen: {inv.name} is submitted; "
                    f"inbound Woo update blocked (woo_status={woo_status!r})",
                    woo_order_id=woo_id,
                )
                return {
                    "status": "skipped",
                    "reason": "submitted_frozen",
                    "woo_order_id": woo_id,
                    "invoice": inv.name,
                }
            try:
                current_woo_order_id = getattr(inv, "woo_order_id", None)
                current_woo_order_number = getattr(inv, "woo_order_number", None)
                needs_db_set = inv.docstatus in (1, 2)

                if str(current_woo_order_id or "") != str(woo_id):
                    if needs_db_set:
                        inv.db_set("woo_order_id", woo_id, commit=False)
                    else:
                        inv.woo_order_id = woo_id

                if order.get("number") and str(current_woo_order_number or "") != str(order.get("number")):
                    if needs_db_set:
                        inv.db_set("woo_order_number", order.get("number"), commit=False)
                    else:
                        inv.woo_order_number = order.get("number")
            except Exception:
                pass
            if inv.docstatus != 2:
                if inv.docstatus == 0:
                    inv.set("items", [])
                    for it in lines:
                        if default_warehouse:
                            it["warehouse"] = default_warehouse
                        inv.append("items", it)
                    if price_list:
                        inv.selling_price_list = price_list
                    # Prevent ERPNext pricing rules from overriding bundle rates
                    inv.ignore_pricing_rule = 1
                    # Fix posting_date for backdated historical invoices
                    if is_historical:
                        inv.posting_date = _resolve_posting_date(order, is_historical)
                        inv.set_posting_time = 1
                if billing_addr or shipping_addr:
                    inv.customer_address = billing_addr or shipping_addr
                    inv.shipping_address_name = shipping_addr or billing_addr
                # Set POS Profile from Territory if available
                try:
                    if pos_profile:
                        if inv.docstatus == 1:
                            _apply_invoice_pos_profile(inv, pos_profile, submitted=True)
                        else:
                            _apply_invoice_pos_profile(inv, pos_profile, submitted=False)
                except Exception:
                    pass
                if delivery_date_val:
                    inv.custom_delivery_date = delivery_date_val
                if time_from_val:
                    inv.custom_delivery_time_from = time_from_val
                if duration_val is not None:
                    inv.custom_delivery_duration = int(duration_val) * 60  # seconds
                if custom_payment_method:
                    try:
                        if inv.docstatus == 1:
                            inv.db_set("custom_payment_method", custom_payment_method, commit=False)
                        else:
                            inv.custom_payment_method = custom_payment_method
                    except Exception:
                        pass
                # Apply attribution fields (marketing source, UTM, referrer, device, session)
                if attribution:
                    try:
                        if inv.docstatus == 1:
                            for _attr_field, _attr_val in attribution.items():
                                try:
                                    inv.db_set(_attr_field, _attr_val, commit=False)
                                except Exception:
                                    pass
                        else:
                            for _attr_field, _attr_val in attribution.items():
                                try:
                                    setattr(inv, _attr_field, _attr_val)
                                except Exception:
                                    pass
                    except Exception:
                        pass
                try:
                    delivery_result = _apply_delivery_charge_policy(
                        inv,
                        territory_name=territory_name,
                        has_free_shipping_bundle=bundle_context.get("has_free_shipping_bundle", False),
                        cache=cache,
                        customer_name=customer,
                        pos_profile_name=pos_profile,
                        channel="woo",
                    )
                except Exception:
                    pass
                if inv.docstatus == 1 and delivery_result and delivery_result.get("changed"):
                    inv.flags.ignore_validate_update_after_submit = True
                inv.save(ignore_permissions=True, ignore_version=True)
                if inv.docstatus == 1 and delivery_result and delivery_result.get("changed"):
                    _enqueue_delivery_charge_repost(inv)

            try:
                if status_map.get("custom_state"):
                    inv.db_set("sales_invoice_state", status_map["custom_state"], commit=False)
            except Exception:
                pass

            # Update custom acceptance status and sales invoice state based on WooCommerce status
            try:
                if woo_status == "completed":
                    inv.db_set("custom_acceptance_status", "Accepted", commit=False)
                    inv.db_set("custom_sales_invoice_state", "Delivered", commit=False)
                elif woo_status == "out-for-delivery":
                    inv.db_set("custom_acceptance_status", "Accepted", commit=False)
                    inv.db_set("custom_sales_invoice_state", "Out for Delivery", commit=False)
                elif woo_status in ("cancelled", "refunded", "failed"):
                    inv.db_set("custom_acceptance_status", "Accepted", commit=False)
                    inv.db_set("custom_sales_invoice_state", "Cancelled", commit=False)
                    # Populate cancellation classification fields (owned by jarz_pos; defensive check)
                    try:
                        _meta = frappe.get_meta("Sales Invoice")
                        if _meta.get_field("custom_cancellation_type"):
                            _cancel_type = "WooCommerce Refunded" if woo_status == "refunded" else "WooCommerce Cancelled"
                            _cancel_reason = f"Order {woo_status} on WooCommerce (Order #{woo_id})"
                            inv.db_set("custom_cancellation_type", _cancel_type, commit=False)
                            inv.db_set("custom_cancellation_reason", _cancel_reason, commit=False)
                    except Exception:
                        pass
            except Exception:
                pass

            try:
                if status_map["docstatus"] == 1 and inv.docstatus == 0:
                    _submit_invoice_with_accounting_guards(inv, pos_profile=pos_profile)
                elif status_map["docstatus"] == 2 and inv.docstatus in (0, 1):
                    if inv.docstatus == 0:
                        _submit_invoice_with_accounting_guards(inv, pos_profile=pos_profile)
                    inv.cancel()
            except Exception:
                raise
        else:
            if default_warehouse:
                for it in lines:
                    it["warehouse"] = default_warehouse
            resolved_posting_date = _resolve_posting_date(order, is_historical)
            inv_data = {
                "doctype": "Sales Invoice",
                "customer": customer,
                "currency": order.get("currency") or getattr(settings, "default_currency", None) or "USD",
                "posting_date": resolved_posting_date,
                # Must set set_posting_time=1 to prevent ERPNext's validate from
                # overriding a backdated posting_date back to today.
                "set_posting_time": 1 if is_historical else 0,
                "company": getattr(settings, "default_company", None) or frappe.defaults.get_global_default("company"),
                "woo_order_id": woo_id,
                "woo_order_number": order.get("number"),
                "customer_address": billing_addr or shipping_addr,
                "shipping_address_name": shipping_addr or billing_addr,
                "items": lines,
            }
            # Prevent ERPNext pricing rules from overriding the rates set by
            # BundleProcessor (or any other rate logic) during insert/save.
            inv_data["ignore_pricing_rule"] = 1
            # Capture customer note as remarks
            customer_note = (order.get("customer_note") or "").strip()
            if customer_note:
                inv_data["remarks"] = customer_note
            # Capture WooCommerce transaction ID
            transaction_id = (order.get("transaction_id") or "").strip()
            if transaction_id:
                try:
                    inv_data["woo_transaction_id"] = transaction_id
                except Exception:
                    pass
            if delivery_date_val:
                inv_data["custom_delivery_date"] = delivery_date_val
            if time_from_val:
                inv_data["custom_delivery_time_from"] = time_from_val
            if duration_val is not None:
                inv_data["custom_delivery_duration"] = int(duration_val) * 60
            if price_list:
                inv_data["selling_price_list"] = price_list
            if custom_payment_method:
                inv_data["custom_payment_method"] = custom_payment_method

            if woo_status == "completed":
                inv_data["custom_acceptance_status"] = "Accepted"
                inv_data["custom_sales_invoice_state"] = "Delivered"
            elif woo_status == "out-for-delivery":
                inv_data["custom_acceptance_status"] = "Accepted"
                inv_data["custom_sales_invoice_state"] = "Out for Delivery"
            elif woo_status in ("cancelled", "refunded", "failed"):
                inv_data["custom_acceptance_status"] = "Accepted"
                inv_data["custom_sales_invoice_state"] = "Cancelled"
                # Populate cancellation classification fields (owned by jarz_pos; defensive check)
                try:
                    if frappe.get_meta("Sales Invoice").get_field("custom_cancellation_type"):
                        inv_data["custom_cancellation_type"] = "WooCommerce Refunded" if woo_status == "refunded" else "WooCommerce Cancelled"
                        inv_data["custom_cancellation_reason"] = f"Order {woo_status} on WooCommerce (Order #{woo_id})"
                except Exception:
                    pass

            # Merge all attribution fields (safe — only non-empty values returned)
            if attribution:
                inv_data.update(attribution)

            inv = frappe.get_doc(inv_data)
            try:
                if pos_profile:
                    inv.pos_profile = pos_profile
                    try:
                        inv.custom_kanban_profile = pos_profile
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                if not territory_name:
                    territory_name = frappe.db.get_value("Customer", customer, "territory")
                _apply_delivery_charge_policy(
                    inv,
                    territory_name=territory_name,
                    has_free_shipping_bundle=bundle_context.get("has_free_shipping_bundle", False),
                    cache=cache,
                    customer_name=customer,
                    pos_profile_name=pos_profile,
                    channel="woo",
                )
            except Exception:
                pass
            inv.insert(ignore_permissions=True)
            if status_map["docstatus"] == 1:
                _submit_invoice_with_accounting_guards(inv, pos_profile=pos_profile)
            elif status_map["docstatus"] == 2:
                if inv.docstatus == 0:
                    _submit_invoice_with_accounting_guards(inv, pos_profile=pos_profile)
                inv.cancel()

        _maybe_create_payment_entry_for_invoice(
            inv,
            order,
            status_map,
            custom_payment_method,
            woo_payment_method,
            is_historical=is_historical,
            cache=cache,
            skip_payment_entry=skip_payment_entry,
        )

        try:
            if status_map.get("custom_state"):
                inv.db_set("sales_invoice_state", status_map["custom_state"], commit=False)
        except Exception:
            pass

        if woo_status == "failed" and inv:
            _add_payment_failure_comment(inv.name, woo_id)

        map_name = existing_map.get("name") if existing_map else None
        if map_name:
            map_doc = frappe.get_doc("WooCommerce Order Map", map_name)
            map_doc.update({
                "woo_order_number": order.get("number"),
                LINK_FIELD: inv.name,
                "status": order.get("status"),
                "currency": order.get("currency"),
                "total": order.get("total"),
                "payment_method": order.get("payment_method"),
                "synced_on": frappe.utils.now_datetime(),
                "hash": order_hash,
                "needs_territory_recheck": 1 if _territory_fallback_used else 0,
            })
            map_doc.save(ignore_permissions=True)
        else:
            frappe.get_doc({
                "doctype": "WooCommerce Order Map",
                "woo_order_id": woo_id,
                "woo_order_number": order.get("number"),
                LINK_FIELD: inv.name,
                "status": order.get("status"),
                "currency": order.get("currency"),
                "total": order.get("total"),
                "payment_method": order.get("payment_method"),
                "synced_on": frappe.utils.now_datetime(),
                "hash": order_hash,
            }).insert(ignore_permissions=True)

        # Keep cache fresh so reruns benefit from O(1) skip
        if cache and woo_id:
            cache.order_map_set.add(int(woo_id))

        # --- Customer first-touch attribution ---
        # Only write if the customer has no first-touch data yet (first order wins).
        if attribution and customer:
            try:
                existing_first = frappe.db.get_value("Customer", customer, "woo_first_source_type")
                if not existing_first:
                    first_touch_data = {
                        customer_field: attribution[inv_field]
                        for inv_field, customer_field in ATTRIBUTION_FIRST_TOUCH_FIELDS.items()
                        if inv_field in attribution
                    }
                    if first_touch_data:
                        frappe.db.set_value("Customer", customer, first_touch_data, update_modified=False)
            except Exception:
                pass  # Non-critical — do not block order sync

        return {"status": action, "invoice": inv.name, "woo_order_id": woo_id}
    except Exception as e:  # noqa: BLE001
        frappe.db.rollback()
        # Clean up orphan map entry left by the rollback (if committed separately)
        try:
            orphan_map = frappe.db.get_value("WooCommerce Order Map", {"woo_order_id": woo_id}, "name")
            if orphan_map:
                orphan_inv = frappe.db.get_value("WooCommerce Order Map", orphan_map, LINK_FIELD)
                if not orphan_inv:
                    frappe.delete_doc("WooCommerce Order Map", orphan_map, force=1, ignore_permissions=True)
                    frappe.db.commit()
        except Exception:
            pass
        return {"status": "error", "reason": str(e), "woo_order_id": woo_id}
    finally:
        # Always clear the outbound-suppression flag
        frappe.flags.ignore_woo_outbound = False
        if lock:
            try:
                lock.release()
            except Exception:
                pass
        if db_lock_acquired:
            try:
                frappe.db.sql("SELECT RELEASE_LOCK(%s)", (db_lock_key,))
            except Exception:
                pass


def pull_recent_orders_phase1(
    limit: int = 20,
    dry_run: bool = False,
    force: bool = False,
    allow_update: bool = True,
    is_historical: bool = False,
    status: str | None = None,
    after: str | None = None,
    before: str | None = None,
    modified_after: str | None = None,
    modified_before: str | None = None,
    orderby: str | None = None,
    order: str | None = None,
    max_pages: int = 1,
    status_filter_set: set[str] | None = None,
) -> dict[str, Any]:
    """Pull recent orders from WooCommerce.
    
    Args:
        limit: Number of orders to fetch
        dry_run: If True, don't create invoices
        force: Force recreation (delete existing mappings)
        allow_update: Allow updating existing invoices
        is_historical: True for historical migration (completed/cancelled only, marked as paid)
                      False for live orders (all statuses, marked as unpaid)
        status: Optional WooCommerce status filter
        after: Optional WooCommerce lower timestamp bound (ISO 8601)
        before: Optional WooCommerce upper timestamp bound (ISO 8601)
        modified_after: Optional WooCommerce lower modified timestamp bound (ISO 8601)
        modified_before: Optional WooCommerce upper modified timestamp bound (ISO 8601)
        orderby: Optional WooCommerce sort field
        order: Optional WooCommerce sort direction
        max_pages: Maximum pages to scan from WooCommerce
    """
    settings = frappe.get_single("WooCommerce Settings")
    ensure_custom_fields()

    client = WooClient(
        base_url=settings.base_url,
        consumer_key=settings.consumer_key,
        consumer_secret=settings.get_password("consumer_secret"),
    )

    params: dict[str, Any] = {"per_page": max(1, min(int(limit), 100))}
    effective_status = status
    if is_historical and not effective_status:
        effective_status = "completed,cancelled,refunded"
    if effective_status:
        params["status"] = effective_status
    if after:
        params["after"] = after
    if before:
        params["before"] = before
    if modified_after:
        params["modified_after"] = modified_after
    if modified_before:
        params["modified_before"] = modified_before
    if orderby:
        params["orderby"] = orderby
    if order:
        params["order"] = order

    orders, pages_fetched, total_pages = _list_orders_window(
        client,
        params=params,
        max_pages=max_pages,
    )
    orders_fetched_raw = len(orders)
    if status_filter_set:
        orders = [
            o for o in orders
            if (o.get("status") or "").strip().lower() in status_filter_set
        ]
    metrics: dict[str, Any] = {
        "orders_fetched": len(orders),
        "orders_fetched_raw": orders_fetched_raw,
        "filtered_out": orders_fetched_raw - len(orders),
        "pages_fetched": pages_fetched,
        "total_pages": total_pages,
        "processed": 0,
        "created": 0,
        "skipped": 0,
        "errors": 0,
        "results_sample": [],
        "dry_run": dry_run,
        "force": force,
        "allow_update": allow_update,
        "is_historical": is_historical,
        "status": effective_status,
        "after": after,
        "before": before,
        "modified_after": modified_after,
        "modified_before": modified_before,
        "orderby": orderby,
        "order": order,
        "max_pages": max_pages,
    }
    skip_reasons: dict[str, int] = {}
    latest_seen_modified: datetime | None = None
    latest_seen_order_id = 0

    for o in orders:
        modified_at, order_id = _extract_order_cursor(o)
        if modified_at is not None:
            if latest_seen_modified is None or modified_at > latest_seen_modified:
                latest_seen_modified = modified_at
                latest_seen_order_id = order_id
            elif modified_at == latest_seen_modified and order_id > latest_seen_order_id:
                latest_seen_order_id = order_id

        result = (
            process_order_phase1(o, settings, allow_update=allow_update, is_historical=is_historical)
            if not dry_run else {"status": "dry_run", "woo_order_id": o.get("id")}
        )
        if not dry_run and result.get("status") != "error":
            frappe.db.commit()
        metrics["processed"] += 1
        if result["status"] in ("created", "updated"):
            metrics["created"] += 1
        elif result["status"] == "error":
            metrics["errors"] += 1
        elif result["status"] in ("skipped", "dry_run"):
            metrics["skipped"] += 1
            if result["status"] == "skipped":
                reason = str(result.get("reason") or "unknown")
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
        if len(metrics["results_sample"]) < 10:
            metrics["results_sample"].append(result)

    metrics["skip_reasons"] = skip_reasons
    metrics["fetched_order_ids_sample"] = [o.get("id") for o in orders[:20] if o.get("id") is not None]
    metrics["latest_seen_modified_gmt"] = (
        _format_datetime_for_woo(latest_seen_modified) if latest_seen_modified is not None else None
    )
    metrics["latest_seen_order_id"] = latest_seen_order_id or None

    return metrics


def pull_single_order_phase1(order_id: int | str, dry_run: bool = False, force: bool = False, allow_update: bool = True) -> dict[str, Any]:
    settings = frappe.get_single("WooCommerce Settings")
    ensure_custom_fields()
    client = WooClient(
        base_url=settings.base_url,
        consumer_key=settings.consumer_key,
        consumer_secret=settings.get_password("consumer_secret"),
    )
    order = client.get_order(order_id)
    if not order:
        return {"success": False, "reason": "order_not_found", "order_id": order_id}

    # Prefer updating the existing invoice; only delete mapping if explicitly forced and no update allowed
    if force and not allow_update:
        existing = frappe.db.get_value("WooCommerce Order Map", {"woo_order_id": order_id}, "name")
        if existing:
            frappe.delete_doc("WooCommerce Order Map", existing, force=1, ignore_permissions=True)

    if dry_run:
        status_map = _map_status(order.get("status"))
        line_count = len(order.get("line_items", []) or [])
        return {
            "success": True,
            "dry_run": True,
            "order_id": order_id,
            "status_mapping": status_map,
            "line_items": line_count,
            "already_mapped": frappe.db.exists("WooCommerce Order Map", {"woo_order_id": order_id}) is not None,
        }

    result = process_order_phase1(order, settings, allow_update=allow_update)
    skipped_success_reasons = {
        "already_mapped",
        "unchanged",
        "pending_payment",
        "processing",
        "locked",
        "db_locked",
        "submitted_frozen",  # PROD-WOO-001: submitted SI is intentionally frozen
    }
    result["success"] = result.get("status") in ("created", "updated") or (
        result.get("status") == "skipped" and result.get("reason") in skipped_success_reasons
    )
    return result


def backfill_orders_by_ids_phase1(
    order_ids: list[int | str] | tuple[int | str, ...] | str,
    *,
    dry_run: bool = False,
    force: bool = False,
    allow_update: bool = True,
) -> dict[str, Any]:
    if isinstance(order_ids, str):
        parsed_ids = [part.strip() for part in order_ids.split(",") if part.strip()]
    else:
        parsed_ids = [str(order_id).strip() for order_id in order_ids if str(order_id).strip()]

    results: list[dict[str, Any]] = []
    summary = {
        "requested": len(parsed_ids),
        "processed": 0,
        "created": 0,
        "updated": 0,
        "skipped": 0,
        "errors": 0,
        "results": results,
        "dry_run": dry_run,
        "force": force,
        "allow_update": allow_update,
    }

    for order_id in parsed_ids:
        result = pull_single_order_phase1(
            order_id=order_id,
            dry_run=dry_run,
            force=force,
            allow_update=allow_update,
        )
        results.append(result)
        summary["processed"] += 1
        status = result.get("status")
        if status == "created":
            summary["created"] += 1
        elif status == "updated":
            summary["updated"] += 1
        elif status == "error":
            summary["errors"] += 1
        else:
            summary["skipped"] += 1

    return summary


def reconcile_recent_orders_phase1(
    lookback_minutes: int | None = None,
    *,
    dry_run: bool = False,
    statuses: str | None = None,
    max_pages: int | None = None,
    allow_update: bool = True,
) -> dict[str, Any]:
    settings = frappe.get_single("WooCommerce Settings")
    ensure_custom_fields()

    effective_lookback = max(
        1,
        int(
            lookback_minutes
            or _get_setting_int(
                settings,
                "order_reconcile_lookback_minutes",
                DEFAULT_RECONCILE_LOOKBACK_MINUTES,
            )
        ),
    )
    effective_max_pages = max(
        1,
        int(
            max_pages
            or _get_setting_int(
                settings,
                "order_reconcile_max_pages",
                DEFAULT_RECONCILE_MAX_PAGES,
            )
        ),
    )
    modified_after = _format_datetime_for_woo(
        datetime.now(timezone.utc) - timedelta(minutes=effective_lookback)
    )
    effective_api_status = RECONCILE_API_STATUS_FILTER if not statuses else statuses
    effective_filter_set = set(RECONCILE_TARGET_WOO_STATUSES) if not statuses else None
    result = pull_recent_orders_phase1(
        limit=100,
        dry_run=dry_run,
        force=False,
        allow_update=allow_update,
        is_historical=False,
        status=effective_api_status,
        modified_after=modified_after,
        orderby="modified",
        order="asc",
        max_pages=effective_max_pages,
        status_filter_set=effective_filter_set,
    )
    result["lookback_minutes"] = effective_lookback
    return result


def _run_order_cursor_sync(
    *,
    cursor_name: str,
    operation: str,
    event_name: str,
    error_event: str,
    status: str | None,
    overlap_field: str,
    default_overlap_minutes: int,
    pages_field: str,
    default_max_pages: int,
    bootstrap_lookback_minutes: int,
) -> dict[str, Any]:
    settings = frappe.get_single("WooCommerce Settings")
    ensure_custom_fields()

    overlap_minutes = _get_setting_int(settings, overlap_field, default_overlap_minutes)
    max_pages = _get_setting_int(settings, pages_field, default_max_pages)
    cursor_before_dt, cursor_before_order_id = _get_order_sync_cursor(settings, cursor_name)

    if cursor_before_dt is not None:
        modified_after_dt = cursor_before_dt - timedelta(minutes=overlap_minutes)
        cold_start = False
    else:
        modified_after_dt = datetime.now(timezone.utc) - timedelta(
            minutes=max(overlap_minutes, bootstrap_lookback_minutes)
        )
        cold_start = True

    started_on = frappe.utils.now_datetime()
    log_doc = create_sync_log_entry(
        operation,
        "Started",
        {
            "cursor_name": cursor_name,
            "status": status,
            "overlap_minutes": overlap_minutes,
            "max_pages": max_pages,
            "cold_start": cold_start,
            "modified_after": _format_datetime_for_woo(modified_after_dt),
        },
        started_on=started_on,
    )

    try:
        result = pull_recent_orders_phase1(
            limit=100,
            dry_run=False,
            force=False,
            allow_update=True,
            is_historical=False,
            status=status,
            modified_after=_format_datetime_for_woo(modified_after_dt),
            orderby="modified",
            order="asc",
            max_pages=max_pages,
        )
        result.update(
            {
                "cursor_name": cursor_name,
                "cursor_before_modified_gmt": (
                    _format_datetime_for_woo(cursor_before_dt) if cursor_before_dt is not None else None
                ),
                "cursor_before_order_id": cursor_before_order_id or None,
                "cold_start": cold_start,
                "overlap_minutes": overlap_minutes,
            }
        )

        _update_order_sync_cursor_from_metrics(settings, cursor_name, result)
        frappe.db.commit()

        cursor_after_dt, cursor_after_order_id = _get_order_sync_cursor(settings, cursor_name)
        result["cursor_after_modified_gmt"] = (
            _format_datetime_for_woo(cursor_after_dt) if cursor_after_dt is not None else None
        )
        result["cursor_after_order_id"] = cursor_after_order_id or None

        log_status = "Partial" if result.get("errors") else "Success"
        finish_sync_log_entry(log_doc, log_status, result, started_on=started_on)
        frappe.logger().info({"event": event_name, "result": result})
        return result
    except Exception:  # noqa: BLE001
        finish_sync_log_entry(
            log_doc,
            "Failed",
            "Exception",
            traceback=frappe.get_traceback(),
            started_on=started_on,
        )
        frappe.logger().error({"event": error_event, "traceback": frappe.get_traceback()})
        raise


def sync_orders_cron_phase1():  # pragma: no cover - scheduler entry for live orders
    """Cron job for live order sync (every 2 minutes).
    
    Fetches recent orders, skips pending payment, creates unpaid submitted invoices.
    Guarded by WooCommerce Settings → Enable Inbound Order Sync.
    """
    if not frappe.db.get_single_value("WooCommerce Settings", "enable_inbound_orders"):
        return
    try:
        _run_order_cursor_sync(
            cursor_name="live",
            operation="CronLive",
            event_name="woo_order_sync_live",
            error_event="woo_order_sync_live_error",
            status=None,
            overlap_field="live_order_overlap_minutes",
            default_overlap_minutes=DEFAULT_LIVE_ORDER_OVERLAP_MINUTES,
            pages_field="live_order_max_pages",
            default_max_pages=DEFAULT_LIVE_ORDER_MAX_PAGES,
            bootstrap_lookback_minutes=DEFAULT_LIVE_BOOTSTRAP_LOOKBACK_MINUTES,
        )
    except Exception:  # noqa: BLE001
        return


def sync_cancelled_orders_cron():  # pragma: no cover - scheduler entry for cancelled/refunded orders
    """Catch cancelled/refunded Woo orders that fall outside the short live polling window."""
    if not frappe.db.get_single_value("WooCommerce Settings", "enable_inbound_orders"):
        return
    try:
        _run_order_cursor_sync(
            cursor_name="cancelled",
            operation="CronCancelled",
            event_name="woo_order_sync_cancelled",
            error_event="woo_order_sync_cancelled_error",
            status="cancelled,refunded,failed",
            overlap_field="cancelled_order_overlap_minutes",
            default_overlap_minutes=DEFAULT_CANCELLED_ORDER_OVERLAP_MINUTES,
            pages_field="cancelled_order_max_pages",
            default_max_pages=DEFAULT_CANCELLED_ORDER_MAX_PAGES,
            bootstrap_lookback_minutes=DEFAULT_CANCELLED_BOOTSTRAP_LOOKBACK_MINUTES,
        )
    except Exception:  # noqa: BLE001
        return


def reconcile_recent_orders_cron():  # pragma: no cover - scheduler entry for missed orders
    """Hourly recovery sweep that backfills any Woo orders missed by webhook or live cron."""
    if not frappe.db.get_single_value("WooCommerce Settings", "enable_inbound_orders"):
        return

    settings = frappe.get_single("WooCommerce Settings")
    lookback_minutes = _get_setting_int(
        settings,
        "order_reconcile_lookback_minutes",
        DEFAULT_RECONCILE_LOOKBACK_MINUTES,
    )
    max_pages = _get_setting_int(
        settings,
        "order_reconcile_max_pages",
        DEFAULT_RECONCILE_MAX_PAGES,
    )

    started_on = frappe.utils.now_datetime()
    log_doc = create_sync_log_entry(
        "Reconcile",
        "Started",
        {
            "lookback_minutes": lookback_minutes,
            "max_pages": max_pages,
            "api_status_filter": RECONCILE_API_STATUS_FILTER,
            "target_statuses": list(RECONCILE_TARGET_WOO_STATUSES),
        },
        started_on=started_on,
    )

    try:
        result = reconcile_recent_orders_phase1(
            lookback_minutes=lookback_minutes,
            dry_run=False,
            max_pages=max_pages,
            allow_update=True,
        )
        log_status = "Partial" if result.get("errors") else "Success"
        finish_sync_log_entry(log_doc, log_status, result, started_on=started_on)
        frappe.logger().info({"event": "woo_order_sync_reconcile", "result": result})
    except Exception:  # noqa: BLE001
        finish_sync_log_entry(
            log_doc,
            "Failed",
            "Exception",
            traceback=frappe.get_traceback(),
            started_on=started_on,
        )
        frappe.logger().error({"event": "woo_order_sync_reconcile_error", "traceback": frappe.get_traceback()})


def run_pos_profile_update_cli():  # pragma: no cover
        """Convenience entry point to update latest 10 orders with POS profile mapping.

        Usage (inside container):
            bench --site <site> execute jarz_woocommerce_integration.services.order_sync.run_pos_profile_update_cli
        """
        return pull_recent_orders_phase1(limit=10, dry_run=False, force=True, allow_update=True)


# ---------------------------------------------------------------------------
# Full Historical Migration (API-triggered, background-job safe)
# ---------------------------------------------------------------------------

MIGRATION_PROGRESS_KEY = "woo_historical_migration_progress"


def _run_full_historical_migration(
    date_from: str | None = None,
    date_to: str | None = None,
    batch_size: int = 100,
    statuses: str = "any",
    start_page: int = 1,
    page_sample_interval: int = 0,
    commit_every: int = 5,
    defer_payment_entries: bool = False,
    end_page: int = 0,
    worker_id: str = "",
) -> dict:
    """Paginated migration of ALL WooCommerce orders into ERPNext.

    Designed to run as a long-running background job. Progress is written to
    Redis so callers can poll ``migration_progress()``.

    Args:
        date_from: ISO date string (YYYY-MM-DD) – only orders created after this date.
        date_to:   ISO date string (YYYY-MM-DD) – only orders created before this date.
        batch_size: Orders per page (max 100 per WooCommerce API).
        statuses: Comma-separated Woo statuses, or "any" for all.
        start_page: Page number to start from (for resuming interrupted migrations).
        page_sample_interval: When > 0, only process every Nth page (skip API fetch
            for others). Use for sampled test runs — e.g. 5 means process pages
            1, 6, 11, 16... covering ~20% of orders across all time frames.
            Default 0 = process every page (full migration).
        commit_every: Commit the DB transaction every N processed pages (default 5).
            Reduces DB commit overhead significantly vs per-page commits.
            On crash, at most commit_every * batch_size orders must be re-processed.
        defer_payment_entries: When True, skip Payment Entry creation during order
            processing. Run _batch_create_payment_entries() as a separate pass after
            all pages are complete. Eliminates ~50% of doc-creation overhead for
            completed orders.
        end_page: Stop after processing this page number (0 = no limit, process all
            pages). Use with worker_id for parallel multi-worker runs where each
            worker covers a distinct page range.
        worker_id: Optional identifier for parallel runs. When set, progress is written
            to Redis key ``woo_historical_migration_progress:{worker_id}`` instead of
            the default key, allowing multiple workers to run simultaneously.
            Use get_parallel_migration_progress() to aggregate all worker stats.
    """
    import gc
    import time as _time

    sample_mode = int(page_sample_interval) > 0
    sample_interval = int(page_sample_interval) if sample_mode else 1
    commit_every = max(1, int(commit_every))
    end_page = int(end_page)

    settings = frappe.get_single("WooCommerce Settings")
    ensure_custom_fields()

    # Suppress Frappe non-essential hooks during bulk migration:
    # - in_migrate: skips notifications, document following, route conflict checks, server scripts
    # NOTE: in_import is intentionally NOT set — it suppresses set_missing_values() in ERPNext's
    # SellingController which causes SI fields (selling_price_list, price_list_currency,
    # plc_conversion_rate) to be empty, failing mandatory validation.
    _prev_in_migrate = getattr(frappe.flags, "in_migrate", False)
    frappe.flags.in_migrate = True

    # Choose Redis progress key: worker-scoped for parallel runs, default for serial
    progress_key = (
        f"{MIGRATION_PROGRESS_KEY}:{worker_id.strip()}"
        if worker_id and worker_id.strip()
        else MIGRATION_PROGRESS_KEY
    )

    client = WooClient(
        base_url=settings.base_url,
        consumer_key=settings.consumer_key,
        consumer_secret=settings.get_password("consumer_secret"),
    )

    # Pre-load all lookup tables into memory for fast dict-based resolution
    company = getattr(settings, "default_company", None) or frappe.defaults.get_global_default("company")
    cache = MigrationCache(company=company)
    cache.load()
    frappe.logger().info(
        f"MigrationCache loaded: {len(cache.sku_to_item)} items, "
        f"{len(cache.item_prices)} prices, {len(cache.bundle_map)} bundles, "
        f"{len(cache.territory_chain)} territories, {len(cache.order_map_set)} existing maps, "
        f"{len(cache.company_accounts)} accounts, {len(cache.item_groups)} item_groups"
    )

    # Build base params
    base_params: dict = {
        "per_page": min(int(batch_size), 100),
        "orderby": "date",
        "order": "asc",  # oldest first so we fill history chronologically
    }
    if statuses and statuses != "any":
        base_params["status"] = statuses
    if date_from:
        base_params["after"] = f"{date_from}T00:00:00"
    if date_to:
        base_params["before"] = f"{date_to}T23:59:59"

    # First request to learn total counts
    base_params["page"] = int(start_page)
    orders, total_count, total_pages = client.list_orders_with_meta(params=base_params)

    stats = {
        "total_woo_orders": total_count,
        "total_pages": total_pages,
        "orders_fetched": 0,
        "processed": 0,
        "created": 0,
        "skipped": 0,
        "errors": 0,
        "error_details": [],
        "pages_processed": 0,
        "pages_skipped": 0,
        "last_completed_page": 0,
        "start_page": int(start_page),
        "end_page": end_page or total_pages,
        "batch_size": int(batch_size),
        "sample_mode": sample_mode,
        "page_sample_interval": int(page_sample_interval),
        "defer_payment_entries": bool(defer_payment_entries),
        "worker_id": worker_id or "",
        "date_from": date_from,
        "date_to": date_to,
        "statuses": statuses,
        "started_at": frappe.utils.now_datetime().isoformat(),
        "finished_at": None,
        "running": True,
    }

    def _save_progress():
        try:
            from frappe.utils.background_jobs import get_redis_conn
            r = get_redis_conn()
            import json as _json
            r.set(progress_key, _json.dumps(stats, default=str), ex=86400)
        except Exception:
            pass

    def _process_page(page_orders: list[dict], page_num: int):
        stats["orders_fetched"] += len(page_orders)
        for o in page_orders:
            try:
                result = process_order_phase1(
                    o, settings,
                    allow_update=False,
                    is_historical=True,
                    cache=cache,
                    skip_payment_entry=bool(defer_payment_entries),
                )
                stats["processed"] += 1
                st = result.get("status", "")
                if st in ("created", "updated"):
                    stats["created"] += 1
                elif st == "error":
                    stats["errors"] += 1
                    if len(stats["error_details"]) < 50:
                        stats["error_details"].append({
                            "woo_order_id": result.get("woo_order_id"),
                            "reason": result.get("reason", ""),
                        })
                else:
                    stats["skipped"] += 1
            except Exception as exc:
                stats["processed"] += 1
                stats["errors"] += 1
                wid = o.get("id")
                frappe.log_error(f"Migration error order {wid}: {exc}", "Historical Migration")
                if len(stats["error_details"]) < 50:
                    stats["error_details"].append({"woo_order_id": wid, "reason": str(exc)[:200]})

        stats["pages_processed"] = page_num
        stats["last_completed_page"] = page_num

        # Lightweight GC every 20 pages
        if page_num % 20 == 0:
            try:
                gc.collect()
            except Exception:
                pass
        _save_progress()

    try:
        # Process first page (already fetched)
        _process_page(orders, int(start_page))

        # Effective last page: honour end_page cap if set
        effective_last_page = end_page if end_page > 0 else (total_pages or 1)

        # Remaining pages
        pages_since_commit = 1  # first page already processed
        for page in range(int(start_page) + 1, effective_last_page + 1):
            # Sample mode: skip pages that don't fall on the interval
            if sample_mode and (page - int(start_page)) % sample_interval != 0:
                stats["pages_skipped"] += 1
                continue

            base_params["page"] = page
            try:
                page_orders, _, _ = client.list_orders_with_meta(params=base_params)
            except Exception as fetch_err:
                frappe.log_error(f"Migration fetch error page {page}: {fetch_err}", "Historical Migration")
                stats["errors"] += 1
                continue
            if not page_orders:
                break
            _process_page(page_orders, page)
            pages_since_commit += 1

            # Commit every commit_every pages (not per-page) — reduces DB overhead
            if pages_since_commit >= commit_every:
                try:
                    frappe.db.commit()
                except Exception as commit_err:
                    frappe.log_error(
                        f"DB commit failed after page {page}: {commit_err}",
                        "Historical Migration Commit",
                    )
                pages_since_commit = 0

            # Brief pause every 10 pages to avoid overwhelming the WooCommerce API
            if page % 10 == 0:
                _time.sleep(1)
                sample_label = " [SAMPLE]" if sample_mode else ""
                worker_label = f" [worker={worker_id}]" if worker_id else ""
                frappe.logger().info(
                    f"Migration checkpoint{sample_label}{worker_label}: page {page}/{effective_last_page}, "
                    f"created={stats['created']}, skipped={stats['skipped']}, errors={stats['errors']}"
                )

        # Final commit for any remaining uncommitted pages
        try:
            frappe.db.commit()
        except Exception as commit_err:
            frappe.log_error(
                f"DB final commit failed: {commit_err}",
                "Historical Migration Commit",
            )

        stats["finished_at"] = frappe.utils.now_datetime().isoformat()
        stats["running"] = False
        _save_progress()

        frappe.logger().info({"event": "woo_full_historical_migration_complete", "stats": stats, "worker_id": worker_id})
        return stats

    finally:
        # Restore Frappe flags regardless of success or failure
        frappe.flags.in_migrate = _prev_in_migrate


def get_migration_progress() -> dict:
    """Read current migration progress from Redis (default / non-parallel key)."""
    try:
        from frappe.utils.background_jobs import get_redis_conn
        import json as _json
        r = get_redis_conn()
        raw = r.get(MIGRATION_PROGRESS_KEY)
        if raw:
            return _json.loads(raw)
    except Exception:
        pass
    return {"running": False, "message": "No migration in progress or data expired."}


def get_parallel_migration_progress() -> dict:
    """Aggregate progress across all active parallel workers.

    Scans Redis for all keys matching ``woo_historical_migration_progress:*``
    and the default key, then returns per-worker breakdown plus aggregated totals.
    Workers are considered running if any of them have ``running: True``.

    Returns a dict with:
        - ``workers``: list of per-worker progress dicts
        - ``total_created``, ``total_errors``, ``total_skipped``, ``total_fetched``
        - ``any_running``: bool — True if at least one worker is still active
        - ``all_finished``:  bool — True when all workers have ``running: False``
    """
    try:
        from frappe.utils.background_jobs import get_redis_conn
        import json as _json
        r = get_redis_conn()

        workers = []

        # Collect all worker-scoped keys
        for key in r.scan_iter(f"{MIGRATION_PROGRESS_KEY}:*"):
            raw = r.get(key)
            if raw:
                try:
                    workers.append(_json.loads(raw))
                except Exception:
                    pass

        # Also check the default key (non-parallel / single-worker run)
        raw_default = r.get(MIGRATION_PROGRESS_KEY)
        if raw_default:
            try:
                default_data = _json.loads(raw_default)
                # Only include if it's a real run (not stale empty)
                if default_data.get("started_at"):
                    workers.append(default_data)
            except Exception:
                pass

        if not workers:
            return {
                "workers": [],
                "total_created": 0,
                "total_errors": 0,
                "total_skipped": 0,
                "total_fetched": 0,
                "any_running": False,
                "all_finished": True,
                "message": "No active or recent migration workers found.",
            }

        aggregated = {
            "workers": workers,
            "total_created": sum(w.get("created", 0) for w in workers),
            "total_errors": sum(w.get("errors", 0) for w in workers),
            "total_skipped": sum(w.get("skipped", 0) for w in workers),
            "total_fetched": sum(w.get("orders_fetched", 0) for w in workers),
            "any_running": any(w.get("running", False) for w in workers),
            "all_finished": all(not w.get("running", True) for w in workers),
        }
        return aggregated

    except Exception as e:
        return {"error": str(e), "workers": [], "any_running": False, "all_finished": False}


BATCH_PE_PROGRESS_KEY = "woo_batch_pe_progress"


def _batch_create_payment_entries(
    statuses: str = "completed",
    commit_every: int = 50,
) -> dict:
    """Create Payment Entries in a single batch pass for all Woo-linked submitted
    Sales Invoices that still have an outstanding balance.

    Use after running _run_full_historical_migration(defer_payment_entries=True).
    This function is idempotent — invoices already at outstanding=0 are silently
    skipped, so it is safe to re-run if interrupted.

    Args:
        statuses: Comma-separated WooCommerce order statuses to include (default "completed").
            Only completed orders normally have Payment Entries. Other statuses are
            included as a safeguard for partial reruns.
        commit_every: Commit the DB transaction every N Payment Entries (default 50).

    Returns:
        dict with keys: created, skipped, errors, error_details, running=False.
    """
    commit_every = max(1, int(commit_every))
    status_list = [s.strip() for s in (statuses or "completed").split(",") if s.strip()]

    settings = frappe.get_single("WooCommerce Settings")
    company = getattr(settings, "default_company", None) or frappe.defaults.get_global_default("company")

    # Light cache for account lookups only — no item/territory loading needed
    cache = MigrationCache(company=company)
    cache._load_company_accounts()

    stats = {
        "created": 0,
        "skipped": 0,
        "errors": 0,
        "error_details": [],
        "statuses": statuses,
        "running": True,
        "started_at": frappe.utils.now_datetime().isoformat(),
        "finished_at": None,
    }

    def _save_pe_progress():
        try:
            from frappe.utils.background_jobs import get_redis_conn
            import json as _json
            r = get_redis_conn()
            r.set(BATCH_PE_PROGRESS_KEY, _json.dumps(stats, default=str), ex=86400)
        except Exception:
            pass

    _save_pe_progress()

    # Fetch all Woo-linked submitted invoices with outstanding > 0 for the target statuses
    # Join via WooCommerce Order Map to filter by WooCommerce status
    try:
        placeholders = ", ".join(["%s"] * len(status_list))
        rows = frappe.db.sql(
            f"""
            SELECT si.name, si.customer, si.outstanding_amount, si.grand_total,
                   si.custom_payment_method, si.posting_date, si.company,
                   wm.status as woo_status
            FROM `tabSales Invoice` si
            JOIN `tabWooCommerce Order Map` wm ON wm.erpnext_sales_invoice = si.name
            WHERE si.docstatus = 1
              AND IFNULL(si.outstanding_amount, 0) > 0
              AND si.woo_order_id IS NOT NULL
              AND wm.status IN ({placeholders})
            ORDER BY si.posting_date ASC, si.name ASC
            """,
            tuple(status_list),
            as_dict=True,
        )
    except Exception as fetch_err:
        frappe.log_error(str(fetch_err), "Batch PE: Invoice Fetch Error")
        stats["running"] = False
        stats["finished_at"] = frappe.utils.now_datetime().isoformat()
        stats["error_details"].append({"reason": f"Invoice fetch failed: {fetch_err}"})
        _save_pe_progress()
        return stats

    total = len(rows)
    frappe.logger().info(f"Batch PE: found {total} invoices with outstanding > 0")
    commits_since = 0

    for i, row in enumerate(rows):
        invoice_name = row["name"]
        payment_method = row.get("custom_payment_method") or ""
        posting_date = row.get("posting_date")

        if not payment_method:
            stats["skipped"] += 1
            continue

        try:
            pe_name = _create_payment_entry(
                invoice_name,
                payment_method,
                posting_date=str(posting_date) if posting_date else None,
                cache=cache,
            )
            if pe_name:
                stats["created"] += 1
            else:
                stats["skipped"] += 1
        except Exception as pe_err:
            stats["errors"] += 1
            frappe.log_error(
                f"Batch PE error for {invoice_name}: {pe_err}",
                "Batch Payment Entry Error",
            )
            if len(stats["error_details"]) < 50:
                stats["error_details"].append({"invoice": invoice_name, "reason": str(pe_err)[:200]})

        commits_since += 1
        if commits_since >= commit_every:
            try:
                frappe.db.commit()
            except Exception as commit_err:
                frappe.log_error(str(commit_err), "Batch PE Commit Error")
            commits_since = 0

        # Save progress every 100 invoices
        if (i + 1) % 100 == 0:
            _save_pe_progress()
            frappe.logger().info(
                f"Batch PE progress: {i + 1}/{total}, created={stats['created']}, "
                f"skipped={stats['skipped']}, errors={stats['errors']}"
            )

    # Final commit
    try:
        frappe.db.commit()
    except Exception as commit_err:
        frappe.log_error(str(commit_err), "Batch PE Final Commit Error")

    stats["running"] = False
    stats["finished_at"] = frappe.utils.now_datetime().isoformat()
    _save_pe_progress()
    frappe.logger().info({"event": "woo_batch_pe_complete", "stats": stats})
    return stats


def get_batch_pe_progress() -> dict:
    """Read current batch Payment Entry creation progress from Redis."""
    try:
        from frappe.utils.background_jobs import get_redis_conn
        import json as _json
        r = get_redis_conn()
        raw = r.get(BATCH_PE_PROGRESS_KEY)
        if raw:
            return _json.loads(raw)
    except Exception:
        pass
    return {"running": False, "message": "No batch PE in progress or data expired."}

