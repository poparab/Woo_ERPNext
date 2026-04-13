from __future__ import annotations

from typing import Any, Tuple

from frappe.utils.background_jobs import get_redis_conn

import frappe

from jarz_woocommerce_integration.utils.http_client import WooClient
from jarz_woocommerce_integration.utils.custom_fields import ensure_custom_fields
from jarz_woocommerce_integration.services.customer_sync import ensure_customer_with_addresses


class MigrationCache:
    """In-memory lookup caches for historical migration.

    Pre-loads Items, Item Prices, Bundles, and Territory chains so that
    per-order processing can use dict lookups instead of DB queries.
    """

    def __init__(self, price_list: str | None = None, company: str | None = None):
        self.sku_to_item: dict[str, str] = {}          # item_code → item_code
        self.woo_pid_to_item: dict[str, str] = {}      # woo_product_id → item_code
        self.item_prices: dict[tuple[str, str], float] = {}  # (price_list, item_code) → rate
        self.bundle_map: dict[str, str] = {}            # woo_bundle_id → bundle_code
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
            "SELECT name, woo_bundle_id FROM `tabWoo Jarz Bundle`",
            as_dict=True,
        )
        for r in rows:
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

    def resolve_item(self, sku: str, product_id) -> str | None:
        """Resolve a Woo line item to an ERPNext item_code via cache."""
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

    def get_territory_data(self, territory_name: str | None) -> dict:
        if not territory_name:
            return {}
        return self.territory_chain.get(territory_name, {})


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
    if s == "processing":
        # Processing = payment received, not shipped yet. Submit but never mark as paid.
        return {"docstatus": 1, "custom_state": "Processing", "is_paid": False}
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


def _build_bundle_selections(
    line_items: list[dict],
    parent_product_id: int | str,
    parent_qty: int,
    cache: "MigrationCache | None" = None,
) -> dict:
    """Parse WooCommerce child line items to build *selected_items* for BundleProcessor.

    WooCommerce Smart Bundles (WOOSB) sends a parent line plus individual child
    lines whose ``meta_data`` contains ``_woosb_parent_id == parent_product_id``.
    Each child carries the actual item the customer chose (identified by ``sku``
    or ``product_id``) and the total quantity across all bundle units.

    Returns
    -------
    dict
        ``{item_group_name: [{"item_code": ..., "selected_qty": ...}, ...]}``
        Empty dict when any child cannot be mapped (caller should fall back to
        default bundle expansion).
    """
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
        wc_qty = int(float(wc.get("quantity") or 0))
        if wc_qty <= 0:
            continue

        # Resolve to ERPNext Item code
        wc_item_code = None
        if cache:
            wc_item_code = cache.resolve_item(wc_sku, wc_product_id)
        else:
            if wc_sku and frappe.db.exists("Item", wc_sku):
                wc_item_code = wc_sku
            elif wc_product_id:
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
        group_list = selected_items.setdefault(wc_item_group, [])
        found = False
        for entry in group_list:
            if entry["item_code"] == wc_item_code:
                entry["selected_qty"] += per_bundle_qty
                found = True
                break
        if not found:
            group_list.append({
                "item_code": wc_item_code,
                "selected_qty": per_bundle_qty,
            })

    if selected_items:
        frappe.logger().info(
            f"Bundle selections built from WooCommerce children: "
            f"{{{', '.join(f'{g}: {len(v)} item(s)' for g, v in selected_items.items())}}}"
        )
    return selected_items


def _build_invoice_items(order: dict, price_list: str | None = None, cache: "MigrationCache | None" = None, is_historical: bool = False) -> Tuple[list[dict], list[dict]]:
    """Build Sales Invoice Item rows from Woo order line_items.

        Pricing policy:
        - Live sync: Ignore WooCommerce prices/totals completely.
          Use ERPNext Price List rates for normal items (Item Price by price_list).
        - Historical sync: Use WooCommerce line item prices. If zero, fall back to ERPNext Price List rates.
    - Prefer Woo Jarz Bundle expansion for bundles (uses internal pricing from Woo Jarz Bundle),
            even when Woo sends woosb parent/child lines; expand once from the parent and skip
            the related children to avoid duplication.

    Returns: (items, missing_items_info)
    missing contains entries for lines we could not map (no item code/sku).
    """
    items: list[dict] = []
    missing: list[dict] = []

    line_items = order.get("line_items") or []

    # 0) Pre-scan for woosb children and collect their parent IDs
    def _get_parent_id_from_meta(md_list: list[dict] | None) -> str | None:
        for md in (md_list or []):
            key = (md.get("key") or md.get("display_key") or "").strip()
            if key == "_woosb_parent_id":
                val = (md.get("value") or md.get("display_value") or "").strip()
                return str(val) if val is not None else None
        return None

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
                selected_items = _build_bundle_selections(line_items, product_id, int(qty), cache=cache)

                try:
                    bp = BundleProcessor(bundle_code, int(qty), selected_items=selected_items)
                    bp.load_bundle()
                    bundle_lines = bp.get_invoice_items()
                except Exception as sel_err:
                    # If selection-based expansion fails, retry with defaults
                    if selected_items:
                        frappe.logger().warning(
                            f"Bundle {bundle_code}: selection-based expansion failed "
                            f"({sel_err}), retrying with default items"
                        )
                        bp = BundleProcessor(bundle_code, int(qty))
                        bp.load_bundle()
                        bundle_lines = bp.get_invoice_items()
                    else:
                        raise

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

        # 2) If this is a woosb child for a parent we've already handled via Woo Jarz Bundle, skip it
        parent_id_in_meta = _get_parent_id_from_meta(li.get("meta_data"))
        if parent_id_in_meta and str(parent_id_in_meta) in handled_parents:
            continue

        # 3) Fall back to direct Item by SKU or woo_product_id - for regular items
        item_code = None
        if cache:
            item_code = cache.resolve_item(sku, product_id)
        else:
            if sku and frappe.db.exists("Item", sku):
                item_code = sku
            elif product_id:
                item_code = frappe.db.get_value("Item", {"woo_product_id": str(product_id)}, "name")

        # 3b) Historical: name-based fallback for product_id=0 legacy bundles
        if not item_code and is_historical:
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

        # Pricing: historical uses Woo prices (fallback to ERPNext), live uses ERPNext only
        erp_price = None
        if cache:
            erp_price = cache.get_price(item_code, price_list)
        else:
            try:
                if price_list:
                    erp_price = frappe.db.get_value("Item Price", {"item_code": item_code, "price_list": price_list}, "price_list_rate")
            except Exception:
                erp_price = None

        if is_historical:
            # Historical: prefer WooCommerce line item price, fallback to ERPNext
            woo_price = 0.0
            try:
                woo_price = float(li.get("price") or 0)
            except (ValueError, TypeError):
                pass
            if woo_price == 0 and qty > 0:
                try:
                    woo_price = float(li.get("subtotal") or 0) / qty
                except (ValueError, TypeError, ZeroDivisionError):
                    pass
            if woo_price > 0:
                rate_value = woo_price
            else:
                rate_value = float(erp_price or 0) if erp_price is not None else 0
        else:
            # Live: ERPNext pricing only (unchanged)
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
    return items, missing


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

    # Skip if order hasn't changed since last sync (hash match + valid invoice link)
    if existing_map and existing_map.get("hash") == order_hash and existing_map.get(LINK_FIELD):
        return {"status": "skipped", "reason": "unchanged", "woo_order_id": woo_id}

    # Hard idempotency: if a Sales Invoice already exists with this woo_order_id, use it
    linked_invoice_name = None
    duplicate_invoices = []
    try:
        si_list = frappe.get_all(
            "Sales Invoice",
            filters={"woo_order_id": woo_id},
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

    # Resolve Territory -> POS Profile and warehouse (with fallback to settings.default_warehouse)
    territory_name = None
    pos_profile = None
    default_warehouse = None
    try:
        territory_name = frappe.db.get_value("Customer", customer, "territory")
        if cache and territory_name:
            td = cache.get_territory_data(territory_name)
            pos_profile = td.get("pos_profile")
            default_warehouse = td.get("warehouse")
        elif territory_name:
            pos_profile = frappe.db.get_value("Territory", territory_name, "pos_profile")
            if pos_profile:
                default_warehouse = frappe.db.get_value("POS Profile", pos_profile, "warehouse")
        if not default_warehouse:
            default_warehouse = getattr(settings, "default_warehouse", None)
    except Exception:
        pass

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

    lines, missing = _build_invoice_items(order, price_list=price_list, cache=cache, is_historical=is_historical)
    if missing:
        return {"status": "skipped", "reason": "unmapped_items", "details": missing, "woo_order_id": woo_id}
    if not lines:
        return {"status": "skipped", "reason": "no_lines", "woo_order_id": woo_id}

    # Extract WooCommerce shipping total (preferred over territory-based delivery income)
    woo_shipping_total = 0.0
    try:
        woo_shipping_total = float(order.get("shipping_total") or 0)
    except (ValueError, TypeError):
        pass

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
            if existing_map and (existing_map.get("status") or "").lower() == "processing":
                return {"status": "skipped", "reason": "processing", "woo_order_id": woo_id}
            raise map_err

    try:
        delivery_date_val, time_from_val, duration_val = _parse_delivery_parts(order)
        if not (delivery_date_val and time_from_val and (duration_val is not None)):
            delivery_date_val = None
            time_from_val = None
            duration_val = None

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
            if inv.docstatus != 2:
                if inv.docstatus == 0:
                    inv.set("items", [])
                    for it in lines:
                        if default_warehouse:
                            it["warehouse"] = default_warehouse
                        inv.append("items", it)
                    if price_list:
                        inv.selling_price_list = price_list
                if billing_addr or shipping_addr:
                    inv.customer_address = billing_addr or shipping_addr
                    inv.shipping_address_name = shipping_addr or billing_addr
                # Set POS Profile from Territory if available
                try:
                    if pos_profile:
                        if inv.docstatus == 1:
                            inv.db_set("pos_profile", pos_profile, commit=False)
                            try:
                                inv.db_set("custom_kanban_profile", pos_profile, commit=False)
                            except Exception:
                                pass
                            try:
                                inv.db_set("is_pos", 1, commit=False)
                            except Exception:
                                pass
                        else:
                            inv.pos_profile = pos_profile
                            inv.custom_kanban_profile = pos_profile
                            inv.is_pos = 1
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
                # Apply delivery charges: prefer WooCommerce shipping_total, fall back to territory
                try:
                    delivery_amt = woo_shipping_total
                    delivery_desc = "Shipping Income (WooCommerce)"
                    if not delivery_amt and territory_name:
                        if cache:
                            td = cache.get_territory_data(territory_name)
                            delivery_amt = td.get("delivery_income", 0)
                        elif frappe.db.exists("Territory", territory_name):
                            delivery_amt = float(frappe.db.get_value("Territory", territory_name, "delivery_income") or 0)
                        if delivery_amt:
                            delivery_desc = f"Shipping Income ({territory_name})"
                    if delivery_amt and delivery_amt > 0:
                        add_delivery_charges_to_taxes(inv, delivery_amt, delivery_description=delivery_desc)
                except Exception:
                    pass
                inv.save(ignore_permissions=True, ignore_version=True)

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
                elif woo_status in ("cancelled", "refunded", "failed"):
                    inv.db_set("custom_acceptance_status", "Accepted", commit=False)
                    inv.db_set("custom_sales_invoice_state", "Cancelled", commit=False)
            except Exception:
                pass

            try:
                if status_map["docstatus"] == 1 and inv.docstatus == 0:
                    inv.submit()
                elif status_map["docstatus"] == 2 and inv.docstatus in (0, 1):
                    if inv.docstatus == 0:
                        inv.submit()
                    inv.cancel()
            except Exception:
                pass
        else:
            if default_warehouse:
                for it in lines:
                    it["warehouse"] = default_warehouse
            inv_data = {
                "doctype": "Sales Invoice",
                "customer": customer,
                "currency": order.get("currency") or getattr(settings, "default_currency", None) or "USD",
                "posting_date": _resolve_posting_date(order, is_historical),
                "company": getattr(settings, "default_company", None) or frappe.defaults.get_global_default("company"),
                "woo_order_id": woo_id,
                "woo_order_number": order.get("number"),
                "customer_address": billing_addr or shipping_addr,
                "shipping_address_name": shipping_addr or billing_addr,
                "items": lines,
            }
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
            elif woo_status in ("cancelled", "refunded", "failed"):
                inv_data["custom_acceptance_status"] = "Accepted"
                inv_data["custom_sales_invoice_state"] = "Cancelled"

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
            # Apply delivery charges: prefer WooCommerce shipping_total, fall back to territory
            try:
                if not territory_name:
                    territory_name = frappe.db.get_value("Customer", customer, "territory")
                delivery_amt = woo_shipping_total
                delivery_desc = "Shipping Income (WooCommerce)"
                if not delivery_amt and territory_name:
                    if cache:
                        td = cache.get_territory_data(territory_name)
                        delivery_amt = td.get("delivery_income", 0)
                    elif frappe.db.exists("Territory", territory_name):
                        delivery_amt = float(frappe.db.get_value("Territory", territory_name, "delivery_income") or 0)
                    if delivery_amt:
                        delivery_desc = f"Shipping Income ({territory_name})"
                if delivery_amt and delivery_amt > 0:
                    add_delivery_charges_to_taxes(inv, delivery_amt, delivery_description=delivery_desc)
            except Exception:
                pass
            inv.insert(ignore_permissions=True)
            if status_map["docstatus"] == 1:
                inv.submit()
                try:
                    if pos_profile:
                        inv.db_set("pos_profile", pos_profile, commit=False)
                        try:
                            inv.db_set("custom_kanban_profile", pos_profile, commit=False)
                        except Exception:
                            pass
                        inv.db_set("is_pos", 1, commit=False)
                except Exception:
                    pass
            elif status_map["docstatus"] == 2:
                if inv.docstatus == 0:
                    inv.submit()
                inv.cancel()

        if inv and inv.docstatus == 1 and status_map.get("is_paid") and not skip_payment_entry:
            if custom_payment_method:
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
            else:
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


def pull_recent_orders_phase1(limit: int = 20, dry_run: bool = False, force: bool = False, allow_update: bool = True, is_historical: bool = False) -> dict[str, Any]:
    """Pull recent orders from WooCommerce.
    
    Args:
        limit: Number of orders to fetch
        dry_run: If True, don't create invoices
        force: Force recreation (delete existing mappings)
        allow_update: Allow updating existing invoices
        is_historical: True for historical migration (completed/cancelled only, marked as paid)
                      False for live orders (all statuses, marked as unpaid)
    """
    settings = frappe.get_single("WooCommerce Settings")
    ensure_custom_fields()

    client = WooClient(
        base_url=settings.base_url,
        consumer_key=settings.consumer_key,
        consumer_secret=settings.get_password("consumer_secret"),
    )

    # Build params based on mode
    params = {"per_page": limit}
    if is_historical:
        # Historical: only fetch completed and cancelled orders
        params["status"] = "completed,cancelled,refunded"
    
    orders = client.list_orders(params=params)
    metrics: dict[str, Any] = {
        "orders_fetched": len(orders),
        "processed": 0,
        "created": 0,
        "skipped": 0,
        "errors": 0,
        "results_sample": [],
        "dry_run": dry_run,
        "force": force,
        "allow_update": allow_update,
        "is_historical": is_historical,
    }

    for o in orders:
        result = (
            process_order_phase1(o, settings, allow_update=allow_update, is_historical=is_historical)
            if not dry_run else {"status": "dry_run", "woo_order_id": o.get("id")}
        )
        metrics["processed"] += 1
        if result["status"] in ("created", "updated"):
            metrics["created"] += 1
        elif result["status"] == "error":
            metrics["errors"] += 1
        elif result["status"] in ("skipped", "dry_run"):
            metrics["skipped"] += 1
        if len(metrics["results_sample"]) < 10:
            metrics["results_sample"].append(result)

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
    result["success"] = result.get("status") in ("created", "updated")
    return result


def sync_orders_cron_phase1():  # pragma: no cover - scheduler entry for live orders
    """Cron job for live order sync (every 2 minutes).
    
    Fetches recent orders, skips pending payment, creates unpaid submitted invoices.
    """
    try:
        res = pull_recent_orders_phase1(limit=20, is_historical=False)
        frappe.logger().info({"event": "woo_order_sync_live", "result": res})
    except Exception:  # noqa: BLE001
        frappe.logger().error({"event": "woo_order_sync_live_error", "traceback": frappe.get_traceback()})


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
    # - in_import: skips email notifications, posting-time auto-set (allows historical dates)
    # Both flags are standard Frappe bulk-operation patterns. All GL, validation, and
    # permissions hooks still run normally.
    _prev_in_migrate = getattr(frappe.flags, "in_migrate", False)
    _prev_in_import = getattr(frappe.flags, "in_import", False)
    frappe.flags.in_migrate = True
    frappe.flags.in_import = True

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
        frappe.flags.in_import = _prev_in_import


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

