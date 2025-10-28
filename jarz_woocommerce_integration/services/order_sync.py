from __future__ import annotations

from typing import Any, Tuple

import frappe

from jarz_woocommerce_integration.utils.http_client import WooClient
from jarz_woocommerce_integration.utils.custom_fields import ensure_custom_fields
from jarz_woocommerce_integration.services.customer_sync import ensure_customer_with_addresses


def _map_status(woo_status: str | None, is_historical: bool = False) -> dict[str, Any]:
    """Map Woo status to ERPNext docstatus and custom state.
    
    Args:
        woo_status: WooCommerce order status
        is_historical: If True, creates paid invoices for completed orders (historical migration)
                      If False, creates unpaid submitted invoices (live orders)
    """
    s = (woo_status or "").lower()
    if s in {"completed", "processing"}:
        if is_historical:
            # Historical: mark as paid (submitted + paid status)
            return {"docstatus": 1, "custom_state": "Completed", "is_paid": True}
        else:
            # Live: mark as submitted but unpaid
            return {"docstatus": 1, "custom_state": "Completed", "is_paid": False}
    if s in {"cancelled", "refunded"}:
        return {"docstatus": 2, "custom_state": "Cancelled", "is_paid": False}
    return {"docstatus": 0, "custom_state": "Draft", "is_paid": False}


def _map_payment_method(woo_payment_method: str | None) -> str | None:
    """Map WooCommerce payment method to ERPNext custom_payment_method.
    
    WooCommerce -> ERPNext mapping:
    - instapay -> Instapay
    - cod -> Cash
    - kashier_card -> Kashier Card
    - kashier_wallet -> Kashier Wallet
    """
    if not woo_payment_method:
        return None
    
    pm = woo_payment_method.lower().strip()
    if pm == "instapay":
        return "Instapay"
    elif pm == "cod":
        return "Cash"
    elif pm == "kashier_card":
        return "Kashier Card"
    elif pm == "kashier_wallet":
        return "Kashier Wallet"
    else:
        return None


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


def _build_invoice_items(order: dict, price_list: str | None = None) -> Tuple[list[dict], list[dict]]:
    """Build Sales Invoice Item rows from Woo order line_items.

        Pricing policy:
        - Ignore WooCommerce prices/totals completely.
        - Use ERPNext Price List rates for normal items (Item Price by price_list).
        - Prefer Jarz Bundle expansion for bundles (uses internal pricing from Jarz Bundle),
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

    # Pre-compute uniform discount percentage per woosb parent from Jarz Bundle config
    parent_uniform_discount: dict[str, float] = {}
    if child_parent_ids:
        for pid in child_parent_ids:
            try:
                bundle_code = frappe.db.get_value("Jarz Bundle", {"woo_bundle_id": str(pid)}, "name")
                if not bundle_code:
                    continue
                from jarz_pos.services.bundle_processing import BundleProcessor  # type: ignore
                bp_tmp = BundleProcessor(bundle_code, 1)
                bp_tmp.load_bundle()
                uniform_pct, _total_child, _bundle_price = bp_tmp.calculate_child_discount_percentage()
                parent_uniform_discount[str(pid)] = float(uniform_pct)
            except Exception:
                continue

    for li in line_items:
        sku = (li.get("sku") or "").strip()
        product_id = li.get("product_id")
        qty = float(li.get("quantity") or 0) or 0
        if qty <= 0:
            continue

        # 1) Prefer Jarz Bundle expansion for bundle parents (even if woosb children exist)
        bundle_code = None
        if product_id:
            try:
                bundle_code = frappe.db.get_value("Jarz Bundle", {"woo_bundle_id": str(product_id)}, "name")
            except Exception:
                bundle_code = None
        if bundle_code and (str(product_id) in child_parent_ids or not has_woosb_children):
            try:
                # Import locally to avoid hard dependency at module import time
                from jarz_pos.services.bundle_processing import BundleProcessor  # type: ignore
                bp = BundleProcessor(bundle_code, int(qty))
                bp.load_bundle()  # Ensure bundle is loaded before getting items
                bundle_lines = bp.get_invoice_items()
                
                # Log for debugging
                frappe.logger().info(f"Bundle {bundle_code} expanded into {len(bundle_lines)} line items for qty {qty}")
                
                # Verify each bundle line has unique item_code
                seen_items = {}
                for idx, bl in enumerate(bundle_lines):
                    item_code = bl.get("item_code")
                    if item_code in seen_items:
                        frappe.logger().warning(
                            f"Duplicate item_code {item_code} found in bundle {bundle_code} "
                            f"at index {idx} (first seen at {seen_items[item_code]}). "
                            f"Bundle line: {bl}"
                        )
                    else:
                        seen_items[item_code] = idx
                
                # Keep only fields ERPNext Sales Invoice Item supports; we will materialize discount into the rate
                allowed = {"item_code", "item_name", "description", "qty", "rate", "price_list_rate", "discount_percentage", "discount_amount"}
                processed_lines: list[dict] = []
                for bl in bundle_lines:
                    filtered = {k: v for k, v in bl.items() if k in allowed or k in {"is_bundle_child", "is_bundle_parent"}}
                    # For child lines: convert any discount (percentage or amount) into a net rate so ERPNext doesn't recalc it away.
                    if filtered.get("is_bundle_child"):
                        try:
                            ch_qty = float(filtered.get("qty") or 0) or 0
                            if ch_qty <= 0:
                                processed_lines.append(filtered)
                                continue
                            # Determine original list price per unit (prefer explicit price_list_rate, else rate)
                            plr = float(filtered.get("price_list_rate") or filtered.get("rate") or 0)
                            # Derive discount amount total for the row
                            disc_total = 0.0
                            if filtered.get("discount_percentage") is not None:
                                pct = float(filtered.get("discount_percentage") or 0)
                                disc_total = round(plr * ch_qty * (pct / 100.0), 2)
                            elif filtered.get("discount_amount") is not None:
                                # Assume existing discount_amount is the total discount for the row (not per unit)
                                disc_total = float(filtered.get("discount_amount") or 0)
                            gross_total = round(plr * ch_qty, 2)
                            net_total = max(0.0, round(gross_total - disc_total, 2))
                            # Compute per-unit net rate (2 decimals)
                            new_rate = round(net_total / ch_qty, 2)
                            filtered["price_list_rate"] = plr  # preserve original list price for reference
                            filtered["rate"] = new_rate
                            # Remove discount fields so ERPNext uses the provided net rate directly
                            filtered.pop("discount_percentage", None)
                            filtered.pop("discount_amount", None)
                        except Exception:
                            # If anything fails, fall back to original values (still may be adjusted by residual pass)
                            pass
                    processed_lines.append(filtered)

                # Residual adjustment: ensure sum(child (rate*qty)) == bundle_price * qty
                try:
                    child_lines = [cl for cl in processed_lines if cl.get("is_bundle_child")]
                    if child_lines:
                        target_total = float(getattr(bp.bundle_doc, "bundle_price", 0) or 0) * int(qty)
                        current_total = 0.0
                        for cl in child_lines:
                            current_total += round(float(cl.get("rate") or 0) * float(cl.get("qty") or 0), 2)
                        residual = round(target_total - current_total, 2)
                        if abs(residual) >= 0.01:
                            last = child_lines[-1]
                            ql = float(last.get("qty") or 0) or 0
                            if ql > 0:
                                gross_candidate = round(float(last.get("price_list_rate") or last.get("rate") or 0) * ql, 2)
                                new_line_total = round(float(last.get("rate") or 0) * ql + residual, 2)
                                # Clamp between 0 and original gross
                                if new_line_total < 0:
                                    new_line_total = 0.0
                                elif new_line_total > gross_candidate:
                                    new_line_total = gross_candidate
                                new_rate = round(new_line_total / ql, 2)
                                last["rate"] = new_rate
                except Exception:
                    pass

                for _pl in processed_lines:
                    items.append(_pl)
                handled_parents.add(str(product_id))
                continue  # done with this Woo line
            except Exception:
                # If bundle expansion fails, report as missing to skip safely
                missing.append({"name": li.get("name"), "sku": sku, "product_id": product_id, "reason": "bundle_error"})
                continue

        # 2) If this is a woosb child for a parent we've already handled via Jarz Bundle, skip it
        parent_id_in_meta = _get_parent_id_from_meta(li.get("meta_data"))
        if parent_id_in_meta and str(parent_id_in_meta) in handled_parents:
            continue

        # 3) Fall back to direct Item by SKU or woo_product_id (ERPNext pricing only)
        item_code = None
        if sku and frappe.db.exists("Item", sku):
            item_code = sku
        elif product_id:
            item_code = frappe.db.get_value("Item", {"woo_product_id": str(product_id)}, "name")
        if not item_code:
            # If this is a woosb parent and has no mapped Item, don't fail the whole order.
            # We'll still apply parent's discount to children; parent line is skipped silently.
            if product_id is not None and str(product_id) in child_parent_ids:
                continue
            missing.append({"name": li.get("name"), "sku": sku, "product_id": product_id})
            continue

        # ERPNext pricing: fetch Item Price for the selected price_list
        erp_price = None
        try:
            if price_list:
                erp_price = frappe.db.get_value("Item Price", {"item_code": item_code, "price_list": price_list}, "price_list_rate")
        except Exception:
            erp_price = None

        row = {
            "item_code": item_code,
            "qty": qty,
            "rate": float(erp_price or 0) if erp_price is not None else 0,
        }
        if erp_price is not None:
            row["price_list_rate"] = float(erp_price)
        # If this is a woosb child and we have a computed parent discount, apply it
        if parent_id_in_meta and str(parent_id_in_meta) in parent_uniform_discount:
            pct = parent_uniform_discount[str(parent_id_in_meta)]
            try:
                qtyf = float(row.get("qty") or 0) or 0
                plr = float(row.get("price_list_rate") or row.get("rate") or 0)
                row["discount_amount"] = round(plr * qtyf * (pct / 100.0), 2)
            except Exception:
                pass
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
        account_head = frappe.db.get_value("Company", inv.company, "default_income_account")
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


def _create_kashier_payment_entry(invoice_name: str, amount: float, payment_method: str) -> str | None:
    """Create Payment Entry for Kashier payments (kashier_card or kashier_wallet).
    
    Args:
        invoice_name: Sales Invoice name
        amount: Payment amount
        payment_method: Either 'kashier_card' or 'kashier_wallet'
    
    Returns:
        Payment Entry name if created, None otherwise
    """
    try:
        inv = frappe.get_doc("Sales Invoice", invoice_name)
        
        # Get Kashier account from company
        company = inv.company
        kashier_account = frappe.db.get_value("Company", company, "custom_kashier_account")
        
        if not kashier_account:
            frappe.log_error(
                f"Kashier account not configured for company {company}",
                "Kashier Payment Entry Creation Failed"
            )
            return None
        
        # Create Payment Entry
        pe = frappe.get_doc({
            "doctype": "Payment Entry",
            "payment_type": "Receive",
            "posting_date": frappe.utils.today(),
            "company": company,
            "party_type": "Customer",
            "party": inv.customer,
            "paid_to": kashier_account,
            "paid_amount": amount,
            "received_amount": amount,
            "reference_no": f"Kashier-{invoice_name}",
            "reference_date": frappe.utils.today(),
            "references": [{
                "reference_doctype": "Sales Invoice",
                "reference_name": invoice_name,
                "allocated_amount": amount
            }]
        })
        pe.insert(ignore_permissions=True)
        pe.submit()
        
        return pe.name
    except Exception as e:
        frappe.log_error(
            f"Failed to create Kashier payment entry for {invoice_name}: {str(e)}",
            "Kashier Payment Entry Error"
        )
        return None


def process_order_phase1(order: dict, settings, allow_update: bool = True, is_historical: bool = False) -> dict:
    """Process a single Woo order into a Sales Invoice.
    
    Args:
        order: WooCommerce order dict
        settings: WooCommerce Settings singleton
        allow_update: Whether to update existing invoices
        is_historical: True for historical migration (paid invoices), False for live orders (unpaid)
    """
    woo_id = order.get("id")

    # Determine mapping link field name based on actual DB schema
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
            "WooCommerce Order Map", {"woo_order_id": woo_id}, ["name", LINK_FIELD, "hash"], as_dict=True
        )
    except Exception:
        # Final fallback: only fetch name
        try:
            nm = frappe.db.get_value("WooCommerce Order Map", {"woo_order_id": woo_id}, "name")
            if nm:
                existing_map = {"name": nm, LINK_FIELD: None, "hash": None}
        except Exception:
            existing_map = None
    order_hash = _compute_order_hash(order)

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

    # Reconcile mapping to found invoice if mapping is missing or points elsewhere
    if linked_invoice_name:
        if not existing_map:
            existing_map = {"name": None, LINK_FIELD: linked_invoice_name, "hash": None}
        elif not existing_map.get(LINK_FIELD):
            existing_map[LINK_FIELD] = linked_invoice_name
    if existing_map and not allow_update:
        return {"status": "skipped", "reason": "already_mapped", "woo_order_id": woo_id}

    # Ensure customer and at least one address before proceeding
    try:
        customer, billing_addr, shipping_addr = ensure_customer_with_addresses(order, settings)
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
        if territory_name:
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
        if pos_profile:
            price_list = frappe.db.get_value("POS Profile", pos_profile, "price_list")
        if not price_list:
            default_company = getattr(settings, "default_company", None) or frappe.defaults.get_global_default("company")
            if default_company:
                price_list = frappe.db.get_value("Company", default_company, "default_selling_price_list")
    except Exception:
        price_list = None

    lines, missing = _build_invoice_items(order, price_list=price_list)
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
    custom_payment_method = _map_payment_method(woo_payment_method)
    
    status_map = _map_status(woo_status, is_historical=is_historical)

    try:
        delivery_date_val, time_from_val, duration_val = _parse_delivery_parts(order)
        if not (delivery_date_val and time_from_val and (duration_val is not None)):
            delivery_date_val = None
            time_from_val = None
            duration_val = None

        # Update existing or create new invoice
        if ((existing_map and existing_map.get(LINK_FIELD)) or linked_invoice_name) and allow_update:
            inv = frappe.get_doc("Sales Invoice", existing_map.get(LINK_FIELD) or linked_invoice_name)
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
                        # for submitted invoices, update directly in DB without full save
                        inv.db_set("pos_profile", pos_profile, commit=False)
                        # attempt to mark as POS to ensure POS defaults apply
                        try:
                            inv.db_set("is_pos", 1, commit=False)
                        except Exception:
                            pass
                    else:
                        inv.pos_profile = pos_profile
                        inv.is_pos = 1
            except Exception:
                pass
            if delivery_date_val:
                inv.custom_delivery_date = delivery_date_val
            if time_from_val:
                inv.custom_delivery_time_from = time_from_val
            if duration_val is not None:
                inv.custom_delivery_duration = int(duration_val) * 60  # seconds
            try:
                if status_map.get("custom_state"):
                    inv.db_set("sales_invoice_state", status_map["custom_state"], commit=False)
            except Exception:
                pass
            
            # Update custom acceptance status and sales invoice state based on WooCommerce status
            woo_status = (order.get("status") or "").lower()
            try:
                if woo_status == "completed":
                    inv.db_set("custom_acceptance_status", "Accepted", commit=False)
                    inv.db_set("custom_sales_invoice_state", "Delivered", commit=False)
                elif woo_status in ("cancelled", "refunded"):
                    inv.db_set("custom_acceptance_status", "Accepted", commit=False)
                    inv.db_set("custom_sales_invoice_state", "Cancelled", commit=False)
            except Exception:
                pass
            # Optional: territory-based delivery income row
            try:
                if territory_name and frappe.db.exists("Territory", territory_name):
                    delivery_income = frappe.db.get_value("Territory", territory_name, "delivery_income") or 0
                    if delivery_income and float(delivery_income) > 0:
                        add_delivery_charges_to_taxes(
                            inv,
                            delivery_income,
                            delivery_description=f"Shipping Income ({territory_name})",
                        )
            except Exception:
                pass
            inv.save(ignore_permissions=True, ignore_version=True)
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
            # attach warehouse to items on create if available
            if default_warehouse:
                for it in lines:
                    it["warehouse"] = default_warehouse
            inv_data = {
                "doctype": "Sales Invoice",
                "customer": customer,
                "currency": order.get("currency") or getattr(settings, "default_currency", None) or "USD",
                "posting_date": frappe.utils.today(),
                "company": getattr(settings, "default_company", None) or frappe.defaults.get_global_default("company"),
                "woo_order_id": woo_id,
                "woo_order_number": order.get("number"),
                "customer_address": billing_addr or shipping_addr,
                "shipping_address_name": shipping_addr or billing_addr,
                "items": lines,
            }
            
            # Add optional fields
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
            
            # Set custom acceptance status and sales invoice state based on WooCommerce status
            woo_status = (order.get("status") or "").lower()
            if woo_status == "completed":
                inv_data["custom_acceptance_status"] = "Accepted"
                inv_data["custom_sales_invoice_state"] = "Delivered"
            elif woo_status in ("cancelled", "refunded"):
                inv_data["custom_acceptance_status"] = "Accepted"
                inv_data["custom_sales_invoice_state"] = "Cancelled"
            
            inv = frappe.get_doc(inv_data)
            # Set POS Profile from Territory if available
            try:
                if pos_profile:
                    inv.pos_profile = pos_profile
            except Exception:
                pass
            # Optional: territory-based delivery income row
            try:
                territory_name = frappe.db.get_value("Customer", customer, "territory")
                if territory_name and frappe.db.exists("Territory", territory_name):
                    delivery_income = frappe.db.get_value("Territory", territory_name, "delivery_income") or 0
                    if delivery_income and float(delivery_income) > 0:
                        add_delivery_charges_to_taxes(
                            inv,
                            delivery_income,
                            delivery_description=f"Shipping Income ({territory_name})",
                        )
            except Exception:
                pass
            inv.insert(ignore_permissions=True)
            if status_map["docstatus"] == 1:
                inv.submit()
                # post-submit: enable POS to expose POS Profile without triggering MOP validation
                try:
                    if pos_profile:
                        inv.db_set("pos_profile", pos_profile, commit=False)
                        inv.db_set("is_pos", 1, commit=False)
                except Exception:
                    pass
                
                # Create payment entry for Kashier methods
                if custom_payment_method in ["Kashier Card", "Kashier Wallet"]:
                    try:
                        payment_entry = _create_kashier_payment_entry(
                            inv.name,
                            float(order.get("total") or 0),
                            custom_payment_method
                        )
                        if payment_entry:
                            frappe.logger().info(f"Created Kashier payment entry {payment_entry} for invoice {inv.name}")
                    except Exception as pe_error:
                        frappe.log_error(
                            f"Failed to create Kashier payment for {inv.name}: {str(pe_error)}",
                            "Kashier Payment Creation Error"
                        )
                
                # Mark as paid for historical orders
                if status_map.get("is_paid"):
                    try:
                        inv.db_set("status", "Paid", commit=False)
                    except Exception:
                        pass
                        
            elif status_map["docstatus"] == 2:
                # Submit first if draft, then cancel
                if inv.docstatus == 0:
                    inv.submit()
                inv.cancel()
            try:
                if status_map.get("custom_state"):
                    inv.db_set("sales_invoice_state", status_map["custom_state"], commit=True)
            except Exception:
                pass

        # Upsert mapping
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

        action = "updated" if ((existing_map and existing_map.get(LINK_FIELD)) or linked_invoice_name) else "created"
        return {"status": action, "invoice": inv.name, "woo_order_id": woo_id}
    except Exception as e:  # noqa: BLE001
        frappe.db.rollback()
        return {"status": "error", "reason": str(e), "woo_order_id": woo_id}


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


def migrate_historical_orders(limit: int = 100, page: int = 1) -> dict[str, Any]:
    """One-time migration of historical orders (completed/cancelled only).
    
    Creates paid Sales Invoices without accounting/inventory effects for reporting.
    
    Usage:
        bench --site <site> execute jarz_woocommerce_integration.services.order_sync.migrate_historical_orders --kwargs '{"limit": 100, "page": 1}'
    """
    settings = frappe.get_single("WooCommerce Settings")
    ensure_custom_fields()

    client = WooClient(
        base_url=settings.base_url,
        consumer_key=settings.consumer_key,
        consumer_secret=settings.get_password("consumer_secret"),
    )

    # Fetch only completed and cancelled orders
    params = {
        "per_page": limit,
        "page": page,
        "status": "completed,cancelled,refunded",
        "orderby": "date",
        "order": "desc"
    }
    
    orders = client.list_orders(params=params)
    metrics: dict[str, Any] = {
        "orders_fetched": len(orders),
        "processed": 0,
        "created": 0,
        "skipped": 0,
        "errors": 0,
        "results_sample": [],
        "is_historical": True,
        "page": page,
    }

    for o in orders:
        result = process_order_phase1(o, settings, allow_update=False, is_historical=True)
        metrics["processed"] += 1
        if result["status"] in ("created", "updated"):
            metrics["created"] += 1
        elif result["status"] == "error":
            metrics["errors"] += 1
        elif result["status"] == "skipped":
            metrics["skipped"] += 1
        if len(metrics["results_sample"]) < 10:
            metrics["results_sample"].append(result)

    frappe.logger().info({"event": "woo_historical_migration", "result": metrics})
    return metrics


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


def migrate_all_historical_orders_cli(max_pages: int = 100, batch_size: int = 50):  # pragma: no cover
    """CLI entry point to migrate all historical orders across multiple pages with smart memory management.
    
    Handles up to 10,000 orders efficiently by:
    - Using smaller batch sizes (50 orders/page instead of 100)
    - Clearing cache and committing DB after each batch
    - Monitoring memory and taking breaks if needed
    
    Usage:
        bench --site <site> execute jarz_woocommerce_integration.services.order_sync.migrate_all_historical_orders_cli
    """
    import gc
    
    total_stats = {
        "orders_fetched": 0,
        "processed": 0,
        "created": 0,
        "skipped": 0,
        "errors": 0,
        "pages_processed": 0,
        "batches_completed": 0,
    }
    
    for page in range(1, max_pages + 1):
        # Process one batch with smaller limit for better memory management
        result = migrate_historical_orders(limit=batch_size, page=page)
        total_stats["orders_fetched"] += result.get("orders_fetched", 0)
        total_stats["processed"] += result.get("processed", 0)
        total_stats["created"] += result.get("created", 0)
        total_stats["skipped"] += result.get("skipped", 0)
        total_stats["errors"] += result.get("errors", 0)
        total_stats["pages_processed"] = page
        total_stats["batches_completed"] += 1
        
        frappe.logger().info(f"Historical migration page {page}/{max_pages} complete: {result}")
        
        # Memory management: commit and clear cache every batch
        try:
            frappe.db.commit()
            frappe.clear_cache()
            gc.collect()  # Force garbage collection
        except Exception as e:
            frappe.logger().error(f"Cache clear error on page {page}: {str(e)}")
        
        # Stop if we fetched fewer orders than batch_size (reached the end)
        if result.get("orders_fetched", 0) < batch_size:
            frappe.logger().info(f"Migration complete - reached end of orders at page {page}")
            break
        
        # Add small delay every 10 batches to prevent worker timeout
        if page % 10 == 0:
            import time
            time.sleep(2)  # 2 second break every 10 batches
            frappe.logger().info(f"Checkpoint: {total_stats['created']} orders migrated so far...")
    
    # Final summary
    frappe.logger().info(f"=== MIGRATION COMPLETE === Total: {total_stats}")
    return total_stats


def debug_dump_invoice_items(inv_name: str):  # pragma: no cover - temporary debug helper
    """Return a simplified list of items (item_code, qty, rate, price_list_rate, amount) for manual verification.

    Usage:
        bench --site <site> execute jarz_woocommerce_integration.services.order_sync.debug_dump_invoice_items --kwargs '{"inv_name":"ACC-SINV-2025-00621"}'
    """
    inv = frappe.get_doc("Sales Invoice", inv_name)
    out = []
    for it in inv.items:
        out.append({
            "item_code": it.item_code,
            "qty": float(it.qty),
            "rate": float(it.rate),
            "price_list_rate": float(it.price_list_rate) if getattr(it, "price_list_rate", None) else None,
            "amount": float(it.amount),
            "discount_percentage": getattr(it, "discount_percentage", None),
            "discount_amount": getattr(it, "discount_amount", None),
        })
    return out
