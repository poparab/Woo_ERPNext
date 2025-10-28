"""Bundle processing helpers local to the WooCommerce integration.

This module mirrors the pricing logic previously provided by the jarz_pos app so the
integration can operate without that dependency. The implementation is intentionally
self-contained and interacts only with DocTypes defined in this app or core ERPNext.
"""

from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import cint, flt


BUNDLE_LOGGER = "jarz_woocommerce.bundle"
BUNDLE_DOCTYPE = "woo_jarz_bundle"
BUNDLE_ITEM_GROUP_DOCTYPE = "woo_jarz_bundle_item_group"


class BundleProcessor:
    """Expand a woo_jarz_bundle record into ERPNext invoice line items with correct pricing."""

    def __init__(self, bundle_code: str, quantity: int = 1, selected_items: dict | None = None):
        self.bundle_code = bundle_code
        self.quantity = quantity
        self.bundle_doc = None
        self.parent_item = None
        self.bundle_items: list[dict] = []
        # Expected shape: { group_name: [ {id/item_code/name, qty, rate?}, ... ] }
        self.selected_items = selected_items or {}

    # ---------------------------------------------------------------------
    # Bundle loading helpers
    # ---------------------------------------------------------------------
    def load_bundle(self):
        """Fetch the woo_jarz_bundle document and prepare child item data."""
        try:
            self.bundle_doc = frappe.get_doc(BUNDLE_DOCTYPE, self.bundle_code)
        except Exception as exc:  # pragma: no cover - frappe throws richly
            frappe.log_error(str(exc), f"Bundle load failed: {self.bundle_code}")
            raise

        if not self.bundle_doc:
            frappe.throw(_("Bundle {0} not found").format(self.bundle_code))

        if not self.bundle_doc.erpnext_item:
            frappe.throw(_("Bundle {0} has no ERPNext item configured").format(self.bundle_code))

        self.parent_item = frappe.get_doc("Item", self.bundle_doc.erpnext_item)

        for row in self.bundle_doc.items:
            item_group = row.item_group
            required_qty = cint(row.quantity)

            candidate_items = frappe.get_all(
                "Item",
                filters={
                    "item_group": item_group,
                    "disabled": 0,
                    "has_variants": 0,
                },
                fields=["name", "item_name", "standard_rate", "stock_uom"],
            )

            if not candidate_items:
                frappe.throw(_(f"No available items found in item group '{item_group}'"))

            allowed_codes = {row["name"] for row in candidate_items}

            selections = self._aggregate_selected_items(item_group, required_qty)
            if selections:
                invalid = [code for code in selections if code not in allowed_codes]
                if invalid:
                    frappe.throw(
                        _(
                            "Bundle {0}: invalid selections for group '{1}': {2}"
                        ).format(self.bundle_code, item_group, ", ".join(invalid))
                    )

                frappe.logger(BUNDLE_LOGGER).info(
                    {
                        "event": "bundle_selections_applied",
                        "bundle": self.bundle_code,
                        "group": item_group,
                        "required": required_qty,
                        "selections": {code: data["qty"] for code, data in selections.items()},
                    }
                )

                for item_code, data in selections.items():
                    item_doc = frappe.get_doc("Item", item_code)
                    fallback_rate = self.get_item_rate(item_doc.name)
                    rate = flt(data.get("rate") or fallback_rate)
                    self.bundle_items.append(
                        {
                            "item": item_doc,
                            "qty": data["qty"],
                            "rate": rate,
                            "item_group": item_group,
                        }
                    )
                continue

            # Default behaviour: use first available item in the group
            default_item = candidate_items[0]
            item_doc = frappe.get_doc("Item", default_item["name"])
            self.bundle_items.append(
                {
                    "item": item_doc,
                    "qty": required_qty,
                    "rate": self.get_item_rate(item_doc.name),
                    "item_group": item_group,
                }
            )

        frappe.logger(BUNDLE_LOGGER).info(
            {
                "event": "bundle_loaded",
                "bundle": self.bundle_code,
                "parent_item": self.parent_item.name,
                "child_count": len(self.bundle_items),
            }
        )

    def _aggregate_selected_items(self, item_group_name: str, required_quantity: int) -> dict:
        if not self.selected_items:
            return {}

        matched_key = None
        for key in self.selected_items:
            if key == item_group_name:
                matched_key = key
                break
            if isinstance(key, str) and key.lower() == item_group_name.lower():
                matched_key = key
                break

        if matched_key is None:
            return {}

        entries = self.selected_items.get(matched_key) or []
        if not entries:
            frappe.throw(
                _(
                    "Bundle {0}: no items supplied for required group '{1}'"
                ).format(self.bundle_code, item_group_name)
            )

        aggregated: dict[str, dict[str, float]] = {}
        total_selected = 0

        for entry in entries:
            if not isinstance(entry, dict):
                continue
            data = dict(entry)
            item_code = data.get("id") or data.get("item_code") or data.get("name")
            if not item_code:
                continue

            aggregated.setdefault(item_code, {"qty": 0, "rate": None})

            qty_increment = 1
            for key in (
                "selected_quantity",
                "selection_quantity",
                "selected_qty",
                "selected_count",
                "count",
            ):
                value = data.get(key)
                if value in (None, ""):
                    continue
                try:
                    candidate = cint(value)
                    if candidate > 0:
                        qty_increment = candidate
                        break
                except Exception:
                    continue

            aggregated[item_code]["qty"] += qty_increment
            total_selected += qty_increment

            price_value = data.get("price")
            if price_value is None:
                price_value = data.get("rate")
            if price_value is not None:
                aggregated[item_code]["rate"] = flt(price_value)

        if not aggregated:
            frappe.throw(
                _(
                    "Bundle {0}: no valid items supplied for group '{1}'"
                ).format(self.bundle_code, item_group_name)
            )

        if required_quantity:
            required_quantity = cint(required_quantity)
            if total_selected != required_quantity:
                frappe.logger(BUNDLE_LOGGER).warning(
                    {
                        "event": "bundle_selection_mismatch",
                        "bundle": self.bundle_code,
                        "group": item_group_name,
                        "required": required_quantity,
                        "received": total_selected,
                        "entries": {code: data["qty"] for code, data in aggregated.items()},
                        "raw_count": len(entries),
                    }
                )
                frappe.throw(
                    _(
                        "Bundle {0}: expected {1} selection(s) from '{2}', received {3}"
                    ).format(
                        self.bundle_code, required_quantity, item_group_name, total_selected
                    )
                )

        return aggregated

    def get_item_rate(self, item_code: str) -> float:
        try:
            item_doc = frappe.get_doc("Item", item_code)
            if item_doc.standard_rate and flt(item_doc.standard_rate) > 0:
                return flt(item_doc.standard_rate)

            price_list_entry = frappe.get_all(
                "Item Price",
                filters={
                    "item_code": item_code,
                    "selling": 1,
                    "price_list_rate": [">", 0],
                },
                fields=["price_list_rate"],
                order_by="creation desc",
                limit=1,
            )
            if price_list_entry:
                return flt(price_list_entry[0].price_list_rate)

            if item_doc.valuation_rate and flt(item_doc.valuation_rate) > 0:
                return flt(item_doc.valuation_rate)

            frappe.log_error(
                f"No rate found for item: {item_code}, setting default rate of 100",
                "Bundle Processing",
            )
            return 100.0
        except Exception as exc:
            frappe.log_error(
                f"Error getting rate for {item_code}: {str(exc)}",
                "Bundle Processing",
            )
            return 100.0

    # ---------------------------------------------------------------------
    # Pricing helpers
    # ---------------------------------------------------------------------
    def calculate_child_discount_percentage(self) -> tuple[float, float, float]:
        bundle_price = flt(self.bundle_doc.bundle_price)
        if bundle_price <= 0:
            frappe.throw(_(f"Bundle {self.bundle_code} price is not set or invalid"))

        total_child_price = sum(
            flt(item["rate"]) * flt(item["qty"]) * self.quantity for item in self.bundle_items
        )
        if total_child_price <= 0:
            frappe.throw(_(f"Bundle {self.bundle_code} has zero total child price"))

        if bundle_price > total_child_price + 1e-9:
            frappe.logger(BUNDLE_LOGGER).warning(
                {
                    "event": "bundle_price_exceeds_children",
                    "bundle": self.bundle_code,
                    "bundle_price": bundle_price,
                    "child_total": total_child_price,
                    "note": "Bundle price higher than selected items - children discount set to 0",
                }
            )
            return 0.0, total_child_price, bundle_price

        discount_percentage = ((total_child_price - bundle_price) / total_child_price) * 100.0
        discount_percentage = max(0.0, min(100.0, discount_percentage))
        return discount_percentage, total_child_price, bundle_price

    def get_invoice_items(self) -> list[dict]:
        if not self.bundle_doc:
            self.load_bundle()

        rate_precision = frappe.get_precision("Sales Invoice Item", "rate") or 2
        amount_precision = frappe.get_precision("Sales Invoice Item", "amount") or 2

        parent_rate = self.get_item_rate(self.parent_item.name)
        parent_line = {
            "item_code": self.parent_item.name,
            "item_name": self.parent_item.item_name,
            "description": f"Bundle: {self.parent_item.description or self.parent_item.item_name}",
            "qty": self.quantity,
            "rate": flt(parent_rate, rate_precision),
            "price_list_rate": flt(parent_rate, rate_precision),
            "discount_percentage": 100.0,
            "is_bundle_parent": 1,
            "bundle_code": self.bundle_code,
        }

        invoice_items = [parent_line]

        uniform_pct, _child_gross, bundle_price = self.calculate_child_discount_percentage()

        child_lines = []
        running_total = 0.0
        for item in self.bundle_items:
            unit_rate = flt(item["rate"], rate_precision)
            qty_total = flt(item["qty"]) * self.quantity
            expected_rate = unit_rate * (1 - uniform_pct / 100.0)
            line_total = flt(expected_rate * qty_total, amount_precision)
            running_total += line_total

            child_lines.append(
                {
                    "item_code": item["item"].name,
                    "item_name": item["item"].item_name,
                    "description": item["item"].description or item["item"].item_name,
                    "qty": qty_total,
                    "rate": unit_rate,
                    "price_list_rate": unit_rate,
                    "discount_percentage": uniform_pct,
                    "is_bundle_child": 1,
                    "parent_bundle": self.bundle_code,
                    "_unit_rate": unit_rate,
                    "_qty_total": qty_total,
                    "_expected_line_total": line_total,
                }
            )

        target_total = flt(bundle_price, amount_precision)
        residual = flt(target_total - running_total, amount_precision)
        min_step = 1 / (10 ** amount_precision)

        if child_lines and abs(residual) >= min_step and uniform_pct > 0:
            last = child_lines[-1]
            unit_rate = last["_unit_rate"]
            qty_total = last["_qty_total"]
            expected_total = last["_expected_line_total"]

            desired_total = flt(expected_total + residual, amount_precision)
            desired_total = min(max(0.0, desired_total), unit_rate * qty_total)

            if unit_rate > 0 and qty_total > 0:
                desired_rate = desired_total / qty_total
                adjusted_pct = ((unit_rate - desired_rate) / unit_rate) * 100.0
                adjusted_pct = min(max(0.0, adjusted_pct), 100.0)
                last["discount_percentage"] = flt(adjusted_pct, 6)

        for line in child_lines:
            line.pop("_unit_rate", None)
            line.pop("_qty_total", None)
            line.pop("_expected_line_total", None)
            invoice_items.append(line)

        frappe.logger(BUNDLE_LOGGER).info(
            {
                "event": "bundle_expanded",
                "bundle": self.bundle_code,
                "parent_discount_pct": 100.0,
                "children_discount_pct": uniform_pct,
                "quantity": self.quantity,
                "target_total": bundle_price,
                "residual": residual,
            }
        )
        return invoice_items


def process_bundle_for_invoice(bundle_identifier: str, quantity: int = 1, selected_items: dict | None = None) -> list[dict]:
    frappe.logger(BUNDLE_LOGGER).info(
        {
            "event": "process_bundle_for_invoice",
            "identifier": bundle_identifier,
            "quantity": quantity,
        }
    )

    bundle_code = None
    bundle_doc = None

    bundle_records = frappe.get_all(
        BUNDLE_DOCTYPE,
        filters={"erpnext_item": bundle_identifier},
        fields=["name", "bundle_name", "erpnext_item", "bundle_price"],
        limit=1,
    )

    if bundle_records:
        bundle_code = bundle_records[0]["name"]
    elif frappe.db.exists(BUNDLE_DOCTYPE, bundle_identifier):
        bundle_code = bundle_identifier
        bundle_doc = frappe.get_doc(BUNDLE_DOCTYPE, bundle_code)
    else:
        frappe.throw(
            _(
                f"No {BUNDLE_DOCTYPE} found for identifier '{bundle_identifier}'. Checked erpnext_item and bundle ID."
            )
        )

    processor = BundleProcessor(bundle_code, quantity, selected_items=selected_items)
    if bundle_doc:
        processor.bundle_doc = bundle_doc
    result = processor.get_invoice_items()

    frappe.logger(BUNDLE_LOGGER).info(
        {
            "event": "process_bundle_complete",
            "bundle": bundle_code,
            "line_count": len(result),
        }
    )
    return result


def validate_bundle_configuration(bundle_code: str) -> tuple[bool, str]:
    try:
        bundle_doc = frappe.get_doc(BUNDLE_DOCTYPE, bundle_code)
    except Exception as exc:
        return False, f"Bundle validation error: {str(exc)}"

    if not bundle_doc:
        return False, f"Bundle {bundle_code} not found"
    if not bundle_doc.erpnext_item:
        return False, "Bundle has no ERPNext item configured"
    if not frappe.db.exists("Item", bundle_doc.erpnext_item):
        return False, f"ERPNext item {bundle_doc.erpnext_item} does not exist"
    if not bundle_doc.items:
        return False, "Bundle has no child items configured"

    for row in bundle_doc.items:
        item_group = row.item_group
        if not frappe.db.exists("Item Group", item_group):
            return False, f"Item group {item_group} does not exist"
        items_in_group = frappe.get_all(
            "Item",
            filters={"item_group": item_group, "disabled": 0},
            limit=1,
        )
        if not items_in_group:
            return False, f"No available items found in item group {item_group}"

    if not bundle_doc.bundle_price or flt(bundle_doc.bundle_price) <= 0:
        return False, "Bundle price not set or invalid"

    return True, "Bundle configuration is valid"


def validate_bundle_configuration_by_item(bundle_identifier: str) -> tuple[bool, str, str | None]:
    bundle_code = None

    records = frappe.get_all(
        BUNDLE_DOCTYPE,
        filters={"erpnext_item": bundle_identifier},
        fields=["name"],
        limit=1,
    )
    if records:
        bundle_code = records[0]["name"]
    elif frappe.db.exists(BUNDLE_DOCTYPE, bundle_identifier):
        bundle_code = bundle_identifier
    else:
        return False, f"No {BUNDLE_DOCTYPE} found for identifier '{bundle_identifier}'", None

    is_valid, message = validate_bundle_configuration(bundle_code)
    return is_valid, message, bundle_code


@frappe.whitelist()
def test_bundle_pricing(bundle_identifier: str, qty: int = 1) -> dict:
    processor = BundleProcessor(bundle_identifier, qty)
    processor.load_bundle()
    discount_pct, total_child_price, bundle_price = processor.calculate_child_discount_percentage()
    items = processor.get_invoice_items()
    child_discounted_sum = 0.0
    for line in items:
        if line.get("is_bundle_child"):
            original = line["rate"] * line["qty"]
            discount = original * (line.get("discount_percentage", 0) / 100.0)
            child_discounted_sum += flt(original - discount, 2)
    return {
        "bundle_identifier": bundle_identifier,
        "qty": qty,
        "discount_percentage_uniform_base": discount_pct,
        "bundle_price": bundle_price,
        "total_child_gross": total_child_price,
        "children_discounted_sum": flt(child_discounted_sum, 2),
        "difference": flt(bundle_price - child_discounted_sum, 2),
        "items_generated": items,
    }
