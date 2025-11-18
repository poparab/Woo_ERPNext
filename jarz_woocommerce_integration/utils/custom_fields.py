from __future__ import annotations

import frappe


REQUIRED_FIELDS = [
    # Sales Invoice fields
    {
        "dt": "Sales Invoice",
        "fieldname": "woo_order_id",
        "fieldtype": "Int",
        "label": "Woo Order ID",
        "insert_after": "title",
    },
    {
        "dt": "Sales Invoice",
        "fieldname": "woo_order_number",
        "fieldtype": "Data",
        "label": "Woo Order Number",
        "insert_after": "woo_order_id",
    },
    {
        "dt": "Sales Invoice",
        "fieldname": "sales_invoice_state",
        "fieldtype": "Data",
        "label": "Sales Invoice State",
        "insert_after": "status",
    },
    {
        "dt": "Sales Invoice",
        "fieldname": "woo_outbound_status",
        "fieldtype": "Data",
        "label": "Woo Outbound Status",
        "insert_after": "sales_invoice_state",
    },
    {
        "dt": "Sales Invoice",
        "fieldname": "woo_outbound_error",
        "fieldtype": "Small Text",
        "label": "Woo Outbound Error",
        "insert_after": "woo_outbound_status",
    },
    {
        "dt": "Sales Invoice",
        "fieldname": "woo_outbound_synced_on",
        "fieldtype": "Datetime",
        "label": "Woo Outbound Synced On",
        "insert_after": "woo_outbound_error",
    },
    # Item field mapping to Woo product (id or sku mapping reference)
    {
        "dt": "Item",
        "fieldname": "woo_product_id",
        "fieldtype": "Data",
        "label": "Woo Product ID",
        "insert_after": "item_name",
    },
    {
        "dt": "Customer",
        "fieldname": "woo_customer_id",
        "fieldtype": "Data",
        "label": "Woo Customer ID",
        "insert_after": "customer_group",
    },
    {
        "dt": "Customer",
        "fieldname": "woo_outbound_status",
        "fieldtype": "Data",
        "label": "Woo Outbound Status",
        "insert_after": "woo_customer_id",
    },
    {
        "dt": "Customer",
        "fieldname": "woo_outbound_error",
        "fieldtype": "Small Text",
        "label": "Woo Outbound Error",
        "insert_after": "woo_outbound_status",
    },
    {
        "dt": "Customer",
        "fieldname": "woo_outbound_synced_on",
        "fieldtype": "Datetime",
        "label": "Woo Outbound Synced On",
        "insert_after": "woo_outbound_error",
    },
    # Jarz Bundle mapping to Woo bundle id
    {
        "dt": "Jarz Bundle",
        "fieldname": "woo_bundle_id",
        "fieldtype": "Data",
        "label": "Woo Bundle ID",
        "insert_after": "bundle_price",
    },
    # Territory -> POS Profile mapping (used to set Sales Invoice.pos_profile)
    {
        "dt": "Territory",
        "fieldname": "pos_profile",
        "fieldtype": "Link",
        "options": "POS Profile",
        "label": "POS Profile",
        "insert_after": "territory_manager",
    },
    # Territory -> Delivery Income (used to add an 'Actual' shipping income row)
    {
        "dt": "Territory",
        "fieldname": "delivery_income",
        "fieldtype": "Currency",
        "label": "Delivery Income",
        "insert_after": "pos_profile",
    },
]


def ensure_custom_fields():  # pragma: no cover - install / migration helper
    """Create required custom fields if they don't exist."""
    # Remove legacy duplicate Item custom field if both exist (e.g., 'Woo Product ID' vs 'woo_product_id')
    try:
        # Legacy variants by label or alternate fieldname patterns
        legacy_label_variants = [
            "Woo Product Id",  # capitalization variant
            "Woo Product ID",  # duplicate label variant (kept but if double we prefer primary fieldname)
            "woocommerce_product_id",
            "woo_product_id"  # lowercase label variant (if mis-entered as label)
        ]
        legacy_fieldname_variants = [
            "custom_woo_product_id",
            "woocommerce_product_id",
        ]
        primary_cf_name = "Item-woo_product_id"
        if frappe.db.exists("Custom Field", primary_cf_name):
            # Delete by label
            for legacy_label in legacy_label_variants:
                duplicates = frappe.get_all(
                    "Custom Field",
                    filters={"dt": "Item", "label": legacy_label},
                    pluck="name",
                )
                for dup in duplicates:
                    if dup != primary_cf_name:
                        frappe.delete_doc("Custom Field", dup, ignore_permissions=True)
            # Delete by fieldname
            for legacy_fn in legacy_fieldname_variants:
                if legacy_fn != "woo_product_id":
                    cf_name = f"Item-{legacy_fn}"
                    if frappe.db.exists("Custom Field", cf_name):
                        frappe.delete_doc("Custom Field", cf_name, ignore_permissions=True)
    except Exception:
        frappe.log_error(frappe.get_traceback(), "Woo Integration: cleanup duplicate item custom fields")

    for spec in REQUIRED_FIELDS:
        dt = spec["dt"]
        fn = spec["fieldname"]
        # Skip if already present in DocType meta (standard or previously added custom field)
        try:
            meta = frappe.get_meta(dt)
            if meta and meta.get_field(fn):
                continue
        except Exception:
            pass
        # Skip if any Custom Field exists with same dt+fieldname (regardless of name)
        try:
            exists_same_fn = frappe.get_all("Custom Field", filters={"dt": dt, "fieldname": fn}, limit=1)
            if exists_same_fn:
                continue
        except Exception:
            pass
        # Skip if the conventional name exists
        if frappe.db.exists("Custom Field", f"{dt}-{fn}"):
            continue
        cf = frappe.get_doc({
            "doctype": "Custom Field",
            "dt": dt,
            "fieldname": fn,
            "fieldtype": spec["fieldtype"],
            "label": spec["label"],
            "insert_after": spec.get("insert_after"),
            "options": spec.get("options"),
            "read_only": 0,
            "no_copy": 1,
        })
        cf.insert(ignore_permissions=True)
    frappe.clear_cache()
