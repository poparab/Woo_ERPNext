import frappe


@frappe.whitelist(allow_guest=False)
def list_integration_custom_fields():
    """Return custom fields for relevant doctypes (used to verify fixture export filters)."""
    doctypes = [
        "Territory",
        "Sales Invoice",
        "WooCommerce Settings",
        "Customer",
        "Address",
    ]
    rows = frappe.get_all(
        "Custom Field",
        filters={"dt": ["in", doctypes]},
        fields=["name", "dt", "fieldname", "fieldtype", "insert_after"],
        order_by="dt, fieldname",
    )
    return rows
