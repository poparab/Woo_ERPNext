import frappe

def execute():
    """Create custom field woo_code on Territory if missing."""
    if frappe.db.exists("Custom Field", {"dt": "Territory", "fieldname": "woo_code"}):
        return
    cf = frappe.get_doc({
        "doctype": "Custom Field",
        "dt": "Territory",
        "fieldname": "woo_code",
        "label": "Woo Code",
        "fieldtype": "Data",
        "insert_after": "territory_name",
        "read_only": 1,
        "unique": 0,
        "allow_in_quick_entry": 0,
        "translatable": 0,
        "no_copy": 1,
        "depends_on": None,
        "mandatory_depends_on": None,
        "permlevel": 0,
        "in_list_view": 0,
        "in_standard_filter": 0,
        "in_global_search": 0,
        "in_preview": 0,
        "in_filter": 0,
    })
    cf.insert(ignore_permissions=True)
    frappe.db.commit()
