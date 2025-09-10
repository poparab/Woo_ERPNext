import frappe


def execute():
    """Ensure Custom Field custom_woo_code exists on Territory and populate values.

    Mirrors earlier woo_code logic but with corrected fieldname custom_woo_code.
    """
    fieldname = "custom_woo_code"
    if not frappe.db.exists("Custom Field", {"dt": "Territory", "fieldname": fieldname}):
        cf = frappe.get_doc({
            "doctype": "Custom Field",
            "dt": "Territory",
            "fieldname": fieldname,
            "label": "Woo Code",
            "fieldtype": "Data",
            "insert_after": "territory_name",
            "read_only": 1,
            "no_copy": 1,
        })
        cf.insert(ignore_permissions=True)
        frappe.db.commit()

    # Column should exist now; populate blank entries
    terrs = frappe.get_all(
        "Territory",
        filters={"is_group": 0},
        fields=["name", fieldname],
    )
    updated = 0
    for t in terrs:
        if not t.get(fieldname):
            frappe.db.set_value("Territory", t["name"], fieldname, t["name"], update_modified=False)
            updated += 1
    if updated:
        frappe.db.commit()