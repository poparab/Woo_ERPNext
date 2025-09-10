import frappe


def execute():
    # Ensure custom field woo_code exists on Territory
    if not frappe.db.exists("Custom Field", {"dt": "Territory", "fieldname": "woo_code"}):
        cf = frappe.get_doc({
            "doctype": "Custom Field",
            "dt": "Territory",
            "fieldname": "woo_code",
            "label": "Woo Code",
            "fieldtype": "Data",
            "insert_after": "territory_name",
            "read_only": 1,
            "in_list_view": 0,
            "in_standard_filter": 0,
            "in_global_search": 0,
            "no_copy": 1,
        })
        cf.insert(ignore_permissions=True)

    # Populate missing values: set woo_code = name for non-group child territories under Egypt if blank
    terrs = frappe.get_all(
        "Territory",
        filters={"is_group": 0},
        fields=["name", "parent_territory", "woo_code"],
    )
    updated = 0
    for t in terrs:
        if not t.get("woo_code"):
            frappe.db.set_value("Territory", t["name"], "woo_code", t["name"], update_modified=False)
            updated += 1
    if updated:
        frappe.db.commit()
