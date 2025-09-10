import frappe


def execute():
    """Safety patch: ensure Custom Field woo_code and DB column exist and populate values.

    This handles cases where earlier patches ran while scheduler paused or migrations skipped,
    leaving the column absent.
    """
    if not frappe.db.exists("Custom Field", {"dt": "Territory", "fieldname": "woo_code"}):
        cf = frappe.get_doc({
            "doctype": "Custom Field",
            "dt": "Territory",
            "fieldname": "woo_code",
            "label": "Woo Code",
            "fieldtype": "Data",
            "insert_after": "territory_name",
            "read_only": 1,
            "no_copy": 1,
        })
        cf.insert(ignore_permissions=True)
        frappe.db.commit()

    # If column still not present (edge case), abort (Frappe will create after cache rebuild)
    try:
        cols = [c[0] for c in frappe.db.sql("desc `tabTerritory`")]
    except Exception:  # noqa: BLE001
        return
    if "woo_code" not in cols:
        return

    terrs = frappe.get_all(
        "Territory",
        filters={"is_group": 0},
        fields=["name", "woo_code"],
    )
    updated = 0
    for t in terrs:
        if not t.get("woo_code"):
            frappe.db.set_value("Territory", t["name"], "woo_code", t["name"], update_modified=False)
            updated += 1
    if updated:
        frappe.db.commit()