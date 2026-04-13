import frappe


def execute():
    """One-time patch: auto-discover and set custom_kashier_account on each Company.

    Finds any Account whose name contains 'kashier' (case-insensitive) and sets it
    as the Company's Kashier payment account if the field is currently empty.
    Idempotent — never overwrites an existing value.
    """
    companies = frappe.get_all("Company", pluck="name")
    for company in companies:
        current = frappe.db.get_value("Company", company, "custom_kashier_account")
        if current:
            continue  # already set — skip

        match = frappe.db.sql(
            "SELECT name FROM `tabAccount`"
            " WHERE company = %s AND LOWER(name) LIKE %s"
            " ORDER BY name LIMIT 1",
            (company, "%kashier%"),
        )
        if not match:
            continue  # no Kashier account found for this company

        account_name = match[0][0]
        frappe.db.set_value("Company", company, "custom_kashier_account", account_name)
        frappe.db.commit()
