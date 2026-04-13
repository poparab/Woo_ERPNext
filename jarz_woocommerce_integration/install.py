import frappe
from jarz_woocommerce_integration.utils.custom_fields import ensure_custom_fields


def after_install():  # pragma: no cover
    module_name = "Jarz WooCommerce Integration"
    if not frappe.db.exists("Module Def", module_name):
        md = frappe.get_doc({
            "doctype": "Module Def",
            "module_name": module_name,
            "app_name": "jarz_woocommerce_integration",
        })
        md.insert(ignore_permissions=True)
    try:
        frappe.reload_doc("jarz_woocommerce_integration", "doctype", "woocommerce_settings")
    except Exception:  # noqa: BLE001
        frappe.log_error(frappe.get_traceback(), "WooCommerce Settings Reload Failed")
    ensure_custom_fields()
    _ensure_company_defaults()
    frappe.clear_cache()


def after_migrate():  # pragma: no cover
    ensure_custom_fields()
    _ensure_company_defaults()
    frappe.clear_cache()


def _ensure_company_defaults():  # pragma: no cover
    """Idempotently set Company-level defaults required by the integration.

    Runs on every install and migrate so all environments stay consistent.
    Only writes if the value is currently empty — never overwrites a manually
    configured value.
    """
    companies = frappe.get_all("Company", pluck="name")
    for company in companies:
        _set_kashier_account_if_missing(company)


def _set_kashier_account_if_missing(company: str):  # pragma: no cover
    """Discover and set custom_kashier_account for *company* if not already set."""
    try:
        current = frappe.db.get_value("Company", company, "custom_kashier_account")
        if current:
            return  # already configured — never overwrite
        # Find an account whose name contains 'kashier' (case-insensitive)
        match = frappe.db.sql(
            "SELECT name FROM `tabAccount`"
            " WHERE company = %s AND LOWER(name) LIKE %s"
            " ORDER BY name LIMIT 1",
            (company, "%kashier%"),
        )
        if not match:
            return  # no Kashier account exists for this company — skip silently
        account_name = match[0][0]
        frappe.db.set_value("Company", company, "custom_kashier_account", account_name)
        frappe.db.commit()
    except Exception:  # noqa: BLE001
        frappe.log_error(frappe.get_traceback(), f"Woo Integration: set kashier account for {company}")