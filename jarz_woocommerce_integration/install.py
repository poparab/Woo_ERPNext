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
    frappe.clear_cache()


def after_migrate():  # pragma: no cover
    ensure_custom_fields()
    frappe.clear_cache()