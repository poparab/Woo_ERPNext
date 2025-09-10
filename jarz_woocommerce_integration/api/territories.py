import frappe
from jarz_woocommerce_integration.services.territory_sync import (
    sync_territories,
    get_territories_missing_custom_woo_code,
    populate_custom_woo_codes,
)


@frappe.whitelist(allow_guest=False)
def pull_states():
    return {"success": True, "data": sync_territories()}


@frappe.whitelist(allow_guest=False)
def missing_custom_woo_code():
    """Return list of territories missing custom_woo_code (should normally be empty)."""
    return {"success": True, "data": get_territories_missing_custom_woo_code()}


@frappe.whitelist(allow_guest=False)
def populate_custom_woo_code():
    """Populate custom_woo_code field for existing territories where available (idempotent)."""
    return {"success": True, "data": populate_custom_woo_codes()}