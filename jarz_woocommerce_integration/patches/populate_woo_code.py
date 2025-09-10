import frappe
from jarz_woocommerce_integration.services.territory_sync import CODE_TO_DISPLAY

def execute():
    # For each code present in CODE_TO_DISPLAY, if a Territory with that name exists and woo_code empty, set it.
    updated = 0
    for code in CODE_TO_DISPLAY.keys():
        if frappe.db.exists('Territory', code):
            if not frappe.db.get_value('Territory', code, 'woo_code'):
                frappe.db.set_value('Territory', code, 'woo_code', code, update_modified=False)
                updated += 1
    if updated:
        frappe.db.commit()
