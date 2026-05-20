"""Manual sync API — whitelisted endpoints for the ERPNext form "Push to WooCommerce" button.

These call the existing sync functions synchronously (not enqueued) so the UI receives
immediate feedback. force=True bypasses the enable_outbound_* settings check since this
is an explicit manual override from an authorised user.
"""

import frappe
from jarz_woocommerce_integration.services import sync_events
from jarz_woocommerce_integration.services.outbound_sync import (
    sync_sales_invoice,
    sync_customer,
)


@frappe.whitelist()
def push_sales_invoice(invoice_name: str) -> dict:
    """Manually push a Sales Invoice to WooCommerce."""
    frappe.has_permission("Sales Invoice", "write", invoice_name, throw=True)
    try:
        result = sync_sales_invoice(invoice_name, reason="manual_button", force=True)
        sync_events.record_manual_push_audit_event(
            object_type="Sales Invoice",
            docname=invoice_name,
            result=result,
        )
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "manual_sync.push_sales_invoice")
        sync_events.record_manual_push_audit_event(
            object_type="Sales Invoice",
            docname=invoice_name,
            error=str(exc),
        )
        frappe.throw(str(exc))
    return result


@frappe.whitelist()
def push_customer(customer_name: str) -> dict:
    """Manually push a Customer to WooCommerce."""
    frappe.has_permission("Customer", "write", customer_name, throw=True)
    try:
        result = sync_customer(customer_name, reason="manual_button", force=True)
        sync_events.record_manual_push_audit_event(
            object_type="Customer",
            docname=customer_name,
            result=result,
        )
    except Exception as exc:
        frappe.log_error(frappe.get_traceback(), "manual_sync.push_customer")
        sync_events.record_manual_push_audit_event(
            object_type="Customer",
            docname=customer_name,
            error=str(exc),
        )
        frappe.throw(str(exc))
    return result
