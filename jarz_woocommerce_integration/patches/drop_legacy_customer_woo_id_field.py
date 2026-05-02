from __future__ import annotations

import frappe

from jarz_woocommerce_integration.services.customer_woo_id_migration import drop_legacy_customer_woo_id_field


def execute():
    result = drop_legacy_customer_woo_id_field(force=False)
    frappe.logger("jarz_woocommerce.customer_woo_id_migration").info(
        {"event": "drop_legacy_customer_woo_id_field", "result": result}
    )