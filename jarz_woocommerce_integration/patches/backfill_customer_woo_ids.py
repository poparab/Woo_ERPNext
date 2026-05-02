from __future__ import annotations

import frappe

from jarz_woocommerce_integration.services.customer_woo_id_migration import migrate_customer_woo_ids


def execute():
    result = migrate_customer_woo_ids(dry_run=False, clear_legacy=True)
    frappe.logger("jarz_woocommerce.customer_woo_id_migration").info(
        {"event": "backfill_customer_woo_ids", "result": result}
    )