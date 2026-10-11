"""Put the Woo order's name back on existing Woo invoices.

Until ``services/invoice_display_name.py`` existed, ERPNext overwrote
``Sales Invoice.customer_name`` with the Customer's (billing) name on every save,
so receipts printed the billing name even when the order shipped to someone
else. The order sync already stored the right name in ``woo_order_display_name``
(shipping name, else billing name); this copies it into ``customer_name``.

A direct column write, not a re-save: these invoices are submitted, and
``customer_name`` is display-only — no ledger, payment or stock row reads it.
Idempotent: a second run matches nothing.
"""

from __future__ import annotations

import frappe


def execute() -> None:
	columns = frappe.db.get_table_columns("Sales Invoice") or []
	if "woo_order_display_name" not in columns:
		return

	frappe.db.sql(
		"""
		UPDATE `tabSales Invoice`
		SET customer_name = TRIM(woo_order_display_name)
		WHERE IFNULL(TRIM(woo_order_display_name), '') != ''
		  AND IFNULL(customer_name, '') != TRIM(woo_order_display_name)
		"""
	)
	print(f"Woo: invoice display-name backfill updated {frappe.db.sql('SELECT ROW_COUNT()')[0][0]} rows")
