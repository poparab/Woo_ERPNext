"""Drop the sha1 digest from WooCommerce customer-note rows.

The first shape (8d6f567) stored "<note>\\n\\n— WooCommerce customer note (b9df781f297f)",
and the app shows that digest to staff on the card and in the notes sheet. The
row is now matched on its full text, so the digest carries nothing.

``frappe.db.set_value``, not a re-save: no doc_event fires, so nothing is pushed
back out to WooCommerce, and ``modified`` is left alone. Idempotent: a second run
matches nothing.
"""

from __future__ import annotations

import frappe

from jarz_woocommerce_integration.services.order_sync import (
	_INVOICE_NOTE_DOCTYPE,
	_LEGACY_CUSTOMER_NOTE_DIGEST,
)


def execute() -> None:
	if not frappe.db.exists("DocType", _INVOICE_NOTE_DOCTYPE):
		return

	rows = frappe.get_all(
		_INVOICE_NOTE_DOCTYPE,
		filters={"note": ["like", "%WooCommerce customer note (%"]},
		fields=["name", "note"],
	)
	updated = 0
	for row in rows:
		cleaned = _LEGACY_CUSTOMER_NOTE_DIGEST.sub(r"\1", row.note or "")
		if cleaned != row.note:
			frappe.db.set_value(_INVOICE_NOTE_DOCTYPE, row.name, "note", cleaned, update_modified=False)
			updated += 1
	print(f"Woo: stripped the digest from {updated} customer-note rows")
