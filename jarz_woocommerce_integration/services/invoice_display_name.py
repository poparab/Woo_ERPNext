"""Keep a Woo invoice's ``customer_name`` on the name the Woo order carries.

The order sync writes ``customer_name`` from the order's contact snapshot: the
shipping name, else the billing name (``woo_order_display_name``, see
``order_sync._extract_order_contact_snapshot``). ERPNext then threw it away.
``Sales Invoice.customer_name`` is ``fetch_from: customer.customer_name``
without ``fetch_if_empty``, so ``Document._validate_links`` re-fetches it from
the Customer on every insert, save, submit and cancel — and the Customer record
is named from the billing block. Order 16550 on staging: snapshot "على ممدوح على
ابو الوفا" (shipping), invoice and printed receipt "على ممدوح" (billing).

``_validate_links`` runs before any doc_event, so re-asserting the snapshot here
wins. ``before_update_after_submit`` matters beyond the receipt: without it a
post-submit save would re-fetch the billing name and then fail
``validate_update_after_submit`` on a field that is not allow-on-submit.
"""

from __future__ import annotations

from typing import Any


def keep_woo_display_name(doc: Any, method: str | None = None) -> None:
	display_name = str(doc.get("woo_order_display_name") or "").strip()
	if display_name and doc.get("customer_name") != display_name:
		doc.customer_name = display_name
