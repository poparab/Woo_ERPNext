"""The "retired duplicate" marker on a WooCommerce Order Map row.

A concurrent outbound push used to create two or three Woo orders for one POS
invoice (ACC-SINV-2026-18733 -> Woo 17863/17864/17865). Every extra order got its
own Order Map row linking the REAL invoice, so anything the store later did to
the duplicate -- cancel it, trash it, delete it -- was applied to the real,
delivered and paid invoice by the inbound paths.

Unlinking such a row is not an option either: with no map row and no Sales
Invoice carrying that woo_order_id, inbound would import the cancelled duplicate
as a brand-new invoice. So the row stays, linked, and is marked instead. Every
path that reads an Order Map row and then acts on the linked invoice must treat
a row carrying this status as "not ours to act on".

``status`` is a plain Data field on the Order Map, so this needs no schema
change. The value deliberately is not a WooCommerce order status, so no inbound
sync can ever write it by accident -- and inbound must never overwrite it, which
is why ``order_sync._process_order_phase1`` returns before touching the row.

Kept dependency-free so every service can import it without a cycle.
"""

from __future__ import annotations

from typing import Any

RETIRED_DUPLICATE_MAP_STATUS = "retired-duplicate"

#: The skip reason inbound paths report for a retired row.
RETIRED_DUPLICATE_REASON = "retired_duplicate"


def is_retired_duplicate_status(status: Any) -> bool:
    return str(status or "").strip().lower() == RETIRED_DUPLICATE_MAP_STATUS


def is_retired_duplicate_row(row: Any) -> bool:
    """True when an Order Map row (dict-like or object) carries the marker."""
    if not row:
        return False
    if isinstance(row, dict):
        return is_retired_duplicate_status(row.get("status"))
    return is_retired_duplicate_status(getattr(row, "status", None))
