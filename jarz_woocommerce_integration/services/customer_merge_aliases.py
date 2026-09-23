"""Keep an absorbed Customer's WooCommerce binding when two Customers merge.

Merging Customers (``rename_doc(merge=True)`` -- the POS app's merge-as-branch,
the dedupe tool, or Desk) deletes the source record, and with it the source's
``woo_customer_id``. The survivor keeps its own id, so the absorbed Woo
account stops resolving to anyone: its next order, profile event or order
update falls through to phone/email, finds nobody, and mints the duplicate the
merge just removed.

``after_rename`` carries the binding across instead. Frappe calls it on the
SURVIVOR after every link has been rewritten but BEFORE it deletes the source
(``rename_doc``: after_rename, then ``delete_doc``), so the source's ids are
read straight from its still-present row -- no hand-off state that a renamed
target name or a ``validate=False`` rename could lose. If the survivor has no
id of its own it adopts the source's; otherwise the source's id (and any
aliases it already carried) joins ``woo_customer_id_aliases``, which
``find_customer_by_woo_id`` consults when no Customer holds the id as its own.

A plain rename (no merge) moves the row with its fields, so the hook ignores it.
"""

from __future__ import annotations

from typing import Any

import frappe

from jarz_woocommerce_integration.utils.customer_woo_id import (
    customer_woo_id_column_exists,
    get_customer_woo_id,
    get_customer_woo_id_aliases,
    record_woo_id_aliases,
    set_customer_woo_id,
)


def carry_source_binding(doc: Any, method: str | None = None, olddn: str | None = None,
                         newdn: str | None = None, merge: bool = False, *args, **kwargs) -> None:
    """Customer.after_rename: give the survivor the absorbed account's ids."""
    if not merge or not customer_woo_id_column_exists():
        return None
    target = newdn or getattr(doc, "name", None)
    if not olddn or not target or olddn == target:
        return None
    ids = []
    primary = get_customer_woo_id(olddn)
    if primary:
        ids.append(primary)
    ids.extend(get_customer_woo_id_aliases(olddn))
    if not ids:
        return None
    remaining = list(ids)
    if not get_customer_woo_id(target):
        # Nothing to alias against: the survivor simply becomes that account.
        set_customer_woo_id(target, remaining.pop(0))
    if remaining:
        record_woo_id_aliases(target, remaining)
    frappe.logger("woo").info(
        f"customer_merge_woo_binding source={olddn!r} target={target!r} "
        f"primary={get_customer_woo_id(target)!r} aliases={get_customer_woo_id_aliases(target)}"
    )
    return None
