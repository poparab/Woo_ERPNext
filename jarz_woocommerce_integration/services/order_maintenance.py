"""Operator maintenance for WooCommerce Order Map rows.

Bench-callable only (deliberately not ``@frappe.whitelist``: it cancels orders on
the live store). Dry-run is the default::

    bench --site <site> execute \
        jarz_woocommerce_integration.services.order_maintenance.retire_duplicate_woo_order \
        --kwargs "{'woo_order_id': 17863, 'keep_woo_order_id': 17864}"

and the same with ``'apply': True`` once the plan reads right.
"""

from __future__ import annotations

from typing import Any

import frappe
import requests

from jarz_woocommerce_integration.services.order_map_status import (
    RETIRED_DUPLICATE_MAP_STATUS,
    is_retired_duplicate_status,
)
from jarz_woocommerce_integration.utils.http_client import WooAPIError

ORDER_MAP_DOCTYPE = "WooCommerce Order Map"

#: The MariaDB named lock inbound order processing holds per Woo order
#: (order_sync._process_order_phase1: ``f"woo-order-{woo_id}"``).
INBOUND_ORDER_LOCK_PREFIX = "woo-order-"
_INBOUND_ORDER_LOCK_WAIT_SECONDS = 10


def _acquire_inbound_order_lock(woo_order_id: int) -> bool:
    try:
        rows = frappe.db.sql(
            "SELECT GET_LOCK(%s, %s)",
            (f"{INBOUND_ORDER_LOCK_PREFIX}{woo_order_id}", _INBOUND_ORDER_LOCK_WAIT_SECONDS),
        )
    except Exception:  # noqa: BLE001 - cannot tell: do not mark without the lock
        return False
    return bool(rows and rows[0] and rows[0][0] == 1)


def _release_inbound_order_lock(woo_order_id: int) -> None:
    try:
        frappe.db.sql("SELECT RELEASE_LOCK(%s)", (f"{INBOUND_ORDER_LOCK_PREFIX}{woo_order_id}",))
    except Exception:  # noqa: BLE001
        pass


def _as_int(value: Any) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _review_reason(keep_woo_order_id: int) -> str:
    return f"Duplicate of #{keep_woo_order_id} created by concurrent outbound push"


def _duplicate_note_body(keep_woo_order_id: int) -> str:
    # Fixed text: `_post_woo_order_note` dedupes on the exact body, so re-running
    # the retirement never stacks a second note.
    return (
        f"Duplicate of order #{keep_woo_order_id}, created by a sync error between "
        f"ERPNext and the store. This is not a real order: do not prepare, deliver "
        f"or charge it. The customer's real order is #{keep_woo_order_id}."
    )


def _refuse(result: dict[str, Any], reason: str, detail: str | None = None) -> dict[str, Any]:
    result.update({"status": "refused", "reason": reason})
    if detail:
        result["detail"] = detail
    return result


def retire_duplicate_woo_order(woo_order_id: Any, keep_woo_order_id: Any, apply: Any = False) -> dict[str, Any]:
    """Retire a duplicate Woo order whose Order Map row links a real invoice.

    Refuses unless the duplicate's map row links an invoice whose own
    ``woo_order_id`` is ``keep_woo_order_id`` (and the two ids differ). With
    ``apply`` it then, in this order:

    1. marks the map row ``retired-duplicate`` and COMMITS -- before anything
       touches the store, so the inbound webhook our cancel triggers already
       finds the marker and skips instead of cancelling the real invoice;
    2. marks the order in the outbound echo cache;
    3. PUTs the Woo order to ``cancelled``;
    4. adds a private order note naming the real order.

    Re-running is safe: an already-retired row is not rewritten, the PUT is
    idempotent and the note is deduplicated.
    """
    from jarz_woocommerce_integration.services import outbound_sync

    duplicate_id = _as_int(woo_order_id)
    keep_id = _as_int(keep_woo_order_id)
    do_apply = _truthy(apply)
    result: dict[str, Any] = {
        "woo_order_id": duplicate_id,
        "keep_woo_order_id": keep_id,
        "apply": do_apply,
    }

    if not duplicate_id or not keep_id:
        return _refuse(result, "missing_order_ids")
    if duplicate_id == keep_id:
        return _refuse(result, "duplicate_equals_keep")

    link_field = outbound_sync._resolve_order_map_link_field()
    map_row = frappe.db.get_value(
        ORDER_MAP_DOCTYPE,
        {"woo_order_id": duplicate_id},
        ["name", link_field, "status"],
        as_dict=True,
    )
    if not map_row:
        return _refuse(result, "no_order_map_for_duplicate")
    invoice_name = map_row.get(link_field)
    result["order_map"] = map_row.get("name")
    result["invoice"] = invoice_name
    if not invoice_name:
        return _refuse(result, "order_map_links_no_invoice")

    invoice = frappe.db.get_value("Sales Invoice", invoice_name, ["name", "woo_order_id"], as_dict=True)
    if not invoice:
        return _refuse(result, "invoice_not_found")
    invoice_woo_id = _as_int(invoice.get("woo_order_id"))
    result["invoice_woo_order_id"] = invoice_woo_id
    if invoice_woo_id != keep_id:
        return _refuse(
            result,
            "invoice_does_not_keep_that_order",
            f"{invoice_name}.woo_order_id is {invoice_woo_id or 'empty'}, not {keep_id}",
        )

    keep_row = frappe.db.get_value(
        ORDER_MAP_DOCTYPE,
        {"woo_order_id": keep_id},
        ["name", link_field, "status"],
        as_dict=True,
    )
    if keep_row:
        if is_retired_duplicate_status(keep_row.get("status")):
            return _refuse(result, "keep_order_is_retired", f"Order Map {keep_row.get('name')}")
        if keep_row.get(link_field) and keep_row.get(link_field) != invoice_name:
            return _refuse(
                result,
                "keep_order_links_another_invoice",
                f"Order Map {keep_row.get('name')} -> {keep_row.get(link_field)}",
            )
    else:
        result["warning"] = f"no Order Map row for the kept order #{keep_id}"

    already_retired = is_retired_duplicate_status(map_row.get("status"))
    note_body = _duplicate_note_body(keep_id)
    result["already_retired"] = already_retired
    result["plan"] = [
        (
            f"leave Order Map {map_row.get('name')} as already retired"
            if already_retired
            else f"set Order Map {map_row.get('name')} status {map_row.get('status')!r} -> "
            f"{RETIRED_DUPLICATE_MAP_STATUS!r} and commit"
        ),
        f"mark Woo #{duplicate_id} in the outbound echo cache",
        f"PUT Woo #{duplicate_id} status -> 'cancelled'",
        f"add private note to Woo #{duplicate_id}: {note_body}",
    ]

    if not do_apply:
        result["status"] = "dry_run"
        return result

    # 1. Marker first, and durable, before the store is touched. Held under the
    #    inbound per-order lock (the one order_sync._process_order_phase1 takes):
    #    an inbound sync of this order already in flight could otherwise write
    #    its stale snapshot -- `status` included -- over the marker, and the
    #    cancel below would then reach the real invoice.
    if not already_retired:
        if not _acquire_inbound_order_lock(duplicate_id):
            return _refuse(
                result,
                "inbound_sync_in_progress",
                f"{INBOUND_ORDER_LOCK_PREFIX}{duplicate_id} is held; re-run in a minute",
            )
        try:
            # Re-read under the lock: the in-flight sync may have changed it.
            # REPEATABLE READ would serve the snapshot of the reads above, so
            # end that transaction first (nothing has been written yet; the
            # named lock belongs to the session and survives the rollback).
            frappe.db.rollback()
            current = frappe.db.get_value(
                ORDER_MAP_DOCTYPE, map_row["name"], [link_field, "status"], as_dict=True
            ) or {}
            if current.get(link_field) != invoice_name:
                frappe.db.rollback()
                return _refuse(
                    result,
                    "order_map_changed",
                    f"Order Map {map_row['name']} now links {current.get(link_field) or 'nothing'}",
                )
            if not is_retired_duplicate_status(current.get("status")):
                frappe.db.set_value(
                    ORDER_MAP_DOCTYPE,
                    map_row["name"],
                    {
                        "status": RETIRED_DUPLICATE_MAP_STATUS,
                        "needs_manual_review": 0,
                        "manual_review_reason": _review_reason(keep_id),
                    },
                    update_modified=True,
                )
            frappe.db.commit()
        finally:
            _release_inbound_order_lock(duplicate_id)
    else:
        frappe.db.commit()
    result["order_map_retired"] = True

    # 2. Our own write must read as an echo to anything that still looks.
    outbound_sync._mark_outbound_push_in_flight(duplicate_id)

    settings, _cfg = outbound_sync._get_settings()
    try:
        client = outbound_sync._build_client(settings)
    except ValueError as exc:
        result.update({"status": "partial", "reason": f"woo_client_unavailable:{exc}"})
        return result

    # 3. Cancel the duplicate on the store. Safe to repeat, so a failure here is
    #    reported as partial and fixed by re-running.
    try:
        response = client.put(f"orders/{duplicate_id}", {"status": "cancelled"})
        result["woo_status"] = (response or {}).get("status") if isinstance(response, dict) else None
    except WooAPIError as exc:
        result.update({
            "status": "partial",
            "reason": "woo_cancel_failed",
            "detail": exc.message or f"status_code={exc.status_code}",
        })
        return result
    except requests.RequestException as exc:
        result.update({"status": "partial", "reason": "woo_cancel_failed", "detail": str(exc)})
        return result

    # 4. Tell staff, privately, what this order is.
    try:
        result["note"] = outbound_sync._post_woo_order_note(client, duplicate_id, note_body)
    except requests.RequestException as exc:
        result["note"] = f"failed: {exc}"
    result["status"] = "applied" if result["note"] in ("posted", "already_posted") else "partial"
    return result
