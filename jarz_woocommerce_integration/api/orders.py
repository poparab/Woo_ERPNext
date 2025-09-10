import frappe
from jarz_woocommerce_integration.services.order_sync import (
    pull_recent_orders_phase1,
    pull_single_order_phase1,
)  # single-level package path (standard frappe app layout)


@frappe.whitelist(allow_guest=False)
def pull_recent_phase1(limit: int = 20, dry_run: int = 0, force: int = 0):
    """Pull recent orders (Phase 1) with optional dry-run.

    Args:
        limit: max orders to evaluate (1..100)
        dry_run: if truthy, don't create anything
    """
    limit = max(1, min(int(limit), 100))
    return {
        "success": True,
        "data": pull_recent_orders_phase1(limit=limit, dry_run=bool(int(dry_run)), force=bool(int(force))),
    }


@frappe.whitelist(allow_guest=False)
def pull_order_phase1(order_id: int | str = None, dry_run: int = 0, force: int = 0):
    """Pull a single Woo order by id (Phase 1) for targeted debugging.

    Args:
        order_id: Woo order id
        dry_run: simulate without DB writes
        force: delete existing mapping record and reprocess
    """
    if not order_id:
        frappe.throw("order_id required")
    data = pull_single_order_phase1(
        order_id=order_id, dry_run=bool(int(dry_run)), force=bool(int(force))
    )
    return {"success": True, "data": data}


@frappe.whitelist(allow_guest=False)
def pull_recent_pos_profile_update():
    """Convenience endpoint: pull 10 recent orders with updates and force enabled.

    This is used to quickly populate Sales Invoice.pos_profile based on Territory.pos_profile
    after deploying the mapping logic, without wrestling with CLI kwargs quoting.
    """
    return {
        "success": True,
        "data": pull_recent_orders_phase1(limit=10, dry_run=False, force=True, allow_update=True),
    }
