from __future__ import annotations


def run_pos_profile_update_cli():  # pragma: no cover
    """Pull latest 10 Woo orders with updates and force to populate pos_profile on invoices.

    Usage:
        bench --site <site> execute jarz_woocommerce_integration.jarz_woocommerce_integration.cli.run_pos_profile_update_cli
    """
    from .services.order_sync import pull_recent_orders_phase1

    return pull_recent_orders_phase1(limit=10, dry_run=False, force=True, allow_update=True)


def inspect_invoice_pos_profile(name: str):  # pragma: no cover
    """Return POS Profile info for a given Sales Invoice.

    Usage:
        bench --site <site> execute jarz_woocommerce_integration.jarz_woocommerce_integration.cli.inspect_invoice_pos_profile --kwargs '{"name": "ACC-SINV-2025-00001"}'
    """
    import frappe
    if not frappe.db.exists("Sales Invoice", name):
        return {"exists": False, "invoice": name}
    inv = frappe.get_doc("Sales Invoice", name)
    customer = inv.customer
    territory = frappe.db.get_value("Customer", customer, "territory") if customer else None
    terr_pos = frappe.db.get_value("Territory", territory, "pos_profile") if territory else None
    return {
        "exists": True,
        "invoice": name,
        "pos_profile": inv.pos_profile,
        "customer": customer,
        "territory": territory,
        "territory_pos_profile": terr_pos,
    }


def list_recent_woo_invoices_pos_profile(limit: int = 10):  # pragma: no cover
    """List recent Woo-mapped invoices with their POS Profile mapping context."""
    import frappe
    invs = frappe.get_all(
        "Sales Invoice",
        filters=[["Sales Invoice", "woo_order_id", "is", "set"]],
        fields=["name", "woo_order_id", "customer"],
        order_by="creation desc",
        page_length=limit,
    )
    out = []
    for inv in invs:
        territory = frappe.db.get_value("Customer", inv["customer"], "territory") if inv.get("customer") else None
        terr_pos = frappe.db.get_value("Territory", territory, "pos_profile") if territory else None
        pos_profile = frappe.db.get_value("Sales Invoice", inv["name"], "pos_profile")
        out.append({
            "invoice": inv["name"],
            "woo_order_id": inv.get("woo_order_id"),
            "customer": inv.get("customer"),
            "territory": territory,
            "territory_pos_profile": terr_pos,
            "pos_profile": pos_profile,
        })
    return out
