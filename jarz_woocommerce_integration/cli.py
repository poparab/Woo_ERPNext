from __future__ import annotations


def audit_customer_woo_id_migration_cli(limit: int = 20):  # pragma: no cover
    """Inspect customer Woo ID migration status.

    Usage:
        bench --site <site> execute jarz_woocommerce_integration.jarz_woocommerce_integration.cli.audit_customer_woo_id_migration_cli
    """
    from .services.customer_woo_id_migration import audit_customer_woo_id_migration

    return audit_customer_woo_id_migration(limit=limit)


def migrate_customer_woo_ids_cli(dry_run: bool = True, clear_legacy: bool = True):  # pragma: no cover
    """Backfill canonical Woo customer IDs from legacy data.

    Usage:
        bench --site <site> execute jarz_woocommerce_integration.jarz_woocommerce_integration.cli.migrate_customer_woo_ids_cli --kwargs '{"dry_run": true}'
    """
    from .services.customer_woo_id_migration import migrate_customer_woo_ids

    return migrate_customer_woo_ids(dry_run=dry_run, clear_legacy=clear_legacy)


def drop_legacy_customer_woo_id_field_cli(force: bool = False):  # pragma: no cover
    """Drop the legacy custom_woo_customer_id field once audit is clean.

    Usage:
        bench --site <site> execute jarz_woocommerce_integration.jarz_woocommerce_integration.cli.drop_legacy_customer_woo_id_field_cli
    """
    from .services.customer_woo_id_migration import drop_legacy_customer_woo_id_field

    return drop_legacy_customer_woo_id_field(force=force)


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


def backfill_missing_order_maps_cli(limit: int = 0):  # pragma: no cover
    """Create missing WooCommerce Order Map rows from existing Woo-linked invoices."""
    import frappe

    link_field = "erpnext_sales_invoice"
    try:
        cols = frappe.db.get_table_columns("WooCommerce Order Map") or []
        if link_field not in cols and "sales_invoice" in cols:
            link_field = "sales_invoice"
    except Exception:
        pass

    invoices = frappe.get_all(
        "Sales Invoice",
        filters=[["Sales Invoice", "woo_order_id", "is", "set"]],
        fields=["name", "woo_order_id", "currency", "grand_total"],
        order_by="creation desc",
        page_length=limit or 50000,
    )

    processed_ids = set()
    created = 0
    updated = 0
    already_present = 0
    duplicate_invoices_skipped = 0
    sample = []

    for inv in invoices:
        woo_order_id = inv.get("woo_order_id")
        if woo_order_id in processed_ids:
            duplicate_invoices_skipped += 1
            continue
        processed_ids.add(woo_order_id)

        map_name = frappe.db.get_value("WooCommerce Order Map", {"woo_order_id": woo_order_id}, "name")
        if map_name:
            current_link = frappe.db.get_value("WooCommerce Order Map", map_name, link_field)
            if current_link:
                already_present += 1
                continue

            map_doc = frappe.get_doc("WooCommerce Order Map", map_name)
            map_doc.update({
                link_field: inv.get("name"),
                "currency": inv.get("currency"),
                "total": inv.get("grand_total"),
                "synced_on": frappe.utils.now_datetime(),
            })
            map_doc.save(ignore_permissions=True)
            updated += 1
        else:
            frappe.get_doc({
                "doctype": "WooCommerce Order Map",
                "woo_order_id": int(woo_order_id),
                link_field: inv.get("name"),
                "currency": inv.get("currency"),
                "total": inv.get("grand_total"),
                "synced_on": frappe.utils.now_datetime(),
            }).insert(ignore_permissions=True)
            created += 1

        if len(sample) < 10:
            sample.append({"invoice": inv.get("name"), "woo_order_id": woo_order_id})

    frappe.db.commit()
    return {
        "scanned": len(invoices),
        "unique_woo_orders": len(processed_ids),
        "created": created,
        "updated": updated,
        "already_present": already_present,
        "duplicate_invoices_skipped": duplicate_invoices_skipped,
        "sample": sample,
    }
