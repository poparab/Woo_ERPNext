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


def run_customer_cleanup_cli(
    dry_run: bool = True,
    per_page: int = 100,
    start_page: int = 1,
    max_pages: int | None = None,
    commit_every: int = 100,
    hard_delete_orphans: bool = False,
):  # pragma: no cover
    """Run the one-time customer and address cleanup.

    Usage:
        bench --site <site> execute jarz_woocommerce_integration.cli.run_customer_cleanup_cli --kwargs '{"dry_run": true}'
    """
    from .services.customer_cleanup import run_customer_cleanup

    return run_customer_cleanup(
        dry_run=dry_run,
        per_page=per_page,
        start_page=start_page,
        max_pages=max_pages,
        commit_every=commit_every,
        hard_delete_orphans=hard_delete_orphans,
    )


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


def repair_invoice_15781_customer():  # pragma: no cover
    """One-time repair: reassign ACC-SINV-2026-15781 from Mina Atef to a new guest customer.

    Woo order 14746 was a guest order (customer_id=0).  The sync incorrectly
    reused ERP customer 'Mina Atef' (woo_customer_id=3708) via email match.
    This function:
      1. Creates a new Customer 'كريم سيد محمود' with the order's phone number.
      2. Creates a new Billing address for the new customer (copied from the
         address already linked on the invoice).
      3. Reassigns the Sales Invoice (customer, customer_name, customer_address,
         shipping_address_name).
      4. Reassigns the GL Entry party for that invoice.

    Usage:
        bench --site frontend execute jarz_woocommerce_integration.jarz_woocommerce_integration.cli.repair_invoice_15781_customer
    """
    import frappe

    INVOICE = "ACC-SINV-2026-15781"
    NEW_NAME = "كريم سيد محمود"
    ORDER_PHONE = "01146269820"
    ORDER_TERRITORY = "EGOBOUR"
    OLD_ADDRESS = "Mina Atef-Billing-1"

    if not frappe.db.exists("Sales Invoice", INVOICE):
        return {"error": f"Invoice {INVOICE} not found"}

    current_customer = frappe.db.get_value("Sales Invoice", INVOICE, "customer")
    if current_customer != "Mina Atef":
        return {"skipped": True, "reason": f"Invoice customer is already '{current_customer}', not 'Mina Atef'"}

    # 1. Create new Customer
    cust = frappe.get_doc({
        "doctype": "Customer",
        "customer_name": NEW_NAME,
        "customer_type": "Individual",
        "territory": ORDER_TERRITORY,
        "mobile_no": ORDER_PHONE,
        "disabled": 0,
    })
    cust.flags.ignore_woo_outbound = True
    cust.insert(ignore_permissions=True)
    new_customer_name = cust.name

    # 2. Create new Address (copy data from OLD_ADDRESS)
    old_addr = frappe.get_doc("Address", OLD_ADDRESS)
    new_addr = frappe.get_doc({
        "doctype": "Address",
        "address_title": NEW_NAME,
        "address_type": "Billing",
        "address_line1": old_addr.address_line1,
        "address_line2": old_addr.address_line2 or "",
        "city": old_addr.city,
        "state": old_addr.state or "",
        "pincode": old_addr.pincode or "",
        "country": old_addr.country or "Egypt",
        "links": [{
            "doctype": "Address",
            "link_doctype": "Customer",
            "link_name": new_customer_name,
        }],
    })
    new_addr.insert(ignore_permissions=True)
    new_address_name = new_addr.name

    # 3. Reassign the Sales Invoice
    frappe.db.set_value("Sales Invoice", INVOICE, {
        "customer": new_customer_name,
        "customer_name": NEW_NAME,
        "customer_address": new_address_name,
        "shipping_address_name": new_address_name,
    }, update_modified=False)

    # 4. Reassign GL Entry party
    frappe.db.sql(
        "UPDATE `tabGL Entry` SET party=%s WHERE voucher_no=%s AND party=%s AND party_type='Customer'",
        (new_customer_name, INVOICE, "Mina Atef"),
    )
    frappe.db.commit()

    return {
        "success": True,
        "invoice": INVOICE,
        "new_customer": new_customer_name,
        "new_address": new_address_name,
        "old_customer": "Mina Atef",
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
