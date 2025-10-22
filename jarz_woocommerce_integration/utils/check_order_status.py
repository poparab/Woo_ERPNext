"""Check order sync status before full migration."""
import frappe


def check_order_sync_status_cli():
    """Check current order sync status and provide migration plan.
    
    Usage:
        bench --site <site> execute jarz_woocommerce_integration.utils.check_order_status.check_order_sync_status_cli
    """
    
    print("\n📊 Order Sync Status Check\n")
    
    # Count synced orders via WooCommerce Order Map
    synced_count = frappe.db.count("WooCommerce Order Map")
    print(f"✅ Orders already synced: {synced_count}")
    
    # Get linked sales invoices
    order_maps = frappe.get_all(
        "WooCommerce Order Map",
        fields=["woo_order_id", "sales_invoice", "hash"],
        limit=5,
        order_by="modified desc"
    )
    
    if order_maps:
        print(f"\n� Sample Recent Synced Orders:")
        for om in order_maps:
            if om.sales_invoice:
                inv = frappe.get_doc("Sales Invoice", om.sales_invoice)
                print(f"  WooOrder #{om.woo_order_id} → Invoice {inv.name}: {inv.customer} - {inv.grand_total} EGP ({inv.posting_date})")
            else:
                print(f"  WooOrder #{om.woo_order_id}: (mapped but no invoice yet)")
    
    # Count customers with territories
    total_customers = frappe.db.count("Customer")
    customers_with_territory = frappe.db.count("Customer", {"territory": ["!=", ""]})
    
    print(f"\n👥 Customer Territory Status:")
    print(f"  Total Customers: {total_customers}")
    print(f"  With Territory: {customers_with_territory} ({100*customers_with_territory/total_customers:.1f}%)")
    print(f"  Without Territory: {total_customers - customers_with_territory} ({100*(total_customers - customers_with_territory)/total_customers:.1f}%)")
    
    # Check for errors in last sync
    sync_logs = frappe.get_all(
        "Error Log",
        filters={
            "method": ["like", "%order%"],
            "creation": [">=", frappe.utils.add_days(frappe.utils.nowdate(), -1)]
        },
        fields=["name", "error", "creation"],
        limit=3
    )
    
    if sync_logs:
        print(f"\n⚠️  Recent Sync Errors (last 24h):")
        for log in sync_logs:
            print(f"  {log.creation}: {log.error[:100]}...")
    else:
        print(f"\n✅ No recent sync errors (last 24h)")
    
    print(f"\n🎯 Migration Plan:")
    print(f"  - {customers_with_territory} customers ready with territories")
    print(f"  - {synced_count} orders already migrated")
    print(f"  - Target: ~10,000 total historical orders")
    print(f"  - Remaining: ~{10000 - synced_count} orders to migrate")
    print(f"  - Strategy: 50 orders/batch, memory management, 2s breaks")
    
    return {
        "synced_orders": synced_count,
        "total_customers": total_customers,
        "customers_with_territory": customers_with_territory,
    }
