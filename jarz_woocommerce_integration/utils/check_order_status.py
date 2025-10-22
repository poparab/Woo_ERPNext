"""Check order sync status before full migration."""
import frappe


def check_order_sync_status_cli():
    """Check current order sync status and provide migration plan.
    
    Usage:
        bench --site <site> execute jarz_woocommerce_integration.utils.check_order_status.check_order_sync_status_cli
    """
    
    print("\n📊 Order Sync Status Check\n")
    
    # Count synced orders
    synced_count = frappe.db.count("Sales Invoice", {"custom_woo_order_id": ["!=", ""]})
    print(f"✅ Orders already synced: {synced_count}")
    
    # Count by status
    statuses = frappe.db.sql("""
        SELECT status, COUNT(*) as cnt 
        FROM `tabSales Invoice` 
        WHERE custom_woo_order_id IS NOT NULL AND custom_woo_order_id != ''
        GROUP BY status
        ORDER BY cnt DESC
    """, as_dict=1)
    
    if statuses:
        print(f"\n📋 Synced Orders by Status:")
        for s in statuses:
            print(f"  {s.status}: {s.cnt}")
    
    # Count customers with territories
    total_customers = frappe.db.count("Customer")
    customers_with_territory = frappe.db.count("Customer", {"territory": ["!=", ""]})
    
    print(f"\n👥 Customer Territory Status:")
    print(f"  Total Customers: {total_customers}")
    print(f"  With Territory: {customers_with_territory} ({100*customers_with_territory/total_customers:.1f}%)")
    print(f"  Without Territory: {total_customers - customers_with_territory} ({100*(total_customers - customers_with_territory)/total_customers:.1f}%)")
    
    # Sample orders without invoices
    sample_woo_orders = frappe.db.sql("""
        SELECT custom_woo_order_id, posting_date, customer, grand_total
        FROM `tabSales Invoice`
        WHERE custom_woo_order_id IS NOT NULL AND custom_woo_order_id != ''
        ORDER BY posting_date DESC
        LIMIT 5
    """, as_dict=1)
    
    print(f"\n📦 Sample Recent Synced Orders:")
    for order in sample_woo_orders:
        print(f"  Order #{order.custom_woo_order_id}: {order.customer} - {order.grand_total} EGP ({order.posting_date})")
    
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
        "statuses": statuses,
    }
