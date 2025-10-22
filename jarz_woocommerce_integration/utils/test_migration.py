"""Test ultra-optimized migration with detailed logging."""
import frappe


def test_ultra_optimized_migration_cli():
    """Test the ultra-optimized migration on a small batch.
    
    Usage:
        bench --site <site> execute jarz_woocommerce_integration.utils.test_migration.test_ultra_optimized_migration_cli
    """
    
    from jarz_woocommerce_integration.utils.migrate_ultra_optimized import (
        migrate_historical_orders_optimized,
        OrderSyncCache
    )
    
    print("\n🧪 Testing Ultra-Optimized Migration")
    print("=" * 60)
    
    # Test with just 1 page of 10 orders
    print("\n1. Testing single page (10 orders)...")
    cache = OrderSyncCache()
    
    try:
        result = migrate_historical_orders_optimized(
            limit=10,
            page=1,
            cache=cache
        )
        
        print(f"\n✅ Test Result:")
        print(f"   Orders Fetched: {result.get('orders_fetched', 0)}")
        print(f"   Processed: {result.get('processed', 0)}")
        print(f"   Created: {result.get('created', 0)}")
        print(f"   Skipped: {result.get('skipped', 0)}")
        print(f"   Errors: {result.get('errors', 0)}")
        
        if result.get('orders_fetched', 0) == 0:
            print("\n⚠️  No orders fetched - might have reached end of WooCommerce orders")
            print("   Checking total orders in WooCommerce...")
            
            # Try to get order count from WooCommerce
            from jarz_woocommerce_integration.utils.http_client import WooClient
            settings = frappe.get_single("WooCommerce Settings")
            client = WooClient(
                base_url=settings.base_url,
                consumer_key=settings.consumer_key,
                consumer_secret=settings.get_password("consumer_secret"),
            )
            
            # Check page 1
            test_orders = client.list_orders(params={"per_page": 1, "page": 1, "status": "completed,cancelled,refunded"})
            print(f"   WooCommerce has orders: {len(test_orders) > 0}")
            
        # Check current sync status
        print("\n2. Current Sync Status:")
        order_map_count = frappe.db.count("WooCommerce Order Map")
        print(f"   Total Synced Orders: {order_map_count}")
        
        # Get sample of recent syncs
        recent = frappe.db.sql("""
            SELECT woo_order_id, status, synced_on
            FROM `tabWooCommerce Order Map`
            ORDER BY synced_on DESC
            LIMIT 5
        """, as_dict=1)
        
        if recent:
            print(f"\n   Recent Syncs:")
            for r in recent:
                print(f"     Order #{r.woo_order_id}: {r.status} @ {r.synced_on}")
        
        print("\n" + "=" * 60)
        print("✅ Test Complete")
        
        return result
        
    except Exception as e:
        print(f"\n❌ Test Failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}
