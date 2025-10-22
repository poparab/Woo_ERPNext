"""Simple CLI wrapper for optimized historical migration."""
import frappe
from jarz_woocommerce_integration.services.order_sync import migrate_historical_orders
import gc
import time


def migrate_all_orders_optimized_cli():  # pragma: no cover
    """Optimized historical migration with 100 orders/page and better performance.
    
    Improvements over standard migration:
    - Larger batch size (100 vs 50 orders/page)
    - Database indexes already added for faster lookups
    - Memory management with garbage collection
    - Progress checkpoints every 10 batches
    
    Usage:
        bench --site <site> execute jarz_woocommerce_integration.utils.migrate_optimized.migrate_all_orders_optimized_cli
    """
    
    max_pages = 200  # Up to 20,000 orders (200 pages × 100 orders/page)
    batch_size = 100  # Optimized batch size
    
    total_stats = {
        "orders_fetched": 0,
        "processed": 0,
        "created": 0,
        "skipped": 0,
        "errors": 0,
        "pages_processed": 0,
        "batches_completed": 0,
    }
    
    print("\n🚀 Starting Optimized Historical Migration")
    print(f"   Batch Size: {batch_size} orders/page")
    print(f"   Max Pages: {max_pages}")
    print(f"   Max Orders: {max_pages * batch_size:,}")
    print(f"   Optimizations: Database indexes + Memory management\n")
    
    for page in range(1, max_pages + 1):
        # Process one batch
        result = migrate_historical_orders(limit=batch_size, page=page)
        total_stats["orders_fetched"] += result.get("orders_fetched", 0)
        total_stats["processed"] += result.get("processed", 0)
        total_stats["created"] += result.get("created", 0)
        total_stats["skipped"] += result.get("skipped", 0)
        total_stats["errors"] += result.get("errors", 0)
        total_stats["pages_processed"] = page
        total_stats["batches_completed"] += 1
        
        frappe.logger().info(f"Historical migration page {page}/{max_pages} complete: {result}")
        
        # Progress output every page
        if page % 1 == 0:  # Show every page
            print(f"  📄 Page {page}/{max_pages}: Fetched {result.get('orders_fetched', 0)}, "
                  f"Created {result.get('created', 0)}, Skipped {result.get('skipped', 0)}, "
                  f"Errors {result.get('errors', 0)}")
        
        # Memory management: commit and clear cache every batch
        try:
            frappe.db.commit()
            frappe.clear_cache()
            gc.collect()  # Force garbage collection
        except Exception as e:
            frappe.logger().error(f"Cache clear error on page {page}: {str(e)}")
        
        # Stop if we fetched fewer orders than batch_size (reached the end)
        if result.get("orders_fetched", 0) < batch_size:
            print(f"\n✅ Migration complete - reached end of orders at page {page}")
            frappe.logger().info(f"Migration complete - reached end of orders at page {page}")
            break
        
        # Add small delay every 10 batches to prevent worker timeout
        if page % 10 == 0:
            time.sleep(2)  # 2 second break every 10 batches
            print(f"  ⏸️  Checkpoint: {total_stats['created']} orders migrated so far...")
            frappe.logger().info(f"Checkpoint: {total_stats['created']} orders migrated so far...")
    
    # Final summary
    print(f"\n🎉 MIGRATION COMPLETE")
    print(f"   Total Orders Fetched: {total_stats['orders_fetched']:,}")
    print(f"   Total Processed: {total_stats['processed']:,}")
    print(f"   Created: {total_stats['created']:,}")
    print(f"   Skipped: {total_stats['skipped']:,}")
    print(f"   Errors: {total_stats['errors']:,}")
    print(f"   Pages Processed: {total_stats['pages_processed']}")
    
    frappe.logger().info(f"=== MIGRATION COMPLETE === Total: {total_stats}")
    return total_stats
