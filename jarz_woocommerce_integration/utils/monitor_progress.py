"""Monitor order migration progress."""
import frappe
import time


def monitor_migration_progress_cli():
    """Monitor the progress of order migration in real-time.
    
    Usage:
        bench --site <site> execute jarz_woocommerce_integration.utils.monitor_progress.monitor_migration_progress_cli
    """
    
    print("\n📈 Order Migration Progress Monitor\n")
    print("Checking every 30 seconds... (Press Ctrl+C to stop)\n")
    
    start_count = frappe.db.count("WooCommerce Order Map")
    start_time = time.time()
    
    print(f"Starting count: {start_count} orders synced")
    print(f"Target: ~10,000 orders total")
    print(f"Remaining: ~{10000 - start_count} orders\n")
    
    try:
        while True:
            time.sleep(30)
            
            current_count = frappe.db.count("WooCommerce Order Map")
            elapsed = time.time() - start_time
            orders_added = current_count - start_count
            
            if orders_added > 0:
                rate = orders_added / (elapsed / 60)  # orders per minute
                remaining = 10000 - current_count
                est_minutes = remaining / rate if rate > 0 else 0
                
                print(f"[{time.strftime('%H:%M:%S')}] Orders: {current_count} (+{orders_added}) | "
                      f"Rate: {rate:.1f} orders/min | "
                      f"ETA: {est_minutes:.0f} min")
            else:
                print(f"[{time.strftime('%H:%M:%S')}] Orders: {current_count} (no change)")
            
            if current_count >= 10000:
                print(f"\n✅ Migration complete! Total orders: {current_count}")
                break
                
    except KeyboardInterrupt:
        print(f"\n\nMonitoring stopped. Final count: {frappe.db.count('WooCommerce Order Map')} orders")


if __name__ == "__main__":
    monitor_migration_progress_cli()
