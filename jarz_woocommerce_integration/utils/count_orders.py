"""Quick count of synced orders."""
import frappe


def count_synced_orders():
    """Print count of synced orders."""
    count = frappe.db.count("WooCommerce Order Map")
    print(f"\n📊 Total Synced Orders: {count}")
    print(f"Target: ~10,000 orders")
    print(f"Remaining: ~{10000 - count} orders")
    print(f"Progress: {100 * count / 10000:.1f}%\n")
    return count
