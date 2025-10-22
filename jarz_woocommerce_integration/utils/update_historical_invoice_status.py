"""Update existing historical invoices with custom acceptance status and sales invoice state.

This script updates all existing Sales Invoices created from WooCommerce orders
to set the custom fields based on their original WooCommerce status:

- Completed orders: custom_acceptance_status = "Accepted", custom_sales_invoice_state = "Delivered"
- Cancelled/Refunded orders: custom_acceptance_status = "Accepted", custom_sales_invoice_state = "Cancelled"

Usage:
    bench --site [site] execute \
      jarz_woocommerce_integration.utils.update_historical_invoice_status.update_historical_invoice_status_cli
"""
import frappe


def update_historical_invoice_status_cli():
    """Update all existing WooCommerce invoices with custom status fields."""
    
    frappe.init(site=frappe.local.site)
    frappe.connect()
    
    print("\n" + "="*70)
    print("🔄 Updating Historical Invoice Status Fields")
    print("="*70)
    print("\nFetching all WooCommerce Order Maps...")
    
    # Get all order maps with their WooCommerce status
    order_maps = frappe.db.sql("""
        SELECT 
            name,
            woo_order_id,
            sales_invoice,
            woo_status
        FROM `tabWooCommerce Order Map`
        WHERE sales_invoice IS NOT NULL
        ORDER BY creation
    """, as_dict=True)
    
    if not order_maps:
        print("\n❌ No WooCommerce orders found with invoices")
        print("="*70 + "\n")
        frappe.destroy()
        return
    
    print(f"📊 Found {len(order_maps)} orders with invoices")
    print("\nProcessing updates...\n")
    
    updated_completed = 0
    updated_cancelled = 0
    skipped = 0
    errors = 0
    
    for idx, order_map in enumerate(order_maps, 1):
        try:
            invoice_name = order_map.sales_invoice
            woo_status = (order_map.woo_status or "").lower()
            woo_order_id = order_map.woo_order_id
            
            # Check if invoice exists
            if not frappe.db.exists("Sales Invoice", invoice_name):
                skipped += 1
                continue
            
            # Determine the custom field values based on WooCommerce status
            if woo_status == "completed":
                acceptance_status = "Accepted"
                sales_invoice_state = "Delivered"
                updated_completed += 1
                status_label = "✓ Completed → Delivered"
            elif woo_status in ("cancelled", "refunded"):
                acceptance_status = "Accepted"
                sales_invoice_state = "Cancelled"
                updated_cancelled += 1
                status_label = "✗ Cancelled"
            else:
                # Skip other statuses (processing, pending, etc.)
                skipped += 1
                continue
            
            # Update the invoice
            frappe.db.set_value(
                "Sales Invoice",
                invoice_name,
                {
                    "custom_acceptance_status": acceptance_status,
                    "custom_sales_invoice_state": sales_invoice_state
                },
                update_modified=False
            )
            
            # Progress reporting every 100 orders
            if idx % 100 == 0:
                print(f"Progress: {idx}/{len(order_maps)} - {status_label} Order #{woo_order_id}")
            
        except Exception as e:
            errors += 1
            frappe.logger().error(f"Error updating invoice for order {order_map.woo_order_id}: {str(e)}")
            continue
    
    # Commit all changes
    frappe.db.commit()
    
    # Final summary
    print("\n" + "="*70)
    print("✅ Update Complete!")
    print("="*70)
    print(f"\n📊 Summary:")
    print(f"   Total Orders:    {len(order_maps)}")
    print(f"   ✓ Completed:     {updated_completed} (set to Delivered)")
    print(f"   ✗ Cancelled:     {updated_cancelled} (set to Cancelled)")
    print(f"   ⊘ Skipped:       {skipped} (other statuses or missing invoices)")
    print(f"   ❌ Errors:       {errors}")
    print("\n" + "="*70 + "\n")
    
    frappe.destroy()


if __name__ == "__main__":
    update_historical_invoice_status_cli()
