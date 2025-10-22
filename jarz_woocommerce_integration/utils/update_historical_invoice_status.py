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
    
    # Get all Sales Invoices with woo_order_id
    invoices = frappe.db.sql("""
        SELECT 
            name,
            woo_order_id,
            docstatus
        FROM `tabSales Invoice`
        WHERE woo_order_id IS NOT NULL
        AND woo_order_id != ''
        ORDER BY creation
    """, as_dict=True)
    
    if not invoices:
        print("\n❌ No WooCommerce invoices found")
        print("="*70 + "\n")
        frappe.destroy()
        return
    
    print(f"📊 Found {len(invoices)} invoices with WooCommerce orders")
    print("\nProcessing updates...\n")
    
    updated_completed = 0
    updated_cancelled = 0
    skipped = 0
    errors = 0
    
    for idx, invoice in enumerate(invoices, 1):
        try:
            invoice_name = invoice.name
            woo_order_id = invoice.woo_order_id
            docstatus = invoice.docstatus
            
            # Determine status based on docstatus
            # docstatus: 0=Draft, 1=Submitted, 2=Cancelled
            if docstatus == 1:
                # Submitted = Completed order
                acceptance_status = "Accepted"
                sales_invoice_state = "Delivered"
                updated_completed += 1
                status_label = "✓ Submitted → Delivered"
            elif docstatus == 2:
                # Cancelled invoice = Cancelled/Refunded order
                acceptance_status = "Accepted"
                sales_invoice_state = "Cancelled"
                updated_cancelled += 1
                status_label = "✗ Cancelled"
            else:
                # Skip draft invoices
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
                print(f"Progress: {idx}/{len(invoices)} - {status_label} Order #{woo_order_id}")
            
        except Exception as e:
            errors += 1
            frappe.logger().error(f"Error updating invoice {invoice.name}: {str(e)}")
            continue
    
    # Commit all changes
    frappe.db.commit()
    
    # Final summary
    print("\n" + "="*70)
    print("✅ Update Complete!")
    print("="*70)
    print(f"\n📊 Summary:")
    print(f"   Total Invoices:  {len(invoices)}")
    print(f"   ✓ Completed:     {updated_completed} (set to Delivered)")
    print(f"   ✗ Cancelled:     {updated_cancelled} (set to Cancelled)")
    print(f"   ⊘ Skipped:       {skipped} (draft invoices)")
    print(f"   ❌ Errors:       {errors}")
    print("\n" + "="*70 + "\n")
    
    frappe.destroy()


if __name__ == "__main__":
    update_historical_invoice_status_cli()
