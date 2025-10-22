"""Update customer territories from their existing addresses in ERPNext."""
import frappe


def update_customer_territories_from_addresses_cli():
    """Update customer territories by reading their existing addresses.
    
    This scans all customers, looks at their shipping/billing addresses,
    extracts the state field, and assigns the matching territory.
    
    Usage:
        bench --site <site> execute jarz_woocommerce_integration.utils.update_customer_territories.update_customer_territories_from_addresses_cli
    """
    from jarz_woocommerce_integration.services.customer_sync import _resolve_territory_from_state
    
    print("\n🔄 Updating customer territories from existing addresses...\n")
    
    # Get all customers
    customers = frappe.get_all("Customer", fields=["name", "territory"])
    
    updated = 0
    skipped = 0
    no_address = 0
    no_match = 0
    
    for cust in customers:
        customer_name = cust.name
        current_territory = cust.territory
        
        # Get addresses for this customer
        addresses = frappe.db.sql("""
            SELECT a.name, a.address_type, a.state
            FROM `tabAddress` a
            INNER JOIN `tabDynamic Link` dl ON dl.parent = a.name
            WHERE dl.link_doctype = 'Customer' 
            AND dl.link_name = %s
            AND a.disabled = 0
            ORDER BY CASE WHEN a.address_type = 'Shipping' THEN 1 ELSE 2 END
        """, (customer_name,), as_dict=1)
        
        if not addresses:
            no_address += 1
            continue
        
        # Try to find territory from addresses (prefer shipping)
        territory_found = None
        for addr in addresses:
            if addr.state:
                territory_found = _resolve_territory_from_state(addr.state)
                if territory_found:
                    break
        
        if not territory_found:
            no_match += 1
            continue
        
        # Update if different or empty
        if current_territory != territory_found:
            try:
                frappe.db.set_value("Customer", customer_name, "territory", territory_found, update_modified=False)
                updated += 1
                if updated % 100 == 0:
                    frappe.db.commit()
                    print(f"  Progress: {updated} customers updated...")
            except Exception as e:
                frappe.logger().error(f"Failed to update territory for {customer_name}: {e}")
                skipped += 1
        else:
            skipped += 1
    
    # Final commit
    frappe.db.commit()
    
    print(f"\n✅ Territory Update Complete:")
    print(f"  Total Customers: {len(customers)}")
    print(f"  Updated: {updated}")
    print(f"  Skipped (already correct): {skipped}")
    print(f"  No Address: {no_address}")
    print(f"  No Territory Match: {no_match}")
    
    return {
        "total": len(customers),
        "updated": updated,
        "skipped": skipped,
        "no_address": no_address,
        "no_match": no_match,
    }
