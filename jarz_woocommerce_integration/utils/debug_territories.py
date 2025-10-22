"""Check why some customers don't have territories assigned."""
import frappe
import json


def check_customers_without_territory_cli():
    """Check a sample of customers without territory to debug the issue."""
    
    # Get sample customers without territory
    customers_no_terr = frappe.db.sql("""
        SELECT name, customer_name, email_id
        FROM `tabCustomer`
        WHERE (territory IS NULL OR territory = '')
        LIMIT 10
    """, as_dict=1)
    
    print(f"\n🔍 Checking {len(customers_no_terr)} sample customers without territory:\n")
    
    for cust in customers_no_terr:
        print(f"  Customer: {cust.customer_name} ({cust.email_id})")
        
        # Check their addresses
        addresses = frappe.db.sql("""
            SELECT a.name, a.address_type, a.state, a.city, a.address_line1
            FROM `tabAddress` a
            INNER JOIN `tabDynamic Link` dl ON dl.parent = a.name
            WHERE dl.link_doctype = 'Customer' 
            AND dl.link_name = %s
            AND a.disabled = 0
        """, (cust.name,), as_dict=1)
        
        if not addresses:
            print(f"    ❌ No addresses found")
        else:
            for addr in addresses:
                print(f"    📍 {addr.address_type} Address:")
                print(f"       State: {addr.state or '(empty)'}")
                print(f"       City: {addr.city or '(empty)'}")
                
                # Try to resolve territory from state
                if addr.state:
                    from jarz_woocommerce_integration.services.customer_sync import _resolve_territory_from_state
                    territory = _resolve_territory_from_state(addr.state)
                    if territory:
                        print(f"       ✅ Would map to territory: {territory}")
                    else:
                        print(f"       ❌ No territory match found for: {addr.state}")
                        
                        # Check for similar territories
                        state_lower = addr.state.lower()
                        similar_territories = frappe.db.sql("""
                            SELECT name, territory_name
                            FROM `tabTerritory`
                            WHERE is_group = 0
                            AND (LOWER(name) LIKE %s OR LOWER(territory_name) LIKE %s)
                            LIMIT 3
                        """, (f"%{state_lower}%", f"%{state_lower}%"), as_dict=1)
                        
                        if similar_territories:
                            print(f"       💡 Similar territories found:")
                            for t in similar_territories:
                                print(f"          - {t.name} ({t.territory_name})")
        
        print()
    
    # Also check territory name formats
    print(f"\n📋 Sample Territory Names in System:")
    territories = frappe.db.sql("""
        SELECT name, territory_name
        FROM `tabTerritory`
        WHERE is_group = 0
        LIMIT 10
    """, as_dict=1)
    
    for t in territories:
        print(f"  {t.name}: {t.territory_name}")
