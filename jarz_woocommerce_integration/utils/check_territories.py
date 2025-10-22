"""Quick CLI to check customer territory distribution."""
import frappe


def check_customer_territories_cli():
    """Check how many customers have territories assigned."""
    
    # Count customers with and without territories
    total = frappe.db.count("Customer")
    with_territory = frappe.db.sql("""
        SELECT COUNT(*) as cnt 
        FROM `tabCustomer` 
        WHERE territory IS NOT NULL AND territory != ''
    """, as_dict=1)[0].cnt
    
    without_territory = total - with_territory
    
    # Get territory distribution
    territory_dist = frappe.db.sql("""
        SELECT territory, COUNT(*) as customer_count 
        FROM `tabCustomer` 
        WHERE territory IS NOT NULL AND territory != '' 
        GROUP BY territory 
        ORDER BY customer_count DESC 
        LIMIT 15
    """, as_dict=1)
    
    print(f"\n📊 Customer Territory Distribution:")
    print(f"  Total Customers: {total}")
    print(f"  With Territory: {with_territory} ({100*with_territory/total:.1f}%)")
    print(f"  Without Territory: {without_territory} ({100*without_territory/total:.1f}%)")
    
    print(f"\n🏘️  Top Territories:")
    for row in territory_dist:
        print(f"  {row.territory}: {row.customer_count} customers")
    
    # Check if territories have POS profiles
    print(f"\n🏬 Checking POS Profile Coverage:")
    territories_with_pos = frappe.db.sql("""
        SELECT COUNT(*) as cnt 
        FROM `tabTerritory` 
        WHERE pos_profile IS NOT NULL AND pos_profile != ''
    """, as_dict=1)[0].cnt
    
    total_territories = frappe.db.count("Territory")
    print(f"  Total Territories: {total_territories}")
    print(f"  With POS Profile: {territories_with_pos}")
    print(f"  Without POS Profile: {total_territories - territories_with_pos}")
    
    return {
        "total_customers": total,
        "with_territory": with_territory,
        "without_territory": without_territory,
        "territory_distribution": territory_dist,
        "territories_with_pos": territories_with_pos,
        "total_territories": total_territories,
    }
