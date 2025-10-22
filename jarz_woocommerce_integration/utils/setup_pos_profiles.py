"""Helper script to check Territory POS Profile configuration and resync customers."""

import frappe


def check_territory_pos_profiles():
    """Check which territories have POS profiles configured."""
    territories = frappe.get_all(
        "Territory",
        filters={"parent_territory": ["is", "set"]},
        fields=["name", "pos_profile", "parent_territory"]
    )
    
    with_pos = []
    without_pos = []
    
    for t in territories:
        if t.get("pos_profile"):
            with_pos.append(t)
        else:
            without_pos.append(t)
    
    print("\n=== TERRITORIES WITH POS PROFILE ===")
    print(f"Total: {len(with_pos)}")
    for t in with_pos:
        print(f"  ✓ {t['name']} → {t['pos_profile']}")
    
    print("\n=== TERRITORIES WITHOUT POS PROFILE ===")
    print(f"Total: {len(without_pos)}")
    for t in without_pos[:20]:  # Show first 20
        print(f"  ✗ {t['name']} (parent: {t.get('parent_territory', 'N/A')})")
    if len(without_pos) > 20:
        print(f"  ... and {len(without_pos) - 20} more")
    
    return {"with_pos": len(with_pos), "without_pos": len(without_pos)}


def list_available_pos_profiles():
    """List all available POS Profiles."""
    pos_profiles = frappe.get_all(
        "POS Profile",
        fields=["name", "warehouse", "price_list", "company"],
        order_by="name"
    )
    
    print("\n=== AVAILABLE POS PROFILES ===")
    print(f"Total: {len(pos_profiles)}")
    for p in pos_profiles:
        print(f"  • {p['name']}")
        print(f"    Warehouse: {p.get('warehouse', 'Not Set')}")
        print(f"    Price List: {p.get('price_list', 'Not Set')}")
        print(f"    Company: {p.get('company', 'Not Set')}")
    
    return pos_profiles


def assign_default_pos_profile_to_territories(pos_profile_name=None):
    """Assign a default POS Profile to all territories without one.
    
    Args:
        pos_profile_name: Name of POS Profile to assign (default: use first available)
    """
    if not pos_profile_name:
        # Get first available POS Profile
        pos_profiles = frappe.get_all("POS Profile", fields=["name"], limit=1)
        if not pos_profiles:
            print("\n❌ ERROR: No POS Profiles found. Please create one first.")
            return {"success": False, "message": "No POS Profiles available"}
        pos_profile_name = pos_profiles[0]["name"]
    
    # Check if POS Profile exists
    if not frappe.db.exists("POS Profile", pos_profile_name):
        print(f"\n❌ ERROR: POS Profile '{pos_profile_name}' not found.")
        return {"success": False, "message": f"POS Profile '{pos_profile_name}' not found"}
    
    # Get territories without POS Profile
    territories = frappe.get_all(
        "Territory",
        filters={
            "parent_territory": ["is", "set"],
            "pos_profile": ["in", ["", None]]
        },
        fields=["name"]
    )
    
    print(f"\n=== ASSIGNING POS PROFILE '{pos_profile_name}' TO TERRITORIES ===")
    print(f"Territories to update: {len(territories)}")
    
    updated = 0
    errors = []
    
    for t in territories:
        try:
            frappe.db.set_value("Territory", t["name"], "pos_profile", pos_profile_name, update_modified=False)
            updated += 1
            if updated <= 10:  # Show first 10
                print(f"  ✓ {t['name']} → {pos_profile_name}")
        except Exception as e:
            errors.append({"territory": t["name"], "error": str(e)})
            print(f"  ✗ {t['name']} - Error: {str(e)}")
    
    frappe.db.commit()
    
    if updated > 10:
        print(f"  ... and {updated - 10} more territories updated")
    
    print(f"\n✅ Updated {updated} territories")
    if errors:
        print(f"❌ {len(errors)} errors encountered")
    
    return {
        "success": True,
        "updated": updated,
        "errors": len(errors),
        "pos_profile": pos_profile_name
    }


def resync_all_customers():
    """Resync all WooCommerce customers to update their territories and other fields."""
    from jarz_woocommerce_integration.services.customer_sync import sync_recent_customers
    
    print("\n=== RESYNCING ALL CUSTOMERS ===")
    print("This will update customer territories from their addresses...")
    
    # Sync in batches
    result = sync_recent_customers(limit=500, modified_after=None)
    
    print(f"\n✅ Customer Resync Complete:")
    print(f"  Processed: {result.get('processed', 0)}")
    print(f"  Updated: {result.get('updated', 0)}")
    print(f"  Created: {result.get('created', 0)}")
    print(f"  Skipped: {result.get('skipped', 0)}")
    print(f"  Errors: {result.get('errors', 0)}")
    
    return result


def verify_customer_territories():
    """Check customer territory assignment after resync."""
    customers = frappe.db.sql("""
        SELECT 
            name,
            customer_name,
            territory,
            woo_customer_id
        FROM `tabCustomer`
        WHERE woo_customer_id IS NOT NULL
        ORDER BY modified DESC
        LIMIT 50
    """, as_dict=True)
    
    with_territory = 0
    without_territory = 0
    
    print("\n=== CUSTOMER TERRITORY VERIFICATION ===")
    print(f"Recent customers checked: {len(customers)}")
    
    for c in customers[:10]:  # Show first 10
        if c.get("territory"):
            with_territory += 1
            # Get POS Profile for this territory
            pos_profile = frappe.db.get_value("Territory", c["territory"], "pos_profile")
            status = f"✓ Territory: {c['territory']}, POS: {pos_profile or 'None'}"
        else:
            without_territory += 1
            status = "✗ No Territory"
        
        print(f"  {c['name'][:15]:15} - {c['customer_name'][:25]:25} - {status}")
    
    for c in customers[10:]:
        if c.get("territory"):
            with_territory += 1
        else:
            without_territory += 1
    
    if len(customers) > 10:
        print(f"  ... and {len(customers) - 10} more customers")
    
    print(f"\n📊 Summary:")
    print(f"  With Territory: {with_territory}")
    print(f"  Without Territory: {without_territory}")
    
    return {
        "with_territory": with_territory,
        "without_territory": without_territory,
        "total": len(customers)
    }


def full_setup_and_resync(pos_profile_name=None):
    """Complete setup: check territories, assign POS profiles, resync customers.
    
    Args:
        pos_profile_name: POS Profile to assign to territories (default: auto-select)
    """
    print("=" * 80)
    print("FULL POS PROFILE SETUP AND CUSTOMER RESYNC")
    print("=" * 80)
    
    # Step 1: Check current state
    print("\n[1/5] Checking current territory configuration...")
    territory_status = check_territory_pos_profiles()
    
    # Step 2: List available POS Profiles
    print("\n[2/5] Listing available POS Profiles...")
    pos_profiles = list_available_pos_profiles()
    
    if not pos_profiles:
        print("\n❌ CANNOT PROCEED: No POS Profiles found.")
        print("Please create at least one POS Profile first:")
        print("  1. Go to: POS Profile List")
        print("  2. Create new POS Profile")
        print("  3. Set warehouse, price list, and company")
        print("  4. Save")
        print("  5. Run this script again")
        return {"success": False, "message": "No POS Profiles available"}
    
    # Step 3: Assign POS Profile to territories
    if territory_status["without_pos"] > 0:
        print(f"\n[3/5] Assigning POS Profile to {territory_status['without_pos']} territories...")
        assign_result = assign_default_pos_profile_to_territories(pos_profile_name)
        if not assign_result["success"]:
            return assign_result
    else:
        print("\n[3/5] All territories already have POS Profiles ✓")
    
    # Step 4: Resync customers
    print("\n[4/5] Resyncing customers from WooCommerce...")
    customer_result = resync_all_customers()
    
    # Step 5: Verify
    print("\n[5/5] Verifying customer territory assignments...")
    verification = verify_customer_territories()
    
    print("\n" + "=" * 80)
    print("✅ SETUP COMPLETE")
    print("=" * 80)
    print(f"Territories with POS Profile: {territory_status['with_pos'] + assign_result.get('updated', 0)}")
    print(f"Customers resynced: {customer_result.get('processed', 0)}")
    print(f"Customers with territories: {verification['with_territory']}")
    
    return {
        "success": True,
        "territories_updated": assign_result.get("updated", 0),
        "customers_synced": customer_result.get("processed", 0),
        "customers_with_territory": verification["with_territory"]
    }
