# POS Profile Assignment Logic for WooCommerce Orders

## Overview
The system automatically assigns POS Profile to Sales Invoices based on the customer's delivery Territory. This ensures proper warehouse selection, pricing, and payment configuration.

## Assignment Flow

### Step-by-Step Process

1. **Customer Identification**
   - Extract customer from WooCommerce order
   - Ensure customer exists in ERPNext (create if needed)

2. **Territory Resolution**
   ```python
   territory_name = frappe.db.get_value("Customer", customer, "territory")
   ```
   - Read the `territory` field from the Customer master
   - Territory is typically set during customer sync based on WooCommerce delivery zones

3. **POS Profile Lookup**
   ```python
   if territory_name:
       pos_profile = frappe.db.get_value("Territory", territory_name, "pos_profile")
   ```
   - Each Territory in ERPNext has a custom field `pos_profile` (Link to POS Profile)
   - If territory has a POS Profile configured, use it

4. **Warehouse Assignment**
   ```python
   if pos_profile:
       default_warehouse = frappe.db.get_value("POS Profile", pos_profile, "warehouse")
   ```
   - Get the default warehouse from the POS Profile
   - This warehouse is used for all invoice items
   - Fallback to `WooCommerce Settings.default_warehouse` if not found

5. **Price List Resolution**
   ```python
   if pos_profile:
       price_list = frappe.db.get_value("POS Profile", pos_profile, "price_list")
   ```
   - Use the Price List configured in the POS Profile
   - This determines item pricing for the invoice
   - Fallback to Company's default selling price list

6. **Invoice Creation**
   - Set `pos_profile` on the Sales Invoice
   - Set `is_pos = 1` to enable POS features
   - Apply warehouse and price list from POS Profile

## Example Configuration

### Territory Setup
```
Territory: Nasr City
├─ pos_profile: Cairo POS Profile
└─ delivery_income: 20.00
```

### POS Profile Setup
```
POS Profile: Cairo POS Profile
├─ warehouse: Cairo Main Store
├─ price_list: Cairo Retail Prices
├─ company: Jarz Company
└─ applicable_for_users: [All]
```

### Customer Setup
```
Customer: Ahmed Mohamed
├─ territory: Nasr City
└─ (automatically gets Cairo POS Profile via Territory)
```

### Result
When an order comes for Ahmed Mohamed:
- ✅ Sales Invoice gets `pos_profile = Cairo POS Profile`
- ✅ Items use warehouse `Cairo Main Store`
- ✅ Prices from `Cairo Retail Prices`
- ✅ Payment method fields available

## Benefits of Territory-Based POS Profile

1. **Automatic Warehouse Selection**
   - Orders automatically go to the right warehouse based on delivery area
   - No manual intervention needed

2. **Zone-Based Pricing**
   - Different territories can have different price lists
   - Support for regional pricing strategies

3. **Territory-Specific Configuration**
   - Payment methods per region
   - Delivery charges per territory
   - Tax configurations per zone

4. **Scalability**
   - Easy to add new delivery zones
   - Configure once per territory
   - All customers in that territory inherit settings

## Configuration Steps

### 1. Set Up Custom Field in Territory
```sql
-- This should already exist from fixtures
Custom Field: pos_profile
DocType: Territory
Fieldtype: Link
Options: POS Profile
```

### 2. Create POS Profiles per Zone
Example:
- Cairo POS Profile → Cairo Main Store → Cairo Retail Prices
- Giza POS Profile → Giza Branch → Giza Retail Prices
- Alexandria POS Profile → Alex Warehouse → Alex Retail Prices

### 3. Link Territories to POS Profiles
```
Territory Master:
- Nasr City → Cairo POS Profile
- Maadi → Cairo POS Profile
- Mohandessin → Giza POS Profile
- Dokki → Giza POS Profile
```

### 4. Ensure Customers Have Territories
- Customer sync automatically sets territory based on WooCommerce delivery zones
- Manual customers should have territory set

## Troubleshooting

### Issue: No POS Profile Assigned
**Check:**
1. Does the customer have a territory?
   ```sql
   SELECT territory FROM `tabCustomer` WHERE name = 'CUSTOMER-NAME'
   ```

2. Does the territory have a POS Profile?
   ```sql
   SELECT pos_profile FROM `tabTerritory` WHERE name = 'TERRITORY-NAME'
   ```

3. Does the POS Profile exist?
   ```sql
   SELECT name, warehouse, price_list FROM `tabPOS Profile` WHERE name = 'PROFILE-NAME'
   ```

### Issue: Wrong Warehouse Used
**Solution:**
- Verify POS Profile has correct warehouse configured
- Check Territory → POS Profile linkage
- Ensure warehouse exists and is active

### Issue: Wrong Prices
**Solution:**
- Verify POS Profile has correct price list
- Ensure Item Prices exist for that price list
- Check price list is active

## Fallback Behavior

If POS Profile assignment fails at any step:

1. **No Territory on Customer**
   - Use `WooCommerce Settings.default_warehouse`
   - Use Company's default selling price list
   - No POS Profile set

2. **No POS Profile on Territory**
   - Use `WooCommerce Settings.default_warehouse`
   - Use Company's default selling price list
   - No POS Profile set

3. **No Warehouse on POS Profile**
   - Use `WooCommerce Settings.default_warehouse`
   - Continue with POS Profile for other settings

## SQL Queries for Verification

### Check Territory to POS Profile Mapping
```sql
SELECT 
    name as territory,
    pos_profile,
    delivery_income
FROM `tabTerritory`
WHERE pos_profile IS NOT NULL;
```

### Check Customer Territories
```sql
SELECT 
    name as customer_id,
    customer_name,
    territory
FROM `tabCustomer`
WHERE woo_customer_id IS NOT NULL;
```

### Check POS Profiles and Warehouses
```sql
SELECT 
    name as pos_profile,
    warehouse,
    price_list,
    company
FROM `tabPOS Profile`;
```

### Check Recent Invoices with POS Profile
```sql
SELECT 
    name as invoice,
    customer,
    pos_profile,
    posting_date,
    grand_total
FROM `tabSales Invoice`
WHERE woo_order_id IS NOT NULL
ORDER BY creation DESC
LIMIT 50;
```

## Best Practices

1. **One POS Profile per Major Zone**
   - Don't create too many POS Profiles
   - Group similar territories together

2. **Consistent Naming**
   - Use clear names: "Cairo Downtown POS", "Giza West POS"
   - Include region/zone in name

3. **Regular Audits**
   - Verify all active territories have POS Profiles
   - Check for orphaned configurations

4. **Test Before Production**
   - Create test order for each territory
   - Verify correct warehouse/prices assigned

5. **Monitor Coverage**
   ```sql
   -- Find customers without territories
   SELECT name, customer_name
   FROM `tabCustomer`
   WHERE territory IS NULL AND woo_customer_id IS NOT NULL;
   
   -- Find territories without POS Profiles
   SELECT name
   FROM `tabTerritory`
   WHERE pos_profile IS NULL AND parent_territory IS NOT NULL;
   ```

## Summary

The POS Profile assignment is **automatic and territory-based**:
- **Input**: WooCommerce order with customer address
- **Lookup**: Customer → Territory → POS Profile
- **Output**: Sales Invoice with warehouse, price list, and POS settings

This ensures consistent, zone-appropriate handling of all orders without manual intervention.
