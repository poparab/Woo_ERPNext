# Outbound Sync Setup Documentation

## Custom Fields for Production

### ✅ Status: Production Ready

The custom fields for tracking WooCommerce outbound sync status are now **fully configured and ready for production deployment**.

### What Was Done

1. **Custom Fields Created** on staging server:
   - **Customer DocType**: `woo_outbound_status`, `woo_outbound_error`, `woo_outbound_synced_on`
   - **Sales Invoice DocType**: `woo_outbound_status`, `woo_outbound_error`, `woo_outbound_synced_on`

2. **Fixtures Exported** (Date: November 16, 2025):
   - All custom fields have been exported to `/jarz_woocommerce_integration/fixtures/custom_field.json`
   - This file contains 40KB of custom field definitions
   - File has been pulled to local development workspace

3. **Hooks Configuration** (Already in place):
   ```python
   fixtures = [
       {
           "dt": "Custom Field",
           "filters": [
               [
                   "dt",
                   "in",
                   [
                       "Territory",
                       "Sales Invoice",
                       "WooCommerce Settings",
                       "Customer",
                       "Address",
                       "Item",
                       "Woo Jarz Bundle",
                   ],
               ]
           ],
       }
   ]
   ```

### For Production Deployment

When you deploy to production, the custom fields will be **automatically created** during the migration process:

1. The app installation will import fixtures from `fixtures/custom_field.json`
2. The `after_migrate` hook will ensure all fields are properly set up
3. The database columns will be created automatically via `bench migrate`

**No manual field creation needed on production!**

### Verification Steps for Production

After deploying to production, verify with:
```bash
# Check custom fields exist
bench --site <production-site> console
>>> import frappe
>>> frappe.get_all('Custom Field', filters={'dt': 'Customer', 'fieldname': ['like', 'woo_outbound%']}, fields=['fieldname', 'label'])
>>> frappe.get_all('Custom Field', filters={'dt': 'Sales Invoice', 'fieldname': ['like', 'woo_outbound%']}, fields=['fieldname', 'label'])
```

Expected output: 3 fields each for Customer and Sales Invoice.

---

## WooCommerce API URL Configuration

### ✅ URL Source: WooCommerce Settings DocType

The WooCommerce API URL is **NOT hardcoded** - it's configured dynamically from the WooCommerce Settings DocType.

### How It Works

1. **Settings Storage**:
   - DocType: `WooCommerce Settings`
   - Field: `base_url` (Data type)
   - Example: `https://orderjarz.com`

2. **Client Initialization** (`utils/http_client.py`):
   ```python
   @dataclass(slots=True)
   class WooClient:
       base_url: str          # ← From WooCommerce Settings
       consumer_key: str      # ← From WooCommerce Settings
       consumer_secret: str   # ← From WooCommerce Settings (encrypted)
       api_version: str = "v3"
       timeout: int = 30
   ```

3. **URL Building** (`services/outbound_sync.py`):
   ```python
   def _build_client(settings: WooCommerceSettings) -> WooClient:
       base_url = (getattr(settings, "base_url", "") or "").strip().rstrip("/")
       consumer_key = (getattr(settings, "consumer_key", "") or "").strip()
       consumer_secret = settings.get_consumer_secret()
       
       if not base_url or not consumer_key or not consumer_secret:
           raise ValueError("missing_credentials")
       
       return WooClient(
           base_url=base_url,
           consumer_key=consumer_key,
           consumer_secret=consumer_secret,
           api_version=settings.api_version or "v3",
       )
   ```

4. **API Endpoint Construction** (`utils/http_client.py`):
   ```python
   def _build_url(self, resource: str) -> str:
       resource = resource.lstrip("/")
       if resource.startswith("wp-json"):
           return f"{self.base_url}/{resource}"
       return f"{self.base_url}/wp-json/wc/{self.api_version}/{resource}"
   ```

### For Different Environments

You can easily configure different WooCommerce stores per environment:

- **Staging**: Set `base_url` in WooCommerce Settings to staging store URL
- **Production**: Set `base_url` to production store URL (e.g., `https://orderjarz.com`)
- **Development**: Set `base_url` to local/test store URL

### Current Staging Configuration

- **Base URL**: `https://orderjarz.com` (configured in WooCommerce Settings)
- **API Version**: `v3` (default)
- **Authentication**: HTTP Basic Auth (consumer_key as username, consumer_secret as password)

### Authentication Method

- **Method**: HTTP Basic Authentication
- **Implementation**: Uses `requests.auth.HTTPBasicAuth`
- **Fixed on**: November 16, 2025 (changed from URL query parameters to proper HTTP Basic Auth)

---

## Summary

| Question | Answer | Status |
|----------|--------|--------|
| **Are custom fields ready for production?** | Yes, fixtures exported and configured | ✅ Ready |
| **What URL is used for WooCommerce API?** | From `WooCommerce Settings.base_url` field | ✅ Dynamic |
| **Is anything hardcoded?** | No, fully configurable per environment | ✅ Flexible |

Both aspects are production-ready and follow best practices for Frappe app development!
