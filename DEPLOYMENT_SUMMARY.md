# WooCommerce Integration - Two-Phase Order Sync Deployment Summary

## Overview
Successfully implemented and deployed a two-phase WooCommerce order synchronization system with payment method mapping and webhook support.

## Implementation Date
October 22, 2025

## Git Commits
- Main implementation: `1a2a9f6` - "Implement two-phase order sync with payment method mapping and webhook support"
- Payment method fix: `47dd799` - "Fix payment method mapping to use title case"
- Cancellation fix: `a333fd9` - "Fix: Submit draft invoice before cancelling to avoid docstatus error"
- Bulk migration: `95b02ef` - "Add bulk historical migration CLI wrapper for easier execution"

## Features Implemented

### 1. Two-Phase Order Synchronization

#### Phase 1: Historical Migration (One-time)
- **Purpose**: Migrate all historical completed/cancelled orders for reporting
- **Behavior**:
  - Fetches only completed, cancelled, and refunded orders
  - Creates Sales Invoices marked as **paid** (for reporting purposes)
  - No inventory or accounting effects (report-only)
  - Skips orders already synced
- **Execution**: Manual via CLI command
- **Status**: ✅ Completed - **903 historical orders migrated**

#### Phase 2: Live Order Sync (Automated)
- **Purpose**: Continuous synchronization of new orders
- **Behavior**:
  - Creates **unpaid submitted** Sales Invoices
  - Full accounting and inventory effects
  - **Skips pending payment orders** - retries every 2 minutes
  - Processes all order statuses (processing, completed, etc.)
- **Schedule**: Every 2 minutes via cron job
- **Status**: ✅ Active

### 2. Payment Method Mapping

WooCommerce payment methods are automatically mapped to ERPNext `custom_payment_method`:

| WooCommerce | ERPNext Field Value |
|-------------|-------------------|
| `instapay` | `Instapay` |
| `cod` | `Cash` |
| `kashier_card` | `Kashier Card` |
| `kashier_wallet` | `Kashier Wallet` |

### 3. Kashier Payment Entry Creation

For orders with Kashier payment methods (`kashier_card` or `kashier_wallet`):
- Automatically creates **Payment Entry** after invoice submission
- Links payment to Kashier account (configured in Company settings: `custom_kashier_account`)
- Allocates full amount against the Sales Invoice
- Marks invoice as paid

**Note**: Ensure `custom_kashier_account` is configured in Company master for automatic payment entries.

### 4. Pending Payment Handling

For live orders with status `pending` or `on-hold`:
- Orders are **skipped** during sync
- Scheduler retries every **2 minutes**
- Once payment is confirmed and status changes, order is processed automatically

### 5. Webhook Integration

Real-time order synchronization via WooCommerce webhooks:

**Endpoint**: `/api/method/jarz_woocommerce_integration.api.webhook.order_webhook`

**Supported Events**:
- `order.created` - New order notification
- `order.updated` - Order status/details changed

**Security**:
- Webhook signature verification using HMAC-SHA256
- Configure `webhook_secret` in WooCommerce Settings for signature validation

**Setup in WooCommerce Admin**:
1. Navigate to: WooCommerce > Settings > Advanced > Webhooks
2. Create webhook for `Order created`:
   - Delivery URL: `https://your-domain.com/api/method/jarz_woocommerce_integration.api.webhook.order_webhook`
   - Topic: `Order created`
   - Secret: (copy from ERPNext WooCommerce Settings)
3. Create webhook for `Order updated`:
   - Same delivery URL
   - Topic: `Order updated`
   - Same secret

## Deployment Steps Completed

1. ✅ Code implementation (order_sync.py, webhook.py, hooks.py)
2. ✅ Git commit and push to GitHub (poparab/Woo_ERPNext)
3. ✅ Pull code to staging server
4. ✅ Run `bench migrate` to update database schema
5. ✅ Restart Docker containers (backend, scheduler, queue-short, queue-long)
6. ✅ Execute historical migration: **903 orders created, 97 skipped, 0 errors**
7. ✅ Verify live sync scheduler active (every 2 minutes)

## Server Details

- **Staging Server**: 13.37.227.174
- **Site Name**: frontend
- **Docker Setup**: ERPNext in containers (erpnext_docker)
- **Apps**:
  - frappe
  - erpnext
  - jarz_pos
  - jarz_woocommerce_integration (GitHub: poparab/Woo_ERPNext)

## CLI Commands Reference

### Historical Migration
```bash
# Migrate first 100 historical orders (page 1)
bench --site frontend execute jarz_woocommerce_integration.services.order_sync.migrate_historical_orders

# Migrate specific page
bench --site frontend execute jarz_woocommerce_integration.services.order_sync.migrate_historical_orders --kwargs '{"page": 2}'

# Bulk migrate all historical orders (up to 1000 orders / 10 pages)
bench --site frontend execute jarz_woocommerce_integration.services.order_sync.migrate_all_historical_orders_cli
```

### Live Order Sync (Manual Test)
```bash
# Pull last 20 orders (live mode - unpaid invoices)
bench --site frontend execute jarz_woocommerce_integration.services.order_sync.pull_recent_orders_phase1
```

### Webhook Test
```bash
# Test webhook endpoint with curl
curl -X POST https://your-domain.com/api/method/jarz_woocommerce_integration.api.webhook.order_webhook \
  -H "Content-Type: application/json" \
  -H "X-WC-Webhook-Topic: order.created" \
  -d '{"id": 9999, "status": "processing", ...}'
```

## Scheduler Configuration

Cron jobs active in `hooks.py`:

```python
scheduler_events = {
    "cron": {
        # Customer sync every 15 minutes
        "*/15 * * * *": [
            "jarz_woocommerce_integration.services.customer_sync.sync_customers_cron"
        ],
        # Territory sync every 6 hours
        "0 */6 * * *": [
            "jarz_woocommerce_integration.services.territory_sync.sync_territories_cron"
        ],
        # Live order sync every 2 minutes
        "*/2 * * * *": [
            "jarz_woocommerce_integration.services.order_sync.sync_orders_cron_phase1"
        ]
    }
}
```

## Migration Results

### Historical Migration (One-time Execution)
- **Total Orders Fetched**: 1,000
- **Orders Processed**: 1,000
- **Invoices Created**: 903
- **Skipped (Already Mapped)**: 97
- **Errors**: 0
- **Pages Processed**: 10 (100 orders/page)

All historical orders successfully migrated as **paid Sales Invoices** for reporting purposes.

## Testing Checklist

- [x] Historical migration completes without errors
- [x] Live order sync creates unpaid submitted invoices
- [x] Pending payment orders are skipped
- [x] Payment methods mapped correctly (Instapay, Cash, Kashier Card, Kashier Wallet)
- [ ] Kashier Payment Entry creation (requires `custom_kashier_account` configuration)
- [ ] Webhook receives order.created events
- [ ] Webhook receives order.updated events
- [x] Scheduler running every 2 minutes
- [ ] Customer sync active (every 15 minutes)
- [ ] Territory sync active (every 6 hours)

## Configuration Requirements

### Required Custom Fields

1. **Sales Invoice**:
   - `custom_payment_method` (Select): Options: "", "Cash", "Instapay", "Mobile Wallet", "Kashier Card", "Kashier Wallet"
   - `woo_order_id` (Data)
   - `woo_order_number` (Data)
   - `custom_delivery_date` (Date)
   - `custom_delivery_time_from` (Time)
   - `custom_delivery_duration` (Int) - in seconds

2. **Company**:
   - `custom_kashier_account` (Link: Account) - Required for automatic Kashier payment entries

3. **WooCommerce Settings** (Single):
   - `webhook_secret` (Password) - For webhook signature verification

### WooCommerce Settings Configuration

Navigate to: WooCommerce Settings doctype

Required fields:
- Base URL
- Consumer Key
- Consumer Secret
- Webhook Secret (optional but recommended)
- Default Company
- Default Warehouse
- Default Currency

## Monitoring & Logs

### View Scheduler Logs
```bash
docker-compose logs -f scheduler
```

### View Background Job Logs
```bash
docker-compose logs -f queue-short
```

### Check Error Log in ERPNext
Navigate to: Desk > Error Log

Filter by:
- "WooCommerce Webhook Error"
- "Kashier Payment Entry Error"
- "woo_order_sync_live_error"

## Known Issues & Resolutions

### Issue 1: Payment Method Validation Error
**Error**: `Payment Method cannot be "cash". It should be one of "", "Cash", ...`

**Resolution**: ✅ Fixed in commit `47dd799` - Payment methods now use title case matching ERPNext field options.

### Issue 2: Cannot Cancel Draft Invoice
**Error**: `Cannot change docstatus from 0 (Draft) to 2 (Cancelled)`

**Resolution**: ✅ Fixed in commit `a333fd9` - Draft invoices are now submitted before cancellation.

### Issue 3: Scheduler Import Errors
**Error**: `ModuleNotFoundError: No module named 'jarz_woocommerce_integration'`

**Resolution**: ✅ Restart scheduler container after code updates: `docker-compose restart scheduler`

## Next Steps

1. **Configure Kashier Account**: Set `custom_kashier_account` in Company master to enable automatic payment entries
2. **Set Up Webhooks in WooCommerce**: Configure order.created and order.updated webhooks pointing to staging endpoint
3. **Monitor Live Sync**: Watch Error Log and scheduler logs for 24-48 hours to ensure stable operation
4. **Test Real Orders**: Create test orders in WooCommerce and verify end-to-end flow:
   - Order creation
   - Webhook delivery
   - Invoice creation
   - Payment entry (for Kashier)
   - Status updates
5. **Production Deployment**: After successful staging verification, deploy to production using same process

## Support & Troubleshooting

### Common Commands

**Restart All Services**:
```bash
cd erpnext_docker
docker-compose restart backend scheduler queue-short queue-long
```

**Clear Redis Cache**:
```bash
docker-compose exec backend bench --site frontend clear-cache
```

**Re-run Migration**:
```bash
docker-compose exec backend bench --site frontend migrate
```

**Manual Order Sync Test**:
```bash
docker-compose exec backend bench --site frontend execute jarz_woocommerce_integration.services.order_sync.pull_recent_orders_phase1
```

### Contact
For issues or questions:
- GitHub Repository: [poparab/Woo_ERPNext](https://github.com/poparab/Woo_ERPNext)
- Email: ar.abuelwafa@orderjarz.com

---

**Deployment Status**: ✅ **SUCCESSFUL**

**Date Completed**: October 22, 2025

**Verified By**: GitHub Copilot AI Agent
