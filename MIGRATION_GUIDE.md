# WooCommerce Historical Order Migration Guide

## Quick Start

### Step 1: Add Database Indexes (one-time per environment)
```bash
docker exec erp_backend_1 bench --site frontend execute \
  jarz_woocommerce_integration.utils.add_sync_indexes.add_sync_indexes_cli
```

### Step 2: Pause Scheduler
```bash
docker exec erp_backend_1 bench --site frontend set-config pause_scheduler 1
```

### Step 3: Run Migration (3 sequential runs)
```bash
# Completed orders — bulk, creates submitted invoices + Payment Entries
docker exec erp_backend_1 bench --site frontend execute \
  jarz_woocommerce_integration.services.order_sync._run_full_historical_migration \
  --kwargs '{"statuses": "completed", "batch_size": 50}'

# Cancelled / Refunded / Failed — draft invoices, no GL impact
docker exec erp_backend_1 bench --site frontend execute \
  jarz_woocommerce_integration.services.order_sync._run_full_historical_migration \
  --kwargs '{"statuses": "cancelled,refunded,failed", "batch_size": 50}'

# Processing — submitted invoices, no Payment Entries
docker exec erp_backend_1 bench --site frontend execute \
  jarz_woocommerce_integration.services.order_sync._run_full_historical_migration \
  --kwargs '{"statuses": "processing", "batch_size": 50}'
```

### Step 4: Monitor Progress
```bash
docker exec erp_backend_1 bench --site frontend execute \
  jarz_woocommerce_integration.services.order_sync.get_migration_progress
```

### Step 5: Re-enable Scheduler
```bash
docker exec erp_backend_1 bench --site frontend set-config pause_scheduler 0
```

### Step 6: Post-Processing (mandatory)
```bash
docker exec erp_backend_1 bench --site frontend execute \
  jarz_woocommerce_integration.utils.update_historical_invoice_status.update_historical_invoice_status_cli
```

---

## Resume an Interrupted Migration

Check progress to find `last_completed_page`, then resume:
```bash
docker exec erp_backend_1 bench --site frontend execute \
  jarz_woocommerce_integration.services.order_sync._run_full_historical_migration \
  --kwargs '{"statuses": "completed", "batch_size": 50, "start_page": <last_completed_page + 1>}'
```

---

## Performance

- **Speed**: ~100-150 orders/minute
- **Resume support**: Yes — `start_page` parameter
- **Progress tracking**: Redis key `woo_historical_migration_progress`
- **Cache**: `MigrationCache` pre-loads all items, prices, bundles, territories once
- **Safety**: Per-order error isolation, outbound sync suppressed automatically

---

## Migration Tools

```
services/
└── order_sync.py                      # Canonical runner: _run_full_historical_migration()

utils/
├── add_sync_indexes.py                # One-time DB index setup (run before migration)
├── update_historical_invoice_status.py # Post-processing (run after migration)
└── migrate_ultra_optimized.py         # DEPRECATED — use _run_full_historical_migration()
```

---

## Troubleshooting

### Check Specific Order
```bash
docker exec erp_backend_1 bench --site frontend execute \
  frappe.db.get_value \
  --args "['WooCommerce Order Map', {'woo_order_id': 6284}, ['name', 'status', 'erpnext_sales_invoice']]"
```

### View Errors
Navigate to: **Error Log** in ERPNext UI (filter by "order_sync" or "Migration")

---

**Last Updated**: April 2026
