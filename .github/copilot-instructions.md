# Jarz WooCommerce Integration – Copilot Instructions

## Project Overview
Single ERPNext app located at `apps/jarz_woocommerce_integration`. It ingests WooCommerce data (customers, territories, orders) and creates/upgrades Sales Invoices using bundle logic maintained inside this app.

## Backend Structure (ERPNext Custom App)

### Service & API Modules
- `jarz_woocommerce_integration/services/`
  - `order_sync.py` – end-to-end order pull, bundle handling, invoice creation (`_build_invoice_items` drives bundle pricing math).
  - `customer_sync.py` – idempotent customer + address sync with default flags.
  - `territory_sync.py` – Woo delivery zones → ERPNext Territory mappings (`pos_profile`).
- `jarz_woocommerce_integration/api/`
  - `orders.py`, `webhooks.py` – REST + webhook entry points; always enqueue long jobs via `frappe.enqueue`.
- `utils/woocommerce_client.py` – REST client, pagination helpers, retry patterns.

### Key DocTypes & Custom Fields
- `WooCommerce Settings` – credentials, webhook secret, pull toggles.
- `WooCommerce Sync Log` – audit log for pulls/webhooks.
- `Sales Invoice` custom fields – `woo_order_id`, bundle flags, POS profile references.
- `Territory.pos_profile` (custom field) – required for warehouse/price list lookups in order sync.

## Critical Logic Expectations
- **Bundles**: `_build_invoice_items` (inside `order_sync.py`) expands bundles and distributes discounts—parent rows keep `discount_percentage = 100`, child rows keep `price_list_rate + discount_percentage`. Do not collapse or recompute rates elsewhere.
- **Customers**: `_ensure_customer` matches on Woo ID/email; `_set_address_as_default` must keep exactly one primary/shipping flag per customer.
- **Orders**: `ORDER_UPDATE_ALLOWED_FIELDS` controls updates for submitted invoices—never widen without business approval.
- **Retries**: Woo webhook handlers may receive duplicates; keep idempotency by `woo_order_id` and timestamp checks.

## External Dependencies
- WooCommerce REST schema & webhook payloads – follow existing field mappings in `order_sync.py` and helper utilities.
- Frappe background workers – long tasks must run via queues defined in `hooks.py` cron schedules.

## Developer Workflow & Commands
1. **Bench context**: `cd /home/frappe/frappe-bench` inside container.
2. **Manual pulls**:
   - Recent orders: `bench --site <site> execute jarz_woocommerce_integration.services.order_sync.pull_recent_orders_phase1 --kwargs "dict(limit=50,allow_update=True)"`
   - Single order: `...pull_single_order_phase1 --kwargs "dict(order_id=12345,allow_update=True)"`
   - Customers: `...services.customer_sync.sync_customers_cron` (or REST wrapper).
3. **Tests**: `bench --site <site> run-tests --app jarz_woocommerce_integration` (tests live under `jarz_woocommerce_integration/tests`).
4. **Formatting**: `pre-commit run --all-files` uses ruff, eslint, prettier, pyupgrade.
5. **Restart services**: `bench restart` (or Docker `restart erp_backend_1` on staging).

## Implementation Patterns
- Place new long-running jobs in `services/` and expose via thin wrappers in `api/` (keep HTTP handlers light).
- Use `frappe.logger()` with context dicts; avoid `print`.
- Keep Woo identifiers (`woo_order_id`, `woo_product_id`, `custom_woo_customer_id`) for deduplication.
- When adding Custom Fields or fixtures, update `hooks.py` fixtures list and regenerate fixtures.

## Common Pitfalls
- Missing Woo credentials raise `WooCommerceError`—re-use helper functions for status handling.
- Multi-site benches: every bench command **must** include `--site`; default dev site is often `development.localhost`.
- Bundle mismatch: if items differ from Woo order, confirm `woo_jarz_bundle` content and adjust the bundle-handling helpers—avoid hard-coding flavor substitutions.
- Webhook signature failures: ensure `WooCommerce Settings.webhook_secret` matches Woo; handler echoes `{ "ack": true }` on empty payloads during setup.

## Collaboration Notes
- Keep bundle logic centralized—modify `_build_invoice_items` (or extracted helpers) rather than scattering calculations across request handlers.
- Consolidate tests—avoid duplicate test modules for similar flows.
- Ask maintainers to clarify undocumented behavior before expanding allowed invoice updates or webhook scope.
