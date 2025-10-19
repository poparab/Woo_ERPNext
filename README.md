### Jarz WooCommerce Integration

Integration to import WooCommerce orders as Sales Invoices

### Webhook Support

Phase 1 now includes optional real‑time syncing for Customers and Orders.

Configure in WooCommerce (WP Admin > WooCommerce > Settings > Advanced > Webhooks):

1. Create (or edit) a webhook:
	- Status: Active
	- Topic: Order created (repeat separately for Order updated) OR use Order updated only (covers creation + changes depending on store setup)
		- Delivery URL: `https://YOUR-ERP-HOST/api/method/jarz_woocommerce_integration.jarz_woocommerce_integration.api.orders.woo_order_webhook`
	- Secret: (paste the same secret you store in WooCommerce Settings inside ERPNext)
2. Repeat for Customer created / updated using endpoint:
		- `https://YOUR-ERP-HOST/api/method/jarz_woocommerce_integration.jarz_woocommerce_integration.api.webhooks.woo_customer_webhook`
3. Save.

ERPNext Setup:
1. Open "WooCommerce Settings" DocType
2. Set Base URL, Consumer Key & Secret (for REST fallback/pulls)
3. Enter Webhook Secret (Password field)
4. Save.

Security / Signature:
- Both endpoints validate `X-WC-Webhook-Signature` (base64 HMAC-SHA256 of raw body) using the shared secret.
- Empty payloads during WooCommerce webhook creation are ACKed (`{"ack": true}`) without signature failure so you can finish setup.

Processing Model:
- Webhook request enqueues a background job and returns immediately (`{"queued": true}`)
- The job fetches the full order via REST (`pull_single_order_phase1`) ensuring consistent transform rules with manual pulls.
- Idempotent: existing Sales Invoices matched by `woo_order_id` are updated (lines replaced if still Draft; selective field updates if Submitted as allowed).
- Sync outcomes are recorded in `WooCommerce Sync Log` with operation = `Webhook`.

Fallback / Manual Pull:
- Continue to use manual pull endpoints if needed:
	- `/api/method/jarz_woocommerce_integration.jarz_woocommerce_integration.api.orders.pull_recent_phase1`
	- `/api/method/jarz_woocommerce_integration.jarz_woocommerce_integration.api.orders.pull_order_phase1?order_id=123`

Troubleshooting:
- Signature mismatch (403): ensure secret identical, no whitespace, and that your site is reachable via HTTPS.
- Webhook shows delivered but invoice not created: check `WooCommerce Sync Log` and server logs for `woo_order_webhook_*` events.
- To test locally, POST an order JSON with header `X-WC-Webhook-Signature` using the computed signature (see developer helper in `api/webhooks.py`).

Limitations (current phase):
- Refunds / cancellations beyond status mapping not yet implemented.
- Inventory push-back not yet enabled.
- Partial update of already submitted invoices limited to selected fields (status, custom state, POS profile, delivery fields).

### Bulk Customer Sync

Use the new endpoint to import or refresh all WooCommerce customers:

`/api/method/jarz_woocommerce_integration.jarz_woocommerce_integration.api.customers.sync_all?per_page=100`

Parameters:
- `per_page` (default 100, Woo max usually 100)
- `max_pages` (optional safety cap)

Response sample:
```json
{
	"success": true,
	"data": {
		"processed": 245,
		"approx_created_or_updated": 245,
		"sample": [ {"customer": "Cust 1", "billing": "ADDR-..." } ]
	}
}
```

Behavior:
- Idempotent: existing customers matched by email or display name.
- Adds Billing / Shipping addresses if a matching line1 not already linked.
- Does not delete stale customers.

Run again any time to backfill new Woo signups.


## Manual Backfill – Quick Commands (inside container)

Assumptions:
- You are already inside the Frappe container shell (bash)
- For local development, site name is `development.localhost`

Tip: The `--kwargs` is evaluated as a Python expression by bench; using `dict(...)` avoids quoting issues.

First, switch to the bench folder:

```bash
cd /workspace/development/frappe-bench
```

Customers (one page example):

```bash
bench --site development.localhost execute jarz_woocommerce_integration.api.customers.sync_all --kwargs "dict(per_page=100,max_pages=1)"
```

Territories (full sync):

```bash
bench --site development.localhost execute jarz_woocommerce_integration.api.territories.sync_all
```

Orders – recent (Phase 1, 100 orders):

```bash
bench --site development.localhost execute jarz_woocommerce_integration.services.order_sync.pull_recent_orders_phase1 --kwargs "dict(limit=100,allow_update=True,force=True)"
```

Single Order (replace 12345):

```bash
bench --site development.localhost execute jarz_woocommerce_integration.services.order_sync.pull_single_order_phase1 --kwargs "dict(order_id=12345,allow_update=True)"
```

### Territory Sync details

Endpoint: `/api/method/jarz_woocommerce_integration.api.territories.sync_all`

Behavior:
- Pulls Woo delivery areas/zones (per your store configuration) into ERPNext `Territory` records.
- Updates `Territory.pos_profile` and `custom_woo_code` when provided.
- Idempotent; safe to re-run.

Troubleshooting:
- Ensure `WooCommerce Settings` has correct Base URL and credentials.
- Run `bench restart` if you deploy new code and don’t see endpoints.


### Installation

You can install this app using the [bench](https://github.com/frappe/bench) CLI:

```bash
cd $PATH_TO_YOUR_BENCH
bench get-app $URL_OF_THIS_REPO --branch develop
bench install-app jarz_woocommerce_integration
```

### Contributing

This app uses `pre-commit` for code formatting and linting. Please [install pre-commit](https://pre-commit.com/#installation) and enable it for this repository:

```bash
cd apps/jarz_woocommerce_integration
pre-commit install
```

Pre-commit is configured to use the following tools for checking and formatting your code:

- ruff
- eslint
- prettier
- pyupgrade

### CI

This app can use GitHub Actions for CI. The following workflows are configured:

- CI: Installs this app and runs unit tests on every push to `develop` branch.
- Linters: Runs [Frappe Semgrep Rules](https://github.com/frappe/semgrep-rules) and [pip-audit](https://pypi.org/project/pip-audit/) on every pull request.


### License

mit
