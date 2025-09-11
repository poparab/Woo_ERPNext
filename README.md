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
