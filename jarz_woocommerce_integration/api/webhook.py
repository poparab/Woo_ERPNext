"""Legacy order webhook entrypoint (kept for backward compatibility).

This delegates to the main order webhook handler in api.orders.woo_order_webhook so
both URLs behave identically and use the same signature validation (base64 HMAC).
"""

from __future__ import annotations

import frappe

from jarz_woocommerce_integration.api.orders import woo_order_webhook as _orders_webhook


@frappe.whitelist(allow_guest=True, methods=["POST"])
def order_webhook():
    """Alias to the primary order webhook handler.

    Woo setups pointing to /api/method/jarz_woocommerce_integration.api.webhook.order_webhook
    will now receive the same behavior as the recommended endpoint
    /api/method/jarz_woocommerce_integration.jarz_woocommerce_integration.api.orders.woo_order_webhook.
    """
    return _orders_webhook()
