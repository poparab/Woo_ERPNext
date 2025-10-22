"""WooCommerce Webhook Handlers for real-time order sync."""

from __future__ import annotations

import hashlib
import hmac
from typing import Any

import frappe
from frappe import _


@frappe.whitelist(allow_guest=True, methods=["POST"])
def order_webhook():
    """Handle WooCommerce order webhook for real-time sync.
    
    Endpoint: /api/method/jarz_woocommerce_integration.api.webhook.order_webhook
    
    Supports events:
    - order.created
    - order.updated
    """
    try:
        # Get webhook payload
        payload = frappe.request.get_data(as_text=True)
        if not payload:
            frappe.log_error("Empty webhook payload", "WooCommerce Webhook Error")
            return {"success": False, "message": "Empty payload"}
        
        # Verify webhook signature
        signature = frappe.request.headers.get("X-WC-Webhook-Signature")
        if not _verify_webhook_signature(payload, signature):
            frappe.log_error(
                f"Invalid webhook signature: {signature}",
                "WooCommerce Webhook Security Error"
            )
            return {"success": False, "message": "Invalid signature"}
        
        # Parse JSON payload
        import json
        order_data = json.loads(payload)
        
        # Get webhook event type
        event = frappe.request.headers.get("X-WC-Webhook-Topic", "")
        
        if not event.startswith("order."):
            return {"success": False, "message": f"Unsupported event: {event}"}
        
        # Process order asynchronously
        frappe.enqueue(
            _process_webhook_order,
            queue="short",
            timeout=300,
            order_data=order_data,
            event=event,
        )
        
        return {"success": True, "message": "Order queued for processing"}
        
    except Exception as e:
        frappe.log_error(
            f"Webhook processing error: {str(e)}\n{frappe.get_traceback()}",
            "WooCommerce Webhook Error"
        )
        return {"success": False, "message": str(e)}


def _verify_webhook_signature(payload: str, signature: str | None) -> bool:
    """Verify WooCommerce webhook signature using HMAC-SHA256.
    
    Args:
        payload: Raw request body
        signature: X-WC-Webhook-Signature header value
    
    Returns:
        True if signature is valid, False otherwise
    """
    if not signature:
        # If no signature required, skip verification
        # In production, you should always require signature
        return True
    
    try:
        settings = frappe.get_single("WooCommerce Settings")
        webhook_secret = settings.get_password("webhook_secret")
        
        if not webhook_secret:
            # No secret configured, allow webhook
            frappe.logger().warning("No webhook secret configured - skipping signature verification")
            return True
        
        # Compute HMAC-SHA256
        expected_signature = hmac.new(
            webhook_secret.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256
        ).digest().hex()
        
        # Compare signatures (constant-time comparison)
        return hmac.compare_digest(signature, expected_signature)
        
    except Exception as e:
        frappe.log_error(
            f"Signature verification error: {str(e)}",
            "WooCommerce Webhook Signature Error"
        )
        return False


def _process_webhook_order(order_data: dict, event: str) -> None:
    """Process order from webhook (async background job).
    
    Args:
        order_data: WooCommerce order JSON
        event: Webhook event type (order.created, order.updated)
    """
    try:
        from jarz_woocommerce_integration.services.order_sync import process_order_phase1
        from jarz_woocommerce_integration.utils.custom_fields import ensure_custom_fields
        
        settings = frappe.get_single("WooCommerce Settings")
        ensure_custom_fields()
        
        # Process as live order (not historical)
        result = process_order_phase1(
            order_data,
            settings,
            allow_update=True,
            is_historical=False
        )
        
        frappe.logger().info({
            "event": f"woo_webhook_{event}",
            "order_id": order_data.get("id"),
            "result": result
        })
        
        if result.get("status") == "error":
            frappe.log_error(
                f"Webhook order processing failed: {result.get('reason')}",
                f"WooCommerce Webhook Order {order_data.get('id')}"
            )
            
    except Exception as e:
        frappe.log_error(
            f"Webhook order processing error: {str(e)}\n{frappe.get_traceback()}",
            f"WooCommerce Webhook Order {order_data.get('id', 'unknown')}"
        )
