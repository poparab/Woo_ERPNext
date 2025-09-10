import base64
import hashlib
import hmac
import json

import frappe


def compute_sig(secret: str, payload: bytes) -> str:
    return base64.b64encode(hmac.new(secret.encode(), payload, hashlib.sha256).digest()).decode()


def test_order_webhook_ack():
    # Simulate handshake (no id) should ACK even without signature
    resp = frappe.get_attr("jarz_woocommerce_integration.jarz_woocommerce_integration.api.orders.woo_order_webhook")()  # type: ignore
    assert resp.get("ack") is True


def test_order_webhook_process(monkeypatch):  # pragma: no cover - environment dependent
    settings = frappe.get_single("WooCommerce Settings")
    secret = getattr(settings, "webhook_secret", None) or "testsecret"
    order_payload = {"id": 999999, "status": "processing", "line_items": []}
    raw = json.dumps(order_payload).encode()
    sig = compute_sig(secret, raw)

    # monkeypatch request context
    class DummyReq:
        data = raw
        headers = {"X-WC-Webhook-Signature": sig}
        path = "/api/method/jarz_woocommerce_integration.api.orders.woo_order_webhook"

    monkeypatch.setattr(frappe, "request", DummyReq())
    monkeypatch.setattr(frappe, "get_request_header", lambda k: DummyReq.headers.get(k))

    resp = frappe.get_attr("jarz_woocommerce_integration.jarz_woocommerce_integration.api.orders.woo_order_webhook")()  # type: ignore
    assert resp.get("queued") is True
