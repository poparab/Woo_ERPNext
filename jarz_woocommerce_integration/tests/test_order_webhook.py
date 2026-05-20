import base64
import hashlib
import hmac
import json
from types import SimpleNamespace

import frappe

from jarz_woocommerce_integration.api import orders, webhooks


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


def test_order_webhook_uses_inbox_event_when_enabled(monkeypatch):
    secret = "testsecret"
    order_payload = {"id": 123456, "status": "processing", "line_items": []}
    raw = json.dumps(order_payload).encode()
    sig = compute_sig(secret, raw)
    receipt_log = SimpleNamespace(db_set=lambda *args, **kwargs: None)
    event_calls = []

    class DummyReq:
        data = raw
        headers = {"X-WC-Webhook-Signature": sig}
        path = "/api/method/jarz_woocommerce_integration.api.orders.woo_order_webhook"

    fake_logger = SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
    )
    fake_db = SimpleNamespace(commit=lambda: None)

    monkeypatch.setattr(orders.WooCommerceSettings, "get_settings", lambda: SimpleNamespace(name="WooCommerce Settings", webhook_secret=secret))
    monkeypatch.setattr(orders, "create_sync_log_entry", lambda *args, **kwargs: receipt_log)
    monkeypatch.setattr(orders, "finish_sync_log_entry", lambda *args, **kwargs: None)
    monkeypatch.setattr(orders.frappe, "request", DummyReq())
    monkeypatch.setattr(orders.frappe, "get_request_header", lambda k: DummyReq.headers.get(k))
    monkeypatch.setattr(orders.frappe, "logger", lambda *args, **kwargs: fake_logger)
    monkeypatch.setattr(orders.frappe, "db", fake_db)
    monkeypatch.setattr(orders.frappe, "local", SimpleNamespace(response=SimpleNamespace(http_status_code=200)))
    monkeypatch.setattr(orders.frappe, "form_dict", {})
    monkeypatch.setattr(orders.frappe, "enqueue", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("direct enqueue should not run")))
    monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.should_use_order_webhook_inbox", lambda settings: True)
    monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.create_inbound_order_event", lambda *args, **kwargs: SimpleNamespace(name="WOOEVT-00021"))
    monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.is_shadow_mode_enabled", lambda settings: False)
    monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.enqueue_sync_event", lambda *args, **kwargs: event_calls.append((args, kwargs)))

    resp = orders.woo_order_webhook()

    assert resp == {"success": True, "queued": True, "event_name": "WOOEVT-00021"}
    assert event_calls[0][0][0] == "WOOEVT-00021"


def test_customer_webhook_uses_inbox_event_when_enabled(monkeypatch):
    secret = "testsecret"
    customer_payload = {"id": 777, "email": "test@example.com"}
    raw = json.dumps(customer_payload).encode()
    sig = compute_sig(secret, raw)
    event_calls = []

    class DummyReq:
        data = raw
        headers = {"X-WC-Webhook-Signature": sig}
        path = "/api/method/jarz_woocommerce_integration.api.webhooks.woo_customer_webhook"

    fake_logger = SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
    )
    fake_db = SimpleNamespace(commit=lambda: None)

    monkeypatch.setattr(webhooks.WooCommerceSettings, "get_settings", lambda: SimpleNamespace(name="WooCommerce Settings", webhook_secret=secret))
    monkeypatch.setattr(webhooks.frappe, "request", DummyReq())
    monkeypatch.setattr(webhooks.frappe, "get_request_header", lambda k: DummyReq.headers.get(k))
    monkeypatch.setattr(webhooks.frappe, "logger", lambda *args, **kwargs: fake_logger)
    monkeypatch.setattr(webhooks.frappe, "db", fake_db)
    monkeypatch.setattr(webhooks.frappe, "local", SimpleNamespace(response=SimpleNamespace(http_status_code=200)))
    monkeypatch.setattr(webhooks.frappe, "form_dict", {})
    monkeypatch.setattr(webhooks.frappe, "enqueue", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("direct enqueue should not run")))
    monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.should_use_customer_webhook_inbox", lambda settings: True)
    monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.create_inbound_customer_event", lambda *args, **kwargs: SimpleNamespace(name="WOOEVT-00022"))
    monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.is_shadow_mode_enabled", lambda settings: False)
    monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.enqueue_sync_event", lambda *args, **kwargs: event_calls.append((args, kwargs)))

    resp = webhooks.woo_customer_webhook()

    assert resp == {"success": True, "queued": True, "event_name": "WOOEVT-00022"}
    assert event_calls[0][0][0] == "WOOEVT-00022"
