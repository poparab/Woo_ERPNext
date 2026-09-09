import unittest
import base64
import hashlib
import hmac
import json
from types import SimpleNamespace

import frappe

from jarz_woocommerce_integration.api import orders, webhooks
from jarz_woocommerce_integration.tests._monkeypatch import MonkeyPatch


def compute_sig(secret: str, payload: bytes) -> str:
    return base64.b64encode(hmac.new(secret.encode(), payload, hashlib.sha256).digest()).decode()


def resolve_webhook_secret(settings) -> str:
    try:
        from frappe.utils.password import get_decrypted_password
    except Exception:  # noqa: BLE001
        get_decrypted_password = None

    if get_decrypted_password:
        try:
            secret = get_decrypted_password("WooCommerce Settings", settings.name, "webhook_secret") or ""
            if secret:
                return secret
        except Exception:  # noqa: BLE001
            pass

    return getattr(settings, "webhook_secret", None) or "testsecret"


class TestOrderWebhook(unittest.TestCase):
    """Webhook signature handling and inbox-event vs direct-enqueue dispatch."""

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)

    def test_order_webhook_ack(self):
        # Simulate handshake (no id) should ACK even without signature
        resp = frappe.get_attr("jarz_woocommerce_integration.jarz_woocommerce_integration.api.orders.woo_order_webhook")()  # type: ignore
        assert resp.get("ack") is True

    def test_order_webhook_process(self):  # pragma: no cover - environment dependent
        settings = frappe.get_single("WooCommerce Settings")
        secret = resolve_webhook_secret(settings)
        order_payload = {"id": 999999, "status": "processing", "line_items": []}
        raw = json.dumps(order_payload).encode()
        sig = compute_sig(secret, raw)

        # self.monkeypatch request context
        class DummyReq:
            data = raw
            headers = {"X-WC-Webhook-Signature": sig}
            path = "/api/method/jarz_woocommerce_integration.api.orders.woo_order_webhook"

        self.monkeypatch.setattr(frappe, "request", DummyReq())
        self.monkeypatch.setattr(frappe, "get_request_header", lambda k: DummyReq.headers.get(k))

        resp = frappe.get_attr("jarz_woocommerce_integration.jarz_woocommerce_integration.api.orders.woo_order_webhook")()  # type: ignore
        assert resp.get("queued") is True

    def test_order_webhook_uses_inbox_event_when_enabled(self):
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

        self.monkeypatch.setattr(orders.WooCommerceSettings, "get_settings", lambda: SimpleNamespace(name="WooCommerce Settings", webhook_secret=secret))
        self.monkeypatch.setattr(orders, "create_sync_log_entry", lambda *args, **kwargs: receipt_log)
        self.monkeypatch.setattr(orders, "finish_sync_log_entry", lambda *args, **kwargs: None)
        self.monkeypatch.setattr(orders.frappe, "request", DummyReq())
        self.monkeypatch.setattr(orders.frappe, "get_request_header", lambda k: DummyReq.headers.get(k))
        self.monkeypatch.setattr(orders.frappe, "logger", lambda *args, **kwargs: fake_logger)
        self.monkeypatch.setattr(orders.frappe, "db", fake_db)
        self.monkeypatch.setattr(orders.frappe, "local", SimpleNamespace(response=SimpleNamespace(http_status_code=200)))
        self.monkeypatch.setattr(orders.frappe, "form_dict", {})
        self.monkeypatch.setattr(orders.frappe, "enqueue", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("direct enqueue should not run")))
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.should_use_order_webhook_inbox", lambda settings: True)
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.create_inbound_order_event", lambda *args, **kwargs: SimpleNamespace(name="WOOEVT-00021"))
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.is_shadow_mode_enabled", lambda settings: False)
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.enqueue_sync_event", lambda *args, **kwargs: event_calls.append((args, kwargs)))

        resp = orders.woo_order_webhook()

        assert resp == {"success": True, "queued": True, "event_name": "WOOEVT-00021"}
        assert event_calls[0][0][0] == "WOOEVT-00021"

    def test_order_webhook_shadow_insert_failure_keeps_direct_enqueue(self):
        secret = "testsecret"
        order_payload = {"id": 123457, "status": "processing", "line_items": []}
        raw = json.dumps(order_payload).encode()
        sig = compute_sig(secret, raw)
        receipt_log = SimpleNamespace(db_set=lambda *args, **kwargs: None)
        enqueue_calls = []
        shadow_failures = []

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

        self.monkeypatch.setattr(orders.WooCommerceSettings, "get_settings", lambda: SimpleNamespace(name="WooCommerce Settings", webhook_secret=secret))
        self.monkeypatch.setattr(orders, "create_sync_log_entry", lambda *args, **kwargs: receipt_log)
        self.monkeypatch.setattr(orders, "finish_sync_log_entry", lambda *args, **kwargs: None)
        self.monkeypatch.setattr(orders.frappe, "request", DummyReq())
        self.monkeypatch.setattr(orders.frappe, "get_request_header", lambda k: DummyReq.headers.get(k))
        self.monkeypatch.setattr(orders.frappe, "logger", lambda *args, **kwargs: fake_logger)
        self.monkeypatch.setattr(orders.frappe, "db", fake_db)
        self.monkeypatch.setattr(orders.frappe, "local", SimpleNamespace(response=SimpleNamespace(http_status_code=200)))
        self.monkeypatch.setattr(orders.frappe, "form_dict", {})
        self.monkeypatch.setattr(orders.frappe, "enqueue", lambda *args, **kwargs: enqueue_calls.append((args, kwargs)))
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.should_use_order_webhook_inbox", lambda settings: True)
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.create_inbound_order_event", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("shadow insert failed")))
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.is_shadow_mode_enabled", lambda settings: True)
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.report_shadow_insert_failure", lambda *args, **kwargs: shadow_failures.append((args, kwargs)))
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.enqueue_sync_event", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("event enqueue should not run")))

        resp = orders.woo_order_webhook()

        assert resp.get("queued") is True
        assert resp.get("job_name", "").startswith("woo_order_123457")
        assert len(enqueue_calls) == 1
        assert len(shadow_failures) == 1

    def test_customer_webhook_uses_inbox_event_when_enabled(self):
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

        self.monkeypatch.setattr(webhooks.WooCommerceSettings, "get_settings", lambda: SimpleNamespace(name="WooCommerce Settings", webhook_secret=secret))
        self.monkeypatch.setattr(webhooks.frappe, "request", DummyReq())
        self.monkeypatch.setattr(webhooks.frappe, "get_request_header", lambda k: DummyReq.headers.get(k))
        self.monkeypatch.setattr(webhooks.frappe, "logger", lambda *args, **kwargs: fake_logger)
        self.monkeypatch.setattr(webhooks.frappe, "db", fake_db)
        self.monkeypatch.setattr(webhooks.frappe, "local", SimpleNamespace(response=SimpleNamespace(http_status_code=200)))
        self.monkeypatch.setattr(webhooks.frappe, "form_dict", {})
        self.monkeypatch.setattr(webhooks.frappe, "enqueue", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("direct enqueue should not run")))
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.should_use_customer_webhook_inbox", lambda settings: True)
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.create_inbound_customer_event", lambda *args, **kwargs: SimpleNamespace(name="WOOEVT-00022"))
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.is_shadow_mode_enabled", lambda settings: False)
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.enqueue_sync_event", lambda *args, **kwargs: event_calls.append((args, kwargs)))

        resp = webhooks.woo_customer_webhook()

        assert resp == {"success": True, "queued": True, "event_name": "WOOEVT-00022"}
        assert event_calls[0][0][0] == "WOOEVT-00022"

    def test_customer_webhook_shadow_insert_failure_keeps_direct_enqueue(self):
        secret = "testsecret"
        customer_payload = {"id": 778, "email": "test@example.com"}
        raw = json.dumps(customer_payload).encode()
        sig = compute_sig(secret, raw)
        enqueue_calls = []
        shadow_failures = []

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

        self.monkeypatch.setattr(webhooks.WooCommerceSettings, "get_settings", lambda: SimpleNamespace(name="WooCommerce Settings", webhook_secret=secret))
        self.monkeypatch.setattr(webhooks.frappe, "request", DummyReq())
        self.monkeypatch.setattr(webhooks.frappe, "get_request_header", lambda k: DummyReq.headers.get(k))
        self.monkeypatch.setattr(webhooks.frappe, "logger", lambda *args, **kwargs: fake_logger)
        self.monkeypatch.setattr(webhooks.frappe, "db", fake_db)
        self.monkeypatch.setattr(webhooks.frappe, "local", SimpleNamespace(response=SimpleNamespace(http_status_code=200)))
        self.monkeypatch.setattr(webhooks.frappe, "form_dict", {})
        self.monkeypatch.setattr(webhooks.frappe, "enqueue", lambda *args, **kwargs: enqueue_calls.append((args, kwargs)))
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.should_use_customer_webhook_inbox", lambda settings: True)
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.create_inbound_customer_event", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("shadow insert failed")))
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.is_shadow_mode_enabled", lambda settings: True)
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.report_shadow_insert_failure", lambda *args, **kwargs: shadow_failures.append((args, kwargs)))
        self.monkeypatch.setattr("jarz_woocommerce_integration.services.sync_events.enqueue_sync_event", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("event enqueue should not run")))

        resp = webhooks.woo_customer_webhook()

        assert resp.get("queued") is True
        assert resp.get("job_name", "").startswith("woo_customer_778")
        assert len(enqueue_calls) == 1
        assert len(shadow_failures) == 1
