# Migrated test from nested package
from types import SimpleNamespace

from frappe.tests.utils import FrappeTestCase

from jarz_woocommerce_integration.api import settings as settings_api


class TestWooCommerceSettings(FrappeTestCase):
    pass


def test_get_order_webhook_status_data_flags_missing_topic(monkeypatch):
    primary_url = "https://erp.example.com/api/method/jarz_woocommerce_integration.api.orders.woo_order_webhook"
    legacy_url = "https://erp.example.com/api/method/jarz_woocommerce_integration.api.webhook.order_webhook"

    monkeypatch.setattr(settings_api, "_get_expected_order_delivery_urls", lambda: [primary_url, legacy_url])
    monkeypatch.setattr(
        settings_api,
        "_build_delivery_url",
        lambda method: primary_url if method == settings_api.PRIMARY_ORDER_WEBHOOK_METHOD else legacy_url,
    )
    monkeypatch.setattr(
        settings_api,
        "_list_order_webhooks",
        lambda client: [
            {"id": 11, "topic": "order.created", "delivery_url": primary_url, "status": "active"},
            {"id": 12, "topic": "order.updated", "delivery_url": primary_url, "status": "paused"},
        ],
    )

    data = settings_api._get_order_webhook_status_data(object())

    assert data["topics"]["order.created"]["active"] is True
    assert data["topics"]["order.updated"]["active"] is False
    assert data["all_required_active"] is False


def test_ensure_order_webhooks_repairs_and_creates(monkeypatch):
    primary_url = "https://erp.example.com/api/method/jarz_woocommerce_integration.api.orders.woo_order_webhook"
    legacy_url = "https://erp.example.com/api/method/jarz_woocommerce_integration.api.webhook.order_webhook"

    class DummyClient:
        def __init__(self):
            self.put_calls = []
            self.post_calls = []

        def put(self, resource, data):
            self.put_calls.append((resource, data))
            return {}

        def post(self, resource, data):
            self.post_calls.append((resource, data))
            return {}

    dummy_client = DummyClient()
    settings = SimpleNamespace(name="WooCommerce Settings")

    monkeypatch.setattr(settings_api, "_get_saved_client", lambda: (settings, dummy_client))
    monkeypatch.setattr(settings_api, "_get_saved_webhook_secret", lambda _settings: "supersecret")
    monkeypatch.setattr(
        settings_api,
        "_build_delivery_url",
        lambda method: primary_url if method == settings_api.PRIMARY_ORDER_WEBHOOK_METHOD else legacy_url,
    )
    monkeypatch.setattr(
        settings_api,
        "_get_order_webhook_status_data",
        lambda client: {
            "topics": {
                "order.created": {"active": False, "webhook_id": 21, "delivery_url": legacy_url, "status": "paused"},
                "order.updated": {"active": False, "webhook_id": None, "delivery_url": None, "status": None},
            }
        },
    )
    monkeypatch.setattr(settings_api, "get_order_webhook_status", lambda: {"success": True, "all_required_active": True})

    result = settings_api.ensure_order_webhooks()

    assert dummy_client.put_calls == [
        (
            "webhooks/21",
            {
                "name": "Jarz ERP order.created",
                "topic": "order.created",
                "delivery_url": primary_url,
                "secret": "supersecret",
                "status": "active",
            },
        )
    ]
    assert dummy_client.post_calls == [
        (
            "webhooks",
            {
                "name": "Jarz ERP order.updated",
                "topic": "order.updated",
                "delivery_url": primary_url,
                "secret": "supersecret",
                "status": "active",
            },
        )
    ]
    assert result == {"success": True, "all_required_active": True}


def test_validate_inbound_setup_uses_shared_probe_and_reports_jobs(monkeypatch):
    settings = SimpleNamespace(enable_inbound_orders=1)
    scheduled_jobs = [
        {
            "name": "order_sync.sync_orders_cron_phase1",
            "method": "jarz_woocommerce_integration.services.order_sync.sync_orders_cron_phase1",
            "stopped": 0,
        },
        {
            "name": "order_sync.reconcile_recent_orders_cron",
            "method": "jarz_woocommerce_integration.services.order_sync.reconcile_recent_orders_cron",
            "stopped": 0,
        },
    ]
    webhook_status = {"all_required_active": True, "topics": {}}
    captured = {}

    monkeypatch.setattr(settings_api, "_get_saved_client", lambda: (settings, object()))
    monkeypatch.setattr(settings_api, "_get_saved_webhook_secret", lambda _settings: "supersecret")
    monkeypatch.setattr(settings_api, "_get_order_webhook_status_data", lambda client: webhook_status)
    monkeypatch.setattr(settings_api.frappe, "get_all", lambda *args, **kwargs: scheduled_jobs)
    monkeypatch.setattr(settings_api, "_minutes_ago_for_woo", lambda minutes: "2026-05-01T12:51:04Z")

    def fake_pull_recent_orders_phase1(**kwargs):
        captured.update(kwargs)
        return {"orders_fetched": 0, "processed": 0, "errors": 0}

    monkeypatch.setattr(settings_api, "pull_recent_orders_phase1", fake_pull_recent_orders_phase1)

    result = settings_api.validate_inbound_setup()

    assert captured["modified_after"] == "2026-05-01T12:51:04Z"
    assert captured["dry_run"] is True
    assert captured["orderby"] == "modified"
    assert result["enable_inbound_orders"] is True
    assert result["required_webhooks_active"] is True
    assert result["scheduled_jobs"] == scheduled_jobs
    assert result["dry_run_probe"] == {"orders_fetched": 0, "processed": 0, "errors": 0}

