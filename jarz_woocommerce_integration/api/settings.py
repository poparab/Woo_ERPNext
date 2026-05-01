import frappe
from frappe import _
from frappe.utils.password import get_decrypted_password

from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
    WooCommerceSettings,
)
from jarz_woocommerce_integration.services.order_sync import (
    _minutes_ago_for_woo,
    pull_recent_orders_phase1,
)
from jarz_woocommerce_integration.utils.http_client import WooAPIError, WooClient


ORDER_WEBHOOK_TOPICS = ("order.created", "order.updated")
PRIMARY_ORDER_WEBHOOK_METHOD = "jarz_woocommerce_integration.api.orders.woo_order_webhook"
LEGACY_ORDER_WEBHOOK_METHOD = "jarz_woocommerce_integration.api.webhook.order_webhook"


@frappe.whitelist(allow_guest=False)
def test_connection(base_url: str | None = None, consumer_key: str | None = None, consumer_secret: str | None = None, api_version: str = "v3"):
    """Test connectivity & credentials against WooCommerce REST API.

    Args:
        base_url: Store base URL (e.g. https://example.com)
        consumer_key: WooCommerce REST API consumer key
        consumer_secret: WooCommerce REST API consumer secret
        api_version: API version (default v3)

    Returns:
        dict: { success: bool, store_info: {...}, rate_limit: {...} }
    """
    if not (base_url and consumer_key and consumer_secret):
        frappe.throw(_("Missing required parameters: base_url, consumer_key, consumer_secret"))

    client = WooClient(
        base_url=base_url.strip().rstrip('/'),
        consumer_key=consumer_key.strip(),
        consumer_secret=consumer_secret.strip(),
        api_version=api_version.lower(),
    )

    try:
        status = client.get("system_status")  # lightweight endpoint (may be blocked on some sites)
    except WooAPIError as e:
        # Fallback: attempt to fetch a single order to at least validate credentials
        if e.status_code in {401, 403}:
            frappe.throw(_(f"Authentication failed: {e.message}"))
        fallback_info = {}
        try:
            orders = client.list_orders(per_page=1)
            fallback_info = {"orders_endpoint_access": True, "orders_sample_count": len(orders)}
        except Exception:  # noqa: BLE001
            frappe.throw(_(f"WooCommerce API Error (system_status & orders failed): {e.message}"))
        status = {"environment": {}, "active_theme": {}, **fallback_info}
    except Exception as e:  # noqa: BLE001
        frappe.log_error(title="WooCommerce Connection Error", message=frappe.get_traceback())
        frappe.throw(_(f"Unexpected error connecting to WooCommerce: {e}"))

    return {
        "success": True,
        "store_info": {
            "version": status.get("environment", {}).get("version"),
            "home_url": status.get("environment", {}).get("home_url"),
            "site_url": status.get("environment", {}).get("site_url"),
            "wc_version": status.get("environment", {}).get("wc_version"),
            "theme": status.get("active_theme", {}).get("name"),
        },
    }


@frappe.whitelist(allow_guest=False)
def test_saved_connection():
    """Test WooCommerce connectivity using credentials stored in WooCommerce Settings."""

    settings = WooCommerceSettings.get_settings()
    secret = get_decrypted_password("WooCommerce Settings", settings.name, "consumer_secret")
    if not (getattr(settings, "base_url", None) and getattr(settings, "consumer_key", None) and secret):
        frappe.throw(_("WooCommerce Settings are missing base_url, consumer_key, or consumer_secret"))

    return test_connection(
        base_url=settings.base_url,
        consumer_key=settings.consumer_key,
        consumer_secret=secret,
        api_version=getattr(settings, "api_version", "v3") or "v3",
    )


def _get_saved_client() -> tuple[WooCommerceSettings, WooClient]:
    settings = WooCommerceSettings.get_settings()
    secret = get_decrypted_password("WooCommerce Settings", settings.name, "consumer_secret")
    if not (getattr(settings, "base_url", None) and getattr(settings, "consumer_key", None) and secret):
        frappe.throw(_("WooCommerce Settings are missing base_url, consumer_key, or consumer_secret"))

    client = WooClient(
        base_url=settings.base_url,
        consumer_key=settings.consumer_key,
        consumer_secret=secret,
        api_version=getattr(settings, "api_version", "v3") or "v3",
    )
    return settings, client


def _get_saved_webhook_secret(settings: WooCommerceSettings) -> str:
    try:
        secret = get_decrypted_password("WooCommerce Settings", settings.name, "webhook_secret") or ""
    except Exception:  # noqa: BLE001
        secret = ""
    if not secret:
        secret = getattr(settings, "webhook_secret", "") or ""
    return str(secret).strip()


def _get_site_host_name() -> str:
    host_name = str(getattr(frappe.conf, "host_name", "") or "").rstrip("/")
    if not host_name:
        try:
            host_name = str((frappe.get_site_config() or {}).get("host_name", "") or "").rstrip("/")
        except Exception:  # noqa: BLE001
            host_name = ""
    if not host_name:
        frappe.throw(_("Site host_name is not configured"))
    return host_name


def _build_delivery_url(method_path: str) -> str:
    return f"{_get_site_host_name()}/api/method/{method_path}"


def _get_expected_order_delivery_urls() -> list[str]:
    return [
        _build_delivery_url(PRIMARY_ORDER_WEBHOOK_METHOD),
        _build_delivery_url(LEGACY_ORDER_WEBHOOK_METHOD),
    ]


def _list_order_webhooks(client: WooClient) -> list[dict]:
    data = client.get("webhooks", params={"per_page": 100})
    if not isinstance(data, list):
        return []
    return [row for row in data if isinstance(row, dict) and row.get("topic") in ORDER_WEBHOOK_TOPICS]


def _get_order_webhook_status_data(client: WooClient) -> dict:
    expected_urls = {url.rstrip("/") for url in _get_expected_order_delivery_urls()}
    primary_url = _build_delivery_url(PRIMARY_ORDER_WEBHOOK_METHOD)
    webhooks = _list_order_webhooks(client)

    topics: dict[str, dict] = {}
    for topic in ORDER_WEBHOOK_TOPICS:
        matching = [
            row for row in webhooks
            if row.get("topic") == topic and str(row.get("delivery_url") or "").rstrip("/") in expected_urls
        ]
        active_match = next(
            (row for row in matching if str(row.get("status") or "").lower() == "active"),
            None,
        )
        topics[topic] = {
            "active": active_match is not None,
            "webhook_id": active_match.get("id") if active_match else (matching[0].get("id") if matching else None),
            "delivery_url": active_match.get("delivery_url") if active_match else (matching[0].get("delivery_url") if matching else None),
            "status": active_match.get("status") if active_match else (matching[0].get("status") if matching else None),
            "matches": matching,
        }

    return {
        "expected_delivery_url": primary_url,
        "accepted_delivery_urls": sorted(expected_urls),
        "all_required_active": all(topics[topic]["active"] for topic in ORDER_WEBHOOK_TOPICS),
        "topics": topics,
        "webhooks": webhooks,
    }


@frappe.whitelist(allow_guest=False)
def get_order_webhook_status():
    """Inspect required Woo order webhooks on the connected store."""
    _settings, client = _get_saved_client()
    data = _get_order_webhook_status_data(client)
    return {"success": True, **data}


@frappe.whitelist(allow_guest=False)
def ensure_order_webhooks():
    """Create or reactivate the required Woo order.created and order.updated webhooks."""
    settings, client = _get_saved_client()
    secret = _get_saved_webhook_secret(settings)
    if not secret:
        frappe.throw(_("Webhook secret is not configured in WooCommerce Settings"))

    target_url = _build_delivery_url(PRIMARY_ORDER_WEBHOOK_METHOD)
    status_data = _get_order_webhook_status_data(client)

    for topic in ORDER_WEBHOOK_TOPICS:
        topic_data = status_data["topics"][topic]
        if topic_data["active"] and str(topic_data["delivery_url"] or "").rstrip("/") == target_url.rstrip("/"):
            continue

        payload = {
            "name": f"Jarz ERP {topic}",
            "topic": topic,
            "delivery_url": target_url,
            "secret": secret,
            "status": "active",
        }
        webhook_id = topic_data.get("webhook_id")
        if webhook_id:
            client.put(f"webhooks/{webhook_id}", data=payload)
        else:
            client.post("webhooks", data=payload)

    return get_order_webhook_status()


@frappe.whitelist(allow_guest=False)
def validate_inbound_setup():
    """Validate the inbound Woo order pipeline prerequisites and run a tiny dry-run poll probe."""
    settings, client = _get_saved_client()
    webhook_status = _get_order_webhook_status_data(client)

    scheduled_jobs = frappe.get_all(
        "Scheduled Job Type",
        filters={
            "method": [
                "in",
                [
                    "jarz_woocommerce_integration.services.order_sync.sync_orders_cron_phase1",
                    "jarz_woocommerce_integration.services.order_sync.sync_cancelled_orders_cron",
                    "jarz_woocommerce_integration.services.order_sync.reconcile_recent_orders_cron",
                ],
            ]
        },
        fields=["name", "method", "stopped"],
    )

    dry_run_probe = pull_recent_orders_phase1(
        limit=1,
        dry_run=True,
        allow_update=True,
        modified_after=_minutes_ago_for_woo(60),
        orderby="modified",
        order="desc",
        max_pages=1,
    )

    return {
        "success": True,
        "enable_inbound_orders": bool(getattr(settings, "enable_inbound_orders", 0)),
        "webhook_secret_set": bool(_get_saved_webhook_secret(settings)),
        "required_webhooks_active": webhook_status["all_required_active"],
        "webhook_status": webhook_status,
        "scheduled_jobs": scheduled_jobs,
        "dry_run_probe": dry_run_probe,
    }
