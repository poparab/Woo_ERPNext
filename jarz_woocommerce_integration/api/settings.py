import frappe
from frappe import _
from jarz_woocommerce_integration.utils.http_client import WooClient, WooAPIError


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
