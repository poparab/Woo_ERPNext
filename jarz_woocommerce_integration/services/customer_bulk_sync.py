from __future__ import annotations

import time
from typing import Dict, Any

import frappe

from jarz_woocommerce_integration.utils.http_client import WooClient
from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
    WooCommerceSettings,
)
from jarz_woocommerce_integration.services.customer_sync import (
    _ensure_customer,
    _create_address,
    _find_existing_address_for_customer,
)


def _init_client(settings: WooCommerceSettings) -> WooClient:
    return WooClient(
        base_url=settings.base_url.rstrip("/"),
        consumer_key=settings.consumer_key,
        consumer_secret=settings.get_password("consumer_secret"),
        api_version=settings.api_version or "v3",
    )


def _sync_single_customer(cust: Dict[str, Any]) -> dict:
    email = cust.get("email")
    first_name = cust.get("first_name") or ""
    last_name = cust.get("last_name") or ""
    # Ensure customer
    customer_name = _ensure_customer(email, first_name, last_name, cust.get("id"))  # type: ignore[arg-type]

    # Addresses from Woo: billing + shipping aggregated under 'billing' & 'shipping'
    # Woo customer object: may include 'billing' and 'shipping' with address fields
    created_or_updated = {"customer": customer_name, "billing": None, "shipping": None}
    billing = cust.get("billing") or {}
    shipping = cust.get("shipping") or {}

    def _upsert_address(kind: str, data: dict):
        line1 = (data.get("address_1") or "").strip()
        if not line1:
            return None
        existing = _find_existing_address_for_customer(customer_name, kind.capitalize(), line1)
        if existing:
            return existing
        return _create_address(customer_name, kind.capitalize(), data, data.get("phone"), email)

    try:
        billing_addr = _upsert_address("billing", billing)
        shipping_addr = _upsert_address("shipping", shipping)
        created_or_updated["billing"] = billing_addr
        created_or_updated["shipping"] = shipping_addr
    except Exception as e:  # noqa: BLE001
        frappe.logger().warning({
            "event": "bulk_customer_address_error",
            "customer": customer_name,
            "woo_id": cust.get("id"),
            "error": str(e),
        })
    return created_or_updated


def sync_all_customers(per_page: int = 100, max_pages: int | None = None, sleep: float = 0.0) -> dict:
    """Pull and upsert ALL Woo customers.

    Args:
        per_page: Woo page size (max 100 typical)
        max_pages: optional safety cap (None = continue until empty)
        sleep: seconds to sleep between pages to reduce load
    Returns summary dict.
    """
    settings = WooCommerceSettings.get_settings()
    client = _init_client(settings)

    page = 1
    processed = 0
    created = 0  # currently we don't distinguish created vs existing easily
    results = []

    while True:
        if max_pages and page > max_pages:
            break
        data = client.list_customers(params={"per_page": per_page, "page": page, "orderby": "id", "order": "asc"})
        if not data:
            break
        for cust in data:
            res = _sync_single_customer(cust)
            results.append(res)
            processed += 1
        page += 1
        if sleep:
            time.sleep(sleep)

    return {
        "processed": processed,
        "approx_created_or_updated": processed,
        "sample": results[:5],
    }
