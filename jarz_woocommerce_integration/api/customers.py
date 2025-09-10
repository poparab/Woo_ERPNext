import frappe
from jarz_woocommerce_integration.services.customer_sync import sync_customers
from jarz_woocommerce_integration.jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (  # standardized nested path
    WooCommerceSettings,
)
from jarz_woocommerce_integration.utils.http_client import WooClient


@frappe.whitelist(allow_guest=False)
def pull_customers(batch_size: int = 100, full_refresh: int | bool = 0):
    """Pull WooCommerce customers.

    Incremental by default (creation date after last_synced_customer_created). Use
    full_refresh=1 to ignore high-water mark and reprocess existing customers (e.g. to backfill addresses).

    Args:
        batch_size: per_page size when calling Woo API (max 100 recommended)
        full_refresh: 1/true to rescan all pages from start
    """
    batch_size = min(int(batch_size), 100)
    return {"success": True, "data": sync_customers(batch_size=batch_size, full_refresh=full_refresh)}


@frappe.whitelist(allow_guest=False)
def pull_customers_full(batch_size: int = 100):
    """Convenience endpoint to run a full refresh (ignores high-water mark)."""
    batch_size = min(int(batch_size), 100)
    return {"success": True, "data": sync_customers(batch_size=batch_size, full_refresh=1)}


@frappe.whitelist(allow_guest=False)
def pull_customers_full_debug(batch_size: int = 50):
    """Full refresh with debug samples (first 5 customers address decisions)."""
    batch_size = min(int(batch_size), 100)
    return {"success": True, "data": sync_customers(batch_size=batch_size, full_refresh=1, debug=1)}


@frappe.whitelist(allow_guest=False)
def backfill_customer_ids(batch_size: int = 100):
    """Full refresh specifically for backfilling custom_woo_customer_id.

    Same as pull_customers_full but explicitly named for clarity during rollout.
    Returns the standard sync metrics.
    """
    batch_size = min(int(batch_size), 100)
    return {"success": True, "data": sync_customers(batch_size=batch_size, full_refresh=1)}


@frappe.whitelist(allow_guest=False)
def debug_customer(email: str):
    """Return Woo raw customer (first match) and ERPNext objects for a given email."""
    email = (email or "").strip().lower()
    if not email:
        frappe.throw("email required")
    settings = WooCommerceSettings.get_settings()
    client = WooClient(
        base_url=settings.base_url,
        consumer_key=settings.consumer_key,
        consumer_secret=settings.decrypted_consumer_secret,
        api_version=settings.api_version or "v3",
    )
    # Woo doesn't have direct lookup by email param in core; fetch small pages and scan
    page = 1
    woo_customer = None
    while page <= 5 and not woo_customer:  # limit scan
        customers = client.list_customers(params={"per_page": 50, "page": page})
        if not customers:
            break
        for c in customers:
            if (c.get("email") or "").strip().lower() == email:
                woo_customer = c
                break
        if len(customers) < 50:
            break
        page += 1
    erp_customer = None
    erp_addresses = []
    if frappe.db.exists("Customer", {"email_id": email}):
        cname = frappe.db.get_value("Customer", {"email_id": email}, "name")
        erp_customer = frappe.get_doc("Customer", cname).as_dict()
        addr_links = frappe.get_all(
            "Dynamic Link",
            filters={
                "link_doctype": "Customer",
                "link_name": cname,
                "parenttype": "Address",
            },
            fields=["parent"],
        )
        for al in addr_links:
            try:
                erp_addresses.append(frappe.get_doc("Address", al.parent).as_dict())
            except Exception:  # noqa: BLE001
                pass
    return {"success": True, "data": {"woo": woo_customer, "erp_customer": erp_customer, "erp_addresses": erp_addresses}}


@frappe.whitelist(allow_guest=False)
def customer_field_summary(limit: int = 20):
    """Return a summary of top-level and nested customer fields from WooCommerce.

    Args:
        limit: number of customers to sample (pagination first pages)
    """
    settings = WooCommerceSettings.get_settings()
    client = WooClient(
        base_url=settings.base_url,
        consumer_key=settings.consumer_key,
        consumer_secret=settings.decrypted_consumer_secret,
        api_version=settings.api_version or "v3",
    )
    collected: dict[str, set] = {}
    page = 1
    remaining = int(limit)
    while remaining > 0:
        batch_size =  min(remaining, 100)
        customers = client.list_customers(params={"per_page": batch_size, "page": page})
        if not customers:
            break
        for c in customers:
            for k, v in c.items():
                if isinstance(v, (dict, list)):
                    # Just note its type; nested expansion kept separate
                    collected.setdefault(k, set()).add(type(v).__name__)
                else:
                    collected.setdefault(k, set()).add(type(v).__name__)
        remaining -= len(customers)
        if len(customers) < batch_size:
            break
        page += 1

    # Convert to sorted lists
    summary = {k: sorted(list(v)) for k, v in sorted(collected.items())}
    return {"success": True, "data": {"field_types": summary}}
