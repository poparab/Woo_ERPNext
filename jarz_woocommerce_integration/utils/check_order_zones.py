"""Check if WooCommerce orders contain delivery zone information."""
import frappe
from jarz_woocommerce_integration.utils.http_client import WooClient
from jarz_woocommerce_integration.jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import WooCommerceSettings
import json


def check_order_zones_cli():
    """Check a sample order for delivery zone data."""
    settings = WooCommerceSettings.get_settings()
    client = WooClient(
        base_url=settings.base_url,
        consumer_key=settings.consumer_key,
        consumer_secret=settings.get_password("consumer_secret"),
        api_version=settings.api_version or "v3",
    )
    
    # Fetch a recent order
    orders = client.list("/orders", params={"per_page": 1, "page": 1})
    if not orders:
        print("No orders found")
        return
    
    order = orders[0]
    order_id = order.get("id")
    
    print(f"\n📦 Checking Order #{order_id}")
    print(f"\n📍 Shipping Address:")
    shipping = order.get("shipping", {})
    for key, val in shipping.items():
        print(f"  {key}: {val}")
    
    print(f"\n🚚 Shipping Lines:")
    for line in order.get("shipping_lines", []):
        print(f"  Method: {line.get('method_title')}")
        print(f"  Method ID: {line.get('method_id')}")
        print(f"  Total: {line.get('total')}")
        if line.get("meta_data"):
            print(f"  Meta Data:")
            for meta in line.get("meta_data", []):
                print(f"    {meta.get('key')}: {meta.get('value')}")
    
    print(f"\n🏷️  Order Meta Data (zone-related):")
    for meta in order.get("meta_data", []):
        key = str(meta.get("key", "")).lower()
        if "zone" in key or "delivery" in key or "area" in key:
            print(f"  {meta.get('key')}: {meta.get('value')}")
    
    # Check customer data
    print(f"\n👤 Customer ID: {order.get('customer_id')}")
    if order.get("customer_id"):
        try:
            customer = client.get(f"/customers/{order.get('customer_id')}")
            print(f"  Email: {customer.get('email')}")
            print(f"  Meta Data:")
            for meta in customer.get("meta_data", []):
                key = str(meta.get("key", "")).lower()
                if "zone" in key or "delivery" in key or "area" in key:
                    print(f"    {meta.get('key')}: {meta.get('value')}")
        except Exception as e:
            print(f"  Could not fetch customer: {e}")
    
    return order
