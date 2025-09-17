import frappe


def test_sync_all_customers_api_importable():
    fn = frappe.get_attr("jarz_woocommerce_integration.jarz_woocommerce_integration.api.customers.sync_all")
    assert callable(fn)
