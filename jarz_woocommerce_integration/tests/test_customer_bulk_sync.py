import frappe
import unittest
from unittest.mock import patch

from jarz_woocommerce_integration.services import customer_bulk_sync


def test_sync_all_customers_api_importable():
    fn = frappe.get_attr("jarz_woocommerce_integration.jarz_woocommerce_integration.api.customers.sync_all")
    assert callable(fn)


class TestCustomerBulkSync(unittest.TestCase):
    def test_sync_single_customer_passes_woo_customer_id_to_ensure_customer(self):
        payload = {
            "id": 42,
            "email": "test@example.com",
            "username": "woo-user",
            "first_name": "Test",
            "last_name": "Customer",
            "billing": {},
            "shipping": {},
        }

        with patch.object(customer_bulk_sync, "_ensure_customer", return_value="CUST-0001") as ensure_customer:
            result = customer_bulk_sync._sync_single_customer(payload)

        ensure_customer.assert_called_once()
        self.assertEqual(ensure_customer.call_args.kwargs["woo_customer_id"], 42)
        self.assertEqual(result, {"customer": "CUST-0001", "billing": None, "shipping": None})

    def test_sync_single_customer_accepts_line2_only_address(self):
        payload = {
            "id": 42,
            "email": "test@example.com",
            "first_name": "Test",
            "last_name": "Customer",
            "billing": {"address_1": "", "address_2": "Apartment 4", "city": "Cairo"},
            "shipping": {},
        }

        with patch.object(customer_bulk_sync, "_ensure_customer", return_value="CUST-0001"), \
             patch.object(customer_bulk_sync, "_find_existing_address_for_customer", return_value=None) as find_existing, \
             patch.object(customer_bulk_sync, "_create_address", return_value="ADDR-0001") as create_address:
            result = customer_bulk_sync._sync_single_customer(payload)

        find_existing.assert_called_once_with("CUST-0001", "Billing", payload["billing"])
        create_address.assert_called_once_with("CUST-0001", "Billing", payload["billing"], None, "test@example.com")
        self.assertEqual(result["billing"], "ADDR-0001")

    def test_sync_single_customer_requires_full_payload_before_reusing_billing_for_shipping(self):
        payload = {
            "id": 42,
            "email": "test@example.com",
            "first_name": "Test",
            "last_name": "Customer",
            "billing": {"address_1": "12 Road", "address_2": "", "city": "Cairo"},
            "shipping": {"address_1": "12 Road", "address_2": "", "city": "Giza"},
        }

        with patch.object(customer_bulk_sync, "_ensure_customer", return_value="CUST-0001"), \
             patch.object(customer_bulk_sync, "_find_existing_address_for_customer", return_value=None), \
             patch.object(customer_bulk_sync, "_create_address", side_effect=["ADDR-BILL", "ADDR-SHIP"]) as create_address:
            result = customer_bulk_sync._sync_single_customer(payload)

        self.assertEqual(create_address.call_count, 2)
        self.assertEqual(result["billing"], "ADDR-BILL")
        self.assertEqual(result["shipping"], "ADDR-SHIP")


class TestCustomerAddressCanonicalization(unittest.TestCase):
    def test_find_existing_address_for_customer_requires_full_payload_match(self):
        fake_db = unittest.mock.Mock()
        fake_db.sql.return_value = [
            {
                "name": "ADDR-1",
                "address_line1": "12 Road",
                "address_line2": "",
                "city": "Giza",
                "state": "",
                "pincode": "",
                "country": "Egypt",
            }
        ]

        from jarz_woocommerce_integration.services import customer_sync

        with patch.object(customer_sync.frappe, "db", fake_db), \
             patch.object(customer_sync, "_resolve_country", return_value="Egypt"):
            found = customer_sync._find_existing_address_for_customer(
                "CUST-1",
                "Billing",
                {"address_1": "12 Road", "address_2": "", "city": "Cairo", "country": "EG"},
            )

        self.assertIsNone(found)

    def test_find_existing_address_for_customer_accepts_line2_only_source_address(self):
        fake_db = unittest.mock.Mock()
        fake_db.sql.return_value = [
            {
                "name": "ADDR-1",
                "address_line1": "Apartment 4",
                "address_line2": "",
                "city": "Cairo",
                "state": "",
                "pincode": "",
                "country": "Egypt",
            }
        ]

        from jarz_woocommerce_integration.services import customer_sync

        with patch.object(customer_sync.frappe, "db", fake_db), \
             patch.object(customer_sync, "_resolve_country", return_value="Egypt"):
            found = customer_sync._find_existing_address_for_customer(
                "CUST-1",
                "Billing",
                {"address_1": "", "address_2": "Apartment 4", "city": "Cairo", "country": "EG"},
            )

        self.assertEqual(found, "ADDR-1")
