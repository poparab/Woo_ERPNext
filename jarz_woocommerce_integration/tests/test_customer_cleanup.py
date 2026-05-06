import unittest

from jarz_woocommerce_integration.services import customer_cleanup


class TestCustomerCleanupResolution(unittest.TestCase):
    def test_resolve_woo_customer_blocks_duplicate_canonical_group(self):
        indexes = {
            "by_name": {},
            "by_canonical_id": {"3357": ["CUST-1", "CUST-2"]},
            "by_phone": {},
            "by_email": {},
            "by_username": {},
        }

        result = customer_cleanup._resolve_woo_customer(
            {
                "id": 3357,
                "billing": {"phone": "01000000000"},
                "shipping": {},
                "email": "test@example.com",
                "username": "test@example.com",
            },
            indexes,
        )

        self.assertEqual(result["bucket"], "blocked_duplicate_woo_id")
        self.assertEqual(result["customers"], ["CUST-1", "CUST-2"])

    def test_resolve_woo_customer_prefers_unique_phone_merge(self):
        indexes = {
            "by_name": {},
            "by_canonical_id": {},
            "by_phone": {"01000000000": ["CUST-PHONE"]},
            "by_email": {"test@example.com": ["CUST-EMAIL"]},
            "by_username": {"test@example.com": ["CUST-USER"]},
        }

        result = customer_cleanup._resolve_woo_customer(
            {
                "id": 999,
                "billing": {"phone": "01000000000"},
                "shipping": {},
                "email": "test@example.com",
                "username": "test@example.com",
            },
            indexes,
        )

        self.assertEqual(result, {"bucket": "phone_merge", "customer": "CUST-PHONE"})

    def test_resolve_woo_customer_marks_email_only_conflict_as_blocked(self):
        indexes = {
            "by_name": {},
            "by_canonical_id": {},
            "by_phone": {},
            "by_email": {"test@example.com": ["CUST-EMAIL"]},
            "by_username": {},
        }

        result = customer_cleanup._resolve_woo_customer(
            {
                "id": 999,
                "billing": {},
                "shipping": {},
                "email": "test@example.com",
                "username": "",
            },
            indexes,
        )

        self.assertEqual(result["bucket"], "blocked_email_conflict")


class TestCustomerCleanupPlanning(unittest.TestCase):
    def test_plan_address_cleanup_keeps_one_row_per_signature_and_removes_stale(self):
        current_rows = [
            {
                "name": "ADDR-KEEP",
                "address_line1": "12 Road",
                "address_line2": "",
                "city": "Cairo",
                "state": "",
                "pincode": "",
                "country": "Egypt",
                "is_primary_address": 1,
                "is_shipping_address": 0,
            },
            {
                "name": "ADDR-DUP",
                "address_line1": "12 Road",
                "address_line2": "",
                "city": "Cairo",
                "state": "",
                "pincode": "",
                "country": "Egypt",
                "is_primary_address": 0,
                "is_shipping_address": 0,
            },
            {
                "name": "ADDR-OLD",
                "address_line1": "99 Old St",
                "address_line2": "",
                "city": "Giza",
                "state": "",
                "pincode": "",
                "country": "Egypt",
                "is_primary_address": 0,
                "is_shipping_address": 0,
            },
        ]
        desired_signatures = {
            ("12 road", "", "cairo", "", "", "egypt"): {
                "address_type": "Billing",
                "data": {"address_1": "12 Road", "city": "Cairo", "country": "EG"},
                "email": None,
                "phone": None,
            },
            ("20 Ave", "", "Alex", "", "", "Egypt"): {
                "address_type": "Shipping",
                "data": {"address_1": "20 Ave", "city": "Alex", "country": "EG"},
                "email": None,
                "phone": None,
            },
        }
        normalized_desired = {
            customer_cleanup._source_address_signature(value["data"]): value
            for value in desired_signatures.values()
        }

        plan = customer_cleanup._plan_address_cleanup(current_rows, normalized_desired)

        self.assertEqual(len(plan["duplicate_rows_to_retire"]), 1)
        self.assertEqual(plan["duplicate_rows_to_retire"][0]["name"], "ADDR-DUP")
        self.assertEqual(len(plan["extra_rows_to_retire"]), 1)
        self.assertEqual(plan["extra_rows_to_retire"][0]["name"], "ADDR-OLD")
        self.assertEqual(len(plan["missing_signatures"]), 1)

    def test_collect_desired_sources_reuses_same_signature_for_billing_and_shipping(self):
        payload = {
            "email": "test@example.com",
            "billing": {"address_1": "12 Road", "city": "Cairo", "country": "EG"},
            "shipping": {"address_1": "12 Road", "city": "Cairo", "country": "EG"},
        }

        desired = customer_cleanup._collect_desired_sources(payload)

        self.assertEqual(len(desired["signatures"]), 1)
        self.assertEqual(desired["default_billing_signature"], desired["default_shipping_signature"])