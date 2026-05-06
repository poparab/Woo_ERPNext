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
    def test_window_cleanup_uses_full_desired_state_for_phone_merged_customer(self):
        original_fetch = customer_cleanup._fetch_all_woo_customers
        original_load_customers = customer_cleanup._load_customer_rows
        original_load_addresses = customer_cleanup._load_address_rows

        try:
            customer_cleanup._fetch_all_woo_customers = lambda **kwargs: (
                [
                    {
                        "id": 100,
                        "email": "cust-a@example.com",
                        "username": "cust-a",
                        "billing": {"phone": "01000000000", "address_1": "12 Road", "city": "Cairo", "country": "EG"},
                        "shipping": {},
                    },
                    {
                        "id": 200,
                        "email": "cust-b@example.com",
                        "username": "cust-b",
                        "billing": {"phone": "01000000000", "address_1": "20 Ave", "city": "Giza", "country": "EG"},
                        "shipping": {},
                    },
                ],
                2,
                2,
            )
            customer_cleanup._load_customer_rows = lambda: [
                {
                    "name": "CUST-PHONE",
                    "disabled": 0,
                    "woo_customer_id": "100",
                    "mobile_no": "01000000000",
                    "custom_woo_customer_id": None,
                    "phone": None,
                    "email_id": "cust-a@example.com",
                    "woo_username": "cust-a",
                }
            ]
            customer_cleanup._load_address_rows = lambda: [
                {
                    "customer_name": "CUST-PHONE",
                    "name": "ADDR-1",
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
                    "customer_name": "CUST-PHONE",
                    "name": "ADDR-2",
                    "address_line1": "20 Ave",
                    "address_line2": "",
                    "city": "Giza",
                    "state": "",
                    "pincode": "",
                    "country": "Egypt",
                    "is_primary_address": 0,
                    "is_shipping_address": 1,
                },
            ]

            result = customer_cleanup.run_customer_cleanup(
                dry_run=True,
                per_page=1,
                start_page=1,
                max_pages=1,
            )

            self.assertEqual(result["customers_planned_for_cleanup"], 1)
            self.assertEqual(result["addresses_created"], 0)
            self.assertEqual(result["extra_rows_retired"], 0)
            self.assertEqual(result["duplicate_rows_retired"], 0)
            self.assertEqual(result["customers_with_address_changes"], 0)
        finally:
            customer_cleanup._fetch_all_woo_customers = original_fetch
            customer_cleanup._load_customer_rows = original_load_customers
            customer_cleanup._load_address_rows = original_load_addresses

    def test_fetch_customer_page_window_respects_start_and_max_pages(self):
        class DummyClient:
            def __init__(self):
                self.requested_pages = []

            def list_customers(self, params):
                page = params["page"]
                self.requested_pages.append(page)
                batches = {
                    3: [{"id": 301}, {"id": 302}],
                    4: [{"id": 401}],
                    5: [],
                }
                return batches.get(page, [])

        client = DummyClient()

        customers, pages_fetched, last_page_fetched = customer_cleanup._fetch_customer_page_window(
            client,
            per_page=2,
            start_page=3,
            max_pages=2,
        )

        self.assertEqual(client.requested_pages, [3, 4])
        self.assertEqual(customers, [{"id": 301}, {"id": 302}, {"id": 401}])
        self.assertEqual(pages_fetched, 2)
        self.assertEqual(last_page_fetched, 4)

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

    def test_collect_desired_sources_normalizes_phone_values(self):
        payload = {
            "email": "test@example.com",
            "billing": {
                "address_1": "12 Road",
                "city": "Cairo",
                "country": "EG",
                "phone": "+20\u00a0100\u00a0670\u00a09577",
            },
            "shipping": {},
        }

        desired = customer_cleanup._collect_desired_sources(payload)
        source = next(iter(desired["signatures"].values()))

        self.assertEqual(source["phone"], "+201006709577")