import unittest

from jarz_woocommerce_integration.services import customer_woo_id_migration


class TestCustomerWooIdMigration(unittest.TestCase):
    def test_migrate_customer_woo_ids_backfills_safe_legacy_only_rows(self):
        updates = []

        rows = [
            {
                "name": "CUST-0001",
                "woo_customer_id": None,
                "custom_woo_customer_id": 3095,
                "email_id": "test@example.com",
                "mobile_no": "01000000000",
                "phone": None,
            }
        ]

        with unittest.mock.patch.object(customer_woo_id_migration, "_load_customer_rows", return_value=rows), \
             unittest.mock.patch.object(customer_woo_id_migration, "has_legacy_customer_woo_id", return_value=True), \
             unittest.mock.patch.object(customer_woo_id_migration, "set_customer_woo_id", side_effect=lambda *args, **kwargs: updates.append((args, kwargs))), \
             unittest.mock.patch.object(customer_woo_id_migration.frappe.db, "commit"):
            result = customer_woo_id_migration.migrate_customer_woo_ids(dry_run=False, clear_legacy=True)

        self.assertEqual(result["updated"], 1)
        self.assertEqual(result["skipped_conflicts"], 0)
        self.assertEqual(result["duplicate_groups"], 0)
        self.assertEqual(updates[0][0][0], "CUST-0001")
        self.assertEqual(updates[0][0][1], "3095")

    def test_migrate_customer_woo_ids_skips_conflicts_and_duplicate_groups(self):
        rows = [
            {"name": "CUST-A", "woo_customer_id": "111", "custom_woo_customer_id": 222, "email_id": None, "mobile_no": None, "phone": None},
            {"name": "CUST-B", "woo_customer_id": None, "custom_woo_customer_id": 333, "email_id": None, "mobile_no": None, "phone": None},
            {"name": "CUST-C", "woo_customer_id": None, "custom_woo_customer_id": 333, "email_id": None, "mobile_no": None, "phone": None},
        ]

        with unittest.mock.patch.object(customer_woo_id_migration, "_load_customer_rows", return_value=rows), \
             unittest.mock.patch.object(customer_woo_id_migration, "has_legacy_customer_woo_id", return_value=True), \
             unittest.mock.patch.object(customer_woo_id_migration, "set_customer_woo_id") as set_customer_woo_id_mock:
            result = customer_woo_id_migration.migrate_customer_woo_ids(dry_run=True)

        self.assertEqual(result["updated"], 0)
        self.assertEqual(result["skipped_conflicts"], 1)
        self.assertEqual(result["duplicate_groups"], 1)
        self.assertEqual(result["skipped_duplicates"], 2)
        set_customer_woo_id_mock.assert_not_called()