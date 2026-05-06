from types import SimpleNamespace
import unittest

from jarz_woocommerce_integration.api import orders, webhooks
from jarz_woocommerce_integration.services import customer_sync, outbound_sync
from jarz_woocommerce_integration.utils import customer_woo_id


class DummyCustomerClient:
    def __init__(self):
        self.put_calls = []
        self.post_calls = []

    def put(self, path, payload):
        self.put_calls.append((path, payload))
        return {"id": 3095}

    def post(self, path, payload):
        self.post_calls.append((path, payload))
        return {"id": 3096}


class TestCustomerWooIdRuntime(unittest.TestCase):
    def test_update_customer_identity_reenables_disabled_customer(self):
        updates = []

        def fake_get_value(doctype, name_or_filters, fieldname):
            if doctype != "Customer":
                return None
            if isinstance(name_or_filters, dict):
                return None
            if name_or_filters == "CUST-0001" and fieldname == "disabled":
                return 1
            if name_or_filters == "CUST-0001" and fieldname in {"woo_customer_id", "woo_username", "mobile_no", "email_id"}:
                return None
            return None

        def fake_set_value(doctype, name, values, update_modified=False):
            updates.append((doctype, name, values, update_modified))

        fake_db = SimpleNamespace(get_value=fake_get_value, set_value=fake_set_value)

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=lambda doctype, field: field in {"woo_customer_id", "woo_username"}):
            customer_sync._update_customer_identity(
                "CUST-0001",
                woo_customer_id=3095,
                username="woo-user",
                phone_norm="+201000000000",
                email="test@example.com",
                customer_cache={},
            )

        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0][2]["disabled"], 0)
        self.assertEqual(updates[0][2]["woo_customer_id"], "3095")

    def test_ensure_customer_create_path_forces_disabled_zero(self):
        created_docs = []

        class DummyDoc:
            def __init__(self, fields):
                self.fields = fields
                self.name = fields["customer_name"]
                self.flags = SimpleNamespace(ignore_woo_outbound=False)

            def insert(self, ignore_permissions=True):
                created_docs.append((self.fields.copy(), ignore_permissions))
                return self

        def fake_get_value(doctype, name_or_filters, fieldname):
            return None

        fake_db = SimpleNamespace(get_value=fake_get_value, set_value=lambda *args, **kwargs: None)

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync.frappe, "get_doc", side_effect=lambda fields: DummyDoc(fields)), \
             unittest.mock.patch.object(customer_sync.frappe, "flags", SimpleNamespace()), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=lambda doctype, field: field == "woo_username"), \
             unittest.mock.patch("frappe.utils.background_jobs.get_redis_conn", side_effect=Exception()):
            customer_name = customer_sync._ensure_customer(
                "test@example.com",
                "Test",
                "Customer",
                None,
                username="woo-user",
                phone="+201000000000",
                woo_customer_id=None,
                customer_cache=None,
            )

        self.assertEqual(customer_name, "Test Customer")
        self.assertEqual(created_docs[0][0]["disabled"], 0)
        self.assertTrue(created_docs[0][1])

    def test_normalize_woo_customer_id_rejects_blank_and_zero(self):
        self.assertIsNone(customer_woo_id.normalize_woo_customer_id(None))
        self.assertIsNone(customer_woo_id.normalize_woo_customer_id(""))
        self.assertIsNone(customer_woo_id.normalize_woo_customer_id("0"))
        self.assertEqual(customer_woo_id.normalize_woo_customer_id("003095"), "3095")

    def test_build_customer_payload_uses_line2_when_line1_missing(self):
        customer = SimpleNamespace(
            name="CUST-0001",
            customer_name="Test Customer",
            email_id="test@example.com",
            mobile_no="01000000000",
            phone=None,
            customer_primary_address="ADDR-BILL-001",
            customer_shipping_address="ADDR-SHIP-001",
            territory="",
            flags=SimpleNamespace(ignore_woo_outbound=False),
        )
        address_rows = {
            "ADDR-BILL-001": {
                "address_line1": "",
                "address_line2": "Apartment 4",
                "city": "Cairo",
                "state": "Cairo Governorate",
                "pincode": "11511",
                "country": "EG",
                "phone": None,
                "email_id": None,
            },
            "ADDR-SHIP-001": {
                "address_line1": "",
                "address_line2": "Villa 8",
                "city": "Giza",
                "state": "Giza Governorate",
                "pincode": "12557",
                "country": "EG",
                "phone": None,
                "email_id": None,
            },
        }

        def fake_get_value(doctype, name, fields, as_dict=False):
            self.assertEqual(doctype, "Address")
            self.assertTrue(as_dict)
            return address_rows.get(name)

        with unittest.mock.patch.object(outbound_sync.frappe, "db", SimpleNamespace(get_value=fake_get_value)):
            payload = outbound_sync._build_customer_payload(customer)

        self.assertEqual(payload["billing"]["address_1"], "Apartment 4")
        self.assertEqual(payload["billing"]["address_2"], "")
        self.assertEqual(payload["billing"]["city"], "Cairo")
        self.assertEqual(payload["shipping"]["address_1"], "Villa 8")
        self.assertEqual(payload["shipping"]["address_2"], "")
        self.assertEqual(payload["shipping"]["state"], "Giza Governorate")

    def test_ensure_customer_backfills_canonical_woo_customer_id_on_email_match(self):
        updates = []

        def fake_get_value(doctype, name_or_filters, fieldname):
            if doctype != "Customer":
                return None
            if isinstance(name_or_filters, dict):
                if name_or_filters == {"woo_customer_id": "3095"}:
                    return None
                if name_or_filters == {"woo_username": "woo-user"}:
                    return None
                if name_or_filters == {"mobile_no": "+201000000000"}:
                    return None
                if name_or_filters == {"email_id": "test@example.com"}:
                    return "CUST-0001"
                return None
            if name_or_filters == "CUST-0001" and fieldname == "woo_customer_id":
                return None
            if name_or_filters == "CUST-0001" and fieldname == "woo_username":
                return None
            if name_or_filters == "CUST-0001" and fieldname == "mobile_no":
                return None
            if name_or_filters == "CUST-0001" and fieldname == "email_id":
                return "test@example.com"
            return None

        def fake_set_value(doctype, name, values, update_modified=False):
            updates.append((doctype, name, values, update_modified))

        fake_db = SimpleNamespace(get_value=fake_get_value, set_value=fake_set_value)

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=lambda doctype, field: field in {"woo_customer_id", "woo_username"}):
            customer_name = customer_sync._ensure_customer(
                "test@example.com",
                "Test",
                "Customer",
                None,
                username="woo-user",
                phone="+201000000000",
                woo_customer_id=3095,
                customer_cache={},
            )

        self.assertEqual(customer_name, "CUST-0001")
        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0][0], "Customer")
        self.assertEqual(updates[0][1], "CUST-0001")
        self.assertEqual(updates[0][2]["woo_customer_id"], "3095")

    def test_ensure_customer_uses_phone_as_primary_merge_key(self):
        updates = []

        def fake_get_value(doctype, name_or_filters, fieldname):
            if doctype != "Customer":
                return None
            if isinstance(name_or_filters, dict):
                if name_or_filters == {"woo_customer_id": "3095"}:
                    return None
                if name_or_filters == {"mobile_no": "+201000000000"}:
                    return "CUST-PHONE"
                if name_or_filters == {"woo_username": "woo-user"}:
                    return None
                if name_or_filters == {"email_id": "test@example.com"}:
                    return None
                return None
            if name_or_filters == "CUST-PHONE" and fieldname == "woo_customer_id":
                return "111"
            if name_or_filters == "CUST-PHONE" and fieldname in {"disabled", "woo_username", "mobile_no", "email_id"}:
                return None
            return None

        def fake_set_value(doctype, name, values, update_modified=False):
            updates.append((doctype, name, values, update_modified))

        fake_db = SimpleNamespace(get_value=fake_get_value, set_value=fake_set_value)

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=lambda doctype, field: field in {"woo_customer_id", "woo_username"}):
            customer_name = customer_sync._ensure_customer(
                "test@example.com",
                "Test",
                "Customer",
                None,
                username="woo-user",
                phone="+201000000000",
                woo_customer_id=3095,
                customer_cache={},
            )

        self.assertEqual(customer_name, "CUST-PHONE")
        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0][1], "CUST-PHONE")

    def test_ensure_customer_does_not_reuse_email_match_with_conflicting_woo_id(self):
        created_docs = []

        class DummyDoc:
            def __init__(self, fields):
                self.fields = fields
                self.name = fields["customer_name"]
                self.flags = SimpleNamespace(ignore_woo_outbound=False)

            def insert(self, ignore_permissions=True):
                created_docs.append((self.fields.copy(), ignore_permissions))
                return self

        def fake_get_value(doctype, name_or_filters, fieldname):
            if doctype != "Customer":
                return None
            if isinstance(name_or_filters, dict):
                if name_or_filters == {"woo_customer_id": "3095"}:
                    return None
                if name_or_filters == {"mobile_no": "+201000000000"}:
                    return None
                if name_or_filters == {"woo_username": "woo-user"}:
                    return None
                if name_or_filters == {"email_id": "test@example.com"}:
                    return "CUST-EMAIL"
                return None
            if name_or_filters == "CUST-EMAIL" and fieldname == "woo_customer_id":
                return "111"
            return None

        fake_db = SimpleNamespace(get_value=fake_get_value, set_value=lambda *args, **kwargs: None)

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync.frappe, "get_doc", side_effect=lambda fields: DummyDoc(fields)), \
             unittest.mock.patch.object(customer_sync.frappe, "flags", SimpleNamespace()), \
             unittest.mock.patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=lambda doctype, field: field in {"woo_customer_id", "woo_username"}), \
             unittest.mock.patch("frappe.utils.background_jobs.get_redis_conn", side_effect=Exception()):
            customer_name = customer_sync._ensure_customer(
                "test@example.com",
                "Test",
                "Customer",
                None,
                username="woo-user",
                phone="+201000000000",
                woo_customer_id=3095,
                customer_cache=None,
            )

        self.assertEqual(customer_name, "Test Customer")
        self.assertEqual(created_docs[0][0]["woo_customer_id"], "3095")

    def test_sync_customer_updates_when_canonical_woo_customer_id_exists(self):
        client = DummyCustomerClient()
        customer = SimpleNamespace(
            name="CUST-0001",
            customer_name="Test Customer",
            woo_customer_id="3095",
            email_id="test@example.com",
            mobile_no="01000000000",
            phone=None,
            customer_primary_address=None,
            customer_shipping_address=None,
            flags=SimpleNamespace(ignore_woo_outbound=False),
        )
        db_updates = []

        cfg = outbound_sync.OutboundConfig(
            enable_customer_push=True,
            enable_order_push=True,
            payment_cod="cod",
            payment_instapay="instapay",
            payment_wallet="wallet",
            shipping_method_id="flat_rate",
            shipping_method_title="Shipping",
        )

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), cfg)), \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
             unittest.mock.patch.object(outbound_sync, "_build_customer_payload", return_value={"email": "test@example.com"}), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc", return_value=customer), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "db", SimpleNamespace(set_value=lambda *args, **kwargs: db_updates.append((args, kwargs)))):
            result = outbound_sync.sync_customer("CUST-0001", reason="test")

        self.assertEqual(result, {"status": "ok", "woo_customer_id": 3095})
        self.assertEqual(client.put_calls, [("customers/3095", {"email": "test@example.com"})])
        self.assertEqual(client.post_calls, [])

    def test_sync_customer_blocks_legacy_only_customer_before_create(self):
        client = DummyCustomerClient()
        customer = SimpleNamespace(
            name="CUST-LEGACY",
            customer_name="Legacy Customer",
            woo_customer_id=None,
            custom_woo_customer_id=3095,
            email_id="legacy@example.com",
            mobile_no="01000000000",
            phone=None,
            customer_primary_address=None,
            customer_shipping_address=None,
            flags=SimpleNamespace(ignore_woo_outbound=False),
        )
        status_updates = []

        cfg = outbound_sync.OutboundConfig(
            enable_customer_push=True,
            enable_order_push=True,
            payment_cod="cod",
            payment_instapay="instapay",
            payment_wallet="wallet",
            shipping_method_id="flat_rate",
            shipping_method_title="Shipping",
        )

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), cfg)), \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc", return_value=customer), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "db", SimpleNamespace(set_value=lambda *args, **kwargs: status_updates.append((args, kwargs)))):
            result = outbound_sync.sync_customer("CUST-LEGACY", reason="test")

        self.assertEqual(result["status"], "error")
        self.assertEqual(client.put_calls, [])
        self.assertEqual(client.post_calls, [])
        self.assertTrue(status_updates)

    def test_order_webhook_job_runs_as_administrator(self):
        fake_logger = SimpleNamespace(info=lambda *args, **kwargs: None, error=lambda *args, **kwargs: None)

        with unittest.mock.patch.object(orders.frappe, "set_user") as set_user, \
             unittest.mock.patch.object(orders, "create_sync_log_entry", return_value=SimpleNamespace()), \
             unittest.mock.patch.object(orders, "finish_sync_log_entry"), \
             unittest.mock.patch.object(orders, "pull_single_order_phase1", return_value={"success": True}), \
             unittest.mock.patch.object(orders.frappe, "logger", return_value=fake_logger):
            orders._process_order_webhook({"id": 123})

        set_user.assert_called_once_with("Administrator")

    def test_customer_webhook_job_runs_as_administrator(self):
        fake_logger = SimpleNamespace(info=lambda *args, **kwargs: None, error=lambda *args, **kwargs: None)

        with unittest.mock.patch.object(webhooks.frappe, "set_user") as set_user, \
             unittest.mock.patch.object(webhooks.WooCommerceSettings, "get_settings", return_value=SimpleNamespace()), \
             unittest.mock.patch.object(webhooks, "process_customer_record", return_value={}), \
             unittest.mock.patch.object(webhooks.frappe, "logger", return_value=fake_logger):
            webhooks._enqueue_customer_process({"id": 456}, {})

        set_user.assert_called_once_with("Administrator")