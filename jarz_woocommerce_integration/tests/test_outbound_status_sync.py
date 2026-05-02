from types import SimpleNamespace
import unittest

from jarz_woocommerce_integration.services import order_sync, outbound_sync


class DummyInvoice:
    def __init__(self, *, sales_invoice_state: str, woo_order_id: int = 14500, docstatus: int = 1):
        self.name = "ACC-SINV-TEST-001"
        self.customer = "CUST-TEST-001"
        self.currency = "EGP"
        self.docstatus = docstatus
        self.sales_invoice_state = sales_invoice_state
        self.custom_sales_invoice_state = sales_invoice_state
        self.woo_order_id = woo_order_id
        self.woo_order_number = None
        self.outstanding_amount = 10
        self.flags = SimpleNamespace(ignore_woo_outbound=False)

    def get(self, fieldname, default=None):
        return getattr(self, fieldname, default)


class DummyClient:
    def __init__(self, existing_order=None):
        self.existing_order = dict(existing_order or {})
        self.get_calls = []
        self.put_calls = []
        self.post_calls = []

    def get(self, path):
        self.get_calls.append(path)
        return dict(self.existing_order)

    def put(self, path, payload):
        self.put_calls.append((path, payload))
        return {"id": 14500, "number": "14500"}

    def post(self, path, payload):
        self.post_calls.append((path, payload))
        return {"id": 14500, "number": "14500"}


def _patch_common(monkeypatch, invoice, client, *, order_map_exists=True):
    settings = SimpleNamespace()
    cfg = outbound_sync.OutboundConfig(
        enable_customer_push=True,
        enable_order_push=True,
        payment_cod="cod",
        payment_instapay="instapay",
        payment_wallet="wallet",
        shipping_method_id="flat_rate",
        shipping_method_title="Shipping",
    )
    customer = SimpleNamespace(name=invoice.customer, woo_customer_id="88")
    db_updates = []

    def fake_get_doc(doctype, name):
        if doctype == "Sales Invoice":
            return invoice
        if doctype == "Customer":
            return customer
        raise AssertionError(f"Unexpected doctype: {doctype}")

    def fake_set_value(doctype, name, values, update_modified=False):
        db_updates.append((doctype, name, values, update_modified))

    monkeypatch.setattr(outbound_sync, "_get_settings", lambda: (settings, cfg))
    monkeypatch.setattr(outbound_sync, "_build_client", lambda settings_obj: client)
    monkeypatch.setattr(outbound_sync, "_build_order_payload", lambda *args, **kwargs: {"status": outbound_sync._determine_status(invoice)})
    monkeypatch.setattr(outbound_sync.frappe, "get_doc", fake_get_doc)
    monkeypatch.setattr(
        outbound_sync.frappe,
        "db",
        SimpleNamespace(
            exists=lambda doctype, filters: order_map_exists,
            set_value=fake_set_value,
        ),
    )
    monkeypatch.setattr(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False))
    return db_updates


class TestOutboundStatusSync(unittest.TestCase):
    def setUp(self):
        self.patcher = unittest.mock.patch

    def test_determine_status_maps_invoice_states_to_woo_status(self):
        self.assertEqual(outbound_sync._determine_status(DummyInvoice(sales_invoice_state="Out for Delivery")), "out-for-delivery")
        self.assertEqual(outbound_sync._determine_status(DummyInvoice(sales_invoice_state="Delivered")), "completed")
        self.assertEqual(outbound_sync._determine_status(DummyInvoice(sales_invoice_state="Completed")), "completed")
        self.assertEqual(outbound_sync._determine_status(DummyInvoice(sales_invoice_state="Cancelled", docstatus=2)), "cancelled")

    def test_sync_sales_invoice_allows_mapped_woo_order_status_updates(self):
        invoice = DummyInvoice(sales_invoice_state="Out for Delivery")
        client = DummyClient(existing_order={"id": 14500, "status": "processing"})

        with unittest.mock.patch.object(outbound_sync, "_get_settings") as mock_get_settings, \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
             unittest.mock.patch.object(outbound_sync, "_build_order_payload", return_value={"status": "out-for-delivery"}), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc") as mock_get_doc, \
             unittest.mock.patch.object(outbound_sync.frappe.db, "exists", return_value=True), \
             unittest.mock.patch.object(outbound_sync.frappe.db, "set_value") as mock_set_value, \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)):
            mock_get_settings.return_value = (
                SimpleNamespace(),
                outbound_sync.OutboundConfig(
                    enable_customer_push=True,
                    enable_order_push=True,
                    payment_cod="cod",
                    payment_instapay="instapay",
                    payment_wallet="wallet",
                    shipping_method_id="flat_rate",
                    shipping_method_title="Shipping",
                ),
            )
            mock_get_doc.side_effect = lambda doctype, name: invoice if doctype == "Sales Invoice" else SimpleNamespace(name=invoice.customer, woo_customer_id="88")

            result = outbound_sync.sync_sales_invoice(invoice.name, reason="test")

        self.assertEqual(result, {"status": "ok", "woo_order_id": 14500})
        self.assertEqual(client.put_calls, [("orders/14500", {"status": "out-for-delivery"})])
        self.assertEqual(mock_set_value.call_count, 1)
        _, _, updates = mock_set_value.call_args.args[:3]
        self.assertEqual(updates["woo_outbound_status"], "Synced")
        self.assertEqual(updates["woo_outbound_error"], "")
        self.assertEqual(updates["woo_order_number"], "14500")

    def test_sync_sales_invoice_skips_mirrored_status_for_mapped_woo_order(self):
        invoice = DummyInvoice(sales_invoice_state="Delivered")
        client = DummyClient(existing_order={"id": 14500, "status": "completed"})

        with unittest.mock.patch.object(outbound_sync, "_get_settings") as mock_get_settings, \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
             unittest.mock.patch.object(outbound_sync, "_build_order_payload", return_value={"status": "completed"}), \
             unittest.mock.patch.object(outbound_sync, "_mark_invoice_status") as mock_mark_status, \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc") as mock_get_doc, \
             unittest.mock.patch.object(outbound_sync.frappe.db, "exists", return_value=True), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)):
            mock_get_settings.return_value = (
                SimpleNamespace(),
                outbound_sync.OutboundConfig(
                    enable_customer_push=True,
                    enable_order_push=True,
                    payment_cod="cod",
                    payment_instapay="instapay",
                    payment_wallet="wallet",
                    shipping_method_id="flat_rate",
                    shipping_method_title="Shipping",
                ),
            )
            mock_get_doc.side_effect = lambda doctype, name: invoice if doctype == "Sales Invoice" else SimpleNamespace(name=invoice.customer, woo_customer_id="88")

            result = outbound_sync.sync_sales_invoice(invoice.name, reason="test")

        self.assertEqual(result, {"skipped": True, "reason": "already_in_sync", "woo_order_id": 14500})
        self.assertEqual(client.put_calls, [])
        mock_mark_status.assert_called_once_with(invoice.name, status="Synced")

    def test_map_status_supports_out_for_delivery(self):
        self.assertEqual(order_sync._map_status("out-for-delivery"), {
            "docstatus": 1,
            "custom_state": "Out for Delivery",
            "is_paid": False,
        })

    def test_build_order_payload_allows_status_only_update_when_existing_line_items_do_not_match(self):
        invoice = DummyInvoice(sales_invoice_state="Delivered")
        invoice.customer_address = None
        invoice.shipping_address_name = None
        cfg = outbound_sync.OutboundConfig(
            enable_customer_push=True,
            enable_order_push=True,
            payment_cod="cod",
            payment_instapay="instapay",
            payment_wallet="wallet",
            shipping_method_id="flat_rate",
            shipping_method_title="Shipping",
        )
        customer = SimpleNamespace(
            customer_name="Test Customer",
            woo_customer_id="88",
            email_id="test@example.com",
            mobile_no="01000000000",
            phone=None,
        )
        line_items = [{
            "product_id": 101,
            "variation_id": None,
            "quantity": 1,
            "meta_data": [{"key": "erpnext_item_code", "value": "ITEM-001"}],
            "name": "ITEM-001",
        }]
        existing_order = {
            "id": 14500,
            "status": "processing",
            "line_items": [{
                "id": 55,
                "product_id": 202,
                "variation_id": 0,
                "meta_data": [],
            }],
        }

        with unittest.mock.patch.object(outbound_sync, "_collect_line_items", return_value=(line_items, [])), \
             unittest.mock.patch.object(outbound_sync, "_compute_shipping_total", return_value=0), \
             unittest.mock.patch.object(outbound_sync, "_build_customer_payload", return_value={
                 "billing": {"address_1": "Street 1", "email": "test@example.com", "phone": "01000000000"},
                 "shipping": {"address_1": "Street 1", "email": "test@example.com", "phone": "01000000000"},
             }), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc", return_value=customer):
            payload = outbound_sync._build_order_payload(invoice, cfg, existing_order=existing_order)

        self.assertEqual(payload["status"], "completed")
        self.assertNotIn("line_items", payload)
        self.assertIn({"key": "unmapped_line_items", "value": "ITEM-001"}, payload["meta_data"])