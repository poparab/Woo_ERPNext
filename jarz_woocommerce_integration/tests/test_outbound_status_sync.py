from types import SimpleNamespace
import unittest

from jarz_woocommerce_integration.services import order_sync, outbound_sync


class DummyInvoice:
    def __init__(
        self,
        *,
        sales_invoice_state: str,
        custom_sales_invoice_state: str | None = None,
        woo_order_id: int = 14500,
        docstatus: int = 1,
        amended_from: str | None = None,
    ):
        self.name = "ACC-SINV-TEST-001"
        self.customer = "CUST-TEST-001"
        self.currency = "EGP"
        self.docstatus = docstatus
        self.sales_invoice_state = sales_invoice_state
        self.custom_sales_invoice_state = custom_sales_invoice_state if custom_sales_invoice_state is not None else sales_invoice_state
        self.woo_order_id = woo_order_id
        self.woo_order_number = None
        self.outstanding_amount = 10
        self.flags = SimpleNamespace(ignore_woo_outbound=False)
        self.custom_acceptance_status = "Pending"
        self.custom_accepted_by = None
        self.custom_accepted_on = None
        self.custom_delivery_date = None
        self.custom_delivery_time_from = None
        self.custom_delivery_duration = None
        self.custom_delivery_time = None
        self.delivery_date = None
        self.delivery_time = None
        self.customer_address = None
        self.shipping_address_name = None
        self.custom_payment_method = None
        self.mode_of_payment = None
        self.amended_from = amended_from
        self.items = []
        self._before_save = None

    def get(self, fieldname, default=None):
        return getattr(self, fieldname, default)

    def get_doc_before_save(self):
        return self._before_save

    def has_value_changed(self, fieldname):
        previous = self.get_doc_before_save()
        if not previous:
            return False
        return previous.get(fieldname) != self.get(fieldname)


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


class DummyMissingOrderClient(DummyClient):
    def __init__(self, *, created_order_id=16600):
        super().__init__(existing_order=None)
        self.created_order_id = created_order_id

    def get(self, path):
        self.get_calls.append(path)
        raise outbound_sync.WooAPIError(404, path, "Invalid ID.", {"message": "Invalid ID."})

    def post(self, path, payload):
        self.post_calls.append((path, payload))
        return {"id": self.created_order_id, "number": str(self.created_order_id)}


class DummyCustomer:
    def __init__(self, *, woo_customer_id: str | None = "3095"):
        self.name = "CUST-TEST-001"
        self.customer_name = "Test Customer"
        self.woo_customer_id = woo_customer_id
        self.email_id = "test@example.com"
        self.mobile_no = "01000000000"
        self.phone = None
        self.customer_primary_address = "ADDR-BILL-001"
        self.customer_shipping_address = "ADDR-SHIP-001"
        self.territory = "Nasr City"
        self.customer_group = "Retail"
        self.flags = SimpleNamespace(ignore_woo_outbound=False)
        self._before_save = None

    def get(self, fieldname, default=None):
        return getattr(self, fieldname, default)

    def get_doc_before_save(self):
        return self._before_save

    def has_value_changed(self, fieldname):
        previous = self.get_doc_before_save()
        if not previous:
            return False
        return previous.get(fieldname) != self.get(fieldname)


class DummyAddress:
    def __init__(
        self,
        *,
        name: str = "ADDR-SHIP-002",
        address_type: str = "Shipping",
        is_shipping_address: int = 1,
        address_line1: str = "Street 2",
        customer_name: str = "CUST-TEST-001",
    ):
        self.name = name
        self.address_type = address_type
        self.is_shipping_address = is_shipping_address
        self.address_line1 = address_line1
        self.address_line2 = None
        self.city = "Cairo"
        self.state = None
        self.pincode = None
        self.country = "Egypt"
        self.phone = "01000000000"
        self.email_id = "test@example.com"
        self.links = [SimpleNamespace(link_doctype="Customer", link_name=customer_name)]
        self.flags = SimpleNamespace(ignore_woo_outbound=False)
        self._before_save = None

    def get(self, fieldname, default=None):
        return getattr(self, fieldname, default)

    def get_doc_before_save(self):
        return self._before_save

    def has_value_changed(self, fieldname):
        previous = self.get_doc_before_save()
        if not previous:
            return False
        return previous.get(fieldname) != self.get(fieldname)


def _outbound_cfg():
    return outbound_sync.OutboundConfig(
        enable_customer_push=True,
        enable_order_push=True,
        payment_cod="cod",
        payment_instapay="instapay",
        payment_wallet="wallet",
        shipping_method_id="flat_rate",
        shipping_method_title="Shipping",
    )


def _db_stub(*, exists=None, get_value=None, set_value=None):
    stub = SimpleNamespace()
    if exists is not None:
        stub.exists = exists if callable(exists) else (lambda *args, **kwargs: exists)
    if get_value is not None:
        stub.get_value = get_value
    if set_value is not None:
        stub.set_value = set_value
    return stub


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


def _build_payload_for_delivery_test(invoice):
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

    with unittest.mock.patch.object(outbound_sync, "_collect_line_items", return_value=(line_items, [])), \
         unittest.mock.patch.object(outbound_sync, "_compute_shipping_total", return_value=0), \
         unittest.mock.patch.object(outbound_sync, "_build_customer_payload", return_value={
             "billing": {"address_1": "Street 1", "email": "test@example.com", "phone": "01000000000"},
             "shipping": {"address_1": "Street 1", "email": "test@example.com", "phone": "01000000000"},
         }), \
         unittest.mock.patch.object(outbound_sync.frappe, "get_doc", return_value=customer):
        return outbound_sync._build_order_payload(invoice, cfg)


class TestOutboundStatusSync(unittest.TestCase):
    def setUp(self):
        self.patcher = unittest.mock.patch

    def test_enqueue_customer_sync_skips_when_customer_flag_marks_inbound(self):
        customer = DummyCustomer()
        customer.flags.ignore_woo_outbound = True
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_customer_sync(customer, method="after_insert")

        self.assertEqual(enqueue_calls, [])

    def test_enqueue_customer_sync_keeps_shipping_address_updates_for_existing_linked_customer(self):
        previous = DummyCustomer()
        current = DummyCustomer()
        current.customer_shipping_address = "ADDR-SHIP-002"
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_customer_sync(current, method="on_update")

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(enqueue_calls[0][1]["customer_name"], current.name)

    def test_enqueue_customer_sync_skips_billing_address_only_updates(self):
        previous = DummyCustomer()
        current = DummyCustomer()
        current.customer_primary_address = "ADDR-BILL-002"
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_customer_sync(current, method="on_update")

        self.assertEqual(enqueue_calls, [])

    def test_enqueue_customer_sync_keeps_territory_updates_for_existing_linked_customer(self):
        previous = DummyCustomer()
        current = DummyCustomer()
        current.territory = "Heliopolis"
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_customer_sync(current, method="on_update")

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(enqueue_calls[0][1]["customer_name"], current.name)

    def test_enqueue_linked_customer_sync_for_address_keeps_shipping_updates(self):
        previous = DummyAddress(address_line1="Old Shipping Line")
        current = DummyAddress(address_line1="New Shipping Line")
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_linked_customer_sync_for_address(current, method="on_update")

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(enqueue_calls[0][1]["customer_name"], "CUST-TEST-001")
        self.assertEqual(enqueue_calls[0][1]["scope"], "shipping")

    def test_enqueue_linked_customer_sync_for_address_skips_billing_only_updates(self):
        previous = DummyAddress(address_type="Billing", is_shipping_address=0, address_line1="Old Billing Line")
        current = DummyAddress(address_type="Billing", is_shipping_address=0, address_line1="New Billing Line")
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_linked_customer_sync_for_address(current, method="on_update")

        self.assertEqual(enqueue_calls, [])

    def test_determine_status_maps_invoice_states_to_woo_status(self):
        self.assertEqual(outbound_sync._determine_status(DummyInvoice(sales_invoice_state="Out for Delivery")), "out-for-delivery")
        self.assertEqual(outbound_sync._determine_status(DummyInvoice(sales_invoice_state="Delivered")), "completed")
        self.assertEqual(outbound_sync._determine_status(DummyInvoice(sales_invoice_state="Completed")), "completed")
        self.assertEqual(outbound_sync._determine_status(DummyInvoice(sales_invoice_state="Cancelled", docstatus=2)), "cancelled")

    def test_determine_status_prefers_later_custom_state_over_stale_legacy_state(self):
        invoice = DummyInvoice(sales_invoice_state="Ready", custom_sales_invoice_state="Delivered")

        self.assertEqual(outbound_sync._determine_status(invoice), "completed")

    def test_enqueue_invoice_sync_skips_acceptance_only_updates_when_status_is_unchanged(self):
        previous = DummyInvoice(sales_invoice_state="Recieved")
        current = DummyInvoice(sales_invoice_state="Recieved")
        current.custom_acceptance_status = "Accepted"
        current.custom_accepted_by = "user@example.com"
        current.custom_accepted_on = "2026-05-02 15:14:07"
        current._before_save = previous

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
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(settings, cfg)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_invoice_sync(current, method="on_update_after_submit")

        self.assertEqual(enqueue_calls, [])

    def test_enqueue_invoice_sync_keeps_status_updates_when_status_changes(self):
        previous = DummyInvoice(sales_invoice_state="Out for Delivery")
        current = DummyInvoice(sales_invoice_state="Delivered")
        current.custom_acceptance_status = "Accepted"
        current.custom_accepted_by = "user@example.com"
        current.custom_accepted_on = "2026-05-02 15:14:07"
        current._before_save = previous

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
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(settings, cfg)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_invoice_sync(current, method="on_update_after_submit")

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(enqueue_calls[0][1]["invoice_name"], current.name)

    def test_enqueue_invoice_sync_skips_when_frappe_flag_marks_inbound(self):
        current = DummyInvoice(sales_invoice_state="Delivered")
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=True)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_invoice_sync(current, method="on_update_after_submit")

        self.assertEqual(enqueue_calls, [])

    def test_enqueue_invoice_sync_skips_ready_only_status_updates(self):
        previous = DummyInvoice(sales_invoice_state="Recieved")
        current = DummyInvoice(sales_invoice_state="Ready")
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
               unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=True)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_invoice_sync(current, method="on_update_after_submit")

        self.assertEqual(enqueue_calls, [])

    def test_enqueue_invoice_sync_skips_payment_method_only_updates(self):
        previous = DummyInvoice(sales_invoice_state="Out for Delivery")
        previous.custom_payment_method = "Cash"
        current = DummyInvoice(sales_invoice_state="Out for Delivery")
        current.custom_payment_method = "Wallet"
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
               unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=True)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_invoice_sync(current, method="on_update_after_submit")

        self.assertEqual(enqueue_calls, [])

    def test_enqueue_invoice_sync_keeps_delivery_window_updates(self):
        previous = DummyInvoice(sales_invoice_state="Out for Delivery")
        previous.custom_delivery_date = "2026-05-02"
        current = DummyInvoice(sales_invoice_state="Out for Delivery")
        current.custom_delivery_date = "2026-05-03"
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
               unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=True)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_invoice_sync(current, method="on_update_after_submit")

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(enqueue_calls[0][1]["invoice_name"], current.name)

    def test_enqueue_invoice_sync_keeps_submitted_item_updates(self):
        previous = DummyInvoice(sales_invoice_state="Out for Delivery")
        previous.items = [
            SimpleNamespace(
                item_code="ITEM-001",
                qty=1,
                rate=100,
                amount=100,
                price_list_rate=100,
                discount_percentage=0,
                is_bundle_parent=0,
                is_bundle_child=0,
                parent_bundle=None,
                bundle_code=None,
            )
        ]
        current = DummyInvoice(sales_invoice_state="Out for Delivery")
        current.items = [
            SimpleNamespace(
                item_code="ITEM-001",
                qty=2,
                rate=100,
                amount=200,
                price_list_rate=100,
                discount_percentage=0,
                is_bundle_parent=0,
                is_bundle_child=0,
                parent_bundle=None,
                bundle_code=None,
            )
        ]
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
               unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=True)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_invoice_sync(current, method="on_update_after_submit")

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(enqueue_calls[0][1]["invoice_name"], current.name)

    def test_enqueue_invoice_sync_keeps_missing_order_mapping_reconcile(self):
        previous = DummyInvoice(sales_invoice_state="Out for Delivery")
        current = DummyInvoice(sales_invoice_state="Out for Delivery")
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
                         unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_invoice_sync(current, method="on_update_after_submit")

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(enqueue_calls[0][1]["invoice_name"], current.name)

    def test_sync_sales_invoice_allows_mapped_woo_order_status_updates(self):
        invoice = DummyInvoice(sales_invoice_state="Out for Delivery")
        client = DummyClient(existing_order={"id": 14500, "status": "processing"})
        mock_set_value = unittest.mock.MagicMock()

        with unittest.mock.patch.object(outbound_sync, "_get_settings") as mock_get_settings, \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
             unittest.mock.patch.object(outbound_sync, "_build_order_payload", return_value={"status": "out-for-delivery"}), \
               unittest.mock.patch.object(outbound_sync, "now_datetime", return_value="2026-05-03 12:00:00"), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc") as mock_get_doc, \
             unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=True, set_value=mock_set_value)), \
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
               unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=True)), \
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

    def test_sync_sales_invoice_updates_delivery_metadata_even_when_status_matches(self):
        invoice = DummyInvoice(sales_invoice_state="Delivered")
        client = DummyClient(existing_order={
            "id": 14500,
            "status": "completed",
            "meta_data": [{"key": "_orddd_delivery_date", "value": "Sunday, May 03, 2026"}],
        })
        mock_set_value = unittest.mock.MagicMock()

        with unittest.mock.patch.object(outbound_sync, "_get_settings") as mock_get_settings, \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
             unittest.mock.patch.object(outbound_sync, "_build_order_payload", return_value={
                 "status": "completed",
                 "meta_data": [{"key": "_orddd_delivery_date", "value": "Wednesday, May 06, 2026"}],
             }), \
             unittest.mock.patch.object(outbound_sync, "now_datetime", return_value="2026-05-03 12:00:00"), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc") as mock_get_doc, \
             unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=True, set_value=mock_set_value)), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)):
            mock_get_settings.return_value = (SimpleNamespace(), _outbound_cfg())
            mock_get_doc.side_effect = lambda doctype, name: invoice if doctype == "Sales Invoice" else SimpleNamespace(name=invoice.customer, woo_customer_id="88")

            result = outbound_sync.sync_sales_invoice(invoice.name, reason="test")

        self.assertEqual(result, {"status": "ok", "woo_order_id": 14500})
        self.assertEqual(client.put_calls, [("orders/14500", {
            "status": "completed",
            "meta_data": [{"key": "_orddd_delivery_date", "value": "Wednesday, May 06, 2026"}],
        })])

    def test_resolve_customer_shipping_address_name_prefers_linked_shipping_address(self):
        customer = SimpleNamespace(name="CUST-TEST-001", customer_primary_address="ADDR-BILL-001")

        with unittest.mock.patch.object(outbound_sync, "_get_linked_customer_addresses", return_value=[
            {"name": "ADDR-BILL-001", "address_type": "Billing", "is_primary_address": 1, "is_shipping_address": 0},
            {"name": "ADDR-SHIP-001", "address_type": "Billing", "is_primary_address": 0, "is_shipping_address": 1},
        ]):
            resolved = outbound_sync._resolve_customer_shipping_address_name(customer)

        self.assertEqual(resolved, "ADDR-SHIP-001")

    def test_sync_customer_shipping_scope_updates_shipping_without_billing(self):
        customer = SimpleNamespace(
            name="CUST-TEST-001",
            customer_name="Test Customer",
            woo_customer_id="3095",
            email_id="test@example.com",
            mobile_no="01000000000",
            phone=None,
            customer_primary_address="ADDR-BILL-001",
            territory="Nasr City",
            flags=SimpleNamespace(ignore_woo_outbound=False),
        )
        client = DummyClient()
        mock_set_value = unittest.mock.MagicMock()

        def fake_get_address_payload(address_name, **kwargs):
            if address_name == "ADDR-BILL-001":
                return {"address_1": "Billing Line", "email": "test@example.com", "phone": "01000000000"}
            if address_name == "ADDR-SHIP-001":
                return {"address_1": "Shipping Line", "email": "test@example.com", "phone": "01000000000"}
            return {}

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
             unittest.mock.patch.object(outbound_sync, "_resolve_customer_billing_address_name", return_value="ADDR-BILL-001"), \
             unittest.mock.patch.object(outbound_sync, "_resolve_customer_shipping_address_name", return_value="ADDR-SHIP-001"), \
             unittest.mock.patch.object(outbound_sync, "_get_address_payload", side_effect=fake_get_address_payload), \
             unittest.mock.patch.object(outbound_sync, "get_customer_woo_id", return_value="3095"), \
             unittest.mock.patch.object(outbound_sync, "has_unmigrated_legacy_customer_woo_id", return_value=False), \
             unittest.mock.patch.object(outbound_sync, "now_datetime", return_value="2026-05-03 12:00:00"), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc", return_value=customer), \
             unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(set_value=mock_set_value)), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)):
            result = outbound_sync.sync_customer(customer.name, reason="test", scope="shipping")

        self.assertEqual(result, {"status": "ok", "woo_customer_id": 14500})
        self.assertEqual(client.put_calls, [("customers/3095", {
            "shipping": {
                "address_1": "Shipping Line",
                "email": "test@example.com",
                "phone": "01000000000",
            },
        })])

    def test_sync_customer_territory_scope_updates_metadata_only(self):
        customer = SimpleNamespace(
            name="CUST-TEST-001",
            customer_name="Test Customer",
            woo_customer_id="3095",
            email_id="test@example.com",
            mobile_no="01000000000",
            phone=None,
            customer_primary_address="ADDR-BILL-001",
            territory="Nasr City",
            flags=SimpleNamespace(ignore_woo_outbound=False),
        )
        client = DummyClient()
        mock_set_value = unittest.mock.MagicMock()

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
             unittest.mock.patch.object(outbound_sync, "_resolve_customer_billing_address_name", return_value="ADDR-BILL-001"), \
             unittest.mock.patch.object(outbound_sync, "_resolve_customer_shipping_address_name", return_value=None), \
             unittest.mock.patch.object(outbound_sync, "_get_address_payload", return_value={}), \
             unittest.mock.patch.object(outbound_sync, "get_customer_woo_id", return_value="3095"), \
             unittest.mock.patch.object(outbound_sync, "has_unmigrated_legacy_customer_woo_id", return_value=False), \
             unittest.mock.patch.object(outbound_sync, "now_datetime", return_value="2026-05-03 12:00:00"), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc", return_value=customer), \
             unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(set_value=mock_set_value)), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)):
            result = outbound_sync.sync_customer(customer.name, reason="test", scope="territory")

        self.assertEqual(result, {"status": "ok", "woo_customer_id": 14500})
        self.assertEqual(client.put_calls, [("customers/3095", {
            "meta_data": [{"key": "erpnext_territory", "value": "Nasr City"}],
        })])

    def test_collect_line_items_skips_registered_bundle_parent_rows_without_runtime_flag(self):
        invoice = SimpleNamespace(items=[
            SimpleNamespace(
                item_code="BUNDLE-001",
                item_name="Bundle Parent",
                qty=1,
                price_list_rate=432,
                rate=0,
                amount=0,
                discount_percentage=100,
            ),
            SimpleNamespace(
                item_code="ITEM-DISCOUNT",
                item_name="Discounted Child",
                qty=1,
                price_list_rate=120,
                rate=60,
                amount=60,
                discount_percentage=50,
            ),
        ])

        def fake_get_value(doctype, name, fields, as_dict=False):
            if doctype != "Item":
                raise AssertionError(f"Unexpected doctype: {doctype}")
            return {
                "woo_product_id": "202" if name == "BUNDLE-001" else "303",
                "item_name": "Bundle Parent" if name == "BUNDLE-001" else "Discounted Child",
            }

        with unittest.mock.patch.object(outbound_sync, "_get_registered_bundle_product_ids", return_value={"202"}), \
               unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(get_value=fake_get_value)):
            line_items, missing = outbound_sync._collect_line_items(invoice)

        self.assertEqual(missing, [])
        self.assertEqual(len(line_items), 1)
        self.assertEqual(line_items[0]["name"], "Discounted Child")
        self.assertEqual(line_items[0]["subtotal"], "120.00")
        self.assertEqual(line_items[0]["total"], "60.00")

    def test_collect_line_items_includes_explicit_bundle_parent_at_zero_and_links_children(self):
        invoice = SimpleNamespace(items=[
            SimpleNamespace(
                item_code="BUNDLE-001",
                item_name="Bundle Parent",
                qty=1,
                price_list_rate=432,
                rate=0,
                amount=0,
                discount_percentage=100,
                is_bundle_parent=1,
                bundle_code="BUNDLE-CODE-001",
                parent_bundle=None,
                is_bundle_child=0,
            ),
            SimpleNamespace(
                item_code="ITEM-CHILD",
                item_name="Bundle Child",
                qty=1,
                price_list_rate=120,
                rate=120,
                amount=120,
                discount_percentage=0,
                is_bundle_parent=0,
                bundle_code=None,
                parent_bundle="BUNDLE-CODE-001",
                is_bundle_child=1,
            ),
        ])

        def fake_get_value(doctype, name, fields, as_dict=False):
            if doctype != "Item":
                raise AssertionError(f"Unexpected doctype: {doctype}")
            if name == "BUNDLE-001":
                return {"woo_product_id": "202", "item_name": "Bundle Parent"}
            return {"woo_product_id": "303", "item_name": "Bundle Child"}

        with unittest.mock.patch.object(outbound_sync, "_get_registered_bundle_product_ids", return_value={"202"}), \
               unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(get_value=fake_get_value)):
            line_items, missing = outbound_sync._collect_line_items(invoice)

        self.assertEqual(missing, [])
        self.assertEqual(len(line_items), 2)

        parent_entry = line_items[0]
        self.assertEqual(parent_entry["name"], "Bundle Parent")
        self.assertEqual(parent_entry["subtotal"], "0.00")
        self.assertEqual(parent_entry["total"], "0.00")
        self.assertEqual(parent_entry["product_id"], 202)
        self.assertEqual(parent_entry["meta_data"], [{"key": "erpnext_item_code", "value": "BUNDLE-001"}])

        child_entry = line_items[1]
        self.assertEqual(child_entry["name"], "Bundle Child")
        self.assertEqual(child_entry["subtotal"], "120.00")
        self.assertEqual(child_entry["total"], "120.00")
        self.assertEqual(child_entry["product_id"], 303)
        self.assertEqual(
            child_entry["meta_data"],
            [
                {"key": "erpnext_item_code", "value": "ITEM-CHILD"},
                {"key": "_woosb_parent_id", "value": "202"},
            ],
        )

    def test_sync_sales_invoice_replaces_stale_woo_order_id_after_missing_remote_order(self):
        invoice = DummyInvoice(sales_invoice_state="Ready", woo_order_id=14500)
        client = DummyMissingOrderClient(created_order_id=16600)
        mock_set_value = unittest.mock.MagicMock()

        with unittest.mock.patch.object(outbound_sync, "_get_settings") as mock_get_settings, \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
             unittest.mock.patch.object(outbound_sync, "_build_order_payload", return_value={"status": "processing"}), \
               unittest.mock.patch.object(outbound_sync, "now_datetime", return_value="2026-05-03 12:00:00"), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc") as mock_get_doc, \
             unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=True, set_value=mock_set_value)), \
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

        self.assertEqual(result, {"status": "ok", "woo_order_id": 16600})
        self.assertEqual(client.post_calls, [("orders", {"status": "processing"})])
        _, _, updates = mock_set_value.call_args.args[:3]
        self.assertEqual(updates["woo_order_id"], 16600)
        self.assertEqual(updates["woo_order_number"], "16600")

    def test_sync_sales_invoice_reuses_amended_source_woo_order_and_relinks_order_map(self):
        invoice = DummyInvoice(sales_invoice_state="Ready", woo_order_id=None, amended_from="ACC-SINV-OLD-001")
        client = DummyClient(existing_order={"id": 14500, "status": "processing", "line_items": []})
        mock_set_value = unittest.mock.MagicMock()

        def fake_get_value(doctype, name_or_filters, fieldname=None, as_dict=False):
            if doctype == "Sales Invoice" and name_or_filters == "ACC-SINV-OLD-001" and fieldname == "woo_order_id":
                return 14500
            if doctype == "WooCommerce Order Map" and name_or_filters == {"woo_order_id": 14500} and fieldname == "name":
                return "WOO-MAP-001"
            return None

        with unittest.mock.patch.object(outbound_sync, "_get_settings") as mock_get_settings, \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
             unittest.mock.patch.object(outbound_sync, "_build_order_payload", return_value={"status": "processing"}), \
             unittest.mock.patch.object(outbound_sync, "now_datetime", return_value="2026-05-03 12:00:00"), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc") as mock_get_doc, \
             unittest.mock.patch.object(
                 outbound_sync.frappe,
                 "db",
                 _db_stub(
                     exists=True,
                     get_value=fake_get_value,
                     set_value=mock_set_value,
                 ),
             ), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)):
            mock_get_settings.return_value = (SimpleNamespace(), _outbound_cfg())
            mock_get_doc.side_effect = lambda doctype, name: invoice if doctype == "Sales Invoice" else SimpleNamespace(name=invoice.customer, woo_customer_id="88")

            result = outbound_sync.sync_sales_invoice(invoice.name, reason="test")

        self.assertEqual(result, {"status": "ok", "woo_order_id": 14500})
        self.assertEqual(client.put_calls, [("orders/14500", {"status": "processing"})])
        first_update = mock_set_value.call_args_list[0].args[2]
        self.assertEqual(first_update["woo_order_id"], 14500)
        self.assertEqual(mock_set_value.call_args_list[1].args[0], "WooCommerce Order Map")
        self.assertEqual(mock_set_value.call_args_list[1].args[1], "WOO-MAP-001")
        self.assertEqual(mock_set_value.call_args_list[1].args[2]["erpnext_sales_invoice"], invoice.name)

    def test_map_status_supports_out_for_delivery(self):
        self.assertEqual(order_sync._map_status("out-for-delivery"), {
            "docstatus": 1,
            "custom_state": "Out for Delivery",
            "is_paid": False,
        })

    def test_build_order_payload_allows_status_only_update_when_existing_line_items_do_not_match(self):
        invoice = DummyInvoice(sales_invoice_state="Delivered")
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

    def test_build_order_payload_formats_delivery_slot_from_start_time_and_duration(self):
        invoice = DummyInvoice(sales_invoice_state="Delivered")
        invoice.custom_delivery_date = "2026-05-02"
        invoice.custom_delivery_time_from = "19:00:00"
        invoice.custom_delivery_duration = 5400

        payload = _build_payload_for_delivery_test(invoice)
        metadata = {entry["key"]: entry["value"] for entry in payload["meta_data"]}

        self.assertEqual(metadata["_orddd_timestamp"], "1777680000")
        self.assertEqual(metadata["Delivery Date"], "Saturday, May 02, 2026")
        self.assertEqual(metadata["_orddd_time_slot"], "19:00 - 20:30")
        self.assertEqual(metadata["Time Slot"], "19:00 - 20:30")

    def test_build_order_payload_formats_delivery_slot_from_two_hour_duration(self):
        invoice = DummyInvoice(sales_invoice_state="Delivered")
        invoice.custom_delivery_date = "2026-05-02"
        invoice.custom_delivery_time_from = "19:00:00"
        invoice.custom_delivery_duration = 7200

        payload = _build_payload_for_delivery_test(invoice)
        metadata = {entry["key"]: entry["value"] for entry in payload["meta_data"]}

        self.assertEqual(metadata["_orddd_time_slot"], "19:00 - 21:00")
        self.assertEqual(metadata["Time Slot"], "19:00 - 21:00")

    def test_build_order_payload_uses_date_only_timestamp_without_fake_noon(self):
        invoice = DummyInvoice(sales_invoice_state="Delivered")
        invoice.custom_delivery_date = "2026-05-02"

        payload = _build_payload_for_delivery_test(invoice)
        metadata = {entry["key"]: entry["value"] for entry in payload["meta_data"]}

        self.assertEqual(metadata["_orddd_timestamp"], "1777680000")
        self.assertNotIn("_orddd_time_slot", metadata)
        self.assertNotIn("Time Slot", metadata)

    def test_build_order_payload_preserves_legacy_single_time_fallback(self):
        invoice = DummyInvoice(sales_invoice_state="Delivered")
        invoice.custom_delivery_date = "2026-05-02"
        invoice.custom_delivery_time = "19:00:00"

        payload = _build_payload_for_delivery_test(invoice)
        metadata = {entry["key"]: entry["value"] for entry in payload["meta_data"]}

        self.assertEqual(metadata["_orddd_timestamp"], "1777680000")
        self.assertEqual(metadata["_orddd_time_slot"], "19:00")
        self.assertEqual(metadata["Time Slot"], "19:00")