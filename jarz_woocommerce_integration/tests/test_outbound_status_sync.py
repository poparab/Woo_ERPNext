from datetime import time as dt_time, timedelta
from types import SimpleNamespace
import unittest
# Explicit: this module uses unittest.mock throughout, which is NOT pulled in by
# `import unittest` alone. It only ever worked because another test module
# imported it first, making the suite order-dependent.
import unittest.mock

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


class DummyResponseClient(DummyClient):
    def __init__(self, *, response=None, existing_order=None):
        super().__init__(existing_order=existing_order)
        self.response = dict(response or {"id": 14500, "number": "14500", "status": "processing", "payment_method": "cod"})

    def put(self, path, payload):
        self.put_calls.append((path, payload))
        return dict(self.response)

    def post(self, path, payload):
        self.post_calls.append((path, payload))
        return dict(self.response)


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

    def test_enqueue_customer_sync_routes_to_event_outbox_when_enabled(self):
        customer = DummyCustomer()
        enqueue_calls = []
        event_enqueue_calls = []
        fake_event = SimpleNamespace(name="WOOEVT-00011")

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(enable_sync_event_ledger=1), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.should_use_customer_outbox", return_value=True), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.create_outbound_customer_event", return_value=fake_event) as create_event, \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.is_shadow_mode_enabled", return_value=False), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.enqueue_sync_event", side_effect=lambda *args, **kwargs: event_enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_customer_sync(customer, method="after_insert")

        self.assertEqual(enqueue_calls, [])
        create_event.assert_called_once()
        self.assertEqual(event_enqueue_calls[0][0][0], fake_event.name)

    def test_enqueue_customer_sync_keeps_direct_enqueue_in_shadow_mode(self):
        customer = DummyCustomer()
        enqueue_calls = []
        event_enqueue_calls = []
        fake_event = SimpleNamespace(name="WOOEVT-00012")

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(enable_sync_event_ledger=1), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.should_use_customer_outbox", return_value=True), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.create_outbound_customer_event", return_value=fake_event), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.is_shadow_mode_enabled", return_value=True), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.enqueue_sync_event", side_effect=lambda *args, **kwargs: event_enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_customer_sync(customer, method="after_insert")

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(event_enqueue_calls, [])

    def test_enqueue_customer_sync_shadow_insert_failure_reports_and_falls_back(self):
        customer = DummyCustomer()
        enqueue_calls = []
        event_enqueue_calls = []
        shadow_failures = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(enable_sync_event_ledger=1), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.should_use_customer_outbox", return_value=True), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.create_outbound_customer_event", side_effect=RuntimeError("shadow insert failed")), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.is_shadow_mode_enabled", return_value=True), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.report_shadow_insert_failure", side_effect=lambda *args, **kwargs: shadow_failures.append((args, kwargs))), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.enqueue_sync_event", side_effect=lambda *args, **kwargs: event_enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_customer_sync(customer, method="after_insert")

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(event_enqueue_calls, [])
        self.assertEqual(len(shadow_failures), 1)

    def test_enqueue_invoice_sync_routes_to_event_outbox_when_enabled(self):
        current = DummyInvoice(sales_invoice_state="Delivered")
        enqueue_calls = []
        event_enqueue_calls = []
        fake_event = SimpleNamespace(name="WOOEVT-00013")

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(enable_sync_event_ledger=1), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.should_use_invoice_outbox", return_value=True), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.create_outbound_invoice_event", return_value=fake_event) as create_event, \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.is_shadow_mode_enabled", return_value=False), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.enqueue_sync_event", side_effect=lambda *args, **kwargs: event_enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_invoice_sync(current, method="on_submit")

        self.assertEqual(enqueue_calls, [])
        create_event.assert_called_once()
        self.assertEqual(event_enqueue_calls[0][0][0], fake_event.name)

    def test_enqueue_invoice_sync_shadow_insert_failure_reports_and_falls_back(self):
        current = DummyInvoice(sales_invoice_state="Delivered")
        enqueue_calls = []
        event_enqueue_calls = []
        shadow_failures = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(enable_sync_event_ledger=1), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.should_use_invoice_outbox", return_value=True), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.create_outbound_invoice_event", side_effect=RuntimeError("shadow insert failed")), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.is_shadow_mode_enabled", return_value=True), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.report_shadow_insert_failure", side_effect=lambda *args, **kwargs: shadow_failures.append((args, kwargs))), \
             unittest.mock.patch("jarz_woocommerce_integration.services.sync_events.enqueue_sync_event", side_effect=lambda *args, **kwargs: event_enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_invoice_sync(current, method="on_submit")

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(event_enqueue_calls, [])
        self.assertEqual(len(shadow_failures), 1)

    def test_enqueue_linked_invoice_sync_for_payment_entry_enqueues_unique_sales_invoices(self):
        payment_entry = SimpleNamespace(references=[
            SimpleNamespace(reference_doctype="Sales Invoice", reference_name="ACC-SINV-TEST-001"),
            SimpleNamespace(reference_doctype="Sales Invoice", reference_name="ACC-SINV-TEST-001"),
            SimpleNamespace(reference_doctype="Sales Invoice", reference_name="ACC-SINV-TEST-002"),
            SimpleNamespace(reference_doctype="Journal Entry", reference_name="ACC-JV-TEST-001"),
        ])
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "enqueue_invoice_sync", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_linked_invoice_sync_for_payment_entry(payment_entry, method="on_submit")

        self.assertEqual(enqueue_calls, [
            (("ACC-SINV-TEST-001",), {"reason": "payment_entry_on_submit"}),
            (("ACC-SINV-TEST-002",), {"reason": "payment_entry_on_submit"}),
        ])

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

    def test_enqueue_invoice_sync_keeps_payment_method_only_updates(self):
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

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(enqueue_calls[0][1]["invoice_name"], current.name)

    def test_enqueue_invoice_sync_keeps_shipping_address_only_updates(self):
        previous = DummyInvoice(sales_invoice_state="Out for Delivery")
        previous.shipping_address_name = "ADDR-SHIP-001"
        current = DummyInvoice(sales_invoice_state="Out for Delivery")
        current.shipping_address_name = "ADDR-SHIP-002"
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
               unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=True)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_invoice_sync(current, method="on_update_after_submit")

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(enqueue_calls[0][1]["invoice_name"], current.name)

    def test_enqueue_invoice_sync_keeps_outstanding_amount_only_updates(self):
        previous = DummyInvoice(sales_invoice_state="Recieved")
        previous.outstanding_amount = 10
        current = DummyInvoice(sales_invoice_state="Recieved")
        current.outstanding_amount = 0
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
               unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=True)), \
             unittest.mock.patch.object(outbound_sync.frappe, "enqueue", side_effect=lambda *args, **kwargs: enqueue_calls.append((args, kwargs))):
            outbound_sync.enqueue_invoice_sync(current, method="on_update_after_submit")

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(enqueue_calls[0][1]["invoice_name"], current.name)

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

    def test_sync_sales_invoice_updates_paid_state_even_when_status_matches(self):
        invoice = DummyInvoice(sales_invoice_state="Recieved")
        client = DummyResponseClient(
            existing_order={
                "id": 14500,
                "status": "processing",
                "payment_method": "cod",
                "payment_method_title": "Cash",
                "date_paid": None,
                "date_paid_gmt": None,
            },
            response={
                "id": 14500,
                "number": "14500",
                "status": "processing",
                "payment_method": "cod",
                "payment_method_title": "Cash",
                "date_paid": "2026-05-03T12:00:00",
                "date_paid_gmt": "2026-05-03T09:00:00",
            },
        )
        payload = {
            "status": "processing",
            "payment_method": "cod",
            "payment_method_title": "Cash",
            "set_paid": True,
        }
        mock_set_value = unittest.mock.MagicMock()

        with unittest.mock.patch.object(outbound_sync, "_get_settings") as mock_get_settings, \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
             unittest.mock.patch.object(outbound_sync, "_build_order_payload", return_value=payload), \
             unittest.mock.patch.object(outbound_sync, "now_datetime", return_value="2026-05-03 12:00:00"), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc") as mock_get_doc, \
             unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=True, set_value=mock_set_value)), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)):
            mock_get_settings.return_value = (SimpleNamespace(), _outbound_cfg())
            mock_get_doc.side_effect = lambda doctype, name: invoice if doctype == "Sales Invoice" else SimpleNamespace(name=invoice.customer, woo_customer_id="88")

            result = outbound_sync.sync_sales_invoice(invoice.name, reason="test")

        self.assertEqual(result, {"status": "ok", "woo_order_id": 14500})
        self.assertEqual(client.put_calls, [("orders/14500", payload)])

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
        # No `name`: WooCommerce names its own products now, and the line is
        # identified by the item code it carries.
        self.assertNotIn("name", line_items[0])
        self.assertEqual(
            line_items[0]["meta_data"][0],
            {"key": "erpnext_item_code", "value": "ITEM-DISCOUNT"},
        )
        self.assertEqual(line_items[0]["subtotal"], "120.00")
        self.assertEqual(line_items[0]["total"], "60.00")

    def test_collect_line_items_emits_the_woosb_native_bundle_shape(self):
        """Priced parent, zero-rated children, ``_woosb_ids`` on the parent.

        Rewritten from ``..._includes_explicit_bundle_parent_at_zero_...``: that
        version pinned the *inverse* of what a website-created order looks like
        (verified against production order 16895), which is the whole of F-10.
        The ERPNext-side rows are unchanged — this is a wire format only, and the
        sum of the emitted line totals is identical either way.
        """
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
        # No `name`: the store's own product title must survive the push.
        self.assertNotIn("name", parent_entry)
        # The bundle price, taken from the children's ERPNext amounts.
        self.assertEqual(parent_entry["subtotal"], "120.00")
        self.assertEqual(parent_entry["total"], "120.00")
        self.assertEqual(parent_entry["product_id"], 202)
        self.assertEqual(
            parent_entry["meta_data"],
            [
                {"key": "erpnext_item_code", "value": "BUNDLE-001"},
                {
                    "key": "_woosb_ids",
                    "value": "303/{token}/1/{{}}".format(
                        token=outbound_sync._woosb_selection_token("ITEM-CHILD")
                    ),
                },
            ],
        )

        child_entry = line_items[1]
        self.assertNotIn("name", child_entry)
        self.assertEqual(child_entry["subtotal"], "0.00")
        self.assertEqual(child_entry["total"], "0.00")
        self.assertEqual(child_entry["product_id"], 303)
        self.assertEqual(
            child_entry["meta_data"],
            [
                {"key": "erpnext_item_code", "value": "ITEM-CHILD"},
                {"key": "_woosb_parent_id", "value": "202"},
            ],
        )

        # The invariant that protects the order total through the inversion.
        self.assertEqual(
            round(sum(outbound_sync.flt(entry["total"]) for entry in line_items), 2),
            round(sum(outbound_sync.flt(item.amount) for item in invoice.items), 2),
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

    def test_sync_sales_invoice_creates_missing_order_map_for_new_outbound_order(self):
        invoice = DummyInvoice(sales_invoice_state="Ready", woo_order_id=None)
        invoice.grand_total = 280
        client = DummyResponseClient(response={
            "id": 16600,
            "number": "16600",
            "status": "processing",
            "payment_method": "cod",
            "line_items": [],
        })
        mock_set_value = unittest.mock.MagicMock()
        inserted_maps = []

        def fake_get_doc(doctype_or_dict, name=None):
            if doctype_or_dict == "Sales Invoice":
                return invoice
            if doctype_or_dict == "Customer":
                return SimpleNamespace(name=invoice.customer, woo_customer_id="88")
            if isinstance(doctype_or_dict, dict) and doctype_or_dict.get("doctype") == "WooCommerce Order Map":
                doc = unittest.mock.MagicMock()
                doc.insert.side_effect = lambda ignore_permissions=True: inserted_maps.append(dict(doctype_or_dict)) or doc
                return doc
            raise AssertionError(f"Unexpected get_doc call: {doctype_or_dict!r}")

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
             unittest.mock.patch.object(outbound_sync, "_build_order_payload", return_value={"status": "processing", "payment_method": "cod"}), \
             unittest.mock.patch.object(outbound_sync, "now_datetime", return_value="2026-05-03 12:00:00"), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc", side_effect=fake_get_doc), \
             unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=False, set_value=mock_set_value, get_value=lambda *args, **kwargs: None)), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(order_sync, "_compute_order_hash", return_value="hash-16600"):
            result = outbound_sync.sync_sales_invoice(invoice.name, reason="test")

        self.assertEqual(result, {"status": "ok", "woo_order_id": 16600})
        self.assertEqual(len(inserted_maps), 1)
        self.assertEqual(inserted_maps[0]["woo_order_id"], 16600)
        self.assertEqual(inserted_maps[0]["erpnext_sales_invoice"], invoice.name)
        self.assertEqual(inserted_maps[0]["status"], "processing")
        self.assertEqual(inserted_maps[0]["payment_method"], "cod")
        self.assertEqual(inserted_maps[0]["hash"], "hash-16600")

    def test_sync_sales_invoice_creates_missing_order_map_for_amended_invoice(self):
        invoice = DummyInvoice(sales_invoice_state="Ready", woo_order_id=None, amended_from="ACC-SINV-OLD-001")
        invoice.grand_total = 400
        client = DummyResponseClient(response={
            "id": 14500,
            "number": "14500",
            "status": "processing",
            "payment_method": "cod",
            "line_items": [],
        }, existing_order={"id": 14500, "status": "processing", "line_items": []})
        mock_set_value = unittest.mock.MagicMock()
        inserted_maps = []

        def fake_get_value(doctype, name_or_filters, fieldname=None, as_dict=False):
            if doctype == "Sales Invoice" and name_or_filters == "ACC-SINV-OLD-001" and fieldname == "woo_order_id":
                return 14500
            if doctype == "WooCommerce Order Map" and name_or_filters == {"woo_order_id": 14500} and fieldname == "name":
                return None
            return None

        def fake_get_doc(doctype_or_dict, name=None):
            if doctype_or_dict == "Sales Invoice":
                return invoice
            if doctype_or_dict == "Customer":
                return SimpleNamespace(name=invoice.customer, woo_customer_id="88")
            if isinstance(doctype_or_dict, dict) and doctype_or_dict.get("doctype") == "WooCommerce Order Map":
                doc = unittest.mock.MagicMock()
                doc.insert.side_effect = lambda ignore_permissions=True: inserted_maps.append(dict(doctype_or_dict)) or doc
                return doc
            raise AssertionError(f"Unexpected get_doc call: {doctype_or_dict!r}")

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
             unittest.mock.patch.object(outbound_sync, "_build_order_payload", return_value={"status": "processing", "payment_method": "cod"}), \
             unittest.mock.patch.object(outbound_sync, "now_datetime", return_value="2026-05-03 12:00:00"), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc", side_effect=fake_get_doc), \
             unittest.mock.patch.object(outbound_sync.frappe, "db", _db_stub(exists=False, get_value=fake_get_value, set_value=mock_set_value)), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(order_sync, "_compute_order_hash", return_value="hash-14500"):
            result = outbound_sync.sync_sales_invoice(invoice.name, reason="test")

        self.assertEqual(result, {"status": "ok", "woo_order_id": 14500})
        self.assertEqual(len(inserted_maps), 1)
        self.assertEqual(inserted_maps[0]["woo_order_id"], 14500)
        self.assertEqual(inserted_maps[0]["erpnext_sales_invoice"], invoice.name)
        self.assertEqual(inserted_maps[0]["hash"], "hash-14500")

    def test_map_status_supports_out_for_delivery(self):
        self.assertEqual(order_sync._map_status("out-for-delivery"), {
            "docstatus": 1,
            "custom_state": "Out for Delivery",
            "is_paid": False,
        })

    def test_map_status_supports_processing_equivalent_aliases(self):
        expected = {
            "docstatus": 1,
            "custom_state": "Processing",
            "is_paid": False,
        }

        for status in (
            "pre-nasrcity",
            "pre-ismailia",
            "pre-hadayk",
            "pre-hadayek",
            "pre-dokki",
        ):
            with self.subTest(status=status):
                self.assertEqual(order_sync._map_status(status), expected)

    def test_build_order_payload_adds_and_removes_line_items_when_existing_do_not_match(self):
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

        # ERPNext is authoritative for the line set once the invoice is submitted.
        # A desired line with no counterpart on the order is a NEW item and must be
        # sent without an id so Woo appends it; the order's leftover line is one
        # ERPNext no longer carries and is removed with an integer quantity of 0.
        # Dropping either (the old behaviour) is what stopped items added in
        # ERPNext from ever reaching the store.
        line_items = payload["line_items"]
        self.assertEqual(len(line_items), 2)

        added = next(entry for entry in line_items if entry.get("product_id") == 101)
        self.assertNotIn("id", added)

        removal = next(entry for entry in line_items if entry.get("id") == 55)
        self.assertEqual(removal["quantity"], 0)
        self.assertIsInstance(removal["quantity"], int)

        self.assertNotIn(
            "unmapped_line_items",
            {meta.get("key") for meta in payload["meta_data"]},
        )

    def test_attach_existing_line_ids_reuses_remaining_bundle_child_slot_for_amended_swap(self):
        desired_line_items = [
            {
                "product_id": 12438,
                "variation_id": None,
                "quantity": 1,
                "meta_data": [{"key": "erpnext_item_code", "value": "Jarz Gathering Box"}],
                "name": "Jarz Gathering Box",
            },
            {
                "product_id": 371,
                "variation_id": None,
                "quantity": 1,
                "meta_data": [
                    {"key": "erpnext_item_code", "value": "Strawberry Medium"},
                    {"key": "_woosb_parent_id", "value": "12438"},
                ],
                "name": "Strawberry Medium",
            },
            {
                "product_id": 369,
                "variation_id": None,
                "quantity": 1,
                "meta_data": [
                    {"key": "erpnext_item_code", "value": "Blueberry Medium"},
                    {"key": "_woosb_parent_id", "value": "12438"},
                ],
                "name": "Blueberry Medium",
            },
            {
                "product_id": 217,
                "variation_id": None,
                "quantity": 1,
                "meta_data": [
                    {"key": "erpnext_item_code", "value": "Lotus Medium"},
                    {"key": "_woosb_parent_id", "value": "12438"},
                ],
                "name": "Lotus Medium",
            },
            {
                "product_id": 2284,
                "variation_id": None,
                "quantity": 1,
                "meta_data": [
                    {"key": "erpnext_item_code", "value": "Mango Medium"},
                    {"key": "_woosb_parent_id", "value": "12438"},
                ],
                "name": "Mango Medium",
            },
        ]
        existing_line_items = [
            {
                "id": 47733,
                "product_id": 12438,
                "variation_id": 0,
                "quantity": 1,
                "meta_data": [{"key": "erpnext_item_code", "value": "Jarz Gathering Box"}],
            },
            {
                "id": 47734,
                "product_id": 371,
                "variation_id": 0,
                "quantity": 1,
                "meta_data": [
                    {"key": "erpnext_item_code", "value": "Strawberry Medium"},
                    {"key": "_woosb_parent_id", "value": "12438"},
                ],
            },
            {
                "id": 47735,
                "product_id": 369,
                "variation_id": 0,
                "quantity": 1,
                "meta_data": [
                    {"key": "erpnext_item_code", "value": "Blueberry Medium"},
                    {"key": "_woosb_parent_id", "value": "12438"},
                ],
            },
            {
                "id": 47736,
                "product_id": 367,
                "variation_id": 0,
                "quantity": 1,
                "meta_data": [
                    {"key": "erpnext_item_code", "value": "Chocolate Hazelnut Medium"},
                    {"key": "_woosb_parent_id", "value": "12438"},
                ],
            },
            {
                "id": 47737,
                "product_id": 2284,
                "variation_id": 0,
                "quantity": 1,
                "meta_data": [
                    {"key": "erpnext_item_code", "value": "Mango Medium"},
                    {"key": "_woosb_parent_id", "value": "12438"},
                ],
            },
        ]

        matched, added, orphaned = outbound_sync._attach_existing_line_ids(
            desired_line_items, existing_line_items
        )

        self.assertEqual(added, [])
        self.assertEqual(orphaned, [])
        self.assertEqual(len(matched), 5)
        lotus_entry = next(
            entry
            for entry in matched
            if any(
                meta.get("key") == "erpnext_item_code" and meta.get("value") == "Lotus Medium"
                for meta in entry.get("meta_data", [])
            )
        )
        self.assertEqual(lotus_entry["id"], 47736)
        self.assertEqual(lotus_entry["product_id"], 217)

    def test_build_order_payload_formats_delivery_slot_from_start_time_and_duration(self):
        invoice = DummyInvoice(sales_invoice_state="Delivered")
        invoice.custom_delivery_date = "2026-05-02"
        invoice.custom_delivery_time_from = "19:00:00"
        invoice.custom_delivery_duration = 5400

        payload = _build_payload_for_delivery_test(invoice)
        metadata = {entry["key"]: entry["value"] for entry in payload["meta_data"]}

        self.assertEqual(metadata["_orddd_timestamp"], "1777680000")
        # Matches the shape ORDDD writes on a Woo-native checkout ("4 August, 2026"),
        # not the "%A, %B %d, %Y" the outbound push used to invent.
        self.assertEqual(metadata["Delivery Date"], "2 May, 2026")
        self.assertEqual(metadata["_orddd_delivery_date"], "2 May, 2026")
        self.assertEqual(metadata["_orddd_time_slot"], "19:00 - 20:30")
        self.assertEqual(metadata["Time Slot"], "19:00 - 20:30")
        self.assertEqual(metadata["_orddd_timeslot_timestamp"], "1777748400")
        self.assertEqual(metadata["_orddd_delivery_date_label"], "Delivery Date")
        self.assertEqual(metadata["_orddd_time_slot_label"], "Time Slot")

    def test_build_order_payload_reads_time_field_returned_as_timedelta(self):
        """Frappe hands a `Time` column back as a timedelta since midnight.

        That type used to fall through the coercer and return None, so every POS
        order reached WooCommerce with a delivery date but no time slot at all.
        """
        invoice = DummyInvoice(sales_invoice_state="Delivered")
        invoice.custom_delivery_date = "2026-05-02"
        invoice.custom_delivery_time_from = timedelta(seconds=82800)  # 23:00
        invoice.custom_delivery_duration = 5400.0

        payload = _build_payload_for_delivery_test(invoice)
        metadata = {entry["key"]: entry["value"] for entry in payload["meta_data"]}

        self.assertEqual(metadata["_orddd_time_slot"], "23:00 - 00:30")
        self.assertEqual(metadata["Time Slot"], "23:00 - 00:30")

    def test_build_order_payload_keeps_a_midnight_slot(self):
        """A midnight start is a real slot, not an absent one.

        A Frappe `Time` column of 00:00 comes back as `timedelta(0)`, which is
        falsy. The coercer's `if not raw` guard read that as "no time set" and
        silently dropped the whole time-slot block, so the Woo order showed a
        delivery date with no slot -- while every other hour of the day worked.
        """
        invoice = DummyInvoice(sales_invoice_state="Delivered")
        invoice.custom_delivery_date = "2026-05-02"
        invoice.custom_delivery_time_from = timedelta(0)  # 00:00
        invoice.custom_delivery_duration = 3600.0

        payload = _build_payload_for_delivery_test(invoice)
        metadata = {entry["key"]: entry["value"] for entry in payload["meta_data"]}

        self.assertEqual(metadata["_orddd_time_slot"], "00:00 - 01:00")
        self.assertEqual(metadata["Time Slot"], "00:00 - 01:00")
        # Midnight start == the date's own midnight timestamp, no offset.
        self.assertEqual(
            metadata["_orddd_timeslot_timestamp"], metadata["_orddd_timestamp"]
        )

    def test_build_order_payload_keeps_a_midnight_slot_from_every_representation(self):
        """The three shapes a 00:00 start can arrive in must agree."""
        for raw in (timedelta(0), dt_time(0, 0), "00:00:00"):
            with self.subTest(raw=raw):
                invoice = DummyInvoice(sales_invoice_state="Delivered")
                invoice.custom_delivery_date = "2026-05-02"
                invoice.custom_delivery_time_from = raw
                invoice.custom_delivery_duration = 3600.0

                payload = _build_payload_for_delivery_test(invoice)
                metadata = {e["key"]: e["value"] for e in payload["meta_data"]}

                self.assertEqual(metadata["Time Slot"], "00:00 - 01:00")

    def test_build_order_payload_falls_back_to_a_midnight_legacy_time(self):
        """The legacy `or` chain dropped a midnight `custom_delivery_time` too."""
        invoice = DummyInvoice(sales_invoice_state="Delivered")
        invoice.custom_delivery_date = "2026-05-02"
        invoice.custom_delivery_time_from = None
        invoice.custom_delivery_duration = None
        invoice.custom_delivery_time = timedelta(0)  # 00:00

        payload = _build_payload_for_delivery_test(invoice)
        metadata = {entry["key"]: entry["value"] for entry in payload["meta_data"]}

        self.assertEqual(metadata["Time Slot"], "00:00")

    def test_coerce_delivery_time_still_rejects_genuinely_unset_values(self):
        """Widening the guard must not make blanks look like midnight."""
        for raw in (None, "", "   ", "not a time"):
            with self.subTest(raw=raw):
                self.assertIsNone(outbound_sync._coerce_delivery_time(raw))

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

    def test_build_order_payload_prefers_invoice_snapshot_contact_names(self):
        invoice = DummyInvoice(sales_invoice_state="Delivered")
        invoice.customer_name = "Canonical Customer Name"
        invoice.woo_order_display_name = "Snapshot Display"
        invoice.woo_billing_name = "Billing Snapshot"
        invoice.woo_shipping_name = "Shipping Snapshot"
        invoice.woo_order_phone = "0111222333"
        invoice.woo_order_email = "snapshot@example.com"

        payload = _build_payload_for_delivery_test(invoice)

        self.assertEqual(payload["billing"]["first_name"], "Billing")
        self.assertEqual(payload["billing"]["last_name"], "Snapshot")
        self.assertEqual(payload["billing"]["company"], "Billing Snapshot")
        self.assertEqual(payload["shipping"]["first_name"], "Shipping")
        self.assertEqual(payload["shipping"]["last_name"], "Snapshot")
        self.assertEqual(payload["shipping"]["company"], "Shipping Snapshot")
        self.assertEqual(payload["billing"]["phone"], "0111222333")
        self.assertEqual(payload["shipping"]["email"], "snapshot@example.com")

    def test_build_order_payload_sets_paid_meta_for_paid_cod_orders(self):
        invoice = DummyInvoice(sales_invoice_state="Recieved")
        invoice.outstanding_amount = 0
        invoice.custom_payment_method = "Cash"
        invoice.modified = "2026-05-03 12:34:56"

        payload = _build_payload_for_delivery_test(invoice)
        metadata = {entry["key"]: entry["value"] for entry in payload["meta_data"]}

        self.assertEqual(metadata["_date_paid"], "2026-05-03T12:34:56")
        self.assertEqual(metadata["_date_paid_gmt"], "2026-05-03T12:34:56")

    def test_build_order_payload_skips_paid_meta_for_paid_non_cod_orders(self):
        invoice = DummyInvoice(sales_invoice_state="Recieved")
        invoice.outstanding_amount = 0
        invoice.custom_payment_method = "Instapay"
        invoice.modified = "2026-05-03 12:34:56"

        payload = _build_payload_for_delivery_test(invoice)
        metadata = {entry["key"]: entry["value"] for entry in payload["meta_data"]}

        self.assertNotIn("_date_paid", metadata)
        self.assertNotIn("_date_paid_gmt", metadata)