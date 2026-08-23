"""Order origin (WooCommerce attribution) on the orders this app creates.

Every order we pushed over the REST API read **"Unknown"** in the store's
Origin column, because WooCommerce builds that column from the
``_wc_order_attribution_*`` meta its own checkout script posts and a REST create
carries none of it. The store therefore could not tell a POS sale from a web
sale -- in the orders list or in Analytics -> Attribution.

Three things are pinned here:

1. **A created order names itself.** ``source_type`` is ``utm``: the only value
   that lets us supply our own label. ``typein``/``admin`` render hardcoded
   strings ("Direct"/"Web admin") and anything unrecognised falls through to
   "Unknown", which is exactly the state this feature exists to end.

2. **An update never carries the origin.** A web-born order holds real
   attribution -- the campaign that actually won the sale. Stamping ours on
   every status PUT would overwrite it with a POS label and silently destroy the
   store's marketing data.

3. **The origin keys stay out of the already-in-sync comparison.** Put them in
   and every web order looks permanently dirty, which is the same data loss
   arriving through a different door.

Pure unittest + mocks: no site, no DB, no network.
"""

from __future__ import annotations

import unittest
import unittest.mock
from types import SimpleNamespace

from jarz_woocommerce_integration.services import outbound_sync, tracking_link


PREFIX = outbound_sync._WOO_ATTRIBUTION_META_PREFIX


class FakeInvoice:
    """Minimal Sales Invoice stand-in with the fields the payload builder reads."""

    def __init__(self, *, pos_profile: str | None = None):
        self.name = "ACC-SINV-2026-00042"
        self.customer = "CUST-0001"
        self.customer_name = "Test Customer"
        self.currency = "EGP"
        self.docstatus = 1
        self.custom_sales_invoice_state = "Received"
        self.sales_invoice_state = "Received"
        self.pos_profile = pos_profile
        # Non-zero on purpose: keeps set_paid False so _build_paid_metadata
        # returns early and no assertion here depends on the site timezone.
        self.outstanding_amount = 10
        self.custom_payment_method = None
        self.mode_of_payment = None
        self.customer_address = None
        self.shipping_address_name = None
        self.custom_tracking_token = None
        self.custom_tracking_url = None
        self.items = []
        self.flags = SimpleNamespace(ignore_woo_outbound=False)

    def get(self, fieldname, default=None):
        return getattr(self, fieldname, default)


def _outbound_cfg(*, origin: str = "ERPNext POS"):
    return outbound_sync.OutboundConfig(
        enable_customer_push=True,
        enable_order_push=True,
        payment_cod="cod",
        payment_instapay="instapay",
        payment_wallet="wallet",
        shipping_method_id="flat_rate",
        shipping_method_title="Shipping",
        order_origin_source=origin,
    )


def _build_payload(invoice, cfg, *, existing_order=None):
    line_items = [{
        "product_id": 555,
        "quantity": 1,
        "total": "100.00",
        "subtotal": "100.00",
    }]
    customer = SimpleNamespace(
        name=invoice.customer,
        customer_name="Test Customer",
        woo_customer_id="88",
        email_id="test@example.com",
        mobile_no="01000000000",
        phone=None,
    )
    address = {"address_1": "12 Nile Street", "email": "test@example.com", "phone": "01000000000"}
    settings = SimpleNamespace(enable_outbound_tracking_url=0, tracking_base_url=None)

    with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(settings, cfg)), \
         unittest.mock.patch.object(outbound_sync, "_collect_line_items", return_value=(line_items, [])), \
         unittest.mock.patch.object(outbound_sync, "_compute_shipping_total", return_value=0), \
         unittest.mock.patch.object(
             outbound_sync,
             "_build_customer_payload",
             return_value={"billing": dict(address), "shipping": dict(address)},
         ), \
         unittest.mock.patch.object(outbound_sync, "get_customer_woo_id", return_value="88"), \
         unittest.mock.patch.object(tracking_link, "invoice_token_field_available", return_value=False), \
         unittest.mock.patch.object(outbound_sync.frappe, "get_doc", return_value=customer):
        return outbound_sync._build_order_payload(invoice, cfg, existing_order=existing_order)


def _meta(payload):
    return {entry["key"]: entry["value"] for entry in payload.get("meta_data") or []}


class TestOrderOriginOnCreate(unittest.TestCase):
    def test_a_created_order_carries_the_attribution_source(self):
        meta = _meta(_build_payload(FakeInvoice(), _outbound_cfg()))
        self.assertEqual(meta[f"{PREFIX}source_type"], "utm")
        self.assertEqual(meta[f"{PREFIX}utm_source"], "ERPNext POS")
        self.assertEqual(meta[outbound_sync._ORIGIN_META_KEY], "ERPNext POS")

    def test_the_label_is_configurable(self):
        meta = _meta(_build_payload(FakeInvoice(), _outbound_cfg(origin="Jarz POS")))
        self.assertEqual(meta[f"{PREFIX}utm_source"], "Jarz POS")
        self.assertEqual(meta[outbound_sync._ORIGIN_META_KEY], "Jarz POS")

    def test_a_blank_label_falls_back_rather_than_pushing_an_empty_origin(self):
        meta = _meta(_build_payload(FakeInvoice(), _outbound_cfg(origin="   ")))
        self.assertEqual(
            meta[f"{PREFIX}utm_source"], outbound_sync._DEFAULT_ORDER_ORIGIN_SOURCE
        )

    def test_a_till_sale_names_its_pos_profile(self):
        meta = _meta(
            _build_payload(FakeInvoice(pos_profile="Jarz POS - Nasr City"), _outbound_cfg())
        )
        self.assertEqual(meta[f"{PREFIX}utm_medium"], "pos")
        self.assertEqual(meta[f"{PREFIX}utm_campaign"], "Jarz POS - Nasr City")

    def test_a_non_pos_invoice_has_no_campaign(self):
        meta = _meta(_build_payload(FakeInvoice(), _outbound_cfg()))
        self.assertEqual(meta[f"{PREFIX}utm_medium"], "erpnext")
        self.assertNotIn(f"{PREFIX}utm_campaign", meta)


class TestOrderOriginIsCreateOnly(unittest.TestCase):
    def test_an_update_never_overwrites_the_stores_own_attribution(self):
        existing_order = {
            "id": 16901,
            "status": "processing",
            "line_items": [],
            "meta_data": [
                {"key": f"{PREFIX}source_type", "value": "organic"},
                {"key": f"{PREFIX}utm_source", "value": "google"},
            ],
        }
        meta = _meta(_build_payload(FakeInvoice(), _outbound_cfg(), existing_order=existing_order))
        for key in (
            f"{PREFIX}source_type",
            f"{PREFIX}utm_source",
            f"{PREFIX}utm_medium",
            outbound_sync._ORIGIN_META_KEY,
        ):
            self.assertNotIn(key, meta)

    def test_origin_keys_are_not_compared_for_staleness(self):
        # Comparing them would mark every web order dirty forever, and each PUT
        # would then replace its real attribution with ours.
        for key in (
            f"{PREFIX}source_type",
            f"{PREFIX}utm_source",
            f"{PREFIX}utm_medium",
            f"{PREFIX}utm_campaign",
            outbound_sync._ORIGIN_META_KEY,
        ):
            self.assertNotIn(key, outbound_sync._ORDER_SYNC_META_KEYS_TO_COMPARE)


class TestApplyOriginMetadata(unittest.TestCase):
    def test_it_is_idempotent_and_never_duplicates_a_key(self):
        payload: dict = {}
        cfg = _outbound_cfg()
        invoice = FakeInvoice(pos_profile="Jarz POS - Nasr City")
        outbound_sync._apply_origin_metadata(payload, invoice, cfg)
        outbound_sync._apply_origin_metadata(payload, invoice, cfg)
        keys = [entry["key"] for entry in payload["meta_data"]]
        self.assertEqual(len(keys), len(set(keys)))

    def test_it_leaves_a_key_the_payload_already_carries(self):
        payload = {"meta_data": [{"key": f"{PREFIX}utm_source", "value": "google"}]}
        outbound_sync._apply_origin_metadata(payload, FakeInvoice(), _outbound_cfg())
        self.assertEqual(_meta(payload)[f"{PREFIX}utm_source"], "google")


if __name__ == "__main__":
    unittest.main()
