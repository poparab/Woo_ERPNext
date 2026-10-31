"""The POS pin reaches the store as DropPin order meta, like a website pin.

Before this, a POS order's pin lived only in ERPNext: the store showed the order
with no location (``droppin.has_pin`` false) while every website order carried
``_dpn_lat`` / ``_dpn_lng``. Covers:

1. The keys and values DropPin itself writes, with DropPin's source words.
2. Nothing is sent when there is nothing to say (no pin, centroid, removed pin,
   store already holds the point), so status pushes stay quiet.
3. A pin already on the store is replaced only by a staff pin set for THIS
   order after the store's pin, never by a customer pin from another order.
4. The echo: the inbound sync must not read our own pushed pin back as a
   ``customer_pin`` (invariant 4), while a pin set later on the store still is.
5. The in-sync check treats a pin change as a reason to PUT.
"""

from __future__ import annotations

import unittest

from jarz_woocommerce_integration.services import geo_passthrough, outbound_sync

INVOICE_TS = 1_793_400_000
LATER_TS = INVOICE_TS + 600
EARLIER_TS = INVOICE_TS - 86_400


def _address(lat=30.094669, lng=31.377227, source="pos_link", accuracy=0.0):
    return {
        "custom_latitude": lat,
        "custom_longitude": lng,
        "custom_geo_source": source,
        "custom_geo_accuracy_m": accuracy,
    }


def _meta_map(entries):
    return {entry["key"]: entry["value"] for entry in entries}


def _build(address, existing=None, *, updated=LATER_TS, created=INVOICE_TS, flag=""):
    return geo_passthrough.build_outbound_pin_meta(
        address,
        existing,
        address_updated_ts=updated,
        invoice_created_ts=created,
        pin_flag=flag,
        now_ts=LATER_TS,
    )


class OutboundPinMetaShapeTests(unittest.TestCase):
    def test_new_order_carries_droppin_keys(self):
        meta = _meta_map(_build(_address(), None, updated=EARLIER_TS))
        self.assertEqual(meta["_dpn_lat"], "30.094669")
        self.assertEqual(meta["_dpn_lng"], "31.377227")
        self.assertEqual(meta["_dpn_pin_source"], "staff_manual")
        self.assertEqual(meta["_dpn_pin_set_at"], str(EARLIER_TS))
        self.assertEqual(meta["_dpn_pin_set_by"], "0")
        self.assertEqual(meta["_dpn_accuracy"], "")
        self.assertEqual(meta[geo_passthrough.PUSHED_PIN_META_KEY], "30.094669,31.377227")

    def test_coverage_verdict_is_never_invented(self):
        meta = _meta_map(_build(_address(), None))
        for key in ("_dpn_area_status", "_dpn_area_mismatch", "_dpn_suggested_area", "_dpn_area_code"):
            self.assertNotIn(key, meta)

    def test_saved_customer_pin_is_a_prefill(self):
        meta = _meta_map(_build(_address(source="customer_pin"), None))
        self.assertEqual(meta["_dpn_pin_source"], "prefill")

    def test_known_accuracy_is_sent_in_whole_metres(self):
        meta = _meta_map(_build(_address(source="courier_verified", accuracy=12.4), None))
        self.assertEqual(meta["_dpn_accuracy"], "12")
        self.assertEqual(meta["_dpn_pin_source"], "staff_manual")

    def test_set_at_falls_back_to_now(self):
        meta = _meta_map(_build(_address(), None, updated=None))
        self.assertEqual(meta["_dpn_pin_set_at"], str(LATER_TS))


class OutboundPinMetaSilenceTests(unittest.TestCase):
    def test_no_pin(self):
        self.assertEqual(_build(_address(lat=0, lng=0), None), [])
        self.assertEqual(_build(None, None), [])

    def test_outside_egypt(self):
        self.assertEqual(_build(_address(lat=48.8566, lng=2.3522), None), [])

    def test_territory_centroid_is_not_a_pin(self):
        self.assertEqual(_build(_address(source="territory_centroid"), None), [])

    def test_customer_removed_pin_on_store(self):
        self.assertEqual(_build(_address(), None, flag="area_mismatch,pin_removed"), [])

    def test_store_already_holds_the_point(self):
        existing = {"_dpn_lat": "30.0946690", "_dpn_lng": "31.377227", "_dpn_pin_source": "staff_manual"}
        self.assertEqual(_build(_address(), existing), [])


class OutboundPinMetaReplaceTests(unittest.TestCase):
    STORE = {
        "_dpn_lat": "30.0806923",
        "_dpn_lng": "31.3064801",
        "_dpn_pin_source": "customer_checkout",
        "_dpn_pin_set_at": str(INVOICE_TS - 60),
        "_dpn_area_status": "inside",
        "_dpn_area_mismatch": "yes",
        "_dpn_suggested_area": "EGHELIO",
    }

    def test_staff_correction_after_the_order_replaces_and_clears_verdict(self):
        meta = _meta_map(_build(_address(), dict(self.STORE), updated=LATER_TS))
        self.assertEqual(meta["_dpn_lat"], "30.094669")
        self.assertEqual(meta["_dpn_pin_source"], "staff_manual")
        self.assertEqual(meta["_dpn_area_status"], "")
        self.assertEqual(meta["_dpn_area_mismatch"], "")
        self.assertEqual(meta["_dpn_suggested_area"], "")
        self.assertNotIn("_dpn_outside_coverage", meta)

    def test_address_pin_older_than_the_order_never_replaces(self):
        self.assertEqual(_build(_address(), dict(self.STORE), updated=EARLIER_TS), [])

    def test_address_pin_older_than_store_pin_never_replaces(self):
        store = dict(self.STORE, _dpn_pin_set_at=str(LATER_TS + 60))
        self.assertEqual(_build(_address(), store, updated=LATER_TS), [])

    def test_customer_pin_from_another_order_never_replaces(self):
        self.assertEqual(_build(_address(source="customer_pin"), dict(self.STORE), updated=LATER_TS), [])

    def test_undated_address_pin_never_replaces(self):
        self.assertEqual(_build(_address(), dict(self.STORE), updated=None), [])


class PushedPinEchoTests(unittest.TestCase):
    def _order(self, meta):
        return {
            "billing": {"address_1": "x"},
            "shipping": {"address_1": "x"},
            "meta_data": [{"key": key, "value": value} for key, value in meta.items()],
        }

    def test_own_push_is_not_read_back_as_customer_pin(self):
        entries = _build(_address(), None)
        order = self._order(_meta_map(entries))
        self.assertIsNone(geo_passthrough.extract_order_pin(order))
        billing, shipping = geo_passthrough.resolve_order_pins(order)
        self.assertIsNone(billing)
        self.assertIsNone(shipping)

    def test_snapshot_still_records_the_pushed_pin(self):
        order = self._order(_meta_map(_build(_address(), None)))
        delivery = geo_passthrough.resolve_delivery_pin(order)
        self.assertAlmostEqual(delivery["latitude"], 30.094669)
        self.assertEqual(delivery["source"], "staff_manual")
        self.assertEqual(delivery["flags"], "")

    def test_pin_set_on_store_after_our_push_is_read(self):
        meta = _meta_map(_build(_address(), None))
        meta.update({"_dpn_lat": "30.0604488", "_dpn_lng": "31.3527736", "_dpn_pin_source": "customer_link"})
        pin = geo_passthrough.extract_order_pin(self._order(meta))
        self.assertIsNotNone(pin)
        self.assertAlmostEqual(pin.latitude, 30.060449)


class InSyncComparisonTests(unittest.TestCase):
    def _existing(self, **meta):
        return {"status": "processing", "meta_data": [{"key": k, "value": v} for k, v in meta.items()]}

    def test_pin_meta_marks_order_dirty(self):
        payload = {"status": "processing", "meta_data": _build(_address(), None)}
        self.assertTrue(outbound_sync._order_payload_requires_update(self._existing(), payload))

    def test_no_pin_meta_keeps_order_in_sync(self):
        payload = {"status": "processing", "meta_data": []}
        existing = self._existing(_dpn_lat="30.0946690", _dpn_lng="31.377227")
        self.assertFalse(outbound_sync._order_payload_requires_update(existing, payload))


if __name__ == "__main__":
    unittest.main()
