"""Per-invoice delivery-pin snapshot, Egypt bounds and DropPin review flags.

What these guard:

1. A pin outside Egypt (Paris was accepted on staging) is never usable as a
   delivery pin, but is still detected and surfaced as ``outside_egypt``.
2. DropPin's ``_dpn_area_status`` / ``_dpn_area_mismatch`` / ``_dpn_pin_source``
   are read and mapped to the invoice flag/source fields.
3. The snapshot is written only when it changes, via a hook-free
   ``db.set_value(update_modified=False)``, and never touches the courier's
   proof-of-delivery GPS fields.
4. A bilingual Comment is added only when the flag set changes to non-empty.
5. The order poll pre-check lets an order through when its pin changed on Woo
   (the order hash excludes meta) and stays quiet when nothing changed or when
   the snapshot fields do not exist (never an endless requeue).
6. The ``unchanged`` branch of order processing refreshes the pin.
"""

from __future__ import annotations

from contextlib import contextmanager
import unittest
from unittest.mock import MagicMock, patch

from jarz_woocommerce_integration.services import geo_passthrough, order_sync


CAIRO = (30.0444, 31.2357)
PARIS = (48.8566, 2.3522)
SNAPSHOT_FIELDS = geo_passthrough.INVOICE_PIN_FIELDS
COURIER_GPS_FIELDS = ("custom_delivery_" + "latitude", "custom_delivery_" + "longitude")


def _order(*, meta=None, billing=None, shipping=None, order_id=17001):
    return {
        "id": order_id,
        "number": str(order_id),
        "status": "processing",
        "billing": billing if billing is not None else {
            "address_1": "12 Nile Street",
            "address_2": "",
            "city": "Cairo",
            "country": "EG",
        },
        "shipping": shipping if shipping is not None else {},
        "meta_data": meta if meta is not None else [],
    }


def _dpn_meta(lat="30.0444", lng="31.2357", **extra):
    meta = [
        {"key": "_dpn_lat", "value": lat},
        {"key": "_dpn_lng", "value": lng},
    ]
    for key, value in extra.items():
        meta.append({"key": f"_dpn_{key}", "value": value})
    return meta


def _stored(lat=0.0, lng=0.0, source="", flags=""):
    return {
        "custom_delivery_pin_latitude": lat,
        "custom_delivery_pin_longitude": lng,
        "custom_delivery_pin_source": source,
        "custom_delivery_pin_flag": flags,
    }


@contextmanager
def _snapshot_db(stored_row, *, fields_available=True, address_row=None, set_value_error=None):
    """Stub the SI reads/writes and Comment inserts the snapshot code makes."""
    writes: list[dict] = []
    comments: list[dict] = []

    def _get_value(doctype, name=None, fieldname=None, *args, **kwargs):
        if doctype == "Sales Invoice" and isinstance(fieldname, (list, tuple)) and \
                tuple(fieldname) == tuple(SNAPSHOT_FIELDS):
            return dict(stored_row) if stored_row is not None else None
        if doctype == "Sales Invoice":
            return address_row or {}
        return None

    def _set_value(doctype, name, fieldname, value=None, **kwargs):
        if set_value_error:
            raise set_value_error
        writes.append({"doctype": doctype, "name": name, "fieldname": fieldname, "kwargs": kwargs})

    def _get_doc(payload, *args, **kwargs):
        doc = MagicMock()
        doc.insert.side_effect = lambda **kw: comments.append(payload)
        return doc

    with patch.object(geo_passthrough, "invoice_pin_fields_available", return_value=fields_available), \
         patch.object(geo_passthrough.frappe.db, "get_value", side_effect=_get_value), \
         patch.object(geo_passthrough.frappe.db, "set_value", side_effect=_set_value), \
         patch.object(geo_passthrough.frappe, "get_doc", side_effect=_get_doc):
        yield writes, comments


# ---------------------------------------------------------------------------
# A. Egypt bounds
# ---------------------------------------------------------------------------


class TestEgyptBounds(unittest.TestCase):
    def test_cairo_is_a_usable_pin(self):
        pin = geo_passthrough.make_pin(*CAIRO)
        self.assertEqual((pin.latitude, pin.longitude), CAIRO)

    def test_paris_is_rejected_as_a_delivery_pin(self):
        self.assertIsNone(geo_passthrough.make_pin(*PARIS))
        self.assertIsNone(geo_passthrough.extract_order_pin(_order(meta=_dpn_meta(*map(str, PARIS)))))

    def test_box_edges(self):
        self.assertIsNotNone(geo_passthrough.make_pin(22.0, 24.5))
        self.assertIsNotNone(geo_passthrough.make_pin(32.0, 37.0))
        self.assertIsNone(geo_passthrough.make_pin(21.99, 31.0))
        self.assertIsNone(geo_passthrough.make_pin(30.0, 37.01))

    def test_maps_link_outside_egypt_is_not_a_pin_but_is_detectable(self):
        link = "https://maps.google.com/?q=48.8566,2.3522"
        self.assertIsNone(geo_passthrough.parse_maps_link(link))
        raw = geo_passthrough.parse_maps_link(link, egypt_only=False)
        self.assertEqual((raw.latitude, raw.longitude), PARIS)

    def test_paris_pin_never_reaches_the_address(self):
        with patch.object(geo_passthrough, "geo_fields_available", return_value=True), \
             patch.object(geo_passthrough.frappe.db, "get_value", return_value={}), \
             patch.object(geo_passthrough.frappe.db, "set_value") as set_value:
            self.assertFalse(geo_passthrough.apply_geo_pin("ADDR-1", *PARIS))
        set_value.assert_not_called()

    def test_paris_pin_is_flagged_and_carries_no_coordinates(self):
        delivery = geo_passthrough.resolve_delivery_pin(_order(meta=_dpn_meta(*map(str, PARIS))))
        self.assertEqual(delivery["latitude"], 0.0)
        self.assertEqual(delivery["longitude"], 0.0)
        self.assertEqual(delivery["source"], "")
        self.assertEqual(delivery["flags"], "outside_egypt")

    def test_paris_meta_with_a_cairo_link_uses_the_link_and_keeps_the_flag(self):
        delivery = geo_passthrough.resolve_delivery_pin(_order(
            meta=_dpn_meta(*map(str, PARIS)),
            billing={"address_1": "12 Nile Street",
                     "address_2": "https://maps.google.com/?q=30.0444,31.2357"},
        ))
        self.assertEqual((delivery["latitude"], delivery["longitude"]), CAIRO)
        self.assertEqual(delivery["source"], "maps_link")
        self.assertEqual(delivery["flags"], "outside_egypt")


# ---------------------------------------------------------------------------
# B. Flags and source
# ---------------------------------------------------------------------------


class TestDeliveryPinFlags(unittest.TestCase):
    def _flags(self, **extra):
        return geo_passthrough.resolve_delivery_pin(_order(meta=_dpn_meta(**extra)))["flags"]

    def test_inside_area_is_not_flagged(self):
        self.assertEqual(self._flags(area_status="inside", area_mismatch="no"), "")
        self.assertEqual(self._flags(area_status="Inside"), "")

    def test_any_non_inside_area_status_is_outside_area(self):
        for status in ("outside", "OUTSIDE", "unknown_zone"):
            with self.subTest(status=status):
                self.assertEqual(self._flags(area_status=status), "outside_area")

    def test_blank_area_status_is_not_flagged(self):
        self.assertEqual(self._flags(area_status=""), "")

    def test_area_mismatch_truthy_values(self):
        for value in ("yes", "YES", "true", "1", True):
            with self.subTest(value=value):
                self.assertEqual(self._flags(area_mismatch=value), "area_mismatch")
        for value in ("no", "0", "false", ""):
            with self.subTest(value=value):
                self.assertEqual(self._flags(area_mismatch=value), "")

    def test_flags_are_sorted_and_comma_joined(self):
        delivery = geo_passthrough.resolve_delivery_pin(_order(meta=_dpn_meta(
            *map(str, PARIS), area_status="outside", area_mismatch="yes",
        )))
        self.assertEqual(delivery["flags"], "area_mismatch,outside_area,outside_egypt")

    def test_order_without_pin_or_flags_is_empty(self):
        self.assertEqual(
            geo_passthrough.resolve_delivery_pin(_order()),
            {"latitude": 0.0, "longitude": 0.0, "source": "", "flags": ""},
        )

    def test_non_dict_is_survivable(self):
        self.assertEqual(geo_passthrough.resolve_delivery_pin(None)["flags"], "")


class TestDeliveryPinSource(unittest.TestCase):
    def test_droppin_source_is_passed_through(self):
        for raw in ("prefill", "customer_checkout", "Customer_Checkout"):
            with self.subTest(raw=raw):
                delivery = geo_passthrough.resolve_delivery_pin(_order(meta=_dpn_meta(pin_source=raw)))
                self.assertEqual(delivery["source"], raw.lower())
                self.assertEqual((delivery["latitude"], delivery["longitude"]), CAIRO)

    def test_droppin_without_source_falls_back(self):
        delivery = geo_passthrough.resolve_delivery_pin(_order(meta=_dpn_meta()))
        self.assertEqual(delivery["source"], "droppin")

    def test_jarz_meta_is_jarz_checkout(self):
        delivery = geo_passthrough.resolve_delivery_pin(_order(meta=[
            {"key": "_jarz_lat", "value": "30.0444"},
            {"key": "_jarz_lng", "value": "31.2357"},
        ]))
        self.assertEqual(delivery["source"], "jarz_checkout")

    def test_address_link_is_maps_link(self):
        delivery = geo_passthrough.resolve_delivery_pin(_order(billing={
            "address_1": "12 Nile Street",
            "address_2": "https://maps.google.com/?q=30.0444,31.2357",
        }))
        self.assertEqual(delivery["source"], "maps_link")
        self.assertEqual((delivery["latitude"], delivery["longitude"]), CAIRO)

    def test_routing_matches_resolve_order_pins(self):
        """Shipping with text is the destination; a billing link does not leak in."""
        order = _order(
            billing={"address_1": "12 Nile Street",
                     "address_2": "https://maps.google.com/?q=29.9000,31.1000"},
            shipping={"address_1": "5 Tahrir Square",
                      "address_2": "https://maps.google.com/?q=30.0444,31.2357"},
        )
        delivery = geo_passthrough.resolve_delivery_pin(order)
        _billing_pin, shipping_pin = geo_passthrough.resolve_order_pins(order)
        self.assertEqual((delivery["latitude"], delivery["longitude"]),
                         (shipping_pin.latitude, shipping_pin.longitude))

    def test_meta_pin_outranks_the_destination_link(self):
        delivery = geo_passthrough.resolve_delivery_pin(_order(
            meta=_dpn_meta(pin_source="customer_checkout"),
            shipping={"address_1": "5 Tahrir Square",
                      "address_2": "https://maps.google.com/?q=29.9000,31.1000"},
        ))
        self.assertEqual((delivery["latitude"], delivery["longitude"]), CAIRO)
        self.assertEqual(delivery["source"], "customer_checkout")


# ---------------------------------------------------------------------------
# C. Snapshot write
# ---------------------------------------------------------------------------


class TestApplyInvoicePinSnapshot(unittest.TestCase):
    def test_snapshot_field_names_never_touch_the_courier_gps(self):
        for field in COURIER_GPS_FIELDS:
            self.assertNotIn(field, SNAPSHOT_FIELDS)
        self.assertEqual(SNAPSHOT_FIELDS, (
            "custom_delivery_pin_latitude",
            "custom_delivery_pin_longitude",
            "custom_delivery_pin_source",
            "custom_delivery_pin_flag",
        ))

    def test_first_write_on_an_empty_snapshot(self):
        order = _order(meta=_dpn_meta(pin_source="customer_checkout"))
        with _snapshot_db(_stored()) as (writes, comments):
            self.assertTrue(geo_passthrough.apply_invoice_pin_snapshot(order, "SINV-1"))
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0]["doctype"], "Sales Invoice")
        self.assertEqual(writes[0]["name"], "SINV-1")
        self.assertEqual(writes[0]["fieldname"], {
            "custom_delivery_pin_latitude": 30.0444,
            "custom_delivery_pin_longitude": 31.2357,
            "custom_delivery_pin_source": "customer_checkout",
            "custom_delivery_pin_flag": "",
        })
        self.assertIs(writes[0]["kwargs"].get("update_modified"), False)
        for field in COURIER_GPS_FIELDS:
            self.assertNotIn(field, writes[0]["fieldname"])
        self.assertEqual(comments, [])

    def test_identical_snapshot_is_not_rewritten(self):
        order = _order(meta=_dpn_meta(pin_source="prefill"))
        with _snapshot_db(_stored(30.0444, 31.2357, "prefill", "")) as (writes, comments):
            self.assertFalse(geo_passthrough.apply_invoice_pin_snapshot(order, "SINV-1"))
        self.assertEqual(writes, [])
        self.assertEqual(comments, [])

    def test_stored_none_values_read_as_empty(self):
        with _snapshot_db(_stored(None, None, None, None)) as (writes, _comments):
            self.assertFalse(geo_passthrough.apply_invoice_pin_snapshot(_order(), "SINV-1"))
        self.assertEqual(writes, [])

    def test_moved_pin_is_rewritten(self):
        order = _order(meta=_dpn_meta(pin_source="customer_checkout"))
        with _snapshot_db(_stored(29.9, 31.1, "customer_checkout", "")) as (writes, comments):
            self.assertTrue(geo_passthrough.apply_invoice_pin_snapshot(order, "SINV-1"))
        self.assertEqual(writes[0]["fieldname"]["custom_delivery_pin_latitude"], 30.0444)
        self.assertEqual(comments, [])

    def test_new_flags_add_one_bilingual_comment(self):
        order = _order(meta=_dpn_meta(area_status="outside"))
        with _snapshot_db(_stored(30.0444, 31.2357, "droppin", "")) as (writes, comments):
            self.assertTrue(geo_passthrough.apply_invoice_pin_snapshot(order, "SINV-1"))
        self.assertEqual(writes[0]["fieldname"]["custom_delivery_pin_flag"], "outside_area")
        self.assertEqual(len(comments), 1)
        comment = comments[0]
        self.assertEqual(comment["doctype"], "Comment")
        self.assertEqual(comment["comment_type"], "Comment")
        self.assertEqual(comment["reference_doctype"], "Sales Invoice")
        self.assertEqual(comment["reference_name"], "SINV-1")
        self.assertIn("outside the delivery area", comment["content"])
        self.assertIn("خارج منطقة التوصيل", comment["content"])

    def test_unchanged_flags_add_no_comment_even_when_the_pin_moves(self):
        order = _order(meta=_dpn_meta(area_status="outside"))
        with _snapshot_db(_stored(29.9, 31.1, "droppin", "outside_area")) as (writes, comments):
            self.assertTrue(geo_passthrough.apply_invoice_pin_snapshot(order, "SINV-1"))
        self.assertEqual(len(writes), 1)
        self.assertEqual(comments, [])

    def test_cleared_flags_add_no_comment(self):
        order = _order(meta=_dpn_meta(area_status="inside"))
        with _snapshot_db(_stored(30.0444, 31.2357, "droppin", "outside_area")) as (writes, comments):
            self.assertTrue(geo_passthrough.apply_invoice_pin_snapshot(order, "SINV-1"))
        self.assertEqual(writes[0]["fieldname"]["custom_delivery_pin_flag"], "")
        self.assertEqual(comments, [])

    def test_changed_flag_set_adds_a_comment(self):
        order = _order(meta=_dpn_meta(area_status="outside", area_mismatch="yes"))
        with _snapshot_db(_stored(30.0444, 31.2357, "droppin", "outside_area")) as (_writes, comments):
            geo_passthrough.apply_invoice_pin_snapshot(order, "SINV-1")
        self.assertEqual(len(comments), 1)

    def test_missing_fields_is_a_no_op(self):
        order = _order(meta=_dpn_meta())
        with _snapshot_db(_stored(), fields_available=False) as (writes, comments):
            self.assertFalse(geo_passthrough.apply_invoice_pin_snapshot(order, "SINV-1"))
        self.assertEqual(writes, [])
        self.assertEqual(comments, [])

    def test_database_failure_is_swallowed(self):
        order = _order(meta=_dpn_meta())
        with _snapshot_db(_stored(), set_value_error=Exception("deadlock")) as (_writes, comments):
            self.assertFalse(geo_passthrough.apply_invoice_pin_snapshot(order, "SINV-1"))
        self.assertEqual(comments, [])

    def test_no_invoice_is_a_no_op(self):
        with _snapshot_db(_stored()) as (writes, _comments):
            self.assertFalse(geo_passthrough.apply_invoice_pin_snapshot(_order(meta=_dpn_meta()), None))
        self.assertEqual(writes, [])


# ---------------------------------------------------------------------------
# D. Poll pre-check
# ---------------------------------------------------------------------------


class TestPollPreCheckSeesPinEdits(unittest.TestCase):
    MAP_ROW = {
        "name": "MAP-1",
        "erpnext_sales_invoice": "SINV-1",
        "hash": "H",
        "status": "processing",
        "woo_contact_hash": "C",
        "woo_territory_hash": "T",
    }

    @contextmanager
    def _env(self, stored_row, *, fields_available=True):
        def _get_value(doctype, name=None, fieldname=None, *args, **kwargs):
            if doctype == "WooCommerce Order Map":
                return dict(self.MAP_ROW)
            if doctype == "Sales Invoice" and fieldname == "docstatus":
                return 1
            if doctype == "Sales Invoice" and isinstance(fieldname, (list, tuple)):
                return dict(stored_row) if stored_row is not None else None
            return None

        with patch.object(order_sync.frappe.db, "get_table_columns",
                          return_value=["erpnext_sales_invoice", "woo_contact_hash", "woo_territory_hash"]), \
             patch.object(order_sync.frappe.db, "get_value", side_effect=_get_value), \
             patch.object(order_sync, "_compute_order_hash", return_value="H"), \
             patch.object(order_sync, "_extract_order_contact_snapshot",
                          return_value={"woo_contact_hash": "C"}), \
             patch.object(order_sync, "_extract_order_territory_snapshot",
                          return_value={"woo_territory_hash": "T"}), \
             patch.object(order_sync, "_map_status", return_value={"docstatus": 1}), \
             patch.object(geo_passthrough, "invoice_pin_fields_available",
                          return_value=fields_available):
            yield

    def test_identical_pin_is_unchanged(self):
        order = _order(meta=_dpn_meta(pin_source="prefill"))
        with self._env(_stored(30.0444, 31.2357, "prefill", "")):
            self.assertTrue(order_sync._polled_order_is_unchanged(order))

    def test_pin_moved_on_woo_is_changed(self):
        order = _order(meta=_dpn_meta(pin_source="prefill"))
        with self._env(_stored(29.9, 31.1, "prefill", "")):
            self.assertFalse(order_sync._polled_order_is_unchanged(order))

    def test_new_flag_on_woo_is_changed(self):
        order = _order(meta=_dpn_meta(pin_source="prefill", area_mismatch="yes"))
        with self._env(_stored(30.0444, 31.2357, "prefill", "")):
            self.assertFalse(order_sync._polled_order_is_unchanged(order))

    def test_old_invoice_without_pin_or_snapshot_is_not_requeued(self):
        with self._env(_stored(0, 0, None, None)):
            self.assertTrue(order_sync._polled_order_is_unchanged(_order()))

    def test_old_invoice_with_a_woo_pin_and_empty_snapshot_is_backfilled_once(self):
        order = _order(meta=_dpn_meta())
        with self._env(_stored(0, 0, None, None)):
            self.assertFalse(order_sync._polled_order_is_unchanged(order))
        # After phase1 wrote the snapshot, the next poll is quiet again.
        with self._env(_stored(30.0444, 31.2357, "droppin", "")):
            self.assertTrue(order_sync._polled_order_is_unchanged(order))

    def test_missing_snapshot_fields_keep_the_old_behaviour(self):
        order = _order(meta=_dpn_meta())
        with self._env(_stored(), fields_available=False):
            self.assertTrue(order_sync._polled_order_is_unchanged(order))


# ---------------------------------------------------------------------------
# D. Unchanged branch refresh
# ---------------------------------------------------------------------------


class TestUnchangedBranchRefreshesThePin(unittest.TestCase):
    def test_process_order_unchanged_branch_calls_the_refresh(self):
        map_row = dict(TestPollPreCheckSeesPinEdits.MAP_ROW)
        redis = MagicMock()
        redis.lock.return_value.acquire.return_value = True
        refresh = MagicMock(return_value={"snapshot_written": True, "address_refreshed": True})
        order = _order(meta=_dpn_meta())

        with patch.object(order_sync, "get_redis_conn", return_value=redis), \
             patch.object(order_sync.frappe.db, "sql", return_value=[[1]]), \
             patch.object(order_sync.frappe.db, "get_table_columns",
                          return_value=["erpnext_sales_invoice"]), \
             patch.object(order_sync.frappe.db, "get_value", return_value=map_row), \
             patch.object(order_sync, "_compute_order_hash", return_value="H"), \
             patch.object(order_sync, "_extract_order_contact_snapshot",
                          return_value={"woo_contact_hash": "C"}), \
             patch.object(order_sync, "_extract_order_territory_snapshot",
                          return_value={"woo_territory_hash": "T"}), \
             patch.object(order_sync, "_order_unchanged_since_last_sync", return_value=True), \
             patch.object(order_sync, "_refresh_linked_invoice_pins", refresh):
            result = order_sync._process_order_phase1(order, MagicMock())

        self.assertEqual(result["reason"], "unchanged")
        refresh.assert_called_once()
        args, kwargs = refresh.call_args
        self.assertIs(args[0], order)
        self.assertEqual(args[1], "SINV-1")

    def test_refresh_failure_never_breaks_the_skip(self):
        with patch.object(order_sync._geo_passthrough, "refresh_invoice_pins",
                          side_effect=Exception("boom")):
            outcome = order_sync._refresh_linked_invoice_pins(_order(), "SINV-1", woo_id=1)
        self.assertEqual(outcome, {"snapshot_written": False, "address_refreshed": False})

    def test_edited_pin_moves_snapshot_and_address(self):
        order = _order(meta=_dpn_meta(pin_source="customer_checkout"))
        apply_pins = MagicMock(return_value={"billing_applied": True, "shipping_applied": False})
        address_row = {"customer_address": "ADDR-B", "shipping_address_name": "ADDR-S"}
        with _snapshot_db(_stored(29.9, 31.1, "customer_checkout", ""), address_row=address_row) \
                as (writes, _comments), \
             patch.object(geo_passthrough, "apply_order_pins", apply_pins):
            result = geo_passthrough.refresh_invoice_pins(order, "SINV-1")

        self.assertEqual(result, {"snapshot_written": True, "address_refreshed": True})
        self.assertEqual(len(writes), 1)
        apply_pins.assert_called_once_with(
            order, billing_address="ADDR-B", shipping_address="ADDR-S"
        )

    def test_backfill_of_an_empty_snapshot_leaves_the_address_alone(self):
        """The Address already got this pin at first sync; re-applying an old
        order's pin could roll it back over a newer order's pin."""
        order = _order(meta=_dpn_meta())
        apply_pins = MagicMock()
        with _snapshot_db(_stored()) as (writes, _comments), \
             patch.object(geo_passthrough, "apply_order_pins", apply_pins):
            result = geo_passthrough.refresh_invoice_pins(order, "SINV-1")
        self.assertTrue(result["snapshot_written"])
        self.assertEqual(len(writes), 1)
        apply_pins.assert_not_called()

    def test_identical_pin_writes_nothing(self):
        order = _order(meta=_dpn_meta())
        apply_pins = MagicMock()
        with _snapshot_db(_stored(30.0444, 31.2357, "droppin", "")) as (writes, _comments), \
             patch.object(geo_passthrough, "apply_order_pins", apply_pins):
            result = geo_passthrough.refresh_invoice_pins(order, "SINV-1")
        self.assertEqual(result, {"snapshot_written": False, "address_refreshed": False})
        self.assertEqual(writes, [])
        apply_pins.assert_not_called()


# ---------------------------------------------------------------------------
# Manual repair also fixes the snapshot
# ---------------------------------------------------------------------------


class TestResyncOrderPinFixesSnapshot(unittest.TestCase):
    def test_resync_calls_the_snapshot(self):
        client = MagicMock()
        client.get_order.return_value = _order(meta=_dpn_meta())
        with patch.object(geo_passthrough, "resolve_invoice_addresses",
                          return_value=("SINV-1", "ADDR-B", "ADDR-S")), \
             patch.object(geo_passthrough, "apply_order_pins",
                          return_value={"billing_applied": False, "shipping_applied": False}), \
             patch.object(geo_passthrough, "apply_invoice_pin_snapshot", return_value=True) as snap:
            result = geo_passthrough.resync_order_pin(17001, client=client)
        snap.assert_called_once()
        self.assertEqual(snap.call_args[0][1], "SINV-1")
        self.assertTrue(result["invoice_snapshot_written"])
        self.assertTrue(result["success"])


if __name__ == "__main__":
    unittest.main()
