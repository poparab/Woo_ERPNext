"""Courier lane O1 — WooCommerce customer geo pin passthrough.

Covers the six things that can go wrong:

1. A brand new Address does not get the pin.
2. A *returning* customer's existing Address does not get the pin -- the trap,
   because ``existing or _create_address(...)`` short-circuits and a create-only
   implementation is invisible in testing with fresh data.
3. A lower-confidence write clobbers a better pin (or, just as bad, raises and
   takes the whole order sync down with it).
4. A geo-only write wakes the Woo Address outbound hooks and pushes a customer +
   invoice sync per address.
5. This module's copy of the confidence ladder drifts from COURIER_CONTRACTS
   section 4, or it claims a rank above ``customer_pin``. There are two
   authorised writers of the Address geo fields and they duplicate the ladder
   rather than import across the domain boundary, so the drift guard below is
   what stands in for a single-writer rule.
6. A moved pin keeps the accuracy radius of the point it moved away from.
"""

from __future__ import annotations

from contextlib import contextmanager
import inspect
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from jarz_woocommerce_integration.services import customer_sync, geo_passthrough, outbound_sync


CUSTOMER_PIN_RANK = geo_passthrough.CONFIDENCE_RANK["customer_pin"]
COURIER_RANK = geo_passthrough.CONFIDENCE_RANK["courier_verified"]

CAIRO = (30.0444, 31.2357)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _order(*, meta=None, billing=None, shipping=None, order_id=16834):
    return {
        "id": order_id,
        "billing": billing if billing is not None else {
            "address_1": "12 Nile Street",
            "address_2": "",
            "city": "Cairo",
            "state": "Dokki - الدقي",
            "postcode": "12311",
            "country": "EG",
            "email": "ahmed@example.com",
            "first_name": "Ahmed",
            "last_name": "Mohamed",
            "phone": "01012345678",
        },
        "shipping": shipping if shipping is not None else {},
        "meta_data": meta if meta is not None else [],
    }


def _pin_meta(lat="30.0444", lng="31.2357"):
    return [
        {"key": "_wc_order_attribution_source_type", "value": "typein"},
        {"key": "_jarz_lat", "value": lat},
        {"key": "_jarz_lng", "value": lng},
    ]


@contextmanager
def _patched_db(stored_row, *, fields_available=True):
    """Stub the two DB calls ``apply_geo_pin`` makes; capture every set_value."""
    writes: list[dict] = []

    def _set_value(doctype, name, fieldname, value=None, **kwargs):
        writes.append({
            "doctype": doctype,
            "name": name,
            "fieldname": fieldname,
            "value": value,
            "kwargs": kwargs,
        })

    with patch.object(geo_passthrough, "geo_fields_available", return_value=fields_available), \
         patch.object(geo_passthrough.frappe.db, "get_value", return_value=stored_row), \
         patch.object(geo_passthrough.frappe.db, "set_value", side_effect=_set_value), \
         patch.object(geo_passthrough.frappe, "log_error"):
        yield writes


class _DummyAddress:
    """Minimal stand-in for an Address Document (mirrors the outbound tests)."""

    def __init__(self, **overrides):
        self.name = "ADDR-GEO-001"
        self.address_type = "Shipping"
        self.is_shipping_address = 1
        self.address_line1 = "12 Nile Street"
        self.address_line2 = "Location: https://maps.google.com/?q=30.0444,31.2357"
        self.city = "Cairo"
        self.state = "Dokki"
        self.pincode = "12311"
        self.country = "Egypt"
        self.phone = "01012345678"
        self.email_id = "ahmed@example.com"
        self.custom_latitude = None
        self.custom_longitude = None
        self.custom_geo_source = None
        self.custom_geo_confidence = 0
        self.links = [SimpleNamespace(link_doctype="Customer", link_name="CUST-GEO-001")]
        self.flags = SimpleNamespace(ignore_woo_outbound=False)
        self._before_save = None
        for key, value in overrides.items():
            setattr(self, key, value)

    def get(self, fieldname, default=None):
        return getattr(self, fieldname, default)

    def get_doc_before_save(self):
        return self._before_save

    def has_value_changed(self, fieldname):
        previous = self._before_save
        if previous is None:
            return False
        return getattr(previous, fieldname, None) != getattr(self, fieldname, None)


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


# ---------------------------------------------------------------------------
# Confidence ladder — frozen copy of COURIER_CONTRACTS.md section 4
# ---------------------------------------------------------------------------


class TestConfidenceLadderFrozen(unittest.TestCase):
    """The ladder is duplicated, not imported — so it must not drift.

    COURIER_CONTRACTS section 3 authorises two writers of the Address geo
    fields: ``jarz_pos/services/geo_resolution.py`` and this app's
    ``geo_passthrough``. They may not import each other (domain isolation), so
    each carries its own copy of the section 4 table. If the two copies ever
    disagree, "never downgrade" means different things in each and a pin can be
    silently overwritten by a worse one. This test is the guard that replaces
    the single-writer rule.
    """

    # Transcribed from COURIER_CONTRACTS.md section 4. Do not derive this from
    # the module under test — that would guard nothing.
    CONTRACT_LADDER = {
        "territory_centroid": 10,
        "pos_link": 20,
        "customer_pin": 30,
        "courier_verified": 40,
        "manual_override": 50,
    }

    def test_ladder_matches_the_contract_exactly(self):
        self.assertEqual(geo_passthrough.CONFIDENCE_RANK, self.CONTRACT_LADDER)

    def test_ladder_has_no_extra_or_missing_sources(self):
        self.assertEqual(
            set(geo_passthrough.CONFIDENCE_RANK), set(self.CONTRACT_LADDER)
        )

    def test_ranks_are_ordered_the_way_the_contract_says(self):
        ordered = [
            "territory_centroid",
            "pos_link",
            "customer_pin",
            "courier_verified",
            "manual_override",
        ]
        ranks = [geo_passthrough.CONFIDENCE_RANK[name] for name in ordered]
        self.assertEqual(ranks, sorted(ranks))
        self.assertEqual(len(set(ranks)), len(ranks), "ranks must be distinct")

    def test_this_module_is_capped_at_customer_pin(self):
        self.assertEqual(geo_passthrough.MAX_WRITABLE_RANK, 30)
        self.assertEqual(
            geo_passthrough.MAX_WRITABLE_RANK,
            geo_passthrough.CONFIDENCE_RANK["customer_pin"],
        )

    def test_default_source_is_customer_pin(self):
        default = inspect.signature(geo_passthrough.apply_geo_pin).parameters["source"].default
        self.assertEqual(default, "customer_pin")

    def test_a_rank_above_the_cap_is_refused_at_the_write(self):
        """A Woo payload can never attest to a courier or a manager."""
        row = {
            "custom_geo_source": None,
            "custom_geo_confidence": 0,
            "custom_latitude": None,
            "custom_longitude": None,
        }
        for source in ("courier_verified", "manual_override"):
            with self.subTest(source=source):
                with _patched_db(row) as writes:
                    applied = geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO, source=source)
                self.assertFalse(applied)
                self.assertEqual(writes, [])

    def test_every_rank_this_module_can_write_is_at_or_below_the_cap(self):
        row = {"custom_geo_source": None, "custom_geo_confidence": 0}
        for source in geo_passthrough.CONFIDENCE_RANK:
            with self.subTest(source=source):
                with _patched_db(row) as writes:
                    geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO, source=source)
                for write in writes:
                    self.assertLessEqual(
                        write["fieldname"]["custom_geo_confidence"],
                        geo_passthrough.MAX_WRITABLE_RANK,
                    )

    def test_the_woo_sync_only_ever_asks_for_customer_pin(self):
        self.assertEqual(geo_passthrough.GEO_SOURCE_CUSTOMER_PIN, "customer_pin")
        self.assertEqual(
            customer_sync.GEO_SOURCE_CUSTOMER_PIN, geo_passthrough.GEO_SOURCE_CUSTOMER_PIN
        )


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


class TestPinExtraction(unittest.TestCase):
    def test_order_meta_pin_is_extracted(self):
        pin = geo_passthrough.extract_order_pin(_order(meta=_pin_meta()))
        self.assertEqual((pin.latitude, pin.longitude), CAIRO)

    def test_order_meta_falls_back_to_display_key_and_value(self):
        pin = geo_passthrough.extract_order_pin(_order(meta=[
            {"display_key": "_jarz_lat", "display_value": "30.0444"},
            {"display_key": "_jarz_lng", "display_value": "31.2357"},
        ]))
        self.assertEqual((pin.latitude, pin.longitude), CAIRO)

    def test_order_without_pin_meta_returns_none(self):
        self.assertIsNone(geo_passthrough.extract_order_pin(_order()))

    def test_numeric_meta_values_are_accepted(self):
        pin = geo_passthrough.extract_order_pin(_order(meta=[
            {"key": "_jarz_lat", "value": 30.0444},
            {"key": "_jarz_lng", "value": 31.2357},
        ]))
        self.assertEqual((pin.latitude, pin.longitude), CAIRO)

    def test_blank_and_garbage_meta_are_rejected(self):
        for lat, lng in (("", ""), ("abc", "31.2357"), (None, None), ("0", "0")):
            with self.subTest(lat=lat, lng=lng):
                self.assertIsNone(
                    geo_passthrough.extract_order_pin(_order(meta=_pin_meta(lat, lng)))
                )

    def test_out_of_range_coordinates_are_rejected(self):
        self.assertIsNone(geo_passthrough.make_pin(120.0, 31.2357))
        self.assertIsNone(geo_passthrough.make_pin(30.0444, 200.0))

    def test_null_island_is_rejected(self):
        self.assertIsNone(geo_passthrough.make_pin(0, 0))
        self.assertIsNone(geo_passthrough.make_pin("0.0", "0.0"))

    def test_maps_link_shapes_are_parsed(self):
        cases = (
            "https://maps.google.com/?q=30.0444,31.2357",
            "https://www.google.com/maps?q=loc:30.0444,31.2357&z=17",
            "https://www.google.com/maps/@30.0444,31.2357,17z",
            "https://www.google.com/maps/place/Jarz/data=!3m1!4b1!3d30.0444!4d31.2357",
            "https://www.google.com/maps/dir/?api=1&destination=30.0444%2C31.2357",
            "30.0444, 31.2357",
        )
        for link in cases:
            with self.subTest(link=link):
                pin = geo_passthrough.parse_maps_link(link)
                self.assertIsNotNone(pin, link)
                self.assertEqual((pin.latitude, pin.longitude), CAIRO)

    def test_short_maps_link_carries_no_coordinates(self):
        self.assertIsNone(geo_passthrough.parse_maps_link("https://maps.app.goo.gl/abc123"))

    def test_postcode_and_phone_never_look_like_a_pin(self):
        self.assertIsNone(geo_passthrough.parse_maps_link("12311"))
        self.assertIsNone(geo_passthrough.parse_maps_link("01012345678"))
        self.assertIsNone(geo_passthrough.parse_maps_link("Apartment 12, floor 3"))

    def test_maps_link_in_woo_address_line_is_extracted(self):
        pin = geo_passthrough.extract_address_pin({
            "address_1": "12 Nile Street",
            "address_2": "Location: https://maps.google.com/?q=30.0444,31.2357",
        })
        self.assertEqual((pin.latitude, pin.longitude), CAIRO)

    def test_stored_address_line_keys_are_not_a_customer_pin_source(self):
        """A POS-written address_line2 link is pos_link provenance, not ours."""
        self.assertIsNone(geo_passthrough.extract_address_pin({
            "address_line1": "12 Nile Street",
            "address_line2": "Location: https://maps.google.com/?q=30.0444,31.2357",
        }))


class TestPinRouting(unittest.TestCase):
    def test_order_pin_goes_to_billing_when_there_is_no_shipping_address(self):
        billing_pin, shipping_pin = geo_passthrough.resolve_order_pins(
            _order(meta=_pin_meta())
        )
        self.assertEqual((billing_pin.latitude, billing_pin.longitude), CAIRO)
        self.assertIsNone(shipping_pin)

    def test_order_pin_goes_to_shipping_when_one_exists(self):
        billing_pin, shipping_pin = geo_passthrough.resolve_order_pins(
            _order(meta=_pin_meta(), shipping={"address_1": "5 Tahrir Square", "city": "Cairo"})
        )
        self.assertIsNone(billing_pin)
        self.assertEqual((shipping_pin.latitude, shipping_pin.longitude), CAIRO)

    def test_explicit_meta_outranks_a_parsed_link(self):
        _billing_pin, shipping_pin = geo_passthrough.resolve_order_pins(
            _order(
                meta=_pin_meta(),
                shipping={
                    "address_1": "5 Tahrir Square",
                    "address_2": "https://maps.google.com/?q=29.9000,31.1000",
                },
            )
        )
        self.assertEqual((shipping_pin.latitude, shipping_pin.longitude), CAIRO)

    def test_address_link_is_used_when_there_is_no_meta_pin(self):
        billing_pin, _shipping_pin = geo_passthrough.resolve_order_pins(
            _order(billing={
                "address_1": "12 Nile Street",
                "address_2": "https://maps.google.com/?q=30.0444,31.2357",
            })
        )
        self.assertEqual((billing_pin.latitude, billing_pin.longitude), CAIRO)

    def test_non_dict_payload_is_survivable(self):
        self.assertEqual(geo_passthrough.resolve_order_pins(None), (None, None))
        self.assertIsNone(geo_passthrough.extract_order_pin("not-an-order"))


# ---------------------------------------------------------------------------
# apply_geo_pin — the confidence ladder
# ---------------------------------------------------------------------------


class TestApplyGeoPin(unittest.TestCase):
    def test_pin_is_written_to_an_address_with_no_geo_data(self):
        row = {
            "custom_geo_source": None,
            "custom_geo_confidence": 0,
            "custom_latitude": None,
            "custom_longitude": None,
        }
        with _patched_db(row) as writes:
            applied = geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO)

        self.assertTrue(applied)
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0]["fieldname"], {
            "custom_latitude": 30.0444,
            "custom_longitude": 31.2357,
            "custom_geo_source": "customer_pin",
            "custom_geo_confidence": CUSTOMER_PIN_RANK,
            # The pin moved (from nothing), so accuracy is written -- explicitly
            # NULL, because a Woo payload carries none. Contract section 3.
            "custom_geo_accuracy_m": None,
        })

    def test_write_never_touches_the_document_layer(self):
        row = {"custom_geo_source": None, "custom_geo_confidence": 0}
        with _patched_db(row) as writes:
            geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO)
        # update_modified=False keeps the Address timestamp stable and db.set_value
        # bypasses the doc layer, so on_update (and the Woo hooks) never fire.
        self.assertIs(writes[0]["kwargs"].get("update_modified"), False)

    def test_source_and_confidence_are_always_written_together(self):
        row = {"custom_geo_source": None, "custom_geo_confidence": 0}
        with _patched_db(row) as writes:
            geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO)
        payload = writes[0]["fieldname"]
        self.assertIn("custom_geo_source", payload)
        self.assertIn("custom_geo_confidence", payload)
        self.assertEqual(
            geo_passthrough.CONFIDENCE_RANK[payload["custom_geo_source"]],
            payload["custom_geo_confidence"],
        )

    def test_equal_rank_refresh_is_accepted(self):
        row = {
            "custom_geo_source": "customer_pin",
            "custom_geo_confidence": CUSTOMER_PIN_RANK,
            "custom_latitude": 29.9,
            "custom_longitude": 31.1,
        }
        with _patched_db(row) as writes:
            applied = geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO)
        self.assertTrue(applied)
        self.assertEqual(len(writes), 1)

    def test_lower_confidence_write_is_ignored_and_never_raises(self):
        row = {
            "custom_geo_source": "courier_verified",
            "custom_geo_confidence": COURIER_RANK,
            "custom_latitude": 29.9,
            "custom_longitude": 31.1,
        }
        with _patched_db(row) as writes:
            applied = geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO)

        self.assertFalse(applied)
        self.assertEqual(writes, [], "A better pin was clobbered by a routine re-sync.")

    def test_lower_confidence_is_detected_from_the_rank_not_the_string(self):
        # Alphabetically "courier_verified" < "customer_pin"; by rank it is higher.
        self.assertLess(
            geo_passthrough.CONFIDENCE_RANK["customer_pin"],
            geo_passthrough.CONFIDENCE_RANK["courier_verified"],
        )
        self.assertGreater("customer_pin", "courier_verified")

    def test_stored_confidence_alone_still_blocks_a_downgrade(self):
        row = {
            "custom_geo_source": "",
            "custom_geo_confidence": 50,
            "custom_latitude": 29.9,
            "custom_longitude": 31.1,
        }
        with _patched_db(row) as writes:
            applied = geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO)
        self.assertFalse(applied)
        self.assertEqual(writes, [])

    def test_identical_pin_is_not_rewritten(self):
        row = {
            "custom_geo_source": "customer_pin",
            "custom_geo_confidence": CUSTOMER_PIN_RANK,
            "custom_latitude": 30.0444,
            "custom_longitude": 31.2357,
        }
        with _patched_db(row) as writes:
            applied = geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO)
        self.assertTrue(applied)
        self.assertEqual(writes, [])

    def test_unusable_coordinates_do_not_reach_the_database(self):
        with _patched_db({}) as writes:
            self.assertFalse(geo_passthrough.apply_geo_pin("ADDR-1", None, None))
            self.assertFalse(geo_passthrough.apply_geo_pin("ADDR-1", "", ""))
            self.assertFalse(geo_passthrough.apply_geo_pin("ADDR-1", 0, 0))
            self.assertFalse(geo_passthrough.apply_geo_pin(None, *CAIRO))
        self.assertEqual(writes, [])

    def test_unknown_source_is_refused(self):
        with _patched_db({}) as writes:
            self.assertFalse(
                geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO, source="vibes")
            )
        self.assertEqual(writes, [])

    def test_missing_geo_custom_fields_is_a_loud_no_op(self):
        geo_passthrough._missing_fields_reported = False
        with _patched_db({}, fields_available=False) as writes:
            applied = geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO)
        self.assertFalse(applied)
        self.assertEqual(writes, [])

    def test_only_allow_listed_fields_are_ever_written(self):
        row = {"custom_geo_source": None, "custom_geo_confidence": 0}
        with _patched_db(row) as writes:
            geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO, accuracy_m=12.5)
        written = set(writes[0]["fieldname"])
        self.assertTrue(written <= set(geo_passthrough._ALLOWED_UPDATE_FIELDS))
        self.assertEqual(writes[0]["fieldname"]["custom_geo_accuracy_m"], 12.5)

    def test_database_failure_is_swallowed(self):
        row = {"custom_geo_source": None, "custom_geo_confidence": 0}
        with patch.object(geo_passthrough, "geo_fields_available", return_value=True), \
             patch.object(geo_passthrough.frappe.db, "get_value", return_value=row), \
             patch.object(geo_passthrough.frappe.db, "set_value",
                          side_effect=Exception("deadlock")):
            self.assertFalse(geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO))


class TestAccuracyTravelsWithTheCoordinates(unittest.TestCase):
    """Contract section 3: a write that moves the pin must write accuracy too.

    Woo pins carry no accuracy, so they NULL it. Leaving the previous value
    behind produces a radius describing a point that is no longer there, which
    silently corrupts the consensus-hardening job downstream.
    """

    def test_stale_accuracy_is_nulled_when_a_woo_pin_overwrites_another_source(self):
        # A pos_link pin (rank 20) measured to 4.5 m, now superseded by a
        # customer_pin (rank 30) at a genuinely different point.
        row = {
            "custom_geo_source": "pos_link",
            "custom_geo_confidence": geo_passthrough.CONFIDENCE_RANK["pos_link"],
            "custom_latitude": 29.9000,
            "custom_longitude": 31.1000,
            "custom_geo_accuracy_m": 4.5,
        }
        with _patched_db(row) as writes:
            applied = geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO)

        self.assertTrue(applied)
        payload = writes[0]["fieldname"]
        self.assertIn("custom_geo_accuracy_m", payload)
        self.assertIsNone(
            payload["custom_geo_accuracy_m"],
            "The 4.5 m radius describes the old point, not this one.",
        )
        self.assertNotEqual(payload["custom_geo_accuracy_m"], 4.5)

    def test_moving_the_pin_always_writes_the_accuracy_field(self):
        row = {
            "custom_geo_source": "customer_pin",
            "custom_geo_confidence": CUSTOMER_PIN_RANK,
            "custom_latitude": 29.9,
            "custom_longitude": 31.1,
            "custom_geo_accuracy_m": 12.0,
        }
        with _patched_db(row) as writes:
            geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO)
        self.assertIn("custom_geo_accuracy_m", writes[0]["fieldname"])
        self.assertIsNone(writes[0]["fieldname"]["custom_geo_accuracy_m"])

    def test_a_first_pin_writes_a_null_accuracy_rather_than_omitting_it(self):
        row = {
            "custom_geo_source": None,
            "custom_geo_confidence": 0,
            "custom_latitude": None,
            "custom_longitude": None,
        }
        with _patched_db(row) as writes:
            geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO)
        self.assertIsNone(writes[0]["fieldname"]["custom_geo_accuracy_m"])

    def test_a_supplied_accuracy_is_written_not_nulled(self):
        row = {
            "custom_geo_source": None,
            "custom_geo_confidence": 0,
            "custom_latitude": None,
            "custom_longitude": None,
        }
        with _patched_db(row) as writes:
            geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO, accuracy_m=8.25)
        self.assertEqual(writes[0]["fieldname"]["custom_geo_accuracy_m"], 8.25)

    def test_accuracy_survives_a_source_change_that_does_not_move_the_pin(self):
        """Accuracy is still true of coordinates that did not change."""
        row = {
            "custom_geo_source": "pos_link",
            "custom_geo_confidence": geo_passthrough.CONFIDENCE_RANK["pos_link"],
            "custom_latitude": 30.0444,
            "custom_longitude": 31.2357,
            "custom_geo_accuracy_m": 4.5,
        }
        with _patched_db(row) as writes:
            applied = geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO)

        self.assertTrue(applied)
        payload = writes[0]["fieldname"]
        self.assertEqual(payload["custom_geo_source"], "customer_pin")
        self.assertNotIn("custom_geo_accuracy_m", payload)


# ---------------------------------------------------------------------------
# customer_sync integration — BOTH branches
# ---------------------------------------------------------------------------


class TestEnsureCustomerAppliesPin(unittest.TestCase):
    """``existing or _create_address(...)`` short-circuits; both sides must pin."""

    @contextmanager
    def _sync_env(self, *, existing_address):
        apply_pin = MagicMock(return_value=True)
        with patch.object(customer_sync, "_ensure_customer", return_value="CUST-GEO-001"), \
             patch.object(customer_sync, "_find_existing_address_for_customer",
                          return_value=existing_address), \
             patch.object(customer_sync, "_create_address", return_value="ADDR-NEW"), \
             patch.object(customer_sync, "_set_address_as_default"), \
             patch.object(customer_sync, "_resolve_country", return_value="Egypt"), \
             patch.object(customer_sync, "_resolve_territory_from_state", return_value=None), \
             patch.object(customer_sync, "_apply_geo_pin", apply_pin):
            yield apply_pin

    def test_new_address_gets_the_pin(self):
        order = _order(meta=_pin_meta())
        with self._sync_env(existing_address=None) as apply_pin:
            customer, billing_addr, _shipping = customer_sync.ensure_customer_with_addresses(
                order, SimpleNamespace()
            )

        self.assertEqual(customer, "CUST-GEO-001")
        self.assertEqual(billing_addr, "ADDR-NEW")
        apply_pin.assert_called_once_with(
            "ADDR-NEW", 30.0444, 31.2357, geo_passthrough.GEO_SOURCE_CUSTOMER_PIN
        )

    def test_existing_address_gets_the_pin_updated(self):
        order = _order(meta=_pin_meta())
        with self._sync_env(existing_address="ADDR-EXISTING") as apply_pin:
            _customer, billing_addr, _shipping = customer_sync.ensure_customer_with_addresses(
                order, SimpleNamespace()
            )

        self.assertEqual(billing_addr, "ADDR-EXISTING")
        apply_pin.assert_called_once_with(
            "ADDR-EXISTING", 30.0444, 31.2357, geo_passthrough.GEO_SOURCE_CUSTOMER_PIN
        )

    def test_distinct_shipping_address_receives_the_order_pin(self):
        order = _order(
            meta=_pin_meta(),
            shipping={
                "address_1": "5 Tahrir Square",
                "address_2": "",
                "city": "Cairo",
                "state": "Downtown",
                "postcode": "11511",
                "country": "EG",
            },
        )
        with self._sync_env(existing_address=None) as apply_pin:
            _customer, billing_addr, shipping_addr = customer_sync.ensure_customer_with_addresses(
                order, SimpleNamespace()
            )

        self.assertEqual(billing_addr, "ADDR-NEW")
        self.assertEqual(shipping_addr, "ADDR-NEW")
        # Billing carries no pin (no link in its lines); shipping carries the meta pin.
        apply_pin.assert_called_once_with(
            "ADDR-NEW", 30.0444, 31.2357, geo_passthrough.GEO_SOURCE_CUSTOMER_PIN
        )

    def test_order_without_a_pin_writes_nothing(self):
        with self._sync_env(existing_address="ADDR-EXISTING") as apply_pin:
            customer_sync.ensure_customer_with_addresses(_order(), SimpleNamespace())
        apply_pin.assert_not_called()

    def test_pin_failure_never_breaks_the_order(self):
        """A geo write blowing up must not cost the store an invoice."""
        order = _order(meta=_pin_meta())
        with self._sync_env(existing_address="ADDR-EXISTING") as apply_pin:
            apply_pin.side_effect = Exception("column custom_latitude unknown")
            customer, billing_addr, _shipping = customer_sync.ensure_customer_with_addresses(
                order, SimpleNamespace()
            )
        self.assertEqual(customer, "CUST-GEO-001")
        self.assertEqual(billing_addr, "ADDR-EXISTING")

    def test_apply_customer_pin_ignores_a_missing_address_or_pin(self):
        with patch.object(customer_sync, "_apply_geo_pin") as apply_pin:
            self.assertFalse(customer_sync._apply_customer_pin(None, geo_passthrough.GeoPin(*CAIRO)))
            self.assertFalse(customer_sync._apply_customer_pin("ADDR-1", None))
        apply_pin.assert_not_called()


# ---------------------------------------------------------------------------
# Outbound safety — a geo-only save must not reach WooCommerce
# ---------------------------------------------------------------------------


class TestGeoWriteDoesNotFanOut(unittest.TestCase):
    def _geo_only_change(self):
        previous = _DummyAddress()
        current = _DummyAddress(
            custom_latitude=30.0444,
            custom_longitude=31.2357,
            custom_geo_source="customer_pin",
            custom_geo_confidence=CUSTOMER_PIN_RANK,
        )
        current._before_save = previous
        return current

    def test_geo_only_update_does_not_enqueue_a_customer_push(self):
        current = self._geo_only_change()
        enqueue_calls = []

        with patch.object(outbound_sync, "_get_settings",
                          return_value=(SimpleNamespace(), _outbound_cfg())), \
             patch.object(outbound_sync.frappe, "flags",
                          SimpleNamespace(ignore_woo_outbound=False)), \
             patch.object(outbound_sync.frappe, "enqueue",
                          side_effect=lambda *a, **kw: enqueue_calls.append((a, kw))):
            outbound_sync.enqueue_linked_customer_sync_for_address(current, method="on_update")

        self.assertEqual(enqueue_calls, [])

    def test_geo_only_update_does_not_enqueue_an_invoice_push(self):
        current = self._geo_only_change()
        enqueue_calls = []
        get_all = MagicMock(return_value=[{"name": "ACC-SINV-0001"}])

        with patch.object(outbound_sync, "_get_settings",
                          return_value=(SimpleNamespace(), _outbound_cfg())), \
             patch.object(outbound_sync.frappe, "flags",
                          SimpleNamespace(ignore_woo_outbound=False)), \
             patch.object(outbound_sync.frappe, "get_all", get_all), \
             patch.object(outbound_sync.frappe, "enqueue",
                          side_effect=lambda *a, **kw: enqueue_calls.append((a, kw))):
            outbound_sync.enqueue_linked_invoice_sync_for_address(current, method="on_update")

        self.assertEqual(enqueue_calls, [])
        get_all.assert_not_called()

    def test_a_text_edit_still_does_fan_out(self):
        """Control: the gate is field-based, not disabled outright."""
        previous = _DummyAddress()
        current = _DummyAddress(address_line1="14 Nile Street")
        current._before_save = previous
        enqueue_calls = []

        with patch.object(outbound_sync, "_get_settings",
                          return_value=(SimpleNamespace(), _outbound_cfg())), \
             patch.object(outbound_sync.frappe, "flags",
                          SimpleNamespace(ignore_woo_outbound=False)), \
             patch.object(outbound_sync.frappe, "enqueue",
                          side_effect=lambda *a, **kw: enqueue_calls.append((a, kw))):
            outbound_sync.enqueue_linked_customer_sync_for_address(current, method="on_update")

        self.assertEqual(len(enqueue_calls), 1)

    def test_geo_write_payload_shares_no_field_with_the_outbound_gate(self):
        row = {"custom_geo_source": None, "custom_geo_confidence": 0}
        with _patched_db(row) as writes:
            geo_passthrough.apply_geo_pin("ADDR-1", *CAIRO, accuracy_m=8.0)
        written = set(writes[0]["fieldname"])
        self.assertEqual(
            written & set(outbound_sync._CUSTOMER_ADDRESS_OUTBOUND_UPDATE_FIELDS), set()
        )


if __name__ == "__main__":
    unittest.main()
