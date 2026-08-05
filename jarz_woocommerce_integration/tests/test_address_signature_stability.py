"""Regression guard: a coordinate must never enter the address dedup signature.

``customer_sync._address_signature_parts`` hashes exactly six *text* fields.
``_find_existing_address_for_customer`` matches a customer's stored Addresses
against that tuple, so the signature is the identity of an address.

If latitude/longitude (or any other geo field) were ever folded into it, every
map-pin update would change the signature, the lookup would miss, and the sync
would create a **second** Address for the same customer -- silently, on every
order, forever. Courier lane O1 writes pins to ``custom_*`` fields precisely to
stay out of this tuple; this file is the guard that keeps it that way.

See also ``services/geo_passthrough.py`` (invariant 1).
"""

from __future__ import annotations

import inspect
import unittest
from unittest.mock import patch

from jarz_woocommerce_integration.services import customer_sync, geo_passthrough, outbound_sync


# The frozen field list, in order. Changing this constant is not a fix.
EXPECTED_SIGNATURE_FIELDS: tuple[str, ...] = (
    "address_line1",
    "address_line2",
    "city",
    "state",
    "postcode",
    "country",
)

EXPECTED_ARITY = 6

# Substrings that must not appear anywhere in the signature code path.
_GEO_TOKENS = (
    "latitude",
    "longitude",
    "custom_geo",
    "custom_lat",
    "custom_lon",
    "_jarz_lat",
    "_jarz_lng",
    "geo_pin",
    "gps_location",
)

# Every function that participates in building or comparing a signature.
_SIGNATURE_CODE_PATH = (
    "_normalize_address_text",
    "_coerce_source_address_lines",
    "_address_signature_parts",
    "_source_address_signature",
    "_stored_address_signature",
    "_has_usable_source_address",
    "_same_source_address",
    "_find_existing_address_for_customer",
)


class TestAddressSignatureArity(unittest.TestCase):
    """The tuple is six wide and always has been."""

    def test_parameter_list_is_unchanged(self):
        params = tuple(inspect.signature(customer_sync._address_signature_parts).parameters)
        self.assertEqual(
            params,
            EXPECTED_SIGNATURE_FIELDS,
            "The address dedup signature field list changed. Adding a field here "
            "re-keys every stored Address and forks duplicates on the next sync.",
        )

    def test_signature_parts_returns_exactly_six_values(self):
        signature = customer_sync._address_signature_parts(
            "12 Nile Street", "Location: https://maps.google.com/?q=30.0,31.0",
            "Cairo", "Dokki", "12311", "Egypt",
        )
        self.assertIsInstance(signature, tuple)
        self.assertEqual(len(signature), EXPECTED_ARITY)

    def test_stored_signature_returns_exactly_six_values(self):
        signature = customer_sync._stored_address_signature({
            "address_line1": "12 Nile Street",
            "address_line2": "",
            "city": "Cairo",
            "state": "Dokki",
            "pincode": "12311",
            "country": "Egypt",
            # A stored row also carries the geo fields; they must be ignored.
            "custom_latitude": 30.0444,
            "custom_longitude": 31.2357,
            "custom_geo_source": "customer_pin",
            "custom_geo_confidence": 30,
        })
        self.assertIsInstance(signature, tuple)
        self.assertEqual(len(signature), EXPECTED_ARITY)

    def test_source_signature_returns_exactly_six_values(self):
        with patch.object(customer_sync, "_resolve_country", return_value="Egypt"):
            signature = customer_sync._source_address_signature({
                "address_1": "12 Nile Street",
                "address_2": "",
                "city": "Cairo",
                "state": "Dokki",
                "postcode": "12311",
                "country": "EG",
            })
        self.assertIsInstance(signature, tuple)
        self.assertEqual(len(signature), EXPECTED_ARITY)


class TestSignatureIgnoresCoordinates(unittest.TestCase):
    """A pin change must be invisible to address identity."""

    def _payload(self, **extra):
        base = {
            "address_1": "12 Nile Street",
            "address_2": "",
            "city": "Cairo",
            "state": "Dokki",
            "postcode": "12311",
            "country": "EG",
        }
        base.update(extra)
        return base

    def test_adding_coordinates_does_not_change_the_source_signature(self):
        with patch.object(customer_sync, "_resolve_country", return_value="Egypt"):
            without = customer_sync._source_address_signature(self._payload())
            with_pin = customer_sync._source_address_signature(
                self._payload(latitude="30.0444", longitude="31.2357")
            )
        self.assertEqual(without, with_pin)

    def test_changing_coordinates_does_not_change_the_stored_signature(self):
        row = {
            "address_line1": "12 Nile Street",
            "address_line2": "",
            "city": "Cairo",
            "state": "Dokki",
            "pincode": "12311",
            "country": "Egypt",
        }
        first = customer_sync._stored_address_signature(
            {**row, "custom_latitude": 30.0444, "custom_longitude": 31.2357}
        )
        second = customer_sync._stored_address_signature(
            {**row, "custom_latitude": 29.9, "custom_longitude": 31.1}
        )
        self.assertEqual(first, second)

    def test_signature_code_path_never_mentions_a_geo_field(self):
        for function_name in _SIGNATURE_CODE_PATH:
            source = inspect.getsource(getattr(customer_sync, function_name)).lower()
            for token in _GEO_TOKENS:
                self.assertNotIn(
                    token,
                    source,
                    f"{function_name} references {token!r}. Geo data must stay out of "
                    "the address dedup signature.",
                )


class TestGeoFieldsAreOutsideEveryAddressTrigger(unittest.TestCase):
    """The fields lane O1 writes touch neither dedup nor outbound push."""

    def test_geo_write_fields_are_disjoint_from_the_signature(self):
        self.assertEqual(
            set(geo_passthrough.GEO_WRITE_FIELDS) & set(EXPECTED_SIGNATURE_FIELDS),
            set(),
        )
        # The stored-row spellings differ (pincode vs postcode) -- check both.
        self.assertEqual(
            set(geo_passthrough.GEO_WRITE_FIELDS) & {"pincode", "postcode"},
            set(),
        )

    def test_geo_write_fields_are_disjoint_from_the_outbound_trigger_set(self):
        self.assertEqual(
            set(geo_passthrough.GEO_WRITE_FIELDS)
            & set(outbound_sync._CUSTOMER_ADDRESS_OUTBOUND_UPDATE_FIELDS),
            set(),
            "A geo write would fan out a customer + invoice push to WooCommerce.",
        )

    def test_outbound_trigger_set_contains_no_custom_field(self):
        # The whole safety argument rests on this. If a custom_* field is ever
        # added to the gate, geo writes stop being free.
        offenders = [
            fieldname
            for fieldname in outbound_sync._CUSTOMER_ADDRESS_OUTBOUND_UPDATE_FIELDS
            if fieldname.startswith("custom_")
        ]
        self.assertEqual(offenders, [])

    def test_allowed_update_fields_cannot_reach_a_text_field(self):
        allowed = set(geo_passthrough._ALLOWED_UPDATE_FIELDS)
        self.assertTrue(all(name.startswith("custom_") for name in allowed))
        self.assertEqual(
            allowed & set(outbound_sync._CUSTOMER_ADDRESS_OUTBOUND_UPDATE_FIELDS), set()
        )
