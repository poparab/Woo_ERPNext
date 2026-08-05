"""Cover for the stale delivery zone left behind after an ERPNext territory edit.

Editing an address territory in ERPNext writes the Territory into
``Address.city`` only. ``Address.state`` — the field WooCommerce reads the
delivery zone from — kept the zone the store first sent, so the store ended up
showing the new territory in City and the old one in State/County (order 5233,
City "EGDOKKI" next to State "Downtown - وسط البلد").
"""

import unittest
import unittest.mock

from jarz_woocommerce_integration.services import customer_sync, outbound_sync


_TERRITORY_ROWS = {
    "EGDOKKI": {"territory_name": "EGDOKKI", "custom_woo_code": "EGDOKKI", "is_group": 0},
    "EGDOWNTOWN": {"territory_name": "EGDOWNTOWN", "custom_woo_code": "EGDOWNTOWN", "is_group": 0},
    # A zone the static CODE_TO_DISPLAY map has never heard of.
    "EGNEWZONE": {
        "territory_name": "New Zone",
        "custom_woo_code": "EGNEWZONE",
        "custom_territory_name_ar": "منطقه جديده",
        "is_group": 0,
    },
    # Governorate group: a free-text Woo city must never resolve onto it.
    "Cairo": {"territory_name": "Cairo", "custom_woo_code": None, "is_group": 1},
}


class _FakeDB:
    def __init__(self):
        self.writes = []

    def has_column(self, doctype, fieldname):
        return doctype == "Territory" and fieldname in {
            "custom_woo_code",
            "custom_territory_name_ar",
        }

    def get_value(self, doctype, name, fields, as_dict=False):
        row = _TERRITORY_ROWS.get(name) or {}
        if as_dict:
            return {field: row.get(field) for field in fields}
        return row.get(fields)

    def set_value(self, doctype, name, fieldname, value, update_modified=True):
        self.writes.append((doctype, name, fieldname, value))


class AddressStateRealignmentTests(unittest.TestCase):
    def _realign(self, city, state, *, resolves):
        db = _FakeDB()

        def fake_resolve(value, territory_state_cache=None):
            return resolves.get(str(value or "").strip())

        with unittest.mock.patch.object(outbound_sync.frappe, "db", db), \
             unittest.mock.patch.object(customer_sync, "_resolve_territory_from_state", fake_resolve):
            result = outbound_sync._realign_address_state_with_territory("ADDR-1", city, state)
        return result, db.writes

    def test_stale_zone_is_replaced_by_the_territory_now_on_the_address(self):
        state, writes = self._realign(
            "EGDOKKI",
            "Downtown - وسط البلد",
            resolves={"EGDOKKI": "EGDOKKI", "Downtown - وسط البلد": "EGDOWNTOWN"},
        )
        self.assertEqual(state, "Dokki - الدقي")
        self.assertEqual(writes, [("Address", "ADDR-1", "state", "Dokki - الدقي")])

    def test_realigned_state_is_written_back_so_inbound_matching_still_finds_the_address(self):
        _state, writes = self._realign(
            "EGDOKKI",
            "Downtown - وسط البلد",
            resolves={"EGDOKKI": "EGDOKKI", "Downtown - وسط البلد": "EGDOWNTOWN"},
        )
        # Inbound order matching hashes city + state; a stored value that
        # disagrees with the store's forks a duplicate Address on the next pull.
        self.assertTrue(writes)

    def test_an_agreeing_state_is_left_untouched(self):
        state, writes = self._realign(
            "EGDOKKI",
            "Dokki - الدقي",
            resolves={"EGDOKKI": "EGDOKKI", "Dokki - الدقي": "EGDOKKI"},
        )
        self.assertEqual(state, "Dokki - الدقي")
        self.assertEqual(writes, [])

    def test_free_text_city_leaves_the_zone_alone(self):
        state, writes = self._realign(
            "dgftcvy",
            "Downtown - وسط البلد",
            resolves={"Downtown - وسط البلد": "EGDOWNTOWN"},
        )
        self.assertEqual(state, "Downtown - وسط البلد")
        self.assertEqual(writes, [])

    def test_a_city_matching_a_group_territory_never_rewrites_the_zone(self):
        state, writes = self._realign(
            "Cairo",
            "Downtown - وسط البلد",
            resolves={"Cairo": "Cairo", "Downtown - وسط البلد": "EGDOWNTOWN"},
        )
        self.assertEqual(state, "Downtown - وسط البلد")
        self.assertEqual(writes, [])

    def test_a_zone_missing_from_the_static_map_falls_back_to_the_territory_labels(self):
        state, _writes = self._realign(
            "EGNEWZONE",
            "Downtown - وسط البلد",
            resolves={"EGNEWZONE": "EGNEWZONE", "Downtown - وسط البلد": "EGDOWNTOWN"},
        )
        self.assertEqual(state, "New Zone - منطقه جديده")


class OrderAddressChangeDetectionTests(unittest.TestCase):
    """An address-only edit used to be dropped as "unchanged" before it shipped."""

    def _order(self, **shipping):
        address = {
            "address_1": "12 Tahrir St",
            "address_2": "",
            "city": "EGDOKKI",
            "state": "Dokki - الدقي",
            "postcode": "",
        }
        address.update(shipping)
        return {"status": "processing", "shipping": address, "billing": dict(address)}

    def test_a_changed_delivery_zone_marks_the_order_dirty(self):
        existing = self._order(state="Downtown - وسط البلد")
        payload = {"status": "processing", "shipping": self._order()["shipping"]}
        self.assertTrue(outbound_sync._order_payload_requires_update(existing, payload))

    def test_a_changed_street_marks_the_order_dirty(self):
        existing = self._order(address_1="9 Old St")
        payload = {"status": "processing", "billing": self._order()["billing"]}
        self.assertTrue(outbound_sync._order_payload_requires_update(existing, payload))

    def test_an_identical_address_does_not_churn_the_order(self):
        existing = self._order()
        payload = {
            "status": "processing",
            "shipping": self._order()["shipping"],
            "billing": self._order()["billing"],
        }
        self.assertFalse(outbound_sync._order_payload_requires_update(existing, payload))

    def test_case_and_whitespace_noise_does_not_churn_the_order(self):
        existing = self._order()
        noisy = dict(self._order()["shipping"], city=" egdokki ")
        payload = {"status": "processing", "shipping": noisy}
        self.assertFalse(outbound_sync._order_payload_requires_update(existing, payload))

    def test_country_is_not_compared(self):
        # Woo answers "EG", ERPNext stores "Egypt" — comparing it would mark
        # every order dirty forever.
        existing = dict(self._order())
        existing["shipping"] = dict(existing["shipping"], country="EG")
        payload = {"status": "processing", "shipping": dict(self._order()["shipping"], country="Egypt")}
        self.assertFalse(outbound_sync._order_payload_requires_update(existing, payload))

    def test_an_empty_payload_address_never_wipes_the_store_copy(self):
        existing = self._order()
        payload = {"status": "processing", "shipping": {}, "billing": {}}
        self.assertFalse(outbound_sync._order_payload_requires_update(existing, payload))


if __name__ == "__main__":
    unittest.main()
