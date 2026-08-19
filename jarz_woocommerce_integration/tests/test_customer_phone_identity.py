"""Phone identity canonicalisation for Customer resolution.

Production stores the same subscriber as ``01111034268``, ``+201111034268`` and
``201111034268``.  ``_ensure_customer`` matches ``mobile_no`` with an exact
comparison, so before canonicalisation those were three separate identities and
crossing between them minted a duplicate Customer every time.
"""

import unittest
from unittest.mock import patch

from jarz_woocommerce_integration.services import customer_sync


class TestNormalizePhone(unittest.TestCase):
    """_normalize_phone folds every Egyptian spelling onto the local form."""

    def test_egyptian_spellings_fold_to_local_form(self):
        cases = {
            "01111034268": "01111034268",
            "+201111034268": "01111034268",
            "201111034268": "01111034268",
            "00201111034268": "01111034268",
            "0111 103 4268": "01111034268",
            "+20 111 103 4268": "01111034268",
            "0111-103-4268": "01111034268",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(customer_sync._normalize_phone(raw), expected)

    def test_blank_input_returns_none(self):
        for raw in (None, "", "   ", "abc"):
            with self.subTest(raw=raw):
                self.assertIsNone(customer_sync._normalize_phone(raw))

    def test_non_egyptian_numbers_are_not_rewritten(self):
        """Only the Egyptian country code is folded; nothing else is guessed at."""
        self.assertEqual(customer_sync._normalize_phone("+15551234567"), "+15551234567")
        self.assertEqual(customer_sync._normalize_phone("+441632960961"), "+441632960961")

    def test_is_idempotent(self):
        once = customer_sync._normalize_phone("+201111034268")
        self.assertEqual(customer_sync._normalize_phone(once), once)

    def test_short_number_starting_with_20_is_left_alone(self):
        """A 20-prefixed value that is not a full E.164 Egyptian number is not touched."""
        self.assertEqual(customer_sync._normalize_phone("2012345"), "2012345")


class TestPhoneVariants(unittest.TestCase):
    """_phone_variants enumerates the spellings actually present in the database."""

    def test_returns_canonical_first(self):
        variants = customer_sync._phone_variants("+201111034268")
        self.assertEqual(variants[0], "01111034268")

    def test_covers_every_stored_spelling(self):
        variants = customer_sync._phone_variants("01111034268")
        for expected in ("01111034268", "+201111034268", "201111034268"):
            with self.subTest(expected=expected):
                self.assertIn(expected, variants)

    def test_all_spellings_produce_the_same_variant_set(self):
        base = set(customer_sync._phone_variants("01111034268"))
        for raw in ("+201111034268", "201111034268", "00201111034268"):
            with self.subTest(raw=raw):
                self.assertTrue(base.issubset(set(customer_sync._phone_variants(raw))))

    def test_has_no_duplicates(self):
        variants = customer_sync._phone_variants("01111034268")
        self.assertEqual(len(variants), len(set(variants)))

    def test_blank_returns_empty_list(self):
        self.assertEqual(customer_sync._phone_variants(None), [])
        self.assertEqual(customer_sync._phone_variants(""), [])

    def test_foreign_number_yields_only_itself(self):
        self.assertEqual(customer_sync._phone_variants("+15551234567"), ["+15551234567"])


class TestFindCustomerByPhone(unittest.TestCase):
    """_find_customer_by_phone queries every spelling, mobile_no before phone."""

    def test_queries_mobile_no_with_all_variants(self):
        captured = {}

        def _get_values(doctype, filters, field, **_kw):
            captured["doctype"] = doctype
            captured["filters"] = filters
            return ["CUST-0001"]

        with patch.object(customer_sync.frappe.db, "get_values", side_effect=_get_values):
            result = customer_sync._find_customer_by_phone("01111034268")

        self.assertEqual(result, "CUST-0001")
        self.assertEqual(captured["doctype"], "Customer")
        operator, values = captured["filters"]["mobile_no"]
        self.assertEqual(operator, "in")
        self.assertIn("01111034268", values)
        self.assertIn("+201111034268", values)

    def test_finds_a_record_stored_in_the_other_spelling(self):
        """The exact bug: incoming 0…, stored +20… — this must still resolve."""
        stored = {"+201111034268": "CUST-STORED-INTL"}

        def _get_values(_doctype, filters, _field, **_kw):
            _operator, values = filters["mobile_no"]
            return [stored[v] for v in values if v in stored]

        with patch.object(customer_sync.frappe.db, "get_values", side_effect=_get_values), \
             patch.object(customer_sync, "_field_exists", return_value=False):
            result = customer_sync._find_customer_by_phone("01111034268")

        self.assertEqual(result, "CUST-STORED-INTL")

    def test_falls_back_to_phone_column_when_mobile_no_misses(self):
        def _get_values(_doctype, filters, _field, **_kw):
            return ["CUST-PHONE-COL"] if "phone" in filters else []

        with patch.object(customer_sync.frappe.db, "get_values", side_effect=_get_values), \
             patch.object(customer_sync, "_field_exists", return_value=True):
            result = customer_sync._find_customer_by_phone("01111034268")

        self.assertEqual(result, "CUST-PHONE-COL")

    def test_skips_phone_column_when_field_absent(self):
        seen = []

        def _get_values(_doctype, filters, _field, **_kw):
            seen.append(next(iter(filters)))
            return []

        with patch.object(customer_sync.frappe.db, "get_values", side_effect=_get_values), \
             patch.object(customer_sync, "_field_exists", return_value=False):
            customer_sync._find_customer_by_phone("01111034268")

        self.assertEqual(seen, ["mobile_no"])

    def test_blank_phone_never_queries(self):
        with patch.object(customer_sync.frappe.db, "get_values") as get_values:
            self.assertIsNone(customer_sync._find_customer_by_phone(None))
        get_values.assert_not_called()


class TestPickEstablishedCustomer(unittest.TestCase):
    """Which twin wins when a number still resolves to more than one Customer.

    504 phone numbers on production are held by several Customers, so this runs
    for real until the data is cleaned. Picking arbitrarily would scatter a
    customer's orders across their duplicates.
    """

    def test_single_candidate_needs_no_query(self):
        with patch.object(customer_sync.frappe.db, "sql") as sql:
            self.assertEqual(customer_sync._pick_established_customer(["CUST-1"]), "CUST-1")
        sql.assert_not_called()

    def test_prefers_the_record_holding_the_most_recent_invoice(self):
        """The production shape: the suffixed record is the one with the history."""
        def _sql(_query, values=None):
            self.assertIn("مصطفى مسعد-15570", values)
            return [("مصطفى مسعد-15570",)]

        with patch.object(customer_sync.frappe.db, "sql", side_effect=_sql):
            winner = customer_sync._pick_established_customer(
                ["مصطفى مسعد", "مصطفى مسعد-15570"]
            )
        self.assertEqual(winner, "مصطفى مسعد-15570")

    def test_falls_back_to_the_oldest_when_none_has_orders(self):
        with patch.object(customer_sync.frappe.db, "sql", return_value=[]), \
             patch.object(customer_sync.frappe.db, "get_values", return_value=["CUST-OLD"]):
            self.assertEqual(
                customer_sync._pick_established_customer(["CUST-NEW", "CUST-OLD"]),
                "CUST-OLD",
            )

    def test_is_deterministic_even_when_every_query_fails(self):
        with patch.object(customer_sync.frappe.db, "sql", side_effect=RuntimeError("boom")), \
             patch.object(customer_sync.frappe.db, "get_values", side_effect=RuntimeError("boom")):
            first = customer_sync._pick_established_customer(["CUST-B", "CUST-A"])
            second = customer_sync._pick_established_customer(["CUST-A", "CUST-B"])
        self.assertEqual(first, second)

    def test_empty_input_returns_none(self):
        self.assertIsNone(customer_sync._pick_established_customer([]))
        self.assertIsNone(customer_sync._pick_established_customer([None, ""]))


if __name__ == "__main__":
    unittest.main()
