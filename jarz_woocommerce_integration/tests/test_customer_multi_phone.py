"""Two phone numbers typed into Woo's single phone box.

Woo order 17052 (``01016518620/01062261342``) never reached ERPNext.  Stripping
every non-digit *fused* the pair into the 22-character ``0101651862001062261342``,
which no phone-number pattern accepts.  ``Customer.mobile_no`` is ``Read Only``
and unvalidated so the Customer was created anyway; ``Address.phone`` is
``Data``/``Phone`` and threw ``InvalidPhoneNumberError`` a few lines later, which
``order_sync`` could only surface as ``customer_error:internal_error``.  The order
was dropped with no Order Map row while every ``CronLive`` run logged ``Success``.

Production holds seven Customers with a fused number, the oldest from April, so
the input shape recurs and the separator is not always a slash.
"""

import unittest

from jarz_woocommerce_integration.services import customer_sync


#: Frappe's own ``PHONE_NUMBER_PATTERN``, restated so these tests assert against
#: the real constraint without needing a bootstrapped site.
import re

_FRAPPE_PHONE_PATTERN = re.compile(r"[0-9 +_\-,.*#()]{1,20}$")


def _frappe_would_accept(value: str) -> bool:
    return bool(_FRAPPE_PHONE_PATTERN.match(value))


class TestMultiPhoneNormalisation(unittest.TestCase):
    """A field holding two numbers collapses to the first, never to their fusion."""

    def test_order_17052_slash_separated_pair(self):
        self.assertEqual(
            customer_sync._normalize_phone("01016518620/01062261342"),
            "01016518620",
        )

    def test_every_separator_a_customer_actually_uses(self):
        cases = {
            "01016518620/01062261342": "01016518620",
            "01016518620 / 01062261342": "01016518620",
            "01016518620,01062261342": "01016518620",
            "01016518620، 01062261342": "01016518620",
            "01016518620;01062261342": "01016518620",
            "01016518620|01062261342": "01016518620",
            "01016518620 or 01062261342": "01016518620",
            "01016518620 و 01062261342": "01016518620",
            "01016518620\n01062261342": "01016518620",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(customer_sync._normalize_phone(raw), expected)

    def test_pairs_written_with_no_separator_at_all(self):
        """The real production rows: two numbers simply concatenated."""
        cases = {
            "0101651862001062261342": "01016518620",
            "+201210409690+201157355136": "01210409690",
            "+20106743767401067437674": "01067437674",
            "0109050681901094553213": "01090506819",
            "0115765142901000765402": "01157651429",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(customer_sync._normalize_phone(raw), expected)

    def test_result_is_always_a_number_frappe_accepts(self):
        for raw in ("01016518620/01062261342", "0101651862001062261342",
                    "+201210409690+201157355136", "01016518620 or 01062261342"):
            with self.subTest(raw=raw):
                self.assertTrue(_frappe_would_accept(customer_sync._normalize_phone(raw)))


class TestSingleNumberFormattingIsUntouched(unittest.TestCase):
    """Spaces, dashes and parens format ONE number — splitting on them is a bug."""

    def test_intra_number_punctuation_still_folds_normally(self):
        cases = {
            "01111034268": "01111034268",
            "+201111034268": "01111034268",
            "201111034268": "01111034268",
            "00201111034268": "01111034268",
            "0111 103 4268": "01111034268",
            "0111-103-4268": "01111034268",
            "+20 111 103 4268": "01111034268",
        }
        for raw, expected in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(customer_sync._normalize_phone(raw), expected)

    def test_non_egyptian_numbers_are_still_not_rewritten(self):
        self.assertEqual(customer_sync._normalize_phone("+15551234567"), "+15551234567")
        self.assertEqual(customer_sync._normalize_phone("+441632960961"), "+441632960961")

    def test_blank_input_still_returns_none(self):
        for raw in (None, "", "   ", "abc", "/", " , "):
            with self.subTest(raw=raw):
                self.assertIsNone(customer_sync._normalize_phone(raw))

    def test_is_idempotent(self):
        once = customer_sync._normalize_phone("01016518620/01062261342")
        self.assertEqual(customer_sync._normalize_phone(once), once)


class TestPhoneVariantsStillMatchLegacyRows(unittest.TestCase):
    """The seven fused numbers already in production must remain findable.

    Changing the canonical form would otherwise orphan them: lookups compare
    ``mobile_no`` with an exact ``IN``, so the stored fused spelling has to stay
    in the variant list or those Customers get duplicated on their next order.
    """

    def test_fused_spelling_survives_as_a_variant(self):
        variants = customer_sync._phone_variants("01016518620/01062261342")
        self.assertEqual(variants[0], "01016518620")
        self.assertIn("0101651862001062261342", variants)

    def test_canonical_variants_are_still_generated(self):
        variants = customer_sync._phone_variants("01016518620/01062261342")
        for expected in ("01016518620", "+201016518620", "201016518620"):
            with self.subTest(expected=expected):
                self.assertIn(expected, variants)

    def test_has_no_duplicates(self):
        variants = customer_sync._phone_variants("01016518620/01062261342")
        self.assertEqual(len(variants), len(set(variants)))


class TestSafePhoneValue(unittest.TestCase):
    """An unusable phone is dropped; it never gets to veto the order."""

    def test_pair_reduces_to_the_first_number(self):
        self.assertEqual(
            customer_sync._safe_phone_value("01016518620/01062261342"),
            "01016518620",
        )

    def test_garbage_becomes_empty_rather_than_raising(self):
        for raw in (None, "", "   ", "n/a", "no phone"):
            with self.subTest(raw=raw):
                self.assertEqual(customer_sync._safe_phone_value(raw), "")

    def test_output_is_always_storable_in_a_phone_field(self):
        for raw in ("01016518620/01062261342", "0101651862001062261342",
                    "+201210409690+201157355136", "01111034268", "+441632960961",
                    "n/a", "", None):
            with self.subTest(raw=raw):
                value = customer_sync._safe_phone_value(raw)
                self.assertTrue(value == "" or _frappe_would_accept(value))
                self.assertLessEqual(len(value), 20)

    def test_ordinary_numbers_pass_through_canonicalised(self):
        self.assertEqual(customer_sync._safe_phone_value("+201111034268"), "01111034268")
        self.assertEqual(customer_sync._safe_phone_value("0111 103 4268"), "01111034268")


class TestSplitPhoneCandidates(unittest.TestCase):
    def test_returns_every_number_the_customer_gave(self):
        self.assertEqual(
            customer_sync._split_phone_candidates("01016518620/01062261342"),
            ["01016518620", "01062261342"],
        )

    def test_single_number_is_one_candidate(self):
        self.assertEqual(
            customer_sync._split_phone_candidates("0111-103-4268"),
            ["0111-103-4268"],
        )

    def test_empty_input_is_no_candidates(self):
        for raw in (None, "", "  "):
            with self.subTest(raw=raw):
                self.assertEqual(customer_sync._split_phone_candidates(raw), [])


if __name__ == "__main__":
    unittest.main()
