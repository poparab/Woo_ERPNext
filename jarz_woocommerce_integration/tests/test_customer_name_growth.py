"""Regression tests for the customer name that grew by one surname per sync.

Production showed ``ilo specialty coffee`` stored as "ilo specialty coffee
specialty coffee specialty coffee ..." (seven times), and 56 other customers
carrying a shorter version of the same repetition.

The loop had two halves:

* the Woo account was created on the shop with the WHOLE name in
  ``billing.first_name`` and an empty ``billing.last_name``;
* outbound sync pushes the ERP name SPLIT across
  ``shipping.first_name``/``shipping.last_name``.

Inbound read each field with its own ``billing or shipping`` fallback, so the
name it rebuilt was ``billing.first_name + " " + shipping.last_name`` — the full
name plus the surname again. Outbound then re-split that longer name into
shipping, and the next round added the surname once more.
"""
from __future__ import annotations

import unittest
import unittest.mock

from jarz_woocommerce_integration.services import customer_sync


class PickNameParts(unittest.TestCase):
    """A name comes from one block; two blocks never contribute halves."""

    def test_billing_name_is_not_completed_from_shipping(self):
        billing = {"first_name": "ilo specialty coffee", "last_name": ""}
        shipping = {"first_name": "ilo", "last_name": "specialty coffee specialty coffee"}

        first, last = customer_sync._pick_name_parts(billing, shipping)

        self.assertEqual((first, last), ("ilo specialty coffee", ""))
        self.assertEqual(
            customer_sync._normalize_name(first, last, None, None),
            "ilo specialty coffee",
        )

    def test_shipping_is_used_when_billing_names_nobody(self):
        billing = {"first_name": "", "last_name": "", "phone": "01000000000"}
        shipping = {"first_name": "Mona", "last_name": "Saleh"}

        self.assertEqual(customer_sync._pick_name_parts(billing, shipping), ("Mona", "Saleh"))

    def test_split_name_in_one_block_still_joins(self):
        billing = {"first_name": "Mona", "last_name": "Saleh"}
        shipping = {"first_name": "Mona", "last_name": "Saleh Saleh"}

        first, last = customer_sync._pick_name_parts(billing, shipping)
        self.assertEqual(customer_sync._normalize_name(first, last, None, None), "Mona Saleh")

    def test_no_named_block_falls_through_to_the_caller_defaults(self):
        self.assertEqual(customer_sync._pick_name_parts({}, {}, None), ("", ""))


class RepeatedTailGrowth(unittest.TestCase):
    """Second line of defence: a sync may rename, never grow a repeat."""

    def test_detects_the_production_shape(self):
        self.assertTrue(
            customer_sync._is_repeated_tail_growth(
                "ilo specialty coffee", "ilo specialty coffee specialty coffee"
            )
        )

    def test_detects_growth_on_an_already_grown_name(self):
        grown = "ilo specialty coffee specialty coffee specialty coffee"
        self.assertTrue(
            customer_sync._is_repeated_tail_growth(grown, grown + " specialty coffee")
        )

    def test_single_word_surname_repeat(self):
        self.assertTrue(customer_sync._is_repeated_tail_growth("محمد حمدي", "محمد حمدي حمدي"))

    def test_a_genuine_rename_is_not_growth(self):
        self.assertFalse(customer_sync._is_repeated_tail_growth("Mona Saleh", "Mona Saleh Ibrahim"))
        self.assertFalse(customer_sync._is_repeated_tail_growth("Mona Saleh", "Mona Ibrahim"))
        self.assertFalse(customer_sync._is_repeated_tail_growth("", "Mona Saleh"))

    def test_partial_word_is_not_a_repeat(self):
        # "Mohamed" ends with the letters "med" but not with the WORD "med".
        self.assertFalse(customer_sync._is_repeated_tail_growth("Mohamed", "Mohamed med"))


class IdentityWriteGuard(unittest.TestCase):
    """``_update_customer_identity`` must not persist a repeated-tail name."""

    def _run(self, current: str, display_name: str) -> dict:
        written: dict = {}

        def get_value(doctype, name, fieldname=None, **_kwargs):
            if fieldname == "customer_name":
                return current
            return None

        def set_value(doctype, name, values, update_modified=False):
            written.update(values if isinstance(values, dict) else {})

        fake_db = unittest.mock.MagicMock()
        fake_db.get_value.side_effect = get_value
        fake_db.set_value.side_effect = set_value

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
                unittest.mock.patch.object(customer_sync, "get_customer_woo_id", return_value=None), \
                unittest.mock.patch.object(customer_sync, "_field_exists", return_value=False):
            customer_sync._update_customer_identity(
                "ilo specialty coffee",
                woo_customer_id=None,
                username=None,
                phone_norm=None,
                email=None,
                customer_cache=None,
                display_name=display_name,
                overwrite_existing=True,
            )
        return written

    def test_repeated_tail_is_refused(self):
        written = self._run("ilo specialty coffee", "ilo specialty coffee specialty coffee")
        self.assertNotIn("customer_name", written)

    def test_real_rename_still_lands(self):
        written = self._run("ilo specialty coffee", "ilo specialty coffee Heliopolis")
        self.assertEqual(written.get("customer_name"), "ilo specialty coffee Heliopolis")


if __name__ == "__main__":
    unittest.main()
