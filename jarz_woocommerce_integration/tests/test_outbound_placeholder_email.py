"""The placeholder email is an identity, so it has to be unique per Customer.

Slugging the docname with ``[^a-zA-Z0-9]`` deletes every Arabic character, so
most of the customer base collapsed onto ``customer@placeholder.com`` and the
ERPNext-disambiguated ``… - 2`` names collapsed onto ``2@placeholder.com``.
WooCommerce answered "email already registered", the reconcile branch adopted the
one shared account, and 215 unrelated Customers ended up on woo_customer_id 3357.
"""

import unittest
from unittest.mock import MagicMock, patch

from jarz_woocommerce_integration.services import outbound_sync


class TestPlaceholderEmail(unittest.TestCase):

    def test_latin_names_are_unchanged(self):
        """These customers already own a Woo account keyed to exactly this address."""
        cases = {
            "Ahmed Mohamed": "ahmedmohamed@placeholder.com",
            "Hind Eltayeb - 1": "hindeltayeb1@placeholder.com",
            "logy essam": "logyessam@placeholder.com",
        }
        for docname, expected in cases.items():
            with self.subTest(docname=docname):
                self.assertEqual(outbound_sync._placeholder_email(docname, "01111034268"), expected)

    def test_arabic_names_no_longer_collapse_onto_one_address(self):
        first = outbound_sync._placeholder_email("حنان تحسين", "01120421114")
        second = outbound_sync._placeholder_email("حسين صابر ابو زيد", "01033157113")
        self.assertNotEqual(first, second)
        self.assertNotIn("customer@placeholder.com", (first, second))

    def test_numeric_slug_names_no_longer_collapse(self):
        """'X - 2' used to sanitize down to the bare '2'."""
        first = outbound_sync._placeholder_email("حسين صابر ابو زيد - 2", "01033157113")
        second = outbound_sync._placeholder_email("عماد - 2", "01111706356")
        self.assertNotEqual(first, second)
        for email in (first, second):
            with self.subTest(email=email):
                self.assertFalse(email.startswith("2@"))

    def test_arabic_name_is_keyed_to_the_phone(self):
        self.assertEqual(
            outbound_sync._placeholder_email("حنان تحسين", "01120421114"),
            "cust01120421114@placeholder.com",
        )

    def test_same_phone_yields_the_same_address(self):
        """Stability matters: a changing placeholder would re-create Woo accounts."""
        self.assertEqual(
            outbound_sync._placeholder_email("حنان تحسين", "01120421114"),
            outbound_sync._placeholder_email("حنان تحسين", "01120421114"),
        )

    def test_falls_back_to_a_name_digest_without_a_phone(self):
        email = outbound_sync._placeholder_email("حنان تحسين", None)
        self.assertTrue(email.startswith("cust"))
        self.assertTrue(email.endswith("@placeholder.com"))
        self.assertEqual(email, outbound_sync._placeholder_email("حنان تحسين", None))
        self.assertNotEqual(email, outbound_sync._placeholder_email("حسين صابر", None))

    def test_placeholder_phone_is_not_treated_as_an_identity(self):
        """0000000000 is the missing-phone stand-in and is shared by many customers."""
        first = outbound_sync._placeholder_email("حنان تحسين", "0000000000")
        second = outbound_sync._placeholder_email("عماد", "0000000000")
        self.assertNotEqual(first, second)

    def test_every_address_is_a_valid_single_mailbox(self):
        for docname in ("حنان تحسين", "عماد - 2", "Ahmed Mohamed", ""):
            with self.subTest(docname=docname):
                email = outbound_sync._placeholder_email(docname, "01111034268")
                self.assertEqual(email.count("@"), 1)
                self.assertTrue(email.split("@")[0])


class TestReconcileRefusesClaimedAccounts(unittest.TestCase):
    """Adopting an already-bound Woo account is what poisoned woo_customer_id."""

    def test_guard_is_wired_into_the_reconcile_branch(self):
        import inspect

        source = inspect.getsource(outbound_sync)
        marker = "customer_woo_id_is_claimed_by_other(woo_customer_id, customer_name)"
        self.assertIn(marker, source)
        # the refusal must happen before the id is stored
        self.assertLess(
            source.index(marker),
            source.index("set_customer_woo_id(customer_name, woo_customer_id"),
        )

    def test_refusal_is_reported_as_an_error_status(self):
        import inspect

        source = inspect.getsource(outbound_sync)
        guard = source.index("woo_outbound_customer_id_already_claimed")
        tail = source[guard:guard + 900]
        self.assertIn('_mark_customer_status(customer_name, status="error"', tail)
        self.assertIn('return {"status": "error"', tail)


if __name__ == "__main__":
    unittest.main()
