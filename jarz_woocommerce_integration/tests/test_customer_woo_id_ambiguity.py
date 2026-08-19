"""woo_customer_id must identify exactly one Customer, or identify none.

Production has ids claimed by hundreds of unrelated Customers — 215 of them on
id 3357 alone — because the outbound push generated a colliding placeholder
email and adopted whatever WooCommerce account matched it.  ``find_customer_by_woo_id``
is step zero of every resolution, so an unordered lookup over a poisoned id
hands back an arbitrary stranger.
"""

import unittest
from unittest.mock import patch

from jarz_woocommerce_integration.services import customer_sync
from jarz_woocommerce_integration.utils import customer_woo_id


class TestFindCustomerByWooId(unittest.TestCase):

    def test_returns_the_single_holder(self):
        with patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
             patch.object(customer_woo_id.frappe, "get_all", return_value=["CUST-0001"]):
            self.assertEqual(customer_woo_id.find_customer_by_woo_id(5973), "CUST-0001")

    def test_refuses_to_guess_when_the_id_is_claimed_by_several(self):
        """Falling through to the phone lookup beats attaching an order to a stranger."""
        with patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
             patch.object(customer_woo_id.frappe, "get_all", return_value=["CUST-A", "CUST-B"]):
            self.assertIsNone(customer_woo_id.find_customer_by_woo_id(3357))

    def test_returns_none_when_nothing_holds_the_id(self):
        with patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
             patch.object(customer_woo_id.frappe, "get_all", return_value=[]):
            self.assertIsNone(customer_woo_id.find_customer_by_woo_id(9999))

    def test_zero_and_blank_ids_are_never_looked_up(self):
        with patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
             patch.object(customer_woo_id.frappe, "get_all") as get_all:
            for value in (0, "0", "", None, "abc"):
                with self.subTest(value=value):
                    self.assertIsNone(customer_woo_id.find_customer_by_woo_id(value))
            get_all.assert_not_called()

    def test_lookup_is_bounded_and_deterministic(self):
        captured = {}

        def _get_all(_doctype, **kwargs):
            captured.update(kwargs)
            return ["CUST-0001"]

        with patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
             patch.object(customer_woo_id.frappe, "get_all", side_effect=_get_all):
            customer_woo_id.find_customer_by_woo_id(5973)

        self.assertEqual(captured["limit"], 2)
        self.assertEqual(captured["order_by"], "creation asc")
        self.assertEqual(captured["filters"], {"woo_customer_id": "5973"})


class TestCustomerWooIdIsClaimedByOther(unittest.TestCase):

    def test_true_when_another_customer_holds_it(self):
        with patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
             patch.object(customer_woo_id.frappe, "get_all", return_value=["CUST-OTHER"]):
            self.assertTrue(customer_woo_id.customer_woo_id_is_claimed_by_other(3357, "CUST-MINE"))

    def test_false_when_only_this_customer_holds_it(self):
        with patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
             patch.object(customer_woo_id.frappe, "get_all", return_value=["CUST-MINE"]):
            self.assertFalse(customer_woo_id.customer_woo_id_is_claimed_by_other(3357, "CUST-MINE"))

    def test_false_when_unclaimed(self):
        with patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
             patch.object(customer_woo_id.frappe, "get_all", return_value=[]):
            self.assertFalse(customer_woo_id.customer_woo_id_is_claimed_by_other(3357, "CUST-MINE"))

    def test_query_failure_does_not_block_the_write(self):
        """A broken guard must not stop legitimate syncing."""
        with patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
             patch.object(customer_woo_id.frappe, "get_all", side_effect=RuntimeError("boom")):
            self.assertFalse(customer_woo_id.customer_woo_id_is_claimed_by_other(3357, "CUST-MINE"))


class TestUpdateCustomerIdentityGuard(unittest.TestCase):
    """_update_customer_identity must not stamp an id another Customer holds."""

    def _run(self, *, claimed):
        written = {}

        def _set_value(_doctype, _name, updates, update_modified=True):
            written.update(updates)

        with patch.object(customer_sync.frappe.db, "get_value", return_value=None), \
             patch.object(customer_sync.frappe.db, "set_value", side_effect=_set_value), \
             patch.object(customer_sync, "_field_exists", return_value=True), \
             patch.object(customer_sync, "get_customer_woo_id", return_value=None), \
             patch.object(customer_sync, "customer_woo_id_is_claimed_by_other", return_value=claimed):
            customer_sync._update_customer_identity(
                "CUST-MINE",
                woo_customer_id=3357,
                username=None,
                phone_norm="01111034268",
                email=None,
                customer_cache=None,
            )
        return written

    def test_writes_an_unclaimed_id(self):
        written = self._run(claimed=False)
        self.assertEqual(written.get("woo_customer_id"), "3357")

    def test_skips_a_claimed_id_but_still_writes_the_rest(self):
        written = self._run(claimed=True)
        self.assertNotIn("woo_customer_id", written)
        self.assertEqual(written.get("mobile_no"), "01111034268")


if __name__ == "__main__":
    unittest.main()
