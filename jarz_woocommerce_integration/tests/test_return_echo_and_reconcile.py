"""A fully returned order is closed out in ERPNext and pushed to Woo as
``cancelled`` (2026-11-17). Neither the inbound echo of that push nor the
cancellation reconcile may treat it as a live order to cancel or re-pull."""

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from jarz_woocommerce_integration.services import cancellation_reconcile, order_sync, outbound_sync


class TestReconcileClassifiesFullReturnsAsAligned(unittest.TestCase):
	def test_fully_returned_original_is_matched_returned(self):
		rows = [{"docstatus": 1, "return_status": "Fully Returned", "cancellation_type": ""}]
		self.assertEqual(cancellation_reconcile._classify_order(rows), "MATCH_RETURNED")

	def test_partially_returned_original_is_still_active(self):
		rows = [{"docstatus": 1, "return_status": "Partially Returned", "cancellation_type": ""}]
		self.assertEqual(cancellation_reconcile._classify_order(rows), "MATCH_ACTIVE")

	def test_a_live_duplicate_beside_a_returned_one_is_still_active(self):
		rows = [
			{"docstatus": 1, "return_status": "Fully Returned"},
			{"docstatus": 1, "return_status": ""},
		]
		self.assertEqual(cancellation_reconcile._classify_order(rows), "MATCH_ACTIVE")

	def test_rows_without_the_column_behave_as_before(self):
		self.assertEqual(cancellation_reconcile._classify_order([{"docstatus": 1}]), "MATCH_ACTIVE")


class TestEchoGuardAgreesWithOutbound(unittest.TestCase):
	def test_flags_alone_are_enough(self):
		inv = SimpleNamespace(docstatus=1, custom_return_status="Fully Returned", custom_sales_invoice_state="")
		self.assertTrue(order_sync._is_fully_returned_in_erpnext(inv))

	def test_credit_notes_covering_the_invoice_count_without_flags(self):
		"""A credit note raised by hand in Desk: outbound pushes ``cancelled``,
		so inbound must recognise the echo the same way."""
		inv = SimpleNamespace(docstatus=1, custom_return_status="", custom_sales_invoice_state="Delivered")
		with patch.object(outbound_sync, "_is_fully_returned", return_value=True) as check:
			self.assertTrue(order_sync._is_fully_returned_in_erpnext(inv))
		check.assert_called_once_with(inv)

	def test_an_outbound_check_failure_means_not_returned(self):
		inv = SimpleNamespace(docstatus=1, custom_return_status="", custom_sales_invoice_state="Delivered")
		with patch.object(outbound_sync, "_is_fully_returned", side_effect=RuntimeError("db")):
			self.assertFalse(order_sync._is_fully_returned_in_erpnext(inv))

	def test_a_draft_is_never_treated_as_returned(self):
		inv = SimpleNamespace(docstatus=0, custom_return_status="", custom_sales_invoice_state="")
		with patch.object(outbound_sync, "_is_fully_returned", return_value=True) as check:
			self.assertFalse(order_sync._is_fully_returned_in_erpnext(inv))
		check.assert_not_called()


if __name__ == "__main__":
	unittest.main()
