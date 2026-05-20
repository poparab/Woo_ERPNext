import unittest

from jarz_woocommerce_integration.api import manual_sync


class TestManualSync(unittest.TestCase):
    def test_push_sales_invoice_records_success_audit(self):
        with unittest.mock.patch.object(manual_sync.frappe, "has_permission", return_value=True), \
             unittest.mock.patch.object(manual_sync, "sync_sales_invoice", return_value={"status": "ok"}) as sync_invoice, \
             unittest.mock.patch.object(manual_sync.sync_events, "record_manual_push_audit_event") as record_audit:
            result = manual_sync.push_sales_invoice("SINV-0001")

        sync_invoice.assert_called_once_with("SINV-0001", reason="manual_button", force=True)
        record_audit.assert_called_once_with(
            object_type="Sales Invoice",
            docname="SINV-0001",
            result={"status": "ok"},
        )
        self.assertEqual(result, {"status": "ok"})

    def test_push_customer_records_failed_audit_before_throw(self):
        with unittest.mock.patch.object(manual_sync.frappe, "has_permission", return_value=True), \
             unittest.mock.patch.object(manual_sync, "sync_customer", side_effect=RuntimeError("boom")) as sync_customer, \
             unittest.mock.patch.object(manual_sync.frappe, "log_error", return_value=None), \
             unittest.mock.patch.object(manual_sync.frappe, "get_traceback", return_value="traceback"), \
             unittest.mock.patch.object(manual_sync.sync_events, "record_manual_push_audit_event") as record_audit, \
             unittest.mock.patch.object(manual_sync.frappe, "throw", side_effect=RuntimeError("boom")):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                manual_sync.push_customer("CUST-0001")

        sync_customer.assert_called_once_with("CUST-0001", reason="manual_button", force=True)
        record_audit.assert_called_once_with(
            object_type="Customer",
            docname="CUST-0001",
            error="boom",
        )