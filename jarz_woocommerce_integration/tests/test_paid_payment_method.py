"""A paid invoice must keep its payment method across a later Woo update.

Woo 17783: a COD order was settled in the POS by a line manager via Kashier.
jarz_pos re-stamped ``custom_payment_method = "Kashier Card"`` and posted a
Payment Entry into the Kashier ledger, but the store still carried ``cod``. The
next Woo webhook (a status change in Woo admin) mapped that to ``Cash`` and
wrote it onto the submitted invoice, so the badge and receipt promised cash
collection again for an order that was already paid.

Dispatch's Courier Outstanding transfer is also a Payment Entry against the
invoice, but it is not the customer paying, so it must not count.

Written as ``unittest.TestCase`` classes on purpose: ``bench run-tests``
collects through unittest discovery and never sees module-level ``test_*``.
"""

import unittest
from types import SimpleNamespace

from jarz_woocommerce_integration.services import order_sync
from jarz_woocommerce_integration.tests._monkeypatch import MonkeyPatch
from jarz_woocommerce_integration.tests.test_credit_payment_method import _FakeInvoice


class TestInboundEchoCannotDowngradePaidInvoice(unittest.TestCase):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)
        self.sync_logs = []
        self.payment_entries = []  # rows: {"name", "paid_to", "invoice"}
        self.monkeypatch.setattr(
            order_sync,
            "create_sync_log_entry",
            lambda operation, status, message, **kwargs: self.sync_logs.append(
                {"operation": operation, "status": status, "message": message, **kwargs}
            ),
        )
        self.monkeypatch.setattr(
            order_sync.frappe,
            "logger",
            lambda *args, **kwargs: SimpleNamespace(
                warning=lambda payload: None, info=lambda payload: None
            ),
        )
        self.monkeypatch.setattr(order_sync.frappe, "get_all", self._get_all, raising=False)

    def _get_all(self, doctype, filters=None, fields=None, pluck=None, **kwargs):
        filters = filters or {}
        if doctype == "Payment Entry Reference":
            assert filters.get("reference_doctype") == "Sales Invoice"
            return [
                r["name"] for r in self.payment_entries
                if r["invoice"] == filters.get("reference_name")
            ]
        if doctype == "Payment Entry":
            # The query must ask for submitted receipts only.
            assert filters.get("docstatus") == 1
            assert filters.get("payment_type") == "Receive"
            names = set(filters["name"][1])
            return [
                {"name": r["name"], "paid_to": r["paid_to"]}
                for r in self.payment_entries
                if r["name"] in names
            ]
        raise AssertionError(f"unexpected get_all({doctype!r})")

    def _pay(self, invoice, name, paid_to):
        self.payment_entries.append({"name": name, "paid_to": paid_to, "invoice": invoice})

    def test_cash_does_not_overwrite_kashier_paid_invoice(self):
        inv = _FakeInvoice(name="ACC-SINV-17783", custom_payment_method="Kashier Card")
        self._pay("ACC-SINV-17783", "ACC-PAY-0001", "Kashier - J")

        order_sync._apply_inbound_payment_method(inv, "Cash", woo_id=17783)

        self.assertEqual(inv.db_set_calls, [])
        self.assertEqual(inv.custom_payment_method, "Kashier Card")
        self.assertEqual(len(self.sync_logs), 1)
        self.assertEqual(self.sync_logs[0]["operation"], "PaidPaymentMethodPreserved")
        self.assertEqual(self.sync_logs[0]["woo_order_id"], 17783)
        self.assertIn("ACC-PAY-0001", self.sync_logs[0]["message"])

    def test_courier_outstanding_transfer_is_not_a_customer_payment(self):
        inv = _FakeInvoice(name="ACC-SINV-20001", custom_payment_method="Kashier Card")
        self._pay("ACC-SINV-20001", "ACC-PAY-0002", "Courier Outstanding - J")

        order_sync._apply_inbound_payment_method(inv, "Cash", woo_id=20001)

        self.assertEqual(inv.db_set_calls, [("custom_payment_method", "Cash")])
        self.assertEqual(self.sync_logs, [])

    def test_unpaid_invoice_still_takes_the_inbound_value(self):
        inv = _FakeInvoice(name="ACC-SINV-20002", custom_payment_method="Instapay")

        order_sync._apply_inbound_payment_method(inv, "Cash", woo_id=20002)

        self.assertEqual(inv.db_set_calls, [("custom_payment_method", "Cash")])
        self.assertEqual(self.sync_logs, [])

    def test_paid_invoice_may_still_move_between_non_cash_methods(self):
        # Only a downgrade to Cash is blocked; the store correcting one online
        # method to another is not the bug.
        inv = _FakeInvoice(name="ACC-SINV-20003", custom_payment_method="Kashier Card")
        self._pay("ACC-SINV-20003", "ACC-PAY-0003", "Kashier - J")

        order_sync._apply_inbound_payment_method(inv, "Kashier Wallet", woo_id=20003)

        self.assertEqual(inv.db_set_calls, [("custom_payment_method", "Kashier Wallet")])
        self.assertEqual(self.sync_logs, [])

    def test_cash_on_a_cash_paid_invoice_is_a_plain_write(self):
        inv = _FakeInvoice(name="ACC-SINV-20004", custom_payment_method="Cash")
        self._pay("ACC-SINV-20004", "ACC-PAY-0004", "Cash - J")

        order_sync._apply_inbound_payment_method(inv, "Cash", woo_id=20004)

        self.assertEqual(inv.db_set_calls, [("custom_payment_method", "Cash")])
        self.assertEqual(self.sync_logs, [])

    def test_lookup_failure_falls_back_to_the_old_write(self):
        def _boom(*args, **kwargs):
            raise Exception("db down")

        self.monkeypatch.setattr(order_sync.frappe, "get_all", _boom, raising=False)
        inv = _FakeInvoice(name="ACC-SINV-20005", custom_payment_method="Kashier Card")

        order_sync._apply_inbound_payment_method(inv, "Cash", woo_id=20005)

        self.assertEqual(inv.db_set_calls, [("custom_payment_method", "Cash")])

    def test_draft_invoice_is_not_queried(self):
        inv = _FakeInvoice(name="ACC-SINV-20006", docstatus=0, custom_payment_method="Kashier Card")
        self.monkeypatch.setattr(
            order_sync.frappe,
            "get_all",
            lambda *a, **k: (_ for _ in ()).throw(AssertionError("queried a draft")),
            raising=False,
        )

        order_sync._apply_inbound_payment_method(inv, "Cash", woo_id=20006)

        self.assertEqual(inv.custom_payment_method, "Cash")


if __name__ == "__main__":
    unittest.main()
