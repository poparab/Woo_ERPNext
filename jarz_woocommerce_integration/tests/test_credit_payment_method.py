"""``Credit`` -- the B2B on-account sale -- must survive the Woo round trip.

jarz_pos writes ``custom_payment_method = "Credit"`` when a shop takes an order
on account: delivered, not paid, the receivable left open. This app used to
destroy that in two places:

* outbound, ``Credit`` was not in the map, so it hit the unknown-value fallback
  and advertised the order to the customer as cash-on-delivery (``cod``);
* inbound, the next update mapped that ``cod`` back to ``Cash`` and wrote it
  onto the *submitted* invoice, erasing the debt from the jarz_pos ledger and
  credit-limit queries that key on that column while the money was still owed.

Written as ``unittest.TestCase`` classes on purpose: ``bench run-tests``
collects through unittest discovery and never sees module-level ``test_*``.
"""

import unittest
import unittest.mock
from types import SimpleNamespace

from jarz_woocommerce_integration.services import order_sync, payment_map
from jarz_woocommerce_integration.tests._monkeypatch import MonkeyPatch


def _cfg(**overrides):
    values = {
        "payment_cod": "cod",
        "payment_instapay": "instapay",
        "payment_wallet": "wallet",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _FakeInvoice:
    """Enough Sales Invoice surface for ``_apply_inbound_payment_method``.

    ``fields`` deliberately holds only what the caller passes: a bench that has
    not migrated jarz_pos's credit stamp has no ``custom_credit_terms_days``
    column at all, and the guard has to survive that.
    """

    def __init__(self, name="ACC-SINV-00001", docstatus=1, **fields):
        self.name = name
        self.docstatus = docstatus
        self._fields = dict(fields)
        self.db_set_calls = []
        for key, value in fields.items():
            setattr(self, key, value)

    def get(self, fieldname, default=None):
        return self._fields.get(fieldname, default)

    def db_set(self, fieldname, value, commit=False):
        self.db_set_calls.append((fieldname, value))
        self._fields[fieldname] = value
        setattr(self, fieldname, value)


class TestCreditOutboundMapping(unittest.TestCase):
    """Outbound must never advertise a credit sale as cash-on-delivery."""

    def test_credit_does_not_ship_as_cod(self):
        method_id, _title = payment_map.erpnext_to_woo("Credit", _cfg())

        self.assertNotEqual(method_id, "cod")
        self.assertEqual(method_id, "credit")

    def test_credit_pushes_an_unambiguous_title(self):
        # A bare "Credit" on a customer-facing order reads as *credit card*, so
        # this is the one value whose canonical title beats the raw spelling.
        _method_id, title = payment_map.erpnext_to_woo("Credit", _cfg())

        self.assertEqual(title, "Credit (On Account)")

    def test_credit_is_a_mapped_value_not_the_unknown_fallback(self):
        with unittest.mock.patch.object(payment_map.LOGGER, "warning") as warning:
            payment_map.erpnext_to_woo("Credit", _cfg())

        warning.assert_not_called()

    def test_credit_aliases_all_reach_the_same_id(self):
        for raw in ("credit", "Credit", "on account", "On-Account", "onaccount", "on_account"):
            with self.subTest(raw=raw):
                self.assertEqual(payment_map.erpnext_to_woo(raw, _cfg())[0], "credit")

    def test_configured_cod_id_cannot_leak_onto_credit(self):
        cfg = _cfg(payment_cod="cash_on_delivery")

        self.assertEqual(payment_map.erpnext_to_woo("Credit", cfg)[0], "credit")

    def test_credit_round_trips(self):
        self.assertEqual(payment_map.woo_to_erpnext("credit"), "Credit")
        self.assertEqual(payment_map.woo_to_erpnext("on_account"), "Credit")
        self.assertEqual(payment_map.woo_to_erpnext("on-account"), "Credit")

    def test_cod_still_means_cash(self):
        # The fix must not disturb the five methods that were already right.
        self.assertEqual(payment_map.woo_to_erpnext("cod"), "Cash")
        self.assertEqual(payment_map.erpnext_to_woo("Cash", _cfg())[0], "cod")

    def test_is_credit_helper(self):
        self.assertTrue(payment_map.is_credit("Credit"))
        self.assertTrue(payment_map.is_credit("on account"))
        self.assertFalse(payment_map.is_credit("Cash"))
        self.assertFalse(payment_map.is_credit(""))
        self.assertFalse(payment_map.is_credit(None))


class TestInboundEchoCannotDowngradeCredit(unittest.TestCase):
    """The inbound echo must never rewrite a live credit invoice to Cash."""

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)
        self.sync_logs = []
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

    def test_cash_never_overwrites_a_submitted_credit_invoice(self):
        inv = _FakeInvoice(docstatus=1, custom_payment_method="Credit")

        order_sync._apply_inbound_payment_method(inv, "Cash", woo_id=14763)

        self.assertEqual(inv.db_set_calls, [])
        self.assertEqual(inv.custom_payment_method, "Credit")
        self.assertEqual(len(self.sync_logs), 1)
        self.assertEqual(self.sync_logs[0]["operation"], "CreditPaymentMethodPreserved")
        self.assertEqual(self.sync_logs[0]["woo_order_id"], 14763)

    def test_credit_terms_stamp_alone_is_enough_to_protect(self):
        # Something already clobbered the payment method; the frozen stamp
        # jarz_pos writes at order creation still says this is on account.
        inv = _FakeInvoice(
            docstatus=1, custom_payment_method="Cash", custom_credit_terms_days=30
        )

        order_sync._apply_inbound_payment_method(inv, "Instapay", woo_id=14764)

        self.assertEqual(inv.db_set_calls, [])
        self.assertEqual(len(self.sync_logs), 1)

    def test_zero_credit_terms_does_not_protect(self):
        inv = _FakeInvoice(
            docstatus=1, custom_payment_method="Cash", custom_credit_terms_days=0
        )

        order_sync._apply_inbound_payment_method(inv, "Instapay", woo_id=14765)

        self.assertEqual(inv.db_set_calls, [("custom_payment_method", "Instapay")])
        self.assertEqual(self.sync_logs, [])

    def test_a_draft_credit_invoice_is_protected_too(self):
        inv = _FakeInvoice(docstatus=0, custom_payment_method="Credit")

        order_sync._apply_inbound_payment_method(inv, "Cash", woo_id=14766)

        self.assertEqual(inv.custom_payment_method, "Credit")
        self.assertEqual(len(self.sync_logs), 1)

    def test_credit_may_be_written_onto_a_credit_invoice(self):
        # The store now echoes "credit" back, which maps to "Credit". That is
        # not a downgrade, so it must not be treated as one.
        inv = _FakeInvoice(docstatus=1, custom_payment_method="Credit")

        order_sync._apply_inbound_payment_method(inv, "Credit", woo_id=14767)

        self.assertEqual(inv.db_set_calls, [("custom_payment_method", "Credit")])
        self.assertEqual(self.sync_logs, [])

    def test_ordinary_invoices_are_unaffected(self):
        submitted = _FakeInvoice(docstatus=1, custom_payment_method="Cash")
        order_sync._apply_inbound_payment_method(submitted, "Instapay", woo_id=14768)
        self.assertEqual(submitted.db_set_calls, [("custom_payment_method", "Instapay")])

        draft = _FakeInvoice(docstatus=0, custom_payment_method=None)
        order_sync._apply_inbound_payment_method(draft, "Cash", woo_id=14769)
        self.assertEqual(draft.custom_payment_method, "Cash")

        self.assertEqual(self.sync_logs, [])

    def test_missing_credit_terms_column_does_not_raise(self):
        # Un-migrated bench: no such field anywhere on the doc.
        inv = _FakeInvoice(docstatus=1, custom_payment_method="Cash")

        order_sync._apply_inbound_payment_method(inv, "Instapay", woo_id=14770)

        self.assertEqual(inv.db_set_calls, [("custom_payment_method", "Instapay")])

    def test_blank_inbound_value_writes_nothing(self):
        inv = _FakeInvoice(docstatus=1, custom_payment_method="Credit")

        order_sync._apply_inbound_payment_method(inv, "", woo_id=14771)
        order_sync._apply_inbound_payment_method(inv, None, woo_id=14772)

        self.assertEqual(inv.db_set_calls, [])
        self.assertEqual(self.sync_logs, [])


class TestCreditIsNeverAutoPaid(unittest.TestCase):
    """An on-account sale is unpaid by definition; no store status settles it."""

    def test_historical_completed_credit_order_creates_no_payment_entry(self):
        self.assertFalse(
            order_sync._should_treat_inbound_order_as_paid(
                "completed", "Credit", status_map={"is_paid": True}, is_historical=True
            )
        )

    def test_live_processing_credit_order_is_not_paid(self):
        self.assertFalse(
            order_sync._should_treat_inbound_order_as_paid(
                "processing", "Credit", status_map={"is_paid": False}, is_historical=False
            )
        )

    def test_kashier_classification_is_unchanged(self):
        self.assertTrue(
            order_sync._should_treat_inbound_order_as_paid(
                "processing", "Kashier Card", status_map={"is_paid": False}, is_historical=False
            )
        )


class TestCreditSurvivesAmendment(unittest.TestCase):
    """The replacement invoice is built from a Woo payload that never knew."""

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)
        self.monkeypatch.setattr(
            order_sync.frappe,
            "logger",
            lambda *args, **kwargs: SimpleNamespace(
                warning=lambda payload: None, info=lambda payload: None
            ),
        )

    def _patch_source(self, values: dict, missing_columns=(), settled=False):
        def _get_value(doctype, name, fieldname, *args, **kwargs):
            if fieldname in missing_columns:
                raise Exception(f"Unknown column {fieldname}")
            return values.get(fieldname)

        # Site-less runs have no ``frappe.db`` to patch attributes onto. Install a
        # stand-in rather than skipping: a skipped test that reads as coverage is
        # how this app ended up with 93 tests executing nothing.
        if getattr(order_sync.frappe, "db", None) is None:
            self.monkeypatch.setattr(
                order_sync.frappe, "db", SimpleNamespace(), raising=False
            )

        self.monkeypatch.setattr(
            order_sync.frappe.db, "get_value", _get_value, raising=False
        )
        self.monkeypatch.setattr(
            order_sync.frappe.db,
            "exists",
            lambda *args, **kwargs: "PER-0001" if settled else None,
            raising=False,
        )

    def test_credit_source_forces_credit_on_the_replacement(self):
        self._patch_source({"custom_payment_method": "Credit", "custom_credit_terms_days": 30})
        inv_data = {"custom_payment_method": "Cash"}

        order_sync._carry_credit_terms_from_source(inv_data, "ACC-SINV-00001", woo_id=14780)

        self.assertEqual(inv_data["custom_payment_method"], "Credit")
        self.assertEqual(inv_data["custom_credit_terms_days"], 30)

    def test_cash_source_is_left_alone(self):
        self._patch_source({"custom_payment_method": "Cash", "custom_credit_terms_days": 0})
        inv_data = {"custom_payment_method": "Cash"}

        order_sync._carry_credit_terms_from_source(inv_data, "ACC-SINV-00002", woo_id=14781)

        self.assertEqual(inv_data, {"custom_payment_method": "Cash"})

    def test_missing_credit_terms_column_does_not_raise(self):
        self._patch_source(
            {"custom_payment_method": "Credit"}, missing_columns=("custom_credit_terms_days",)
        )
        inv_data = {"custom_payment_method": "Cash"}

        order_sync._carry_credit_terms_from_source(inv_data, "ACC-SINV-00003", woo_id=14782)

        self.assertEqual(inv_data["custom_payment_method"], "Credit")
        self.assertNotIn("custom_credit_terms_days", inv_data)

    def test_an_already_settled_credit_source_keeps_the_old_paid_lane(self):
        # The receivable is closed, so there is nothing left to protect and the
        # paid-amendment lane must keep the method it can build a payment from.
        self._patch_source(
            {"custom_payment_method": "Credit", "custom_credit_terms_days": 30}, settled=True
        )
        inv_data = {"custom_payment_method": "Cash"}

        order_sync._carry_credit_terms_from_source(inv_data, "ACC-SINV-00004", woo_id=14784)

        self.assertEqual(inv_data, {"custom_payment_method": "Cash"})

    def test_no_source_is_a_no_op(self):
        inv_data = {"custom_payment_method": "Cash"}

        order_sync._carry_credit_terms_from_source(inv_data, None, woo_id=14783)

        self.assertEqual(inv_data, {"custom_payment_method": "Cash"})


if __name__ == "__main__":
    unittest.main()
