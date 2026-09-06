"""Payment classification and invoice-submit guards for the Woo order sync.

Written against pytest originally; converted to unittest because CI runs
``bench run-tests``, whose unittest discovery collects only TestCase subclasses.
As module-level functions these ten tests never ran. See tests/_monkeypatch.py.
"""

import unittest
from types import SimpleNamespace

from jarz_woocommerce_integration.services import order_sync
from jarz_woocommerce_integration.tests._monkeypatch import MonkeyPatch


class TestInboundOrderPaidClassification(unittest.TestCase):
    """Which Woo status + payment-method pairs count as already paid on arrival."""

    def test_should_treat_inbound_order_as_paid_for_live_kashier_processing(self):
        status_map = {"is_paid": False}

        assert order_sync._should_treat_inbound_order_as_paid(
            "processing", "Kashier Card", status_map=status_map, is_historical=False
        ) is True
        assert order_sync._should_treat_inbound_order_as_paid(
            "processing", "Kashier Wallet", status_map=status_map, is_historical=False
        ) is True
        assert order_sync._should_treat_inbound_order_as_paid(
            "processing", "Cash", status_map=status_map, is_historical=False
        ) is False

    def test_should_treat_inbound_order_as_paid_for_live_kashier_processing_aliases(self):
        status_map = {"is_paid": False}

        for status in (
            "pre-nasrcity",
            "pre-ismailia",
            "pre-hadayk",
            "pre-dokki",
        ):
            assert order_sync._should_treat_inbound_order_as_paid(
                status, "Kashier Card", status_map=status_map, is_historical=False
            ) is True
            assert order_sync._should_treat_inbound_order_as_paid(
                status, "Kashier Wallet", status_map=status_map, is_historical=False
            ) is True
            assert order_sync._should_treat_inbound_order_as_paid(
                status, "Cash", status_map=status_map, is_historical=False
            ) is False

    def test_reconcile_statuses_include_processing_aliases(self):
        statuses = set(order_sync.RECONCILE_ORDER_STATUSES.split(","))

        for status in (
            "pre-nasrcity",
            "pre-ismailia",
            "pre-hadayk",
            "pre-dokki",
        ):
            assert status in statuses

    def test_legacy_pre_hadayek_alias_normalizes_to_processing_behavior(self):
        status_map = {"is_paid": False}

        assert order_sync._normalize_woo_status("pre-hadayek") == "pre-hadayk"
        assert order_sync._is_processing_equivalent_woo_status("pre-hadayek") is True
        assert order_sync._should_treat_inbound_order_as_paid(
            "pre-hadayek",
            "Kashier Card",
            status_map=status_map,
            is_historical=False,
        ) is True

    def test_should_treat_inbound_order_as_paid_skips_non_payable_statuses(self):
        status_map = {"is_paid": False}

        assert order_sync._should_treat_inbound_order_as_paid(
            "cancelled", "Kashier Card", status_map=status_map, is_historical=False
        ) is False
        assert order_sync._should_treat_inbound_order_as_paid(
            "refunded", "Kashier Wallet", status_map=status_map, is_historical=False
        ) is False
        assert order_sync._should_treat_inbound_order_as_paid(
            "failed", "Kashier Card", status_map=status_map, is_historical=False
        ) is False


class TestPaymentEntryCreation(unittest.TestCase):
    """A paid Kashier order gets exactly one Payment Entry, and never a second."""

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)

    def test_maybe_create_payment_entry_for_invoice_creates_payment_for_processing_kashier(self):
        calls = []
        info_logs = []
        invoice = SimpleNamespace(name="ACC-SINV-TEST-001", docstatus=1)
        order = {"id": 14620, "status": "processing"}
        status_map = {"is_paid": False}

        self.monkeypatch.setattr(order_sync.frappe.db, "exists", lambda *args, **kwargs: None)
        self.monkeypatch.setattr(order_sync, "_resolve_posting_date", lambda order, is_historical: "2026-05-01")
        self.monkeypatch.setattr(
            order_sync,
            "_create_payment_entry",
            lambda invoice_name, payment_method, posting_date=None, cache=None: calls.append(
                {
                    "invoice_name": invoice_name,
                    "payment_method": payment_method,
                    "posting_date": posting_date,
                    "cache": cache,
                }
            ) or "ACC-PAY-TEST-001",
        )
        self.monkeypatch.setattr(order_sync.frappe, "logger", lambda: SimpleNamespace(info=lambda payload: info_logs.append(payload)))

        order_sync._maybe_create_payment_entry_for_invoice(
            invoice,
            order,
            status_map,
            "Kashier Card",
            "kashier_card",
            is_historical=False,
            cache=None,
            skip_payment_entry=False,
        )

        assert calls == [
            {
                "invoice_name": "ACC-SINV-TEST-001",
                "payment_method": "Kashier Card",
                "posting_date": "2026-05-01",
                "cache": None,
            }
        ]
        assert info_logs[0]["payment_entry"] == "ACC-PAY-TEST-001"

    def test_maybe_create_payment_entry_for_invoice_skips_duplicate_completed_kashier(self):
        calls = []
        invoice = SimpleNamespace(name="ACC-SINV-TEST-002", docstatus=1)
        order = {"id": 14621, "status": "completed"}
        status_map = {"is_paid": False}

        self.monkeypatch.setattr(order_sync.frappe.db, "exists", lambda *args, **kwargs: "PER-0001")
        self.monkeypatch.setattr(
            order_sync,
            "_create_payment_entry",
            lambda *args, **kwargs: calls.append({"args": args, "kwargs": kwargs}) or "ACC-PAY-TEST-002",
        )

        order_sync._maybe_create_payment_entry_for_invoice(
            invoice,
            order,
            status_map,
            "Kashier Wallet",
            "kashier_wallet",
            is_historical=False,
            cache=None,
            skip_payment_entry=False,
        )

        assert calls == []


class TestInvoiceSubmitGuards(unittest.TestCase):
    """is_pos is set only after submit, and a missing ledger is repaired or raised."""

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)

    def test_apply_invoice_pos_profile_sets_is_pos_only_after_submit(self):
        draft_invoice = SimpleNamespace(pos_profile=None, custom_kanban_profile=None, is_pos=0)

        order_sync._apply_invoice_pos_profile(draft_invoice, "Nasr city", submitted=False)

        assert draft_invoice.pos_profile == "Nasr city"
        assert draft_invoice.custom_kanban_profile == "Nasr city"
        assert draft_invoice.is_pos == 0

    def test_submit_invoice_with_accounting_guards_repairs_missing_ledgers(self):
        submit_calls = []
        repair_calls = []
        pos_profile_calls = []
        invoice = SimpleNamespace(
            name="ACC-SINV-TEST-003",
            flags=SimpleNamespace(ignore_permissions=False),
            submit=lambda: submit_calls.append("submit"),
            make_gl_entries=lambda: repair_calls.append("make_gl_entries"),
        )
        accounting_checks = iter([(False, False), (True, True)])

        self.monkeypatch.setattr(order_sync, "_get_invoice_accounting_flags", lambda invoice_name: next(accounting_checks))
        self.monkeypatch.setattr(
            order_sync,
            "_apply_invoice_pos_profile",
            lambda inv, pos_profile, submitted: pos_profile_calls.append(
                {"invoice": inv.name, "pos_profile": pos_profile, "submitted": submitted}
            ),
        )

        order_sync._submit_invoice_with_accounting_guards(invoice, pos_profile="Nasr city")

        assert submit_calls == ["submit"]
        assert repair_calls == ["make_gl_entries"]
        assert invoice.flags.ignore_permissions is True
        assert pos_profile_calls == [
            {"invoice": "ACC-SINV-TEST-003", "pos_profile": "Nasr city", "submitted": True}
        ]

    def test_submit_invoice_with_accounting_guards_raises_when_ledgers_stay_missing(self):
        """The repair itself blows up: the failure is logged, then submit throws.

        `make_gl_entries` has to actually raise here. `_ensure_submitted_invoice_accounting`
        logs "GL repair failed" from inside `except Exception`, so a fake that
        returns quietly asserts a log that the source never emits on that path --
        which is what this assertion did, unnoticed, for as long as the module
        went uncollected. The repair-succeeds branch is covered separately below.
        """
        submit_calls = []
        repair_calls = []
        logged_errors = []

        def failing_repair():
            repair_calls.append("make_gl_entries")
            raise RuntimeError("make_gl_entries could not post the ledger")

        invoice = SimpleNamespace(
            name="ACC-SINV-TEST-004",
            flags=SimpleNamespace(ignore_permissions=False),
            submit=lambda: submit_calls.append("submit"),
            make_gl_entries=failing_repair,
        )

        self.monkeypatch.setattr(order_sync, "_get_invoice_accounting_flags", lambda invoice_name: (False, False))
        self.monkeypatch.setattr(order_sync.frappe, "log_error", lambda message, title: logged_errors.append((message, title)))

        def fail_throw(message):
            raise RuntimeError(message)

        self.monkeypatch.setattr(order_sync.frappe, "throw", fail_throw)

        with self.assertRaisesRegex(RuntimeError, "required accounting entries"):
            order_sync._submit_invoice_with_accounting_guards(invoice, pos_profile="Nasr city")

        assert submit_calls == ["submit"]
        assert repair_calls == ["make_gl_entries"]
        assert invoice.flags.ignore_permissions is True
        assert logged_errors == [
            (
                "GL repair failed for ACC-SINV-TEST-004 after submit (gl=0, ple=0)",
                "GL Entry Repair Error",
            )
        ]

    def test_submit_invoice_guards_throw_without_logging_when_repair_returns_clean(self):
        """A repair that reports success but changes nothing still fails the submit.

        No "GL repair failed" log here -- nothing raised -- but the flags are
        re-read after the repair and are still missing, so the throw stands. This
        is the branch the sibling test above was accidentally exercising.
        """
        repair_calls = []
        logged_errors = []
        invoice = SimpleNamespace(
            name="ACC-SINV-TEST-005",
            flags=SimpleNamespace(ignore_permissions=False),
            submit=lambda: None,
            make_gl_entries=lambda: repair_calls.append("make_gl_entries"),
        )

        self.monkeypatch.setattr(order_sync, "_get_invoice_accounting_flags", lambda invoice_name: (False, False))
        self.monkeypatch.setattr(order_sync.frappe, "log_error", lambda message, title: logged_errors.append((message, title)))

        def fail_throw(message):
            raise RuntimeError(message)

        self.monkeypatch.setattr(order_sync.frappe, "throw", fail_throw)

        with self.assertRaisesRegex(RuntimeError, "required accounting entries"):
            order_sync._submit_invoice_with_accounting_guards(invoice, pos_profile="Nasr city")

        assert repair_calls == ["make_gl_entries"]
        assert logged_errors == []
