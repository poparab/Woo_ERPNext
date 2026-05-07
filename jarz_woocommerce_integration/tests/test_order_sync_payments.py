from types import SimpleNamespace

import pytest

from jarz_woocommerce_integration.services import order_sync


def test_should_treat_inbound_order_as_paid_for_live_kashier_processing():
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


def test_should_treat_inbound_order_as_paid_for_live_kashier_processing_aliases():
    status_map = {"is_paid": False}

    for status in (
        "pre-nasrcity",
        "pre-ismailia",
        "pre-hadayk",
        "pre-hadayek",
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


def test_reconcile_statuses_include_processing_aliases():
    statuses = set(order_sync.RECONCILE_ORDER_STATUSES.split(","))

    for status in (
        "pre-nasrcity",
        "pre-ismailia",
        "pre-hadayk",
        "pre-hadayek",
        "pre-dokki",
    ):
        assert status in statuses


def test_should_treat_inbound_order_as_paid_skips_non_payable_statuses():
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


def test_maybe_create_payment_entry_for_invoice_creates_payment_for_processing_kashier(monkeypatch):
    calls = []
    info_logs = []
    invoice = SimpleNamespace(name="ACC-SINV-TEST-001", docstatus=1)
    order = {"id": 14620, "status": "processing"}
    status_map = {"is_paid": False}

    monkeypatch.setattr(order_sync.frappe.db, "exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(order_sync, "_resolve_posting_date", lambda order, is_historical: "2026-05-01")
    monkeypatch.setattr(
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
    monkeypatch.setattr(order_sync.frappe, "logger", lambda: SimpleNamespace(info=lambda payload: info_logs.append(payload)))

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


def test_maybe_create_payment_entry_for_invoice_skips_duplicate_completed_kashier(monkeypatch):
    calls = []
    invoice = SimpleNamespace(name="ACC-SINV-TEST-002", docstatus=1)
    order = {"id": 14621, "status": "completed"}
    status_map = {"is_paid": False}

    monkeypatch.setattr(order_sync.frappe.db, "exists", lambda *args, **kwargs: "PER-0001")
    monkeypatch.setattr(
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


def test_apply_invoice_pos_profile_sets_is_pos_only_after_submit():
    draft_invoice = SimpleNamespace(pos_profile=None, custom_kanban_profile=None, is_pos=0)

    order_sync._apply_invoice_pos_profile(draft_invoice, "Nasr city", submitted=False)

    assert draft_invoice.pos_profile == "Nasr city"
    assert draft_invoice.custom_kanban_profile == "Nasr city"
    assert draft_invoice.is_pos == 0


def test_submit_invoice_with_accounting_guards_repairs_missing_ledgers(monkeypatch):
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

    monkeypatch.setattr(order_sync, "_get_invoice_accounting_flags", lambda invoice_name: next(accounting_checks))
    monkeypatch.setattr(
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


def test_submit_invoice_with_accounting_guards_raises_when_ledgers_stay_missing(monkeypatch):
    submit_calls = []
    repair_calls = []
    logged_errors = []
    invoice = SimpleNamespace(
        name="ACC-SINV-TEST-004",
        flags=SimpleNamespace(ignore_permissions=False),
        submit=lambda: submit_calls.append("submit"),
        make_gl_entries=lambda: repair_calls.append("make_gl_entries"),
    )

    monkeypatch.setattr(order_sync, "_get_invoice_accounting_flags", lambda invoice_name: (False, False))
    monkeypatch.setattr(order_sync.frappe, "log_error", lambda message, title: logged_errors.append((message, title)))

    def fail_throw(message):
        raise RuntimeError(message)

    monkeypatch.setattr(order_sync.frappe, "throw", fail_throw)

    with pytest.raises(RuntimeError, match="required accounting entries"):
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