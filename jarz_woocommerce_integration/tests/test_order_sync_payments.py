from types import SimpleNamespace

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