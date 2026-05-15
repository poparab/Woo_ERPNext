"""
PROD-WOO-001: Submitted Sales Invoice Freeze
=============================================

Tests that process_order_phase1 and pull_single_order_phase1 respect the
"submitted_frozen" contract: once an SI is submitted (docstatus=1) no inbound
Woo update may mutate it — no save, no db_set, no cancel.  All changes from
ERPNext submission onwards flow outbound via outbound_sync.py.

Draft SIs (docstatus=0) keep the current full update behaviour.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

from jarz_woocommerce_integration.services import order_sync


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_woo_order(woo_id: int = 14763, status: str = "completed") -> dict:
    return {
        "id": woo_id,
        "status": status,
        "number": str(woo_id),
        "currency": "EGP",
        "total": "150.00",
        "payment_method": "cod",
        "payment_method_title": "Cash on Delivery",
        "transaction_id": "",
        "customer_note": "",
        "line_items": [
            {
                "id": 1,
                "product_id": 101,
                "variation_id": 0,
                "sku": "SKU-001",
                "name": "Product 1",
                "quantity": 1,
                "price": "150.00",
                "total": "150.00",
                "meta_data": [],
            }
        ],
        "fee_lines": [],
        "tax_lines": [],
        "billing": {
            "first_name": "Ahmed",
            "last_name": "Test",
            "address_1": "123 Main St",
            "city": "Cairo",
            "country": "EG",
            "email": "ahmed@test.com",
            "phone": "01234567890",
        },
        "shipping": {
            "first_name": "Ahmed",
            "last_name": "Test",
            "address_1": "123 Main St",
            "city": "Cairo",
            "country": "EG",
        },
        "meta_data": [],
        "date_created_gmt": "2026-01-01T00:00:00",
        "date_modified_gmt": "2026-01-02T00:00:00",
    }


def _make_settings():
    return SimpleNamespace(
        base_url="https://example.com",
        consumer_key="ck_test",
        get_password=lambda fieldname: "cs_test",
        default_warehouse="Main Warehouse - TC",
        default_company="_Test Company",
        default_currency="EGP",
        default_pos_profile=None,
        default_selling_price_list=None,
    )


def _setup_common_mocks(monkeypatch, *, si_docstatus: int, woo_id: int = 14763):
    """
    Patch all frappe I/O needed to reach the candidate_doc branch inside
    process_order_phase1.  Returns (sync_log_inserts, fake_inv).

    ``sync_log_inserts`` collects every dict passed to frappe.get_doc when the
    doctype is "WooCommerce Sync Log", letting tests assert on logged entries.
    ``fake_inv`` is the stub Sales Invoice; tests can inspect it after the call.
    """
    inv_name = "ACC-SINV-00001"

    fake_inv = SimpleNamespace(
        doctype="Sales Invoice",
        name=inv_name,
        docstatus=si_docstatus,
        woo_order_id=woo_id,
        woo_order_number=str(woo_id),
        # Extra attributes needed when docstatus=0 path runs past the guard
        selling_price_list=None,
        ignore_pricing_rule=0,
        posting_date=None,
        set_posting_time=0,
        customer_address=None,
        shipping_address_name=None,
        custom_delivery_date=None,
        custom_delivery_time_from=None,
        custom_delivery_duration=None,
        custom_payment_method=None,
        custom_acceptance_status=None,
        custom_sales_invoice_state=None,
        flags=SimpleNamespace(),
    )
    fake_inv.set = lambda field, val: None
    fake_inv.get = lambda field, default=None: [] if field == "taxes" else default
    fake_inv.append = lambda field, val: None
    fake_inv.save = MagicMock()
    fake_inv.db_set = MagicMock()
    fake_inv.cancel = MagicMock()

    sync_log_inserts: list[dict] = []

    # Redis lock — always acquired
    fake_lock = MagicMock()
    fake_lock.acquire.return_value = True
    fake_lock.release.return_value = None
    monkeypatch.setattr(order_sync, "get_redis_conn", lambda: MagicMock(lock=lambda *a, **kw: fake_lock))

    # DB advisory lock — always acquired; RELEASE_LOCK is a no-op
    def _fake_sql(query, values=None, *args, **kwargs):
        if "GET_LOCK" in (query or ""):
            return [[1]]
        return []

    monkeypatch.setattr(order_sync.frappe.db, "sql", _fake_sql)

    # LINK_FIELD column resolution
    monkeypatch.setattr(
        order_sync.frappe.db,
        "get_table_columns",
        lambda table: ["name", "erpnext_sales_invoice", "hash", "status"],
    )

    # No existing WooCommerce Order Map → will be created fresh
    monkeypatch.setattr(order_sync.frappe.db, "get_value", lambda *a, **kw: None)

    # SI already exists in ERPNext
    def _fake_get_all(doctype, filters=None, fields=None, *args, **kwargs):
        if doctype == "Sales Invoice":
            return [{"name": inv_name, "creation": "2026-01-01 00:00:00"}]
        return []

    monkeypatch.setattr(order_sync.frappe, "get_all", _fake_get_all)

    # frappe.get_doc dispatch
    def _fake_get_doc(doctype_or_dict, name=None, *args, **kwargs):
        if isinstance(doctype_or_dict, dict):
            dt = doctype_or_dict.get("doctype")
            if dt == "WooCommerce Sync Log":
                doc = MagicMock()
                doc.name = "LOG-00001"
                doc.insert = MagicMock(return_value=doc)
                sync_log_inserts.append(dict(doctype_or_dict))
                return doc
            if dt == "WooCommerce Order Map":
                doc = MagicMock()
                doc.name = "WOO-MAP-00001"
                doc.insert = MagicMock(return_value=doc)
                doc.save = MagicMock()
                doc.update = MagicMock()
                return doc
            return MagicMock()
        if doctype_or_dict == "Sales Invoice" and name == inv_name:
            return fake_inv
        if doctype_or_dict == "WooCommerce Order Map":
            doc = MagicMock()
            doc.name = "WOO-MAP-00001"
            doc.save = MagicMock()
            doc.update = MagicMock()
            return doc
        return MagicMock()

    monkeypatch.setattr(order_sync.frappe, "get_doc", _fake_get_doc)

    monkeypatch.setattr(order_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False))
    monkeypatch.setattr(order_sync.frappe.db, "commit", lambda: None)
    monkeypatch.setattr(order_sync.frappe.utils, "now_datetime", lambda: "2026-05-12 12:00:00")

    monkeypatch.setattr(
        order_sync,
        "ensure_customer_with_addresses",
        lambda order, settings, **kwargs: ("Customer 1", "Billing-001", "Shipping-001"),
    )
    monkeypatch.setattr(
        order_sync,
        "_build_invoice_items",
        lambda order, price_list=None, cache=None, is_historical=False: (
            [{"item_code": "ITEM-001", "qty": 1, "rate": 150.0, "amount": 150.0}],
            [],
            {},
        ),
    )

    return sync_log_inserts, fake_inv


# ---------------------------------------------------------------------------
# Tests — submitted SI is frozen
# ---------------------------------------------------------------------------

def test_submitted_si_returns_skipped_submitted_frozen(monkeypatch):
    """
    When process_order_phase1 encounters an existing submitted SI (docstatus=1),
    it must immediately return {status: skipped, reason: submitted_frozen}.
    No save, db_set, or cancel may be called.
    """
    sync_log_calls, fake_inv = _setup_common_mocks(monkeypatch, si_docstatus=1)
    order = _make_woo_order(woo_id=14763, status="completed")

    result = order_sync.process_order_phase1(order, _make_settings(), allow_update=True)

    assert result["status"] == "skipped"
    assert result["reason"] == "submitted_frozen"
    assert result["invoice"] == "ACC-SINV-00001"
    assert result["woo_order_id"] == 14763

    # No accounting mutations
    fake_inv.save.assert_not_called()
    fake_inv.db_set.assert_not_called()
    fake_inv.cancel.assert_not_called()


def test_submitted_si_freeze_writes_sync_log_entry(monkeypatch):
    """
    The guard must write a WooCommerce Sync Log entry with status=Skipped and
    a message containing 'submitted_frozen' for finance observability.
    """
    sync_log_calls, _ = _setup_common_mocks(monkeypatch, si_docstatus=1)
    order = _make_woo_order(woo_id=14763, status="completed")

    order_sync.process_order_phase1(order, _make_settings(), allow_update=True)

    frozen_entries = [
        e for e in sync_log_calls
        if e.get("status") == "Skipped" and "submitted_frozen" in (e.get("message") or "")
    ]
    assert frozen_entries, (
        "Expected at least one WooCommerce Sync Log entry with status='Skipped' "
        "and 'submitted_frozen' in the message"
    )
    assert frozen_entries[0].get("woo_order_id") == 14763


def test_woo_cancellation_of_submitted_si_is_blocked(monkeypatch):
    """
    Even when the Woo order status is 'cancelled', a submitted SI must not be
    cancelled by inbound sync (docstatus remains 1; cancel() is never called).
    """
    sync_log_calls, fake_inv = _setup_common_mocks(monkeypatch, si_docstatus=1)
    order = _make_woo_order(woo_id=14763, status="cancelled")

    result = order_sync.process_order_phase1(order, _make_settings(), allow_update=True)

    assert result["status"] == "skipped"
    assert result["reason"] == "submitted_frozen"
    assert fake_inv.docstatus == 1, "SI docstatus must remain 1 — cancel() must not have been called"
    fake_inv.cancel.assert_not_called()


def test_woo_refund_of_submitted_si_is_blocked(monkeypatch):
    """Refunded Woo status must also be blocked from cancelling a submitted SI."""
    _, fake_inv = _setup_common_mocks(monkeypatch, si_docstatus=1)
    order = _make_woo_order(woo_id=14763, status="refunded")

    result = order_sync.process_order_phase1(order, _make_settings(), allow_update=True)

    assert result["reason"] == "submitted_frozen"
    fake_inv.cancel.assert_not_called()


# ---------------------------------------------------------------------------
# Regression — draft SI still flows through the update path
# ---------------------------------------------------------------------------

def test_draft_si_is_not_frozen(monkeypatch):
    """
    A draft SI (docstatus=0) must NOT be frozen — the guard must be a no-op
    and the function must NOT return submitted_frozen.
    """
    sync_log_calls, fake_inv = _setup_common_mocks(monkeypatch, si_docstatus=0)

    # Stub out deeper frappe operations that run past the guard for drafts
    monkeypatch.setattr(
        order_sync,
        "_apply_delivery_charge_policy",
        lambda inv, territory_name=None, has_free_shipping_bundle=False, cache=None: {"changed": False},
    )
    monkeypatch.setattr(order_sync, "_apply_invoice_pos_profile", lambda *a, **kw: None)
    monkeypatch.setattr(order_sync, "_submit_invoice_with_accounting_guards", lambda *a, **kw: None)
    monkeypatch.setattr(order_sync.frappe.db, "exists", lambda *a, **kw: None)

    order = _make_woo_order(woo_id=14763, status="processing")

    result = order_sync.process_order_phase1(order, _make_settings(), allow_update=True)

    assert result.get("reason") != "submitted_frozen", (
        "Draft SI must not be frozen — submitted_frozen guard must only fire for docstatus=1"
    )
    assert not any(
        "submitted_frozen" in (e.get("message") or "") for e in sync_log_calls
    ), "No submitted_frozen sync log entry should be written for a draft SI"


# ---------------------------------------------------------------------------
# pull_single_order_phase1 — submitted_frozen treated as success
# ---------------------------------------------------------------------------

def test_submitted_frozen_is_success_in_pull_single(monkeypatch):
    """
    pull_single_order_phase1 must classify submitted_frozen as a successful skip
    (success=True) so it does not inflate error counters in cron/reconcile stats.
    """
    frozen_result = {
        "status": "skipped",
        "reason": "submitted_frozen",
        "woo_order_id": 14763,
        "invoice": "ACC-SINV-00001",
    }
    monkeypatch.setattr(order_sync, "process_order_phase1", lambda *a, **kw: frozen_result)

    fake_order = _make_woo_order(woo_id=14763)
    settings = _make_settings()

    class _DummyClient:
        def __init__(self, *a, **kw):
            pass

        def get_order(self, order_id):
            return fake_order

    monkeypatch.setattr(order_sync, "WooClient", _DummyClient)
    monkeypatch.setattr(order_sync.frappe, "get_single", lambda doctype: settings)
    monkeypatch.setattr(order_sync, "ensure_custom_fields", lambda: None)

    result = order_sync.pull_single_order_phase1(14763)

    assert result["success"] is True, (
        "submitted_frozen must be treated as success=True so it does not appear as an error"
    )
    assert result["status"] == "skipped"
    assert result["reason"] == "submitted_frozen"
