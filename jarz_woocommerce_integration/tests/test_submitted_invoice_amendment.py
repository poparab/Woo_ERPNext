"""
WooCommerce Inbound Invoice Amendment
=====================================

Tests for the item-edit detection and amendment-orchestration flow introduced
in Phase 2 of the Woo-driven invoice amendment feature.

Coverage:
  - _compute_order_hash now includes per-line fingerprints, payment_method, shipping.
  - process_order_phase1: OFD permanent hard-lock (custom_was_out_for_delivery).
  - process_order_phase1: item-edit detection via hash comparison.
  - process_order_phase1: enqueue amendment job when enable_inbound_amendment=1
    and status is eligible.
  - process_order_phase1: flag for manual review when flag is off or status ineligible.
  - process_order_phase1: submitted_frozen still returned when hash is unchanged.
  - order_amendment.run_woo_amendment_job: guards (OFD, eligibility, status, depth,
    period-close, idempotency).
  - order_amendment.run_woo_amendment_job: happy-path success.
  - stamp_out_for_delivery_flag: sets custom_was_out_for_delivery permanently.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest

from jarz_woocommerce_integration.services import order_sync
from jarz_woocommerce_integration.services.order_sync import _compute_order_hash


# ---------------------------------------------------------------------------
# Test fixtures / helpers
# ---------------------------------------------------------------------------

def _make_woo_order(
    woo_id: int = 99001,
    status: str = "processing",
    total: str = "200.00",
    payment_method: str = "cod",
    line_items: list | None = None,
    shipping_lines: list | None = None,
    date_modified: str = "2026-06-01T10:00:00",
) -> dict:
    if line_items is None:
        line_items = [
            {
                "id": 1,
                "product_id": 501,
                "variation_id": 0,
                "quantity": 2,
                "total": "200.00",
                "subtotal": "200.00",
                "meta_data": [],
            }
        ]
    if shipping_lines is None:
        shipping_lines = [{"total": "20.00"}]
    return {
        "id": woo_id,
        "status": status,
        "number": str(woo_id),
        "currency": "EGP",
        "total": total,
        "payment_method": payment_method,
        "payment_method_title": "COD",
        "transaction_id": "",
        "line_items": line_items,
        "shipping_lines": shipping_lines,
        "fee_lines": [],
        "tax_lines": [],
        "billing": {"first_name": "Test", "postcode": "11511"},
        "shipping": {"first_name": "Test", "postcode": "11511"},
        "meta_data": [],
        "date_modified": date_modified,
        "date_created": "2026-05-20T09:00:00",
    }


def _make_settings(enable_amendment: int = 0):
    return SimpleNamespace(
        base_url="https://example.com",
        consumer_key="ck_test",
        name="WooCommerce Settings",
        get_password=lambda fieldname: "cs_test",
        default_warehouse="Main Warehouse - TC",
        default_company="_Test Company",
        default_currency="EGP",
        default_pos_profile=None,
        default_selling_price_list=None,
        enable_inbound_amendment=enable_amendment,
    )


def _make_fake_inv(
    woo_id: int = 99001,
    docstatus: int = 1,
    was_ofd: int = 0,
    inv_state: str = "New",
    name: str = "ACC-SINV-99001",
) -> SimpleNamespace:
    inv = SimpleNamespace(
        doctype="Sales Invoice",
        name=name,
        docstatus=docstatus,
        woo_order_id=woo_id,
        woo_order_number=str(woo_id),
        custom_was_out_for_delivery=was_ofd,
        custom_sales_invoice_state=inv_state,
        pos_profile="Main POS",
        custom_kanban_profile="Main POS",
        customer="Test Customer",
        company="_Test Company",
        posting_date="2026-06-01",
        flags=SimpleNamespace(),
    )
    inv.set = lambda field, val: None
    inv.get = lambda field, default=None: getattr(inv, field, default)
    inv.append = lambda field, val: None
    inv.save = MagicMock()
    inv.db_set = MagicMock()
    inv.cancel = MagicMock()
    inv.add_comment = MagicMock()
    return inv


def _setup_submitted_mocks(monkeypatch, *, fake_inv, stored_hash: str = ""):
    """Patch frappe I/O to reach the submitted_frozen / amendment detection branch."""
    inv_name = fake_inv.name
    sync_log_inserts: list[dict] = []

    fake_lock = MagicMock()
    fake_lock.acquire.return_value = True
    monkeypatch.setattr(order_sync, "get_redis_conn", lambda: MagicMock(lock=lambda *a, **kw: fake_lock))

    def _fake_sql(query, values=None, *args, **kwargs):
        return [[1]]

    monkeypatch.setattr(order_sync.frappe.db, "sql", _fake_sql)
    monkeypatch.setattr(order_sync.frappe.db, "get_table_columns", lambda t: ["name", "erpnext_sales_invoice", "hash"])

    # Order Map with stored hash
    def _fake_db_get_value(doctype, filters_or_name=None, fieldname=None, *args, **kwargs):
        if doctype == "WooCommerce Order Map":
            if isinstance(fieldname, list):
                return SimpleNamespace(name="WOOMAP-00001", erpnext_sales_invoice=inv_name, hash=stored_hash)
            return stored_hash
        return None

    monkeypatch.setattr(order_sync.frappe.db, "get_value", _fake_db_get_value)
    monkeypatch.setattr(order_sync.frappe.db, "get_all", lambda *a, **kw: [])
    monkeypatch.setattr(order_sync.frappe.db, "set_value", MagicMock())

    def _fake_get_all(doctype, filters=None, fields=None, *args, **kwargs):
        if doctype == "Sales Invoice":
            return [{"name": inv_name, "creation": "2026-01-01"}]
        return []

    monkeypatch.setattr(order_sync.frappe, "get_all", _fake_get_all)

    def _fake_get_doc(doctype_or_dict, name=None, *args, **kwargs):
        if isinstance(doctype_or_dict, dict):
            dt = doctype_or_dict.get("doctype")
            if dt == "WooCommerce Sync Log":
                sync_log_inserts.append(doctype_or_dict)
                doc = MagicMock()
                doc.insert = MagicMock()
                return doc
            return MagicMock()
        if doctype_or_dict == "Sales Invoice":
            return fake_inv
        return MagicMock()

    monkeypatch.setattr(order_sync.frappe, "get_doc", _fake_get_doc)
    monkeypatch.setattr(order_sync.frappe, "enqueue", MagicMock())
    monkeypatch.setattr(order_sync, "_check_and_repair_submitted_invoice_drift", lambda *a, **kw: None)
    monkeypatch.setattr(order_sync, "_flag_order_map_for_manual_review", MagicMock())
    monkeypatch.setattr(order_sync.frappe.utils, "now_datetime", lambda: "2026-06-01 12:00:00")

    return sync_log_inserts


# ---------------------------------------------------------------------------
# _compute_order_hash
# ---------------------------------------------------------------------------

class TestComputeOrderHash:
    def test_same_order_same_hash(self):
        order = _make_woo_order()
        assert _compute_order_hash(order) == _compute_order_hash(order)

    def test_different_quantity_different_hash(self):
        order_a = _make_woo_order(line_items=[
            {"product_id": 1, "variation_id": 0, "quantity": 1, "total": "100.00", "subtotal": "100.00"}
        ])
        order_b = _make_woo_order(line_items=[
            {"product_id": 1, "variation_id": 0, "quantity": 2, "total": "200.00", "subtotal": "200.00"}
        ])
        assert _compute_order_hash(order_a) != _compute_order_hash(order_b)

    def test_different_product_different_hash(self):
        order_a = _make_woo_order(line_items=[
            {"product_id": 1, "variation_id": 0, "quantity": 1, "total": "100.00", "subtotal": "100.00"}
        ])
        order_b = _make_woo_order(line_items=[
            {"product_id": 2, "variation_id": 0, "quantity": 1, "total": "100.00", "subtotal": "100.00"}
        ])
        assert _compute_order_hash(order_a) != _compute_order_hash(order_b)

    def test_different_payment_method_different_hash(self):
        order_a = _make_woo_order(payment_method="cod")
        order_b = _make_woo_order(payment_method="instapay")
        assert _compute_order_hash(order_a) != _compute_order_hash(order_b)

    def test_different_shipping_total_different_hash(self):
        order_a = _make_woo_order(shipping_lines=[{"total": "20.00"}])
        order_b = _make_woo_order(shipping_lines=[{"total": "30.00"}])
        assert _compute_order_hash(order_a) != _compute_order_hash(order_b)

    def test_line_item_order_invariant(self):
        """Hash must be the same regardless of line item order in the Woo payload."""
        lines_a = [
            {"product_id": 1, "variation_id": 0, "quantity": 1, "total": "50.00", "subtotal": "50.00"},
            {"product_id": 2, "variation_id": 0, "quantity": 2, "total": "100.00", "subtotal": "100.00"},
        ]
        lines_b = list(reversed(lines_a))
        assert _compute_order_hash(_make_woo_order(line_items=lines_a)) == _compute_order_hash(
            _make_woo_order(line_items=lines_b)
        )

    def test_returns_hex_string(self):
        h = _compute_order_hash(_make_woo_order())
        assert isinstance(h, str) and len(h) == 64


# ---------------------------------------------------------------------------
# process_order_phase1 — OFD hard-lock
# ---------------------------------------------------------------------------

class TestOFDHardLock:
    def test_was_ofd_flag_blocks(self, monkeypatch):
        fake_inv = _make_fake_inv(was_ofd=1, inv_state="Delivered")
        _setup_submitted_mocks(monkeypatch, fake_inv=fake_inv, stored_hash="oldhash")
        result = order_sync.process_order_phase1(
            _make_woo_order(), _make_settings(enable_amendment=1)
        )
        assert result["status"] == "skipped"
        assert result["reason"] == "out_for_delivery_locked"

    def test_live_ofd_state_blocks(self, monkeypatch):
        fake_inv = _make_fake_inv(was_ofd=0, inv_state="Out for Delivery")
        _setup_submitted_mocks(monkeypatch, fake_inv=fake_inv, stored_hash="oldhash")
        result = order_sync.process_order_phase1(
            _make_woo_order(), _make_settings(enable_amendment=1)
        )
        assert result["status"] == "skipped"
        assert result["reason"] == "out_for_delivery_locked"

    def test_non_ofd_state_proceeds_to_hash_check(self, monkeypatch):
        order = _make_woo_order()
        current_hash = _compute_order_hash(order)
        fake_inv = _make_fake_inv(was_ofd=0, inv_state="New")
        _setup_submitted_mocks(monkeypatch, fake_inv=fake_inv, stored_hash=current_hash)
        result = order_sync.process_order_phase1(order, _make_settings())
        # hash unchanged → submitted_frozen
        assert result["reason"] == "submitted_frozen"


# ---------------------------------------------------------------------------
# process_order_phase1 — item-edit detection
# ---------------------------------------------------------------------------

class TestItemEditDetection:
    def test_hash_unchanged_returns_submitted_frozen(self, monkeypatch):
        order = _make_woo_order()
        current_hash = _compute_order_hash(order)
        fake_inv = _make_fake_inv()
        _setup_submitted_mocks(monkeypatch, fake_inv=fake_inv, stored_hash=current_hash)
        result = order_sync.process_order_phase1(order, _make_settings())
        assert result["status"] == "skipped"
        assert result["reason"] == "submitted_frozen"

    def test_hash_changed_flag_off_returns_needs_manual_review(self, monkeypatch):
        order = _make_woo_order()
        fake_inv = _make_fake_inv()
        logs = _setup_submitted_mocks(monkeypatch, fake_inv=fake_inv, stored_hash="oldhash")
        result = order_sync.process_order_phase1(order, _make_settings(enable_amendment=0))
        assert result["status"] == "skipped"
        assert result["reason"] == "needs_manual_review"
        logged_ops = [log.get("operation") for log in logs]
        assert "ItemEditDetected" in logged_ops

    def test_hash_changed_flag_on_eligible_status_enqueues(self, monkeypatch):
        order = _make_woo_order(status="processing")
        fake_inv = _make_fake_inv()
        logs = _setup_submitted_mocks(monkeypatch, fake_inv=fake_inv, stored_hash="oldhash")
        result = order_sync.process_order_phase1(order, _make_settings(enable_amendment=1))
        assert result["status"] == "queued"
        assert result["reason"] == "amendment_enqueued"
        order_sync.frappe.enqueue.assert_called_once()
        call_kwargs = order_sync.frappe.enqueue.call_args
        assert "order_amendment.run_woo_amendment_job" in call_kwargs[0][0]

    def test_hash_changed_flag_on_ineligible_status_flags_manual_review(self, monkeypatch):
        order = _make_woo_order(status="completed")
        fake_inv = _make_fake_inv()
        _setup_submitted_mocks(monkeypatch, fake_inv=fake_inv, stored_hash="oldhash")
        result = order_sync.process_order_phase1(order, _make_settings(enable_amendment=1))
        assert result["status"] == "skipped"
        assert result["reason"] == "needs_manual_review"
        order_sync._flag_order_map_for_manual_review.assert_called_once()

    def test_on_hold_status_also_enqueues(self, monkeypatch):
        order = _make_woo_order(status="on-hold")
        fake_inv = _make_fake_inv()
        _setup_submitted_mocks(monkeypatch, fake_inv=fake_inv, stored_hash="oldhash")
        result = order_sync.process_order_phase1(order, _make_settings(enable_amendment=1))
        assert result["status"] == "queued"


# ---------------------------------------------------------------------------
# run_woo_amendment_job — unit guards (pure mocking, no Frappe DB)
# ---------------------------------------------------------------------------

class TestRunWooAmendmentJobGuards:
    """Test the guard logic in run_woo_amendment_job using a monkeypatched frappe."""

    def _patch_frappe(self, monkeypatch, *, order_map_row=None, source_si=None,
                       sql_returns=None, eligibility=None,
                       amend_depth=0, period_block=None):
        import jarz_woocommerce_integration.services.order_amendment as oa

        # SQL advisory locks: always grant unless overridden
        sql_sequence = sql_returns or [[[1]], [[1]]]
        sql_call_count = [0]

        def _fake_sql(query, values=None, *a, **kw):
            idx = min(sql_call_count[0], len(sql_sequence) - 1)
            sql_call_count[0] += 1
            return sql_sequence[idx]

        monkeypatch.setattr(oa.frappe.db, "sql", _fake_sql)
        monkeypatch.setattr(oa.frappe.db, "get_value", lambda *a, **kw: order_map_row or None)
        monkeypatch.setattr(oa.frappe.db, "count", lambda *a, **kw: amend_depth)
        monkeypatch.setattr(oa.frappe.db, "set_value", MagicMock())

        def _fake_get_single(name):
            return SimpleNamespace(name=name, enable_inbound_amendment=1)

        monkeypatch.setattr(oa.frappe, "get_single", _fake_get_single)

        if source_si is not None:
            monkeypatch.setattr(oa.frappe, "get_doc", lambda dt, name: source_si)

        if eligibility is not None:
            monkeypatch.setattr(
                oa,
                "get_invoice_amendment_eligibility",  # patching oa.get_invoice_amendment_eligibility
                lambda inv: eligibility,
                raising=False,
            )

        if period_block is not None:
            monkeypatch.setattr(oa, "_check_period_closed", lambda inv: period_block, raising=False)
        else:
            monkeypatch.setattr(oa, "_check_period_closed", lambda inv: None, raising=False)

        monkeypatch.setattr(oa, "_flag_needs_review", MagicMock())
        monkeypatch.setattr(oa, "_write_sync_log", MagicMock())
        monkeypatch.setattr(oa, "_resolve_link_field", lambda: "erpnext_sales_invoice")

        return oa

    def test_advisory_lock_fail_returns_skipped(self, monkeypatch):
        import jarz_woocommerce_integration.services.order_amendment as oa

        def _fail_sql(query, values=None, *a, **kw):
            return [[0]]

        monkeypatch.setattr(oa.frappe.db, "sql", _fail_sql)
        monkeypatch.setattr(oa, "_write_sync_log", MagicMock())
        result = oa.run_woo_amendment_job(99001, _make_woo_order(), "WooCommerce Settings")
        assert result["status"] == "skipped"
        assert result["reason"] == "locked"

    def test_no_order_map_returns_skipped(self, monkeypatch):
        oa = self._patch_frappe(monkeypatch, order_map_row=None)
        result = oa.run_woo_amendment_job(99001, _make_woo_order(), "WooCommerce Settings")
        assert result["status"] == "skipped"
        assert result["reason"] == "no_order_map"

    def test_ofd_hard_lock_blocks(self, monkeypatch):
        order_map_row = SimpleNamespace(
            name="WOOMAP-00001",
            erpnext_sales_invoice="ACC-SINV-99001",
            hash="oldhash",
        )
        source_si = _make_fake_inv(was_ofd=1, inv_state="Delivered")
        oa = self._patch_frappe(monkeypatch, order_map_row=order_map_row, source_si=source_si)
        result = oa.run_woo_amendment_job(99001, _make_woo_order(), "WooCommerce Settings")
        assert result["status"] == "skipped"
        assert result["reason"] == "out_for_delivery_locked"

    def test_eligibility_block_flags_review(self, monkeypatch):
        order_map_row = SimpleNamespace(
            name="WOOMAP-00001",
            erpnext_sales_invoice="ACC-SINV-99001",
            hash="oldhash",
        )
        source_si = _make_fake_inv()
        eligibility = {
            "can_amend": False,
            "amendment_block_code": "delivery_trip_exists",
            "amendment_block_reason": "Invoice is on a delivery trip",
        }
        oa = self._patch_frappe(
            monkeypatch,
            order_map_row=order_map_row,
            source_si=source_si,
            eligibility=eligibility,
        )
        with patch(
            "jarz_woocommerce_integration.services.order_amendment.get_invoice_amendment_eligibility",
            return_value=eligibility,
        ):
            result = oa.run_woo_amendment_job(99001, _make_woo_order(), "WooCommerce Settings")
        assert result["status"] == "skipped"
        assert result["reason"] == "eligibility_blocked"

    def test_ineligible_woo_status_flags_review(self, monkeypatch):
        order_map_row = SimpleNamespace(
            name="WOOMAP-00001",
            erpnext_sales_invoice="ACC-SINV-99001",
            hash="oldhash",
        )
        source_si = _make_fake_inv()
        eligibility = {"can_amend": True, "amendment_block_code": None, "amendment_block_reason": None}
        oa = self._patch_frappe(
            monkeypatch,
            order_map_row=order_map_row,
            source_si=source_si,
            eligibility=eligibility,
        )
        with patch(
            "jarz_woocommerce_integration.services.order_amendment.get_invoice_amendment_eligibility",
            return_value=eligibility,
        ):
            result = oa.run_woo_amendment_job(
                99001, _make_woo_order(status="completed"), "WooCommerce Settings"
            )
        assert result["status"] == "skipped"
        assert result["reason"] == "woo_status_not_eligible"

    def test_amend_depth_exceeded_flags_review(self, monkeypatch):
        order_map_row = SimpleNamespace(
            name="WOOMAP-00001",
            erpnext_sales_invoice="ACC-SINV-99001",
            hash="oldhash",
        )
        source_si = _make_fake_inv()
        eligibility = {"can_amend": True, "amendment_block_code": None, "amendment_block_reason": None}
        oa = self._patch_frappe(
            monkeypatch,
            order_map_row=order_map_row,
            source_si=source_si,
            eligibility=eligibility,
            amend_depth=3,
        )
        with patch(
            "jarz_woocommerce_integration.services.order_amendment.get_invoice_amendment_eligibility",
            return_value=eligibility,
        ):
            result = oa.run_woo_amendment_job(99001, _make_woo_order(), "WooCommerce Settings")
        assert result["status"] == "skipped"
        assert result["reason"] == "amend_depth_exceeded"

    def test_period_closed_flags_review(self, monkeypatch):
        order_map_row = SimpleNamespace(
            name="WOOMAP-00001",
            erpnext_sales_invoice="ACC-SINV-99001",
            hash="oldhash",
        )
        source_si = _make_fake_inv()
        eligibility = {"can_amend": True, "amendment_block_code": None, "amendment_block_reason": None}
        oa = self._patch_frappe(
            monkeypatch,
            order_map_row=order_map_row,
            source_si=source_si,
            eligibility=eligibility,
            period_block="Accounting Period 'May-2026' is closed",
        )
        with patch(
            "jarz_woocommerce_integration.services.order_amendment.get_invoice_amendment_eligibility",
            return_value=eligibility,
        ):
            result = oa.run_woo_amendment_job(99001, _make_woo_order(), "WooCommerce Settings")
        assert result["status"] == "skipped"
        assert result["reason"] == "period_closed"


# ---------------------------------------------------------------------------
# stamp_out_for_delivery_flag
# ---------------------------------------------------------------------------

class TestStampOutForDeliveryFlag:
    def test_stamps_flag_on_first_ofd_transition(self):
        from jarz_pos.events.sales_invoice import stamp_out_for_delivery_flag

        db_set_calls = []

        class FakeMeta:
            def get_field(self, fieldname):
                return True if fieldname == "custom_was_out_for_delivery" else None

        fake_frappe = SimpleNamespace(
            get_meta=lambda dt: FakeMeta(),
            db=SimpleNamespace(set_value=lambda *a, **kw: db_set_calls.append((a, kw))),
        )

        doc = SimpleNamespace(
            name="ACC-SINV-00042",
            custom_was_out_for_delivery=0,
            custom_sales_invoice_state="Out for Delivery",
            sales_invoice_state=None,
        )

        import jarz_pos.events.sales_invoice as si_events
        original_frappe = si_events.frappe
        si_events.frappe = fake_frappe
        try:
            stamp_out_for_delivery_flag(doc)
        finally:
            si_events.frappe = original_frappe

        assert db_set_calls, "set_value should have been called"
        assert doc.custom_was_out_for_delivery == 1

    def test_does_not_stamp_when_already_flagged(self):
        from jarz_pos.events.sales_invoice import stamp_out_for_delivery_flag

        db_set_calls = []

        class FakeMeta:
            def get_field(self, fieldname):
                return True

        fake_frappe = SimpleNamespace(
            get_meta=lambda dt: FakeMeta(),
            db=SimpleNamespace(set_value=lambda *a, **kw: db_set_calls.append((a, kw))),
        )

        doc = SimpleNamespace(
            name="ACC-SINV-00042",
            custom_was_out_for_delivery=1,
            custom_sales_invoice_state="Out for Delivery",
            sales_invoice_state=None,
        )

        import jarz_pos.events.sales_invoice as si_events
        original_frappe = si_events.frappe
        si_events.frappe = fake_frappe
        try:
            stamp_out_for_delivery_flag(doc)
        finally:
            si_events.frappe = original_frappe

        assert not db_set_calls, "set_value should NOT be called when flag is already 1"

    def test_does_not_stamp_for_non_ofd_state(self):
        from jarz_pos.events.sales_invoice import stamp_out_for_delivery_flag

        db_set_calls = []

        class FakeMeta:
            def get_field(self, fieldname):
                return True

        fake_frappe = SimpleNamespace(
            get_meta=lambda dt: FakeMeta(),
            db=SimpleNamespace(set_value=lambda *a, **kw: db_set_calls.append((a, kw))),
        )

        doc = SimpleNamespace(
            name="ACC-SINV-00042",
            custom_was_out_for_delivery=0,
            custom_sales_invoice_state="Delivered",
            sales_invoice_state=None,
        )

        import jarz_pos.events.sales_invoice as si_events
        original_frappe = si_events.frappe
        si_events.frappe = fake_frappe
        try:
            stamp_out_for_delivery_flag(doc)
        finally:
            si_events.frappe = original_frappe

        assert not db_set_calls
