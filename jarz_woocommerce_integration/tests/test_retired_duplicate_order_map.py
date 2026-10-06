"""Retired-duplicate Order Map rows (``order_map_status``) and their cleanup tool.

The 2026-10-06 outbound race left extra Woo orders whose Order Map rows link the
REAL invoice (WOOMAP-13274 -> Woo 17863 and WOOMAP-13275 -> Woo 17865, both on
ACC-SINV-2026-18733). Cancelling such a duplicate on the store must never reach
that invoice. These tests pin every path that would otherwise act on it, plus
``order_maintenance.retire_duplicate_woo_order``.

Pure unit tests: ``frappe.db``, the Woo client and the sync log are fakes.
"""

import unittest
import unittest.mock
from datetime import datetime
from types import SimpleNamespace

from jarz_woocommerce_integration.services import (
    cancellation_reconcile,
    geo_passthrough,
    order_amendment,
    order_maintenance,
    order_sync,
    outbound_sync,
    sync_events,
)
from jarz_woocommerce_integration.services.order_map_status import (
    RETIRED_DUPLICATE_MAP_STATUS,
    RETIRED_DUPLICATE_REASON,
)

INVOICE = "ACC-SINV-2026-18733"
RETIRED_ROW = {
    "name": "WOOMAP-13274",
    "woo_order_id": 17863,
    "erpnext_sales_invoice": INVOICE,
    "hash": "dup-hash",
    "status": RETIRED_DUPLICATE_MAP_STATUS,
}


class _DummyLock:
    def __init__(self):
        self.released = False

    def acquire(self, blocking=False):
        return True

    def release(self):
        self.released = True


def _lock_sql(log):
    def fake_sql(query, params=None, as_dict=False):
        log.append(" ".join(str(query).split()))
        if "GET_LOCK" in query or "RELEASE_LOCK" in query:
            return [(1,)]
        raise AssertionError(f"unexpected SQL: {query}")

    return fake_sql


class TestInboundSkipsRetiredRows(unittest.TestCase):
    def test_process_order_phase1_skips_before_any_write(self):
        sql_log = []
        redis_lock = _DummyLock()
        db = SimpleNamespace(
            sql=_lock_sql(sql_log),
            get_table_columns=lambda doctype: ["erpnext_sales_invoice"],
            get_value=lambda *args, **kwargs: dict(RETIRED_ROW),
            set_value=unittest.mock.MagicMock(),
            commit=unittest.mock.MagicMock(),
        )
        get_all = unittest.mock.MagicMock(return_value=[])
        get_doc = unittest.mock.MagicMock()
        delete_doc = unittest.mock.MagicMock()
        ensure_customer = unittest.mock.MagicMock()
        sync_log = unittest.mock.MagicMock()

        with unittest.mock.patch.object(order_sync, "get_redis_conn", return_value=SimpleNamespace(lock=lambda *a, **k: redis_lock)), \
             unittest.mock.patch.object(order_sync.frappe, "db", db), \
             unittest.mock.patch.object(order_sync.frappe, "get_all", get_all), \
             unittest.mock.patch.object(order_sync.frappe, "get_doc", get_doc), \
             unittest.mock.patch.object(order_sync.frappe, "delete_doc", delete_doc), \
             unittest.mock.patch.object(order_sync, "ensure_customer_with_addresses", ensure_customer), \
             unittest.mock.patch.object(order_sync, "create_sync_log_entry", sync_log):
            result = order_sync.process_order_phase1(
                {"id": 17863, "status": "cancelled", "line_items": []}, SimpleNamespace()
            )

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], RETIRED_DUPLICATE_REASON)
        self.assertIs(result["success"], True)
        # Nothing touched: no SI lookup, no map write (the marker survives), no cancel.
        get_all.assert_not_called()
        get_doc.assert_not_called()
        delete_doc.assert_not_called()
        db.set_value.assert_not_called()
        ensure_customer.assert_not_called()
        # One sync-log line, and both per-order locks released.
        sync_log.assert_called_once()
        self.assertEqual(sync_log.call_args.args[:2], ("InboundSkip", "Skipped"))
        self.assertTrue(redis_lock.released)
        self.assertTrue(any("RELEASE_LOCK" in q for q in sql_log))

    def test_retired_skip_counts_as_success_everywhere(self):
        self.assertIn(RETIRED_DUPLICATE_REASON, order_sync.SKIPPED_SUCCESS_REASONS)

        updates = []
        event_doc = SimpleNamespace(
            name="WOOEVT-1",
            direction="Inbound",
            object_type="Order",
            attempt_count=1,
            max_attempts=8,
            db_set=lambda values, update_modified=False: updates.append(values),
        )
        with unittest.mock.patch.object(sync_events, "now_datetime", return_value=datetime(2026, 10, 7, 9, 0)):
            outcome = sync_events._apply_inbound_result(
                event_doc,
                {"status": "skipped", "reason": RETIRED_DUPLICATE_REASON, "success": True},
            )
        # "retired_duplicate" contains the RETRY token "duplicate"; it must classify as skip.
        self.assertEqual(outcome["status"], "skipped")
        self.assertEqual(updates[-1]["status"], "Skipped")

    def test_pull_single_order_counts_it_as_success_and_keeps_the_row_on_force(self):
        settings = SimpleNamespace(base_url="https://example.com", consumer_key="ck", get_password=lambda f: "cs")

        class Client:
            def __init__(self, *args, **kwargs):
                pass

            def get_order(self, order_id):
                return {"id": 17863, "status": "cancelled"}

        delete_doc = unittest.mock.MagicMock()
        with unittest.mock.patch.object(order_sync, "WooClient", Client), \
             unittest.mock.patch.object(order_sync, "ensure_custom_fields", lambda: None), \
             unittest.mock.patch.object(order_sync.frappe, "get_single", return_value=settings), \
             unittest.mock.patch.object(order_sync.frappe.db, "get_value", return_value=dict(RETIRED_ROW)), \
             unittest.mock.patch.object(order_sync.frappe, "delete_doc", delete_doc), \
             unittest.mock.patch.object(
                 order_sync, "process_order_phase1",
                 return_value={"status": "skipped", "reason": RETIRED_DUPLICATE_REASON, "woo_order_id": 17863},
             ):
            result = order_sync.pull_single_order_phase1(17863, force=True, allow_update=False)

        delete_doc.assert_not_called()
        self.assertIs(result["success"], True)

    def test_polled_order_with_retired_row_creates_no_event(self):
        with unittest.mock.patch.object(order_sync.frappe.db, "get_table_columns", return_value=["erpnext_sales_invoice"]), \
             unittest.mock.patch.object(order_sync.frappe.db, "get_value", return_value=dict(RETIRED_ROW)):
            self.assertTrue(order_sync._polled_order_is_unchanged({"id": 17863, "status": "cancelled"}))

    def test_contact_refresh_never_writes_the_duplicate_onto_the_invoice(self):
        settings = SimpleNamespace(base_url="https://example.com", consumer_key="ck", get_password=lambda f: "cs")

        class Client:
            def __init__(self, *args, **kwargs):
                pass

            def get_order(self, order_id):
                return {"id": 17863, "status": "cancelled", "billing": {}, "shipping": {}}

        set_value = unittest.mock.MagicMock()
        with unittest.mock.patch.object(order_sync, "WooClient", Client), \
             unittest.mock.patch.object(order_sync, "ensure_custom_fields", lambda: None), \
             unittest.mock.patch.object(order_sync.frappe, "get_single", return_value=settings), \
             unittest.mock.patch.object(order_sync.frappe.db, "get_table_columns", return_value=["erpnext_sales_invoice"]), \
             unittest.mock.patch.object(order_sync.frappe.db, "get_value", return_value=dict(RETIRED_ROW)), \
             unittest.mock.patch.object(order_sync.frappe.db, "set_value", set_value):
            result = order_sync.refresh_order_contact_snapshot(17863)

        self.assertEqual(result["reason"], RETIRED_DUPLICATE_REASON)
        set_value.assert_not_called()

    def test_deleted_order_sweep_never_probes_or_cancels_through_a_retired_row(self):
        probed = []

        class Client:
            def __init__(self, *args, **kwargs):
                pass

            def get(self, path):
                probed.append(path)
                return {"status": "completed"}

        rows = [
            dict(RETIRED_ROW),
            {"name": "WOOMAP-13000", "woo_order_id": 17000, "erpnext_sales_invoice": "ACC-SINV-2026-17000", "status": "completed"},
        ]
        settings = SimpleNamespace(base_url="https://example.com", consumer_key="ck", api_version="v3", get_password=lambda f: "cs")
        terminal = unittest.mock.MagicMock()

        with unittest.mock.patch.object(order_sync.frappe.db, "get_single_value", return_value=1), \
             unittest.mock.patch.object(order_sync.frappe.utils, "now_datetime", return_value=datetime(2026, 10, 7, 9, 0)), \
             unittest.mock.patch.object(order_sync.frappe.utils, "add_days", side_effect=lambda value, days: value), \
             unittest.mock.patch.object(order_sync.frappe, "get_single", return_value=settings), \
             unittest.mock.patch.object(order_sync.frappe, "get_all", return_value=rows), \
             unittest.mock.patch.object(order_sync, "WooClient", Client), \
             unittest.mock.patch.object(order_sync, "_handle_terminal_status_on_submitted_invoice", terminal), \
             unittest.mock.patch.object(order_sync, "create_sync_log_entry"):
            summary = order_sync.reconcile_deleted_orders_cron()

        self.assertEqual(summary["errors"], 0)
        self.assertEqual(summary["probed"], 1)
        self.assertEqual(probed, ["orders/17000"])
        terminal.assert_not_called()

    def test_woo_amendment_job_never_amends_the_real_invoice_from_a_duplicate(self):
        sql_log = []
        db = SimpleNamespace(
            sql=_lock_sql(sql_log),
            get_table_columns=lambda doctype: ["erpnext_sales_invoice"],
            get_value=lambda *args, **kwargs: dict(RETIRED_ROW),
        )
        get_doc = unittest.mock.MagicMock()

        with unittest.mock.patch.object(order_amendment.frappe, "db", db), \
             unittest.mock.patch.object(order_amendment.frappe, "get_single", return_value=SimpleNamespace(enable_inbound_amendment=1)), \
             unittest.mock.patch.object(order_amendment.frappe, "get_doc", get_doc):
            result = order_amendment.run_woo_amendment_job(17863, {"id": 17863, "line_items": []})

        self.assertEqual(result["reason"], RETIRED_DUPLICATE_REASON)
        get_doc.assert_not_called()
        self.assertTrue(any("RELEASE_LOCK" in q for q in sql_log))

    def test_geo_pins_are_not_applied_through_a_retired_row(self):
        with unittest.mock.patch.object(geo_passthrough, "_order_map_link_field", return_value="erpnext_sales_invoice"), \
             unittest.mock.patch.object(geo_passthrough.frappe.db, "get_value", return_value=dict(RETIRED_ROW)):
            self.assertEqual(geo_passthrough.resolve_invoice_addresses(17863), (None, None, None))

    def test_cancellation_reconcile_does_not_match_through_a_retired_row(self):
        calls = []

        def fake_sql(query, params=None, as_dict=False):
            calls.append((" ".join(query.split()), params))
            return []

        with unittest.mock.patch.object(cancellation_reconcile, "_resolve_order_map_link_field", return_value="erpnext_sales_invoice"), \
             unittest.mock.patch.object(cancellation_reconcile.frappe.db, "sql", side_effect=fake_sql):
            cancellation_reconcile._load_invoice_matches([17863])

        mapped_query, mapped_params = calls[1]
        self.assertIn("IFNULL(wm.status, '') != %s", mapped_query)
        self.assertEqual(mapped_params[-1], RETIRED_DUPLICATE_MAP_STATUS)
        self.assertEqual(mapped_query.count("%s"), len(mapped_params))


class TestOutboundIgnoresRetiredRows(unittest.TestCase):
    def test_amended_recovery_never_returns_a_retired_order(self):
        invoice = SimpleNamespace(amended_from="ACC-SINV-2026-18733", get=lambda f, d=None: None)
        rows = [
            {"woo_order_id": 17863, "status": RETIRED_DUPLICATE_MAP_STATUS},
            {"woo_order_id": 17864, "status": "completed"},
        ]
        with unittest.mock.patch.object(outbound_sync, "_resolve_order_map_link_field", return_value="erpnext_sales_invoice"), \
             unittest.mock.patch.object(outbound_sync.frappe.db, "get_value", return_value=None), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_all", return_value=rows):
            self.assertEqual(outbound_sync._recover_amended_invoice_woo_order_id(invoice), 17864)

        with unittest.mock.patch.object(outbound_sync, "_resolve_order_map_link_field", return_value="erpnext_sales_invoice"), \
             unittest.mock.patch.object(outbound_sync.frappe.db, "get_value", return_value=None), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_all", return_value=rows[:1]):
            self.assertIsNone(outbound_sync._recover_amended_invoice_woo_order_id(invoice))

    def test_handover_never_moves_a_retired_row(self):
        def fake_get_value(doctype, filters, fieldname=None, as_dict=False):
            if doctype == "Sales Invoice":
                return 17863
            return dict(RETIRED_ROW)

        set_value = unittest.mock.MagicMock()
        with unittest.mock.patch.object(outbound_sync, "_resolve_order_map_link_field", return_value="erpnext_sales_invoice"), \
             unittest.mock.patch.object(outbound_sync, "now_datetime", return_value="2026-10-07 09:00:00"), \
             unittest.mock.patch.object(outbound_sync.frappe.db, "get_value", side_effect=fake_get_value), \
             unittest.mock.patch.object(outbound_sync.frappe.db, "set_value", set_value):
            outbound_sync._handover_woo_order_to_replacement(
                woo_order_id=17863, source_invoice=INVOICE, replacement_invoice="ACC-SINV-2026-18733-1"
            )

        self.assertTrue(all(call.args[0] != "WooCommerce Order Map" for call in set_value.call_args_list))


class _MaintenanceDB:
    def __init__(self, log, *, invoice_woo_order_id=17864, duplicate_status="processing"):
        self.log = log
        self.invoice_woo_order_id = invoice_woo_order_id
        self.duplicate_status = duplicate_status

    def get_value(self, doctype, filters, fieldname=None, as_dict=False):
        if doctype == "Sales Invoice":
            return {"name": INVOICE, "woo_order_id": self.invoice_woo_order_id}
        woo_id = filters.get("woo_order_id")
        if woo_id == 17863:
            return {"name": "WOOMAP-13274", "erpnext_sales_invoice": INVOICE, "status": self.duplicate_status}
        if woo_id == 17864:
            return {"name": "WOOMAP-13273", "erpnext_sales_invoice": INVOICE, "status": "completed"}
        return None

    def set_value(self, doctype, name, values, update_modified=False):
        self.log.append(("set_value", doctype, name, dict(values)))

    def commit(self):
        self.log.append(("commit",))


class TestRetireDuplicateWooOrder(unittest.TestCase):
    def _run(self, *, apply, **db_kwargs):
        log = []
        db = _MaintenanceDB(log, **db_kwargs)

        class Client:
            def put(self, path, payload):
                log.append(("put", path, dict(payload)))
                return {"id": 17863, "status": "cancelled"}

        def fake_note(client, woo_order_id, body):
            log.append(("note", woo_order_id, body))
            return "posted"

        with unittest.mock.patch.object(order_maintenance.frappe, "db", db), \
             unittest.mock.patch.object(outbound_sync, "_resolve_order_map_link_field", return_value="erpnext_sales_invoice"), \
             unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), None)), \
             unittest.mock.patch.object(outbound_sync, "_build_client", return_value=Client()), \
             unittest.mock.patch.object(outbound_sync, "_mark_outbound_push_in_flight", side_effect=lambda woo_id: log.append(("echo", woo_id))), \
             unittest.mock.patch.object(outbound_sync, "_post_woo_order_note", side_effect=fake_note):
            result = order_maintenance.retire_duplicate_woo_order(17863, 17864, apply=apply)
        return result, log

    def test_dry_run_returns_the_plan_and_touches_nothing(self):
        result, log = self._run(apply=False)

        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(result["order_map"], "WOOMAP-13274")
        self.assertEqual(result["invoice"], INVOICE)
        self.assertEqual(len(result["plan"]), 4)
        self.assertEqual(log, [])

    def test_apply_marks_and_commits_before_touching_the_store(self):
        result, log = self._run(apply=True)

        self.assertEqual(result["status"], "applied")
        self.assertEqual([entry[0] for entry in log], ["set_value", "commit", "echo", "put", "note"])
        _, doctype, name, values = log[0]
        self.assertEqual((doctype, name), ("WooCommerce Order Map", "WOOMAP-13274"))
        self.assertEqual(values["status"], RETIRED_DUPLICATE_MAP_STATUS)
        self.assertEqual(values["needs_manual_review"], 0)
        self.assertEqual(values["manual_review_reason"], "Duplicate of #17864 created by concurrent outbound push")
        self.assertEqual(log[3][1:], ("orders/17863", {"status": "cancelled"}))
        self.assertIn("#17864", log[4][2])
        self.assertIn("not a real order", log[4][2])

    def test_rerun_on_a_retired_row_does_not_rewrite_it(self):
        result, log = self._run(apply=True, duplicate_status=RETIRED_DUPLICATE_MAP_STATUS)

        self.assertTrue(result["already_retired"])
        self.assertNotIn("set_value", [entry[0] for entry in log])
        self.assertEqual([entry[0] for entry in log], ["commit", "echo", "put", "note"])

    def test_refuses_when_the_invoice_keeps_a_different_order(self):
        result, log = self._run(apply=True, invoice_woo_order_id=17865)

        self.assertEqual(result["status"], "refused")
        self.assertEqual(result["reason"], "invoice_does_not_keep_that_order")
        self.assertEqual(log, [])

    def test_refuses_to_retire_the_kept_order_itself(self):
        result = order_maintenance.retire_duplicate_woo_order(17864, 17864, apply=True)

        self.assertEqual(result["status"], "refused")
        self.assertEqual(result["reason"], "duplicate_equals_keep")


if __name__ == "__main__":
    unittest.main()
