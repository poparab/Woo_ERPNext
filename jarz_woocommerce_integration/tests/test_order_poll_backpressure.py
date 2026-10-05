"""The order poll must keep moving, and must not flood the worker queue.

Regression cover for the 2026-10-04 production slowdown: something on the store
touched all ~11,700 orders inside eight minutes. The live poll reads
``cursor - 15 min`` with a 600-order budget, so it re-read the same 600 orders
every 2 minutes for 16 hours without ever moving its cursor, and queued every
one of them again each time. The short queue sat at ~700 jobs (above Frappe's
550 cap), the box ran out of Lightsail CPU credits, and every app request slowed.
"""

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import mock

from jarz_woocommerce_integration.services import order_sync, sync_events

CURSOR = datetime(2026, 10, 4, 19, 27, 18, tzinfo=timezone.utc)


def _metrics(latest, pages_fetched=6, total_pages=118, max_pages=6):
    return {
        "pages_fetched": pages_fetched,
        "total_pages": total_pages,
        "max_pages": max_pages,
        "latest_seen_modified_gmt": latest,
        "latest_seen_order_id": 3130,
        "errors": 0,
        "queued": 0,
    }


class TestWindowSaturation(unittest.TestCase):
    def test_full_budget_with_nothing_past_the_cursor_is_saturated(self):
        assert order_sync._window_saturated_without_progress(_metrics("2026-10-04T19:27:18Z"), CURSOR)

    def test_progress_past_the_cursor_is_not_saturated(self):
        assert not order_sync._window_saturated_without_progress(_metrics("2026-10-04T19:27:19Z"), CURSOR)

    def test_a_window_that_fit_the_budget_is_not_saturated(self):
        metrics = _metrics("2026-10-04T19:27:18Z", pages_fetched=3, total_pages=3)
        assert not order_sync._window_saturated_without_progress(metrics, CURSOR)

    def test_last_page_reached_exactly_is_not_saturated(self):
        metrics = _metrics("2026-10-04T19:27:18Z", pages_fetched=6, total_pages=6)
        assert not order_sync._window_saturated_without_progress(metrics, CURSOR)


class TestCursorNeverMovesBackwards(unittest.TestCase):
    def _run(self, latest, latest_id=1):
        writes = []
        with mock.patch.object(order_sync, "_get_order_sync_cursor", return_value=(CURSOR, 3130)), \
                mock.patch.object(order_sync, "_set_order_sync_cursor",
                                  side_effect=lambda s, n, dt, oid: writes.append((dt, oid))):
            order_sync._update_order_sync_cursor_from_metrics(
                SimpleNamespace(), "live",
                {"latest_seen_modified_gmt": latest, "latest_seen_order_id": latest_id},
            )
        return writes[0]

    def test_overlap_only_read_keeps_the_cursor(self):
        assert self._run("2026-10-04T19:20:00Z") == (CURSOR, 3130)

    def test_lower_order_id_at_the_same_second_keeps_the_cursor(self):
        assert self._run("2026-10-04T19:27:18Z", latest_id=17) == (CURSOR, 3130)

    def test_newer_order_advances_the_cursor(self):
        dt, oid = self._run("2026-10-05T10:45:53Z", latest_id=17841)
        assert dt == datetime(2026, 10, 5, 10, 45, 53, tzinfo=timezone.utc)
        assert oid == 17841

    def test_empty_read_keeps_the_order_id(self):
        assert self._run(None, latest_id=0) == (None, 3130)


class TestRunReadsPastASaturatedOverlap(unittest.TestCase):
    def test_second_read_starts_exactly_at_the_cursor(self):
        windows = []

        def fake_window(**kwargs):
            windows.append(kwargs["modified_after"])
            if len(windows) == 1:
                return _metrics("2026-10-04T19:27:18Z")
            return _metrics("2026-10-05T10:45:53Z", pages_fetched=1, total_pages=1)

        settings = SimpleNamespace()
        with mock.patch.object(order_sync.frappe, "get_single", return_value=settings), \
                mock.patch.object(order_sync, "ensure_custom_fields"), \
                mock.patch.object(order_sync, "_get_setting_int", side_effect=lambda s, f, d: d), \
                mock.patch.object(order_sync, "_get_order_sync_cursor", return_value=(CURSOR, 3130)), \
                mock.patch.object(order_sync, "create_sync_log_entry"), \
                mock.patch.object(order_sync, "finish_sync_log_entry"), \
                mock.patch.object(order_sync, "_update_order_sync_cursor_from_metrics"), \
                mock.patch.object(order_sync, "_enqueue_order_window_events", side_effect=fake_window), \
                mock.patch.object(sync_events, "should_use_order_polling_events", return_value=True), \
                mock.patch.object(order_sync.frappe.db, "commit"), \
                mock.patch.object(order_sync.frappe, "logger"), \
                mock.patch.object(order_sync.frappe.utils, "now_datetime", return_value=datetime(2026, 10, 5, 14, 0)):
            result = order_sync._run_order_cursor_sync(
                cursor_name="live", operation="CronLive", event_name="e", error_event="ee",
                status=None, overlap_field="live_order_overlap_minutes", default_overlap_minutes=15,
                pages_field="live_order_max_pages", default_max_pages=6, bootstrap_lookback_minutes=60,
            )

        assert windows == ["2026-10-04T19:12:18Z", "2026-10-04T19:27:18Z"]
        assert result["overlap_saturated"] is True
        assert result["latest_seen_modified_gmt"] == "2026-10-05T10:45:53Z"

    def test_unsaturated_run_reads_once(self):
        calls = []

        def fake_window(**kwargs):
            calls.append(kwargs["modified_after"])
            return _metrics("2026-10-04T19:30:00Z", pages_fetched=1, total_pages=1)

        with mock.patch.object(order_sync.frappe, "get_single", return_value=SimpleNamespace()), \
                mock.patch.object(order_sync, "ensure_custom_fields"), \
                mock.patch.object(order_sync, "_get_setting_int", side_effect=lambda s, f, d: d), \
                mock.patch.object(order_sync, "_get_order_sync_cursor", return_value=(CURSOR, 3130)), \
                mock.patch.object(order_sync, "create_sync_log_entry"), \
                mock.patch.object(order_sync, "finish_sync_log_entry"), \
                mock.patch.object(order_sync, "_update_order_sync_cursor_from_metrics"), \
                mock.patch.object(order_sync, "_enqueue_order_window_events", side_effect=fake_window), \
                mock.patch.object(sync_events, "should_use_order_polling_events", return_value=True), \
                mock.patch.object(order_sync.frappe.db, "commit"), \
                mock.patch.object(order_sync.frappe, "logger"), \
                mock.patch.object(order_sync.frappe.utils, "now_datetime", return_value=datetime(2026, 10, 5, 14, 0)):
            result = order_sync._run_order_cursor_sync(
                cursor_name="live", operation="CronLive", event_name="e", error_event="ee",
                status=None, overlap_field="live_order_overlap_minutes", default_overlap_minutes=15,
                pages_field="live_order_max_pages", default_max_pages=6, bootstrap_lookback_minutes=60,
            )

        assert len(calls) == 1
        assert "overlap_saturated" not in result


class TestPollQueuesOnlyRealWork(unittest.TestCase):
    ORDERS = [
        {"id": 1750, "status": "completed", "date_modified_gmt": "2026-10-04T19:27:18"},  # untouched content
        {"id": 3130, "status": "completed", "date_modified_gmt": "2026-10-04T19:27:18"},  # event already done
        {"id": 17841, "status": "processing", "date_modified_gmt": "2026-10-05T10:45:53"},  # real new order
    ]

    def _run(self, enqueue_returns=True):
        events = {
            3130: SimpleNamespace(name="WOOEVT-1", status="Skipped"),
            17841: SimpleNamespace(name="WOOEVT-2", status="Pending"),
        }
        enqueued = []

        def fake_enqueue(name, after_commit=True):
            enqueued.append(name)
            return enqueue_returns

        settings = SimpleNamespace(base_url="https://woo.test", consumer_key="ck", get_password=lambda f: "cs")
        with mock.patch.object(order_sync, "ensure_custom_fields"), \
                mock.patch.object(order_sync, "_list_orders_window", return_value=(list(self.ORDERS), 1, 1)), \
                mock.patch.object(order_sync, "_polled_order_is_unchanged",
                                  side_effect=lambda o, **kw: o["id"] == 1750), \
                mock.patch.object(sync_events, "create_inbound_order_reference_event",
                                  side_effect=lambda **kw: events[kw["order_id"]]), \
                mock.patch.object(sync_events, "enqueue_sync_event", side_effect=fake_enqueue), \
                mock.patch.object(order_sync.frappe.db, "commit"):
            result = order_sync._enqueue_order_window_events(
                settings=settings, event_type="order_poll", limit=100, max_pages=6,
            )
        return result, enqueued

    def test_only_the_real_order_is_queued(self):
        result, enqueued = self._run()
        assert enqueued == ["WOOEVT-2"]
        assert (result["unchanged"], result["already_handled"], result["queued"], result["errors"]) == (1, 1, 1, 0)
        # The cursor still sees every order, including the skipped ones.
        assert result["latest_seen_order_id"] == 17841

    def test_full_queue_is_deferred_not_an_error(self):
        result, _ = self._run(enqueue_returns=False)
        assert result["deferred_to_sweeper"] == 1
        assert result["errors"] == 0
        assert result["queued"] == 0


class TestEnqueueSyncEvent(unittest.TestCase):
    def test_one_deduplicated_job_per_event(self):
        with mock.patch.object(sync_events.frappe.db, "get_value", return_value="Pending"), \
                mock.patch.object(sync_events.frappe, "enqueue") as enqueue:
            assert sync_events.enqueue_sync_event("WOOEVT-9", after_commit=False) is True
        kwargs = enqueue.call_args.kwargs
        assert kwargs["job_id"] == "woo-sync-event::WOOEVT-9"
        assert kwargs["deduplicate"] is True
        assert kwargs["event_name"] == "WOOEVT-9"

    def test_finished_event_is_never_queued(self):
        for status in sorted(sync_events.TERMINAL_STATUSES):
            with mock.patch.object(sync_events.frappe.db, "get_value", return_value=status), \
                    mock.patch.object(sync_events.frappe, "enqueue") as enqueue:
                assert sync_events.enqueue_sync_event("WOOEVT-9") is False
            enqueue.assert_not_called()

    def test_full_queue_returns_false_instead_of_raising(self):
        full = sync_events.frappe.ValidationError("Too many queued background jobs (550). Please retry after some time.")
        with mock.patch.object(sync_events.frappe.db, "get_value", return_value="Pending"), \
                mock.patch.object(sync_events.frappe, "enqueue", side_effect=full), \
                mock.patch.object(sync_events, "LOGGER"):
            assert sync_events.enqueue_sync_event("WOOEVT-9") is False

    def test_other_validation_errors_still_raise(self):
        with mock.patch.object(sync_events.frappe.db, "get_value", return_value="Pending"), \
                mock.patch.object(sync_events.frappe, "enqueue",
                                  side_effect=sync_events.frappe.ValidationError("something else")):
            with self.assertRaises(sync_events.frappe.ValidationError):
                sync_events.enqueue_sync_event("WOOEVT-9")


if __name__ == "__main__":
    unittest.main()
