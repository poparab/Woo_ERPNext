from datetime import datetime, timezone
from types import SimpleNamespace

from jarz_woocommerce_integration.services import order_sync


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        base = cls(2026, 5, 1, 13, 51, 4, tzinfo=timezone.utc)
        if tz is None:
            return base.replace(tzinfo=None)
        return base.astimezone(tz)


def test_minutes_ago_for_woo_uses_real_utc(monkeypatch):
    monkeypatch.setattr(order_sync, "datetime", FrozenDateTime)

    assert order_sync._minutes_ago_for_woo(30) == "2026-05-01T13:21:04Z"


def test_pull_recent_orders_phase1_tracks_cursor_and_skip_reasons(monkeypatch):
    captured = {}
    orders = [
        {"id": 101, "date_modified_gmt": "2026-05-01T13:00:00", "status": "processing"},
        {"id": 102, "date_modified_gmt": "2026-05-01T13:05:00", "status": "processing"},
        {"id": 103, "date_modified_gmt": "2026-05-01T13:05:00", "status": "processing"},
    ]

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        def list_orders_with_meta(self, params=None):
            captured["params"] = dict(params or {})
            return orders, len(orders), 1

    settings = SimpleNamespace(
        base_url="https://example.com",
        consumer_key="ck_test",
        get_password=lambda fieldname: "cs_test",
    )

    def fake_process(order, settings, allow_update=True, is_historical=False, **kwargs):
        if order["id"] == 101:
            return {"status": "skipped", "reason": "already_mapped", "woo_order_id": 101}
        if order["id"] == 102:
            return {"status": "created", "woo_order_id": 102}
        return {"status": "error", "woo_order_id": 103}

    commit_calls = []

    monkeypatch.setattr(order_sync, "WooClient", DummyClient)
    monkeypatch.setattr(order_sync, "ensure_custom_fields", lambda: None)
    monkeypatch.setattr(order_sync.frappe, "get_single", lambda doctype: settings)
    monkeypatch.setattr(order_sync, "process_order_phase1", fake_process)
    monkeypatch.setattr(order_sync.frappe.db, "commit", lambda: commit_calls.append(True))

    metrics = order_sync.pull_recent_orders_phase1(
        limit=100,
        dry_run=False,
        modified_after="2026-05-01T12:00:00Z",
        orderby="modified",
        order="asc",
        max_pages=2,
    )

    assert captured["params"]["modified_after"] == "2026-05-01T12:00:00Z"
    assert captured["params"]["orderby"] == "modified"
    assert captured["params"]["order"] == "asc"
    assert metrics["created"] == 1
    assert metrics["errors"] == 1
    assert metrics["skipped"] == 1
    assert metrics["skip_reasons"] == {"already_mapped": 1}
    assert metrics["latest_seen_modified_gmt"] == "2026-05-01T13:05:00Z"
    assert metrics["latest_seen_order_id"] == 103
    assert len(commit_calls) == 2


def test_backfill_orders_by_ids_phase1_aggregates_statuses(monkeypatch):
    results = {
        "14620": {"status": "created", "woo_order_id": 14620},
        "14619": {"status": "updated", "woo_order_id": 14619},
        "14618": {"status": "error", "woo_order_id": 14618},
    }

    monkeypatch.setattr(
        order_sync,
        "pull_single_order_phase1",
        lambda order_id, dry_run=False, force=False, allow_update=True: results[str(order_id)],
    )

    summary = order_sync.backfill_orders_by_ids_phase1("14620,14619,14618")

    assert summary["requested"] == 3
    assert summary["processed"] == 3
    assert summary["created"] == 1
    assert summary["updated"] == 1
    assert summary["errors"] == 1
    assert summary["skipped"] == 0


def test_reconcile_recent_orders_phase1_uses_modified_after_window(monkeypatch):
    captured = {}
    settings = SimpleNamespace(
        order_reconcile_lookback_minutes=60,
        order_reconcile_max_pages=5,
    )

    monkeypatch.setattr(order_sync, "datetime", FrozenDateTime)
    monkeypatch.setattr(order_sync.frappe, "get_single", lambda doctype: settings)
    monkeypatch.setattr(order_sync, "ensure_custom_fields", lambda: None)

    def fake_pull_recent_orders_phase1(**kwargs):
        captured.update(kwargs)
        return {"orders_fetched": 0, "processed": 0, "errors": 0}

    monkeypatch.setattr(order_sync, "pull_recent_orders_phase1", fake_pull_recent_orders_phase1)

    result = order_sync.reconcile_recent_orders_phase1()

    assert captured["modified_after"] == "2026-05-01T12:51:04Z"
    assert captured["status"] == "any"
    assert captured["status_filter_set"] == set(order_sync.RECONCILE_TARGET_WOO_STATUSES)
    assert captured["orderby"] == "modified"
    assert captured["order"] == "asc"
    assert captured["max_pages"] == 5
    assert result["lookback_minutes"] == 60


def test_pull_recent_orders_phase1_filters_by_status_filter_set(monkeypatch):
    """status_filter_set drops orders whose status is not in the target set."""
    raw_orders = [
        {"id": 1, "date_modified_gmt": "2026-05-01T10:00:00", "status": "processing"},
        {"id": 2, "date_modified_gmt": "2026-05-01T10:01:00", "status": "pre-nasrcity"},
        {"id": 3, "date_modified_gmt": "2026-05-01T10:02:00", "status": "pending"},
        {"id": 4, "date_modified_gmt": "2026-05-01T10:03:00", "status": "on-hold"},
        {"id": 5, "date_modified_gmt": "2026-05-01T10:04:00", "status": "completed"},
    ]
    processed_ids = []
    target_set = {"processing", "pre-nasrcity", "completed"}

    class DummyClient:
        base_url = "https://woo.test"
        consumer_key = "ck"
        consumer_secret = "cs"

    def fake_list_orders_window(client, params, max_pages):
        return raw_orders, 1, 1

    def fake_process(order, settings, allow_update, is_historical):
        processed_ids.append(order["id"])
        return {"status": "created", "woo_order_id": order["id"]}

    settings = SimpleNamespace(
        base_url="https://woo.test",
        consumer_key="ck",
        get_password=lambda f: "cs",
    )

    monkeypatch.setattr(order_sync, "_list_orders_window", fake_list_orders_window)
    monkeypatch.setattr(order_sync, "process_order_phase1", fake_process)
    monkeypatch.setattr(order_sync, "ensure_custom_fields", lambda: None)
    monkeypatch.setattr(order_sync.frappe, "get_single", lambda doctype: settings)
    monkeypatch.setattr(order_sync.frappe.db, "commit", lambda: None)

    metrics = order_sync.pull_recent_orders_phase1(
        limit=10,
        status="any",
        max_pages=1,
        status_filter_set=target_set,
    )

    assert metrics["orders_fetched"] == 3
    assert metrics["orders_fetched_raw"] == 5
    assert metrics["filtered_out"] == 2
    assert set(processed_ids) == {1, 2, 5}


def test_reconcile_recent_orders_phase1_sends_any_to_woo(monkeypatch):
    """reconcile_recent_orders_phase1 must send status='any' to Woo, never the pre-* names."""
    captured = {}
    settings = SimpleNamespace(
        order_reconcile_lookback_minutes=60,
        order_reconcile_max_pages=5,
    )

    monkeypatch.setattr(order_sync, "datetime", FrozenDateTime)
    monkeypatch.setattr(order_sync.frappe, "get_single", lambda doctype: settings)
    monkeypatch.setattr(order_sync, "ensure_custom_fields", lambda: None)

    def fake_pull_recent_orders_phase1(**kwargs):
        captured.update(kwargs)
        return {"orders_fetched": 0, "processed": 0, "errors": 0}

    monkeypatch.setattr(order_sync, "pull_recent_orders_phase1", fake_pull_recent_orders_phase1)

    order_sync.reconcile_recent_orders_phase1()

    assert captured["status"] == "any", (
        "reconcile must send status=any to Woo, not the comma-joined pre-* list"
    )
    # Ensure no pre-* status names are sent to Woo API
    api_status = captured.get("status", "")
    for bad in ("pre-nasrcity", "pre-ismailia", "pre-hadayk", "pre-hadayek", "pre-dokki"):
        assert bad not in api_status, f"{bad!r} must not be sent to Woo API"
    # But all target statuses are covered by the client-side filter set
    assert captured["status_filter_set"] == set(order_sync.RECONCILE_TARGET_WOO_STATUSES)