from datetime import datetime, timezone
from types import SimpleNamespace

import frappe

from jarz_woocommerce_integration.services import order_sync


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        base = cls(2026, 5, 1, 13, 51, 4, tzinfo=timezone.utc)
        if tz is None:
            return base.replace(tzinfo=None)
        return base.astimezone(tz)


def _patch_customer_failure_path(monkeypatch, exc):
    class DummyLock:
        def acquire(self, blocking=False):
            return True

        def release(self):
            return None

    class DummyRedis:
        def lock(self, *args, **kwargs):
            return DummyLock()

    def fake_sql(query, params=None, as_dict=False):
        del params, as_dict
        if "GET_LOCK" in query:
            return [(1,)]
        if "RELEASE_LOCK" in query:
            return [(1,)]
        raise AssertionError(query)

    monkeypatch.setattr(order_sync, "get_redis_conn", lambda: DummyRedis())
    monkeypatch.setattr(order_sync.frappe.db, "sql", fake_sql)
    monkeypatch.setattr(order_sync.frappe.db, "get_table_columns", lambda doctype: ["erpnext_sales_invoice"])
    monkeypatch.setattr(order_sync.frappe.db, "get_value", lambda *args, **kwargs: None)
    monkeypatch.setattr(order_sync.frappe, "get_all", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        order_sync,
        "ensure_customer_with_addresses",
        lambda *args, **kwargs: (_ for _ in ()).throw(exc),
    )


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


def test_pull_single_order_phase1_locked_skip_is_not_success(monkeypatch):
    settings = SimpleNamespace(
        base_url="https://example.com",
        consumer_key="ck_test",
        get_password=lambda fieldname: "cs_test",
    )
    order = {"id": 14763, "status": "processing", "line_items": []}

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_order(self, order_id):
            assert order_id == 14763
            return dict(order)

    monkeypatch.setattr(order_sync, "WooClient", DummyClient)
    monkeypatch.setattr(order_sync, "ensure_custom_fields", lambda: None)
    monkeypatch.setattr(order_sync.frappe, "get_single", lambda doctype: settings)
    monkeypatch.setattr(
        order_sync,
        "process_order_phase1",
        lambda *args, **kwargs: {"status": "skipped", "reason": "locked", "woo_order_id": 14763},
    )

    result = order_sync.pull_single_order_phase1(14763)

    assert result["status"] == "skipped"
    assert result["reason"] == "locked"
    assert result["success"] is False


def test_process_order_phase1_normalizes_duplicate_customer_error(monkeypatch):
    duplicate = frappe.UniqueValidationError(
        "Customer",
        "CUST-0042",
        Exception("Duplicate entry 'CUST-0042' for key 'PRIMARY'"),
    )
    _patch_customer_failure_path(monkeypatch, duplicate)

    result = order_sync.process_order_phase1(
        {"id": 15010, "status": "processing", "billing": {}, "shipping": {}},
        SimpleNamespace(),
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "customer_error:duplicate_key_race"
    assert "Duplicate entry" in result["detail"]


def test_process_order_phase1_normalizes_internal_customer_error(monkeypatch):
    _patch_customer_failure_path(monkeypatch, RuntimeError("boom"))

    result = order_sync.process_order_phase1(
        {"id": 15011, "status": "processing", "billing": {}, "shipping": {}},
        SimpleNamespace(),
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "customer_error:internal_error"
    assert result["detail"] == "boom"


def test_refresh_order_contact_snapshot_updates_invoice_and_map(monkeypatch):
    settings = SimpleNamespace(
        base_url="https://example.com",
        consumer_key="ck_test",
        get_password=lambda fieldname: "cs_test",
    )
    order = {
        "id": 14504,
        "number": "14504",
        "status": "processing",
        "currency": "EGP",
        "total": "250.00",
        "payment_method": "cod",
        "customer_id": 991,
        "customer_email": "billing@example.com",
        "billing": {
            "first_name": "Account",
            "last_name": "Holder",
            "email": "billing@example.com",
        },
        "shipping": {
            "first_name": "Delivery",
            "last_name": "Recipient",
            "phone": "01001234567",
        },
    }
    updates = []
    commit_calls = []

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_order(self, order_id):
            assert str(order_id) == "14504"
            return dict(order)

    def fake_get_value(doctype, name_or_filters, fieldname=None, as_dict=False):
        if doctype == "WooCommerce Order Map" and name_or_filters == {"woo_order_id": 14504}:
            return {"name": "WOOMAP-0001", "erpnext_sales_invoice": "ACC-SINV-0001"}
        if doctype == "Sales Invoice" and name_or_filters == "ACC-SINV-0001":
            current = {field: "" for field in fieldname}
            current["customer_name"] = "Canonical Customer"
            return current
        if doctype == "WooCommerce Order Map" and name_or_filters == "WOOMAP-0001":
            current = {field: "" for field in fieldname}
            current["woo_order_number"] = "14504"
            current["erpnext_sales_invoice"] = "ACC-SINV-0001"
            return current
        raise AssertionError((doctype, name_or_filters, fieldname, as_dict))

    def fake_set_value(doctype, name, values, update_modified=False):
        updates.append((doctype, name, dict(values), update_modified))

    monkeypatch.setattr(order_sync, "WooClient", DummyClient)
    monkeypatch.setattr(order_sync, "ensure_custom_fields", lambda: None)
    monkeypatch.setattr(order_sync.frappe, "get_single", lambda doctype: settings)
    monkeypatch.setattr(order_sync.frappe.db, "get_value", fake_get_value)
    monkeypatch.setattr(order_sync.frappe.db, "set_value", fake_set_value)
    monkeypatch.setattr(order_sync.frappe.db, "commit", lambda: commit_calls.append(True))
    monkeypatch.setattr(order_sync.frappe.db, "get_table_columns", lambda doctype: ["erpnext_sales_invoice"])
    monkeypatch.setattr(order_sync.frappe.utils, "now_datetime", lambda: "2026-05-21 12:00:00")

    result = order_sync.refresh_order_contact_snapshot("14504")

    assert result["status"] == "updated"
    assert result["invoice"] == "ACC-SINV-0001"
    assert "customer_name" in result["invoice_fields_updated"]
    assert updates[0][0] == "Sales Invoice"
    assert updates[0][1] == "ACC-SINV-0001"
    assert updates[0][2]["customer_name"] == "Delivery Recipient"
    assert updates[1][0] == "WooCommerce Order Map"
    assert updates[1][2]["woo_order_display_name"] == "Delivery Recipient"
    assert updates[1][2]["synced_on"] == "2026-05-21 12:00:00"
    assert commit_calls == [True]


def test_order_territory_snapshot_prefers_shipping_state(monkeypatch):
    calls = []

    def fake_resolve(state_value, territory_state_cache=None):
        calls.append((state_value, territory_state_cache))
        return {"EGNASRCITY": "EGNASRCITY", "EGZAWYA": "EGZAWYA"}.get(state_value)

    cache = {}
    monkeypatch.setattr(order_sync, "_resolve_territory_from_state", fake_resolve)

    snapshot = order_sync._extract_order_territory_snapshot(
        {
            "billing": {"state": "EGZAWYA"},
            "shipping": {"state": "EGNASRCITY"},
        },
        territory_state_cache=cache,
    )

    assert snapshot["woo_billing_state"] == "EGZAWYA"
    assert snapshot["woo_shipping_state"] == "EGNASRCITY"
    assert snapshot["resolved_order_territory"] == "EGNASRCITY"
    assert snapshot["woo_territory_hash"]
    assert calls == [("EGNASRCITY", cache)]


def test_order_map_snapshot_values_include_territory_snapshot(monkeypatch):
    monkeypatch.setattr(order_sync.frappe.utils, "now_datetime", lambda: "2026-06-01 10:00:00")
    contact_snapshot = order_sync._extract_order_contact_snapshot(
        {
            "id": 15031,
            "billing": {"first_name": "Billing", "email": "b@example.com"},
            "shipping": {"first_name": "Shipping", "phone": "0100"},
        }
    )
    territory_snapshot = {
        "woo_billing_state": "EGZAWYA",
        "woo_shipping_state": "EGNASRCITY",
        "resolved_order_territory": "EGNASRCITY",
        "woo_territory_hash": "hash-1",
    }

    values = order_sync._build_order_map_snapshot_values(
        {"number": "15031", "status": "processing", "currency": "EGP", "payment_method": "cod"},
        contact_snapshot,
        link_field="erpnext_sales_invoice",
        invoice_name="ACC-SINV-2026-16213",
        order_hash="order-hash",
        needs_territory_recheck=0,
        territory_snapshot=territory_snapshot,
    )

    assert values["erpnext_sales_invoice"] == "ACC-SINV-2026-16213"
    assert values["hash"] == "order-hash"
    assert values["needs_territory_recheck"] == 0
    assert values["woo_shipping_state"] == "EGNASRCITY"
    assert values["resolved_order_territory"] == "EGNASRCITY"
    assert values["woo_territory_hash"] == "hash-1"


def test_process_order_phase1_sets_invoice_territory_from_order_state(monkeypatch):
    created = {}
    map_docs = {}

    class DummyLock:
        def acquire(self, blocking=False):
            return True

        def release(self):
            return None

    class DummyRedis:
        def lock(self, *args, **kwargs):
            return DummyLock()

    class FakeMapDoc:
        def __init__(self, values):
            self.values = dict(values)
            self.name = self.values.get("name") or "WOOMAP-NEW"

        def insert(self, ignore_permissions=True):
            map_docs[self.name] = self
            return self

        def update(self, values):
            self.values.update(values)

        def save(self, ignore_permissions=True):
            map_docs[self.name] = self
            return self

    class FakeInvoice:
        def __init__(self, values):
            self.__dict__.update(values)
            self.name = "ACC-SINV-NEW"
            self.docstatus = 0
            self.flags = SimpleNamespace()
            self.items = list(values.get("items") or [])

        def get(self, fieldname, default=None):
            return getattr(self, fieldname, default)

        def set(self, fieldname, value):
            setattr(self, fieldname, value)

        def append(self, fieldname, value):
            getattr(self, fieldname).append(value)

        def insert(self, ignore_permissions=True):
            created["invoice"] = self
            return self

        def save(self, *args, **kwargs):
            return self

        def db_set(self, fieldname, value, commit=False):
            setattr(self, fieldname, value)

        def cancel(self):
            self.docstatus = 2

    def fake_sql(query, params=None, as_dict=False):
        del params, as_dict
        if "GET_LOCK" in query:
            return [(1,)]
        if "RELEASE_LOCK" in query:
            return [(1,)]
        raise AssertionError(query)

    def fake_get_value(doctype, name_or_filters=None, fieldname=None, *args, **kwargs):
        if doctype == "WooCommerce Order Map":
            return None
        if doctype == "Customer" and fieldname == "territory":
            return "EGZAWYA"
        if doctype == "Territory" and name_or_filters == "EGNASRCITY" and fieldname == "pos_profile":
            return "Nasr city"
        if doctype == "POS Profile" and name_or_filters == "Nasr city" and fieldname == "warehouse":
            return "Nasr city - J"
        if doctype == "POS Profile" and name_or_filters == "Nasr city" and fieldname == "selling_price_list":
            return "Standard Selling"
        if doctype == "Company" and name_or_filters == "_Test Company" and fieldname == "default_selling_price_list":
            return "Standard Selling"
        return None

    def fake_get_doc(doctype_or_dict, name=None, *args, **kwargs):
        if isinstance(doctype_or_dict, dict):
            dt = doctype_or_dict.get("doctype")
            if dt == "Sales Invoice":
                return FakeInvoice(doctype_or_dict)
            if dt == "WooCommerce Order Map":
                return FakeMapDoc(doctype_or_dict)
            doc = SimpleNamespace(name="LOG-0001")
            doc.insert = lambda ignore_permissions=True: doc
            return doc
        if doctype_or_dict == "WooCommerce Order Map":
            return map_docs[name]
        raise AssertionError((doctype_or_dict, name))

    settings = SimpleNamespace(
        base_url="https://example.com",
        consumer_key="ck_test",
        get_password=lambda fieldname: "cs_test",
        default_company="_Test Company",
        default_currency="EGP",
        default_pos_profile="Dokki",
    )
    order = {
        "id": 15031,
        "number": "15031",
        "status": "processing",
        "currency": "EGP",
        "total": "240.00",
        "payment_method": "cod",
        "payment_method_title": "Cash",
        "billing": {"first_name": "Billing", "state": "EGZAWYA", "email": "billing@example.com"},
        "shipping": {"first_name": "Shipping", "state": "EGNASRCITY", "phone": "01001234567"},
        "line_items": [{"product_id": 1, "variation_id": 0, "quantity": 1, "total": "240.00", "subtotal": "240.00"}],
        "shipping_lines": [],
        "fee_lines": [],
        "tax_lines": [],
        "meta_data": [],
    }

    monkeypatch.setattr(order_sync, "get_redis_conn", lambda: DummyRedis())
    monkeypatch.setattr(order_sync.frappe.db, "sql", fake_sql)
    monkeypatch.setattr(order_sync.frappe.db, "get_table_columns", lambda doctype: ["erpnext_sales_invoice"])
    monkeypatch.setattr(order_sync.frappe.db, "get_value", fake_get_value)
    monkeypatch.setattr(order_sync.frappe.db, "exists", lambda doctype, name=None: False)
    monkeypatch.setattr(order_sync.frappe.db, "commit", lambda: None)
    monkeypatch.setattr(order_sync.frappe, "get_all", lambda *args, **kwargs: [])
    monkeypatch.setattr(order_sync.frappe, "get_doc", fake_get_doc)
    monkeypatch.setattr(order_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False))
    monkeypatch.setattr(order_sync.frappe.utils, "today", lambda: "2026-06-01")
    monkeypatch.setattr(order_sync.frappe.utils, "now_datetime", lambda: "2026-06-01 12:00:00")
    monkeypatch.setattr(
        order_sync,
        "ensure_customer_with_addresses",
        lambda *args, **kwargs: ("Test Customer", "Billing-001", "Shipping-001"),
    )
    monkeypatch.setattr(
        order_sync,
        "_resolve_territory_from_state",
        lambda state_value, territory_state_cache=None: {"EGNASRCITY": "EGNASRCITY", "EGZAWYA": "EGZAWYA"}.get(state_value),
    )
    monkeypatch.setattr(
        order_sync,
        "_build_invoice_items",
        lambda *args, **kwargs: ([{"item_code": "ITEM-001", "qty": 1, "rate": 240.0}], [], {}),
    )
    monkeypatch.setattr(order_sync, "_apply_delivery_charge_policy", lambda *args, **kwargs: {"changed": False})
    monkeypatch.setattr(order_sync, "_maybe_create_payment_entry_for_invoice", lambda *args, **kwargs: None)
    monkeypatch.setattr(order_sync, "_submit_invoice_with_accounting_guards", lambda *args, **kwargs: None)

    result = order_sync.process_order_phase1(order, settings)

    assert result["status"] == "created"
    assert created["invoice"].territory == "EGNASRCITY"
    assert created["invoice"].pos_profile == "Nasr city"
    assert created["invoice"].items[0]["warehouse"] == "Nasr city - J"
    assert map_docs["WOOMAP-NEW"].values["resolved_order_territory"] == "EGNASRCITY"
    assert map_docs["WOOMAP-NEW"].values["woo_shipping_state"] == "EGNASRCITY"


def test_refresh_order_contact_snapshot_dry_run_reports_without_writing(monkeypatch):
    settings = SimpleNamespace(
        base_url="https://example.com",
        consumer_key="ck_test",
        get_password=lambda fieldname: "cs_test",
    )
    order = {
        "id": 14505,
        "number": "14505",
        "status": "processing",
        "currency": "EGP",
        "total": "250.00",
        "payment_method": "cod",
        "billing": {"email": "guest@example.com"},
        "shipping": {},
    }
    writes = []

    class DummyClient:
        def __init__(self, *args, **kwargs):
            pass

        def get_order(self, order_id):
            return dict(order)

    def fake_get_value(doctype, name_or_filters, fieldname=None, as_dict=False):
        if doctype == "WooCommerce Order Map" and name_or_filters == {"woo_order_id": 14505}:
            return {"name": "WOOMAP-0002", "erpnext_sales_invoice": "ACC-SINV-0002"}
        if doctype == "Sales Invoice" and name_or_filters == "ACC-SINV-0002":
            return {field: "" for field in fieldname}
        if doctype == "WooCommerce Order Map" and name_or_filters == "WOOMAP-0002":
            return {field: "" for field in fieldname}
        raise AssertionError((doctype, name_or_filters, fieldname, as_dict))

    monkeypatch.setattr(order_sync, "WooClient", DummyClient)
    monkeypatch.setattr(order_sync, "ensure_custom_fields", lambda: None)
    monkeypatch.setattr(order_sync.frappe, "get_single", lambda doctype: settings)
    monkeypatch.setattr(order_sync.frappe.db, "get_value", fake_get_value)
    monkeypatch.setattr(order_sync.frappe.db, "set_value", lambda *args, **kwargs: writes.append((args, kwargs)))
    monkeypatch.setattr(order_sync.frappe.db, "commit", lambda: writes.append("commit"))
    monkeypatch.setattr(order_sync.frappe.db, "get_table_columns", lambda doctype: ["erpnext_sales_invoice"])

    result = order_sync.refresh_order_contact_snapshot("14505", dry_run=True)

    assert result["status"] == "updated"
    assert result["dry_run"] is True
    assert writes == []


def test_backfill_order_contact_snapshots_by_ids_aggregates_statuses(monkeypatch):
    results = {
        "14504": {"status": "updated", "woo_order_id": 14504},
        "14505": {"status": "skipped", "reason": "unchanged", "woo_order_id": 14505},
        "14506": {"status": "error", "woo_order_id": 14506},
    }

    monkeypatch.setattr(
        order_sync,
        "refresh_order_contact_snapshot",
        lambda order_id, dry_run=False: results[str(order_id)],
    )

    summary = order_sync.backfill_order_contact_snapshots_by_ids("14504,14505,14506")

    assert summary["requested"] == 3
    assert summary["processed"] == 3
    assert summary["updated"] == 1
    assert summary["skipped"] == 1
    assert summary["errors"] == 1


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


def test_sync_cancelled_orders_cron_uses_any_with_local_status_filter(monkeypatch):
    captured = {}

    def fake_run_order_cursor_sync(**kwargs):
        captured.update(kwargs)
        return {}

    monkeypatch.setattr(order_sync.frappe.db, "get_single_value", lambda doctype, fieldname: 1)
    monkeypatch.setattr(order_sync, "_run_order_cursor_sync", fake_run_order_cursor_sync)

    order_sync.sync_cancelled_orders_cron()

    assert captured["status"] == "any"
    assert captured["status_filter_set"] == set(order_sync.CANCELLED_CURSOR_TARGET_WOO_STATUSES)