"""The per-invoice push lock in ``outbound_sync.sync_sales_invoice``.

Regression for ACC-SINV-2026-18733 (2026-10-06): four outbox events for one
invoice, three of them POSTed, three Woo orders (17863/17864/17865). Covers:

* under the lock the push re-reads the CURRENT id and PUTs instead of POSTing;
* an Order Map row linking the invoice also counts (retired rows never do, and
  the row matching the invoice's own id wins);
* lock not acquired -> no write to the store and a retryable, breaker-neutral
  outbox outcome;
* REPEATABLE READ: an outbox push commits before the re-read; an inline push
  uses a locking read and defers the lock release to its caller's commit;
* the outbox no longer claims a second push for an object already in flight.

Pure unit tests: ``frappe.db`` and the Woo client are fakes.
"""

import json
import unittest
import unittest.mock
from datetime import datetime
from types import SimpleNamespace

from jarz_woocommerce_integration.services import outbound_sync, sync_events

INVOICE = "ACC-SINV-2026-18733"
_NO_ROW = object()


class _Callbacks:
    def __init__(self):
        self.functions = []

    def add(self, func):
        self.functions.append(func)

    def run(self):
        while self.functions:
            self.functions.pop(0)()


class _FakeDB:
    """Records every call in order, so tests can assert on sequencing."""

    def __init__(self, *, lock_result=1, lock_raises=False, invoice_woo_order_id=_NO_ROW, map_rows=()):
        self.lock_result = lock_result
        self.lock_raises = lock_raises
        self.invoice_woo_order_id = invoice_woo_order_id
        self.map_rows = list(map_rows)
        self.calls = []
        self.after_commit = _Callbacks()
        self.after_rollback = _Callbacks()

    def sql(self, query, params=None, as_dict=False):
        flat = " ".join(str(query).split())
        self.calls.append(("sql", flat, params))
        if "GET_LOCK" in flat:
            if self.lock_raises:
                raise RuntimeError("MySQL server has gone away")
            return ((self.lock_result,),)
        if "RELEASE_LOCK" in flat:
            return ((1,),)
        if "FROM `tabSales Invoice`" in flat:
            if self.invoice_woo_order_id is _NO_ROW:
                return ()
            return ((self.invoice_woo_order_id,),)
        if "FROM `tabWooCommerce Order Map`" in flat:
            return [dict(row) for row in self.map_rows]
        raise AssertionError(f"unexpected SQL: {flat}")

    def commit(self):
        self.calls.append(("commit",))

    def exists(self, doctype, filters=None):
        return True

    def get_value(self, *args, **kwargs):
        return None

    def set_value(self, doctype, name, values, update_modified=False):
        self.calls.append(("set_value", doctype, name, dict(values) if isinstance(values, dict) else values))

    def get_table_columns(self, doctype):
        return ["erpnext_sales_invoice"]

    # --- helpers for assertions -------------------------------------------
    def index_of(self, predicate):
        for index, call in enumerate(self.calls):
            if predicate(call):
                return index
        return -1

    def sql_calls(self, needle):
        return [call for call in self.calls if call[0] == "sql" and needle in call[1]]

    def invoice_updates(self):
        return [call[3] for call in self.calls if call[0] == "set_value" and call[1] == "Sales Invoice"]


class _Invoice:
    def __init__(self, woo_order_id=None):
        self.name = INVOICE
        self.customer = "CUST-0001"
        self.currency = "EGP"
        self.docstatus = 1
        self.woo_order_id = woo_order_id
        self.grand_total = 100
        self.amended_from = None
        self.is_return = 0
        self.flags = SimpleNamespace()

    def get(self, fieldname, default=None):
        return getattr(self, fieldname, default)


class _Client:
    def __init__(self, created_id=17999):
        self.created_id = created_id
        self.gets, self.puts, self.posts = [], [], []

    def get(self, path):
        self.gets.append(path)
        return {"id": int(path.split("/")[1]), "status": "processing"}

    def put(self, path, payload):
        self.puts.append(path)
        woo_id = int(path.split("/")[1])
        return {"id": woo_id, "number": str(woo_id), "status": "processing"}

    def post(self, path, payload):
        self.posts.append(path)
        return {"id": self.created_id, "number": str(self.created_id), "status": "processing"}


def _cfg():
    return outbound_sync.OutboundConfig(
        enable_customer_push=True,
        enable_order_push=True,
        payment_cod="cod",
        payment_instapay="instapay",
        payment_wallet="wallet",
        shipping_method_id="flat_rate",
        shipping_method_title="Shipping",
    )


def _run_push(db, invoice, client, *, own_transaction):
    customer = SimpleNamespace(name=invoice.customer)
    mark_status = unittest.mock.MagicMock()

    def fake_get_doc(doctype, name=None):
        return invoice if doctype == "Sales Invoice" else customer

    patches = [
        unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _cfg())),
        unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client),
        unittest.mock.patch.object(outbound_sync, "_build_order_payload", return_value={"status": "processing"}),
        unittest.mock.patch.object(outbound_sync, "_payload_total_mismatch", return_value=None),
        # Always a real change, so the write path (PUT or POST) is what is tested.
        unittest.mock.patch.object(outbound_sync, "_order_payload_requires_update", return_value=True),
        unittest.mock.patch.object(outbound_sync, "_check_response_total", return_value=None),
        unittest.mock.patch.object(outbound_sync, "_detect_unpayable_transition", return_value=False),
        unittest.mock.patch.object(outbound_sync, "_extract_response_snapshots", return_value=({}, {})),
        unittest.mock.patch.object(outbound_sync, "_contact_snapshot_invoice_values", return_value={}),
        unittest.mock.patch.object(outbound_sync, "_relink_order_map_to_invoice"),
        unittest.mock.patch.object(outbound_sync, "_publish_woo_order_assigned"),
        unittest.mock.patch.object(outbound_sync, "_build_delivery_details_note", return_value=""),
        unittest.mock.patch.object(outbound_sync, "_mark_outbound_push_in_flight"),
        unittest.mock.patch.object(outbound_sync, "_mark_invoice_status", mark_status),
        unittest.mock.patch.object(outbound_sync, "get_customer_woo_id", return_value="88"),
        unittest.mock.patch.object(outbound_sync, "has_unmigrated_legacy_customer_woo_id", return_value=False),
        unittest.mock.patch.object(outbound_sync, "now_datetime", return_value="2026-10-06 18:21:51"),
        unittest.mock.patch.object(outbound_sync.frappe, "get_doc", side_effect=fake_get_doc),
        unittest.mock.patch.object(outbound_sync.frappe, "db", db),
        unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)),
    ]
    for patcher in patches:
        patcher.start()
    try:
        result = outbound_sync.sync_sales_invoice(INVOICE, reason="test", own_transaction=own_transaction)
    finally:
        for patcher in reversed(patches):
            patcher.stop()
    return result, mark_status


class TestCreatePathRechecksUnderTheLock(unittest.TestCase):
    def test_id_committed_by_another_worker_is_put_not_posted(self):
        # The event loaded the invoice before the previous push committed 17863
        # (WOOEVT-865505 in the incident). Under the lock the fresh read wins.
        db = _FakeDB(invoice_woo_order_id=17863)
        client = _Client()

        result, _ = _run_push(db, _Invoice(woo_order_id=None), client, own_transaction=True)

        self.assertEqual(result, {"status": "ok", "woo_order_id": 17863})
        self.assertEqual(client.posts, [])
        self.assertEqual(client.puts, ["orders/17863"])
        self.assertTrue(all("woo_order_id" not in u for u in db.invoice_updates()))

    def test_order_map_row_linking_the_invoice_is_put_not_posted(self):
        # The invoice column reads blank (stale or clobbered by a full save), but
        # an Order Map row already links this invoice: never create another order.
        db = _FakeDB(
            invoice_woo_order_id=None,
            map_rows=[
                {"woo_order_id": 17865, "status": "retired-duplicate"},
                {"woo_order_id": 17864, "status": "processing"},
            ],
        )
        client = _Client()

        result, _ = _run_push(db, _Invoice(woo_order_id=None), client, own_transaction=True)

        self.assertEqual(result, {"status": "ok", "woo_order_id": 17864})
        self.assertEqual(client.posts, [])
        self.assertEqual(client.puts, ["orders/17864"])
        # ...and the id is written back onto the invoice.
        self.assertEqual(db.invoice_updates()[0]["woo_order_id"], 17864)

    def test_order_map_row_matching_the_invoice_id_is_preferred(self):
        db = _FakeDB(
            invoice_woo_order_id=None,
            map_rows=[
                {"woo_order_id": 17863, "status": "processing"},
                {"woo_order_id": 17864, "status": "processing"},
            ],
        )
        client = _Client()

        _run_push(db, _Invoice(woo_order_id=17864), client, own_transaction=True)

        self.assertEqual(client.puts, ["orders/17864"])
        self.assertEqual(client.posts, [])

    def test_only_retired_rows_still_creates_the_order(self):
        db = _FakeDB(invoice_woo_order_id=None, map_rows=[{"woo_order_id": 17865, "status": "retired-duplicate"}])
        client = _Client(created_id=18000)

        result, _ = _run_push(db, _Invoice(woo_order_id=None), client, own_transaction=True)

        self.assertEqual(result, {"status": "ok", "woo_order_id": 18000})
        self.assertEqual(client.posts, ["orders"])
        self.assertEqual(db.invoice_updates()[0]["woo_order_id"], 18000)

    def test_outbox_push_commits_before_the_reread_and_after_the_writeback(self):
        db = _FakeDB(invoice_woo_order_id=None)
        client = _Client(created_id=18001)

        _run_push(db, _Invoice(woo_order_id=None), client, own_transaction=True)

        got_lock = db.index_of(lambda c: c[0] == "sql" and "GET_LOCK" in c[1])
        reread = db.index_of(lambda c: c[0] == "sql" and "FROM `tabSales Invoice`" in c[1])
        writeback = db.index_of(lambda c: c[0] == "set_value" and c[1] == "Sales Invoice")
        released = db.index_of(lambda c: c[0] == "sql" and "RELEASE_LOCK" in c[1])
        commits = [i for i, c in enumerate(db.calls) if c == ("commit",)]

        self.assertTrue(got_lock < commits[0] < reread, db.calls)
        self.assertNotIn("FOR UPDATE", db.calls[reread][1])
        self.assertTrue(any(writeback < i < released for i in commits), db.calls)
        self.assertEqual(db.after_commit.functions, [])

    def test_inline_push_uses_a_locking_read_and_releases_after_the_callers_commit(self):
        db = _FakeDB(invoice_woo_order_id=None)
        client = _Client(created_id=18002)

        _run_push(db, _Invoice(woo_order_id=None), client, own_transaction=False)

        self.assertNotIn(("commit",), db.calls, "an inline push must never commit its caller's work")
        self.assertIn("FOR UPDATE", db.sql_calls("FROM `tabSales Invoice`")[0][1])
        self.assertEqual(db.sql_calls("RELEASE_LOCK"), [], "released before the write-back is visible")
        self.assertEqual(len(db.after_rollback.functions), 1)

        # The caller commits: Frappe runs after_commit right after the COMMIT.
        with unittest.mock.patch.object(outbound_sync.frappe, "db", db):
            db.after_commit.run()
        self.assertEqual(len(db.sql_calls("RELEASE_LOCK")), 1)


class TestLockNotAcquired(unittest.TestCase):
    def _assert_no_write(self, db):
        client = _Client()
        result, mark_status = _run_push(db, _Invoice(woo_order_id=None), client, own_transaction=True)

        self.assertEqual(
            result,
            {"status": "error", "detail": outbound_sync.INVOICE_PUSH_LOCKED_DETAIL, "retryable": True},
        )
        self.assertEqual((client.gets, client.puts, client.posts), ([], [], []))
        mark_status.assert_not_called()
        self.assertEqual(db.sql_calls("FROM `tabSales Invoice`"), [])
        self.assertEqual(db.sql_calls("RELEASE_LOCK"), [])
        return result

    def test_lock_timeout_does_not_post(self):
        self._assert_no_write(_FakeDB(lock_result=0))

    def test_lock_error_fails_closed(self):
        self._assert_no_write(_FakeDB(lock_raises=True))

    def test_outbox_retries_a_locked_push_without_tripping_the_breaker(self):
        updates = []
        event_doc = SimpleNamespace(
            name="WOOEVT-865508",
            direction="Outbound",
            object_type="Sales Invoice",
            attempt_count=1,
            max_attempts=8,
            db_set=lambda values, update_modified=False: updates.append(values),
        )
        locked = {"status": "error", "detail": outbound_sync.INVOICE_PUSH_LOCKED_DETAIL, "retryable": True}

        with unittest.mock.patch.object(sync_events, "now_datetime", return_value=datetime(2026, 10, 6, 18, 22)), \
             unittest.mock.patch.object(sync_events, "_record_outbound_circuit_breaker_failure") as breaker:
            outcome = sync_events._apply_outbound_result(event_doc, locked)

        self.assertEqual(outcome["status"], "retry")
        self.assertEqual(updates[-1]["status"], "RetryScheduled")
        breaker.assert_not_called()

    def test_classification_of_new_reasons(self):
        self.assertEqual(sync_events._classify_text_reason(outbound_sync.INVOICE_PUSH_LOCKED_DETAIL), "retry")
        self.assertEqual(sync_events._classify_text_reason("retired_duplicate"), "skip")


class TestOutboxDispatch(unittest.TestCase):
    def test_invoice_push_is_dispatched_as_owning_its_transaction(self):
        event_doc = SimpleNamespace(object_type="Sales Invoice", local_docname=INVOICE, source_id=INVOICE)

        with unittest.mock.patch.object(outbound_sync, "sync_sales_invoice", return_value={"status": "ok"}) as push:
            sync_events._dispatch_outbound_event(event_doc, {"reason": "event"})

        self.assertTrue(push.call_args.kwargs["own_transaction"])

    def test_claim_defers_an_outbound_event_whose_sibling_is_processing(self):
        cfg = sync_events.SyncEventConfig(
            enabled=True,
            shadow_mode=False,
            use_outbox_for_customer_push=False,
            use_outbox_for_invoice_push=True,
            use_inbox_for_order_webhook=False,
            use_inbox_for_customer_webhook=False,
            use_inbox_for_order_polling=False,
            use_event_reconciliation=True,
            worker_enabled=True,
            max_attempts=8,
            batch_size=25,
            success_retention_days=90,
            lock_ttl_seconds=900,
            shadow_alert_threshold=5,
            circuit_breaker_threshold=10,
            circuit_breaker_window_seconds=300,
            circuit_breaker_cooldown_seconds=300,
        )
        fixed_now = datetime(2026, 10, 6, 18, 21, 56)
        sql_calls = []

        def fake_sql(query, params=None, as_dict=False):
            sql_calls.append((" ".join(query.split()), params))
            return []

        with unittest.mock.patch.object(sync_events, "now_datetime", return_value=fixed_now), \
             unittest.mock.patch.object(sync_events, "get_sync_event_config", return_value=cfg), \
             unittest.mock.patch.object(sync_events.frappe.db, "sql", side_effect=fake_sql):
            sync_events._claim_due_sync_events(batch_size=1, settings=SimpleNamespace(), event_name="WOOEVT-865508")

        query, params = sql_calls[0]
        self.assertIn("FOR UPDATE SKIP LOCKED", query)
        self.assertIn("busy.status = 'Processing'", query)
        self.assertIn("busy.source_id = ev.source_id", query)
        self.assertIn("busy.locked_until >= %s", query)
        self.assertEqual(query.count("%s"), len(params))
        self.assertIn("WOOEVT-865508", params)

    def test_finished_outbound_event_requeues_its_waiting_sibling(self):
        event_doc = SimpleNamespace(
            name="WOOEVT-865505",
            direction="Outbound",
            object_type="Sales Invoice",
            source_id=INVOICE,
            payload_json=json.dumps({"invoice_name": INVOICE, "reason": "event"}),
            attempt_count=1,
            max_attempts=8,
            db_set=lambda values, update_modified=False: None,
        )

        with unittest.mock.patch.object(sync_events, "now_datetime", return_value=datetime(2026, 10, 6, 18, 22, 2)), \
             unittest.mock.patch.object(sync_events, "_dispatch_outbound_event", return_value={"status": "ok"}), \
             unittest.mock.patch.object(sync_events, "_clear_outbound_circuit_breaker"), \
             unittest.mock.patch.object(sync_events.frappe.db, "commit", return_value=None), \
             unittest.mock.patch.object(sync_events.frappe.db, "sql", return_value=[{"name": "WOOEVT-865508"}]), \
             unittest.mock.patch.object(sync_events, "enqueue_sync_event") as enqueue:
            outcome = sync_events._process_claimed_event(event_doc)

        self.assertEqual(outcome["status"], "succeeded")
        enqueue.assert_called_once_with("WOOEVT-865508", after_commit=False)


class TestLockKey(unittest.TestCase):
    def test_key_fits_the_mariadb_limit(self):
        self.assertEqual(outbound_sync._invoice_push_lock_key(INVOICE), f"woo-outbound-inv:{INVOICE}")
        long_key = outbound_sync._invoice_push_lock_key("ACC-SINV-2026-18733-" + "1-" * 40)
        self.assertLessEqual(len(long_key), 64)


if __name__ == "__main__":
    unittest.main()
