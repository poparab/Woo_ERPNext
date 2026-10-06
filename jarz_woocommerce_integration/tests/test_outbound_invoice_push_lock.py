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

    def __init__(self, *, lock_result=1, lock_raises=False, invoice_woo_order_id=_NO_ROW, map_rows=(), retired_ids=(), other_owners=None):
        self.retired_ids = {int(x) for x in retired_ids}
        # woo_order_id -> another live invoice that records it
        self.other_owners = dict(other_owners or {})
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
        if "FROM `tabSales Invoice` WHERE woo_order_id = %s AND name != %s" in flat:
            owner = self.other_owners.get(int(params[0]))
            return ((owner,),) if owner else ()
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

    def get_value(self, doctype=None, filters=None, fieldname=None, *args, **kwargs):
        if (
            doctype == "WooCommerce Order Map"
            and fieldname == "status"
            and isinstance(filters, dict)
            and int(filters.get("woo_order_id") or 0) in self.retired_ids
        ):
            return "retired-duplicate"
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
    def __init__(self, created_id=17999, store_orders=None):
        self.created_id = created_id
        self.store_orders = list(store_orders or [])
        self.gets, self.puts, self.posts, self.lookups = [], [], [], []
        self.post_kwargs = []

    def get(self, path, params=None):
        if path == "orders":
            self.lookups.append(dict(params or {}))
            return list(self.store_orders)
        self.gets.append(path)
        return {"id": int(path.split("/")[1]), "status": "processing"}

    def put(self, path, payload):
        self.puts.append(path)
        woo_id = int(path.split("/")[1])
        return {"id": woo_id, "number": str(woo_id), "status": "processing"}

    def post(self, path, payload, **kwargs):
        self.posts.append(path)
        self.post_kwargs.append(kwargs)
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


def _run_push(db, invoice, client, *, own_transaction, payload=None, in_doubt_hold=False):
    customer = SimpleNamespace(name=invoice.customer)
    built_payload = dict(payload or {"status": "processing"})
    # Every payload build, so tests can see create-vs-update and its base order.
    db.build_calls = []
    db.noted_in_doubt = unittest.mock.MagicMock()

    def fake_build(*args, **kwargs):
        db.build_calls.append(kwargs)
        return dict(built_payload)
    mark_status = unittest.mock.MagicMock()

    def fake_get_doc(doctype, name=None):
        return invoice if doctype == "Sales Invoice" else customer

    patches = [
        unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _cfg())),
        unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client),
        unittest.mock.patch.object(outbound_sync, "_build_order_payload", side_effect=fake_build),
        # The in-doubt hold lives in the Redis cache; keep tests independent of it.
        unittest.mock.patch.object(outbound_sync, "_create_recently_in_doubt", return_value=in_doubt_hold),
        unittest.mock.patch.object(outbound_sync, "_note_create_in_doubt", db.noted_in_doubt),
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


_CREATE_PAYLOAD = {"status": "processing", "customer_id": 7705, "billing": {"phone": "01205476482"}}


def _store_order(woo_id, invoice=INVOICE, status="processing"):
    return {
        "id": woo_id,
        "status": status,
        "meta_data": [{"key": "erpnext_sales_invoice", "value": invoice}],
    }


class _FailingCreateClient(_Client):
    """The create's reply is lost (or refused) -- the store may or may not hold it."""

    def __init__(self, error, **kwargs):
        super().__init__(**kwargs)
        self.error = error

    def post(self, path, payload, **kwargs):
        self.posts.append(path)
        self.post_kwargs.append(kwargs)
        raise self.error


class _LookupFailsClient(_Client):
    def get(self, path, params=None):
        if path == "orders":
            raise outbound_sync.WooAPIError(503, "orders", "Service Unavailable")
        return super().get(path, params)


class TestCreateIsSentOnce(unittest.TestCase):
    """A create is not idempotent: one attempt, recovery by store lookup."""

    def test_create_is_a_single_attempt(self):
        db = _FakeDB(invoice_woo_order_id=None)
        client = _Client(created_id=18100)

        result, _ = _run_push(db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertEqual(result, {"status": "ok", "woo_order_id": 18100})
        self.assertEqual(client.posts, ["orders"])
        self.assertEqual(client.post_kwargs, [{"max_attempts": 1}])
        # The lookup ran first, by customer, newest ids first.
        self.assertEqual(client.lookups[0]["customer"], 7705)
        self.assertEqual((client.lookups[0]["orderby"], client.lookups[0]["order"]), ("id", "desc"))

    def test_order_left_by_a_lost_reply_is_adopted_not_created_again(self):
        # 17863 was created by an earlier attempt whose reply never arrived, so
        # ERPNext recorded nothing. The store still names this invoice in its meta.
        db = _FakeDB(invoice_woo_order_id=None)
        client = _Client(store_orders=[_store_order(17999, invoice="ACC-SINV-2026-00001"), _store_order(17863)])

        result, _ = _run_push(db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertEqual(result, {"status": "ok", "woo_order_id": 17863})
        self.assertEqual(client.posts, [])
        self.assertEqual(client.puts, ["orders/17863"])
        self.assertEqual(db.invoice_updates()[0]["woo_order_id"], 17863)

    def test_oldest_live_order_wins_and_retired_or_trashed_ones_never_count(self):
        db = _FakeDB(invoice_woo_order_id=None, retired_ids=[17863])
        client = _Client(
            store_orders=[
                _store_order(17866),
                _store_order(17865, status="trash"),
                _store_order(17864),
                _store_order(17863),
            ]
        )

        result, _ = _run_push(db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertEqual(result["woo_order_id"], 17864)
        self.assertEqual(client.puts, ["orders/17864"])
        self.assertEqual(client.posts, [])

    def test_lookup_falls_back_to_the_billing_phone(self):
        db = _FakeDB(invoice_woo_order_id=None)
        client = _Client(created_id=18101)
        payload = {"status": "processing", "billing": {"phone": "01205476482"}}

        _run_push(db, _Invoice(), client, own_transaction=True, payload=payload)

        self.assertEqual(
            client.lookups,
            [{"search": "01205476482", "per_page": 50, "orderby": "id", "order": "desc", "_fields": "id,status,meta_data"}],
        )

    def test_customer_orders_are_searched_once_without_the_phone(self):
        db = _FakeDB(invoice_woo_order_id=None)
        client = _Client(created_id=18102)

        _run_push(db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertEqual(len(client.lookups), 1)
        self.assertEqual(client.lookups[0]["customer"], 7705)
        self.assertNotIn("search", client.lookups[0])

    def test_store_that_cannot_be_asked_gets_no_create(self):
        db = _FakeDB(invoice_woo_order_id=None)
        client = _LookupFailsClient()

        result, _ = _run_push(db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertEqual(
            result,
            {"status": "error", "detail": outbound_sync.ORDER_LOOKUP_FAILED_DETAIL, "retryable": True},
        )
        self.assertEqual(client.posts, [])
        self.assertEqual(sync_events._classify_text_reason(result["detail"]), "retry")

    def test_transient_create_failure_is_in_doubt_and_never_resent(self):
        for error in (
            outbound_sync.WooTransientError(502, "orders", "Bad Gateway (transient, 1 attempts)"),
            outbound_sync.requests.Timeout("read timed out"),
            outbound_sync.requests.ConnectionError("connection reset"),
        ):
            with self.subTest(error=type(error).__name__):
                db = _FakeDB(invoice_woo_order_id=None)
                client = _FailingCreateClient(error)

                result, _ = _run_push(db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD)

                self.assertEqual(
                    result,
                    {"status": "error", "detail": outbound_sync.ORDER_CREATE_IN_DOUBT_DETAIL, "retryable": True},
                )
                self.assertEqual(client.posts, ["orders"], "a create in doubt must not be sent twice")
                self.assertEqual(db.invoice_updates(), [])
                self.assertEqual(sync_events._classify_text_reason(result["detail"]), "retry")

    def test_a_real_refusal_is_still_an_error(self):
        db = _FakeDB(invoice_woo_order_id=None)
        client = _FailingCreateClient(outbound_sync.WooAPIError(400, "orders", "Customer ID is invalid."))

        result, _ = _run_push(db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertEqual(result, {"status": "error", "detail": "Customer ID is invalid."})

    def test_invoice_column_naming_a_retired_order_resolves_to_the_real_one(self):
        # A stale full save put the retired duplicate's id back on the invoice.
        db = _FakeDB(
            invoice_woo_order_id=17865,
            retired_ids=[17865],
            map_rows=[
                {"woo_order_id": 17865, "status": "retired-duplicate"},
                {"woo_order_id": 17864, "status": "completed"},
            ],
        )
        client = _Client()

        result, _ = _run_push(db, _Invoice(woo_order_id=17865), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertEqual(result, {"status": "ok", "woo_order_id": 17864})
        self.assertEqual(client.puts, ["orders/17864"])
        self.assertEqual(client.posts, [])
        self.assertEqual(db.invoice_updates()[0]["woo_order_id"], 17864)


class TestHandoverRefusesRetiredOrders(unittest.TestCase):
    def test_retired_order_never_reaches_the_replacement_invoice(self):
        db = _FakeDB(retired_ids=[17865])

        with unittest.mock.patch.object(outbound_sync.frappe, "db", db):
            outbound_sync._handover_woo_order_to_replacement(
                woo_order_id=17865,
                source_invoice=INVOICE,
                replacement_invoice=INVOICE + "-1",
            )

        self.assertEqual([c for c in db.calls if c[0] == "set_value"], [])


class TestInlineCallersOwnTheirTransaction(unittest.TestCase):
    def test_manual_push_lets_the_push_commit(self):
        from jarz_woocommerce_integration.api import manual_sync

        with unittest.mock.patch.object(manual_sync.access, "ensure_operator_access"), \
             unittest.mock.patch.object(manual_sync.frappe, "has_permission"), \
             unittest.mock.patch.object(manual_sync.sync_events, "record_manual_push_audit_event"), \
             unittest.mock.patch.object(manual_sync, "sync_sales_invoice", return_value={"status": "ok"}) as push:
            manual_sync.push_sales_invoice(INVOICE)

        self.assertTrue(push.call_args.kwargs["own_transaction"])

    def test_queued_return_sync_owns_its_transaction_and_inline_does_not(self):
        for force, expected in ((False, True), (True, False)):
            with self.subTest(force=force):
                credit_note = SimpleNamespace(name="ACC-SINV-RET-1", return_against=INVOICE)
                with unittest.mock.patch.object(outbound_sync, "_is_outbound_suppressed", return_value=False), \
                     unittest.mock.patch.object(outbound_sync.frappe, "enqueue") as enqueue:
                    outbound_sync._enqueue_invoice_return_sync(credit_note, method="on_submit", force=force)
                self.assertEqual(enqueue.call_args.kwargs["own_transaction"], expected)


class _Store404Client(_Client):
    """The order the invoice records was deleted from the store."""

    def __init__(self, gone_id, **kwargs):
        super().__init__(**kwargs)
        self.gone_id = gone_id

    def put(self, path, payload):
        if path == f"orders/{self.gone_id}":
            self.puts.append(path)
            raise outbound_sync.WooAPIError(404, path, "Invalid ID.")
        return super().put(path, payload)


class _NoIdReplyClient(_Client):
    def post(self, path, payload, **kwargs):
        self.posts.append(path)
        self.post_kwargs.append(kwargs)
        return {}


class _BlankLookupClient(_Client):
    def get(self, path, params=None):
        if path == "orders":
            self.lookups.append(dict(params or {}))
            return {}  # a blank 200 parses to {}
        return super().get(path, params)


class TestCreateRecoveryEdges(unittest.TestCase):
    """Follow-ups from the review of 83d3488."""

    def test_order_already_recorded_by_another_invoice_goes_to_review(self):
        db = _FakeDB(invoice_woo_order_id=None, other_owners={17863: "ACC-SINV-2026-18740"})
        client = _Client(store_orders=[_store_order(17863)])

        result, _ = _run_push(db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertEqual(result["detail"], outbound_sync.ORDER_OWNED_ELSEWHERE_DETAIL)
        self.assertEqual(result["conflict"], {17863: "ACC-SINV-2026-18740"})
        self.assertEqual((client.puts, client.posts), ([], []))
        self.assertEqual(sync_events._classify_text_reason(result["detail"]), "review")

    def test_blank_lookup_reply_is_not_read_as_no_order(self):
        db = _FakeDB(invoice_woo_order_id=None)
        client = _BlankLookupClient()

        result, _ = _run_push(db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertEqual(result["detail"], outbound_sync.ORDER_LOOKUP_FAILED_DETAIL)
        self.assertEqual(client.posts, [])

    def test_create_reply_without_an_id_is_in_doubt_not_synced(self):
        db = _FakeDB(invoice_woo_order_id=None)
        client = _NoIdReplyClient()

        result, mark_status = _run_push(db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertEqual(result["detail"], outbound_sync.ORDER_CREATE_IN_DOUBT_DETAIL)
        self.assertEqual(db.invoice_updates(), [])
        self.assertNotIn("Synced", [c.kwargs.get("status") for c in mark_status.call_args_list])

    def test_create_in_doubt_starts_a_hold_and_the_hold_blocks_a_new_create(self):
        db = _FakeDB(invoice_woo_order_id=None)
        client = _FailingCreateClient(outbound_sync.requests.Timeout("read timed out"))
        _run_push(db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD)
        db.noted_in_doubt.assert_called_once_with(INVOICE)

        # A push inside the hold (e.g. the manual button) finds nothing yet and
        # must not create: the lost request may still be saving on the store.
        db = _FakeDB(invoice_woo_order_id=None)
        client = _Client(created_id=18200)
        result, _ = _run_push(
            db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD, in_doubt_hold=True
        )
        self.assertEqual(result["detail"], outbound_sync.ORDER_CREATE_IN_DOUBT_DETAIL)
        self.assertEqual(client.posts, [])

    def test_hold_never_blocks_adopting_an_order_that_exists(self):
        db = _FakeDB(invoice_woo_order_id=None)
        client = _Client(store_orders=[_store_order(17863)])
        result, _ = _run_push(
            db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD, in_doubt_hold=True
        )
        self.assertEqual(result, {"status": "ok", "woo_order_id": 17863})

    def test_adoption_builds_the_update_from_the_full_order(self):
        db = _FakeDB(invoice_woo_order_id=None)
        client = _Client(store_orders=[_store_order(17863)])

        _run_push(db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertIn("orders/17863", client.gets, "the lookup reads id/status/meta only")
        last_build = db.build_calls[-1]
        self.assertFalse(last_build["is_create"])
        self.assertEqual(last_build["existing_order"]["id"], 17863)

    def test_an_open_order_beats_one_staff_cancelled(self):
        db = _FakeDB(invoice_woo_order_id=None)
        client = _Client(store_orders=[_store_order(17864), _store_order(17863, status="cancelled")])

        result, _ = _run_push(db, _Invoice(), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertEqual(result["woo_order_id"], 17864)

    def test_deleted_order_is_recreated_with_a_create_payload(self):
        db = _FakeDB(invoice_woo_order_id=17863)
        client = _Store404Client(17863, created_id=18300)

        result, _ = _run_push(db, _Invoice(woo_order_id=17863), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertEqual(result, {"status": "ok", "woo_order_id": 18300})
        self.assertEqual(client.posts, ["orders"])
        self.assertEqual(client.post_kwargs, [{"max_attempts": 1}])
        last_build = db.build_calls[-1]
        self.assertTrue(last_build["is_create"])
        self.assertIsNone(last_build["existing_order"])

    def test_deleted_order_hands_over_to_another_order_the_invoice_owns(self):
        db = _FakeDB(invoice_woo_order_id=17863)
        client = _Store404Client(17863, store_orders=[_store_order(17870), _store_order(17863)])

        result, _ = _run_push(db, _Invoice(woo_order_id=17863), client, own_transaction=True, payload=_CREATE_PAYLOAD)

        self.assertEqual(result, {"status": "ok", "woo_order_id": 17870})
        self.assertEqual(client.posts, [])
        self.assertEqual(client.puts, ["orders/17863", "orders/17870"])
        last_build = db.build_calls[-1]
        self.assertFalse(last_build["is_create"])
        self.assertEqual(last_build["existing_order"]["id"], 17870)
        self.assertEqual(db.invoice_updates()[0]["woo_order_id"], 17870)


class TestAmendedSourceNeverRevivesARetiredOrder(unittest.TestCase):
    def test_source_column_naming_a_retired_order_falls_through_to_the_map(self):
        amendment = SimpleNamespace(amended_from=INVOICE, get=lambda key, default=None: INVOICE)

        def get_value(doctype, filters, fieldname=None, *args, **kwargs):
            if doctype == "Sales Invoice":
                return 17865
            if doctype == "WooCommerce Order Map" and fieldname == "status":
                return "retired-duplicate" if filters.get("woo_order_id") == 17865 else "completed"
            return None

        db = SimpleNamespace(get_value=get_value, get_table_columns=lambda doctype: ["erpnext_sales_invoice"])
        map_rows = [
            {"woo_order_id": 17865, "status": "retired-duplicate"},
            {"woo_order_id": 17864, "status": "completed"},
        ]
        with unittest.mock.patch.object(outbound_sync.frappe, "db", db), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_all", return_value=map_rows), \
             unittest.mock.patch.object(outbound_sync, "_resolve_order_map_link_field", return_value="erpnext_sales_invoice"):
            recovered = outbound_sync._recover_amended_invoice_woo_order_id(amendment)

        self.assertEqual(recovered, 17864)
