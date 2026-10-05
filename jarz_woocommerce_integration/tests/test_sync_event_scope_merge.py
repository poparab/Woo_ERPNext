"""Outbound customer events must not lose the scope of the events they supersede.

Before the fix, ``create_outbound_customer_event`` flipped every open
customer_push for the customer to ``Superseded`` regardless of scope. The POS
saving a new primary shipping address enqueues ``scope="shipping"`` (Address
insert) and then ``scope="core"`` (Customer save); "core" superseded "shipping"
and Woo kept the old shipping address (staging WOOEVT-501108 -> WOOEVT-501110).

These tests drive the real ``create_outbound_customer_event`` against a small
in-memory ledger that stands in for ``tabWooCommerce Sync Event``.
"""

import json
import unittest
import unittest.mock
from datetime import datetime, timedelta
from types import SimpleNamespace

from jarz_woocommerce_integration.services import outbound_sync, sync_events

NOW = datetime(2026, 10, 5, 12, 0, 0)
MODIFIED = "2026-10-05 11:59:00"
CUSTOMER = "CUST-SCOPE-001"


class _FakeLedger:
	"""In-memory stand-in for the Sync Event table.

	The SELECT deliberately returns every row for the customer (it does not apply
	the status/lock filters itself), so the Python-side re-check in
	``_collect_supersedable_customer_events`` is what keeps a locked or finished
	row out of the merge; the tests also assert the SQL carries the same clauses.
	The UPDATE applies the real ``_supersede_pending_events`` semantics.
	"""

	def __init__(self, rows):
		self.rows = {row["name"]: dict(row) for row in rows}
		self.select_queries = []
		self.update_queries = []
		self.created = []
		self.existing_on_insert = None

	def sql(self, query, params=None, as_dict=False):
		text = query.strip()
		if text.upper().startswith("SELECT"):
			self.select_queries.append((text, params))
			source_id = params[2]
			return [
				{
					"name": row["name"],
					"status": row["status"],
					"locked_until": row.get("locked_until"),
					"payload_json": row.get("payload_json"),
				}
				for row in self.rows.values()
				if row.get("source_id") == source_id
			]
		if text.upper().startswith("UPDATE"):
			self.update_queries.append((text, params))
			completed_on, _direction, _object_type, source_id, exclude, now = params[:6]
			names = set(params[8:]) if "name IN" in text else None
			for row in self.rows.values():
				if row.get("source_id") != source_id or row["name"] == exclude:
					continue
				if row["status"] not in ("Pending", "RetryScheduled"):
					continue
				locked_until = row.get("locked_until")
				if locked_until is not None and not locked_until < now:
					continue
				if names is not None and row["name"] not in names:
					continue
				row["status"] = "Superseded"
				row["completed_on"] = completed_on
			return []
		raise AssertionError(f"unexpected SQL: {text[:80]}")

	def create_sync_event(self, **kwargs):
		self.created.append(kwargs)
		if self.existing_on_insert is not None:
			# Duplicate idempotency key: create_sync_event returns the existing row.
			return self.existing_on_insert
		name = f"WOOEVT-NEW-{len(self.created)}"
		payload_text = sync_events._to_json(kwargs["payload_json"])
		self.rows[name] = {
			"name": name,
			"source_id": kwargs["source_id"],
			"status": "Pending",
			"locked_until": None,
			"payload_json": payload_text,
		}
		return SimpleNamespace(name=name, status="Pending", locked_until=None, payload_json=payload_text)


def _row(name, *, scope=None, payload_json=None, status="Pending", locked_until=None, force=False):
	if payload_json is None:
		payload_json = json.dumps({"customer_name": CUSTOMER, "reason": "event", "scope": scope, "force": force})
	return {
		"name": name,
		"source_id": CUSTOMER,
		"status": status,
		"locked_until": locked_until,
		"payload_json": payload_json,
	}


class TestOutboundCustomerScopeMerge(unittest.TestCase):
	def _run(self, ledger, *, scope, force=False):
		with unittest.mock.patch.object(sync_events, "now_datetime", return_value=NOW), \
			 unittest.mock.patch.object(sync_events, "_get_doc_modified", return_value=MODIFIED), \
			 unittest.mock.patch.object(sync_events, "create_sync_event", side_effect=ledger.create_sync_event), \
			 unittest.mock.patch.object(sync_events.frappe.db, "sql", side_effect=ledger.sql), \
			 unittest.mock.patch.object(sync_events.frappe.db, "get_value", return_value="3095"):
			return sync_events.create_outbound_customer_event(CUSTOMER, reason="on_update", scope=scope, force=force)

	@staticmethod
	def _created_scope(ledger):
		return ledger.created[-1]["payload_json"]["scope"]

	def test_pending_shipping_plus_new_core_merges_both_and_supersedes_old(self):
		ledger = _FakeLedger([_row("WOOEVT-501108", scope="shipping")])

		event = self._run(ledger, scope="core")

		merged = self._created_scope(ledger)
		self.assertEqual(outbound_sync._normalize_customer_sync_scopes(merged), {"core", "shipping"})
		self.assertEqual(merged, "core,shipping")
		self.assertEqual(
			ledger.created[-1]["idempotency_key"],
			f"out:erp:Customer:{CUSTOMER}:core,shipping:{MODIFIED}",
		)
		self.assertEqual(ledger.created[-1]["payload_json"]["merged_from"], ["WOOEVT-501108"])
		self.assertEqual(ledger.rows["WOOEVT-501108"]["status"], "Superseded")
		self.assertEqual(ledger.rows[event.name]["status"], "Pending")
		# The candidate read mirrors _supersede_pending_events' WHERE clause.
		select_sql = ledger.select_queries[0][0]
		self.assertIn("status IN ('Pending', 'RetryScheduled')", select_sql)
		self.assertIn("locked_until IS NULL OR locked_until < %s", select_sql)
		self.assertIn("local_doctype = %s", select_sql)
		self.assertIn("local_docname = %s", select_sql)
		# The supersede is restricted to the rows whose scope was merged.
		self.assertIn("name IN", ledger.update_queries[0][0])

	def test_pending_full_plus_new_core_merges_to_full(self):
		ledger = _FakeLedger([_row("WOOEVT-1", scope=None)])

		self._run(ledger, scope="core")

		self.assertIsNone(self._created_scope(ledger))
		self.assertEqual(ledger.created[-1]["idempotency_key"], f"out:erp:Customer:{CUSTOMER}:full:{MODIFIED}")
		self.assertEqual(ledger.rows["WOOEVT-1"]["status"], "Superseded")

	def test_pending_core_plus_new_full_stays_full(self):
		ledger = _FakeLedger([_row("WOOEVT-2", scope="core")])

		self._run(ledger, scope=None)

		self.assertIsNone(self._created_scope(ledger))
		self.assertEqual(ledger.rows["WOOEVT-2"]["status"], "Superseded")

	def test_locked_pending_event_is_neither_merged_nor_superseded(self):
		ledger = _FakeLedger([
			_row("WOOEVT-LOCKED", scope="shipping", locked_until=NOW + timedelta(minutes=5)),
		])

		self._run(ledger, scope="core")

		self.assertEqual(self._created_scope(ledger), "core")
		self.assertNotIn("merged_from", ledger.created[-1]["payload_json"])
		self.assertEqual(ledger.rows["WOOEVT-LOCKED"]["status"], "Pending")

	def test_expired_lock_counts_as_unlocked(self):
		ledger = _FakeLedger([
			_row("WOOEVT-STALE", scope="shipping", locked_until=NOW - timedelta(seconds=1)),
		])

		self._run(ledger, scope="core")

		self.assertEqual(self._created_scope(ledger), "core,shipping")
		self.assertEqual(ledger.rows["WOOEVT-STALE"]["status"], "Superseded")

	def test_finished_events_are_not_merged(self):
		ledger = _FakeLedger([
			_row("WOOEVT-DONE", scope="shipping", status="Succeeded"),
			_row("WOOEVT-RUNNING", scope="territory", status="Processing"),
		])

		self._run(ledger, scope="core")

		self.assertEqual(self._created_scope(ledger), "core")
		self.assertEqual(ledger.rows["WOOEVT-DONE"]["status"], "Succeeded")
		self.assertEqual(ledger.rows["WOOEVT-RUNNING"]["status"], "Processing")

	def test_malformed_payload_is_treated_as_full_push(self):
		# Full (None) is the safe reading: the worker itself would parse this row
		# with _from_json -> {"raw": ...}, find no scope and push the whole
		# customer, so merging None preserves exactly what the row would have done.
		ledger = _FakeLedger([_row("WOOEVT-BAD", payload_json="{not json")])

		self._run(ledger, scope="core")

		self.assertIsNone(self._created_scope(ledger))
		self.assertEqual(ledger.rows["WOOEVT-BAD"]["status"], "Superseded")

	def test_non_string_scope_is_treated_as_full_push(self):
		ledger = _FakeLedger([_row("WOOEVT-ODD", payload_json=json.dumps({"scope": ["shipping"]}))])

		self._run(ledger, scope="core")

		self.assertIsNone(self._created_scope(ledger))

	def test_dict_payload_json_is_read(self):
		ledger = _FakeLedger([_row("WOOEVT-DICT", payload_json={"scope": "territory", "force": False})])

		self._run(ledger, scope="core")

		self.assertEqual(self._created_scope(ledger), "core,territory")

	def test_three_scopes_merge_across_several_pending_events(self):
		ledger = _FakeLedger([
			_row("WOOEVT-A", scope="shipping"),
			_row("WOOEVT-B", scope="territory", status="RetryScheduled"),
		])

		self._run(ledger, scope="core")

		self.assertEqual(self._created_scope(ledger), "core,shipping,territory")
		self.assertEqual(ledger.rows["WOOEVT-A"]["status"], "Superseded")
		self.assertEqual(ledger.rows["WOOEVT-B"]["status"], "Superseded")

	def test_force_from_a_superseded_event_is_kept(self):
		ledger = _FakeLedger([_row("WOOEVT-F", scope="shipping", force=True)])

		self._run(ledger, scope="core", force=False)

		self.assertTrue(ledger.created[-1]["payload_json"]["force"])

	def test_no_pending_events_keeps_the_original_key_and_runs_no_update(self):
		ledger = _FakeLedger([])

		self._run(ledger, scope="core")

		self.assertEqual(self._created_scope(ledger), "core")
		self.assertEqual(ledger.created[-1]["idempotency_key"], f"out:erp:Customer:{CUSTOMER}:core:{MODIFIED}")
		self.assertEqual(ledger.update_queries, [])

	def test_duplicate_key_returning_finished_event_does_not_supersede_pending(self):
		# The idempotency key resolved to a row that already ran. It will never
		# push again, so the pending shipping event must survive to push itself.
		ledger = _FakeLedger([_row("WOOEVT-SHIP", scope="shipping")])
		ledger.existing_on_insert = SimpleNamespace(
			name="WOOEVT-OLD",
			status="Succeeded",
			locked_until=None,
			payload_json=json.dumps({"scope": "core,shipping"}),
		)

		event = self._run(ledger, scope="core")

		self.assertEqual(event.name, "WOOEVT-OLD")
		self.assertEqual(ledger.rows["WOOEVT-SHIP"]["status"], "Pending")
		self.assertEqual(ledger.update_queries, [])

	def test_duplicate_key_returning_open_covering_event_supersedes_the_rest(self):
		# The returned row is itself one of the pending candidates and already
		# carries the merged scope: it stays Pending, the others are superseded.
		ledger = _FakeLedger([
			_row("WOOEVT-OPEN", scope="core,shipping"),
			_row("WOOEVT-SHIP", scope="shipping"),
		])
		ledger.existing_on_insert = SimpleNamespace(
			name="WOOEVT-OPEN",
			status="Pending",
			locked_until=None,
			payload_json=ledger.rows["WOOEVT-OPEN"]["payload_json"],
		)

		self._run(ledger, scope="core")

		self.assertEqual(ledger.rows["WOOEVT-OPEN"]["status"], "Pending")
		self.assertEqual(ledger.rows["WOOEVT-SHIP"]["status"], "Superseded")

	def test_duplicate_key_returning_narrower_event_does_not_supersede(self):
		# Legacy rows can carry a scope narrower than the key suggests; if the
		# returned row would not push everything, nothing is superseded.
		ledger = _FakeLedger([_row("WOOEVT-SHIP", scope="shipping")])
		ledger.existing_on_insert = SimpleNamespace(
			name="WOOEVT-NARROW",
			status="Pending",
			locked_until=None,
			payload_json=json.dumps({"scope": "core"}),
		)

		self._run(ledger, scope="core")

		self.assertEqual(ledger.rows["WOOEVT-SHIP"]["status"], "Pending")


class TestSupersedeOnlyNames(unittest.TestCase):
	def test_empty_only_names_runs_no_sql(self):
		with unittest.mock.patch.object(sync_events.frappe.db, "sql") as sql:
			sync_events._supersede_pending_events(
				direction="Outbound",
				object_type="Customer",
				source_id=CUSTOMER,
				exclude_event_name="WOOEVT-NEW",
				local_doctype="Customer",
				local_docname=CUSTOMER,
				only_names=["WOOEVT-NEW"],
			)
		sql.assert_not_called()

	def test_without_only_names_query_is_unrestricted(self):
		with unittest.mock.patch.object(sync_events, "now_datetime", return_value=NOW), \
			 unittest.mock.patch.object(sync_events.frappe.db, "sql") as sql:
			sync_events._supersede_pending_events(
				direction="Outbound",
				object_type="Sales Invoice",
				source_id="ACC-SINV-1",
				exclude_event_name="WOOEVT-NEW",
				local_doctype="Sales Invoice",
				local_docname="ACC-SINV-1",
			)
		query, params = sql.call_args.args
		self.assertNotIn("name IN", query)
		self.assertEqual(
			params,
			(NOW, "Outbound", "Sales Invoice", "ACC-SINV-1", "WOOEVT-NEW", NOW, "Sales Invoice", "ACC-SINV-1"),
		)


class TestMergedScopeReachesTheWooPayload(unittest.TestCase):
	"""The consumer must understand the serializer's multi-scope format."""

	def test_merged_scope_round_trips_through_the_consumer_parser(self):
		merged = sync_events._merge_customer_sync_scopes("shipping", "core")
		self.assertEqual(merged, "core,shipping")
		self.assertEqual(outbound_sync._normalize_customer_sync_scopes(merged), {"core", "shipping"})

	def test_core_and_shipping_payload_carries_identity_and_new_shipping_address(self):
		customer = SimpleNamespace(
			name=CUSTOMER,
			customer_name="Test Customer",
			email_id="test@example.com",
			mobile_no="01000000000",
			phone=None,
		)

		def fake_get_address_payload(address_name, **kwargs):
			if address_name == "ADDR-SHIP-NEW":
				return {"address_1": "New Shipping Line", "phone": "01000000000"}
			return {"address_1": "Billing Line", "phone": "01000000000"}

		with unittest.mock.patch.object(outbound_sync, "_resolve_customer_billing_address_name", return_value="ADDR-BILL"), \
			 unittest.mock.patch.object(outbound_sync, "_resolve_customer_shipping_address_name", return_value="ADDR-SHIP-NEW"), \
			 unittest.mock.patch.object(outbound_sync, "_get_address_payload", side_effect=fake_get_address_payload), \
			 unittest.mock.patch.object(outbound_sync, "_build_customer_metadata", return_value=[]):
			payload = outbound_sync._build_customer_payload(customer, scope="core,shipping")

		self.assertEqual(payload["email"], "test@example.com")
		self.assertIn("first_name", payload)
		self.assertEqual(payload["shipping"]["address_1"], "New Shipping Line")
		self.assertEqual(payload["shipping"]["email"], "test@example.com")
		self.assertNotIn("address_1", payload.get("billing", {}))
		self.assertNotIn("meta_data", payload)


if __name__ == "__main__":
	unittest.main()
