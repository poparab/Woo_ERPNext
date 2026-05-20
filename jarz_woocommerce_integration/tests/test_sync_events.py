import json
import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace

from jarz_woocommerce_integration.services import sync_events


def _event_doc(**overrides):
	updates = []
	doc = SimpleNamespace(
		name="WOOEVT-00001",
		direction="Outbound",
		object_type="Customer",
		payload_json=json.dumps({"reason": "event", "scope": "full", "force": False}),
		local_docname="CUST-TEST-001",
		source_id="CUST-TEST-001",
		woo_order_id=None,
		attempt_count=1,
		max_attempts=8,
		db_set=lambda values, update_modified=False: updates.append(values),
	)
	for key, value in overrides.items():
		setattr(doc, key, value)
	return doc, updates


class TestSyncEvents(unittest.TestCase):
	def test_process_sync_event_outbound_success_marks_succeeded(self):
		event_doc, updates = _event_doc()

		with unittest.mock.patch.object(sync_events, "_claim_sync_event", return_value=event_doc), \
			 unittest.mock.patch.object(sync_events, "_dispatch_outbound_event", return_value={"status": "ok", "woo_customer_id": 77}), \
			 unittest.mock.patch.object(sync_events.frappe.db, "commit", return_value=None):
			result = sync_events.process_sync_event(event_doc.name)

		self.assertEqual(result["status"], "succeeded")
		self.assertTrue(any(update.get("status") == "Succeeded" for update in updates))

	def test_process_sync_event_retries_locked_order(self):
		event_doc, updates = _event_doc(
			direction="Inbound",
			object_type="Order",
			payload_json=json.dumps({"id": 123}),
			source_id="123",
			woo_order_id=123,
		)

		with unittest.mock.patch.object(sync_events, "_claim_sync_event", return_value=event_doc), \
			 unittest.mock.patch.object(sync_events, "_dispatch_inbound_event", return_value={"status": "skipped", "reason": "locked"}), \
			 unittest.mock.patch.object(sync_events.frappe.db, "commit", return_value=None):
			result = sync_events.process_sync_event(event_doc.name)

		self.assertEqual(result["status"], "retry")
		self.assertTrue(any(update.get("status") == "RetryScheduled" for update in updates))

	def test_process_due_sync_events_skips_when_worker_disabled(self):
		cfg = sync_events.SyncEventConfig(
			enabled=True,
			shadow_mode=False,
			use_outbox_for_customer_push=False,
			use_outbox_for_invoice_push=False,
			use_inbox_for_order_webhook=False,
			use_inbox_for_customer_webhook=False,
			use_inbox_for_order_polling=False,
			use_event_reconciliation=False,
			worker_enabled=False,
			max_attempts=8,
			batch_size=25,
			success_retention_days=90,
			lock_ttl_seconds=900,
			shadow_alert_threshold=5,
			circuit_breaker_threshold=10,
			circuit_breaker_window_seconds=300,
			circuit_breaker_cooldown_seconds=300,
		)

		with unittest.mock.patch.object(sync_events.WooCommerceSettings, "get_settings", return_value=SimpleNamespace()), \
			 unittest.mock.patch.object(sync_events, "get_sync_event_config", return_value=cfg):
			result = sync_events.process_due_sync_events()

		self.assertEqual(result, {"skipped": True, "reason": "worker_disabled"})

	def test_claim_due_sync_events_uses_skip_locked_and_lock_ttl(self):
		fixed_now = datetime(2024, 1, 2, 3, 4, 5)
		settings = SimpleNamespace()
		cfg = sync_events.SyncEventConfig(
			enabled=True,
			shadow_mode=False,
			use_outbox_for_customer_push=False,
			use_outbox_for_invoice_push=False,
			use_inbox_for_order_webhook=False,
			use_inbox_for_customer_webhook=False,
			use_inbox_for_order_polling=False,
			use_event_reconciliation=False,
			worker_enabled=True,
			max_attempts=8,
			batch_size=25,
			success_retention_days=90,
			lock_ttl_seconds=321,
			shadow_alert_threshold=5,
			circuit_breaker_threshold=10,
			circuit_breaker_window_seconds=300,
			circuit_breaker_cooldown_seconds=300,
		)
		sql_calls = []

		def sql_side_effect(query, params=None, as_dict=False):
			sql_calls.append((query, params, as_dict))
			if "FOR UPDATE SKIP LOCKED" in query:
				return [{"name": "WOOEVT-00031"}]
			return []

		with unittest.mock.patch.object(sync_events, "now_datetime", return_value=fixed_now), \
			 unittest.mock.patch.object(sync_events, "get_sync_event_config", return_value=cfg), \
			 unittest.mock.patch.object(sync_events.frappe.db, "sql", side_effect=sql_side_effect), \
			 unittest.mock.patch.object(sync_events.frappe.db, "commit", return_value=None), \
			 unittest.mock.patch.object(sync_events.frappe, "get_doc", side_effect=lambda doctype, name: SimpleNamespace(name=name)):
			rows = sync_events._claim_due_sync_events(batch_size=2, settings=settings)

		self.assertEqual(rows[0].name, "WOOEVT-00031")
		self.assertIn("FOR UPDATE SKIP LOCKED", sql_calls[0][0])
		self.assertEqual(sql_calls[1][1][1], fixed_now + timedelta(seconds=321))

	def test_process_sync_event_defers_outbound_when_breaker_open(self):
		open_until = datetime(2024, 1, 2, 4, 0, 0)

		with unittest.mock.patch.object(sync_events.WooCommerceSettings, "get_settings", return_value=SimpleNamespace()), \
			 unittest.mock.patch.object(sync_events, "_get_outbound_circuit_breaker_open_until", return_value=open_until), \
			 unittest.mock.patch.object(sync_events.frappe.db, "get_value", return_value={"name": "WOOEVT-00032", "direction": "Outbound", "status": "Pending"}), \
			 unittest.mock.patch.object(sync_events, "_defer_single_event_for_breaker") as defer_event, \
			 unittest.mock.patch.object(sync_events, "_claim_sync_event") as claim_event:
			result = sync_events.process_sync_event("WOOEVT-00032")

		claim_event.assert_not_called()
		defer_event.assert_called_once_with("WOOEVT-00032", open_until)
		self.assertEqual(result["reason"], "breaker_open")

	def test_process_due_sync_events_defers_outbound_batches_while_breaker_open(self):
		open_until = datetime(2024, 1, 2, 5, 0, 0)
		settings = SimpleNamespace()
		cfg = sync_events.SyncEventConfig(
			enabled=True,
			shadow_mode=False,
			use_outbox_for_customer_push=False,
			use_outbox_for_invoice_push=False,
			use_inbox_for_order_webhook=False,
			use_inbox_for_customer_webhook=False,
			use_inbox_for_order_polling=False,
			use_event_reconciliation=False,
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

		with unittest.mock.patch.object(sync_events.WooCommerceSettings, "get_settings", return_value=settings), \
			 unittest.mock.patch.object(sync_events, "get_sync_event_config", return_value=cfg), \
			 unittest.mock.patch.object(sync_events, "_recover_stale_processing_events", return_value=2), \
			 unittest.mock.patch.object(sync_events, "_get_outbound_circuit_breaker_open_until", return_value=open_until), \
			 unittest.mock.patch.object(sync_events, "_defer_outbound_events_for_breaker", return_value=4), \
			 unittest.mock.patch.object(sync_events, "_claim_due_sync_events", return_value=[] ) as claim_events:
			result = sync_events.process_due_sync_events()

		claim_events.assert_called_once_with(batch_size=25, settings=settings, allow_outbound=False)
		self.assertEqual(result["processed"], 0)
		self.assertEqual(result["recovered_stale"], 2)
		self.assertEqual(result["deferred_by_breaker"], 4)
		self.assertEqual(result["breaker_open_until"], open_until.isoformat())

	def test_record_manual_push_audit_event_marks_failed_terminal_status(self):
		updates = []
		fake_event = SimpleNamespace(db_set=lambda values, update_modified=False: updates.append(values))

		with unittest.mock.patch.object(sync_events, "create_sync_event", return_value=fake_event) as create_event:
			sync_events.record_manual_push_audit_event(
				object_type="Sales Invoice",
				docname="SINV-0001",
				error="boom",
			)

		self.assertEqual(create_event.call_args.kwargs["status"], "Failed")
		self.assertEqual(create_event.call_args.kwargs["event_type"], "manual_push")
		self.assertTrue(any(update.get("manual_review_reason") == "manual_push_failed" for update in updates))

	def test_create_sync_event_reuses_existing_row_on_unique_validation_duplicate(self):
		cfg = sync_events.SyncEventConfig(
			enabled=True,
			shadow_mode=False,
			use_outbox_for_customer_push=False,
			use_outbox_for_invoice_push=False,
			use_inbox_for_order_webhook=False,
			use_inbox_for_customer_webhook=False,
			use_inbox_for_order_polling=False,
			use_event_reconciliation=False,
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
		duplicate_exc = sync_events.frappe.UniqueValidationError(
			"WooCommerce Sync Event",
			"WOOEVT-00099",
			Exception("duplicate idempotency key"),
		)
		fake_local = SimpleNamespace(message_log=[{"message": "preexisting"}])

		def insert_side_effect(ignore_permissions=True):
			fake_local.message_log.append({"message": "Idempotency Key must be unique"})
			raise duplicate_exc

		insert_doc = SimpleNamespace(insert=insert_side_effect)
		existing_doc = SimpleNamespace(name="WOOEVT-00005")

		def fake_get_doc(*args, **kwargs):
			if len(args) == 1 and isinstance(args[0], dict):
				return insert_doc
			if len(args) == 2 and args[0] == sync_events.EVENT_DOCTYPE and args[1] == "WOOEVT-00005":
				return existing_doc
			raise AssertionError(f"Unexpected get_doc args: {args!r}")

		with unittest.mock.patch.object(sync_events.WooCommerceSettings, "get_settings", return_value=SimpleNamespace()), \
			 unittest.mock.patch.object(sync_events, "get_sync_event_config", return_value=cfg), \
			 unittest.mock.patch.object(sync_events, "now_datetime", return_value=datetime(2024, 1, 2, 3, 4, 5)), \
			 unittest.mock.patch.object(sync_events.frappe, "local", fake_local), \
			 unittest.mock.patch.object(sync_events.frappe, "get_doc", side_effect=fake_get_doc), \
			 unittest.mock.patch.object(sync_events.frappe.db, "get_value", return_value="WOOEVT-00005"):
			result = sync_events.create_sync_event(
				direction="Outbound",
				event_type="customer_push",
				source_system="ERPNext",
				target_system="WooCommerce",
				object_type="Customer",
				source_id="CUST-001",
				idempotency_key="out:erp:Customer:CUST-001:full:2024-01-02T03:04:05",
			)

		self.assertIs(result, existing_doc)
		self.assertEqual(fake_local.message_log, [{"message": "preexisting"}])