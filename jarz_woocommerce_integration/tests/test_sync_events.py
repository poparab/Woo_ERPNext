import json
import unittest
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
		)

		with unittest.mock.patch.object(sync_events.WooCommerceSettings, "get_settings", return_value=SimpleNamespace()), \
			 unittest.mock.patch.object(sync_events, "get_sync_event_config", return_value=cfg):
			result = sync_events.process_due_sync_events()

		self.assertEqual(result, {"skipped": True, "reason": "worker_disabled"})