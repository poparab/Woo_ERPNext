from datetime import datetime, timedelta
from types import SimpleNamespace
import unittest

from jarz_woocommerce_integration.api import sync_events as sync_event_api


class TestSyncEventOperationsApi(unittest.TestCase):
	def test_set_review_state_records_operator_audit_fields(self):
		fixed_now = datetime(2024, 1, 2, 3, 4, 5)
		captured = {}

		with unittest.mock.patch.object(sync_event_api, "_require_sync_event_access", return_value=None), \
			 unittest.mock.patch.object(sync_event_api, "_set_event_fields", side_effect=lambda event_name, updates: captured.update({"event_name": event_name, "updates": updates})), \
			 unittest.mock.patch.object(sync_event_api, "now_datetime", return_value=fixed_now), \
			 unittest.mock.patch.object(sync_event_api.frappe, "session", SimpleNamespace(user="ops@example.com")):
			result = sync_event_api.set_review_state("WOOEVT-00021", "Resolved", "Handled")

		self.assertEqual(result, {"success": True, "event_name": "WOOEVT-00021", "review_state": "Resolved"})
		self.assertEqual(captured["event_name"], "WOOEVT-00021")
		self.assertEqual(captured["updates"]["last_operation"], "review_state:Resolved")
		self.assertEqual(captured["updates"]["last_operation_by"], "ops@example.com")
		self.assertEqual(captured["updates"]["last_operation_on"], fixed_now)
		self.assertEqual(captured["updates"]["review_state"], "Resolved")
		self.assertEqual(captured["updates"]["resolution_notes"], "Handled")

	def test_set_retention_policy_sets_retain_until(self):
		fixed_now = datetime(2024, 2, 1, 9, 30, 0)
		captured = {}

		with unittest.mock.patch.object(sync_event_api, "_require_sync_event_access", return_value=None), \
			 unittest.mock.patch.object(sync_event_api, "_set_event_fields", side_effect=lambda event_name, updates: captured.update({"event_name": event_name, "updates": updates})), \
			 unittest.mock.patch.object(sync_event_api, "now_datetime", return_value=fixed_now), \
			 unittest.mock.patch.object(sync_event_api.frappe, "session", SimpleNamespace(user="ops@example.com")):
			result = sync_event_api.set_retention_policy("WOOEVT-00022", retain=1, retain_days=45)

		self.assertEqual(result["success"], True)
		self.assertEqual(result["event_name"], "WOOEVT-00022")
		self.assertEqual(result["is_retention_exempt"], 1)
		self.assertEqual(captured["updates"]["last_operation"], "retention:enabled")
		self.assertEqual(captured["updates"]["is_retention_exempt"], 1)
		self.assertEqual(captured["updates"]["retain_until"], fixed_now + timedelta(days=45))

	def test_clear_outbound_breaker_releases_deferred_rows(self):
		sql_calls = []
		logger = SimpleNamespace(info=lambda *args, **kwargs: None)

		def sql_side_effect(query, params=None, as_dict=False):
			sql_calls.append((query, params, as_dict))
			if "ROW_COUNT" in query:
				return [{"row_count": 4}]
			return []

		with unittest.mock.patch.object(sync_event_api, "_require_sync_event_access", return_value=None), \
			 unittest.mock.patch.object(sync_event_api, "now_datetime", return_value=datetime(2024, 3, 1, 12, 0, 0)), \
			 unittest.mock.patch.object(sync_event_api.sync_events, "_clear_outbound_circuit_breaker") as clear_breaker, \
			 unittest.mock.patch.object(sync_event_api.frappe.db, "sql", side_effect=sql_side_effect), \
			 unittest.mock.patch.object(sync_event_api.frappe.db, "commit", return_value=None), \
			 unittest.mock.patch.object(sync_event_api.frappe, "logger", return_value=logger), \
			 unittest.mock.patch.object(sync_event_api.frappe, "session", SimpleNamespace(user="ops@example.com")):
			result = sync_event_api.clear_outbound_breaker()

		clear_breaker.assert_called_once_with()
		self.assertEqual(result, {"success": True, "released_rows": 4})
		self.assertIn("Paused by outbound circuit breaker%", sql_calls[0][0])