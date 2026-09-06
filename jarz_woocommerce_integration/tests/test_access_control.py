"""Access-control gates for the Woo integration's whitelisted endpoints.

Before these gates existed, every whitelisted endpoint in this app except
``api/sync_events.py`` had no role check beyond Frappe's default "must be
logged in" — including ``services/customer_dedupe.run_dedupe``, which calls
``frappe.rename_doc(..., merge=True, ignore_permissions=True)`` and can
permanently merge and delete Customer records site-wide.

These tests cover the shared gates in ``services/access.py`` directly, the
preserved fallback behaviour of ``api/sync_events._require_sync_event_access``
after its refactor to use the shared helper, and a representative sample of
the endpoints newly gated across the app (one per module/tier is enough to
prove the guard is wired in as the first line of the function body — the
gate's own logic is fully covered by the ``TestEnsureOperatorAccess`` /
``TestEnsureSystemManager`` cases below).
"""

from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import patch

import frappe

from jarz_woocommerce_integration.api import customers as customers_api
from jarz_woocommerce_integration.api import geo as geo_api
from jarz_woocommerce_integration.api import sync_events as sync_event_api
from jarz_woocommerce_integration.api import territories as territories_api
from jarz_woocommerce_integration.constants import ROLES
from jarz_woocommerce_integration.services import access, customer_dedupe


def _session(user: str):
    return SimpleNamespace(user=user)


class TestEnsureOperatorAccess(unittest.TestCase):
    def test_administrator_is_always_allowed(self):
        with patch.object(access.frappe, "session", _session("Administrator")), \
             patch.object(access.frappe, "get_roles", return_value=[]):
            access.ensure_operator_access()  # must not raise

    def test_system_manager_role_is_allowed(self):
        with patch.object(access.frappe, "session", _session("manager@example.com")), \
             patch.object(access.frappe, "get_roles", return_value=["System Manager"]):
            access.ensure_operator_access()  # must not raise

    def test_sync_operator_role_is_allowed(self):
        with patch.object(access.frappe, "session", _session("ops@example.com")), \
             patch.object(access.frappe, "get_roles", return_value=["WooCommerce Sync Operator"]):
            access.ensure_operator_access()  # must not raise

    def test_unprivileged_user_is_refused(self):
        with patch.object(access.frappe, "session", _session("random@example.com")), \
             patch.object(access.frappe, "get_roles", return_value=["Sales User"]):
            with self.assertRaises(frappe.PermissionError):
                access.ensure_operator_access()

    def test_no_roles_at_all_is_refused(self):
        with patch.object(access.frappe, "session", _session("random@example.com")), \
             patch.object(access.frappe, "get_roles", return_value=None):
            with self.assertRaises(frappe.PermissionError):
                access.ensure_operator_access()


class TestEnsureSystemManager(unittest.TestCase):
    def test_administrator_is_always_allowed(self):
        with patch.object(access.frappe, "session", _session("Administrator")), \
             patch.object(access.frappe, "get_roles", return_value=[]):
            access.ensure_system_manager()  # must not raise

    def test_system_manager_role_is_allowed(self):
        with patch.object(access.frappe, "session", _session("manager@example.com")), \
             patch.object(access.frappe, "get_roles", return_value=["System Manager"]):
            access.ensure_system_manager()  # must not raise

    def test_sync_operator_role_alone_is_refused(self):
        """The stricter gate: operator tier is not enough for a destructive op."""
        with patch.object(access.frappe, "session", _session("ops@example.com")), \
             patch.object(access.frappe, "get_roles", return_value=["WooCommerce Sync Operator"]):
            with self.assertRaises(frappe.PermissionError):
                access.ensure_system_manager()

    def test_unprivileged_user_is_refused(self):
        with patch.object(access.frappe, "session", _session("random@example.com")), \
             patch.object(access.frappe, "get_roles", return_value=[]):
            with self.assertRaises(frappe.PermissionError):
                access.ensure_system_manager()


class TestRolesConstants(unittest.TestCase):
    def test_operator_set_membership(self):
        self.assertEqual(ROLES.OPERATOR, {"System Manager", "WooCommerce Sync Operator"})
        self.assertIn(ROLES.SYSTEM_MANAGER, ROLES.OPERATOR)
        self.assertIn(ROLES.SYNC_OPERATOR, ROLES.OPERATOR)


class TestSyncEventsFallbackPreserved(unittest.TestCase):
    """`_require_sync_event_access` must keep its `frappe.has_permission`
    fallback branch exactly as it behaved before the refactor to the shared
    `services.access` helper."""

    def test_administrator_short_circuits_before_has_permission(self):
        has_permission = unittest.mock.MagicMock(return_value=False)
        with patch.object(sync_event_api.frappe, "session", _session("Administrator")), \
             patch.object(sync_event_api.frappe, "has_permission", has_permission):
            sync_event_api._require_sync_event_access()
        has_permission.assert_not_called()

    def test_operator_role_short_circuits_before_has_permission(self):
        has_permission = unittest.mock.MagicMock(return_value=False)
        with patch.object(sync_event_api.frappe, "session", _session("ops@example.com")), \
             patch.object(sync_event_api.frappe, "get_roles", return_value=["WooCommerce Sync Operator"]), \
             patch.object(sync_event_api.frappe, "has_permission", has_permission):
            sync_event_api._require_sync_event_access(write=True)
        has_permission.assert_not_called()

    def test_no_role_falls_back_to_has_permission_and_is_allowed(self):
        with patch.object(sync_event_api.frappe, "session", _session("user@example.com")), \
             patch.object(sync_event_api.frappe, "get_roles", return_value=["Sales User"]), \
             patch.object(sync_event_api.frappe, "has_permission", return_value=True) as has_permission:
            sync_event_api._require_sync_event_access(write=True)
        has_permission.assert_called_once_with(sync_event_api.EVENT_DOCTYPE, ptype="write")

    def test_no_role_and_no_doctype_permission_is_refused(self):
        with patch.object(sync_event_api.frappe, "session", _session("user@example.com")), \
             patch.object(sync_event_api.frappe, "get_roles", return_value=[]), \
             patch.object(sync_event_api.frappe, "has_permission", return_value=False):
            with self.assertRaises(frappe.PermissionError):
                sync_event_api._require_sync_event_access()

    def test_read_fallback_checks_read_permission_not_write(self):
        with patch.object(sync_event_api.frappe, "session", _session("user@example.com")), \
             patch.object(sync_event_api.frappe, "get_roles", return_value=[]), \
             patch.object(sync_event_api.frappe, "has_permission", return_value=True) as has_permission:
            sync_event_api._require_sync_event_access()
        has_permission.assert_called_once_with(sync_event_api.EVENT_DOCTYPE, ptype="read")


class TestCustomerDedupeGates(unittest.TestCase):
    def test_run_dedupe_refuses_a_plain_operator(self):
        """The most serious pre-existing hole: run_dedupe had no gate at all."""
        with patch.object(customer_dedupe.frappe, "session", _session("ops@example.com")), \
             patch.object(customer_dedupe.frappe, "get_roles", return_value=["WooCommerce Sync Operator"]):
            with self.assertRaises(frappe.PermissionError):
                customer_dedupe.run_dedupe(apply=True)

    def test_run_dedupe_allows_system_manager(self):
        with patch.object(customer_dedupe.frappe, "session", _session("manager@example.com")), \
             patch.object(customer_dedupe.frappe, "get_roles", return_value=["System Manager"]), \
             patch.object(customer_dedupe, "build_plan", return_value={"auto": [], "review": []}):
            result = customer_dedupe.run_dedupe(apply=False)
        self.assertEqual(result["summary"]["groups_considered"], 0)

    def test_review_report_refuses_unprivileged_user(self):
        with patch.object(customer_dedupe.frappe, "session", _session("random@example.com")), \
             patch.object(customer_dedupe.frappe, "get_roles", return_value=[]):
            with self.assertRaises(frappe.PermissionError):
                customer_dedupe.review_report()

    def test_review_report_allows_operator_and_shapes_candidates(self):
        review_group = {
            "phone": "01000000000",
            "size": 2,
            "reason": "different names and no exclusively-shared woo_customer_id",
            "members": [
                {
                    "name": "CUST-A", "customer_name": "Customer A",
                    "email_id": "a@example.com", "creation": "2026-01-01 00:00:00",
                    "disabled": 0, "woo_customer_id": "111",
                    "stats": {"submitted": 3, "draft": 1, "revenue": 450.0},
                },
                {
                    "name": "CUST-B", "customer_name": "Customer B",
                    "email_id": "", "creation": "2026-02-01 00:00:00",
                    "disabled": 0, "woo_customer_id": "",
                    "stats": {"submitted": 0, "draft": 0, "revenue": 0.0},
                },
            ],
        }
        with patch.object(customer_dedupe.frappe, "session", _session("ops@example.com")), \
             patch.object(customer_dedupe.frappe, "get_roles", return_value=["WooCommerce Sync Operator"]), \
             patch.object(customer_dedupe, "build_plan", return_value={"auto": [], "review": [review_group]}):
            review = customer_dedupe.review_report()

        self.assertEqual(len(review), 1)
        group = review[0]
        self.assertEqual(group["group_id"], "01000000000")
        self.assertEqual(group["phone"], "01000000000")
        self.assertEqual(len(group["candidates"]), 2)
        self.assertEqual(group["members"], review_group["members"])  # unchanged, back-compat

        candidate_a = group["candidates"][0]
        self.assertEqual(candidate_a["name"], "CUST-A")
        self.assertEqual(candidate_a["customer_name"], "Customer A")
        self.assertEqual(candidate_a["phone"], "01000000000")
        self.assertEqual(candidate_a["email"], "a@example.com")
        self.assertEqual(candidate_a["created"], "2026-01-01 00:00:00")
        self.assertEqual(candidate_a["invoice_count"], 4)
        self.assertEqual(candidate_a["submitted_invoice_count"], 3)
        self.assertEqual(candidate_a["revenue"], 450.0)

        candidate_b = group["candidates"][1]
        self.assertEqual(candidate_b["email"], "")
        self.assertEqual(candidate_b["invoice_count"], 0)


class TestSpotCheckOperatorGatedEndpoints(unittest.TestCase):
    """One endpoint per module: proves the gate runs as the first statement,
    before any doctype/network access — the gate raises before the function
    can reach anything else, so patching only `frappe.session`/`get_roles`
    (no other collaborator) is sufficient to observe the refusal."""

    def test_customers_sync_all_refuses_unprivileged_user(self):
        with patch.object(customers_api.frappe, "session", _session("random@example.com")), \
             patch.object(customers_api.frappe, "get_roles", return_value=[]):
            with self.assertRaises(frappe.PermissionError):
                customers_api.sync_all()

    def test_customers_debug_customer_refuses_plain_operator(self):
        """System-Manager-only: returns Customer PII by email lookup."""
        with patch.object(customers_api.frappe, "session", _session("ops@example.com")), \
             patch.object(customers_api.frappe, "get_roles", return_value=["WooCommerce Sync Operator"]):
            with self.assertRaises(frappe.PermissionError):
                customers_api.debug_customer("someone@example.com")

    def test_territories_pull_states_refuses_unprivileged_user(self):
        with patch.object(territories_api.frappe, "session", _session("random@example.com")), \
             patch.object(territories_api.frappe, "get_roles", return_value=[]):
            with self.assertRaises(frappe.PermissionError):
                territories_api.pull_states()

    def test_territories_pull_states_allows_operator(self):
        with patch.object(territories_api.frappe, "session", _session("ops@example.com")), \
             patch.object(territories_api.frappe, "get_roles", return_value=["WooCommerce Sync Operator"]), \
             patch.object(territories_api, "sync_territories", return_value={"ok": True}):
            result = territories_api.pull_states()
        self.assertEqual(result, {"success": True, "data": {"ok": True}})

    def test_geo_ensure_permission_refuses_a_user_with_only_address_write(self):
        """A generic Address/write permission is no longer enough on its own."""
        with patch.object(geo_api.frappe, "session", _session("random@example.com")), \
             patch.object(geo_api.frappe, "get_roles", return_value=[]), \
             patch.object(geo_api.frappe, "has_permission", return_value=True):
            with self.assertRaises(frappe.PermissionError):
                geo_api._ensure_geo_permission()

    def test_geo_ensure_permission_allows_operator_with_address_write(self):
        with patch.object(geo_api.frappe, "session", _session("ops@example.com")), \
             patch.object(geo_api.frappe, "get_roles", return_value=["WooCommerce Sync Operator"]), \
             patch.object(geo_api.frappe, "has_permission", return_value=True) as has_permission:
            geo_api._ensure_geo_permission()  # must not raise
        has_permission.assert_called_once_with("Address", ptype="write", throw=True)


if __name__ == "__main__":
    unittest.main()
