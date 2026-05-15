"""Tests for PROD-WOO-004: duplicate Customer insert race recovery.

Covers _safe_insert_customer and the Redis-fallback path in _ensure_customer.
"""
import unittest
from unittest.mock import MagicMock, call, patch

import frappe

from jarz_woocommerce_integration.services import customer_sync


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_customer_doc(name="CUST-0042"):
    doc = MagicMock()
    doc.name = name
    doc.customer_name = "Ahmed Mohamed"
    doc.flags = MagicMock()
    return doc


# ---------------------------------------------------------------------------
# _safe_insert_customer
# ---------------------------------------------------------------------------

class TestSafeInsertCustomer(unittest.TestCase):
    """Unit tests for the _safe_insert_customer recovery helper."""

    def _call(self, doc, *, woo_customer_id=99, username="woouser", phone_norm="01012345678",
              email="ahmed@example.com", order_id=14476):
        return customer_sync._safe_insert_customer(
            doc,
            woo_customer_id=woo_customer_id,
            username=username,
            phone_norm=phone_norm,
            email=email,
            order_id=order_id,
        )

    def test_happy_path_returns_doc_name(self):
        """When insert succeeds the doc name is returned."""
        doc = _make_customer_doc("CUST-0001")
        with patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "release_savepoint"), \
             patch.object(doc, "insert"):
            result = self._call(doc)
        self.assertEqual(result, "CUST-0001")

    def test_recovers_via_woo_customer_id_on_duplicate_entry_error(self):
        """On DuplicateEntryError the existing Customer is returned via woo_id lookup."""
        doc = _make_customer_doc()

        def _raise_on_insert(*_a, **_kw):
            raise frappe.DuplicateEntryError("duplicate")

        with patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "rollback"), \
             patch.object(doc, "insert", side_effect=_raise_on_insert), \
             patch.object(customer_sync, "_field_exists", return_value=True), \
             patch.object(customer_sync, "find_customer_by_woo_id", return_value="CUST-0042") as find_woo:
            result = self._call(doc, woo_customer_id=99)

        find_woo.assert_called_once_with(99)
        self.assertEqual(result, "CUST-0042")

    def test_recovers_via_phone_when_woo_id_not_found(self):
        """Falls through to phone lookup if woo_id lookup returns nothing."""
        doc = _make_customer_doc()

        def _raise_on_insert(*_a, **_kw):
            raise frappe.DuplicateEntryError("duplicate")

        mock_db = MagicMock()
        mock_db.savepoint = MagicMock()
        mock_db.rollback = MagicMock()
        mock_db.get_value = MagicMock(return_value="CUST-PHONE")

        with patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "rollback"), \
             patch.object(doc, "insert", side_effect=_raise_on_insert), \
             patch.object(customer_sync, "_field_exists", return_value=True), \
             patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             patch.object(customer_sync.frappe.db, "get_value", return_value="CUST-PHONE"):
            result = self._call(doc, phone_norm="01012345678")

        self.assertEqual(result, "CUST-PHONE")

    def test_recovers_via_email_as_last_resort(self):
        """Falls through to email lookup when all other identifiers return nothing."""
        doc = _make_customer_doc()

        def _raise_on_insert(*_a, **_kw):
            raise frappe.DuplicateEntryError("duplicate")

        # get_value returns None for phone/username, then the customer for email
        get_value_calls = {"n": 0}
        def _get_value(doctype, filters, field="name"):
            get_value_calls["n"] += 1
            if isinstance(filters, dict) and filters.get("email_id"):
                return "CUST-EMAIL"
            return None

        with patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "rollback"), \
             patch.object(doc, "insert", side_effect=_raise_on_insert), \
             patch.object(customer_sync, "_field_exists", return_value=False), \
             patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             patch.object(customer_sync.frappe.db, "get_value", side_effect=_get_value):
            result = self._call(doc, woo_customer_id=None, phone_norm=None, username=None,
                                email="ahmed@example.com")

        self.assertEqual(result, "CUST-EMAIL")

    def test_suffix_retry_on_genuine_collision(self):
        """If no existing customer found after duplicate, suffix name and retry insert."""
        doc = _make_customer_doc()
        doc.customer_name = "Ahmed Mohamed"
        insert_call_count = {"n": 0}

        def _insert_side_effect(*_a, **_kw):
            insert_call_count["n"] += 1
            if insert_call_count["n"] == 1:
                raise frappe.DuplicateEntryError("duplicate")
            doc.name = "CUST-SUFFIXED"

        with patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "rollback"), \
             patch.object(doc, "insert", side_effect=_insert_side_effect), \
             patch.object(customer_sync, "_field_exists", return_value=False), \
             patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             patch.object(customer_sync.frappe.db, "get_value", return_value=None):
            result = self._call(doc, woo_customer_id=None, phone_norm=None,
                                username=None, email=None, order_id=14476)

        self.assertEqual(insert_call_count["n"], 2)
        self.assertIn("14476", doc.customer_name)
        self.assertEqual(result, "CUST-SUFFIXED")

    def test_non_duplicate_exception_is_reraised(self):
        """Exceptions other than DuplicateEntryError propagate unchanged."""
        doc = _make_customer_doc()

        with patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(doc, "insert", side_effect=RuntimeError("unexpected")):
            with self.assertRaises(RuntimeError):
                self._call(doc)


# ---------------------------------------------------------------------------
# _ensure_customer Redis-fallback path
# ---------------------------------------------------------------------------

class TestEnsureCustomerRedisFallback(unittest.TestCase):
    """When Redis is unavailable _ensure_customer still returns a Customer name
    and DuplicateEntryError from the insert is handled by _safe_insert_customer."""

    def _make_order_billing(self):
        return {
            "first_name": "Ahmed",
            "last_name": "Mohamed",
            "email": "ahmed@example.com",
            "phone": "01012345678",
        }

    def test_redis_unavailable_still_creates_customer(self):
        """When Redis raises, _ensure_customer falls through to create and recovers."""
        doc = _make_customer_doc("CUST-NEW")

        def _make_doc(fields):
            d = MagicMock()
            d.name = "CUST-NEW"
            d.flags = MagicMock()
            d.insert = MagicMock()
            return d

        with patch.object(customer_sync.frappe, "get_doc", side_effect=_make_doc), \
             patch.object(customer_sync.frappe.db, "get_value", return_value=None), \
             patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "release_savepoint"), \
             patch.object(customer_sync, "_field_exists", return_value=False), \
             patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             patch("frappe.utils.background_jobs.get_redis_conn", side_effect=Exception("Redis down")):
            result = customer_sync._ensure_customer(
                "ahmed@example.com", "Ahmed", "Mohamed", 14476,
                username=None, phone="01012345678", woo_customer_id=None,
            )

        self.assertEqual(result, "CUST-NEW")

    def test_redis_unavailable_safe_insert_recovers_duplicate(self):
        """With Redis down and a racing insert, _safe_insert_customer recovery path is hit."""
        insert_calls = {"n": 0}

        def _make_doc(fields):
            d = MagicMock()
            d.name = "CUST-RACE"
            d.flags = MagicMock()

            def _insert(*_a, **_kw):
                insert_calls["n"] += 1
                raise frappe.DuplicateEntryError("duplicate")

            d.insert = _insert
            d.customer_name = fields.get("customer_name", "Unknown")
            return d

        with patch.object(customer_sync.frappe, "get_doc", side_effect=_make_doc), \
             patch.object(customer_sync.frappe.db, "get_value", return_value="CUST-EXISTING"), \
             patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "rollback"), \
             patch.object(customer_sync, "_field_exists", return_value=False), \
             patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             patch("frappe.utils.background_jobs.get_redis_conn", side_effect=Exception("Redis down")):
            result = customer_sync._ensure_customer(
                "ahmed@example.com", "Ahmed", "Mohamed", 14476,
                username=None, phone=None, woo_customer_id=None,
            )

        # Recovery should have returned the pre-existing customer
        self.assertEqual(result, "CUST-EXISTING")
        # Insert was attempted
        self.assertGreater(insert_calls["n"], 0)
