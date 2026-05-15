"""Tests for PROD-WOO-004: duplicate Address insert race recovery.

Covers _safe_insert_address and the Redis lock + re-check path in _create_address.
"""
import unittest
from unittest.mock import MagicMock, patch

import frappe

from jarz_woocommerce_integration.services import customer_sync


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BILLING_DATA = {
    "address_1": "12 Nile Street",
    "address_2": "",
    "city": "Cairo",
    "state": "Dokki",
    "postcode": "12311",
    "country": "EG",
}


def _make_addr_doc(name="ADDR-0001", address_type="Billing"):
    doc = MagicMock()
    doc.name = name
    doc.address_type = address_type
    doc.address_title = "Ahmed Mohamed"
    doc.flags = MagicMock()
    return doc


# ---------------------------------------------------------------------------
# _safe_insert_address
# ---------------------------------------------------------------------------

class TestSafeInsertAddress(unittest.TestCase):
    """Unit tests for the _safe_insert_address recovery helper."""

    def _call(self, addr_doc, *, customer="Ahmed Mohamed", data=None, order_id=14476):
        return customer_sync._safe_insert_address(
            addr_doc,
            customer=customer,
            data=data or _BILLING_DATA,
            order_id=order_id,
        )

    def test_happy_path_returns_addr_name(self):
        """When insert succeeds the address name is returned."""
        addr = _make_addr_doc("ADDR-0001")
        with patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "release_savepoint"), \
             patch.object(addr, "insert"):
            result = self._call(addr)
        self.assertEqual(result, "ADDR-0001")

    def test_recovers_existing_address_on_duplicate_entry_error(self):
        """On DuplicateEntryError the already-existing Address is returned."""
        addr = _make_addr_doc()

        def _raise_on_insert(*_a, **_kw):
            raise frappe.DuplicateEntryError("duplicate")

        with patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "rollback"), \
             patch.object(addr, "insert", side_effect=_raise_on_insert), \
             patch.object(customer_sync, "_find_existing_address_for_customer",
                          return_value="ADDR-EXISTING") as find_existing:
            result = self._call(addr)

        find_existing.assert_called_once_with(
            "Ahmed Mohamed", addr.address_type, _BILLING_DATA
        )
        self.assertEqual(result, "ADDR-EXISTING")

    def test_suffix_retry_when_requery_still_finds_nothing(self):
        """If re-query also returns None, suffix address_title and retry once."""
        addr = _make_addr_doc()
        addr.address_title = "Ahmed Mohamed"
        insert_calls = {"n": 0}

        def _insert_side_effect(*_a, **_kw):
            insert_calls["n"] += 1
            if insert_calls["n"] == 1:
                raise frappe.DuplicateEntryError("duplicate")
            addr.name = "ADDR-SUFFIXED"

        with patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "rollback"), \
             patch.object(addr, "insert", side_effect=_insert_side_effect), \
             patch.object(customer_sync, "_find_existing_address_for_customer",
                          return_value=None):
            result = self._call(addr, order_id=14476)

        self.assertEqual(insert_calls["n"], 2)
        self.assertIn("14476", addr.address_title)
        self.assertEqual(result, "ADDR-SUFFIXED")

    def test_non_duplicate_exception_is_reraised(self):
        """Exceptions other than DuplicateEntryError propagate unchanged."""
        addr = _make_addr_doc()
        with patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(addr, "insert", side_effect=RuntimeError("disk full")):
            with self.assertRaises(RuntimeError):
                self._call(addr)


# ---------------------------------------------------------------------------
# _create_address — Redis lock + re-check path
# ---------------------------------------------------------------------------

class TestCreateAddressRedisLock(unittest.TestCase):
    """_create_address acquires a Redis lock and re-checks before inserting."""

    def _billing(self):
        return dict(_BILLING_DATA)

    def test_recheck_under_lock_avoids_insert_when_address_exists(self):
        """If an address appears between the outer check and the lock, _create_address
        returns it without calling _safe_insert_address."""
        data = self._billing()
        mock_redis = MagicMock()
        mock_lock = MagicMock()
        mock_lock.acquire.return_value = True
        mock_redis.lock.return_value = mock_lock

        with patch.object(customer_sync.frappe, "get_doc", return_value=_make_addr_doc()), \
             patch.object(customer_sync, "_resolve_country", return_value="Egypt"), \
             patch.object(customer_sync, "_find_existing_address_for_customer",
                          return_value="ADDR-CONCURRENT") as find_existing, \
             patch.object(customer_sync, "_safe_insert_address") as safe_insert, \
             patch("jarz_woocommerce_integration.services.customer_sync."
                   "_create_address.__code__",  # keep patch scoped to module-level import
                   customer_sync._create_address.__code__), \
             patch("frappe.utils.background_jobs.get_redis_conn",
                   return_value=mock_redis):
            result = customer_sync._create_address(
                "Ahmed Mohamed", "Billing", data, "01012345678", "ahmed@example.com", 14476
            )

        find_existing.assert_called()
        safe_insert.assert_not_called()
        self.assertEqual(result, "ADDR-CONCURRENT")

    def test_safe_insert_called_when_no_recheck_match(self):
        """When re-check under lock finds nothing, _safe_insert_address is called."""
        data = self._billing()
        mock_redis = MagicMock()
        mock_lock = MagicMock()
        mock_lock.acquire.return_value = True
        mock_redis.lock.return_value = mock_lock

        addr_doc = _make_addr_doc("ADDR-NEW")

        with patch.object(customer_sync.frappe, "get_doc", return_value=addr_doc), \
             patch.object(customer_sync, "_resolve_country", return_value="Egypt"), \
             patch.object(customer_sync, "_find_existing_address_for_customer",
                          return_value=None), \
             patch.object(customer_sync, "_safe_insert_address",
                          return_value="ADDR-NEW") as safe_insert, \
             patch("frappe.utils.background_jobs.get_redis_conn",
                   return_value=mock_redis):
            result = customer_sync._create_address(
                "Ahmed Mohamed", "Billing", data, "01012345678", "ahmed@example.com", 14476
            )

        safe_insert.assert_called_once()
        self.assertEqual(result, "ADDR-NEW")

    def test_redis_unavailable_falls_through_to_safe_insert(self):
        """When Redis raises, the lock is skipped and _safe_insert_address is still called."""
        data = self._billing()
        addr_doc = _make_addr_doc("ADDR-NEW")

        with patch.object(customer_sync.frappe, "get_doc", return_value=addr_doc), \
             patch.object(customer_sync, "_resolve_country", return_value="Egypt"), \
             patch.object(customer_sync, "_find_existing_address_for_customer",
                          return_value=None), \
             patch.object(customer_sync, "_safe_insert_address",
                          return_value="ADDR-NEW") as safe_insert, \
             patch("frappe.utils.background_jobs.get_redis_conn",
                   side_effect=Exception("Redis down")):
            result = customer_sync._create_address(
                "Ahmed Mohamed", "Billing", data, "01012345678", "ahmed@example.com", 14476
            )

        # _safe_insert_address is called directly when no lock is held
        safe_insert.assert_called_once()
        self.assertEqual(result, "ADDR-NEW")


# ---------------------------------------------------------------------------
# Two-order same-customer same-address scenario
# ---------------------------------------------------------------------------

class TestAddressSignatureCollision(unittest.TestCase):
    """Simulates two orders for the same customer with the same shipping address
    ensuring a single Address is returned without error."""

    def test_second_order_reuses_address_created_by_first(self):
        """If order B's _create_address call collides, it must return the existing addr."""
        data = self._addr_data()
        addr_doc = MagicMock()
        addr_doc.name = "ADDR-FIRST"
        addr_doc.address_type = "Billing"
        addr_doc.address_title = "Sami Khalil"
        addr_doc.flags = MagicMock()

        # Simulate: first call succeeds; second call raises DuplicateEntryError then re-query finds ADDR-FIRST
        insert_count = {"n": 0}

        def _insert(*_a, **_kw):
            insert_count["n"] += 1
            if insert_count["n"] == 2:
                raise frappe.DuplicateEntryError("Duplicate entry 'Sami Khalil-Billing'")
            addr_doc.name = "ADDR-FIRST"

        addr_doc.insert = _insert

        with patch.object(customer_sync.frappe, "get_doc", return_value=addr_doc), \
             patch.object(customer_sync, "_resolve_country", return_value="Egypt"), \
             patch.object(customer_sync, "_find_existing_address_for_customer",
                          return_value=None), \
             patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "rollback"), \
             patch.object(customer_sync.frappe.db, "release_savepoint"), \
             patch("frappe.utils.background_jobs.get_redis_conn",
                   side_effect=Exception("Redis down")):
            # First order creates address
            name1 = customer_sync._create_address(
                "Sami Khalil", "Billing", data, "01099999999", "sami@example.com", 100
            )

        # For second order: re-query after collision should return ADDR-FIRST
        with patch.object(customer_sync.frappe, "get_doc", return_value=addr_doc), \
             patch.object(customer_sync, "_resolve_country", return_value="Egypt"), \
             patch.object(customer_sync, "_find_existing_address_for_customer",
                          side_effect=[None, "ADDR-FIRST"]), \
             patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "rollback"), \
             patch.object(customer_sync.frappe.db, "release_savepoint"), \
             patch("frappe.utils.background_jobs.get_redis_conn",
                   side_effect=Exception("Redis down")):
            name2 = customer_sync._create_address(
                "Sami Khalil", "Billing", data, "01099999999", "sami@example.com", 101
            )

        # Both calls must resolve to the same address
        self.assertEqual(name1, name2)

    def _addr_data(self):
        return {
            "address_1": "5 Tahrir Square",
            "address_2": "",
            "city": "Cairo",
            "state": "Downtown",
            "postcode": "11511",
            "country": "EG",
        }
