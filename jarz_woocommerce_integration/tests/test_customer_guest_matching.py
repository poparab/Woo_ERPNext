"""Regression tests for guest-order customer matching.

Covers the bug where a guest Woo order (customer_id=0) incorrectly reused an
ERP Customer that was already bound to a different Woo account via email match.

See: Woo order 14746 / ACC-SINV-2026-15781 post-mortem.
"""
from __future__ import annotations

import unittest
import unittest.mock
from types import SimpleNamespace
from typing import Any, Optional

from jarz_woocommerce_integration.services import customer_sync


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fake_db(customer_store: dict[str, dict]) -> Any:
    """Return a minimal frappe.db stand-in backed by a dict of customer records.

    customer_store: {erp_name: {"mobile_no": ..., "email_id": ..., "woo_customer_id": ..., "woo_username": ...}}
    """

    def get_value(doctype, name_or_filters, fieldname=None):
        if doctype != "Customer":
            return None
        if isinstance(name_or_filters, dict):
            # Filter lookup — search by field equality
            for cname, rec in customer_store.items():
                for flt_field, flt_val in name_or_filters.items():
                    if rec.get(flt_field) == flt_val:
                        return cname
            return None
        # Direct name + fieldname lookup
        rec = customer_store.get(name_or_filters, {})
        return rec.get(fieldname)

    def set_value(doctype, name, values, update_modified=False):
        if doctype == "Customer" and name in customer_store:
            customer_store[name].update(values if isinstance(values, dict) else {})

    return SimpleNamespace(get_value=get_value, set_value=set_value)


def _make_fake_get_doc(created_docs: list) -> Any:
    class _FakeDoc:
        def __init__(self, fields):
            self._fields = fields
            self.name = fields.get("customer_name", "NEW-CUST")
            self.flags = SimpleNamespace(ignore_woo_outbound=False)

        def insert(self, ignore_permissions=True):
            created_docs.append(self._fields.copy())
            return self

    return lambda fields: _FakeDoc(fields)


def _field_exists_woo_fields(doctype, field):
    return field in {"woo_customer_id", "woo_username", "phone"}


def _no_redis():
    raise Exception("no redis in unit tests")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGuestOrderDoesNotReuseWooBoundCustomer(unittest.TestCase):

    def test_guest_email_matches_woo_bound_customer_creates_new(self):
        """Guest order with email matching a Woo-bound ERP customer must create new."""
        customer_store = {
            "Mina Atef": {
                "email_id": "suport@dasem.shop",
                "mobile_no": "01274489120",
                "woo_customer_id": "3708",
                "woo_username": None,
            }
        }
        created_docs: list = []
        fake_db = _make_fake_db(customer_store)

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync.frappe, "get_doc", side_effect=_make_fake_get_doc(created_docs)), \
             unittest.mock.patch.object(customer_sync.frappe, "flags", SimpleNamespace()), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=_field_exists_woo_fields), \
             unittest.mock.patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "get_customer_woo_id", side_effect=lambda n: customer_store.get(n if isinstance(n, str) else n.name, {}).get("woo_customer_id")), \
             unittest.mock.patch.object(customer_sync, "get_legacy_customer_woo_id", return_value=None), \
             unittest.mock.patch("frappe.utils.background_jobs.get_redis_conn", side_effect=_no_redis):

            result = customer_sync._ensure_customer(
                email="suport@dasem.shop",
                first_name="كريم",
                last_name="سيد محمود",
                order_id=14746,
                username=None,
                phone="01146269820",
                woo_customer_id=None,  # guest order
            )

        # A new customer must have been created — NOT 'Mina Atef'
        self.assertNotEqual(result, "Mina Atef")
        self.assertEqual(len(created_docs), 1)
        self.assertEqual(created_docs[0]["customer_name"], "كريم سيد محمود")

    def test_guest_username_matches_woo_bound_customer_creates_new(self):
        """Guest order with woo_username matching a Woo-bound ERP customer creates new."""
        customer_store = {
            "Existing User": {
                "email_id": "other@example.com",
                "mobile_no": "01000000000",
                "woo_customer_id": "999",
                "woo_username": "someuser",
            }
        }
        created_docs: list = []
        fake_db = _make_fake_db(customer_store)

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync.frappe, "get_doc", side_effect=_make_fake_get_doc(created_docs)), \
             unittest.mock.patch.object(customer_sync.frappe, "flags", SimpleNamespace()), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=_field_exists_woo_fields), \
             unittest.mock.patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "get_customer_woo_id", side_effect=lambda n: customer_store.get(n if isinstance(n, str) else n.name, {}).get("woo_customer_id")), \
             unittest.mock.patch.object(customer_sync, "get_legacy_customer_woo_id", return_value=None), \
             unittest.mock.patch("frappe.utils.background_jobs.get_redis_conn", side_effect=_no_redis):

            result = customer_sync._ensure_customer(
                email=None,
                first_name="Test",
                last_name="Guest",
                order_id=99999,
                username="someuser",
                phone="01111111111",
                woo_customer_id=None,  # guest order
            )

        self.assertNotEqual(result, "Existing User")
        self.assertEqual(len(created_docs), 1)

    def test_guest_phone_matches_woo_bound_customer_without_email_creates_new(self):
        """Guest order matching a Woo-bound customer by phone only (no email match) creates new."""
        customer_store = {
            "Real Woo Customer": {
                "email_id": "realwoo@example.com",
                "mobile_no": "01146269820",
                "woo_customer_id": "555",
                "woo_username": None,
            }
        }
        created_docs: list = []
        fake_db = _make_fake_db(customer_store)

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync.frappe, "get_doc", side_effect=_make_fake_get_doc(created_docs)), \
             unittest.mock.patch.object(customer_sync.frappe, "flags", SimpleNamespace()), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=_field_exists_woo_fields), \
             unittest.mock.patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "get_customer_woo_id", side_effect=lambda n: customer_store.get(n if isinstance(n, str) else n.name, {}).get("woo_customer_id")), \
             unittest.mock.patch.object(customer_sync, "get_legacy_customer_woo_id", return_value=None), \
             unittest.mock.patch("frappe.utils.background_jobs.get_redis_conn", side_effect=_no_redis):

            result = customer_sync._ensure_customer(
                email="guest@other.com",  # different email
                first_name="Guest",
                last_name="Person",
                order_id=88888,
                username=None,
                phone="01146269820",  # same phone as Woo-bound customer
                woo_customer_id=None,  # guest
            )

        # Phone matched Woo-bound customer but email differs → must create new
        self.assertNotEqual(result, "Real Woo Customer")
        self.assertEqual(len(created_docs), 1)

    def test_guest_reuses_unbound_customer_by_phone(self):
        """Guest order CAN reuse an ERP customer with no Woo identity (phone match)."""
        customer_store = {
            "Walk-in Ahmed": {
                "email_id": None,
                "mobile_no": "01200000000",
                "woo_customer_id": None,
                "woo_username": None,
            }
        }
        created_docs: list = []
        fake_db = _make_fake_db(customer_store)

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync.frappe, "get_doc", side_effect=_make_fake_get_doc(created_docs)), \
             unittest.mock.patch.object(customer_sync.frappe, "flags", SimpleNamespace()), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=_field_exists_woo_fields), \
             unittest.mock.patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "get_customer_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "get_legacy_customer_woo_id", return_value=None), \
             unittest.mock.patch("frappe.utils.background_jobs.get_redis_conn", side_effect=_no_redis):

            result = customer_sync._ensure_customer(
                email="guest@example.com",
                first_name="Ahmed",
                last_name="",
                order_id=77777,
                username=None,
                phone="01200000000",
                woo_customer_id=None,  # guest
            )

        # Unbound customer may be reused
        self.assertEqual(result, "Walk-in Ahmed")
        self.assertEqual(len(created_docs), 0)

    def test_real_woo_account_still_matches_by_woo_id(self):
        """A real Woo order (woo_customer_id set) resolves via woo_customer_id normally."""
        found = {}

        def fake_find_customer_by_woo_id(woo_id):
            if woo_id == 3708:
                return "Mina Atef"
            return None

        customer_store = {
            "Mina Atef": {
                "email_id": "suport@dasem.shop",
                "mobile_no": "01274489120",
                "woo_customer_id": "3708",
                "woo_username": None,
            }
        }
        fake_db = _make_fake_db(customer_store)
        created_docs: list = []

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync.frappe, "get_doc", side_effect=_make_fake_get_doc(created_docs)), \
             unittest.mock.patch.object(customer_sync.frappe, "flags", SimpleNamespace()), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=_field_exists_woo_fields), \
             unittest.mock.patch.object(customer_sync, "find_customer_by_woo_id", side_effect=fake_find_customer_by_woo_id), \
             unittest.mock.patch.object(customer_sync, "get_customer_woo_id", side_effect=lambda n: customer_store.get(n if isinstance(n, str) else n.name, {}).get("woo_customer_id")), \
             unittest.mock.patch.object(customer_sync, "get_legacy_customer_woo_id", return_value=None), \
             unittest.mock.patch("frappe.utils.background_jobs.get_redis_conn", side_effect=_no_redis):

            result = customer_sync._ensure_customer(
                email="suport@dasem.shop",
                first_name="Mina",
                last_name="Atef",
                order_id=14000,
                username=None,
                phone="01274489120",
                woo_customer_id=3708,  # real Woo account
            )

        # Must resolve to the existing Mina Atef, not create a new one
        self.assertEqual(result, "Mina Atef")
        self.assertEqual(len(created_docs), 0)


class TestCandidateSafeForGuest(unittest.TestCase):

    def test_returns_false_for_customer_with_woo_customer_id(self):
        with unittest.mock.patch.object(customer_sync, "get_customer_woo_id", return_value="3708"), \
             unittest.mock.patch.object(customer_sync, "get_legacy_customer_woo_id", return_value=None):
            self.assertFalse(customer_sync._candidate_safe_for_guest("Mina Atef"))

    def test_returns_false_for_customer_with_woo_username(self):
        with unittest.mock.patch.object(customer_sync, "get_customer_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "get_legacy_customer_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "_field_exists", return_value=True), \
             unittest.mock.patch.object(customer_sync.frappe, "db", SimpleNamespace(
                 get_value=lambda doctype, name, field: "someuser" if field == "woo_username" else None
             )):
            self.assertFalse(customer_sync._candidate_safe_for_guest("Some Customer"))

    def test_returns_true_for_unbound_customer(self):
        with unittest.mock.patch.object(customer_sync, "get_customer_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "get_legacy_customer_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "_field_exists", return_value=True), \
             unittest.mock.patch.object(customer_sync.frappe, "db", SimpleNamespace(
                 get_value=lambda doctype, name, field: None
             )):
            self.assertTrue(customer_sync._candidate_safe_for_guest("Walk-in Customer"))

    def test_returns_true_for_none_name(self):
        self.assertTrue(customer_sync._candidate_safe_for_guest(None))
