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

    def _matches(stored, expected):
        """Equality, plus the ``["in", [...]]`` form the phone lookup now uses."""
        if isinstance(expected, list | tuple) and len(expected) == 2 and expected[0] == "in":
            return stored in expected[1]
        return stored == expected

    def get_value(doctype, name_or_filters, fieldname=None):
        if doctype != "Customer":
            return None
        if isinstance(name_or_filters, dict):
            # Filter lookup — search by field equality
            for cname, rec in customer_store.items():
                for flt_field, flt_val in name_or_filters.items():
                    if _matches(rec.get(flt_field), flt_val):
                        return cname
            return None
        # Direct name + fieldname lookup
        rec = customer_store.get(name_or_filters, {})
        return rec.get(fieldname)

    def get_values(doctype, filters=None, fieldname="name", **_kwargs):
        """List form of get_value — the phone lookup uses it to see every match."""
        if doctype != "Customer" or not isinstance(filters, dict):
            return []
        found = []
        for cname, rec in customer_store.items():
            if all(_matches(rec.get(f), v) for f, v in filters.items()):
                found.append(cname)
        return found

    def set_value(doctype, name, values, update_modified=False):
        if doctype == "Customer" and name in customer_store:
            customer_store[name].update(values if isinstance(values, dict) else {})

    def sql(*_a, **_kw):
        # No Sales Invoices in these fixtures, so the "most recent order" probe
        # finds nothing and the tie-break falls through to the oldest candidate.
        return []

    return SimpleNamespace(
        get_value=get_value, get_values=get_values, set_value=set_value, sql=sql
    )


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


# ---------------------------------------------------------------------------
# FIX 1 — a guest checkout that proves BOTH identifiers is the account holder
# ---------------------------------------------------------------------------

class TestGuestWithExactEmailAndPhoneBinds(unittest.TestCase):
    """The other half of the guard: refusing an exact match mints a shadow.

    A guest order matching a Woo-bound Customer on email *and* phone is the
    account holder who did not log in.  Minting a second record for them creates
    a Customer that can never be bound — its orders stay ``customer_id: 0`` in the
    store and never reach the shopper's My Account.  Production carried 26 such
    records, 18 duplicating a bound sibling, 7 of them holding EGP 5,195 of
    submitted invoices, and the rate was accelerating (13 in 2026-08 alone).
    """

    #: The غادة case: bound account, guest checkout carrying both identifiers.
    STORE = {
        "غادة": {
            "customer_name": "غادة",
            "email_id": "ghada@example.com",
            "mobile_no": "01097503380",
            "woo_customer_id": "4211",
            "woo_username": None,
        }
    }

    def _run(self, store, **order):
        # Deep-ish copy: `_update_customer_identity` writes through the fake db,
        # and a shared inner dict would leak between tests.
        store = {name: dict(rec) for name, rec in store.items()}
        created_docs: list = []
        fake_db = _make_fake_db(store)
        kwargs = {
            "email": None, "first_name": "غادة", "last_name": "",
            "order_id": 17173, "username": None, "phone": None,
            "woo_customer_id": None,
        }
        kwargs.update(order)

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync.frappe, "get_doc", side_effect=_make_fake_get_doc(created_docs)), \
             unittest.mock.patch.object(customer_sync.frappe, "flags", SimpleNamespace()), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=_field_exists_woo_fields), \
             unittest.mock.patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "get_customer_woo_id", side_effect=lambda n: store.get(n if isinstance(n, str) else n.name, {}).get("woo_customer_id")), \
             unittest.mock.patch.object(customer_sync, "get_legacy_customer_woo_id", return_value=None), \
             unittest.mock.patch("frappe.utils.background_jobs.get_redis_conn", side_effect=_no_redis):
            result = customer_sync._ensure_customer(**kwargs)
        return result, created_docs, store

    def test_exact_email_and_phone_binds_instead_of_minting_a_shadow(self):
        result, created, _ = self._run(
            self.STORE,
            email="ghada@example.com",
            phone="01097503380",
        )
        self.assertEqual(result, "غادة")
        self.assertEqual(created, [], "a shadow Customer was minted for the account holder")

    def test_the_match_survives_a_different_stored_phone_spelling(self):
        """Production stores the same subscriber as 0…, +20… and 20…."""
        store = {"غادة": dict(self.STORE["غادة"], mobile_no="+201097503380")}
        result, created, _ = self._run(
            store, email="ghada@example.com", phone="01097503380"
        )
        self.assertEqual(result, "غادة")
        self.assertEqual(created, [])

    def test_email_case_and_padding_do_not_defeat_the_match(self):
        result, created, store = self._run(
            self.STORE,
            email="  GHADA@Example.com ",
            phone="01097503380",
        )
        self.assertEqual(result, "غادة")
        self.assertEqual(created, [])
        # and the stored address is not overwritten with the padded spelling
        self.assertEqual(store["غادة"]["email_id"], "ghada@example.com")

    def test_binding_reuses_without_stamping_a_second_holder_on_the_woo_id(self):
        """Reuse must never write woo_customer_id — two holders blind lookups.

        ``find_customer_by_woo_id`` returns None permanently once an id has two
        holders, which is the mechanism that put 211 Customers on woo id 3357.
        """
        result, created, store = self._run(
            self.STORE, email="ghada@example.com", phone="01097503380"
        )
        self.assertEqual(result, "غادة")
        self.assertEqual(created, [])
        # exactly one record, still holding exactly its own binding
        self.assertEqual(list(store), ["غادة"])
        self.assertEqual(store["غادة"]["woo_customer_id"], "4211")


class TestGuestPartialMatchStillRefuses(unittest.TestCase):
    """THE anti-hijack regression guard.

    One matching field is not identity.  Egyptian households share handsets and
    mailboxes get recycled, so accepting a single match would attach a stranger's
    order to somebody's real WooCommerce account — a far worse outcome than the
    duplicate this fix exists to stop.  Every case below must still mint.
    """

    STORE = {
        "Real Account": {
            "customer_name": "Real Account",
            "email_id": "owner@example.com",
            "mobile_no": "01097503380",
            "woo_customer_id": "4211",
            "woo_username": "owner@example.com",
        }
    }

    def _run(self, **order):
        created_docs: list = []
        store = {k: dict(v) for k, v in self.STORE.items()}
        fake_db = _make_fake_db(store)
        kwargs = {
            "email": None, "first_name": "Some", "last_name": "Guest",
            "order_id": 90001, "username": None, "phone": None,
            "woo_customer_id": None,
        }
        kwargs.update(order)

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync.frappe, "get_doc", side_effect=_make_fake_get_doc(created_docs)), \
             unittest.mock.patch.object(customer_sync.frappe, "flags", SimpleNamespace()), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=_field_exists_woo_fields), \
             unittest.mock.patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "get_customer_woo_id", side_effect=lambda n: store.get(n if isinstance(n, str) else n.name, {}).get("woo_customer_id")), \
             unittest.mock.patch.object(customer_sync, "get_legacy_customer_woo_id", return_value=None), \
             unittest.mock.patch("frappe.utils.background_jobs.get_redis_conn", side_effect=_no_redis):
            result = customer_sync._ensure_customer(**kwargs)
        return result, created_docs

    def _assert_refused(self, result, created):
        self.assertNotEqual(result, "Real Account")
        self.assertEqual(len(created), 1)

    def test_email_matches_but_phone_does_not(self):
        """The shared-mailbox hijack."""
        result, created = self._run(email="owner@example.com", phone="01111111111")
        self._assert_refused(result, created)

    def test_phone_matches_but_email_does_not(self):
        """The shared-handset hijack."""
        result, created = self._run(email="stranger@example.com", phone="01097503380")
        self._assert_refused(result, created)

    def test_email_matches_and_the_order_carries_no_phone_at_all(self):
        """A missing identifier is not a matching one."""
        result, created = self._run(email="owner@example.com", phone=None)
        self._assert_refused(result, created)

    def test_phone_matches_and_the_order_carries_no_email_at_all(self):
        result, created = self._run(email=None, phone="01097503380")
        self._assert_refused(result, created)

    def test_username_matches_but_neither_email_nor_phone_does(self):
        result, created = self._run(
            username="owner@example.com",
            email="stranger@example.com",
            phone="01111111111",
        )
        self._assert_refused(result, created)

    def test_a_near_miss_phone_is_not_a_match(self):
        """One digit apart is a different subscriber, not a spelling variant."""
        result, created = self._run(email="owner@example.com", phone="01097503381")
        self._assert_refused(result, created)


class TestGuestIdentityConfirmed(unittest.TestCase):
    """The predicate itself, isolated from the lookup steps."""

    def _confirm(self, stored, *, email, phone):
        db = SimpleNamespace(
            get_value=lambda _dt, _name, field: stored.get(field)
        )
        with unittest.mock.patch.object(customer_sync.frappe, "db", db), \
             unittest.mock.patch.object(customer_sync, "_field_exists", return_value=True):
            return customer_sync._guest_identity_confirmed(
                "CUST", email=email, phone_norm=phone
            )

    def test_both_matching_is_confirmation(self):
        self.assertTrue(self._confirm(
            {"email_id": "a@b.com", "mobile_no": "01097503380"},
            email="a@b.com", phone="01097503380",
        ))

    def test_the_number_may_live_in_the_phone_field(self):
        self.assertTrue(self._confirm(
            {"email_id": "a@b.com", "mobile_no": None, "phone": "01097503380"},
            email="a@b.com", phone="01097503380",
        ))

    def test_only_email_is_not_confirmation(self):
        self.assertFalse(self._confirm(
            {"email_id": "a@b.com", "mobile_no": "01111111111"},
            email="a@b.com", phone="01097503380",
        ))

    def test_only_phone_is_not_confirmation(self):
        self.assertFalse(self._confirm(
            {"email_id": "other@b.com", "mobile_no": "01097503380"},
            email="a@b.com", phone="01097503380",
        ))

    def test_a_customer_storing_no_email_can_never_be_confirmed(self):
        """Otherwise 'both empty' would read as 'both match'."""
        self.assertFalse(self._confirm(
            {"email_id": None, "mobile_no": "01097503380"},
            email="a@b.com", phone="01097503380",
        ))

    def test_a_failing_probe_refuses_rather_than_adopting(self):
        def _boom(*_a, **_kw):
            raise RuntimeError("db down")

        with unittest.mock.patch.object(
            customer_sync.frappe, "db", SimpleNamespace(get_value=_boom)
        ):
            self.assertFalse(customer_sync._guest_identity_confirmed(
                "CUST", email="a@b.com", phone_norm="01097503380"
            ))


# ---------------------------------------------------------------------------
# FIX 2 — a bare 10-digit Egyptian mobile is the same subscriber
# ---------------------------------------------------------------------------

class TestBareNationalMobile(unittest.TestCase):
    """Woo order 17173 carried ``1097503380`` — the trunk 0 dropped at the keyboard.

    The stored number is ``01097503380``.  Without the fold, ``_phone_variants``
    produced one spelling that matched nothing and the phone step was blind, which
    is the entire غادة case.
    """

    def test_the_trunk_zero_is_restored(self):
        self.assertEqual(customer_sync._normalize_phone("1097503380"), "01097503380")

    def test_the_variant_list_reaches_the_stored_spelling(self):
        variants = customer_sync._phone_variants("1097503380")
        self.assertIn("01097503380", variants)
        # and every other spelling production actually holds
        for spelling in ("+201097503380", "201097503380", "00201097503380"):
            with self.subTest(spelling=spelling):
                self.assertIn(spelling, variants)

    def test_the_bare_spelling_itself_still_matches(self):
        """Rows already stored bare must not stop resolving."""
        self.assertIn("1097503380", customer_sync._phone_variants("1097503380"))

    def test_an_already_canonical_number_is_untouched(self):
        self.assertEqual(customer_sync._normalize_phone("01097503380"), "01097503380")

    def test_international_numbers_are_not_corrupted(self):
        for number in ("+15551234567", "+447911123456", "+971501234567", "+33612345678"):
            with self.subTest(number=number):
                self.assertEqual(customer_sync._normalize_phone(number), number)

    def test_a_ten_digit_number_not_shaped_like_an_egyptian_mobile_is_left_alone(self):
        """Every Egyptian mobile's national number starts with 1; these do not."""
        for number in ("5551234567", "9876543210", "2025550143"):
            with self.subTest(number=number):
                self.assertEqual(customer_sync._normalize_phone(number), number)

    def test_an_eleven_digit_bare_number_is_left_alone(self):
        """A Chinese mobile is 11 digits starting with 1 — out of scope."""
        self.assertEqual(customer_sync._normalize_phone("13800138000"), "13800138000")

    def test_the_egyptian_country_code_folds_are_unchanged(self):
        for spelling in ("+201097503380", "201097503380", "00201097503380"):
            with self.subTest(spelling=spelling):
                self.assertEqual(
                    customer_sync._normalize_phone(spelling), "01097503380"
                )


class TestBareNationalMobileResolvesEndToEnd(unittest.TestCase):

    def test_a_guest_order_with_the_bare_number_finds_the_unbound_customer(self):
        """Isolates the phone fold from the Woo-binding guard."""
        store = {
            "Walk-in غادة": {
                "customer_name": "Walk-in غادة",
                "email_id": None,
                "mobile_no": "01097503380",
                "woo_customer_id": None,
                "woo_username": None,
            }
        }
        created_docs: list = []
        fake_db = _make_fake_db(store)

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync.frappe, "get_doc", side_effect=_make_fake_get_doc(created_docs)), \
             unittest.mock.patch.object(customer_sync.frappe, "flags", SimpleNamespace()), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=_field_exists_woo_fields), \
             unittest.mock.patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "get_customer_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "get_legacy_customer_woo_id", return_value=None), \
             unittest.mock.patch("frappe.utils.background_jobs.get_redis_conn", side_effect=_no_redis):
            result = customer_sync._ensure_customer(
                email="ghada@example.com", first_name="غادة", last_name="",
                order_id=17173, username=None,
                phone="1097503380",  # exactly what Woo sent
                woo_customer_id=None,
            )

        self.assertEqual(result, "Walk-in غادة")
        self.assertEqual(created_docs, [])

    def test_the_bare_number_also_confirms_identity_for_a_bound_customer(self):
        """FIX 1 and FIX 2 together: the actual production غادة case."""
        store = {
            "غادة": {
                "customer_name": "غادة",
                "email_id": "ghada@example.com",
                "mobile_no": "01097503380",
                "woo_customer_id": "4211",
                "woo_username": None,
            }
        }
        created_docs: list = []
        fake_db = _make_fake_db(store)

        with unittest.mock.patch.object(customer_sync.frappe, "db", fake_db), \
             unittest.mock.patch.object(customer_sync.frappe, "get_doc", side_effect=_make_fake_get_doc(created_docs)), \
             unittest.mock.patch.object(customer_sync.frappe, "flags", SimpleNamespace()), \
             unittest.mock.patch.object(customer_sync, "_field_exists", side_effect=_field_exists_woo_fields), \
             unittest.mock.patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             unittest.mock.patch.object(customer_sync, "get_customer_woo_id", side_effect=lambda n: store.get(n if isinstance(n, str) else n.name, {}).get("woo_customer_id")), \
             unittest.mock.patch.object(customer_sync, "get_legacy_customer_woo_id", return_value=None), \
             unittest.mock.patch("frappe.utils.background_jobs.get_redis_conn", side_effect=_no_redis):
            result = customer_sync._ensure_customer(
                email="ghada@example.com", first_name="غادة", last_name="",
                order_id=17173, username=None, phone="1097503380",
                woo_customer_id=None,
            )

        self.assertEqual(result, "غادة")
        self.assertEqual(created_docs, [])


if __name__ == "__main__":
    unittest.main()
