"""The stale-snapshot half of the duplicate-Customer race.

MariaDB runs REPEATABLE READ and Frappe never overrides it, so the recovery
lookups inside ``_safe_insert_customer`` read from a snapshot that predates the
row the racing worker committed.  Every lookup misses, the code concludes it hit
a genuine name collision, and mints ``<name>-<woo_order_id>``.  Production
carries 287 such pairs.

These tests pin the fix: after the snapshot lookups miss, the colliding primary
key is re-read with a locking read (which sees committed data) and the suffix is
applied only when that row belongs to a *different* person.
"""

import unittest
from unittest.mock import MagicMock, patch

import frappe

from jarz_woocommerce_integration.services import customer_sync


def _make_customer_doc(name="مصطفى مسعد", customer_name=None):
    doc = MagicMock()
    doc.name = name
    doc.customer_name = customer_name or name
    doc.flags = MagicMock()
    return doc


class _SnapshotRaceHarness:
    """Simulates a worker whose snapshot cannot see the racing worker's row.

    Every snapshot-served read returns nothing; only the locking read sees the
    committed row. That is exactly the production condition.
    """

    def __init__(self, committed_row=None):
        self.committed_row = committed_row
        self.locking_reads = []

    def sql(self, query, values=None, as_dict=False):
        self.locking_reads.append((query, values))
        if "FOR UPDATE" not in query and "LOCK IN SHARE MODE" not in query:
            raise AssertionError("recovery must use a locking read, not a snapshot read")
        if self.committed_row and values and values[0] == self.committed_row["name"]:
            return [dict(self.committed_row)]
        return []


class TestSnapshotRaceRecovery(unittest.TestCase):
    """_safe_insert_customer must resolve the race instead of suffixing."""

    def _call(self, doc, harness, *, woo_customer_id=5973, username=None,
              phone_norm="01111034268", email="customer_AZhk@placeholder.com",
              order_id=15570, insert_side_effect=None):
        if insert_side_effect is None:
            def insert_side_effect(*_a, **_kw):
                raise frappe.DuplicateEntryError("Duplicate entry for key 'PRIMARY'")

        with patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "release_savepoint"), \
             patch.object(customer_sync.frappe.db, "rollback"), \
             patch.object(customer_sync.frappe.db, "sql", side_effect=harness.sql), \
             patch.object(customer_sync.frappe.db, "get_value", return_value=None), \
             patch.object(customer_sync.frappe.db, "get_values", return_value=[]), \
             patch.object(customer_sync, "_field_exists", return_value=True), \
             patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             patch.object(customer_sync, "_update_customer_identity"), \
             patch.object(doc, "insert", side_effect=insert_side_effect):
            return customer_sync._safe_insert_customer(
                doc,
                woo_customer_id=woo_customer_id,
                username=username,
                phone_norm=phone_norm,
                email=email,
                order_id=order_id,
            )

    def test_returns_the_racing_row_instead_of_suffixing(self):
        """The production case: same woo id on both sides, invisible to the snapshot."""
        harness = _SnapshotRaceHarness({
            "name": "مصطفى مسعد",
            "woo_customer_id": "5973",
            "woo_username": "customer_azhk",
            "mobile_no": "01111034268",
            "email_id": "customer_AZhk@placeholder.com",
        })
        doc = _make_customer_doc("مصطفى مسعد")

        result = self._call(doc, harness)

        self.assertEqual(result, "مصطفى مسعد")
        self.assertNotIn("15570", result)
        self.assertNotIn("15570", doc.customer_name)

    def test_uses_a_locking_read_on_the_colliding_key(self):
        harness = _SnapshotRaceHarness({
            "name": "مصطفى مسعد",
            "woo_customer_id": "5973",
            "woo_username": None,
            "mobile_no": "01111034268",
            "email_id": None,
        })
        doc = _make_customer_doc("مصطفى مسعد")

        self._call(doc, harness)

        self.assertTrue(harness.locking_reads, "recovery never issued the committed read")
        query, values = harness.locking_reads[0]
        # Exclusive, not shared: the recovered record is written to immediately
        # afterwards, and two workers upgrading a shared lock would deadlock.
        self.assertIn("FOR UPDATE", query)
        self.assertIn("`tabCustomer`", query)
        self.assertEqual(values, ("مصطفى مسعد",))

    def test_address_recovery_uses_the_lighter_shared_lock(self):
        """The address path only reads, so it must not take an exclusive lock."""
        captured = []

        def _sql(query, values=None, as_dict=False):
            captured.append(query)
            return []

        with patch.object(customer_sync.frappe.db, "sql", side_effect=_sql):
            customer_sync._read_committed_row(
                "Address", "Someone-Shipping", ("name",), for_update=False
            )

        self.assertIn("LOCK IN SHARE MODE", captured[0])
        self.assertNotIn("FOR UPDATE", captured[0])

    def test_matches_on_phone_stored_in_the_other_spelling(self):
        """Racing row holds +20…, incoming order carries 0… — still the same person."""
        harness = _SnapshotRaceHarness({
            "name": "Hind Eltayeb",
            "woo_customer_id": None,
            "woo_username": None,
            "mobile_no": "+201558576130",
            "email_id": None,
        })
        doc = _make_customer_doc("Hind Eltayeb")

        result = self._call(doc, harness, woo_customer_id=None, phone_norm="01558576130")

        self.assertEqual(result, "Hind Eltayeb")

    def test_matches_on_email_when_no_other_identifier_is_shared(self):
        harness = _SnapshotRaceHarness({
            "name": "Dina mostafa",
            "woo_customer_id": None,
            "woo_username": None,
            "mobile_no": None,
            "email_id": "dina@example.com",
        })
        doc = _make_customer_doc("Dina mostafa")

        result = self._call(doc, harness, woo_customer_id=None, phone_norm=None,
                            email="dina@example.com")

        self.assertEqual(result, "Dina mostafa")

    def test_backfills_identity_onto_the_recovered_record(self):
        """The surviving record should learn the identifiers the loser carried."""
        harness = _SnapshotRaceHarness({
            "name": "مصطفى مسعد",
            "woo_customer_id": "5973",
            "woo_username": None,
            "mobile_no": "01111034268",
            "email_id": None,
        })
        doc = _make_customer_doc("مصطفى مسعد")

        def insert_side_effect(*_a, **_kw):
            raise frappe.DuplicateEntryError("Duplicate entry for key 'PRIMARY'")

        with patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "release_savepoint"), \
             patch.object(customer_sync.frappe.db, "rollback"), \
             patch.object(customer_sync.frappe.db, "sql", side_effect=harness.sql), \
             patch.object(customer_sync.frappe.db, "get_value", return_value=None), \
             patch.object(customer_sync.frappe.db, "get_values", return_value=[]), \
             patch.object(customer_sync, "_field_exists", return_value=True), \
             patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             patch.object(customer_sync, "_update_customer_identity") as update_identity, \
             patch.object(doc, "insert", side_effect=insert_side_effect):
            customer_sync._safe_insert_customer(
                doc,
                woo_customer_id=5973,
                username="customer_azhk",
                phone_norm="01111034268",
                email="customer_AZhk@placeholder.com",
                order_id=15570,
            )

        update_identity.assert_called_once()
        self.assertEqual(update_identity.call_args.args[0], "مصطفى مسعد")


class TestGenuineCollisionStillSuffixes(unittest.TestCase):
    """Two different people with one display name must still be kept apart."""

    def _run(self, committed_row, *, woo_customer_id, phone_norm, email):
        harness = _SnapshotRaceHarness(committed_row)
        doc = _make_customer_doc("Ahmed")
        insert_calls = {"n": 0}

        def insert_side_effect(*_a, **_kw):
            insert_calls["n"] += 1
            if insert_calls["n"] == 1:
                raise frappe.DuplicateEntryError("Duplicate entry for key 'PRIMARY'")
            doc.name = "Ahmed-16481"

        with patch.object(customer_sync.frappe.db, "savepoint"), \
             patch.object(customer_sync.frappe.db, "release_savepoint"), \
             patch.object(customer_sync.frappe.db, "rollback"), \
             patch.object(customer_sync.frappe.db, "sql", side_effect=harness.sql), \
             patch.object(customer_sync.frappe.db, "get_value", return_value=None), \
             patch.object(customer_sync.frappe.db, "get_values", return_value=[]), \
             patch.object(customer_sync, "_field_exists", return_value=True), \
             patch.object(customer_sync, "find_customer_by_woo_id", return_value=None), \
             patch.object(customer_sync, "_update_customer_identity"), \
             patch.object(doc, "insert", side_effect=insert_side_effect):
            result = customer_sync._safe_insert_customer(
                doc,
                woo_customer_id=woo_customer_id,
                username=None,
                phone_norm=phone_norm,
                email=email,
                order_id=16481,
            )
        return result, insert_calls["n"], doc

    def test_different_woo_id_is_a_real_collision(self):
        result, inserts, doc = self._run(
            {
                "name": "Ahmed",
                "woo_customer_id": "111",
                "woo_username": None,
                "mobile_no": "01000000000",
                "email_id": "other@example.com",
            },
            woo_customer_id=222,
            phone_norm="01999999999",
            email="mine@example.com",
        )
        self.assertEqual(inserts, 2)
        self.assertEqual(result, "Ahmed-16481")
        self.assertIn("16481", doc.customer_name)

    def test_no_shared_identifier_is_a_real_collision(self):
        result, inserts, _doc = self._run(
            {
                "name": "Ahmed",
                "woo_customer_id": None,
                "woo_username": None,
                "mobile_no": "01000000000",
                "email_id": None,
            },
            woo_customer_id=None,
            phone_norm="01999999999",
            email=None,
        )
        self.assertEqual(inserts, 2)
        self.assertEqual(result, "Ahmed-16481")

    def test_missing_committed_row_still_suffixes(self):
        """Collision on something other than the name — keep the old behaviour."""
        result, inserts, _doc = self._run(
            None,
            woo_customer_id=222,
            phone_norm="01999999999",
            email="mine@example.com",
        )
        self.assertEqual(inserts, 2)
        self.assertEqual(result, "Ahmed-16481")


class TestCustomerIsSameIdentity(unittest.TestCase):
    """The race/collision decision rule, in isolation."""

    def _row(self, **overrides):
        row = {
            "name": "Someone",
            "woo_customer_id": None,
            "woo_username": None,
            "mobile_no": None,
            "email_id": None,
        }
        row.update(overrides)
        return row

    def test_same_woo_id_matches(self):
        self.assertTrue(customer_sync._customer_is_same_identity(
            self._row(woo_customer_id="5973"),
            woo_customer_id=5973, username=None, phone_norm=None, email=None,
        ))

    def test_different_woo_id_is_decisive_even_if_email_matches(self):
        """A confirmed different Woo account outranks a shared placeholder email."""
        self.assertFalse(customer_sync._customer_is_same_identity(
            self._row(woo_customer_id="111", email_id="customer@placeholder.com"),
            woo_customer_id=222, username=None, phone_norm=None,
            email="customer@placeholder.com",
        ))

    def test_phone_matches_across_spellings(self):
        self.assertTrue(customer_sync._customer_is_same_identity(
            self._row(mobile_no="+201111034268"),
            woo_customer_id=None, username=None, phone_norm="01111034268", email=None,
        ))

    def test_username_matches(self):
        self.assertTrue(customer_sync._customer_is_same_identity(
            self._row(woo_username="customer_azhk"),
            woo_customer_id=None, username="customer_azhk", phone_norm=None, email=None,
        ))

    def test_email_matches_case_insensitively(self):
        self.assertTrue(customer_sync._customer_is_same_identity(
            self._row(email_id="Customer_AZhk@Placeholder.com"),
            woo_customer_id=None, username=None, phone_norm=None,
            email="customer_azhk@placeholder.com",
        ))

    def test_no_evidence_is_not_a_match(self):
        """Two anonymous records with the same name are not assumed to be one person."""
        self.assertFalse(customer_sync._customer_is_same_identity(
            self._row(),
            woo_customer_id=None, username=None, phone_norm=None, email=None,
        ))


class TestReadCommittedRow(unittest.TestCase):
    """_read_committed_row must never be able to abort a sync."""

    def test_falls_back_to_a_snapshot_read_when_the_locking_read_fails(self):
        with patch.object(customer_sync.frappe.db, "sql", side_effect=RuntimeError("no lock support")), \
             patch.object(customer_sync.frappe.db, "get_value", return_value={"name": "CUST-1"}):
            row = customer_sync._read_committed_row("Customer", "CUST-1", ("name",))
        self.assertEqual(row, {"name": "CUST-1"})

    def test_returns_none_when_both_reads_fail(self):
        with patch.object(customer_sync.frappe.db, "sql", side_effect=RuntimeError("boom")), \
             patch.object(customer_sync.frappe.db, "get_value", side_effect=RuntimeError("boom")):
            self.assertIsNone(customer_sync._read_committed_row("Customer", "CUST-1", ("name",)))

    def test_blank_name_never_queries(self):
        with patch.object(customer_sync.frappe.db, "sql") as sql:
            self.assertIsNone(customer_sync._read_committed_row("Customer", "", ("name",)))
        sql.assert_not_called()


if __name__ == "__main__":
    unittest.main()
