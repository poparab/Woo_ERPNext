"""A merged-away Customer's Woo account must keep resolving to the survivor.

Two branches of one shop ("Orbt speciality coffee" on Woo account 6540 and
"Orbt speciality Coffee - 1" on 7000) are merged into one Customer. The
survivor keeps 7000; 6540 would otherwise vanish with the deleted record, and
the next order, order update or profile event from it would find nobody and
mint the duplicate again. These tests pin:

* ``find_customer_by_woo_id`` answers for an alias only when it is the id's
  sole holder, and refuses to guess when anyone else holds it (own or alias);
* the "already claimed" guard counts alias holders, so the id is never stamped
  on a stranger;
* the after_rename hook carries the source's binding to the survivor -- as its primary
  when it has none, otherwise as an alias -- and ignore plain renames;
* a profile sync from an absorbed account routes to the survivor but never
  rewrites its name, phone, email, addresses or territory.
"""

import unittest
from unittest.mock import patch

import frappe

from jarz_woocommerce_integration.services import customer_merge_aliases as hooks
from jarz_woocommerce_integration.services import customer_sync
from jarz_woocommerce_integration.utils import customer_woo_id as cwi


class _Rows:
    """A tiny Customer table: name -> {woo_customer_id, woo_customer_id_aliases}."""

    def __init__(self, rows):
        self.rows = rows
        self.writes = []

    def get_values(self, doctype, filters, fieldname, **kwargs):
        (field, cond), = filters.items()
        out = []
        for name, row in self.rows.items():
            value = str(row.get(field) or "")
            if isinstance(cond, list) and cond[0] == "like":
                if cond[1].strip("%") in value:
                    out.append(name)
            elif value == cond:
                out.append(name)
        return out[: kwargs.get("limit") or None]

    def get_value(self, doctype, name, field, *args, **kwargs):
        return self.rows.get(name, {}).get(field)

    def set_value(self, doctype, name, field, value=None, **kwargs):
        updates = field if isinstance(field, dict) else {field: value}
        self.rows.setdefault(name, {}).update(updates)
        self.writes.append((name, updates))


def _table(rows):
    table = _Rows(rows)
    return table, [
        patch.object(cwi, "_customer_has_column", return_value=True),
        patch.object(cwi.frappe.db, "get_values", side_effect=table.get_values),
        patch.object(cwi.frappe.db, "get_value", side_effect=table.get_value),
        patch.object(cwi.frappe.db, "set_value", side_effect=table.set_value),
    ]


class _Patched(unittest.TestCase):
    ROWS = {}

    def setUp(self):
        self.table, patches = _table({k: dict(v) for k, v in self.ROWS.items()})
        for p in patches:
            p.start()
            self.addCleanup(p.stop)


class TestAliasFormat(unittest.TestCase):
    def test_round_trip_and_normalisation(self):
        self.assertEqual(cwi.parse_woo_id_aliases(",6540, 7011,,0,abc,6540,"), ["6540", "7011"])
        self.assertEqual(cwi.format_woo_id_aliases(["7011", "6540", "7011"]), ",7011,6540,")
        self.assertEqual(cwi.format_woo_id_aliases([]), "")


class TestAliasResolution(_Patched):
    ROWS = {
        "Orbt speciality Coffee - 1": {"woo_customer_id": "7000", "woo_customer_id_aliases": ",6540,"},
        "Someone": {"woo_customer_id": "65400"},
    }

    def test_absorbed_account_resolves_to_the_survivor(self):
        self.assertEqual(cwi.find_customer_by_woo_id(6540), "Orbt speciality Coffee - 1")

    def test_own_id_still_resolves(self):
        self.assertEqual(cwi.find_customer_by_woo_id("7000"), "Orbt speciality Coffee - 1")

    def test_alias_never_matches_a_longer_id(self):
        # ",6540," must not answer for 654, and 65400 belongs to its own holder.
        self.assertIsNone(cwi.find_customer_by_woo_id(654))
        self.assertEqual(cwi.find_customer_by_woo_id(65400), "Someone")

    def test_an_id_also_held_as_someone_elses_own_is_ambiguous(self):
        # Staging 2026-09-23: 5274 was a stranger's own id AND the merged
        # survivor's alias. Resolving to the stranger would hand them the
        # absorbed branch's orders; the only safe answer is "ambiguous".
        self.table.rows["Stranger"] = {"woo_customer_id": "6540"}
        self.assertIsNone(cwi.find_customer_by_woo_id(6540))

    def test_two_alias_holders_are_ambiguous(self):
        self.table.rows["Other"] = {"woo_customer_id_aliases": ",6540,"}
        self.assertIsNone(cwi.find_customer_by_woo_id(6540))

    def test_alias_holder_counts_as_claiming_the_id(self):
        self.assertTrue(cwi.customer_woo_id_is_claimed_by_other(6540, "Brand new customer"))
        self.assertFalse(cwi.customer_woo_id_is_claimed_by_other(6540, "Orbt speciality Coffee - 1"))

    def test_holds_as_alias(self):
        self.assertTrue(cwi.customer_holds_woo_id_as_alias("Orbt speciality Coffee - 1", 6540))
        self.assertFalse(cwi.customer_holds_woo_id_as_alias("Orbt speciality Coffee - 1", 7000))
        self.assertFalse(cwi.customer_holds_woo_id_as_alias("Someone", 6540))

    def test_record_never_aliases_the_primary_and_dedups(self):
        out = cwi.record_woo_id_aliases("Orbt speciality Coffee - 1", ["7000", "6540", "7011"])
        self.assertEqual(out, ["6540", "7011"])
        self.assertEqual(
            self.table.rows["Orbt speciality Coffee - 1"]["woo_customer_id_aliases"], ",6540,7011,"
        )

    def test_record_is_a_no_op_without_the_column(self):
        with patch.object(cwi, "_customer_has_column", return_value=False):
            self.assertEqual(cwi.record_woo_id_aliases("Orbt speciality Coffee - 1", ["1"]), [])


class TestRenameHook(_Patched):
    ROWS = {
        "Orbt speciality coffee": {"woo_customer_id": "6540", "woo_customer_id_aliases": ",5000,"},
        "Orbt speciality Coffee - 1": {"woo_customer_id": "7000"},
        "No Woo": {},
    }

    def _merge(self, source, target):
        # Frappe's order (rename_doc): links rewritten -> after_rename on the
        # survivor -> delete_doc(source). The source row is still readable here.
        hooks.carry_source_binding(None, "after_rename", source, target, True)
        self.table.rows.pop(source, None)

    def test_differing_ids_become_aliases_of_the_survivor(self):
        self._merge("Orbt speciality coffee", "Orbt speciality Coffee - 1")
        survivor = self.table.rows["Orbt speciality Coffee - 1"]
        self.assertEqual(survivor["woo_customer_id"], "7000")
        self.assertEqual(cwi.parse_woo_id_aliases(survivor["woo_customer_id_aliases"]), ["6540", "5000"])
        self.assertEqual(cwi.find_customer_by_woo_id(6540), "Orbt speciality Coffee - 1")
        self.assertEqual(cwi.find_customer_by_woo_id(5000), "Orbt speciality Coffee - 1")
        self.assertEqual(cwi.find_customer_by_woo_id(7000), "Orbt speciality Coffee - 1")

    def test_survivor_without_an_id_adopts_the_sources_as_primary(self):
        self._merge("Orbt speciality coffee", "No Woo")
        survivor = self.table.rows["No Woo"]
        self.assertEqual(survivor["woo_customer_id"], "6540")
        self.assertEqual(cwi.parse_woo_id_aliases(survivor.get("woo_customer_id_aliases")), ["5000"])

    def test_plain_rename_is_ignored(self):
        hooks.carry_source_binding(None, "after_rename", "Orbt speciality coffee", "Renamed", False)
        self.assertEqual(self.table.writes, [])

    def test_source_without_binding_writes_nothing(self):
        self._merge("No Woo", "Orbt speciality Coffee - 1")
        self.assertEqual(self.table.writes, [])

    def test_hook_returns_none_so_rename_doc_output_is_untouched(self):
        self.assertIsNone(hooks.carry_source_binding(None, "after_rename", "Orbt speciality coffee", "X", True))

    def test_no_op_without_the_column(self):
        with patch.object(hooks, "customer_woo_id_column_exists", return_value=False):
            hooks.carry_source_binding(None, "after_rename", "Orbt speciality coffee", "Orbt speciality Coffee - 1", True)
        self.assertEqual(self.table.writes, [])


class TestHookRunsThroughFrappeDispatch(unittest.TestCase):
    """The registered path must be the real function, called the way Frappe calls it."""

    def test_customer_after_rename_is_wired(self):
        from jarz_woocommerce_integration import hooks as app_hooks

        events = app_hooks.doc_events["Customer"]
        self.assertEqual(
            events["after_rename"],
            "jarz_woocommerce_integration.services.customer_merge_aliases.carry_source_binding",
        )
        self.assertNotIn("before_rename", events)
        module, _, attr = events["after_rename"].rpartition(".")
        import importlib

        self.assertIs(getattr(importlib.import_module(module), attr), hooks.carry_source_binding)

    def test_signature_matches_document_hook_runner(self):
        # Document.hook calls f(doc, method, *args) with args = (old, new, merge).
        with patch.object(hooks, "customer_woo_id_column_exists", return_value=True), \
             patch.object(hooks, "get_customer_woo_id", side_effect=lambda n: {"A": "1", "B": "2"}.get(n)), \
             patch.object(hooks, "get_customer_woo_id_aliases", return_value=[]), \
             patch.object(hooks, "record_woo_id_aliases") as record, \
             patch.object(hooks, "set_customer_woo_id") as set_primary:
            hooks.carry_source_binding(object(), "after_rename", "A", "B", True)
        record.assert_called_once_with("B", ["1"])
        set_primary.assert_not_called()


class TestProfileSyncFromAbsorbedAccount(unittest.TestCase):
    PAYLOAD = {
        "id": 6540,
        "email": "orbt.hayat@example.com",
        "billing": {"first_name": "Orbt", "last_name": "Hayat", "phone": "01009853333",
                    "address_1": "Hayat Town Mall", "state": "EGOBOUR"},
        "shipping": {},
    }

    def test_alias_routes_but_never_rewrites_the_survivor(self):
        with patch.object(customer_sync, "_ensure_customer", return_value="Orbt speciality Coffee - 1"), \
             patch.object(customer_sync, "customer_holds_woo_id_as_alias", return_value=True), \
             patch.object(customer_sync, "_update_customer_identity") as identity, \
             patch.object(customer_sync, "_find_existing_address_for_customer") as find_addr, \
             patch.object(customer_sync, "_create_address") as create_addr, \
             patch.object(customer_sync, "_resolve_territory_from_state") as territory:
            out = customer_sync._sync_customer_payload(dict(self.PAYLOAD))
        self.assertEqual(out["customer"], "Orbt speciality Coffee - 1")
        self.assertTrue(out["alias"])
        identity.assert_not_called()
        find_addr.assert_not_called()
        create_addr.assert_not_called()
        territory.assert_not_called()

    def test_own_account_still_updates_identity(self):
        with patch.object(customer_sync, "_ensure_customer", return_value="Orbt speciality Coffee - 1"), \
             patch.object(customer_sync, "customer_holds_woo_id_as_alias", return_value=False), \
             patch.object(customer_sync, "_update_customer_identity") as identity, \
             patch.object(customer_sync, "_has_usable_source_address", return_value=False), \
             patch.object(customer_sync, "_resolve_territory_from_state", return_value=None):
            customer_sync._sync_customer_payload(dict(self.PAYLOAD, id=7000))
        identity.assert_called_once()
        self.assertTrue(identity.call_args.kwargs["overwrite_existing"])


class TestOrderPathResolvesAbsorbedAccount(unittest.TestCase):
    def _ensure(self, alias):
        with patch.object(customer_sync, "_field_exists", return_value=True), \
             patch.object(customer_sync, "find_customer_by_woo_id", return_value="Orbt speciality Coffee - 1"), \
             patch.object(customer_sync, "customer_holds_woo_id_as_alias", return_value=alias), \
             patch.object(customer_sync, "_update_customer_identity") as identity, \
             patch.object(customer_sync, "_safe_insert_customer") as insert:
            name = customer_sync._ensure_customer(
                "orbt.hayat@example.com", "Orbt", "Hayat", 99001,
                phone="01009853333", woo_customer_id=6540 if alias else 7000,
            )
        return name, identity, insert

    def test_new_order_from_absorbed_account_lands_on_survivor_and_writes_nothing(self):
        name, identity, insert = self._ensure(alias=True)
        self.assertEqual(name, "Orbt speciality Coffee - 1")
        insert.assert_not_called()
        # Not even the fill-blanks write: username/phone/email are the old account's.
        identity.assert_not_called()

    def test_own_account_order_still_fills_blanks(self):
        name, identity, insert = self._ensure(alias=False)
        self.assertEqual(name, "Orbt speciality Coffee - 1")
        insert.assert_not_called()
        identity.assert_called_once()
        self.assertFalse(identity.call_args.kwargs.get("overwrite_existing", False))


class TestOutboundKeepsEachOrderOnItsAccount(unittest.TestCase):
    """A status push must never move a branch's order into the other branch's Woo account."""

    def _pick(self, snapshot, existing, own="7000", aliases=("6540",)):
        from jarz_woocommerce_integration.services import outbound_sync

        invoice = {"woo_customer_id_snapshot": snapshot}
        customer = type("C", (), {"name": "Orbt speciality Coffee - 1", "woo_customer_id": own})()
        with patch.object(outbound_sync, "get_customer_woo_id", return_value=own), \
             patch.object(outbound_sync, "get_customer_woo_id_aliases", return_value=list(aliases)):
            return outbound_sync._order_woo_customer_id(
                invoice, customer, {"customer_id": existing} if existing is not None else None
            )

    def test_absorbed_branch_order_stays_on_its_account(self):
        self.assertEqual(self._pick("6540", 6540), "6540")

    def test_snapshot_wins_when_store_already_moved_it(self):
        self.assertEqual(self._pick("6540", 7000), "6540")

    def test_new_pos_order_goes_to_the_survivors_own_account(self):
        self.assertEqual(self._pick("", None), "7000")

    def test_order_reassigned_from_a_stranger_moves_as_before(self):
        self.assertEqual(self._pick("999", 999), "7000")

    def test_without_aliases_it_is_always_the_own_id(self):
        self.assertEqual(self._pick("6540", 6540, aliases=()), "7000")

    def test_kept_account_does_not_trigger_an_update(self):
        from jarz_woocommerce_integration.services import outbound_sync

        # Same customer_id on both sides: that key alone must not force a PUT.
        existing = {"customer_id": 6540, "status": "processing"}
        payload = {"customer_id": 6540, "status": "processing"}
        with patch.object(outbound_sync, "_meta_entries_to_map", return_value={}):
            changed = outbound_sync._order_payload_requires_update(existing, payload)
        self.assertFalse(changed)
        with patch.object(outbound_sync, "_meta_entries_to_map", return_value={}):
            moved = outbound_sync._order_payload_requires_update(existing, dict(payload, customer_id=7000))
        self.assertTrue(moved)


class TestColumnCacheNeverRemembersAMissingAliasColumn(unittest.TestCase):
    def test_negative_answer_for_the_alias_field_is_not_cached(self):
        cwi._CUSTOMER_COLUMN_CACHE.pop(cwi.ALIAS_FIELD, None)
        with patch.object(cwi.frappe.db, "sql", return_value=[]):
            self.assertFalse(cwi._customer_has_column(cwi.ALIAS_FIELD))
        self.assertNotIn(cwi.ALIAS_FIELD, cwi._CUSTOMER_COLUMN_CACHE)
        with patch.object(cwi.frappe.db, "sql", return_value=[{"1": 1}]):
            self.assertTrue(cwi._customer_has_column(cwi.ALIAS_FIELD))
        self.assertTrue(cwi._CUSTOMER_COLUMN_CACHE[cwi.ALIAS_FIELD])
        cwi._CUSTOMER_COLUMN_CACHE.pop(cwi.ALIAS_FIELD, None)


class TestDedupeAndCleanupRespectAliases(unittest.TestCase):
    def test_dedupe_sends_same_name_different_woo_accounts_to_review(self):
        from jarz_woocommerce_integration.services import customer_dedupe

        row = {"customer_name": "Orbt", "mobile_no": "01009853333", "email_id": None,
               "default_currency": None, "disabled": 0, "lead_name": None}
        customers = [
            dict(row, name="Orbt", woo_customer_id="6540", creation="2026-06-01 00:00:00"),
            dict(row, name="Orbt - 1", woo_customer_id="7000", creation="2026-06-02 00:00:00"),
        ]
        with patch.object(customer_dedupe, "_load_customers", return_value=customers), \
             patch.object(customer_dedupe, "_invoice_stats", side_effect=lambda names: {n: {} for n in names}):
            plan = customer_dedupe.build_plan()
        self.assertEqual(plan["auto"], [])
        self.assertIn("different woo_customer_ids", plan["review"][0]["reason"])

    def test_cleanup_classifies_an_absorbed_account_as_alias(self):
        from jarz_woocommerce_integration.services import customer_cleanup

        indexes = customer_cleanup._build_customer_indexes([
            {"name": "Orbt speciality Coffee - 1", "woo_customer_id": "7000",
             "woo_customer_id_aliases": ",6540,", "mobile_no": "01037258106"},
        ])
        out = customer_cleanup._resolve_woo_customer({"id": 6540, "billing": {"phone": "01009853333"}}, indexes)
        self.assertEqual(out, {"bucket": "alias_woo_id", "customer": "Orbt speciality Coffee - 1"})
        own = customer_cleanup._resolve_woo_customer({"id": 7000, "billing": {}}, indexes)
        self.assertEqual(own["bucket"], "exact_woo_id")


if __name__ == "__main__":
    unittest.main()
