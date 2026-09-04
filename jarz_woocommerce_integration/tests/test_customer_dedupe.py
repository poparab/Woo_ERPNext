"""The dedupe tool's judgement, tested where it is most dangerous: what it
refuses to merge.

A shared phone is not a shared person. Production holds 'فاديه توفيق' and
'Karim abulnaga' on one handset; merging them would be a worse outcome than the
duplicates the tool exists to clean up.
"""

from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from jarz_woocommerce_integration.services import customer_dedupe


def _cust(name, phone, woo="", created="2026-06-01 00:00:00", currency=None,
          lead=None, email=None):
    return {
        "name": name, "customer_name": name, "mobile_no": phone,
        "email_id": email, "woo_customer_id": woo, "default_currency": currency,
        "disabled": 0, "lead_name": lead, "creation": created,
    }


def _stats(submitted=0, revenue=0.0, last=""):
    return {"submitted": submitted, "draft": 0, "cancelled": 0,
            "revenue": revenue, "outstanding": 0.0, "last_date": last}


class TestBaseName(unittest.TestCase):
    """Both machine-made suffixes must reduce to the same identity."""

    def test_strips_the_sync_race_order_id_suffix(self):
        self.assertEqual(customer_dedupe.base_name("مصطفى مسعد-15570"), "مصطفى مسعد")

    def test_strips_the_erpnext_collision_counter(self):
        self.assertEqual(customer_dedupe.base_name("عماد - 7"), "عماد")

    def test_strips_stacked_suffixes(self):
        self.assertEqual(customer_dedupe.base_name("احمد كمال - 2"), "احمد كمال")
        self.assertEqual(customer_dedupe.base_name("Salma - 6"), "salma")

    def test_leaves_an_unsuffixed_name_alone(self):
        self.assertEqual(customer_dedupe.base_name("Ahmed Nashaat"), "ahmed nashaat")

    def test_does_not_eat_a_number_that_is_part_of_the_name(self):
        """'Ahmed 9' has no dash, so nothing is stripped."""
        self.assertEqual(customer_dedupe.base_name("Ahmed 9"), "ahmed 9")

    def test_normalises_case_and_whitespace(self):
        self.assertEqual(customer_dedupe.base_name("  Ahmed   Sabry "), "ahmed sabry")


class TestPickSurvivor(unittest.TestCase):
    """The group must collapse onto where the account actually lives."""

    def _member(self, name, submitted, last="", created="2026-06-01 00:00:00"):
        return {"name": name, "creation": created,
                "stats": _stats(submitted=submitted, last=last)}

    def test_prefers_the_record_with_the_most_invoices(self):
        winner = customer_dedupe.pick_survivor([
            self._member("Ahmed - 19", 1, "2026-08-01"),
            self._member("Ahmed", 142, "2026-07-01"),
        ])
        self.assertEqual(winner["name"], "Ahmed")

    def test_count_outranks_recency(self):
        """A newer stray must never beat the account holding the history."""
        winner = customer_dedupe.pick_survivor([
            self._member("stray", 1, "2026-08-18"),
            self._member("account", 40, "2026-01-02"),
        ])
        self.assertEqual(winner["name"], "account")

    def test_recency_breaks_a_count_tie(self):
        winner = customer_dedupe.pick_survivor([
            self._member("older", 3, "2026-02-01"),
            self._member("newer", 3, "2026-08-01"),
        ])
        self.assertEqual(winner["name"], "newer")

    def test_is_deterministic_when_everything_ties(self):
        members = [self._member("b", 0), self._member("a", 0)]
        self.assertEqual(
            customer_dedupe.pick_survivor(members)["name"],
            customer_dedupe.pick_survivor(list(reversed(members)))["name"],
        )


class TestBuildPlanClassification(unittest.TestCase):
    """What lands in AUTO versus REVIEW."""

    def _plan(self, customers, stats=None):
        stats = stats or {}
        with patch.object(customer_dedupe, "_load_customers", return_value=customers), \
             patch.object(customer_dedupe, "_invoice_stats",
                          side_effect=lambda names: {n: stats.get(n, _stats()) for n in names}):
            return customer_dedupe.build_plan()

    def test_same_base_name_is_auto_merged(self):
        plan = self._plan([
            _cust("مصطفى مسعد", "01111034268", "5973"),
            _cust("مصطفى مسعد-15570", "01111034268", "5973", created="2026-06-01 00:00:03"),
        ], {"مصطفى مسعد-15570": _stats(8, 2462.0, "2026-08-06")})
        self.assertEqual(len(plan["auto"]), 1)
        self.assertEqual(plan["auto"][0]["survivor"], "مصطفى مسعد-15570")
        self.assertEqual(plan["auto"][0]["losers"], ["مصطفى مسعد"])
        self.assertEqual(plan["auto"][0]["clean_name"], "مصطفى مسعد")

    def test_different_names_go_to_review(self):
        """The case that makes automation dangerous."""
        plan = self._plan([
            _cust("فاديه توفيق", "01001010178", "196"),
            _cust("Karim abulnaga", "01001010178", "3387"),
        ])
        self.assertEqual(plan["auto"], [])
        self.assertEqual(len(plan["review"]), 1)
        self.assertIn("different names", plan["review"][0]["reason"])

    def test_different_names_sharing_an_exclusive_woo_id_are_auto_merged(self):
        """One real Woo account both records claim is proof enough."""
        plan = self._plan([
            _cust("ايهاب عطيه", "01000038300", "5084"),
            _cust("Ehab Attia", "01000038300", "5084"),
        ])
        self.assertEqual(len(plan["auto"]), 1)
        self.assertIn("5084", plan["auto"][0]["evidence"])

    def test_a_woo_id_held_outside_the_group_is_not_evidence(self):
        """woo id 3357 is held by 215 unrelated customers; it proves nothing."""
        plan = self._plan([
            _cust("خالد مصطفى", "01000810094", "3357"),
            _cust("رنيم الجندى", "01000810094", "3357"),
            _cust("someone else", "01999999999", "3357"),
            _cust("another one", "01888888888", "3357"),
        ])
        self.assertEqual(plan["auto"], [])
        self.assertEqual(len(plan["review"]), 1)

    def test_mixed_currency_goes_to_review(self):
        plan = self._plan([
            _cust("Ahmed", "01111034268", "1", currency="EGP"),
            _cust("Ahmed - 1", "01111034268", "2", currency="USD"),
        ])
        self.assertEqual(plan["auto"], [])
        self.assertIn("currency", plan["review"][0]["reason"])

    def test_singleton_phones_are_not_groups(self):
        plan = self._plan([_cust("Solo", "01111034268", "1")])
        self.assertEqual(plan["auto"], [])
        self.assertEqual(plan["review"], [])

    def test_the_two_phone_spellings_form_one_group(self):
        """+20… and 0… are the same subscriber, so they must group together."""
        plan = self._plan([
            _cust("Hind Eltayeb", "01558576130", "1"),
            _cust("Hind Eltayeb - 1", "+201558576130", "2"),
        ])
        self.assertEqual(len(plan["auto"]), 1)
        self.assertEqual(plan["auto"][0]["size"], 2)


class TestRenameEntryPoint(unittest.TestCase):
    """Which rename_doc the tool calls is not incidental.

    The `frappe.rename_doc` alias has no ignore_permissions parameter and raises
    TypeError if handed one — caught only on a real bench, because the mocks
    accept anything. The model-level function is the one with the full signature.
    """

    def test_uses_the_model_level_function(self):
        import inspect

        self.assertEqual(
            customer_dedupe.rename_doc.__module__, "frappe.model.rename_doc"
        )
        params = inspect.signature(customer_dedupe.rename_doc).parameters
        for required in ("merge", "force", "ignore_permissions", "rebuild_search"):
            with self.subTest(param=required):
                self.assertIn(required, params)


class TestMergeGroupSafety(unittest.TestCase):
    """The invariants a merge must not break."""

    def _group(self):
        return {
            "phone": "01111034268",
            "survivor": "SURV",
            "losers": ["LOSER"],
            "clean_name": "",
            "evidence": "identical base name",
            "members": [
                {"name": "SURV", "lead_name": ""},
                {"name": "LOSER", "lead_name": ""},
            ],
        }

    def test_dry_run_never_writes(self):
        with patch.object(customer_dedupe, "_snapshot", return_value={}), \
             patch.object(customer_dedupe, "rename_doc") as rename:
            result = customer_dedupe.merge_group(self._group(), apply=False)
        rename.assert_not_called()
        self.assertFalse(result["applied"])
        self.assertEqual(result["would_merge"], ["LOSER"])

    def test_a_broken_invariant_rolls_the_group_back(self):
        before = {"submitted": 9, "draft": 0, "cancelled": 0, "revenue": 100.0,
                  "outstanding": 0.0, "gl_rows": 20, "gl_debit": 100.0,
                  "gl_credit": 100.0, "pe_rows": 2, "pe_paid": 100.0, "addresses": 2}
        after = dict(before, submitted=8)  # an invoice went missing

        with patch.object(customer_dedupe, "_snapshot", side_effect=[before, after]), \
             patch.object(customer_dedupe, "_restore_lead_statuses", return_value={}), \
             patch.object(customer_dedupe, "rename_doc"), \
             patch.object(customer_dedupe.frappe.db, "savepoint"), \
             patch.object(customer_dedupe.frappe.db, "release_savepoint"), \
             patch.object(customer_dedupe.frappe.db, "commit") as commit, \
             patch.object(customer_dedupe.frappe.db, "rollback") as rollback, \
             patch.object(customer_dedupe.frappe.db, "exists", return_value=True):
            result = customer_dedupe.merge_group(self._group(), apply=True)

        self.assertFalse(result["applied"])
        self.assertTrue(any("submitted" in p for p in result["problems"]))
        rollback.assert_called_once()
        commit.assert_not_called()

    def test_a_surviving_loser_is_treated_as_failure(self):
        snap = {"submitted": 0, "draft": 0, "cancelled": 0, "revenue": 0.0,
                "outstanding": 0.0, "gl_rows": 0, "gl_debit": 0.0, "gl_credit": 0.0,
                "pe_rows": 0, "pe_paid": 0.0, "addresses": 0}
        with patch.object(customer_dedupe, "_snapshot", return_value=snap), \
             patch.object(customer_dedupe, "_restore_lead_statuses", return_value={}), \
             patch.object(customer_dedupe, "rename_doc"), \
             patch.object(customer_dedupe.frappe.db, "savepoint"), \
             patch.object(customer_dedupe.frappe.db, "release_savepoint"), \
             patch.object(customer_dedupe.frappe.db, "commit"), \
             patch.object(customer_dedupe.frappe.db, "rollback") as rollback, \
             patch.object(customer_dedupe.frappe.db, "exists", return_value=True):
            result = customer_dedupe.merge_group(self._group(), apply=True)
        self.assertFalse(result["applied"])
        self.assertTrue(any("still present" in p for p in result["problems"]))
        rollback.assert_called_once()

    def test_restoring_the_clean_name_is_not_mistaken_for_a_failed_merge(self):
        """The survivor ends up holding a loser's freed name — by design.

        Checking for leftovers after the restore cannot tell that apart from a
        loser that refused to merge, and rolled back three good merges.
        """
        group = dict(self._group(), clean_name="LOSER")
        snap = {"submitted": 3, "draft": 0, "cancelled": 0, "revenue": 300.0,
                "outstanding": 0.0, "gl_rows": 6, "gl_debit": 300.0,
                "gl_credit": 300.0, "pe_rows": 1, "pe_paid": 300.0, "addresses": 1}
        live = {"SURV", "LOSER"}

        def _exists(_doctype, name):
            return name in live

        def _rename(_dt, old, new, **_kw):
            # Absorbing frees the loser's name; the restore then claims it.
            live.discard(old)
            live.add(new)

        with patch.object(customer_dedupe, "_snapshot", return_value=snap), \
             patch.object(customer_dedupe, "_restore_lead_statuses", return_value={}), \
             patch.object(customer_dedupe, "rename_doc", side_effect=_rename), \
             patch.object(customer_dedupe.frappe.db, "savepoint"), \
             patch.object(customer_dedupe.frappe.db, "release_savepoint"), \
             patch.object(customer_dedupe.frappe.db, "commit"), \
             patch.object(customer_dedupe.frappe.db, "rollback") as rollback, \
             patch.object(customer_dedupe.frappe.db, "exists", side_effect=_exists):
            result = customer_dedupe.merge_group(group, apply=True,
                                                 restore_clean_name=True)

        self.assertTrue(result["applied"], result["problems"])
        self.assertEqual(result["final_name"], "LOSER")
        rollback.assert_not_called()

    def test_an_exception_rolls_back_rather_than_propagating(self):
        with patch.object(customer_dedupe, "_snapshot", return_value={}), \
             patch.object(customer_dedupe, "_restore_lead_statuses", return_value={}), \
             patch.object(customer_dedupe, "rename_doc",
                          side_effect=RuntimeError("link exists")), \
             patch.object(customer_dedupe.frappe.db, "savepoint"), \
             patch.object(customer_dedupe.frappe.db, "rollback") as rollback:
            result = customer_dedupe.merge_group(self._group(), apply=True)
        self.assertFalse(result["applied"])
        self.assertIn("link exists", result["problems"][0])
        rollback.assert_called_once()

    def test_lead_statuses_are_put_back(self):
        """Customer.on_trash resets a converted Lead; the rep's stage must survive."""
        snap = {"submitted": 0, "draft": 0, "cancelled": 0, "revenue": 0.0,
                "outstanding": 0.0, "gl_rows": 0, "gl_debit": 0.0, "gl_credit": 0.0,
                "pe_rows": 0, "pe_paid": 0.0, "addresses": 0}
        with patch.object(customer_dedupe, "_snapshot", return_value=snap), \
             patch.object(customer_dedupe, "_restore_lead_statuses",
                          return_value={"LEAD-1": "Converted"}), \
             patch.object(customer_dedupe, "rename_doc"), \
             patch.object(customer_dedupe.frappe.db, "savepoint"), \
             patch.object(customer_dedupe.frappe.db, "release_savepoint"), \
             patch.object(customer_dedupe.frappe.db, "commit"), \
             patch.object(customer_dedupe.frappe.db, "rollback"), \
             patch.object(customer_dedupe.frappe.db, "set_value") as set_value, \
             patch.object(customer_dedupe.frappe.db, "exists",
                          side_effect=lambda dt, n: n == "SURV"):
            customer_dedupe.merge_group(self._group(), apply=True)
        set_value.assert_called_once_with("Lead", "LEAD-1", "status", "Converted",
                                          update_modified=False)


class TestDiff(unittest.TestCase):

    def _snap(self, **over):
        base = {"submitted": 5, "draft": 0, "cancelled": 1, "revenue": 1000.0,
                "outstanding": 50.0, "gl_rows": 12, "gl_debit": 1000.0,
                "gl_credit": 950.0, "pe_rows": 3, "pe_paid": 950.0, "addresses": 3}
        base.update(over)
        return base

    def test_identical_snapshots_have_no_problems(self):
        self.assertEqual(customer_dedupe._diff(self._snap(), self._snap()), [])

    def test_rounding_noise_is_tolerated(self):
        self.assertEqual(
            customer_dedupe._diff(self._snap(), self._snap(revenue=1000.005)), []
        )

    def test_lost_money_is_flagged(self):
        problems = customer_dedupe._diff(self._snap(), self._snap(outstanding=0.0))
        self.assertTrue(any("outstanding" in p for p in problems))

    def test_addresses_may_collapse_but_never_grow(self):
        self.assertEqual(customer_dedupe._diff(self._snap(), self._snap(addresses=2)), [])
        self.assertTrue(customer_dedupe._diff(self._snap(), self._snap(addresses=4)))


# ---------------------------------------------------------------------------
# FIX 5 — a merge must not silently drop a WooCommerce binding
# ---------------------------------------------------------------------------

class TestSurvivorPrefersABoundRecord(unittest.TestCase):
    """`rename_doc(merge=True)` keeps the survivor's fields and deletes the loser.

    Electing an unbound shadow therefore throws the WooCommerce binding away, and
    an unbound Customer's orders can never appear under the shopper's My Account.
    """

    def _member(self, name, submitted, woo="", last="", created="2026-06-01 00:00:00"):
        return {"name": name, "creation": created, "woo_customer_id": woo,
                "stats": _stats(submitted=submitted, last=last)}

    def test_a_bound_record_wins_a_tie_against_an_unbound_one(self):
        winner = customer_dedupe.pick_survivor([
            self._member("shadow", 3, woo="", last="2026-08-01"),
            self._member("bound", 3, woo="4211", last="2026-08-01"),
        ])
        self.assertEqual(winner["name"], "bound")

    def test_the_preference_beats_recency_and_creation_order(self):
        """Both weaker tie-breaks point at the shadow; the binding must still win."""
        winner = customer_dedupe.pick_survivor([
            self._member("shadow", 3, woo="", last="2026-08-30",
                         created="2026-01-01 00:00:00"),
            self._member("bound", 3, woo="4211", last="2026-02-01",
                         created="2026-09-01 00:00:00"),
        ])
        self.assertEqual(winner["name"], "bound")

    def test_invoice_history_still_outranks_a_binding(self):
        """A one-invoice bound stray must not be handed a 142-invoice account.

        When this happens the binding is not lost quietly — `_diff` aborts the
        merge and the group is reported for a human.
        """
        winner = customer_dedupe.pick_survivor([
            self._member("bound stray", 1, woo="4211", last="2026-08-30"),
            self._member("account", 142, woo="", last="2026-07-01"),
        ])
        self.assertEqual(winner["name"], "account")

    def test_a_blank_binding_is_not_treated_as_a_binding(self):
        for empty in ("", "   ", None, 0):
            with self.subTest(empty=empty):
                winner = customer_dedupe.pick_survivor([
                    self._member("a", 1, woo=empty, created="2026-01-01 00:00:00"),
                    self._member("b", 1, woo="4211", created="2026-02-01 00:00:00"),
                ])
                self.assertEqual(winner["name"], "b")

    def test_members_without_the_key_at_all_do_not_crash(self):
        """`pick_survivor` is called with hand-built members in other callers."""
        winner = customer_dedupe.pick_survivor([
            {"name": "b", "creation": "2026-01-01 00:00:00", "stats": _stats(0)},
            {"name": "a", "creation": "2026-01-01 00:00:00", "stats": _stats(0)},
        ])
        self.assertEqual(winner["name"], "a")

    def test_build_plan_supplies_the_key_this_ranking_reads(self):
        """Contract check, end to end through the planner.

        The unbound ``Ahmed`` is older and sorts first by name, so every other
        tie-break in the key points at it. Only the binding preference can make
        ``Ahmed - 2`` survive — which is what stops the merge from deleting the
        record that holds woo id 5973.
        """
        with patch.object(customer_dedupe, "_load_customers", return_value=[
            _cust("Ahmed", "01111034268", "", created="2026-01-01 00:00:00"),
            _cust("Ahmed - 2", "01111034268", "5973", created="2026-05-01 00:00:00"),
        ]), patch.object(customer_dedupe, "_invoice_stats",
                         side_effect=lambda names: {n: _stats() for n in names}):
            plan = customer_dedupe.build_plan()

        self.assertEqual(len(plan["auto"]), 1)
        self.assertIn("woo_customer_id", plan["auto"][0]["members"][0])
        self.assertEqual(plan["auto"][0]["survivor"], "Ahmed - 2")
        self.assertEqual(plan["auto"][0]["losers"], ["Ahmed"])


class TestWooIds(unittest.TestCase):
    """The snapshot probe behind the abort."""

    @staticmethod
    def _db(sql):
        """A whole `frappe.db` stand-in, so this runs with or without a site."""
        return patch.object(customer_dedupe.frappe, "db",
                            SimpleNamespace(sql=sql), create=True)

    def _ids(self, rows, column=True):
        with patch.object(customer_dedupe, "customer_woo_id_column_exists",
                          return_value=column), \
             self._db(lambda *_a, **_kw: rows):
            return customer_dedupe._woo_ids(["A", "B"])

    def test_collects_the_distinct_bindings(self):
        self.assertEqual(self._ids([("4211",), ("5084",), ("4211",)]),
                         ["4211", "5084"])

    def test_blank_and_zero_are_not_bindings(self):
        self.assertEqual(self._ids([("",), (None,), ("0",), ("  ",)]), [])

    def test_a_missing_column_yields_nothing_and_is_not_queried(self):
        sql = MagicMock()
        with patch.object(customer_dedupe, "customer_woo_id_column_exists",
                          return_value=False), self._db(sql):
            self.assertEqual(customer_dedupe._woo_ids(["A"]), [])
        sql.assert_not_called()

    def test_a_failing_probe_degrades_instead_of_breaking_the_run(self):
        def _boom(*_a, **_kw):
            raise RuntimeError("boom")

        with patch.object(customer_dedupe, "customer_woo_id_column_exists",
                          return_value=True), self._db(_boom):
            self.assertEqual(customer_dedupe._woo_ids(["A"]), [])

    def test_an_empty_name_list_is_never_queried(self):
        """An empty IN() clause is a SQL syntax error."""
        sql = MagicMock()
        with patch.object(customer_dedupe, "customer_woo_id_column_exists",
                          return_value=True), self._db(sql):
            self.assertEqual(customer_dedupe._woo_ids([]), [])
        sql.assert_not_called()


class TestDiffGuardsTheBinding(unittest.TestCase):

    def _snap(self, **over):
        base = {"submitted": 5, "draft": 0, "cancelled": 1, "revenue": 1000.0,
                "outstanding": 50.0, "gl_rows": 12, "gl_debit": 1000.0,
                "gl_credit": 950.0, "pe_rows": 3, "pe_paid": 950.0, "addresses": 3,
                "woo_ids": ["4211"]}
        base.update(over)
        return base

    def test_a_dropped_binding_is_a_problem(self):
        problems = customer_dedupe._diff(self._snap(), self._snap(woo_ids=[]))
        self.assertTrue(any("woo_customer_id lost" in p for p in problems), problems)
        self.assertTrue(any("4211" in p for p in problems), problems)

    def test_a_binding_that_survives_is_not_a_problem(self):
        self.assertEqual(customer_dedupe._diff(self._snap(), self._snap()), [])

    def test_losing_one_of_two_bindings_is_still_a_problem(self):
        problems = customer_dedupe._diff(
            self._snap(woo_ids=["4211", "5084"]), self._snap(woo_ids=["4211"])
        )
        self.assertTrue(any("5084" in p for p in problems), problems)

    def test_a_snapshot_without_the_key_does_not_crash(self):
        """Callers hand-build snapshots; a missing key means 'not measured'."""
        bare = {k: v for k, v in self._snap().items() if k != "woo_ids"}
        self.assertEqual(customer_dedupe._diff(bare, dict(bare)), [])


class TestMergeAbortsOnALostBinding(unittest.TestCase):
    """The latent hazard: a repair run would drive straight through this."""

    def _group(self):
        return {
            "phone": "01097503380",
            "survivor": "shadow",
            "losers": ["bound"],
            "clean_name": "",
            "evidence": "identical base name",
            "members": [
                {"name": "shadow", "lead_name": ""},
                {"name": "bound", "lead_name": ""},
            ],
        }

    def _snap(self, **over):
        base = {"submitted": 3, "draft": 0, "cancelled": 0, "revenue": 300.0,
                "outstanding": 0.0, "gl_rows": 6, "gl_debit": 300.0,
                "gl_credit": 300.0, "pe_rows": 1, "pe_paid": 300.0, "addresses": 1,
                "woo_ids": ["4211"]}
        base.update(over)
        return base

    def _merge(self, before, after):
        with patch.object(customer_dedupe, "_snapshot", side_effect=[before, after]), \
             patch.object(customer_dedupe, "_restore_lead_statuses", return_value={}), \
             patch.object(customer_dedupe, "rename_doc"), \
             patch.object(customer_dedupe.frappe.db, "savepoint"), \
             patch.object(customer_dedupe.frappe.db, "release_savepoint"), \
             patch.object(customer_dedupe.frappe.db, "commit") as commit, \
             patch.object(customer_dedupe.frappe.db, "rollback") as rollback, \
             patch.object(customer_dedupe.frappe.db, "exists",
                          side_effect=lambda _dt, n: n == "shadow"):
            result = customer_dedupe.merge_group(self._group(), apply=True)
        return result, commit, rollback

    def test_a_merge_that_drops_the_binding_is_rolled_back(self):
        result, commit, rollback = self._merge(self._snap(), self._snap(woo_ids=[]))

        self.assertFalse(result["applied"])
        self.assertTrue(any("woo_customer_id lost" in p for p in result["problems"]),
                        result["problems"])
        rollback.assert_called_once()
        commit.assert_not_called()

    def test_a_merge_that_keeps_the_binding_still_succeeds(self):
        """The guard must not turn every merge into a failure."""
        result, commit, rollback = self._merge(self._snap(), self._snap())

        self.assertTrue(result["applied"], result["problems"])
        rollback.assert_not_called()
        commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
