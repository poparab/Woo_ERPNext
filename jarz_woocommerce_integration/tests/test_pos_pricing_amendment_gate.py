"""
The Woo item-edit gate compares line identity, never price
==========================================================

Follow-up to Woo order 17756 (2026-10-01; see test_invoice_bound_price_list.py).
Pricing the rebuild from the invoice's own price list closed the B2B case, but
the gate still compared ``rate`` / ``price_list_rate`` / ``discount_*``. Target
lines are always rebuilt at ERPNext list rates (standalone) or BundleProcessor
rates (bundles) — WooCommerce never supplies a price — so any POS pricing
decision on a line (a line discount, a ``custom_rate_override``, a 100%-discount
Sample/Employee line) made every hash change read as "items changed" and the
Woo amendment rebuilt the order at list price.

Now:

(a) an echo on a submitted POS order with a 10% line discount and identical
    items -> submitted_frozen, no enqueue;
(b) the same with a 100%-discount (rate 0) line -> frozen;
(c) a GENUINE edit (extra item) on an order whose untouched line carries a
    rate override -> NeedsReview, no enqueue (the rebuild would drop the price);
    likewise a qty change on a discounted line;
(d) a genuine edit on a plain list-priced order -> still enqueues;
(e) a genuine bundle mix edit (child item codes change, child rates move, the
    bundle TOTAL stays) -> still enqueues; a POS-discounted bundle (total below
    the bundle price) with a genuine edit -> NeedsReview;
(f) the replacement created with ``amended_from`` keeps the source's purpose,
    commercial policy, policy reason, no-courier and pickup flags, and gets no
    shipping row when the source charged none (pickup, or a policy such as
    "B2B Supply" that zeroes shipping income);
(g) the amendment JOB re-checks under the invoice lock: identity now matches ->
    ``items_already_match`` skip; would re-price -> review; rebuild fails ->
    review, never amend.

(a)/(b) run the REAL ``_build_invoice_items`` against the faked Item Price table
from test_invoice_bound_price_list; the others stub the builder so the target
lines are explicit — the gate only consumes ``lines``.
"""
from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from jarz_woocommerce_integration.services import order_sync, outbound_sync, sync_events
from jarz_woocommerce_integration.tests._monkeypatch import MonkeyPatch
from jarz_woocommerce_integration.tests.test_invoice_bound_price_list import (
    JAR_ITEM,
    JAR_QTY,
    LIVE_INVOICE,
    PROFILE_LIST,
    TERRITORY,
    _install,
    _invoice_doc,
    _settings,
    _woo_order,
)

COOKIE_ITEM = "COOKIE-BOX"
LIST_RATE = 120.0  # ITEM_PRICES[PROFILE_LIST] in the shared harness
BUNDLE_ITEM = "BUNDLE-BOX"
BUNDLE_CODE = "WJB-BOX"


def _jar_row(rate: float, *, price_list_rate: float = LIST_RATE, discount_percentage: float = 0.0, **extra) -> dict:
    row = {
        "item_code": JAR_ITEM,
        "qty": JAR_QTY,
        "rate": rate,
        "price_list_rate": price_list_rate,
        "discount_percentage": discount_percentage,
        "discount_amount": round(price_list_rate - rate, 2),
    }
    row.update(extra)
    return row


def _cookie_row(rate: float = 50.0, qty: float = 2) -> dict:
    return {"item_code": COOKIE_ITEM, "qty": qty, "rate": rate, "price_list_rate": rate}


def _bundle_rows(
    children: list[tuple[str, float, float]],
    *,
    parent_rate: float = 0.0,
    link_key: str = BUNDLE_CODE,
    child_qty: float = 2,
) -> list[dict]:
    """One bundle copy in BundleProcessor's layout.

    Parent: ``is_bundle_parent`` + ``bundle_code``, 100% discount. A SAVED
    invoice has rate 0 there; an unsaved BundleProcessor row still carries
    ``rate = price_list_rate`` (pass ``parent_rate=480.0``). Children:
    ``is_bundle_child`` + ``parent_bundle`` = the bundle code, discount derived
    from (rate, price_list_rate).
    """
    rows = [{
        "item_code": BUNDLE_ITEM,
        "qty": 1,
        "rate": parent_rate,
        "price_list_rate": 480.0,
        "discount_percentage": 100,
        "is_bundle_parent": 1,
        "bundle_code": link_key,
    }]
    for item_code, rate, price_list_rate in children:
        discount = round((price_list_rate - rate) / price_list_rate * 100.0, 6) if price_list_rate else 0.0
        rows.append({
            "item_code": item_code,
            "qty": child_qty,
            "rate": rate,
            "price_list_rate": price_list_rate,
            "discount_percentage": discount,
            "is_bundle_child": 1,
            "parent_bundle": link_key,
        })
    return rows


# A bundle priced 480: two children at 150 list, 20% off -> 4 x 120 = 480.
MIX_AB = [("CHILD-A", 120.0, 150.0), ("CHILD-B", 120.0, 150.0)]
# A different mix, same bundle price: 2 x 96 + 2 x 144 = 480.
MIX_AC = [("CHILD-A", 96.0, 120.0), ("CHILD-C", 144.0, 180.0)]


class _GateCase(unittest.TestCase):
    """process_order_phase1 against a submitted, linked invoice on the profile list."""

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)

    def _run(self, invoice_items: list[dict], *, target_lines: list[dict] | None = None, enable_amendment: int = 1):
        live = _invoice_doc(LIVE_INVOICE, docstatus=1, unit_rate=LIST_RATE, selling_price_list=PROFILE_LIST)
        live.items = [dict(row) for row in invoice_items]
        rec = _install(
            self.monkeypatch,
            map_link=LIVE_INVOICE,
            sales_invoices={LIVE_INVOICE: {"docstatus": 1, "selling_price_list": PROFILE_LIST}},
            docs={LIVE_INVOICE: live},
            live_invoice=LIVE_INVOICE,
        )
        if target_lines is not None:
            def stub_build(order, price_list=None, cache=None, is_historical=False):
                lines = [dict(line) for line in target_lines]
                rec["lines"] = [dict(line) for line in lines]
                return lines, [], {}

            self.monkeypatch.setattr(order_sync, "_build_invoice_items", stub_build)
        # Not our own push: the gate must decide on content alone.
        self.monkeypatch.setattr(outbound_sync, "outbound_push_recently_pushed", lambda woo_order_id: False)
        self.sync_log = MagicMock()
        self.monkeypatch.setattr(order_sync, "create_sync_log_entry", self.sync_log)

        result = order_sync.process_order_phase1(_woo_order(), _settings(enable_amendment=enable_amendment))
        return result, live, rec

    def _log_messages(self, operation: str | None = None) -> list[str]:
        messages = []
        for call in self.sync_log.call_args_list:
            if operation and (not call.args or call.args[0] != operation):
                continue
            messages.append(str(call.args[2] if len(call.args) > 2 else ""))
        return messages


class TestEchoOnPosPricedOrderIsFrozen(_GateCase):
    def test_a_line_discount_echo_is_frozen_not_amended(self):
        """(a) 10% POS line discount, identical items: the old gate amended this."""
        result, live, rec = self._run([_jar_row(108.0, discount_percentage=10)])

        # The rebuilt target is at list price — exactly the gap the old gate saw.
        self.assertEqual(rec["lines"][0]["rate"], LIST_RATE)
        self.assertEqual(result.get("reason"), "submitted_frozen", result)
        order_sync.frappe.enqueue.assert_not_called()
        order_sync._flag_order_map_for_manual_review.assert_not_called()
        live.save.assert_not_called()
        # The hash-refresh branch, not the final catch-all frozen return.
        self.assertTrue(
            any("invoice items still match" in m for m in self._log_messages("InboundSkip")),
            self._log_messages(),
        )

    def test_b_full_discount_sample_line_echo_is_frozen(self):
        """(b) A 100%-discount (rate 0) Sample/Employee line is a POS decision too."""
        result, live, rec = self._run([_jar_row(0.0, discount_percentage=100)])

        self.assertEqual(rec["lines"][0]["rate"], LIST_RATE)
        self.assertEqual(result.get("reason"), "submitted_frozen", result)
        order_sync.frappe.enqueue.assert_not_called()
        order_sync._flag_order_map_for_manual_review.assert_not_called()


class TestGenuineEditOnPosPricedOrder(_GateCase):
    def test_c_extra_item_on_rate_override_order_needs_review(self):
        """(c) The customer adds an item; the untouched jar line was sold at 90 by override."""
        result, live, rec = self._run(
            [_jar_row(90.0, custom_rate_override=1)],
            target_lines=[_jar_row(LIST_RATE), _cookie_row()],
        )

        self.assertEqual(result.get("status"), "skipped", result)
        self.assertEqual(result.get("reason"), "needs_manual_review", result)
        self.assertEqual(
            result.get("repriced_lines"),
            [{"item_code": JAR_ITEM, "invoice_rate": 90.0, "rebuild_rate": LIST_RATE}],
        )
        order_sync.frappe.enqueue.assert_not_called()
        order_sync._flag_order_map_for_manual_review.assert_called_once()
        reason = order_sync._flag_order_map_for_manual_review.call_args.kwargs["reason"]
        self.assertIn("POS pricing the rebuild would lose", reason)
        self.assertIn(f"{JAR_ITEM}: invoice rate 90.00 -> rebuild rate 120.00", reason)
        self.assertTrue(
            any("POS pricing" in m for m in self._log_messages("ItemEditDetected")),
            self._log_messages(),
        )
        statuses = [c.args[1] for c in self.sync_log.call_args_list if c.args and c.args[0] == "ItemEditDetected"]
        self.assertEqual(statuses, ["NeedsReview"])

    def test_c2_qty_change_on_a_discounted_line_needs_review(self):
        """The customer changes the qty of the very line the POS discounted."""
        result, live, rec = self._run(
            [_jar_row(108.0, discount_percentage=10)],
            target_lines=[dict(_jar_row(LIST_RATE), qty=JAR_QTY + 1)],
        )

        self.assertEqual(result.get("reason"), "needs_manual_review", result)
        self.assertEqual(
            result.get("repriced_lines"),
            [{"item_code": JAR_ITEM, "invoice_rate": 108.0, "rebuild_rate": LIST_RATE}],
        )
        order_sync.frappe.enqueue.assert_not_called()

    def test_d_extra_item_on_list_priced_order_still_enqueues(self):
        """(d) Nothing to lose: the amendment stays automatic."""
        result, live, rec = self._run(
            [_jar_row(LIST_RATE)],
            target_lines=[_jar_row(LIST_RATE), _cookie_row()],
        )

        self.assertEqual(result.get("status"), "queued", result)
        self.assertEqual(result.get("reason"), "amendment_enqueued", result)
        order_sync.frappe.enqueue.assert_called_once()
        self.assertIn("order_amendment.run_woo_amendment_job", order_sync.frappe.enqueue.call_args.args[0])
        order_sync._flag_order_map_for_manual_review.assert_not_called()

    def test_e_bundle_mix_edit_still_enqueues(self):
        """(e) Child item codes change and child rates move; the bundle total stays 480."""
        invoice = _bundle_rows(MIX_AB) + [_jar_row(LIST_RATE)]
        # The rebuild is unsaved BundleProcessor output: parent rate = list rate.
        target = _bundle_rows(MIX_AC, parent_rate=480.0) + [_jar_row(LIST_RATE)]

        result, live, rec = self._run(invoice, target_lines=target)

        self.assertEqual(result.get("reason"), "amendment_enqueued", result)
        order_sync.frappe.enqueue.assert_called_once()
        order_sync._flag_order_map_for_manual_review.assert_not_called()

    def test_e2_bundle_mix_edit_beside_a_pos_discounted_line_needs_review(self):
        """The bundle edit itself is fine; the discounted standalone line next to it is not."""
        invoice = _bundle_rows(MIX_AB) + [_jar_row(108.0, discount_percentage=10)]
        target = _bundle_rows(MIX_AC, parent_rate=480.0) + [_jar_row(LIST_RATE)]

        result, live, rec = self._run(invoice, target_lines=target)

        self.assertEqual(result.get("reason"), "needs_manual_review", result)
        self.assertEqual([d["item_code"] for d in result.get("repriced_lines")], [JAR_ITEM])
        order_sync.frappe.enqueue.assert_not_called()

    def test_e3_pos_discounted_bundle_with_genuine_edit_needs_review(self):
        """The POS sold the bundle at 432 (extra discount); the rebuild would bill 480."""
        invoice = _bundle_rows([("CHILD-A", 108.0, 150.0), ("CHILD-B", 108.0, 150.0)])
        target = _bundle_rows(MIX_AB, parent_rate=480.0) + [_cookie_row()]

        result, live, rec = self._run(invoice, target_lines=target)

        self.assertEqual(result.get("reason"), "needs_manual_review", result)
        self.assertEqual(
            result.get("repriced_lines"),
            [{"item_code": BUNDLE_ITEM, "kind": "bundle", "invoice_rate": 432.0, "rebuild_rate": 480.0}],
        )
        reason = order_sync._flag_order_map_for_manual_review.call_args.kwargs["reason"]
        self.assertIn(f"{BUNDLE_ITEM} (bundle total): invoice 432.00 -> rebuild 480.00", reason)
        order_sync.frappe.enqueue.assert_not_called()

    def test_flag_off_keeps_the_existing_review_reason(self):
        """The new branch only replaces an enqueue; flag-off review is unchanged."""
        result, live, rec = self._run(
            [_jar_row(90.0)],
            target_lines=[_jar_row(LIST_RATE), _cookie_row()],
            enable_amendment=0,
        )

        self.assertEqual(result.get("reason"), "needs_manual_review", result)
        self.assertNotIn("repriced_lines", result)
        reason = order_sync._flag_order_map_for_manual_review.call_args.kwargs["reason"]
        self.assertIn("enable_inbound_amendment=off", reason)


class TestReasonClassification(unittest.TestCase):
    def test_needs_manual_review_is_a_review(self):
        """The re-price branch reuses ``needs_manual_review`` — no new token."""
        self.assertEqual(sync_events._classify_inbound_order_skip_reason("needs_manual_review"), "review")
        self.assertNotIn("needs_manual_review", order_sync.SKIPPED_SUCCESS_REASONS)

    def test_items_already_match_is_a_successful_skip_in_both_registries(self):
        self.assertIn("items_already_match", sync_events.SKIP_REASON_TOKENS)
        self.assertEqual(sync_events._classify_inbound_order_skip_reason("items_already_match"), "skip")
        self.assertIn("items_already_match", order_sync.SKIPPED_SUCCESS_REASONS)

    def test_recheck_failure_is_a_review_not_a_skip(self):
        self.assertEqual(sync_events._classify_inbound_order_skip_reason("amendment_recheck_failed"), "review")
        self.assertNotIn("amendment_recheck_failed", order_sync.SKIPPED_SUCCESS_REASONS)


# ---------------------------------------------------------------------------
# Helpers in isolation
# ---------------------------------------------------------------------------

class _Inv:
    def __init__(self, items, name="ACC-SINV-TEST"):
        self.items = items
        self.name = name

    def get(self, fieldname, default=None):
        return getattr(self, fieldname, default)


class TestLineIdentity(unittest.TestCase):
    def test_price_fields_are_ignored(self):
        inv = _Inv([_jar_row(0.0, discount_percentage=100)])
        self.assertTrue(order_sync._submitted_invoice_matches_target_lines(inv, [_jar_row(LIST_RATE)]))

    def test_qty_change_is_detected(self):
        inv = _Inv([_jar_row(LIST_RATE)])
        target = [dict(_jar_row(LIST_RATE), qty=JAR_QTY + 1)]
        self.assertFalse(order_sync._submitted_invoice_matches_target_lines(inv, target))

    def test_item_change_is_detected(self):
        inv = _Inv([_jar_row(LIST_RATE)])
        target = [dict(_jar_row(LIST_RATE), item_code="JAR-LARGE")]
        self.assertFalse(order_sync._submitted_invoice_matches_target_lines(inv, target))

    def test_extra_line_is_detected(self):
        inv = _Inv([_jar_row(LIST_RATE)])
        self.assertFalse(order_sync._submitted_invoice_matches_target_lines(inv, [_jar_row(LIST_RATE), _cookie_row()]))

    def test_bundle_structure_is_part_of_identity(self):
        rows = _bundle_rows([("CHILD-A", 120.0, 150.0)])
        moved = [dict(row) for row in rows]
        moved[1]["parent_bundle"] = "OTHER-BUNDLE"
        self.assertTrue(order_sync._submitted_invoice_matches_target_lines(_Inv(rows), [dict(r) for r in rows]))
        self.assertFalse(order_sync._submitted_invoice_matches_target_lines(_Inv(rows), moved))

    def test_bool_and_int_flags_compare_equal(self):
        invoice_rows = _bundle_rows([("CHILD-A", 120.0, 150.0)])
        target_rows = [dict(row) for row in invoice_rows]
        target_rows[0]["is_bundle_parent"] = True
        target_rows[1]["is_bundle_child"] = True
        self.assertTrue(order_sync._submitted_invoice_matches_target_lines(_Inv(invoice_rows), target_rows))


class TestAmendmentWouldRepriceUnchangedLines(unittest.TestCase):
    def _diff(self, invoice_rows, target_rows):
        return order_sync._amendment_would_reprice_unchanged_lines(_Inv(invoice_rows), target_rows)

    def test_list_priced_lines_report_nothing(self):
        self.assertEqual(self._diff([_jar_row(LIST_RATE)], [_jar_row(LIST_RATE), _cookie_row()]), [])

    def test_half_a_piastre_rounds_away(self):
        self.assertEqual(self._diff([_jar_row(119.995)], [_jar_row(LIST_RATE)]), [])

    def test_exactly_one_piastre_is_not_a_difference(self):
        """The docstring says MORE than 0.01; 120.00 - 119.99 must not flag (float-safe)."""
        self.assertEqual(self._diff([_jar_row(119.99)], [_jar_row(LIST_RATE)]), [])
        self.assertEqual(self._diff([_jar_row(0.07)], [_jar_row(0.06)]), [])

    def test_two_piastres_is_a_difference(self):
        self.assertEqual(
            self._diff([_jar_row(119.98)], [_jar_row(LIST_RATE)]),
            [{"item_code": JAR_ITEM, "invoice_rate": 119.98, "rebuild_rate": LIST_RATE}],
        )

    def test_items_on_one_side_only_are_not_compared(self):
        self.assertEqual(self._diff([_cookie_row(rate=10.0)], [_jar_row(LIST_RATE)]), [])

    def test_bundle_children_are_not_compared_per_line(self):
        """A pure mix edit moves child rates but keeps the bundle total."""
        self.assertEqual(self._diff(_bundle_rows(MIX_AB), _bundle_rows(MIX_AC, parent_rate=480.0)), [])

    def test_unsaved_parent_rate_counts_as_zero(self):
        """BundleProcessor's parent row says rate=480 with 100% discount; it bills 0."""
        self.assertEqual(self._diff(_bundle_rows(MIX_AB), _bundle_rows(MIX_AB, parent_rate=480.0)), [])

    def test_pos_discounted_bundle_total_is_reported(self):
        invoice = _bundle_rows([("CHILD-A", 108.0, 150.0), ("CHILD-B", 108.0, 150.0)])
        self.assertEqual(
            self._diff(invoice, _bundle_rows(MIX_AB, parent_rate=480.0)),
            [{"item_code": BUNDLE_ITEM, "kind": "bundle", "invoice_rate": 432.0, "rebuild_rate": 480.0}],
        )

    def test_child_rounding_drift_is_tolerated(self):
        """4 child units -> tolerance 1 + ceil(0.5 * 4) = 3 piastres: 480.00 vs 479.98 passes."""
        target = _bundle_rows([("CHILD-A", 120.0, 150.0), ("CHILD-B", 119.99, 150.0)], parent_rate=480.0)
        self.assertEqual(self._diff(_bundle_rows(MIX_AB), target), [])

    def test_undiscounted_children_mean_a_mix_dependent_total_and_are_skipped(self):
        """Bundle price above the picked children's list sum: BundleProcessor bills the
        children at full price, so the total follows the mix and cannot be compared."""
        invoice = _bundle_rows([("CHILD-A", 100.0, 100.0), ("CHILD-B", 100.0, 100.0)])  # 400, no discount
        target = _bundle_rows([("CHILD-A", 100.0, 100.0), ("CHILD-C", 110.0, 110.0)], parent_rate=480.0)
        self.assertEqual(self._diff(invoice, target), [])

    def test_undiscounted_children_on_another_price_basis_are_reported(self):
        """W2-a: a POS bundle on B2B Selling bills its children at 77 (bundle price 400
        is above their 308 sum, so no discount); the Woo rebuild prices the same
        children from retail 120 and discounts them down to 400. The undiscounted
        skip must not let that through."""
        invoice = _bundle_rows([("CHILD-A", 77.0, 77.0), ("CHILD-B", 77.0, 77.0)])  # 308
        target = _bundle_rows([("CHILD-A", 100.0, 120.0), ("CHILD-B", 100.0, 120.0)], parent_rate=480.0)  # 400
        diff = self._diff(invoice, target)
        self.assertEqual(len(diff), 1, diff)
        self.assertEqual(diff[0]["kind"], "bundle")
        self.assertEqual((diff[0]["invoice_rate"], diff[0]["rebuild_rate"]), (308.0, 400.0))

    def test_undiscounted_mix_edit_on_the_same_basis_is_still_skipped(self):
        """Same children list rates on both sides: the total follows the mix, no report."""
        invoice = _bundle_rows([("CHILD-A", 100.0, 100.0), ("CHILD-B", 100.0, 100.0)])
        target = _bundle_rows([("CHILD-A", 100.0, 100.0), ("CHILD-C", 110.0, 110.0)], parent_rate=480.0)
        self.assertEqual(self._diff(invoice, target), [])

    def test_copies_of_the_same_bundle_are_compared_per_copy(self):
        """Woo 17748 shape: the same bundle twice; only the second copy was discounted."""
        invoice = _bundle_rows(MIX_AB) + _bundle_rows([("CHILD-A", 100.0, 150.0), ("CHILD-B", 100.0, 150.0)])
        target = _bundle_rows(MIX_AB, parent_rate=480.0) + _bundle_rows(MIX_AC, parent_rate=480.0)
        self.assertEqual(
            self._diff(invoice, target),
            [{"item_code": f"{BUNDLE_ITEM}#2", "kind": "bundle", "invoice_rate": 400.0, "rebuild_rate": 480.0}],
        )

    def test_bundles_pair_by_parent_item_when_link_keys_differ(self):
        """A POS invoice may name the bundle link differently from the Woo rebuild."""
        invoice = _bundle_rows([("CHILD-A", 108.0, 150.0), ("CHILD-B", 108.0, 150.0)], link_key="POS-BUNDLE-7")
        target = _bundle_rows(MIX_AB, parent_rate=480.0)
        self.assertEqual([d["item_code"] for d in self._diff(invoice, target)], [BUNDLE_ITEM])

    def test_same_item_as_bundle_child_and_standalone_compares_each_in_its_own_way(self):
        invoice = _bundle_rows([(JAR_ITEM, 60.0, 120.0)]) + [_jar_row(LIST_RATE)]
        target = _bundle_rows([(JAR_ITEM, 60.0, 120.0)], parent_rate=480.0) + [_jar_row(LIST_RATE)]
        self.assertEqual(self._diff(invoice, target), [])

    def test_rows_of_one_item_are_aggregated(self):
        """One jar sold at list, one given away: weighted 60 vs the rebuild's 120."""
        invoice = [
            {"item_code": JAR_ITEM, "qty": 1, "rate": LIST_RATE},
            {"item_code": JAR_ITEM, "qty": 1, "rate": 0.0, "discount_percentage": 100},
        ]
        target = [{"item_code": JAR_ITEM, "qty": 2, "rate": LIST_RATE}]
        self.assertEqual(
            self._diff(invoice, target),
            [{"item_code": JAR_ITEM, "invoice_rate": 60.0, "rebuild_rate": LIST_RATE}],
        )

    def test_missing_rate_falls_back_to_amount_over_qty(self):
        invoice = [{"item_code": JAR_ITEM, "qty": 4, "amount": 400.0}]
        self.assertEqual(
            self._diff(invoice, [_jar_row(LIST_RATE)]),
            [{"item_code": JAR_ITEM, "invoice_rate": 100.0, "rebuild_rate": LIST_RATE}],
        )


class TestRebuildTargetLinesForInvoice(unittest.TestCase):
    """The job's rebuild prices like the gate: the invoice's own list wins."""

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)
        self.monkeypatch.setattr(order_sync.frappe, "logger", lambda *a, **kw: MagicMock())

        def get_value(doctype, name=None, fieldname=None, *args, **kwargs):
            if doctype == "POS Profile" and fieldname == "selling_price_list":
                return PROFILE_LIST
            if doctype == "Sales Invoice" and name == LIVE_INVOICE:
                return {"docstatus": 1, "selling_price_list": "B2B Selling"}
            if doctype == "Price List":
                return {"selling": 1, "enabled": 1}
            return None

        self.monkeypatch.setattr(order_sync.frappe.db, "get_value", get_value)
        self.invoice = SimpleNamespace(name=LIVE_INVOICE, pos_profile="Nasr city", company="_Test Company")

    def test_uses_the_invoice_bound_price_list(self):
        seen = {}

        def build(order, price_list=None, cache=None, is_historical=False):
            seen["price_list"] = price_list
            return [_jar_row(77.0)], [], {}

        self.monkeypatch.setattr(order_sync, "_build_invoice_items", build)
        lines = order_sync._rebuild_target_lines_for_invoice(_woo_order(), self.invoice, woo_id=1)
        self.assertEqual(seen["price_list"], "B2B Selling")
        self.assertEqual(lines[0]["rate"], 77.0)

    def test_unmapped_items_raise_instead_of_returning_half_an_order(self):
        self.monkeypatch.setattr(
            order_sync, "_build_invoice_items", lambda *a, **kw: ([_jar_row(LIST_RATE)], [{"sku": "GONE"}], {})
        )
        with self.assertRaises(ValueError):
            order_sync._rebuild_target_lines_for_invoice(_woo_order(), self.invoice, woo_id=1)

    def test_no_lines_raise(self):
        self.monkeypatch.setattr(order_sync, "_build_invoice_items", lambda *a, **kw: ([], [], {}))
        with self.assertRaises(ValueError):
            order_sync._rebuild_target_lines_for_invoice(_woo_order(), self.invoice, woo_id=1)


# ---------------------------------------------------------------------------
# (f) The replacement keeps the source's context and its zero shipping
# ---------------------------------------------------------------------------

ALL_CONTEXT_FIELDS = (
    "custom_order_purpose",
    "custom_commercial_policy",
    "custom_policy_reason",
    "custom_is_pickup",
    "custom_no_courier",
)
SOURCE_SHIPPING_ROW = {
    "charge_type": "Actual",
    "account_head": "Shipping Income - J",
    "description": f"Shipping Income ({TERRITORY})",
    "tax_amount": 60.0,
}


class _Meta:
    def __init__(self, fields):
        self._fields = set(fields)

    def has_field(self, fieldname):
        return fieldname in self._fields

    def get_field(self, fieldname):
        return SimpleNamespace(fieldname=fieldname) if fieldname in self._fields else None


class TestReplacementKeepsSourceContext(unittest.TestCase):
    SOURCE = LIVE_INVOICE
    TERRITORY_DELIVERY_INCOME = 60.0
    _UNREADABLE = object()

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)

    def _run(self, source_row: dict, *, source_taxes=(), meta_fields=ALL_CONTEXT_FIELDS):
        real_apply_delivery = order_sync._apply_delivery_charge_policy
        source = _invoice_doc(self.SOURCE, docstatus=2, unit_rate=LIST_RATE, selling_price_list=PROFILE_LIST)
        rec = _install(
            self.monkeypatch,
            map_link=self.SOURCE,
            sales_invoices={self.SOURCE: dict({"docstatus": 2, "selling_price_list": PROFILE_LIST}, **source_row)},
            docs={self.SOURCE: source},
            live_invoice=None,
        )
        # Run the REAL delivery policy against a territory that charges 60, so a
        # replacement that wrongly gets a shipping row fails the test.
        self.monkeypatch.setattr(order_sync, "_apply_delivery_charge_policy", real_apply_delivery)
        self.monkeypatch.setattr(order_sync.frappe, "get_meta", lambda doctype: _Meta(meta_fields))
        base_get_value = order_sync.frappe.db.get_value
        base_exists = order_sync.frappe.db.exists
        base_get_all = order_sync.frappe.get_all

        def get_value(doctype, name=None, fieldname=None, *args, **kwargs):
            if doctype == "Territory" and name == TERRITORY and fieldname == "delivery_income":
                return self.TERRITORY_DELIVERY_INCOME
            return base_get_value(doctype, name, fieldname, *args, **kwargs)

        def exists(doctype, name=None, *args, **kwargs):
            if doctype == "Territory" and name == TERRITORY:
                return True
            return base_exists(doctype, name, *args, **kwargs)

        def get_all(doctype, filters=None, fields=None, *args, **kwargs):
            if doctype == "Sales Taxes and Charges":
                if source_taxes is self._UNREADABLE:
                    raise RuntimeError("Lost connection to MySQL server")
                assert (filters or {}).get("parent") == self.SOURCE, filters
                return [dict(row) for row in source_taxes]
            return base_get_all(doctype, filters, fields, *args, **kwargs)

        self.monkeypatch.setattr(order_sync.frappe.db, "get_value", get_value)
        self.monkeypatch.setattr(order_sync.frappe.db, "exists", exists)
        self.monkeypatch.setattr(order_sync.frappe, "get_all", get_all)

        result = order_sync.process_order_phase1(
            _woo_order(), _settings(enable_amendment=1), allow_update=True, amended_from=self.SOURCE
        )
        self.assertEqual(result.get("status"), "created", result)
        self.assertEqual(len(rec["created"]), 1)
        return rec["created"][0]

    def _shipping(self, replacement) -> list[float]:
        return [row["tax_amount"] for row in order_sync._get_delivery_charge_rows(replacement)]

    def test_f_purpose_and_pickup_are_carried_and_no_shipping_is_added(self):
        replacement = self._run({"custom_order_purpose": "B2B Supply", "custom_is_pickup": 1})

        self.assertEqual(replacement.values.get("amended_from"), self.SOURCE)
        self.assertEqual(replacement.values.get("custom_order_purpose"), "B2B Supply")
        self.assertEqual(replacement.values.get("custom_is_pickup"), 1)
        self.assertEqual(self._shipping(replacement), [])

    def test_pickup_wins_even_if_the_source_had_a_shipping_row(self):
        replacement = self._run({"custom_is_pickup": 1}, source_taxes=[SOURCE_SHIPPING_ROW])
        self.assertEqual(self._shipping(replacement), [])

    def test_non_pickup_b2b_supply_source_without_shipping_gets_none(self):
        """W1: a policy that zeroes shipping income (no pickup) must not gain +60."""
        replacement = self._run({
            "custom_order_purpose": "B2B Supply",
            "custom_commercial_policy": "B2B Supply",
            "custom_policy_reason": "Shop supply order",
            "custom_no_courier": 1,
            "custom_is_pickup": 0,
        })

        self.assertEqual(replacement.values.get("custom_order_purpose"), "B2B Supply")
        self.assertEqual(replacement.values.get("custom_commercial_policy"), "B2B Supply")
        self.assertEqual(replacement.values.get("custom_policy_reason"), "Shop supply order")
        self.assertEqual(replacement.values.get("custom_no_courier"), 1)
        self.assertNotIn("custom_is_pickup", replacement.values)
        self.assertEqual(self._shipping(replacement), [])

    def test_b2b_supply_purpose_alone_keeps_zero_shipping(self):
        replacement = self._run({"custom_order_purpose": "B2B Supply", "custom_is_pickup": 0})
        self.assertEqual(self._shipping(replacement), [])

    def test_standard_source_without_shipping_is_re_derived(self):
        """W1-a: a Standard order's zero came from its basket or address (free-shipping
        bundle, delivery promotion, free territory) — the replacement re-derives it."""
        replacement = self._run({"custom_order_purpose": "Standard", "custom_is_pickup": 0})
        self.assertEqual(self._shipping(replacement), [self.TERRITORY_DELIVERY_INCOME])

    def test_control_source_with_shipping_row_keeps_territory_shipping(self):
        """Proves the harness charges 60 — so the suppression tests are meaningful."""
        replacement = self._run(
            {"custom_order_purpose": "Standard", "custom_is_pickup": 0},
            source_taxes=[SOURCE_SHIPPING_ROW],
        )
        self.assertEqual(replacement.values.get("custom_order_purpose"), "Standard")
        self.assertNotIn("custom_no_courier", replacement.values)
        self.assertEqual(self._shipping(replacement), [self.TERRITORY_DELIVERY_INCOME])

    def test_differently_described_source_delivery_row_still_counts_as_shipping(self):
        """A POS row booked elsewhere ("Delivery Charges") is not mistaken for 'no shipping'."""
        replacement = self._run(
            {"custom_is_pickup": 0},
            source_taxes=[{"charge_type": "Actual", "account_head": "Misc - J",
                           "description": "Delivery Charges", "tax_amount": 45.0}],
        )
        self.assertEqual(self._shipping(replacement), [self.TERRITORY_DELIVERY_INCOME])

    def test_unreadable_source_taxes_keep_the_normal_policy(self):
        replacement = self._run({"custom_is_pickup": 0}, source_taxes=self._UNREADABLE)
        self.assertEqual(self._shipping(replacement), [self.TERRITORY_DELIVERY_INCOME])

    def test_blank_source_values_are_not_written(self):
        replacement = self._run({"custom_order_purpose": "", "custom_commercial_policy": None, "custom_is_pickup": 0})
        self.assertNotIn("custom_order_purpose", replacement.values)
        self.assertNotIn("custom_commercial_policy", replacement.values)

    def test_fields_missing_from_meta_are_skipped(self):
        """An un-migrated site without the jarz_pos fields: nothing read, nothing written."""
        replacement = self._run(
            {"custom_order_purpose": "B2B Supply", "custom_is_pickup": 1, "custom_no_courier": 1}, meta_fields=()
        )
        for fieldname in ALL_CONTEXT_FIELDS:
            self.assertNotIn(fieldname, replacement.values)


class TestSourceInvoiceHadShippingRow(unittest.TestCase):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)
        self.monkeypatch.setattr(order_sync, "_get_shipping_income_account", lambda company: "Shipping Income - J")
        self.monkeypatch.setattr(order_sync, "_legacy_shipping_income_account", lambda company: "Freight and Forwarding Charges - J")

    def _had(self, rows):
        self.monkeypatch.setattr(order_sync.frappe, "get_all", lambda *a, **kw: rows)
        return order_sync._source_invoice_had_shipping_row("ACC-SRC", company="_Test Company")

    def test_no_rows_is_no_shipping(self):
        self.assertIs(self._had([]), False)

    def test_zero_amount_row_is_no_shipping(self):
        self.assertIs(self._had([dict(SOURCE_SHIPPING_ROW, tax_amount=0)]), False)

    def test_account_identity_alone_is_shipping(self):
        self.assertIs(self._had([{"account_head": "Shipping Income - J", "description": "Charges", "tax_amount": 60}]), True)

    def test_legacy_freight_account_is_shipping(self):
        self.assertIs(
            self._had([{"account_head": "Freight and Forwarding Charges - J", "description": "x", "tax_amount": 50}]),
            True,
        )

    def test_unrelated_row_is_no_shipping(self):
        self.assertIs(self._had([{"account_head": "VAT - J", "description": "VAT 14%", "tax_amount": 30}]), False)

    def test_unreadable_is_unknown(self):
        def boom(*a, **kw):
            raise RuntimeError("db down")

        self.monkeypatch.setattr(order_sync.frappe, "get_all", boom)
        self.assertIsNone(order_sync._source_invoice_had_shipping_row("ACC-SRC", company="_Test Company"))


class _DraftInvoice:
    """A draft Sales Invoice on the update path, with a shipping row already on it."""

    def __init__(self, *, pickup):
        self.company = "_Test Company"
        self.docstatus = 0
        self.name = "ACC-SINV-DRAFT"
        self.custom_is_pickup = pickup
        self.taxes = [{"charge_type": "Actual", "description": f"Shipping Income ({TERRITORY})", "tax_amount": 60.0}]
        self.items = []

    def get(self, fieldname, default=None):
        return getattr(self, fieldname, default)

    def set(self, fieldname, value):
        setattr(self, fieldname, list(value))

    def append(self, fieldname, value):
        getattr(self, fieldname).append(dict(value))


class TestPickupDeliveryPolicy(unittest.TestCase):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)
        self.monkeypatch.setattr(order_sync.frappe.db, "exists", lambda doctype, name=None, *a, **kw: doctype == "Territory")
        self.monkeypatch.setattr(order_sync.frappe.db, "get_value", lambda *a, **kw: 60.0)
        self.monkeypatch.setattr(order_sync.frappe, "logger", lambda *a, **kw: MagicMock())

    def test_pickup_invoice_gets_no_delivery_charge(self):
        inv = SimpleNamespace(custom_is_pickup=1, company="_Test Company", items=[])
        decision = order_sync._resolve_delivery_charge_policy(TERRITORY, False, inv=inv)
        self.assertEqual((decision["amount"], decision["reason"]), (0.0, "pickup"))

    def test_non_pickup_invoice_still_gets_the_territory_charge(self):
        inv = SimpleNamespace(custom_is_pickup=0, company="_Test Company", items=[])
        decision = order_sync._resolve_delivery_charge_policy(TERRITORY, False, inv=inv)
        self.assertEqual((decision["amount"], decision["reason"]), (60.0, "territory_delivery_income"))

    def test_source_had_no_shipping_is_opt_in_only(self):
        inv = SimpleNamespace(custom_is_pickup=0, company="_Test Company", items=[])
        decision = order_sync._resolve_delivery_charge_policy(TERRITORY, False, inv=inv, source_had_no_shipping=True)
        self.assertEqual((decision["amount"], decision["reason"]), (0.0, "source_had_no_shipping"))

    def test_draft_pickup_update_only_clears_the_shipping_row(self):
        """Draft update path: a pickup draft loses its Shipping Income row; nothing else changes."""
        inv = _DraftInvoice(pickup=1)
        decision = order_sync._apply_delivery_charge_policy(inv, TERRITORY, False)
        self.assertEqual(decision["reason"], "pickup")
        self.assertTrue(decision["changed"])
        self.assertEqual(inv.taxes, [])
        self.assertEqual(inv.docstatus, 0)

    def test_draft_non_pickup_update_is_unchanged(self):
        inv = _DraftInvoice(pickup=0)
        decision = order_sync._apply_delivery_charge_policy(inv, TERRITORY, False)
        self.assertEqual(decision["reason"], "territory_delivery_income")
        self.assertEqual([row["tax_amount"] for row in order_sync._get_delivery_charge_rows(inv)], [60.0])

    def test_mock_like_flag_is_not_pickup(self):
        """A truthy non-Check value (e.g. a MagicMock attribute) must not suppress shipping."""
        self.assertFalse(order_sync._invoice_is_pickup(SimpleNamespace(custom_is_pickup=MagicMock())))
        self.assertFalse(order_sync._invoice_is_pickup(MagicMock()))
        self.assertFalse(order_sync._invoice_is_pickup(None))
        self.assertTrue(order_sync._invoice_is_pickup({"custom_is_pickup": 1}))
        self.assertTrue(order_sync._invoice_is_pickup({"custom_is_pickup": "1"}))


# ---------------------------------------------------------------------------
# (g) The amendment job re-checks the edit under the invoice lock
# ---------------------------------------------------------------------------

from jarz_woocommerce_integration.tests.test_submitted_invoice_amendment import (  # noqa: E402
    _make_fake_inv,
    _make_woo_order as _make_job_order,
    _RunWooAmendmentJobPatcher,
)


class TestAmendmentJobRecheck(_RunWooAmendmentJobPatcher, unittest.TestCase):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)

    def _run_job(self, source_items, *, rebuild):
        order_map_row = SimpleNamespace(name="WOOMAP-00001", erpnext_sales_invoice="ACC-SINV-99001", hash="oldhash")
        source_si = _make_fake_inv(items=[dict(row) for row in source_items])
        oa = self._patch_frappe(
            self.monkeypatch,
            order_map_row=order_map_row,
            source_si=source_si,
            eligibility={"can_amend": True, "amendment_block_code": None, "amendment_block_reason": None},
        )
        self.monkeypatch.setattr(oa, "_find_existing_replacement", lambda *a, **kw: None)
        self.monkeypatch.setattr(
            oa, "_evaluate_paid_amendment",
            lambda source_si, order_payload, settings: {"requires_paid_lane": False, "can_auto_amend": True},
        )
        self.monkeypatch.setattr(order_sync, "_rebuild_target_lines_for_invoice", rebuild)
        self.process = MagicMock(return_value={"invoice": "ACC-SINV-99001-1"})
        self.monkeypatch.setattr(order_sync, "process_order_phase1", self.process)
        result = oa.run_woo_amendment_job(99001, _make_job_order(), "WooCommerce Settings")
        return oa, result

    def test_identity_now_matches_skips_as_success(self):
        """The POS already applied the same edit: nothing to amend."""
        rows = [_jar_row(108.0, discount_percentage=10), _cookie_row()]
        oa, result = self._run_job(rows, rebuild=lambda order, invoice, woo_id=None: [_jar_row(LIST_RATE), _cookie_row()])

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "items_already_match")
        self.assertIn(result["reason"], order_sync.SKIPPED_SUCCESS_REASONS)
        oa._flag_needs_review.assert_not_called()
        self.process.assert_not_called()

    def test_would_reprice_flags_review_and_does_not_amend(self):
        oa, result = self._run_job(
            [_jar_row(90.0)],
            rebuild=lambda order, invoice, woo_id=None: [_jar_row(LIST_RATE), _cookie_row()],
        )

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "needs_manual_review")
        self.assertEqual([d["item_code"] for d in result["repriced_lines"]], [JAR_ITEM])
        oa._flag_needs_review.assert_called_once()
        self.assertIn("POS pricing the rebuild would lose", oa._flag_needs_review.call_args.kwargs["reason"])
        self.process.assert_not_called()

    def test_rebuild_failure_flags_review_and_does_not_amend(self):
        def boom(order, invoice, woo_id=None):
            raise ValueError("unmapped_items: [{'sku': 'GONE'}]")

        oa, result = self._run_job([_jar_row(LIST_RATE)], rebuild=boom)

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "amendment_recheck_failed")
        oa._flag_needs_review.assert_called_once()
        self.assertIn("amendment_recheck_failed", oa._flag_needs_review.call_args.kwargs["reason"])
        self.process.assert_not_called()

    def test_genuine_edit_on_list_priced_order_passes_the_recheck(self):
        import jarz_woocommerce_integration.services.order_amendment as oa

        self.monkeypatch.setattr(
            order_sync, "_rebuild_target_lines_for_invoice",
            lambda order, invoice, woo_id=None: [_jar_row(LIST_RATE), _cookie_row()],
        )
        self.monkeypatch.setattr(oa, "_flag_needs_review", MagicMock())
        self.monkeypatch.setattr(oa, "_write_sync_log", MagicMock())
        source_si = _make_fake_inv(items=[_jar_row(LIST_RATE)])

        self.assertIsNone(oa._recheck_items_against_source(source_si, _make_job_order(), 99001))
        oa._flag_needs_review.assert_not_called()


if __name__ == "__main__":
    unittest.main()
