"""Regression suite for the bundle echo-revert incident (Woo 17278, 2026-09-09).

What happened
-------------
A POS operator amended ``ACC-SINV-2026-18202``'s "Jarz Royal Feast" jar mix from
Pistachio 4 / Blueberry 4 / ChocHazelnut 1 / Redvelvet 1 to ChocHazelnut 4 /
Pistachio 3 / Blueberry 3. The outbound push rewrote the Woo **child lines** to
the new mix but preserved the store's ``_woosb_ids`` on the parent, which still
stated the original. WooCommerce echoed our own PUT back as ``order.updated``
one second later; the inbound gate rebuilt the target lines from that stale
``_woosb_ids``, called the difference a customer edit, and amended the invoice —
reverting the operator and shipping the wrong jars.

Three guards now stand between that sequence and a customer:
  1. the child line items beat a ``_woosb_ids`` that disagrees with them;
  2. outbound refreshes ``_woosb_ids`` on orders this app created, so the two
     stop drifting in the first place;
  3. an outbound write marks the order in-flight, and the inbound amendment gate
     refuses to amend on our own echo.

Why this module exists
----------------------
The suites these tests would naturally live beside — ``test_order_sync_delivery``
and ``test_submitted_invoice_amendment`` — are written pytest-style (bare
``def test_*`` / plain ``class Test:`` with a ``monkeypatch`` argument). CI runs
``bench run-tests``, which is **unittest discovery**, and pytest is not installed
in the bench venv: unittest collects nothing from either file, so tests added
there would never execute. These are ``unittest.TestCase`` so that the guards
above are actually enforced on every push. The helpers are imported from those
modules rather than duplicated.
"""
from __future__ import annotations

import unittest

from jarz_woocommerce_integration.services import order_sync, outbound_sync
from jarz_woocommerce_integration.tests._monkeypatch import MonkeyPatch
from jarz_woocommerce_integration.tests.test_order_sync_delivery import DummyBundleCache
from jarz_woocommerce_integration.tests.test_submitted_invoice_amendment import (
    _make_fake_inv,
    _make_settings,
    _make_woo_order,
    _setup_submitted_mocks,
)


#: The mix Woo order 17278 was created with — and the string our own outbound
#: push then froze on the parent line while rewriting the children underneath it.
STALE_PARENT_WOOSB_IDS = (
    '13826/g497/4/{"attribute_pa_size":"medium"},'
    '13780/88zq/4/{"attribute_pa_size":"medium"},'
    '13783/6mtj/1/{"attribute_pa_size":"medium"},'
    '13773/3sh1/1/{"attribute_pa_size":"medium"}'
)

#: What that string reconstructs to: the ORIGINAL mix, i.e. the wrong answer in
#: every test below where child lines are present.
STALE_PARENT_SELECTIONS = {
    "Medium": [
        {"item_code": "PISTACHIO-MEDIUM", "selected_qty": 4},
        {"item_code": "BLUEBERRY-MEDIUM", "selected_qty": 4},
        {"item_code": "CHOCO-HAZELNUT-MEDIUM", "selected_qty": 1},
        {"item_code": "REDVELVET-MEDIUM", "selected_qty": 1},
    ]
}

BUNDLE_PRODUCT_ID = 123


def _cache() -> DummyBundleCache:
    return DummyBundleCache(
        bundle_code="BUNDLE-ROYAL-FEAST",
        free_shipping=False,
        resolve_map={
            "13826": "PISTACHIO-MEDIUM",
            "13780": "BLUEBERRY-MEDIUM",
            "13783": "CHOCO-HAZELNUT-MEDIUM",
            "13773": "REDVELVET-MEDIUM",
        },
        item_groups={
            "PISTACHIO-MEDIUM": "Medium",
            "BLUEBERRY-MEDIUM": "Medium",
            "CHOCO-HAZELNUT-MEDIUM": "Medium",
            "REDVELVET-MEDIUM": "Medium",
        },
    )


def _parent(woosb_ids: str, line_id: int = 61001) -> dict:
    return {
        "id": line_id,
        "name": "Jarz Royal Feast",
        "product_id": BUNDLE_PRODUCT_ID,
        "variation_id": 0,
        "quantity": 1,
        "sku": "",
        "meta_data": [{"key": "_woosb_ids", "value": woosb_ids}],
    }


def _child(line_id: int, variation_id: int, qty: int) -> dict:
    return {
        "id": line_id,
        "name": f"child-{variation_id}",
        "product_id": 900 + (line_id % 100),
        "variation_id": variation_id,
        "quantity": qty,
        "sku": "",
        "meta_data": [{"key": "_woosb_parent_id", "value": str(BUNDLE_PRODUCT_ID)}],
    }


class TestChildLinesBeatAStaleSelectionString(unittest.TestCase):
    """Guard 1: when the two sources disagree, the children are the truth."""

    def test_woo_17278_stale_parent_woosb_ids_loses_to_the_child_lines(self):
        """The incident itself, reduced to one call.

        `_woosb_ids` says 4/4/1/1, the child lines say ChocHazelnut 4 /
        Pistachio 3 / Blueberry 3. The children are what the store bills, picks
        and ships, so they must win — reading the parent string here is what
        reverted the operator's amendment.
        """
        parent = _parent(STALE_PARENT_WOOSB_IDS)
        line_items = [
            parent,
            _child(61002, 13783, 4),
            _child(61003, 13826, 3),
            _child(61004, 13780, 3),
        ]

        selections = order_sync._build_bundle_selections(
            line_items, BUNDLE_PRODUCT_ID, 1, cache=_cache(), parent_line=parent
        )

        self.assertEqual(
            selections,
            {
                "Medium": [
                    {"item_code": "CHOCO-HAZELNUT-MEDIUM", "selected_qty": 4},
                    {"item_code": "PISTACHIO-MEDIUM", "selected_qty": 3},
                    {"item_code": "BLUEBERRY-MEDIUM", "selected_qty": 3},
                ]
            },
        )
        self.assertNotEqual(selections, STALE_PARENT_SELECTIONS)

    def test_agreeing_sources_are_unaffected(self):
        """The ordinary case must not move: children matching the string."""
        parent = _parent(STALE_PARENT_WOOSB_IDS)
        line_items = [
            parent,
            _child(61002, 13826, 4),
            _child(61003, 13780, 4),
            _child(61004, 13783, 1),
            _child(61005, 13773, 1),
        ]

        selections = order_sync._build_bundle_selections(
            line_items, BUNDLE_PRODUCT_ID, 1, cache=_cache(), parent_line=parent
        )

        self.assertEqual(selections, STALE_PARENT_SELECTIONS)


class TestSelectionStringRemainsTheFallback(unittest.TestCase):
    """Guard 1, the other half: `_woosb_ids` is demoted, not discarded."""

    def test_used_when_the_order_carries_no_child_lines(self):
        parent = _parent(STALE_PARENT_WOOSB_IDS)

        selections = order_sync._build_bundle_selections(
            [parent], BUNDLE_PRODUCT_ID, 1, cache=_cache(), parent_line=parent
        )

        self.assertEqual(selections, STALE_PARENT_SELECTIONS)

    def test_used_when_a_child_cannot_be_mapped(self):
        """A half-readable child scan must not downgrade to bundle defaults.

        `_build_bundle_selections_from_children` abandons the whole bundle as
        soon as one child is unresolvable, and an empty result makes the caller
        expand the bundle's default recipe — a mix nobody chose. The selection
        string is weaker evidence than the children but far better than that.
        """
        parent = _parent(STALE_PARENT_WOOSB_IDS)
        line_items = [
            parent,
            _child(61002, 13783, 4),
            _child(61003, 99999, 3),  # variation absent from the catalogue
        ]

        selections = order_sync._build_bundle_selections(
            line_items, BUNDLE_PRODUCT_ID, 1, cache=_cache(), parent_line=parent
        )

        self.assertEqual(selections, STALE_PARENT_SELECTIONS)

    def test_two_instances_collapse_is_not_possible_when_one_parent_lacks_the_string(self):
        """The ambiguity test counts parents, not parents-that-carry-a-string.

        Outbound omits `_woosb_ids` whenever `_build_woosb_ids_value` renders
        empty (a child with no product id, or qty <= 0), so a second instance can
        legitimately arrive without one. If the count required the string it
        would read 1 here, skip the ambiguity branch, and run the child scan —
        which attributes the children of BOTH instances to EACH parent, doubling
        the bundle's contents on the invoice.
        """
        with_string = _parent(STALE_PARENT_WOOSB_IDS, line_id=61001)
        without_string = {
            "id": 61010,
            "name": "Jarz Royal Feast",
            "product_id": BUNDLE_PRODUCT_ID,
            "variation_id": 0,
            "quantity": 1,
            "sku": "",
            "meta_data": [],
        }
        line_items = [with_string, without_string, _child(61002, 13783, 4), _child(61003, 13826, 3)]

        self.assertEqual(
            order_sync._count_bundle_parent_instances(line_items, BUNDLE_PRODUCT_ID), 2
        )
        # The stringless parent must not be handed the other instance's children.
        self.assertEqual(
            order_sync._build_bundle_selections(
                line_items, BUNDLE_PRODUCT_ID, 1, cache=_cache(), parent_line=without_string
            ),
            {},
        )

    def test_a_child_sharing_the_parents_product_id_is_not_counted_as_an_instance(self):
        parent = _parent(STALE_PARENT_WOOSB_IDS)
        odd_child = {
            "id": 61020,
            "product_id": BUNDLE_PRODUCT_ID,
            "variation_id": 13783,
            "quantity": 4,
            "meta_data": [{"key": "_woosb_parent_id", "value": str(BUNDLE_PRODUCT_ID)}],
        }

        self.assertEqual(
            order_sync._count_bundle_parent_instances([parent, odd_child], BUNDLE_PRODUCT_ID), 1
        )

    def test_two_instances_of_one_bundle_still_use_the_per_line_string(self):
        """The genuinely ambiguous case, which is why the string exists at all.

        Every WooSB child carries ``_woosb_parent_id = <product_id>``, identical
        for both parent lines, so the children cannot be attributed to one
        instance or the other. ``_woosb_ids`` is per-parent-line and is then the
        only source that can tell them apart.
        """
        first = _parent(STALE_PARENT_WOOSB_IDS, line_id=61001)
        second = _parent(
            '13773/3sh1/2/{"attribute_pa_size":"medium"},'
            '13780/88zq/4/{"attribute_pa_size":"medium"}',
            line_id=61010,
        )
        line_items = [first, second, _child(61002, 13783, 4), _child(61003, 13826, 3)]

        selections = order_sync._build_bundle_selections(
            line_items, BUNDLE_PRODUCT_ID, 1, cache=_cache(), parent_line=second
        )

        self.assertEqual(
            selections,
            {
                "Medium": [
                    {"item_code": "REDVELVET-MEDIUM", "selected_qty": 2},
                    {"item_code": "BLUEBERRY-MEDIUM", "selected_qty": 4},
                ]
            },
        )


class TestOutboundRefreshesOurOwnSelectionString(unittest.TestCase):
    """Guard 2: stop freezing a string we wrote ourselves."""

    def _payload_line(self) -> list[dict]:
        return [
            {
                "id": 61001,
                "meta_data": [
                    {"key": "erpnext_item_code", "value": "Jarz Royal Feast"},
                    {"key": "_woosb_ids", "value": "13783/aaaa/4/{}"},
                ],
            }
        ]

    def _existing(self, **order_fields) -> dict:
        order = {
            "id": 17278,
            "line_items": [
                {"id": 61001, "meta_data": [{"key": "_woosb_ids", "value": STALE_PARENT_WOOSB_IDS}]}
            ],
        }
        order.update(order_fields)
        return order

    def _woosb_value(self, lines: list[dict]) -> str | None:
        for entry in lines[0].get("meta_data") or []:
            if entry.get("key") == "_woosb_ids":
                return entry.get("value")
        return None

    def test_a_store_created_order_keeps_its_own_string(self):
        """The original guard, still load-bearing for 10,000+ Woo-origin orders."""
        lines = self._payload_line()

        outbound_sync._preserve_native_bundle_selection(
            lines, self._existing(created_via="checkout")
        )

        self.assertIsNone(self._woosb_value(lines))

    def test_an_order_we_created_gets_our_refreshed_string(self):
        lines = self._payload_line()

        outbound_sync._preserve_native_bundle_selection(
            lines,
            self._existing(
                created_via="rest-api",
                meta_data=[{"key": "_jarz_order_origin", "value": "ERPNext POS"}],
            ),
        )

        self.assertEqual(self._woosb_value(lines), "13783/aaaa/4/{}")

    def test_origin_meta_alone_is_enough(self):
        """Covers orders created before `created_via` mattered to us."""
        lines = self._payload_line()

        outbound_sync._preserve_native_bundle_selection(
            lines,
            self._existing(meta_data=[{"key": "_jarz_order_origin", "value": "ERPNext POS"}]),
        )

        self.assertEqual(self._woosb_value(lines), "13783/aaaa/4/{}")

    def test_created_via_rest_api_alone_is_enough(self):
        """Covers orders created before the origin meta shipped."""
        lines = self._payload_line()

        outbound_sync._preserve_native_bundle_selection(lines, self._existing(created_via="rest-api"))

        self.assertEqual(self._woosb_value(lines), "13783/aaaa/4/{}")


class TestOrderOwnershipDetection(unittest.TestCase):
    """Guard 2's discriminator, which must stay conservative."""

    def test_admin_created_order_is_not_ours(self):
        self.assertFalse(
            outbound_sync._order_is_jarz_originated({"created_via": "admin", "meta_data": []})
        )

    def test_checkout_order_is_not_ours(self):
        self.assertFalse(outbound_sync._order_is_jarz_originated({"created_via": "checkout"}))

    def test_empty_and_none_are_not_ours(self):
        self.assertFalse(outbound_sync._order_is_jarz_originated({}))
        self.assertFalse(outbound_sync._order_is_jarz_originated(None))

    def test_origin_meta_identifies_ours(self):
        self.assertTrue(
            outbound_sync._order_is_jarz_originated(
                {"created_via": "checkout", "meta_data": [{"key": "_jarz_order_origin", "value": "x"}]}
            )
        )


class TestWoosbIdsCannotByItselfTriggerAPush(unittest.TestCase):
    """Guard 2's safety rail — F-18 must not come back.

    Part 2 of a ``_woosb_ids`` entry is a WooSB-internal token we can only
    approximate, and part 3 renders ``{}`` on a cold variation-attribute cache.
    Now that we write the key on our own orders, including it in the
    already-in-sync comparison would make every bundle order look permanently
    dirty and put a PUT on every sync.
    """

    def test_woosb_ids_is_not_in_the_dirty_comparison_set(self):
        self.assertNotIn("_woosb_ids", outbound_sync._ORDER_LINE_META_KEYS_TO_COMPARE)
        # The keys that genuinely describe what was sold stay in.
        self.assertIn("erpnext_item_code", outbound_sync._ORDER_LINE_META_KEYS_TO_COMPARE)
        self.assertIn("_woosb_parent_id", outbound_sync._ORDER_LINE_META_KEYS_TO_COMPARE)


class TestOutboundEchoIsNeverAnAmendment(unittest.TestCase):
    """Guard 3: our own `order.updated` must not amend anything."""

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)

    def test_the_marker_blocks_the_amendment(self):
        """Same inputs as the enqueue case — only the marker differs."""
        order = _make_woo_order(status="processing")
        fake_inv = _make_fake_inv()
        logs = _setup_submitted_mocks(self.monkeypatch, fake_inv=fake_inv, stored_hash="oldhash")
        self.monkeypatch.setattr(
            outbound_sync, "outbound_push_recently_pushed", lambda woo_order_id: True
        )

        result = order_sync.process_order_phase1(order, _make_settings(enable_amendment=1))

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "outbound_echo_suppressed")
        self.assertEqual(result["invoice"], fake_inv.name)
        order_sync.frappe.enqueue.assert_not_called()
        # Our own write is not a customer edit; flagging it would train staff to
        # ignore the flag.
        order_sync._flag_order_map_for_manual_review.assert_not_called()
        self.assertTrue(
            any("outbound_echo_suppressed" in str(log.get("message") or "") for log in logs)
        )

    def test_without_the_marker_the_same_payload_still_enqueues(self):
        """The guard must be the marker, not a blanket loosening of the gate."""
        order = _make_woo_order(status="processing")
        fake_inv = _make_fake_inv()
        _setup_submitted_mocks(self.monkeypatch, fake_inv=fake_inv, stored_hash="oldhash")
        self.monkeypatch.setattr(
            outbound_sync, "outbound_push_recently_pushed", lambda woo_order_id: False
        )

        result = order_sync.process_order_phase1(order, _make_settings(enable_amendment=1))

        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["reason"], "amendment_enqueued")
        order_sync.frappe.enqueue.assert_called_once()


class TestSuppressedEchoIsNotAnAlarm(unittest.TestCase):
    """A suppressed echo is the guard working — it must not book a NeedsReview.

    `_classify_text_reason` falls through to "review" for any reason it does not
    recognise, and that lands a terminal NeedsReview Woo Sync Event. Unregistered,
    the new skip reason would raise one on *every* outbound push and train staff
    to ignore the flag that exists to catch real website edits — the exact
    outcome the suppression branch's own comment says it is avoiding.
    """

    def test_the_reason_is_registered_as_a_skip_not_a_review(self):
        from jarz_woocommerce_integration.services import sync_events

        self.assertIn("outbound_echo", sync_events.SKIP_REASON_TOKENS)
        self.assertEqual(sync_events._classify_text_reason("outbound_echo_suppressed"), "skip")

    def test_a_suppressed_echo_is_not_counted_as_a_failed_pull(self):
        self.assertIn("outbound_echo_suppressed", order_sync.SKIPPED_SUCCESS_REASONS)
        # The reasons that were already there must stay there.
        self.assertIn("submitted_frozen", order_sync.SKIPPED_SUCCESS_REASONS)
        self.assertIn("locked", order_sync.SKIPPED_SUCCESS_REASONS)


class TestEchoMarkerLifecycle(unittest.TestCase):
    """The marker itself: set before the write, read by the gate, never cleared."""

    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)
        self.store: dict[str, object] = {}

        class _Cache:
            def set_value(_self, key, value, expires_in_sec=None):
                self.store[key] = (value, expires_in_sec)

            def get_value(_self, key):
                entry = self.store.get(key)
                return entry[0] if entry else None

        self.monkeypatch.setattr(outbound_sync.frappe, "cache", lambda: _Cache())

    def test_mark_then_read_is_true_for_that_order_only(self):
        outbound_sync._mark_outbound_push_in_flight(17278)

        self.assertTrue(outbound_sync.outbound_push_recently_pushed(17278))
        self.assertTrue(outbound_sync.outbound_push_recently_pushed("17278"))
        self.assertFalse(outbound_sync.outbound_push_recently_pushed(17279))

    def test_the_marker_expires_rather_than_living_forever(self):
        outbound_sync._mark_outbound_push_in_flight(17278)

        _value, ttl = self.store[outbound_sync._outbound_echo_cache_key(17278)]
        self.assertEqual(ttl, outbound_sync._OUTBOUND_ECHO_TTL)
        self.assertGreater(ttl, 0)

    def test_a_missing_order_id_is_a_no_op(self):
        outbound_sync._mark_outbound_push_in_flight(None)

        self.assertEqual(self.store, {})
        self.assertFalse(outbound_sync.outbound_push_recently_pushed(None))

    def test_a_cache_outage_never_breaks_a_push(self):
        class _Broken:
            def set_value(self, *_a, **_kw):
                raise RuntimeError("redis down")

            def get_value(self, *_a, **_kw):
                raise RuntimeError("redis down")

        self.monkeypatch.setattr(outbound_sync.frappe, "cache", lambda: _Broken())

        outbound_sync._mark_outbound_push_in_flight(17278)  # must not raise
        # Reads fail open: a Redis outage must not freeze every inbound
        # amendment on the site. The hash comparison is the fallback.
        self.assertFalse(outbound_sync.outbound_push_recently_pushed(17278))
