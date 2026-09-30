"""Outbound sync of an order holding two copies of the same bundle (Woo 17748).

What happened
-------------
Woo 17748 (-> ``ACC-SINV-2026-18615-1``, 2026-09-30) is a website order with
two "Jarz Royal Feast" (Jarz Bundle ``gf9k3rfeg5``, 960 each, groups Medium x8 +
Medium x2) and one plain line. A POS operator amended the first copy's jar mix.
Two defects stood between that amendment and a correct store order:

1. ``_collect_line_items`` keyed every bundle structure by the bundle link key,
   so the second parent row overwrote the first (the first parent line went out
   at 0.00), and both copies' children were merged into single lines under the
   second parent, whose ``_woosb_ids`` then described all 20 jars. Downstream,
   ``_attach_existing_line_ids`` matched greedily across the whole order, so a
   child of copy A could take the id of the same jar in copy B.
2. On a store-created order ``_preserve_native_bundle_selection`` stripped our
   ``_woosb_ids`` from every payload. With 2+ copies inbound *must* read that
   per-parent string (the children cannot be attributed), so the store kept the
   pre-amendment mix and the next inbound re-evaluation would have reverted the
   operator's edit — the Woo 17278 incident again.

Run by hand (the Woo repo has no CI)::

    bench --site <site> run-tests --app jarz_woocommerce_integration \
        --module jarz_woocommerce_integration.tests.test_outbound_multi_instance_bundle
"""
from __future__ import annotations

import copy
import json
import unittest

from jarz_woocommerce_integration.services import order_sync, outbound_sync
from jarz_woocommerce_integration.tests.test_order_sync_delivery import DummyBundleCache
from jarz_woocommerce_integration.tests.test_outbound_parity import (
    _NATIVE_BUNDLE_MAPPING,
    _collect,
    _erpnext_amount_sum,
    _invoice,
    _item,
    _line_total_sum,
    _meta,
    _native_bundle_invoice,
)


# ---------------------------------------------------------------------------
# Fixtures — shaped like Woo 17748
# ---------------------------------------------------------------------------

FEAST = "Jarz Royal Feast"
FEAST_BUNDLE = "gf9k3rfeg5"
FEAST_PID = 14000

MAPPING = {
    FEAST: {"woo_product_id": str(FEAST_PID)},
    "PISTACHIO-MEDIUM": {"woo_product_id": "2286", "woo_variation_id": "13826"},
    "STRAWBERRY-MEDIUM": {"woo_product_id": "371", "woo_variation_id": "13790"},
    "BLUEBERRY-MEDIUM": {"woo_product_id": "369", "woo_variation_id": "13780"},
    "CHOCO-HAZELNUT-MEDIUM": {"woo_product_id": "367", "woo_variation_id": "13783"},
    "COOKIE-BOX": {"woo_product_id": "500"},
}

ATTR = '{"attribute_pa_size":"medium"}'


def _feast_parent():
    return _item(FEAST, qty=1, amount=0, price_list_rate=960, rate=0,
                 discount_percentage=100, is_bundle_parent=1, bundle_code=FEAST_BUNDLE)


def _jar(item_code, qty):
    # 120 list, 20% off -> 96 a jar; ten jars -> 960 a bundle.
    return _item(item_code, qty=qty, amount=96 * qty, price_list_rate=120, rate=96,
                 discount_percentage=20, is_bundle_child=1, parent_bundle=FEAST_BUNDLE)


def _cookie():
    return _item("COOKIE-BOX", qty=1, amount=150, price_list_rate=150, rate=150)


def _amended_invoice():
    """The replacement invoice: copy A's strawberry swapped for pistachio.

    Rows are written parent-then-its-children, as both builders do.
    """
    return _invoice([
        _cookie(),
        _feast_parent(),                      # copy A
        _jar("PISTACHIO-MEDIUM", 4),          #   was STRAWBERRY 4
        _jar("BLUEBERRY-MEDIUM", 4),
        _jar("CHOCO-HAZELNUT-MEDIUM", 2),
        _feast_parent(),                      # copy B (unchanged)
        _jar("PISTACHIO-MEDIUM", 3),
        _jar("STRAWBERRY-MEDIUM", 5),
        _jar("CHOCO-HAZELNUT-MEDIUM", 2),
    ], name="ACC-SINV-2026-18615-1")


def _collect_17748():
    return _collect(_amended_invoice(), MAPPING, registered={str(FEAST_PID)})


def _tok(item_code):
    return outbound_sync._woosb_selection_token(item_code)


def _totals(raw):
    return outbound_sync._woosb_selection_totals(raw)


#: What the store wrote at checkout (plugin tokens, resolved attributes).
STORE_A_ORIGINAL = f"13790/ab12/4/{ATTR},13780/cd34/4/{ATTR},13783/ef56/2/{ATTR}"
STORE_B = f"13826/gh78/3/{ATTR},13790/ab12/5/{ATTR},13783/ef56/2/{ATTR}"


def _store_parent(line_id, woosb_ids):
    return {
        "id": line_id, "product_id": FEAST_PID, "variation_id": 0, "quantity": 1,
        "subtotal": "960.00", "total": "960.00",
        "meta_data": [{"key": "_woosb_ids", "value": woosb_ids}],
    }


def _store_child(line_id, product_id, variation_id, qty, item_code=None):
    meta = [{"key": "_woosb_parent_id", "value": str(FEAST_PID)}]
    if item_code:
        meta.insert(0, {"key": "erpnext_item_code", "value": item_code})
    return {
        "id": line_id, "product_id": product_id, "variation_id": variation_id, "quantity": qty,
        "subtotal": "0.00", "total": "0.00", "meta_data": meta,
    }


def _store_order_17748(created_via="checkout", **fields):
    """The website order as WooSB laid it out: A, A's children, B, B's children."""
    order = {
        "id": 17748,
        "status": "processing",
        "created_via": created_via,
        "meta_data": [],
        "line_items": [
            _store_parent(201, STORE_A_ORIGINAL),
            _store_child(202, 371, 13790, 4),    # A strawberry — now pistachio in ERPNext
            _store_child(203, 369, 13780, 4),    # A blueberry
            _store_child(204, 367, 13783, 2),    # A choco
            _store_parent(205, STORE_B),
            _store_child(206, 2286, 13826, 3),   # B pistachio
            _store_child(207, 371, 13790, 5),    # B strawberry
            _store_child(208, 367, 13783, 2),    # B choco
            {"id": 209, "product_id": 500, "variation_id": 0, "quantity": 1,
             "subtotal": "150.00", "total": "150.00", "meta_data": []},
        ],
    }
    order.update(fields)
    return order


def _inbound_cache():
    return DummyBundleCache(
        bundle_code="BUNDLE-ROYAL-FEAST",
        free_shipping=False,
        resolve_map={
            "13826": "PISTACHIO-MEDIUM",
            "13790": "STRAWBERRY-MEDIUM",
            "13780": "BLUEBERRY-MEDIUM",
            "13783": "CHOCO-HAZELNUT-MEDIUM",
        },
        item_groups={
            "PISTACHIO-MEDIUM": "Medium",
            "STRAWBERRY-MEDIUM": "Medium",
            "BLUEBERRY-MEDIUM": "Medium",
            "CHOCO-HAZELNUT-MEDIUM": "Medium",
        },
    )


def _by_id(lines):
    return {entry.get("id"): entry for entry in lines if entry.get("id")}


def _push(existing_order):
    """The line-item half of `_build_order_payload`, without the network."""
    line_items, missing = _collect_17748()
    matched, added, orphaned = outbound_sync._attach_existing_line_ids(
        line_items, existing_order.get("line_items") or []
    )
    removals = outbound_sync._build_line_item_removals(orphaned, protected_ids=set())
    payload_lines = matched + added + removals
    outbound_sync._preserve_native_bundle_selection(payload_lines, existing_order)
    return payload_lines, added, orphaned, missing


# ---------------------------------------------------------------------------
# Defect 1 — the builder, per bundle instance
# ---------------------------------------------------------------------------

class TestTwoCopiesOfOneBundleAreTwoBundles(unittest.TestCase):
    def setUp(self):
        self.line_items, self.missing = _collect_17748()
        self.parents = [e for e in self.line_items if e.get("product_id") == FEAST_PID]

    def test_both_parent_lines_are_emitted_and_each_is_priced_960(self):
        self.assertEqual(self.missing, [])
        self.assertEqual(len(self.parents), 2)
        for parent in self.parents:
            self.assertEqual(parent["subtotal"], "960.00")
            self.assertEqual(parent["total"], "960.00")

    def test_each_copy_keeps_only_its_own_children_in_order(self):
        """Parent A, A's three jars, parent B, B's three jars — never merged across copies."""
        codes = [_meta(e)["erpnext_item_code"] for e in self.line_items]
        quantities = [e["quantity"] for e in self.line_items]
        self.assertEqual(codes, [
            "COOKIE-BOX",
            FEAST, "PISTACHIO-MEDIUM", "BLUEBERRY-MEDIUM", "CHOCO-HAZELNUT-MEDIUM",
            FEAST, "PISTACHIO-MEDIUM", "STRAWBERRY-MEDIUM", "CHOCO-HAZELNUT-MEDIUM",
        ])
        # Pistachio 4 (A) and 3 (B) stay two lines; choco 2 + 2 likewise.
        self.assertEqual(quantities, [1, 1, 4, 4, 2, 1, 3, 5, 2])
        for child in self.line_items[2:5] + self.line_items[6:9]:
            self.assertEqual(child["total"], "0.00")
            self.assertEqual(_meta(child)["_woosb_parent_id"], str(FEAST_PID))

    def test_each_parent_woosb_ids_describes_its_own_ten_jars(self):
        first, second = (_meta(p)["_woosb_ids"] for p in self.parents)
        self.assertEqual(first, ",".join([
            f"13826/{_tok('PISTACHIO-MEDIUM')}/4/{{}}",
            f"13780/{_tok('BLUEBERRY-MEDIUM')}/4/{{}}",
            f"13783/{_tok('CHOCO-HAZELNUT-MEDIUM')}/2/{{}}",
        ]))
        self.assertEqual(second, ",".join([
            f"13826/{_tok('PISTACHIO-MEDIUM')}/3/{{}}",
            f"13790/{_tok('STRAWBERRY-MEDIUM')}/5/{{}}",
            f"13783/{_tok('CHOCO-HAZELNUT-MEDIUM')}/2/{{}}",
        ]))
        self.assertEqual(sum(_totals(first).values()), 10)
        self.assertEqual(sum(_totals(second).values()), 10)

    def test_line_totals_still_add_up_to_the_invoice(self):
        self.assertEqual(_line_total_sum(self.line_items), _erpnext_amount_sum(_amended_invoice()))
        self.assertEqual(_line_total_sum(self.line_items), 2070.0)

    def test_inbound_rebuilds_each_copy_from_its_own_string(self):
        """The round trip that decides whether the operator's edit survives."""
        selections = [
            order_sync._build_bundle_selections(
                self.line_items, FEAST_PID, 1, cache=_inbound_cache(), parent_line=parent
            )
            for parent in self.parents
        ]
        self.assertEqual(selections[0], {"Medium": [
            {"item_code": "PISTACHIO-MEDIUM", "selected_qty": 4},
            {"item_code": "BLUEBERRY-MEDIUM", "selected_qty": 4},
            {"item_code": "CHOCO-HAZELNUT-MEDIUM", "selected_qty": 2},
        ]})
        self.assertEqual(selections[1], {"Medium": [
            {"item_code": "PISTACHIO-MEDIUM", "selected_qty": 3},
            {"item_code": "STRAWBERRY-MEDIUM", "selected_qty": 5},
            {"item_code": "CHOCO-HAZELNUT-MEDIUM", "selected_qty": 2},
        ]})

    def test_same_product_in_two_groups_merges_within_one_copy_only(self):
        """Medium x8 and Medium x2 may both pick pistachio: one line per copy, not per order."""
        invoice = _invoice([
            _feast_parent(),
            _jar("PISTACHIO-MEDIUM", 4), _jar("BLUEBERRY-MEDIUM", 4), _jar("PISTACHIO-MEDIUM", 2),
            _feast_parent(),
            _jar("PISTACHIO-MEDIUM", 8), _jar("PISTACHIO-MEDIUM", 2),
        ])

        line_items, _missing = _collect(invoice, MAPPING, registered={str(FEAST_PID)})

        self.assertEqual(
            [(_meta(e)["erpnext_item_code"], e["quantity"]) for e in line_items],
            [(FEAST, 1), ("PISTACHIO-MEDIUM", 6), ("BLUEBERRY-MEDIUM", 4),
             (FEAST, 1), ("PISTACHIO-MEDIUM", 10)],
        )
        self.assertEqual([line_items[0]["total"], line_items[3]["total"]], ["960.00", "960.00"])
        self.assertEqual(_totals(_meta(line_items[0])["_woosb_ids"]), {"13826": 6, "13780": 4})
        self.assertEqual(_totals(_meta(line_items[3])["_woosb_ids"]), {"13826": 10})


class TestSingleCopyIsByteIdentical(unittest.TestCase):
    """The instance keying must not move a byte of a one-bundle payload."""

    def test_native_single_bundle_payload_is_unchanged(self):
        line_items, _missing = _collect(
            _native_bundle_invoice(), _NATIVE_BUNDLE_MAPPING, registered={"12446"}
        )

        def child(code, product_id, variation_id, qty):
            return {
                "quantity": qty, "subtotal": "0.00", "total": "0.00",
                "meta_data": [
                    {"key": "erpnext_item_code", "value": code},
                    {"key": "_woosb_parent_id", "value": "12446"},
                ],
                "product_id": product_id, "variation_id": variation_id,
            }

        expected = [
            {
                "quantity": 1, "subtotal": "600.00", "total": "600.00",
                "meta_data": [
                    {"key": "erpnext_item_code", "value": "BUNDLE-12446"},
                    {"key": "_woosb_ids", "value": ",".join([
                        f"13780/{_tok('JAR-369')}/2/{{}}",
                        f"13783/{_tok('JAR-367')}/1/{{}}",
                        f"13767/{_tok('JAR-217')}/1/{{}}",
                        f"13826/{_tok('JAR-2286')}/1/{{}}",
                        f"13813/{_tok('JAR-2284')}/1/{{}}",
                    ])},
                ],
                "product_id": 12446,
            },
            child("JAR-369", 369, 13780, 2),
            child("JAR-367", 367, 13783, 1),
            child("JAR-217", 217, 13767, 1),
            child("JAR-2286", 2286, 13826, 1),
            child("JAR-2284", 2284, 13813, 1),
        ]
        # json.dumps preserves key order, so this is a byte-level comparison.
        self.assertEqual(json.dumps(line_items), json.dumps(expected))

    def test_single_copy_with_a_merged_child_is_unchanged(self):
        invoice = _invoice([
            _feast_parent(),
            _jar("PISTACHIO-MEDIUM", 8),
            _jar("PISTACHIO-MEDIUM", 2),
        ])

        line_items, _missing = _collect(invoice, MAPPING, registered={str(FEAST_PID)})

        expected = [
            {
                "quantity": 1, "subtotal": "960.00", "total": "960.00",
                "meta_data": [
                    {"key": "erpnext_item_code", "value": FEAST},
                    {"key": "_woosb_ids", "value": f"13826/{_tok('PISTACHIO-MEDIUM')}/10/{{}}"},
                ],
                "product_id": FEAST_PID,
            },
            {
                "quantity": 10, "subtotal": "0.00", "total": "0.00",
                "meta_data": [
                    {"key": "erpnext_item_code", "value": "PISTACHIO-MEDIUM"},
                    {"key": "_woosb_parent_id", "value": str(FEAST_PID)},
                ],
                "product_id": 2286, "variation_id": 13826,
            },
        ]
        self.assertEqual(json.dumps(line_items), json.dumps(expected))

    def test_instance_key_of_the_first_copy_is_the_link_key(self):
        self.assertEqual(outbound_sync._bundle_instance_key("gf9k3rfeg5", 0), "gf9k3rfeg5")
        self.assertEqual(outbound_sync._bundle_instance_key("gf9k3rfeg5", 1), "gf9k3rfeg5#2")


# ---------------------------------------------------------------------------
# Defect 1, downstream — pairing payload lines with the store's lines
# ---------------------------------------------------------------------------

class TestExistingLineIdsPairCopyByCopy(unittest.TestCase):
    def test_17748_each_copy_keeps_its_own_line_ids(self):
        """A's new pistachio must reuse A's strawberry slot, not B's pistachio line.

        The old greedy match gave A's pistachio line 206 (B's), appended B's
        pistachio as a new line, gave B's strawberry line 202 (A's) and
        deleted 207 — copy A lost a line and copy B gained one on the store.
        """
        line_items, _missing = _collect_17748()

        matched, added, orphaned = outbound_sync._attach_existing_line_ids(
            line_items, _store_order_17748()["line_items"]
        )

        self.assertEqual(added, [])
        self.assertEqual(orphaned, [])
        self.assertEqual(
            [(e["id"], _meta(e)["erpnext_item_code"], e["quantity"]) for e in matched],
            [
                (209, "COOKIE-BOX", 1),
                (201, FEAST, 1),
                (202, "PISTACHIO-MEDIUM", 4),
                (203, "BLUEBERRY-MEDIUM", 4),
                (204, "CHOCO-HAZELNUT-MEDIUM", 2),
                (205, FEAST, 1),
                (206, "PISTACHIO-MEDIUM", 3),
                (207, "STRAWBERRY-MEDIUM", 5),
                (208, "CHOCO-HAZELNUT-MEDIUM", 2),
            ],
        )

    def test_instance_tags_follow_position(self):
        tags, counts = outbound_sync._bundle_instance_tags(_store_order_17748()["line_items"])
        pid = str(FEAST_PID)
        self.assertEqual(counts, {pid: 2})
        self.assertEqual(tags, [
            (pid, 0), (pid, 0), (pid, 0), (pid, 0),
            (pid, 1), (pid, 1), (pid, 1), (pid, 1),
            None,
        ])

    def test_a_child_appended_by_an_earlier_push_is_reused_not_churned(self):
        """A line we appended sits after the last copy, so position mis-attributes it.

        Round 2 lets it be matched as a leftover of the same bundle; without it
        the line would be deleted and re-appended on every sync, and the order
        would never compare as in sync.
        """
        existing = [
            _store_parent(301, STORE_A_ORIGINAL),
            _store_child(302, 369, 13780, 4, item_code="BLUEBERRY-MEDIUM"),
            _store_child(303, 367, 13783, 2, item_code="CHOCO-HAZELNUT-MEDIUM"),
            _store_parent(304, STORE_B),
            _store_child(305, 2286, 13826, 3, item_code="PISTACHIO-MEDIUM"),
            _store_child(306, 371, 13790, 5, item_code="STRAWBERRY-MEDIUM"),
            _store_child(307, 367, 13783, 2, item_code="CHOCO-HAZELNUT-MEDIUM"),
            {"id": 309, "product_id": 500, "variation_id": 0, "quantity": 1, "meta_data": []},
            # Copy A's pistachio, appended by the previous push. Given qty 5
            # here so it cannot also be reached through the same-qty slot reuse.
            _store_child(308, 2286, 13826, 5, item_code="PISTACHIO-MEDIUM"),
        ]
        invoice = _invoice([
            _cookie(),
            _feast_parent(),
            _jar("PISTACHIO-MEDIUM", 5), _jar("BLUEBERRY-MEDIUM", 3), _jar("CHOCO-HAZELNUT-MEDIUM", 2),
            _feast_parent(),
            _jar("PISTACHIO-MEDIUM", 3), _jar("STRAWBERRY-MEDIUM", 5), _jar("CHOCO-HAZELNUT-MEDIUM", 2),
        ])
        line_items, _missing = _collect(invoice, MAPPING, registered={str(FEAST_PID)})

        matched, added, orphaned = outbound_sync._attach_existing_line_ids(line_items, existing)

        self.assertEqual(added, [])
        self.assertEqual(orphaned, [])
        ids = [(e["id"], _meta(e)["erpnext_item_code"], e["quantity"]) for e in matched]
        self.assertIn((308, "PISTACHIO-MEDIUM", 5), ids)   # A's, reused
        self.assertIn((305, "PISTACHIO-MEDIUM", 3), ids)   # B's, untouched
        self.assertIn((302, "BLUEBERRY-MEDIUM", 3), ids)

    def test_a_single_copy_still_matches_greedily_as_before(self):
        """No repeated bundle: one scope, one pass — the pre-17748 behaviour.

        The swapped jar still reuses the first free same-qty child slot, and the
        leftover sibling is reported as orphaned, exactly as before.
        """
        matched, added, orphaned = outbound_sync._attach_existing_line_ids(
            [
                {"product_id": FEAST_PID, "quantity": 1,
                 "meta_data": [{"key": "erpnext_item_code", "value": FEAST}]},
                {"product_id": 2286, "variation_id": 13826, "quantity": 4,
                 "meta_data": [{"key": "erpnext_item_code", "value": "PISTACHIO-MEDIUM"},
                               {"key": "_woosb_parent_id", "value": str(FEAST_PID)}]},
            ],
            [
                _store_parent(401, STORE_A_ORIGINAL),
                _store_child(402, 371, 13790, 4),
                _store_child(403, 369, 13780, 4),
            ],
        )

        self.assertEqual(added, [])
        self.assertEqual([e["id"] for e in matched], [401, 402])
        self.assertEqual([e["id"] for e in orphaned], [403])


# ---------------------------------------------------------------------------
# Defect 2 — keep our _woosb_ids on a changed copy of a repeated bundle
# ---------------------------------------------------------------------------

class TestRepeatedBundleSelectionRefresh(unittest.TestCase):
    def test_store_created_order_with_a_changed_copy_keeps_ours_on_that_parent_only(self):
        payload_lines, added, orphaned, _missing = _push(_store_order_17748())

        self.assertEqual((added, orphaned), ([], []))
        lines = _by_id(payload_lines)
        # Copy A: WE swapped strawberry for pistachio -> our string goes out.
        self.assertEqual(
            _totals(_meta(lines[201])["_woosb_ids"]),
            {"13826": 4, "13780": 4, "13783": 2},
        )
        # Copy B: nobody changed it -> the plugin's own string is preserved.
        self.assertNotIn("_woosb_ids", _meta(lines[205]))

    def test_after_the_push_inbound_reads_the_operators_mix_back(self):
        """Model the store after our PUT and let the inbound gate read it."""
        store = _store_order_17748()
        payload_lines, _added, _orphaned, _missing = _push(store)

        # WooCommerce merges line meta: a key we sent replaces, one we omitted stays.
        after = copy.deepcopy(store)
        by_id = _by_id(after["line_items"])
        for sent in payload_lines:
            line = by_id[sent["id"]]
            for key in ("product_id", "variation_id", "quantity", "subtotal", "total"):
                if key in sent:
                    line[key] = sent[key]
            merged = {m["key"]: m["value"] for m in line["meta_data"]}
            merged.update(_meta(sent))
            line["meta_data"] = [{"key": k, "value": v} for k, v in merged.items()]

        cache = _inbound_cache()
        first = order_sync._build_bundle_selections(
            after["line_items"], FEAST_PID, 1, cache=cache, parent_line=by_id[201]
        )
        second = order_sync._build_bundle_selections(
            after["line_items"], FEAST_PID, 1, cache=cache, parent_line=by_id[205]
        )
        self.assertEqual(first, {"Medium": [
            {"item_code": "PISTACHIO-MEDIUM", "selected_qty": 4},
            {"item_code": "BLUEBERRY-MEDIUM", "selected_qty": 4},
            {"item_code": "CHOCO-HAZELNUT-MEDIUM", "selected_qty": 2},
        ]})
        self.assertEqual(second, {"Medium": [
            {"item_code": "PISTACHIO-MEDIUM", "selected_qty": 3},
            {"item_code": "STRAWBERRY-MEDIUM", "selected_qty": 5},
            {"item_code": "CHOCO-HAZELNUT-MEDIUM", "selected_qty": 2},
        ]})
        # Copy B's string is still the plugin's, token and attributes intact.
        self.assertEqual(_meta(by_id[205])["_woosb_ids"], STORE_B)

        # And the NEXT push finds both copies in agreement: nothing kept, and
        # the order compares as already in sync — no ping-pong, no F-18.
        next_payload, next_added, next_orphaned, _ = _push(after)
        self.assertEqual((next_added, next_orphaned), ([], []))
        for entry in next_payload:
            self.assertNotIn("_woosb_ids", _meta(entry))
        self.assertFalse(outbound_sync._order_payload_requires_update(
            after, {"status": "processing", "line_items": next_payload}
        ))

    def test_store_created_repeated_bundle_with_nothing_changed_is_preserved(self):
        store = _store_order_17748()
        # Store's copy A already states the amended mix (in the plugin's own tokens).
        store["line_items"][0]["meta_data"][0]["value"] = (
            f"13826/zz99/4/{ATTR},13780/cd34/4/{ATTR},13783/ef56/2/{ATTR}"
        )
        store["line_items"][1] = _store_child(202, 2286, 13826, 4)

        payload_lines, _added, _orphaned, _missing = _push(store)

        for entry in payload_lines:
            self.assertNotIn("_woosb_ids", _meta(entry))

    def test_store_created_single_copy_is_preserved_as_today(self):
        """One copy: inbound reads the children, so the plugin's string is left alone."""
        store = {
            "id": 17000, "status": "processing", "created_via": "checkout", "meta_data": [],
            "line_items": [
                _store_parent(501, STORE_A_ORIGINAL),
                _store_child(502, 371, 13790, 4),
                _store_child(503, 369, 13780, 4),
                _store_child(504, 367, 13783, 2),
            ],
        }
        payload_lines = [
            {"id": 501, "product_id": FEAST_PID, "quantity": 1, "meta_data": [
                {"key": "erpnext_item_code", "value": FEAST},
                {"key": "_woosb_ids", "value": "13826/aaaa/4/{},13780/bbbb/4/{},13783/cccc/2/{}"},
            ]},
            {"id": 502, "product_id": 2286, "variation_id": 13826, "quantity": 4, "meta_data": [
                {"key": "_woosb_parent_id", "value": str(FEAST_PID)},
            ]},
        ]

        outbound_sync._preserve_native_bundle_selection(payload_lines, store)

        self.assertNotIn("_woosb_ids", _meta(payload_lines[0]))

    def test_our_own_order_keeps_every_string_as_before(self):
        store = _store_order_17748(
            created_via="rest-api",
            meta_data=[{"key": "_jarz_order_origin", "value": "ERPNext POS"}],
        )

        payload_lines, _added, _orphaned, _missing = _push(store)

        lines = _by_id(payload_lines)
        self.assertIn("_woosb_ids", _meta(lines[201]))
        self.assertIn("_woosb_ids", _meta(lines[205]))

    def test_an_unreadable_store_string_is_never_treated_as_disagreement(self):
        store = _store_order_17748()
        store["line_items"][0]["meta_data"][0]["value"] = "garbage-without-parts"

        payload_lines, _added, _orphaned, _missing = _push(store)

        self.assertNotIn("_woosb_ids", _meta(_by_id(payload_lines)[201]))

    def test_totals_ignore_token_and_attributes(self):
        self.assertEqual(
            _totals(f"13826/zz99/4/{ATTR},13780/cd34/4/{ATTR}"),
            _totals("13826/aaaa/4/{},13780/bbbb/4/{}"),
        )
        self.assertIsNone(_totals(""))
        self.assertIsNone(_totals("13826/zz99/x/{}"))

    def test_woosb_ids_still_cannot_trigger_a_push_on_its_own(self):
        self.assertNotIn("_woosb_ids", outbound_sync._ORDER_LINE_META_KEYS_TO_COMPARE)


if __name__ == "__main__":
    unittest.main()
