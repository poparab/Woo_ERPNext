"""Woo coupon mapping and the should-exist rule (pure unit tests, client mocked)."""

import unittest
import unittest.mock as mock
from datetime import datetime, timedelta

from jarz_woocommerce_integration.services import coupon_sync as cs
from jarz_woocommerce_integration.utils.http_client import WooAPIError


def _doc(**kw):
    base = dict(
        name="BACK-7K3QXZ", sync_to_woo=1, enabled=1, discount_type="Percentage",
        discount_value=20, usage_limit=0, applies_to="Whole Order", item_scope=[],
    )
    base.update(kw)
    return base


class _D(dict):
    """dict with attribute access, like a Document for our purposes."""

    def __getattr__(self, k):
        return self.get(k)


def D(**kw):
    return _D(_doc(**kw))


NOW = datetime(2026, 10, 27, 12, 0, 0)


class TestShouldExist(unittest.TestCase):
    def test_ok(self):
        self.assertEqual(cs.should_exist(D(), 0, NOW), (True, "ok"))

    def test_off_flag_disabled(self):
        self.assertEqual(cs.should_exist(D(sync_to_woo=0), 0, NOW)[1], "sync_to_woo_off")
        self.assertEqual(cs.should_exist(D(enabled=0), 0, NOW)[1], "disabled")

    def test_expiry(self):
        past = (NOW - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        fut = (NOW + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        self.assertEqual(cs.should_exist(D(active_to=past), 0, NOW)[1], "expired")
        self.assertTrue(cs.should_exist(D(active_to=fut), 0, NOW)[0])

    def test_usage(self):
        self.assertTrue(cs.should_exist(D(usage_limit=3), 2, NOW)[0])
        self.assertEqual(cs.should_exist(D(usage_limit=3), 3, NOW)[1], "fully_used")
        self.assertTrue(cs.should_exist(D(usage_limit=0), 99, NOW)[0])


class TestPayload(unittest.TestCase):
    def test_percentage(self):
        p = cs.build_payload(
            D(stackable=0, usage_limit=5, active_to="2026-11-01 23:59:00", min_order_amount=100,
              max_discount_amount=50, customer_phone="01000000000"),
            None,
        )
        self.assertEqual(p["code"], "back-7k3qxz")
        self.assertEqual((p["discount_type"], p["amount"], p["free_shipping"]), ("percent", "20", False))
        self.assertTrue(p["individual_use"])
        self.assertEqual(p["usage_limit"], 5)
        self.assertEqual(p["date_expires"], "2026-11-01T23:59:00")
        self.assertEqual(p["minimum_amount"], "100")
        self.assertNotIn("product_ids", p)
        self.assertIn({"key": "_jarz_max_discount", "value": "50"}, p["meta_data"])
        self.assertIn({"key": "_jarz_phone", "value": "01000000000"}, p["meta_data"])
        self.assertEqual(p["description"], "ERPNext Jarz Promo Code BACK-7K3QXZ")

    def test_fixed_and_free_delivery(self):
        f = cs.build_payload(D(discount_type="Fixed Amount", discount_value=30, stackable=1), None)
        self.assertEqual((f["discount_type"], f["amount"], f["individual_use"]), ("fixed_cart", "30", False))
        d = cs.build_payload(D(discount_type="Free Delivery"), None)
        self.assertEqual((d["discount_type"], d["amount"], d["free_shipping"]), ("fixed_cart", "0", True))
        self.assertIsNone(d["usage_limit"])
        self.assertIsNone(d["date_expires"])
        self.assertEqual(d["meta_data"], [])
        self.assertNotIn("minimum_amount", d)


class TestScope(unittest.TestCase):
    def test_specific_items_prefers_variation(self):
        rows = [{"item_code": "A"}, {"item_code": "B"}, {"item_code": "C"}]
        items = [
            {"name": "A", "woo_variation_id": "11", "woo_product_id": "1"},
            {"name": "B", "woo_variation_id": "", "woo_product_id": "2"},
            {"name": "C", "woo_variation_id": None, "woo_product_id": None},
        ]
        with mock.patch.object(cs.frappe, "get_all", return_value=items):
            ids = cs.resolve_product_ids(D(applies_to="Specific Items", item_scope=rows))
        self.assertEqual(ids, [11, 2])

    def test_item_groups_and_empty(self):
        with mock.patch.object(cs.frappe, "get_all", return_value=[]) as ga:
            ids = cs.resolve_product_ids(D(applies_to="Item Groups", item_scope=[{"item_group": "G"}]))
        self.assertEqual(ids, [])
        self.assertEqual(ga.call_args_list[0].kwargs["filters"]["item_group"], ["in", ["G"]])


# Live shapes on production, 2026-11-05: every enabled bundle is one size.
BUNDLES = [
    {"name": "b-trio", "woo_bundle_id": "12441"},     # Large x3
    {"name": "b-grand", "woo_bundle_id": "12479"},    # Large x6 + Large x2
    {"name": "b-feast", "woo_bundle_id": "12485"},    # Medium x8 + Medium x2
    {"name": "b-mixed", "woo_bundle_id": "10025"},    # Large x6 + Medium x2
    {"name": "b-noid", "woo_bundle_id": ""},          # never mirrored to Woo
    {"name": "b-junk", "woo_bundle_id": " 12479 "},   # duplicate id, stray spaces
]
BUNDLE_ROWS = [
    {"parent": "b-trio", "item_group": "Large"},
    {"parent": "b-grand", "item_group": "Large"},
    {"parent": "b-grand", "item_group": "Large"},
    {"parent": "b-feast", "item_group": "Medium"},
    {"parent": "b-feast", "item_group": "Medium"},
    {"parent": "b-mixed", "item_group": "Large"},
    {"parent": "b-mixed", "item_group": "Medium"},
    {"parent": "b-noid", "item_group": "Large"},
    {"parent": "b-junk", "item_group": "Large"},
]
LARGE_ITEMS = [
    {"name": "Lotus Large", "woo_variation_id": "13766", "woo_product_id": "217"},
    {"name": "Carrot cake Large", "woo_variation_id": None, "woo_product_id": "11031"},
]


def _fake_get_all(items=LARGE_ITEMS, bundles=BUNDLES, rows=BUNDLE_ROWS):
    def get_all(doctype, filters=None, fields=None, **kw):
        if doctype == "Item":
            return list(items)
        if doctype == cs.BUNDLE_DOCTYPE:
            assert filters == {"disabled": 0}
            return list(bundles)
        if doctype == cs.BUNDLE_ROW_DOCTYPE:
            wanted = set(filters["parent"][1])
            return [r for r in rows if r["parent"] in wanted]
        raise AssertionError(doctype)

    return get_all


class TestBundleScope(unittest.TestCase):
    """A free-Large-jar code must reach the Large bundles' parent lines on Woo."""

    def test_large_code_names_large_only_bundles(self):
        with mock.patch.object(cs.frappe, "get_all", side_effect=_fake_get_all()):
            ids = cs.resolve_product_ids(D(applies_to="Item Groups", item_scope=[{"item_group": "Large"}]))
        # loose jars first, then Trio and Grand Eight; not Royal Feast (Medium),
        # not the mixed 6L+2M, not the unmirrored bundle, no duplicate.
        self.assertEqual(ids, [13766, 11031, 12441, 12479])

    def test_two_groups_admit_the_mixed_bundle(self):
        with mock.patch.object(cs.frappe, "get_all", side_effect=_fake_get_all()):
            ids = cs.resolve_product_ids(D(
                applies_to="Item Groups", item_scope=[{"item_group": "Large"}, {"item_group": "Medium"}],
            ))
        self.assertEqual(ids[2:], [12441, 12479, 12485, 10025])

    def test_bundles_alone_still_sync(self):
        with mock.patch.object(cs.frappe, "get_all", side_effect=_fake_get_all(items=[])):
            ids = cs.resolve_product_ids(D(applies_to="Item Groups", item_scope=[{"item_group": "Medium"}]))
        self.assertEqual(ids, [12485])

    def test_specific_items_and_whole_order_skip_bundles(self):
        with mock.patch.object(cs.frappe, "get_all", side_effect=_fake_get_all()) as ga:
            ids = cs.resolve_product_ids(D(applies_to="Specific Items", item_scope=[{"item_code": "Lotus Large"}]))
        self.assertEqual(ids, [13766, 11031])
        self.assertEqual([c.args[0] for c in ga.call_args_list], ["Item"])
        self.assertIsNone(cs.resolve_product_ids(D(applies_to="Whole Order")))

    def test_missing_bundle_doctype_is_harmless(self):
        def get_all(doctype, **kw):
            if doctype == "Item":
                return list(LARGE_ITEMS)
            raise Exception("Table 'tabJarz Bundle' doesn't exist")

        with mock.patch.object(cs.frappe, "get_all", side_effect=get_all):
            ids = cs.resolve_product_ids(D(applies_to="Item Groups", item_scope=[{"item_group": "Large"}]))
        self.assertEqual(ids, [13766, 11031])

    def test_payload_carries_bundle_ids_and_cap(self):
        with mock.patch.object(cs.frappe, "get_all", side_effect=_fake_get_all()):
            doc = D(name="BACKYVQM2K", discount_value=100, applies_to="Item Groups",
                    item_scope=[{"item_group": "Large"}], max_discount_amount=160, stackable=0)
            p = cs.build_payload(doc, cs.resolve_product_ids(doc))
        self.assertIn(12479, p["product_ids"])
        self.assertEqual((p["discount_type"], p["amount"]), ("percent", "100"))
        self.assertIn({"key": "_jarz_max_discount", "value": "160"}, p["meta_data"])


class TestResync(unittest.TestCase):
    def test_enqueues_every_live_item_group_code(self):
        with mock.patch.object(cs.frappe, "only_for"), \
                mock.patch.object(cs.frappe, "get_all", return_value=["BACKA", "BACKB"]) as ga, \
                mock.patch.object(cs, "_enqueue") as enq:
            r = cs.resync_item_group_coupons()
        self.assertEqual(r, {"enqueued": ["BACKA", "BACKB"]})
        self.assertEqual(ga.call_args.kwargs["filters"],
                         {"sync_to_woo": 1, "enabled": 1, "applies_to": "Item Groups"})
        self.assertEqual([c.args[0] for c in enq.call_args_list], ["BACKA", "BACKB"])


class TestSync(unittest.TestCase):
    def _run(self, doc, client, redemptions=0):
        patches = [
            mock.patch.object(cs, "_gate_open", return_value=True),
            mock.patch.object(cs.frappe, "get_doc", return_value=doc),
            mock.patch.object(cs.frappe.db, "exists", return_value=True, create=True),
            mock.patch.object(cs, "active_redemptions", return_value=redemptions),
            mock.patch.object(cs, "_store_woo_id"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        return cs._sync(doc.name, client=client)

    def test_creates_when_absent(self):
        c = mock.Mock()
        c.get.return_value = []
        c.post.return_value = {"id": 77}
        r = self._run(D(), c)
        self.assertEqual((r["action"], r["woo_coupon_id"]), ("created", "77"))
        c.post.assert_called_once()

    def test_updates_stored_id(self):
        c = mock.Mock()
        c.put.return_value = {"id": 5}
        r = self._run(D(woo_coupon_id="5"), c)
        self.assertEqual(r["action"], "updated")
        c.post.assert_not_called()

    def test_put_404_falls_back_to_adopt(self):
        c = mock.Mock()
        c.put.side_effect = [WooAPIError(404, "u", "gone"), {"id": 9}]
        c.get.return_value = [{"id": 9, "code": "back-7k3qxz"}]
        r = self._run(D(woo_coupon_id="5"), c)
        self.assertEqual((r["action"], r["woo_coupon_id"]), ("updated", "9"))

    def test_deletes_when_fully_used(self):
        c = mock.Mock()
        r = self._run(D(woo_coupon_id="5", usage_limit=2), c, redemptions=2)
        self.assertEqual((r["action"], r["reason"]), ("deleted", "fully_used"))
        c.delete.assert_called_once_with("coupons/5", params={"force": "true"})

    def test_scope_with_no_ids_is_skipped(self):
        c = mock.Mock()
        with mock.patch.object(cs, "resolve_product_ids", return_value=[]), \
                mock.patch.object(cs.frappe, "log_error"):
            r = self._run(D(applies_to="Specific Items"), c)
        self.assertEqual(r["action"], "skipped")
        c.post.assert_not_called()

    def test_gate_off_is_noop(self):
        c = mock.Mock()
        with mock.patch.object(cs, "_gate_open", return_value=False), \
                mock.patch.object(cs, "_note_disabled"):
            r = cs._sync("X", client=c)
        self.assertEqual(r["action"], "skipped")
        c.get.assert_not_called()
