"""A POS amendment that turns a bundle into plain lines must zero the store's parent line.

Production case, 2026-09-30: Woo 17751 / ``ACC-SINV-2026-18618-1``. The order
held one "Jarz Royal Feast" (8 Medium + 2 Medium). The branch had no Blueberry
Medium, so the order was re-issued as plain lines (8 Medium at 96, 2 Blueberry
Large at 136): invoice 1,040, no ``is_bundle_parent`` row, no ``parent_bundle``.

The outbound push repriced the jars, but `_protected_existing_line_ids` kept
parent line 62812 off the removal list because its product is still named by
the children's ``_woosb_parent_id``. It stayed at 960, the store total became
2,000, and `_check_response_total` marked the invoice Error.

Pinned here:

* that parent line is pushed back at ``subtotal``/``total`` "0.00" (not
  removed), and the payload then sums to ``grand_total``;
* once zero, the order compares as in sync, so it is not pushed on every sync;
* the protection is unchanged for a legacy flag-free invoice (its 100%-off
  parent row is still on the invoice), for an invoice that still holds the
  bundle, for an unmapped bundle, and for a line protected as an unmapped item.
"""

from types import SimpleNamespace
import unittest
import unittest.mock

from jarz_woocommerce_integration.services import outbound_sync


ROYAL_FEAST = 12446
PARENT_LINE = 62812

#: The 17751 jars. Large Blueberry is a different variation of the same product.
_MAPPING = {
    "Jarz Royal Feast": {"woo_product_id": str(ROYAL_FEAST)},
    "Molten Medium": {"woo_product_id": "11162", "woo_variation_id": "13802"},
    "Tiramisu Medium": {"woo_product_id": "11140", "woo_variation_id": "13806"},
    "Strawberry Medium": {"woo_product_id": "369", "woo_variation_id": "13780"},
    "Blueberry Medium": {"woo_product_id": "217", "woo_variation_id": "13767"},
    "Blueberry Large": {"woo_product_id": "217", "woo_variation_id": "13768"},
}


def _item(item_code, *, qty, amount, price_list_rate, discount_percentage=0, **flags):
    return SimpleNamespace(
        item_code=item_code,
        item_name=item_code,
        qty=qty,
        amount=amount,
        rate=amount / qty if qty else 0,
        price_list_rate=price_list_rate,
        discount_percentage=discount_percentage,
        is_bundle_parent=flags.pop("is_bundle_parent", 0),
        is_bundle_child=flags.pop("is_bundle_child", 0),
        parent_bundle=flags.pop("parent_bundle", None),
        bundle_code=flags.pop("bundle_code", None),
    )


def _invoice(items, *, grand_total=None):
    return SimpleNamespace(
        name="ACC-SINV-2026-18618-1",
        customer="CUST-17751",
        company="Jarz",
        docstatus=1,
        items=items,
        grand_total=grand_total if grand_total is not None else sum(item.amount for item in items),
    )


def _item_db(mapping):
    def get_value(doctype, name, fields=None, as_dict=False, **kwargs):
        if doctype != "Item":
            return None
        row = mapping.get(name)
        if row is None:
            return None
        return {"item_name": name, **row}

    return SimpleNamespace(get_value=get_value)


def _meta(**values):
    return [{"key": key, "value": value} for key, value in values.items()]


def _store_line(line_id, product_id, variation_id, quantity, subtotal, total, **meta):
    return {
        "id": line_id,
        "product_id": product_id,
        "variation_id": variation_id,
        "quantity": quantity,
        "subtotal": subtotal,
        "total": total,
        "meta_data": _meta(**meta),
    }


def _store_order(*, parent_total="960.00"):
    """Woo 17751 as the store held it after the failed push."""
    child = {"_woosb_parent_id": str(ROYAL_FEAST)}
    return {
        "id": 17751,
        "line_items": [
            _store_line(PARENT_LINE, ROYAL_FEAST, 0, 1, parent_total, parent_total,
                        erpnext_item_code="Jarz Royal Feast",
                        _woosb_ids="13802/ab12/1/{},13806/cd34/1/{}"),
            _store_line(62813, 11162, 13802, 1, "120.00", "96.00",
                        erpnext_item_code="Molten Medium", discount_percentage="20", **child),
            _store_line(62814, 11140, 13806, 1, "120.00", "96.00",
                        erpnext_item_code="Tiramisu Medium", discount_percentage="20", **child),
            _store_line(62816, 369, 13780, 2, "240.00", "192.00",
                        erpnext_item_code="Strawberry Medium", discount_percentage="20", **child),
            _store_line(62822, 217, 13768, 2, "320.00", "272.00",
                        erpnext_item_code="Blueberry Large", discount_percentage="15"),
        ],
    }


def _unbundled_invoice():
    """The amended invoice: plain lines, no bundle flags at all."""
    return _invoice([
        _item("Molten Medium", qty=1, amount=96, price_list_rate=120, discount_percentage=20),
        _item("Tiramisu Medium", qty=1, amount=96, price_list_rate=120, discount_percentage=20),
        _item("Strawberry Medium", qty=2, amount=192, price_list_rate=120, discount_percentage=20),
        _item("Blueberry Large", qty=2, amount=272, price_list_rate=160, discount_percentage=15),
    ])


def _bundled_invoice():
    """The same order while it still held the bundle."""
    return _invoice([
        _item("Jarz Royal Feast", qty=1, amount=0, price_list_rate=960, discount_percentage=100,
              is_bundle_parent=1, bundle_code="JB-RF"),
        _item("Molten Medium", qty=1, amount=96, price_list_rate=120, discount_percentage=20,
              is_bundle_child=1, parent_bundle="JB-RF"),
        _item("Tiramisu Medium", qty=1, amount=96, price_list_rate=120, discount_percentage=20,
              is_bundle_child=1, parent_bundle="JB-RF"),
        _item("Strawberry Medium", qty=2, amount=192, price_list_rate=120, discount_percentage=20,
              is_bundle_child=1, parent_bundle="JB-RF"),
        _item("Blueberry Medium", qty=2, amount=192, price_list_rate=120, discount_percentage=20,
              is_bundle_child=1, parent_bundle="JB-RF"),
    ])


class _FakeCache:
    def get_value(self, key):
        return None

    def set_value(self, key, value, expires_in_sec=None):
        pass


def _line_updates(invoice, existing_order, *, mapping=_MAPPING, registered=frozenset()):
    """The ``line_items`` part of `_build_order_payload`, with the store stubbed out."""
    with unittest.mock.patch.object(
        outbound_sync, "_get_registered_bundle_product_ids", return_value=set(registered)
    ), unittest.mock.patch.object(outbound_sync.frappe, "db", _item_db(mapping)), \
            unittest.mock.patch.object(outbound_sync.frappe, "cache", _FakeCache):
        line_items, missing = outbound_sync._collect_line_items(invoice)
        matched, added, orphaned = outbound_sync._attach_existing_line_ids(
            line_items, existing_order.get("line_items") or []
        )
        protected = outbound_sync._protected_existing_line_ids(
            invoice, existing_order, protected_item_codes=set(missing)
        )
        removals = outbound_sync._build_line_item_removals(orphaned, protected_ids=protected)
        zeroed = outbound_sync._abandoned_bundle_parent_updates(
            invoice,
            existing_order,
            orphaned,
            protected_ids=protected,
            protected_item_codes=set(missing),
        )
    return {
        "payload": matched + added + zeroed + removals,
        "zeroed": zeroed,
        "removals": removals,
        "protected": protected,
    }


def _by_id(entries):
    return {entry.get("id"): entry for entry in entries if entry.get("id")}


class TestUnbundledParentLineIsZeroed(unittest.TestCase):
    def test_17751_parent_line_is_pushed_at_zero_not_deleted(self):
        invoice = _unbundled_invoice()
        result = _line_updates(invoice, _store_order())

        self.assertIn(PARENT_LINE, result["protected"])
        self.assertEqual(result["removals"], [], "the parent line must not be deleted")
        self.assertEqual(len(result["zeroed"]), 1)
        zeroed = result["zeroed"][0]
        self.assertEqual(zeroed["id"], PARENT_LINE)
        self.assertEqual(zeroed["subtotal"], "0.00")
        self.assertEqual(zeroed["total"], "0.00")
        # Woo reads quantity 0 as "delete this line": it must carry the real one.
        self.assertEqual(zeroed["quantity"], 1)
        self.assertEqual(zeroed["product_id"], ROYAL_FEAST)

    def test_17751_payload_reaches_grand_total_once_the_parent_is_zeroed(self):
        invoice = _unbundled_invoice()
        payload = {"line_items": _line_updates(invoice, _store_order())["payload"]}

        self.assertIsNone(outbound_sync._payload_total_mismatch(payload, invoice))
        # The store's arithmetic after the PUT: every line the payload touches
        # takes the payload's total, every other line keeps the store's.
        pushed = _by_id(payload["line_items"])
        store_total = sum(
            outbound_sync.flt((pushed.get(line["id"]) or line)["total"])
            for line in _store_order()["line_items"]
        )
        self.assertAlmostEqual(store_total, 656.0)
        self.assertAlmostEqual(store_total, invoice.grand_total)

    def test_zeroing_does_not_touch_the_children_or_the_new_line(self):
        result = _line_updates(_unbundled_invoice(), _store_order())
        pushed = _by_id(result["payload"])

        self.assertEqual(pushed[62813]["total"], "96.00")
        self.assertEqual(pushed[62816]["quantity"], 2)
        self.assertEqual(pushed[62822]["total"], "272.00")

    def test_a_zeroed_parent_line_does_not_keep_the_order_dirty(self):
        invoice = _unbundled_invoice()
        store = _store_order(parent_total="0.00")
        payload = {"status": "processing", "line_items": _line_updates(invoice, store)["payload"]}
        store["status"] = "processing"

        self.assertFalse(outbound_sync._order_payload_requires_update(store, payload))

    def test_a_parent_line_still_at_bundle_price_marks_the_order_dirty(self):
        invoice = _unbundled_invoice()
        store = _store_order()
        payload = {"status": "processing", "line_items": _line_updates(invoice, store)["payload"]}
        store["status"] = "processing"

        self.assertTrue(outbound_sync._order_payload_requires_update(store, payload))

    def test_zeroing_is_logged_as_a_warning(self):
        with unittest.mock.patch.object(outbound_sync.LOGGER, "warning") as warning:
            _line_updates(_unbundled_invoice(), _store_order())

        events = [call.args[0].get("event") for call in warning.call_args_list]
        self.assertIn("woo_outbound_abandoned_bundle_parent_zeroed", events)


class TestProtectionIsKept(unittest.TestCase):
    def test_invoice_that_still_holds_the_bundle_pushes_the_parent_priced(self):
        store = _store_order()
        store["line_items"][-1] = _store_line(
            62817, 217, 13767, 2, "0.00", "0.00",
            erpnext_item_code="Blueberry Medium", _woosb_parent_id=str(ROYAL_FEAST),
        )
        result = _line_updates(_bundled_invoice(), store)
        pushed = _by_id(result["payload"])

        self.assertEqual(result["zeroed"], [])
        self.assertEqual(pushed[PARENT_LINE]["total"], "576.00")

    def test_legacy_flag_free_invoice_keeps_the_parent_line_untouched(self):
        """Its 100%-off parent row is inferred, dropped from the payload, and still on the invoice."""
        invoice = _invoice([
            _item("Jarz Royal Feast", qty=1, amount=0, price_list_rate=960, discount_percentage=100),
            _item("Molten Medium", qty=1, amount=96, price_list_rate=120, discount_percentage=20),
            _item("Tiramisu Medium", qty=1, amount=96, price_list_rate=120, discount_percentage=20),
        ])
        result = _line_updates(invoice, _store_order(), registered={str(ROYAL_FEAST)})

        self.assertIn(PARENT_LINE, result["protected"])
        self.assertNotIn(PARENT_LINE, _by_id(result["payload"]))
        self.assertEqual(result["zeroed"], [])
        self.assertEqual(result["removals"][0]["quantity"], 0)
        self.assertNotIn(PARENT_LINE, _by_id(result["removals"]))

    def test_unmapped_bundle_on_the_invoice_keeps_the_parent_line_untouched(self):
        """A flattened bundle cannot be tied to a store product, so nothing is zeroed."""
        mapping = {key: value for key, value in _MAPPING.items() if key != "Jarz Royal Feast"}
        invoice = _bundled_invoice()

        with unittest.mock.patch.object(outbound_sync.LOGGER, "warning") as warning:
            result = _line_updates(invoice, _store_order(), mapping=mapping)

        self.assertIn(PARENT_LINE, result["protected"])
        self.assertEqual(result["zeroed"], [])
        self.assertNotIn(PARENT_LINE, _by_id(result["payload"]))
        events = [call.args[0].get("event") for call in warning.call_args_list]
        self.assertIn("woo_outbound_bundle_parent_kept_unplaceable_invoice", events)

    def test_an_unmapped_row_anywhere_keeps_the_parent_line_untouched(self):
        invoice = _unbundled_invoice()
        invoice.items.append(_item("MYSTERY-ITEM", qty=1, amount=0, price_list_rate=0))

        result = _line_updates(invoice, _store_order())

        self.assertEqual(result["zeroed"], [])

    def test_a_line_protected_as_an_unmapped_item_is_not_zeroed(self):
        """A parent-product line whose ERPNext Item lost its mapping keeps its money."""
        mapping = {key: value for key, value in _MAPPING.items() if key != "Jarz Royal Feast"}
        store = _store_order()
        orphaned = [store["line_items"][0]]

        with unittest.mock.patch.object(
            outbound_sync, "_get_registered_bundle_product_ids", return_value=set()
        ), unittest.mock.patch.object(outbound_sync.frappe, "db", _item_db(mapping)):
            zeroed = outbound_sync._abandoned_bundle_parent_updates(
                _unbundled_invoice(),
                store,
                orphaned,
                protected_ids={PARENT_LINE},
                protected_item_codes={"Jarz Royal Feast"},
            )

        self.assertEqual(zeroed, [])

    def test_an_orphan_that_is_not_a_bundle_parent_is_still_removed(self):
        store = _store_order()
        store["line_items"].append(
            _store_line(62830, 555, 0, 1, "50.00", "50.00", erpnext_item_code="Removed Item")
        )
        result = _line_updates(_unbundled_invoice(), store)

        self.assertEqual(result["removals"], [{"id": 62830, "quantity": 0}])
        self.assertEqual([entry["id"] for entry in result["zeroed"]], [PARENT_LINE])


if __name__ == "__main__":
    unittest.main()
