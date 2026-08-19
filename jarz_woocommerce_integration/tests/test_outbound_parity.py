"""Woo <-> POS order parity: the outbound wire format (WP-1).

Every test here pins a difference between an order we push and an order the
website creates, measured against real production orders:

* F-01  a header discount must reach the store as a negative fee line
* F-03  the pushed total must equal ``grand_total``
* F-04  shipping is the charge row on the shipping-income account, not a
        description that happens to say "delivery"
* F-10  bundles go out WooSB-native: priced parent, zero children, ``_woosb_ids``
* F-11  the legacy bundle-parent guess must not eat a genuine giveaway line
* F-12  an Item mapped by ``woo_variation_id`` must resolve, and an unmappable
        line must not fail the whole order
* F-15  a fully returned order is ``refunded``
* F-18  the already-in-sync comparison only looks at meta keys we own
* F-19  the store must date the order when the sale happened
* F-25  the realtime event must go to a room somebody is in
"""

from types import SimpleNamespace
import unittest
import unittest.mock

from jarz_woocommerce_integration.services import order_sync, outbound_sync


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _cfg(**overrides):
    values = {
        "enable_customer_push": True,
        "enable_order_push": True,
        "payment_cod": "cod",
        "payment_instapay": "instapay",
        "payment_wallet": "wallet",
        # Unset in production, which is the case that matters.
        "shipping_method_id": "",
        "shipping_method_title": "",
    }
    values.update(overrides)
    return outbound_sync.OutboundConfig(**values)


def _item(item_code, *, qty, amount, price_list_rate=None, rate=None, **flags):
    """A Sales Invoice Item row."""
    price_list_rate = amount / qty if price_list_rate is None and qty else (price_list_rate or 0)
    return SimpleNamespace(
        item_code=item_code,
        item_name=flags.pop("item_name", item_code),
        qty=qty,
        amount=amount,
        rate=rate if rate is not None else (amount / qty if qty else 0),
        price_list_rate=price_list_rate,
        discount_percentage=flags.pop("discount_percentage", 0),
        is_bundle_parent=flags.pop("is_bundle_parent", 0),
        is_bundle_child=flags.pop("is_bundle_child", 0),
        parent_bundle=flags.pop("parent_bundle", None),
        bundle_code=flags.pop("bundle_code", None),
    )


def _invoice(items, **fields):
    values = {
        "name": "ACC-SINV-TEST-9001",
        "customer": "CUST-TEST-001",
        "currency": "EGP",
        "company": "Jarz",
        "docstatus": 1,
        "items": items,
    }
    values.update(fields)
    return SimpleNamespace(**values)


def _item_db(mapping):
    """``frappe.db`` stub answering the Item mapping lookups.

    ``mapping`` is ``{item_code: {"woo_product_id": ..., "woo_variation_id": ...}}``.
    An item absent from it has no Woo mapping at all.
    """
    def get_value(doctype, name, fields=None, as_dict=False, **kwargs):
        if doctype != "Item":
            return None
        if isinstance(name, dict):
            # `_resolve_bundle_selection_item_code`-style filter lookup
            for item_code, row in mapping.items():
                for key, wanted in name.items():
                    if str(row.get(key) or "") == str(wanted):
                        return item_code
            return None
        row = mapping.get(name)
        if row is None:
            return None
        return {"item_name": name, **row}

    return SimpleNamespace(get_value=get_value)


#: Mirrors production order 16895 — bundle 12446 with five size variations.
_NATIVE_BUNDLE_MAPPING = {
    "BUNDLE-12446": {"woo_product_id": "12446"},
    "JAR-369": {"woo_product_id": "369", "woo_variation_id": "13780"},
    "JAR-367": {"woo_product_id": "367", "woo_variation_id": "13783"},
    "JAR-217": {"woo_product_id": "217", "woo_variation_id": "13767"},
    "JAR-2286": {"woo_product_id": "2286", "woo_variation_id": "13826"},
    "JAR-2284": {"woo_product_id": "2284", "woo_variation_id": "13813"},
}


def _native_bundle_invoice():
    return _invoice([
        _item("BUNDLE-12446", qty=1, amount=0, price_list_rate=600, rate=0,
              discount_percentage=100, is_bundle_parent=1, bundle_code="JB-12446"),
        _item("JAR-369", qty=2, amount=200, price_list_rate=120, rate=100,
              discount_percentage=16.667, is_bundle_child=1, parent_bundle="JB-12446"),
        _item("JAR-367", qty=1, amount=100, price_list_rate=120, rate=100,
              discount_percentage=16.667, is_bundle_child=1, parent_bundle="JB-12446"),
        _item("JAR-217", qty=1, amount=100, price_list_rate=120, rate=100,
              discount_percentage=16.667, is_bundle_child=1, parent_bundle="JB-12446"),
        _item("JAR-2286", qty=1, amount=100, price_list_rate=120, rate=100,
              discount_percentage=16.667, is_bundle_child=1, parent_bundle="JB-12446"),
        _item("JAR-2284", qty=1, amount=100, price_list_rate=120, rate=100,
              discount_percentage=16.667, is_bundle_child=1, parent_bundle="JB-12446"),
    ])


def _collect(invoice, mapping, *, registered=frozenset()):
    with unittest.mock.patch.object(
        outbound_sync, "_get_registered_bundle_product_ids", return_value=set(registered)
    ), unittest.mock.patch.object(outbound_sync.frappe, "db", _item_db(mapping)):
        return outbound_sync._collect_line_items(invoice)


def _line_total_sum(line_items):
    return round(sum(outbound_sync.flt(entry.get("total") or 0) for entry in line_items), 2)


def _erpnext_amount_sum(invoice):
    return round(sum(outbound_sync.flt(item.amount or 0) for item in invoice.items), 2)


def _meta(entry):
    return {meta["key"]: meta["value"] for meta in entry.get("meta_data", [])}


# ---------------------------------------------------------------------------
# F-10 — the bundle wire shape
# ---------------------------------------------------------------------------

class TestBundleWireShape(unittest.TestCase):
    def test_parent_carries_the_price_and_children_are_free(self):
        invoice = _native_bundle_invoice()

        line_items, missing = _collect(invoice, _NATIVE_BUNDLE_MAPPING, registered={"12446"})

        self.assertEqual(missing, [])
        self.assertEqual(len(line_items), 6)

        parent = line_items[0]
        self.assertEqual(parent["product_id"], 12446)
        self.assertNotIn("variation_id", parent)
        self.assertEqual(parent["quantity"], 1)
        # The whole bundle price, taken from the children's ERPNext amounts.
        self.assertEqual(parent["subtotal"], "600.00")
        self.assertEqual(parent["total"], "600.00")

        for child in line_items[1:]:
            self.assertEqual(child["subtotal"], "0.00")
            self.assertEqual(child["total"], "0.00")
            self.assertEqual(_meta(child)["_woosb_parent_id"], "12446")

    def test_children_carry_their_variation_id(self):
        invoice = _native_bundle_invoice()

        line_items, _missing = _collect(invoice, _NATIVE_BUNDLE_MAPPING, registered={"12446"})

        by_item = {_meta(entry)["erpnext_item_code"]: entry for entry in line_items}
        self.assertEqual(by_item["JAR-369"]["product_id"], 369)
        self.assertEqual(by_item["JAR-369"]["variation_id"], 13780)
        self.assertEqual(by_item["JAR-2284"]["variation_id"], 13813)

    def test_parent_carries_a_four_part_woosb_ids_string(self):
        invoice = _native_bundle_invoice()

        line_items, _missing = _collect(invoice, _NATIVE_BUNDLE_MAPPING, registered={"12446"})
        woosb_ids = _meta(line_items[0])["_woosb_ids"]

        entries = order_sync._split_woosb_ids(woosb_ids)
        self.assertEqual(len(entries), 5)

        identifiers, quantities = [], []
        for entry in entries:
            parts = entry.split("/", 3)
            self.assertEqual(len(parts), 4, entry)
            identifiers.append(parts[0])
            self.assertTrue(parts[1], "the WooSB token slot must not be empty")
            quantities.append(parts[2])
            self.assertEqual(parts[3], "{}")

        # part[0] is the variation id, exactly as native order 16895 writes it.
        self.assertEqual(identifiers, ["13780", "13783", "13767", "13826", "13813"])
        # part[2] is the TOTAL quantity across the parent line.
        self.assertEqual(quantities, ["2", "1", "1", "1", "1"])

    def test_woosb_token_is_deterministic(self):
        invoice = _native_bundle_invoice()

        first, _ = _collect(invoice, _NATIVE_BUNDLE_MAPPING, registered={"12446"})
        second, _ = _collect(_native_bundle_invoice(), _NATIVE_BUNDLE_MAPPING, registered={"12446"})

        self.assertEqual(_meta(first[0])["_woosb_ids"], _meta(second[0])["_woosb_ids"])
        # A random token would re-create the permanently-dirty comparator F-18 fixes.
        self.assertIn(
            outbound_sync._woosb_selection_token("JAR-369"),
            _meta(first[0])["_woosb_ids"],
        )

    def test_woosb_identifiers_resolve_back_to_the_same_items(self):
        """The round trip our own inbound parser has to make."""
        invoice = _native_bundle_invoice()
        line_items, _missing = _collect(invoice, _NATIVE_BUNDLE_MAPPING, registered={"12446"})
        woosb_ids = _meta(line_items[0])["_woosb_ids"]

        resolved = []
        with unittest.mock.patch.object(
            order_sync.frappe, "db", _item_db(_NATIVE_BUNDLE_MAPPING)
        ):
            for entry in order_sync._split_woosb_ids(woosb_ids):
                identifier = entry.split("/", 3)[0]
                resolved.append(order_sync._resolve_bundle_selection_item_code(identifier))

        self.assertEqual(
            resolved, ["JAR-369", "JAR-367", "JAR-217", "JAR-2286", "JAR-2284"]
        )

    def test_per_bundle_quantity_survives_a_multi_bundle_order(self):
        """``_woosb_ids`` qty is per parent *line*; inbound divides by parent qty."""
        invoice = _invoice([
            _item("BUNDLE-12446", qty=3, amount=0, price_list_rate=600, rate=0,
                  discount_percentage=100, is_bundle_parent=1, bundle_code="JB-12446"),
            _item("JAR-369", qty=6, amount=600, price_list_rate=120, rate=100,
                  is_bundle_child=1, parent_bundle="JB-12446"),
        ])

        line_items, _missing = _collect(invoice, _NATIVE_BUNDLE_MAPPING, registered={"12446"})

        parent = line_items[0]
        self.assertEqual(parent["quantity"], 3)
        entry = order_sync._split_woosb_ids(_meta(parent)["_woosb_ids"])[0]
        total_qty = int(entry.split("/", 3)[2])
        self.assertEqual(total_qty, 6)
        self.assertEqual(total_qty // parent["quantity"], 2)  # two jars per bundle

    def test_duplicate_child_rows_are_merged_into_one_line(self):
        """Production order 15806 showed product 2351 twice, qty 4 then qty 1."""
        invoice = _invoice([
            _item("BUNDLE-12444", qty=1, amount=0, price_list_rate=640, rate=0,
                  discount_percentage=100, is_bundle_parent=1, bundle_code="JB-12444"),
            _item("JAR-2351", qty=4, amount=512, price_list_rate=160, rate=128,
                  is_bundle_child=1, parent_bundle="JB-12444"),
            _item("JAR-2351", qty=1, amount=128, price_list_rate=160, rate=128,
                  is_bundle_child=1, parent_bundle="JB-12444"),
        ])
        mapping = {
            "BUNDLE-12444": {"woo_product_id": "12444"},
            "JAR-2351": {"woo_product_id": "2351"},
        }

        line_items, missing = _collect(invoice, mapping, registered={"12444"})

        self.assertEqual(missing, [])
        self.assertEqual(len(line_items), 2)
        child = line_items[1]
        self.assertEqual(child["quantity"], 5)
        self.assertEqual(line_items[0]["total"], "640.00")
        entries = order_sync._split_woosb_ids(_meta(line_items[0])["_woosb_ids"])
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].split("/", 3)[2], "5")

    def test_line_totals_still_add_up_to_the_invoice(self):
        invoice = _native_bundle_invoice()

        line_items, _missing = _collect(invoice, _NATIVE_BUNDLE_MAPPING, registered={"12446"})

        self.assertEqual(_line_total_sum(line_items), _erpnext_amount_sum(invoice))
        self.assertEqual(_line_total_sum(line_items), 600.0)


# ---------------------------------------------------------------------------
# F-12 — unmappable items must not kill the order
# ---------------------------------------------------------------------------

class TestUnmappableItems(unittest.TestCase):
    def test_bundle_with_no_woo_product_is_flattened_not_failed(self):
        """12 of 26 Jarz Bundles have no Woo product; 11 invoices were stuck on it."""
        invoice = _invoice([
            _item("BUNDLE-UNMAPPED", qty=1, amount=0, price_list_rate=600, rate=0,
                  discount_percentage=100, is_bundle_parent=1, bundle_code="JB-NONE"),
            _item("JAR-369", qty=2, amount=200, price_list_rate=120, rate=100,
                  is_bundle_child=1, parent_bundle="JB-NONE"),
            _item("JAR-367", qty=1, amount=100, price_list_rate=120, rate=100,
                  is_bundle_child=1, parent_bundle="JB-NONE"),
        ])

        line_items, missing = _collect(invoice, _NATIVE_BUNDLE_MAPPING)

        # Nothing "missing": the parent is worth zero and its children carry the money.
        self.assertEqual(missing, [])
        self.assertEqual(len(line_items), 2)
        for entry in line_items:
            self.assertNotIn("_woosb_parent_id", _meta(entry))
        self.assertEqual(_line_total_sum(line_items), _erpnext_amount_sum(invoice))
        self.assertEqual(_line_total_sum(line_items), 300.0)

    def test_unmappable_standalone_line_is_dropped_and_recorded(self):
        invoice = _invoice([
            _item("JAR-369", qty=1, amount=100, price_list_rate=120, rate=100),
            _item("MYSTERY-ITEM", qty=1, amount=250),
        ])

        line_items, missing = _collect(invoice, _NATIVE_BUNDLE_MAPPING)

        self.assertEqual(missing, ["MYSTERY-ITEM"])
        self.assertEqual(len(line_items), 1)

    def test_variation_only_item_resolves_instead_of_failing(self):
        invoice = _invoice([_item("JAR-VAR", qty=1, amount=100, price_list_rate=100)])
        mapping = {"JAR-VAR": {"woo_variation_id": "13780"}}

        line_items, missing = _collect(invoice, mapping)

        self.assertEqual(missing, [])
        self.assertEqual(line_items[0]["variation_id"], 13780)
        self.assertNotIn("product_id", line_items[0])

    def test_composite_product_identifier_still_splits(self):
        invoice = _invoice([_item("JAR-COMBO", qty=1, amount=100, price_list_rate=100)])
        mapping = {"JAR-COMBO": {"woo_product_id": "369:13780"}}

        line_items, _missing = _collect(invoice, mapping)

        self.assertEqual(line_items[0]["product_id"], 369)
        self.assertEqual(line_items[0]["variation_id"], 13780)

    def test_a_dropped_line_does_not_delete_its_store_counterpart(self):
        existing_order = {
            "line_items": [
                {"id": 41, "product_id": 999, "quantity": 1,
                 "meta_data": [{"key": "erpnext_item_code", "value": "MYSTERY-ITEM"}]},
            ]
        }
        invoice = _invoice([_item("MYSTERY-ITEM", qty=1, amount=250)])

        with unittest.mock.patch.object(
            outbound_sync, "_get_registered_bundle_product_ids", return_value=set()
        ):
            protected = outbound_sync._protected_existing_line_ids(
                invoice, existing_order, protected_item_codes={"MYSTERY-ITEM"}
            )
            removals = outbound_sync._build_line_item_removals(
                existing_order["line_items"], protected_ids=protected
            )

        self.assertEqual(protected, {41})
        self.assertEqual(removals, [])


# ---------------------------------------------------------------------------
# F-11 — the legacy bundle-parent guess
# ---------------------------------------------------------------------------

class TestBundleParentInference(unittest.TestCase):
    def test_guess_still_drops_a_legacy_parent_but_says_so(self):
        invoice = _invoice([
            _item("LEGACY-BUNDLE", qty=1, amount=0, price_list_rate=432, rate=0,
                  discount_percentage=100),
            _item("JAR-369", qty=1, amount=100, price_list_rate=120, rate=100),
        ])
        mapping = {"LEGACY-BUNDLE": {"woo_product_id": "202"}, **_NATIVE_BUNDLE_MAPPING}

        with unittest.mock.patch.object(outbound_sync.LOGGER, "warning") as warning:
            line_items, missing = _collect(invoice, mapping, registered={"202"})

        self.assertEqual(missing, [])
        self.assertEqual(len(line_items), 1)
        events = [call.args[0].get("event") for call in warning.call_args_list]
        self.assertIn("woo_outbound_inferred_bundle_parent_dropped", events)

    def test_a_giveaway_survives_on_an_invoice_that_uses_the_explicit_flags(self):
        """The bug: a 100%-off bundle-registered item vanished from the order."""
        invoice = _invoice([
            _item("BUNDLE-12446", qty=1, amount=0, price_list_rate=600, rate=0,
                  discount_percentage=100, is_bundle_parent=1, bundle_code="JB-12446"),
            _item("JAR-369", qty=1, amount=600, price_list_rate=600, rate=600,
                  is_bundle_child=1, parent_bundle="JB-12446"),
            # A free gift of a product that also happens to be a registered bundle.
            _item("LEGACY-BUNDLE", qty=1, amount=0, price_list_rate=432, rate=0,
                  discount_percentage=100),
        ])
        mapping = {"LEGACY-BUNDLE": {"woo_product_id": "202"}, **_NATIVE_BUNDLE_MAPPING}

        line_items, missing = _collect(invoice, mapping, registered={"202", "12446"})

        emitted = {_meta(entry)["erpnext_item_code"] for entry in line_items}
        self.assertEqual(missing, [])
        self.assertIn("LEGACY-BUNDLE", emitted)

    def test_a_giveaway_survives_on_a_single_line_invoice(self):
        invoice = _invoice([
            _item("LEGACY-BUNDLE", qty=1, amount=0, price_list_rate=432, rate=0,
                  discount_percentage=100),
        ])

        line_items, _missing = _collect(
            invoice, {"LEGACY-BUNDLE": {"woo_product_id": "202"}}, registered={"202"}
        )

        self.assertEqual(len(line_items), 1)
        self.assertEqual(line_items[0]["total"], "0.00")


# ---------------------------------------------------------------------------
# F-01 — the header discount
# ---------------------------------------------------------------------------

class TestDiscountFeeLines(unittest.TestCase):
    def test_header_discount_becomes_a_negative_fee_line(self):
        invoice = _invoice([], discount_amount=640, apply_discount_on="Grand Total")

        fee_lines = outbound_sync._build_discount_fee_lines(invoice)

        self.assertEqual(fee_lines, [
            {"name": "Discount", "total": "-640.00", "tax_status": "none"},
        ])

    def test_fee_line_is_named_from_the_promo_codes(self):
        invoice = _invoice([], discount_amount=50, custom_promo_codes='["EID20", "VIP"]')

        fee_lines = outbound_sync._build_discount_fee_lines(invoice)

        self.assertEqual(fee_lines[0]["name"], "Discount (EID20, VIP)")

    def test_malformed_promo_codes_fall_back_to_the_plain_label(self):
        invoice = _invoice([], discount_amount=50, custom_promo_codes="not json")

        self.assertEqual(outbound_sync._build_discount_fee_lines(invoice)[0]["name"], "Discount")

    def test_no_discount_emits_no_fee_line(self):
        self.assertEqual(outbound_sync._build_discount_fee_lines(_invoice([])), [])

    def test_existing_fee_line_is_updated_in_place(self):
        invoice = _invoice([], discount_amount=25)
        existing_order = {"fee_lines": [{"id": 7, "name": "Discount", "total": "-40.00"}]}

        fee_lines = outbound_sync._build_discount_fee_lines(invoice, existing_order=existing_order)

        self.assertEqual(len(fee_lines), 1)
        self.assertEqual(fee_lines[0]["id"], 7)
        self.assertEqual(fee_lines[0]["total"], "-25.00")

    def test_a_removed_discount_is_retracted_from_the_store(self):
        invoice = _invoice([], discount_amount=0)
        existing_order = {"fee_lines": [{"id": 7, "name": "Discount", "total": "-40.00"}]}

        fee_lines = outbound_sync._build_discount_fee_lines(invoice, existing_order=existing_order)

        self.assertEqual(fee_lines, [{"id": 7, "name": None}])

    def test_a_positive_fee_added_in_wp_admin_is_left_alone(self):
        invoice = _invoice([], discount_amount=0)
        existing_order = {"fee_lines": [{"id": 9, "name": "Gift wrap", "total": "25.00"}]}

        self.assertEqual(
            outbound_sync._build_discount_fee_lines(invoice, existing_order=existing_order), []
        )

    def test_fee_line_drift_forces_a_resync(self):
        payload = {
            "status": "processing",
            "fee_lines": [{"name": "Discount", "total": "-640.00"}],
        }
        unchanged = {"status": "processing", "fee_lines": [{"name": "Discount", "total": "-640.0"}]}
        changed = {"status": "processing", "fee_lines": []}

        self.assertFalse(outbound_sync._order_payload_requires_update(unchanged, payload))
        self.assertTrue(outbound_sync._order_payload_requires_update(changed, payload))

    def test_discount_fields_are_watched_for_changes(self):
        self.assertIn("discount_amount", outbound_sync._INVOICE_OUTBOUND_PAYLOAD_FIELDS)
        self.assertIn("apply_discount_on", outbound_sync._INVOICE_OUTBOUND_PAYLOAD_FIELDS)
        self.assertIn("discount_amount", outbound_sync._OUTBOUND_RELEVANT_UPDATE_FIELDS)


# ---------------------------------------------------------------------------
# F-04 — shipping detection
# ---------------------------------------------------------------------------

class TestShippingTotal(unittest.TestCase):
    def _compute(self, invoice, account="Freight and Forwarding Charges - J"):
        with unittest.mock.patch.object(
            outbound_sync, "_resolve_shipping_income_account", return_value=account
        ):
            return outbound_sync._compute_shipping_total(invoice)

    def test_matches_on_the_shipping_income_account(self):
        invoice = _invoice([], taxes=[
            SimpleNamespace(charge_type="Actual", account_head="Freight and Forwarding Charges - J",
                            description="Shipping Income (Nasr City)", tax_amount=40),
        ])

        self.assertEqual(self._compute(invoice), 40)

    def test_an_item_called_delivery_box_is_no_longer_shipping(self):
        invoice = _invoice(
            [_item("Delivery Box", qty=1, amount=250, price_list_rate=250)],
            taxes=[],
        )

        self.assertEqual(self._compute(invoice), 0.0)

    def test_a_row_on_another_account_falls_back_to_the_description_and_logs(self):
        invoice = _invoice([], taxes=[
            SimpleNamespace(charge_type="Actual", account_head="Other Charges - J",
                            description="Shipping Income (Maadi)", tax_amount=30),
        ])

        with unittest.mock.patch.object(outbound_sync.LOGGER, "warning") as warning:
            total = self._compute(invoice)

        self.assertEqual(total, 30)
        events = [call.args[0].get("event") for call in warning.call_args_list]
        self.assertIn("woo_outbound_shipping_matched_by_description", events)

    def test_a_non_actual_row_is_ignored(self):
        invoice = _invoice([], taxes=[
            SimpleNamespace(charge_type="On Net Total",
                            account_head="Freight and Forwarding Charges - J",
                            description="Shipping", tax_amount=40),
        ])

        self.assertEqual(self._compute(invoice), 0.0)


class TestShippingLine(unittest.TestCase):
    def test_paid_delivery_uses_the_native_title(self):
        entry = outbound_sync._build_shipping_line(40, _cfg())

        self.assertEqual(entry["method_id"], "flat_rate")
        self.assertEqual(entry["method_title"], "Delivery")
        self.assertEqual(entry["total"], "40.00")

    def test_waived_delivery_uses_the_native_free_title(self):
        entry = outbound_sync._build_shipping_line(0, _cfg())

        self.assertEqual(entry["method_id"], "flat_rate")
        self.assertEqual(entry["method_title"], "Free Delivery")
        self.assertEqual(entry["total"], "0.00")

    def test_configured_values_still_win(self):
        cfg = _cfg(shipping_method_id="local_pickup", shipping_method_title="Pickup")

        entry = outbound_sync._build_shipping_line(40, cfg)

        self.assertEqual(entry["method_id"], "local_pickup")
        self.assertEqual(entry["method_title"], "Pickup")

    def test_existing_line_is_updated_in_place(self):
        entry = outbound_sync._build_shipping_line(
            40, _cfg(), existing_order={"shipping_lines": [{"id": 12}]}
        )

        self.assertEqual(entry["id"], 12)


# ---------------------------------------------------------------------------
# F-18 — the already-in-sync comparator
# ---------------------------------------------------------------------------

class TestLineItemComparator(unittest.TestCase):
    def test_woo_added_meta_keys_do_not_make_the_order_dirty(self):
        ours = [{
            "product_id": 369, "variation_id": 13780, "quantity": 2,
            "subtotal": "0.00", "total": "0.00",
            "meta_data": [
                {"key": "erpnext_item_code", "value": "JAR-369"},
                {"key": "_woosb_parent_id", "value": "12446"},
            ],
        }]
        theirs = [{
            "id": 0, "product_id": 369, "variation_id": 13780, "quantity": 2,
            "subtotal": "0.00", "total": "0.00",
            "meta_data": [
                {"key": "erpnext_item_code", "value": "JAR-369"},
                {"key": "_woosb_parent_id", "value": "12446"},
                {"key": "_reduced_stock", "value": "2"},
                {"key": "pa_size", "value": "medium"},
            ],
        }]

        self.assertEqual(
            outbound_sync._normalize_order_line_items(ours),
            outbound_sync._normalize_order_line_items(theirs),
        )

    def test_numeric_meta_values_compare_by_meaning(self):
        ours = [{"product_id": 1, "quantity": 1, "subtotal": "1.00", "total": "1.00",
                 "meta_data": [{"key": "discount_percentage", "value": 50.0}]}]
        theirs = [{"product_id": 1, "quantity": 1, "subtotal": "1.00", "total": "1.00",
                   "meta_data": [{"key": "discount_percentage", "value": "50"}]}]

        self.assertEqual(
            outbound_sync._normalize_order_line_items(ours),
            outbound_sync._normalize_order_line_items(theirs),
        )

    def test_a_real_line_change_is_still_detected(self):
        ours = [{"product_id": 369, "quantity": 2, "subtotal": "0.00", "total": "0.00",
                 "meta_data": [{"key": "erpnext_item_code", "value": "JAR-369"}]}]
        theirs = [{"product_id": 369, "quantity": 1, "subtotal": "0.00", "total": "0.00",
                   "meta_data": [{"key": "erpnext_item_code", "value": "JAR-369"}]}]

        self.assertNotEqual(
            outbound_sync._normalize_order_line_items(ours),
            outbound_sync._normalize_order_line_items(theirs),
        )

    def test_only_the_keys_we_own_take_part(self):
        self.assertEqual(
            outbound_sync._ORDER_LINE_META_KEYS_TO_COMPARE,
            frozenset({"erpnext_item_code", "discount_percentage", "_woosb_parent_id", "_woosb_ids"}),
        )

    def test_a_key_we_deliberately_do_not_send_is_not_compared(self):
        """A preserved native ``_woosb_ids`` must not make the order permanently dirty."""
        payload = {
            "status": "processing",
            "line_items": [{
                "id": 5, "product_id": 12446, "quantity": 1,
                "subtotal": "600.00", "total": "600.00",
                "meta_data": [{"key": "erpnext_item_code", "value": "BUNDLE-12446"}],
            }],
        }
        existing = {
            "status": "processing",
            "line_items": [{
                "id": 5, "product_id": 12446, "quantity": 1,
                "subtotal": "600.00", "total": "600.00",
                "meta_data": [
                    {"key": "erpnext_item_code", "value": "BUNDLE-12446"},
                    {"key": "_woosb_ids", "value": '13780/88zq/2/{"attribute_pa_size":"medium"}'},
                ],
            }],
        }

        self.assertFalse(outbound_sync._order_payload_requires_update(existing, payload))


class TestNativeBundleSelectionPreserved(unittest.TestCase):
    def test_a_store_written_selection_string_is_left_alone(self):
        payload_lines = [{
            "id": 5,
            "meta_data": [
                {"key": "erpnext_item_code", "value": "BUNDLE-12446"},
                {"key": "_woosb_ids", "value": "13780/abcd/2/{}"},
            ],
        }]
        existing_order = {"line_items": [{
            "id": 5,
            "meta_data": [
                {"key": "_woosb_ids", "value": '13780/88zq/2/{"attribute_pa_size":"medium"}'},
            ],
        }]}

        outbound_sync._preserve_native_bundle_selection(payload_lines, existing_order)

        keys = {meta["key"] for meta in payload_lines[0]["meta_data"]}
        self.assertEqual(keys, {"erpnext_item_code"})

    def test_our_own_selection_string_survives_when_the_store_has_none(self):
        payload_lines = [{
            "id": 5,
            "meta_data": [{"key": "_woosb_ids", "value": "13780/abcd/2/{}"}],
        }]
        existing_order = {"line_items": [{"id": 5, "meta_data": []}]}

        outbound_sync._preserve_native_bundle_selection(payload_lines, existing_order)

        self.assertEqual(payload_lines[0]["meta_data"][0]["key"], "_woosb_ids")

    def test_a_brand_new_line_is_untouched(self):
        payload_lines = [{"meta_data": [{"key": "_woosb_ids", "value": "13780/abcd/2/{}"}]}]

        outbound_sync._preserve_native_bundle_selection(payload_lines, {"line_items": [{"id": 9}]})

        self.assertEqual(payload_lines[0]["meta_data"][0]["key"], "_woosb_ids")


# ---------------------------------------------------------------------------
# F-15 / F-16 — returns
# ---------------------------------------------------------------------------

class TestReturnStatus(unittest.TestCase):
    def _invoice(self, **fields):
        values = {
            "name": "ACC-SINV-TEST-9001",
            "docstatus": 1,
            "grand_total": 500,
            "custom_sales_invoice_state": "Delivered",
            "sales_invoice_state": "Delivered",
        }
        values.update(fields)
        return SimpleNamespace(**values)

    def test_the_canonical_return_flag_maps_to_refunded(self):
        """``custom_return_status`` is the purpose-built field; it wins."""
        invoice = self._invoice(custom_return_status="Fully Returned")

        with unittest.mock.patch.object(outbound_sync, "_returned_amount") as returned_amount:
            self.assertEqual(outbound_sync._determine_status(invoice), "refunded")

        # Authoritative enough that the accounting fallback is not even consulted.
        returned_amount.assert_not_called()

    def test_a_partially_returned_flag_leaves_the_order_completed(self):
        invoice = self._invoice(custom_return_status="Partially Returned")

        with unittest.mock.patch.object(outbound_sync, "_returned_amount", return_value=120.0):
            self.assertEqual(outbound_sync._determine_status(invoice), "completed")

    def test_a_lagging_flag_is_overruled_by_the_credit_notes(self):
        """The flag is written across two saves, so blank/partial is not proof."""
        invoice = self._invoice(custom_return_status="Partially Returned")

        with unittest.mock.patch.object(outbound_sync, "_returned_amount", return_value=500.0):
            self.assertEqual(outbound_sync._determine_status(invoice), "refunded")

    def test_a_missing_flag_falls_through_to_the_other_signals(self):
        invoice = self._invoice(custom_return_status="")

        with unittest.mock.patch.object(outbound_sync, "_returned_amount", return_value=0.0):
            self.assertEqual(outbound_sync._determine_status(invoice), "completed")

    def test_return_is_full_prefers_the_flag_then_the_sums(self):
        self.assertTrue(
            outbound_sync._return_is_full("Fully Returned", returned_total=0, original_total=500)
        )
        self.assertTrue(
            outbound_sync._return_is_full("Partially Returned", returned_total=500, original_total=500)
        )
        self.assertFalse(
            outbound_sync._return_is_full("Partially Returned", returned_total=120, original_total=500)
        )
        self.assertFalse(outbound_sync._return_is_full(None, returned_total=0, original_total=500))

    def test_returned_state_maps_to_refunded(self):
        invoice = self._invoice(custom_sales_invoice_state="Returned", sales_invoice_state="Returned")

        with unittest.mock.patch.object(outbound_sync, "_returned_amount", return_value=0.0):
            self.assertEqual(outbound_sync._determine_status(invoice), "refunded")

    def test_a_full_credit_note_maps_to_refunded(self):
        invoice = self._invoice()

        with unittest.mock.patch.object(outbound_sync, "_returned_amount", return_value=500.0):
            self.assertEqual(outbound_sync._determine_status(invoice), "refunded")

    def test_a_partial_credit_note_leaves_the_order_completed(self):
        invoice = self._invoice()

        with unittest.mock.patch.object(outbound_sync, "_returned_amount", return_value=120.0):
            self.assertEqual(outbound_sync._determine_status(invoice), "completed")

    def test_cancellation_still_wins_over_a_return(self):
        invoice = self._invoice(docstatus=2)

        with unittest.mock.patch.object(outbound_sync, "_returned_amount", return_value=500.0):
            self.assertEqual(outbound_sync._determine_status(invoice), "cancelled")

    def test_refunded_is_terminal_and_approved_for_push(self):
        self.assertTrue(outbound_sync.is_terminal_woo_status("refunded"))
        self.assertIn("refunded", outbound_sync._APPROVED_INVOICE_OUTBOUND_STATUSES)

    def test_a_credit_note_is_routed_to_the_return_handler_not_dropped(self):
        credit_note = SimpleNamespace(
            name="ACC-SINV-TEST-RET-1",
            is_return=1,
            return_against="ACC-SINV-TEST-9001",
            docstatus=1,
            flags=SimpleNamespace(ignore_woo_outbound=False),
        )
        credit_note.get = lambda field, default=None: getattr(credit_note, field, default)
        enqueued = []

        with unittest.mock.patch.object(
            outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _cfg())
        ), unittest.mock.patch.object(
            outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)
        ), unittest.mock.patch.object(
            outbound_sync.frappe, "enqueue",
            side_effect=lambda *args, **kwargs: enqueued.append((args, kwargs)),
        ):
            outbound_sync.enqueue_invoice_sync(credit_note, method="on_submit")

        self.assertEqual(len(enqueued), 1)
        self.assertIn("sync_invoice_return", enqueued[0][0][0])
        self.assertEqual(enqueued[0][1]["credit_note_name"], "ACC-SINV-TEST-RET-1")

    def test_a_credit_note_is_still_never_pushed_as_an_order(self):
        credit_note = SimpleNamespace(
            name="ACC-SINV-TEST-RET-1",
            is_return=1,
            docstatus=1,
            flags=SimpleNamespace(ignore_woo_outbound=False),
        )
        credit_note.get = lambda field, default=None: getattr(credit_note, field, default)

        with unittest.mock.patch.object(
            outbound_sync, "_get_settings", return_value=(SimpleNamespace(), _cfg())
        ), unittest.mock.patch.object(
            outbound_sync.frappe, "get_doc", return_value=credit_note
        ), unittest.mock.patch.object(
            outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)
        ):
            result = outbound_sync.sync_sales_invoice("ACC-SINV-TEST-RET-1")

        self.assertEqual(result, {"skipped": True, "reason": "credit_note"})


# ---------------------------------------------------------------------------
# F-03 — the total assertion
# ---------------------------------------------------------------------------

class TestTotalAssertion(unittest.TestCase):
    def test_a_dropped_discount_is_caught(self):
        """The live case: invoice ACC-SINV-2026-17758, 670 pushed vs 30 owed."""
        invoice = SimpleNamespace(name="ACC-SINV-2026-17758", grand_total=30)
        payload = {"line_items": [{"total": "640.00"}]}

        detail = outbound_sync._check_response_total(
            {"total": "670.00", "id": 1},
            invoice,
            invoice_name=invoice.name,
            payload=payload,
        )

        self.assertIsNotNone(detail)
        self.assertIn("670.00", detail)
        self.assertIn("30.00", detail)

    def test_a_matching_total_passes(self):
        invoice = SimpleNamespace(name="X", grand_total=670)
        payload = {"line_items": [{"total": "640.00"}]}

        self.assertIsNone(
            outbound_sync._check_response_total(
                {"total": "670.00"}, invoice, invoice_name="X", payload=payload
            )
        )

    def test_rounding_noise_is_tolerated(self):
        invoice = SimpleNamespace(name="X", grand_total=670.02)
        payload = {"line_items": [{"total": "1"}, {"total": "2"}, {"total": "3"}]}

        self.assertIsNone(
            outbound_sync._check_response_total(
                {"total": "670.00"}, invoice, invoice_name="X", payload=payload
            )
        )

    def test_a_status_only_update_is_not_asserted(self):
        """Woo-origin orders are repriced from the ERPNext price list on the way in."""
        invoice = SimpleNamespace(name="X", grand_total=30)

        self.assertIsNone(
            outbound_sync._check_response_total(
                {"total": "670.00"}, invoice, invoice_name="X", payload={"status": "completed"}
            )
        )


# ---------------------------------------------------------------------------
# F-19 — the order's creation date
# ---------------------------------------------------------------------------

class TestCreationDates(unittest.TestCase):
    def test_posting_moment_is_sent_local_and_utc(self):
        invoice = SimpleNamespace(posting_date="2026-08-19", posting_time="14:32:00")

        with unittest.mock.patch("frappe.utils.get_system_timezone", return_value="Africa/Nairobi"):
            dates = outbound_sync._build_order_creation_dates(invoice)

        # Nairobi is a fixed UTC+3 with no DST, so this is exact all year.
        self.assertEqual(dates["date_created"], "2026-08-19T14:32:00")
        self.assertEqual(dates["date_created_gmt"], "2026-08-19T11:32:00")

    def test_utc_site_sends_the_same_instant_twice(self):
        invoice = SimpleNamespace(posting_date="2026-08-19", posting_time="14:32:00")

        with unittest.mock.patch("frappe.utils.get_system_timezone", return_value="UTC"):
            dates = outbound_sync._build_order_creation_dates(invoice)

        self.assertEqual(dates["date_created"], dates["date_created_gmt"])

    def test_no_posting_date_sends_nothing(self):
        self.assertEqual(
            outbound_sync._build_order_creation_dates(SimpleNamespace(posting_date=None)), {}
        )

    def test_an_unresolvable_timezone_sends_local_only(self):
        invoice = SimpleNamespace(posting_date="2026-08-19", posting_time="14:32:00")

        with unittest.mock.patch("frappe.utils.get_system_timezone", side_effect=RuntimeError("no tz")):
            dates = outbound_sync._build_order_creation_dates(invoice)

        self.assertEqual(dates, {"date_created": "2026-08-19T14:32:00"})


# ---------------------------------------------------------------------------
# F-23 / F-25 — one-way set_paid, and the realtime room
# ---------------------------------------------------------------------------

class TestPaidAndRealtime(unittest.TestCase):
    def test_unpayable_transition_is_detected(self):
        invoice = SimpleNamespace(outstanding_amount=250)
        paid_order = {"date_paid": "2026-08-18T12:00:00"}

        self.assertTrue(outbound_sync._detect_unpayable_transition(invoice, paid_order))

    def test_a_settled_invoice_is_not_an_unpayable_transition(self):
        invoice = SimpleNamespace(outstanding_amount=0)
        paid_order = {"date_paid": "2026-08-18T12:00:00"}

        self.assertFalse(outbound_sync._detect_unpayable_transition(invoice, paid_order))

    def test_an_unpaid_store_order_is_not_an_unpayable_transition(self):
        invoice = SimpleNamespace(outstanding_amount=250)

        self.assertFalse(
            outbound_sync._detect_unpayable_transition(invoice, {"date_paid": None})
        )


# ---------------------------------------------------------------------------
# F-25 — the realtime room
# ---------------------------------------------------------------------------

def _realtime_get_all(*, profiles, users):
    """`frappe.get_all` stub for the POS Profile -> POS Profile User lookup."""
    def get_all(doctype, filters=None, fields=None, pluck=None, **kwargs):
        if doctype == "POS Profile":
            wanted = (filters or {}).get("name", ["in", []])[1]
            return [name for name in profiles if name in wanted]
        if doctype == "POS Profile User":
            parents = (filters or {}).get("parent", ["in", []])[1]
            return [
                {"user": user}
                for parent, parent_users in users.items()
                if parent in parents
                for user in parent_users
            ]
        raise AssertionError(f"Unexpected doctype: {doctype}")

    return get_all


class TestBranchScopedRealtime(unittest.TestCase):
    """F-25. Two ways to get this wrong; this call site has been on both.

    ``user="*"`` addresses ``user:*``, a room nobody joins. Omitting ``user``
    falls through to the site-wide ``all`` room, which shows one branch's orders
    to every other branch. The destination is the invoice's POS Profile users.
    """

    def _publish(self, *, pos_profile, profiles, users, publish=None):
        calls = []
        publish = publish or (lambda *args, **kwargs: calls.append((args, kwargs)))

        with unittest.mock.patch.object(
            outbound_sync.frappe, "get_all",
            side_effect=_realtime_get_all(profiles=profiles, users=users),
        ), unittest.mock.patch.object(
            outbound_sync.frappe, "publish_realtime", side_effect=publish
        ):
            outbound_sync._publish_woo_order_assigned(
                "ACC-SINV-TEST-9001", 16895,
                invoice=SimpleNamespace(pos_profile=pos_profile),
            )
        return calls

    def test_event_goes_to_each_user_of_the_invoices_pos_profile(self):
        calls = self._publish(
            pos_profile="Nasr City",
            profiles=["Nasr City", "Maadi"],
            users={"Nasr City": ["rider@jarz.com", "manager@jarz.com"], "Maadi": ["other@jarz.com"]},
        )

        self.assertEqual(len(calls), 2)
        self.assertEqual(
            sorted(kwargs["user"] for _args, kwargs in calls),
            ["manager@jarz.com", "rider@jarz.com"],
        )
        for args, kwargs in calls:
            self.assertEqual(args[0], "kanban_update")
            self.assertEqual(args[1]["pos_profile"], "Nasr City")
            self.assertEqual(args[1]["woo_order_id"], 16895)
            # Neither failure mode: not the dead wildcard, not the site room.
            self.assertNotEqual(kwargs["user"], "*")

    def test_another_branchs_users_are_never_addressed(self):
        calls = self._publish(
            pos_profile="Nasr City",
            profiles=["Nasr City", "Maadi"],
            users={"Nasr City": ["rider@jarz.com"], "Maadi": ["other@jarz.com"]},
        )

        self.assertEqual([kwargs["user"] for _args, kwargs in calls], ["rider@jarz.com"])

    def test_a_disabled_profile_receives_nothing(self):
        """Its users can no longer load the order the event refers to."""
        calls = self._publish(
            pos_profile="Closed Branch",
            profiles=[],  # the `disabled = 0` filter matched nothing
            users={"Closed Branch": ["rider@jarz.com"]},
        )

        self.assertEqual(calls, [])

    def test_no_recipients_drops_the_event_instead_of_broadcasting(self):
        calls = self._publish(
            pos_profile="Nasr City", profiles=["Nasr City"], users={"Nasr City": []}
        )

        # The site-wide fallback is the leak, so there must be no call at all.
        self.assertEqual(calls, [])

    def test_an_invoice_with_no_pos_profile_drops_the_event(self):
        calls = self._publish(pos_profile=None, profiles=["Nasr City"], users={"Nasr City": ["r@j.com"]})

        self.assertEqual(calls, [])

    def test_guest_is_never_a_recipient_and_users_are_deduplicated(self):
        calls = self._publish(
            pos_profile="Nasr City",
            profiles=["Nasr City"],
            users={"Nasr City": ["Guest", "rider@jarz.com", "rider@jarz.com", ""]},
        )

        self.assertEqual([kwargs["user"] for _args, kwargs in calls], ["rider@jarz.com"])

    def test_one_failing_recipient_does_not_silence_the_others(self):
        delivered = []

        def publish(*args, **kwargs):
            if kwargs.get("user") == "broken@jarz.com":
                raise RuntimeError("socket gone")
            delivered.append(kwargs["user"])

        self._publish(
            pos_profile="Nasr City",
            profiles=["Nasr City"],
            users={"Nasr City": ["broken@jarz.com", "rider@jarz.com"]},
            publish=publish,
        )

        self.assertEqual(delivered, ["rider@jarz.com"])

    def test_a_lookup_failure_drops_the_event_rather_than_broadcasting(self):
        calls = []

        with unittest.mock.patch.object(
            outbound_sync.frappe, "get_all", side_effect=RuntimeError("db down")
        ), unittest.mock.patch.object(
            outbound_sync.frappe, "publish_realtime",
            side_effect=lambda *args, **kwargs: calls.append((args, kwargs)),
        ):
            outbound_sync._publish_woo_order_assigned(
                "ACC-SINV-TEST-9001", 16895,
                invoice=SimpleNamespace(pos_profile="Nasr City"),
            )

        self.assertEqual(calls, [])


# ---------------------------------------------------------------------------
# F-22 — address state realignment runs without a push
# ---------------------------------------------------------------------------

class TestAddressRealignment(unittest.TestCase):
    def _address(self, **fields):
        values = {"name": "ADDR-001", "city": "Nasr City", "state": "Stale Zone",
                  "flags": SimpleNamespace(ignore_woo_outbound=False)}
        values.update(fields)
        address = SimpleNamespace(**values)
        address.get = lambda field, default=None: getattr(address, field, default)
        address.get_doc_before_save = lambda: None
        address.has_value_changed = lambda field: True
        return address

    def test_hook_realigns_on_insert(self):
        address = self._address()

        with unittest.mock.patch.object(
            outbound_sync, "_realign_address_state_with_territory", return_value="Nasr City - مدينة نصر"
        ) as realign, unittest.mock.patch.object(
            outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)
        ):
            outbound_sync.realign_address_state(address, "after_insert")

        realign.assert_called_once_with("ADDR-001", "Nasr City", "Stale Zone")
        self.assertEqual(address.state, "Nasr City - مدينة نصر")

    def test_hook_stands_down_during_inbound_sync(self):
        address = self._address()
        address.flags.ignore_woo_outbound = True

        with unittest.mock.patch.object(
            outbound_sync, "_realign_address_state_with_territory"
        ) as realign:
            outbound_sync.realign_address_state(address, "on_update")

        realign.assert_not_called()

    def test_hook_skips_an_update_that_touched_neither_city_nor_state(self):
        address = self._address()
        address.has_value_changed = lambda field: False

        with unittest.mock.patch.object(
            outbound_sync, "_realign_address_state_with_territory"
        ) as realign, unittest.mock.patch.object(
            outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)
        ):
            outbound_sync.realign_address_state(address, "on_update")

        realign.assert_not_called()

    def test_hook_never_raises(self):
        address = self._address()

        with unittest.mock.patch.object(
            outbound_sync, "_realign_address_state_with_territory", side_effect=RuntimeError("boom")
        ), unittest.mock.patch.object(
            outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)
        ):
            outbound_sync.realign_address_state(address, "on_update")

    def test_dry_run_keeps_the_payload_builder_write_free(self):
        writes = []

        with unittest.mock.patch.object(
            outbound_sync, "_resolve_leaf_territory", return_value="Nasr City"
        ), unittest.mock.patch.object(
            outbound_sync, "_territory_woo_zone_label", return_value="Nasr City - مدينة نصر"
        ), unittest.mock.patch.object(
            outbound_sync.frappe, "db",
            SimpleNamespace(set_value=lambda *a, **k: writes.append((a, k))),
        ), unittest.mock.patch.object(
            outbound_sync.frappe, "flags", SimpleNamespace(woo_payload_dry_run=True)
        ):
            state = outbound_sync._realign_address_state_with_territory(
                "ADDR-001", "Nasr City", "Stale Zone"
            )

        self.assertEqual(state, "Nasr City - مدينة نصر")
        self.assertEqual(writes, [])


# ---------------------------------------------------------------------------
# End to end: the payload arithmetic the store will see
# ---------------------------------------------------------------------------

def _build_payload(invoice, *, line_items, shipping_total, cfg=None, existing_order=None):
    customer = SimpleNamespace(
        customer_name="Test Customer",
        woo_customer_id="88",
        email_id="test@example.com",
        mobile_no="01000000000",
        phone=None,
    )
    with unittest.mock.patch.object(
        outbound_sync, "_collect_line_items", return_value=(line_items, [])
    ), unittest.mock.patch.object(
        outbound_sync, "_compute_shipping_total", return_value=shipping_total
    ), unittest.mock.patch.object(
        outbound_sync, "_returned_amount", return_value=0.0
    ), unittest.mock.patch.object(
        outbound_sync, "_build_customer_payload", return_value={
            "billing": {"address_1": "Street 1", "email": "t@example.com", "phone": "0100"},
            "shipping": {"address_1": "Street 1", "email": "t@example.com", "phone": "0100"},
        }
    ), unittest.mock.patch.object(outbound_sync.frappe, "get_doc", return_value=customer):
        return outbound_sync._build_order_payload(
            invoice, cfg or _cfg(), existing_order=existing_order
        )


class TestPayloadArithmetic(unittest.TestCase):
    def test_discounted_order_totals_to_grand_total(self):
        """ACC-SINV-2026-17758: 640 of lines, 30 of shipping, 640 discount, 30 owed."""
        invoice = _invoice(
            [],
            grand_total=30,
            discount_amount=640,
            apply_discount_on="Grand Total",
            outstanding_amount=30,
            custom_sales_invoice_state="Recieved",
        )
        line_items = [{"product_id": 1, "quantity": 1, "subtotal": "640.00", "total": "640.00",
                       "meta_data": [{"key": "erpnext_item_code", "value": "JAR-369"}]}]

        payload = _build_payload(invoice, line_items=line_items, shipping_total=30)

        lines = sum(outbound_sync.flt(entry["total"]) for entry in payload["line_items"])
        shipping = sum(outbound_sync.flt(entry["total"]) for entry in payload["shipping_lines"])
        fees = sum(outbound_sync.flt(entry["total"]) for entry in payload.get("fee_lines", []))

        self.assertEqual(round(lines + shipping + fees, 2), 30.0)
        self.assertEqual(payload["fee_lines"][0]["total"], "-640.00")

    def test_undiscounted_order_totals_to_grand_total(self):
        invoice = _invoice(
            [],
            grand_total=670,
            outstanding_amount=670,
            custom_sales_invoice_state="Recieved",
        )
        line_items = [{"product_id": 1, "quantity": 1, "subtotal": "640.00", "total": "640.00",
                       "meta_data": [{"key": "erpnext_item_code", "value": "JAR-369"}]}]

        payload = _build_payload(invoice, line_items=line_items, shipping_total=30)

        lines = sum(outbound_sync.flt(entry["total"]) for entry in payload["line_items"])
        shipping = sum(outbound_sync.flt(entry["total"]) for entry in payload["shipping_lines"])

        self.assertNotIn("fee_lines", payload)
        self.assertEqual(round(lines + shipping, 2), 670.0)

    def test_creation_dates_ride_the_create_but_not_the_update(self):
        invoice = _invoice(
            [],
            grand_total=100,
            outstanding_amount=100,
            posting_date="2026-08-19",
            posting_time="14:32:00",
            custom_sales_invoice_state="Recieved",
        )
        line_items = [{"product_id": 1, "quantity": 1, "subtotal": "100.00", "total": "100.00",
                       "meta_data": [{"key": "erpnext_item_code", "value": "JAR-369"}]}]

        created = _build_payload(invoice, line_items=line_items, shipping_total=0)
        updated = _build_payload(
            invoice, line_items=line_items, shipping_total=0,
            existing_order={"id": 16895, "line_items": []},
        )

        self.assertEqual(created["date_created"], "2026-08-19T14:32:00")
        self.assertNotIn("date_created", updated)

    def test_payment_method_reaches_the_payload_through_the_shared_map(self):
        invoice = _invoice(
            [],
            grand_total=100,
            outstanding_amount=0,
            custom_payment_method="Kashier Card",
            custom_sales_invoice_state="Recieved",
        )
        line_items = [{"product_id": 1, "quantity": 1, "subtotal": "100.00", "total": "100.00",
                       "meta_data": [{"key": "erpnext_item_code", "value": "JAR-369"}]}]

        payload = _build_payload(invoice, line_items=line_items, shipping_total=0)

        self.assertEqual(payload["payment_method"], "kashier_card")
        self.assertEqual(payload["payment_method_title"], "Kashier Card")


class TestDeliveryDetailsNote(unittest.TestCase):
    """The ORDDD plugin records the slot in THREE places; we only wrote two.

    The third is a private order note, and it is the line staff read in the order
    screen. Production order 16898 (pushed) had no delivery line while native
    16897 alongside it did. These pin the plugin's own wording byte-for-byte,
    taken from real orders 16895/16896/16897, including the single space after
    the second ``<br>`` and the fact that the note is 12-hour while the meta keys
    are 24-hour.
    """

    def _inv(self, date_str, time_from, duration_seconds):
        return _invoice(
            [],
            custom_delivery_date=date_str,
            custom_delivery_time_from=time_from,
            custom_delivery_duration=duration_seconds,
        )

    def test_note_matches_the_plugins_own_wording(self):
        # Order 16897, verbatim.
        self.assertEqual(
            outbound_sync._build_delivery_details_note(
                self._inv("2026-08-19", "13:00:00", 5400)
            ),
            "Delivery details: <br><strong>Delivery Date</strong>: 19 August, 2026"
            "<br> <strong>Time Slot</strong>: 01:00 PM - 02:30 PM",
        )

    def test_midnight_slot_renders_12_am_not_00_am(self):
        # Orders 16895 and 16896, verbatim. `%I` must give 12, never 00.
        self.assertEqual(
            outbound_sync._build_delivery_details_note(
                self._inv("2026-08-19", "00:00:00", 3600)
            ),
            "Delivery details: <br><strong>Delivery Date</strong>: 19 August, 2026"
            "<br> <strong>Time Slot</strong>: 12:00 AM - 01:00 AM",
        )

    def test_slot_crossing_midnight_wraps_the_end_time(self):
        note = outbound_sync._build_delivery_details_note(
            self._inv("2026-08-19", "23:00:00", 5400)
        )
        self.assertIn("11:00 PM - 12:30 AM", note)
        self.assertNotIn("24:", note)

    def test_no_delivery_date_yields_no_note(self):
        self.assertEqual(outbound_sync._build_delivery_details_note(_invoice([])), "")

    def test_date_without_a_slot_still_gets_the_date_line(self):
        note = outbound_sync._build_delivery_details_note(
            _invoice([], custom_delivery_date="2026-08-19")
        )
        self.assertEqual(
            note,
            "Delivery details: <br><strong>Delivery Date</strong>: 19 August, 2026",
        )
        self.assertNotIn("Time Slot", note)

    def test_note_and_meta_come_from_one_source_and_agree(self):
        """12-hour in the note, 24-hour in the meta — same underlying window.

        Both read `_resolve_delivery_window`, so this fails the moment the two
        formats are computed independently again and start to drift.
        """
        inv = self._inv("2026-08-19", "13:00:00", 5400)
        meta = {row["key"]: row["value"] for row in outbound_sync._build_delivery_metadata(inv)}
        note = outbound_sync._build_delivery_details_note(inv)

        self.assertEqual(meta["Time Slot"], "13:00 - 14:30")
        self.assertEqual(meta["_orddd_time_slot"], "13:00 - 14:30")
        self.assertIn("01:00 PM - 02:30 PM", note)
        # The date is spelled identically in both.
        self.assertEqual(meta["Delivery Date"], "19 August, 2026")
        self.assertIn(meta["Delivery Date"], note)


if __name__ == "__main__":
    unittest.main()
