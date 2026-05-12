from types import SimpleNamespace

from jarz_woocommerce_integration.services import bundle_processing, order_sync


class DummyTerritoryCache:
    def __init__(self, territories=None):
        self._territories = territories or {}

    def get_territory_data(self, territory_name):
        return self._territories.get(territory_name, {})


class DummyBundleCache:
    def __init__(self, bundle_code, free_shipping, resolve_map=None, item_groups=None):
        self._bundle_code = bundle_code
        self._free_shipping = free_shipping
        self._resolve_map = {str(key): value for key, value in (resolve_map or {}).items()}
        self.item_groups = dict(item_groups or {})

    def get_bundle_code(self, product_id):
        return self._bundle_code if str(product_id) == "123" else None

    def bundle_has_free_shipping(self, bundle_code):
        return bundle_code == self._bundle_code and self._free_shipping

    def resolve_item(self, sku, product_id, variation_id=None):
        if sku:
            item_code = self._resolve_map.get(str(sku))
            if item_code:
                return item_code

        if variation_id not in (None, "", 0, "0"):
            item_code = self._resolve_map.get(str(variation_id))
            if item_code:
                return item_code

        if product_id not in (None, "", 0, "0"):
            return self._resolve_map.get(str(product_id))

        return None


class DummyInvoice:
    def __init__(self, taxes=None, items=None, remarks=""):
        self.company = "_Test Company"
        self.docstatus = 0
        self.doctype = "Sales Invoice"
        self.name = "ACC-SINV-TEST"
        self._taxes = list(taxes or [])
        self._items = list(items or [])
        self.remarks = remarks
        self.calculate_calls = 0

    def get(self, fieldname, default=None):
        if fieldname == "taxes":
            return self._taxes
        if fieldname == "items":
            return self._items
        return default

    def set(self, fieldname, value):
        if fieldname == "taxes":
            self._taxes = list(value)

    def append(self, fieldname, value):
        if fieldname == "taxes":
            self._taxes.append(dict(value))

    def calculate_taxes_and_totals(self):
        self.calculate_calls += 1


def _mock_delivery_rule(minimum_threshold=999.0, channels=None):
    return SimpleNamespace(
        rule_name="Free Delivery >= 999 EGP",
        rule_type="Free Delivery",
        company=None,
        territory=None,
        customer_group=None,
        pos_profile=None,
        threshold_basis="Merchandise Subtotal",
        minimum_threshold=minimum_threshold,
        maximum_threshold=None,
        minimum_item_qty=None,
        active_from=None,
        active_to=None,
        apply_to_shipping_income=1,
        apply_to_legacy_delivery_charges=1,
        channels=[SimpleNamespace(channel=channel) for channel in (channels or [])],
    )


def _fake_frappe_for_delivery_rule(rule, customer_group="Retail"):
    def fake_exists(doctype, name=None):
        return doctype == "DocType" and name == "Jarz Promotion Rule"

    def fake_get_value(doctype, name_or_filters, fieldname=None):
        if doctype == "Customer" and fieldname == "customer_group":
            return customer_group
        return None

    return SimpleNamespace(
        db=SimpleNamespace(exists=fake_exists, get_value=fake_get_value),
        get_all=lambda *args, **kwargs: ["PROMO-0001"],
        get_doc=lambda doctype, name=None: rule,
        utils=SimpleNamespace(now_datetime=lambda: order_sync.datetime(2026, 5, 13, 12, 0, 0)),
        logger=lambda: SimpleNamespace(info=lambda *args, **kwargs: None),
    )


def test_resolve_delivery_charge_policy_uses_territory_only():
    cache = DummyTerritoryCache({"EG6OCT": {"delivery_income": 60}})

    decision = order_sync._resolve_delivery_charge_policy(
        "EG6OCT",
        has_free_shipping_bundle=False,
        cache=cache,
    )

    assert decision == {
        "amount": 60.0,
        "description": "Shipping Income (EG6OCT)",
        "reason": "territory_delivery_income",
    }


def test_resolve_delivery_charge_policy_honors_free_shipping_bundle():
    cache = DummyTerritoryCache({"EG6OCT": {"delivery_income": 60}})

    decision = order_sync._resolve_delivery_charge_policy(
        "EG6OCT",
        has_free_shipping_bundle=True,
        cache=cache,
    )

    assert decision == {
        "amount": 0.0,
        "description": None,
        "reason": "free_shipping_bundle",
    }


def test_apply_delivery_charge_policy_replaces_existing_shipping_with_territory(monkeypatch):
    monkeypatch.setattr(order_sync, "_get_shipping_income_account", lambda company: "Freight - TEST")
    invoice = DummyInvoice(
        taxes=[
            {"charge_type": "Actual", "description": "Shipping Income (WooCommerce)", "tax_amount": 45},
            {"charge_type": "Actual", "description": "VAT", "tax_amount": 14},
        ]
    )
    cache = DummyTerritoryCache({"EG6OCT": {"delivery_income": 60}})

    decision = order_sync._apply_delivery_charge_policy(
        invoice,
        territory_name="EG6OCT",
        has_free_shipping_bundle=False,
        cache=cache,
    )

    assert decision["changed"] is True
    assert decision["after_rows"] == [
        {"description": "Shipping Income (EG6OCT)", "tax_amount": 60.0}
    ]
    assert invoice.calculate_calls == 1
    assert invoice.get("taxes") == [
        {"charge_type": "Actual", "description": "VAT", "tax_amount": 14},
        {
            "charge_type": "Actual",
            "description": "Shipping Income (EG6OCT)",
            "tax_amount": 60.0,
            "account_head": "Freight - TEST",
        },
    ]


def test_apply_delivery_charge_policy_removes_shipping_for_free_bundle(monkeypatch):
    monkeypatch.setattr(order_sync, "_get_shipping_income_account", lambda company: "Freight - TEST")
    invoice = DummyInvoice(
        taxes=[
            {"charge_type": "Actual", "description": "Shipping Income (EG6OCT)", "tax_amount": 60},
            {"charge_type": "Actual", "description": "VAT", "tax_amount": 14},
        ]
    )
    cache = DummyTerritoryCache({"EG6OCT": {"delivery_income": 60}})

    decision = order_sync._apply_delivery_charge_policy(
        invoice,
        territory_name="EG6OCT",
        has_free_shipping_bundle=True,
        cache=cache,
    )

    assert decision["changed"] is True
    assert decision["after_rows"] == []
    assert invoice.calculate_calls == 1
    assert invoice.get("taxes") == [
        {"charge_type": "Actual", "description": "VAT", "tax_amount": 14}
    ]


def test_apply_delivery_charge_policy_honors_delivery_promotion(monkeypatch):
    monkeypatch.setattr(order_sync, "_get_shipping_income_account", lambda company: "Freight - TEST")
    monkeypatch.setattr(order_sync, "frappe", _fake_frappe_for_delivery_rule(_mock_delivery_rule()))

    invoice = DummyInvoice(
        taxes=[
            {"charge_type": "Actual", "description": "Shipping Income (EG6OCT)", "tax_amount": 60},
            {"charge_type": "Actual", "description": "VAT", "tax_amount": 14},
        ],
        items=[
            {"item_code": "ITEM-1", "qty": 2, "price_list_rate": 500},
        ],
    )
    cache = DummyTerritoryCache({"EG6OCT": {"delivery_income": 60}})

    decision = order_sync._apply_delivery_charge_policy(
        invoice,
        territory_name="EG6OCT",
        has_free_shipping_bundle=False,
        cache=cache,
        customer_name="CUST-001",
        pos_profile_name="Main POS",
        channel="woo",
    )

    assert decision["reason"] == "delivery_promotion"
    assert decision["promotion_rule_name"] == "Free Delivery >= 999 EGP"
    assert decision["after_rows"] == []
    assert invoice.get("taxes") == [
        {"charge_type": "Actual", "description": "VAT", "tax_amount": 14}
    ]
    assert "[DELIVERY PROMO] Free Delivery >= 999 EGP | merchandise_subtotal=1000.00" in invoice.remarks


def test_apply_delivery_charge_policy_ignores_non_woo_channel_restriction(monkeypatch):
    monkeypatch.setattr(order_sync, "_get_shipping_income_account", lambda company: "Freight - TEST")
    monkeypatch.setattr(
        order_sync,
        "frappe",
        _fake_frappe_for_delivery_rule(_mock_delivery_rule(channels=["flutter"])),
    )

    invoice = DummyInvoice(
        taxes=[
            {"charge_type": "Actual", "description": "VAT", "tax_amount": 14},
        ],
        items=[
            {"item_code": "ITEM-1", "qty": 2, "price_list_rate": 500},
        ],
    )
    cache = DummyTerritoryCache({"EG6OCT": {"delivery_income": 60}})

    decision = order_sync._apply_delivery_charge_policy(
        invoice,
        territory_name="EG6OCT",
        has_free_shipping_bundle=False,
        cache=cache,
        customer_name="CUST-001",
        pos_profile_name="Main POS",
        channel="woo",
    )

    assert decision["reason"] == "territory_delivery_income"
    assert decision["after_rows"] == [
        {"description": "Shipping Income (EG6OCT)", "tax_amount": 60.0}
    ]
    assert invoice.get("taxes") == [
        {"charge_type": "Actual", "description": "VAT", "tax_amount": 14},
        {
            "charge_type": "Actual",
            "description": "Shipping Income (EG6OCT)",
            "tax_amount": 60.0,
            "account_head": "Freight - TEST",
        },
    ]
    assert invoice.remarks == ""


def test_woo_order_has_free_shipping_when_zero_total_and_free_shipping_method():
    assert order_sync._woo_order_has_free_shipping(
        {
            "shipping_total": "0.00",
            "shipping_lines": [
                {"method_id": "free_shipping", "method_title": "Free Delivery", "total": "0.00"}
            ],
        }
    ) is True


def test_woo_order_has_free_shipping_rejects_positive_shipping_total():
    assert order_sync._woo_order_has_free_shipping(
        {
            "shipping_total": "50.00",
            "shipping_lines": [
                {"method_id": "flat_rate", "method_title": "Shipping", "total": "50.00"}
            ],
        }
    ) is False


def test_build_invoice_items_tracks_free_shipping_bundle_metadata(monkeypatch):
    class DummyBundleProcessor:
        def __init__(self, bundle_code, qty, selected_items=None):
            self.bundle_code = bundle_code
            self.qty = qty
            self.selected_items = selected_items

        def load_bundle(self):
            return None

        def get_invoice_items(self):
            return [
                {
                    "item_code": "BUNDLE-PARENT",
                    "qty": 1,
                    "rate": 0,
                    "price_list_rate": 100,
                    "discount_percentage": 100,
                    "is_bundle_parent": True,
                }
            ]

    monkeypatch.setattr(bundle_processing, "BundleProcessor", DummyBundleProcessor)
    cache = DummyBundleCache(bundle_code="BUNDLE-001", free_shipping=True)
    order = {
        "line_items": [
            {
                "name": "Bundle Parent",
                "product_id": 123,
                "variation_id": 0,
                "quantity": 1,
                "sku": "",
                "meta_data": [],
            }
        ]
    }

    items, missing, bundle_context = order_sync._build_invoice_items(order, cache=cache)

    assert missing == []
    assert items[0]["item_code"] == "BUNDLE-PARENT"
    assert bundle_context == {
        "bundle_codes": ["BUNDLE-001"],
        "free_shipping_bundle_codes": ["BUNDLE-001"],
        "has_free_shipping_bundle": True,
    }


def test_build_invoice_items_uses_parent_woosb_ids_per_bundle_instance(monkeypatch):
    captured_selected_items = []

    class DummyBundleProcessor:
        def __init__(self, bundle_code, qty, selected_items=None):
            self.bundle_code = bundle_code
            self.qty = qty
            self.selected_items = selected_items

        def load_bundle(self):
            captured_selected_items.append(self.selected_items)

        def get_invoice_items(self):
            return [
                {
                    "item_code": "BUNDLE-PARENT",
                    "qty": 1,
                    "rate": 0,
                    "price_list_rate": 100,
                    "discount_percentage": 100,
                    "is_bundle_parent": True,
                }
            ]

    monkeypatch.setattr(bundle_processing, "BundleProcessor", DummyBundleProcessor)
    cache = DummyBundleCache(
        bundle_code="BUNDLE-001",
        free_shipping=False,
        resolve_map={
            "13780": "BLUEBERRY-MEDIUM",
            "13783": "CHOCO-HAZELNUT-MEDIUM",
            "13802": "MOLTEN-MEDIUM",
            "13777": "STRAWBERRY-MEDIUM",
            "13773": "REDVELVET-MEDIUM",
            "13826": "PISTACHIO-MEDIUM",
            "13806": "TIRAMISU-MEDIUM",
        },
        item_groups={
            "BLUEBERRY-MEDIUM": "Medium",
            "CHOCO-HAZELNUT-MEDIUM": "Medium",
            "MOLTEN-MEDIUM": "Medium",
            "STRAWBERRY-MEDIUM": "Medium",
            "REDVELVET-MEDIUM": "Medium",
            "PISTACHIO-MEDIUM": "Medium",
            "TIRAMISU-MEDIUM": "Medium",
        },
    )
    order = {
        "line_items": [
            {
                "id": 48380,
                "name": "Jarz Sweet Six",
                "product_id": 123,
                "variation_id": 0,
                "quantity": 1,
                "sku": "",
                "meta_data": [
                    {
                        "key": "_woosb_ids",
                        "value": '13780/88zq/4/{"attribute_pa_size":"medium"},13783/6mtj/1/{"attribute_pa_size":"medium"},13802/ibpt/1/{"attribute_pa_size":"medium"}',
                    }
                ],
            },
            {
                "id": 48381,
                "name": "Blueberry",
                "product_id": 369,
                "variation_id": 13780,
                "quantity": 4,
                "sku": "",
                "meta_data": [{"key": "_woosb_parent_id", "value": "123"}],
            },
            {
                "id": 48382,
                "name": "Chocolate Hazelnut",
                "product_id": 367,
                "variation_id": 13783,
                "quantity": 1,
                "sku": "",
                "meta_data": [{"key": "_woosb_parent_id", "value": "123"}],
            },
            {
                "id": 48383,
                "name": "Molten",
                "product_id": 11162,
                "variation_id": 13802,
                "quantity": 1,
                "sku": "",
                "meta_data": [{"key": "_woosb_parent_id", "value": "123"}],
            },
            {
                "id": 48384,
                "name": "Jarz Sweet Six",
                "product_id": 123,
                "variation_id": 0,
                "quantity": 1,
                "sku": "",
                "meta_data": [
                    {
                        "key": "_woosb_ids",
                        "value": '13777/anox/2/{"attribute_pa_size":"medium"},13773/3sh1/1/{"attribute_pa_size":"medium"},13826/g497/1/{"attribute_pa_size":"medium"},13806/ch6o/2/{"attribute_pa_size":"medium"}',
                    }
                ],
            },
            {
                "id": 48385,
                "name": "Strawberry",
                "product_id": 371,
                "variation_id": 13777,
                "quantity": 2,
                "sku": "",
                "meta_data": [{"key": "_woosb_parent_id", "value": "123"}],
            },
            {
                "id": 48386,
                "name": "Redvelvet",
                "product_id": 2251,
                "variation_id": 13773,
                "quantity": 1,
                "sku": "",
                "meta_data": [{"key": "_woosb_parent_id", "value": "123"}],
            },
            {
                "id": 48387,
                "name": "Pistachio",
                "product_id": 2286,
                "variation_id": 13826,
                "quantity": 1,
                "sku": "",
                "meta_data": [{"key": "_woosb_parent_id", "value": "123"}],
            },
            {
                "id": 48388,
                "name": "Tiramisu",
                "product_id": 11140,
                "variation_id": 13806,
                "quantity": 2,
                "sku": "",
                "meta_data": [{"key": "_woosb_parent_id", "value": "123"}],
            },
        ]
    }

    items, missing, bundle_context = order_sync._build_invoice_items(order, cache=cache)

    assert missing == []
    assert len(items) == 2
    assert bundle_context == {
        "bundle_codes": ["BUNDLE-001"],
        "free_shipping_bundle_codes": [],
        "has_free_shipping_bundle": False,
    }
    assert captured_selected_items == [
        {
            "Medium": [
                {"item_code": "BLUEBERRY-MEDIUM", "selected_qty": 4},
                {"item_code": "CHOCO-HAZELNUT-MEDIUM", "selected_qty": 1},
                {"item_code": "MOLTEN-MEDIUM", "selected_qty": 1},
            ]
        },
        {
            "Medium": [
                {"item_code": "STRAWBERRY-MEDIUM", "selected_qty": 2},
                {"item_code": "REDVELVET-MEDIUM", "selected_qty": 1},
                {"item_code": "PISTACHIO-MEDIUM", "selected_qty": 1},
                {"item_code": "TIRAMISU-MEDIUM", "selected_qty": 2},
            ]
        },
    ]


def test_build_invoice_items_does_not_retry_default_bundle_when_explicit_selection_fails(monkeypatch):
    constructor_calls = []

    class DummyBundleProcessor:
        def __init__(self, bundle_code, qty, selected_items=None):
            constructor_calls.append(selected_items)
            self.selected_items = selected_items

        def load_bundle(self):
            raise ValueError("selection validation failed")

        def get_invoice_items(self):
            return []

    monkeypatch.setattr(bundle_processing, "BundleProcessor", DummyBundleProcessor)
    cache = DummyBundleCache(
        bundle_code="BUNDLE-001",
        free_shipping=False,
        resolve_map={"13780": "BLUEBERRY-MEDIUM"},
        item_groups={"BLUEBERRY-MEDIUM": "Medium"},
    )
    order = {
        "line_items": [
            {
                "id": 48380,
                "name": "Jarz Sweet Six",
                "product_id": 123,
                "variation_id": 0,
                "quantity": 1,
                "sku": "",
                "meta_data": [
                    {
                        "key": "_woosb_ids",
                        "value": '13780/88zq/1/{"attribute_pa_size":"medium"}',
                    }
                ],
            }
        ]
    }

    items, missing, bundle_context = order_sync._build_invoice_items(order, cache=cache)

    assert items == []
    assert missing == [
        {"name": "Jarz Sweet Six", "sku": "", "product_id": 123, "reason": "bundle_error"}
    ]
    assert bundle_context == {
        "bundle_codes": [],
        "free_shipping_bundle_codes": [],
        "has_free_shipping_bundle": False,
    }
    assert constructor_calls == [
        {"Medium": [{"item_code": "BLUEBERRY-MEDIUM", "selected_qty": 1}]}
    ]


def test_enqueue_delivery_charge_repost_uses_delete_cancelled_entries(monkeypatch):
    captured = {}

    class DummyRepostDoc:
        def insert(self, ignore_permissions=False):
            captured["insert_ignore_permissions"] = ignore_permissions

        def submit(self):
            captured["submitted"] = True

    def fake_get_doc(payload):
        captured["payload"] = payload
        return DummyRepostDoc()

    monkeypatch.setattr(order_sync.frappe, "get_doc", fake_get_doc)

    invoice = SimpleNamespace(
        docstatus=1,
        company="Jarz",
        doctype="Sales Invoice",
        name="ACC-SINV-TEST-001",
    )

    order_sync._enqueue_delivery_charge_repost(invoice)

    assert captured["payload"] == {
        "doctype": "Repost Accounting Ledger",
        "company": "Jarz",
        "delete_cancelled_entries": 1,
        "vouchers": [
            {"voucher_type": "Sales Invoice", "voucher_no": "ACC-SINV-TEST-001"}
        ],
    }
    assert captured["insert_ignore_permissions"] is True
    assert captured["submitted"] is True