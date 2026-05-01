from types import SimpleNamespace

from jarz_woocommerce_integration.services import bundle_processing, order_sync


class DummyTerritoryCache:
    def __init__(self, territories=None):
        self._territories = territories or {}

    def get_territory_data(self, territory_name):
        return self._territories.get(territory_name, {})


class DummyBundleCache:
    item_groups = {}

    def __init__(self, bundle_code, free_shipping):
        self._bundle_code = bundle_code
        self._free_shipping = free_shipping

    def get_bundle_code(self, product_id):
        return self._bundle_code if str(product_id) == "123" else None

    def bundle_has_free_shipping(self, bundle_code):
        return bundle_code == self._bundle_code and self._free_shipping


class DummyInvoice:
    def __init__(self, taxes=None):
        self.company = "_Test Company"
        self.docstatus = 0
        self.doctype = "Sales Invoice"
        self.name = "ACC-SINV-TEST"
        self._taxes = list(taxes or [])
        self.calculate_calls = 0

    def get(self, fieldname, default=None):
        if fieldname == "taxes":
            return self._taxes
        return default

    def set(self, fieldname, value):
        if fieldname == "taxes":
            self._taxes = list(value)

    def append(self, fieldname, value):
        if fieldname == "taxes":
            self._taxes.append(dict(value))

    def calculate_taxes_and_totals(self):
        self.calculate_calls += 1


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