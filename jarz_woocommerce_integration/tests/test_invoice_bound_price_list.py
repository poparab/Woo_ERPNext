"""
Inbound Woo pricing must follow the invoice's own price list
============================================================

Regression suite for Woo order 17756 (2026-10-01). A POS-created B2B order for
"Cloud nine specialty coffee" was sold on the "B2B Selling" price list (Medium
jar 77 EGP) and mirrored to Woo. The operator amended it in the POS
(ACC-SINV-2026-18624-1, 4 x 77 = 308). Woo fired ``order.updated`` with the same
four lines at 77.00, but inbound priced them from the branch POS Profile's
default list, "Standard Selling" (120). The line signature includes rate and
price_list_rate, so the item-edit gate read the price gap as a customer edit,
enqueued a Woo amendment, and the replacement ACC-SINV-2026-18624-2 billed the
shop 540.

``_process_order_phase1`` now prices an already-invoiced order from that
invoice's ``selling_price_list`` (``_resolve_invoice_bound_price_list``):

(a) linked submitted invoice on "B2B Selling" -> lines priced from B2B Selling,
    they match the invoice, no amendment is enqueued;
(b) ``amended_from`` source on "B2B Selling" -> the replacement is priced from
    B2B Selling and its header carries B2B Selling;
(c) linked invoice already on the profile list -> unchanged behaviour;
(d) disabled / non-selling / missing list (or a DB error, or a draft) -> falls
    back to the profile list.

These run the REAL ``_build_invoice_items`` against a faked ``Item Price`` table,
so the assertion is on the rates the gate actually compares, not on a stub.
"""
from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from jarz_woocommerce_integration.services import order_sync
from jarz_woocommerce_integration.tests._monkeypatch import MonkeyPatch


WOO_ID = 17756
MAP_NAME = "WOOMAP-17756"
LIVE_INVOICE = "ACC-SINV-2026-18624-1"
TERRITORY = "EGNASRCITY"
POS_PROFILE = "Nasr city"
PROFILE_LIST = "Standard Selling"
B2B_LIST = "B2B Selling"
JAR_ITEM = "JAR-MEDIUM"
JAR_QTY = 4
ITEM_PRICES = {PROFILE_LIST: 120.0, B2B_LIST: 77.0}
ENABLED_SELLING = {"selling": 1, "enabled": 1}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _woo_order(status: str = "processing") -> dict:
    """The 17756 payload shape: four Medium jars at the B2B price."""
    return {
        "id": WOO_ID,
        "number": str(WOO_ID),
        "status": status,
        "currency": "EGP",
        "total": "308.00",
        "payment_method": "cod",
        "payment_method_title": "Cash on Delivery",
        "transaction_id": "",
        "customer_note": "",
        "line_items": [
            {
                "id": 1,
                "product_id": 9001,
                "variation_id": 0,
                "sku": JAR_ITEM,
                "name": "Medium jar",
                "quantity": JAR_QTY,
                "price": "77.00",
                "subtotal": "308.00",
                "total": "308.00",
                "meta_data": [],
            }
        ],
        "shipping_lines": [],
        "fee_lines": [],
        "tax_lines": [],
        "coupon_lines": [],
        "billing": {"first_name": "Cloud nine", "state": TERRITORY, "phone": "01001234567"},
        "shipping": {"first_name": "Cloud nine", "state": TERRITORY},
        "meta_data": [],
    }


def _settings(enable_amendment: int = 1):
    return SimpleNamespace(
        name="WooCommerce Settings",
        base_url="https://example.com",
        consumer_key="ck_test",
        get_password=lambda fieldname: "cs_test",
        default_company="_Test Company",
        default_currency="EGP",
        default_pos_profile=None,
        default_selling_price_list=None,
        enable_inbound_amendment=enable_amendment,
        enable_pre_ofd_paid_amendment=0,
    )


def _invoice_doc(name: str, *, docstatus: int, unit_rate: float, selling_price_list: str):
    """A Sales Invoice as jarz_pos leaves it: one standalone jar row at list price."""
    inv = SimpleNamespace(
        doctype="Sales Invoice",
        name=name,
        docstatus=docstatus,
        woo_order_id=WOO_ID,
        woo_order_number=str(WOO_ID),
        customer="Cloud nine specialty coffee",
        selling_price_list=selling_price_list,
        custom_was_out_for_delivery=0,
        custom_sales_invoice_state="Accepted",
        items=[
            {
                "item_code": JAR_ITEM,
                "qty": JAR_QTY,
                "rate": unit_rate,
                "price_list_rate": unit_rate,
                "discount_percentage": 0,
                "discount_amount": 0,
            }
        ],
        flags=SimpleNamespace(),
    )
    inv.get = lambda field, default=None: getattr(inv, field, default)
    inv.set = lambda field, value: setattr(inv, field, value)
    inv.append = lambda field, value: getattr(inv, field).append(value)
    inv.save = MagicMock()
    inv.db_set = MagicMock()
    inv.cancel = MagicMock()
    return inv


class _CreatedInvoice:
    """Captures the replacement invoice built on the create path."""

    def __init__(self, values: dict):
        self.__dict__.update(values)
        self.values = dict(values)
        self.name = "ACC-SINV-2026-18624-2"
        self.docstatus = 0
        self.flags = SimpleNamespace()
        self.items = list(values.get("items") or [])

    def get(self, fieldname, default=None):
        return getattr(self, fieldname, default)

    def set(self, fieldname, value):
        setattr(self, fieldname, value)

    def append(self, fieldname, value):
        getattr(self, fieldname).append(value)

    def insert(self, ignore_permissions=True):
        return self

    def save(self, *args, **kwargs):
        return self

    def db_set(self, fieldname, value, commit=False):
        setattr(self, fieldname, value)

    def cancel(self):
        self.docstatus = 2


def _install(
    monkeypatch,
    *,
    map_link: str,
    sales_invoices: dict[str, dict],
    docs: dict[str, object],
    live_invoice: str | None,
    price_lists: dict[str, dict] | None = None,
) -> dict:
    """Patch frappe I/O so process_order_phase1 runs end to end for order 17756.

    ``sales_invoices`` is the Sales Invoice table as get_value sees it,
    ``price_lists`` the Price List table (defaults to both lists enabled+selling).
    Returns a recorder with the price lists handed to _build_invoice_items, the
    lines it produced, every Price List lookup, the created invoice and the logger.
    """
    if price_lists is None:
        price_lists = {PROFILE_LIST: dict(ENABLED_SELLING), B2B_LIST: dict(ENABLED_SELLING)}

    rec: dict = {"price_lists": [], "lines": None, "price_list_lookups": [], "created": []}

    fake_lock = MagicMock()
    fake_lock.acquire.return_value = True
    monkeypatch.setattr(order_sync, "get_redis_conn", lambda: MagicMock(lock=lambda *a, **kw: fake_lock))

    def fake_sql(query, values=None, *args, **kwargs):
        if "GET_LOCK" in (query or ""):
            return [[1]]
        return []

    def _pick(row: dict, fieldname):
        if isinstance(fieldname, (list, tuple)):
            return {f: row.get(f) for f in fieldname}
        return row.get(fieldname)

    def fake_get_value(doctype, name=None, fieldname=None, *args, **kwargs):
        if doctype == "WooCommerce Order Map":
            if isinstance(fieldname, list):
                return {
                    "name": MAP_NAME,
                    "erpnext_sales_invoice": map_link,
                    # Stale on purpose: the echo guard leaves it stale, so the
                    # next pass always re-runs the line comparison.
                    "hash": "stale-hash",
                    "status": "processing",
                }
            return None
        if doctype == "Territory" and name == TERRITORY and fieldname == "pos_profile":
            return POS_PROFILE
        if doctype == "POS Profile" and name == POS_PROFILE:
            if fieldname == "warehouse":
                return "Nasr city - J"
            if fieldname == "selling_price_list":
                return PROFILE_LIST
            return None
        if doctype == "Sales Invoice":
            row = sales_invoices.get(name)
            return _pick(row, fieldname) if row is not None else None
        if doctype == "Price List":
            rec["price_list_lookups"].append(name)
            row = price_lists.get(name)
            return _pick(row, fieldname) if row is not None else None
        if doctype == "Item Price" and isinstance(name, dict):
            if name.get("item_code") == JAR_ITEM:
                return ITEM_PRICES.get(name.get("price_list"))
            return None
        return None

    def fake_exists(doctype, name=None, *args, **kwargs):
        return doctype == "Item" and name == JAR_ITEM

    def fake_get_all(doctype, filters=None, fields=None, *args, **kwargs):
        if doctype == "Sales Invoice" and live_invoice:
            return [{"name": live_invoice, "creation": "2026-10-01 22:43:00"}]
        return []

    def fake_get_doc(doctype_or_dict, name=None, *args, **kwargs):
        if isinstance(doctype_or_dict, dict):
            if doctype_or_dict.get("doctype") == "Sales Invoice":
                created = _CreatedInvoice(doctype_or_dict)
                rec["created"].append(created)
                return created
            return MagicMock()
        if doctype_or_dict == "Sales Invoice":
            if name in docs:
                return docs[name]
            raise AssertionError(f"unexpected Sales Invoice {name!r}")
        return MagicMock()

    real_build = order_sync._build_invoice_items

    def spy_build(order, price_list=None, cache=None, is_historical=False):
        rec["price_lists"].append(price_list)
        lines, missing, ctx = real_build(order, price_list=price_list, cache=cache, is_historical=is_historical)
        rec["lines"] = [dict(line) for line in lines]
        return lines, missing, ctx

    logger = MagicMock()
    rec["logger"] = logger

    monkeypatch.setattr(order_sync.frappe.db, "sql", fake_sql)
    monkeypatch.setattr(order_sync.frappe.db, "get_table_columns", lambda table: ["name", "erpnext_sales_invoice", "hash", "status"])
    monkeypatch.setattr(order_sync.frappe.db, "get_value", fake_get_value)
    monkeypatch.setattr(order_sync.frappe.db, "exists", fake_exists)
    monkeypatch.setattr(order_sync.frappe.db, "set_value", MagicMock())
    monkeypatch.setattr(order_sync.frappe.db, "commit", lambda: None)
    monkeypatch.setattr(order_sync.frappe.db, "rollback", lambda: None)
    monkeypatch.setattr(order_sync.frappe, "get_all", fake_get_all)
    monkeypatch.setattr(order_sync.frappe, "get_doc", fake_get_doc)
    monkeypatch.setattr(order_sync.frappe, "enqueue", MagicMock())
    monkeypatch.setattr(order_sync.frappe, "log_error", MagicMock())
    monkeypatch.setattr(order_sync.frappe, "logger", lambda *a, **kw: logger)
    monkeypatch.setattr(order_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False))
    monkeypatch.setattr(order_sync.frappe.utils, "now_datetime", lambda: "2026-10-01 22:43:00")
    monkeypatch.setattr(order_sync.frappe.utils, "today", lambda: "2026-10-01")

    monkeypatch.setattr(order_sync, "_build_invoice_items", spy_build)
    monkeypatch.setattr(
        order_sync,
        "ensure_customer_with_addresses",
        lambda *args, **kwargs: ("Cloud nine specialty coffee", "Billing-001", "Shipping-001"),
    )
    monkeypatch.setattr(
        order_sync,
        "_resolve_territory_from_state",
        lambda state_value, territory_state_cache=None: TERRITORY if state_value == TERRITORY else None,
    )
    monkeypatch.setattr(order_sync, "_check_and_repair_submitted_invoice_drift", lambda *a, **kw: None)
    monkeypatch.setattr(order_sync, "_ensure_invoice_is_pos_flag", lambda *a, **kw: False)
    monkeypatch.setattr(order_sync, "_flag_order_map_for_manual_review", MagicMock())
    monkeypatch.setattr(order_sync, "_apply_delivery_charge_policy", lambda *a, **kw: {"changed": False})
    monkeypatch.setattr(order_sync, "_apply_noncoupon_woo_discount", lambda *a, **kw: 0.0)
    monkeypatch.setattr(order_sync, "_maybe_create_payment_entry_for_invoice", lambda *a, **kw: None)
    monkeypatch.setattr(order_sync, "_submit_invoice_with_accounting_guards", lambda *a, **kw: None)
    monkeypatch.setattr(order_sync, "_reconcile_woo_line_prices", lambda *a, **kw: {"matched": True})
    return rec


def _logged_events(logger: MagicMock) -> list[str]:
    events = []
    for call in logger.warning.call_args_list:
        payload = call.args[0] if call.args else None
        if isinstance(payload, dict) and payload.get("event"):
            events.append(payload["event"])
    return events


def _jar_line(lines: list[dict]) -> dict:
    jar_lines = [line for line in lines or [] if line.get("item_code") == JAR_ITEM]
    assert len(jar_lines) == 1, lines
    return jar_lines[0]


# ---------------------------------------------------------------------------
# End-to-end through process_order_phase1
# ---------------------------------------------------------------------------

class TestInvoiceBoundPriceListInbound(unittest.TestCase):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)

    def test_linked_b2b_invoice_prices_lines_from_b2b_and_does_not_amend(self):
        """(a) The exact 17756 echo: same lines, B2B invoice -> frozen, not amended."""
        live = _invoice_doc(LIVE_INVOICE, docstatus=1, unit_rate=77.0, selling_price_list=B2B_LIST)
        rec = _install(
            self.monkeypatch,
            map_link=LIVE_INVOICE,
            sales_invoices={LIVE_INVOICE: {"docstatus": 1, "selling_price_list": B2B_LIST}},
            docs={LIVE_INVOICE: live},
            live_invoice=LIVE_INVOICE,
        )

        result = order_sync.process_order_phase1(_woo_order(), _settings(enable_amendment=1))

        self.assertEqual(rec["price_lists"], [B2B_LIST])
        jar = _jar_line(rec["lines"])
        self.assertEqual(jar["rate"], 77.0)
        self.assertEqual(jar["price_list_rate"], 77.0)
        self.assertTrue(order_sync._submitted_invoice_matches_target_lines(live, rec["lines"]))
        self.assertEqual(result.get("reason"), "submitted_frozen", result)
        order_sync.frappe.enqueue.assert_not_called()
        live.save.assert_not_called()
        self.assertIn("woo_invoice_price_list_preserved", _logged_events(rec["logger"]))

    def test_amended_from_b2b_source_prices_replacement_from_b2b(self):
        """(b) The amendment job's replacement keeps the source invoice's list."""
        source_name = LIVE_INVOICE
        source = _invoice_doc(source_name, docstatus=2, unit_rate=77.0, selling_price_list=B2B_LIST)
        rec = _install(
            self.monkeypatch,
            map_link=source_name,
            sales_invoices={source_name: {"docstatus": 2, "selling_price_list": B2B_LIST}},
            docs={source_name: source},
            # The job cancels the source before calling in, so no live invoice remains.
            live_invoice=None,
        )

        result = order_sync.process_order_phase1(
            _woo_order(),
            _settings(enable_amendment=1),
            allow_update=True,
            amended_from=source_name,
        )

        self.assertEqual(result.get("status"), "created", result)
        self.assertEqual(rec["price_lists"], [B2B_LIST])
        self.assertEqual(len(rec["created"]), 1)
        replacement = rec["created"][0]
        self.assertEqual(replacement.values.get("selling_price_list"), B2B_LIST)
        self.assertEqual(replacement.values.get("amended_from"), source_name)
        jar = _jar_line(replacement.values.get("items"))
        self.assertEqual(jar["rate"], 77.0)
        self.assertEqual(jar["price_list_rate"], 77.0)
        self.assertEqual(jar["qty"] * jar["rate"], 308.0)

    def test_linked_invoice_on_profile_list_is_unchanged(self):
        """(c) A website order: invoice list == profile list, nothing substitutes."""
        live = _invoice_doc(LIVE_INVOICE, docstatus=1, unit_rate=120.0, selling_price_list=PROFILE_LIST)
        rec = _install(
            self.monkeypatch,
            map_link=LIVE_INVOICE,
            sales_invoices={LIVE_INVOICE: {"docstatus": 1, "selling_price_list": PROFILE_LIST}},
            docs={LIVE_INVOICE: live},
            live_invoice=LIVE_INVOICE,
        )

        result = order_sync.process_order_phase1(_woo_order(), _settings(enable_amendment=1))

        self.assertEqual(rec["price_lists"], [PROFILE_LIST])
        self.assertEqual(_jar_line(rec["lines"])["rate"], 120.0)
        self.assertEqual(rec["price_list_lookups"], [])  # same list: no Price List read at all
        self.assertEqual(result.get("reason"), "submitted_frozen", result)
        order_sync.frappe.enqueue.assert_not_called()
        self.assertNotIn("woo_invoice_price_list_preserved", _logged_events(rec["logger"]))

    def test_disabled_invoice_list_falls_back_to_profile_list(self):
        """(d) A disabled list is not adopted; pricing is exactly the old behaviour."""
        live = _invoice_doc(LIVE_INVOICE, docstatus=1, unit_rate=77.0, selling_price_list=B2B_LIST)
        rec = _install(
            self.monkeypatch,
            map_link=LIVE_INVOICE,
            sales_invoices={LIVE_INVOICE: {"docstatus": 1, "selling_price_list": B2B_LIST}},
            docs={LIVE_INVOICE: live},
            live_invoice=LIVE_INVOICE,
            price_lists={
                PROFILE_LIST: dict(ENABLED_SELLING),
                B2B_LIST: {"selling": 1, "enabled": 0},
            },
        )

        order_sync.process_order_phase1(_woo_order(), _settings(enable_amendment=1))

        self.assertEqual(rec["price_lists"], [PROFILE_LIST])
        self.assertEqual(_jar_line(rec["lines"])["rate"], 120.0)
        self.assertIn("woo_invoice_price_list_not_usable", _logged_events(rec["logger"]))


# ---------------------------------------------------------------------------
# _resolve_invoice_bound_price_list in isolation
# ---------------------------------------------------------------------------

class TestResolveInvoiceBoundPriceList(unittest.TestCase):
    def setUp(self):
        self.monkeypatch = MonkeyPatch()
        self.addCleanup(self.monkeypatch.undo)
        self.logger = MagicMock()
        self.monkeypatch.setattr(order_sync.frappe, "logger", lambda *a, **kw: self.logger)

    def _tables(self, *, sales_invoices: dict, price_lists: dict):
        def fake_get_value(doctype, name=None, fieldname=None, *args, **kwargs):
            table = {"Sales Invoice": sales_invoices, "Price List": price_lists}.get(doctype, {})
            row = table.get(name)
            if row is None:
                return None
            if isinstance(fieldname, (list, tuple)):
                return {f: row.get(f) for f in fieldname}
            return row.get(fieldname)

        self.monkeypatch.setattr(order_sync.frappe.db, "get_value", fake_get_value)

    def _resolve(self, **kwargs):
        return order_sync._resolve_invoice_bound_price_list(PROFILE_LIST, woo_id=WOO_ID, **kwargs)

    def test_linked_submitted_b2b_invoice_wins(self):
        self._tables(
            sales_invoices={LIVE_INVOICE: {"docstatus": 1, "selling_price_list": B2B_LIST}},
            price_lists={B2B_LIST: dict(ENABLED_SELLING)},
        )
        self.assertEqual(self._resolve(linked_invoice_name=LIVE_INVOICE), B2B_LIST)

    def test_amended_from_wins_over_linked_invoice(self):
        self._tables(
            sales_invoices={
                "ACC-SOURCE": {"docstatus": 2, "selling_price_list": B2B_LIST},
                LIVE_INVOICE: {"docstatus": 1, "selling_price_list": "Employee"},
            },
            price_lists={B2B_LIST: dict(ENABLED_SELLING), "Employee": dict(ENABLED_SELLING)},
        )
        self.assertEqual(
            self._resolve(amended_from="ACC-SOURCE", linked_invoice_name=LIVE_INVOICE),
            B2B_LIST,
        )

    def test_disabled_list_falls_back(self):
        self._tables(
            sales_invoices={LIVE_INVOICE: {"docstatus": 1, "selling_price_list": B2B_LIST}},
            price_lists={B2B_LIST: {"selling": 1, "enabled": 0}},
        )
        self.assertEqual(self._resolve(linked_invoice_name=LIVE_INVOICE), PROFILE_LIST)

    def test_non_selling_list_falls_back(self):
        self._tables(
            sales_invoices={LIVE_INVOICE: {"docstatus": 1, "selling_price_list": "Standard Buying"}},
            price_lists={"Standard Buying": {"selling": 0, "enabled": 1}},
        )
        self.assertEqual(self._resolve(linked_invoice_name=LIVE_INVOICE), PROFILE_LIST)

    def test_missing_list_falls_back(self):
        self._tables(
            sales_invoices={LIVE_INVOICE: {"docstatus": 1, "selling_price_list": "Deleted List"}},
            price_lists={},
        )
        self.assertEqual(self._resolve(linked_invoice_name=LIVE_INVOICE), PROFILE_LIST)

    def test_amended_from_with_missing_list_falls_back(self):
        self._tables(
            sales_invoices={"ACC-SOURCE": {"docstatus": 2, "selling_price_list": "Deleted List"}},
            price_lists={},
        )
        self.assertEqual(self._resolve(amended_from="ACC-SOURCE"), PROFILE_LIST)

    def test_draft_linked_invoice_is_not_used(self):
        self._tables(
            sales_invoices={LIVE_INVOICE: {"docstatus": 0, "selling_price_list": B2B_LIST}},
            price_lists={B2B_LIST: dict(ENABLED_SELLING)},
        )
        self.assertEqual(self._resolve(linked_invoice_name=LIVE_INVOICE), PROFILE_LIST)

    def test_blank_invoice_list_falls_back(self):
        self._tables(
            sales_invoices={LIVE_INVOICE: {"docstatus": 1, "selling_price_list": ""}},
            price_lists={},
        )
        self.assertEqual(self._resolve(linked_invoice_name=LIVE_INVOICE), PROFILE_LIST)

    def test_no_invoice_returns_resolved_without_db(self):
        def _boom(*a, **kw):
            raise AssertionError("no invoice -> no lookup")

        self.monkeypatch.setattr(order_sync.frappe.db, "get_value", _boom)
        self.assertEqual(self._resolve(), PROFILE_LIST)

    def test_db_error_falls_back_and_does_not_raise(self):
        def _boom(*a, **kw):
            raise RuntimeError("Lost connection to MySQL server")

        self.monkeypatch.setattr(order_sync.frappe.db, "get_value", _boom)
        self.assertEqual(self._resolve(linked_invoice_name=LIVE_INVOICE), PROFILE_LIST)
        self.assertEqual(self._resolve(amended_from="ACC-SOURCE"), PROFILE_LIST)


if __name__ == "__main__":
    unittest.main()
