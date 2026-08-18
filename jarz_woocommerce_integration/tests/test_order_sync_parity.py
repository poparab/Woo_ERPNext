"""Regression tests for the Woo <-> POS order-parity work package (WP-2).

Covers:

* **F-24** delivery date/slot parsing — the meridiem fix, and the weekday-form
  date. The overriding requirement is that every format that parses TODAY keeps
  parsing byte-identically: production writes ``"5 August, 2026"`` and
  ``"19:00 - 20:30"`` and those two shapes carry essentially all ~10.7k
  historical Woo orders.
* **F-02** payment mapping delegates to ``services.payment_map`` while keeping the
  legacy aliases the store still sends.
* **F-17** ``is_pos`` is only ever written by direct DB write, never through
  ERPNext validation.
* **F-20** Woo/ERPNext price reconciliation flags divergence without flagging
  ordinary discounts or bundles.
* **F-21** inbound Woo order notes are filtered and keyed idempotently.
* **F-07** territory resolution failures are made visible.
"""

from types import SimpleNamespace

import pytest

from jarz_woocommerce_integration.services import order_sync


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _order_with_delivery_meta(*, date_value=None, slot_value=None) -> dict:
    meta = []
    if date_value is not None:
        meta.append({"key": "Delivery Date", "value": date_value})
    if slot_value is not None:
        meta.append({"key": "Time Slot", "value": slot_value})
    return {"id": 4242, "meta_data": meta}


@pytest.fixture
def silence_throttled_log(monkeypatch):
    """Capture the throttled Error Log instead of touching a site."""
    calls = []
    monkeypatch.setattr(
        order_sync,
        "_log_throttled_error",
        lambda **kwargs: calls.append(kwargs),
    )
    return calls


# ---------------------------------------------------------------------------
# F-24 — delivery DATE formats
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw_value,expected",
    [
        # --- production formats. These MUST NOT MOVE. -----------------------
        ("5 August, 2026", "2026-08-05"),
        ("18 August, 2026", "2026-08-18"),
        ("8 August, 2026", "2026-08-08"),
        ("4 August, 2026", "2026-08-04"),
        ("2026-08-02", "2026-08-02"),
        ("2026-06-03", "2026-06-03"),
        # --- weekday form: parsed nowhere before this change ----------------
        ("Sunday, August 02, 2026", "2026-08-02"),
        ("Saturday, June 06, 2026", "2026-06-06"),
        ("Monday, August 03, 2026", "2026-08-03"),
        ("Wednesday, June 03, 2026", "2026-06-03"),
        ("Sunday, 02 August 2026", "2026-08-02"),
    ],
)
def test_parse_delivery_date_formats(raw_value, expected):
    date_part, _time_from, _duration = order_sync._parse_delivery_parts(
        _order_with_delivery_meta(date_value=raw_value)
    )
    assert date_part == expected


def test_parse_delivery_date_unparsable_is_reported(silence_throttled_log):
    date_part, _time_from, _duration = order_sync._parse_delivery_parts(
        _order_with_delivery_meta(date_value="whenever you like")
    )
    assert date_part is None
    assert silence_throttled_log, "an unparsable delivery date must not fail silently"
    assert silence_throttled_log[0]["title"] == "Woo: unparsable Delivery Date meta"


# ---------------------------------------------------------------------------
# F-24 — delivery TIME SLOT formats
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw_value,expected_from,expected_duration",
    [
        # --- 24-hour forms: every one of these is a regression case ---------
        ("19:00 - 20:30", "19:00:00", 90),      # production, most common
        ("13:00 - 14:30", "13:00:00", 90),      # production / our own outbound
        ("17:30 - 19:00", "17:30:00", 90),
        ("12:00-14:00", "12:00:00", 120),       # native, no spaces
        ("13:00 - 15:00", "13:00:00", 120),
        ("00:00 - 02:00", "00:00:00", 120),     # midnight start
        ("23:00 - 00:30", "23:00:00", 90),      # crosses midnight
        ("22:00 - 23:30", "22:00:00", 90),
        # --- 12-hour forms: read 12 hours early before this change ----------
        ("01:00 PM - 02:00 PM", "13:00:00", 60),
        ("1:00 PM - 3:00 PM", "13:00:00", 120),
        ("1:00 pm - 3:00 pm", "13:00:00", 120),
        ("1:00 p.m. - 3:00 p.m.", "13:00:00", 120),
        ("1:00PM - 3:00PM", "13:00:00", 120),
        # --- the two cases naive meridiem handling gets wrong ---------------
        ("12:00 AM - 2:00 AM", "00:00:00", 120),  # 12 AM is hour 0
        ("12:00 PM - 2:00 PM", "12:00:00", 120),  # 12 PM is hour 12
        ("12:00 AM - 02:00 AM", "00:00:00", 120),
        ("12:00 PM - 02:00 PM", "12:00:00", 120),
        # --- meridiem on one side only --------------------------------------
        ("1:00 - 3:00 PM", "13:00:00", 120),
        ("11:00 AM - 1:00 PM", "11:00:00", 120),
        # --- separators the parser already normalised ------------------------
        ("13:00 to 14:30", "13:00:00", 90),
        ("13:00 – 14:30", "13:00:00", 90),
    ],
)
def test_parse_delivery_time_slot_formats(raw_value, expected_from, expected_duration):
    _date, time_from, duration = order_sync._parse_delivery_parts(
        _order_with_delivery_meta(slot_value=raw_value)
    )
    assert (time_from, duration) == (expected_from, expected_duration)


def test_parse_delivery_slot_unparsable_is_reported(silence_throttled_log):
    _date, time_from, duration = order_sync._parse_delivery_parts(
        _order_with_delivery_meta(slot_value="sometime this afternoon")
    )
    assert (time_from, duration) == (None, None)
    assert silence_throttled_log[0]["title"] == "Woo: unparsable Time Slot meta"


def test_parse_delivery_parts_reads_full_production_shape():
    date_part, time_from, duration = order_sync._parse_delivery_parts(
        _order_with_delivery_meta(date_value="18 August, 2026", slot_value="19:00 - 20:30")
    )
    assert (date_part, time_from, duration) == ("2026-08-18", "19:00:00", 90)


@pytest.mark.parametrize(
    "raw,expected",
    [("PM", "pm"), ("pm", "pm"), ("p.m.", "pm"), ("A.M.", "am"), ("am", "am"), ("", None), (None, None)],
)
def test_normalize_meridiem(raw, expected):
    assert order_sync._normalize_meridiem(raw) == expected


@pytest.mark.parametrize(
    "hour,meridiem,expected",
    [
        (12, "am", 0),   # midnight
        (12, "pm", 12),  # noon
        (1, "pm", 13),
        (11, "pm", 23),
        (1, "am", 1),
        (13, None, 13),  # 24-hour input is passed through untouched
    ],
)
def test_apply_meridiem(hour, meridiem, expected):
    assert order_sync._apply_meridiem(hour, meridiem) == expected


def test_extract_meridiem_slot_declines_24_hour_input():
    """No meridiem anywhere -> None, so 24-hour slots keep their original path."""
    assert order_sync._extract_meridiem_slot("19:00-20:30") is None


# ---------------------------------------------------------------------------
# F-02 — payment map delegation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "woo_method,woo_title,expected",
    [
        ("cod", "Cash on delivery", "Cash"),
        ("instapay", "InstaPay", "Instapay"),
        ("wallet", "Mobile Wallet", "Mobile Wallet"),
        ("kashier_card", "Kashier", "Kashier Card"),
        ("kashier_wallet", "Kashier", "Kashier Wallet"),
        # legacy aliases the store still sends and that we never emit
        ("card", "Credit Card", "Kashier Card"),
        ("kashier", "Kashier Wallet", "Kashier Wallet"),
        ("kashier", "Kashier Card", "Kashier Card"),
        ("kashier", "", "Kashier Card"),
        ("COD", "Cash on delivery", "Cash"),
        (" cod ", None, "Cash"),
        # unknown / blank
        ("paypal", "PayPal", None),
        ("", "", None),
        (None, None, None),
    ],
)
def test_map_payment_method_matches_legacy_behaviour(woo_method, woo_title, expected):
    assert order_sync._map_payment_method(woo_method, woo_title) == expected


def test_map_payment_method_uses_the_shared_table(monkeypatch):
    """It must delegate, not keep a private copy of the table."""
    from jarz_woocommerce_integration.services import payment_map

    seen = {}

    def fake_woo_to_erpnext(method, title=None):
        seen["args"] = (method, title)
        return "Cash"

    monkeypatch.setattr(payment_map, "woo_to_erpnext", fake_woo_to_erpnext)
    assert order_sync._map_payment_method("cod", "Cash on delivery") == "Cash"
    assert seen["args"] == ("cod", "Cash on delivery")


# ---------------------------------------------------------------------------
# F-17 — is_pos
# ---------------------------------------------------------------------------

class _DummyInvoiceDoc:
    def __init__(self, *, docstatus=0, is_pos=0, payments=None):
        self.docstatus = docstatus
        self.is_pos = is_pos
        self.name = "ACC-SINV-TEST-0001"
        self._payments = list(payments or [])

    def get(self, fieldname, default=None):
        if fieldname == "payments":
            return self._payments
        return default


def test_neutralize_is_pos_clears_the_flag_on_a_payment_less_draft():
    doc = _DummyInvoiceDoc(docstatus=0, is_pos=1)
    order_sync._neutralize_is_pos_before_document_write(doc)
    assert doc.is_pos == 0


def test_neutralize_is_pos_leaves_a_real_pos_invoice_alone():
    doc = _DummyInvoiceDoc(docstatus=0, is_pos=1, payments=[{"mode_of_payment": "Cash", "amount": 100}])
    order_sync._neutralize_is_pos_before_document_write(doc)
    assert doc.is_pos == 1


def test_neutralize_is_pos_never_touches_a_submitted_document():
    """Changing it after submit would trip validate_update_after_submit."""
    doc = _DummyInvoiceDoc(docstatus=1, is_pos=1)
    order_sync._neutralize_is_pos_before_document_write(doc)
    assert doc.is_pos == 1


def test_ensure_invoice_is_pos_flag_writes_without_touching_modified(monkeypatch):
    writes = []

    fake_frappe = SimpleNamespace(
        db=SimpleNamespace(
            get_value=lambda doctype, name, fields, as_dict=False: {
                "is_pos": 0,
                "pos_profile": "Nasr city",
            },
            set_value=lambda doctype, name, field, value, update_modified=True: writes.append(
                (doctype, name, field, value, update_modified)
            ),
        ),
        log_error=lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(order_sync, "frappe", fake_frappe)

    assert order_sync._ensure_invoice_is_pos_flag("ACC-SINV-0001") is True
    assert writes == [("Sales Invoice", "ACC-SINV-0001", "is_pos", 1, False)]


def test_ensure_invoice_is_pos_flag_refuses_without_a_pos_profile(monkeypatch, silence_throttled_log):
    writes = []

    fake_frappe = SimpleNamespace(
        db=SimpleNamespace(
            get_value=lambda doctype, name, fields, as_dict=False: {"is_pos": 0, "pos_profile": None},
            set_value=lambda *args, **kwargs: writes.append(args),
        ),
        log_error=lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(order_sync, "frappe", fake_frappe)

    assert order_sync._ensure_invoice_is_pos_flag("ACC-SINV-0002") is False
    assert writes == []
    assert silence_throttled_log, "refusing to set is_pos must be logged, not silent"


def test_ensure_invoice_is_pos_flag_is_idempotent(monkeypatch):
    writes = []
    fake_frappe = SimpleNamespace(
        db=SimpleNamespace(
            get_value=lambda doctype, name, fields, as_dict=False: {
                "is_pos": 1,
                "pos_profile": "Nasr city",
            },
            set_value=lambda *args, **kwargs: writes.append(args),
        ),
        log_error=lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(order_sync, "frappe", fake_frappe)

    assert order_sync._ensure_invoice_is_pos_flag("ACC-SINV-0003") is True
    assert writes == []


# ---------------------------------------------------------------------------
# F-20 — Woo/ERPNext price reconciliation
# ---------------------------------------------------------------------------

def test_reconcile_prices_matches_when_price_lists_agree():
    order = {"line_items": [{"subtotal": "1000.00", "total": "1000.00"}]}
    lines = [{"item_code": "CAKE", "qty": 2, "rate": 500, "price_list_rate": 500}]

    decision = order_sync._reconcile_woo_line_prices(order, lines)

    assert decision["matched"] is True
    assert decision["woo_line_subtotal"] == 1000.0
    assert decision["erpnext_line_total"] == 1000.0
    assert decision["delta"] == 0.0


def test_reconcile_prices_flags_a_real_divergence():
    order = {"line_items": [{"subtotal": "1000.00", "total": "1000.00"}]}
    lines = [{"item_code": "CAKE", "qty": 2, "rate": 400, "price_list_rate": 400}]

    decision = order_sync._reconcile_woo_line_prices(order, lines)

    assert decision["matched"] is False
    assert decision["delta"] == -200.0
    assert decision["compared_against"] == "subtotal"


def test_reconcile_prices_ignores_an_ordinary_woo_discount():
    """A discounted line must not read as a price-list divergence.

    Woo reduces line ``total``; ERPNext keeps the list price on the line and
    carries the discount at header level, so ``subtotal`` is the comparable side.
    """
    order = {
        "line_items": [{"subtotal": "960.00", "total": "720.00"}],
        "discount_total": "240.00",
    }
    lines = [{"item_code": "BUNDLE-CHILD", "qty": 1, "rate": 960, "price_list_rate": 960}]

    decision = order_sync._reconcile_woo_line_prices(order, lines)

    assert decision["matched"] is True
    assert decision["woo_line_total"] == 720.0
    assert decision["woo_discount_total"] == 240.0


def test_reconcile_prices_handles_a_bundle_round_trip():
    """Woo puts the price on the parent; ERPNext splits it across the children."""
    order = {
        "line_items": [
            {"subtotal": "480.00", "total": "480.00"},   # bundle parent
            {"subtotal": "0.00", "total": "0.00"},       # children carry no price
            {"subtotal": "0.00", "total": "0.00"},
        ]
    }
    lines = [
        {"item_code": "BUNDLE", "qty": 1, "price_list_rate": 480, "discount_percentage": 100,
         "is_bundle_parent": 1},
        {"item_code": "CHILD-A", "qty": 2, "price_list_rate": 150, "discount_percentage": 20,
         "is_bundle_child": 1},
        {"item_code": "CHILD-B", "qty": 2, "price_list_rate": 150, "discount_percentage": 20,
         "is_bundle_child": 1},
    ]

    decision = order_sync._reconcile_woo_line_prices(order, lines)

    assert decision["erpnext_line_total"] == 480.0
    assert decision["matched"] is True


def test_reconcile_prices_falls_back_to_total_without_subtotals():
    order = {"line_items": [{"total": "500.00"}]}
    lines = [{"item_code": "CAKE", "qty": 1, "price_list_rate": 500}]

    decision = order_sync._reconcile_woo_line_prices(order, lines)

    assert decision["compared_against"] == "total"
    assert decision["matched"] is True


def test_price_tolerance_uses_the_larger_of_absolute_and_relative():
    assert order_sync._woo_price_tolerance(1000.0) == 5.0   # 0.5% of 1000
    assert order_sync._woo_price_tolerance(10.0) == 1.0     # absolute floor wins


def test_price_tolerance_can_be_overridden_from_settings():
    settings = SimpleNamespace(woo_price_tolerance_amount=25, woo_price_tolerance_percent=0)
    assert order_sync._woo_price_tolerance(1000.0, settings) == 25.0


def test_price_mismatch_flags_but_never_raises(monkeypatch):
    flagged = {}
    logged = {}
    monkeypatch.setattr(
        order_sync,
        "_flag_order_map_for_manual_review",
        lambda **kwargs: flagged.update(kwargs),
    )
    monkeypatch.setattr(
        order_sync,
        "create_sync_log_entry",
        lambda operation, status, message, **kwargs: logged.update(
            {"operation": operation, "status": status, "message": message}
        ),
    )

    decision = order_sync._reconcile_woo_line_prices(
        {"line_items": [{"subtotal": "1000.00", "total": "1000.00"}]},
        [{"item_code": "CAKE", "qty": 1, "price_list_rate": 400}],
    )
    order_sync._flag_woo_price_mismatch(woo_id=17, invoice_name="ACC-SINV-9", decision=decision)

    assert flagged["woo_id"] == 17
    assert flagged["invoice_name"] == "ACC-SINV-9"
    assert "ERPNext price list" in flagged["reason"]
    assert logged["operation"] == "PriceMismatch"
    assert logged["status"] == "NeedsReview"


# ---------------------------------------------------------------------------
# F-21 — inbound Woo order notes
# ---------------------------------------------------------------------------

class _FakeNotesClient:
    def __init__(self, notes):
        self._notes = notes
        self.requested = []

    def get(self, resource, params=None):
        self.requested.append(resource)
        return self._notes


def test_woo_note_is_ours_detects_our_own_outbound_signature():
    assert order_sync._woo_note_is_ours("Ring the bell\n\n— Ahmed (Jarz POS)") is True
    assert order_sync._woo_note_is_ours("Ring the bell\n\n— Jarz POS") is True
    assert order_sync._woo_note_is_ours("Leave with the doorman") is False


def test_woo_note_is_ours_detects_a_note_we_already_pulled_in():
    body = f"Leave with the doorman\n\n{order_sync.WOO_ORDER_NOTE_MARKER}98"
    assert order_sync._woo_note_is_ours(body) is True


def test_sync_woo_order_notes_filters_system_and_our_own_notes(monkeypatch):
    created = []
    monkeypatch.setattr(order_sync, "_invoice_note_doctype_available", lambda: True)
    monkeypatch.setattr(order_sync, "_materialize_changed_customer_note", lambda order, invoice: 0)
    monkeypatch.setattr(
        order_sync,
        "_materialize_invoice_note",
        lambda invoice_name, body, *, marker, author=None: created.append(
            {"invoice": invoice_name, "body": body, "marker": marker, "author": author}
        )
        or True,
    )

    client = _FakeNotesClient(
        [
            {"id": 11, "author": "system", "note": "Order status changed from Processing to Completed."},
            {"id": 12, "author": "Ahmed", "note": "Called the customer\n\n— Ahmed (Jarz POS)"},
            {"id": 13, "author": "Shop Manager", "note": "Customer asked for delivery after 8pm"},
            {"id": 14, "author": "Shop Manager", "note": "   "},
        ]
    )

    result = order_sync.sync_woo_order_notes(
        {"id": 5150},
        "ACC-SINV-0007",
        settings=SimpleNamespace(enable_inbound_order_notes=1),
        client=client,
    )

    assert client.requested == ["orders/5150/notes"]
    assert result["status"] == "synced"
    assert result["created"] == 1
    assert len(created) == 1
    assert created[0]["marker"] == f"{order_sync.WOO_ORDER_NOTE_MARKER}13"
    assert created[0]["body"].startswith("Customer asked for delivery after 8pm")
    assert created[0]["body"].endswith(f"{order_sync.WOO_ORDER_NOTE_MARKER}13")
    assert created[0]["invoice"] == "ACC-SINV-0007"


def test_sync_woo_order_notes_respects_the_settings_switch(monkeypatch):
    monkeypatch.setattr(order_sync, "_invoice_note_doctype_available", lambda: True)
    client = _FakeNotesClient([{"id": 1, "author": "x", "note": "hello"}])

    result = order_sync.sync_woo_order_notes(
        {"id": 900},
        "ACC-SINV-0008",
        settings=SimpleNamespace(enable_inbound_order_notes=0),
        client=client,
    )

    assert result == {"status": "skipped", "created": 0, "examined": 0, "reason": "disabled"}
    assert client.requested == []


def test_sync_woo_order_notes_degrades_when_the_doctype_is_absent(monkeypatch, silence_throttled_log):
    monkeypatch.setattr(order_sync, "_invoice_note_doctype_available", lambda: False)
    client = _FakeNotesClient([{"id": 1, "author": "x", "note": "hello"}])

    result = order_sync.sync_woo_order_notes(
        {"id": 901}, "ACC-SINV-0009", settings=SimpleNamespace(), client=client
    )

    assert result["reason"] == "doctype_missing"
    assert client.requested == []
    assert silence_throttled_log, "a missing DocType must degrade loudly, not silently"


def test_enqueue_woo_order_note_sync_queues_after_commit(monkeypatch):
    calls = []
    monkeypatch.setattr(
        order_sync,
        "frappe",
        SimpleNamespace(
            enqueue=lambda method, **kwargs: calls.append((method, kwargs)),
            log_error=lambda *a, **k: None,
        ),
    )

    queued = order_sync.enqueue_woo_order_note_sync(
        {"id": 5150, "customer_note": "ring the bell"},
        "ACC-SINV-0012",
        settings=SimpleNamespace(),
    )

    assert queued is True
    method, kwargs = calls[0]
    assert method == "jarz_woocommerce_integration.services.order_sync.sync_woo_order_notes_job"
    assert kwargs["enqueue_after_commit"] is True  # must not run before the invoice exists
    assert kwargs["woo_order_id"] == 5150
    assert kwargs["invoice_name"] == "ACC-SINV-0012"
    assert kwargs["customer_note"] == "ring the bell"


def test_enqueue_woo_order_note_sync_skips_when_disabled_or_unlinked(monkeypatch):
    calls = []
    monkeypatch.setattr(
        order_sync,
        "frappe",
        SimpleNamespace(enqueue=lambda *a, **k: calls.append(a), log_error=lambda *a, **k: None),
    )

    assert order_sync.enqueue_woo_order_note_sync({"id": 1}, "", settings=SimpleNamespace()) is False
    assert order_sync.enqueue_woo_order_note_sync({}, "ACC-SINV-1", settings=SimpleNamespace()) is False
    assert (
        order_sync.enqueue_woo_order_note_sync(
            {"id": 1}, "ACC-SINV-1", settings=SimpleNamespace(enable_inbound_order_notes=0)
        )
        is False
    )
    assert calls == []


def test_enqueue_woo_order_note_sync_never_raises(monkeypatch):
    """An enrichment must not be able to fail an order sync."""

    def _boom(*args, **kwargs):
        raise RuntimeError("redis is down")

    monkeypatch.setattr(
        order_sync,
        "frappe",
        SimpleNamespace(enqueue=_boom, log_error=lambda *a, **k: None, get_traceback=lambda: "tb"),
    )

    assert (
        order_sync.enqueue_woo_order_note_sync({"id": 7}, "ACC-SINV-7", settings=SimpleNamespace())
        is False
    )


def test_inbound_order_notes_default_on_when_the_field_does_not_exist():
    assert order_sync._inbound_order_notes_enabled(SimpleNamespace()) is True
    assert order_sync._inbound_order_notes_enabled(SimpleNamespace(enable_inbound_order_notes=0)) is False
    assert order_sync._inbound_order_notes_enabled(SimpleNamespace(enable_inbound_order_notes=1)) is True


def test_customer_note_already_in_remarks_is_not_duplicated(monkeypatch):
    monkeypatch.setattr(
        order_sync,
        "frappe",
        SimpleNamespace(db=SimpleNamespace(get_value=lambda *a, **k: "Please call on arrival")),
    )
    monkeypatch.setattr(
        order_sync,
        "_materialize_invoice_note",
        lambda *a, **k: pytest.fail("must not duplicate the create-path remarks"),
    )

    assert (
        order_sync._materialize_changed_customer_note(
            {"customer_note": "Please call on arrival"}, "ACC-SINV-0010"
        )
        == 0
    )


def test_changed_customer_note_is_materialised(monkeypatch):
    created = []
    monkeypatch.setattr(
        order_sync,
        "frappe",
        SimpleNamespace(db=SimpleNamespace(get_value=lambda *a, **k: "Please call on arrival")),
    )
    monkeypatch.setattr(
        order_sync,
        "_materialize_invoice_note",
        lambda invoice_name, body, *, marker, author=None: created.append((body, marker)) or True,
    )

    count = order_sync._materialize_changed_customer_note(
        {"customer_note": "Changed my mind — leave it with the doorman"}, "ACC-SINV-0011"
    )

    assert count == 1
    body, marker = created[0]
    assert body.startswith("Changed my mind")
    assert marker.startswith(order_sync.WOO_CUSTOMER_NOTE_MARKER)
    assert body.endswith(marker)


# ---------------------------------------------------------------------------
# F-07 — territory resolution visibility
# ---------------------------------------------------------------------------

def test_territory_audit_is_quiet_when_the_order_resolved_itself(silence_throttled_log):
    detail = order_sync._audit_order_territory_resolution(
        woo_id=1,
        territory_snapshot={
            "resolved_order_territory": "EG6OCT",
            "woo_shipping_state": "6th of October",
            "woo_billing_state": "6th of October",
        },
        effective_territory="EG6OCT",
        customer="CUST-0001",
    )
    assert detail == ""
    assert silence_throttled_log == []


def test_territory_audit_reports_the_customer_fallback(silence_throttled_log):
    detail = order_sync._audit_order_territory_resolution(
        woo_id=2,
        territory_snapshot={
            "resolved_order_territory": "",
            "woo_shipping_state": "Zamalek",
            "woo_billing_state": "",
        },
        effective_territory="EGCAIRO",
        customer="CUST-0002",
    )
    assert "Order-level territory unresolved" in detail
    assert "EGCAIRO" in detail
    assert silence_throttled_log, "an unresolved order territory must reach the Error Log"


def test_territory_audit_reports_a_fully_unresolved_order(silence_throttled_log):
    detail = order_sync._audit_order_territory_resolution(
        woo_id=3,
        territory_snapshot={
            "resolved_order_territory": "",
            "woo_shipping_state": "",
            "woo_billing_state": "",
        },
        effective_territory=None,
        customer="CUST-0003",
    )
    assert "NO territory" in detail
    assert silence_throttled_log


def test_order_map_values_only_carry_a_territory_error_when_asked():
    snapshot = {"customer_name": "Someone"}
    without = order_sync._build_order_map_snapshot_values({"id": 1}, snapshot)
    assert "last_territory_error" not in without

    with_error = order_sync._build_order_map_snapshot_values(
        {"id": 1}, snapshot, territory_error="unresolved"
    )
    assert with_error["last_territory_error"] == "unresolved"
