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

Plain ``unittest`` on purpose: pytest is not installed in the bench Python
environment, so a pytest-only module cannot run anywhere it matters. Table-driven
cases use ``subTest`` so a failure still names the offending input.

Nothing here needs a site. Every test that would otherwise reach the database
replaces ``order_sync.frappe`` wholesale rather than patching attributes on
``frappe.db``, which needs a bound ``frappe.local``.
"""

from types import SimpleNamespace
import unittest
# Explicit: this module uses unittest.mock, which `import unittest` alone does
# not pull in. Relying on another test module to have imported it first makes the
# suite order-dependent.
import unittest.mock

from jarz_woocommerce_integration.services import order_sync


def _order_with_delivery_meta(*, date_value=None, slot_value=None) -> dict:
    meta = []
    if date_value is not None:
        meta.append({"key": "Delivery Date", "value": date_value})
    if slot_value is not None:
        meta.append({"key": "Time Slot", "value": slot_value})
    return {"id": 4242, "meta_data": meta}


class _FakeNotesClient:
    def __init__(self, notes):
        self._notes = notes
        self.requested = []

    def get(self, resource, params=None):
        self.requested.append(resource)
        return self._notes


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


class ParityTestCase(unittest.TestCase):
    """Shared patching helpers (the unittest equivalent of the old fixtures)."""

    def patch_attr(self, target, name, value):
        patcher = unittest.mock.patch.object(target, name, value)
        patcher.start()
        self.addCleanup(patcher.stop)
        return value

    def capture_throttled_logs(self):
        """Collect throttled Error Logs instead of touching a site."""
        calls = []
        self.patch_attr(order_sync, "_log_throttled_error", lambda **kwargs: calls.append(kwargs))
        return calls


# ---------------------------------------------------------------------------
# F-24 — delivery DATE formats
# ---------------------------------------------------------------------------

class TestDeliveryDateParsing(ParityTestCase):
    def test_parse_delivery_date_formats(self):
        cases = [
            # --- production formats. These MUST NOT MOVE. -------------------
            ("5 August, 2026", "2026-08-05"),
            ("18 August, 2026", "2026-08-18"),
            ("8 August, 2026", "2026-08-08"),
            ("4 August, 2026", "2026-08-04"),
            ("2026-08-02", "2026-08-02"),
            ("2026-06-03", "2026-06-03"),
            # --- weekday form: parsed nowhere before this change ------------
            ("Sunday, August 02, 2026", "2026-08-02"),
            ("Saturday, June 06, 2026", "2026-06-06"),
            ("Monday, August 03, 2026", "2026-08-03"),
            ("Wednesday, June 03, 2026", "2026-06-03"),
            ("Sunday, 02 August 2026", "2026-08-02"),
        ]
        for raw_value, expected in cases:
            with self.subTest(raw_value=raw_value):
                date_part, _time_from, _duration = order_sync._parse_delivery_parts(
                    _order_with_delivery_meta(date_value=raw_value)
                )
                self.assertEqual(date_part, expected)

    def test_parse_delivery_date_unparsable_is_reported(self):
        logs = self.capture_throttled_logs()

        date_part, _time_from, _duration = order_sync._parse_delivery_parts(
            _order_with_delivery_meta(date_value="whenever you like")
        )

        self.assertIsNone(date_part)
        self.assertTrue(logs, "an unparsable delivery date must not fail silently")
        self.assertEqual(logs[0]["title"], "Woo: unparsable Delivery Date meta")


# ---------------------------------------------------------------------------
# F-24 — delivery TIME SLOT formats
# ---------------------------------------------------------------------------

class TestDeliverySlotParsing(ParityTestCase):
    def test_parse_delivery_time_slot_formats(self):
        cases = [
            # --- 24-hour forms: every one of these is a regression case -----
            ("19:00 - 20:30", "19:00:00", 90),      # production, most common
            ("13:00 - 14:30", "13:00:00", 90),      # production / our own outbound
            ("17:30 - 19:00", "17:30:00", 90),
            ("12:00-14:00", "12:00:00", 120),       # native, no spaces
            ("13:00 - 15:00", "13:00:00", 120),
            ("00:00 - 02:00", "00:00:00", 120),     # midnight start
            ("23:00 - 00:30", "23:00:00", 90),      # crosses midnight
            ("22:00 - 23:30", "22:00:00", 90),
            # --- 12-hour forms: read 12 hours early before this change ------
            ("01:00 PM - 02:00 PM", "13:00:00", 60),
            ("1:00 PM - 3:00 PM", "13:00:00", 120),
            ("1:00 pm - 3:00 pm", "13:00:00", 120),
            ("1:00 p.m. - 3:00 p.m.", "13:00:00", 120),
            ("1:00PM - 3:00PM", "13:00:00", 120),
            # --- the two cases naive meridiem handling gets wrong -----------
            ("12:00 AM - 2:00 AM", "00:00:00", 120),   # 12 AM is hour 0
            ("12:00 PM - 2:00 PM", "12:00:00", 120),   # 12 PM is hour 12
            ("12:00 AM - 02:00 AM", "00:00:00", 120),
            ("12:00 PM - 02:00 PM", "12:00:00", 120),
            # --- meridiem on one side only ----------------------------------
            ("1:00 - 3:00 PM", "13:00:00", 120),
            ("11:00 AM - 1:00 PM", "11:00:00", 120),
            # --- separators the parser already normalised -------------------
            ("13:00 to 14:30", "13:00:00", 90),
            ("13:00 – 14:30", "13:00:00", 90),
        ]
        for raw_value, expected_from, expected_duration in cases:
            with self.subTest(raw_value=raw_value):
                _date, time_from, duration = order_sync._parse_delivery_parts(
                    _order_with_delivery_meta(slot_value=raw_value)
                )
                self.assertEqual((time_from, duration), (expected_from, expected_duration))

    def test_parse_delivery_slot_unparsable_is_reported(self):
        logs = self.capture_throttled_logs()

        _date, time_from, duration = order_sync._parse_delivery_parts(
            _order_with_delivery_meta(slot_value="sometime this afternoon")
        )

        self.assertEqual((time_from, duration), (None, None))
        self.assertEqual(logs[0]["title"], "Woo: unparsable Time Slot meta")

    def test_parse_delivery_parts_reads_full_production_shape(self):
        date_part, time_from, duration = order_sync._parse_delivery_parts(
            _order_with_delivery_meta(date_value="18 August, 2026", slot_value="19:00 - 20:30")
        )
        self.assertEqual((date_part, time_from, duration), ("2026-08-18", "19:00:00", 90))

    def test_normalize_meridiem(self):
        cases = [
            ("PM", "pm"),
            ("pm", "pm"),
            ("p.m.", "pm"),
            ("A.M.", "am"),
            ("am", "am"),
            ("", None),
            (None, None),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(order_sync._normalize_meridiem(raw), expected)

    def test_apply_meridiem(self):
        cases = [
            (12, "am", 0),    # midnight
            (12, "pm", 12),   # noon
            (1, "pm", 13),
            (11, "pm", 23),
            (1, "am", 1),
            (13, None, 13),   # 24-hour input is passed through untouched
        ]
        for hour, meridiem, expected in cases:
            with self.subTest(hour=hour, meridiem=meridiem):
                self.assertEqual(order_sync._apply_meridiem(hour, meridiem), expected)

    def test_extract_meridiem_slot_declines_24_hour_input(self):
        """No meridiem anywhere -> None, so 24-hour slots keep their original path."""
        self.assertIsNone(order_sync._extract_meridiem_slot("19:00-20:30"))


# ---------------------------------------------------------------------------
# F-02 — payment map delegation
# ---------------------------------------------------------------------------

class TestPaymentMethodMap(ParityTestCase):
    def test_map_payment_method_matches_legacy_behaviour(self):
        cases = [
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
        ]
        for woo_method, woo_title, expected in cases:
            with self.subTest(woo_method=woo_method, woo_title=woo_title):
                self.assertEqual(order_sync._map_payment_method(woo_method, woo_title), expected)

    def test_map_payment_method_uses_the_shared_table(self):
        """It must delegate, not keep a private copy of the table."""
        from jarz_woocommerce_integration.services import payment_map

        seen = {}

        def fake_woo_to_erpnext(method, title=None):
            seen["args"] = (method, title)
            return "Cash"

        self.patch_attr(payment_map, "woo_to_erpnext", fake_woo_to_erpnext)

        self.assertEqual(order_sync._map_payment_method("cod", "Cash on delivery"), "Cash")
        self.assertEqual(seen["args"], ("cod", "Cash on delivery"))


# ---------------------------------------------------------------------------
# F-17 — is_pos
# ---------------------------------------------------------------------------

class TestIsPosFlag(ParityTestCase):
    def test_neutralize_is_pos_clears_the_flag_on_a_payment_less_draft(self):
        doc = _DummyInvoiceDoc(docstatus=0, is_pos=1)
        order_sync._neutralize_is_pos_before_document_write(doc)
        self.assertEqual(doc.is_pos, 0)

    def test_neutralize_is_pos_leaves_a_real_pos_invoice_alone(self):
        doc = _DummyInvoiceDoc(
            docstatus=0, is_pos=1, payments=[{"mode_of_payment": "Cash", "amount": 100}]
        )
        order_sync._neutralize_is_pos_before_document_write(doc)
        self.assertEqual(doc.is_pos, 1)

    def test_neutralize_is_pos_never_touches_a_submitted_document(self):
        """Changing it after submit would trip validate_update_after_submit."""
        doc = _DummyInvoiceDoc(docstatus=1, is_pos=1)
        order_sync._neutralize_is_pos_before_document_write(doc)
        self.assertEqual(doc.is_pos, 1)

    def test_ensure_invoice_is_pos_flag_writes_without_touching_modified(self):
        writes = []
        self.patch_attr(
            order_sync,
            "frappe",
            SimpleNamespace(
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
            ),
        )

        self.assertIs(order_sync._ensure_invoice_is_pos_flag("ACC-SINV-0001"), True)
        self.assertEqual(writes, [("Sales Invoice", "ACC-SINV-0001", "is_pos", 1, False)])

    def test_ensure_invoice_is_pos_flag_refuses_without_a_pos_profile(self):
        logs = self.capture_throttled_logs()
        writes = []
        self.patch_attr(
            order_sync,
            "frappe",
            SimpleNamespace(
                db=SimpleNamespace(
                    get_value=lambda doctype, name, fields, as_dict=False: {
                        "is_pos": 0,
                        "pos_profile": None,
                    },
                    set_value=lambda *args, **kwargs: writes.append(args),
                ),
                log_error=lambda *args, **kwargs: None,
            ),
        )

        self.assertIs(order_sync._ensure_invoice_is_pos_flag("ACC-SINV-0002"), False)
        self.assertEqual(writes, [])
        self.assertTrue(logs, "refusing to set is_pos must be logged, not silent")

    def test_ensure_invoice_is_pos_flag_is_idempotent(self):
        writes = []
        self.patch_attr(
            order_sync,
            "frappe",
            SimpleNamespace(
                db=SimpleNamespace(
                    get_value=lambda doctype, name, fields, as_dict=False: {
                        "is_pos": 1,
                        "pos_profile": "Nasr city",
                    },
                    set_value=lambda *args, **kwargs: writes.append(args),
                ),
                log_error=lambda *args, **kwargs: None,
            ),
        )

        self.assertIs(order_sync._ensure_invoice_is_pos_flag("ACC-SINV-0003"), True)
        self.assertEqual(writes, [])


# ---------------------------------------------------------------------------
# F-20 — Woo/ERPNext price reconciliation
# ---------------------------------------------------------------------------

class TestPriceReconciliation(ParityTestCase):
    def test_reconcile_prices_matches_when_price_lists_agree(self):
        order = {"line_items": [{"subtotal": "1000.00", "total": "1000.00"}]}
        lines = [{"item_code": "CAKE", "qty": 2, "rate": 500, "price_list_rate": 500}]

        decision = order_sync._reconcile_woo_line_prices(order, lines)

        self.assertIs(decision["matched"], True)
        self.assertEqual(decision["woo_line_subtotal"], 1000.0)
        self.assertEqual(decision["erpnext_line_total"], 1000.0)
        self.assertEqual(decision["delta"], 0.0)

    def test_reconcile_prices_flags_a_real_divergence(self):
        order = {"line_items": [{"subtotal": "1000.00", "total": "1000.00"}]}
        lines = [{"item_code": "CAKE", "qty": 2, "rate": 400, "price_list_rate": 400}]

        decision = order_sync._reconcile_woo_line_prices(order, lines)

        self.assertIs(decision["matched"], False)
        self.assertEqual(decision["delta"], -200.0)
        self.assertEqual(decision["compared_against"], "subtotal")

    def test_reconcile_prices_ignores_an_ordinary_woo_discount(self):
        """A discounted line must not read as a price-list divergence.

        Woo reduces line ``total``; ERPNext keeps the list price on the line and
        carries the discount at header level, so ``subtotal`` is the comparable
        side.
        """
        order = {
            "line_items": [{"subtotal": "960.00", "total": "720.00"}],
            "discount_total": "240.00",
        }
        lines = [{"item_code": "BUNDLE-CHILD", "qty": 1, "rate": 960, "price_list_rate": 960}]

        decision = order_sync._reconcile_woo_line_prices(order, lines)

        self.assertIs(decision["matched"], True)
        self.assertEqual(decision["woo_line_total"], 720.0)
        self.assertEqual(decision["woo_discount_total"], 240.0)

    def test_reconcile_prices_handles_a_bundle_round_trip(self):
        """Woo puts the price on the parent; ERPNext splits it across the children."""
        order = {
            "line_items": [
                {"subtotal": "480.00", "total": "480.00"},   # bundle parent
                {"subtotal": "0.00", "total": "0.00"},       # children carry no price
                {"subtotal": "0.00", "total": "0.00"},
            ]
        }
        lines = [
            {"item_code": "BUNDLE", "qty": 1, "price_list_rate": 480,
             "discount_percentage": 100, "is_bundle_parent": 1},
            {"item_code": "CHILD-A", "qty": 2, "price_list_rate": 150,
             "discount_percentage": 20, "is_bundle_child": 1},
            {"item_code": "CHILD-B", "qty": 2, "price_list_rate": 150,
             "discount_percentage": 20, "is_bundle_child": 1},
        ]

        decision = order_sync._reconcile_woo_line_prices(order, lines)

        self.assertEqual(decision["erpnext_line_total"], 480.0)
        self.assertIs(decision["matched"], True)

    def test_reconcile_prices_falls_back_to_total_without_subtotals(self):
        order = {"line_items": [{"total": "500.00"}]}
        lines = [{"item_code": "CAKE", "qty": 1, "price_list_rate": 500}]

        decision = order_sync._reconcile_woo_line_prices(order, lines)

        self.assertEqual(decision["compared_against"], "total")
        self.assertIs(decision["matched"], True)

    def test_price_tolerance_uses_the_larger_of_absolute_and_relative(self):
        self.assertEqual(order_sync._woo_price_tolerance(1000.0), 5.0)  # 0.5% of 1000
        self.assertEqual(order_sync._woo_price_tolerance(10.0), 1.0)    # absolute floor wins

    def test_price_tolerance_can_be_overridden_from_settings(self):
        settings = SimpleNamespace(woo_price_tolerance_amount=25, woo_price_tolerance_percent=0)
        self.assertEqual(order_sync._woo_price_tolerance(1000.0, settings), 25.0)

    def test_price_mismatch_flags_but_never_raises(self):
        flagged = {}
        logged = {}
        self.patch_attr(
            order_sync,
            "_flag_order_map_for_manual_review",
            lambda **kwargs: flagged.update(kwargs),
        )
        self.patch_attr(
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
        order_sync._flag_woo_price_mismatch(
            woo_id=17, invoice_name="ACC-SINV-9", decision=decision
        )

        self.assertEqual(flagged["woo_id"], 17)
        self.assertEqual(flagged["invoice_name"], "ACC-SINV-9")
        self.assertIn("ERPNext price list", flagged["reason"])
        self.assertEqual(logged["operation"], "PriceMismatch")
        self.assertEqual(logged["status"], "NeedsReview")


# ---------------------------------------------------------------------------
# F-21 — inbound Woo order notes
# ---------------------------------------------------------------------------

class TestInboundOrderNotes(ParityTestCase):
    def test_woo_note_is_ours_detects_our_own_outbound_signature(self):
        self.assertIs(order_sync._woo_note_is_ours("Ring the bell\n\n— Ahmed (Jarz POS)"), True)
        self.assertIs(order_sync._woo_note_is_ours("Ring the bell\n\n— Jarz POS"), True)
        self.assertIs(order_sync._woo_note_is_ours("Leave with the doorman"), False)

    def test_woo_note_is_ours_detects_a_note_we_already_pulled_in(self):
        body = f"Leave with the doorman\n\n{order_sync.WOO_ORDER_NOTE_MARKER}98"
        self.assertIs(order_sync._woo_note_is_ours(body), True)

    def test_sync_woo_order_notes_filters_system_and_our_own_notes(self):
        created = []
        self.patch_attr(order_sync, "_invoice_note_doctype_available", lambda: True)
        self.patch_attr(order_sync, "_materialize_changed_customer_note", lambda order, invoice: 0)
        self.patch_attr(
            order_sync,
            "_materialize_invoice_note",
            lambda invoice_name, body, *, marker, author=None: created.append(
                {"invoice": invoice_name, "body": body, "marker": marker, "author": author}
            )
            or True,
        )

        client = _FakeNotesClient(
            [
                {"id": 11, "author": "system",
                 "note": "Order status changed from Processing to Completed."},
                {"id": 12, "author": "Ahmed",
                 "note": "Called the customer\n\n— Ahmed (Jarz POS)"},
                {"id": 13, "author": "Shop Manager",
                 "note": "Customer asked for delivery after 8pm"},
                {"id": 14, "author": "Shop Manager", "note": "   "},
            ]
        )

        result = order_sync.sync_woo_order_notes(
            {"id": 5150},
            "ACC-SINV-0007",
            settings=SimpleNamespace(enable_inbound_order_notes=1),
            client=client,
        )

        self.assertEqual(client.requested, ["orders/5150/notes"])
        self.assertEqual(result["status"], "synced")
        self.assertEqual(result["created"], 1)
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0]["marker"], f"{order_sync.WOO_ORDER_NOTE_MARKER}13")
        self.assertTrue(created[0]["body"].startswith("Customer asked for delivery after 8pm"))
        self.assertTrue(created[0]["body"].endswith(f"{order_sync.WOO_ORDER_NOTE_MARKER}13"))
        self.assertEqual(created[0]["invoice"], "ACC-SINV-0007")

    def test_sync_woo_order_notes_respects_the_settings_switch(self):
        self.patch_attr(order_sync, "_invoice_note_doctype_available", lambda: True)
        client = _FakeNotesClient([{"id": 1, "author": "x", "note": "hello"}])

        result = order_sync.sync_woo_order_notes(
            {"id": 900},
            "ACC-SINV-0008",
            settings=SimpleNamespace(enable_inbound_order_notes=0),
            client=client,
        )

        self.assertEqual(
            result, {"status": "skipped", "created": 0, "examined": 0, "reason": "disabled"}
        )
        self.assertEqual(client.requested, [])

    def test_sync_woo_order_notes_degrades_when_the_doctype_is_absent(self):
        logs = self.capture_throttled_logs()
        self.patch_attr(order_sync, "_invoice_note_doctype_available", lambda: False)
        client = _FakeNotesClient([{"id": 1, "author": "x", "note": "hello"}])

        result = order_sync.sync_woo_order_notes(
            {"id": 901}, "ACC-SINV-0009", settings=SimpleNamespace(), client=client
        )

        self.assertEqual(result["reason"], "doctype_missing")
        self.assertEqual(client.requested, [])
        self.assertTrue(logs, "a missing DocType must degrade loudly, not silently")

    def test_enqueue_woo_order_note_sync_queues_after_commit(self):
        calls = []
        self.patch_attr(
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

        self.assertIs(queued, True)
        method, kwargs = calls[0]
        self.assertEqual(
            method,
            "jarz_woocommerce_integration.services.order_sync.sync_woo_order_notes_job",
        )
        # must not run before the invoice it enriches is committed
        self.assertIs(kwargs["enqueue_after_commit"], True)
        self.assertEqual(kwargs["woo_order_id"], 5150)
        self.assertEqual(kwargs["invoice_name"], "ACC-SINV-0012")
        self.assertEqual(kwargs["customer_note"], "ring the bell")

    def test_enqueue_woo_order_note_sync_skips_when_disabled_or_unlinked(self):
        calls = []
        self.patch_attr(
            order_sync,
            "frappe",
            SimpleNamespace(
                enqueue=lambda *a, **k: calls.append(a),
                log_error=lambda *a, **k: None,
            ),
        )

        self.assertIs(
            order_sync.enqueue_woo_order_note_sync({"id": 1}, "", settings=SimpleNamespace()),
            False,
        )
        self.assertIs(
            order_sync.enqueue_woo_order_note_sync({}, "ACC-SINV-1", settings=SimpleNamespace()),
            False,
        )
        self.assertIs(
            order_sync.enqueue_woo_order_note_sync(
                {"id": 1},
                "ACC-SINV-1",
                settings=SimpleNamespace(enable_inbound_order_notes=0),
            ),
            False,
        )
        self.assertEqual(calls, [])

    def test_enqueue_woo_order_note_sync_never_raises(self):
        """An enrichment must not be able to fail an order sync."""

        def _boom(*args, **kwargs):
            raise RuntimeError("redis is down")

        self.patch_attr(
            order_sync,
            "frappe",
            SimpleNamespace(
                enqueue=_boom,
                log_error=lambda *a, **k: None,
                get_traceback=lambda: "tb",
            ),
        )

        self.assertIs(
            order_sync.enqueue_woo_order_note_sync(
                {"id": 7}, "ACC-SINV-7", settings=SimpleNamespace()
            ),
            False,
        )

    def test_inbound_order_notes_default_on_when_the_field_does_not_exist(self):
        self.assertIs(order_sync._inbound_order_notes_enabled(SimpleNamespace()), True)
        self.assertIs(
            order_sync._inbound_order_notes_enabled(
                SimpleNamespace(enable_inbound_order_notes=0)
            ),
            False,
        )
        self.assertIs(
            order_sync._inbound_order_notes_enabled(
                SimpleNamespace(enable_inbound_order_notes=1)
            ),
            True,
        )

    def test_customer_note_already_in_remarks_is_not_duplicated(self):
        self.patch_attr(
            order_sync,
            "frappe",
            SimpleNamespace(db=SimpleNamespace(get_value=lambda *a, **k: "Please call on arrival")),
        )
        self.patch_attr(
            order_sync,
            "_materialize_invoice_note",
            lambda *a, **k: self.fail("must not duplicate the create-path remarks"),
        )

        self.assertEqual(
            order_sync._materialize_changed_customer_note(
                {"customer_note": "Please call on arrival"}, "ACC-SINV-0010"
            ),
            0,
        )

    def test_changed_customer_note_is_materialised(self):
        created = []
        self.patch_attr(
            order_sync,
            "frappe",
            SimpleNamespace(db=SimpleNamespace(get_value=lambda *a, **k: "Please call on arrival")),
        )
        self.patch_attr(
            order_sync,
            "_materialize_invoice_note",
            lambda invoice_name, body, *, marker, author=None: created.append((body, marker))
            or True,
        )

        count = order_sync._materialize_changed_customer_note(
            {"customer_note": "Changed my mind — leave it with the doorman"}, "ACC-SINV-0011"
        )

        self.assertEqual(count, 1)
        body, marker = created[0]
        self.assertTrue(body.startswith("Changed my mind"))
        self.assertTrue(marker.startswith(order_sync.WOO_CUSTOMER_NOTE_MARKER))
        self.assertTrue(body.endswith(marker))


# ---------------------------------------------------------------------------
# F-07 — territory resolution visibility
# ---------------------------------------------------------------------------

class TestTerritoryAudit(ParityTestCase):
    def test_territory_audit_is_quiet_when_the_order_resolved_itself(self):
        logs = self.capture_throttled_logs()

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

        self.assertEqual(detail, "")
        self.assertEqual(logs, [])

    def test_territory_audit_reports_the_customer_fallback(self):
        logs = self.capture_throttled_logs()

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

        self.assertIn("Order-level territory unresolved", detail)
        self.assertIn("EGCAIRO", detail)
        self.assertTrue(logs, "an unresolved order territory must reach the Error Log")

    def test_territory_audit_reports_a_fully_unresolved_order(self):
        logs = self.capture_throttled_logs()

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

        self.assertIn("NO territory", detail)
        self.assertTrue(logs)

    def test_order_map_values_only_carry_a_territory_error_when_asked(self):
        # Replaces the whole frappe module: _build_order_map_snapshot_values calls
        # frappe.utils.now_datetime(), which needs a site.
        self.patch_attr(
            order_sync,
            "frappe",
            SimpleNamespace(utils=SimpleNamespace(now_datetime=lambda: "2026-08-19 12:00:00")),
        )
        snapshot = {"customer_name": "Someone"}

        without = order_sync._build_order_map_snapshot_values({"id": 1}, snapshot)
        self.assertNotIn("last_territory_error", without)

        with_error = order_sync._build_order_map_snapshot_values(
            {"id": 1}, snapshot, territory_error="unresolved"
        )
        self.assertEqual(with_error["last_territory_error"], "unresolved")


if __name__ == "__main__":
    unittest.main()
