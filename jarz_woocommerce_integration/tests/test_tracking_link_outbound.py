"""Customer tracking link + fulfilment status mapping (lane E3).

Three things are pinned here, and each one has already cost real money
somewhere:

1. **A failed delivery attempt is not a terminal Woo status.**
   COURIER_CONTRACTS.md section 1 keeps a failed stop at ``Out for Delivery``
   with a reason code, so the store must stay ``out-for-delivery`` too --
   never ``completed``, never ``cancelled``, never ``delivery-failed``. A
   customer told by email that their order failed, while a courier is coming
   back tomorrow, is a support call we invented.

2. **The tracking link actually reaches the order.** Minting a token is often
   the only change on an already-submitted invoice, so both the enqueue gate
   and the "is the remote copy stale?" check have to notice it. If either one
   shrugs, the store silently never grows a Track button and it looks like
   nobody has a token.

3. **``_address_signature_parts`` is still a six-tuple of the same six text
   fields in the same order.** Guarded again here, locally, because this
   feature works next door to the geo passthrough: fold a coordinate into that
   tuple and every pin update forks a duplicate Address, forever, silently.

Pure unittest + mocks: no site, no DB, no network.
"""

from __future__ import annotations

import inspect
import unittest
import unittest.mock
from types import SimpleNamespace

from jarz_woocommerce_integration.services import (
    customer_sync,
    geo_passthrough,
    outbound_sync,
    tracking_link,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class FakeInvoice:
    """Minimal Sales Invoice stand-in with the fields this lane reads."""

    def __init__(
        self,
        *,
        state: str = "Out for Delivery",
        docstatus: int = 1,
        custom_tracking_token: str | None = None,
        custom_tracking_url: str | None = None,
        custom_delivery_failure_reason: str | None = None,
        custom_delivery_attempt_no: int = 0,
    ):
        self.name = "ACC-SINV-2026-00042"
        self.customer = "CUST-0001"
        self.customer_name = "Test Customer"
        self.currency = "EGP"
        self.docstatus = docstatus
        self.custom_sales_invoice_state = state
        self.sales_invoice_state = state
        self.custom_tracking_token = custom_tracking_token
        self.custom_tracking_url = custom_tracking_url
        self.custom_delivery_failure_reason = custom_delivery_failure_reason
        self.custom_delivery_attempt_no = custom_delivery_attempt_no
        self.woo_order_id = 16901
        # Non-zero on purpose: it keeps set_paid False, so _build_paid_metadata
        # returns early and the payload assertions never depend on the site's
        # timezone (now_datetime needs System Settings).
        self.outstanding_amount = 10
        self.custom_payment_method = None
        self.mode_of_payment = None
        self.customer_address = None
        self.shipping_address_name = None
        self.items = []
        self.flags = SimpleNamespace(ignore_woo_outbound=False)
        self._before_save = None

    def get(self, fieldname, default=None):
        return getattr(self, fieldname, default)

    def get_doc_before_save(self):
        return self._before_save

    def has_value_changed(self, fieldname):
        previous = self.get_doc_before_save()
        if not previous:
            return False
        return previous.get(fieldname) != self.get(fieldname)


def _settings(*, enabled: bool = True, base_url: str = "https://erp.example.com/track"):
    return SimpleNamespace(
        enable_outbound_tracking_url=1 if enabled else 0,
        tracking_base_url=base_url,
    )


def _outbound_cfg():
    return outbound_sync.OutboundConfig(
        enable_customer_push=True,
        enable_order_push=True,
        payment_cod="cod",
        payment_instapay="instapay",
        payment_wallet="wallet",
        shipping_method_id="flat_rate",
        shipping_method_title="Shipping",
    )


TOKEN = "a1b2c3d4e5f60718293a4b5c6d7e8f90"


# ---------------------------------------------------------------------------
# 1. Status mapping
# ---------------------------------------------------------------------------

class TestFulfilmentStatusMapping(unittest.TestCase):
    def test_out_for_delivery_maps_to_the_custom_woo_status(self):
        self.assertEqual(
            outbound_sync._determine_status(FakeInvoice(state="Out for Delivery")),
            "out-for-delivery",
        )

    def test_delivered_maps_to_completed(self):
        self.assertEqual(
            outbound_sync._determine_status(FakeInvoice(state="Delivered")),
            "completed",
        )

    def test_cancelled_maps_to_cancelled(self):
        self.assertEqual(
            outbound_sync._determine_status(FakeInvoice(state="Cancelled", docstatus=2)),
            "cancelled",
        )

    def test_earlier_pipeline_states_map_to_processing(self):
        for state in ("Recieved", "In Progress", "Ready"):
            with self.subTest(state=state):
                self.assertEqual(
                    outbound_sync._determine_status(FakeInvoice(state=state)),
                    "processing",
                )


class TestFailedAttemptIsNotTerminal(unittest.TestCase):
    """The whole point of lane E3's status rule."""

    def _failed(self, attempt_no=2, reason="CUSTOMER_UNREACHABLE"):
        return FakeInvoice(
            state="Out for Delivery",
            custom_delivery_failure_reason=reason,
            custom_delivery_attempt_no=attempt_no,
        )

    def test_failed_attempt_keeps_the_order_out_for_delivery(self):
        self.assertEqual(outbound_sync._determine_status(self._failed()), "out-for-delivery")

    def test_failed_attempt_never_produces_a_terminal_status(self):
        status = outbound_sync._determine_status(self._failed())
        self.assertFalse(
            outbound_sync.is_terminal_woo_status(status),
            "A failed delivery attempt must never tell the customer the order is over. "
            "COURIER_CONTRACTS.md section 1: a failed stop stays Out for Delivery with a "
            "reason code; the courier comes back.",
        )
        self.assertNotIn(status, ("completed", "cancelled"))

    def test_failed_attempt_never_produces_the_delivery_failed_status(self):
        # wc-delivery-failed is registered by the WordPress plugin so a human can
        # set it in wp-admin. ERPNext never emits it.
        self.assertNotEqual(
            outbound_sync._determine_status(self._failed()),
            outbound_sync.WOO_STATUS_DELIVERY_FAILED,
        )

    def test_repeated_failures_still_keep_the_order_out_for_delivery(self):
        for attempt in (1, 2, 3, 9):
            with self.subTest(attempt=attempt):
                self.assertEqual(
                    outbound_sync._determine_status(self._failed(attempt_no=attempt)),
                    "out-for-delivery",
                )

    def test_a_failure_reason_without_an_attempt_count_is_still_not_terminal(self):
        invoice = FakeInvoice(
            state="Out for Delivery",
            custom_delivery_failure_reason="WRONG_ADDRESS",
            custom_delivery_attempt_no=0,
        )
        self.assertEqual(outbound_sync._determine_status(invoice), "out-for-delivery")

    def test_a_stale_failure_reason_cannot_un_complete_a_delivered_order(self):
        # The contract clears the reason on success; this must not depend on it.
        invoice = FakeInvoice(
            state="Delivered",
            custom_delivery_failure_reason="CUSTOMER_UNREACHABLE",
            custom_delivery_attempt_no=1,
        )
        self.assertEqual(outbound_sync._determine_status(invoice), "completed")

    def test_delivery_failed_is_not_an_approved_outbound_status(self):
        self.assertNotIn(
            outbound_sync.WOO_STATUS_DELIVERY_FAILED,
            outbound_sync._APPROVED_INVOICE_OUTBOUND_STATUSES,
        )

    def test_terminal_status_set_is_exactly_completed_cancelled_and_refunded(self):
        # ``refunded`` joined the set when outbound learned to emit it (F-15).
        # It belongs here for the same reason the other two do: it tells the
        # customer the order is over. What this test actually guards is the
        # invariant below it — that a failed delivery attempt can never produce
        # any of them — so the set is pinned rather than left to drift.
        self.assertEqual(
            set(outbound_sync._TERMINAL_WOO_STATUSES),
            {"completed", "cancelled", "refunded"},
        )

    def test_a_failed_attempt_produces_no_terminal_status(self):
        self.assertNotIn(
            outbound_sync.WOO_STATUS_DELIVERY_FAILED,
            outbound_sync._TERMINAL_WOO_STATUSES,
        )
        self.assertNotIn(
            outbound_sync.WOO_STATUS_OUT_FOR_DELIVERY,
            outbound_sync._TERMINAL_WOO_STATUSES,
        )

    def test_the_failure_fields_are_the_two_named_in_the_contract(self):
        self.assertEqual(
            outbound_sync._DELIVERY_FAILURE_FIELDS,
            ("custom_delivery_failure_reason", "custom_delivery_attempt_no"),
        )

    def test_a_failure_alone_does_not_enqueue_a_pointless_push(self):
        """A failed attempt changes nothing WooCommerce holds.

        The order stays out-for-delivery, so pushing would rewrite the store to
        exactly what it already says -- once per missed doorbell. The customer
        still learns about the attempt: it is on the ERPNext tracking page their
        browser is polling, which is the whole point of the split.
        """
        for fieldname in outbound_sync._DELIVERY_FAILURE_FIELDS:
            self.assertNotIn(fieldname, outbound_sync._OUTBOUND_RELEVANT_UPDATE_FIELDS)
            self.assertNotIn(fieldname, outbound_sync._INVOICE_OUTBOUND_PAYLOAD_FIELDS)

        previous = FakeInvoice(state="Out for Delivery")
        current = FakeInvoice(
            state="Out for Delivery",
            custom_delivery_failure_reason="CUSTOMER_UNREACHABLE",
            custom_delivery_attempt_no=1,
        )
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(_settings(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(
                 outbound_sync.frappe, "db",
                 SimpleNamespace(exists=lambda *a, **k: True),
             ), \
             unittest.mock.patch.object(
                 outbound_sync.frappe, "enqueue",
                 side_effect=lambda *a, **k: enqueue_calls.append((a, k)),
             ):
            outbound_sync.enqueue_invoice_sync(current, method="on_update_after_submit")

        self.assertEqual(enqueue_calls, [])

    def test_a_failed_attempt_alone_does_not_change_the_pushed_status(self):
        """Before/after a failure, the status we would push is identical."""
        clean = FakeInvoice(state="Out for Delivery")
        failed = FakeInvoice(
            state="Out for Delivery",
            custom_delivery_failure_reason="POSTPONED_BY_CUSTOMER",
            custom_delivery_attempt_no=1,
        )
        self.assertEqual(
            outbound_sync._determine_status(clean),
            outbound_sync._determine_status(failed),
        )


# ---------------------------------------------------------------------------
# 2. Tracking URL / token
# ---------------------------------------------------------------------------

class TestTrackingUrlComposition(unittest.TestCase):
    def test_token_is_appended_as_a_path_segment(self):
        self.assertEqual(
            tracking_link.build_tracking_url("https://erp.example.com/track", TOKEN),
            f"https://erp.example.com/track/{TOKEN}",
        )

    def test_trailing_slash_on_the_base_is_tolerated(self):
        self.assertEqual(
            tracking_link.build_tracking_url("https://erp.example.com/track/", TOKEN),
            f"https://erp.example.com/track/{TOKEN}",
        )

    def test_placeholder_lets_the_token_live_in_the_query_string(self):
        self.assertEqual(
            tracking_link.build_tracking_url("https://erp.example.com/track?t={token}", TOKEN),
            f"https://erp.example.com/track?t={TOKEN}",
        )

    def test_missing_base_url_yields_no_url(self):
        self.assertEqual(tracking_link.build_tracking_url("", TOKEN), "")

    def test_missing_token_yields_no_url(self):
        self.assertEqual(tracking_link.build_tracking_url("https://erp.example.com/track", ""), "")

    def test_non_http_base_is_refused(self):
        self.assertEqual(
            tracking_link.build_tracking_url("javascript:alert(1)//", TOKEN), ""
        )

    def test_a_token_that_is_not_opaque_is_refused(self):
        # This value reaches customers by email and WhatsApp; it is generated,
        # never typed, so anything shaped like prose or markup is a wrong field.
        for junk in (
            "Cash on delivery",
            "<script>alert(1)</script>",
            "../../etc/passwd",
            "tok en",
            "short",
        ):
            with self.subTest(junk=junk):
                self.assertEqual(tracking_link.normalize_token(junk), "")
                self.assertEqual(
                    tracking_link.build_tracking_url("https://erp.example.com/track", junk),
                    "",
                )

    def test_a_well_formed_token_survives_normalisation(self):
        self.assertEqual(tracking_link.normalize_token(f"  {TOKEN} "), TOKEN)


class TestResolveInvoiceTracking(unittest.TestCase):
    def test_url_is_built_from_the_configured_base_and_the_token(self):
        url, token = tracking_link.resolve_invoice_tracking(
            FakeInvoice(custom_tracking_token=TOKEN), _settings()
        )
        self.assertEqual(url, f"https://erp.example.com/track/{TOKEN}")
        self.assertEqual(token, TOKEN)

    def test_a_url_stored_on_the_invoice_wins_over_a_composed_one(self):
        url, token = tracking_link.resolve_invoice_tracking(
            FakeInvoice(
                custom_tracking_token=TOKEN,
                custom_tracking_url="https://track.example.com/x/abc",
            ),
            _settings(),
        )
        self.assertEqual(url, "https://track.example.com/x/abc")
        self.assertEqual(token, TOKEN)

    def test_no_token_means_no_url(self):
        self.assertEqual(
            tracking_link.resolve_invoice_tracking(FakeInvoice(), _settings()),
            ("", ""),
        )

    def test_a_dict_row_is_accepted_as_well_as_a_doc(self):
        url, token = tracking_link.resolve_invoice_tracking(
            {"custom_tracking_token": TOKEN, "custom_tracking_url": None}, _settings()
        )
        self.assertEqual(url, f"https://erp.example.com/track/{TOKEN}")
        self.assertEqual(token, TOKEN)


class TestBuildTrackingMetadata(unittest.TestCase):
    def setUp(self):
        tracking_link.reset_config_report_state()
        self.field_patch = unittest.mock.patch.object(
            tracking_link, "invoice_token_field_available", return_value=True
        )
        self.field_patch.start()
        self.addCleanup(self.field_patch.stop)

    def test_both_keys_are_emitted_with_the_exact_agreed_names(self):
        metadata = tracking_link.build_tracking_metadata(
            FakeInvoice(custom_tracking_token=TOKEN), _settings()
        )
        self.assertEqual(
            metadata,
            [
                {"key": "_jarz_tracking_url", "value": f"https://erp.example.com/track/{TOKEN}"},
                {"key": "_jarz_tracking_token", "value": TOKEN},
            ],
        )

    def test_the_meta_key_constants_match_the_wordpress_plugin(self):
        # Both halves of the contract are literal strings in two languages; this
        # is the only place they can be compared.
        self.assertEqual(tracking_link.TRACKING_URL_META_KEY, "_jarz_tracking_url")
        self.assertEqual(tracking_link.TRACKING_TOKEN_META_KEY, "_jarz_tracking_token")

    def test_kill_switch_off_emits_nothing(self):
        self.assertEqual(
            tracking_link.build_tracking_metadata(
                FakeInvoice(custom_tracking_token=TOKEN), _settings(enabled=False)
            ),
            [],
        )

    def test_no_token_emits_nothing(self):
        self.assertEqual(
            tracking_link.build_tracking_metadata(FakeInvoice(), _settings()), []
        )

    def test_blank_base_url_emits_nothing_and_files_one_error_log(self):
        with unittest.mock.patch.object(tracking_link.frappe, "log_error") as log_error, \
             unittest.mock.patch.object(
                 tracking_link.frappe, "cache",
                 return_value=SimpleNamespace(
                     get_value=lambda *a, **k: None, set_value=lambda *a, **k: None
                 ),
             ):
            first = tracking_link.build_tracking_metadata(
                FakeInvoice(custom_tracking_token=TOKEN), _settings(base_url="")
            )
            second = tracking_link.build_tracking_metadata(
                FakeInvoice(custom_tracking_token=TOKEN), _settings(base_url="")
            )

        self.assertEqual(first, [])
        self.assertEqual(second, [])
        # Loud once, not per order: this fires from a per-invoice code path.
        self.assertEqual(log_error.call_count, 1)

    def test_missing_token_field_emits_nothing_and_is_reported(self):
        """The pre-migration case: the flag is on but jarz_pos has not shipped.

        Reported rather than skipped in silence, because "no invoice has a token"
        and "the field does not exist" look identical from the outside and only one
        of them is a deploy that has not finished.
        """
        # A nested patch simply wins over setUp's; no stop/start dance needed.
        with unittest.mock.patch.object(
            tracking_link, "invoice_token_field_available", return_value=False
        ), unittest.mock.patch.object(tracking_link.frappe, "log_error") as log_error, \
             unittest.mock.patch.object(
                 tracking_link.frappe, "cache",
                 return_value=SimpleNamespace(
                     get_value=lambda *a, **k: None, set_value=lambda *a, **k: None
                 ),
             ):
            metadata = tracking_link.build_tracking_metadata(
                FakeInvoice(custom_tracking_token=TOKEN), _settings()
            )

        self.assertEqual(metadata, [])
        self.assertEqual(log_error.call_count, 1)

    def test_it_never_raises_on_a_broken_invoice(self):
        class Exploding:
            name = "ACC-SINV-BROKEN"

            def get(self, *_args, **_kwargs):
                raise RuntimeError("boom")

            def __getattr__(self, item):
                raise RuntimeError("boom")

        self.assertEqual(
            tracking_link.build_tracking_metadata(Exploding(), _settings()), []
        )


# ---------------------------------------------------------------------------
# 3. The push itself
# ---------------------------------------------------------------------------

class TestTrackingMetadataReachesTheOrderPayload(unittest.TestCase):
    def setUp(self):
        tracking_link.reset_config_report_state()

    def _payload(self, invoice, settings):
        line_items = [{
            "product_id": 101,
            "variation_id": None,
            "quantity": 1,
            "meta_data": [{"key": "erpnext_item_code", "value": "ITEM-001"}],
            "name": "ITEM-001",
        }]
        customer = SimpleNamespace(
            name=invoice.customer,
            customer_name="Test Customer",
            woo_customer_id="88",
            email_id="test@example.com",
            mobile_no="01000000000",
            phone=None,
        )
        address = {"address_1": "12 Nile Street", "email": "test@example.com", "phone": "01000000000"}

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(settings, _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync, "_collect_line_items", return_value=(line_items, [])), \
             unittest.mock.patch.object(outbound_sync, "_compute_shipping_total", return_value=0), \
             unittest.mock.patch.object(outbound_sync, "_build_customer_payload", return_value={"billing": dict(address), "shipping": dict(address)}), \
             unittest.mock.patch.object(outbound_sync, "get_customer_woo_id", return_value="88"), \
             unittest.mock.patch.object(tracking_link, "invoice_token_field_available", return_value=True), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc", return_value=customer):
            return outbound_sync._build_order_payload(invoice, _outbound_cfg())

    def _meta(self, payload):
        return {entry["key"]: entry["value"] for entry in payload.get("meta_data") or []}

    def test_order_payload_carries_the_tracking_url_and_token(self):
        payload = self._payload(
            FakeInvoice(state="Out for Delivery", custom_tracking_token=TOKEN), _settings()
        )
        meta = self._meta(payload)
        self.assertEqual(payload["status"], "out-for-delivery")
        self.assertEqual(meta["_jarz_tracking_url"], f"https://erp.example.com/track/{TOKEN}")
        self.assertEqual(meta["_jarz_tracking_token"], TOKEN)

    def test_a_failed_attempt_still_pushes_the_link_and_keeps_out_for_delivery(self):
        payload = self._payload(
            FakeInvoice(
                state="Out for Delivery",
                custom_tracking_token=TOKEN,
                custom_delivery_failure_reason="CUSTOMER_UNREACHABLE",
                custom_delivery_attempt_no=2,
            ),
            _settings(),
        )
        self.assertEqual(payload["status"], "out-for-delivery")
        self.assertFalse(outbound_sync.is_terminal_woo_status(payload["status"]))
        self.assertEqual(self._meta(payload)["_jarz_tracking_token"], TOKEN)

    def test_no_tracking_keys_when_the_kill_switch_is_off(self):
        payload = self._payload(
            FakeInvoice(custom_tracking_token=TOKEN), _settings(enabled=False)
        )
        meta = self._meta(payload)
        self.assertNotIn("_jarz_tracking_url", meta)
        self.assertNotIn("_jarz_tracking_token", meta)
        # The rest of the payload is untouched by the switch.
        self.assertIn("erpnext_sales_invoice", meta)

    def test_no_tracking_keys_when_the_invoice_has_no_token(self):
        meta = self._meta(self._payload(FakeInvoice(), _settings()))
        self.assertNotIn("_jarz_tracking_url", meta)


class TestTrackingLinkChangeIsNotSwallowed(unittest.TestCase):
    """A minted token must survive both "is it worth queueing?" gates."""

    def test_a_new_tracking_url_marks_the_remote_order_stale(self):
        existing_order = {"id": 16901, "status": "out-for-delivery", "meta_data": []}
        payload = {
            "status": "out-for-delivery",
            "meta_data": [
                {"key": "_jarz_tracking_url", "value": f"https://erp.example.com/track/{TOKEN}"},
                {"key": "_jarz_tracking_token", "value": TOKEN},
            ],
        }
        self.assertTrue(outbound_sync._order_payload_requires_update(existing_order, payload))

    def test_an_unchanged_tracking_url_does_not_mark_it_stale(self):
        meta = [
            {"key": "_jarz_tracking_url", "value": f"https://erp.example.com/track/{TOKEN}"},
            {"key": "_jarz_tracking_token", "value": TOKEN},
        ]
        existing_order = {"id": 16901, "status": "out-for-delivery", "meta_data": list(meta)}
        payload = {"status": "out-for-delivery", "meta_data": list(meta)}
        self.assertFalse(outbound_sync._order_payload_requires_update(existing_order, payload))

    def test_historical_orders_without_a_token_are_not_marked_stale(self):
        # The regression that would matter at scale: ~40k historical orders
        # suddenly "requiring an update" on the next reconcile sweep.
        existing_order = {"id": 16901, "status": "completed", "meta_data": []}
        payload = {"status": "completed", "meta_data": [{"key": "erpnext_sales_invoice", "value": "X"}]}
        self.assertFalse(outbound_sync._order_payload_requires_update(existing_order, payload))

    def test_tracking_meta_keys_are_in_the_drift_comparison_set(self):
        for key in tracking_link.TRACKING_META_KEYS:
            self.assertIn(key, outbound_sync._ORDER_SYNC_META_KEYS_TO_COMPARE)

    def test_minting_a_token_after_submit_still_enqueues_a_push(self):
        previous = FakeInvoice(state="Out for Delivery", custom_tracking_token=None)
        current = FakeInvoice(state="Out for Delivery", custom_tracking_token=TOKEN)
        current._before_save = previous
        enqueue_calls = []

        with unittest.mock.patch.object(outbound_sync, "_get_settings", return_value=(_settings(), _outbound_cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace(ignore_woo_outbound=False)), \
             unittest.mock.patch.object(
                 outbound_sync.frappe, "db",
                 SimpleNamespace(exists=lambda *a, **k: True),
             ), \
             unittest.mock.patch.object(
                 outbound_sync.frappe, "enqueue",
                 side_effect=lambda *a, **k: enqueue_calls.append((a, k)),
             ):
            outbound_sync.enqueue_invoice_sync(current, method="on_update_after_submit")

        self.assertEqual(len(enqueue_calls), 1)
        self.assertEqual(enqueue_calls[0][1]["invoice_name"], current.name)

    def test_the_tracking_fields_are_in_the_invoice_update_trigger_sets(self):
        for fieldname in tracking_link.INVOICE_TRACKING_FIELDS:
            self.assertIn(fieldname, outbound_sync._OUTBOUND_RELEVANT_UPDATE_FIELDS)
            self.assertIn(fieldname, outbound_sync._INVOICE_OUTBOUND_PAYLOAD_FIELDS)


class TestGeoPinMetaKeysMatchTheWordPressPlugin(unittest.TestCase):
    """The E1 half of the wire contract, pinned from the side that reads it.

    The WordPress plugin writes `_jarz_lat` / `_jarz_lng` (plus a combined
    `_jarz_location` copy). If either side is renamed alone the pin does not error
    -- it silently stops arriving, which is indistinguishable from "no customer
    used the map". These asserts are the tripwire.
    """

    def test_the_primary_lat_lng_keys_are_the_ones_the_plugin_writes(self):
        self.assertEqual(geo_passthrough._ORDER_LAT_META_KEYS[0], "_jarz_lat")
        self.assertEqual(geo_passthrough._ORDER_LNG_META_KEYS[0], "_jarz_lng")

    def test_the_combined_fallback_key_is_still_read(self):
        self.assertIn("_jarz_location", geo_passthrough._ORDER_COMBINED_META_KEYS)

    def test_an_order_carrying_the_plugin_keys_yields_a_pin(self):
        order = {
            "meta_data": [
                {"key": "_jarz_lat", "value": "30.0626"},
                {"key": "_jarz_lng", "value": "31.2497"},
                {"key": "_jarz_location", "value": "30.0626,31.2497"},
                {"key": "_jarz_pin_accuracy_m", "value": "18.4"},
            ],
        }
        pin = geo_passthrough.extract_order_pin(order)
        self.assertIsNotNone(pin)
        self.assertAlmostEqual(pin.latitude, 30.0626, places=6)
        self.assertAlmostEqual(pin.longitude, 31.2497, places=6)

    def test_the_plugin_accuracy_key_is_deliberately_not_consumed(self):
        # COURIER_CONTRACTS.md section 3: a Woo pin carries no accuracy, so the
        # Address is written with 0 ("not reported"). The key the plugin stores is
        # for a human staring at wp-admin; it must never quietly become an input,
        # because a radius that outlives its coordinates corrupts every distance
        # calculation downstream.
        source = inspect.getsource(geo_passthrough)
        self.assertNotIn("_jarz_pin_accuracy_m", source)

    def test_null_island_is_rejected_the_same_way_on_both_sides(self):
        order = {
            "meta_data": [
                {"key": "_jarz_lat", "value": "0"},
                {"key": "_jarz_lng", "value": "0"},
            ],
        }
        self.assertIsNone(geo_passthrough.extract_order_pin(order))


class TestTrackingStaysOutOfTheAddressWrites(unittest.TestCase):
    """The tracking work must not have widened any address-shaped door."""

    def test_no_tracking_field_joined_the_address_outbound_trigger_set(self):
        offenders = [
            fieldname
            for fieldname in outbound_sync._CUSTOMER_ADDRESS_OUTBOUND_UPDATE_FIELDS
            if fieldname.startswith("custom_")
        ]
        self.assertEqual(offenders, [])

    def test_the_geo_write_allow_list_is_unchanged_and_geo_only(self):
        self.assertEqual(
            set(geo_passthrough._ALLOWED_UPDATE_FIELDS),
            {
                "custom_latitude",
                "custom_longitude",
                "custom_geo_source",
                "custom_geo_confidence",
                "custom_geo_accuracy_m",
            },
        )
        self.assertNotIn("address_line2", geo_passthrough._ALLOWED_UPDATE_FIELDS)
        for fieldname in tracking_link.INVOICE_TRACKING_FIELDS:
            self.assertNotIn(fieldname, geo_passthrough._ALLOWED_UPDATE_FIELDS)

    def test_this_app_does_not_declare_the_tracking_token_field(self):
        # jarz_pos owns it, and tabSales Invoice is at MariaDB's 65,535-byte row
        # limit (247 columns) -- no app may add another varchar column to it.
        from jarz_woocommerce_integration.utils import custom_fields

        declared = {
            entry.get("fieldname")
            for entry in custom_fields.REQUIRED_FIELDS
            if entry.get("dt") == "Sales Invoice"
        }
        for fieldname in tracking_link.INVOICE_TRACKING_FIELDS:
            self.assertNotIn(fieldname, declared)

    def test_no_cross_app_import_sneaked_in(self):
        """Talking *about* the POS app is fine and necessary; importing it is not.

        The token this module reads is minted by the POS app, so the docstrings
        name it constantly -- a grep for the bare package name would fail on the
        very comments that document the boundary. Only an import crosses it.

        The needles are assembled at runtime so this file does not itself contain
        a literal cross-app import statement.
        """
        pos_app = "jarz" + "_pos"
        needles = ("import " + pos_app, "from " + pos_app)

        for module in (tracking_link, outbound_sync):
            source = inspect.getsource(module)
            for needle in needles:
                self.assertNotIn(
                    needle,
                    source,
                    f"{module.__name__} imports {pos_app}. The two apps are independent.",
                )


# ---------------------------------------------------------------------------
# 4. Address dedup signature -- the standing regression
# ---------------------------------------------------------------------------

class TestAddressSignatureIsStillSixTextFields(unittest.TestCase):
    """Duplicated on purpose from tests/test_address_signature_stability.py.

    The signature is the identity of an Address. Latitude/longitude entering it
    changes the signature on every pin update, the lookup misses, and the sync
    forks a second Address for the same customer -- silently, per order, forever.
    Any lane that works near the geo fields carries this guard so the breakage
    surfaces in the diff that causes it.
    """

    EXPECTED = ("address_line1", "address_line2", "city", "state", "postcode", "country")

    def test_parameter_names_and_order_are_unchanged(self):
        self.assertEqual(
            tuple(inspect.signature(customer_sync._address_signature_parts).parameters),
            self.EXPECTED,
        )

    def test_it_returns_a_six_tuple(self):
        signature = customer_sync._address_signature_parts(
            "12 Nile Street", "Apt 4", "Cairo", "Dokki", "12311", "Egypt"
        )
        self.assertIsInstance(signature, tuple)
        self.assertEqual(len(signature), 6)

    def test_a_coordinate_cannot_change_an_address_identity(self):
        row = {
            "address_line1": "12 Nile Street",
            "address_line2": "",
            "city": "Cairo",
            "state": "Dokki",
            "pincode": "12311",
            "country": "Egypt",
        }
        before = customer_sync._stored_address_signature(
            {**row, "custom_latitude": 30.0444, "custom_longitude": 31.2357}
        )
        after = customer_sync._stored_address_signature(
            {**row, "custom_latitude": 30.0500, "custom_longitude": 31.2400}
        )
        self.assertEqual(before, after)

    def test_the_signature_builder_mentions_no_geo_field(self):
        source = inspect.getsource(customer_sync._address_signature_parts).lower()
        for token in ("latitude", "longitude", "custom_geo", "_jarz_lat", "_jarz_lng"):
            self.assertNotIn(token, source)


if __name__ == "__main__":
    unittest.main()
