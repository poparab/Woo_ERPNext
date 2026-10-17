"""Geo passthrough follow-ups: billing fallback, pin removal, short links, re-sync safety.

* L3  Billing carries the location, shipping only text: the delivery snapshot
      and the shipping side use billing's pin. Billing never overrides a
      shipping location that exists.
* L4  The customer removes the pin on the website: the snapshot becomes blank
      coordinates + ``pin_removed`` (not an empty snapshot). Never set for an
      order that never had a pin; cleared when a pin comes back.
* L5  Short Maps links are expanded in a BACKGROUND job by a hardened expander.
      The network is mocked everywhere in this file.
* L1  A routine re-sync with UNCHANGED customer coordinates must not overwrite a
      newer staff correction (``pos_link``) on the Address.
"""

from __future__ import annotations

from contextlib import contextmanager
import unittest
from unittest.mock import MagicMock, patch

from jarz_woocommerce_integration.services import customer_sync, geo_passthrough as gp


CAIRO = (30.0444, 31.2357)
PARIS = (48.8566, 2.3522)
LINK_CAIRO = "https://maps.google.com/?q=30.0444,31.2357"
LINK_OTHER = "https://maps.google.com/?q=29.9000,31.1000"
SHORT = "https://maps.app.goo.gl/Xb8kq2Ld3sYp9"
PN_RANK = gp.CONFIDENCE_RANK


def _order(*, meta=None, billing=None, shipping=None, order_id=17001):
    return {
        "id": order_id,
        "number": str(order_id),
        "billing": billing if billing is not None else {
            "address_1": "12 Nile Street",
            "address_2": "",
            "city": "Cairo",
            "country": "EG",
        },
        "shipping": shipping if shipping is not None else {},
        "meta_data": meta if meta is not None else [],
    }


SHIP_TEXT = {"address_1": "5 Tahrir Square", "address_2": "", "city": "Cairo", "country": "EG"}


def _billing_with(address_2="", **extra):
    block = {"address_1": "12 Nile Street", "address_2": address_2, "city": "Cairo", "country": "EG"}
    block.update(extra)
    return block


def _ship_with(address_2="", **extra):
    block = dict(SHIP_TEXT)
    block["address_2"] = address_2
    block.update(extra)
    return block


def _meta(lat="30.0444", lng="31.2357"):
    return [{"key": "_dpn_lat", "value": lat}, {"key": "_dpn_lng", "value": lng}]


def _stored(lat=0.0, lng=0.0, source="", flags=""):
    return {
        "custom_delivery_pin_latitude": lat,
        "custom_delivery_pin_longitude": lng,
        "custom_delivery_pin_source": source,
        "custom_delivery_pin_flag": flags,
    }


@contextmanager
def _snapshot_db(stored_row, *, address_row=None, address_rows=None):
    """Stub the SI snapshot reads/writes, Address reads and Comment inserts."""
    writes: list[dict] = []
    comments: list[dict] = []

    def _get_value(doctype, name=None, fieldname=None, *args, **kwargs):
        if doctype == "Sales Invoice" and isinstance(fieldname, (list, tuple)) and \
                tuple(fieldname) == tuple(gp.INVOICE_PIN_FIELDS):
            return dict(stored_row) if stored_row is not None else None
        if doctype == "Sales Invoice":
            return address_row or {}
        if doctype == "Address":
            return (address_rows or {}).get(name)
        return None

    def _set_value(doctype, name, fieldname, value=None, **kwargs):
        writes.append({"doctype": doctype, "name": name, "fieldname": fieldname, "kwargs": kwargs})

    def _get_doc(payload, *args, **kwargs):
        doc = MagicMock()
        doc.insert.side_effect = lambda **kw: comments.append(payload)
        return doc

    with patch.object(gp, "invoice_pin_fields_available", return_value=True), \
         patch.object(gp.frappe.db, "get_value", side_effect=_get_value), \
         patch.object(gp.frappe.db, "set_value", side_effect=_set_value), \
         patch.object(gp.frappe, "get_doc", side_effect=_get_doc):
        yield writes, comments


@contextmanager
def _no_queue():
    """frappe.enqueue + cache stubs so the snapshot writer's enqueue is observable."""
    cache = MagicMock()
    cache.get_value.return_value = None
    with patch.object(gp.frappe, "enqueue", create=True) as enqueue, \
         patch.object(gp.frappe, "cache", create=True, return_value=cache):
        yield enqueue, cache


def _no_network(*_args, **_kwargs):
    raise AssertionError("network I/O in the order-sync path")


# ---------------------------------------------------------------------------
# L3 -- billing fallback
# ---------------------------------------------------------------------------


class TestBillingFallback(unittest.TestCase):
    def test_billing_link_with_text_only_shipping_pins_the_delivery(self):
        order = _order(billing=_billing_with(LINK_CAIRO), shipping=_ship_with())
        delivery = gp.resolve_delivery_pin(order)
        self.assertEqual((delivery["latitude"], delivery["longitude"]), CAIRO)
        self.assertEqual(delivery["source"], "maps_link")
        self.assertEqual(delivery["flags"], "")

    def test_billing_coordinates_keep_their_source_label(self):
        order = _order(
            billing=_billing_with(latitude="30.0444", longitude="31.2357"),
            shipping=_ship_with(),
        )
        delivery = gp.resolve_delivery_pin(order)
        self.assertEqual((delivery["latitude"], delivery["longitude"]), CAIRO)
        self.assertEqual(delivery["source"], "address_coordinates")

    def test_order_pins_share_the_fallback_and_flag_it(self):
        order = _order(billing=_billing_with(LINK_CAIRO), shipping=_ship_with())
        billing_pin, shipping_pin, from_billing = gp.resolve_order_pins_ex(order)
        self.assertEqual((billing_pin.latitude, billing_pin.longitude), CAIRO)
        self.assertEqual((shipping_pin.latitude, shipping_pin.longitude), CAIRO)
        self.assertTrue(from_billing)
        self.assertEqual(gp.resolve_order_pins(order), (billing_pin, shipping_pin))

    def test_shipping_location_is_never_overridden_by_billing(self):
        order = _order(billing=_billing_with(LINK_OTHER), shipping=_ship_with(LINK_CAIRO))
        delivery = gp.resolve_delivery_pin(order)
        _b, shipping_pin, from_billing = gp.resolve_order_pins_ex(order)
        self.assertEqual((delivery["latitude"], delivery["longitude"]), CAIRO)
        self.assertEqual((shipping_pin.latitude, shipping_pin.longitude), CAIRO)
        self.assertFalse(from_billing)

    def test_order_meta_pin_outranks_the_billing_fallback(self):
        order = _order(
            meta=_meta(), billing=_billing_with(LINK_OTHER), shipping=_ship_with()
        )
        delivery = gp.resolve_delivery_pin(order)
        _b, shipping_pin, from_billing = gp.resolve_order_pins_ex(order)
        self.assertEqual((delivery["latitude"], delivery["longitude"]), CAIRO)
        self.assertEqual((shipping_pin.latitude, shipping_pin.longitude), CAIRO)
        self.assertFalse(from_billing)

    def test_no_fallback_flag_when_billing_is_the_destination(self):
        order = _order(billing=_billing_with(LINK_CAIRO))
        _b, shipping_pin, from_billing = gp.resolve_order_pins_ex(order)
        self.assertIsNone(shipping_pin)
        self.assertFalse(from_billing)

    def test_billing_pin_outside_egypt_is_flagged_not_used(self):
        link = "https://maps.google.com/?q=48.8566,2.3522"
        order = _order(billing=_billing_with(link), shipping=_ship_with())
        delivery = gp.resolve_delivery_pin(order)
        self.assertEqual((delivery["latitude"], delivery["longitude"]), (0.0, 0.0))
        self.assertEqual(delivery["flags"], "outside_egypt")

    def test_non_dict_payload_is_survivable(self):
        self.assertEqual(gp.resolve_order_pins_ex(None), (None, None, False))

    def test_apply_order_pins_writes_the_borrowed_pin_fill_only(self):
        order = _order(billing=_billing_with(LINK_CAIRO), shipping=_ship_with())
        with patch.object(gp, "apply_geo_pin", return_value=True) as apply_pin:
            gp.apply_order_pins(order, billing_address="ADDR-B", shipping_address="ADDR-S")
        calls = {call.args[0]: call.kwargs for call in apply_pin.call_args_list}
        self.assertEqual(calls["ADDR-B"], {})
        self.assertEqual(calls["ADDR-S"], {"fill_only": True})

    def test_apply_order_pins_same_address_is_not_fill_only(self):
        order = _order(billing=_billing_with(LINK_CAIRO), shipping=_ship_with())
        with patch.object(gp, "apply_geo_pin", return_value=True) as apply_pin:
            gp.apply_order_pins(order, billing_address="ADDR-1", shipping_address="ADDR-1")
        for call in apply_pin.call_args_list:
            self.assertEqual(call.kwargs, {})

    def test_fill_only_refuses_an_address_that_already_has_a_pin(self):
        row = {
            "custom_geo_source": "customer_pin",
            "custom_geo_confidence": PN_RANK["customer_pin"],
            "custom_latitude": 29.9,
            "custom_longitude": 31.1,
        }
        with patch.object(gp, "geo_fields_available", return_value=True), \
             patch.object(gp.frappe.db, "get_value", return_value=row), \
             patch.object(gp.frappe.db, "set_value") as set_value:
            applied = gp.apply_geo_pin("ADDR-S", *CAIRO, fill_only=True)
        self.assertFalse(applied)
        set_value.assert_not_called()

    def test_fill_only_writes_onto_an_empty_or_centroid_address(self):
        for row in (
            {"custom_geo_source": None, "custom_geo_confidence": 0},
            {
                "custom_geo_source": "territory_centroid",
                "custom_geo_confidence": PN_RANK["territory_centroid"],
                "custom_latitude": 30.0,
                "custom_longitude": 31.0,
            },
        ):
            with patch.object(gp, "geo_fields_available", return_value=True), \
                 patch.object(gp.frappe.db, "get_value", return_value=row), \
                 patch.object(gp.frappe.db, "set_value") as set_value:
                applied = gp.apply_geo_pin("ADDR-S", *CAIRO, fill_only=True)
            self.assertTrue(applied)
            set_value.assert_called_once()

    def test_customer_sync_borrowed_shipping_pin_is_fill_only(self):
        order = _order(
            billing=_billing_with(LINK_CAIRO, email="a@example.com", first_name="A", last_name="B"),
            shipping=_ship_with(),
        )
        apply_pin = MagicMock(return_value=True)
        names = iter(["ADDR-B", "ADDR-S"])
        with patch.object(customer_sync, "_ensure_customer", return_value="CUST-1"), \
             patch.object(customer_sync, "_find_existing_address_for_customer", return_value=None), \
             patch.object(customer_sync, "_create_address", side_effect=lambda *a, **k: next(names)), \
             patch.object(customer_sync, "_set_address_as_default"), \
             patch.object(customer_sync, "_resolve_country", return_value="Egypt"), \
             patch.object(customer_sync, "_resolve_territory_from_state", return_value=None), \
             patch.object(customer_sync, "_same_source_address", return_value=False), \
             patch.object(customer_sync, "_last_received_delivery_pin", return_value=None), \
             patch.object(customer_sync, "_apply_geo_pin", apply_pin):
            customer_sync.ensure_customer_with_addresses(order, MagicMock())
        calls = {call.args[0]: call.kwargs for call in apply_pin.call_args_list}
        self.assertEqual(calls["ADDR-B"], {})
        self.assertEqual(calls["ADDR-S"], {"fill_only": True})


# ---------------------------------------------------------------------------
# L4 -- pin removed
# ---------------------------------------------------------------------------


class TestPinRemoved(unittest.TestCase):
    def test_flag_is_a_known_token_with_an_exact_value(self):
        self.assertEqual(gp.PIN_FLAG_PIN_REMOVED, "pin_removed")
        self.assertEqual(gp._normalize_flags("pin_removed,bogus"), "pin_removed")

    def test_removal_blanks_the_coordinates_and_sets_the_flag(self):
        order = _order()  # the website no longer sends a pin
        with _snapshot_db(_stored(*CAIRO, "droppin", "")) as (writes, comments), _no_queue():
            outcome = gp._write_invoice_pin_snapshot(order, "SINV-1")
        self.assertTrue(outcome["written"])
        payload = writes[0]["fieldname"]
        self.assertEqual(payload["custom_delivery_pin_flag"], "pin_removed")
        self.assertEqual(payload["custom_delivery_pin_latitude"], 0.0)
        self.assertEqual(payload["custom_delivery_pin_longitude"], 0.0)
        self.assertEqual(payload["custom_delivery_pin_source"], "")
        self.assertEqual(len(comments), 1)  # the dispatcher is told once

    def test_never_set_for_an_order_that_never_had_a_pin(self):
        with _snapshot_db(_stored()) as (writes, comments), _no_queue():
            outcome = gp._write_invoice_pin_snapshot(_order(), "SINV-1")
        self.assertFalse(outcome["written"])
        self.assertEqual(writes, [])
        self.assertEqual(comments, [])

    def test_a_removed_pin_stays_removed_without_rewriting(self):
        stored = _stored(0.0, 0.0, "", "pin_removed")
        with _snapshot_db(stored) as (writes, _c), _no_queue():
            outcome = gp._write_invoice_pin_snapshot(_order(), "SINV-1")
            differs = gp.invoice_pin_snapshot_differs(_order(), "SINV-1")
        self.assertFalse(outcome["written"])
        self.assertEqual(writes, [])
        self.assertFalse(differs)

    def test_poll_precheck_requeues_a_removal_exactly_once(self):
        with _snapshot_db(_stored(*CAIRO, "droppin", "")):
            self.assertTrue(gp.invoice_pin_snapshot_differs(_order(), "SINV-1"))
        with _snapshot_db(_stored(0.0, 0.0, "", "pin_removed")):
            self.assertFalse(gp.invoice_pin_snapshot_differs(_order(), "SINV-1"))

    def test_a_new_pin_clears_the_flag(self):
        stored = _stored(0.0, 0.0, "", "pin_removed")
        with _snapshot_db(stored) as (writes, _c), _no_queue():
            outcome = gp._write_invoice_pin_snapshot(_order(meta=_meta()), "SINV-1")
        self.assertTrue(outcome["written"])
        payload = writes[0]["fieldname"]
        self.assertEqual(payload["custom_delivery_pin_flag"], "")
        self.assertEqual(
            (payload["custom_delivery_pin_latitude"], payload["custom_delivery_pin_longitude"]), CAIRO
        )

    def test_another_flag_takes_the_place_of_pin_removed(self):
        """An outside-Egypt pin already blanks the card; no second flag."""
        order = _order(meta=_meta("48.8566", "2.3522"))
        with _snapshot_db(_stored(*CAIRO, "droppin", "")) as (writes, _c), _no_queue():
            gp._write_invoice_pin_snapshot(order, "SINV-1")
        self.assertEqual(writes[0]["fieldname"]["custom_delivery_pin_flag"], "outside_egypt")

    def test_removal_does_not_move_the_address(self):
        apply_pins = MagicMock()
        with _snapshot_db(_stored(*CAIRO, "droppin", "")), _no_queue(), \
             patch.object(gp, "apply_order_pins", apply_pins):
            result = gp.refresh_invoice_pins(_order(), "SINV-1")
        self.assertTrue(result["snapshot_written"])
        self.assertFalse(result["address_refreshed"])
        apply_pins.assert_not_called()

    def test_removal_comment_text_is_bilingual(self):
        text = gp._flag_comment_text("pin_removed", _order())
        self.assertIn("removed the map pin", text)
        self.assertIn("دبوس", text)


# ---------------------------------------------------------------------------
# L5 -- the hardened expander (network mocked)
# ---------------------------------------------------------------------------


def _chain(*steps):
    """A ``_redirect_status_and_headers`` double: one ``(status, headers)`` per call."""
    queue = list(steps)
    seen: list[tuple[str, str]] = []

    def fake(url, method, timeout):
        seen.append((method, url))
        return queue.pop(0)

    return fake, seen


class TestExpandShortLink(unittest.TestCase):
    def _expand(self, *steps, **kwargs):
        fake, seen = _chain(*steps)
        with patch.object(gp, "_redirect_status_and_headers", fake):
            return gp.expand_short_link(kwargs.pop("url", SHORT), **kwargs), seen

    def test_follows_a_redirect_to_the_real_url(self):
        expanded, seen = self._expand(
            (302, {"Location": LINK_CAIRO}),
            (200, {}),
        )
        self.assertEqual(expanded, LINK_CAIRO)
        self.assertEqual([call[0] for call in seen], ["HEAD", "HEAD"])

    def test_location_header_is_matched_case_insensitively(self):
        expanded, _seen = self._expand((301, {"location": LINK_CAIRO}), (200, {}))
        self.assertEqual(expanded, LINK_CAIRO)

    def test_relative_location_is_resolved_against_the_current_hop(self):
        expanded, seen = self._expand((302, {"Location": "/maps?q=30.0444,31.2357"}), (200, {}))
        # The relative hop is resolved against the current (allowlisted) host...
        self.assertEqual(seen[1][1], "https://maps.app.goo.gl/maps?q=30.0444,31.2357")
        # ...and landing back on a shortener is unresolved.
        self.assertEqual(expanded, "")

    def test_head_refused_retries_the_same_hop_with_get(self):
        expanded, seen = self._expand(
            (405, {}),
            (302, {"Location": LINK_CAIRO}),
            (200, {}),
        )
        self.assertEqual(expanded, LINK_CAIRO)
        self.assertEqual([call[0] for call in seen][:2], ["HEAD", "GET"])

    def test_legacy_http_shortener_is_upgraded_to_https(self):
        expanded, seen = self._expand(
            (302, {"Location": LINK_CAIRO}),
            (200, {}),
            url="http://goo.gl/maps/AbCdEf",
        )
        self.assertEqual(expanded, LINK_CAIRO)
        self.assertEqual(seen[0][1], "https://goo.gl/maps/AbCdEf")

    def test_a_hop_off_the_allowlist_ends_the_chain_without_being_fetched(self):
        expanded, seen = self._expand((302, {"Location": "https://evil.example/steal"}))
        self.assertEqual(expanded, "")
        self.assertEqual(len(seen), 1)

    def test_a_downgrade_to_plain_http_on_a_later_hop_is_refused(self):
        expanded, seen = self._expand((302, {"Location": "http://maps.google.com/?q=1,2"}))
        self.assertEqual(expanded, "")
        self.assertEqual(len(seen), 1)

    def test_start_host_off_the_allowlist_is_never_fetched(self):
        fake = MagicMock()
        with patch.object(gp, "_redirect_status_and_headers", fake):
            self.assertEqual(gp.expand_short_link("https://evil.example/x"), "")
            self.assertEqual(gp.expand_short_link("ftp://maps.app.goo.gl/x"), "")
            self.assertEqual(gp.expand_short_link(""), "")
            self.assertEqual(gp.expand_short_link(None), "")
        fake.assert_not_called()

    def test_hop_count_is_bounded(self):
        calls = []

        def loop(url, method, timeout):
            calls.append(url)
            return 302, {"Location": "https://maps.app.goo.gl/next"}

        with patch.object(gp, "_redirect_status_and_headers", loop):
            self.assertEqual(gp.expand_short_link(SHORT), "")
        self.assertLessEqual(len(calls), gp.SHORT_LINK_MAX_HOPS + 1)

    def test_error_status_is_unresolved(self):
        expanded, _seen = self._expand((404, {}))
        self.assertEqual(expanded, "")

    def test_a_request_error_never_raises(self):
        with patch.object(gp, "_redirect_status_and_headers", side_effect=OSError("boom")):
            self.assertEqual(gp.expand_short_link(SHORT), "")

    def test_one_overall_deadline(self):
        clock = iter([0.0, 1.0, 100.0, 200.0])
        with patch.object(gp.time, "monotonic", lambda: next(clock)), \
             patch.object(
                 gp, "_redirect_status_and_headers",
                 return_value=(302, {"Location": "https://maps.app.goo.gl/b"}),
             ) as request:
            self.assertEqual(gp.expand_short_link(SHORT, timeout=5.0), "")
        self.assertEqual(request.call_count, 1)  # the second hop never started

    def test_deadline_is_capped(self):
        remaining = []

        def fake(url, method, timeout):
            remaining.append(timeout)
            return 200, {}

        with patch.object(gp, "_redirect_status_and_headers", fake):
            gp.expand_short_link(LINK_CAIRO, timeout=999)
        self.assertLessEqual(remaining[0], 8.0)

    def test_apple_maps_is_an_allowed_landing_host(self):
        apple = "https://maps.apple.com/?ll=30.0444,31.2357"
        expanded, _seen = self._expand((302, {"Location": apple}), (200, {}))
        self.assertEqual(expanded, apple)


class TestRealRequestHelper(unittest.TestCase):
    """The one function that touches urllib: redirects off, body unread."""

    def test_redirects_are_not_followed_and_the_body_is_never_read(self):
        import urllib.request

        response = MagicMock()
        response.status = 302
        response.headers = {"Location": LINK_CAIRO}
        opener = MagicMock()
        opener.open.return_value = response
        handlers = []

        def build_opener(*given):
            handlers.extend(given)
            return opener

        with patch.object(urllib.request, "build_opener", build_opener):
            status, headers = gp._redirect_status_and_headers(SHORT, "HEAD", 2.0)

        self.assertEqual((status, headers["Location"]), (302, LINK_CAIRO))
        response.read.assert_not_called()
        response.close.assert_called_once()
        self.assertEqual(len(handlers), 1)
        # The handler refuses to build a follow-up request: urllib stops at the 3xx.
        self.assertIsNone(handlers[0]().redirect_request(None, None, 302, "Found", {}, LINK_CAIRO))
        _args, kwargs = opener.open.call_args
        self.assertEqual(kwargs.get("timeout", _args[1] if len(_args) > 1 else None), 2.0)


# ---------------------------------------------------------------------------
# L5 -- finding the link, enqueueing, the job
# ---------------------------------------------------------------------------


class TestFindShortLink(unittest.TestCase):
    def test_finds_links_with_and_without_a_scheme(self):
        cases = [
            ("see https://maps.app.goo.gl/AbC123.", "https://maps.app.goo.gl/AbC123"),
            ("maps.app.goo.gl/AbC123", "maps.app.goo.gl/AbC123"),
            ("https://goo.gl/maps/xYz", "https://goo.gl/maps/xYz"),
            ("https://share.google/Qwe12", "https://share.google/Qwe12"),
            ("http://goo.gl/maps/old", "http://goo.gl/maps/old"),
        ]
        for text, expected in cases:
            order = _order(billing=_billing_with(text))
            self.assertEqual(gp.find_short_link(order), expected, text)

    def test_ignores_non_short_and_lookalike_hosts(self):
        for text in (LINK_CAIRO, "https://notgoo.gl/x", "https://evilmaps.app.goo.gl.example.com/x", ""):
            order = _order(billing=_billing_with(text))
            self.assertEqual(gp.find_short_link(order), "", text)

    def test_destination_then_billing_fallback(self):
        order = _order(billing=_billing_with(SHORT), shipping=_ship_with())
        self.assertEqual(gp.find_short_link(order), SHORT)

    def test_meta_location_value(self):
        order = _order(meta=[{"key": "_jarz_location", "value": SHORT}])
        self.assertEqual(gp.find_short_link(order), SHORT)

    def test_non_dict_is_survivable(self):
        self.assertEqual(gp.find_short_link(None), "")


class TestEnqueueShortLinkExpansion(unittest.TestCase):
    def test_a_short_link_only_order_queues_one_short_queue_job(self):
        order = _order(billing=_billing_with(SHORT))
        with _snapshot_db(_stored()), _no_queue() as (enqueue, _cache), \
             patch.object(gp, "expand_short_link", _no_network):
            outcome = gp._write_invoice_pin_snapshot(order, "SINV-1")
        self.assertFalse(outcome["written"])  # nothing to store yet
        enqueue.assert_called_once()
        kwargs = enqueue.call_args.kwargs
        self.assertEqual(enqueue.call_args.args[0], "jarz_woocommerce_integration.services.geo_passthrough.expand_short_link_job")
        self.assertEqual(kwargs["queue"], "short")
        self.assertTrue(kwargs["enqueue_after_commit"])
        self.assertTrue(kwargs["deduplicate"])
        self.assertEqual(kwargs["invoice_name"], "SINV-1")
        self.assertEqual(kwargs["link"], SHORT)
        self.assertTrue(kwargs["job_id"].startswith("woo_geo_expand_SINV-1_"))

    def test_the_payload_is_trimmed_to_geo_fields(self):
        order = _order(
            meta=[{"key": "_jarz_location", "value": SHORT}, {"key": "_other", "value": "x"}],
            billing=_billing_with(SHORT, email="private@example.com", phone="0100"),
        )
        with _no_queue() as (enqueue, _cache):
            gp.enqueue_short_link_expansion(order, "SINV-1", gp._snapshot_values(gp._empty_delivery_pin()), _stored())
        sliced = enqueue.call_args.kwargs["order_slice"]
        self.assertNotIn("email", sliced["billing"])
        self.assertNotIn("phone", sliced["billing"])
        self.assertEqual([m["key"] for m in sliced["meta_data"]], ["_jarz_location"])

    def test_no_job_when_an_inline_pin_already_resolved(self):
        order = _order(billing=_billing_with(LINK_CAIRO), meta=[{"key": "_jarz_location", "value": SHORT}])
        with _snapshot_db(_stored()), _no_queue() as (enqueue, _cache):
            gp._write_invoice_pin_snapshot(order, "SINV-1")
        enqueue.assert_not_called()

    def test_no_job_when_the_snapshot_already_has_coordinates(self):
        order = _order(billing=_billing_with(SHORT))
        with _snapshot_db(_stored(*CAIRO, "maps_link", "")), _no_queue() as (enqueue, _cache):
            gp._write_invoice_pin_snapshot(order, "SINV-1")
        enqueue.assert_not_called()

    def test_no_job_when_the_link_failed_recently(self):
        order = _order(billing=_billing_with(SHORT))
        with _snapshot_db(_stored()), _no_queue() as (enqueue, cache):
            cache.get_value.return_value = "1"
            gp._write_invoice_pin_snapshot(order, "SINV-1")
        enqueue.assert_not_called()

    def test_no_job_without_a_link(self):
        with _snapshot_db(_stored()), _no_queue() as (enqueue, _cache):
            gp._write_invoice_pin_snapshot(_order(), "SINV-1")
        enqueue.assert_not_called()

    def test_enqueue_failure_never_breaks_the_snapshot(self):
        order = _order(billing=_billing_with(SHORT))
        with _snapshot_db(_stored()), _no_queue() as (enqueue, _cache):
            enqueue.side_effect = Exception("Too many queued background jobs")
            outcome = gp._write_invoice_pin_snapshot(order, "SINV-1")
        self.assertFalse(outcome["written"])

    def test_a_resolved_short_link_is_not_blanked_by_the_next_sync(self):
        """The payload still holds only the raw link; the stored pin must survive."""
        order = _order(billing=_billing_with(SHORT))
        stored = _stored(*CAIRO, "maps_link", "")
        with _snapshot_db(stored) as (writes, _c), _no_queue():
            outcome = gp._write_invoice_pin_snapshot(order, "SINV-1")
            differs = gp.invoice_pin_snapshot_differs(order, "SINV-1")
        self.assertFalse(outcome["written"])
        self.assertEqual(writes, [])
        self.assertFalse(differs)

    def test_replacing_the_short_link_by_nothing_is_a_removal(self):
        stored = _stored(*CAIRO, "maps_link", "")
        with _snapshot_db(stored) as (writes, _c), _no_queue():
            gp._write_invoice_pin_snapshot(_order(), "SINV-1")
        self.assertEqual(writes[0]["fieldname"]["custom_delivery_pin_flag"], "pin_removed")


class TestExpandShortLinkJob(unittest.TestCase):
    ADDRESSES = {"customer_address": "ADDR-B", "shipping_address_name": "ADDR-S"}

    def _slice(self, **kwargs):
        return gp._geo_order_slice(_order(billing=_billing_with(SHORT), **kwargs))

    def test_expanded_link_goes_through_the_normal_pipeline(self):
        with _snapshot_db(_stored(), address_row=self.ADDRESSES) as (writes, _c), _no_queue(), \
             patch.object(gp, "expand_short_link", return_value=LINK_CAIRO), \
             patch.object(gp, "apply_order_pins", return_value={"billing_applied": True, "shipping_applied": False}) as apply_pins:
            result = gp.expand_short_link_job("SINV-1", SHORT, self._slice())

        self.assertTrue(result["applied"])
        payload = writes[0]["fieldname"]
        self.assertEqual(
            (payload["custom_delivery_pin_latitude"], payload["custom_delivery_pin_longitude"]), CAIRO
        )
        self.assertEqual(payload["custom_delivery_pin_source"], "maps_link")
        apply_pins.assert_called_once()
        self.assertEqual(apply_pins.call_args.kwargs["billing_address"], "ADDR-B")
        self.assertEqual(apply_pins.call_args.kwargs["shipping_address"], "ADDR-S")
        self.assertTrue(apply_pins.call_args.kwargs["protect_pos_link"])

    def test_address_write_is_capped_at_customer_pin(self):
        calls = []
        row = {"custom_geo_source": None, "custom_geo_confidence": 0}
        with _snapshot_db(_stored(), address_row=self.ADDRESSES, address_rows={"ADDR-B": row, "ADDR-S": row}), \
             _no_queue(), \
             patch.object(gp, "expand_short_link", return_value=LINK_CAIRO), \
             patch.object(gp, "geo_fields_available", return_value=True), \
             patch.object(gp, "apply_geo_pin", side_effect=lambda *a, **k: calls.append((a, k)) or True):
            gp.expand_short_link_job("SINV-1", SHORT, self._slice())
        self.assertTrue(calls)
        for args, _kwargs in calls:
            self.assertEqual(args[3], gp.GEO_SOURCE_CUSTOMER_PIN)
            self.assertLessEqual(PN_RANK[args[3]], gp.MAX_WRITABLE_RANK)

    def test_expansion_outside_egypt_sets_the_flag_and_pins_nothing(self):
        far = "https://www.google.com/maps/@48.8566,2.3522,15z"
        with _snapshot_db(_stored(), address_row=self.ADDRESSES) as (writes, _c), _no_queue(), \
             patch.object(gp, "expand_short_link", return_value=far), \
             patch.object(gp, "apply_geo_pin") as apply_pin:
            result = gp.expand_short_link_job("SINV-1", SHORT, self._slice())
        self.assertTrue(result["applied"])
        payload = writes[0]["fieldname"]
        self.assertEqual(payload["custom_delivery_pin_flag"], "outside_egypt")
        self.assertEqual(payload["custom_delivery_pin_latitude"], 0.0)
        apply_pin.assert_not_called()

    def test_shared_place_link_stays_unresolved_quietly(self):
        place = "https://www.google.com/maps?q=Cafe+Name&ftid=0x14583fa:0x2b9e"
        log = MagicMock()
        with _snapshot_db(_stored(), address_row=self.ADDRESSES) as (writes, _c), \
             _no_queue() as (_enqueue, cache), \
             patch.object(gp, "LOGGER", log), \
             patch.object(gp.frappe, "log_error", create=True) as log_error, \
             patch.object(gp, "expand_short_link", return_value=place), \
             patch.object(gp, "apply_order_pins") as apply_pins:
            result = gp.expand_short_link_job("SINV-1", SHORT, self._slice())
        self.assertFalse(result["applied"])
        self.assertEqual(result["reason"], "unresolved")
        self.assertEqual(writes, [])
        apply_pins.assert_not_called()
        log_error.assert_not_called()
        log.warning.assert_not_called()
        log.error.assert_not_called()
        log.debug.assert_called_once()
        cache.set_value.assert_called_once()  # remembered: no re-fetch on every poll

    def test_failed_expansion_is_remembered_not_raised(self):
        with _snapshot_db(_stored(), address_row=self.ADDRESSES) as (writes, _c), \
             _no_queue() as (_enqueue, cache), \
             patch.object(gp, "expand_short_link", return_value=""):
            result = gp.expand_short_link_job("SINV-1", SHORT, self._slice())
        self.assertEqual(result["reason"], "unresolved")
        self.assertEqual(writes, [])
        cache.set_value.assert_called_once()

    def test_skipped_when_a_better_pin_arrived_meanwhile(self):
        expand = MagicMock(return_value=LINK_CAIRO)
        with _snapshot_db(_stored(*CAIRO, "droppin", ""), address_row=self.ADDRESSES) as (writes, _c), \
             _no_queue(), patch.object(gp, "expand_short_link", expand):
            result = gp.expand_short_link_job("SINV-1", SHORT, self._slice())
        self.assertFalse(result["applied"])
        self.assertEqual(writes, [])
        expand.assert_not_called()

    def test_skipped_when_an_address_holds_a_staff_pin(self):
        staff = {
            "custom_geo_source": "pos_link",
            "custom_geo_confidence": PN_RANK["pos_link"],
            "custom_latitude": 30.1,
            "custom_longitude": 31.3,
        }
        with _snapshot_db(_stored(), address_row=self.ADDRESSES, address_rows={"ADDR-B": staff}) as (writes, _c), \
             _no_queue(), \
             patch.object(gp, "expand_short_link", return_value=LINK_CAIRO), \
             patch.object(gp, "apply_order_pins") as apply_pins:
            result = gp.expand_short_link_job("SINV-1", SHORT, self._slice())
        self.assertEqual(result["reason"], "address_has_staff_pin")
        self.assertEqual(writes, [])
        apply_pins.assert_not_called()

    def test_job_never_raises(self):
        with _snapshot_db(_stored(), address_row=self.ADDRESSES), _no_queue(), \
             patch.object(gp, "expand_short_link", side_effect=Exception("boom")):
            result = gp.expand_short_link_job("SINV-1", SHORT, self._slice())
        self.assertFalse(result["applied"])

    def test_order_sync_path_does_no_network(self):
        order = _order(billing=_billing_with(SHORT), shipping=_ship_with())
        with _snapshot_db(_stored()), _no_queue(), \
             patch.object(gp, "expand_short_link", _no_network), \
             patch.object(gp, "_redirect_status_and_headers", _no_network):
            gp.apply_invoice_pin_snapshot(order, "SINV-1")
            gp.refresh_invoice_pins(order, "SINV-1")
            gp.invoice_pin_snapshot_differs(order, "SINV-1")
            gp.resolve_delivery_pin(order)
            gp.resolve_order_pins(order)


# ---------------------------------------------------------------------------
# L1 -- a routine re-sync must not undo a staff correction
# ---------------------------------------------------------------------------

STAFF_ROW = {
    "custom_geo_source": "pos_link",
    "custom_geo_confidence": PN_RANK["pos_link"],
    "custom_latitude": 30.1,
    "custom_longitude": 31.3,
}


class TestResyncKeepsStaffCorrection(unittest.TestCase):
    def test_protect_pos_link_skips_the_overwrite(self):
        with patch.object(gp, "geo_fields_available", return_value=True), \
             patch.object(gp.frappe.db, "get_value", return_value=dict(STAFF_ROW)), \
             patch.object(gp.frappe.db, "set_value") as set_value:
            applied = gp.apply_geo_pin("ADDR-1", *CAIRO, protect_pos_link=True)
        self.assertFalse(applied)
        set_value.assert_not_called()

    def test_without_the_guard_customer_pin_still_outranks_pos_link(self):
        """The ladder itself is unchanged: this is what the guard exists for."""
        with patch.object(gp, "geo_fields_available", return_value=True), \
             patch.object(gp.frappe.db, "get_value", return_value=dict(STAFF_ROW)), \
             patch.object(gp.frappe.db, "set_value") as set_value:
            self.assertTrue(gp.apply_geo_pin("ADDR-1", *CAIRO))
        set_value.assert_called_once()

    def test_the_guard_does_not_block_other_stored_sources(self):
        row = {
            "custom_geo_source": "customer_pin",
            "custom_geo_confidence": PN_RANK["customer_pin"],
            "custom_latitude": 29.9,
            "custom_longitude": 31.1,
        }
        with patch.object(gp, "geo_fields_available", return_value=True), \
             patch.object(gp.frappe.db, "get_value", return_value=row), \
             patch.object(gp.frappe.db, "set_value") as set_value:
            self.assertTrue(gp.apply_geo_pin("ADDR-1", *CAIRO, protect_pos_link=True))
        set_value.assert_called_once()

    def test_last_received_pin_reads_the_invoice_snapshot(self):
        order = _order(meta=_meta())
        map_row = {"erpnext_sales_invoice": "SINV-1", "status": "Synced"}

        def get_value(doctype, name=None, fieldname=None, *args, **kwargs):
            if doctype == "WooCommerce Order Map":
                return map_row
            return _stored(*CAIRO, "droppin", "")

        with patch.object(gp, "invoice_pin_fields_available", return_value=True), \
             patch.object(gp, "_order_map_link_field", return_value="erpnext_sales_invoice"), \
             patch.object(gp.frappe.db, "get_value", side_effect=get_value):
            self.assertEqual(gp.last_received_delivery_pin(order), CAIRO)

    def test_last_received_pin_is_none_for_new_unmapped_or_pinless_orders(self):
        order = _order(meta=_meta())
        with patch.object(gp, "invoice_pin_fields_available", return_value=True), \
             patch.object(gp, "_order_map_link_field", return_value="erpnext_sales_invoice"), \
             patch.object(gp.frappe.db, "get_value", return_value=None):
            self.assertIsNone(gp.last_received_delivery_pin(order))
        self.assertIsNone(gp.last_received_delivery_pin(None))
        self.assertIsNone(gp.last_received_delivery_pin({"meta_data": []}))

        def pinless(doctype, name=None, fieldname=None, *args, **kwargs):
            if doctype == "WooCommerce Order Map":
                return {"erpnext_sales_invoice": "SINV-1", "status": "Synced"}
            return _stored()

        with patch.object(gp, "invoice_pin_fields_available", return_value=True), \
             patch.object(gp, "_order_map_link_field", return_value="erpnext_sales_invoice"), \
             patch.object(gp.frappe.db, "get_value", side_effect=pinless):
            self.assertIsNone(gp.last_received_delivery_pin(order))

    def test_last_received_pin_ignores_a_retired_duplicate(self):
        row = {"erpnext_sales_invoice": "SINV-1", "status": "Retired Duplicate"}
        with patch.object(gp, "invoice_pin_fields_available", return_value=True), \
             patch.object(gp, "_order_map_link_field", return_value="erpnext_sales_invoice"), \
             patch.object(gp, "is_retired_duplicate_row", return_value=True), \
             patch.object(gp.frappe.db, "get_value", return_value=row):
            self.assertIsNone(gp.last_received_delivery_pin(_order(meta=_meta())))

    def _sync(self, order, *, last_received):
        apply_pin = MagicMock(return_value=True)
        with patch.object(customer_sync, "_ensure_customer", return_value="CUST-1"), \
             patch.object(customer_sync, "_find_existing_address_for_customer", return_value="ADDR-1"), \
             patch.object(customer_sync, "_create_address", return_value="ADDR-NEW"), \
             patch.object(customer_sync, "_set_address_as_default"), \
             patch.object(customer_sync, "_resolve_country", return_value="Egypt"), \
             patch.object(customer_sync, "_resolve_territory_from_state", return_value=None), \
             patch.object(customer_sync, "_last_received_delivery_pin", return_value=last_received), \
             patch.object(customer_sync, "_apply_geo_pin", apply_pin):
            customer_sync.ensure_customer_with_addresses(order, MagicMock())
        return apply_pin

    def test_unchanged_coordinates_are_applied_with_the_guard(self):
        apply_pin = self._sync(_order(meta=_meta()), last_received=CAIRO)
        apply_pin.assert_called_once_with(
            "ADDR-1", 30.0444, 31.2357, gp.GEO_SOURCE_CUSTOMER_PIN, protect_pos_link=True
        )

    def test_a_genuinely_new_pin_is_applied_unguarded(self):
        apply_pin = self._sync(_order(meta=_meta()), last_received=(29.9, 31.1))
        apply_pin.assert_called_once_with("ADDR-1", 30.0444, 31.2357, gp.GEO_SOURCE_CUSTOMER_PIN)

    def test_a_first_sync_applies_unguarded(self):
        apply_pin = self._sync(_order(meta=_meta()), last_received=None)
        apply_pin.assert_called_once_with("ADDR-1", 30.0444, 31.2357, gp.GEO_SOURCE_CUSTOMER_PIN)

    def test_a_failing_baseline_lookup_does_not_break_the_order(self):
        apply_pin = MagicMock(return_value=True)
        with patch.object(customer_sync, "_ensure_customer", return_value="CUST-1"), \
             patch.object(customer_sync, "_find_existing_address_for_customer", return_value="ADDR-1"), \
             patch.object(customer_sync, "_set_address_as_default"), \
             patch.object(customer_sync, "_resolve_country", return_value="Egypt"), \
             patch.object(customer_sync, "_resolve_territory_from_state", return_value=None), \
             patch.object(customer_sync, "_last_received_delivery_pin", side_effect=Exception("db down")), \
             patch.object(customer_sync, "_apply_geo_pin", apply_pin):
            customer, billing, _shipping = customer_sync.ensure_customer_with_addresses(
                _order(meta=_meta()), MagicMock()
            )
        self.assertEqual((customer, billing), ("CUST-1", "ADDR-1"))
        apply_pin.assert_called_once()

    def test_unchanged_refresh_branch_already_leaves_the_address_alone(self):
        """refresh_invoice_pins moves the Address only when the pin was edited."""
        order = _order(meta=_meta())
        apply_pins = MagicMock()
        with _snapshot_db(_stored(*CAIRO, "droppin", "")) as (writes, _c), _no_queue(), \
             patch.object(gp, "apply_order_pins", apply_pins):
            result = gp.refresh_invoice_pins(order, "SINV-1")
        self.assertEqual(result, {"snapshot_written": False, "address_refreshed": False})
        apply_pins.assert_not_called()


if __name__ == "__main__":
    unittest.main()
