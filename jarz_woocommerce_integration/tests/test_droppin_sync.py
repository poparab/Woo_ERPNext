"""Tests for the DropPin outbound push.

Four things are worth testing here and they are all failure modes that produce
*no error anywhere* when they go wrong:

* **The signature covers the exact bytes sent.** Re-serialising between signing
  and sending changes one byte of whitespace and every request comes back 401
  with a body that says only ``{"ok":false}``, by design. The test verifies the
  digest against an independently computed HMAC rather than against the
  function's own output.
* **An empty secret must not sign.** Their own sample receiver HMACs with an
  empty key when the secret is unset, so a missing configuration on either side
  produces signatures that any other party with an empty key can also make.
  This side refuses.
* **``updated_at`` is normalised and clamped.** DropPin discards positions older
  than the one it holds but accepts arbitrarily future ones. A single fix
  stamped hours ahead — which our own storage can produce, because it keeps the
  handset's string verbatim and that string is sometimes local-with-no-offset —
  is then stored, and every genuine fix afterwards is "older" and ignored. The
  marker freezes and nothing logs it.
* **Event ids are stable per fact, not per attempt.** DropPin ignores a repeat
  of the same ``event_id`` for 24 h, which is what makes our retries safe — and
  what makes a *re-opened* leg need a different id from the first one, or the
  map never opens the second time.

Pure ``unittest`` with mocks — no site, no network.
"""

import hashlib
import hmac
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import frappe  # noqa: F401  (binds frappe.local / frappe._)

from jarz_woocommerce_integration.services import droppin_sync

SECRET = "sk_test_do_not_use_in_production"


class SigningTests(unittest.TestCase):
    def test_signature_matches_an_independent_hmac(self):
        body = droppin_sync.encode_body({"wc_order_id": 1042, "status": "delivered"})
        header = droppin_sync.sign(body, SECRET, ts=1755700000)

        expected = hmac.new(
            SECRET.encode(), b"1755700000." + body, hashlib.sha256
        ).hexdigest()
        self.assertEqual(header, f"t=1755700000,v1={expected}")

    def test_header_shape_is_exactly_what_the_spec_documents(self):
        header = droppin_sync.sign(b"{}", SECRET, ts=1)
        t_part, v_part = header.split(",")
        self.assertEqual(t_part, "t=1")
        self.assertTrue(v_part.startswith("v1="))
        self.assertEqual(len(v_part[len("v1="):]), 64)  # lowercase sha256 hex
        self.assertEqual(v_part.lower(), v_part)

    def test_body_is_encoded_once_and_is_stable(self):
        payload = {"wc_order_id": 1042, "driver": {"name": "أحمد"}}
        first = droppin_sync.encode_body(payload)
        second = droppin_sync.encode_body(payload)
        self.assertEqual(first, second)
        # Arabic goes over this wire unescaped — the courier is named on an
        # Arabic page and \\u0623... would be shown literally by a naive reader.
        self.assertIn("أحمد".encode("utf-8"), first)
        self.assertNotIn(b"\\u", first)

    def test_body_has_no_incidental_whitespace(self):
        """Compactness is not cosmetic: it is the encoding that was signed."""
        body = droppin_sync.encode_body({"a": 1, "b": 2})
        self.assertEqual(body, b'{"a":1,"b":2}')

    def test_empty_secret_refuses_to_sign(self):
        self.assertIsNone(droppin_sync._signed_headers(b"{}", ""))
        self.assertIsNone(droppin_sync._signed_headers(b"{}", "   ".strip()))

    def test_a_real_secret_produces_both_required_headers(self):
        headers = droppin_sync._signed_headers(b"{}", SECRET)
        self.assertIsNotNone(headers)
        self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")
        self.assertTrue(headers["X-DropPin-Signature"].startswith("t="))


class UpdatedAtTests(unittest.TestCase):
    """The timestamp that silently freezes a marker."""

    def test_utc_iso_passes_through_as_utc(self):
        # A past timestamp on purpose: a value near "now" would be rewritten by
        # the future-clamp below and the assertion would be testing the clamp
        # instead of the pass-through.
        self.assertEqual(
            droppin_sync.normalise_updated_at("2026-08-20T15:44:12Z"),
            "2026-08-20T15:44:12Z",
        )

    def test_offset_is_converted_not_dropped(self):
        # 18:44:12+03:00 is 15:44:12Z. Dropping the offset instead of converting
        # is the three-hour error this function exists to prevent.
        self.assertEqual(
            droppin_sync.normalise_updated_at("2026-08-20T18:44:12+03:00"),
            "2026-08-20T15:44:12Z",
        )

    def test_output_always_carries_an_explicit_zone(self):
        for value in ("2026-08-26 18:44:12", "2026-08-26T18:44:12Z", None, ""):
            with self.subTest(value=value):
                self.assertTrue(droppin_sync.normalise_updated_at(value).endswith("Z"))

    def test_a_future_timestamp_is_clamped_to_now(self):
        ahead = datetime.now(timezone.utc) + timedelta(hours=3)
        out = droppin_sync.normalise_updated_at(ahead.isoformat())
        parsed = datetime.fromisoformat(out.replace("Z", "+00:00"))
        self.assertLessEqual(
            parsed,
            datetime.now(timezone.utc) + timedelta(seconds=5),
            "a fix hours in the future would freeze the marker until it passed",
        )

    def test_small_drift_is_preserved_rather_than_clamped(self):
        """Clock skew of a few seconds is normal and must not be rewritten, or
        every fix would carry the server's clock and the ordering DropPin relies
        on would come from the wrong machine."""
        ahead = datetime.now(timezone.utc) + timedelta(seconds=30)
        out = droppin_sync.normalise_updated_at(
            ahead.isoformat().replace("+00:00", "Z")
        )
        parsed = datetime.fromisoformat(out.replace("Z", "+00:00"))
        self.assertGreater(parsed, datetime.now(timezone.utc) + timedelta(seconds=5))

    def test_a_naive_timestamp_is_read_in_the_SITE_timezone(self):
        """The bug real-stack verification caught that the mocks could not.

        A handset writes a bare "YYYY-MM-DD HH:MM:SS" in Cairo time. The
        containers run UTC, so reading it with the host's zone made an 18:44
        Cairo fix into 18:44Z -- three hours ahead. That is the exact shape of
        failure this whole function exists to prevent, and the clamp only hid
        it by rewriting a timestamp that was perfectly recoverable.
        """
        from zoneinfo import ZoneInfo

        with patch.object(
            droppin_sync, "_site_timezone", return_value=ZoneInfo("Africa/Cairo")
        ):
            out = droppin_sync.normalise_updated_at("2026-08-20 18:44:12")
        self.assertEqual(out, "2026-08-20T15:44:12Z")

    def test_site_timezone_falls_back_to_utc_when_unreadable(self):
        self.assertIsNotNone(droppin_sync._site_timezone())

    def test_garbage_falls_back_to_now_instead_of_raising(self):
        out = droppin_sync.normalise_updated_at("not a timestamp")
        self.assertTrue(out.endswith("Z"))


class EventIdTests(unittest.TestCase):
    def test_status_id_is_stable_so_a_retry_is_a_duplicate(self):
        self.assertEqual(
            droppin_sync.status_event_id(1042, "delivered"),
            droppin_sync.status_event_id(1042, "delivered"),
        )

    def test_different_orders_never_collide(self):
        self.assertNotEqual(
            droppin_sync.status_event_id(1042, "delivered"),
            droppin_sync.status_event_id(1043, "delivered"),
        )

    def test_a_reopened_leg_gets_a_new_id(self):
        """Otherwise DropPin dedupes the second start and the map, having been
        closed when the courier skipped the stop, never opens again."""
        first = droppin_sync.leg_event_id(1042, "started", "2026-08-26 18:04:00")
        second = droppin_sync.leg_event_id(1042, "started", "2026-08-26 18:41:00")
        self.assertNotEqual(first, second)

    def test_position_id_follows_the_fix_not_the_send(self):
        same_fix = droppin_sync.position_event_id(1042, "2026-08-26T15:44:12Z")
        self.assertEqual(
            same_fix, droppin_sync.position_event_id(1042, "2026-08-26T15:44:12Z")
        )
        self.assertNotEqual(
            same_fix, droppin_sync.position_event_id(1042, "2026-08-26T15:44:42Z")
        )


class LegReadTests(unittest.TestCase):
    """Must agree with ``jarz_pos.services.delivery_leg.is_leg_open`` exactly.

    The two apps may not import each other, so the rule is restated in both. If
    they ever disagree, one of them is showing a customer a map the other says
    should be closed.
    """

    def test_truth_table(self):
        cases = [
            ({}, False),
            ({droppin_sync.LEG_STARTED_FIELD: "2026-08-26 18:04:00"}, True),
            (
                {
                    droppin_sync.LEG_STARTED_FIELD: "2026-08-26 18:04:00",
                    droppin_sync.LEG_ENDED_FIELD: "2026-08-26 18:20:00",
                },
                False,
            ),
            (
                {
                    droppin_sync.LEG_STARTED_FIELD: "2026-08-26 18:41:00",
                    droppin_sync.LEG_ENDED_FIELD: "2026-08-26 18:20:00",
                },
                True,
            ),
            (
                {
                    droppin_sync.LEG_STARTED_FIELD: "2026-08-26 18:04:00",
                    droppin_sync.LEG_ENDED_FIELD: "2026-08-26 18:04:00",
                },
                False,
            ),
        ]
        for row, expected in cases:
            with self.subTest(row=row):
                self.assertEqual(droppin_sync._leg_is_open(row), expected)


class SendGuardTests(unittest.TestCase):
    """Everything that must stop a request before it reaches the network."""

    def _settings(self, **overrides):
        s = MagicMock()
        s.name = "WooCommerce Settings"
        s.enable_droppin_dispatch = 1
        s.droppin_base_url = "https://demo.orderjarz.com"
        s.base_url = "https://demo.orderjarz.com"
        for k, v in overrides.items():
            setattr(s, k, v)
        return s

    def test_disabled_sends_nothing(self):
        settings = self._settings(enable_droppin_dispatch=0)
        with patch.object(droppin_sync, "_post") as post:
            result = droppin_sync.send_update(1042, event_id="e", status="delivered",
                                              settings=settings)
        post.assert_not_called()
        self.assertFalse(result["sent"])
        self.assertEqual(result["reason"], "disabled")

    def test_a_pos_native_order_sends_nothing(self):
        """``woo_order_id`` is an Int custom field, so a POS-native order reads
        back as 0 rather than NULL. Posting order 0 to the store is meaningless."""
        settings = self._settings()
        with patch.object(droppin_sync, "_post") as post:
            result = droppin_sync.send_update(0, event_id="e", status="delivered",
                                              settings=settings)
        post.assert_not_called()
        self.assertEqual(result["reason"], "not_a_woo_order")

    def test_payload_carries_only_what_was_given(self):
        settings = self._settings()
        with patch.object(droppin_sync, "_post", return_value={"sent": True}) as post:
            droppin_sync.send_update(
                1042, event_id="evt-88", leg="started",
                driver={"name": "أحمد"}, settings=settings,
            )
        payload = post.call_args.args[0]
        self.assertEqual(payload["wc_order_id"], 1042)
        self.assertEqual(payload["event_id"], "evt-88")
        self.assertEqual(payload["leg"], "started")
        self.assertNotIn("status", payload)
        self.assertNotIn("location", payload)

    def test_missing_secret_fails_closed_at_the_post(self):
        settings = self._settings()
        with patch.object(droppin_sync, "_shared_secret", return_value=""), \
                patch("requests.post") as post:
            result = droppin_sync._post({"wc_order_id": 1042}, settings=settings)
        post.assert_not_called()
        self.assertFalse(result["sent"])
        self.assertEqual(result["reason"], "no_secret")

    def test_the_signed_bytes_are_the_bytes_sent(self):
        """The whole reason ``data=`` is used instead of ``json=``."""
        settings = self._settings()
        response = MagicMock(status_code=200)
        response.json.return_value = {"ok": True}
        with patch.object(droppin_sync, "_shared_secret", return_value=SECRET), \
                patch("requests.post", return_value=response) as post:
            droppin_sync._post({"wc_order_id": 1042, "leg": "started"}, settings=settings)

        sent_body = post.call_args.kwargs["data"]
        header = post.call_args.kwargs["headers"]["X-DropPin-Signature"]
        ts = header.split(",")[0].split("=")[1]
        digest = header.split("v1=")[1]
        expected = hmac.new(
            SECRET.encode(), f"{ts}.".encode() + sent_body, hashlib.sha256
        ).hexdigest()
        self.assertEqual(digest, expected)
        self.assertIsInstance(sent_body, bytes)
        self.assertNotIn("json", post.call_args.kwargs)

    def test_transport_failure_is_reported_not_raised(self):
        settings = self._settings()
        with patch.object(droppin_sync, "_shared_secret", return_value=SECRET), \
                patch("requests.post", side_effect=OSError("connection reset")):
            result = droppin_sync._post({"wc_order_id": 1042}, settings=settings)
        self.assertFalse(result["sent"])
        self.assertEqual(result["reason"], "transport")

    def test_endpoint_falls_back_to_the_store_url(self):
        settings = self._settings(droppin_base_url="")
        self.assertEqual(
            droppin_sync._endpoint(settings),
            "https://demo.orderjarz.com" + droppin_sync.ENDPOINT_PATH,
        )

    def test_trailing_slash_does_not_double_up(self):
        settings = self._settings(droppin_base_url="https://demo.orderjarz.com/")
        self.assertEqual(
            droppin_sync._endpoint(settings),
            "https://demo.orderjarz.com" + droppin_sync.ENDPOINT_PATH,
        )


class PositionReadTests(unittest.TestCase):
    ROW = {
        "custom_kanban_profile": "Branch A",
        "custom_courier_party": "HR-EMP-0001",
        "custom_courier_party_type": "Employee",
    }

    def _with_cache(self, value):
        mock = MagicMock()
        cache = MagicMock()
        cache.get_value.return_value = value
        mock.cache.return_value = cache
        return patch.object(droppin_sync, "frappe", mock)

    def test_a_stored_fix_becomes_droppins_shape(self):
        with self._with_cache(
            json.dumps({"lat": 30.0444, "lng": 31.2357, "ts": "2026-08-20T15:44:12Z"})
        ):
            position = droppin_sync._courier_position(dict(self.ROW))
        self.assertEqual(position["lat"], 30.0444)
        self.assertEqual(position["lng"], 31.2357)
        self.assertEqual(position["updated_at"], "2026-08-20T15:44:12Z")

    def test_a_mocked_fix_is_never_forwarded(self):
        """Third check on the same fact, and deliberately so: this is the last
        hop before a coordinate reaches a member of the public."""
        with self._with_cache(
            json.dumps({"lat": 30.04, "lng": 31.23, "is_mocked": 1})
        ):
            self.assertIsNone(droppin_sync._courier_position(dict(self.ROW)))

    def test_null_island_is_a_failed_fix_not_a_location(self):
        with self._with_cache(json.dumps({"lat": 0, "lng": 0})):
            self.assertIsNone(droppin_sync._courier_position(dict(self.ROW)))

    def test_out_of_range_coordinates_are_rejected(self):
        with self._with_cache(json.dumps({"lat": 130.0, "lng": 31.23})):
            self.assertIsNone(droppin_sync._courier_position(dict(self.ROW)))

    def test_an_empty_cache_is_not_an_error(self):
        with self._with_cache(None):
            self.assertIsNone(droppin_sync._courier_position(dict(self.ROW)))

    def test_unassigned_order_has_no_position(self):
        with self._with_cache(json.dumps({"lat": 30.04, "lng": 31.23})):
            row = dict(self.ROW, custom_courier_party="")
            self.assertIsNone(droppin_sync._courier_position(row))

    def test_the_redis_key_matches_the_frozen_cross_app_contract(self):
        self.assertEqual(
            droppin_sync.COURIER_LOCATION_KEY_TEMPLATE,
            "courier:loc:{branch}:{party}",
            "this key is written by jarz_courier and read by jarz_pos; changing "
            "it in one app silently stops position pushes in another",
        )


class StatusMappingTests(unittest.TestCase):
    def test_only_states_we_mean_to_publish_are_mapped(self):
        self.assertEqual(
            droppin_sync._STATE_TO_DROPPIN.get("out_for_delivery"), "out_for_delivery"
        )
        self.assertEqual(droppin_sync._STATE_TO_DROPPIN.get("delivered"), "delivered")
        self.assertEqual(droppin_sync._STATE_TO_DROPPIN.get("cancelled"), "cancelled")

    def test_en_route_is_never_emitted(self):
        """DropPin derives it from ``leg: started`` and says never to send it."""
        self.assertNotIn("en_route", droppin_sync._STATE_TO_DROPPIN.values())

    def test_a_failed_attempt_maps_to_nothing(self):
        """A failed delivery is not a status in this system. The invoice stays
        Out for Delivery with a reason code, and telling a customer their order
        failed while a courier is coming back tomorrow creates a support call."""
        self.assertIsNone(droppin_sync._STATE_TO_DROPPIN.get("failed"))
        self.assertIsNone(droppin_sync._STATE_TO_DROPPIN.get("delivery_failed"))

    def test_unmapped_internal_states_publish_nothing(self):
        for state in ("recieved", "in_progress", "ready", "awaiting_recount", ""):
            with self.subTest(state=state):
                self.assertIsNone(droppin_sync._STATE_TO_DROPPIN.get(state))

    def test_board_state_reads_the_alias_the_kanban_probes_first(self):
        row = {"custom_sales_invoice_state": "Out for Delivery", "state": "Delivered"}
        self.assertEqual(droppin_sync._board_state(row), "out_for_delivery")


if __name__ == "__main__":
    unittest.main()
