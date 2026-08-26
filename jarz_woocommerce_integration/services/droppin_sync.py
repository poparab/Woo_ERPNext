"""Outbound delivery updates to DropPin — statuses, driver, position, trip legs.

The inbound half of the DropPin protocol only. Orders are **not** taken from
DropPin: ``services.order_sync`` already pulls them over the WooCommerce REST
API, handles the pinless orders DropPin deliberately holds back, and is the
tested intake path. DropPin's own docs confirm this direction needs nothing but
the shared secret — no ERPNext credentials, no webhook, no new guest endpoint,
and therefore no new public attack surface on this site.

What goes over the wire
-----------------------
One signed ``POST`` per event to ``<base>/wp-json/dpn/v1/delivery-update``:

===============================  ==================================================
Our side                         What DropPin does with it
===============================  ==================================================
board state -> Out for Delivery   customer reads *Out for delivery*; no map
leg started                       map opens **for this order only**, driver shown
courier position                  the marker moves
leg ended                         map closes; order status untouched
board state -> Delivered          *Delivered*, map closes, Woo order -> Completed
board state -> Cancelled          *Cancelled*, leg closes automatically
===============================  ==================================================

A **failed attempt sends nothing**. It is not a status in this system — the
invoice stays Out for Delivery and carries a reason code plus an attempt number
(COURIER_CONTRACTS §1) — and Egypt is a reschedule-heavy market where telling a
customer their delivery failed, while a courier is coming back tomorrow, creates
the support call we were trying to avoid. The failed *leg* still closes, so the
map shuts; only the wording stays silent.

Why this lives in the WooCommerce app
-------------------------------------
It signs and POSTs to the WordPress store, so it belongs beside every other
outbound call to that store, sharing its kill switch and its error surface. It
cannot live in ``jarz_courier``: that app may not import — or reach — the
WooCommerce domain, and a delivery push hidden inside the courier app would be
invisible on the dashboard where outbound failures are actually noticed. The
Woo outbound kill switches were once left off for seven weeks without anybody
seeing it, which is the whole argument for keeping this traffic in one place.

Two rules that are not negotiable
---------------------------------
**1. Nothing here may fail a delivery.** Every public function swallows its
exceptions and returns a result dict. A courier standing at a customer's door
must never see an error because a WordPress plugin was slow, and a delivery must
never roll back because a marker did not move.

**2. Sign the exact bytes that are sent.** The body is serialised once, signed,
and handed to ``requests`` as ``data=`` — never re-encoded by passing ``json=``.
A re-serialisation between signing and sending changes one byte of whitespace
and every request fails a signature check for reasons no log will explain.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import frappe
from frappe.utils import cint, get_datetime

from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
    WooCommerceSettings,
)

LOGGER = frappe.logger("jarz_woocommerce.droppin")

#: Path DropPin exposes. Same on staging and production.
ENDPOINT_PATH = "/wp-json/dpn/v1/delivery-update"

#: DropPin rejects a signature timestamp outside ±300 s, so a slow request is
#: worse than a failed one — it can arrive already invalid. Kept well under it.
REQUEST_TIMEOUT_SEC = 15

#: The frozen Redis key ``jarz_courier`` writes and ``jarz_pos`` reads. Restated
#: here rather than imported: this app may not import either of those (domain
#: isolation), and the key is a documented cross-app wire contract exactly like
#: the ``custom_*`` invoice columns this module also reads. Changing it there
#: without changing it here breaks position pushes silently, which is why the
#: string appears in a named constant in all three apps rather than inline.
COURIER_LOCATION_KEY_TEMPLATE = "courier:loc:{branch}:{party}"

#: Statuses DropPin understands that we actually emit. ``en_route`` is
#: deliberately absent — DropPin derives it from ``leg: started`` and explicitly
#: says never to send it.
STATUS_OUT_FOR_DELIVERY = "out_for_delivery"
STATUS_DELIVERED = "delivered"
STATUS_CANCELLED = "cancelled"

#: Board state (``_state_key``-normalised) -> the status we send. A state absent
#: from this map sends nothing at all, which is the right default: an unmapped
#: internal column is not customer vocabulary. ``recieved`` is the misspelling
#: that is live production data on every historical row.
_STATE_TO_DROPPIN: Dict[str, str] = {
    "out_for_delivery": STATUS_OUT_FOR_DELIVERY,
    "delivered": STATUS_DELIVERED,
    "cancelled": STATUS_CANCELLED,
    "canceled": STATUS_CANCELLED,
}

#: Board-state aliases, in the order the kanban probes them.
_STATE_FIELD_ALIASES = (
    "custom_sales_invoice_state",
    "sales_invoice_state",
    "custom_state",
    "state",
)

LEG_STARTED_FIELD = "custom_leg_started_at"
LEG_ENDED_FIELD = "custom_leg_ended_at"


# ─────────────────────────────────────────────────────────────────────────────
# Settings
# ─────────────────────────────────────────────────────────────────────────────

def _settings():
    try:
        return WooCommerceSettings.get_settings()
    except Exception:
        return None


def dispatch_enabled(settings: Any = None) -> bool:
    """Master kill switch. Off by default, and off is the correct default.

    Until the shared secret is configured this cannot work at all, so shipping
    it on would produce a failed HTTP call on every delivery transition.
    """
    settings = settings if settings is not None else _settings()
    if settings is None:
        return False
    return bool(cint(getattr(settings, "enable_droppin_dispatch", 0) or 0))


def _shared_secret(settings: Any) -> str:
    """The signing secret, or ``""``.

    An empty secret must never be used to sign. Their own sample receiver
    computes an HMAC with an empty key when the secret is unset, which turns a
    missing configuration into an endpoint that validates anything signed the
    same way; :func:`_signed_headers` refuses instead.
    """
    try:
        from frappe.utils.password import get_decrypted_password

        return str(
            get_decrypted_password(
                "WooCommerce Settings", settings.name, "droppin_shared_secret"
            )
            or ""
        ).strip()
    except Exception:
        return ""


def _endpoint(settings: Any) -> str:
    """Full inbound URL. Falls back to the store's own base URL.

    DropPin runs inside the same WordPress install as the shop, so the base is
    normally identical — the override exists because their staging store and the
    production store are different hosts and the migration between them should
    not require a code change.
    """
    base = str(
        getattr(settings, "droppin_base_url", "") or getattr(settings, "base_url", "") or ""
    ).strip().rstrip("/")
    return f"{base}{ENDPOINT_PATH}" if base else ""


# ─────────────────────────────────────────────────────────────────────────────
# Time — the one that silently freezes the marker
# ─────────────────────────────────────────────────────────────────────────────

#: How far ahead of the server clock an incoming fix may claim to be before it
#: is treated as a bad device clock. DropPin discards positions *older* than the
#: one it holds but accepts arbitrarily future ones, so a single fix stamped
#: hours ahead is stored and then makes every genuine fix afterwards "older" —
#: answered `stale_ignored`, marker frozen, no error on either side. Two minutes
#: covers ordinary clock drift and nothing else.
MAX_FUTURE_SKEW_SEC = 120


def normalise_updated_at(value: Any) -> str:
    """A courier ping's timestamp as UTC ISO-8601 with an explicit offset.

    The stored value cannot be trusted to carry a timezone. ``jarz_courier``
    keeps the handset's own string verbatim — deliberately, so a morning of
    queued fixes is not re-dated to the moment the courier found signal — and
    that string is ISO-with-``Z`` from the native Android app, an epoch-derived
    value from the OwnTracks path an iPhone courier uses, or a bare local
    ``"YYYY-MM-DD HH:MM:SS"`` when the client sent no usable time at all.

    Send that third shape through untouched and, during Egyptian summer time, it
    reads as two or three hours in the future. Hence: parse whatever arrives,
    assume server-local when it carries no offset, convert to UTC, and clamp
    anything beyond :data:`MAX_FUTURE_SKEW_SEC` back to now.
    """
    now = datetime.now(timezone.utc)
    parsed: Optional[datetime] = None

    if value not in (None, ""):
        try:
            parsed = get_datetime(value)
        except Exception:
            parsed = None

    if parsed is None:
        return now.isoformat(timespec="seconds").replace("+00:00", "Z")

    if parsed.tzinfo is None:
        # Naive: it came off a clock in the site's timezone, so read it there
        # rather than pretending it is UTC.
        parsed = parsed.astimezone() if hasattr(parsed, "astimezone") else parsed
        if parsed.tzinfo is None:  # pragma: no cover - defensive
            parsed = parsed.replace(tzinfo=timezone.utc)

    parsed = parsed.astimezone(timezone.utc)
    if parsed > now + timedelta(seconds=MAX_FUTURE_SKEW_SEC):
        parsed = now
    return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")


# ─────────────────────────────────────────────────────────────────────────────
# Signing
# ─────────────────────────────────────────────────────────────────────────────

def encode_body(payload: Dict[str, Any]) -> bytes:
    """Serialise once. The returned bytes are what gets signed AND sent.

    ``ensure_ascii=False`` because Arabic goes over this wire unescaped — a
    courier is called أحمد on the customer's page — and ``separators`` so the
    encoding is stable rather than dependent on a default that could change.
    """
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sign(body: bytes, secret: str, ts: Optional[int] = None) -> str:
    """``t=<unix>,v1=<hex>`` over ``"<t>." + raw body bytes``."""
    stamp = str(int(ts if ts is not None else time.time()))
    digest = hmac.new(
        secret.encode("utf-8"), f"{stamp}.".encode("utf-8") + body, hashlib.sha256
    ).hexdigest()
    return f"t={stamp},v1={digest}"


def _signed_headers(body: bytes, secret: str) -> Optional[Dict[str, str]]:
    if not secret:
        # Fail closed. Signing with an empty key produces a valid-looking
        # signature that any other party with an empty key can also produce.
        return None
    return {
        "Content-Type": "application/json; charset=utf-8",
        "X-DropPin-Signature": sign(body, secret),
    }


# ─────────────────────────────────────────────────────────────────────────────
# The one send
# ─────────────────────────────────────────────────────────────────────────────

def _post(payload: Dict[str, Any], *, settings: Any) -> Dict[str, Any]:
    """POST one signed update. Never raises."""
    import requests

    url = _endpoint(settings)
    if not url:
        return {"sent": False, "reason": "no_endpoint"}

    secret = _shared_secret(settings)
    body = encode_body(payload)
    headers = _signed_headers(body, secret)
    if headers is None:
        LOGGER.error({"event": "droppin_no_secret", "url": url})
        return {"sent": False, "reason": "no_secret"}

    try:
        response = requests.post(
            url, data=body, headers=headers, timeout=REQUEST_TIMEOUT_SEC
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.error({"event": "droppin_post_failed", "error": str(exc), "url": url})
        return {"sent": False, "reason": "transport", "error": str(exc)}

    ok = 200 <= response.status_code < 300
    result: Dict[str, Any] = {"sent": ok, "status_code": response.status_code}
    try:
        result["response"] = response.json()
    except Exception:  # noqa: BLE001
        result["response"] = (response.text or "")[:500]

    if not ok:
        # 401 here means the signature, the timestamp or the secret is wrong —
        # DropPin returns a flat `{"ok":false}` with no detail by design, so the
        # only place to see why is their Log screen. Say so in the log rather
        # than leaving somebody to guess.
        LOGGER.error(
            {
                "event": "droppin_rejected",
                "status_code": response.status_code,
                "payload_keys": sorted(payload),
                "wc_order_id": payload.get("wc_order_id"),
                "hint": (
                    "401 => signature/secret/clock. Check WooCommerce -> Settings"
                    " -> DropPin -> Log for the rejection reason."
                ),
            }
        )
    return result


def send_update(
    wc_order_id: int,
    *,
    event_id: str,
    status: Optional[str] = None,
    leg: Optional[str] = None,
    driver: Optional[Dict[str, Any]] = None,
    location: Optional[Dict[str, Any]] = None,
    trip_id: Optional[str] = None,
    settings: Any = None,
) -> Dict[str, Any]:
    """Build and send one delivery update. The only way out of this module.

    ``event_id`` is required and must be **stable for the fact being reported**,
    not unique per attempt: DropPin ignores a repeat of the same ``event_id`` for
    24 hours, so a stable one is what makes our own retries safe. Deriving it
    from ``(order, fact)`` rather than from a clock is the whole mechanism.
    """
    settings = settings if settings is not None else _settings()
    if settings is None:
        return {"sent": False, "reason": "no_settings"}
    if not dispatch_enabled(settings):
        return {"sent": False, "reason": "disabled"}
    if not wc_order_id:
        # A POS-native order has woo_order_id 0 and does not exist in the store.
        return {"sent": False, "reason": "not_a_woo_order"}

    payload: Dict[str, Any] = {"wc_order_id": int(wc_order_id), "event_id": event_id}
    if status:
        payload["status"] = status
    if leg:
        payload["leg"] = leg
    if driver:
        payload["driver"] = driver
    if location:
        payload["location"] = location
    if trip_id:
        payload["trip_id"] = trip_id

    return _post(payload, settings=settings)


# ─────────────────────────────────────────────────────────────────────────────
# Reading our own side
# ─────────────────────────────────────────────────────────────────────────────

def _state_key(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "_").replace("-", "_")


def _board_state(row: Dict[str, Any]) -> str:
    for alias in _STATE_FIELD_ALIASES:
        if row.get(alias):
            return _state_key(row.get(alias))
    return ""


def _leg_is_open(row: Dict[str, Any]) -> bool:
    """Mirrors ``jarz_pos.services.delivery_leg.is_leg_open``.

    Restated rather than imported for the same reason as the Redis key above.
    Kept to the identical ``started >= ended`` rule so the two apps cannot
    disagree about whether a customer should be watching a map.
    """
    started = row.get(LEG_STARTED_FIELD)
    if not started:
        return False
    ended = row.get(LEG_ENDED_FIELD)
    if not ended:
        return True
    try:
        # `>` not `>=` — a tie is a leg opened and closed in the same second,
        # which is what a terminal outcome writes. Must match
        # jarz_pos.services.delivery_leg.is_leg_open exactly.
        return get_datetime(started) > get_datetime(ended)
    except Exception:
        return False


def _driver_block(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Name and phone for the customer's driver card, or ``None``.

    Sent with ``leg: started`` and never repeated: DropPin remembers each field
    until a new value arrives, and it only reveals the card while that leg is
    open, so there is nothing to decide here about visibility.

    Only the **first name** goes out. The customer needs to recognise who is at
    the door, not to be handed a staff member's full identity on a link that can
    be forwarded to anyone — the same rule the ERPNext tracking page already
    applies.
    """
    party_type = str(row.get("custom_courier_party_type") or "").strip()
    party = str(row.get("custom_courier_party") or "").strip()
    if not (party_type and party):
        return None

    try:
        if party_type == "Employee":
            full_name, phone = frappe.db.get_value(
                "Employee", party, ["employee_name", "cell_number"]
            ) or ("", "")
        else:
            full_name, phone = frappe.db.get_value(
                "Supplier", party, ["supplier_name", "mobile_no"]
            ) or ("", "")
    except Exception:
        return None

    first_name = (str(full_name or "").strip().split() or [""])[0]
    if not first_name:
        return None

    block: Dict[str, Any] = {"name": first_name}
    phone = str(phone or "").strip()
    if phone:
        # DropPin masks it on the page unless the shop says otherwise, so the
        # real number is what it wants; the display decision is theirs.
        block["phone"] = phone
    return block


def _courier_position(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Last known fix for this order's courier, in DropPin's shape, or ``None``.

    A mocked fix is dropped rather than sent. ``jarz_courier`` already refuses to
    store one and ``jarz_pos`` drops it again before rendering; this is the third
    check, and it is here for the same reason the second one is — it is the last
    hop before a coordinate reaches a member of the public.
    """
    branch = str(row.get("custom_kanban_profile") or row.get("pos_profile") or "").strip()
    party = str(row.get("custom_courier_party") or "").strip()
    if not party:
        return None

    key = COURIER_LOCATION_KEY_TEMPLATE.format(branch=branch, party=party)
    try:
        raw = frappe.cache().get_value(key)
    except Exception:
        return None
    if not raw:
        return None

    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except Exception:
            return None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return None
    if not isinstance(raw, dict) or raw.get("is_mocked"):
        return None

    lat = raw.get("lat", raw.get("latitude"))
    lng = raw.get("lng", raw.get("longitude"))
    try:
        lat_f, lng_f = float(lat), float(lng)
    except (TypeError, ValueError):
        return None
    if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lng_f <= 180.0):
        return None
    if lat_f == 0.0 and lng_f == 0.0:
        # Null Island is a failed fix, not a location in the Gulf of Guinea.
        return None

    return {
        "lat": round(lat_f, 6),
        "lng": round(lng_f, 6),
        "updated_at": normalise_updated_at(raw.get("ts") or raw.get("updated_at")),
    }


#: Columns every push needs. One query per invoice, no document load.
_INVOICE_FIELDS = (
    "name",
    "docstatus",
    "woo_order_id",
    "custom_courier_party_type",
    "custom_courier_party",
    "custom_kanban_profile",
    "pos_profile",
    LEG_STARTED_FIELD,
    LEG_ENDED_FIELD,
) + _STATE_FIELD_ALIASES


def _invoice_row(invoice_name: str) -> Optional[Dict[str, Any]]:
    """Read the invoice, asking only for columns this site actually has."""
    try:
        meta = frappe.get_meta("Sales Invoice")
        fields = [
            f for f in _INVOICE_FIELDS
            if f in ("name", "docstatus") or meta.get_field(f)
        ]
        return frappe.db.get_value("Sales Invoice", invoice_name, fields, as_dict=True)
    except Exception:
        LOGGER.error({"event": "droppin_invoice_read_failed", "invoice": invoice_name})
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Event ids — stable per fact, so their 24h dedupe absorbs our retries
# ─────────────────────────────────────────────────────────────────────────────

def status_event_id(wc_order_id: int, status: str) -> str:
    return f"jarz-status-{wc_order_id}-{status}"


def leg_event_id(wc_order_id: int, leg: str, started_at: Any) -> str:
    """Includes the leg's own start time so a **re-opened** leg is a new fact.

    A courier who skips an order and returns to it must reopen the map. Keying
    on ``(order, "started")`` alone would make the second start a duplicate of
    the first and DropPin would ignore it — leaving the customer on a page that
    never opens again.
    """
    stamp = str(started_at or "").replace(" ", "T")
    return f"jarz-leg-{wc_order_id}-{leg}-{stamp}"


def position_event_id(wc_order_id: int, updated_at: str) -> str:
    """Keyed on the fix's own timestamp: resending the same fix is a duplicate,
    a genuinely new fix is not."""
    return f"jarz-pos-{wc_order_id}-{updated_at}"


# ─────────────────────────────────────────────────────────────────────────────
# Entry points
# ─────────────────────────────────────────────────────────────────────────────

def push_invoice_state(invoice_name: str, *, settings: Any = None) -> Dict[str, Any]:
    """Send whatever this invoice's current board state and leg imply.

    Called from the Sales Invoice hook after every submitted-invoice update, so
    it must be cheap and it must be safe to call when nothing relevant changed —
    a stable ``event_id`` means a redundant send is answered
    ``{"ok":true,"duplicate":true}`` and costs one request.
    """
    settings = settings if settings is not None else _settings()
    if not dispatch_enabled(settings):
        return {"sent": False, "reason": "disabled"}

    row = _invoice_row(invoice_name)
    if not row:
        return {"sent": False, "reason": "no_invoice"}

    wc_order_id = cint(row.get("woo_order_id") or 0)
    if not wc_order_id:
        return {"sent": False, "reason": "not_a_woo_order"}

    results: List[Dict[str, Any]] = []

    status = _STATE_TO_DROPPIN.get(_board_state(row))
    if status:
        results.append(
            send_update(
                wc_order_id,
                event_id=status_event_id(wc_order_id, status),
                status=status,
                settings=settings,
            )
        )

    # The leg is sent after the status on purpose. DropPin's timeline goes
    # "out for delivery" -> "on the way to you"; arriving in the other order
    # would show the customer a live map and then walk the timeline backwards.
    if _leg_is_open(row):
        results.append(
            send_update(
                wc_order_id,
                event_id=leg_event_id(wc_order_id, "started", row.get(LEG_STARTED_FIELD)),
                leg="started",
                driver=_driver_block(row),
                settings=settings,
            )
        )
    elif row.get(LEG_ENDED_FIELD) and not status:
        # Only when no status went out. `delivered` and `cancelled` close the leg
        # on DropPin's side by themselves, so sending both would be a redundant
        # request per delivery for the whole fleet.
        results.append(
            send_update(
                wc_order_id,
                event_id=leg_event_id(wc_order_id, "ended", row.get(LEG_ENDED_FIELD)),
                leg="ended",
                settings=settings,
            )
        )

    return {"sent": any(r.get("sent") for r in results), "results": results}


def push_open_leg_positions() -> Dict[str, Any]:
    """Scheduled: move the marker for every order with an open leg.

    Driven from a cron rather than from the ping ingest because ``jarz_courier``
    may not reach this app at all. That inverts the flow — this app asks "who is
    mid-leg?" instead of being told "a fix arrived" — and the inversion is what
    makes the fan-out bounded: one request per *open leg*, not one per fix. With
    one open leg per courier by construction, that is one request per active
    courier per tick however chattily the handsets report.
    """
    settings = _settings()
    if not dispatch_enabled(settings):
        return {"sent": 0, "reason": "disabled"}

    try:
        meta = frappe.get_meta("Sales Invoice")
        if not meta.get_field(LEG_STARTED_FIELD):
            return {"sent": 0, "reason": "no_leg_fields"}
        rows = frappe.get_all(
            "Sales Invoice",
            filters={
                "docstatus": 1,
                LEG_STARTED_FIELD: ["is", "set"],
                "woo_order_id": [">", 0],
            },
            fields=list(_INVOICE_FIELDS),
            order_by=f"{LEG_STARTED_FIELD} desc",
            limit=200,
        )
    except Exception:
        frappe.log_error(frappe.get_traceback(), "droppin: open-leg scan failed")
        return {"sent": 0, "reason": "scan_failed"}

    sent = 0
    skipped = 0
    for row in rows:
        if not _leg_is_open(row):
            continue
        position = _courier_position(row)
        if not position:
            skipped += 1
            continue
        result = send_update(
            cint(row.get("woo_order_id")),
            event_id=position_event_id(
                cint(row.get("woo_order_id")), position["updated_at"]
            ),
            location=position,
            settings=settings,
        )
        if result.get("sent"):
            sent += 1

    LOGGER.error({"event": "droppin_positions_pushed", "sent": sent, "no_fix": skipped})
    return {"sent": sent, "no_fix": skipped, "open_legs": len(rows)}


def enqueue_droppin_update(doc: Any, method: Optional[str] = None) -> None:
    """``Sales Invoice`` hook. Never raises, never blocks the save.

    Enqueued rather than sent inline: an HTTP call inside
    ``on_update_after_submit`` puts a WordPress plugin's response time on the
    critical path of a courier tapping Delivered.
    """
    try:
        if not dispatch_enabled():
            return
        if cint(getattr(doc, "docstatus", 0)) != 1:
            return
        if not cint(getattr(doc, "woo_order_id", 0) or 0):
            return
        frappe.enqueue(
            "jarz_woocommerce_integration.services.droppin_sync.push_invoice_state",
            queue="short",
            invoice_name=doc.name,
            enqueue_after_commit=True,
        )
    except Exception:
        # A dispatch push must never be the reason a delivery does not save.
        LOGGER.error(
            {
                "event": "droppin_enqueue_failed",
                "invoice": getattr(doc, "name", None),
                "method": method,
            }
        )
