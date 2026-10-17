"""WooCommerce -> ERPNext geo pin passthrough (courier lane O1).

Why this module exists
----------------------
The Woo checkout can carry a customer-supplied map pin. Two shapes exist on this
store:

1. Order meta ``_jarz_lat`` / ``_jarz_lng`` written by our own checkout plugin,
   or ``_dpn_lat`` / ``_dpn_lng`` written by DropPin's. Both are read; see the
   key tuples below for why reading only one family is a silent failure.
2. A Google Maps link typed into an address line at checkout.

Both are supplied by the *customer*, so they land on the ``Address`` geo custom
fields at confidence ``customer_pin`` (rank 30 of the frozen ladder).

Four invariants govern every line below. Breaking any one of them is a
production incident, not a style regression.

**1. The address dedup signature must never see a coordinate.**
``customer_sync._address_signature_parts`` hashes exactly six *text* fields
(``address_line1``, ``address_line2``, ``city``, ``state``, ``postcode``,
``country``). Latitude and longitude are deliberately absent. Were a coordinate
ever folded into that tuple, every pin update would change the signature and
fork a duplicate ``Address`` for the customer instead of matching the existing
one. ``tests/test_address_signature_stability.py`` is the regression guard.

**2. A geo write must never fan out to WooCommerce.**
``outbound_sync._CUSTOMER_ADDRESS_OUTBOUND_UPDATE_FIELDS`` gates both Address
outbound hooks and contains no ``custom_*`` field, so a geo-only change cannot
enqueue a customer + invoice push back to Woo. That safety evaporates the moment
a text edit is bundled into the same save, so this module writes the geo fields
and nothing else -- via ``frappe.db.set_value(..., update_modified=False)``,
which bypasses the document layer entirely and therefore never fires
``on_update`` at all.

**3. Never downgrade a pin, never write above ``customer_pin``, never raise.**
A write is accepted only when ``incoming_rank >= stored_rank``. A lower rank is
ignored silently: a routine re-sync of an order whose address has since been
courier-verified must neither fail nor clobber the better pin. In the other
direction this module is *capped* at ``customer_pin`` (rank 30) -- a WooCommerce
payload can never attest to ``courier_verified`` or ``manual_override``, and the
cap is enforced at the write rather than only asserted in a test. Every public
function here returns a value instead of raising, because they run inside order
sync.

Contract section 3 authorises exactly two writers of these fields:
``jarz_pos/services/geo_resolution.py`` (any source) and this module (capped).
Both implement the ladder independently from contract section 4, which is the
shared source of truth -- ``tests/test_geo_passthrough.py`` holds the drift guard
that keeps the two tables identical.

**3b. Accuracy travels with the coordinates.**
Any write that *moves* the pin also writes the accuracy, or explicitly clears it
to 0 when the incoming source carries none. Woo pins carry none, so they zero it:
a radius measured for the previous point does not describe the new one, and
leaving it silently corrupts the downstream consensus-hardening job. Accuracy is
preserved when the coordinates are unchanged, because it is still true of them.
Cleared to 0 rather than NULL because Frappe Float columns are NOT NULL DEFAULT
0 -- see the note at the write site.

**4. Only WooCommerce payloads are a source of ``customer_pin``.**
This module deliberately does not scrape the stored ``address_line2``
``"Location: <url>"`` strings the POS writes. Those are ``pos_link`` provenance
(rank 20) and belong to the jarz_pos backfill; stamping them ``customer_pin``
(rank 30) here would permanently outrank and block the correct classification.
This module also never rewrites ``address_line2`` -- it is part of the dedup
signature *and* of the outbound push trigger set.
"""

from __future__ import annotations

from functools import partial
import re
from typing import Any, Callable, Iterator, NamedTuple, Optional
from urllib.parse import unquote

import frappe

from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
    WooCommerceSettings,
)
from jarz_woocommerce_integration.services.order_map_status import is_retired_duplicate_row
from jarz_woocommerce_integration.utils.http_client import WooClient

LOGGER = frappe.logger("jarz_woocommerce.geo")


# ---------------------------------------------------------------------------
# Frozen confidence ladder (COURIER_CONTRACTS.md section 4)
# ---------------------------------------------------------------------------
# Ranks, never string comparison. Alphabetical ordering puts "courier_verified"
# below "customer_pin" and "pos_link" above "manual_override", which would
# silently invert the never-downgrade rule.
CONFIDENCE_RANK: dict[str, int] = {
    "territory_centroid": 10,
    "pos_link": 20,
    "customer_pin": 30,
    # A courier capture made through the web build. Unverifiable for mock GPS,
    # so it sits below courier_verified. Woo can never write it — MAX_WRITABLE_RANK
    # caps this module at customer_pin — but the table must mirror section 4
    # exactly or the drift test fails.
    "courier_web": 35,
    "courier_verified": 40,
    "manual_override": 50,
}

GEO_SOURCE_CUSTOMER_PIN = "customer_pin"

# COURIER_CONTRACTS.md section 3 authorises exactly two writers of the Address
# geo fields: jarz_pos/services/geo_resolution.py (any source) and this module,
# **capped at customer_pin**. A WooCommerce payload is by definition
# customer-supplied; it can never attest to a courier having stood at the door,
# nor to a manager's deliberate override. The cap is enforced at the write, not
# merely asserted in a test, so no future caller can smuggle a higher rank in
# through the ``source`` argument.
MAX_WRITABLE_RANK = CONFIDENCE_RANK[GEO_SOURCE_CUSTOMER_PIN]

# The only Address fields this module is allowed to write. Every one is a
# ``custom_*`` field, which is exactly what keeps the Woo outbound hooks quiet.
GEO_WRITE_FIELDS: tuple[str, ...] = (
    "custom_latitude",
    "custom_longitude",
    "custom_geo_source",
    "custom_geo_confidence",
)

# Optional extra, written only when the caller actually supplies a value. Never
# invented, never cleared: a stale accuracy radius is bad data, a missing one is
# merely unknown.
GEO_ACCURACY_FIELD = "custom_geo_accuracy_m"

_ALLOWED_UPDATE_FIELDS = frozenset(GEO_WRITE_FIELDS) | {GEO_ACCURACY_FIELD}

_ADDRESS_READ_FIELDS = (
    "custom_geo_source",
    "custom_geo_confidence",
    "custom_latitude",
    "custom_longitude",
)


# ---------------------------------------------------------------------------
# Payload keys
# ---------------------------------------------------------------------------

# Two checkout plugins can write a pin on this store, so both key families are
# read. ``_jarz_*`` is our own plugin (MapLibre + OSM picker); ``_dpn_*`` is
# DropPin's picker, which additionally does coverage checking we have no
# equivalent for.
#
# Reading both is what makes the changeover safe in either direction. Our
# extractor knowing only ``_jarz_*`` meant that the day DropPin's picker replaced
# ours, every customer pin would stop reaching ERPNext -- no error, no failed
# sync, just an Address that never gets coordinates and a `customer_pin` rung of
# the confidence ladder that is permanently empty. Order matters only when an
# order somehow carries both, which happens exactly while both plugins are
# active; ``_jarz_*`` wins there because it is the one our own checkout wrote.
_ORDER_LAT_META_KEYS = (
    "_jarz_lat",
    "jarz_lat",
    "_jarz_latitude",
    "jarz_latitude",
    "_dpn_lat",
    "dpn_lat",
)
_ORDER_LNG_META_KEYS = (
    "_jarz_lng",
    "jarz_lng",
    "_jarz_long",
    "_jarz_longitude",
    "jarz_longitude",
    "_dpn_lng",
    "dpn_lng",
)
# Some plugin builds pack both halves into one "lat,lng" (or a Maps URL) value.
_ORDER_COMBINED_META_KEYS = ("_jarz_location", "jarz_location", "_jarz_latlng", "jarz_latlng")

# WooCommerce address payload keys only. ``address_line1``/``address_line2`` are
# intentionally absent -- see invariant 4 in the module docstring.
_WOO_ADDRESS_TEXT_KEYS = ("address_1", "address_2")

_LAT_LIMIT = 90.0
_LNG_LIMIT = 180.0
_COORD_PRECISION = 6
_COORD_EPSILON = 5e-7

# Egypt bounding box. Every branch this store delivers from is in Egypt, so a
# pin outside this box is never a usable delivery point -- it is a customer who
# dropped the pin while abroad, a browser that geolocated a VPN exit, or a
# prefill from a saved address in another country (a Paris pin was accepted on
# staging before this existed). It is not discarded silently: the delivery-pin
# path reports it as the ``outside_egypt`` flag so a human sees it.
#
# A latitude/longitude swap of a Cairo or Alexandria point (30,31 -> 31,30)
# stays inside this box and cannot be detected here; nothing tries to.
EGYPT_LAT_MIN = 22.0
EGYPT_LAT_MAX = 32.0
EGYPT_LNG_MIN = 24.5
EGYPT_LNG_MAX = 37.0

# ---------------------------------------------------------------------------
# Map-link / coordinate-text patterns (used by ``parse_maps_link``)
# ---------------------------------------------------------------------------
#
# Two classes of input, with different strictness:
#
# * URL coordinate *slots* (``?q=``, ``@``, ``/place/``, ``cp=``, ``mlat=`` ...).
#   The slot itself says "this is a coordinate", so integer pairs are allowed
#   there (``@30,31,17z``, ``?q=30,31``) -- with a guard each, see the
#   ``_integer_*`` helpers.
# * Bare text. A pair of numbers in prose is only a coordinate when it carries
#   markers (degree sign, hemisphere letter, DMS) or, unmarked, at least three
#   decimals on both halves -- so "Building 30, 31 street", street numbers,
#   "Villa 12.500", postcodes and phone numbers never match.

# Arabic-Indic (U+0660-0669) and Persian (U+06F0-06F9) digits, the Arabic
# decimal separator (U+066B), Arabic comma (U+060C) and Arabic semicolon
# (U+061B), folded to ASCII before any pattern runs. An Arabic phone keyboard
# types these by default.
_DIGIT_FOLD: dict[int, str] = {
    **{0x0660 + offset: str(offset) for offset in range(10)},
    **{0x06F0 + offset: str(offset) for offset in range(10)},
    0x066B: ".",
    0x060C: ",",
    0x061B: ";",
}

# ``https%3A%2F%2Fmaps...`` arrives when a link was copied out of another URL
# (a share sheet, a redirect, a WhatsApp preview). Twice-encoded happens too.
_MAX_PERCENT_DECODE_PASSES = 2

# A number in a URL slot: integer or decimal, optional sign.
_NUM = r"[-+]?\d{1,3}(?:\.\d+)?"
# Separator inside a URL slot: "," or "%2C", optionally followed by the space
# Google inserts when it builds a search URL ("30.0444,+31.2357").
_URL_SEP = r"\s*(?:,|%2C)(?:\s|\+|%20)*"
_END = r"(?![\d.])"

# Google place payload: ...!3d30.0444!4d31.2357 -- the dropped pin itself.
_PLACE_PIN_RE = re.compile(r"!3d(" + _NUM + r")!4d(" + _NUM + r")" + _END)
# ?q= / ?query= / &ll= / &destination= / q=loc:lat,lng / Apple ?coordinate=
_QUERY_SLOT_RE = re.compile(
    r"[?&;](?:q|query|ll|sll|center|destination|daddr|saddr|coordinate)="
    r"(?:loc:)?\s*(" + _NUM + r")" + _URL_SEP + r"(" + _NUM + r")" + _END,
    re.IGNORECASE,
)
# Bing: ?cp=30.0444~31.2357
_BING_CP_RE = re.compile(
    r"[?&;]cp=(" + _NUM + r")(?:~|%7E)(" + _NUM + r")" + _END, re.IGNORECASE
)
# OpenStreetMap marker: ?mlat=30.0444&mlon=31.2357 (either order)
_OSM_MLAT_RE = re.compile(r"[?&;]mlat=(" + _NUM + r")" + _END, re.IGNORECASE)
_OSM_MLON_RE = re.compile(r"[?&;]mlon=(" + _NUM + r")" + _END, re.IGNORECASE)
# Google path slot: /maps/place/lat,lng  /maps/search/lat,+lng  /maps/dir//lat,lng
_PATH_SLOT_RE = re.compile(
    r"/(?:place|search|dir)/+(" + _NUM + r")" + _URL_SEP + r"(" + _NUM + r")" + _END,
    re.IGNORECASE,
)
# /maps/@30.0444,31.2357,17z  (the @ pair is lat,lng -- same order as ?q=).
# The viewport centre, so it ranks below every explicit pin slot.
_AT_VIEWPORT_RE = re.compile(r"@(" + _NUM + r"),\s*(" + _NUM + r")" + _END)
# OpenStreetMap viewport: #map=17/30.0444/31.2357
_OSM_MAP_HASH_RE = re.compile(
    r"[#&]map=\d{1,2}(?:\.\d+)?/(" + _NUM + r")/(" + _NUM + r")" + _END, re.IGNORECASE
)
# RFC 5870 geo URI (Android share sheet): geo:30.0444,31.2357
_GEO_URI_RE = re.compile(
    r"(?<![A-Za-z])geo:(" + _NUM + r")" + _URL_SEP + r"(" + _NUM + r")" + _END,
    re.IGNORECASE,
)

# An integer pair in a URL slot followed by a word is an address search
# ("?q=30,+31+street"), not a coordinate.
_WORD_TAIL_RE = re.compile(r"(?:\s|\+|%20)*[^\W\d_]")
# An integer @ pair is only trusted when Google's zoom suffix follows (",17z").
_ZOOM_TAIL_RE = re.compile(r",\s*\d+(?:\.\d+)?[a-z]", re.IGNORECASE)

# Open Location Code (Plus Codes). Full codes only: 8 digits, "+", 2-7 digits.
# Short codes ("26VP+Q7 Cairo") need a reference location and are rejected;
# padded codes ("8G2H0000+") contain "0", which is not in the alphabet.
_OLC_ALPHABET = "23456789CFGHJMPQRVWX"
_OLC_PAIR_LENGTH = 10
_OLC_MAX_LENGTH = 15
_OLC_GRID_COLUMNS = 4
_OLC_GRID_ROWS = 5
_PLUS_CODE_RE = re.compile(
    r"(?<![0-9A-Za-z])([" + _OLC_ALPHABET + r"]{8})\+([" + _OLC_ALPHABET + r"]{2,7})"
    r"(?![0-9A-Za-z])",
    re.IGNORECASE,
)

_DEGREE = "[°º˚]"
_MINUTE_MARK = "['′’]"
_SECOND_MARK = "(?:\"|''|″|”)"

# DMS as Google copies it: 30°02'39.8"N 31°14'08.6"E (seconds optional).
_DMS_TOKEN = (
    r"(\d{1,3})\s*" + _DEGREE + r"\s*(\d{1,2}(?:\.\d+)?)\s*" + _MINUTE_MARK + r"\s*"
    r"(?:(\d{1,2}(?:\.\d+)?)\s*" + _SECOND_MARK + r"\s*)?([NSEW])(?![A-Za-z])"
)
_DMS_PAIR_RE = re.compile(
    r"(?<![\w.])" + _DMS_TOKEN + r"[\s,;+]*" + _DMS_TOKEN, re.IGNORECASE
)

# Decimal degrees with markers: "30.0444° N, 31.2357° E", "30.0444N 31.2357E".
# Each half must carry a degree sign, a hemisphere letter, or both; an unmarked
# pair is left to the stricter bare-pair rule. Groups per half: number, degree
# sign, letter after the degree sign, letter with no degree sign.
_HEMI_TOKEN = (
    r"(-?\d{1,3}\.\d+)(?!\d)\s*"
    r"(?:(" + _DEGREE + r")\s*([NSEW])?|([NSEW]))(?![A-Za-z])"
)
_HEMI_PAIR_RE = re.compile(
    r"(?<![\w.\-])" + _HEMI_TOKEN + r"[\s,;+]*" + _HEMI_TOKEN, re.IGNORECASE
)

# Bare "30.0444, 31.2357" pasted without a URL (comma, semicolon or space
# separated). Three decimals minimum, so prices, postcodes and phone numbers
# cannot match.
_BARE_PAIR_RE = re.compile(
    r"(?<![\d.\-])(-?\d{1,3}\.\d{3,})(?:\s*[,;]\s*|\s+)(-?\d{1,3}\.\d{3,})(?![\d.])"
)


class GeoPin(NamedTuple):
    """A validated coordinate pair, already rounded to the field precision."""

    latitude: float
    longitude: float


# ---------------------------------------------------------------------------
# Coercion / validation
# ---------------------------------------------------------------------------


def _to_float(value: Any) -> Optional[float]:
    """Best-effort float coercion that never raises and rejects bool/NaN/inf."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            number = float(text)
        except ValueError:
            return None
    if number != number or number in (float("inf"), float("-inf")):  # NaN / inf
        return None
    return number


def _make_world_pin(latitude: Any, longitude: Any) -> Optional[GeoPin]:
    """Validate a coordinate pair anywhere on Earth (no Egypt check).

    Only used to *detect* a pin that exists but is outside Egypt; such a pin is
    never written anywhere as a coordinate.
    """
    lat = _to_float(latitude)
    lng = _to_float(longitude)
    if lat is None or lng is None:
        return None
    if abs(lat) > _LAT_LIMIT or abs(lng) > _LNG_LIMIT:
        return None
    # Null Island is what an empty form field or a failed geocode looks like --
    # never a real delivery address for this store.
    if abs(lat) < _COORD_EPSILON and abs(lng) < _COORD_EPSILON:
        return None
    return GeoPin(round(lat, _COORD_PRECISION), round(lng, _COORD_PRECISION))


def pin_in_egypt(pin: Optional[GeoPin]) -> bool:
    """True when ``pin`` falls inside the Egypt bounding box."""
    if pin is None:
        return False
    return (
        EGYPT_LAT_MIN <= pin.latitude <= EGYPT_LAT_MAX
        and EGYPT_LNG_MIN <= pin.longitude <= EGYPT_LNG_MAX
    )


def make_pin(latitude: Any, longitude: Any) -> Optional[GeoPin]:
    """Validate a coordinate pair, returning ``None`` for anything unusable.

    Usable means a real point inside Egypt: every downstream consumer (Address
    geo fields, the invoice delivery snapshot, courier routing) needs a point a
    rider can drive to.
    """
    pin = _make_world_pin(latitude, longitude)
    if pin is None or not pin_in_egypt(pin):
        return None
    return pin


def _pin_factory(egypt_only: bool):
    return make_pin if egypt_only else _make_world_pin


def _same_coordinate(left: Any, right: Optional[float]) -> bool:
    left_value = _to_float(left)
    if left_value is None or right is None:
        return False
    return abs(left_value - right) < _COORD_EPSILON


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


def _candidate_forms(raw: str) -> list[str]:
    """``raw`` plus up to two percent-decoded forms, digits folded to ASCII.

    Order is raw first, then each decode pass; duplicates are dropped.
    """
    forms = [raw]
    current = raw
    for _ in range(_MAX_PERCENT_DECODE_PASSES):
        if "%" not in current:
            break
        try:
            decoded = unquote(current)
        except Exception:  # noqa: BLE001
            break
        if decoded == current:
            break
        forms.append(decoded)
        current = decoded
    folded: list[str] = []
    for form in forms:
        normalized = form.translate(_DIGIT_FOLD)
        if normalized not in folded:
            folded.append(normalized)
    return folded


def _integer_pair_always_ok(_form: str, _end: int) -> bool:
    return True


def _integer_pair_not_followed_by_word(form: str, end: int) -> bool:
    """``?q=30,31`` is a pin; ``?q=30,+31+street`` is an address search."""
    return _WORD_TAIL_RE.match(form, end) is None


def _integer_pair_followed_by_zoom(form: str, end: int) -> bool:
    """``@30,31,17z`` is a Google viewport; a bare ``@30,31`` is not trusted."""
    return _ZOOM_TAIL_RE.match(form, end) is not None


def _slot_pairs(
    pattern: re.Pattern[str],
    integer_ok: Callable[[str, int], bool],
    form: str,
) -> Iterator[tuple[Any, Any]]:
    """Every ``(lat, lng)`` the two-group ``pattern`` finds in ``form``.

    A pair where *both* halves are integers is yielded only when
    ``integer_ok(form, match_end)`` agrees.
    """
    for match in pattern.finditer(form):
        latitude, longitude = match.group(1), match.group(2)
        if "." not in latitude and "." not in longitude and not integer_ok(form, match.end()):
            continue
        yield latitude, longitude


def _osm_marker_pairs(form: str) -> Iterator[tuple[Any, Any]]:
    lat_match = _OSM_MLAT_RE.search(form)
    lng_match = _OSM_MLON_RE.search(form)
    if lat_match and lng_match:
        yield lat_match.group(1), lng_match.group(1)


def _decode_plus_code(code: str) -> Optional[tuple[float, float]]:
    """Centre of a *full* Open Location Code, or ``None``.

    Standard OLC decoding: five lat/lng digit pairs at resolutions 20, 1, 0.05,
    0.0025, 0.000125 degrees, then up to five 4x5 grid refinement digits.
    """
    digits = str(code or "").upper().replace("+", "")
    if not (_OLC_PAIR_LENGTH <= len(digits) <= _OLC_MAX_LENGTH):
        return None
    if any(ch not in _OLC_ALPHABET for ch in digits):
        return None
    values = [_OLC_ALPHABET.index(ch) for ch in digits]
    # First latitude digit spans 0-180 in 20-degree steps (max index 8); first
    # longitude digit spans 0-360 (max index 17).
    if values[0] > 8 or values[1] > 17:
        return None
    latitude = -90.0
    longitude = -180.0
    lat_size = lng_size = 20.0
    for index in range(0, _OLC_PAIR_LENGTH, 2):
        if index:
            lat_size /= 20.0
            lng_size /= 20.0
        latitude += values[index] * lat_size
        longitude += values[index + 1] * lng_size
    for value in values[_OLC_PAIR_LENGTH:]:
        lat_size /= _OLC_GRID_ROWS
        lng_size /= _OLC_GRID_COLUMNS
        row, column = divmod(value, _OLC_GRID_COLUMNS)
        latitude += row * lat_size
        longitude += column * lng_size
    centre_lat = min(latitude + lat_size / 2.0, 90.0)
    centre_lng = longitude + lng_size / 2.0
    if centre_lng > 180.0:
        centre_lng -= 360.0
    return centre_lat, centre_lng


def _plus_code_pairs(form: str) -> Iterator[tuple[Any, Any]]:
    for match in _PLUS_CODE_RE.finditer(form):
        decoded = _decode_plus_code(f"{match.group(1)}+{match.group(2)}")
        if decoded is not None:
            yield decoded


def _orient_pair(
    first: float, first_hemisphere: str, second: float, second_hemisphere: str
) -> Optional[tuple[float, float]]:
    """Apply hemisphere letters and return ``(lat, lng)``.

    A letter decides its half's axis and sign (S/W negative). With no letters
    the order is lat, lng. Two letters on the same axis are rejected.
    """
    first_hemisphere = (first_hemisphere or "").upper()
    second_hemisphere = (second_hemisphere or "").upper()

    def _axis(letter: str) -> str:
        if letter in ("N", "S"):
            return "lat"
        if letter in ("E", "W"):
            return "lng"
        return ""

    first_axis = _axis(first_hemisphere)
    second_axis = _axis(second_hemisphere)
    if not first_axis and not second_axis:
        first_axis, second_axis = "lat", "lng"
    elif not first_axis:
        first_axis = "lng" if second_axis == "lat" else "lat"
    elif not second_axis:
        second_axis = "lng" if first_axis == "lat" else "lat"
    if first_axis == second_axis:
        return None

    def _signed(value: float, letter: str) -> float:
        if letter in ("S", "W"):
            return -abs(value)
        if letter in ("N", "E"):
            return abs(value)
        return value

    first = _signed(first, first_hemisphere)
    second = _signed(second, second_hemisphere)
    return (first, second) if first_axis == "lat" else (second, first)


def _dms_to_degrees(degrees: str, minutes: str, seconds: Optional[str]) -> Optional[float]:
    deg = _to_float(degrees)
    mins = _to_float(minutes)
    secs = _to_float(seconds) if seconds else 0.0
    if deg is None or mins is None or secs is None:
        return None
    if mins >= 60 or secs >= 60:
        return None
    return deg + mins / 60.0 + secs / 3600.0


def _dms_pairs(form: str) -> Iterator[tuple[Any, Any]]:
    for match in _DMS_PAIR_RE.finditer(form):
        d1, m1, s1, h1, d2, m2, s2, h2 = match.groups()
        first = _dms_to_degrees(d1, m1, s1)
        second = _dms_to_degrees(d2, m2, s2)
        if first is None or second is None:
            continue
        oriented = _orient_pair(first, h1, second, h2)
        if oriented is not None:
            yield oriented


def _hemisphere_pairs(form: str) -> Iterator[tuple[Any, Any]]:
    for match in _HEMI_PAIR_RE.finditer(form):
        n1, _deg1, h1_after_deg, h1_only, n2, _deg2, h2_after_deg, h2_only = match.groups()
        # The regex already demands a marker on both halves.
        h1 = h1_after_deg or h1_only or ""
        h2 = h2_after_deg or h2_only or ""
        first = _to_float(n1)
        second = _to_float(n2)
        if first is None or second is None:
            continue
        oriented = _orient_pair(first, h1, second, h2)
        if oriented is not None:
            yield oriented


# Precedence, highest first. The ``!3d!4d`` dropped pin outranks everything;
# explicit pin slots outrank viewport centres (``@``, ``#map=``); URL forms
# outrank free-text forms. Each extractor is tried on every candidate form
# (raw, then percent-decoded) before the next extractor runs, so decoding never
# lets a weaker format jump the queue.
_COORD_EXTRACTORS: tuple[Callable[[str], Iterator[tuple[Any, Any]]], ...] = (
    partial(_slot_pairs, _PLACE_PIN_RE, _integer_pair_always_ok),
    partial(_slot_pairs, _QUERY_SLOT_RE, _integer_pair_not_followed_by_word),
    partial(_slot_pairs, _BING_CP_RE, _integer_pair_not_followed_by_word),
    _osm_marker_pairs,
    partial(_slot_pairs, _PATH_SLOT_RE, _integer_pair_not_followed_by_word),
    partial(_slot_pairs, _AT_VIEWPORT_RE, _integer_pair_followed_by_zoom),
    partial(_slot_pairs, _OSM_MAP_HASH_RE, _integer_pair_always_ok),
    partial(_slot_pairs, _GEO_URI_RE, _integer_pair_not_followed_by_word),
    _plus_code_pairs,
    _dms_pairs,
    _hemisphere_pairs,
    partial(_slot_pairs, _BARE_PAIR_RE, _integer_pair_always_ok),
)


def parse_maps_link(text: Any, *, egypt_only: bool = True) -> Optional[GeoPin]:
    """Pull a coordinate pair out of a map link or a pasted location string.

    Accepted: Google (``!3d!4d``, ``?q=``/``q=loc:``, ``/place/``, ``/search/``,
    ``@lat,lng,zoom``), Apple ``?ll=``/``?coordinate=``, OpenStreetMap
    ``?mlat=&mlon=`` and ``#map=z/lat/lng``, Bing ``cp=lat~lng``, ``geo:`` URIs,
    full Plus Codes, DMS, decimal degrees with ° / N-S-E-W markers, and bare
    "lat, lng" / "lat; lng" / "lat lng" pairs with at least three decimals.
    Percent-encoded input is decoded (up to two passes) and Arabic-Indic /
    Persian digits and Arabic separators are folded to ASCII first.

    Short ``maps.app.goo.gl`` links carry no coordinates and resolving them needs
    a network round trip, so they return ``None`` rather than being followed.

    ``egypt_only=False`` is for detection only (the ``outside_egypt`` flag); the
    default keeps every caller that writes coordinates inside Egypt.

    Never raises.
    """
    if not text:
        return None
    factory = _pin_factory(egypt_only)
    raw = str(text)
    if len(raw) > 2000:
        raw = raw[:2000]
    try:
        forms = _candidate_forms(raw)
        for extractor in _COORD_EXTRACTORS:
            for form in forms:
                for latitude, longitude in extractor(form):
                    pin = factory(latitude, longitude)
                    if pin:
                        return pin
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({"event": "woo_maps_link_parse_failed", "error": str(exc)})
        return None
    return None


def _meta_value_map(meta_data: Any) -> dict[str, Any]:
    """Flatten Woo ``meta_data`` into ``{lowercased key: value}``.

    Mirrors ``order_sync._extract_attribution``: ``display_key``/``display_value``
    are the documented fallbacks when the raw pair is absent.
    """
    result: dict[str, Any] = {}
    if not isinstance(meta_data, (list, tuple)):
        return result
    for entry in meta_data:
        if not isinstance(entry, dict):
            continue
        key = str(entry.get("key") or entry.get("display_key") or "").strip().lower()
        if not key:
            continue
        value = entry.get("value")
        if value is None or (isinstance(value, str) and not value.strip()):
            value = entry.get("display_value")
        result.setdefault(key, value)
    return result


def _first_present(values: dict[str, Any], keys: tuple[str, ...]) -> Any:
    """First non-empty value among ``keys``, so an alias can rescue a blank."""
    for key in keys:
        if key not in values:
            continue
        value = values[key]
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _first_present_key(values: dict[str, Any], keys: tuple[str, ...]) -> Optional[str]:
    """Key whose value ``_first_present`` would return, or ``None``."""
    for key in keys:
        if key not in values:
            continue
        value = values[key]
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return key
    return None


# Pin-source labels written to ``Sales Invoice.custom_delivery_pin_source``.
PIN_SOURCE_JARZ_CHECKOUT = "jarz_checkout"
PIN_SOURCE_DROPPIN_DEFAULT = "droppin"
PIN_SOURCE_MAPS_LINK = "maps_link"
PIN_SOURCE_ADDRESS_COORDINATES = "address_coordinates"

# DropPin's own provenance / coverage verdict keys (never read before).
_DPN_PIN_SOURCE_KEYS = ("_dpn_pin_source", "dpn_pin_source")
_DPN_AREA_STATUS_KEYS = ("_dpn_area_status", "dpn_area_status")
_DPN_AREA_MISMATCH_KEYS = ("_dpn_area_mismatch", "dpn_area_mismatch")
_TRUTHY_TOKENS = frozenset({"yes", "true", "1", "y", "on"})


def _droppin_source_label(meta: dict[str, Any]) -> str:
    raw = str(_first_present(meta, _DPN_PIN_SOURCE_KEYS) or "").strip().lower()
    if not raw:
        return PIN_SOURCE_DROPPIN_DEFAULT
    return raw[:140].strip()


def _order_meta_pin(
    meta: dict[str, Any], *, egypt_only: bool = True
) -> tuple[Optional[GeoPin], str]:
    """``(pin, source_label)`` for the order-level meta pin, or ``(None, "")``."""
    if not meta:
        return None, ""
    factory = _pin_factory(egypt_only)
    lat_key = _first_present_key(meta, _ORDER_LAT_META_KEYS)
    pin = factory(
        _first_present(meta, _ORDER_LAT_META_KEYS),
        _first_present(meta, _ORDER_LNG_META_KEYS),
    )
    if pin:
        if lat_key and "dpn" in lat_key:
            return pin, _droppin_source_label(meta)
        return pin, PIN_SOURCE_JARZ_CHECKOUT
    for key in _ORDER_COMBINED_META_KEYS:
        pin = parse_maps_link(meta.get(key), egypt_only=egypt_only)
        if pin:
            return pin, PIN_SOURCE_JARZ_CHECKOUT
    return None, ""


def extract_order_pin(order: Any) -> Optional[GeoPin]:
    """Return the explicit ``_jarz_lat``/``_jarz_lng`` pin from an order payload."""
    if not isinstance(order, dict):
        return None
    meta = _meta_value_map(order.get("meta_data"))
    pin, _source = _order_meta_pin(meta, egypt_only=True)
    return pin


def _address_pin_with_source(
    address_data: Any, *, egypt_only: bool = True
) -> tuple[Optional[GeoPin], str]:
    if not isinstance(address_data, dict):
        return None, ""
    factory = _pin_factory(egypt_only)
    pin = factory(address_data.get("latitude"), address_data.get("longitude"))
    if pin:
        return pin, PIN_SOURCE_ADDRESS_COORDINATES
    for key in _WOO_ADDRESS_TEXT_KEYS:
        pin = parse_maps_link(address_data.get(key), egypt_only=egypt_only)
        if pin:
            return pin, PIN_SOURCE_MAPS_LINK
    return None, ""


def extract_address_pin(address_data: Any) -> Optional[GeoPin]:
    """Return a pin carried by a single WooCommerce billing/shipping payload.

    Only Woo payload keys are inspected. A stored ``Address`` row is never a
    valid input here -- its ``address_line2`` Maps link is ``pos_link``
    provenance, not ``customer_pin``.
    """
    pin, _source = _address_pin_with_source(address_data, egypt_only=True)
    return pin


def _woo_address_has_text(data: Any) -> bool:
    if not isinstance(data, dict):
        return False
    return any(str(data.get(key) or "").strip() for key in _WOO_ADDRESS_TEXT_KEYS)


def resolve_order_pins(order: Any) -> tuple[Optional[GeoPin], Optional[GeoPin]]:
    """Return ``(billing_pin, shipping_pin)`` for a WooCommerce order payload.

    Routing rules:

    * The order-level ``_jarz_lat``/``_jarz_lng`` meta describes the *delivery*
      destination, so it belongs to the shipping address whenever the order has
      one. Orders with no shipping block ship to billing, so it falls back there.
    * A Maps link found inside one address only ever applies to that address.
    * An explicit meta pin outranks a parsed link: it is structured data, link
      parsing is a heuristic.

    When billing and shipping are the same physical address, ``customer_sync``
    resolves both to the same ``Address`` record. Both writes then land on that
    one record at equal rank, and the order-level pin -- applied second -- wins.
    """
    if not isinstance(order, dict):
        return None, None
    billing = order.get("billing") or {}
    shipping = order.get("shipping") or {}
    order_pin = extract_order_pin(order)
    has_shipping = _woo_address_has_text(shipping)
    billing_pin = (None if has_shipping else order_pin) or extract_address_pin(billing)
    shipping_pin = (order_pin if has_shipping else None) or extract_address_pin(shipping)
    return billing_pin, shipping_pin


# Tokens written (sorted, comma-joined) to ``Sales Invoice.custom_delivery_pin_flag``.
PIN_FLAG_OUTSIDE_AREA = "outside_area"
PIN_FLAG_AREA_MISMATCH = "area_mismatch"
PIN_FLAG_OUTSIDE_EGYPT = "outside_egypt"
PIN_FLAGS: tuple[str, ...] = (PIN_FLAG_AREA_MISMATCH, PIN_FLAG_OUTSIDE_AREA, PIN_FLAG_OUTSIDE_EGYPT)


def _empty_delivery_pin() -> dict[str, Any]:
    return {"latitude": 0.0, "longitude": 0.0, "source": "", "flags": ""}


def _normalize_flags(value: Any) -> str:
    """Canonical flag string: known tokens only, de-duplicated, sorted."""
    if not value:
        return ""
    if isinstance(value, str):
        tokens = value.split(",")
    else:
        tokens = list(value)
    clean = {str(token).strip().lower() for token in tokens}
    return ",".join(sorted(token for token in clean if token in PIN_FLAGS))


def resolve_delivery_pin(order: Any) -> dict[str, Any]:
    """The pin for the order's *delivery* destination, plus its review flags.

    Returns ``{"latitude", "longitude", "source", "flags"}`` -- exactly the shape
    of the four ``Sales Invoice`` snapshot fields:

    * Routing is the same as :func:`resolve_order_pins`: the destination is the
      shipping block when it carries text, otherwise billing; an order-meta pin
      outranks a Maps link parsed out of that block's address lines.
    * ``latitude``/``longitude`` are ``0.0`` when there is no usable pin
      (Frappe Float columns are NOT NULL DEFAULT 0; never None). ``source`` is
      then ``""``.
    * ``source`` is the raw origin: DropPin's ``_dpn_pin_source`` (``prefill``,
      ``customer_checkout`` ...), ``jarz_checkout`` for our own ``_jarz_*``
      meta, ``maps_link`` for a link parsed from the address text,
      ``address_coordinates`` for explicit lat/lng keys on the Woo address.
    * ``flags`` is a sorted comma-joined subset of ``outside_area`` (DropPin's
      ``_dpn_area_status`` set and not ``inside``), ``area_mismatch``
      (``_dpn_area_mismatch`` truthy) and ``outside_egypt`` (a pin was supplied
      but lies outside the Egypt box, so it is not used).

    Never raises.
    """
    result = _empty_delivery_pin()
    try:
        if not isinstance(order, dict):
            return result
        meta = _meta_value_map(order.get("meta_data"))
        billing = order.get("billing") or {}
        shipping = order.get("shipping") or {}
        destination = shipping if _woo_address_has_text(shipping) else billing

        flags: set[str] = set()

        meta_pin, meta_source = _order_meta_pin(meta, egypt_only=True)
        address_pin, address_source = _address_pin_with_source(destination, egypt_only=True)
        if meta_pin:
            pin, source = meta_pin, meta_source
        elif address_pin:
            pin, source = address_pin, address_source
        else:
            pin, source = None, ""

        # Detection only: a world-valid pin that the Egypt check discarded.
        for raw_pin, _raw_source in (
            _order_meta_pin(meta, egypt_only=False),
            _address_pin_with_source(destination, egypt_only=False),
        ):
            if raw_pin is not None and not pin_in_egypt(raw_pin):
                flags.add(PIN_FLAG_OUTSIDE_EGYPT)

        area_status = str(_first_present(meta, _DPN_AREA_STATUS_KEYS) or "").strip().lower()
        if area_status and area_status != "inside":
            flags.add(PIN_FLAG_OUTSIDE_AREA)
        mismatch = str(_first_present(meta, _DPN_AREA_MISMATCH_KEYS) or "").strip().lower()
        if mismatch in _TRUTHY_TOKENS:
            flags.add(PIN_FLAG_AREA_MISMATCH)

        if pin is not None:
            result["latitude"] = pin.latitude
            result["longitude"] = pin.longitude
            result["source"] = source
        result["flags"] = _normalize_flags(flags)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({"event": "woo_delivery_pin_resolve_failed", "error": str(exc)})
        return _empty_delivery_pin()
    return result


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

_missing_fields_reported = False


def geo_fields_available() -> bool:
    """True when lane A4's Address geo custom fields exist on this site."""
    try:
        meta = frappe.get_meta("Address")
        if not meta:
            return False
        return all(bool(meta.get_field(fieldname)) for fieldname in GEO_WRITE_FIELDS)
    except Exception:  # noqa: BLE001
        return False


def _report_missing_geo_fields() -> None:
    """Make the pre-migration no-op visible exactly once per worker process.

    A silent skip here is indistinguishable from "no customer ever sent a pin",
    which is precisely how a bug hides for a month. ``frappe.logger`` output is
    filtered at ERROR level on staging/production, so one Error Log row is filed
    as well -- once, not per order.
    """
    global _missing_fields_reported  # noqa: PLW0603
    LOGGER.warning({"event": "woo_geo_pin_skipped", "reason": "address_geo_fields_missing"})
    if _missing_fields_reported:
        return
    _missing_fields_reported = True
    try:
        frappe.log_error(
            title="Woo: Address geo fields missing - pin passthrough disabled",
            message=(
                "jarz_woocommerce_integration.services.geo_passthrough found no "
                f"{', '.join(GEO_WRITE_FIELDS)} on Address. Customer map pins from "
                "WooCommerce are being discarded. These fields are owned by jarz_pos "
                "(courier lane A4); run `bench migrate` once that lane is deployed."
            ),
        )
    except Exception:  # noqa: BLE001
        pass


def stored_confidence_rank(row: Optional[dict[str, Any]]) -> int:
    """Rank currently held by an Address row.

    ``custom_geo_source`` is the label and ``custom_geo_confidence`` its derived
    twin; they are always written together and must agree. If stored data
    disagrees anyway, take the higher of the pair so an ambiguous row can never
    be downgraded by accident.
    """
    if not row:
        return 0
    source = str(row.get("custom_geo_source") or "").strip()
    rank_from_source = CONFIDENCE_RANK.get(source, 0)
    try:
        stored_rank = int(row.get("custom_geo_confidence") or 0)
    except (TypeError, ValueError):
        stored_rank = 0
    return max(rank_from_source, stored_rank)


def apply_geo_pin(
    address_name: Optional[str],
    latitude: Any,
    longitude: Any,
    source: str = GEO_SOURCE_CUSTOMER_PIN,
    *,
    accuracy_m: Any = None,
) -> bool:
    """Write a pin onto an Address, honouring the confidence ladder.

    This is the single write path for the Woo side of the geo fields, and it is
    called on **both** branches of ``existing or _create_address(...)``: a
    create-only implementation would silently never update a returning
    customer's pin, and returning customers are most customers.

    Two rank rules apply, in both directions. A write is accepted only when
    ``incoming_rank >= stored_rank`` (never downgrade), and refused outright when
    ``incoming_rank > MAX_WRITABLE_RANK`` (this module is capped at
    ``customer_pin``). A write that *moves* the coordinates also writes
    ``custom_geo_accuracy_m`` -- NULL when the caller supplies none -- so a
    radius can never outlive the point it was measured for.

    Returns ``True`` when the Address carries the incoming pin at the incoming
    source once the call returns (written now, or already identical). Returns
    ``False`` when there was nothing usable to write, the write was outranked or
    over the cap, or the write failed. Never raises -- this runs inside order
    sync.
    """
    if not address_name:
        return False

    pin = make_pin(latitude, longitude)
    if pin is None:
        return False

    source_label = str(source or "").strip()
    incoming_rank = CONFIDENCE_RANK.get(source_label, 0)
    if not incoming_rank:
        LOGGER.warning({
            "event": "woo_geo_pin_rejected",
            "reason": "unknown_source",
            "address": address_name,
            "source": source,
        })
        return False

    if incoming_rank > MAX_WRITABLE_RANK:
        # This module is capped at customer_pin. Anything above it belongs to
        # jarz_pos/services/geo_resolution.py, the other authorised writer.
        LOGGER.warning({
            "event": "woo_geo_pin_rejected",
            "reason": "rank_above_cap",
            "address": address_name,
            "source": source_label,
            "incoming_rank": incoming_rank,
            "max_writable_rank": MAX_WRITABLE_RANK,
        })
        return False

    if not geo_fields_available():
        _report_missing_geo_fields()
        return False

    try:
        row = frappe.db.get_value(
            "Address", address_name, list(_ADDRESS_READ_FIELDS), as_dict=True
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_geo_pin_read_failed",
            "address": address_name,
            "error": str(exc),
        })
        return False
    if not row:
        return False

    current_rank = stored_confidence_rank(row)
    if incoming_rank < current_rank:
        # Silently ignored, never an error: a routine re-sync must not fail
        # because the address already carries a better pin.
        LOGGER.info({
            "event": "woo_geo_pin_ignored",
            "reason": "lower_confidence",
            "address": address_name,
            "incoming_source": source_label,
            "incoming_rank": incoming_rank,
            "current_rank": current_rank,
        })
        return False

    coordinates_match = (
        _same_coordinate(row.get("custom_latitude"), pin.latitude)
        and _same_coordinate(row.get("custom_longitude"), pin.longitude)
    )
    source_matches = str(row.get("custom_geo_source") or "").strip() == source_label
    if coordinates_match and source_matches and accuracy_m is None:
        return True

    updates: dict[str, Any] = {
        "custom_latitude": pin.latitude,
        "custom_longitude": pin.longitude,
        "custom_geo_source": source_label,
        "custom_geo_confidence": incoming_rank,
    }
    accuracy = _to_float(accuracy_m)
    if accuracy is not None and accuracy >= 0:
        updates[GEO_ACCURACY_FIELD] = round(accuracy, 2)
    elif not coordinates_match:
        # Contract section 3: any write that moves the coordinates must write the
        # new accuracy, or clear it when the incoming source carries none.
        # WooCommerce pins never carry one. Leaving the previous value would
        # leave a radius describing a point that is no longer there, which
        # silently corrupts the downstream consensus-hardening job. Accuracy is
        # only meaningful paired with the coordinates it was measured for, so it
        # is preserved when the pin has not actually moved.
        #
        # Cleared to 0, NOT None. Frappe creates Float columns as NOT NULL
        # DEFAULT 0, so None raises (1048) "Column 'custom_geo_accuracy_m' cannot
        # be null" and takes the entire pin write with it. Since a Woo pin never
        # has an accuracy, that would have failed EVERY passthrough write —
        # caught on staging only because a live column rejected it; a mocked
        # frappe.db.set_value accepts None happily.
        #
        # 0 means "no accuracy reported", not "accurate to 0 m".
        updates[GEO_ACCURACY_FIELD] = 0.0

    # Structural belt-and-braces: nothing outside the geo allow-list can reach
    # the DB from here, so no text field can ever join the same write and wake
    # the Woo outbound hooks.
    updates = {key: value for key, value in updates.items() if key in _ALLOWED_UPDATE_FIELDS}

    try:
        # update_modified=False keeps the Address timestamp untouched (no
        # TimestampMismatchError for a concurrently open Desk form) and
        # db.set_value bypasses the document layer, so on_update never fires.
        frappe.db.set_value("Address", address_name, updates, update_modified=False)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_geo_pin_write_failed",
            "address": address_name,
            "error": str(exc),
        })
        return False

    LOGGER.info({
        "event": "woo_geo_pin_applied",
        "address": address_name,
        "source": source_label,
        "confidence": incoming_rank,
        "previous_rank": current_rank,
    })
    return True


# ---------------------------------------------------------------------------
# Order-level helpers (used by api/geo.py and the repair job)
# ---------------------------------------------------------------------------


def apply_order_pins(
    order: Any,
    *,
    billing_address: Optional[str] = None,
    shipping_address: Optional[str] = None,
) -> dict[str, Any]:
    """Apply a Woo order's pins to already-resolved Address names."""
    billing_pin, shipping_pin = resolve_order_pins(order)
    result: dict[str, Any] = {
        "billing_address": billing_address,
        "shipping_address": shipping_address,
        "billing_pin": billing_pin._asdict() if billing_pin else None,
        "shipping_pin": shipping_pin._asdict() if shipping_pin else None,
        "billing_applied": False,
        "shipping_applied": False,
    }
    if billing_address and billing_pin:
        result["billing_applied"] = apply_geo_pin(
            billing_address, billing_pin.latitude, billing_pin.longitude, GEO_SOURCE_CUSTOMER_PIN
        )
    if shipping_address and shipping_pin:
        result["shipping_applied"] = apply_geo_pin(
            shipping_address, shipping_pin.latitude, shipping_pin.longitude, GEO_SOURCE_CUSTOMER_PIN
        )
    return result


# ---------------------------------------------------------------------------
# Per-invoice delivery-pin snapshot
# ---------------------------------------------------------------------------
#
# The Address pin is shared by every order a customer places to that address,
# so a later order with a different pin silently changed what earlier invoices
# showed. The snapshot freezes, per Sales Invoice, the pin THIS order carried.
#
# The four fields are created by jarz_pos (it owns the Sales Invoice schema);
# until they exist on a site everything below is a no-op. Writes go through
# ``frappe.db.set_value(..., update_modified=False)``: it works on a submitted
# invoice, fires no document hook and therefore can never reach the Woo
# outbound push (invariant 2 above applies to invoices just as to Addresses).

# Deliberately the *_pin_* pair: the plain custom_delivery_{lat,long}itude fields
# already exist on Sales Invoice and hold the courier's proof-of-delivery GPS.
# This module must never write those.
SI_PIN_LATITUDE_FIELD = "custom_delivery_pin_latitude"
SI_PIN_LONGITUDE_FIELD = "custom_delivery_pin_longitude"
SI_PIN_SOURCE_FIELD = "custom_delivery_pin_source"
SI_PIN_FLAG_FIELD = "custom_delivery_pin_flag"
INVOICE_PIN_FIELDS: tuple[str, ...] = (
    SI_PIN_LATITUDE_FIELD,
    SI_PIN_LONGITUDE_FIELD,
    SI_PIN_SOURCE_FIELD,
    SI_PIN_FLAG_FIELD,
)

_FLAG_MESSAGES: dict[str, str] = {
    PIN_FLAG_OUTSIDE_AREA: (
        "Customer pin is outside the delivery area chosen at checkout - check the "
        "location before dispatch / دبوس العميل خارج منطقة التوصيل المختارة - راجع "
        "الموقع قبل الإرسال"
    ),
    PIN_FLAG_AREA_MISMATCH: (
        "Customer pin does not match the area selected at checkout - confirm the "
        "address before dispatch / دبوس العميل لا يطابق المنطقة المختارة عند الطلب - "
        "تأكد من العنوان قبل الإرسال"
    ),
    PIN_FLAG_OUTSIDE_EGYPT: (
        "Customer pin is outside Egypt and was ignored - get the correct location "
        "from the customer / دبوس العميل خارج مصر وتم تجاهله - اطلب الموقع الصحيح "
        "من العميل"
    ),
}


def invoice_pin_fields_available() -> bool:
    """True when jarz_pos's four Sales Invoice delivery-pin fields exist."""
    try:
        meta = frappe.get_meta("Sales Invoice")
        if not meta:
            return False
        return all(bool(meta.get_field(fieldname)) for fieldname in INVOICE_PIN_FIELDS)
    except Exception:  # noqa: BLE001
        return False


def _snapshot_values(delivery: dict[str, Any]) -> dict[str, Any]:
    return {
        SI_PIN_LATITUDE_FIELD: float(delivery.get("latitude") or 0.0),
        SI_PIN_LONGITUDE_FIELD: float(delivery.get("longitude") or 0.0),
        SI_PIN_SOURCE_FIELD: str(delivery.get("source") or ""),
        SI_PIN_FLAG_FIELD: _normalize_flags(delivery.get("flags")),
    }


def _stored_float(value: Any) -> float:
    number = _to_float(value)
    return number if number is not None else 0.0


def _snapshot_is_empty(row: Optional[dict[str, Any]]) -> bool:
    if not row:
        return True
    return (
        abs(_stored_float(row.get(SI_PIN_LATITUDE_FIELD))) < _COORD_EPSILON
        and abs(_stored_float(row.get(SI_PIN_LONGITUDE_FIELD))) < _COORD_EPSILON
        and not str(row.get(SI_PIN_SOURCE_FIELD) or "").strip()
        and not _normalize_flags(row.get(SI_PIN_FLAG_FIELD))
    )


def _snapshots_equal(stored: Optional[dict[str, Any]], desired: dict[str, Any]) -> bool:
    """Compare a stored SI row with :func:`_snapshot_values` output.

    An absent / never-written snapshot reads as 0, 0, "", "" -- equal to an
    order with no pin and no flags, so historical pin-less orders never look
    stale.
    """
    stored = stored or {}
    return (
        abs(_stored_float(stored.get(SI_PIN_LATITUDE_FIELD)) - desired[SI_PIN_LATITUDE_FIELD]) < _COORD_EPSILON
        and abs(_stored_float(stored.get(SI_PIN_LONGITUDE_FIELD)) - desired[SI_PIN_LONGITUDE_FIELD]) < _COORD_EPSILON
        and str(stored.get(SI_PIN_SOURCE_FIELD) or "").strip() == desired[SI_PIN_SOURCE_FIELD]
        and _normalize_flags(stored.get(SI_PIN_FLAG_FIELD)) == desired[SI_PIN_FLAG_FIELD]
    )


def _read_invoice_snapshot(invoice_name: str) -> Optional[dict[str, Any]]:
    row = frappe.db.get_value("Sales Invoice", invoice_name, list(INVOICE_PIN_FIELDS), as_dict=True)
    return dict(row) if row else None


def _flag_comment_text(flags: str, order: Any) -> str:
    woo_ref = ""
    if isinstance(order, dict):
        woo_ref = str(order.get("number") or order.get("id") or "").strip()
    header = f"WooCommerce order {woo_ref}: delivery pin needs review" if woo_ref else (
        "WooCommerce delivery pin needs review"
    )
    lines = [header]
    for token in flags.split(","):
        message = _FLAG_MESSAGES.get(token)
        if message:
            lines.append(message)
    return "<br>".join(lines)


def _add_flag_comment(invoice_name: str, flags: str, order: Any) -> bool:
    try:
        frappe.get_doc({
            "doctype": "Comment",
            "comment_type": "Comment",
            "reference_doctype": "Sales Invoice",
            "reference_name": invoice_name,
            "content": _flag_comment_text(flags, order),
        }).insert(ignore_permissions=True)
        return True
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_delivery_pin_comment_failed",
            "invoice": invoice_name,
            "flags": flags,
            "error": str(exc),
        })
        return False


def _write_invoice_pin_snapshot(order: Any, invoice_name: Any) -> dict[str, Any]:
    """Write the snapshot when it differs. Never raises.

    Returns ``{"written", "previous", "current", "comment_added"}`` where
    ``previous`` is the stored row before the write (``None`` when it could not
    be read) and ``current`` the values this order resolves to.
    """
    outcome: dict[str, Any] = {
        "written": False,
        "previous": None,
        "current": None,
        "comment_added": False,
    }
    if not invoice_name or not invoice_pin_fields_available():
        return outcome
    try:
        desired = _snapshot_values(resolve_delivery_pin(order))
        outcome["current"] = desired
        stored = _read_invoice_snapshot(invoice_name)
        if stored is None:
            return outcome
        outcome["previous"] = stored
        if _snapshots_equal(stored, desired):
            return outcome
        frappe.db.set_value("Sales Invoice", invoice_name, dict(desired), update_modified=False)
        outcome["written"] = True
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_delivery_pin_snapshot_failed",
            "invoice": invoice_name,
            "error": str(exc),
        })
        return outcome

    new_flags = desired[SI_PIN_FLAG_FIELD]
    old_flags = _normalize_flags((outcome["previous"] or {}).get(SI_PIN_FLAG_FIELD))
    if new_flags and new_flags != old_flags:
        outcome["comment_added"] = _add_flag_comment(invoice_name, new_flags, order)

    LOGGER.info({
        "event": "woo_delivery_pin_snapshot_written",
        "invoice": invoice_name,
        "source": desired[SI_PIN_SOURCE_FIELD],
        "flags": new_flags,
    })
    return outcome


def apply_invoice_pin_snapshot(order: Any, invoice_name: Any) -> bool:
    """Freeze this order's delivery pin + flags onto its Sales Invoice.

    Writes the four ``custom_delivery_*`` fields only when they differ from what
    is stored, on any docstatus, without touching ``modified`` or firing hooks.
    When the flag set becomes non-empty and differs from the stored one, one
    bilingual Comment is added to the invoice for the dispatcher. Returns True
    when something was written. No-op (False) when the fields do not exist yet.
    Never raises.
    """
    return bool(_write_invoice_pin_snapshot(order, invoice_name).get("written"))


def invoice_pin_snapshot_differs(order: Any, invoice_name: Any) -> bool:
    """True when the order's delivery pin/flags disagree with the invoice snapshot.

    Used by the order poll's no-op pre-check. Returns False -- "nothing to do",
    the pre-existing behaviour -- whenever the snapshot cannot be written
    (fields missing) or read, so it can never requeue an order forever.
    An old invoice with an empty snapshot and an order with no pin/flags is
    equal; one whose order does carry a pin differs exactly once (backfill).
    """
    if not invoice_name or not invoice_pin_fields_available():
        return False
    try:
        stored = _read_invoice_snapshot(invoice_name)
        if stored is None:
            return False
        desired = _snapshot_values(resolve_delivery_pin(order))
        return not _snapshots_equal(stored, desired)
    except Exception:  # noqa: BLE001
        return False


def refresh_invoice_pins(order: Any, invoice_name: Any) -> dict[str, Any]:
    """Re-apply an already-synced order's pin: invoice snapshot, then Address.

    Called from the ``unchanged`` branch of order processing, which a pin edit
    on Woo reaches because the order hash deliberately excludes meta.

    The Address is only moved when this order's pin was *edited* -- the stored
    snapshot held a different pin. An empty stored snapshot is the backfill of
    an invoice that predates the snapshot fields: its Address already received
    this pin when the order first synced, and re-applying it now could roll the
    shared Address back over a newer order's pin. Never raises.
    """
    result = {"snapshot_written": False, "address_refreshed": False}
    try:
        outcome = _write_invoice_pin_snapshot(order, invoice_name)
        result["snapshot_written"] = bool(outcome.get("written"))
        if not outcome.get("written"):
            return result
        previous = outcome.get("previous") or {}
        current = outcome.get("current") or {}
        if _snapshot_is_empty(previous):
            return result
        new_lat = _stored_float(current.get(SI_PIN_LATITUDE_FIELD))
        new_lng = _stored_float(current.get(SI_PIN_LONGITUDE_FIELD))
        if abs(new_lat) < _COORD_EPSILON and abs(new_lng) < _COORD_EPSILON:
            return result
        if (
            abs(_stored_float(previous.get(SI_PIN_LATITUDE_FIELD)) - new_lat) < _COORD_EPSILON
            and abs(_stored_float(previous.get(SI_PIN_LONGITUDE_FIELD)) - new_lng) < _COORD_EPSILON
        ):
            return result
        row = frappe.db.get_value(
            "Sales Invoice",
            invoice_name,
            ["customer_address", "shipping_address_name"],
            as_dict=True,
        ) or {}
        applied = apply_order_pins(
            order,
            billing_address=row.get("customer_address"),
            shipping_address=row.get("shipping_address_name"),
        )
        result["address_refreshed"] = bool(
            applied.get("billing_applied") or applied.get("shipping_applied")
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_invoice_pin_refresh_failed",
            "invoice": invoice_name,
            "error": str(exc),
        })
    return result


def _order_map_link_field() -> str:
    try:
        from jarz_woocommerce_integration.services.outbound_sync import (
            _resolve_order_map_link_field,
        )

        return _resolve_order_map_link_field()
    except Exception:  # noqa: BLE001
        return "erpnext_sales_invoice"


def resolve_invoice_addresses(woo_order_id: Any) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """``(sales_invoice, customer_address, shipping_address_name)`` for a Woo id."""
    link_field = _order_map_link_field()
    try:
        map_row = frappe.db.get_value(
            "WooCommerce Order Map", {"woo_order_id": woo_order_id}, [link_field, "status"], as_dict=True
        )
    except Exception:  # noqa: BLE001
        map_row = None
    invoice = None
    # A retired duplicate links the REAL invoice; never pin its addresses from
    # the duplicate order.
    if isinstance(map_row, dict) and not is_retired_duplicate_row(map_row):
        invoice = map_row.get(link_field)
    if not invoice:
        return None, None, None
    try:
        row = (
            frappe.db.get_value(
                "Sales Invoice",
                invoice,
                ["customer_address", "shipping_address_name"],
                as_dict=True,
            )
            or {}
        )
    except Exception:  # noqa: BLE001
        row = {}
    return invoice, row.get("customer_address"), row.get("shipping_address_name")


def build_client() -> WooClient:
    settings = WooCommerceSettings.get_settings()
    base_url = (getattr(settings, "base_url", "") or "").strip().rstrip("/")
    consumer_key = (getattr(settings, "consumer_key", "") or "").strip()
    consumer_secret = settings.get_consumer_secret()
    if not base_url or not consumer_key or not consumer_secret:
        raise ValueError("missing_credentials")
    return WooClient(
        base_url=base_url,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        api_version=(getattr(settings, "api_version", None) or "v3"),
    )


def preview_order_pin(woo_order_id: Any, *, client: Any = None) -> dict[str, Any]:
    """Read-only: what would lane O1 write for this order, and where."""
    client = client or build_client()
    order = client.get_order(woo_order_id)
    if not order:
        return {"success": False, "error": "order_not_found", "woo_order_id": woo_order_id}
    billing_pin, shipping_pin = resolve_order_pins(order)
    invoice, billing_address, shipping_address = resolve_invoice_addresses(woo_order_id)
    return {
        "success": True,
        "woo_order_id": woo_order_id,
        "sales_invoice": invoice,
        "geo_fields_available": geo_fields_available(),
        "source": GEO_SOURCE_CUSTOMER_PIN,
        "confidence": CONFIDENCE_RANK[GEO_SOURCE_CUSTOMER_PIN],
        "billing_address": billing_address,
        "shipping_address": shipping_address,
        "billing_pin": billing_pin._asdict() if billing_pin else None,
        "shipping_pin": shipping_pin._asdict() if shipping_pin else None,
        "delivery_pin": resolve_delivery_pin(order),
        "invoice_pin_fields_available": invoice_pin_fields_available(),
    }


def resync_order_pin(woo_order_id: Any, *, client: Any = None) -> dict[str, Any]:
    """Re-apply the customer pin for one already-synced Woo order.

    Writes geo fields only. It never creates or edits an Address, so it cannot
    fork a duplicate and cannot push anything back to WooCommerce.
    """
    client = client or build_client()
    order = client.get_order(woo_order_id)
    if not order:
        return {"success": False, "error": "order_not_found", "woo_order_id": woo_order_id}
    invoice, billing_address, shipping_address = resolve_invoice_addresses(woo_order_id)
    if not invoice:
        return {"success": False, "error": "order_not_mapped", "woo_order_id": woo_order_id}
    data = apply_order_pins(
        order, billing_address=billing_address, shipping_address=shipping_address
    )
    # The manual repair also repairs this invoice's own frozen copy of the pin.
    data["delivery_pin"] = resolve_delivery_pin(order)
    data["invoice_snapshot_written"] = apply_invoice_pin_snapshot(order, invoice)
    data.update({"success": True, "woo_order_id": woo_order_id, "sales_invoice": invoice})
    return data


def resync_order_pins_job(woo_order_ids: list[Any]) -> dict[str, Any]:
    """Background job body for the bulk repair endpoint (top-level for RQ)."""
    frappe.set_user("Administrator")
    summary = {"total": 0, "applied": 0, "skipped": 0, "errors": 0, "details": []}
    try:
        client = build_client()
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({"event": "woo_geo_backfill_aborted", "error": str(exc)})
        return {"success": False, "error": str(exc), **summary}

    for woo_order_id in woo_order_ids or []:
        summary["total"] += 1
        try:
            result = resync_order_pin(woo_order_id, client=client)
        except Exception as exc:  # noqa: BLE001
            summary["errors"] += 1
            summary["details"].append({"woo_order_id": woo_order_id, "error": str(exc)})
            continue
        if (
            result.get("billing_applied")
            or result.get("shipping_applied")
            or result.get("invoice_snapshot_written")
        ):
            summary["applied"] += 1
        else:
            summary["skipped"] += 1
        summary["details"].append(result)

    LOGGER.info({
        "event": "woo_geo_backfill_finished",
        "total": summary["total"],
        "applied": summary["applied"],
        "skipped": summary["skipped"],
        "errors": summary["errors"],
    })
    return {"success": True, **summary}
