"""ERPNext -> WooCommerce customer tracking link (lane E3).

What this module is, and deliberately is not
--------------------------------------------
ERPNext mints an opaque tracking token per Sales Invoice and serves a public
tracking page for it. WooCommerce's only job is to **store and display a URL**:
it never polls ERPNext, never receives a courier position, never holds live
state. Every live update happens in the *customer's browser*, talking directly
to the ERPNext page. There is therefore no Woo<->ERPNext live traffic to build
here -- only two order meta keys to keep current:

* ``_jarz_tracking_url``   -- the full, customer-shareable URL
* ``_jarz_tracking_token`` -- the opaque token alone (so the WordPress side can
  rebuild the URL if the public host ever moves, without a re-sync)

Both are written as part of the ordinary outbound order payload, so they travel
through the existing outbox/reconcile machinery and inherit its retries,
idempotency and kill-switches. There is no second HTTP call and no new queue.

Domain isolation
----------------
The token is minted by ``jarz_pos``. This module **must not import it** --
``jarz_pos`` and ``jarz_woocommerce_integration`` are independent apps. The
contract between them is a plain Sales Invoice custom field, read defensively:

* ``custom_tracking_token`` -- the opaque token (owned by ``jarz_pos``)
* ``custom_tracking_url``   -- optional; when a site stores the whole URL, it
  wins over anything built here

Neither field is declared by this app. ``tabSales Invoice`` is at MariaDB's hard
65,535-byte row limit (247 columns -- see COURIER_CONTRACTS.md section 2), so no
app may add another varchar column to it; and declaring a field another app owns
invites ``cleanup.remove_colliding_custom_fields_for_fixtures`` to delete
whichever copy migrated second. We read, we never own.

Silence is the failure mode to fear
-----------------------------------
A tracking link that quietly stops being pushed looks exactly like "no order has
a token yet". The staging outbound kill-switches sat off for seven weeks in
exactly that shape. So every skip that is *configuration* rather than *data* --
flag on but base URL blank, flag on but the token field missing -- files one
rate-limited Error Log, and the flag itself is surfaced on the sync dashboard.
Nothing here raises: an order must ship even when its tracking link cannot.
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote

import frappe

LOGGER = frappe.logger("jarz_woocommerce.tracking")


# ---------------------------------------------------------------------------
# Wire contract -- the exact keys both sides agree on
# ---------------------------------------------------------------------------
#: WooCommerce order meta keys. The leading underscore makes them "protected"
#: meta in WordPress (hidden from the admin custom-fields box), which is what we
#: want -- they are machine data, not an editable note. Protected meta on
#: *orders* is still returned by and writable through the WC REST v3 API; the
#: ORDDD ``_orddd_*`` keys this app already round-trips are the living proof.
TRACKING_URL_META_KEY = "_jarz_tracking_url"
TRACKING_TOKEN_META_KEY = "_jarz_tracking_token"

TRACKING_META_KEYS: tuple[str, ...] = (TRACKING_URL_META_KEY, TRACKING_TOKEN_META_KEY)

#: Sales Invoice fields read (never written, never declared) by this module.
INVOICE_TOKEN_FIELD = "custom_tracking_token"
INVOICE_URL_FIELD = "custom_tracking_url"

INVOICE_TRACKING_FIELDS: tuple[str, ...] = (INVOICE_TOKEN_FIELD, INVOICE_URL_FIELD)

#: Substituted into ``tracking_base_url`` when present, so an operator can put
#: the token anywhere in the URL (``/track/{token}``, ``?t={token}``, ...).
TOKEN_PLACEHOLDER = "{token}"

#: An opaque token is generated, not typed. Anything outside this alphabet is a
#: sign the field holds something else entirely (a name, a note, an HTML
#: fragment) and must not be interpolated into a URL that reaches customers by
#: email and WhatsApp.
_TOKEN_RE = re.compile(r"^[A-Za-z0-9_.\-]{8,128}$")

_ALLOWED_URL_SCHEMES = ("https://", "http://")


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def tracking_enabled(settings: Any) -> bool:
    """Whether the tracking-link push is switched on.

    Defaults to off. This is the third master outbound kill-switch and, like the
    two before it, it is surfaced on the sync dashboard
    (``api/sync_events._settings_summary``) rather than only living in Settings.
    """
    return bool(getattr(settings, "enable_outbound_tracking_url", 0))


def tracking_base_url(settings: Any) -> str:
    return str(getattr(settings, "tracking_base_url", "") or "").strip()


_reported_config_problems: set[str] = set()


def _report_config_problem(problem: str, detail: str) -> None:
    """Surface a configuration-shaped skip exactly once per hour per problem.

    ``frappe.logger().info()/.warning()`` is below the servers' default log level
    and would never be seen, so this files an Error Log. Rate-limited through the
    cache because it is reached from a per-order code path; the in-process set is
    a second belt so a cache outage cannot turn it into a flood.
    """
    LOGGER.warning({"event": "woo_tracking_skipped", "reason": problem})
    if problem in _reported_config_problems:
        return
    try:
        cache_key = f"woo_tracking_config_problem::{problem}"
        if frappe.cache().get_value(cache_key):
            _reported_config_problems.add(problem)
            return
        frappe.cache().set_value(cache_key, "1", expires_in_sec=3600)
    except Exception:  # noqa: BLE001
        pass
    _reported_config_problems.add(problem)
    try:
        frappe.log_error(
            title="WooCommerce: tracking link not pushed ({0})".format(problem),
            message=detail,
        )
    except Exception:  # noqa: BLE001
        pass


def reset_config_report_state() -> None:
    """Test seam: clear the once-per-process report latch."""
    _reported_config_problems.clear()


# ---------------------------------------------------------------------------
# Token / URL handling
# ---------------------------------------------------------------------------

def normalize_token(raw: Any) -> str:
    """Return a syntactically valid opaque token, or ``""``."""
    token = str(raw or "").strip()
    if not token:
        return ""
    return token if _TOKEN_RE.match(token) else ""


def _normalize_stored_url(raw: Any) -> str:
    url = str(raw or "").strip()
    if not url:
        return ""
    if not url.lower().startswith(_ALLOWED_URL_SCHEMES):
        return ""
    # A newline in a URL that is injected into an email body is a header-ish
    # smell; there is no legitimate whitespace in a tracking link.
    if any(char.isspace() for char in url):
        return ""
    return url


def build_tracking_url(base_url: Any, token: Any) -> str:
    """Compose the public tracking URL from the configured base and a token.

    Two supported shapes, and no guessing beyond them:

    1. The base contains ``{token}`` -- it is substituted in place. Use this for
       query-string routes (``https://erp.example.com/track?t={token}``).
    2. Otherwise the token is appended as a path segment
       (``https://erp.example.com/track`` -> ``https://erp.example.com/track/<token>``).

    Returns ``""`` for anything unusable rather than a half-formed URL: a broken
    link mailed to a customer is worse than no link at all.
    """
    base = str(base_url or "").strip()
    safe_token = normalize_token(token)
    if not base or not safe_token:
        return ""
    if not base.lower().startswith(_ALLOWED_URL_SCHEMES):
        return ""
    encoded = quote(safe_token, safe="")
    if TOKEN_PLACEHOLDER in base:
        return base.replace(TOKEN_PLACEHOLDER, encoded)
    return f"{base.rstrip('/')}/{encoded}"


# ---------------------------------------------------------------------------
# Invoice field access
# ---------------------------------------------------------------------------

def invoice_token_field_available() -> bool:
    """True when ``jarz_pos``'s tracking token field exists on this site."""
    try:
        meta = frappe.get_meta("Sales Invoice")
        return bool(meta and meta.get_field(INVOICE_TOKEN_FIELD))
    except Exception:  # noqa: BLE001
        return False


def _invoice_value(invoice: Any, fieldname: str) -> Any:
    """Read a field off a doc-or-dict without assuming it exists."""
    if invoice is None:
        return None
    getter = getattr(invoice, "get", None)
    if callable(getter):
        try:
            value = getter(fieldname)
            if value is not None:
                return value
        except Exception:  # noqa: BLE001
            pass
    return getattr(invoice, fieldname, None)


def resolve_invoice_tracking(invoice: Any, settings: Any) -> tuple[str, str]:
    """Return ``(tracking_url, tracking_token)`` for an invoice.

    A URL stored on the invoice wins over one built here: if a site has recorded
    the exact link it serves, that is authoritative and we are not entitled to
    re-derive it. Otherwise the URL is composed from the configured base URL and
    the token. Either half may come back empty, and an empty URL means "push
    nothing" -- never "push a blank".
    """
    token = normalize_token(_invoice_value(invoice, INVOICE_TOKEN_FIELD))
    stored_url = _normalize_stored_url(_invoice_value(invoice, INVOICE_URL_FIELD))
    if stored_url:
        return stored_url, token
    if not token:
        return "", ""
    return build_tracking_url(tracking_base_url(settings), token), token


# ---------------------------------------------------------------------------
# Payload construction
# ---------------------------------------------------------------------------

def build_tracking_metadata(invoice: Any, settings: Any) -> list[dict[str, str]]:
    """WooCommerce ``meta_data`` entries carrying the tracking link.

    Returns ``[]`` whenever there is nothing trustworthy to send -- switch off,
    no token, no base URL. Returning ``[]`` matters beyond tidiness: the keys are
    only compared for drift when the payload actually contains them
    (``outbound_sync._order_payload_requires_update``), so an invoice with no
    token creates no churn and the tens of thousands of historical orders are
    never marked dirty by this feature.

    Never raises. An order ships with or without its tracking link.
    """
    try:
        if not tracking_enabled(settings):
            return []

        if not invoice_token_field_available():
            _report_config_problem(
                "token_field_missing",
                "WooCommerce Settings.enable_outbound_tracking_url is ON but "
                f"Sales Invoice.{INVOICE_TOKEN_FIELD} does not exist on this site, so no "
                "tracking link can be pushed to WooCommerce. That field is owned by "
                "jarz_pos (the tracking lane); run `bench migrate` once it is deployed. "
                "This app deliberately does not declare the field -- Sales Invoice is at "
                "the MariaDB row limit and the field belongs to the other app.",
            )
            return []

        url, token = resolve_invoice_tracking(invoice, settings)
        if not url:
            if normalize_token(_invoice_value(invoice, INVOICE_TOKEN_FIELD)) and not tracking_base_url(settings):
                _report_config_problem(
                    "base_url_missing",
                    "WooCommerce Settings.enable_outbound_tracking_url is ON and invoices "
                    "carry a tracking token, but tracking_base_url is blank, so no URL can "
                    "be composed and WooCommerce orders keep no tracking link.\n\n"
                    "Set it to the public ERPNext tracking page, e.g.\n"
                    "  https://erp.orderjarz.com/track\n"
                    "or, if the token belongs in the query string, use the placeholder:\n"
                    "  https://erp.orderjarz.com/track?t={token}\n\n"
                    "Deliberately not guessed: a wrong URL reaches customers by email and "
                    "WhatsApp, which is worse than no link.",
                )
            return []

        metadata = [{"key": TRACKING_URL_META_KEY, "value": url}]
        if token:
            metadata.append({"key": TRACKING_TOKEN_META_KEY, "value": token})
        return metadata
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning({
            "event": "woo_tracking_metadata_failed",
            "invoice": getattr(invoice, "name", None),
            "error": str(exc),
        })
        return []


def describe_invoice_tracking(invoice_name: str) -> dict[str, Any]:
    """Read-only diagnostic: what would be pushed for one invoice, and why not.

    Used by the operations dashboard and by hand when "the customer has no
    tracking button" needs an answer that is not a guess.
    """
    from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
        WooCommerceSettings,
    )

    result: dict[str, Any] = {
        "invoice": invoice_name,
        "enabled": False,
        "token_field_available": invoice_token_field_available(),
        "base_url": "",
        "token": "",
        "tracking_url": "",
        "meta_keys": list(TRACKING_META_KEYS),
        "reason": None,
    }
    try:
        settings = WooCommerceSettings.get_settings()
    except Exception as exc:  # noqa: BLE001
        result["reason"] = f"settings_unavailable:{exc}"
        return result

    result["enabled"] = tracking_enabled(settings)
    result["base_url"] = tracking_base_url(settings)
    if not result["enabled"]:
        result["reason"] = "enable_outbound_tracking_url_off"
        return result
    if not result["token_field_available"]:
        result["reason"] = "token_field_missing"
        return result

    try:
        invoice = frappe.db.get_value(
            "Sales Invoice", invoice_name, list(INVOICE_TRACKING_FIELDS), as_dict=True
        )
    except Exception:  # noqa: BLE001
        invoice = None
    if not invoice:
        result["reason"] = "invoice_not_found"
        return result

    url, token = resolve_invoice_tracking(invoice, settings)
    result["token"] = token
    result["tracking_url"] = url
    if not token:
        result["reason"] = "no_token_on_invoice"
    elif not url:
        result["reason"] = "base_url_missing"
    return result


__all__ = [
    "INVOICE_TOKEN_FIELD",
    "INVOICE_TRACKING_FIELDS",
    "INVOICE_URL_FIELD",
    "TOKEN_PLACEHOLDER",
    "TRACKING_META_KEYS",
    "TRACKING_TOKEN_META_KEY",
    "TRACKING_URL_META_KEY",
    "build_tracking_metadata",
    "build_tracking_url",
    "describe_invoice_tracking",
    "invoice_token_field_available",
    "normalize_token",
    "reset_config_report_state",
    "resolve_invoice_tracking",
    "tracking_base_url",
    "tracking_enabled",
]
