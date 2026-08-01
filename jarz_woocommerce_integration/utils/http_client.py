from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlsplit, urlunsplit

import requests  # third-party
from requests.auth import HTTPBasicAuth
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning


# --- Retry / backoff tuning -------------------------------------------------
# Woo stores intermittently answer 200 with an HTML error / WAF challenge /
# maintenance page instead of JSON. Those are almost always transient, so we
# retry a small bounded number of times before giving up.
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_BASE_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 8.0

# Status codes worth retrying (rate limiting / upstream hiccups). Kept in step
# with services.sync_events.TRANSIENT_HTTP_STATUS_CODES, plus 408 (Request
# Timeout), which is transient by definition.
RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})

# --- Log sanitisation -------------------------------------------------------
BODY_SNIPPET_LIMIT = 200
SENSITIVE_QUERY_KEYS = {
    "consumer_key",
    "consumer_secret",
    "oauth_consumer_key",
    "oauth_signature",
    "oauth_token",
    "oauth_nonce",
    "access_token",
    "token",
    "password",
    "secret",
    "signature",
    "key",
}
REDACTED = "***REDACTED***"
# Woo credentials are recognisable (ck_… / cs_…); scrub them from any text we log.
_SECRET_TOKEN_RE = re.compile(r"\b(?:ck|cs)_[0-9a-zA-Z]{8,}", re.IGNORECASE)


class _PrettyRouteUnavailable(Exception):
    """Internal signal: ``/wp-json/…`` is not reaching the REST API on this store.

    Never escapes :meth:`WooClient._perform_request` — it exists purely to hand
    control to the ``?rest_route=`` fallback. See :func:`_rest_route_for`.
    """

    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail


class WooAPIError(Exception):
    """Custom exception for WooCommerce API errors."""

    def __init__(self, status_code: int, url: str, message: str, payload: dict | None = None):
        super().__init__(f"{status_code} {message}")
        self.status_code = status_code
        self.url = url
        self.message = message
        self.payload = payload or {}


class WooTransientError(WooAPIError):
    """A retryable WooCommerce failure that survived every retry attempt.

    Raised for things that are the store's problem rather than ours: HTTP 200
    with a non-JSON body (HTML error page / WAF challenge / rate-limit
    interstitial), 429s and 5xx. Callers should treat these as *soft* failures —
    skip/reschedule the affected work instead of reporting an error-level event.

    Subclasses :class:`WooAPIError` so existing ``except WooAPIError`` handlers
    keep behaving exactly as they did before.
    """


def sanitize_url(url: str) -> str:
    """Return ``url`` with any credential-bearing query parameters redacted."""

    try:
        parts = urlsplit(str(url))
    except Exception:  # noqa: BLE001
        return "<unparsable-url>"
    if not parts.query:
        return _sanitize_text(str(url))
    pairs = [
        (key, REDACTED if key.lower() in SENSITIVE_QUERY_KEYS else value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
    ]
    clean = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(pairs), parts.fragment))
    return _sanitize_text(clean)


def _sanitize_text(text: str) -> str:
    """Strip anything that looks like a Woo consumer key/secret from ``text``."""

    return _SECRET_TOKEN_RE.sub(REDACTED, str(text))


def body_snippet(response: Any, limit: int = BODY_SNIPPET_LIMIT) -> str:
    """Short, whitespace-collapsed, credential-free preview of a response body.

    Used purely for diagnostics (Sentry/logs) so we can tell *why* a 200 was not
    JSON: an HTML error page, a "Just a moment..." challenge, an empty body, ...
    """

    try:
        text = response.text or ""
    except Exception:  # noqa: BLE001
        return "<unreadable-body>"
    collapsed = " ".join(str(text).split())
    if not collapsed:
        return "<empty-body>"
    collapsed = _sanitize_text(collapsed)
    if len(collapsed) > limit:
        return collapsed[:limit] + "..."
    return collapsed


def content_type(response: Any) -> str:
    try:
        return str(response.headers.get("Content-Type") or "unknown")
    except Exception:  # noqa: BLE001
        return "unknown"


def _retry_after_seconds(response: Any) -> float | None:
    """Honour a numeric ``Retry-After`` header, bounded to MAX_BACKOFF_SECONDS."""

    try:
        raw = response.headers.get("Retry-After")
    except Exception:  # noqa: BLE001
        return None
    if not raw:
        return None
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return None  # HTTP-date form — fall back to our own backoff
    if value < 0:
        return None
    return min(value, MAX_BACKOFF_SECONDS)


def _backoff_delay(attempt: int, base_seconds: float) -> float:
    """Exponential backoff with jitter: ~base, ~2*base, ~4*base (capped)."""

    if base_seconds <= 0:
        return 0.0
    delay = min(base_seconds * (2 ** max(0, attempt - 1)), MAX_BACKOFF_SECONDS)
    # Partial jitter — keeps half the delay deterministic, spreads the rest.
    return delay * (0.5 + random.random() / 2)


def _should_bypass_ssl_verification(base_url: str) -> bool:
    """Allow a temporary SSL bypass only for staging ERP talking to demo Woo."""

    try:
        import frappe
    except Exception:  # noqa: BLE001
        return False

    hostname = (urlparse(base_url).hostname or "").lower()
    if hostname != "demo.orderjarz.com":
        return False

    host_name = str(getattr(frappe.conf, "host_name", "") or "").rstrip("/")
    if not host_name:
        try:
            host_name = str(frappe.get_site_config().get("host_name", "") or "").rstrip("/")
        except Exception:  # noqa: BLE001
            host_name = ""

    return host_name == "https://erpstg.orderjarz.com"


# WooCommerce only runs consumer key/secret authentication when the request URI
# contains "<rest-prefix>/wc/" — see WC_REST_Authentication::is_request_to_rest_api().
# A bare ?rest_route= URI does not match, so WordPress falls through to core user
# authentication and every call comes back 401 "invalid_username" no matter how
# good the credentials are. Carrying a throwaway query arg whose value holds that
# literal substring restores the match. Routing is unaffected: WordPress dispatches
# purely on rest_route and ignores unknown args.
WC_AUTH_URI_MARKER = "_wc_rest_route=/wp-json/wc/"


def _rest_route_for(resource: str, api_version: str) -> str:
    """Map a client resource onto the REST route used by ``?rest_route=``.

    ``orders`` -> ``/wc/v3/orders``; ``wp-json/jarz/v1/delivery-areas`` ->
    ``/jarz/v1/delivery-areas``.
    """

    resource = resource.lstrip("/")
    if resource == "wp-json":
        return "/"
    if resource.startswith("wp-json/"):
        return "/" + resource[len("wp-json/"):]
    return f"/wc/{api_version}/{resource}"


def _is_json_response(response: Any) -> bool:
    """True when the body actually parses as JSON.

    A live WP REST API answers JSON for *everything*, successes and errors
    alike. An HTML body therefore means we never reached the REST API — the
    request was served by the theme, the web server, or a WAF.
    """

    try:
        response.json()
    except Exception:  # noqa: BLE001
        return False
    return True


def _log_rest_route_fallback(base_url: str, detail: str) -> None:
    """Record the permalink degradation once an hour, per store.

    Deliberately an Error Log rather than ``logger().info()``: default log level
    on staging/production is ERROR, so info/warning would never be seen.
    """

    try:
        import frappe

        cache_key = f"woo_rest_route_fallback::{urlparse(base_url).hostname or base_url}"
        if frappe.cache().get_value(cache_key):
            return
        frappe.cache().set_value(cache_key, "1", expires_in_sec=3600)
        frappe.log_error(
            title="WooCommerce: falling back to ?rest_route=",
            message=(
                f"The /wp-json/ route on {sanitize_url(base_url)} is not reaching the REST API, "
                f"so this client switched to the ?rest_route= form.\n\n"
                f"Sync keeps working, but fix the store: this is almost always WordPress "
                f"Settings -> Permalinks set to 'Plain'. Choose 'Post name' and save.\n\n"
                f"Trigger: {detail}"
            ),
        )
    except Exception:  # noqa: BLE001
        # Logging must never take the sync down with it.
        pass


@dataclass
class WooClient:
    base_url: str
    consumer_key: str
    consumer_secret: str
    api_version: str = "v3"
    timeout: int = 60
    verify_ssl: bool | None = None
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    backoff_base_seconds: float = DEFAULT_BACKOFF_BASE_SECONDS
    # Talk ?rest_route= instead of /wp-json/. Auto-enabled (and sticky) the first
    # time the pretty route proves to be unrouted; set it up-front to skip the probe.
    use_rest_route: bool = False
    _session: requests.Session | None = None

    def __post_init__(self):
        self._session = requests.Session()
        self._session.auth = HTTPBasicAuth(self.consumer_key, self.consumer_secret)
        if self.verify_ssl is None:
            self.verify_ssl = not _should_bypass_ssl_verification(self.base_url)
        self._session.verify = self.verify_ssl
        if self.verify_ssl is False:
            disable_warnings(InsecureRequestWarning)

    def _build_url(self, resource: str, *, rest_route: bool = False) -> str:
        resource = resource.lstrip("/")
        if rest_route:
            # ?rest_route= is served by index.php directly, so it needs no
            # rewrite rules and survives "Plain" permalinks. WC_AUTH_URI_MARKER
            # keeps WooCommerce's authentication engaged — without it the call
            # routes fine but 401s. Both must be present.
            path, _, query = resource.partition("?")
            route = quote(_rest_route_for(path, self.api_version), safe="/")
            url = f"{self.base_url}/?rest_route={route}&{WC_AUTH_URI_MARKER}"
            return f"{url}&{query}" if query else url
        if resource.startswith("wp-json"):
            return f"{self.base_url}/{resource}"
        return f"{self.base_url}/wp-json/wc/{self.api_version}/{resource}"

    def _sleep_before_retry(self, attempt: int, response: Any | None = None) -> None:
        if self.backoff_base_seconds <= 0:
            return  # explicitly disabled (tests / callers that want fail-fast)
        delay = _retry_after_seconds(response) if response is not None else None
        if delay is None:
            delay = _backoff_delay(attempt, self.backoff_base_seconds)
        if delay > 0:
            time.sleep(delay)

    def _perform_request(
        self,
        method: str,
        resource: str,
        *,
        params: dict | None = None,
        data: dict | None = None,
    ) -> tuple[Any, dict]:
        """Issue a Woo request, falling back to ``?rest_route=`` if needed.

        Stores whose WordPress permalinks are set to "Plain" never register the
        ``wp-json`` rewrite rule, so ``/wp-json/…`` is answered by the theme or
        the web server instead of the REST API. That took staging's sync down
        for seven weeks. On the one unambiguous signature of that — a
        non-retryable 4xx carrying a non-JSON body — retry through
        ``?rest_route=``, which needs no rewrite rules, and stay on that form
        for the rest of this client's life.

        Transient shapes (200/5xx/429 with an HTML body) deliberately do *not*
        trigger the fallback: those mean a WAF challenge or a struggling store,
        where a second URL shape would only double the load.

        Raises:
            WooAPIError: for genuine, non-transient errors (auth, 4xx, ...) — loud
                and immediate, no pointless retries.
            WooTransientError: when a retryable failure (200-with-non-JSON, 429,
                5xx) survived every attempt. The message carries a sanitized body
                snippet + Content-Type so the cause is visible in logs/Sentry.
        """

        try:
            return self._perform_request_in_mode(
                method, resource, params=params, data=data, rest_route=self.use_rest_route
            )
        except _PrettyRouteUnavailable as signal:
            detail = signal.detail

        self.use_rest_route = True
        _log_rest_route_fallback(self.base_url, detail)
        return self._perform_request_in_mode(
            method, resource, params=params, data=data, rest_route=True
        )

    def _perform_request_in_mode(
        self,
        method: str,
        resource: str,
        *,
        params: dict | None = None,
        data: dict | None = None,
        rest_route: bool = False,
    ) -> tuple[Any, dict]:
        """Issue a Woo request in one URL mode, retrying transient failures.

        Raises :class:`_PrettyRouteUnavailable` (pretty mode only) on a
        non-retryable 4xx with a non-JSON body — the REST API was never reached.
        """

        url = self._build_url(resource, rest_route=rest_route)
        safe_url = sanitize_url(url)
        session = self._session or requests
        attempts = max(1, int(self.max_attempts or DEFAULT_MAX_ATTEMPTS))

        for attempt in range(1, attempts + 1):
            is_last = attempt >= attempts
            try:
                response = session.request(
                    method.upper(), url, params=params, json=data, timeout=self.timeout
                )
            except requests.RequestException:
                # Connection resets / timeouts are transient too. Re-raise the
                # original exception if we run out of attempts so existing
                # callers keep seeing the exception type they expect today.
                if is_last:
                    raise
                self._sleep_before_retry(attempt)
                continue

            status_code = response.status_code

            if status_code >= 400:
                payload = self._error_payload(response)
                message = payload.get("message") or "HTTP error"
                # A live REST API answers JSON even when it is refusing us, so an
                # HTML error body means the request never got there.
                unrouted = not rest_route and not _is_json_response(response)
                if status_code in RETRYABLE_STATUS_CODES:
                    if is_last:
                        # Deliberately no fallback here: a 5xx/429 HTML body is
                        # a struggling or throttling store, and a second URL
                        # shape would just double the load we put on it.
                        raise WooTransientError(
                            status_code,
                            safe_url,
                            f"{message} (transient, {attempts} attempts)",
                            payload,
                        )
                    self._sleep_before_retry(attempt, response)
                    continue
                if unrouted:
                    # Classic missing-rewrite signature: the web server 404s
                    # /wp-json/ because no such directory exists on disk.
                    raise _PrettyRouteUnavailable(
                        f"HTTP {status_code} with a {content_type(response)} body at {safe_url}"
                    )
                # Auth failures / 4xx are real bugs on our side — stay loud.
                raise WooAPIError(status_code, safe_url, message, payload)

            try:
                body = response.json() if response.text else {}
            except ValueError as exc:
                # HTTP 200 with a non-JSON body: HTML error/maintenance page, WAF
                # or bot-block challenge, rate-limit interstitial, ...
                snippet = body_snippet(response)
                ctype = content_type(response)
                if is_last:
                    # Also no fallback: HTTP 200 + HTML is a WAF challenge or a
                    # maintenance page, not a missing rewrite rule.
                    raise WooTransientError(
                        status_code,
                        safe_url,
                        (
                            f"Invalid JSON response after {attempts} attempts: {exc} "
                            f"| content_type={ctype} | body={snippet}"
                        ),
                        {
                            "message": "Invalid JSON response",
                            "content_type": ctype,
                            "body_snippet": snippet,
                            "attempts": attempts,
                            "url": safe_url,
                        },
                    ) from exc
                self._sleep_before_retry(attempt, response)
                continue

            return body, dict(response.headers)

        # Unreachable: every branch above either returns or raises on the last attempt.
        raise WooTransientError(0, safe_url, f"Request failed after {attempts} attempts")

    @staticmethod
    def _error_payload(response: Any) -> dict:
        """Best-effort structured payload for a >=400 response."""

        try:
            payload = response.json()
        except Exception:  # noqa: BLE001
            payload = None
        if not isinstance(payload, dict):
            payload = {
                "message": body_snippet(response, 500),
                "content_type": content_type(response),
            }
        return payload

    def _request(self, method: str, resource: str, *, params: dict | None = None, data: dict | None = None) -> dict:
        body, _headers = self._perform_request(method, resource, params=params, data=data)
        return body

    def _request_raw(self, method: str, resource: str, *, params: dict | None = None, data: dict | None = None) -> tuple[dict | list, dict]:
        """Like _request but returns (parsed_body, response_headers) tuple."""
        return self._perform_request(method, resource, params=params, data=data)

    def get(self, resource: str, params: dict | None = None) -> dict:
        return self._request("GET", resource, params=params)

    def post(self, resource: str, data: dict) -> dict:
        return self._request("POST", resource, data=data)

    def put(self, resource: str, data: dict) -> dict:
        return self._request("PUT", resource, data=data)

    def delete(self, resource: str, params: dict | None = None) -> dict:
        return self._request("DELETE", resource, params=params)

    def list_orders(self, per_page: int | None = None, params: dict | None = None) -> list[dict]:
        """List orders with flexible pagination.

        Backwards compatible: if only per_page provided it behaves like original
        method. New order sync passes a params dict including page/per_page.
        """
        if params is None:
            # Legacy usage path
            per_page = per_page or 10
            query = {"per_page": per_page}
        else:
            # Ensure auth + not mutating caller's dict
            query = params.copy()
            if per_page is not None and "per_page" not in query:
                query["per_page"] = per_page
        data = self.get("orders", params=query)
        return data if isinstance(data, list) else []

    def list_orders_with_meta(self, params: dict | None = None) -> tuple[list[dict], int, int]:
        """List orders and return (orders, total_count, total_pages) from WP headers."""
        query = (params or {}).copy()
        body, headers = self._request_raw("GET", "orders", params=query)
        orders = body if isinstance(body, list) else []
        # Normalise header keys to lowercase — some servers/proxies lowercase them
        norm = {k.lower(): v for k, v in headers.items()}
        try:
            total_count = int(norm.get("x-wp-total", 0))
        except (ValueError, TypeError):
            total_count = 0
        try:
            total_pages = int(norm.get("x-wp-totalpages", 0))
        except (ValueError, TypeError):
            total_pages = 0
        return orders, total_count, total_pages

    def get_order(self, order_id: int | str) -> dict | None:
        try:
            data = self.get(f"orders/{order_id}")
            return data if isinstance(data, dict) else None
        except WooAPIError:
            return None

    def list_customers(self, params: dict | None = None) -> list[dict]:
        params = params or {"per_page": 100}
        data = self.get("customers", params=params)
        return data if isinstance(data, list) else []

    def get_customer(self, customer_id: int | str) -> dict | None:
        try:
            data = self.get(f"customers/{customer_id}")
            return data if isinstance(data, dict) else None
        except WooAPIError:
            return None

    def list_delivery_areas(self) -> list[dict]:
        """Fetch custom delivery areas from custom REST route if available.

        Expects Woo snippet exposing: /wp-json/jarz/v1/delivery-areas returning
        {"areas": [{"code":...,"label":...,"en":...,"ar":...,"express":bool}, ...]}
        """
        try:
            data = self.get("wp-json/jarz/v1/delivery-areas")
        except WooAPIError:
            return []
        areas = data.get("areas") if isinstance(data, dict) else []
        return areas if isinstance(areas, list) else []
