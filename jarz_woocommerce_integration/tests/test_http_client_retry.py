"""Transient-failure handling for the WooCommerce HTTP client.

Regression cover for Sentry JARZ-FRAPPE-BACKEND-2P: the store answers HTTP 200
with an HTML error / WAF challenge page instead of JSON, which used to raise a
hard WooAPIError and abort the whole order-sync window.
"""

import pytest
import requests

from jarz_woocommerce_integration.services import order_sync
from jarz_woocommerce_integration.utils import http_client
from jarz_woocommerce_integration.utils.http_client import (
    WooAPIError,
    WooClient,
    WooTransientError,
)

HTML_BODY = (
    "<!DOCTYPE html>\n<html>\n  <head><title>Just a moment...</title></head>\n"
    "  <body>\n    Checking your browser before accessing the store.\n  </body>\n</html>"
)


class FakeResponse:
    """Minimal stand-in for requests.Response."""

    def __init__(self, status_code=200, text="", headers=None, json_body=None, json_raises=True):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}
        self._json_body = json_body
        self._json_raises = json_raises

    def json(self):
        if self._json_body is not None:
            return self._json_body
        if self._json_raises:
            # requests raises a ValueError subclass here
            raise ValueError("Expecting value: line 1 column 1 (char 0)")
        return {}


class FakeSession:
    """Records calls and replays a scripted list of responses."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []
        self.auth = None
        self.verify = None

    def request(self, method, url, params=None, json=None, timeout=None):
        self.calls.append({"method": method, "url": url, "params": params, "timeout": timeout})
        if not self._responses:
            raise AssertionError("FakeSession ran out of scripted responses")
        nxt = self._responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


def _client(responses, **kwargs):
    """Build a WooClient wired to a FakeSession, with sleeping disabled."""
    kwargs.setdefault("backoff_base_seconds", 0)  # no real sleeping in tests
    client = WooClient(
        base_url="https://woo.test",
        consumer_key="ck_abcdef0123456789",
        consumer_secret="cs_abcdef0123456789",
        **kwargs,
    )
    session = FakeSession(responses)
    client._session = session
    return client, session


def _json_response(body, headers=None):
    return FakeResponse(status_code=200, text="[]", headers=headers or {}, json_body=body)


def _html_response():
    return FakeResponse(
        status_code=200,
        text=HTML_BODY,
        headers={"Content-Type": "text/html; charset=UTF-8"},
    )


# --- (a) 200 + HTML body: retries, then fails soft with a diagnosable message ---


def test_html_200_retries_then_raises_transient_with_body_snippet():
    client, session = _client([_html_response(), _html_response(), _html_response()])

    with pytest.raises(WooTransientError) as excinfo:
        client.list_orders_with_meta(params={"per_page": 10})

    # Retried the configured number of times (default 3), not just once.
    assert len(session.calls) == 3

    message = str(excinfo.value)
    # Diagnosable: says WHY the JSON parse failed.
    assert "Invalid JSON response after 3 attempts" in message
    assert "content_type=text/html; charset=UTF-8" in message
    assert "<!DOCTYPE html>" in message
    assert "Just a moment..." in message
    # Snippet is whitespace-collapsed and bounded.
    assert "\n" not in message
    assert excinfo.value.status_code == 200
    assert excinfo.value.payload["content_type"] == "text/html; charset=UTF-8"
    assert excinfo.value.payload["attempts"] == 3


def test_transient_error_is_a_woo_api_error_subclass():
    """Existing `except WooAPIError` handlers must keep working unchanged."""
    assert issubclass(WooTransientError, WooAPIError)


def test_body_snippet_is_bounded_and_collapsed():
    response = FakeResponse(status_code=200, text="a\n   b\t" + ("x" * 500))
    snippet = http_client.body_snippet(response)
    assert len(snippet) <= http_client.BODY_SNIPPET_LIMIT + 3  # + "..."
    assert snippet.startswith("a b")
    assert snippet.endswith("...")


def test_empty_body_snippet_is_labelled():
    assert http_client.body_snippet(FakeResponse(status_code=200, text="")) == "<empty-body>"


def test_snippets_and_urls_never_leak_credentials():
    """No consumer key/secret may reach logs or Sentry."""
    leaky = FakeResponse(
        status_code=200,
        text="Error: invalid key ck_abcdef0123456789 secret cs_abcdef0123456789",
    )
    snippet = http_client.body_snippet(leaky)
    assert "ck_abcdef0123456789" not in snippet
    assert "cs_abcdef0123456789" not in snippet
    assert http_client.REDACTED in snippet

    safe = http_client.sanitize_url(
        "https://woo.test/wp-json/wc/v3/orders?consumer_key=ck_abc123456789&per_page=10"
    )
    assert "ck_abc123456789" not in safe
    assert "per_page=10" in safe
    # urlencode percent-escapes the asterisks, so match the bare token.
    assert "REDACTED" in safe
    # Non-sensitive URLs are passed through untouched.
    assert (
        http_client.sanitize_url("https://woo.test/wp-json/wc/v3/orders")
        == "https://woo.test/wp-json/wc/v3/orders"
    )


# --- (b) 200 + valid JSON: succeeds on the first try, no retries ---


def test_valid_json_succeeds_first_try():
    orders = [{"id": 1, "status": "processing"}]
    client, session = _client(
        [_json_response(orders, headers={"X-WP-Total": "1", "X-WP-TotalPages": "1"})]
    )

    result, total_count, total_pages = client.list_orders_with_meta(params={"per_page": 10})

    assert result == orders
    assert (total_count, total_pages) == (1, 1)
    assert len(session.calls) == 1  # no pointless retries on the happy path


# --- (c) transient on attempt 1, valid JSON on attempt 2 -> succeeds ---


def test_transient_html_then_valid_json_succeeds():
    orders = [{"id": 7, "status": "completed"}]
    client, session = _client(
        [_html_response(), _json_response(orders, headers={"X-WP-Total": "1", "X-WP-TotalPages": "1"})]
    )

    result, _total, _pages = client.list_orders_with_meta(params={"per_page": 10})

    assert result == orders
    assert len(session.calls) == 2  # recovered on the retry


def test_429_then_valid_json_succeeds():
    orders = [{"id": 9}]
    rate_limited = FakeResponse(
        status_code=429,
        text='{"message": "Too many requests"}',
        headers={"Retry-After": "0"},
        json_body={"message": "Too many requests"},
    )
    client, session = _client([rate_limited, _json_response(orders)])

    result, _total, _pages = client.list_orders_with_meta(params={"per_page": 10})

    assert result == orders
    assert len(session.calls) == 2


def test_502_exhausts_retries_and_raises_transient():
    def bad_gateway():
        return FakeResponse(status_code=502, text="<html>502 Bad Gateway</html>")

    client, session = _client([bad_gateway(), bad_gateway(), bad_gateway()])

    with pytest.raises(WooTransientError):
        client.list_orders_with_meta(params={"per_page": 10})

    assert len(session.calls) == 3


def test_connection_error_retries_then_reraises_original():
    """Network errors retry but keep their original type for existing callers."""
    client, session = _client(
        [
            requests.ConnectionError("connection reset"),
            requests.ConnectionError("connection reset"),
            requests.ConnectionError("connection reset"),
        ]
    )

    with pytest.raises(requests.ConnectionError):
        client.list_orders_with_meta(params={"per_page": 10})

    assert len(session.calls) == 3


# --- (d) 401/403 raise promptly, no retries, and stay loud (not transient) ---


@pytest.mark.parametrize("status_code", [401, 403])
def test_auth_failure_raises_immediately_without_retries(status_code):
    denied = FakeResponse(
        status_code=status_code,
        text='{"message": "Sorry, you cannot list resources."}',
        json_body={"message": "Sorry, you cannot list resources.", "code": "woocommerce_rest_cannot_view"},
    )
    client, session = _client([denied])

    with pytest.raises(WooAPIError) as excinfo:
        client.list_orders_with_meta(params={"per_page": 10})

    # Genuine auth errors stay loud: a hard WooAPIError, NOT a soft transient one.
    assert not isinstance(excinfo.value, WooTransientError)
    assert excinfo.value.status_code == status_code
    assert "cannot list resources" in str(excinfo.value)
    assert len(session.calls) == 1  # no pointless retries


def test_404_raises_immediately_without_retries():
    client, session = _client(
        [FakeResponse(status_code=404, text='{"message": "Invalid ID."}', json_body={"message": "Invalid ID."})]
    )

    with pytest.raises(WooAPIError) as excinfo:
        client.list_orders_with_meta(params={"per_page": 10})

    assert not isinstance(excinfo.value, WooTransientError)
    assert len(session.calls) == 1


# --- (e) "Plain" permalinks: /wp-json/ unrouted -> ?rest_route= fallback ----
#
# WordPress only registers the `wp-json` rewrite rule when permalinks are NOT
# "Plain". Without it the web server 404s /wp-json/ with an HTML body, because
# no such directory exists on disk. Regression cover for the seven-week staging
# sync outage of 2026-06-13.


def _html_404():
    return FakeResponse(
        status_code=404,
        text="<!DOCTYPE html><html><body>Not Found</body></html>",
        headers={"Content-Type": "text/html"},
    )


def test_html_404_falls_back_to_rest_route_and_succeeds():
    client, session = _client([_html_404(), _json_response([{"id": 1}])])

    orders, _total, _pages = client.list_orders_with_meta(params={"per_page": 10})

    assert orders == [{"id": 1}]
    assert len(session.calls) == 2
    assert session.calls[0]["url"] == "https://woo.test/wp-json/wc/v3/orders"
    assert session.calls[1]["url"] == (
        "https://woo.test/?rest_route=/wc/v3/orders&_wc_rest_route=/wp-json/wc/"
    )
    # Query params still ride along as normal query args.
    assert session.calls[1]["params"] == {"per_page": 10}


def test_rest_route_mode_is_sticky_after_first_fallback():
    """The dead pretty route is probed once, not on every single call."""
    client, session = _client([_html_404(), _json_response([]), _json_response([])])

    client.list_orders_with_meta(params={"per_page": 1})
    client.list_orders_with_meta(params={"per_page": 1})

    assert client.use_rest_route is True
    assert len(session.calls) == 3  # one dead probe, then two direct rest_route calls
    assert all("rest_route" in call["url"] for call in session.calls[1:])


def test_json_404_is_a_real_error_and_does_not_trigger_fallback():
    """"Order not found" is a legitimate JSON 404 — never a routing problem."""
    client, session = _client(
        [FakeResponse(status_code=404, text='{"message": "Invalid ID."}', json_body={"message": "Invalid ID."})]
    )

    with pytest.raises(WooAPIError):
        client.list_orders_with_meta(params={"per_page": 10})

    assert client.use_rest_route is False
    assert len(session.calls) == 1


def test_html_200_does_not_trigger_fallback():
    """A WAF challenge must not double our request load with a second URL shape."""
    client, session = _client([_html_response(), _html_response(), _html_response()])

    with pytest.raises(WooTransientError):
        client.list_orders_with_meta(params={"per_page": 10})

    assert client.use_rest_route is False
    assert len(session.calls) == 3


_MARKER = "&_wc_rest_route=/wp-json/wc/"


@pytest.mark.parametrize(
    "resource,expected",
    [
        ("orders", "https://woo.test/?rest_route=/wc/v3/orders" + _MARKER),
        ("webhooks/25", "https://woo.test/?rest_route=/wc/v3/webhooks/25" + _MARKER),
        # Custom namespaces must NOT be prefixed with /wc/v3.
        (
            "wp-json/jarz/v1/delivery-areas",
            "https://woo.test/?rest_route=/jarz/v1/delivery-areas" + _MARKER,
        ),
    ],
)
def test_build_url_rest_route_forms(resource, expected):
    client, _session = _client([])
    assert client._build_url(resource, rest_route=True) == expected


def test_rest_route_url_always_carries_the_wc_auth_marker():
    """Guard rail: drop this substring and WooCommerce stops authenticating.

    WC_REST_Authentication::is_request_to_rest_api() looks for "wp-json/wc/" in
    the request URI. Without it the call still routes, but every response is a
    401 "invalid_username" — a failure mode that looks like bad credentials.
    """
    client, _session = _client([])
    for resource in ("orders", "products/1", "wp-json/jarz/v1/delivery-areas"):
        assert "wp-json/wc/" in client._build_url(resource, rest_route=True)


def test_build_url_pretty_forms_are_unchanged():
    client, _session = _client([])
    assert client._build_url("orders") == "https://woo.test/wp-json/wc/v3/orders"
    assert (
        client._build_url("wp-json/jarz/v1/delivery-areas")
        == "https://woo.test/wp-json/jarz/v1/delivery-areas"
    )


def test_get_order_still_swallows_transient_and_returns_none():
    """WooTransientError subclasses WooAPIError, so get_order behaviour is unchanged."""
    client, _session = _client([_html_response(), _html_response(), _html_response()])
    assert client.get_order(123) is None


# --- backoff shape ---------------------------------------------------------


def test_backoff_grows_exponentially_and_is_bounded_and_jittered():
    base = 1.0
    delays = [http_client._backoff_delay(attempt, base) for attempt in range(1, 5)]
    # Jittered, but each attempt sits inside its own exponential band.
    assert 0.5 <= delays[0] <= 1.0
    assert 1.0 <= delays[1] <= 2.0
    assert 2.0 <= delays[2] <= 4.0
    assert all(d <= http_client.MAX_BACKOFF_SECONDS for d in delays)
    # Bounded even for absurd attempt counts.
    assert http_client._backoff_delay(50, base) <= http_client.MAX_BACKOFF_SECONDS
    # Disabled when base is 0 (test/fail-fast mode).
    assert http_client._backoff_delay(3, 0) == 0.0


def test_sleep_is_skipped_when_backoff_disabled(monkeypatch):
    slept = []
    monkeypatch.setattr(http_client.time, "sleep", lambda s: slept.append(s))
    client, _session = _client([_html_response(), _html_response(), _html_response()])
    with pytest.raises(WooTransientError):
        client.list_orders_with_meta(params={"per_page": 10})
    assert slept == []


def test_sleep_happens_between_retries_when_backoff_enabled(monkeypatch):
    slept = []
    monkeypatch.setattr(http_client.time, "sleep", lambda s: slept.append(s))
    client, session = _client(
        [_html_response(), _json_response([{"id": 1}])], backoff_base_seconds=1.0
    )

    client.list_orders_with_meta(params={"per_page": 10})

    assert len(session.calls) == 2
    assert len(slept) == 1  # slept once, between the two attempts
    assert 0 < slept[0] <= http_client.MAX_BACKOFF_SECONDS


def test_retry_after_header_is_honoured_and_bounded(monkeypatch):
    slept = []
    monkeypatch.setattr(http_client.time, "sleep", lambda s: slept.append(s))
    rate_limited = FakeResponse(
        status_code=429,
        text='{"message": "Too many requests"}',
        headers={"Retry-After": "2"},
        json_body={"message": "Too many requests"},
    )
    client, _session = _client([rate_limited, _json_response([{"id": 1}])], backoff_base_seconds=1.0)

    client.list_orders_with_meta(params={"per_page": 10})

    assert slept == [2.0]

    # A silly Retry-After is capped rather than parking a worker for an hour.
    assert http_client._retry_after_seconds(
        FakeResponse(status_code=429, headers={"Retry-After": "3600"})
    ) == http_client.MAX_BACKOFF_SECONDS
    # Non-numeric (HTTP-date) form falls back to our own backoff.
    assert (
        http_client._retry_after_seconds(
            FakeResponse(status_code=429, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
        )
        is None
    )


# --- order_sync fails soft instead of aborting the window -------------------


def _transient_exc():
    return WooTransientError(
        200,
        "https://woo.test/wp-json/wc/v3/orders",
        "Invalid JSON response after 3 attempts | content_type=text/html | body=<!DOCTYPE html>",
        {"content_type": "text/html", "body_snippet": "<!DOCTYPE html>"},
    )


def _patch_window_raising_transient(monkeypatch):
    warnings = []

    def fake_list_orders_window(client, params, max_pages):
        raise _transient_exc()

    monkeypatch.setattr(order_sync, "_list_orders_window", fake_list_orders_window)
    monkeypatch.setattr(order_sync, "ensure_custom_fields", lambda: None)
    monkeypatch.setattr(
        order_sync.frappe,
        "logger",
        lambda *a, **k: type("L", (), {"warning": staticmethod(lambda d: warnings.append(d)),
                                       "info": staticmethod(lambda d: None),
                                       "error": staticmethod(lambda d: None)})(),
    )
    return warnings


def test_enqueue_order_window_events_fails_soft_on_transient(monkeypatch):
    """A flaky store must not abort the sync or raise to Sentry."""
    from types import SimpleNamespace

    warnings = _patch_window_raising_transient(monkeypatch)
    settings = SimpleNamespace(
        base_url="https://woo.test", consumer_key="ck", get_password=lambda f: "cs"
    )

    result = order_sync._enqueue_order_window_events(
        settings=settings,
        event_type="order_poll",
        limit=100,
        modified_after="2026-07-14T00:00:00",
        max_pages=3,
    )

    assert result["transient_skip"] is True
    assert result["queued"] == 0
    assert result["errors"] == 1
    # Cursor inputs stay empty -> window is retried next run, nothing lost.
    assert result["latest_seen_modified_gmt"] is None
    assert result["latest_seen_order_id"] is None
    # Logged as a warning, not an error-level event.
    assert len(warnings) == 1
    assert warnings[0]["event"] == "woo_order_window_transient_skip"
    assert warnings[0]["status_code"] == 200


def test_pull_recent_orders_phase1_fails_soft_on_transient(monkeypatch):
    from types import SimpleNamespace

    warnings = _patch_window_raising_transient(monkeypatch)
    settings = SimpleNamespace(
        base_url="https://woo.test", consumer_key="ck", get_password=lambda f: "cs"
    )
    monkeypatch.setattr(order_sync.frappe, "get_single", lambda doctype: settings)

    result = order_sync.pull_recent_orders_phase1(limit=10, max_pages=2)

    assert result["transient_skip"] is True
    assert result["processed"] == 0
    assert result["errors"] == 1
    assert result["latest_seen_modified_gmt"] is None
    assert len(warnings) == 1


def test_transient_skip_detail_carries_no_credentials(monkeypatch):
    warnings = _patch_window_raising_transient(monkeypatch)
    detail = order_sync._transient_window_skip(
        _transient_exc(),
        operation="unit-test",
        params={"per_page": 10, "status": "any"},
        max_pages=1,
    )
    serialized = str(detail)
    assert "ck_" not in serialized
    assert "cs_" not in serialized
    assert detail["params"] == {"per_page": 10, "status": "any"}
    assert warnings == [detail]


# --- sync_events classifies transient failures as retryable, not NeedsReview ---


def _event_doc():
    from types import SimpleNamespace

    return SimpleNamespace(direction="Inbound", attempt_count=1, max_attempts=5)


def test_sync_events_reschedules_transient_error(monkeypatch):
    """A 200-with-HTML failure reports status_code=200, so it must be classified
    by type — otherwise it falls through to text matching and lands in NeedsReview."""
    from jarz_woocommerce_integration.services import sync_events

    calls = []
    monkeypatch.setattr(
        sync_events, "_schedule_retry", lambda doc, **kw: calls.append(("retry", kw)) or {"status": "retry"}
    )
    monkeypatch.setattr(
        sync_events, "_mark_needs_review", lambda *a, **kw: calls.append(("review", kw)) or {"status": "review"}
    )

    result = sync_events._handle_exception(_event_doc(), _transient_exc())

    assert result == {"status": "retry"}
    assert [c[0] for c in calls] == ["retry"]


def test_sync_events_still_flags_auth_error_for_review(monkeypatch):
    """Genuine 4xx must stay loud."""
    from jarz_woocommerce_integration.services import sync_events

    calls = []
    monkeypatch.setattr(
        sync_events, "_schedule_retry", lambda doc, **kw: calls.append(("retry", kw)) or {"status": "retry"}
    )
    monkeypatch.setattr(
        sync_events, "_mark_needs_review", lambda *a, **kw: calls.append(("review", kw)) or {"status": "review"}
    )

    result = sync_events._handle_exception(
        _event_doc(), WooAPIError(403, "https://woo.test/wp-json/wc/v3/orders", "Sorry, you cannot list resources.")
    )

    assert result == {"status": "review"}
    assert [c[0] for c in calls] == ["review"]


def test_non_transient_woo_error_still_propagates(monkeypatch):
    """Auth failures must stay loud: no soft-skip, no swallowing."""
    from types import SimpleNamespace

    def fake_list_orders_window(client, params, max_pages):
        raise WooAPIError(401, "https://woo.test/wp-json/wc/v3/orders", "Invalid signature")

    monkeypatch.setattr(order_sync, "_list_orders_window", fake_list_orders_window)
    monkeypatch.setattr(order_sync, "ensure_custom_fields", lambda: None)
    settings = SimpleNamespace(
        base_url="https://woo.test", consumer_key="ck", get_password=lambda f: "cs"
    )

    with pytest.raises(WooAPIError):
        order_sync._enqueue_order_window_events(
            settings=settings, event_type="order_poll", limit=100, max_pages=1
        )
