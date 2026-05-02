from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import urlparse

import requests  # third-party
from requests.auth import HTTPBasicAuth
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning


class WooAPIError(Exception):
    """Custom exception for WooCommerce API errors."""

    def __init__(self, status_code: int, url: str, message: str, payload: dict | None = None):
        super().__init__(f"{status_code} {message}")
        self.status_code = status_code
        self.url = url
        self.message = message
        self.payload = payload or {}


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


@dataclass
class WooClient:
    base_url: str
    consumer_key: str
    consumer_secret: str
    api_version: str = "v3"
    timeout: int = 60
    verify_ssl: bool | None = None
    _session: requests.Session | None = None

    def __post_init__(self):
        self._session = requests.Session()
        self._session.auth = HTTPBasicAuth(self.consumer_key, self.consumer_secret)
        if self.verify_ssl is None:
            self.verify_ssl = not _should_bypass_ssl_verification(self.base_url)
        self._session.verify = self.verify_ssl
        if self.verify_ssl is False:
            disable_warnings(InsecureRequestWarning)

    def _build_url(self, resource: str) -> str:
        resource = resource.lstrip("/")
        if resource.startswith("wp-json"):
            return f"{self.base_url}/{resource}"
        return f"{self.base_url}/wp-json/wc/{self.api_version}/{resource}"

    def _request(self, method: str, resource: str, *, params: dict | None = None, data: dict | None = None) -> dict:
        url = self._build_url(resource)
        session = self._session or requests
        response = session.request(method.upper(), url, params=params, json=data, timeout=self.timeout)
        if response.status_code >= 400:
            try:
                payload = response.json()
            except Exception:  # noqa: BLE001
                payload = {"message": response.text[:500]}
            raise WooAPIError(response.status_code, url, payload.get("message") or "HTTP error", payload)
        try:
            if response.text:
                return response.json()
            return {}
        except json.JSONDecodeError as exc:  # noqa: PERF203
            raise WooAPIError(response.status_code, url, f"Invalid JSON response: {exc}") from exc

    def _request_raw(self, method: str, resource: str, *, params: dict | None = None, data: dict | None = None) -> tuple[dict | list, dict]:
        """Like _request but returns (parsed_body, response_headers) tuple."""
        url = self._build_url(resource)
        session = self._session or requests
        response = session.request(method.upper(), url, params=params, json=data, timeout=self.timeout)
        if response.status_code >= 400:
            try:
                payload = response.json()
            except Exception:  # noqa: BLE001
                payload = {"message": response.text[:500]}
            raise WooAPIError(response.status_code, url, payload.get("message") or "HTTP error", payload)
        try:
            body = response.json() if response.text else {}
        except json.JSONDecodeError as exc:
            raise WooAPIError(response.status_code, url, f"Invalid JSON response: {exc}") from exc
        return body, dict(response.headers)

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
