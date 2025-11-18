from __future__ import annotations

import json
from dataclasses import dataclass

import requests  # third-party
from requests.auth import HTTPBasicAuth


class WooAPIError(Exception):
    """Custom exception for WooCommerce API errors."""

    def __init__(self, status_code: int, url: str, message: str, payload: dict | None = None):
        super().__init__(f"{status_code} {message}")
        self.status_code = status_code
        self.url = url
        self.message = message
        self.payload = payload or {}


@dataclass(slots=True)
class WooClient:
    base_url: str
    consumer_key: str
    consumer_secret: str
    api_version: str = "v3"
    timeout: int = 30

    def _build_url(self, resource: str) -> str:
        resource = resource.lstrip("/")
        if resource.startswith("wp-json"):
            return f"{self.base_url}/{resource}"
        return f"{self.base_url}/wp-json/wc/{self.api_version}/{resource}"

    def _request(self, method: str, resource: str, *, params: dict | None = None, data: dict | None = None) -> dict:
        url = self._build_url(resource)
        auth = HTTPBasicAuth(self.consumer_key, self.consumer_secret)
        response = requests.request(method.upper(), url, params=params, json=data, auth=auth, timeout=self.timeout)
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
