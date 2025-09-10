from __future__ import annotations

import json
import urllib.parse
from dataclasses import dataclass

import requests  # third-party


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

    def _auth_params(self) -> dict:
        return {
            "consumer_key": self.consumer_key,
            "consumer_secret": self.consumer_secret,
        }

    def get(self, resource: str, params: dict | None = None) -> dict:
        url = self._build_url(resource)
        q = params.copy() if params else {}
        q.update(self._auth_params())
        response = requests.get(url, params=q, timeout=self.timeout)
        if response.status_code >= 400:
            try:
                data = response.json()
            except Exception:  # noqa: BLE001
                data = {"message": response.text[:500]}
            raise WooAPIError(response.status_code, url, data.get("message") or "HTTP error", data)
        try:
            return response.json()
        except json.JSONDecodeError as e:  # noqa: PERF203
            raise WooAPIError(response.status_code, url, f"Invalid JSON response: {e}") from e

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
