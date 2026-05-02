from __future__ import annotations

from typing import Any

import frappe


_CUSTOMER_COLUMN_CACHE: dict[str, bool] = {}


def _customer_has_column(fieldname: str) -> bool:
    cached = _CUSTOMER_COLUMN_CACHE.get(fieldname)
    if cached is not None:
        return cached

    result = False
    try:
        result = fieldname in (frappe.db.get_table_columns("Customer") or [])
    except Exception:
        try:
            meta = frappe.get_meta("Customer")
            result = bool(meta and meta.get_field(fieldname))
        except Exception:
            result = False

    _CUSTOMER_COLUMN_CACHE[fieldname] = result
    return result


def normalize_woo_customer_id(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        try:
            integer_value = int(value)
        except (TypeError, ValueError):
            return None
        return str(integer_value) if integer_value > 0 else None

    text = str(value).strip()
    if not text or not text.isdigit():
        return None

    integer_value = int(text)
    return str(integer_value) if integer_value > 0 else None


def get_customer_woo_id(customer: Any) -> str | None:
    if isinstance(customer, str):
        value = frappe.db.get_value("Customer", customer, "woo_customer_id")
    else:
        value = getattr(customer, "woo_customer_id", None)
        if value is None and getattr(customer, "name", None):
            value = frappe.db.get_value("Customer", customer.name, "woo_customer_id")
    return normalize_woo_customer_id(value)


def has_legacy_customer_woo_id() -> bool:
    return _customer_has_column("custom_woo_customer_id")


def get_legacy_customer_woo_id(customer: Any) -> str | None:
    if not has_legacy_customer_woo_id():
        return None

    if isinstance(customer, str):
        value = frappe.db.get_value("Customer", customer, "custom_woo_customer_id")
    else:
        value = getattr(customer, "custom_woo_customer_id", None)
        if value is None and getattr(customer, "name", None):
            value = frappe.db.get_value("Customer", customer.name, "custom_woo_customer_id")
    return normalize_woo_customer_id(value)


def has_unmigrated_legacy_customer_woo_id(customer: Any) -> bool:
    return not get_customer_woo_id(customer) and bool(get_legacy_customer_woo_id(customer))


def find_customer_by_woo_id(woo_customer_id: Any) -> str | None:
    normalized = normalize_woo_customer_id(woo_customer_id)
    if not normalized or not _customer_has_column("woo_customer_id"):
        return None
    return frappe.db.get_value("Customer", {"woo_customer_id": normalized}, "name")


def set_customer_woo_id(
    customer_name: str,
    woo_customer_id: Any,
    *,
    clear_legacy: bool = False,
    update_modified: bool = False,
) -> str | None:
    normalized = normalize_woo_customer_id(woo_customer_id)
    if not normalized or not _customer_has_column("woo_customer_id"):
        return None

    updates: dict[str, Any] = {"woo_customer_id": normalized}
    if clear_legacy and has_legacy_customer_woo_id():
        updates["custom_woo_customer_id"] = 0
    frappe.db.set_value("Customer", customer_name, updates, update_modified=update_modified)
    return normalized