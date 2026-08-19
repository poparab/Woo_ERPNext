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
        result = bool(
            frappe.db.sql(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND table_name = 'tabCustomer'
                  AND column_name = %s
                LIMIT 1
                """,
                (fieldname,),
                as_dict=True,
            )
        )
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
        try:
            value = frappe.db.get_value("Customer", customer, "woo_customer_id")
        except Exception:
            value = None
    else:
        value = getattr(customer, "woo_customer_id", None)
        if value is None and getattr(customer, "name", None):
            try:
                value = frappe.db.get_value("Customer", customer.name, "woo_customer_id")
            except Exception:
                value = None
    return normalize_woo_customer_id(value)


def has_legacy_customer_woo_id() -> bool:
    return _customer_has_column("custom_woo_customer_id")


def get_legacy_customer_woo_id(customer: Any) -> str | None:
    if not has_legacy_customer_woo_id():
        return None

    if isinstance(customer, str):
        try:
            value = frappe.db.get_value("Customer", customer, "custom_woo_customer_id")
        except Exception:
            value = None
    else:
        value = getattr(customer, "custom_woo_customer_id", None)
        if value is None and getattr(customer, "name", None):
            try:
                value = frappe.db.get_value("Customer", customer.name, "custom_woo_customer_id")
            except Exception:
                value = None
    return normalize_woo_customer_id(value)


def has_unmigrated_legacy_customer_woo_id(customer: Any) -> bool:
    return not get_customer_woo_id(customer) and bool(get_legacy_customer_woo_id(customer))


def find_customer_by_woo_id(woo_customer_id: Any) -> str | None:
    """Resolve the single Customer bound to *woo_customer_id*.

    Returns ``None`` when the id is claimed by more than one Customer.  That is
    not a defensive nicety: production has ids held by hundreds of unrelated
    customers, minted when the outbound push generated a colliding placeholder
    email and adopted whatever account WooCommerce matched it to.  This function
    is step zero of every customer resolution, and an unordered ``get_value``
    over a poisoned id returns an arbitrary stranger — silently attaching an
    order to the wrong person.

    Refusing to answer is the safe failure: the caller falls through to the phone
    lookup, which is reliable.  The ambiguity is logged so the affected ids stay
    visible rather than being papered over.
    """
    normalized = normalize_woo_customer_id(woo_customer_id)
    if not normalized or not _customer_has_column("woo_customer_id"):
        return None

    matches = frappe.get_all(
        "Customer",
        filters={"woo_customer_id": normalized},
        pluck="name",
        limit=2,
        order_by="creation asc",
    )
    if not matches:
        return None
    if len(matches) > 1:
        frappe.logger("woo").warning(
            f"ambiguous_woo_customer_id id={normalized} claimed_by_multiple_customers "
            f"(e.g. {matches[0]!r}, {matches[1]!r}); falling back to phone identity"
        )
        return None
    return matches[0]


def customer_woo_id_is_claimed_by_other(woo_customer_id: Any, customer_name: str) -> bool:
    """Is *woo_customer_id* already stored on a Customer other than *customer_name*?

    Guards every write of the field.  A Woo account maps to exactly one ERPNext
    Customer; stamping a second one on it is what made ``find_customer_by_woo_id``
    ambiguous in the first place.
    """
    normalized = normalize_woo_customer_id(woo_customer_id)
    if not normalized or not _customer_has_column("woo_customer_id"):
        return False
    try:
        holders = frappe.get_all(
            "Customer",
            filters={"woo_customer_id": normalized},
            pluck="name",
            limit=2,
        )
    except Exception:
        return False
    return any(holder != customer_name for holder in holders)


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