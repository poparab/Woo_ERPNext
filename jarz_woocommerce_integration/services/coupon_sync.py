"""Mirror ``Jarz Promo Code`` rows flagged ``sync_to_woo`` as WooCommerce coupons.

The two promo DocTypes belong to the sibling POS app and are reached only by
DocType name through frappe APIs; this module has no code dependency on it.

A code is mirrored while it can still be used, and removed from Woo the moment
it cannot: expired, disabled, deleted, or fully redeemed in either channel.
Codes with ``sync_to_woo = 0`` are never touched, so hand-made Woo coupons
stay as they are.

Gated by ``WooCommerce Settings.enable_outbound_coupons`` (default off).
"""

from __future__ import annotations

from typing import Any

import frappe
from frappe.utils import get_datetime, now_datetime

from jarz_woocommerce_integration.utils.http_client import WooAPIError, WooClient

PROMO_DOCTYPE = "Jarz Promo Code"
REDEMPTION_DOCTYPE = "Jarz Promo Redemption"
SWITCH = "enable_outbound_coupons"
ERROR_TITLE = "woo coupon sync failed"


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

def active_redemptions(code: str) -> int:
    """Count Active redemptions ourselves; ``times_used`` lags the insert."""
    return int(frappe.db.count(REDEMPTION_DOCTYPE, {"promo_code": code, "status": "Active"}) or 0)


def should_exist(doc: Any, redemptions: int, now=None) -> tuple[bool, str]:
    """Return (should_exist_in_woo, reason)."""
    if not int(doc.get("sync_to_woo") or 0):
        return False, "sync_to_woo_off"
    if not int(doc.get("enabled") or 0):
        return False, "disabled"
    active_to = doc.get("active_to")
    if active_to and get_datetime(active_to) <= (now or now_datetime()):
        return False, "expired"
    limit = int(doc.get("usage_limit") or 0)
    if limit and redemptions >= limit:
        return False, "fully_used"
    return True, "ok"


def _woo_id_for_item(item: dict) -> int | None:
    for field in ("woo_variation_id", "woo_product_id"):
        raw = item.get(field)
        if raw not in (None, "", 0, "0"):
            try:
                return int(str(raw).strip())
            except (TypeError, ValueError):
                continue
    return None


def resolve_product_ids(doc: Any) -> list[int] | None:
    """Woo product ids for a scoped code; ``None`` means whole order (no scope)."""
    applies_to = doc.get("applies_to") or "Whole Order"
    if applies_to == "Whole Order":
        return None
    rows = doc.get("item_scope") or []
    fields = ["name", "woo_variation_id", "woo_product_id"]
    items: list[dict] = []
    if applies_to == "Specific Items":
        codes = [r.get("item_code") for r in rows if r.get("item_code")]
        if codes:
            items = frappe.get_all("Item", filters={"name": ["in", codes]}, fields=fields)
    elif applies_to == "Item Groups":
        groups = [r.get("item_group") for r in rows if r.get("item_group")]
        if groups:
            items = frappe.get_all(
                "Item", filters={"item_group": ["in", groups], "disabled": 0}, fields=fields
            )
    ids: list[int] = []
    for item in items:
        woo_id = _woo_id_for_item(item)
        if woo_id is not None and woo_id not in ids:
            ids.append(woo_id)
    return ids


def build_payload(doc: Any, product_ids: list[int] | None) -> dict:
    code = str(doc.get("name") or doc.get("code") or "")
    discount_type = doc.get("discount_type")
    payload: dict[str, Any] = {"code": code.lower()}
    if discount_type == "Percentage":
        payload["discount_type"] = "percent"
        payload["amount"] = str(doc.get("discount_value") or 0)
        payload["free_shipping"] = False
    elif discount_type == "Free Delivery":
        payload["discount_type"] = "fixed_cart"
        payload["amount"] = "0"
        payload["free_shipping"] = True
    else:
        payload["discount_type"] = "fixed_cart"
        payload["amount"] = str(doc.get("discount_value") or 0)
        payload["free_shipping"] = False
    payload["individual_use"] = not int(doc.get("stackable") or 0)
    payload["usage_limit"] = int(doc.get("usage_limit") or 0) or None
    active_to = doc.get("active_to")
    payload["date_expires"] = (
        get_datetime(active_to).strftime("%Y-%m-%dT%H:%M:%S") if active_to else None
    )
    if float(doc.get("min_order_amount") or 0) > 0:
        payload["minimum_amount"] = str(doc.get("min_order_amount"))
    if product_ids is not None:
        payload["product_ids"] = list(product_ids)
    meta = []
    if float(doc.get("max_discount_amount") or 0) > 0:
        meta.append({"key": "_jarz_max_discount", "value": str(doc.get("max_discount_amount"))})
    if doc.get("customer_phone"):
        meta.append({"key": "_jarz_phone", "value": str(doc.get("customer_phone"))})
    payload["meta_data"] = meta
    payload["description"] = f"ERPNext Jarz Promo Code {code}"
    return payload


# ---------------------------------------------------------------------------
# Woo I/O
# ---------------------------------------------------------------------------

def _build_client() -> WooClient:
    s = frappe.get_single("WooCommerce Settings")
    return WooClient(
        base_url=s.base_url.rstrip("/"),
        consumer_key=s.consumer_key,
        consumer_secret=s.get_password("consumer_secret"),
        api_version=s.api_version or "v3",
    )


def _gate_open() -> bool:
    try:
        return bool(int(frappe.db.get_single_value("WooCommerce Settings", SWITCH) or 0))
    except Exception:  # noqa: BLE001
        return False


def _note_disabled() -> None:
    try:
        from jarz_woocommerce_integration.services.outbound_sync import _note_outbound_disabled

        _note_outbound_disabled(SWITCH)
    except Exception:  # noqa: BLE001
        pass


def _find_by_code(client, code: str) -> dict | None:
    rows = client.get("coupons", params={"code": code.lower()})
    if isinstance(rows, list):
        for row in rows:
            if str(row.get("code") or "").lower() == code.lower():
                return row
    return None


def _is_missing(exc: WooAPIError) -> bool:
    status = getattr(exc, "status_code", None)
    return status == 404 or (status == 400 and "invalid" in str(exc).lower())


def _store_woo_id(code: str, value: str) -> None:
    frappe.db.set_value(PROMO_DOCTYPE, code, "woo_coupon_id", value, update_modified=False)


def _delete_from_woo(client, code: str, woo_id: Any) -> dict:
    if not woo_id:
        found = _find_by_code(client, code)
        woo_id = found.get("id") if found else None
    if not woo_id:
        return {"action": "skipped", "woo_coupon_id": None, "reason": "not_in_woo"}
    try:
        client.delete(f"coupons/{woo_id}", params={"force": "true"})
    except WooAPIError as exc:
        if not _is_missing(exc):
            raise
    return {"action": "deleted", "woo_coupon_id": None, "reason": "removed"}


def _sync(code: str, woo_coupon_id: Any = None, client=None) -> dict:
    code = (code or "").strip()
    if not code:
        return {"action": "skipped", "woo_coupon_id": None, "reason": "no_code"}
    if not _gate_open():
        _note_disabled()
        return {"action": "skipped", "woo_coupon_id": None, "reason": "outbound_coupons_disabled"}

    doc = frappe.get_doc(PROMO_DOCTYPE, code) if frappe.db.exists(PROMO_DOCTYPE, code) else None
    client = client or _build_client()

    if doc is None:
        # Deleted: nothing local to read or clear; remove whatever Woo has.
        return _delete_from_woo(client, code, woo_coupon_id)

    stored_id = doc.get("woo_coupon_id") or woo_coupon_id
    if not int(doc.get("sync_to_woo") or 0):
        if stored_id:
            # Flag was switched off after a push: take our coupon down.
            result = _delete_from_woo(client, code, stored_id)
            _store_woo_id(code, "")
            result["reason"] = "sync_to_woo_off"
            return result
        return {"action": "skipped", "woo_coupon_id": None, "reason": "sync_to_woo_off"}

    ok, reason = should_exist(doc, active_redemptions(code))
    if not ok:
        result = _delete_from_woo(client, code, stored_id)
        if stored_id:
            _store_woo_id(code, "")
        result["reason"] = reason
        return result

    product_ids = resolve_product_ids(doc)
    if product_ids is not None and not product_ids:
        frappe.log_error(
            title=ERROR_TITLE,
            message=f"{code}: scoped code resolved to zero Woo product ids; not syncing.",
        )
        return {
            "action": "skipped",
            "woo_coupon_id": stored_id or None,
            "reason": "scope_has_no_woo_ids",
        }

    payload = build_payload(doc, product_ids)
    action = "updated"
    response = None
    if stored_id:
        try:
            response = client.put(f"coupons/{stored_id}", payload)
        except WooAPIError as exc:
            if not _is_missing(exc):
                raise
    if response is None:
        existing = _find_by_code(client, code)
        if existing and existing.get("id"):
            response = client.put(f"coupons/{existing['id']}", payload)
        else:
            response = client.post("coupons", payload)
            action = "created"
    new_id = str((response or {}).get("id") or "")
    if new_id and new_id != str(stored_id or ""):
        _store_woo_id(code, new_id)
    return {"action": action, "woo_coupon_id": new_id or None, "reason": "ok"}


# ---------------------------------------------------------------------------
# Jobs and triggers
# ---------------------------------------------------------------------------

def sync_promo_coupon_job(code: str, woo_coupon_id: Any = None) -> None:
    """Background job. Never raises."""
    try:
        _sync(code, woo_coupon_id)
    except Exception:  # noqa: BLE001
        try:
            frappe.log_error(title=ERROR_TITLE, message=f"{code}\n{frappe.get_traceback()}")
        except Exception:  # noqa: BLE001
            pass


@frappe.whitelist()
def sync_promo_coupon(code: str) -> dict:
    """Run the sync synchronously (System Manager only). For tests/ops."""
    frappe.only_for("System Manager")
    return _sync(code)


def _suppressed(doc) -> bool:
    try:
        from jarz_woocommerce_integration.services.outbound_sync import _is_outbound_suppressed

        return bool(_is_outbound_suppressed(doc))
    except Exception:  # noqa: BLE001
        return False


def _enqueue(code: str, woo_coupon_id: Any = None) -> None:
    kwargs: dict[str, Any] = {"code": code}
    if woo_coupon_id:
        kwargs["woo_coupon_id"] = woo_coupon_id
    frappe.enqueue(
        "jarz_woocommerce_integration.services.coupon_sync.sync_promo_coupon_job",
        queue="short",
        timeout=300,
        enqueue_after_commit=True,
        job_id=f"woo-coupon-sync::{code}",
        deduplicate=True,
        **kwargs,
    )


def enqueue_coupon_sync(doc, method: str | None = None) -> None:
    """Promo Code after_insert / on_update."""
    try:
        if _suppressed(doc):
            return
        flagged = int(doc.get("sync_to_woo") or 0)
        # An unflagged code with no recorded Woo id has nothing to do.
        if not flagged and not doc.get("woo_coupon_id"):
            return
        if not _gate_open():
            _note_disabled()
            return
        _enqueue(str(doc.name))
    except Exception:  # noqa: BLE001
        frappe.log_error(title=ERROR_TITLE, message=frappe.get_traceback())


def enqueue_coupon_delete(doc, method: str | None = None) -> None:
    """Promo Code on_trash: capture code and Woo id before the row is gone."""
    try:
        if _suppressed(doc) or not _gate_open():
            return
        if not int(doc.get("sync_to_woo") or 0) and not doc.get("woo_coupon_id"):
            return
        _enqueue(str(doc.name), doc.get("woo_coupon_id"))
    except Exception:  # noqa: BLE001
        frappe.log_error(title=ERROR_TITLE, message=frappe.get_traceback())


def enqueue_coupon_sync_for_redemption(doc, method: str | None = None) -> None:
    """Redemption after_insert / on_update: re-evaluate its promo code."""
    try:
        code = doc.get("promo_code")
        if not code or _suppressed(doc) or not _gate_open():
            return
        if not frappe.db.get_value(PROMO_DOCTYPE, code, "sync_to_woo"):
            return
        _enqueue(str(code))
    except Exception:  # noqa: BLE001
        frappe.log_error(title=ERROR_TITLE, message=frappe.get_traceback())
