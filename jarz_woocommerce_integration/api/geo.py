"""Geo pin passthrough endpoints (courier lane O1).

Thin transport only — every decision lives in
``services/geo_passthrough.py``. The normal path needs no endpoint at all: pins
are stamped inline by ``customer_sync.ensure_customer_with_addresses`` during
order sync. These exist for the two operational cases:

* ``preview_order_pin`` — read-only: what would this order write, and where?
* ``resync_order_pin`` / ``enqueue_order_pin_resync`` — repair an order whose
  pin was missed (e.g. it synced before the Address geo fields existed).

None of them create or edit an Address, so none can fork a duplicate address or
push anything back to WooCommerce.
"""

from __future__ import annotations

from typing import Any

import frappe

from jarz_woocommerce_integration.services import geo_passthrough


def _ensure_geo_permission() -> None:
    """Writing a pin is an Address write; previewing one is an Address read."""
    frappe.has_permission("Address", ptype="write", throw=True)


def _parse_order_ids(woo_order_ids: Any) -> list[int]:
    """Accept a JSON list, a comma-separated string, or a single id."""
    if woo_order_ids is None:
        return []
    if isinstance(woo_order_ids, str):
        raw = woo_order_ids.strip()
        if not raw:
            return []
        if raw.startswith("["):
            try:
                woo_order_ids = frappe.parse_json(raw)
            except Exception:  # noqa: BLE001
                woo_order_ids = raw.split(",")
        else:
            woo_order_ids = raw.split(",")
    if not isinstance(woo_order_ids, (list, tuple, set)):
        woo_order_ids = [woo_order_ids]

    parsed: list[int] = []
    for value in woo_order_ids:
        try:
            order_id = int(str(value).strip())
        except (TypeError, ValueError):
            continue
        if order_id > 0 and order_id not in parsed:
            parsed.append(order_id)
    return parsed


@frappe.whitelist(allow_guest=False)
def preview_order_pin(woo_order_id: int | str) -> dict:
    """Read-only preview of the pin lane O1 would write for one Woo order.

    Example:
        /api/method/jarz_woocommerce_integration.api.geo.preview_order_pin?woo_order_id=16834
    """
    _ensure_geo_permission()
    try:
        return geo_passthrough.preview_order_pin(woo_order_id)
    except frappe.PermissionError:
        raise
    except Exception as exc:  # noqa: BLE001
        frappe.log_error(frappe.get_traceback(), "geo.preview_order_pin")
        return {"success": False, "error": str(exc), "woo_order_id": woo_order_id}


@frappe.whitelist(allow_guest=False)
def resync_order_pin(woo_order_id: int | str) -> dict:
    """Re-apply the customer pin for a single already-mapped Woo order.

    Runs synchronously (one Woo GET plus one ``db.set_value``) so the caller gets
    immediate feedback, mirroring ``api/manual_sync.py``. Use
    ``enqueue_order_pin_resync`` for anything bulk.
    """
    _ensure_geo_permission()
    try:
        return geo_passthrough.resync_order_pin(woo_order_id)
    except frappe.PermissionError:
        raise
    except Exception as exc:  # noqa: BLE001
        frappe.log_error(frappe.get_traceback(), "geo.resync_order_pin")
        return {"success": False, "error": str(exc), "woo_order_id": woo_order_id}


@frappe.whitelist(allow_guest=False)
def enqueue_order_pin_resync(woo_order_ids: Any) -> dict:
    """Queue a bulk pin repair for an explicit list of Woo order ids.

    One Woo HTTP GET per order, so this is always a background job — the
    endpoint stays a thin wrapper that enqueues and returns.

    Example:
        /api/method/jarz_woocommerce_integration.api.geo.enqueue_order_pin_resync?woo_order_ids=16834,16835
    """
    _ensure_geo_permission()
    order_ids = _parse_order_ids(woo_order_ids)
    if not order_ids:
        return {"success": False, "error": "no_valid_order_ids"}
    try:
        job_name = f"woo_geo_pin_resync_{frappe.utils.now_datetime().isoformat()}"
        frappe.enqueue(
            geo_passthrough.resync_order_pins_job,
            queue="long",
            timeout=3600,
            job_name=job_name,
            woo_order_ids=order_ids,
        )
    except frappe.PermissionError:
        raise
    except Exception as exc:  # noqa: BLE001
        frappe.log_error(frappe.get_traceback(), "geo.enqueue_order_pin_resync")
        return {"success": False, "error": str(exc)}
    return {
        "success": True,
        "queued": True,
        "job_name": job_name,
        "count": len(order_ids),
        "woo_order_ids": order_ids,
    }


@frappe.whitelist(allow_guest=False)
def geo_status() -> dict:
    """Report whether lane A4's Address geo fields exist on this site.

    A pre-migration site silently discards every incoming pin; this makes that
    state queryable instead of invisible.
    """
    _ensure_geo_permission()
    available = geo_passthrough.geo_fields_available()
    return {
        "success": True,
        "geo_fields_available": available,
        "required_fields": list(geo_passthrough.GEO_WRITE_FIELDS),
        "source": geo_passthrough.GEO_SOURCE_CUSTOMER_PIN,
        "confidence": geo_passthrough.CONFIDENCE_RANK[geo_passthrough.GEO_SOURCE_CUSTOMER_PIN],
        # This app is one of two authorised writers and is capped here; anything
        # above belongs to jarz_pos/services/geo_resolution.py.
        "max_writable_rank": geo_passthrough.MAX_WRITABLE_RANK,
        "confidence_ladder": dict(geo_passthrough.CONFIDENCE_RANK),
    }
