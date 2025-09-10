import base64
import hashlib
import hmac
import json
from typing import Any

import frappe
from frappe.utils.password import get_decrypted_password
from jarz_woocommerce_integration.services.order_sync import (
    pull_recent_orders_phase1,
    pull_single_order_phase1,
    process_order_phase1,
)  # single-level package path (standard frappe app layout)
from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
    WooCommerceSettings,
)


def _compute_signature(secret: bytes, raw_body: bytes) -> str:
    return base64.b64encode(hmac.new(secret, raw_body, hashlib.sha256).digest()).decode()


def _verify_signature(raw_body: bytes, provided: str | None, secret: str | None) -> bool:
    if not secret or not provided:
        return False
    expected = _compute_signature(secret.encode(), raw_body)
    return hmac.compare_digest(expected, provided.strip())


@frappe.whitelist(allow_guest=False)
def pull_recent_phase1(limit: int = 20, dry_run: int = 0, force: int = 0):
    """Pull recent orders (Phase 1) with optional dry-run.

    Args:
        limit: max orders to evaluate (1..100)
        dry_run: if truthy, don't create anything
    """
    limit = max(1, min(int(limit), 100))
    return {
        "success": True,
        "data": pull_recent_orders_phase1(limit=limit, dry_run=bool(int(dry_run)), force=bool(int(force))),
    }


@frappe.whitelist(allow_guest=False)
def pull_order_phase1(order_id: int | str = None, dry_run: int = 0, force: int = 0):
    """Pull a single Woo order by id (Phase 1) for targeted debugging.

    Args:
        order_id: Woo order id
        dry_run: simulate without DB writes
        force: delete existing mapping record and reprocess
    """
    if not order_id:
        frappe.throw("order_id required")
    data = pull_single_order_phase1(
        order_id=order_id, dry_run=bool(int(dry_run)), force=bool(int(force))
    )
    return {"success": True, "data": data}


@frappe.whitelist(allow_guest=False)
def pull_recent_pos_profile_update():
    """Convenience endpoint: pull 10 recent orders with updates and force enabled.

    This is used to quickly populate Sales Invoice.pos_profile based on Territory.pos_profile
    after deploying the mapping logic, without wrestling with CLI kwargs quoting.
    """
    return {
        "success": True,
        "data": pull_recent_orders_phase1(limit=10, dry_run=False, force=True, allow_update=True),
    }


@frappe.whitelist(allow_guest=True)
def woo_order_webhook():  # pragma: no cover - network entrypoint
    """Webhook endpoint for WooCommerce order.created / order.updated.

    Uses same signature scheme as customer webhook. Fast ACK + background job.
    """
    raw_body: bytes = frappe.request.data or b""
    sig_header = frappe.get_request_header("X-WC-Webhook-Signature") or ""
    debug_flag = frappe.form_dict.get("d") in ("1", "true", "True")

    # Early receipt log
    try:
        frappe.logger().info({
            "event": "woo_order_webhook_received",
            "len": len(raw_body),
            "has_sig": bool(sig_header),
        })
    except Exception:  # noqa: BLE001
        pass

    settings = WooCommerceSettings.get_settings()
    try:
        secret = get_decrypted_password("WooCommerce Settings", settings.name, "webhook_secret") or ""
    except Exception:  # noqa: BLE001
        secret = ""
    if not secret:
        try:
            raw_attr = getattr(settings, "webhook_secret", "")
            if raw_attr:
                secret = raw_attr
        except Exception:  # noqa: BLE001
            pass

    if secret and not _verify_signature(raw_body, sig_header, secret):
        # Graceful ACK if body empty & no id (Woo handshake)
        try:
            tmp = json.loads(raw_body.decode() or "{}")
        except Exception:  # noqa: BLE001
            tmp = {}
        if not isinstance(tmp, dict) or not tmp.get("id"):
            return {"success": True, "ack": True, "unsigned": True}
        # Signature mismatch
        try:
            exp_pref = _compute_signature(secret.encode(), raw_body)[:18]
        except Exception:  # noqa: BLE001
            exp_pref = "<err>"
        frappe.logger().warning({
            "event": "woo_order_webhook_sig_mismatch",
            "provided_prefix": (sig_header or "")[:18],
            "expected_prefix": exp_pref,
            "body_len": len(raw_body),
        })
        frappe.local.response.http_status_code = 403
        return {"success": False, "error": "invalid_signature"}

    try:
        payload = json.loads(raw_body.decode() or "{}")
    except Exception:  # noqa: BLE001
        payload = {}

    # Handshake when creating webhook (no order object yet)
    if not isinstance(payload, dict) or not payload.get("id"):
        return {"success": True, "ack": True}

    order_id = payload.get("id")

    def _enqueue_process(order_payload: dict[str, Any]):  # background job
        start = frappe.utils.now_datetime()
        log_doc = None
        try:
            # Create sync log placeholder
            try:
                log_doc = frappe.get_doc({
                    "doctype": "WooCommerce Sync Log",
                    "operation": "Webhook",
                    "woo_order_id": order_payload.get("id"),
                    "status": "Started",
                    "message": "Processing",
                    "started_on": start,
                })
                log_doc.insert(ignore_permissions=True)
            except Exception:  # noqa: BLE001
                log_doc = None
            # Fetch full order using existing single-order pull (ensures consistency)
            res = pull_single_order_phase1(order_id=order_payload.get("id"), dry_run=False, force=False, allow_update=True)
            duration = (frappe.utils.now_datetime() - start).total_seconds()
            if log_doc:
                log_doc.db_set({
                    "status": "Success" if res.get("success") else "Failed",
                    "message": json.dumps(res)[:1000],
                    "ended_on": frappe.utils.now_datetime(),
                    "duration": duration,
                }, commit=True)
            frappe.logger().info({
                "event": "woo_order_webhook_processed",
                "order_id": order_payload.get("id"),
                "result": res,
                "duration_ms": duration * 1000,
            })
        except Exception:  # noqa: BLE001
            if log_doc:
                try:
                    log_doc.db_set({
                        "status": "Failed",
                        "message": "Exception",
                        "traceback": frappe.get_traceback()[:2000],
                        "ended_on": frappe.utils.now_datetime(),
                    }, commit=True)
                except Exception:  # noqa: BLE001
                    pass
            frappe.logger().error({
                "event": "woo_order_webhook_error",
                "order_id": order_payload.get("id"),
                "traceback": frappe.get_traceback(),
            })

    job_name = f"woo_order_{order_id}_{frappe.utils.now_datetime().isoformat()}"
    frappe.enqueue(
        _enqueue_process,
        queue="short",
        job_name=job_name,
        enqueue_after_commit=True,
        order_payload=payload,
    )
    frappe.logger().info({
        "event": "woo_order_webhook_enqueued",
        "order_id": order_id,
        "job_name": job_name,
    })
    resp = {"success": True, "queued": True, "job_name": job_name}
    if debug_flag:
        resp["debug"] = {"body_len": len(raw_body)}
    return resp
