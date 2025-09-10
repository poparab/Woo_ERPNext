"""WooCommerce webhook + dev helpers.

Adds async queue processing, signature verification, and a dev-only test endpoint.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Any

import frappe
from frappe.utils import now_datetime
from frappe.utils.password import get_decrypted_password

from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
    WooCommerceSettings,
)
from jarz_woocommerce_integration.services.customer_sync import (
    process_customer_record,
)

__all__ = [
    "woo_customer_webhook",
    "dev_invoke_customer",
]

def _compute_signature(secret: bytes, raw_body: bytes) -> str:
    return base64.b64encode(hmac.new(secret, raw_body, hashlib.sha256).digest()).decode()


def _verify_signature(raw_body: bytes, provided: str | None, secret: str | None) -> bool:
    if not secret:
        return False
    if not provided:
        return False
    expected = _compute_signature(secret.encode(), raw_body)
    return hmac.compare_digest(expected, provided.strip())


def _enqueue_customer_process(payload: dict[str, Any], headers: dict[str, Any]):  # background job
    settings = WooCommerceSettings.get_settings()
    start = now_datetime()
    try:
        metrics = process_customer_record(payload, settings, debug=False, debug_samples=None)
        frappe.logger().info(
            {
                "event": "woo_customer_webhook_processed",
                "customer_id": payload.get("id"),
                "metrics": metrics,
                "duration_ms": (now_datetime() - start).total_seconds() * 1000,
            }
        )
    except Exception:  # noqa: BLE001
        frappe.logger().error(
            {
                "event": "woo_customer_webhook_error",
                "customer_id": payload.get("id"),
                "traceback": frappe.get_traceback(),
            }
        )


@frappe.whitelist(allow_guest=True)
def woo_customer_webhook():  # pragma: no cover - network entrypoint
    """Webhook endpoint for WooCommerce customer.created / customer.updated.

    Validates signature then enqueues processing so request responds fast.
    Returns accepted status + queue id (job name) for traceability.
    """
    # Use exact raw body (empty bytes if none) so signature matches Woo's computation
    raw_body: bytes = frappe.request.data or b""
    debug_flag = frappe.form_dict.get("d") in ("1", "true", "True")
    sig_header = frappe.get_request_header("X-WC-Webhook-Signature") or ""
    # Early receipt log (also helpful if signature later fails)
    try:
        frappe.logger().info(
            {
                "event": "woo_customer_webhook_received",
                "len": len(raw_body),
                "has_sig": bool(sig_header),
                "path": frappe.request.path,
            }
        )
    except Exception:  # noqa: BLE001
        pass
    # (sig_header moved above)
    settings = WooCommerceSettings.get_settings()
    # Retrieve password-type custom field securely
    try:
        secret = (
            get_decrypted_password(
                "WooCommerce Settings", settings.name, "webhook_secret"
            )
            or ""
        )
    except Exception:  # noqa: BLE001
        secret = ""
    # Fallback: if password decryption returned empty but the doc has a plain data value (field currently Data type)
    if not secret:
        try:
            raw_attr = getattr(settings, "webhook_secret", "")
            if raw_attr:
                secret = raw_attr
        except Exception:  # noqa: BLE001
            pass
    # Log secret presence (length only) for debugging in developer mode
    try:
        if frappe.conf.get("developer_mode") and frappe.form_dict.get("d") in ("1", "true", "True"):
            frappe.logger().info(
                {
                    "event": "woo_customer_webhook_secret_status",
                    "has_secret": bool(secret),
                    "secret_length": len(secret or ""),
                }
            )
    except Exception:  # noqa: BLE001
        pass
    if secret and not _verify_signature(raw_body, sig_header, secret):
        # If no signature header AND payload either empty or has no id -> treat as handshake/ack
        if not sig_header:
            try:
                tmp = json.loads(raw_body.decode() or "{}")
            except Exception:  # noqa: BLE001
                tmp = {}
            if not isinstance(tmp, dict) or not tmp.get("id"):
                return {"success": True, "ack": True, "unsigned": True}
            # At this point a customer id is present but signature header is missing -> explicit log
            try:
                frappe.logger().warning(
                    {
                        "event": "woo_customer_webhook_missing_signature",
                        "customer_id_in_body": tmp.get("id"),
                        "body_length": len(raw_body),
                    }
                )
            except Exception:  # noqa: BLE001
                pass
        # Log mismatch for debugging (truncated values)
        try:
            expected_dbg = _compute_signature(secret.encode(), raw_body)[:18]
        except Exception:  # noqa: BLE001
            expected_dbg = "<err>"
        log_payload = {
            "event": "woo_customer_webhook_sig_mismatch",
            "provided_prefix": (sig_header or "")[:18],
            "expected_prefix": expected_dbg,
            "body_length": len(raw_body),
            "has_sig_header": bool(sig_header),
            "customer_id_in_body": None,
        }
        # Try parse body id for context
        try:
            _tmp_j = json.loads(raw_body.decode() or "{}")
            if isinstance(_tmp_j, dict) and _tmp_j.get("id"):
                log_payload["customer_id_in_body"] = _tmp_j.get("id")
        except Exception:  # noqa: BLE001
            pass
        # In developer mode / debug flag include full signatures + body base64 (security: dev only)
        if frappe.conf.get("developer_mode") or frappe.form_dict.get("d") in ("1", "true", "True"):
            try:
                log_payload.update(
                    {
                        "expected_full": _compute_signature(secret.encode(), raw_body),
                        "provided_full": sig_header,
                        "body_base64": base64.b64encode(raw_body).decode(),
                    }
                )
            except Exception:  # noqa: BLE001
                pass
        frappe.logger().warning(log_payload)
        frappe.local.response.http_status_code = 403
        if frappe.conf.get("developer_mode") or debug_flag:
            return {"success": False, "error": "invalid_signature", "debug": log_payload}
        return {"success": False, "error": "invalid_signature"}

    # Parse JSON; if empty or invalid but appears to be an initial Woo handshake (no customer id), ACK gracefully
    try:
        payload = json.loads(raw_body.decode() or "{}")
    except Exception:  # noqa: BLE001
        payload = {}

    # Handshake / test ping when creating webhook in Woo (no customer object yet)
    if not isinstance(payload, dict) or not payload.get("id"):
        # Return 200 success so Woo accepts the webhook configuration; include debug if requested
        base_ack = {"success": True, "ack": True}
        if debug_flag:
            base_ack["debug"] = {
                "headers": {k: v for k, v in (frappe.request.headers or {}).items()},
                "body_base64": base64.b64encode(raw_body).decode(),
            }
        return base_ack

    headers = {k: v for k, v in (frappe.request.headers or {}).items()}
    job_name = f"woo_customer_{payload.get('id')}_{now_datetime().isoformat()}"
    frappe.enqueue(
        _enqueue_customer_process,
        queue="short",
        job_name=job_name,
        enqueue_after_commit=True,
        payload=payload,
        headers=headers,
    )
    frappe.logger().info(
        {
            "event": "woo_customer_webhook_enqueued",
            "customer_id": payload.get("id"),
            "job_name": job_name,
        }
    )
    resp = {"success": True, "queued": True, "job_name": job_name}
    if debug_flag:
        resp["debug"] = {"headers": {k: v for k, v in (frappe.request.headers or {}).items()}, "body_len": len(raw_body)}
    return resp


@frappe.whitelist(allow_guest=False)
def dev_invoke_customer(payload: str | None = None):
    """Developer-mode helper: directly process a JSON payload without signature.

    Only allowed when system is in developer_mode and user has System Manager.
    Accepts JSON string or uses current request data. Returns processing metrics.
    """
    if not frappe.conf.get("developer_mode"):
        frappe.throw("Not allowed outside developer_mode")
    if not frappe.has_permission("System Settings", ptype="write"):
        frappe.throw("Insufficient permission")
    raw_body = (payload or frappe.request.data or b"{}")
    if isinstance(raw_body, bytes):
        raw_body = raw_body.decode()
    try:
        obj = json.loads(raw_body)
    except Exception:  # noqa: BLE001
        frappe.throw("Invalid JSON")
    if not isinstance(obj, dict):
        frappe.throw("Payload must be object")
    settings = WooCommerceSettings.get_settings()
    metrics = process_customer_record(obj, settings, debug=True, debug_samples=[])
    return {"success": True, "data": metrics}


@frappe.whitelist(allow_guest=False)
def dev_compute_expected_signature():  # pragma: no cover - helper
    """Developer helper: return expected signature for posted raw body using stored secret.

    Usage (developer_mode only):
    POST body (raw JSON) to /api/method/jarz_woocommerce_integration.api.webhooks.dev_compute_expected_signature
    Returns base64 HMAC-SHA256. Compare with Woo's X-WC-Webhook-Signature.
    """
    if not frappe.conf.get("developer_mode"):
        frappe.throw("Not allowed outside developer_mode")
    if not frappe.has_permission("System Settings", ptype="write"):
        frappe.throw("Insufficient permission")
    raw_body: bytes = frappe.request.data or b""
    settings = WooCommerceSettings.get_settings()
    try:
        secret = (
            get_decrypted_password(
                "WooCommerce Settings", settings.name, "webhook_secret"
            )
            or ""
        )
    except Exception:  # noqa: BLE001
        secret = ""
    if not secret:
        frappe.throw("webhook_secret not set")
    expected = _compute_signature(secret.encode(), raw_body)
    return {
        "success": True,
        "expected_signature": expected,
        "secret_length": len(secret),
        "body_length": len(raw_body),
        "body_base64": base64.b64encode(raw_body).decode(),
    }
