from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import frappe
from frappe.utils import now_datetime

from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
    WooCommerceSettings,
)
from jarz_woocommerce_integration.utils.http_client import WooAPIError

EVENT_DOCTYPE = "WooCommerce Sync Event"
DEFAULT_PRIORITY = 50
ORDER_PRIORITY = 10
CUSTOMER_PRIORITY = 20
OUTBOUND_PRIORITY = 40
LOCK_TIMEOUT_SECONDS = 900
PROCESSING_STATUSES = ("Pending", "RetryScheduled")
TRANSIENT_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}
RETRYABLE_REASON_TOKENS = (
    "timeout",
    "timed out",
    "temporarily",
    "temporarily unavailable",
    "connection",
    "network",
    "rate limit",
    "429",
    "500",
    "502",
    "503",
    "504",
    "locked",
    "db_locked",
    "deadlock",
    "lock wait",
)
REVIEW_REASON_TOKENS = (
    "invalid_payload",
    "missing credentials",
    "missing_credentials",
    "no billing or shipping address",
    "no address",
    "no_usable_address",
    "missing product",
    "missing item",
    "manual review",
    "ofd",
    "out for delivery",
    "period close",
    "amendment depth",
    "territory",
    "validation",
)
SKIP_REASON_TOKENS = (
    "already_mapped",
    "unchanged",
    "pending_payment",
    "processing",
    "submitted_frozen",
)
RETRYABLE_ORDER_REASONS = {"locked", "db_locked"}

LOGGER = frappe.logger("jarz_woocommerce.sync_events")


@dataclass(slots=True)
class SyncEventConfig:
    enabled: bool
    shadow_mode: bool
    use_outbox_for_customer_push: bool
    use_outbox_for_invoice_push: bool
    use_inbox_for_order_webhook: bool
    use_inbox_for_customer_webhook: bool
    use_inbox_for_order_polling: bool
    use_event_reconciliation: bool
    worker_enabled: bool
    max_attempts: int
    batch_size: int
    success_retention_days: int


def _setting_int(settings: WooCommerceSettings, fieldname: str, default: int) -> int:
    try:
        value = int(getattr(settings, fieldname, default) or default)
    except Exception:
        value = default
    return max(1, value)


def get_sync_event_config(settings: WooCommerceSettings | None = None) -> SyncEventConfig:
    settings = settings or WooCommerceSettings.get_settings()
    return SyncEventConfig(
        enabled=bool(getattr(settings, "enable_sync_event_ledger", 0)),
        shadow_mode=bool(getattr(settings, "sync_event_shadow_mode", 0)),
        use_outbox_for_customer_push=bool(getattr(settings, "use_outbox_for_customer_push", 0)),
        use_outbox_for_invoice_push=bool(getattr(settings, "use_outbox_for_invoice_push", 0)),
        use_inbox_for_order_webhook=bool(getattr(settings, "use_inbox_for_order_webhook", 0)),
        use_inbox_for_customer_webhook=bool(getattr(settings, "use_inbox_for_customer_webhook", 0)),
        use_inbox_for_order_polling=bool(getattr(settings, "use_inbox_for_order_polling", 0)),
        use_event_reconciliation=bool(getattr(settings, "use_event_reconciliation", 0)),
        worker_enabled=bool(getattr(settings, "sync_event_worker_enabled", 0)),
        max_attempts=_setting_int(settings, "sync_event_max_attempts", 8),
        batch_size=_setting_int(settings, "sync_event_batch_size", 25),
        success_retention_days=_setting_int(settings, "sync_event_success_retention_days", 90),
    )


def is_event_ledger_enabled(settings: WooCommerceSettings | None = None) -> bool:
    return get_sync_event_config(settings).enabled


def is_shadow_mode_enabled(settings: WooCommerceSettings | None = None) -> bool:
    cfg = get_sync_event_config(settings)
    return cfg.enabled and cfg.shadow_mode


def should_use_customer_outbox(
    settings: WooCommerceSettings | None = None,
    *,
    reason: str | None = None,
) -> bool:
    cfg = get_sync_event_config(settings)
    if not cfg.enabled:
        return False
    if reason == "reconcile":
        return cfg.use_event_reconciliation or cfg.use_outbox_for_customer_push
    return cfg.use_outbox_for_customer_push


def should_use_invoice_outbox(
    settings: WooCommerceSettings | None = None,
    *,
    reason: str | None = None,
) -> bool:
    cfg = get_sync_event_config(settings)
    if not cfg.enabled:
        return False
    if reason == "reconcile":
        return cfg.use_event_reconciliation or cfg.use_outbox_for_invoice_push
    return cfg.use_outbox_for_invoice_push


def should_use_order_webhook_inbox(settings: WooCommerceSettings | None = None) -> bool:
    cfg = get_sync_event_config(settings)
    return cfg.enabled and cfg.use_inbox_for_order_webhook


def should_use_customer_webhook_inbox(settings: WooCommerceSettings | None = None) -> bool:
    cfg = get_sync_event_config(settings)
    return cfg.enabled and cfg.use_inbox_for_customer_webhook


def should_use_order_polling_events(settings: WooCommerceSettings | None = None) -> bool:
    cfg = get_sync_event_config(settings)
    return cfg.enabled and cfg.use_inbox_for_order_polling


def should_use_reconcile_events(settings: WooCommerceSettings | None = None) -> bool:
    cfg = get_sync_event_config(settings)
    return cfg.enabled and cfg.use_event_reconciliation


def is_worker_enabled(settings: WooCommerceSettings | None = None) -> bool:
    cfg = get_sync_event_config(settings)
    return cfg.enabled and cfg.worker_enabled


def _to_json(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, ensure_ascii=True, default=str)


def _from_json(value: str | None) -> Any:
    if not value:
        return {}
    try:
        return json.loads(value)
    except Exception:
        return {"raw": value}


def _hash_text(value: str | None) -> str:
    if not value:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _event_doc_name(doc_or_name: Any) -> str | None:
    if not doc_or_name:
        return None
    return getattr(doc_or_name, "name", None) or str(doc_or_name)


def _get_doc_modified(doctype: str, docname: str) -> str:
    try:
        return str(frappe.db.get_value(doctype, docname, "modified") or "")
    except Exception:
        return ""


def _build_idempotency_key(prefix: str, identifier: str, discriminator: str | None = None) -> str:
    parts = [prefix, identifier]
    if discriminator:
        parts.append(discriminator)
    return ":".join(parts)


def create_sync_event(
    *,
    direction: str,
    event_type: str,
    source_system: str,
    target_system: str,
    object_type: str,
    source_id: str,
    idempotency_key: str,
    payload_json: Any = None,
    desired_state_json: Any = None,
    remote_response_json: Any = None,
    payload_hash: str | None = None,
    local_doctype: str | None = None,
    local_docname: str | None = None,
    woo_order_id: int | None = None,
    woo_customer_id: str | None = None,
    priority: int = DEFAULT_PRIORITY,
    status: str = "Pending",
    next_attempt_at=None,
    max_attempts: int | None = None,
    correlation_id: str | None = None,
    parent_event: str | None = None,
    sync_log: str | None = None,
    manual_review_reason: str | None = None,
) -> Any:
    payload_text = _to_json(payload_json)
    desired_text = _to_json(desired_state_json)
    response_text = _to_json(remote_response_json)
    payload_hash = payload_hash or _hash_text(payload_text or desired_text)
    settings = WooCommerceSettings.get_settings()
    cfg = get_sync_event_config(settings)
    doc_values = {
        "doctype": EVENT_DOCTYPE,
        "direction": direction,
        "event_type": event_type,
        "source_system": source_system,
        "target_system": target_system,
        "object_type": object_type,
        "source_id": str(source_id),
        "local_doctype": local_doctype,
        "local_docname": local_docname,
        "woo_order_id": woo_order_id,
        "woo_customer_id": woo_customer_id,
        "idempotency_key": idempotency_key,
        "payload_hash": payload_hash,
        "payload_json": payload_text,
        "desired_state_json": desired_text,
        "remote_response_json": response_text,
        "status": status,
        "priority": priority,
        "attempt_count": 0,
        "max_attempts": max_attempts or cfg.max_attempts,
        "next_attempt_at": next_attempt_at or now_datetime(),
        "first_seen_on": now_datetime(),
        "correlation_id": correlation_id,
        "parent_event": parent_event,
        "sync_log": sync_log,
        "manual_review_reason": manual_review_reason,
    }
    try:
        doc = frappe.get_doc(doc_values)
        doc.insert(ignore_permissions=True)
        return doc
    except frappe.DuplicateEntryError:
        existing_name = frappe.db.get_value(EVENT_DOCTYPE, {"idempotency_key": idempotency_key}, "name")
        if existing_name:
            return frappe.get_doc(EVENT_DOCTYPE, existing_name)
        raise


def _supersede_pending_events(
    *,
    direction: str,
    object_type: str,
    source_id: str,
    exclude_event_name: str,
    local_doctype: str | None = None,
    local_docname: str | None = None,
) -> None:
    now = now_datetime()
    filters = [direction, object_type, str(source_id), exclude_event_name, now, now]
    sql = [
        "UPDATE `tabWooCommerce Sync Event`",
        "SET status = 'Superseded', completed_on = %s, locked_until = NULL, last_error = 'Superseded by newer event'",
        "WHERE direction = %s",
        "  AND object_type = %s",
        "  AND source_id = %s",
        "  AND name != %s",
        "  AND status IN ('Pending', 'RetryScheduled')",
        "  AND (locked_until IS NULL OR locked_until < %s)",
    ]
    params = [now, direction, object_type, str(source_id), exclude_event_name, now]
    if local_doctype:
        sql.append("  AND local_doctype = %s")
        params.append(local_doctype)
    if local_docname:
        sql.append("  AND local_docname = %s")
        params.append(local_docname)
    frappe.db.sql("\n".join(sql), tuple(params))


def create_outbound_customer_event(
    customer_name: str,
    *,
    reason: str,
    scope: str | None = None,
    force: bool = False,
) -> Any:
    payload = {
        "customer_name": customer_name,
        "reason": reason,
        "scope": scope,
        "force": bool(force),
    }
    modified = _get_doc_modified("Customer", customer_name)
    discriminator = ":".join(part for part in [scope or "full", modified] if part)
    event = create_sync_event(
        direction="Outbound",
        event_type="customer_push",
        source_system="ERPNext",
        target_system="WooCommerce",
        object_type="Customer",
        source_id=customer_name,
        local_doctype="Customer",
        local_docname=customer_name,
        woo_customer_id=str(frappe.db.get_value("Customer", customer_name, "woo_customer_id") or "") or None,
        idempotency_key=_build_idempotency_key("out:erp:Customer", customer_name, discriminator or None),
        payload_json=payload,
        priority=OUTBOUND_PRIORITY,
    )
    if getattr(event, "name", None):
        _supersede_pending_events(
            direction="Outbound",
            object_type="Customer",
            source_id=customer_name,
            exclude_event_name=event.name,
            local_doctype="Customer",
            local_docname=customer_name,
        )
    return event


def create_outbound_invoice_event(
    invoice_name: str,
    *,
    reason: str,
    cancel: bool = False,
    force: bool = False,
) -> Any:
    payload = {
        "invoice_name": invoice_name,
        "reason": reason,
        "cancel": bool(cancel),
        "force": bool(force),
    }
    modified = _get_doc_modified("Sales Invoice", invoice_name)
    discriminator = ":".join(part for part in ["cancel" if cancel else "sync", modified] if part)
    woo_order_id = frappe.db.get_value("Sales Invoice", invoice_name, "woo_order_id")
    event = create_sync_event(
        direction="Outbound",
        event_type="invoice_push",
        source_system="ERPNext",
        target_system="WooCommerce",
        object_type="Sales Invoice",
        source_id=invoice_name,
        local_doctype="Sales Invoice",
        local_docname=invoice_name,
        woo_order_id=int(woo_order_id) if str(woo_order_id or "").isdigit() else None,
        idempotency_key=_build_idempotency_key("out:erp:Sales Invoice", invoice_name, discriminator or None),
        payload_json=payload,
        priority=OUTBOUND_PRIORITY,
    )
    if getattr(event, "name", None):
        _supersede_pending_events(
            direction="Outbound",
            object_type="Sales Invoice",
            source_id=invoice_name,
            exclude_event_name=event.name,
            local_doctype="Sales Invoice",
            local_docname=invoice_name,
        )
    return event


def create_inbound_order_event(
    order_payload: dict[str, Any],
    *,
    event_type: str,
    payload_hash: str | None = None,
    correlation_id: str | None = None,
    sync_log: Any = None,
) -> Any:
    woo_order_id = int(order_payload.get("id") or 0)
    payload_hash = payload_hash or _hash_text(_to_json(order_payload))
    return create_sync_event(
        direction="Inbound",
        event_type=event_type,
        source_system="WooCommerce",
        target_system="ERPNext",
        object_type="Order",
        source_id=str(woo_order_id),
        woo_order_id=woo_order_id or None,
        idempotency_key=_build_idempotency_key("in:woo:order", str(woo_order_id), payload_hash or None),
        payload_json=order_payload,
        payload_hash=payload_hash,
        priority=ORDER_PRIORITY,
        correlation_id=correlation_id,
        sync_log=_event_doc_name(sync_log),
    )


def create_inbound_customer_event(
    payload: dict[str, Any],
    *,
    payload_hash: str | None = None,
    headers: dict[str, Any] | None = None,
    correlation_id: str | None = None,
) -> Any:
    customer_id = str(payload.get("id") or "")
    normalized_payload = dict(payload)
    if headers:
        normalized_payload["_headers"] = headers
    payload_hash = payload_hash or _hash_text(_to_json(normalized_payload))
    return create_sync_event(
        direction="Inbound",
        event_type="customer_webhook",
        source_system="WooCommerce",
        target_system="ERPNext",
        object_type="Customer",
        source_id=customer_id,
        woo_customer_id=customer_id or None,
        idempotency_key=_build_idempotency_key("in:woo:customer", customer_id or "unknown", payload_hash or None),
        payload_json=normalized_payload,
        payload_hash=payload_hash,
        priority=CUSTOMER_PRIORITY,
        correlation_id=correlation_id,
    )


def create_inbound_order_reference_event(
    *,
    order_id: int | str,
    event_type: str,
    status: str | None = None,
    modified_gmt: str | None = None,
    correlation_id: str | None = None,
) -> Any:
    order_id_text = str(order_id)
    payload = {
        "id": int(order_id) if str(order_id).isdigit() else order_id_text,
        "status": status,
        "modified_gmt": modified_gmt,
        "event_type": event_type,
    }
    discriminator = ":".join(part for part in [status or "", modified_gmt or ""] if part)
    return create_sync_event(
        direction="Inbound",
        event_type=event_type,
        source_system="WooCommerce",
        target_system="ERPNext",
        object_type="Order",
        source_id=order_id_text,
        woo_order_id=int(order_id) if order_id_text.isdigit() else None,
        idempotency_key=_build_idempotency_key("in:woo:order", order_id_text, discriminator or None),
        payload_json=payload,
        priority=ORDER_PRIORITY,
        correlation_id=correlation_id,
    )


def enqueue_sync_event(event_name: str, *, after_commit: bool = True, queue: str = "short") -> None:
    frappe.enqueue(
        "jarz_woocommerce_integration.services.sync_events.process_sync_event",
        queue=queue,
        timeout=900,
        enqueue_after_commit=after_commit,
        event_name=event_name,
    )


def _claim_sync_event(event_name: str) -> Any | None:
    now = now_datetime()
    worker_id = f"{frappe.local.site}:{uuid.uuid4().hex[:12]}"
    lock_until = now + timedelta(seconds=LOCK_TIMEOUT_SECONDS)
    frappe.db.sql(
        """
        UPDATE `tabWooCommerce Sync Event`
        SET status = 'Processing',
            locked_by = %s,
            locked_until = %s,
            started_on = %s,
            attempt_count = IFNULL(attempt_count, 0) + 1
        WHERE name = %s
          AND status IN ('Pending', 'RetryScheduled')
          AND (next_attempt_at IS NULL OR next_attempt_at <= %s)
          AND (locked_until IS NULL OR locked_until < %s)
        """,
        (worker_id, lock_until, now, event_name, now, now),
    )
    row = frappe.db.get_value(
        EVENT_DOCTYPE,
        event_name,
        ["name", "locked_by", "status"],
        as_dict=True,
    )
    if not row or row.get("locked_by") != worker_id or row.get("status") != "Processing":
        return None
    return frappe.get_doc(EVENT_DOCTYPE, event_name)


def _backoff_minutes(attempt_count: int) -> int:
    schedule = {
        1: 1,
        2: 5,
        3: 15,
        4: 60,
        5: 180,
        6: 360,
        7: 720,
        8: 1440,
    }
    return schedule.get(max(1, attempt_count), 1440)


def _mark_event(
    event_doc: Any,
    *,
    status: str,
    remote_response_json: Any = None,
    last_error: str | None = None,
    manual_review_reason: str | None = None,
    traceback_text: str | None = None,
    next_attempt_at=None,
) -> None:
    updates = {
        "status": status,
        "remote_response_json": _to_json(remote_response_json),
        "last_error": (last_error or "")[:500],
        "manual_review_reason": manual_review_reason or "",
        "traceback": traceback_text or "",
        "locked_by": "",
        "locked_until": None,
        "completed_on": now_datetime() if status in {"Succeeded", "Skipped", "Superseded", "NeedsReview", "DeadLetter"} else None,
        "next_attempt_at": next_attempt_at,
    }
    event_doc.db_set(updates, update_modified=False)


def _schedule_retry(event_doc: Any, *, result: Any = None, error_text: str | None = None) -> dict[str, Any]:
    next_attempt = now_datetime() + timedelta(minutes=_backoff_minutes(int(getattr(event_doc, "attempt_count", 1) or 1)))
    _mark_event(
        event_doc,
        status="RetryScheduled",
        remote_response_json=result,
        last_error=error_text,
        next_attempt_at=next_attempt,
    )
    return {"status": "retry", "next_attempt_at": next_attempt.isoformat()}


def _mark_success(event_doc: Any, result: Any) -> dict[str, Any]:
    _mark_event(event_doc, status="Succeeded", remote_response_json=result)
    return {"status": "succeeded", "result": result}


def _mark_skipped(event_doc: Any, result: Any, reason: str | None = None) -> dict[str, Any]:
    _mark_event(event_doc, status="Skipped", remote_response_json=result, last_error=reason)
    return {"status": "skipped", "reason": reason or "skipped", "result": result}


def _mark_needs_review(event_doc: Any, result: Any, reason: str) -> dict[str, Any]:
    _mark_event(
        event_doc,
        status="NeedsReview",
        remote_response_json=result,
        last_error=reason,
        manual_review_reason=reason,
    )
    return {"status": "needs_review", "reason": reason}


def _mark_dead_letter(event_doc: Any, result: Any, reason: str) -> dict[str, Any]:
    _mark_event(event_doc, status="DeadLetter", remote_response_json=result, last_error=reason)
    return {"status": "dead_letter", "reason": reason}


def _classify_text_reason(text: str | None) -> str:
    normalized = (text or "").strip().lower()
    if not normalized:
        return "review"
    if any(token in normalized for token in SKIP_REASON_TOKENS):
        return "skip"
    if any(token in normalized for token in REVIEW_REASON_TOKENS):
        return "review"
    if any(token in normalized for token in RETRYABLE_REASON_TOKENS):
        return "retry"
    return "review"


def _handle_failure(event_doc: Any, *, result: Any = None, detail: str | None = None) -> dict[str, Any]:
    classification = _classify_text_reason(detail)
    if classification == "skip":
        return _mark_skipped(event_doc, result, detail)
    if classification == "retry":
        if int(getattr(event_doc, "attempt_count", 0) or 0) >= int(getattr(event_doc, "max_attempts", 1) or 1):
            return _mark_dead_letter(event_doc, result, detail or "retry_limit_exhausted")
        return _schedule_retry(event_doc, result=result, error_text=detail)
    return _mark_needs_review(event_doc, result, detail or "manual_review_required")


def _handle_exception(event_doc: Any, exc: Exception) -> dict[str, Any]:
    detail = str(exc) or exc.__class__.__name__
    if isinstance(exc, WooAPIError):
        if exc.status_code in TRANSIENT_HTTP_STATUS_CODES:
            if int(getattr(event_doc, "attempt_count", 0) or 0) >= int(getattr(event_doc, "max_attempts", 1) or 1):
                return _mark_dead_letter(event_doc, None, detail)
            return _schedule_retry(event_doc, error_text=detail)
        if exc.status_code >= 400:
            return _mark_needs_review(event_doc, None, detail)
    if int(getattr(event_doc, "attempt_count", 0) or 0) >= int(getattr(event_doc, "max_attempts", 1) or 1):
        return _mark_dead_letter(event_doc, None, detail)
    if _classify_text_reason(detail) == "review":
        return _mark_needs_review(event_doc, None, detail)
    return _schedule_retry(event_doc, error_text=detail)


def _dispatch_outbound_event(event_doc: Any, payload: dict[str, Any]) -> Any:
    from jarz_woocommerce_integration.services import outbound_sync

    if event_doc.object_type == "Customer":
        return outbound_sync.sync_customer(
            event_doc.local_docname or event_doc.source_id,
            reason=payload.get("reason") or "sync_event",
            force=bool(payload.get("force")),
            scope=payload.get("scope"),
        )
    if event_doc.object_type == "Sales Invoice":
        return outbound_sync.sync_sales_invoice(
            event_doc.local_docname or event_doc.source_id,
            reason=payload.get("reason") or "sync_event",
            force=bool(payload.get("force")),
            cancel=bool(payload.get("cancel")),
        )
    raise ValueError(f"Unsupported outbound object_type: {event_doc.object_type}")


def _dispatch_inbound_event(event_doc: Any, payload: dict[str, Any]) -> Any:
    if event_doc.object_type == "Order":
        from jarz_woocommerce_integration.services.order_sync import pull_single_order_phase1

        frappe.set_user("Administrator")
        order_id = event_doc.woo_order_id or payload.get("id") or event_doc.source_id
        return pull_single_order_phase1(order_id=order_id, dry_run=False, force=False, allow_update=True)

    if event_doc.object_type == "Customer":
        from jarz_woocommerce_integration.services.customer_sync import process_customer_record

        frappe.set_user("Administrator")
        settings = WooCommerceSettings.get_settings()
        clean_payload = dict(payload)
        clean_payload.pop("_headers", None)
        return process_customer_record(clean_payload, settings, debug=False, debug_samples=None)

    raise ValueError(f"Unsupported inbound object_type: {event_doc.object_type}")


def _apply_outbound_result(event_doc: Any, result: Any) -> dict[str, Any]:
    if isinstance(result, dict):
        if result.get("status") == "ok" or result.get("success") is True:
            return _mark_success(event_doc, result)
        if result.get("skipped") or result.get("status") == "skipped":
            return _mark_skipped(event_doc, result, str(result.get("reason") or "skipped"))
        if result.get("status") == "error" or result.get("success") is False:
            detail = str(result.get("detail") or result.get("error") or result.get("reason") or "error")
            return _handle_failure(event_doc, result=result, detail=detail)
    return _mark_success(event_doc, result)


def _apply_inbound_result(event_doc: Any, result: Any) -> dict[str, Any]:
    if event_doc.object_type == "Order" and isinstance(result, dict):
        if result.get("success") is True or result.get("status") in {"created", "updated"}:
            return _mark_success(event_doc, result)
        if result.get("status") == "skipped":
            reason = str(result.get("reason") or "skipped")
            if reason in RETRYABLE_ORDER_REASONS:
                return _handle_failure(event_doc, result=result, detail=reason)
            return _mark_skipped(event_doc, result, reason)
        if result.get("status") == "error":
            detail = str(result.get("detail") or result.get("reason") or result.get("message") or "error")
            return _handle_failure(event_doc, result=result, detail=detail)
    if event_doc.object_type == "Customer" and isinstance(result, dict):
        if result.get("status") == "success":
            return _mark_success(event_doc, result)
        if result.get("status") == "error":
            detail = str(result.get("error") or result.get("reason") or "error")
            return _handle_failure(event_doc, result=result, detail=detail)
    if isinstance(result, dict) and result.get("success") is True:
        return _mark_success(event_doc, result)
    return _mark_success(event_doc, result)


def process_sync_event(event_name: str) -> dict[str, Any]:
    event_doc = _claim_sync_event(event_name)
    if not event_doc:
        return {"skipped": True, "reason": "not_due_or_already_claimed", "event_name": event_name}

    payload = _from_json(getattr(event_doc, "payload_json", ""))
    if not isinstance(payload, dict):
        payload = {"raw": payload}

    try:
        if event_doc.direction == "Outbound":
            result = _dispatch_outbound_event(event_doc, payload)
            outcome = _apply_outbound_result(event_doc, result)
        else:
            result = _dispatch_inbound_event(event_doc, payload)
            outcome = _apply_inbound_result(event_doc, result)
        frappe.db.commit()
        return {"event_name": event_name, **outcome}
    except Exception as exc:  # noqa: BLE001
        LOGGER.error({
            "event": "woo_sync_event_process_error",
            "event_name": event_name,
            "traceback": frappe.get_traceback(),
        })
        outcome = _handle_exception(event_doc, exc)
        event_doc.db_set({"traceback": frappe.get_traceback()}, update_modified=False)
        frappe.db.commit()
        return {"event_name": event_name, **outcome}


def process_due_sync_events(batch_size: int | None = None) -> dict[str, Any]:  # pragma: no cover
    settings = WooCommerceSettings.get_settings()
    cfg = get_sync_event_config(settings)
    if not cfg.enabled:
        return {"skipped": True, "reason": "ledger_disabled"}
    if not cfg.worker_enabled:
        return {"skipped": True, "reason": "worker_disabled"}

    effective_batch_size = max(1, int(batch_size or cfg.batch_size))
    now = now_datetime()
    rows = frappe.db.sql(
        """
        SELECT name
        FROM `tabWooCommerce Sync Event`
        WHERE status IN ('Pending', 'RetryScheduled')
          AND (next_attempt_at IS NULL OR next_attempt_at <= %s)
          AND (locked_until IS NULL OR locked_until < %s)
        ORDER BY priority ASC, first_seen_on ASC
        LIMIT %s
        """,
        (now, now, effective_batch_size),
        as_dict=True,
    )

    summary = {"processed": 0, "succeeded": 0, "skipped": 0, "retry": 0, "needs_review": 0, "dead_letter": 0}
    for row in rows:
        result = process_sync_event(row["name"])
        summary["processed"] += 1
        outcome = str(result.get("status") or "").lower()
        if outcome == "succeeded":
            summary["succeeded"] += 1
        elif outcome == "skipped":
            summary["skipped"] += 1
        elif outcome == "retry":
            summary["retry"] += 1
        elif outcome == "needs_review":
            summary["needs_review"] += 1
        elif outcome == "dead_letter":
            summary["dead_letter"] += 1
    if summary["processed"]:
        LOGGER.info({"event": "woo_sync_event_batch_processed", "summary": summary})
    return summary


def retry_sync_event(event_name: str) -> dict[str, Any]:
    doc = frappe.get_doc(EVENT_DOCTYPE, event_name)
    doc.db_set(
        {
            "status": "Pending",
            "last_error": "",
            "manual_review_reason": "",
            "traceback": "",
            "locked_by": "",
            "locked_until": None,
            "next_attempt_at": now_datetime(),
            "completed_on": None,
        },
        update_modified=False,
    )
    enqueue_sync_event(event_name, after_commit=False)
    frappe.db.commit()
    return {"success": True, "event_name": event_name}


def purge_old_sync_events(retention_days: int | None = None) -> dict[str, Any]:  # pragma: no cover
    settings = WooCommerceSettings.get_settings()
    cfg = get_sync_event_config(settings)
    days = max(1, int(retention_days or cfg.success_retention_days))
    cutoff = now_datetime() - timedelta(days=days)
    names = frappe.get_all(
        EVENT_DOCTYPE,
        filters={
            "status": ["in", ["Succeeded", "Skipped", "Superseded"]],
            "completed_on": ["<", cutoff],
        },
        pluck="name",
        limit_page_length=500,
    )
    for name in names:
        frappe.delete_doc(EVENT_DOCTYPE, name, ignore_permissions=True)
    frappe.db.commit()
    return {"deleted": len(names), "cutoff": cutoff.isoformat()}