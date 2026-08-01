from __future__ import annotations

import json
from dataclasses import asdict
from datetime import timedelta
from typing import Any

import frappe
from frappe import _
from frappe.utils import now_datetime

from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
	WooCommerceSettings,
)
from jarz_woocommerce_integration.services import sync_events


EVENT_DOCTYPE = sync_events.EVENT_DOCTYPE
OPERATOR_ROLES = {"System Manager", "WooCommerce Sync Operator"}
ATTENTION_STATUSES = ("Failed", "NeedsReview", "DeadLetter")
OPEN_STATUSES = ("Pending", "RetryScheduled", "Processing")
RETRYABLE_STATUSES = ("Pending", "RetryScheduled", "Failed", "NeedsReview", "DeadLetter")
TERMINAL_STATUSES = tuple(sorted(sync_events.TERMINAL_STATUSES))
REVIEW_STATES = ("Open", "Investigating", "Resolved", "Ignored")

EVENT_LIST_FIELDS = [
	"name",
	"direction",
	"event_type",
	"status",
	"priority",
	"first_seen_on",
	"next_attempt_at",
	"attempt_count",
	"max_attempts",
	"object_type",
	"source_id",
	"woo_order_id",
	"woo_customer_id",
	"local_doctype",
	"local_docname",
	"review_state",
	"is_retention_exempt",
	"retain_until",
	"last_operation",
	"last_operation_on",
	"manual_review_reason",
	"last_error",
	"modified",
]


def _require_sync_event_access(*, write: bool = False) -> None:
	if frappe.session.user == "Administrator":
		return
	roles = set(frappe.get_roles() or [])
	if roles.intersection(OPERATOR_ROLES):
		return
	if frappe.has_permission(EVENT_DOCTYPE, ptype="write" if write else "read"):
		return
	frappe.throw(_("Not permitted to access WooCommerce Sync operations"), frappe.PermissionError)


def _parse_json_arg(value: Any, default: Any) -> Any:
	if value in (None, ""):
		return default
	if isinstance(value, str):
		try:
			return json.loads(value)
		except Exception:
			return default
	return value


def _coerce_positive_int(value: Any, default: int, *, max_value: int | None = None) -> int:
	try:
		coerced = int(value)
	except Exception:
		coerced = default
	coerced = max(1, coerced)
	if max_value is not None:
		coerced = min(coerced, max_value)
	return coerced


def _datetime_to_text(value: Any) -> str | None:
	if not value:
		return None
	try:
		return value.isoformat()
	except Exception:
		return str(value)


def _rows_by_key(query: str, params: tuple[Any, ...], key_field: str) -> dict[str, int]:
	rows = frappe.db.sql(query, params, as_dict=True)
	return {str(row.get(key_field) or ""): int(row.get("count") or 0) for row in rows}


def _count_where(filters: dict[str, Any]) -> int:
	return int(frappe.db.count(EVENT_DOCTYPE, filters=filters) or 0)


def _validate_review_state(review_state: str) -> str:
	value = str(review_state or "").strip()
	if value not in REVIEW_STATES:
		frappe.throw(_("Invalid review state: {0}").format(value or "<empty>"))
	return value


def _set_event_fields(event_name: str, updates: dict[str, Any]) -> None:
	if not frappe.db.exists(EVENT_DOCTYPE, event_name):
		frappe.throw(_("WooCommerce Sync Event not found: {0}").format(event_name))
	frappe.db.set_value(EVENT_DOCTYPE, event_name, updates)
	frappe.db.commit()


def _mark_operator_action(event_name: str, action: str, extra_updates: dict[str, Any] | None = None) -> None:
	now = now_datetime()
	updates = {
		"last_operation": action,
		"last_operation_by": frappe.session.user,
		"last_operation_on": now,
	}
	if extra_updates:
		updates.update(extra_updates)
	_set_event_fields(event_name, updates)


def _get_shadow_failure_state() -> dict[str, Any]:
	try:
		return sync_events._cache_get_json_state(sync_events._shadow_failure_counter_key())  # noqa: SLF001
	except Exception:
		return {}


def _get_breaker_state() -> dict[str, Any]:
	settings = WooCommerceSettings.get_settings()
	state = sync_events._get_outbound_circuit_breaker_state(settings)  # noqa: SLF001
	open_until = state.get("open_until")
	now = now_datetime()
	return {
		"failure_count": len(state.get("failures") or []),
		"open_until": _datetime_to_text(open_until),
		"is_open": bool(open_until and open_until > now),
	}


def _settings_summary() -> dict[str, Any]:
	settings = WooCommerceSettings.get_settings()
	cfg = sync_events.get_sync_event_config(settings)
	summary = asdict(cfg)
	# The two master outbound kill-switches are NOT part of SyncEventConfig, but
	# they are checked before every outbox gate that is — so with them off the
	# dashboard showed a fully green config while no outbound event was written
	# at all. Staging sat like that for seven weeks (2026-06-12 to 2026-08-01)
	# and the whole config looked healthy the entire time. Surface them.
	summary["enable_outbound_customers"] = bool(getattr(settings, "enable_outbound_customers", 0))
	summary["enable_outbound_orders"] = bool(getattr(settings, "enable_outbound_orders", 0))
	return summary


def _recent_events(*, limit: int, statuses: tuple[str, ...] | None = None) -> list[dict[str, Any]]:
	filters: dict[str, Any] = {}
	if statuses:
		filters["status"] = ["in", list(statuses)]
	return frappe.get_all(
		EVENT_DOCTYPE,
		filters=filters,
		fields=EVENT_LIST_FIELDS,
		order_by="modified desc",
		limit_page_length=limit,
	)


@frappe.whitelist(allow_guest=False)
def get_dashboard(window_hours: int | str = 24, limit: int | str = 20) -> dict[str, Any]:
	_require_sync_event_access()
	window = _coerce_positive_int(window_hours, 24, max_value=720)
	row_limit = _coerce_positive_int(limit, 20, max_value=100)
	now = now_datetime()
	since = now - timedelta(hours=window)

	status_counts = _rows_by_key(
		"""
		SELECT status, COUNT(*) AS count
		FROM `tabWooCommerce Sync Event`
		WHERE first_seen_on >= %s
		GROUP BY status
		""",
		(since,),
		"status",
	)
	direction_counts = _rows_by_key(
		"""
		SELECT direction, COUNT(*) AS count
		FROM `tabWooCommerce Sync Event`
		WHERE first_seen_on >= %s
		GROUP BY direction
		""",
		(since,),
		"direction",
	)
	event_type_counts = _rows_by_key(
		"""
		SELECT event_type, COUNT(*) AS count
		FROM `tabWooCommerce Sync Event`
		WHERE first_seen_on >= %s
		GROUP BY event_type
		ORDER BY count DESC
		LIMIT 20
		""",
		(since,),
		"event_type",
	)

	due_rows = frappe.db.sql(
		"""
		SELECT COUNT(*) AS due_now, MIN(first_seen_on) AS oldest_due
		FROM `tabWooCommerce Sync Event`
		WHERE status IN ('Pending', 'RetryScheduled')
		  AND (next_attempt_at IS NULL OR next_attempt_at <= %s)
		  AND (locked_until IS NULL OR locked_until < %s)
		""",
		(now, now),
		as_dict=True,
	)
	due_summary = due_rows[0] if due_rows else {}

	backlog = {
		"pending": _count_where({"status": "Pending"}),
		"retry_scheduled": _count_where({"status": "RetryScheduled"}),
		"processing": _count_where({"status": "Processing"}),
		"needs_attention": _count_where({"status": ["in", list(ATTENTION_STATUSES)]}),
		"due_now": int(due_summary.get("due_now") or 0),
		"oldest_due": _datetime_to_text(due_summary.get("oldest_due")),
		"expired_processing": _count_where({"status": "Processing", "locked_until": ["<", now]}),
	}
	review_state_counts = _rows_by_key(
		"""
		SELECT review_state, COUNT(*) AS count
		FROM `tabWooCommerce Sync Event`
		WHERE first_seen_on >= %s
		  AND IFNULL(review_state, '') != ''
		GROUP BY review_state
		""",
		(since,),
		"review_state",
	)

	return {
		"generated_at": _datetime_to_text(now),
		"window_hours": window,
		"settings": _settings_summary(),
		"breaker": _get_breaker_state(),
		"shadow_failures": _get_shadow_failure_state(),
		"backlog": backlog,
		"status_counts": status_counts,
		"review_state_counts": review_state_counts,
		"direction_counts": direction_counts,
		"event_type_counts": event_type_counts,
		"attention_events": _recent_events(limit=row_limit, statuses=ATTENTION_STATUSES),
		"recent_events": _recent_events(limit=row_limit),
	}


@frappe.whitelist(allow_guest=False)
def get_events(filters: Any = None, limit: int | str = 50) -> list[dict[str, Any]]:
	_require_sync_event_access()
	parsed = _parse_json_arg(filters, {})
	if not isinstance(parsed, dict):
		parsed = {}

	query_filters: dict[str, Any] = {}
	for fieldname in ("status", "direction", "event_type", "object_type", "local_doctype", "review_state"):
		value = parsed.get(fieldname)
		if value:
			query_filters[fieldname] = value

	search = str(parsed.get("search") or "").strip()
	or_filters = None
	if search:
		or_filters = [
			{"name": ["like", f"%{search}%"]},
			{"source_id": ["like", f"%{search}%"]},
			{"local_docname": ["like", f"%{search}%"]},
			{"manual_review_reason": ["like", f"%{search}%"]},
		]
		if search.isdigit():
			or_filters.append({"woo_order_id": int(search)})

	return frappe.get_all(
		EVENT_DOCTYPE,
		filters=query_filters,
		or_filters=or_filters,
		fields=EVENT_LIST_FIELDS,
		order_by="modified desc",
		limit_page_length=_coerce_positive_int(limit, 50, max_value=500),
	)


@frappe.whitelist(allow_guest=False)
def run_worker(batch_size: int | str | None = None) -> dict[str, Any]:
	_require_sync_event_access(write=True)
	result = sync_events.process_due_sync_events(batch_size=_coerce_positive_int(batch_size, 25, max_value=200) if batch_size else None)
	frappe.logger("jarz_woocommerce.sync_events").info({
		"event": "woo_sync_operations_worker_run",
		"user": frappe.session.user,
		"result": result,
	})
	return result


@frappe.whitelist(allow_guest=False)
def retry_event(event_name: str) -> dict[str, Any]:
	_require_sync_event_access(write=True)
	if not event_name:
		frappe.throw(_("Missing event name"))
	status = frappe.db.get_value(EVENT_DOCTYPE, event_name, "status")
	if not status:
		frappe.throw(_("WooCommerce Sync Event not found: {0}").format(event_name))
	if status not in RETRYABLE_STATUSES:
		frappe.throw(_("Only pending, failed, review, or dead-letter events can be retried"))
	result = sync_events.retry_sync_event(event_name)
	frappe.logger("jarz_woocommerce.sync_events").info({
		"event": "woo_sync_operations_event_retry",
		"user": frappe.session.user,
		"event_name": event_name,
		"previous_status": status,
	})
	return result


@frappe.whitelist(allow_guest=False)
def retry_events(event_names: Any) -> dict[str, Any]:
	_require_sync_event_access(write=True)
	names = _parse_json_arg(event_names, [])
	if isinstance(names, str):
		names = [names]
	if not isinstance(names, list):
		frappe.throw(_("event_names must be a list"))

	results = []
	for event_name in names[:100]:
		results.append(retry_event(str(event_name)))
	return {"success": True, "count": len(results), "results": results}


@frappe.whitelist(allow_guest=False)
def process_event_now(event_name: str) -> dict[str, Any]:
	_require_sync_event_access(write=True)
	if not event_name:
		frappe.throw(_("Missing event name"))
	status = frappe.db.get_value(EVENT_DOCTYPE, event_name, "status")
	if not status:
		frappe.throw(_("WooCommerce Sync Event not found: {0}").format(event_name))
	if status in TERMINAL_STATUSES:
		return retry_event(event_name)
	frappe.db.set_value(EVENT_DOCTYPE, event_name, "next_attempt_at", now_datetime(), update_modified=False)
	frappe.db.commit()
	return sync_events.process_sync_event(event_name)


@frappe.whitelist(allow_guest=False)
def set_review_state(event_name: str, review_state: str, resolution_notes: str | None = None) -> dict[str, Any]:
	_require_sync_event_access(write=True)
	if not event_name:
		frappe.throw(_("Missing event name"))
	validated_state = _validate_review_state(review_state)
	_mark_operator_action(
		event_name,
		f"review_state:{validated_state}",
		extra_updates={
			"review_state": validated_state,
			"reviewed_by": frappe.session.user,
			"reviewed_on": now_datetime(),
			"resolution_notes": (resolution_notes or "")[:2000],
		},
	)
	return {"success": True, "event_name": event_name, "review_state": validated_state}


@frappe.whitelist(allow_guest=False)
def set_review_state_bulk(event_names: Any, review_state: str, resolution_notes: str | None = None) -> dict[str, Any]:
	_require_sync_event_access(write=True)
	names = _parse_json_arg(event_names, [])
	if isinstance(names, str):
		names = [names]
	if not isinstance(names, list):
		frappe.throw(_("event_names must be a list"))
	validated_state = _validate_review_state(review_state)
	results = []
	for event_name in names[:100]:
		results.append(set_review_state(str(event_name), validated_state, resolution_notes))
	return {"success": True, "count": len(results), "results": results}


@frappe.whitelist(allow_guest=False)
def set_retention_policy(event_name: str, retain: Any = 1, retain_days: Any = None) -> dict[str, Any]:
	_require_sync_event_access(write=True)
	if not event_name:
		frappe.throw(_("Missing event name"))
	should_retain = frappe.utils.cint(retain) == 1
	retain_until = None
	if should_retain and retain_days not in (None, ""):
		retain_until = now_datetime() + timedelta(days=_coerce_positive_int(retain_days, 30, max_value=3650))
	_mark_operator_action(
		event_name,
		"retention:enabled" if should_retain else "retention:cleared",
		extra_updates={
			"is_retention_exempt": 1 if should_retain else 0,
			"retain_until": retain_until,
		},
	)
	return {
		"success": True,
		"event_name": event_name,
		"is_retention_exempt": 1 if should_retain else 0,
		"retain_until": _datetime_to_text(retain_until),
	}


@frappe.whitelist(allow_guest=False)
def set_retention_policy_bulk(event_names: Any, retain: Any = 1, retain_days: Any = None) -> dict[str, Any]:
	_require_sync_event_access(write=True)
	names = _parse_json_arg(event_names, [])
	if isinstance(names, str):
		names = [names]
	if not isinstance(names, list):
		frappe.throw(_("event_names must be a list"))
	results = []
	for event_name in names[:100]:
		results.append(set_retention_policy(str(event_name), retain=retain, retain_days=retain_days))
	return {"success": True, "count": len(results), "results": results}


@frappe.whitelist(allow_guest=False)
def clear_outbound_breaker() -> dict[str, Any]:
	_require_sync_event_access(write=True)
	now = now_datetime()
	sync_events._clear_outbound_circuit_breaker()  # noqa: SLF001
	frappe.db.sql(
		"""
		UPDATE `tabWooCommerce Sync Event`
		SET next_attempt_at = %s,
		    last_error = ''
		WHERE direction = 'Outbound'
		  AND status IN ('Pending', 'RetryScheduled')
		  AND next_attempt_at > %s
		  AND last_error LIKE 'Paused by outbound circuit breaker%%'
		""",
		(now, now),
	)
	released_rows = 0
	try:
		count_rows = frappe.db.sql("SELECT ROW_COUNT() AS row_count", as_dict=True)
		if count_rows:
			released_rows = int(count_rows[0].get("row_count") or 0)
	except Exception:
		released_rows = 0
	frappe.db.commit()
	frappe.logger("jarz_woocommerce.sync_events").info({
		"event": "woo_sync_operations_clear_breaker",
		"user": frappe.session.user,
		"released_rows": released_rows,
	})
	return {"success": True, "released_rows": released_rows}