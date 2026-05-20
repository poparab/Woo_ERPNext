from __future__ import annotations

from typing import Any

import frappe
from frappe import _
from frappe.utils import add_days, getdate, nowdate


ATTENTION_STATUSES = ("Failed", "NeedsReview", "DeadLetter")


def execute(filters=None):
	filters = frappe._dict(filters or {})
	from_date, to_date = _resolve_dates(filters)
	conditions, params = _build_conditions(filters, from_date, to_date)
	where_clause = " AND ".join(conditions)

	columns = _get_columns()
	data = frappe.db.sql(
		f"""
		SELECT
			name,
			direction,
			event_type,
			status,
			review_state,
			object_type,
			source_id,
			woo_order_id,
			local_docname,
			attempt_count,
			max_attempts,
			first_seen_on,
			next_attempt_at,
			completed_on,
			manual_review_reason,
			last_error,
			is_retention_exempt
		FROM `tabWooCommerce Sync Event`
		WHERE {where_clause}
		ORDER BY first_seen_on DESC
		LIMIT 1000
		""",
		params,
		as_dict=True,
	)
	chart = _get_chart(where_clause, params)
	report_summary = _get_summary(where_clause, params)
	return columns, data, None, chart, report_summary


def _resolve_dates(filters: frappe._dict) -> tuple[Any, Any]:
	from_date = getdate(filters.get("from_date") or add_days(nowdate(), -7))
	to_date = getdate(filters.get("to_date") or nowdate())
	if from_date > to_date:
		frappe.throw(_("From Date cannot be after To Date"))
	return from_date, to_date


def _build_conditions(filters: frappe._dict, from_date, to_date) -> tuple[list[str], dict[str, Any]]:
	conditions = ["DATE(first_seen_on) BETWEEN %(from_date)s AND %(to_date)s"]
	params: dict[str, Any] = {
		"from_date": from_date,
		"to_date": to_date,
	}
	for fieldname in ("direction", "status", "review_state", "event_type", "object_type"):
		value = filters.get(fieldname)
		if value:
			conditions.append(f"{fieldname} = %({fieldname})s")
			params[fieldname] = value
	return conditions, params


def _get_columns() -> list[dict[str, Any]]:
	return [
		{"fieldname": "name", "label": _("Event"), "fieldtype": "Link", "options": "WooCommerce Sync Event", "width": 170},
		{"fieldname": "direction", "label": _("Direction"), "fieldtype": "Data", "width": 90},
		{"fieldname": "event_type", "label": _("Event Type"), "fieldtype": "Data", "width": 140},
		{"fieldname": "status", "label": _("Status"), "fieldtype": "Data", "width": 120},
		{"fieldname": "review_state", "label": _("Review State"), "fieldtype": "Data", "width": 120},
		{"fieldname": "object_type", "label": _("Object Type"), "fieldtype": "Data", "width": 110},
		{"fieldname": "source_id", "label": _("Source ID"), "fieldtype": "Data", "width": 140},
		{"fieldname": "woo_order_id", "label": _("Woo Order ID"), "fieldtype": "Int", "width": 100},
		{"fieldname": "local_docname", "label": _("Local Docname"), "fieldtype": "Data", "width": 150},
		{"fieldname": "attempt_count", "label": _("Attempts"), "fieldtype": "Int", "width": 80},
		{"fieldname": "max_attempts", "label": _("Max"), "fieldtype": "Int", "width": 60},
		{"fieldname": "first_seen_on", "label": _("First Seen"), "fieldtype": "Datetime", "width": 155},
		{"fieldname": "next_attempt_at", "label": _("Next Attempt"), "fieldtype": "Datetime", "width": 155},
		{"fieldname": "completed_on", "label": _("Completed"), "fieldtype": "Datetime", "width": 155},
		{"fieldname": "manual_review_reason", "label": _("Review Reason"), "fieldtype": "Data", "width": 180},
		{"fieldname": "last_error", "label": _("Last Error"), "fieldtype": "Small Text", "width": 260},
		{"fieldname": "is_retention_exempt", "label": _("Kept"), "fieldtype": "Check", "width": 55},
	]


def _get_chart(where_clause: str, params: dict[str, Any]) -> dict[str, Any] | None:
	rows = frappe.db.sql(
		f"""
		SELECT status, COUNT(*) AS count
		FROM `tabWooCommerce Sync Event`
		WHERE {where_clause}
		GROUP BY status
		ORDER BY count DESC
		""",
		params,
		as_dict=True,
	)
	if not rows:
		return None
	return {
		"data": {
			"labels": [row.get("status") for row in rows],
			"datasets": [{"name": _("Events"), "values": [int(row.get("count") or 0) for row in rows]}],
		},
		"type": "bar",
		"colors": ["#11686c"],
	}


def _get_summary(where_clause: str, params: dict[str, Any]) -> list[dict[str, Any]]:
	total = _count(where_clause, params)
	attention = _count(f"{where_clause} AND status IN ('Failed', 'NeedsReview', 'DeadLetter')", params)
	processing = _count(f"{where_clause} AND status = 'Processing'", params)
	retries = _count(f"{where_clause} AND status = 'RetryScheduled'", params)
	kept = _count(f"{where_clause} AND IFNULL(is_retention_exempt, 0) = 1", params)
	return [
		{"label": _("Total"), "value": total, "indicator": "blue"},
		{"label": _("Attention"), "value": attention, "indicator": "orange"},
		{"label": _("Processing"), "value": processing, "indicator": "blue"},
		{"label": _("Retry Scheduled"), "value": retries, "indicator": "orange"},
		{"label": _("Retention Overrides"), "value": kept, "indicator": "green"},
	]


def _count(where_clause: str, params: dict[str, Any]) -> int:
	rows = frappe.db.sql(
		f"SELECT COUNT(*) AS count FROM `tabWooCommerce Sync Event` WHERE {where_clause}",
		params,
		as_dict=True,
	)
	return int(rows[0].get("count") or 0) if rows else 0