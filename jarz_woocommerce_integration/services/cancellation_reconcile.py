from __future__ import annotations

import json
from time import perf_counter
from typing import Any

import frappe

from jarz_woocommerce_integration.services.order_sync import pull_single_order_phase1
from jarz_woocommerce_integration.utils.http_client import WooClient


TARGET_CANCELLATION_TYPES = {"WooCommerce Cancelled", "WooCommerce Refunded"}
DATA_CORRECTION_TYPE = "Data Correction"


def _get_client() -> tuple[Any, WooClient]:
    settings = frappe.get_single("WooCommerce Settings")
    client = WooClient(
        base_url=settings.base_url,
        consumer_key=settings.consumer_key,
        consumer_secret=settings.get_password("consumer_secret"),
    )
    return settings, client


def _resolve_order_map_link_field() -> str:
    try:
        cols = frappe.db.get_table_columns("WooCommerce Order Map") or []
    except Exception:
        cols = []

    if "erpnext_sales_invoice" in cols:
        return "erpnext_sales_invoice"
    if "sales_invoice" in cols:
        return "sales_invoice"
    return "erpnext_sales_invoice"


def _truncate_message(payload: Any, limit: int = 1000) -> str:
    if isinstance(payload, str):
        return payload[:limit]
    try:
        return json.dumps(payload, default=str)[:limit]
    except Exception:
        return str(payload)[:limit]


def _create_sync_log(operation: str, status: str, message: Any) -> Any | None:
    try:
        doc = frappe.get_doc(
            {
                "doctype": "WooCommerce Sync Log",
                "operation": operation,
                "status": status,
                "message": _truncate_message(message),
                "started_on": frappe.utils.now_datetime(),
            }
        )
        doc.insert(ignore_permissions=True)
        return doc
    except Exception:
        return None


def _finish_sync_log(log_doc: Any | None, status: str, message: Any, started_at: float, traceback_text: str = "") -> None:
    if not log_doc:
        return
    try:
        updates = {
            "status": status,
            "message": _truncate_message(message),
            "ended_on": frappe.utils.now_datetime(),
            "duration": round(perf_counter() - started_at, 3),
        }
        if traceback_text:
            updates["traceback"] = traceback_text[:2000]
        log_doc.db_set(updates, commit=True)
    except Exception:
        pass


def _fetch_woo_orders(
    statuses: str = "cancelled",
    per_page: int = 100,
    max_pages: int = 100,
) -> tuple[list[dict[str, Any]], int, int]:
    _, client = _get_client()

    params = {
        "status": statuses,
        "per_page": max(1, min(int(per_page), 100)),
        "orderby": "modified",
        "order": "desc",
        "page": 1,
    }

    orders, total_count, total_pages = client.list_orders_with_meta(params=params)
    collected: dict[int, dict[str, Any]] = {}

    for order in orders:
        if order.get("id"):
            collected[int(order["id"])] = order

    effective_total_pages = total_pages or 1
    last_page = min(effective_total_pages, max(1, int(max_pages or 1)))

    for page in range(2, last_page + 1):
        params["page"] = page
        page_orders, _, _ = client.list_orders_with_meta(params=params)
        if not page_orders:
            break
        for order in page_orders:
            if order.get("id"):
                collected[int(order["id"])] = order
        if len(page_orders) < params["per_page"]:
            break

    return list(collected.values()), total_count, total_pages


def _load_invoice_matches(woo_order_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    if not woo_order_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(woo_order_ids))
    link_field = _resolve_order_map_link_field()

    direct_rows = frappe.db.sql(
        f"""
        SELECT si.name AS invoice_name,
               si.woo_order_id AS woo_order_id,
               si.docstatus,
               IFNULL(si.custom_cancellation_type, '') AS cancellation_type,
               IFNULL(si.custom_cancellation_reason, '') AS cancellation_reason,
               si.modified
        FROM `tabSales Invoice` si
        WHERE si.woo_order_id IN ({placeholders})
        """,
        tuple(woo_order_ids),
        as_dict=True,
    )
    mapped_rows = frappe.db.sql(
        f"""
        SELECT si.name AS invoice_name,
               wm.woo_order_id AS woo_order_id,
               si.docstatus,
               IFNULL(si.custom_cancellation_type, '') AS cancellation_type,
               IFNULL(si.custom_cancellation_reason, '') AS cancellation_reason,
               si.modified
        FROM `tabWooCommerce Order Map` wm
        JOIN `tabSales Invoice` si ON si.name = wm.{link_field}
        WHERE wm.woo_order_id IN ({placeholders})
        """,
        tuple(woo_order_ids),
        as_dict=True,
    )

    matches: dict[int, dict[str, dict[str, Any]]] = {}
    for row in direct_rows + mapped_rows:
        try:
            woo_order_id = int(row.get("woo_order_id"))
        except Exception:
            continue
        invoice_name = row.get("invoice_name")
        if not invoice_name:
            continue
        bucket = matches.setdefault(woo_order_id, {})
        bucket[invoice_name] = row

    return {woo_order_id: list(rows.values()) for woo_order_id, rows in matches.items()}


def _classify_order(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "MISSING"
    if any(int(row.get("docstatus") or 0) in (0, 1) for row in rows):
        return "MATCH_ACTIVE"
    if any(
        int(row.get("docstatus") or 0) == 2 and row.get("cancellation_type") in TARGET_CANCELLATION_TYPES
        for row in rows
    ):
        return "MATCH_CANCELLED"
    if any(
        int(row.get("docstatus") or 0) == 2 and row.get("cancellation_type") == DATA_CORRECTION_TYPE
        for row in rows
    ):
        return "MATCH_DATA_CORR"
    if any(int(row.get("docstatus") or 0) == 2 for row in rows):
        return "MATCH_OTHER_CANCELLED"
    return "MISSING"


def _sample_data_correction_invoices(limit: int = 20) -> list[dict[str, Any]]:
    limit = max(1, int(limit or 20))
    link_field = _resolve_order_map_link_field()
    rows = frappe.db.sql(
        f"""
        SELECT si.name,
               COALESCE(NULLIF(si.woo_order_id, ''), wm.woo_order_id) AS resolved_woo_order_id,
               si.customer,
               si.posting_date,
               si.grand_total,
               IFNULL(si.custom_cancellation_reason, '') AS cancellation_reason
        FROM `tabSales Invoice` si
        LEFT JOIN `tabWooCommerce Order Map` wm ON wm.{link_field} = si.name
        WHERE si.docstatus = 2
          AND IFNULL(si.custom_cancellation_type, '') = %s
        ORDER BY si.modified DESC
        LIMIT %s
        """,
        (DATA_CORRECTION_TYPE, limit),
        as_dict=True,
    )
    if not rows:
        return []

    invoice_names = [row["name"] for row in rows]
    placeholders = ", ".join(["%s"] * len(invoice_names))
    comment_rows = frappe.db.sql(
        f"""
        SELECT reference_name, content
        FROM `tabComment`
        WHERE reference_doctype = 'Sales Invoice'
          AND comment_type = 'Comment'
          AND reference_name IN ({placeholders})
        ORDER BY creation ASC
        """,
        tuple(invoice_names),
        as_dict=True,
    )

    comment_map: dict[str, str] = {}
    for row in comment_rows:
        if row["reference_name"] not in comment_map:
            comment_map[row["reference_name"]] = row.get("content") or ""

    for row in rows:
        row["first_comment"] = comment_map.get(row["name"], "")[:500]

    return rows


def diagnose_cancelled_orders(
    sample_limit: int = 20,
    statuses: str = "cancelled",
    per_page: int = 100,
    max_pages: int = 100,
    log_result: bool = True,
) -> dict[str, Any]:
    started_at = perf_counter()
    log_doc = _create_sync_log("Reconcile-Diagnostic", "Started", {"statuses": statuses}) if log_result else None

    try:
        erp_counts = frappe.db.sql(
            """
            SELECT IFNULL(custom_cancellation_type, '') AS cancellation_type,
                   COUNT(*) AS count
            FROM `tabSales Invoice`
            WHERE docstatus = 2
            GROUP BY IFNULL(custom_cancellation_type, '')
            ORDER BY count DESC, cancellation_type ASC
            """,
            as_dict=True,
        )

        woo_orders, woo_total_count, woo_total_pages = _fetch_woo_orders(
            statuses=statuses,
            per_page=per_page,
            max_pages=max_pages,
        )
        woo_order_ids = sorted(int(order["id"]) for order in woo_orders if order.get("id"))
        invoice_matches = _load_invoice_matches(woo_order_ids)

        match_counts = {
            "MATCH_CANCELLED": 0,
            "MATCH_DATA_CORR": 0,
            "MATCH_OTHER_CANCELLED": 0,
            "MATCH_ACTIVE": 0,
            "MISSING": 0,
        }
        sample_ids = {key: [] for key in match_counts}

        for woo_order_id in woo_order_ids:
            bucket = _classify_order(invoice_matches.get(woo_order_id, []))
            match_counts[bucket] = match_counts.get(bucket, 0) + 1
            if len(sample_ids[bucket]) < 20:
                sample_ids[bucket].append(woo_order_id)

        result = {
            "statuses": statuses,
            "woo_cancelled_orders": len(woo_order_ids),
            "woo_total_reported": woo_total_count,
            "woo_total_pages": woo_total_pages,
            "erp_cancelled_counts": erp_counts,
            "match_counts": match_counts,
            "sample_woo_ids": sample_ids,
            "data_correction_samples": _sample_data_correction_invoices(limit=sample_limit),
        }

        _finish_sync_log(log_doc, "Success", result, started_at)
        return result
    except Exception:
        traceback_text = frappe.get_traceback()
        _finish_sync_log(log_doc, "Failed", "Exception", started_at, traceback_text=traceback_text)
        raise


def reconcile_cancelled_orders(
    dry_run: bool = True,
    statuses: str = "cancelled",
    per_page: int = 100,
    max_pages: int = 100,
    create_missing: bool = True,
) -> dict[str, Any]:
    started_at = perf_counter()
    log_doc = _create_sync_log(
        "Reconcile-Backfill",
        "Started",
        {
            "dry_run": dry_run,
            "statuses": statuses,
            "create_missing": create_missing,
        },
    )

    try:
        woo_orders, woo_total_count, woo_total_pages = _fetch_woo_orders(
            statuses=statuses,
            per_page=per_page,
            max_pages=max_pages,
        )
        woo_by_id = {
            int(order["id"]): order
            for order in woo_orders
            if order.get("id")
        }
        invoice_matches = _load_invoice_matches(list(woo_by_id.keys()))

        stats = {
            "dry_run": dry_run,
            "statuses": statuses,
            "create_missing": create_missing,
            "woo_total_reported": woo_total_count,
            "woo_total_pages": woo_total_pages,
            "processed": 0,
            "already_aligned": 0,
            "reclassified": 0,
            "reprocessed_active": 0,
            "reprocessed_missing": 0,
            "errors": 0,
            "samples": [],
        }

        for woo_order_id, order in woo_by_id.items():
            rows = invoice_matches.get(woo_order_id, [])
            bucket = _classify_order(rows)
            stats["processed"] += 1

            if bucket == "MATCH_CANCELLED":
                stats["already_aligned"] += 1
                continue

            if bucket in ("MATCH_DATA_CORR", "MATCH_OTHER_CANCELLED"):
                cancelled_row = next(
                    (row for row in rows if int(row.get("docstatus") or 0) == 2),
                    rows[0] if rows else None,
                )
                if not cancelled_row:
                    stats["errors"] += 1
                    continue

                updates = {
                    "custom_cancellation_type": "WooCommerce Refunded"
                    if order.get("status") == "refunded"
                    else "WooCommerce Cancelled",
                    "custom_cancellation_reason": (
                        f"Reclassified: Order {order.get('status') or statuses} on WooCommerce "
                        f"(Order #{woo_order_id})"
                    ),
                    "custom_sales_invoice_state": "Cancelled",
                    "custom_acceptance_status": "Accepted",
                    "woo_order_id": woo_order_id,
                }

                if dry_run:
                    if len(stats["samples"]) < 20:
                        stats["samples"].append(
                            {
                                "woo_order_id": woo_order_id,
                                "invoice": cancelled_row["invoice_name"],
                                "action": "reclassify",
                                "from_type": cancelled_row.get("cancellation_type") or "",
                            }
                        )
                else:
                    frappe.db.set_value(
                        "Sales Invoice",
                        cancelled_row["invoice_name"],
                        updates,
                        update_modified=False,
                    )
                    frappe.db.commit()

                stats["reclassified"] += 1
                continue

            if bucket == "MATCH_ACTIVE" or (bucket == "MISSING" and create_missing):
                if dry_run:
                    result = {"success": True, "status": "dry_run", "woo_order_id": woo_order_id}
                else:
                    try:
                        result = pull_single_order_phase1(
                            order_id=woo_order_id,
                            dry_run=False,
                            force=False,
                            allow_update=True,
                        )
                        if result.get("success"):
                            frappe.db.commit()
                    except Exception:
                        try:
                            frappe.db.rollback()
                        except Exception:
                            pass
                        result = {
                            "success": False,
                            "reason": frappe.get_traceback()[:500],
                            "woo_order_id": woo_order_id,
                        }

                if result.get("success"):
                    if bucket == "MATCH_ACTIVE":
                        stats["reprocessed_active"] += 1
                    else:
                        stats["reprocessed_missing"] += 1
                    if len(stats["samples"]) < 20:
                        stats["samples"].append(
                            {
                                "woo_order_id": woo_order_id,
                                "action": "reprocess",
                                "bucket": bucket,
                                "result": result.get("status"),
                            }
                        )
                else:
                    stats["errors"] += 1
                    if len(stats["samples"]) < 20:
                        stats["samples"].append(
                            {
                                "woo_order_id": woo_order_id,
                                "action": "reprocess",
                                "bucket": bucket,
                                "error": result.get("reason") or "unknown",
                            }
                        )
                continue

            if bucket == "MISSING":
                if len(stats["samples"]) < 20:
                    stats["samples"].append(
                        {
                            "woo_order_id": woo_order_id,
                            "action": "missing_skipped",
                            "reason": "create_missing_disabled",
                        }
                    )

        stats["post_diagnostic"] = diagnose_cancelled_orders(
            sample_limit=10,
            statuses=statuses,
            per_page=per_page,
            max_pages=max_pages,
            log_result=False,
        )
        _finish_sync_log(log_doc, "Success", stats, started_at)
        return stats
    except Exception:
        traceback_text = frappe.get_traceback()
        frappe.db.rollback()
        _finish_sync_log(log_doc, "Failed", "Exception", started_at, traceback_text=traceback_text)
        raise
