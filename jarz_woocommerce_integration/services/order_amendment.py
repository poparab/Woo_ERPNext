"""Woo-driven Sales Invoice amendment orchestrator.

Triggered by a background job enqueued from order_sync.process_order_phase1 when
an inbound Woo order update has a different hash from the stored one AND
the invoice is already submitted.

Flow (mirrors _run_invoice_amendment_job in jarz_pos.api.manager, but Woo-native):

    1.  Advisory lock  woo_amend:<woo_order_id>   – prevents duplicate amendment jobs.
    2.  Look up source Sales Invoice via WooCommerce Order Map.
    3.  Advisory lock  inv:<source_si_name>        – shared with the POS path.
    4.  Re-fetch invoice (state may have changed since webhook fired).
    5.  OFD permanent hard-block.
    6.  get_invoice_amendment_eligibility()        – reuse jarz_pos eligibility check.
    7.  Woo status guard (processing / on-hold only).
    8.  Amend-depth cap  (≤ 3 cancelled predecessors for same woo_order_id).
    9.  Period-close guard.
    10. Idempotency:  if replacement already exists with matching hash → skip.
    11. DB savepoint.
    12. Suppress outbound Woo push.
    13. Cancel Payment Entries (simple only).
    14. Cancel source Sales Invoice.
    15. process_order_phase1(amended_from=source.name) → creates replacement.
    16. Relink WooCommerce Order Map + store new hash.
    17. _mark_source_invoice_as_amended.
    18. Audit comments on both invoices.
    19. WooCommerce Sync Log  operation="WooAmendment"  status="Success".
    20. Release advisory locks in finally.
"""
from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

import frappe
from frappe import _

if TYPE_CHECKING:
    pass


_MONEY_TOLERANCE = 0.01


def _coerce_float(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _amounts_match(left: Any, right: Any, *, tolerance: float = _MONEY_TOLERANCE) -> bool:
    return abs(_coerce_float(left) - _coerce_float(right)) <= tolerance


def _find_submitted_payment_entries(invoice_name: str) -> list[str]:
    ref_rows = frappe.get_all(
        "Payment Entry Reference",
        filters={
            "reference_doctype": "Sales Invoice",
            "reference_name": invoice_name,
            "parenttype": "Payment Entry",
        },
        pluck="parent",
        limit_page_length=20,
    ) or []
    payment_entry_names = sorted({row for row in ref_rows if row})
    if not payment_entry_names:
        return []

    submitted = frappe.get_all(
        "Payment Entry",
        filters={"name": ["in", payment_entry_names], "docstatus": 1},
        pluck="name",
        limit_page_length=20,
    ) or []
    return sorted({row for row in submitted if row})


def _get_payment_entry_details(
    invoice_name: str,
    payment_entry_names: list[str] | None = None,
) -> list[dict[str, Any]]:
    payment_entry_names = payment_entry_names or _find_submitted_payment_entries(invoice_name)
    if not payment_entry_names:
        return []

    payment_rows = frappe.get_all(
        "Payment Entry",
        filters={"name": ["in", payment_entry_names], "docstatus": 1},
        fields=[
            "name",
            "paid_amount",
            "received_amount",
            "unallocated_amount",
            "clearance_date",
            "reference_no",
            "reference_date",
            "paid_to",
        ],
        limit_page_length=20,
    ) or []
    if not payment_rows:
        return []

    allocation_rows = frappe.get_all(
        "Payment Entry Reference",
        filters={
            "parent": ["in", payment_entry_names],
            "reference_doctype": "Sales Invoice",
            "reference_name": invoice_name,
            "parenttype": "Payment Entry",
        },
        fields=["parent", "allocated_amount"],
        limit_page_length=40,
    ) or []
    allocated_by_parent: dict[str, float] = {}
    for row in allocation_rows:
        parent = str(row.get("parent") or "").strip()
        if not parent:
            continue
        allocated_by_parent[parent] = allocated_by_parent.get(parent, 0.0) + _coerce_float(
            row.get("allocated_amount")
        )

    details: list[dict[str, Any]] = []
    for row in payment_rows:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        payload = dict(row)
        payload["allocated_amount"] = allocated_by_parent.get(name, 0.0)
        details.append(payload)

    return details


def _evaluate_paid_amendment(source_si: Any, order_payload: dict, settings: Any) -> dict[str, Any]:
    from jarz_woocommerce_integration.services.order_sync import _payment_entries_are_simple_for_invoice

    invoice_name = str(source_si.get("name") or getattr(source_si, "name", "") or "").strip()
    payment_entries = _find_submitted_payment_entries(invoice_name)
    payment_entry_details = _get_payment_entry_details(invoice_name, payment_entries)
    outstanding_amount = _coerce_float(source_si.get("outstanding_amount") or 0)
    invoice_total = _coerce_float(
        source_si.get("grand_total")
        or source_si.get("rounded_total")
        or source_si.get("base_grand_total")
        or 0
    )
    incoming_total = _coerce_float(order_payload.get("total") or 0)
    allocated_total = sum(_coerce_float(row.get("allocated_amount")) for row in payment_entry_details)
    requires_paid_lane = bool(payment_entries) or _amounts_match(outstanding_amount, 0)
    paid_flag_enabled = bool(int(getattr(settings, "enable_pre_ofd_paid_amendment", 0) or 0))

    context = {
        "requires_paid_lane": requires_paid_lane,
        "paid_flag_enabled": paid_flag_enabled,
        "can_auto_amend": True,
        "block_code": None,
        "block_reason": None,
        "payment_entries": payment_entries,
        "payment_entry_details": payment_entry_details,
        "allocated_total": allocated_total,
        "invoice_total": invoice_total,
        "incoming_total": incoming_total,
        "outstanding_amount": outstanding_amount,
    }
    if not requires_paid_lane:
        return context

    if not paid_flag_enabled:
        context["can_auto_amend"] = False
        context["block_code"] = "paid_amendment_disabled"
        context["block_reason"] = (
            "paid invoice amendment requires enable_pre_ofd_paid_amendment=1"
        )
        return context

    if not payment_entries:
        context["can_auto_amend"] = False
        context["block_code"] = "paid_amendment_payment_artifact_missing"
        context["block_reason"] = (
            "invoice appears paid but has no submitted Payment Entry to migrate"
        )
        return context

    if not _payment_entries_are_simple_for_invoice(invoice_name, payment_entries):
        context["can_auto_amend"] = False
        context["block_code"] = "paid_amendment_non_simple_payment"
        context["block_reason"] = (
            "linked Payment Entry references multiple invoices; manual review required"
        )
        return context

    if not _amounts_match(outstanding_amount, 0):
        context["can_auto_amend"] = False
        context["block_code"] = "paid_amendment_not_fully_paid"
        context["block_reason"] = "invoice is not fully paid"
        return context

    if any(_coerce_float(row.get("unallocated_amount")) > _MONEY_TOLERANCE for row in payment_entry_details):
        context["can_auto_amend"] = False
        context["block_code"] = "paid_amendment_unallocated_payment"
        context["block_reason"] = (
            "linked Payment Entry has unallocated amount; manual review required"
        )
        return context

    if any(str(row.get("clearance_date") or "").strip() for row in payment_entry_details):
        context["can_auto_amend"] = False
        context["block_code"] = "paid_amendment_reconciled_payment"
        context["block_reason"] = (
            "linked Payment Entry has a clearance date; manual review required"
        )
        return context

    if not _amounts_match(allocated_total, invoice_total):
        context["can_auto_amend"] = False
        context["block_code"] = "paid_amendment_payment_mismatch"
        context["block_reason"] = (
            "linked Payment Entry total does not match the paid invoice total"
        )
        return context

    if not _amounts_match(invoice_total, incoming_total):
        context["can_auto_amend"] = False
        context["block_code"] = "paid_amendment_amount_delta"
        context["block_reason"] = (
            "incoming Woo order total differs from the already-paid invoice total"
        )
        return context

    return context


def _verify_paid_replacement(
    source_si: Any,
    replacement_si: Any,
    order_payload: dict,
    paid_context: dict[str, Any],
) -> dict[str, Any]:
    from jarz_woocommerce_integration.services.order_sync import _payment_entries_are_simple_for_invoice

    replacement_name = str(replacement_si.get("name") or getattr(replacement_si, "name", "") or "").strip()
    replacement_payment_entries = _find_submitted_payment_entries(replacement_name)
    replacement_payment_details = _get_payment_entry_details(replacement_name, replacement_payment_entries)
    replacement_allocated_total = sum(
        _coerce_float(row.get("allocated_amount")) for row in replacement_payment_details
    )

    if int(replacement_si.get("docstatus") or 0) != 1:
        return {"ok": False, "reason": "replacement_invoice_not_submitted"}
    if str(replacement_si.get("amended_from") or "").strip() != str(source_si.name):
        return {"ok": False, "reason": "replacement_amended_from_mismatch"}
    if str(replacement_si.get("woo_order_id") or "").strip() != str(order_payload.get("id") or "").strip():
        return {"ok": False, "reason": "replacement_woo_order_id_mismatch"}
    if not _amounts_match(replacement_si.get("grand_total") or 0, paid_context.get("incoming_total") or 0):
        return {"ok": False, "reason": "replacement_total_mismatch"}
    if not _amounts_match(replacement_si.get("outstanding_amount") or 0, 0):
        return {"ok": False, "reason": "replacement_not_fully_paid"}
    if not replacement_payment_entries:
        return {"ok": False, "reason": "replacement_payment_missing"}
    if not _payment_entries_are_simple_for_invoice(replacement_name, replacement_payment_entries):
        return {"ok": False, "reason": "replacement_payment_not_simple"}
    if not _amounts_match(replacement_allocated_total, paid_context.get("allocated_total") or 0):
        return {"ok": False, "reason": "replacement_payment_total_mismatch"}

    return {
        "ok": True,
        "replacement_payment_entries": replacement_payment_entries,
        "replacement_payment_details": replacement_payment_details,
        "replacement_allocated_total": replacement_allocated_total,
    }

# ---------------------------------------------------------------------------
# Public entry point (called via frappe.enqueue)
# ---------------------------------------------------------------------------

def run_woo_amendment_job(
    woo_order_id: int,
    order_payload: dict,
    settings_name: str = "WooCommerce Settings",
) -> dict:
    """Orchestrate a Woo-driven cancel-and-recreate amendment of a Sales Invoice.

    This function is designed to be called as a queued background job.  It is safe
    to retry — idempotency checks prevent double-amending the same order.

    Args:
        woo_order_id:   WooCommerce order ID (integer).
        order_payload:  Full Woo order dict (as received from webhook or poller).
        settings_name:  Frappe document name for WooCommerce Settings (default singleton name).

    Returns:
        dict with ``status`` key:
          - ``"success"``        – amendment completed.
          - ``"skipped"``        – amendment not needed / already done / blocked.
          - ``"error"``          – unexpected failure (details in ``reason``).
    """
    woo_order_id = int(woo_order_id)
    lock_key_order = f"woo_amend:{woo_order_id}"
    order_lock_acquired = False
    inv_lock_key: str | None = None
    inv_lock_acquired = False

    try:
        # ── 1. Advisory lock on the order ────────────────────────────────────
        result = frappe.db.sql("SELECT GET_LOCK(%s, 5)", (lock_key_order,))
        order_lock_acquired = bool(result and result[0] and result[0][0] == 1)
        if not order_lock_acquired:
            return {"status": "skipped", "reason": "locked", "woo_order_id": woo_order_id}

        settings = frappe.get_single(settings_name)
        if not bool(int(getattr(settings, "enable_inbound_amendment", 0) or 0)):
            _write_sync_log(
                "WooAmendment",
                "Skipped",
                "auto amendment disabled by WooCommerce Settings",
                woo_order_id,
            )
            return {
                "status": "skipped",
                "reason": "auto_amendment_disabled",
                "woo_order_id": woo_order_id,
            }

        # ── 2. Locate source Sales Invoice via Order Map ──────────────────────
        LINK_FIELD = _resolve_link_field()
        map_row = frappe.db.get_value(
            "WooCommerce Order Map",
            {"woo_order_id": woo_order_id},
            ["name", LINK_FIELD, "hash"],
            as_dict=True,
        )
        if not map_row or not map_row.get(LINK_FIELD):
            return {
                "status": "skipped",
                "reason": "no_order_map",
                "woo_order_id": woo_order_id,
            }

        source_si_name = map_row[LINK_FIELD]

        # Cross-check: the mapped invoice must still exist and not be cancelled.
        try:
            source_si = frappe.get_doc("Sales Invoice", source_si_name)
        except frappe.DoesNotExistError:
            return {
                "status": "skipped",
                "reason": "source_invoice_not_found",
                "woo_order_id": woo_order_id,
                "invoice": source_si_name,
            }

        if int(source_si.docstatus or 0) != 1:
            return {
                "status": "skipped",
                "reason": "source_invoice_not_submitted",
                "woo_order_id": woo_order_id,
                "invoice": source_si_name,
            }

        # ── 3. Advisory lock on the invoice (shared with POS path) ───────────
        inv_lock_key = f"inv:{source_si_name}"
        result2 = frappe.db.sql("SELECT GET_LOCK(%s, 5)", (inv_lock_key,))
        inv_lock_acquired = bool(result2 and result2[0] and result2[0][0] == 1)
        if not inv_lock_acquired:
            return {
                "status": "skipped",
                "reason": "invoice_locked",
                "woo_order_id": woo_order_id,
                "invoice": source_si_name,
            }

        # ── 4. Re-fetch invoice (state may have changed) ──────────────────────
        source_si = frappe.get_doc("Sales Invoice", source_si_name)
        if int(source_si.docstatus or 0) != 1:
            return {
                "status": "skipped",
                "reason": "source_invoice_not_submitted_after_recheck",
                "woo_order_id": woo_order_id,
                "invoice": source_si_name,
            }

        # ── 5. OFD permanent hard-block ───────────────────────────────────────
        inv_state = str(source_si.get("custom_sales_invoice_state") or "").strip()
        was_ofd = bool(int(source_si.get("custom_was_out_for_delivery") or 0))
        if was_ofd or inv_state == "Out for Delivery":
            _write_sync_log(
                "WooAmendment",
                "Blocked",
                f"out_for_delivery_locked: {source_si_name} cannot be amended "
                f"(was_ofd={was_ofd}, state={inv_state!r})",
                woo_order_id,
            )
            return {
                "status": "skipped",
                "reason": "out_for_delivery_locked",
                "woo_order_id": woo_order_id,
                "invoice": source_si_name,
            }

        # ── 6. Amendment eligibility (jarz_pos guard) ─────────────────────────
        from jarz_pos.api.manager import get_invoice_amendment_eligibility
        eligibility = get_invoice_amendment_eligibility(source_si)
        if not eligibility.get("can_amend"):
            _flag_needs_review(
                woo_order_id=woo_order_id,
                invoice_name=source_si_name,
                reason=(
                    f"eligibility_blocked: {eligibility.get('amendment_block_code')} — "
                    f"{eligibility.get('amendment_block_reason')}"
                ),
            )
            _write_sync_log(
                "WooAmendment",
                "Blocked",
                f"eligibility_blocked on {source_si_name}: "
                f"{eligibility.get('amendment_block_code')} — "
                f"{eligibility.get('amendment_block_reason')}",
                woo_order_id,
            )
            return {
                "status": "skipped",
                "reason": "eligibility_blocked",
                "block_code": eligibility.get("amendment_block_code"),
                "woo_order_id": woo_order_id,
                "invoice": source_si_name,
            }

        # ── 7. Woo status guard ───────────────────────────────────────────────
        woo_status = str(order_payload.get("status") or "").lower()
        allowed_statuses = {"processing", "on-hold"}
        if woo_status not in allowed_statuses:
            _flag_needs_review(
                woo_order_id=woo_order_id,
                invoice_name=source_si_name,
                reason=(
                    f"woo_status={woo_status!r} not in {sorted(allowed_statuses)}; "
                    "manual review required"
                ),
            )
            _write_sync_log(
                "WooAmendment",
                "Blocked",
                f"woo_status={woo_status!r} not eligible for amendment on {source_si_name}",
                woo_order_id,
            )
            return {
                "status": "skipped",
                "reason": "woo_status_not_eligible",
                "woo_status": woo_status,
                "woo_order_id": woo_order_id,
                "invoice": source_si_name,
            }

        # ── 8. Amend-depth cap (≤ 3) ─────────────────────────────────────────
        amend_depth = frappe.db.count(
            "Sales Invoice",
            {"woo_order_id": woo_order_id, "docstatus": 2},
        )
        if amend_depth >= 3:
            _flag_needs_review(
                woo_order_id=woo_order_id,
                invoice_name=source_si_name,
                reason=f"amend_depth_exceeded: {amend_depth} cancelled invoices already exist",
            )
            _write_sync_log(
                "WooAmendment",
                "Blocked",
                f"amend_depth_exceeded ({amend_depth}) for woo_order_id={woo_order_id}",
                woo_order_id,
            )
            return {
                "status": "skipped",
                "reason": "amend_depth_exceeded",
                "depth": amend_depth,
                "woo_order_id": woo_order_id,
                "invoice": source_si_name,
            }

        # ── 9. Period-close guard ─────────────────────────────────────────────
        period_block = _check_period_closed(source_si)
        if period_block:
            _flag_needs_review(
                woo_order_id=woo_order_id,
                invoice_name=source_si_name,
                reason=f"period_closed: {period_block}",
            )
            _write_sync_log(
                "WooAmendment",
                "Blocked",
                f"period_closed on {source_si_name}: {period_block}",
                woo_order_id,
            )
            return {
                "status": "skipped",
                "reason": "period_closed",
                "detail": period_block,
                "woo_order_id": woo_order_id,
                "invoice": source_si_name,
            }

        # ── 10. Idempotency: check if replacement already exists ──────────────
        from jarz_woocommerce_integration.services.order_sync import _compute_order_hash
        new_hash = _compute_order_hash(order_payload)
        existing_replacement = _find_existing_replacement(source_si_name, new_hash)
        if existing_replacement:
            return {
                "status": "skipped",
                "reason": "already_amended",
                "replacement_invoice": existing_replacement,
                "woo_order_id": woo_order_id,
            }

        paid_amendment = _evaluate_paid_amendment(source_si, order_payload, settings)
        if paid_amendment.get("requires_paid_lane") and not paid_amendment.get("can_auto_amend"):
            _flag_needs_review(
                woo_order_id=woo_order_id,
                invoice_name=source_si_name,
                reason=(
                    f"{paid_amendment.get('block_code')}: "
                    f"{paid_amendment.get('block_reason')}"
                ),
            )
            _write_sync_log(
                "WooAmendment",
                "Blocked",
                {
                    "invoice": source_si_name,
                    "block_code": paid_amendment.get("block_code"),
                    "block_reason": paid_amendment.get("block_reason"),
                    "payment_entries": paid_amendment.get("payment_entries") or [],
                    "allocated_total": paid_amendment.get("allocated_total"),
                    "invoice_total": paid_amendment.get("invoice_total"),
                    "incoming_total": paid_amendment.get("incoming_total"),
                },
                woo_order_id,
            )
            return {
                "status": "skipped",
                "reason": paid_amendment.get("block_code"),
                "woo_order_id": woo_order_id,
                "invoice": source_si_name,
            }

        # ── 11-19. Execute amendment ──────────────────────────────────────────
        request_id = _build_request_id(woo_order_id, new_hash)
        save_point = _build_savepoint_name(request_id)
        prev_outbound_flag = getattr(frappe.flags, "ignore_woo_outbound", False)

        try:
            frappe.db.savepoint(save_point)
            frappe.flags.ignore_woo_outbound = True

            source_si = frappe.get_doc("Sales Invoice", source_si_name)
            inv_state = str(source_si.get("custom_sales_invoice_state") or "").strip()
            was_ofd = bool(int(source_si.get("custom_was_out_for_delivery") or 0))
            if was_ofd or inv_state == "Out for Delivery":
                frappe.db.rollback(save_point=save_point)
                _write_sync_log(
                    "WooAmendment",
                    "Blocked",
                    f"out_for_delivery_locked after recheck: {source_si_name}",
                    woo_order_id,
                )
                return {
                    "status": "skipped",
                    "reason": "out_for_delivery_locked",
                    "woo_order_id": woo_order_id,
                    "invoice": source_si_name,
                }

            eligibility = get_invoice_amendment_eligibility(source_si)
            if not eligibility.get("can_amend"):
                frappe.db.rollback(save_point=save_point)
                _flag_needs_review(
                    woo_order_id=woo_order_id,
                    invoice_name=source_si_name,
                    reason=(
                        f"eligibility_blocked: {eligibility.get('amendment_block_code')} — "
                        f"{eligibility.get('amendment_block_reason')}"
                    ),
                )
                _write_sync_log(
                    "WooAmendment",
                    "Blocked",
                    f"eligibility_blocked after recheck on {source_si_name}: "
                    f"{eligibility.get('amendment_block_code')} — "
                    f"{eligibility.get('amendment_block_reason')}",
                    woo_order_id,
                )
                return {
                    "status": "skipped",
                    "reason": "eligibility_blocked",
                    "block_code": eligibility.get("amendment_block_code"),
                    "woo_order_id": woo_order_id,
                    "invoice": source_si_name,
                }

            paid_amendment = _evaluate_paid_amendment(source_si, order_payload, settings)
            if paid_amendment.get("requires_paid_lane") and not paid_amendment.get("can_auto_amend"):
                frappe.db.rollback(save_point=save_point)
                _flag_needs_review(
                    woo_order_id=woo_order_id,
                    invoice_name=source_si_name,
                    reason=(
                        f"{paid_amendment.get('block_code')}: "
                        f"{paid_amendment.get('block_reason')}"
                    ),
                )
                _write_sync_log(
                    "WooAmendment",
                    "Blocked",
                    {
                        "invoice": source_si_name,
                        "block_code": paid_amendment.get("block_code"),
                        "block_reason": paid_amendment.get("block_reason"),
                        "payment_entries": paid_amendment.get("payment_entries") or [],
                    },
                    woo_order_id,
                )
                return {
                    "status": "skipped",
                    "reason": paid_amendment.get("block_code"),
                    "woo_order_id": woo_order_id,
                    "invoice": source_si_name,
                }

            payment_entries = paid_amendment.get("payment_entries") or []

            # ── 13. Cancel Payment Entries ────────────────────────────────────
            cancelled_payment_entries = []
            for pe_name in payment_entries:
                pe = frappe.get_doc("Payment Entry", pe_name)
                if int(pe.docstatus or 0) != 1:
                    continue
                pe.flags.ignore_permissions = True
                pe.cancel()
                cancelled_payment_entries.append(pe_name)

            # ── 14. Cancel source Sales Invoice ───────────────────────────────
            source_si.flags.ignore_permissions = True
            source_si.flags.ignore_woo_outbound = True
            source_si.cancel()

            # ── 15. Recreate via process_order_phase1 ─────────────────────────
            from jarz_woocommerce_integration.services.order_sync import process_order_phase1

            creation_result = process_order_phase1(
                order_payload,
                settings,
                allow_update=True,
                amended_from=source_si_name,
            )

            replacement_si_name = (
                creation_result.get("invoice")
                or creation_result.get("invoice_name")
                or creation_result.get("name")
                or ""
            )
            if not replacement_si_name:
                raise RuntimeError(
                    f"process_order_phase1 did not return a replacement invoice name. "
                    f"result={creation_result!r}"
                )

            replacement_si = frappe.get_doc("Sales Invoice", replacement_si_name)
            replacement_payment_entries: list[str] = []
            if paid_amendment.get("requires_paid_lane"):
                replacement_check = _verify_paid_replacement(
                    source_si,
                    replacement_si,
                    order_payload,
                    paid_amendment,
                )
                if not replacement_check.get("ok"):
                    raise RuntimeError(
                        f"paid_replacement_verification_failed: {replacement_check.get('reason')}"
                    )
                replacement_payment_entries = replacement_check.get("replacement_payment_entries") or []

            # ── 16. Relink Order Map ──────────────────────────────────────────
            frappe.db.set_value(
                "WooCommerce Order Map",
                map_row["name"],
                {
                    LINK_FIELD: replacement_si_name,
                    "hash": new_hash,
                    "synced_on": frappe.utils.now_datetime(),
                    "needs_manual_review": 0,
                    "manual_review_reason": None,
                    "manual_review_logged_on": None,
                },
                update_modified=True,
            )

            # ── 17. Mark source invoice as amended ────────────────────────────
            from jarz_pos.api.manager import _mark_source_invoice_as_amended

            _mark_source_invoice_as_amended(
                source_si_name,
                replacement_invoice_name=replacement_si_name,
                request_id=request_id,
                initiated_by="WooCommerce Webhook",
            )

            # ── 18. Audit comments ────────────────────────────────────────────
            from jarz_pos.api.manager import _add_invoice_audit_comment

            _add_invoice_audit_comment(
                source_si_name,
                (
                    f"Superseded by WooCommerce item-edit amendment. "
                    f"Replacement: {replacement_si_name}. "
                    f"Woo Order ID: {woo_order_id}. Request ID: {request_id}."
                    f" Cancelled Payment Entries: {cancelled_payment_entries or []}."
                ),
            )
            _add_invoice_audit_comment(
                replacement_si_name,
                (
                    f"Created as amendment of {source_si_name} via WooCommerce item-edit. "
                    f"Woo Order ID: {woo_order_id}. Request ID: {request_id}."
                    f" Replacement Payment Entries: {replacement_payment_entries or []}."
                ),
            )

            # ── 19. Success sync log ──────────────────────────────────────────
            _write_sync_log(
                "WooAmendment",
                "Success",
                {
                    "source_invoice": source_si_name,
                    "replacement_invoice": replacement_si_name,
                    "request_id": request_id,
                    "paid_amendment": bool(paid_amendment.get("requires_paid_lane")),
                    "cancelled_payment_entries": cancelled_payment_entries,
                    "replacement_payment_entries": replacement_payment_entries,
                    "source_paid_total": paid_amendment.get("allocated_total"),
                    "incoming_total": paid_amendment.get("incoming_total"),
                    "amount_delta": _coerce_float(paid_amendment.get("incoming_total")) - _coerce_float(paid_amendment.get("invoice_total")),
                    "new_hash": new_hash,
                },
                woo_order_id,
            )

            return {
                "status": "success",
                "request_id": request_id,
                "source_invoice": source_si_name,
                "replacement_invoice": replacement_si_name,
                "paid_amendment": bool(paid_amendment.get("requires_paid_lane")),
                "cancelled_payment_entries": cancelled_payment_entries,
                "replacement_payment_entries": replacement_payment_entries,
                "woo_order_id": woo_order_id,
            }

        except Exception as exc:
            frappe.db.rollback(save_point=save_point)
            _flag_needs_review(
                woo_order_id=woo_order_id,
                invoice_name=source_si_name,
                reason=f"amendment_error: {exc}",
            )
            _write_sync_log(
                "WooAmendment",
                "Error",
                f"amendment failed for {source_si_name}: {exc}",
                woo_order_id,
            )
            frappe.log_error(
                frappe.get_traceback(),
                f"run_woo_amendment_job failed for woo_order_id={woo_order_id}",
            )
            return {
                "status": "error",
                "reason": str(exc),
                "woo_order_id": woo_order_id,
                "invoice": source_si_name,
            }
        finally:
            frappe.flags.ignore_woo_outbound = prev_outbound_flag

    finally:
        # ── 20. Release advisory locks ────────────────────────────────────────
        if inv_lock_acquired and inv_lock_key:
            try:
                frappe.db.sql("SELECT RELEASE_LOCK(%s)", (inv_lock_key,))
            except Exception:
                pass
        if order_lock_acquired:
            try:
                frappe.db.sql("SELECT RELEASE_LOCK(%s)", (lock_key_order,))
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _resolve_link_field() -> str:
    """Detect whether the Order Map uses 'erpnext_sales_invoice' or legacy 'sales_invoice'."""
    try:
        cols = frappe.db.get_table_columns("WooCommerce Order Map") or []
        if "erpnext_sales_invoice" in cols:
            return "erpnext_sales_invoice"
        if "sales_invoice" in cols:
            return "sales_invoice"
    except Exception:
        pass
    return "erpnext_sales_invoice"


def _build_request_id(woo_order_id: int, new_hash: str) -> str:
    digest = hashlib.sha256(
        json.dumps(
            {"woo_order_id": woo_order_id, "hash": new_hash},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return f"woo-amd-{woo_order_id}-{digest[:12]}"


def _build_savepoint_name(request_id: str) -> str:
    token = hashlib.sha256(str(request_id).encode("utf-8")).hexdigest()[:16]
    return f"woo_amend_{token}"


def _find_existing_replacement(source_si_name: str, new_hash: str) -> str | None:
    """Return an existing non-cancelled replacement invoice when its hash matches."""
    rows = frappe.get_all(
        "Sales Invoice",
        filters={"amended_from": source_si_name, "docstatus": ["!=", 2]},
        fields=["name"],
        order_by="creation desc",
        limit_page_length=1,
    ) or []
    if not rows:
        return None
    candidate = rows[0]["name"]
    # Check Order Map hash — if hash already matches, the amendment is idempotent.
    link_field = _resolve_link_field()
    stored_hash = frappe.db.get_value(
        "WooCommerce Order Map",
        {link_field: candidate},
        "hash",
    ) or ""
    if stored_hash == new_hash:
        return candidate
    return None


def _check_period_closed(source_si: Any) -> str | None:
    """Return a human-readable reason string if the accounting period is closed."""
    try:
        posting_date = str(source_si.get("posting_date") or "").strip()
        company = str(source_si.get("company") or "").strip()
        if not posting_date or not company:
            return None
        closed = frappe.db.get_value(
            "Accounting Period",
            {
                "company": company,
                "start_date": ["<=", posting_date],
                "end_date": [">=", posting_date],
                "closed": 1,
            },
            "name",
        )
        if closed:
            return f"Accounting Period '{closed}' is closed for {posting_date}"
    except Exception:
        pass
    return None


def _write_sync_log(
    operation: str,
    status: str,
    message: Any,
    woo_order_id: int | None = None,
) -> None:
    try:
        from jarz_woocommerce_integration.services.order_sync import create_sync_log_entry
        create_sync_log_entry(operation, status, message, woo_order_id=woo_order_id)
    except Exception:
        pass


def _flag_needs_review(
    *,
    woo_order_id: int,
    invoice_name: str,
    reason: str,
) -> None:
    try:
        from jarz_woocommerce_integration.services.order_sync import _flag_order_map_for_manual_review
        _flag_order_map_for_manual_review(
            woo_id=woo_order_id,
            invoice_name=invoice_name,
            reason=reason,
        )
    except Exception:
        pass
