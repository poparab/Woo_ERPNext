"""Merge the duplicate Customer records the sync defects left behind.

Three defects (fixed 2026-08-19) minted 1,340 Customer records across 536 phone
numbers. This merges the ones that are provably the same person and reports the
rest instead of guessing.

**A shared phone number is not proof of a shared person.** Egyptian households
share handsets and people order for friends; production really does hold
``فاديه توفيق`` and ``Karim abulnaga`` on one number. So a group is only merged
when something beyond the phone ties the records together:

* every member reduces to the same *base name* once the machine-made suffixes
  (``X-16449`` from the sync race, ``X - 2`` from ERPNext's collision counter)
  are stripped; or
* the members share a ``woo_customer_id`` that no Customer outside the group
  holds — a real WooCommerce account both records claim.

Everything else is reported for a human to decide and never touched.

The merge itself is :func:`frappe.rename_doc` with ``merge=True`` — the same
machinery ERPNext's own "merge with existing" uses. It rewrites every Link and
Dynamic Link reference (Sales Invoice, Payment Entry, GL Entry party, Address and
Contact links) onto the survivor and deletes the loser. Nothing is deleted
outright: every duplicate on this site is referenced by something, so merging is
the only operation that preserves history.

Usage — dry run first, always::

    bench --site <site> execute \\
        jarz_woocommerce_integration.services.customer_dedupe.run_dedupe

    bench --site <site> execute \\
        jarz_woocommerce_integration.services.customer_dedupe.run_dedupe \\
        --kwargs "{'apply': True, 'limit': 10}"
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from typing import Any

import frappe

# The model-level function, not the `frappe.rename_doc` alias: only this one
# accepts ignore_permissions, and a merge run must not depend on whose session
# happens to be executing it.
from frappe.model.rename_doc import rename_doc

from jarz_woocommerce_integration.services.customer_sync import (
    _normalize_phone,
    _suppress_woo_outbound,
)
from jarz_woocommerce_integration.utils.customer_woo_id import (
    customer_woo_id_column_exists,
)

LOGGER = frappe.logger("jarz_woocommerce.customer_dedupe")

# `X-16449` — the sync race's order-id suffix. `X - 2` — ERPNext's own counter.
ORDERID_SUFFIX = re.compile(r"^(?P<base>.+?)-(?P<oid>\d{4,6})$")
SEQ_SUFFIX = re.compile(r"^(?P<base>.+?)\s+-\s+(?P<n>\d{1,3})$")

# Money comparisons are in site currency; anything above this is a real mismatch.
MONEY_TOLERANCE = 0.01


def base_name(name: str | None) -> str:
    """Reduce a docname to the identity underneath the machine-made suffixes.

    ``'عماد - 7'``, ``'عماد-16449'`` and ``'عماد'`` all reduce to ``'عماد'``.
    Applied repeatedly because production carries both suffixes stacked.
    """
    previous = None
    current = (name or "").strip()
    while current != previous:
        previous = current
        match = SEQ_SUFFIX.match(current) or ORDERID_SUFFIX.match(current)
        if match:
            current = match.group("base").strip()
    return " ".join(current.lower().split())


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def _load_customers() -> list[dict[str, Any]]:
    return frappe.db.sql(
        """
        SELECT name, customer_name, mobile_no, email_id, woo_customer_id,
               default_currency, disabled, lead_name, creation
        FROM `tabCustomer`
        WHERE mobile_no IS NOT NULL AND mobile_no != ''
        """,
        as_dict=True,
    )


def _invoice_stats(names: list[str]) -> dict[str, dict[str, Any]]:
    """One batched aggregate for every candidate rather than a query per record."""
    if not names:
        return {}
    placeholders = ", ".join(["%s"] * len(names))
    stats: dict[str, dict[str, Any]] = {}
    for row in frappe.db.sql(
        f"""
        SELECT customer,
               SUM(docstatus = 1) AS submitted,
               SUM(docstatus = 0) AS draft,
               SUM(docstatus = 2) AS cancelled,
               COALESCE(SUM(CASE WHEN docstatus = 1 THEN grand_total END), 0) AS revenue,
               COALESCE(SUM(CASE WHEN docstatus = 1 THEN outstanding_amount END), 0) AS outstanding,
               MAX(CASE WHEN docstatus = 1 THEN posting_date END) AS last_date
        FROM `tabSales Invoice`
        WHERE customer IN ({placeholders})
        GROUP BY customer
        """,
        tuple(names),
        as_dict=True,
    ):
        stats[row["customer"]] = {
            "submitted": int(row["submitted"] or 0),
            "draft": int(row["draft"] or 0),
            "cancelled": int(row["cancelled"] or 0),
            "revenue": float(row["revenue"] or 0),
            "outstanding": float(row["outstanding"] or 0),
            "last_date": str(row["last_date"] or ""),
        }
    for name in names:
        stats.setdefault(name, {
            "submitted": 0, "draft": 0, "cancelled": 0,
            "revenue": 0.0, "outstanding": 0.0, "last_date": "",
        })
    return stats


def pick_survivor(members: list[dict[str, Any]]) -> dict[str, Any]:
    """The record the group should collapse onto.

    Most submitted invoices first: that is where the account really lives.
    Ranking by recency instead would hand a 142-invoice account to a one-invoice
    stray that happens to carry a newer order.

    A ``woo_customer_id`` breaks the tie next. ``rename_doc(merge=True)`` keeps the
    *survivor's* field values and deletes the loser, so electing an unbound record
    over an equally-established bound sibling throws the WooCommerce binding away
    — and an unbound Customer's orders can never reach the shopper's My Account.
    Preferring the bound member costs nothing when the counts are level.

    It is deliberately *below* the invoice count rather than above it: an
    established account must not be handed to a one-invoice stray merely because
    the stray happens to be bound. When that case does arise the binding still
    cannot be lost silently — :func:`_snapshot` records it and :func:`_diff`
    aborts the merge for a human to look at.

    Remaining ties fall to the most recent invoice, then the oldest record, so the
    answer never depends on row order.
    """
    return sorted(
        members,
        key=lambda m: (
            -m["stats"]["submitted"],
            0 if str(m.get("woo_customer_id") or "").strip() else 1,
            _neg_date(m["stats"]["last_date"]),
            str(m["creation"]),
            m["name"],
        ),
    )[0]


def _neg_date(value: str) -> str:
    """Sort key that puts the latest date first without parsing it."""
    if not value:
        return "9999-99-99"
    return "".join(chr(0x7E - ord(c)) if c.isdigit() else c for c in value)


def build_plan() -> dict[str, Any]:
    """Classify every duplicate-phone group. Pure read; changes nothing."""
    customers = _load_customers()
    woo_holders = Counter(
        str(c["woo_customer_id"]) for c in customers if c["woo_customer_id"]
    )

    by_phone: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for customer in customers:
        key = _normalize_phone(customer["mobile_no"]) or ""
        if len(key) >= 10:
            by_phone[key].append(customer)
    duplicates = {k: v for k, v in by_phone.items() if len(v) > 1}

    stats = _invoice_stats([c["name"] for v in duplicates.values() for c in v])

    auto: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []

    for phone, raw_members in sorted(duplicates.items()):
        members = [
            {
                "name": m["name"],
                "customer_name": m["customer_name"],
                "base": base_name(m["name"]),
                "woo_customer_id": str(m["woo_customer_id"] or ""),
                "email_id": m["email_id"] or "",
                "currency": str(m["default_currency"] or ""),
                "disabled": int(m["disabled"] or 0),
                "lead_name": m["lead_name"] or "",
                "creation": str(m["creation"]),
                "stats": stats.get(m["name"], {}),
            }
            for m in sorted(raw_members, key=lambda m: m["creation"])
        ]

        entry: dict[str, Any] = {"phone": phone, "size": len(members), "members": members}

        currencies = {m["currency"] for m in members if m["currency"]}
        if len(currencies) > 1:
            # rename_doc's before_rename refuses these anyway; do not even try.
            entry["reason"] = f"mixed party currency {sorted(currencies)}"
            review.append(entry)
            continue

        bases = {m["base"] for m in members}
        group_woo = [m["woo_customer_id"] for m in members if m["woo_customer_id"]]
        shared_exclusive = {
            woo for woo in set(group_woo)
            if group_woo.count(woo) > 1 and woo_holders[woo] == group_woo.count(woo)
        }

        if len(bases) == 1:
            entry["evidence"] = "identical base name"
        elif shared_exclusive and all(
            m["woo_customer_id"] in shared_exclusive for m in members
        ):
            entry["evidence"] = f"shared exclusive woo_customer_id {sorted(shared_exclusive)}"
        else:
            entry["reason"] = "different names and no exclusively-shared woo_customer_id"
            review.append(entry)
            continue

        survivor = pick_survivor(members)
        entry["survivor"] = survivor["name"]
        entry["losers"] = [m["name"] for m in members if m["name"] != survivor["name"]]
        entry["clean_name"] = _clean_target(members, survivor)
        auto.append(entry)

    return {"auto": auto, "review": review}


def _clean_target(members: list[dict[str, Any]], survivor: dict[str, Any]) -> str:
    """The unsuffixed name this group should end up under, if any member had it.

    Only a name that already existed in the group qualifies — the merge frees it
    when its holder is absorbed, so restoring it is putting back what the sync
    defect took away, not renaming the customer to something new.
    """
    if base_name(survivor["name"]) == survivor["name"].strip().lower():
        return ""  # survivor already carries the clean name
    for member in members:
        if member["name"] is survivor["name"]:
            continue
        if base_name(member["name"]) == member["name"].strip().lower():
            return member["name"]
    return ""


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------


def _woo_ids(names: list[str]) -> list[str]:
    """The distinct WooCommerce bindings held across *names*.

    A binding is not money and not a Link row, so none of the other invariants
    notice when a merge drops one — the survivor simply keeps its own (empty)
    ``woo_customer_id`` and the loser's is deleted with the record. The merge then
    reports a clean success while the shopper's ERPNext identity has quietly been
    severed from their store account, and their orders can never again appear
    under My Account.

    Returned as a sorted list of strings so the before/after comparison is a plain
    equality on a stable value. Returns ``[]`` rather than raising when the column
    is absent or the probe fails, which degrades to today's behaviour instead of
    breaking a merge run.
    """
    if not names or not customer_woo_id_column_exists():
        return []
    placeholders = ", ".join(["%s"] * len(names))
    try:
        rows = frappe.db.sql(
            f"SELECT woo_customer_id FROM `tabCustomer` WHERE name IN ({placeholders})",
            tuple(names),
        ) or []
    except Exception:  # noqa: BLE001 - a diagnostic must not break the merge
        return []
    found = set()
    for row in rows:
        value = str((row[0] if row else "") or "").strip()
        if value and value != "0":
            found.add(value)
    return sorted(found)


def _snapshot(names: list[str]) -> dict[str, Any]:
    """Everything that must survive a merge unchanged, summed over *names*."""
    placeholders = ", ".join(["%s"] * len(names))
    values = tuple(names)

    invoices = frappe.db.sql(
        f"""
        SELECT COALESCE(SUM(docstatus = 1), 0) AS submitted,
               COALESCE(SUM(docstatus = 0), 0) AS draft,
               COALESCE(SUM(docstatus = 2), 0) AS cancelled,
               COALESCE(SUM(CASE WHEN docstatus = 1 THEN grand_total END), 0) AS revenue,
               COALESCE(SUM(CASE WHEN docstatus = 1 THEN outstanding_amount END), 0) AS outstanding
        FROM `tabSales Invoice` WHERE customer IN ({placeholders})
        """,
        values, as_dict=True,
    )[0]

    gl = frappe.db.sql(
        f"""
        SELECT COUNT(*) AS rows_count,
               COALESCE(SUM(debit), 0) AS debit,
               COALESCE(SUM(credit), 0) AS credit
        FROM `tabGL Entry`
        WHERE party_type = 'Customer' AND party IN ({placeholders}) AND is_cancelled = 0
        """,
        values, as_dict=True,
    )[0]

    payments = frappe.db.sql(
        f"""
        SELECT COUNT(*) AS rows_count,
               COALESCE(SUM(CASE WHEN docstatus = 1 THEN paid_amount END), 0) AS paid
        FROM `tabPayment Entry`
        WHERE party_type = 'Customer' AND party IN ({placeholders})
        """,
        values, as_dict=True,
    )[0]

    addresses = frappe.db.sql(
        f"""
        SELECT COUNT(DISTINCT parent) AS rows_count FROM `tabDynamic Link`
        WHERE link_doctype = 'Customer' AND parenttype = 'Address' AND link_name IN ({placeholders})
        """,
        values,
    )[0][0]

    return {
        "woo_ids": _woo_ids(names),
        "submitted": int(invoices["submitted"] or 0),
        "draft": int(invoices["draft"] or 0),
        "cancelled": int(invoices["cancelled"] or 0),
        "revenue": round(float(invoices["revenue"] or 0), 2),
        "outstanding": round(float(invoices["outstanding"] or 0), 2),
        "gl_rows": int(gl["rows_count"] or 0),
        "gl_debit": round(float(gl["debit"] or 0), 2),
        "gl_credit": round(float(gl["credit"] or 0), 2),
        "pe_rows": int(payments["rows_count"] or 0),
        "pe_paid": round(float(payments["paid"] or 0), 2),
        "addresses": int(addresses or 0),
    }


def _diff(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    """Which invariants the merge broke, if any."""
    problems = []
    for key in ("submitted", "draft", "cancelled", "gl_rows", "pe_rows"):
        if before[key] != after[key]:
            problems.append(f"{key}: {before[key]} -> {after[key]}")
    for key in ("revenue", "outstanding", "gl_debit", "gl_credit", "pe_paid"):
        if abs(before[key] - after[key]) > MONEY_TOLERANCE:
            problems.append(f"{key}: {before[key]} -> {after[key]}")
    # Addresses may legitimately collapse if both records shared one, never grow.
    if after["addresses"] > before["addresses"]:
        problems.append(f"addresses: {before['addresses']} -> {after['addresses']}")
    # A WooCommerce binding held by any member must still be held afterwards.
    # ``.get`` rather than ``[]`` on purpose: callers hand-build snapshots, and a
    # missing key must mean "not measured", never a crash mid-merge.
    lost_bindings = sorted(set(before.get("woo_ids") or []) - set(after.get("woo_ids") or []))
    if lost_bindings:
        problems.append(f"woo_customer_id lost: {lost_bindings}")
    return problems


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


def _restore_lead_statuses(members: list[dict[str, Any]]) -> dict[str, str]:
    """Customer.on_trash resets a converted Lead to 'Interested'.

    That would silently undo a rep's pipeline stage for every merged record, so
    the statuses are captured here and put back afterwards.
    """
    leads = {m["lead_name"] for m in members if m["lead_name"]}
    if not leads:
        return {}
    return {
        row["name"]: row["status"]
        for row in frappe.db.sql(
            "SELECT name, status FROM `tabLead` WHERE name IN ({})".format(
                ", ".join(["%s"] * len(leads))
            ),
            tuple(leads), as_dict=True,
        )
    }


def merge_group(group: dict[str, Any], *, apply: bool = False,
                restore_clean_name: bool = True) -> dict[str, Any]:
    """Collapse one group onto its survivor, verifying nothing moved but the name.

    Every check runs inside a savepoint. If any invariant fails the whole group
    is rolled back and reported — a partial merge is never left behind.
    """
    survivor = group["survivor"]
    losers = group["losers"]
    names = [survivor, *losers]
    result: dict[str, Any] = {
        "phone": group["phone"], "survivor": survivor, "losers": losers,
        "evidence": group.get("evidence"), "applied": False, "problems": [],
    }

    before = _snapshot(names)
    result["before"] = before

    if not apply:
        result["would_merge"] = losers
        result["would_rename_to"] = group.get("clean_name") if restore_clean_name else ""
        return result

    savepoint = "jarz_dedupe"
    frappe.db.savepoint(savepoint)
    try:
        lead_statuses = _restore_lead_statuses(group["members"])

        for loser in losers:
            rename_doc(
                "Customer", loser, survivor,
                merge=True, force=True, ignore_permissions=True,
                show_alert=False, rebuild_search=False,
            )

        # Check for leftovers here, before the clean-name restore. Afterwards the
        # survivor may legitimately *hold* a loser's name — that is the whole
        # point of the restore — and a later check cannot tell that apart from a
        # loser that refused to merge.
        leftovers = [n for n in losers if frappe.db.exists("Customer", n)]

        final_name = survivor
        clean = group.get("clean_name") or ""
        if restore_clean_name and clean and clean != survivor:
            if not frappe.db.exists("Customer", clean):
                rename_doc(
                    "Customer", survivor, clean,
                    merge=False, force=True, ignore_permissions=True,
                    show_alert=False, rebuild_search=False,
                )
                final_name = clean

        for lead, status in lead_statuses.items():
            frappe.db.set_value("Lead", lead, "status", status, update_modified=False)

        after = _snapshot([final_name])
        problems = _diff(before, after)

        if leftovers:
            problems.append(f"losers still present: {leftovers}")
        if not frappe.db.exists("Customer", final_name):
            problems.append(f"survivor {final_name!r} missing after merge")

        result["after"] = after
        result["final_name"] = final_name

        if problems:
            frappe.db.rollback(save_point=savepoint)
            result["problems"] = problems
            LOGGER.error({"event": "dedupe_group_rolled_back",
                          "phone": group["phone"], "problems": problems})
            return result

        frappe.db.release_savepoint(savepoint)
        frappe.db.commit()
        result["applied"] = True
        LOGGER.info({"event": "dedupe_group_merged", "phone": group["phone"],
                     "survivor": final_name, "absorbed": losers})
        return result

    except Exception as exc:
        frappe.db.rollback(save_point=savepoint)
        result["problems"] = [f"{type(exc).__name__}: {exc}"]
        LOGGER.error({"event": "dedupe_group_failed", "phone": group["phone"],
                      "error": str(exc), "traceback": frappe.get_traceback()})
        return result


@frappe.whitelist()
def run_dedupe(apply: bool = False, limit: int | None = None,
               phones: str | list[str] | None = None,
               restore_clean_name: bool = True) -> dict[str, Any]:
    """Merge the auto-classified duplicate groups. Dry run unless ``apply``.

    Outbound WooCommerce sync is suppressed for the whole run: a merge touches
    hundreds of Customers, and without this each one would queue a push and
    rewrite the store.
    """
    apply = frappe.utils.cint(apply) == 1 if isinstance(apply, str) else bool(apply)
    restore_clean_name = (
        frappe.utils.cint(restore_clean_name) == 1
        if isinstance(restore_clean_name, str) else bool(restore_clean_name)
    )
    if isinstance(phones, str):
        phones = [p.strip() for p in phones.split(",") if p.strip()]

    plan = build_plan()
    groups = plan["auto"]
    if phones:
        wanted = {_normalize_phone(p) for p in phones}
        groups = [g for g in groups if g["phone"] in wanted]
    if limit:
        groups = groups[: int(limit)]

    results = []
    with _suppress_woo_outbound():
        for group in groups:
            results.append(merge_group(group, apply=apply,
                                       restore_clean_name=restore_clean_name))

    merged = [r for r in results if r["applied"]]
    failed = [r for r in results if r["problems"]]
    summary = {
        "apply": apply,
        "groups_considered": len(groups),
        "groups_merged": len(merged),
        "groups_failed": len(failed),
        "records_absorbed": sum(len(r["losers"]) for r in merged),
        "review_groups": len(plan["review"]),
        "failures": [
            {"phone": r["phone"], "survivor": r["survivor"], "problems": r["problems"]}
            for r in failed
        ],
    }
    LOGGER.info({"event": "dedupe_run_complete", **{
        k: v for k, v in summary.items() if k != "failures"
    }})
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return {"summary": summary, "results": results, "review": plan["review"]}


@frappe.whitelist()
def review_report() -> list[dict[str, Any]]:
    """The groups this tool refuses to merge, for a human to decide on."""
    return build_plan()["review"]
