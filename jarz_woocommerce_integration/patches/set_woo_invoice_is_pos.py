"""Backfill ``is_pos`` (and the POS Profile it depends on) on Woo Sales Invoices.

F-17. Every Sales Invoice is meant to end up as a POS invoice — the POS kanban,
the branch reports and the shift reconciliation all assume it. A population of
Woo-created invoices never got there: 533 rows on production, 543 on staging, and
the correlation is exact — every ``is_pos = 0`` row has ``pos_profile IS NULL``
and no row has a profile without ``is_pos``. Resolving a POS Profile is therefore
both necessary and sufficient.

Measured on production before writing this:

* 0 of the 533 have ``Sales Invoice Payment`` rows — and neither do 10,399 of the
  10,403 invoices already at ``is_pos = 1``. An empty payments table alongside
  ``is_pos = 1`` is this site's normal state, so the flag introduces nothing new.
* All 533 are settled (``outstanding_amount = 0``) and carry GL entries, and
  neither ``is_pos`` nor ``pos_profile`` participates in any ledger.
* ~496 of the 533 share ONE territory — ``EGISM``, a retired catch-all zone with
  no ``pos_profile`` whose last order was 2026-04-17, which is exactly where the
  ``is_pos = 0`` population stops. This patch never writes to that Territory:
  giving a retired zone a branch asserts a business fact nobody has decided, and
  it would make all ~496 invoices start reading as branch/territory mismatches.
  The invoices are stamped directly, and the summary names the territories that
  needed the fallback so the owner can see it.
* ``is_pos = 1`` with a NULL ``pos_profile`` does not exist anywhere here
  (10,403 POS invoices, zero without a profile). Both fields are therefore always
  written together, and a row that cannot get a profile is skipped, not stamped.

Safety rules this patch will not break:

* **Direct DB writes only** (``frappe.db.set_value(..., update_modified=False)``).
  These documents are submitted and carry GL entries. Re-saving or re-submitting
  one to flip a flag would be reckless, and re-validating one would drag in the
  POS payment validation this whole design avoids.
* **Submitted and cancelled only.** Drafts are deliberately left alone: ERPNext's
  ``on_submit`` throws "At least one mode of payment is required for POS invoice."
  for a payment-less POS invoice, so stamping a draft would leave a document
  nobody can submit by hand afterwards. Drafts get the flag the moment the Woo
  lane submits them (``order_sync._ensure_invoice_is_pos_flag``).
* **Never invents a branch.** Resolution stops at the configured
  ``default_pos_profile``, which is a decision the owner already made. If there is
  no usable default the row is skipped and reported, not attached to whichever
  profile happened to be created first.
* **Idempotent and re-runnable — by design, not by accident.** It selects on
  ``is_pos = 0``, so a second run only sees what the first could not resolve.
  That matters: if the settings default is missing (or a Territory gains a POS
  Profile later), fix that and run it again to pick up the remainder::

      bench --site <site> execute jarz_woocommerce_integration.patches.set_woo_invoice_is_pos.preview
      bench --site <site> execute jarz_woocommerce_integration.patches.set_woo_invoice_is_pos.execute

  ``preview()`` writes nothing and reports the same tier/territory breakdown.
"""

from __future__ import annotations

from typing import Any

import frappe

BATCH_COMMIT_SIZE = 200

#: Resolution tiers, most trustworthy first. The tier that answered is counted so
#: the summary says how much of the backfill is real branch attribution and how
#: much leaned on the configured default.
TIER_ALREADY_ON_INVOICE = "already_on_invoice"
TIER_INVOICE_TERRITORY = "invoice_territory"
TIER_CUSTOMER_TERRITORY = "customer_territory"
TIER_SETTINGS_DEFAULT = "settings_default_pos_profile"

#: The only way a row goes unresolved: nothing on the invoice, nothing on either
#: Territory, and no usable ``default_pos_profile`` in WooCommerce Settings. Such
#: a row is skipped and reported — never stamped, because ``is_pos = 1`` with a
#: NULL ``pos_profile`` is a shape this site has never produced (10,403 POS
#: invoices, zero without a profile) and half of ERPNext's POS handling assumes
#: the profile is there.
UNRESOLVED_NO_DEFAULT_CONFIGURED = "no_pos_profile_and_no_settings_default"


def _woo_column_available() -> bool:
    try:
        return "woo_order_id" in (frappe.db.get_table_columns("Sales Invoice") or [])
    except Exception:  # noqa: BLE001
        return False


def _load_valid_pos_profiles() -> set[str]:
    """Enabled POS Profiles. A disabled one is not an answer."""
    try:
        return {
            row["name"]
            for row in frappe.get_all("POS Profile", filters={"disabled": 0}, fields=["name"])
        }
    except Exception:  # noqa: BLE001
        return set()


def _territory_profile(territory: str | None, cache: dict[str, str | None], valid: set[str]) -> str | None:
    territory = (territory or "").strip()
    if not territory:
        return None
    if territory not in cache:
        try:
            cache[territory] = frappe.db.get_value("Territory", territory, "pos_profile") or None
        except Exception:  # noqa: BLE001
            cache[territory] = None
    profile = cache[territory]
    return profile if profile in valid else None


def _customer_territory(customer: str | None, cache: dict[str, str | None]) -> str | None:
    customer = (customer or "").strip()
    if not customer:
        return None
    if customer not in cache:
        try:
            cache[customer] = frappe.db.get_value("Customer", customer, "territory") or None
        except Exception:  # noqa: BLE001
            cache[customer] = None
    return cache[customer]


def _resolve_profile_for_row(row: dict[str, Any], state: dict[str, Any]) -> tuple[str | None, str]:
    """Return ``(pos_profile, tier_or_reason)`` for one invoice row.

    Order: the invoice's own profile, then its Territory, then the customer's
    Territory, then the configured default. The customer's Territory sits in the
    middle deliberately — for the handful of rows with no territory on the
    invoice it is a real answer, and a real answer beats the default.

    The Territory itself is NEVER written. ~496 of these rows belong to ``EGISM``,
    a retired catch-all zone with no POS Profile; assigning it one would assert a
    business fact nobody has decided and would make every one of those invoices
    read as a branch/territory mismatch afterwards. The invoices are stamped
    directly instead.
    """
    existing = str(row.get("pos_profile") or "").strip()
    if existing:
        return existing, TIER_ALREADY_ON_INVOICE

    valid = state["valid_profiles"]

    invoice_territory = str(row.get("territory") or "").strip()
    profile = _territory_profile(invoice_territory, state["territory_cache"], valid)
    if profile:
        return profile, TIER_INVOICE_TERRITORY

    customer_territory = str(
        _customer_territory(row.get("customer"), state["customer_cache"]) or ""
    ).strip()
    profile = _territory_profile(customer_territory, state["territory_cache"], valid)
    if profile:
        return profile, TIER_CUSTOMER_TERRITORY

    # Count which territories are sending us here — this is what makes the EGISM
    # population visible in the summary instead of just a number.
    known_territory = invoice_territory or customer_territory or "(none)"
    state["territory_without_profile"][known_territory] = (
        state["territory_without_profile"].get(known_territory, 0) + 1
    )

    settings_default = state["settings_default"]
    if settings_default and settings_default in valid:
        return settings_default, TIER_SETTINGS_DEFAULT

    return None, UNRESOLVED_NO_DEFAULT_CONFIGURED


def _fetch_candidates() -> list[dict[str, Any]]:
    return frappe.db.sql(
        """
        SELECT name, docstatus, pos_profile, territory, customer, company,
               IFNULL(woo_order_id, 0) AS woo_order_id, posting_date
        FROM `tabSales Invoice`
        WHERE IFNULL(woo_order_id, 0) > 0
          AND IFNULL(is_pos, 0) = 0
        ORDER BY posting_date ASC, name ASC
        """,
        as_dict=True,
    )


def _new_state() -> dict[str, Any]:
    try:
        settings_default = frappe.db.get_single_value("WooCommerce Settings", "default_pos_profile")
    except Exception:  # noqa: BLE001
        settings_default = None
    return {
        "valid_profiles": _load_valid_pos_profiles(),
        "territory_cache": {},
        "customer_cache": {},
        "territory_without_profile": {},
        "settings_default": (settings_default or "").strip() or None,
    }


def _has_kanban_profile_field() -> bool:
    try:
        return bool(frappe.get_meta("Sales Invoice").get_field("custom_kanban_profile"))
    except Exception:  # noqa: BLE001
        return False


def _summarize(summary: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
    title = "Woo: is_pos backfill (preview)" if dry_run else "Woo: is_pos backfill"
    try:
        # log_error, not logger().info(): the default log level on staging and
        # production is ERROR, so an info line would never be seen.
        frappe.log_error(title=title, message=frappe.as_json(summary))
    except Exception:  # noqa: BLE001
        pass
    try:
        print(f"{title}: {summary}")  # visible in the bench migrate output
    except Exception:  # noqa: BLE001
        pass
    return summary


def _run(*, dry_run: bool) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "candidates": 0,
        "updated": 0,
        "skipped_draft": 0,
        "skipped_unresolved": 0,
        "errors": 0,
        "by_tier": {},
        "unresolved_by_reason": {},
        "territories_without_pos_profile": {},
        "unresolved_sample": [],
        "dry_run": dry_run,
    }

    if not _woo_column_available():
        summary["skipped_reason"] = "no_woo_order_id_column"
        return _summarize(summary, dry_run=dry_run)

    rows = _fetch_candidates()
    summary["candidates"] = len(rows)
    if not rows:
        return _summarize(summary, dry_run=dry_run)

    state = _new_state()
    summary["settings_default_pos_profile"] = state["settings_default"]
    write_kanban_profile = _has_kanban_profile_field()
    pending = 0

    for row in rows:
        try:
            if int(row.get("docstatus") or 0) == 0:
                # See the module docstring: a payment-less POS draft can never be
                # submitted by hand afterwards.
                summary["skipped_draft"] += 1
                continue

            profile, tier = _resolve_profile_for_row(row, state)
            if not profile:
                summary["skipped_unresolved"] += 1
                summary["unresolved_by_reason"][tier] = (
                    summary["unresolved_by_reason"].get(tier, 0) + 1
                )
                if len(summary["unresolved_sample"]) < 25:
                    summary["unresolved_sample"].append(
                        {
                            "invoice": row.get("name"),
                            "woo_order_id": row.get("woo_order_id"),
                            "customer": row.get("customer"),
                            "territory": row.get("territory"),
                            "reason": tier,
                        }
                    )
                continue

            summary["by_tier"][tier] = summary["by_tier"].get(tier, 0) + 1
            if dry_run:
                continue

            # BOTH fields, in one write. "is_pos = 1 implies pos_profile is set"
            # holds without exception across the 10,403 POS invoices on this site;
            # flipping the flag alone would break that invariant for the first
            # time and hand ERPNext's POS code (closing entries, get_pos_profile,
            # branch-scoped queries) a shape it has never had to handle.
            updates: dict[str, Any] = {"is_pos": 1}
            if not str(row.get("pos_profile") or "").strip():
                updates["pos_profile"] = profile
                if write_kanban_profile:
                    updates["custom_kanban_profile"] = profile

            frappe.db.set_value("Sales Invoice", row["name"], updates, update_modified=False)
            summary["updated"] += 1
            pending += 1

            if pending >= BATCH_COMMIT_SIZE:
                frappe.db.commit()
                pending = 0
        except Exception:  # noqa: BLE001
            summary["errors"] += 1
            frappe.log_error(
                frappe.get_traceback(),
                f"Woo is_pos backfill failed for {row.get('name')}",
            )

    summary["territories_without_pos_profile"] = state["territory_without_profile"]

    if not dry_run:
        try:
            frappe.db.commit()
        except Exception:  # noqa: BLE001
            summary["errors"] += 1
            frappe.log_error(frappe.get_traceback(), "Woo is_pos backfill final commit failed")

    return _summarize(summary, dry_run=dry_run)


def preview() -> dict[str, Any]:
    """Report what :func:`execute` would do, without writing anything."""
    return _run(dry_run=True)


def execute() -> dict[str, Any]:
    return _run(dry_run=False)
