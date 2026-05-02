from __future__ import annotations

from typing import Any

import frappe

from jarz_woocommerce_integration.utils.customer_woo_id import (
    _CUSTOMER_COLUMN_CACHE,
    has_legacy_customer_woo_id,
    normalize_woo_customer_id,
    set_customer_woo_id,
)


LOGGER = frappe.logger("jarz_woocommerce.customer_woo_id_migration")


def _load_customer_rows() -> list[dict[str, Any]]:
    legacy_select = ", custom_woo_customer_id" if has_legacy_customer_woo_id() else ", NULL AS custom_woo_customer_id"
    return frappe.db.sql(
        f"""
        SELECT
            name,
            email_id,
            mobile_no,
            phone,
            woo_customer_id
            {legacy_select}
        FROM `tabCustomer`
        WHERE IFNULL(disabled, 0) = 0
        """,
        as_dict=True,
    )


def _classify_customer_rows(rows: list[dict[str, Any]]) -> tuple[dict[str, int], dict[str, list[dict[str, Any]]], set[str]]:
    counts = {
        "canonical_only": 0,
        "legacy_only": 0,
        "both_equal": 0,
        "both_conflict": 0,
        "unlinked": 0,
    }
    effective_groups: dict[str, list[dict[str, Any]]] = {}

    for row in rows:
        canonical_id = normalize_woo_customer_id(row.get("woo_customer_id"))
        legacy_id = normalize_woo_customer_id(row.get("custom_woo_customer_id"))

        if canonical_id and legacy_id:
            if canonical_id == legacy_id:
                counts["both_equal"] += 1
            else:
                counts["both_conflict"] += 1
        elif canonical_id:
            counts["canonical_only"] += 1
        elif legacy_id:
            counts["legacy_only"] += 1
        else:
            counts["unlinked"] += 1

        effective_id = canonical_id or legacy_id
        if effective_id:
            effective_groups.setdefault(effective_id, []).append(row)

    duplicate_customer_names: set[str] = set()
    duplicate_groups = {
        woo_customer_id: group
        for woo_customer_id, group in effective_groups.items()
        if len(group) > 1
    }
    for group in duplicate_groups.values():
        duplicate_customer_names.update(row["name"] for row in group)

    return counts, duplicate_groups, duplicate_customer_names


def audit_customer_woo_id_migration(limit: int = 20) -> dict[str, Any]:
    rows = _load_customer_rows()
    counts, duplicate_groups, _ = _classify_customer_rows(rows)

    conflicts = []
    for row in rows:
        canonical_id = normalize_woo_customer_id(row.get("woo_customer_id"))
        legacy_id = normalize_woo_customer_id(row.get("custom_woo_customer_id"))
        if canonical_id and legacy_id and canonical_id != legacy_id:
            conflicts.append(
                {
                    "customer": row["name"],
                    "email": row.get("email_id"),
                    "woo_customer_id": canonical_id,
                    "legacy_woo_customer_id": legacy_id,
                }
            )
            if len(conflicts) >= limit:
                break

    duplicate_samples = []
    for woo_customer_id, group in duplicate_groups.items():
        duplicate_samples.append(
            {
                "woo_customer_id": woo_customer_id,
                "customers": [row["name"] for row in group],
            }
        )
        if len(duplicate_samples) >= limit:
            break

    return {
        "rows_scanned": len(rows),
        "legacy_field_present": has_legacy_customer_woo_id(),
        **counts,
        "duplicate_groups": len(duplicate_groups),
        "duplicate_customers": sum(len(group) for group in duplicate_groups.values()),
        "conflicts_sample": conflicts,
        "duplicate_sample": duplicate_samples,
    }


def migrate_customer_woo_ids(*, dry_run: bool = True, clear_legacy: bool = True) -> dict[str, Any]:
    rows = _load_customer_rows()
    counts, duplicate_groups, duplicate_customer_names = _classify_customer_rows(rows)

    updated = 0
    cleared_legacy = 0
    skipped_conflicts = 0
    skipped_duplicates = 0
    skipped_unlinked = 0
    changed_customers: list[str] = []
    conflict_customers: list[str] = []

    for row in rows:
        customer_name = row["name"]
        canonical_id = normalize_woo_customer_id(row.get("woo_customer_id"))
        legacy_id = normalize_woo_customer_id(row.get("custom_woo_customer_id"))

        if customer_name in duplicate_customer_names:
            skipped_duplicates += 1
            continue

        if canonical_id and legacy_id and canonical_id != legacy_id:
            skipped_conflicts += 1
            if len(conflict_customers) < 20:
                conflict_customers.append(customer_name)
            continue

        if not canonical_id and not legacy_id:
            skipped_unlinked += 1
            continue

        if not canonical_id and legacy_id:
            updated += 1
            changed_customers.append(customer_name)
            if not dry_run:
                set_customer_woo_id(customer_name, legacy_id, clear_legacy=clear_legacy, update_modified=False)
            continue

        if canonical_id and legacy_id and canonical_id == legacy_id and clear_legacy:
            cleared_legacy += 1
            changed_customers.append(customer_name)
            if not dry_run:
                frappe.db.set_value(
                    "Customer",
                    customer_name,
                    {"custom_woo_customer_id": None},
                    update_modified=False,
                )

    if not dry_run and (updated or cleared_legacy):
        frappe.db.commit()

    return {
        "dry_run": dry_run,
        "rows_scanned": len(rows),
        **counts,
        "updated": updated,
        "cleared_legacy": cleared_legacy,
        "skipped_conflicts": skipped_conflicts,
        "skipped_duplicates": skipped_duplicates,
        "skipped_unlinked": skipped_unlinked,
        "duplicate_groups": len(duplicate_groups),
        "duplicate_customers": sum(len(group) for group in duplicate_groups.values()),
        "changed_customers_sample": changed_customers[:20],
        "conflict_customers_sample": conflict_customers,
    }


def drop_legacy_customer_woo_id_field(*, force: bool = False) -> dict[str, Any]:
    if not has_legacy_customer_woo_id():
        return {"removed": False, "reason": "legacy_field_missing"}

    audit = audit_customer_woo_id_migration(limit=50)
    blocking = audit["legacy_only"] or audit["both_equal"] or audit["both_conflict"] or audit["duplicate_groups"]
    if blocking and not force:
        LOGGER.warning({
            "event": "woo_customer_id_drop_blocked",
            "audit": audit,
        })
        return {"removed": False, "reason": "legacy_data_remaining", "audit": audit}

    custom_field_name = frappe.db.get_value(
        "Custom Field",
        {"dt": "Customer", "fieldname": "custom_woo_customer_id"},
        "name",
    )
    if custom_field_name:
        frappe.delete_doc("Custom Field", custom_field_name, force=True, ignore_permissions=True)

    if has_legacy_customer_woo_id():
        frappe.db.sql("ALTER TABLE `tabCustomer` DROP COLUMN `custom_woo_customer_id`")
        _CUSTOMER_COLUMN_CACHE.pop("custom_woo_customer_id", None)

    frappe.db.commit()
    return {"removed": True}