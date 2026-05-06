from __future__ import annotations

from collections import defaultdict
from typing import Any

import frappe

from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
    WooCommerceSettings,
)
from jarz_woocommerce_integration.services.customer_bulk_sync import _sync_single_customer
from jarz_woocommerce_integration.services.customer_sync import (
    _field_exists,
    _find_existing_address_for_customer,
    _has_usable_source_address,
    _normalize_phone,
    _same_source_address,
    _set_address_as_default,
    _source_address_signature,
    _stored_address_signature,
)
from jarz_woocommerce_integration.utils.customer_woo_id import normalize_woo_customer_id
from jarz_woocommerce_integration.utils.http_client import WooClient


LOGGER = frappe.logger("jarz_woocommerce.customer_cleanup")


def _init_client(settings: WooCommerceSettings) -> WooClient:
    return WooClient(
        base_url=settings.base_url.rstrip("/"),
        consumer_key=settings.consumer_key,
        consumer_secret=settings.get_password("consumer_secret"),
        api_version=settings.api_version or "v3",
    )


def _fetch_customer_page_window(
    client: WooClient,
    *,
    per_page: int = 100,
    start_page: int = 1,
    max_pages: int | None = None,
) -> tuple[list[dict[str, Any]], int, int]:
    page = max(1, int(start_page or 1))
    first_page = page
    fetched_pages = 0
    customers: list[dict[str, Any]] = []
    while True:
        if max_pages is not None and fetched_pages >= max_pages:
            break
        batch = client.list_customers(
            params={"per_page": per_page, "page": page, "orderby": "id", "order": "asc"}
        )
        if not batch:
            break
        customers.extend(batch)
        fetched_pages += 1
        if len(batch) < per_page:
            break
        page += 1

    last_page = page if fetched_pages else max(0, first_page - 1)
    return customers, fetched_pages, last_page


def _fetch_all_woo_customers(
    *,
    per_page: int = 100,
    start_page: int = 1,
    max_pages: int | None = None,
) -> tuple[list[dict[str, Any]], int, int]:
    settings = WooCommerceSettings.get_settings()
    client = _init_client(settings)
    return _fetch_customer_page_window(
        client,
        per_page=per_page,
        start_page=start_page,
        max_pages=max_pages,
    )


def _window_bounds(*, per_page: int, start_page: int, max_pages: int | None, total_items: int) -> tuple[int, int]:
    start_index = max(0, (max(1, int(start_page or 1)) - 1) * per_page)
    if max_pages is None:
        end_index = total_items
    else:
        end_index = min(total_items, start_index + (max_pages * per_page))
    return start_index, end_index


def _refresh_db_connection() -> None:
    try:
        frappe.db.close()
    except Exception:
        pass
    frappe.db.connect()


def _load_customer_rows() -> list[dict[str, Any]]:
    select_fields = [
        "name",
        "IFNULL(disabled, 0) AS disabled",
        "woo_customer_id",
        "mobile_no",
    ]
    for fieldname in ("custom_woo_customer_id", "phone", "email_id", "woo_username"):
        if _field_exists("Customer", fieldname):
            select_fields.append(fieldname)
        else:
            select_fields.append(f"NULL AS {fieldname}")

    return frappe.db.sql(
        f"SELECT {', '.join(select_fields)} FROM `tabCustomer`",
        as_dict=True,
    )


def _load_address_rows() -> list[dict[str, Any]]:
    is_primary_expr = "IFNULL(a.is_primary_address, 0) AS is_primary_address"
    is_shipping_expr = "IFNULL(a.is_shipping_address, 0) AS is_shipping_address"
    return frappe.db.sql(
        f"""
        SELECT
            dl.link_name AS customer_name,
            a.name,
            a.address_line1,
            a.address_line2,
            a.city,
            a.state,
            a.pincode,
            a.country,
            {is_primary_expr},
            {is_shipping_expr}
        FROM `tabAddress` a
        JOIN `tabDynamic Link` dl ON dl.parent = a.name
        WHERE dl.link_doctype = 'Customer'
          AND dl.parenttype = 'Address'
          AND IFNULL(a.disabled, 0) = 0
        """,
        as_dict=True,
    )


def _build_customer_indexes(rows: list[dict[str, Any]]) -> dict[str, Any]:
    indexes: dict[str, Any] = {
        "by_name": {},
        "by_canonical_id": defaultdict(list),
        "by_phone": defaultdict(list),
        "by_email": defaultdict(list),
        "by_username": defaultdict(list),
    }

    for row in rows:
        indexes["by_name"][row["name"]] = row

        canonical_id = normalize_woo_customer_id(row.get("woo_customer_id"))
        if canonical_id:
            indexes["by_canonical_id"][canonical_id].append(row["name"])

        for phone_value in (row.get("mobile_no"), row.get("phone")):
            phone_norm = _normalize_phone(phone_value)
            if phone_norm and row["name"] not in indexes["by_phone"][phone_norm]:
                indexes["by_phone"][phone_norm].append(row["name"])

        email_value = str(row.get("email_id") or "").strip().lower()
        if email_value:
            indexes["by_email"][email_value].append(row["name"])

        username_value = str(row.get("woo_username") or "").strip().lower()
        if username_value:
            indexes["by_username"][username_value].append(row["name"])

    return indexes


def _register_customer_in_indexes(
    indexes: dict[str, Any],
    *,
    customer_name: str,
    woo_customer_id: Any,
    phone: str | None,
    email: str | None,
    username: str | None,
) -> None:
    row = indexes["by_name"].setdefault(customer_name, {"name": customer_name})
    row["woo_customer_id"] = normalize_woo_customer_id(woo_customer_id)
    row["mobile_no"] = phone
    row["email_id"] = email
    row["woo_username"] = username

    canonical_id = normalize_woo_customer_id(woo_customer_id)
    if canonical_id and customer_name not in indexes["by_canonical_id"][canonical_id]:
        indexes["by_canonical_id"][canonical_id].append(customer_name)

    phone_norm = _normalize_phone(phone)
    if phone_norm and customer_name not in indexes["by_phone"][phone_norm]:
        indexes["by_phone"][phone_norm].append(customer_name)

    email_value = str(email or "").strip().lower()
    if email_value and customer_name not in indexes["by_email"][email_value]:
        indexes["by_email"][email_value].append(customer_name)

    username_value = str(username or "").strip().lower()
    if username_value and customer_name not in indexes["by_username"][username_value]:
        indexes["by_username"][username_value].append(customer_name)


def _resolve_woo_customer(cust: dict[str, Any], indexes: dict[str, Any]) -> dict[str, Any]:
    billing = cust.get("billing") or {}
    shipping = cust.get("shipping") or {}
    woo_id = normalize_woo_customer_id(cust.get("id"))
    phone = _normalize_phone(billing.get("phone") or shipping.get("phone"))
    email = str(cust.get("email") or billing.get("email") or "").strip().lower()
    username = str(cust.get("username") or "").strip().lower()

    exact_matches = indexes["by_canonical_id"].get(woo_id, []) if woo_id else []
    if len(exact_matches) == 1:
        return {"bucket": "exact_woo_id", "customer": exact_matches[0]}
    if len(exact_matches) > 1:
        return {
            "bucket": "blocked_duplicate_woo_id",
            "reason": "duplicate_canonical_woo_id",
            "customers": list(exact_matches),
        }

    phone_matches = indexes["by_phone"].get(phone, []) if phone else []
    if len(phone_matches) == 1:
        return {"bucket": "phone_merge", "customer": phone_matches[0]}
    if len(phone_matches) > 1:
        return {
            "bucket": "blocked_ambiguous_phone",
            "reason": "ambiguous_phone",
            "customers": list(phone_matches),
        }

    email_matches = indexes["by_email"].get(email, []) if email else []
    username_matches = indexes["by_username"].get(username, []) if username else []
    if not email_matches and not username_matches:
        return {"bucket": "safe_create"}
    if email_matches and username_matches:
        return {
            "bucket": "blocked_email_username_conflict",
            "reason": "email_and_username_conflict_without_phone",
            "email_matches": list(email_matches),
            "username_matches": list(username_matches),
        }
    if email_matches:
        return {
            "bucket": "blocked_email_conflict",
            "reason": "email_conflict_without_phone",
            "email_matches": list(email_matches),
        }
    return {
        "bucket": "blocked_username_conflict",
        "reason": "username_conflict_without_phone",
        "username_matches": list(username_matches),
    }


def _collect_desired_sources(cust: dict[str, Any]) -> dict[str, Any]:
    billing = cust.get("billing") or {}
    shipping = cust.get("shipping") or {}
    email = str(cust.get("email") or billing.get("email") or "").strip().lower() or None
    phone = _normalize_phone(billing.get("phone") or shipping.get("phone"))

    signatures: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    default_billing_signature = None
    default_shipping_signature = None

    for address_type, data in (("Billing", billing), ("Shipping", shipping)):
        if not _has_usable_source_address(data):
            continue
        signature = _source_address_signature(data)
        signatures.setdefault(
            signature,
            {
                "address_type": address_type,
                "data": data,
                "email": email,
                "phone": _normalize_phone(data.get("phone")) or phone,
            },
        )
        if address_type == "Billing" and default_billing_signature is None:
            default_billing_signature = signature
        if address_type == "Shipping" and default_shipping_signature is None:
            default_shipping_signature = signature

    if default_billing_signature and _same_source_address(billing, shipping):
        default_shipping_signature = default_billing_signature

    return {
        "signatures": signatures,
        "default_billing_signature": default_billing_signature,
        "default_shipping_signature": default_shipping_signature,
    }


def _merge_desired_sources(bucket: dict[str, Any], desired: dict[str, Any], woo_id: str | None, resolution: str) -> None:
    bucket["resolution_modes"].add(resolution)
    if woo_id:
        bucket["woo_ids"].add(woo_id)
    for signature, entry in desired["signatures"].items():
        bucket["signatures"].setdefault(signature, entry)
    if bucket["default_billing_signature"] is None and desired["default_billing_signature"] is not None:
        bucket["default_billing_signature"] = desired["default_billing_signature"]
    if bucket["default_shipping_signature"] is None and desired["default_shipping_signature"] is not None:
        bucket["default_shipping_signature"] = desired["default_shipping_signature"]


def _choose_canonical_address_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return sorted(
        rows,
        key=lambda row: (
            -int(row.get("is_primary_address") or 0),
            -int(row.get("is_shipping_address") or 0),
            str(row.get("name") or ""),
        ),
    )[0]


def _plan_address_cleanup(
    current_rows: list[dict[str, Any]],
    desired_signatures: dict[tuple[str, str, str, str, str, str], dict[str, Any]],
) -> dict[str, Any]:
    rows_by_signature: dict[tuple[str, str, str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in current_rows:
        signature = _stored_address_signature(row)
        if any(signature):
            rows_by_signature[signature].append(row)

    keep_rows: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    duplicates_to_retire: list[dict[str, Any]] = []
    for signature, rows in rows_by_signature.items():
        keep_row = _choose_canonical_address_row(rows)
        keep_rows[signature] = keep_row
        duplicates_to_retire.extend([row for row in rows if row["name"] != keep_row["name"]])

    missing_signatures = [
        signature for signature in desired_signatures.keys() if signature not in keep_rows
    ]
    extra_keep_rows = [
        row for signature, row in keep_rows.items() if signature not in desired_signatures
    ]

    return {
        "missing_signatures": missing_signatures,
        "duplicate_rows_to_retire": duplicates_to_retire,
        "extra_rows_to_retire": extra_keep_rows,
        "current_unique_signatures": set(keep_rows.keys()),
    }


def _retire_customer_address_row(
    customer_name: str,
    address_row: dict[str, Any],
    *,
    hard_delete_orphans: bool = False,
) -> dict[str, Any]:
    address_name = address_row["name"]
    result = {"address": address_name, "action": "detached"}

    frappe.db.sql(
        """
        DELETE FROM `tabDynamic Link`
        WHERE parenttype = 'Address'
          AND parent = %s
          AND link_doctype = 'Customer'
          AND link_name = %s
        """,
        (address_name, customer_name),
    )

    updates = {"disabled": 1}
    if _field_exists("Address", "is_primary_address"):
        updates["is_primary_address"] = 0
    if _field_exists("Address", "is_shipping_address"):
        updates["is_shipping_address"] = 0
    frappe.db.set_value("Address", address_name, updates, update_modified=False)

    remaining_links = frappe.db.sql(
        """
        SELECT COUNT(*) AS link_count
        FROM `tabDynamic Link`
        WHERE parenttype = 'Address'
          AND parent = %s
        """,
        (address_name,),
        as_dict=True,
    )[0]["link_count"]

    if not remaining_links and hard_delete_orphans:
        try:
            frappe.delete_doc("Address", address_name, ignore_permissions=True)
            result["action"] = "deleted"
        except Exception as exc:  # noqa: BLE001
            result["action"] = "disabled"
            result["delete_error"] = str(exc)
    else:
        result["action"] = "disabled"

    return result


def _apply_default_addresses(customer_name: str, desired_state: dict[str, Any]) -> dict[str, str | None]:
    applied = {"billing": None, "shipping": None}
    for address_type, key in (("Billing", "default_billing_signature"), ("Shipping", "default_shipping_signature")):
        signature = desired_state.get(key)
        if not signature:
            continue
        source = desired_state["signatures"].get(signature)
        if not source:
            continue
        address_name = _find_existing_address_for_customer(
            customer_name,
            address_type,
            source["data"],
        )
        if address_name:
            _set_address_as_default(address_name, customer_name, address_type)
            applied[address_type.lower()] = address_name
    return applied


def run_customer_cleanup(
    *,
    dry_run: bool = True,
    per_page: int = 100,
    start_page: int = 1,
    max_pages: int | None = None,
    commit_every: int = 100,
    hard_delete_orphans: bool = False,
    sample_limit: int = 20,
) -> dict[str, Any]:
    all_woo_customers, _all_pages_fetched, _all_last_page_fetched = _fetch_all_woo_customers(
        per_page=per_page,
        start_page=1,
        max_pages=None,
    )
    _refresh_db_connection()
    start_index, end_index = _window_bounds(
        per_page=per_page,
        start_page=start_page,
        max_pages=max_pages,
        total_items=len(all_woo_customers),
    )
    window_woo_customers = all_woo_customers[start_index:end_index]
    if window_woo_customers:
        pages_fetched = ((len(window_woo_customers) - 1) // per_page) + 1
        last_page_fetched = start_page + pages_fetched - 1
    else:
        pages_fetched = 0
        last_page_fetched = max(0, start_page - 1)

    indexes = _build_customer_indexes(_load_customer_rows())

    desired_by_customer: dict[str, dict[str, Any]] = {}
    customers_in_window: set[str] = set()
    summary = {
        "dry_run": dry_run,
        "start_page": start_page,
        "max_pages": max_pages,
        "pages_fetched": pages_fetched,
        "last_page_fetched": last_page_fetched,
        "woo_customers_scanned": len(woo_customers),
        "exact_woo_id": 0,
        "phone_merge": 0,
        "safe_create": 0,
        "blocked_duplicate_woo_id": 0,
        "blocked_ambiguous_phone": 0,
        "blocked_email_conflict": 0,
        "blocked_username_conflict": 0,
        "blocked_email_username_conflict": 0,
        "customers_address_rebuilt": 0,
        "customers_with_address_changes": 0,
        "addresses_created": 0,
        "duplicate_rows_retired": 0,
        "extra_rows_retired": 0,
        "addresses_deleted": 0,
        "addresses_disabled": 0,
        "defaults_applied": 0,
        "blocked_samples": [],
        "touched_customers_sample": [],
    }

    mutation_count = 0

    for index, cust in enumerate(all_woo_customers):
        in_window = start_index <= index < end_index
        resolution = _resolve_woo_customer(cust, indexes)
        bucket = resolution["bucket"]
        woo_id = normalize_woo_customer_id(cust.get("id"))
        desired_sources = _collect_desired_sources(cust)

        if bucket.startswith("blocked_"):
            if in_window:
                summary[bucket] += 1
            if in_window and len(summary["blocked_samples"]) < sample_limit:
                billing = cust.get("billing") or {}
                shipping = cust.get("shipping") or {}
                summary["blocked_samples"].append(
                    {
                        "woo_id": woo_id,
                        "bucket": bucket,
                        "reason": resolution.get("reason"),
                        "email": str(cust.get("email") or billing.get("email") or "").strip().lower() or None,
                        "phone": _normalize_phone(billing.get("phone") or shipping.get("phone")),
                        "username": str(cust.get("username") or "").strip().lower() or None,
                        "candidates": resolution.get("customers")
                        or resolution.get("email_matches")
                        or resolution.get("username_matches"),
                    }
                )
            continue

        target_customer = resolution.get("customer")

        if in_window:
            summary[bucket] += 1

        if not dry_run and in_window:
            sync_result = _sync_single_customer(cust)
            target_customer = sync_result["customer"]
            mutation_count += 1
            if bucket == "safe_create":
                billing = cust.get("billing") or {}
                shipping = cust.get("shipping") or {}
                _register_customer_in_indexes(
                    indexes,
                    customer_name=target_customer,
                    woo_customer_id=woo_id,
                    phone=billing.get("phone") or shipping.get("phone"),
                    email=str(cust.get("email") or billing.get("email") or "").strip().lower() or None,
                    username=str(cust.get("username") or "").strip().lower() or None,
                )
            if commit_every and mutation_count % commit_every == 0:
                frappe.db.commit()

        if not target_customer:
            continue

        if in_window:
            customers_in_window.add(target_customer)

        state = desired_by_customer.setdefault(
            target_customer,
            {
                "signatures": {},
                "resolution_modes": set(),
                "woo_ids": set(),
                "default_billing_signature": None,
                "default_shipping_signature": None,
            },
        )
        _merge_desired_sources(state, desired_sources, woo_id, bucket)

    if dry_run:
        address_rows = _load_address_rows()
    else:
        frappe.db.commit()
        address_rows = _load_address_rows()

    addresses_by_customer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in address_rows:
        addresses_by_customer[row["customer_name"]].append(row)

    customers_to_process = list(desired_by_customer.keys()) if max_pages is None and start_page == 1 else sorted(customers_in_window)

    for customer_name in customers_to_process:
        desired_state = desired_by_customer[customer_name]
        current_rows = addresses_by_customer.get(customer_name, [])
        plan = _plan_address_cleanup(current_rows, desired_state["signatures"])
        has_changes = bool(
            plan["missing_signatures"]
            or plan["duplicate_rows_to_retire"]
            or plan["extra_rows_to_retire"]
        )
        if has_changes:
            summary["customers_with_address_changes"] += 1

        if dry_run:
            summary["addresses_created"] += len(plan["missing_signatures"])
            summary["duplicate_rows_retired"] += len(plan["duplicate_rows_to_retire"])
            summary["extra_rows_retired"] += len(plan["extra_rows_to_retire"])
        else:
            for signature in plan["missing_signatures"]:
                source = desired_state["signatures"][signature]
                address_name = _find_existing_address_for_customer(
                    customer_name,
                    source["address_type"],
                    source["data"],
                )
                if not address_name:
                    from jarz_woocommerce_integration.services.customer_sync import _create_address

                    address_name = _create_address(
                        customer_name,
                        source["address_type"],
                        source["data"],
                        source["phone"],
                        source["email"],
                    )
                    summary["addresses_created"] += 1
                    mutation_count += 1

            for row in plan["duplicate_rows_to_retire"]:
                result = _retire_customer_address_row(
                    customer_name,
                    row,
                    hard_delete_orphans=hard_delete_orphans,
                )
                summary["duplicate_rows_retired"] += 1
                summary["addresses_deleted"] += int(result["action"] == "deleted")
                summary["addresses_disabled"] += int(result["action"] != "deleted")
                mutation_count += 1

            for row in plan["extra_rows_to_retire"]:
                result = _retire_customer_address_row(
                    customer_name,
                    row,
                    hard_delete_orphans=hard_delete_orphans,
                )
                summary["extra_rows_retired"] += 1
                summary["addresses_deleted"] += int(result["action"] == "deleted")
                summary["addresses_disabled"] += int(result["action"] != "deleted")
                mutation_count += 1

            defaults = _apply_default_addresses(customer_name, desired_state)
            summary["defaults_applied"] += int(bool(defaults["billing"])) + int(bool(defaults["shipping"]))
            if commit_every and mutation_count and mutation_count % commit_every == 0:
                frappe.db.commit()

        if has_changes:
            summary["customers_address_rebuilt"] += 1
            if len(summary["touched_customers_sample"]) < sample_limit:
                summary["touched_customers_sample"].append(
                    {
                        "customer": customer_name,
                        "resolution_modes": sorted(desired_state["resolution_modes"]),
                        "woo_ids": sorted(desired_state["woo_ids"]),
                        "missing_signatures": len(plan["missing_signatures"]),
                        "duplicate_rows_retired": len(plan["duplicate_rows_to_retire"]),
                        "extra_rows_retired": len(plan["extra_rows_to_retire"]),
                    }
                )

    if not dry_run:
        frappe.db.commit()

    summary["customers_planned_for_cleanup"] = len(customers_to_process)
    return summary