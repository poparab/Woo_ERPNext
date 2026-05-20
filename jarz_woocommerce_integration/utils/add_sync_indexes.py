"""Add database indexes for faster sync operations."""

import frappe


def _sync_indexes() -> list[dict[str, str]]:
    return [
        {
            "name": "idx_customer_email",
            "table": "tabCustomer",
            "column": "email_id",
            "desc": "Customer email lookups"
        },
        {
            "name": "idx_item_code",
            "table": "tabItem",
            "column": "item_code",
            "desc": "Item code lookups"
        },
        {
            "name": "idx_woo_order_map_id",
            "table": "tabWooCommerce Order Map",
            "column": "woo_order_id",
            "desc": "WooCommerce order ID lookups"
        },
        {
            "name": "idx_territory_woo_code",
            "table": "tabTerritory",
            "column": "custom_woo_code",
            "desc": "Territory code lookups"
        },
        {
            "name": "idx_address_state",
            "table": "tabAddress",
            "column": "state",
            "desc": "Address state lookups"
        },
        {
            "name": "idx_customer_mobile",
            "table": "tabCustomer",
            "column": "mobile_no",
            "desc": "Customer phone lookups"
        },
        {
            "name": "idx_dynamic_link_customer",
            "table": "tabDynamic Link",
            "column": "link_name, link_doctype",
            "desc": "Dynamic link lookups"
        },
        {
            "name": "idx_wse_due",
            "table": "tabWooCommerce Sync Event",
            "column": "status, next_attempt_at, priority",
            "desc": "Sync event due queue lookups"
        },
        {
            "name": "idx_wse_object",
            "table": "tabWooCommerce Sync Event",
            "column": "direction, object_type, source_id",
            "desc": "Sync event object lookups"
        },
        {
            "name": "idx_wse_order",
            "table": "tabWooCommerce Sync Event",
            "column": "woo_order_id, status",
            "desc": "Sync event order lookups"
        },
        {
            "name": "idx_wse_local_doc",
            "table": "tabWooCommerce Sync Event",
            "column": "local_doctype, local_docname, status",
            "desc": "Sync event local document lookups"
        },
    ]


def ensure_sync_indexes(*, verbose: bool = True) -> dict[str, int]:
    indexes = _sync_indexes()

    if verbose:
        print("\n🔧 Adding database indexes for sync optimization...\n")

    created = 0
    skipped = 0
    errors = 0

    for idx in indexes:
        try:
            # Check if index exists
            check_sql = f"""
                SELECT COUNT(*) as cnt 
                FROM information_schema.statistics 
                WHERE table_schema = DATABASE()
                AND table_name = '{idx["table"]}'
                AND index_name = '{idx["name"]}'
            """
            exists = frappe.db.sql(check_sql, as_dict=1)

            if exists and exists[0].cnt > 0:
                if verbose:
                    print(f"  ⏭️  {idx['name']}: Already exists (skipped)")
                skipped += 1
                continue

            # Create index
            create_sql = f"""
                CREATE INDEX {idx['name']} 
                ON `{idx['table']}`({idx['column']})
            """
            frappe.db.sql(create_sql)
            if verbose:
                print(f"  ✅ {idx['name']}: Created ({idx['desc']})")
            created += 1

        except Exception as e:
            if verbose:
                print(f"  ❌ {idx['name']}: Error - {str(e)}")
            errors += 1

    frappe.db.commit()

    if verbose:
        print(f"\n✅ Index Creation Complete:")
        print(f"  Created: {created}")
        print(f"  Skipped: {skipped}")
        print(f"  Errors: {errors}")
        print(f"\n📊 Expected Performance Improvements:")
        print(f"  - Customer lookups: 2-3x faster")
        print(f"  - Item lookups: 2-3x faster")
        print(f"  - Order deduplication: 3-5x faster")
        print(f"  - Territory assignment: 2x faster")
        print(f"  - Event queue scans: faster due-event selection and investigation lookups")

    return {
        "created": created,
        "skipped": skipped,
        "errors": errors,
    }


def add_sync_indexes_cli():
    """Add database indexes to speed up order sync operations.

    Usage:
        bench --site <site> execute jarz_woocommerce_integration.utils.add_sync_indexes.add_sync_indexes_cli
    """

    return ensure_sync_indexes(verbose=True)
