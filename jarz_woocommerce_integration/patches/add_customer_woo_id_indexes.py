import frappe

# Every inbound customer/order sync resolves the Woo account to a Customer with
#   woo_customer_id = N           (step zero of find_customer_by_woo_id), and
#   woo_customer_id_aliases LIKE '%,N,%'   (accounts absorbed by a merge).
# Neither column was indexed, so each lookup scanned all of tabCustomer:
# ~22 ms apiece on production on 2026-10-05, several per event, ~3,000 events
# a day, at a time the box was CPU-throttled.
#
# woo_customer_id is a Data column: its index is declared as search_index on the
# Custom Field, so Frappe keeps it across migrates (an index Frappe does not
# know about on a non-text column is DROPPED by the next schema sync). This
# patch only makes sure it exists now, under the name Frappe itself would use.
#
# woo_customer_id_aliases is TEXT, which Frappe never indexes or drops. A short
# prefix index serves the `woo_customer_id_aliases > ''` range the lookup now
# leads with: only Customers that actually hold an alias (1 of ~7,700 today)
# are read, and the LIKE runs on those alone.

ALIAS_INDEX = "woo_customer_id_aliases_prefix"


def _columns() -> set[str]:
    return {row[0] for row in frappe.db.sql("SHOW COLUMNS FROM `tabCustomer`")}


def _indexed(column: str) -> bool:
    return bool(frappe.db.sql("SHOW INDEX FROM `tabCustomer` WHERE Column_name = %s", (column,)))


def execute():
    columns = _columns()
    if "woo_customer_id" in columns and not _indexed("woo_customer_id"):
        frappe.db.sql_ddl("ALTER TABLE `tabCustomer` ADD INDEX `woo_customer_id_index` (`woo_customer_id`)")
    if "woo_customer_id_aliases" in columns and not _indexed("woo_customer_id_aliases"):
        frappe.db.sql_ddl(
            f"ALTER TABLE `tabCustomer` ADD INDEX `{ALIAS_INDEX}` (`woo_customer_id_aliases`(32))"
        )
