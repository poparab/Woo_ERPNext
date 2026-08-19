"""Point every "… Large" Item at the variable product that owns its variation.

Each jar flavour exists twice in WooCommerce: the live **variable** product
("Strawberry cheesecake jar", size Medium/Large) and a **legacy simple** product
("Strawberry cheesecake jar Large") left over from before sizes became
variations. Our Medium Items map to the variable product; every Large Item was
left pointing at the legacy simple one while carrying a ``woo_variation_id``
that belongs to the variable product — a pair that cannot exist in the store.

Measured on production before writing this:

* 22 Items carry a variation mapping. 11 (every Medium) are consistent; 11
  (every Large) are not. The split is exactly by size, so this is one historic
  mapping mistake rather than a scatter of typos.
* The legacy simple products are dead: all of them ``catalog_visibility=hidden``
  and ``stock_status=outofstock``. Nothing but us has sent traffic to them.
* Across the last 100 website orders, **all 90** Large lines use the variable
  product and its Large variation (369/13779, 367/13782, 371/13776, …). The
  store itself settles which mapping is canonical.
* 93 POS-origin orders have pushed a Large jar historically, but **zero** since
  outbound began emitting ``variation_id`` (2026-08-19). Before that we sent the
  legacy product alone — a valid line against the wrong, hidden product; after
  it we would send an impossible product/variation pair. This lands before the
  first order could hit that.

The variation ids were already correct, so only ``woo_product_id`` moves. Each
row below was verified against the live store: the variation was fetched and its
``parent_id``/owner confirmed, and 9 of the 11 additionally appear in real
website orders under the product named here.

Idempotent, and deliberately narrow: an Item is rewritten only when it still
carries the exact wrong product id recorded here, so a hand-fix or a later
remapping is never clobbered.
"""

import frappe

#: woo_variation_id -> (wrong legacy product id, correct variable product id, item)
#: The Item name is for the log only; matching is by variation id, which is
#: stable, unique, and already correct on every row.
LARGE_VARIATION_OWNERS = {
    "13776": ("2260", "371", "Strawberry Large"),
    "13779": ("2259", "369", "Blueberry Large"),
    "13782": ("2258", "367", "Chocolate Hazelnut Large"),
    "13772": ("2261", "2251", "Redvelvet Large"),
    "13766": ("2257", "217", "Lotus Large"),
    "13812": ("2351", "2284", "Mango Large"),
    "13825": ("2353", "2286", "Pistachio Large"),
    "13801": ("11166", "11162", "Molten Large"),
    "13805": ("11144", "11140", "Tiramisu Large"),
    "13797": ("12583", "12585", "Hibiscus Kunafa Large"),
    "13816": ("12594", "12597", "Qamar El Deen ( Apricot ) Kunafa Large"),
}


def execute():
    if not frappe.db.table_exists("Item"):
        return
    columns = set(frappe.db.get_table_columns("Item") or [])
    if "woo_variation_id" not in columns or "woo_product_id" not in columns:
        # The Woo custom fields are not installed on this bench; nothing to fix.
        return

    updated, skipped, absent = [], [], []
    for variation_id, (wrong_product, correct_product, label) in LARGE_VARIATION_OWNERS.items():
        rows = frappe.get_all(
            "Item",
            filters={"woo_variation_id": variation_id},
            fields=["name", "woo_product_id"],
        )
        if not rows:
            absent.append(f"{label} (variation {variation_id})")
            continue
        for row in rows:
            current = str(row.get("woo_product_id") or "").strip()
            # Tolerate the composite "<product>:<variation>" form the mapping
            # helper also accepts, so both shapes are corrected the same way.
            head, _, tail = current.partition(":")
            if head.strip() != wrong_product:
                skipped.append(f"{row['name']} -> {current or 'unset'}")
                continue
            desired = f"{correct_product}:{tail.strip()}" if tail.strip() else correct_product
            frappe.db.set_value(
                "Item", row["name"], "woo_product_id", desired, update_modified=False
            )
            updated.append(f"{row['name']}: {current} -> {desired}")

    frappe.db.commit()

    summary = [
        f"remapped {len(updated)} Item(s) to their variable product",
        f"left alone {len(skipped)} (product id no longer the known-wrong value)",
        f"not present on this site: {len(absent)}",
    ]
    for line in updated:
        summary.append(f"  fixed   {line}")
    for line in skipped:
        summary.append(f"  skipped {line}")
    for line in absent:
        summary.append(f"  missing {line}")
    message = "\n".join(summary)

    print("[jarz_woocommerce_integration] fix_large_variation_product_map\n" + message)
    if updated or skipped:
        try:
            frappe.log_error(
                title="WooCommerce: Large variation product remap",
                message=message,
            )
        except Exception:  # noqa: BLE001
            pass
