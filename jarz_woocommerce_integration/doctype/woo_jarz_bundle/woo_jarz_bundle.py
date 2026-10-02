import frappe
from frappe import _
from frappe.model.document import Document


class WooJarzBundle(Document):
    """Container DocType for Woo bundle definitions scoped to this integration."""

    def validate(self):
        self._ensure_erpnext_item_is_unique()

    def _ensure_erpnext_item_is_unique(self):
        """One Woo bundle per ERPNext bundle item.

        The inbound item-edit gate names a bundle by its ERPNext item
        (``order_sync._bundle_link_items``), because the POS and the Woo
        BundleProcessor record the same bundle under different record names. Two
        Woo bundles sharing one item would then look identical, and swapping one
        for the other on the store (different price or free shipping) would be
        read as "no edit". Production had none when this was added (2026-10-02).
        """
        item = (self.get("erpnext_item") or "").strip()
        if not item:
            return
        other = frappe.db.get_value(
            "Woo Jarz Bundle", {"erpnext_item": item, "name": ["!=", self.name or ""]}, "name"
        )
        if other:
            frappe.throw(
                _("ERPNext item {0} is already used by Woo bundle {1}. Each Woo bundle needs its own ERPNext bundle item.").format(
                    frappe.bold(item), frappe.bold(other)
                ),
                title=_("Duplicate bundle item"),
            )
