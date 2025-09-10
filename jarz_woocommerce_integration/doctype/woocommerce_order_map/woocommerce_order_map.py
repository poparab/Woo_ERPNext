"""Compatibility stub forwarding to nested DocType controller."""

from jarz_woocommerce_integration.jarz_woocommerce_integration.doctype.woocommerce_order_map.woocommerce_order_map import (  # noqa: F401,E501
    WooCommerceOrderMap,
)
import frappe
from frappe.model.document import Document

class WooCommerceOrderMap(Document):
    pass
