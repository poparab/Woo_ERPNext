from __future__ import annotations
import frappe
from frappe.model.document import Document
from frappe.utils.password import get_decrypted_password

class WooCommerceSettings(Document):
    @staticmethod
    def get_settings() -> "WooCommerceSettings":
        return frappe.get_single("WooCommerce Settings")  # type: ignore[return-value]

    def get_consumer_secret(self) -> str | None:
        try:
            return get_decrypted_password("WooCommerce Settings", self.name, "consumer_secret")
        except Exception:  # noqa: BLE001
            return None

    def validate(self):
        if getattr(self, "base_url", None):
            self.base_url = self.base_url.rstrip("/")
