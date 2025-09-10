"""Compatibility stub forwarding to nested DocType controller.

Frappe resolves controller imports at this top-level path. The real implementation
now lives in the nested package to avoid duplication. Keep this thin stub so any
framework dynamic imports continue to succeed.
"""

from jarz_woocommerce_integration.jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (  # noqa: F401,E501
    WooCommerceSettings,
)
"""Controller for the single DocType WooCommerce Settings.

Provides a convenience accessor `WooCommerceSettings.get_settings()` used
throughout the integration services layer. Also exposes a helper to obtain
the decrypted consumer secret without duplicating password logic everywhere.
"""

from __future__ import annotations

import frappe
from frappe.model.document import Document
from frappe.utils.password import get_decrypted_password


class WooCommerceSettings(Document):  # type: ignore[misc]
	"""Single doctype wrapper with helper utilities."""

	@staticmethod
	def get_settings() -> "WooCommerceSettings":
		"""Return the (singleton) settings document, creating it if missing."""
		return frappe.get_single("WooCommerce Settings")  # type: ignore[return-value]

	def get_consumer_secret(self) -> str | None:
		"""Return decrypted consumer secret or None if unset."""
		try:
			if not self.name:
				return None
			return get_decrypted_password("WooCommerce Settings", self.name, "consumer_secret")
		except Exception:  # noqa: BLE001
			return None

	def validate(self):  # noqa: D401  (frappe hook)
		# Basic normalization: strip trailing slashes in base_url
		if getattr(self, "base_url", None):
			self.base_url = self.base_url.rstrip("/")
