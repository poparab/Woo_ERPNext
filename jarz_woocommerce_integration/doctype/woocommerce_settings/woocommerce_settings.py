from __future__ import annotations

"""Compatibility wrapper for WooCommerce Settings DocType controller.

This module defines the controller so Frappe can import it at the standard path.
The actual implementation is equivalent to the nested module version.
"""

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
