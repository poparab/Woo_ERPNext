import unittest

from jarz_woocommerce_integration.services.invoice_display_name import keep_woo_display_name


class _Doc(dict):
	def __getattr__(self, key):
		return self.get(key)

	def __setattr__(self, key, value):
		self[key] = value


class TestKeepWooDisplayName(unittest.TestCase):
	def test_refetched_billing_name_is_replaced_by_woo_display_name(self):
		# Order 16550: ERPNext re-fetched the Customer's billing name.
		doc = _Doc(customer_name="على ممدوح", woo_order_display_name="على ممدوح على ابو الوفا")
		keep_woo_display_name(doc, "validate")
		self.assertEqual(doc.customer_name, "على ممدوح على ابو الوفا")

	def test_non_woo_invoice_keeps_its_customer_name(self):
		doc = _Doc(customer_name="Walk-in", woo_order_display_name="")
		keep_woo_display_name(doc, "validate")
		self.assertEqual(doc.customer_name, "Walk-in")

	def test_whitespace_only_display_name_is_ignored(self):
		doc = _Doc(customer_name="Walk-in", woo_order_display_name="   ")
		keep_woo_display_name(doc, "validate")
		self.assertEqual(doc.customer_name, "Walk-in")


if __name__ == "__main__":
	unittest.main()
