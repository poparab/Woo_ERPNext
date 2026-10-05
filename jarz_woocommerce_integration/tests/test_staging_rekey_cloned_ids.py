"""Staging re-key of production-origin Woo ids.

The utility moves cloned production store ids to ``id + OFFSET`` so the demo store's
counters can never collide with them (2026-10-05/06: a demo customer overwrote a cloned
customer, and demo orders re-pointed six cloned invoices). These tests pin the guard,
the keep/move decision and the SQL it issues, against a fake ``frappe.db``.
"""

import unittest
import unittest.mock
from datetime import datetime
from types import SimpleNamespace

import frappe

from jarz_woocommerce_integration.utils import staging_rekey_cloned_ids as rekey

OFFSET = rekey.OFFSET


def _row(**kwargs):
	return frappe._dict(kwargs)


class _FakeDB:
	"""Answers the three SELECTs ``_plan`` issues and records every UPDATE."""

	def __init__(self, *, invoices=(), maps=(), customers=(), aliases=(), base_url="https://demo.orderjarz.com"):
		self.invoices = [_row(**r) for r in invoices]
		self.maps = [_row(**r) for r in maps]
		self.customers = [_row(**r) for r in customers]
		self.aliases = [_row(**r) for r in aliases]
		self.base_url = base_url
		self.updates = []
		self.set_values = []
		self.commits = 0

	def get_single_value(self, doctype, field):
		return self.base_url

	def has_column(self, doctype, column):
		return True

	def exists(self, doctype, name):
		return any(r.name == name for r in self.invoices) or name == "NATIVE-INV"

	def sql(self, query, params=None, as_dict=False):
		text = " ".join(query.split())
		if text.startswith("UPDATE"):
			self.updates.append((text, params))
			return None
		cutoff = (params or {}).get("cutoff") if isinstance(params, dict) else None

		def before(rows):
			return [r for r in rows if not cutoff or r.creation < cutoff]

		if "FROM `tabSales Invoice`" in text:
			return [r for r in before(self.invoices) if 0 < r.woo_order_id < OFFSET]
		if "FROM `tabWooCommerce Order Map`" in text:
			return [r for r in self.maps if 0 < r.woo_order_id < OFFSET]
		if "FROM `tabCustomer` WHERE IFNULL(woo_customer_id" in text:
			return [r for r in before(self.customers) if 0 < int(r.woo_customer_id) < OFFSET]
		if "woo_customer_id_aliases" in text:
			return list(self.aliases)
		raise AssertionError(f"unexpected SQL: {text}")

	def set_value(self, *args, **kwargs):
		self.set_values.append((args, kwargs))

	def commit(self):
		self.commits += 1


CUTOFF = datetime(2026, 8, 17)
OLD = datetime(2026, 7, 1)
NEW = datetime(2026, 9, 1)


class TestGuard(unittest.TestCase):
	def _run_guard(self, host, base_url="https://demo.orderjarz.com"):
		fake = _FakeDB(base_url=base_url)
		with unittest.mock.patch.object(rekey, "_host_name", return_value=host), \
			unittest.mock.patch.object(frappe, "db", fake):
			rekey._guard()

	def test_refuses_production_host(self):
		with self.assertRaisesRegex(RuntimeError, "production"):
			self._run_guard("https://erp.orderjarz.com")

	def test_refuses_host_without_staging_marker(self):
		with self.assertRaisesRegex(RuntimeError, "staging marker"):
			self._run_guard("https://example.com")

	def test_refuses_when_store_is_production(self):
		with self.assertRaisesRegex(RuntimeError, "non-production store"):
			self._run_guard("https://erpstg.orderjarz.com", base_url="https://orderjarz.com")

	def test_staging_with_demo_store_passes(self):
		self._run_guard("https://erpstg.orderjarz.com")


class TestKeepRules(unittest.TestCase):
	def test_order_kept_only_when_store_meta_names_this_invoice(self):
		store = {16181: {"meta_data": [{"key": "erpnext_sales_invoice", "value": "ACC-SINV-1"}]}}
		self.assertTrue(rekey._order_keeps(store, 16181, "ACC-SINV-1"))
		self.assertFalse(rekey._order_keeps(store, 16181, "ACC-SINV-CLONED"))
		self.assertFalse(rekey._order_keeps(store, 16999, "ACC-SINV-1"))

	def test_customer_kept_on_phone_or_email_match(self):
		store = {5300: {"billing": {"phone": "+20 112 345 6789"}, "email": "A@x.com"}}
		self.assertTrue(rekey._customer_keeps(store, 5300, "01123456789", ""))
		self.assertTrue(rekey._customer_keeps(store, 5300, "", "a@x.com"))
		self.assertFalse(rekey._customer_keeps(store, 5300, "01000000000", "b@x.com"))
		self.assertFalse(rekey._customer_keeps(store, 5301, "01123456789", "a@x.com"))

	def test_aliases_move_only_ids_below_offset(self):
		moved = rekey._rekey_aliases(f"5313,{OFFSET + 7},abc")
		self.assertIn(str(5313 + OFFSET), moved)
		self.assertIn(str(OFFSET + 7), moved)
		self.assertNotIn(str(OFFSET + 7 + OFFSET), moved)


class TestPlanAndApply(unittest.TestCase):
	def _fake(self):
		return _FakeDB(
			invoices=[
				{"name": "CLONED-1", "woo_order_id": 16169, "creation": OLD},
				{"name": "CLONED-PUSHED", "woo_order_id": 15941, "creation": OLD},
				{"name": "NATIVE-INV", "woo_order_id": 16181, "creation": NEW},
			],
			maps=[
				{"name": "MAP-CLONED", "woo_order_id": 16169, "invoice": "CLONED-1", "creation": OLD},
				# A cloned map row staging re-pointed at an invoice it created.
				{"name": "MAP-REUSED", "woo_order_id": 16181, "invoice": "NATIVE-INV", "creation": OLD},
				{"name": "MAP-ORPHAN-OLD", "woo_order_id": 15000, "invoice": None, "creation": OLD},
				{"name": "MAP-ORPHAN-NEW", "woo_order_id": 16300, "invoice": None, "creation": NEW},
			],
			customers=[
				{"name": "CUST-CLONED", "woo_customer_id": "5313", "mobile_no": "0111", "email_id": "", "creation": OLD},
				{"name": "CUST-PUSHED", "woo_customer_id": "5300", "mobile_no": "01123456789", "email_id": "", "creation": OLD},
				{"name": "CUST-NATIVE", "woo_customer_id": "5320", "mobile_no": "0122", "email_id": "", "creation": NEW},
			],
		)

	def _store(self, client, resource, cutoff):
		if resource == "orders":
			return {15941: {"meta_data": [{"key": "erpnext_sales_invoice", "value": "CLONED-PUSHED"}]},
				16169: {"meta_data": [{"key": "erpnext_sales_invoice", "value": "SOME-DEMO-TEST"}]}}
		return {5300: {"billing": {"phone": "01123456789"}, "email": ""},
			5313: {"billing": {"phone": "01120261005"}, "email": "test@orderjarz.local"}}

	def test_plan_with_cutoff_keeps_demo_links_and_follows_invoice(self):
		fake = self._fake()
		with unittest.mock.patch.object(frappe, "db", fake), \
			unittest.mock.patch.object(rekey, "_store_records_since", self._store):
			plan = rekey._plan(CUTOFF, client=object())
		self.assertEqual(plan["Sales Invoice"], ["CLONED-1"])
		self.assertEqual(sorted(plan["WooCommerce Order Map"]), ["MAP-CLONED", "MAP-ORPHAN-OLD"])
		self.assertEqual(plan["Customer"], ["CUST-CLONED"])
		self.assertEqual(sorted(plan["kept"]), ["CLONED-PUSHED", "CUST-PUSHED"])

	def test_plan_without_cutoff_moves_everything(self):
		fake = self._fake()
		with unittest.mock.patch.object(frappe, "db", fake):
			plan = rekey._plan(None, client=None)
		self.assertEqual(sorted(plan["Sales Invoice"]), ["CLONED-1", "CLONED-PUSHED", "NATIVE-INV"])
		self.assertEqual(len(plan["WooCommerce Order Map"]), 4)
		self.assertEqual(len(plan["Customer"]), 3)
		self.assertEqual(plan["kept"], [])

	def _run(self, fake, **kwargs):
		with unittest.mock.patch.object(rekey, "_host_name", return_value="https://erpstg.orderjarz.com"), \
			unittest.mock.patch.object(frappe, "db", fake), \
			unittest.mock.patch.object(frappe, "clear_cache") as clear_cache:
			report = rekey.run(**kwargs)
		return report, clear_cache

	def test_dry_run_writes_nothing(self):
		fake = self._fake()
		report, clear_cache = self._run(fake, dry_run=True)
		self.assertTrue(report["dry_run"])
		self.assertEqual(report["rekey"], {"Sales Invoice": 3, "WooCommerce Order Map": 4, "Customer": 3})
		self.assertEqual(fake.updates, [])
		self.assertEqual(fake.commits, 0)
		clear_cache.assert_not_called()

	def test_apply_updates_by_name_with_floor_guard_and_no_modified(self):
		fake = self._fake()
		report, clear_cache = self._run(fake, dry_run="false")
		self.assertFalse(report["dry_run"])
		tables = [text.split("`")[1] for text, _ in fake.updates]
		self.assertEqual(tables, ["tabSales Invoice", "tabWooCommerce Order Map", "tabCustomer"])
		for text, params in fake.updates:
			self.assertIn(f"+ {OFFSET}", text)
			self.assertIn(f"BETWEEN 1 AND {OFFSET - 1}", text)  # a re-run never moves twice
			self.assertIn("WHERE name IN (", text)
			self.assertNotIn("modified", text)
		self.assertIn("AS CHAR", fake.updates[2][0])  # varchar column stays a string
		self.assertEqual(fake.commits, 1)
		clear_cache.assert_called_once()

	def test_apply_after_refresh_is_a_full_non_dry_run(self):
		with unittest.mock.patch.object(rekey, "run", return_value={"ok": 1}) as run:
			self.assertEqual(rekey.apply_after_refresh(), {"ok": 1})
		run.assert_called_once_with(dry_run=False)


if __name__ == "__main__":
	unittest.main()
