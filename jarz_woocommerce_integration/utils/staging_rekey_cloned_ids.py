"""Move production-origin WooCommerce ids on a STAGING clone out of the demo store's id range.

Why this exists
---------------
Staging is refreshed as a copy of production, so its Sales Invoices, Order Map rows and
Customers carry *production store* ids. Staging itself talks to a different store
(``demo.orderjarz.com``) whose own order and customer counters run through the same
numbers. Every new demo order or customer therefore lands on an id a cloned record
already holds, and inbound sync binds the demo record onto that real cloned record:

* 2026-10-05: a test customer took demo id 5313 and overwrote cloned "Eman mohamed".
* 2026-10-06: re-enabling the scheduler re-pointed the delivery addresses of six
  delivered cloned invoices (ACC-SINV-2026-17130 ...) at demo-order addresses.

Raising the demo store's counters only buys time: the next refresh brings a higher
production ceiling. The cloned ids point at orders in the *production* store, so they
mean nothing against the demo store anyway. Moving them to ``id + OFFSET`` makes a
collision impossible however far either counter grows, keeps every cloned link one
subtraction away from the original, and needs nothing on the store.

What it touches
---------------
Only the identity columns inbound sync matches on: ``Sales Invoice.woo_order_id``,
``WooCommerce Order Map.woo_order_id`` and ``Customer.woo_customer_id`` (+ aliases).
Deliberately untouched: ``woo_order_number`` (what staff search by; never matched on),
product / variation / bundle ids (the demo catalog shares them), the Sync Event / Sync
Log audit history, and jarz_pos tables. ``modified`` is not bumped and no doc events fire.

Which rows move
---------------
* Straight after a refresh (``cutoff=None``): every id below ``OFFSET`` — all of it is
  production-origin.
* On a staging site that has run since its clone (``cutoff=<clone time>``): rows created
  before the cutoff, EXCEPT ones staging itself later bound to a genuine demo record.
  Between a clone and now, staging's outbound sync pushes cloned customers and walk-in
  invoices that had no Woo id and stamps the demo store's id onto them. Those are real
  demo links and must stay. The store decides: a cloned row keeps its id only when the
  demo record with that id was created after the cutoff AND is the same entity (an
  order whose ``erpnext_sales_invoice`` meta names this invoice; a customer whose phone
  or email matches). Order Map rows follow the invoice they link to.

Idempotent: ids already at or above ``OFFSET`` are never moved again.

Run (staging only; the guard refuses anything else)::

    bench --site frontend execute \
        jarz_woocommerce_integration.utils.staging_rekey_cloned_ids.run \
        --kwargs '{"dry_run": false}'
"""

from __future__ import annotations

import re
from typing import Any

import frappe
from frappe.utils import get_datetime

#: Re-keyed id = original production id + OFFSET. Fits int(11) (max 2,147,483,647)
#: for any production id below ~1.1 billion, and no store counter will ever reach it.
OFFSET = 1_000_000_000

ALIAS_FIELD = "woo_customer_id_aliases"
CHUNK = 500

PRODUCTION_HOST_MARKERS = ("erp.orderjarz.com",)
STAGING_HOST_MARKERS = ("erpstg",)
NON_PRODUCTION_STORE_MARKERS = ("demo.", "staging", "stg.", "-stg", "test.", "localhost", "127.0.0.1")


def _host_name() -> str:
	# A seam for tests: patching ``frappe.local.conf`` directly is unsafe, because
	# mock restores an attribute of a werkzeug Local by deleting it, which unbinds
	# ``frappe.conf`` for every later test in the run.
	return str(frappe.local.conf.get("host_name") or "")


def _guard() -> None:
	host = _host_name().lower()
	if any(marker in host for marker in PRODUCTION_HOST_MARKERS):
		raise RuntimeError(f"REFUSED: {host!r} is production.")
	if not any(marker in host for marker in STAGING_HOST_MARKERS):
		raise RuntimeError(f"REFUSED: host_name {host!r} carries no staging marker {STAGING_HOST_MARKERS!r}.")
	base_url = str(frappe.db.get_single_value("WooCommerce Settings", "base_url") or "").lower()
	if not any(marker in base_url for marker in NON_PRODUCTION_STORE_MARKERS):
		raise RuntimeError(
			f"REFUSED: WooCommerce base_url {base_url!r} is not a non-production store; re-keying "
			"is only meaningful when staging talks to a different store than production."
		)


def _digits(value: Any) -> str:
	digits = re.sub(r"\D", "", str(value or ""))
	return digits[-10:] if len(digits) >= 10 else digits


def _store_records_since(client: Any, resource: str, cutoff: Any) -> dict[int, dict]:
	"""Demo-store records created after ``cutoff``, keyed by id (newest-first scan)."""
	found: dict[int, dict] = {}
	page = 1
	while True:
		params = {"per_page": 100, "page": page, "orderby": "id", "order": "desc"}
		params["status" if resource == "orders" else "role"] = "any" if resource == "orders" else "all"
		rows = client.get(resource, params=params) or []
		if not rows:
			break
		reached_older = False
		for row in rows:
			created = row.get("date_created_gmt") or row.get("date_created")
			if created and get_datetime(str(created).rstrip("Z")) < cutoff:
				reached_older = True
				continue
			found[int(row["id"])] = row
		if reached_older or len(rows) < 100:
			break
		page += 1
	return found


def _order_keeps(store_orders: dict[int, dict], woo_id: int, invoice: str) -> bool:
	order = store_orders.get(woo_id)
	if not order:
		return False
	for meta in order.get("meta_data") or []:
		if meta.get("key") == "erpnext_sales_invoice" and str(meta.get("value") or "") == invoice:
			return True
	return False


def _customer_keeps(store_customers: dict[int, dict], woo_id: int, mobile: Any, email: Any) -> bool:
	customer = store_customers.get(woo_id)
	if not customer:
		return False
	store_phone = _digits((customer.get("billing") or {}).get("phone"))
	store_email = str(customer.get("email") or "").strip().lower()
	return bool(
		(store_phone and store_phone == _digits(mobile))
		or (store_email and store_email == str(email or "").strip().lower())
	)


def _plan(cutoff: Any, client: Any | None) -> dict[str, list[str]]:
	"""Names to re-key per doctype, plus the names kept as genuine demo links."""
	low = f"BETWEEN 1 AND {OFFSET - 1}"
	created = " AND creation < %(cutoff)s" if cutoff else ""
	args = {"cutoff": cutoff}
	store_orders = _store_records_since(client, "orders", cutoff) if cutoff else {}
	store_customers = _store_records_since(client, "customers", cutoff) if cutoff else {}

	plan: dict[str, list[str]] = {"Sales Invoice": [], "WooCommerce Order Map": [], "Customer": [], "kept": []}
	rekeyed_invoices: set[str] = set()
	for row in frappe.db.sql(
		f"SELECT name, woo_order_id FROM `tabSales Invoice` WHERE woo_order_id {low}{created}", args, as_dict=True
	):
		if cutoff and _order_keeps(store_orders, int(row.woo_order_id), row.name):
			plan["kept"].append(row.name)
			continue
		plan["Sales Invoice"].append(row.name)
		rekeyed_invoices.add(row.name)

	link = "erpnext_sales_invoice"
	for row in frappe.db.sql(
		f"SELECT name, `{link}` AS invoice, creation FROM `tabWooCommerce Order Map` WHERE woo_order_id {low}",
		as_dict=True,
	):
		if row.invoice and frappe.db.exists("Sales Invoice", row.invoice):
			# Follow the invoice: a cloned map row that staging re-pointed at an invoice
			# it created keeps its demo id, exactly like that invoice does.
			if row.invoice in rekeyed_invoices or (
				not cutoff and row.invoice not in plan["kept"]
			):
				plan["WooCommerce Order Map"].append(row.name)
		elif not cutoff or get_datetime(row.creation) < get_datetime(cutoff):
			plan["WooCommerce Order Map"].append(row.name)

	for row in frappe.db.sql(
		f"SELECT name, woo_customer_id, mobile_no, email_id FROM `tabCustomer` "
		f"WHERE IFNULL(woo_customer_id, '') NOT IN ('', '0') "
		f"AND CAST(woo_customer_id AS UNSIGNED) {low}{created}",
		args,
		as_dict=True,
	):
		if cutoff and _customer_keeps(store_customers, int(row.woo_customer_id), row.mobile_no, row.email_id):
			plan["kept"].append(row.name)
			continue
		plan["Customer"].append(row.name)
	return plan


def _rekey_aliases(value: Any) -> str:
	from jarz_woocommerce_integration.utils.customer_woo_id import (
		format_woo_id_aliases,
		parse_woo_id_aliases,
	)

	moved = []
	for alias in parse_woo_id_aliases(value):
		try:
			number = int(alias)
		except (TypeError, ValueError):
			moved.append(alias)
			continue
		moved.append(str(number + OFFSET) if 0 < number < OFFSET else alias)
	return format_woo_id_aliases(moved)


def _apply(doctype: str, column: str, names: list[str], as_int: bool) -> None:
	new_value = f"`{column}` + {OFFSET}" if as_int else f"CAST(CAST(`{column}` AS UNSIGNED) + {OFFSET} AS CHAR)"
	for start in range(0, len(names), CHUNK):
		chunk = names[start:start + CHUNK]
		placeholders = ", ".join(["%s"] * len(chunk))
		# Raw UPDATE on purpose: no doc events (nothing may push to the store for this)
		# and no `modified` bump (cloned rows keep their real history). The id bound in
		# the WHERE keeps a re-run from moving anything twice.
		frappe.db.sql(
			f"UPDATE `tab{doctype}` SET `{column}` = {new_value} "
			f"WHERE name IN ({placeholders}) AND CAST(`{column}` AS UNSIGNED) BETWEEN 1 AND {OFFSET - 1}",
			tuple(chunk),
		)


def run(dry_run: bool = True, cutoff: str | None = None) -> dict[str, Any]:
	"""Re-key production-origin store ids on staging. Returns counts (and samples).

	``dry_run`` (default True) only plans. Omit ``cutoff`` straight after a refresh;
	pass the clone time on a site that has run since, so staging's own demo links stay.
	"""
	if isinstance(dry_run, str):
		dry_run = dry_run.strip().lower() not in {"0", "false", "no", "off"}
	_guard()
	cutoff_dt = get_datetime(cutoff) if cutoff else None

	client = None
	if cutoff_dt:
		from jarz_woocommerce_integration.doctype.woocommerce_settings.woocommerce_settings import (
			WooCommerceSettings,
		)
		from jarz_woocommerce_integration.services.outbound_sync import _build_client

		client = _build_client(WooCommerceSettings.get_settings())

	plan = _plan(cutoff_dt, client)
	alias_rows = frappe.db.sql(
		f"SELECT name, `{ALIAS_FIELD}` AS aliases FROM `tabCustomer` WHERE IFNULL(`{ALIAS_FIELD}`, '') != ''",
		as_dict=True,
	) if frappe.db.has_column("Customer", ALIAS_FIELD) else []
	kept = set(plan["kept"])
	alias_changes = {
		row.name: _rekey_aliases(row.aliases)
		for row in alias_rows
		if row.name not in kept and _rekey_aliases(row.aliases) != row.aliases
	}

	report = {
		"dry_run": bool(dry_run),
		"cutoff": cutoff,
		"offset": OFFSET,
		"rekey": {key: len(value) for key, value in plan.items() if key != "kept"},
		"aliases": len(alias_changes),
		"kept_as_demo_links": plan["kept"][:50],
		"kept_count": len(plan["kept"]),
	}
	if dry_run:
		return report

	_apply("Sales Invoice", "woo_order_id", plan["Sales Invoice"], as_int=True)
	_apply("WooCommerce Order Map", "woo_order_id", plan["WooCommerce Order Map"], as_int=True)
	_apply("Customer", "woo_customer_id", plan["Customer"], as_int=False)
	for name, value in alias_changes.items():
		frappe.db.set_value("Customer", name, ALIAS_FIELD, value, update_modified=False)
	frappe.db.commit()
	# Woo-id lookups memoise results; drop them so no sync answers from a stale cache.
	frappe.clear_cache()
	return report


def apply_after_refresh() -> dict[str, Any]:
	"""The post-refresh step: re-key everything, no cutoff (every row is production-origin).

	Argument-free so the refresh runbooks can call it over SSH without nested quoting::

	    bench --site frontend execute \
	        jarz_woocommerce_integration.utils.staging_rekey_cloned_ids.apply_after_refresh

	Run it after WooCommerce Settings point at the demo store and BEFORE the scheduler
	is enabled; the guard refuses until base_url is a non-production store.
	"""
	return run(dry_run=False)
