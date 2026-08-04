"""Cover for the three sync gaps found auditing the Woo <-> ERPNext update cycle.

1. Items added or removed on a Sales Invoice never reached the store
   (WooCommerce order 15808): outbound line items with no counterpart on the
   Woo order were computed and then discarded.
2. A shipping-address edit made on the website matched every stored hash, so
   the order was dropped as "unchanged" before any address code ran.
3. Cancelling a submitted invoice as the first half of an amendment pushed
   "cancelled" to the store, racing the replacement's own push.
"""

import copy
from types import SimpleNamespace
import unittest
import unittest.mock

from jarz_woocommerce_integration.services import order_sync, outbound_sync

from jarz_woocommerce_integration.tests.test_outbound_status_sync import DummyInvoice


def _cfg() -> outbound_sync.OutboundConfig:
    return outbound_sync.OutboundConfig(
        enable_customer_push=True,
        enable_order_push=True,
        payment_cod="cod",
        payment_instapay="instapay",
        payment_wallet="wallet",
        shipping_method_id="flat_rate",
        shipping_method_title="Shipping",
    )


def _desired_line(item_code: str, product_id: int, *, quantity: int = 1, meta=()):
    return {
        "product_id": product_id,
        "variation_id": None,
        "quantity": quantity,
        "name": item_code,
        "meta_data": [{"key": "erpnext_item_code", "value": item_code}, *meta],
    }


def _existing_line(line_id: int, product_id: int, *, item_code=None, quantity: int = 1, meta=()):
    meta_data = list(meta)
    if item_code:
        meta_data.insert(0, {"key": "erpnext_item_code", "value": item_code})
    return {
        "id": line_id,
        "product_id": product_id,
        "variation_id": 0,
        "quantity": quantity,
        "meta_data": meta_data,
    }


class OutboundLineItemSyncTests(unittest.TestCase):
    """Regression cover for Woo order 15808."""

    def _build_payload(self, desired_lines, existing_order, *, registered_bundle_ids=frozenset()):
        invoice = DummyInvoice(sales_invoice_state="Recieved")
        customer = SimpleNamespace(
            customer_name="Test Customer",
            woo_customer_id="88",
            email_id="test@example.com",
            mobile_no="01000000000",
            phone=None,
        )
        address = {"address_1": "Street 1", "email": "test@example.com", "phone": "01000000000"}

        with unittest.mock.patch.object(
                 outbound_sync, "_collect_line_items", return_value=(desired_lines, [])), \
             unittest.mock.patch.object(outbound_sync, "_compute_shipping_total", return_value=0), \
             unittest.mock.patch.object(
                 outbound_sync, "_get_registered_bundle_product_ids",
                 return_value=set(registered_bundle_ids)), \
             unittest.mock.patch.object(outbound_sync, "_build_customer_payload", return_value={
                 "billing": dict(address), "shipping": dict(address)}), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc", return_value=customer):
            return outbound_sync._build_order_payload(invoice, _cfg(), existing_order=existing_order)

    def test_item_added_in_erpnext_is_sent_without_an_id_so_woo_appends_it(self):
        existing_order = {
            "id": 15808,
            "status": "processing",
            "line_items": [_existing_line(900, 101, item_code="ITEM-A")],
        }

        payload = self._build_payload(
            [_desired_line("ITEM-A", 101), _desired_line("ITEM-NEW", 202)],
            existing_order,
        )

        by_product = {entry.get("product_id"): entry for entry in payload["line_items"]}
        self.assertEqual(by_product[101]["id"], 900)
        # The line that used to be computed and then discarded.
        self.assertNotIn("id", by_product[202])

    def test_item_removed_in_erpnext_is_deleted_on_woo(self):
        existing_order = {
            "id": 15808,
            "status": "processing",
            "line_items": [
                _existing_line(900, 101, item_code="ITEM-A"),
                _existing_line(901, 102, item_code="ITEM-B"),
            ],
        }

        payload = self._build_payload([_desired_line("ITEM-A", 101)], existing_order)

        removals = [entry for entry in payload["line_items"] if entry.get("quantity") == 0]
        self.assertEqual([entry["id"] for entry in removals], [901])

    def test_removal_quantity_is_a_real_integer(self):
        """WooCommerce tests ``0 === $item['quantity']`` — a string "0" will not delete."""
        existing_order = {
            "id": 15808,
            "status": "processing",
            "line_items": [
                _existing_line(900, 101, item_code="ITEM-A"),
                _existing_line(901, 102, item_code="ITEM-B"),
            ],
        }

        payload = self._build_payload([_desired_line("ITEM-A", 101)], existing_order)

        removal = next(entry for entry in payload["line_items"] if entry["id"] == 901)
        self.assertIsInstance(removal["quantity"], int)
        self.assertEqual(removal["quantity"], 0)

    def test_bundle_parent_referenced_by_a_child_is_never_deleted(self):
        """The parent line exists on Woo but is deliberately absent from the ERPNext
        payload, so the orphan check sees it as removed. Deleting it would strip the
        bundle from the order."""
        existing_order = {
            "id": 15808,
            "status": "processing",
            "line_items": [
                _existing_line(800, 12438),
                _existing_line(
                    801, 371, item_code="Strawberry Medium",
                    meta=[{"key": "_woosb_parent_id", "value": "12438"}],
                ),
            ],
        }

        payload = self._build_payload(
            [_desired_line(
                "Strawberry Medium", 371,
                meta=[{"key": "_woosb_parent_id", "value": "12438"}],
            )],
            existing_order,
        )

        removals = [entry for entry in payload["line_items"] if entry.get("quantity") == 0]
        self.assertEqual(removals, [], "bundle parent line 800 must survive")

    def test_bundle_parent_registered_in_woo_jarz_bundle_is_never_deleted(self):
        """Second, independent protection signal: no child carries _woosb_parent_id,
        but the product is a registered bundle."""
        existing_order = {
            "id": 15808,
            "status": "processing",
            "line_items": [
                _existing_line(800, 12438),
                _existing_line(801, 371, item_code="Strawberry Medium"),
            ],
        }

        payload = self._build_payload(
            [_desired_line("Strawberry Medium", 371)],
            existing_order,
            registered_bundle_ids={"12438"},
        )

        removals = [entry for entry in payload["line_items"] if entry.get("quantity") == 0]
        self.assertEqual(removals, [], "registered bundle parent 800 must survive")

    def test_unchanged_order_yields_neither_additions_nor_removals(self):
        existing_order = {
            "id": 15808,
            "status": "processing",
            "line_items": [_existing_line(900, 101, item_code="ITEM-A")],
        }

        payload = self._build_payload([_desired_line("ITEM-A", 101)], existing_order)

        self.assertEqual(len(payload["line_items"]), 1)
        self.assertEqual(payload["line_items"][0]["id"], 900)
        self.assertNotEqual(payload["line_items"][0]["quantity"], 0)

    def test_attach_existing_line_ids_reports_orphans(self):
        matched, added, orphaned = outbound_sync._attach_existing_line_ids(
            [_desired_line("ITEM-NEW", 202)],
            [_existing_line(900, 101, item_code="ITEM-A")],
        )

        self.assertEqual(matched, [])
        self.assertEqual(len(added), 1)
        self.assertEqual([entry["id"] for entry in orphaned], [900])


class InboundAddressChangeDetectionTests(unittest.TestCase):
    """A website-side address edit used to match every stored hash."""

    BASE_ORDER = {
        "id": 15808,
        "billing": {
            "first_name": "Ali", "last_name": "Hassan", "email": "ali@example.com",
            "phone": "01000000000", "address_1": "12 Nile St", "address_2": "Apt 3",
            "city": "Nasr City", "state": "Cairo", "postcode": "11765", "country": "EG",
        },
        "shipping": {
            "first_name": "Ali", "last_name": "Hassan",
            "phone": "01000000000", "address_1": "12 Nile St", "address_2": "Apt 3",
            "city": "Nasr City", "state": "Cairo", "postcode": "11765", "country": "EG",
        },
        "customer_id": 42,
    }

    def _hash_for(self, **shipping_overrides) -> str:
        order = copy.deepcopy(self.BASE_ORDER)
        order["shipping"].update(shipping_overrides)
        return order_sync._extract_order_contact_snapshot(order)["woo_contact_hash"]

    def test_street_edit_changes_the_contact_hash(self):
        self.assertNotEqual(self._hash_for(), self._hash_for(address_1="14 Nile St"))

    def test_building_or_apartment_edit_changes_the_contact_hash(self):
        self.assertNotEqual(self._hash_for(), self._hash_for(address_2="Apt 9"))

    def test_city_edit_changes_the_contact_hash(self):
        self.assertNotEqual(self._hash_for(), self._hash_for(city="Heliopolis"))

    def test_postcode_edit_changes_the_contact_hash(self):
        self.assertNotEqual(self._hash_for(), self._hash_for(postcode="11341"))

    def test_identical_address_keeps_the_hash_stable(self):
        self.assertEqual(self._hash_for(), self._hash_for())

    def test_whitespace_and_case_noise_does_not_churn_the_hash(self):
        self.assertEqual(self._hash_for(), self._hash_for(address_1="12  nile st "))

    def test_billing_address_edit_is_also_detected(self):
        order = copy.deepcopy(self.BASE_ORDER)
        before = order_sync._extract_order_contact_snapshot(order)["woo_contact_hash"]
        order["billing"]["address_1"] = "99 Other St"
        after = order_sync._extract_order_contact_snapshot(order)["woo_contact_hash"]
        self.assertNotEqual(before, after)

    def test_state_still_belongs_to_the_territory_hash_not_the_contact_hash(self):
        """State drives Territory resolution and has its own hash; keeping it out of
        the contact hash keeps the two triggers from firing each other's work."""
        order = copy.deepcopy(self.BASE_ORDER)
        before = order_sync._extract_order_contact_snapshot(order)["woo_contact_hash"]
        order["shipping"]["state"] = "Giza"
        after = order_sync._extract_order_contact_snapshot(order)["woo_contact_hash"]
        self.assertEqual(before, after)


class _ReachedWooClient(Exception):
    """Sentinel: the sync got as far as talking to the store."""


class AmendmentHandoverTests(unittest.TestCase):
    """Cancelling as the first half of an amendment must not kill the Woo order."""

    def _sync_cancel(self, replacement):
        invoice = DummyInvoice(sales_invoice_state="Recieved", docstatus=2)
        invoice.is_return = 0
        observed = {"handover": None, "reached_woo": False}

        def _fake_build_client(_settings):
            observed["reached_woo"] = True
            raise _ReachedWooClient()

        with unittest.mock.patch.object(
                 outbound_sync, "_get_settings",
                 return_value=(SimpleNamespace(name="WooCommerce Settings"), _cfg())), \
             unittest.mock.patch.object(outbound_sync.frappe, "get_doc", return_value=invoice), \
             unittest.mock.patch.object(
                 outbound_sync, "_find_replacement_invoice", return_value=replacement), \
             unittest.mock.patch.object(
                 outbound_sync, "_handover_woo_order_to_replacement",
                 side_effect=lambda **kw: observed.__setitem__("handover", kw)), \
             unittest.mock.patch.object(outbound_sync, "_mark_invoice_status"), \
             unittest.mock.patch.object(outbound_sync, "_build_client", _fake_build_client), \
             unittest.mock.patch.object(outbound_sync.frappe.db, "exists", return_value=True):
            try:
                observed["result"] = outbound_sync.sync_sales_invoice(invoice.name, cancel=True)
            except _ReachedWooClient:
                observed["result"] = None
        return observed

    def test_cancel_with_a_replacement_hands_the_order_over_instead_of_cancelling_it(self):
        observed = self._sync_cancel("ACC-SINV-TEST-002")

        self.assertFalse(observed["reached_woo"], "the store must not be told the order died")
        self.assertEqual(observed["result"]["reason"], "amended_replacement_owns_order")
        self.assertEqual(observed["result"]["replacement_invoice"], "ACC-SINV-TEST-002")
        self.assertEqual(observed["handover"]["replacement_invoice"], "ACC-SINV-TEST-002")

    def test_a_genuine_cancellation_still_reaches_woo(self):
        """No replacement means the invoice really was cancelled — the cancel must
        still be pushed, or a customer's cancelled order stays live on the store."""
        observed = self._sync_cancel(None)

        self.assertTrue(observed["reached_woo"])
        self.assertIsNone(observed["handover"])


class SubmittedInvoiceAddressRepointTests(unittest.TestCase):
    """The detection half is useless without the application half."""

    class _Invoice:
        name = "ACC-SINV-TEST-001"

        def __init__(self, customer_address=None, shipping_address_name=None, territory="Nasr City"):
            self._values = {
                "customer_address": customer_address,
                "shipping_address_name": shipping_address_name,
                "territory": territory,
            }
            self.written = {}

        def get(self, fieldname, default=None):
            return self._values.get(fieldname, default)

        def db_set(self, fieldname, value, commit=False):
            self.written[fieldname] = value
            self._values[fieldname] = value

    def test_stale_address_links_are_repointed(self):
        inv = self._Invoice(customer_address="ADDR-OLD", shipping_address_name="ADDR-OLD")

        with unittest.mock.patch.object(order_sync, "create_sync_log_entry"):
            changed = order_sync._apply_address_change_to_submitted_invoice(
                inv, woo_id=15808, billing_addr="ADDR-NEW", shipping_addr="ADDR-NEW",
                resolved_territory="Nasr City",
            )

        self.assertTrue(changed)
        self.assertEqual(inv.written["customer_address"], "ADDR-NEW")
        self.assertEqual(inv.written["shipping_address_name"], "ADDR-NEW")

    def test_matching_address_writes_nothing(self):
        inv = self._Invoice(customer_address="ADDR-1", shipping_address_name="ADDR-1")

        changed = order_sync._apply_address_change_to_submitted_invoice(
            inv, woo_id=15808, billing_addr="ADDR-1", shipping_addr="ADDR-1",
            resolved_territory="Nasr City",
        )

        self.assertFalse(changed)
        self.assertEqual(inv.written, {})

    def test_a_territory_shift_is_escalated_rather_than_repriced(self):
        """Moving territory changes delivery income on a submitted invoice, which is a
        GL decision — the webhook flags it instead of making it."""
        inv = self._Invoice(customer_address="ADDR-OLD", shipping_address_name="ADDR-OLD")
        flagged = {}

        with unittest.mock.patch.object(order_sync, "create_sync_log_entry"), \
             unittest.mock.patch.object(
                 order_sync, "_flag_order_map_for_manual_review",
                 side_effect=lambda **kw: flagged.update(kw)):
            order_sync._apply_address_change_to_submitted_invoice(
                inv, woo_id=15808, billing_addr="ADDR-NEW", shipping_addr="ADDR-NEW",
                resolved_territory="Heliopolis",
            )

        self.assertEqual(flagged.get("woo_id"), 15808)
        self.assertIn("Heliopolis", flagged.get("reason", ""))
        self.assertNotIn("territory", inv.written, "territory must not be silently changed")
