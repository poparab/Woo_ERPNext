import unittest
from unittest.mock import patch

from jarz_woocommerce_integration.services import customer_sync


class TestResolveTerritoryFromState(unittest.TestCase):
    def _patch_territory_db(self):
        def _exists(doctype, name=None):
            return doctype == "Territory" and name in {"EGSHAMS", "EGNASRCITY"}

        def _get_value(doctype, filters=None, fieldname=None, *args, **kwargs):
            if doctype != "Territory" or not isinstance(filters, dict):
                return None
            if filters.get("custom_territory_name_ar") == "عين شمس":
                return "EGSHAMS"
            if filters.get("territory_name") == "EGSHAMS":
                return "EGSHAMS"
            if filters.get("custom_woo_code") == "EGSHAMS":
                return "EGSHAMS"
            return None

        return (
            patch.object(customer_sync.frappe.db, "exists", side_effect=_exists),
            patch.object(customer_sync.frappe.db, "get_value", side_effect=_get_value),
            patch.object(customer_sync, "_field_exists", return_value=True),
            patch.object(customer_sync.frappe, "get_all", return_value=[]),
        )

    def test_resolves_ain_shams_bilingual_label(self):
        patches = self._patch_territory_db()
        with patches[0], patches[1], patches[2], patches[3]:
            result = customer_sync._resolve_territory_from_state("Ain Shams - عين شمس")

        self.assertEqual(result, "EGSHAMS")

    def test_resolves_ain_shams_arabic_only_label(self):
        patches = self._patch_territory_db()
        with patches[0], patches[1], patches[2], patches[3]:
            result = customer_sync._resolve_territory_from_state("عين شمس")

        self.assertEqual(result, "EGSHAMS")

    def test_resolves_ain_shams_code(self):
        patches = self._patch_territory_db()
        with patches[0], patches[1], patches[2], patches[3]:
            result = customer_sync._resolve_territory_from_state("EGSHAMS")

        self.assertEqual(result, "EGSHAMS")

    def test_unknown_territory_returns_none_without_default_masking(self):
        patches = self._patch_territory_db()
        with patches[0], patches[1], patches[2], patches[3]:
            result = customer_sync._resolve_territory_from_state("Unknown Zone")

        self.assertIsNone(result)