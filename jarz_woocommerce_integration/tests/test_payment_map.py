"""Bidirectional payment-method map (F-02).

The old outbound mapper used three substring tests, so ``Kashier Card`` matched
none of them and shipped as ``cod`` while ``Kashier Wallet`` matched the wallet
test and shipped as the mobile-wallet id. These lock the table down.
"""

from types import SimpleNamespace
import unittest
import unittest.mock

from jarz_woocommerce_integration.services import payment_map


def _cfg(**overrides):
    values = {
        "payment_cod": "cod",
        "payment_instapay": "instapay",
        "payment_wallet": "wallet",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class TestPaymentMap(unittest.TestCase):
    def test_required_pairs_round_trip_exactly(self):
        pairs = {
            "Cash": "cod",
            "Instapay": "instapay",
            "Mobile Wallet": "wallet",
            "Kashier Card": "kashier_card",
            "Kashier Wallet": "kashier_wallet",
        }
        for erpnext_value, woo_value in pairs.items():
            with self.subTest(erpnext_value=erpnext_value):
                method_id, _title = payment_map.erpnext_to_woo(erpnext_value, _cfg())
                self.assertEqual(method_id, woo_value)
                self.assertEqual(payment_map.woo_to_erpnext(woo_value), erpnext_value)

    def test_kashier_card_no_longer_falls_through_to_cod(self):
        method_id, title = payment_map.erpnext_to_woo("Kashier Card", _cfg())

        self.assertEqual(method_id, "kashier_card")
        self.assertEqual(title, "Kashier Card")

    def test_kashier_wallet_is_not_the_mobile_wallet_id(self):
        method_id, _title = payment_map.erpnext_to_woo("Kashier Wallet", _cfg())

        self.assertEqual(method_id, "kashier_wallet")
        self.assertNotEqual(method_id, "wallet")

    def test_legacy_inbound_aliases_still_accepted(self):
        self.assertEqual(payment_map.woo_to_erpnext("card"), "Kashier Card")
        self.assertEqual(payment_map.woo_to_erpnext("kashier"), "Kashier Card")
        self.assertEqual(
            payment_map.woo_to_erpnext("kashier", "Kashier Wallet"), "Kashier Wallet"
        )

    def test_legacy_aliases_are_never_emitted(self):
        emitted = {method_id for method_id, _title in payment_map.ERPNEXT_TO_WOO.values()}

        self.assertNotIn("card", emitted)
        self.assertNotIn("kashier", emitted)

    def test_configured_ids_win_over_the_literals(self):
        cfg = _cfg(payment_cod="cash_on_delivery", payment_wallet="vodafone_cash")

        self.assertEqual(payment_map.erpnext_to_woo("Cash", cfg)[0], "cash_on_delivery")
        self.assertEqual(payment_map.erpnext_to_woo("Mobile Wallet", cfg)[0], "vodafone_cash")

    def test_kashier_ignores_cfg_because_it_has_no_configurable_id(self):
        cfg = _cfg(payment_cod="cash_on_delivery")

        self.assertEqual(payment_map.erpnext_to_woo("Kashier Card", cfg)[0], "kashier_card")

    def test_blank_value_falls_back_to_cod_without_logging(self):
        with unittest.mock.patch.object(payment_map.LOGGER, "warning") as warning:
            method_id, title = payment_map.erpnext_to_woo("", _cfg())

        self.assertEqual(method_id, "cod")
        self.assertEqual(title, "Cash on Delivery")
        warning.assert_not_called()

    def test_unknown_value_falls_back_to_cod_but_says_so(self):
        with unittest.mock.patch.object(payment_map.LOGGER, "warning") as warning:
            method_id, title = payment_map.erpnext_to_woo("Bank Draft", _cfg())

        self.assertEqual(method_id, "cod")
        # The title keeps what the operator actually chose.
        self.assertEqual(title, "Bank Draft")
        warning.assert_called_once()

    def test_mode_of_payment_spellings_keep_working(self):
        for raw, expected in (
            ("cash", "cod"),
            ("Cash on Delivery", "cod"),
            ("instapay", "instapay"),
            ("Wallet", "wallet"),
        ):
            with self.subTest(raw=raw):
                self.assertEqual(payment_map.erpnext_to_woo(raw, _cfg())[0], expected)

    def test_unknown_woo_method_maps_to_nothing_rather_than_guessing(self):
        self.assertIsNone(payment_map.woo_to_erpnext("bacs"))
        self.assertIsNone(payment_map.woo_to_erpnext(None))
        self.assertIsNone(payment_map.woo_to_erpnext("   "))


if __name__ == "__main__":
    unittest.main()
