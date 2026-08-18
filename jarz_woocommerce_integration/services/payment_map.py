"""Bidirectional WooCommerce <-> ERPNext payment-method map.

Single source of truth for the five payment methods this business actually
uses. Both lanes read it, neither redefines it:

* inbound  (``order_sync``)   Woo ``payment_method`` -> ``custom_payment_method``
* outbound (``outbound_sync``) ``custom_payment_method`` -> Woo ``payment_method``

Why a table and not substring tests
-----------------------------------
The outbound side used to decide with three ``in`` tests::

    if "insta" in raw: ...
    if "wallet" in raw: ...
    return cod

``Kashier Card`` matched none of them and left as ``cod``, and
``Kashier Wallet`` matched the *wallet* test and left as the mobile-wallet id --
so a card settlement reached the store labelled cash-on-delivery. A substring
test cannot express "Kashier Wallet is not Mobile Wallet"; a table can.

The legacy Woo ids (``card``, and bare ``kashier`` with a title-aware
wallet/card split) are still *accepted* inbound because 10k historical orders
carry them. They are never *emitted*: outbound only ever writes the canonical
ids in :data:`ERPNEXT_TO_WOO`.
"""

from __future__ import annotations

from typing import Any

import frappe

LOGGER = frappe.logger("jarz_woocommerce.payment_map")

# --- Canonical ERPNext ``custom_payment_method`` values ---------------------
ERPNEXT_CASH = "Cash"
ERPNEXT_INSTAPAY = "Instapay"
ERPNEXT_MOBILE_WALLET = "Mobile Wallet"
ERPNEXT_KASHIER_CARD = "Kashier Card"
ERPNEXT_KASHIER_WALLET = "Kashier Wallet"

# --- Canonical WooCommerce payment-method ids ------------------------------
WOO_COD = "cod"
WOO_INSTAPAY = "instapay"
WOO_WALLET = "wallet"
WOO_KASHIER_CARD = "kashier_card"
WOO_KASHIER_WALLET = "kashier_wallet"

#: Woo ``payment_method`` (lowercased) -> ERPNext ``custom_payment_method``.
#: Entries below the divider are legacy spellings accepted inbound only.
WOO_TO_ERPNEXT: dict[str, str] = {
    WOO_COD: ERPNEXT_CASH,
    WOO_INSTAPAY: ERPNEXT_INSTAPAY,
    WOO_WALLET: ERPNEXT_MOBILE_WALLET,
    WOO_KASHIER_CARD: ERPNEXT_KASHIER_CARD,
    WOO_KASHIER_WALLET: ERPNEXT_KASHIER_WALLET,
    # --- legacy inbound aliases, never emitted ---
    "card": ERPNEXT_KASHIER_CARD,
}

#: ERPNext ``custom_payment_method`` -> (woo method id, default title).
#: The title is only a *default*: when the invoice carries a human-readable
#: value we push that instead, so the store keeps showing what the operator
#: chose rather than a synthetic label.
ERPNEXT_TO_WOO: dict[str, tuple[str, str]] = {
    ERPNEXT_CASH: (WOO_COD, "Cash on Delivery"),
    ERPNEXT_INSTAPAY: (WOO_INSTAPAY, "Instapay"),
    ERPNEXT_MOBILE_WALLET: (WOO_WALLET, "Mobile Wallet"),
    ERPNEXT_KASHIER_CARD: (WOO_KASHIER_CARD, "Kashier Card"),
    ERPNEXT_KASHIER_WALLET: (WOO_KASHIER_WALLET, "Kashier Wallet"),
}

#: ``OutboundConfig`` attribute that overrides the literal Woo id. Only the
#: three the settings singleton exposes; Kashier has no configurable field, so
#: it always emits the literal.
_CFG_FIELD_BY_ERPNEXT_VALUE: dict[str, str] = {
    ERPNEXT_CASH: "payment_cod",
    ERPNEXT_INSTAPAY: "payment_instapay",
    ERPNEXT_MOBILE_WALLET: "payment_wallet",
}

#: Every ERPNext-side spelling that means one of the canonical values. Covers
#: ``mode_of_payment`` names as well as ``custom_payment_method``, because the
#: outbound payload falls back to the former when the latter is blank.
_ERPNEXT_ALIASES: dict[str, str] = {
    "cash": ERPNEXT_CASH,
    "cash on delivery": ERPNEXT_CASH,
    "cash on delivery (cod)": ERPNEXT_CASH,
    "cod": ERPNEXT_CASH,
    "instapay": ERPNEXT_INSTAPAY,
    "insta pay": ERPNEXT_INSTAPAY,
    "instapay transfer": ERPNEXT_INSTAPAY,
    "mobile wallet": ERPNEXT_MOBILE_WALLET,
    "wallet": ERPNEXT_MOBILE_WALLET,
    "kashier card": ERPNEXT_KASHIER_CARD,
    "kashier_card": ERPNEXT_KASHIER_CARD,
    "kashier wallet": ERPNEXT_KASHIER_WALLET,
    "kashier_wallet": ERPNEXT_KASHIER_WALLET,
}


def canonical_erpnext_value(invoice_value: str | None) -> str | None:
    """Normalise any ERPNext-side spelling onto a canonical value, or ``None``."""
    text = str(invoice_value or "").strip()
    if not text:
        return None
    if text in ERPNEXT_TO_WOO:
        return text
    return _ERPNEXT_ALIASES.get(text.casefold())


def woo_to_erpnext(method: str | None, title: str | None = None) -> str | None:
    """Map a Woo ``payment_method`` onto ``custom_payment_method``.

    ``title`` only matters for the bare legacy ``kashier`` id, where the gateway
    did not distinguish card from wallet in the id and the split lives in the
    human-readable title.
    """
    if not method:
        return None
    pm = str(method).strip().casefold()
    if not pm:
        return None
    if pm == "kashier":
        return (
            ERPNEXT_KASHIER_WALLET
            if "wallet" in str(title or "").casefold()
            else ERPNEXT_KASHIER_CARD
        )
    return WOO_TO_ERPNEXT.get(pm)


def erpnext_to_woo(invoice_value: str | None, cfg: Any = None) -> tuple[str, str]:
    """Map ``custom_payment_method`` onto ``(woo payment_method, title)``.

    A configurable id on ``cfg`` wins over the literal in :data:`ERPNEXT_TO_WOO`
    for the three methods the settings singleton exposes. An unknown or blank
    value falls back to cash-on-delivery, as it always has -- but now says so in
    the log instead of silently pretending the order was COD.
    """
    raw = str(invoice_value or "").strip()
    canonical = canonical_erpnext_value(raw)
    if canonical is None:
        if raw:
            LOGGER.warning({
                "event": "woo_payment_method_unmapped",
                "invoice_value": raw,
                "fallback": ERPNEXT_CASH,
            })
        canonical = ERPNEXT_CASH

    method_id, default_title = ERPNEXT_TO_WOO[canonical]

    cfg_field = _CFG_FIELD_BY_ERPNEXT_VALUE.get(canonical)
    if cfg_field:
        override = str(getattr(cfg, cfg_field, "") or "").strip()
        if override:
            method_id = override

    return method_id, (raw or default_title)
