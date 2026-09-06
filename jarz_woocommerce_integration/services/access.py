"""Shared role gates for whitelisted Woo integration endpoints.

Before this module existed, ``api/sync_events.py`` was the only file in this
app with any role check beyond Frappe's default "must be logged in" — every
other whitelisted endpoint (order pulls, customer bulk sync, territory sync,
the dedupe merge tool, geo pin repair...) was reachable by any authenticated
user. The most serious gap was ``services/customer_dedupe.run_dedupe``: it
calls ``frappe.rename_doc(..., merge=True, ignore_permissions=True)`` with no
gate at all, so any logged-in session could permanently merge and delete
Customer records site-wide.

This module gives every endpoint a LOGGED-IN USER can reach the same two gates,
mirroring the sibling ``jarz_pos`` app's ``_ensure_manager_access()`` pattern
(``jarz_pos/api/cash_transfer.py``) and its ``constants.ROLES`` class — without
importing from ``jarz_pos``. The two apps must never import from each other.

Deliberately NOT covered here, because a role check is the wrong control for
them: the webhook receivers in ``api/webhooks.py`` and ``api/webhook.py`` are
called by WooCommerce itself rather than by a person, and are verified by HMAC
signature; the developer-mode helpers alongside them gate on ``developer_mode``
plus System Settings write. Read "every endpoint" below as "every endpoint with
a human caller" — an earlier version of this docstring claimed more than the
module delivers, which is how ``api/manual_sync.py`` sat ungated behind it.

Two tiers:

* :func:`ensure_operator_access` — for routine sync operations: dashboards,
  retries, manual pushes, territory sync, geo repair, the dedupe review
  queue. Anyone holding a role in ``ROLES.OPERATOR`` (or Administrator).
* :func:`ensure_system_manager` — for genuinely destructive or
  infrastructure-level operations: merging/deleting Customer records,
  rewriting WooCommerce webhooks, an outbound request to an arbitrary
  caller-supplied URL, starting a multi-hour historical migration, or
  returning another customer's PII by email lookup.
"""

from __future__ import annotations

import frappe
from frappe import _

from jarz_woocommerce_integration.constants import ROLES


def _current_roles() -> set[str]:
    return set(frappe.get_roles() or [])


def ensure_operator_access() -> None:
    """Raise ``frappe.PermissionError`` unless the caller is Administrator or
    holds a role in :data:`ROLES.OPERATOR`."""
    if frappe.session.user == "Administrator":
        return
    if _current_roles().intersection(ROLES.OPERATOR):
        return
    frappe.throw(
        _("Not permitted: WooCommerce Sync Operator access required"),
        frappe.PermissionError,
    )


def ensure_system_manager() -> None:
    """Raise ``frappe.PermissionError`` unless the caller is Administrator or
    holds the System Manager role. Stricter than :func:`ensure_operator_access`
    — for the genuinely dangerous operations."""
    if frappe.session.user == "Administrator":
        return
    if ROLES.SYSTEM_MANAGER in _current_roles():
        return
    frappe.throw(
        _("Not permitted: System Manager access required"),
        frappe.PermissionError,
    )
