"""Centralised constants for the Jarz WooCommerce integration app.

Role names used to gate whitelisted endpoints live here so a rename only
requires updating a single file. Mirrors the pattern used by the sibling
``jarz_pos`` app's own ``constants.py`` (its ``ROLES`` class) — this module
does NOT import from ``jarz_pos``; the two apps stay completely independent.
"""

from __future__ import annotations


class ROLES:
    """Role name sets used by :mod:`jarz_woocommerce_integration.services.access`."""

    SYSTEM_MANAGER = "System Manager"
    #: The dedicated role for people who operate the Woo sync pipeline
    #: (dashboard, retries, manual pushes, territory sync) without needing
    #: full System Manager privileges.
    SYNC_OPERATOR = "WooCommerce Sync Operator"

    #: Anyone allowed to call an "operator-grade" endpoint: routine sync
    #: operations that read/replay integration state but are not the
    #: genuinely destructive or infrastructure-level actions.
    OPERATOR = {SYSTEM_MANAGER, SYNC_OPERATOR}
