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
    #: Named here rather than granting ``SYNC_OPERATOR`` to each manager's
    #: user record: Frappe roles do not inherit from one another, so "the
    #: JARZ Manager role has Woo sync access" can only be expressed either as
    #: a per-user assignment that drifts as staff change, or as membership in
    #: this set. The set is the durable form — it ships with the code and
    #: cannot fall out of step with who actually manages the business.
    JARZ_MANAGER = "JARZ Manager"

    #: Anyone allowed to call an "operator-grade" endpoint: routine sync
    #: operations that read/replay integration state but are not the
    #: genuinely destructive or infrastructure-level actions.
    #:
    #: Deliberately does NOT widen the strict tier. Merging customer records,
    #: rewriting the live store's webhooks, calling a caller-supplied URL from
    #: the server and starting a multi-hour migration stay System-Manager-only
    #: (see :func:`services.access.ensure_system_manager`), because those are
    #: irreversible or reach outside this system entirely.
    OPERATOR = {SYSTEM_MANAGER, SYNC_OPERATOR, JARZ_MANAGER}
