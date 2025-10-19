"""Jarz WooCommerce Integration top-level package.

We recently flattened the app structure to remove the third level package that
used to live at ``jarz_woocommerce_integration.jarz_woocommerce_integration``.
Some legacy workers and integrations still import modules via that dotted
path, so we register a lightweight alias that forwards lookups to the new
canonical modules (``jarz_woocommerce_integration.api``, ``.doctype`` …).

This mirrors the approach used in ``jarz_pos`` and keeps the public contract
stable while we tidy up the repository layout.
"""

from __future__ import annotations

__version__ = "0.0.1"
__all__ = ["__version__"]

import importlib as _importlib
import sys as _sys

_LEGACY_ROOT = __name__ + ".jarz_woocommerce_integration"

try:
	_inner_pkg = _importlib.import_module(".jarz_woocommerce_integration", __name__)
except ModuleNotFoundError:
	_inner_pkg = None
else:
	_sys.modules.setdefault(_LEGACY_ROOT, _inner_pkg)
