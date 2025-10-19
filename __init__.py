"""Jarz WooCommerce Integration top-level package.

We flattened the on-disk layout to remove the redundant third directory level
but keep the historical import path ``jarz_woocommerce_integration.
jarz_woocommerce_integration`` alive by directly re-exporting the inner module.
This mirrors how ``jarz_pos`` maintains compatibility.
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
	_sys.modules[_LEGACY_ROOT] = _inner_pkg

	# Keep both lowercase and capitalised patches imports working (bench sometimes
	# references either style in migration logs).
	try:
		_patches_mod = _importlib.import_module(__name__ + ".patches")
	except ModuleNotFoundError:
		_patches_mod = None
	if _patches_mod is not None:
		_sys.modules.setdefault(__name__ + ".patches", _patches_mod)
		_sys.modules.setdefault(__name__ + ".Patches", _patches_mod)
