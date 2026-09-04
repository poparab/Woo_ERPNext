"""Run this app's pure unit tests without a site and without a database.

Why this exists
---------------
``bench run-tests`` must never be pointed at the local site: it is a restore of
production. But every test module in this package is a *pure* unit test — each one
mocks ``frappe.db`` and the WooCommerce client — so none of them needs a database
at all. They only need two things a bare interpreter does not give them:

1. ``frappe.init()``, because several app modules call ``frappe.logger(...)`` at
   import time;
2. a ``frappe.db`` **object** to exist, because ``unittest.mock.patch.object`` can
   only replace an attribute of something that is already there, and most modules
   patch ``frappe.db.get_values`` / ``.sql`` / ``.savepoint`` rather than replacing
   ``frappe.db`` wholesale.

Without (2) those modules raise ``AttributeError: 'NoneType' object has no
attribute 'get_values'`` and the run reports errors that have nothing to do with
the code under test. The stub below satisfies the patcher and raises loudly if any
test ever reaches it unpatched — so a test that silently depends on a real database
fails instead of quietly passing.

Deliberately *not* named ``test_*.py``: Frappe's collector imports every such file,
and this is a runner, not a suite.

Usage (from the bench root, inside the dev container)::

    env/bin/python apps/jarz_woocommerce_integration/jarz_woocommerce_integration/tests/run_siteless.py

or, to run only some modules::

    env/bin/python .../run_siteless.py test_customer_guest_matching test_customer_dedupe

Exit code is 0 only when every selected test passed.
"""

from __future__ import annotations

import os
import sys
import unittest

import frappe


#: The customer-identity suite: the modules that cover guest matching, phone
#: canonicalisation, the woo_customer_id guards, the outbound collision handling
#: and the dedupe tool — plus the regression modules those changes could break.
#:
#: All of these are ``unittest.TestCase``-style and therefore actually execute.
#: Eight other modules in this package are pytest-style (bare ``def test_*`` with
#: ``monkeypatch``); the unittest loader collects **nothing** from them, so
#: including them here would report a green run that had executed no assertions.
#: Pass module names explicitly to run anything else.
DEFAULT_MODULES = (
    "test_customer_guest_matching",
    "test_customer_phone_identity",
    "test_customer_multi_phone",
    "test_customer_woo_id_ambiguity",
    "test_outbound_customer_collision",
    "test_outbound_placeholder_email",
    "test_customer_dedupe",
)

_PACKAGE = "jarz_woocommerce_integration.tests"


class _NoDatabase:
    """Stands in for ``frappe.db`` so ``patch.object`` has a target.

    Any attribute a test forgets to patch raises on *use*, which turns a hidden
    database dependency into a visible failure.
    """

    def __getattr__(self, name):
        def _refuse(*_args, **_kwargs):
            raise AssertionError(
                f"frappe.db.{name}() was called for real in a site-less run — "
                f"this test needs to mock it"
            )

        return _refuse


def _bootstrap() -> None:
    bench_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
    )
    sites_path = os.path.join(bench_root, "sites")
    frappe.init(site="", sites_path=sites_path)
    frappe.local.flags.in_test = True
    if getattr(frappe.local, "db", None) is None:
        frappe.local.db = _NoDatabase()


def main(argv: list[str]) -> int:
    _bootstrap()

    wanted = argv[1:] or list(DEFAULT_MODULES)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for name in wanted:
        suite.addTests(loader.loadTestsFromName(f"{_PACKAGE}.{name}"))

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print(
        f"\nmodules={len(wanted)} tests={result.testsRun} "
        f"failures={len(result.failures)} errors={len(result.errors)}"
    )
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
