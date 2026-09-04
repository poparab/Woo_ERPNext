"""A username collision must reach the claimed-guard, and the guard must name names.

Two defects, both proven on production:

**FIX 3 — the collision never reached the guard.**  ``POST /customers`` runs
WordPress's ``wp_insert_user``, and a username clash there answers
``existing_user_login`` / "Sorry, that username already exists!".  The reconcile
branch tested for ``"already registered"`` — WooCommerce's *email* wording — so a
username clash matched neither, skipped the reconcile *and* the already-claimed
guard, and died at the generic handler with the customer marked errored and
unbound.  ``WOOEVT-490092`` for ``محمود - 9`` carries exactly that message, and
because ``_build_customer_payload`` sets ``username = email``, every customer whose
Woo username equals their email took that unhandled branch.  ``محمود - 8`` and
``محمود - 9`` were minted 83 seconds apart.

**FIX 4 — the guard was undiagnosable.**  Refusing to stamp a second holder on a
Woo id is load-bearing and stays exactly as it is: two holders make
``find_customer_by_woo_id`` return None forever for that id, which is what put 211
Customers on woo id 3357.  But the refusal recorded only *that* the id was taken,
never by whom, which turned every occurrence into a production archaeology dig.

Nothing here loosens the refusal.  These tests pin that it is *reached* and that it
is *legible*.
"""

from types import SimpleNamespace
import unittest
import unittest.mock
from unittest.mock import MagicMock

from jarz_woocommerce_integration.services import outbound_sync
from jarz_woocommerce_integration.utils import customer_woo_id
from jarz_woocommerce_integration.utils.http_client import WooAPIError


_URL = "https://shop.example.com/wp-json/wc/v3/customers"

#: The exact string WordPress returns for `existing_user_login`.
USERNAME_TAKEN = "Sorry, that username already exists!"

#: The exact string WooCommerce returns for a duplicate email.
EMAIL_TAKEN = "An account is already registered with your email address."


class _FakeClient:
    """Records every call so a test can prove which branch ran."""

    def __init__(self, post_error=None, search_result=None):
        self.post_error = post_error
        self.search_result = [] if search_result is None else search_result
        self.calls: list = []

    def post(self, path, payload=None):
        self.calls.append(("post", path))
        if self.post_error is not None:
            raise self.post_error
        return {"id": 4242}

    def get(self, path, params=None):
        self.calls.append(("get", path, dict(params or {})))
        return self.search_result

    def put(self, path, payload=None):
        self.calls.append(("put", path))
        return {"id": int(str(path).rsplit("/", 1)[-1])}

    @property
    def paths(self):
        return [call[0] for call in self.calls]


def _run_sync(
    client,
    *,
    claimed=True,
    holders=("محمود",),
    logger=None,
    mark=None,
    set_id=None,
):
    """Drive `sync_customer` for an unbound Customer against *client*."""
    customer = SimpleNamespace(
        name="محمود - 9",
        customer_name="محمود",
        flags=SimpleNamespace(ignore_woo_outbound=False),
    )
    with unittest.mock.patch.object(
            outbound_sync, "_get_settings",
            return_value=(SimpleNamespace(), SimpleNamespace(enable_customer_push=True))), \
         unittest.mock.patch.object(outbound_sync.frappe, "get_doc", return_value=customer), \
         unittest.mock.patch.object(outbound_sync.frappe, "flags", SimpleNamespace()), \
         unittest.mock.patch.object(outbound_sync.frappe, "db", SimpleNamespace(
             set_value=MagicMock(), commit=MagicMock()), create=True), \
         unittest.mock.patch.object(outbound_sync, "get_customer_woo_id", return_value=None), \
         unittest.mock.patch.object(
             outbound_sync, "has_unmigrated_legacy_customer_woo_id", return_value=False), \
         unittest.mock.patch.object(
             outbound_sync, "_build_customer_payload",
             return_value={"email": "mahmoud@example.com", "username": "mahmoud@example.com"}), \
         unittest.mock.patch.object(outbound_sync, "_build_client", return_value=client), \
         unittest.mock.patch.object(
             outbound_sync, "customer_woo_id_is_claimed_by_other", return_value=claimed), \
         unittest.mock.patch.object(
             outbound_sync, "customer_woo_id_holders", return_value=list(holders)), \
         unittest.mock.patch.object(
             outbound_sync, "set_customer_woo_id", set_id or MagicMock()), \
         unittest.mock.patch.object(
             outbound_sync, "_mark_customer_status", mark or MagicMock()), \
         unittest.mock.patch.object(
             outbound_sync, "now_datetime", lambda: "2026-09-04 12:00:00"), \
         unittest.mock.patch.object(outbound_sync, "LOGGER", logger or MagicMock()):
        return outbound_sync.sync_customer("محمود - 9")


# ---------------------------------------------------------------------------
# FIX 3 — the classifier
# ---------------------------------------------------------------------------

class TestExistingAccountErrorClassifier(unittest.TestCase):

    def _err(self, message, payload=None, status=400):
        return WooAPIError(status, _URL, message, payload)

    def test_the_wordpress_username_clash_is_recognised(self):
        """The whole of FIX 3: this string contains no 'already registered'."""
        self.assertNotIn("already registered", USERNAME_TAKEN.lower())
        self.assertTrue(outbound_sync._is_existing_woo_account_error(self._err(USERNAME_TAKEN)))

    def test_the_woocommerce_email_clash_still_works(self):
        self.assertTrue(outbound_sync._is_existing_woo_account_error(self._err(EMAIL_TAKEN)))

    def test_the_error_code_is_enough_when_the_message_is_translated(self):
        """WordPress translates the message; it never translates the code."""
        err = self._err("Désolé, ce nom d'utilisateur existe déjà !",
                        {"code": "existing_user_login"})
        self.assertTrue(outbound_sync._is_existing_woo_account_error(err))

    def test_the_email_error_code_is_recognised(self):
        err = self._err("etwas ging schief", {"code": "registration-error-email-exists"})
        self.assertTrue(outbound_sync._is_existing_woo_account_error(err))

    def test_an_unrelated_400_is_not_swallowed(self):
        for message in (
            "Customer ID is invalid.",
            "Invalid parameter(s): billing",
            "Missing parameter(s): email",
        ):
            with self.subTest(message=message):
                self.assertFalse(
                    outbound_sync._is_existing_woo_account_error(self._err(message))
                )

    def test_only_a_400_qualifies(self):
        for status in (401, 403, 404, 409, 500):
            with self.subTest(status=status):
                self.assertFalse(
                    outbound_sync._is_existing_woo_account_error(
                        self._err(USERNAME_TAKEN, status=status)
                    )
                )

    def test_a_missing_or_odd_payload_does_not_raise(self):
        for payload in (None, {}, {"code": None}, "not-a-dict", []):
            with self.subTest(payload=payload):
                err = self._err("Invalid parameter(s): billing")
                err.payload = payload
                self.assertFalse(outbound_sync._is_existing_woo_account_error(err))


# ---------------------------------------------------------------------------
# FIX 3 — end to end: the collision reaches the guard, not the generic handler
# ---------------------------------------------------------------------------

class TestUsernameCollisionReachesTheGuard(unittest.TestCase):

    def test_a_username_clash_is_reconciled_and_then_refused(self):
        client = _FakeClient(
            post_error=WooAPIError(400, _URL, USERNAME_TAKEN),
            search_result=[{"id": 3357}],
        )
        mark = MagicMock()
        result = _run_sync(client, claimed=True, holders=("محمود",), mark=mark)

        # It reconciled: the email search actually happened.
        self.assertIn("get", client.paths,
                      "the collision never reached the reconcile branch")
        # It refused: the guard, not the generic handler, produced the answer.
        self.assertEqual(result["status"], "error")
        self.assertIn("refusing to adopt", result["detail"])
        self.assertNotEqual(
            result["detail"], USERNAME_TAKEN,
            "the collision died at the generic handler again",
        )
        mark.assert_called_once()
        self.assertEqual(mark.call_args.kwargs["status"], "error")

    def test_a_username_clash_recognised_only_by_its_code_also_reaches_the_guard(self):
        client = _FakeClient(
            post_error=WooAPIError(400, _URL, "unbekannter Fehler",
                                   {"code": "existing_user_login"}),
            search_result=[{"id": 3357}],
        )
        result = _run_sync(client, claimed=True)
        self.assertIn("get", client.paths)
        self.assertIn("refusing to adopt", result["detail"])

    def test_an_unclaimed_account_is_still_adopted_after_a_username_clash(self):
        """FIX 3 must open the reconcile path, not just the refusal."""
        client = _FakeClient(
            post_error=WooAPIError(400, _URL, USERNAME_TAKEN),
            search_result=[{"id": 3357}],
        )
        set_id = MagicMock()
        result = _run_sync(client, claimed=False, set_id=set_id)

        set_id.assert_called_once()
        self.assertEqual(set_id.call_args.args[1], 3357)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["woo_customer_id"], 3357)
        self.assertIn("put", client.paths)

    def test_an_unrelated_400_still_takes_the_generic_handler(self):
        """Widening the trigger must not swallow genuine bad requests."""
        client = _FakeClient(post_error=WooAPIError(400, _URL, "Customer ID is invalid."))
        result = _run_sync(client)

        self.assertNotIn("get", client.paths, "an unrelated 400 was sent to reconcile")
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["detail"], "Customer ID is invalid.")

    def test_a_broadened_trigger_that_finds_nothing_keeps_the_store_message(self):
        """Diagnosability must survive the wider net."""
        client = _FakeClient(
            post_error=WooAPIError(400, _URL, USERNAME_TAKEN),
            search_result=[],
        )
        result = _run_sync(client)
        self.assertEqual(result["status"], "error")
        self.assertIn(USERNAME_TAKEN, result["detail"])


# ---------------------------------------------------------------------------
# FIX 4 — the refusal names the holder
# ---------------------------------------------------------------------------

class TestGuardNamesTheHoldingCustomer(unittest.TestCase):

    def _refuse(self, holders, logger=None, mark=None):
        client = _FakeClient(
            post_error=WooAPIError(400, _URL, EMAIL_TAKEN),
            search_result=[{"id": 3357}],
        )
        return _run_sync(client, claimed=True, holders=holders,
                         logger=logger, mark=mark)

    def test_the_returned_detail_names_the_holder(self):
        result = self._refuse(("محمود",))
        self.assertIn("محمود", result["detail"])
        self.assertIn("3357", result["detail"])

    def test_the_stored_error_names_the_holder(self):
        """This is the field an operator actually reads on the Customer."""
        mark = MagicMock()
        self._refuse(("محمود",), mark=mark)
        self.assertIn("محمود", mark.call_args.kwargs["error"])

    def test_every_holder_is_named_when_the_id_is_already_poisoned(self):
        result = self._refuse(("محمود", "محمود - 8"))
        for holder in ("محمود", "محمود - 8"):
            with self.subTest(holder=holder):
                self.assertIn(holder, result["detail"])

    def test_the_structured_log_event_carries_the_holders(self):
        logger = MagicMock()
        self._refuse(("محمود",), logger=logger)
        events = [
            call.args[0] for call in logger.error.call_args_list
            if call.args and isinstance(call.args[0], dict)
        ]
        claimed = [
            e for e in events
            if e.get("event") == "woo_outbound_customer_id_already_claimed"
        ]
        self.assertEqual(len(claimed), 1, f"guard event not logged; saw {events}")
        self.assertEqual(claimed[0]["held_by"], ["محمود"])
        self.assertEqual(claimed[0]["woo_id"], 3357)

    def test_an_unknown_holder_degrades_to_a_readable_word(self):
        """The diagnostic must never be the thing that breaks the refusal."""
        result = self._refuse(())
        self.assertEqual(result["status"], "error")
        self.assertIn("unknown", result["detail"])

    def test_the_refusal_itself_is_unchanged(self):
        """Load-bearing: the guard must still refuse, and must not stamp the id."""
        set_id = MagicMock()
        client = _FakeClient(
            post_error=WooAPIError(400, _URL, EMAIL_TAKEN),
            search_result=[{"id": 3357}],
        )
        result = _run_sync(client, claimed=True, set_id=set_id)
        set_id.assert_not_called()
        self.assertNotIn("put", client.paths)
        self.assertEqual(result["status"], "error")


class TestCustomerWooIdHolders(unittest.TestCase):
    """The lookup behind FIX 4."""

    @staticmethod
    def _db(get_values):
        """A whole `frappe.db` stand-in, so this runs with or without a site."""
        return unittest.mock.patch.object(
            customer_woo_id.frappe, "db",
            SimpleNamespace(get_values=get_values), create=True,
        )

    def test_it_returns_the_other_holders(self):
        with unittest.mock.patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
             self._db(lambda *_a, **_kw: ["CUST-A", "CUST-B"]):
            self.assertEqual(
                customer_woo_id.customer_woo_id_holders(3357, exclude="CUST-A"),
                ["CUST-B"],
            )

    def test_the_caller_is_never_listed_as_its_own_conflict(self):
        with unittest.mock.patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
             self._db(lambda *_a, **_kw: ["CUST-A"]):
            self.assertEqual(
                customer_woo_id.customer_woo_id_holders(3357, exclude="CUST-A"), []
            )

    def test_a_failing_probe_returns_empty_rather_than_raising(self):
        def _boom(*_a, **_kw):
            raise RuntimeError("boom")

        with unittest.mock.patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
             self._db(_boom):
            self.assertEqual(customer_woo_id.customer_woo_id_holders(3357), [])

    def test_a_missing_column_is_not_queried(self):
        get_values = MagicMock()
        with unittest.mock.patch.object(customer_woo_id, "_customer_has_column", return_value=False), \
             self._db(get_values):
            self.assertEqual(customer_woo_id.customer_woo_id_holders(3357), [])
        get_values.assert_not_called()

    def test_a_blank_or_zero_id_is_never_looked_up(self):
        get_values = MagicMock()
        with unittest.mock.patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
             self._db(get_values):
            for value in (0, "0", "", None, "abc"):
                with self.subTest(value=value):
                    self.assertEqual(customer_woo_id.customer_woo_id_holders(value), [])
        get_values.assert_not_called()

    def test_the_claimed_predicate_still_answers_the_same_yes_or_no(self):
        """FIX 4 refactored it onto the holder lookup; behaviour must not move."""
        cases = [(["CUST-OTHER"], True), (["CUST-MINE"], False), ([], False)]
        for rows, expected in cases:
            with self.subTest(rows=rows):
                with unittest.mock.patch.object(customer_woo_id, "_customer_has_column", return_value=True), \
                     self._db(lambda *_a, _rows=rows, **_kw: _rows):
                    self.assertEqual(
                        customer_woo_id.customer_woo_id_is_claimed_by_other(3357, "CUST-MINE"),
                        expected,
                    )


if __name__ == "__main__":
    unittest.main()
