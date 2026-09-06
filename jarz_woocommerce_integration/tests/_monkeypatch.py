"""A minimal ``monkeypatch`` stand-in for tests that run under unittest.

``bench run-tests`` -- which is what CI runs -- collects through unittest
discovery, and unittest only ever collects ``unittest.TestCase`` subclasses.
Module-level ``def test_*`` functions and pytest fixtures are invisible to it, so
two suites in this app (``test_http_client_retry``, ``test_order_sync_payments``)
sat uncollected: with pytest absent they failed at import, and with pytest
installed they would have imported and contributed zero tests, which is worse
because it looks green.

Those suites use exactly one fixture, ``monkeypatch``, and exactly one of its
methods, ``setattr``.  Rather than rewrite ~40 test bodies to
``unittest.mock.patch.object``, this provides that method with the same
signature and the same automatic undo, wired up in ``setUp`` via
``addCleanup``.  Reach for ``mock.patch`` in new tests; this exists so the
conversion stayed a mechanical one.
"""

_MISSING = object()


class MonkeyPatch:
    """``setattr`` with an undo stack, applied in reverse order on teardown."""

    def __init__(self):
        self._undo = []

    def setattr(self, target, name, value):
        self._undo.append((target, name, getattr(target, name, _MISSING)))
        setattr(target, name, value)

    def delattr(self, target, name):
        self._undo.append((target, name, getattr(target, name, _MISSING)))
        try:
            delattr(target, name)
        except AttributeError:
            pass

    def undo(self):
        # Reverse order matters: the same attribute may have been patched twice,
        # and only the first recorded value is the pre-test one.
        while self._undo:
            target, name, original = self._undo.pop()
            if original is _MISSING:
                try:
                    delattr(target, name)
                except AttributeError:
                    pass
            else:
                setattr(target, name, original)
