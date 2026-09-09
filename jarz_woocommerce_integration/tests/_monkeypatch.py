"""A minimal ``monkeypatch`` stand-in for tests that run under unittest.

``bench run-tests`` -- which is what CI runs -- collects through unittest
discovery, and unittest only ever collects ``unittest.TestCase`` subclasses.
Module-level ``def test_*`` functions and pytest fixtures are invisible to it, so
seven suites in this app sat uncollected: with pytest absent they failed at
import, and with pytest installed they would have imported and contributed zero
tests, which is worse because it looks green. That was 93 tests reporting as
coverage while executing nothing.

Those suites use exactly one fixture, ``monkeypatch``. Rather than rewrite ~130
test bodies to ``unittest.mock.patch.object``, this provides the handful of
methods they call, with the same signatures and the same automatic undo, wired
up in ``setUp`` via ``addCleanup``:

* ``setattr(target, name, value, raising=True)``, plus pytest's two-argument
  string form ``setattr("pkg.mod.attr", value)``;
* ``setitem(dict, key, value)`` -- used to patch ``sys.modules``;
* ``delattr(target, name)``.

Reach for ``mock.patch`` in new tests; this exists so the conversion stayed a
mechanical one.
"""

import importlib

_MISSING = object()


def _import_path(dotted: str):
    """Resolve a dotted path to a module, or to an attribute inside one."""
    try:
        return importlib.import_module(dotted)
    except ImportError:
        head, _, tail = dotted.rpartition(".")
        if not head:
            raise
        return getattr(_import_path(head), tail)


class MonkeyPatch:
    """``setattr`` with an undo stack, applied in reverse order on teardown."""

    def __init__(self):
        self._undo = []
        self._undo_items = []

    def setattr(self, target, name=_MISSING, value=_MISSING, raising=True):
        # pytest also accepts the two-argument string form,
        # ``setattr("pkg.mod.attr", replacement)``; several suites use it.
        if isinstance(target, str) and value is _MISSING:
            target, _, attr = target.rpartition(".")
            if not target:
                raise ValueError(f"expected a dotted path, got {attr!r}")
            target, name, value = _import_path(target), attr, name

        original = getattr(target, name, _MISSING)
        if original is _MISSING and raising:
            raise AttributeError(f"{target!r} has no attribute {name!r}")
        self._undo.append((target, name, original))
        setattr(target, name, value)

    def setitem(self, dic, name, value):
        """``monkeypatch.setitem`` for mappings (e.g. patching ``sys.modules``)."""
        self._undo_items.append((dic, name, dic[name] if name in dic else _MISSING))
        dic[name] = value

    def delattr(self, target, name):
        self._undo.append((target, name, getattr(target, name, _MISSING)))
        try:
            delattr(target, name)
        except AttributeError:
            pass

    def undo(self):
        # Reverse order matters: the same attribute may have been patched twice,
        # and only the first recorded value is the pre-test one.
        while self._undo_items:
            dic, name, original = self._undo_items.pop()
            if original is _MISSING:
                dic.pop(name, None)
            else:
                dic[name] = original
        while self._undo:
            target, name, original = self._undo.pop()
            if original is _MISSING:
                try:
                    delattr(target, name)
                except AttributeError:
                    pass
            else:
                setattr(target, name, original)
