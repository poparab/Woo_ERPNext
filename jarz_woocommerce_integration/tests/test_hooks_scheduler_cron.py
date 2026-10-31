"""Regression test for the duplicate "* * * * *" cron key in hooks.py.

A duplicate key in the `scheduler_events["cron"]` dict literal silently
dropped process_due_sync_events at import time: Python keeps only the last
value assigned to a repeated dict key, so the retry-queue cron vanished with
no error anywhere and 74 sync events piled up past due in production. This
test asserts both every-minute jobs live under the single "* * * * *" key,
and that no dict literal anywhere in hooks.py has a duplicated key.
"""
import ast
import unittest
from pathlib import Path

from jarz_woocommerce_integration import hooks

HOOKS_PATH = Path(hooks.__file__)


class TestSchedulerCronDedup(unittest.TestCase):
    def test_every_minute_key_holds_only_the_courier_positions(self):
        """Only the courier-position push needs a one-minute tick (the sync-event
        sweep moved to every two minutes for CPU, 2026-10-28)."""
        every_minute = hooks.scheduler_events["cron"]["* * * * *"]

        self.assertEqual(
            every_minute,
            ["jarz_woocommerce_integration.services.droppin_sync.push_open_leg_positions"],
        )

    def test_sync_event_sweep_is_still_scheduled(self):
        """The retry-queue sweep is the job the duplicate key once dropped; it
        must still be scheduled somewhere, now on its two-minute tick."""
        scheduled = [
            method
            for methods in hooks.scheduler_events["cron"].values()
            for method in methods
        ]
        self.assertEqual(
            scheduled.count("jarz_woocommerce_integration.services.sync_events.process_due_sync_events"),
            1,
        )
        self.assertIn(
            "jarz_woocommerce_integration.services.sync_events.process_due_sync_events",
            hooks.scheduler_events["cron"]["*/2 * * * *"],
        )

    def test_no_duplicate_keys_in_any_dict_literal_in_hooks_file(self):
        """Parse hooks.py's AST and fail if any dict literal repeats a key —
        catches this whole class of bug even in dicts not otherwise covered
        by an assertion, and even if the file changes shape later."""
        source = HOOKS_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(HOOKS_PATH))

        duplicates = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            seen = {}
            for key_node in node.keys:
                if key_node is None:
                    # ** unpacking inside a dict literal — nothing to compare.
                    continue
                try:
                    key_value = ast.literal_eval(key_node)
                except (ValueError, TypeError):
                    continue
                if not isinstance(key_value, (str, int, float, bool, tuple)):
                    continue
                seen[key_value] = seen.get(key_value, 0) + 1
            duplicates.extend(
                (key, count, node.lineno)
                for key, count in seen.items()
                if count > 1
            )

        self.assertEqual(
            duplicates,
            [],
            f"Duplicate dict keys found in hooks.py: {duplicates}",
        )


if __name__ == "__main__":
    unittest.main()
