"""Tests for `Prometa.__init__`'s agent_id resolution precedence.

The SDK's agent_id has to match the platform-side registry UUID for
the agent so traces and spans correlate against `Agent.id` in
Postgres. The resolution chain — explicit kwarg, then
PROMETA_AGENT_ID env, then a warning-with-random-fallback — exists
specifically to make that contract visible.

These tests don't run the flush thread to completion (no endpoint is
reachable) but the client constructs cleanly because the buffer is
in-memory and the daemon thread is fine to leave parked at test exit.
"""

from __future__ import annotations

import os
import unittest
import warnings

from prometa import Prometa


class AgentIdResolutionTest(unittest.TestCase):
    """Precedence: explicit kwarg > env > warn-and-random."""

    def setUp(self) -> None:
        # Drop the env var so each test starts from a clean slate. The
        # individual tests that exercise the env path put it back.
        self._saved_env = os.environ.pop("PROMETA_AGENT_ID", None)

    def tearDown(self) -> None:
        if self._saved_env is not None:
            os.environ["PROMETA_AGENT_ID"] = self._saved_env
        else:
            os.environ.pop("PROMETA_AGENT_ID", None)

    def _make_client(self, **kwargs):
        return Prometa(endpoint="http://localhost:0/never-flushed", **kwargs)

    def test_explicit_kwarg_wins_over_env(self) -> None:
        os.environ["PROMETA_AGENT_ID"] = "from-env"
        client = self._make_client(agent_id="from-kwarg")
        self.assertEqual(client.agent_id, "from-kwarg")

    def test_env_used_when_no_kwarg(self) -> None:
        os.environ["PROMETA_AGENT_ID"] = "6ca98816-3538-4da4-a2e2-1e3726ab887a"
        client = self._make_client()
        self.assertEqual(client.agent_id, "6ca98816-3538-4da4-a2e2-1e3726ab887a")

    def test_random_fallback_warns(self) -> None:
        # No kwarg, no env — should still produce a usable id but warn.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            client = self._make_client()

        self.assertEqual(len(client.agent_id), 16)
        self.assertTrue(all(c in "0123456789abcdef" for c in client.agent_id))
        matching = [w for w in caught if "random per-process agent_id" in str(w.message)]
        self.assertEqual(len(matching), 1, "Expected exactly one fallback warning")
        self.assertEqual(matching[0].category, UserWarning)

    def test_empty_string_kwarg_falls_through_to_env(self) -> None:
        # Defensive: an empty string from a missing config lookup
        # should NOT be treated as a real agent_id.
        os.environ["PROMETA_AGENT_ID"] = "from-env"
        client = self._make_client(agent_id="")
        self.assertEqual(client.agent_id, "from-env")


if __name__ == "__main__":
    unittest.main()
