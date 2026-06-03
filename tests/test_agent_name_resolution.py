"""Tests for `Prometa.__init__`'s agent_name resolution.

Mirrors the shape of `test_agent_id_resolution.py`. The platform keys
the Agent registry on `(orgId, solutionId, agent_name)`, so a forgotten
`agent_name` collapses every uninstrumented app in the same solution
onto a single Agent row. The SDK can't refuse to ship spans for a
caller that didn't set the name (would be a hostile breaking change),
but it must make the collision risk loud at startup so the symptom
doesn't surface much later as "everyone shares one Agent row."

Resolution: explicit kwarg > PROMETA_AGENT_NAME env > fallback with
UserWarning.
"""

from __future__ import annotations

import os
import unittest
import warnings

from prometa import Prometa
from prometa.client import DEFAULT_AGENT_NAME


class AgentNameResolutionTest(unittest.TestCase):
    """Precedence: explicit kwarg > env > warned fallback."""

    def setUp(self) -> None:
        self._saved_env = os.environ.pop("PROMETA_AGENT_NAME", None)

    def tearDown(self) -> None:
        if self._saved_env is not None:
            os.environ["PROMETA_AGENT_NAME"] = self._saved_env
        else:
            os.environ.pop("PROMETA_AGENT_NAME", None)

    def _make_client(self, **kwargs):
        return Prometa(
            endpoint="http://localhost:0/never-flushed",
            flush_interval_seconds=3600,
            **kwargs,
        )

    def test_explicit_kwarg_wins_over_env(self) -> None:
        os.environ["PROMETA_AGENT_NAME"] = "from-env"
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            client = self._make_client(agent_name="from-kwarg")
        self.assertEqual(client.agent_name, "from-kwarg")
        self.assertEqual([w for w in caught if issubclass(w.category, UserWarning)], [])

    def test_env_used_when_no_kwarg(self) -> None:
        os.environ["PROMETA_AGENT_NAME"] = "support-assistant"
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            client = self._make_client()
        self.assertEqual(client.agent_name, "support-assistant")
        self.assertEqual([w for w in caught if issubclass(w.category, UserWarning)], [])

    def test_missing_name_falls_back_and_warns(self) -> None:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            client = self._make_client()

        self.assertEqual(client.agent_name, DEFAULT_AGENT_NAME)
        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        self.assertEqual(len(user_warnings), 1)
        message = str(user_warnings[0].message)
        self.assertIn("PROMETA_AGENT_NAME", message)
        self.assertIn("agent_name", message)

    def test_empty_string_kwarg_falls_through_to_env(self) -> None:
        # Defensive: empty string from a config-lookup miss should not
        # be treated as a real agent_name.
        os.environ["PROMETA_AGENT_NAME"] = "from-env"
        client = self._make_client(agent_name="")
        self.assertEqual(client.agent_name, "from-env")

    def test_whitespace_kwarg_falls_through_to_env(self) -> None:
        os.environ["PROMETA_AGENT_NAME"] = "from-env"
        client = self._make_client(agent_name="   ")
        self.assertEqual(client.agent_name, "from-env")

    def test_empty_string_env_falls_through_to_default(self) -> None:
        os.environ["PROMETA_AGENT_NAME"] = ""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            client = self._make_client()
        self.assertEqual(client.agent_name, DEFAULT_AGENT_NAME)
        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        self.assertEqual(len(user_warnings), 1)

    def test_explicit_default_kwarg_does_not_warn(self) -> None:
        # A caller who DELIBERATELY passes "prometa-agent" has made an
        # explicit choice — no warning. The warning fires only when the
        # caller omitted the kwarg entirely AND the env var is unset.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            client = self._make_client(agent_name=DEFAULT_AGENT_NAME)
        self.assertEqual(client.agent_name, DEFAULT_AGENT_NAME)
        self.assertEqual([w for w in caught if issubclass(w.category, UserWarning)], [])


if __name__ == "__main__":
    unittest.main()
