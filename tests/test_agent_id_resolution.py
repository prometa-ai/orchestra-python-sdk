"""Tests for `Prometa.__init__`'s optional agent_id resolution.

Agents now mirror Tool registration semantics. The customer-facing
identity is the `(solution_id, agent_name)` tuple; the platform can
auto-register/attach the canonical Agent row on first sighting. The
SDK still honors an explicit `agent_id` override, but it must not
invent a random per-process fallback when none is configured.

These tests don't run the flush thread to completion (no endpoint is
reachable) but the client constructs cleanly because the buffer is
in-memory and the daemon thread is fine to leave parked at test exit.
"""

from __future__ import annotations

import os
import unittest
import warnings

from prometa import Prometa
from prometa.integrations import _llm_common, openllmetry


class AgentIdResolutionTest(unittest.TestCase):
    """Precedence: explicit kwarg > env > omit."""

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
        # Default agent_name so these tests stay focused on agent_id —
        # without it the new agent_name fallback warning leaks into
        # every test that doesn't override it.
        kwargs.setdefault("agent_name", "test-agent")
        return Prometa(
            endpoint="http://localhost:0/never-flushed",
            flush_interval_seconds=3600,
            **kwargs,
        )

    def _otlp_attrs(self, attrs):
        decoded = {}
        for attr in attrs:
            value = attr["value"]
            decoded[attr["key"]] = next(iter(value.values()))
        return decoded

    def test_explicit_kwarg_wins_over_env(self) -> None:
        os.environ["PROMETA_AGENT_ID"] = "from-env"
        client = self._make_client(agent_id="from-kwarg")
        self.assertEqual(client.agent_id, "from-kwarg")

    def test_env_used_when_no_kwarg(self) -> None:
        os.environ["PROMETA_AGENT_ID"] = "support-assistant-production"
        client = self._make_client()
        self.assertEqual(client.agent_id, "support-assistant-production")

    def test_missing_agent_id_is_omitted_without_warning(self) -> None:
        # No kwarg, no env: platform-side Agent auto-registration owns
        # the canonical id, so the SDK leaves it absent.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            client = self._make_client()

        self.assertIsNone(client.agent_id)
        self.assertEqual(caught, [])

    def test_empty_string_kwarg_falls_through_to_env(self) -> None:
        # Defensive: an empty string from a missing config lookup
        # should NOT be treated as a real agent_id.
        os.environ["PROMETA_AGENT_ID"] = "from-env"
        client = self._make_client(agent_id="")
        self.assertEqual(client.agent_id, "from-env")

    def test_empty_string_env_is_treated_as_absent(self) -> None:
        os.environ["PROMETA_AGENT_ID"] = ""
        client = self._make_client()
        self.assertIsNone(client.agent_id)

    def test_missing_agent_id_is_not_emitted_on_spans_or_resource(self) -> None:
        client = self._make_client(
            solution_id="sol-test",
            agent_name="test-agent",
            stage="test",
        )

        @client.workflow(name="root")
        def root():
            return None

        root()
        span = client._buffer[-1]
        self.assertNotIn("prometa.agent_id", span.attributes)
        self.assertNotIn("gen_ai.agent.id", span.attributes)

        payload = client._build_otlp_payload([span])
        resource_attrs = self._otlp_attrs(
            payload["resourceSpans"][0]["resource"]["attributes"]
        )
        otlp_span_attrs = self._otlp_attrs(
            payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"]
        )
        self.assertEqual(resource_attrs["gen_ai.agent.name"], "test-agent")
        self.assertNotIn("prometa.agent_id", resource_attrs)
        self.assertNotIn("gen_ai.agent.id", resource_attrs)
        self.assertNotIn("prometa.agent_id", otlp_span_attrs)
        self.assertNotIn("gen_ai.agent.id", otlp_span_attrs)

    def test_explicit_agent_id_is_emitted_on_spans_and_resource(self) -> None:
        client = self._make_client(
            solution_id="sol-test",
            agent_name="test-agent",
            agent_id="agt-test-agent-id",
            stage="test",
        )

        @client.workflow(name="root")
        def root():
            return None

        root()
        span = client._buffer[-1]
        self.assertEqual(span.attributes["prometa.agent_id"], "agt-test-agent-id")
        self.assertEqual(span.attributes["gen_ai.agent.id"], "agt-test-agent-id")

        payload = client._build_otlp_payload([span])
        resource_attrs = self._otlp_attrs(
            payload["resourceSpans"][0]["resource"]["attributes"]
        )
        otlp_span_attrs = self._otlp_attrs(
            payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["attributes"]
        )
        self.assertEqual(resource_attrs["prometa.agent_id"], "agt-test-agent-id")
        self.assertEqual(resource_attrs["gen_ai.agent.id"], "agt-test-agent-id")
        self.assertEqual(otlp_span_attrs["prometa.agent_id"], "agt-test-agent-id")

    def test_llm_manual_span_omits_missing_agent_id(self) -> None:
        self._make_client(
            solution_id="sol-test",
            agent_name="test-agent",
            stage="test",
        )

        span = _llm_common.open_manual_span("agent", "chat gpt-4o", {})

        self.assertIsNotNone(span)
        self.assertEqual(span.attributes["gen_ai.agent.name"], "test-agent")
        self.assertNotIn("prometa.agent_id", span.attributes)
        self.assertNotIn("gen_ai.agent.id", span.attributes)

    def test_llm_manual_span_emits_prometa_agent_id(self) -> None:
        self._make_client(
            solution_id="sol-test",
            agent_name="test-agent",
            agent_id="agt-test-agent-id",
            stage="test",
        )

        span = _llm_common.open_manual_span("agent", "chat gpt-4o", {})

        self.assertIsNotNone(span)
        self.assertEqual(span.attributes["prometa.agent_id"], "agt-test-agent-id")
        self.assertEqual(span.attributes["gen_ai.agent.id"], "agt-test-agent-id")

    def test_openllmetry_resource_attrs_omit_missing_agent_id(self) -> None:
        self._make_client(
            solution_id="sol-test",
            agent_name="test-agent",
            stage="test",
        )

        attrs = openllmetry._resource_attributes()

        self.assertEqual(attrs["gen_ai.agent.name"], "test-agent")
        self.assertNotIn("prometa.agent_id", attrs)
        self.assertNotIn("gen_ai.agent.id", attrs)

    def test_openllmetry_resource_attrs_emit_prometa_agent_id(self) -> None:
        self._make_client(
            solution_id="sol-test",
            agent_name="test-agent",
            agent_id="agt-test-agent-id",
            stage="test",
        )

        attrs = openllmetry._resource_attributes()

        self.assertEqual(attrs["prometa.agent_id"], "agt-test-agent-id")
        self.assertEqual(attrs["gen_ai.agent.id"], "agt-test-agent-id")


if __name__ == "__main__":
    unittest.main()
