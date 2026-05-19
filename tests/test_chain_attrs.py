"""Smoke tests for the correlation-chain attribute helpers.

Verifies that the new `set_customer_id` / `set_user_id` /
`set_conversation_id` / `set_request_model` / `set_tool_name` helpers
stamp the correct OTLP attribute keys on the active span — the
platform's correlation-id resolver consumes these keys verbatim, so
any drift here breaks the chain materialisation on the ingest side.

Also verifies the `customer_id` constructor kwarg propagates to every
span via the attribute dictionary (the org-wide default path).

These tests don't require a running platform — they introspect the
in-memory span buffer the Prometa client builds before flushing.
"""

from __future__ import annotations

import unittest

from prometa import (
    Prometa,
    set_customer_id,
    set_user_id,
    set_conversation_id,
    set_request_model,
    set_tool_name,
)


class ChainAttrsTest(unittest.TestCase):
    """Active-span helpers + constructor customer_id propagation."""

    def setUp(self) -> None:
        # Buffer-only mode: endpoint doesn't matter, the test never
        # flushes. The flush thread is daemonised so it won't block
        # test teardown.
        self.prometa = Prometa(
            endpoint="http://localhost:0/never-flushed",
            api_key=None,
            solution_id="sol-test",
            agent_name="test-agent",
            agent_id="test-agent-id",
            stage="test",
        )

    def _latest_span(self):
        # Each test wraps one function so the buffer holds exactly one
        # span after the wrapped call returns.
        self.assertEqual(len(self.prometa._buffer), 1)
        return self.prometa._buffer[-1]

    def test_set_customer_id_stamps_prometa_customer_id(self) -> None:
        @self.prometa.workflow(name="root")
        def handle():
            set_customer_id("cus_acme_42")

        handle()
        attrs = self._latest_span().attributes
        self.assertEqual(attrs.get("prometa.customer_id"), "cus_acme_42")

    def test_set_user_id_stamps_both_otel_and_prometa_keys(self) -> None:
        @self.prometa.workflow(name="root")
        def handle():
            set_user_id("user-jane")

        handle()
        attrs = self._latest_span().attributes
        self.assertEqual(attrs.get("gen_ai.user.id"), "user-jane")
        self.assertEqual(attrs.get("prometa.user.id"), "user-jane")

    def test_set_conversation_id_writes_gen_ai_conversation_id(self) -> None:
        @self.prometa.workflow(name="root")
        def handle():
            set_conversation_id("conv-thread-99")

        handle()
        attrs = self._latest_span().attributes
        self.assertEqual(attrs.get("gen_ai.conversation.id"), "conv-thread-99")

    def test_set_request_model_writes_gen_ai_request_model(self) -> None:
        @self.prometa.workflow(name="root")
        def handle():
            set_request_model("claude-opus-4-7")

        handle()
        attrs = self._latest_span().attributes
        self.assertEqual(attrs.get("gen_ai.request.model"), "claude-opus-4-7")

    def test_set_tool_name_writes_prometa_tool_name(self) -> None:
        # Setter is permitted on any span kind — the platform-side
        # promotion to a tool_id is span-kind aware, but the SDK
        # itself just stamps the attribute.
        @self.prometa.tool(name="search-kb")
        def search():
            set_tool_name("knowledge-base-search")

        search()
        attrs = self._latest_span().attributes
        self.assertEqual(attrs.get("prometa.tool_name"), "knowledge-base-search")

    def test_helpers_return_false_outside_span_context(self) -> None:
        # No active span → every helper returns False, no exception.
        self.assertFalse(set_customer_id("cus_x"))
        self.assertFalse(set_user_id("u"))
        self.assertFalse(set_conversation_id("c"))
        self.assertFalse(set_request_model("m"))
        self.assertFalse(set_tool_name("t"))
        self.assertEqual(len(self.prometa._buffer), 0)

    def test_empty_value_pops_the_attribute(self) -> None:
        @self.prometa.workflow(name="root")
        def handle():
            set_customer_id("cus_first")
            # Clear with empty string.
            set_customer_id("")

        handle()
        attrs = self._latest_span().attributes
        self.assertNotIn("prometa.customer_id", attrs)


class ConstructorCustomerIdTest(unittest.TestCase):
    """Org-wide customer_id propagation from the Prometa constructor."""

    def setUp(self) -> None:
        self.prometa = Prometa(
            endpoint="http://localhost:0/never-flushed",
            api_key=None,
            solution_id="sol-test",
            agent_name="test-agent",
            agent_id="test-agent-id",
            stage="test",
            customer_id="cus_org_wide_default",
        )

    def test_customer_id_propagates_to_every_span(self) -> None:
        @self.prometa.workflow(name="outer")
        def outer():
            @self.prometa.agent(name="inner")
            def inner():
                return None

            inner()

        outer()
        self.assertEqual(len(self.prometa._buffer), 2)
        for span in self.prometa._buffer:
            self.assertEqual(
                span.attributes.get("prometa.customer_id"),
                "cus_org_wide_default",
            )

    def test_per_span_override_inherits_to_children(self) -> None:
        # The per-span `set_customer_id` call should win over the
        # constructor default for that span AND every nested span.
        @self.prometa.workflow(name="outer")
        def outer():
            set_customer_id("cus_override")

            @self.prometa.agent(name="inner")
            def inner():
                return None

            inner()

        outer()
        for span in self.prometa._buffer:
            self.assertEqual(
                span.attributes.get("prometa.customer_id"),
                "cus_override",
            )


if __name__ == "__main__":
    unittest.main()
