"""Tests for Prometa's MCP tool-call instrumentation."""

from __future__ import annotations

import asyncio
import json
import unittest

from prometa import Prometa, raw_channel
from prometa.integrations import mcp
from prometa.integrations._llm_common import MAX_TEXT_ATTR_BYTES


class MCPIntegrationTest(unittest.TestCase):
    def tearDown(self) -> None:
        raw_channel.disable()

    def _make_client(self) -> Prometa:
        return Prometa(
            endpoint="http://localhost:0/never-flushed",
            api_key=None,
            solution_id="sol-test",
            agent_name="test-agent",
            agent_id="agt-test-agent-id",
            stage="test",
            flush_interval_seconds=3600,
        )

    def test_sync_call_tool_emits_classification_aliases_without_raw_by_default(
        self,
    ) -> None:
        client = self._make_client()

        class FakeSession:
            server_name = "declarai"

            def call_tool(self, name, arguments=None):
                return {"ok": True, "arguments": arguments}

        mcp._wrap_call_tool(FakeSession)
        result = FakeSession().call_tool("prepare_action", {"dataset": "claims"})

        self.assertEqual(result["ok"], True)
        span = client._buffer[-1]
        attrs = span.attributes
        self.assertEqual(span.kind, "tool")
        self.assertEqual(attrs["mcp.tool.name"], "prepare_action")
        self.assertEqual(attrs["gen_ai.tool.name"], "prepare_action")
        self.assertEqual(attrs["prometa.tool_name"], "prepare_action")
        self.assertEqual(attrs["mcp.server.name"], "declarai")
        self.assertEqual(attrs["mcp.tool.args_count"], 1)
        self.assertNotIn("prometa.raw.input", attrs)
        self.assertNotIn("prometa.raw.output", attrs)

    def test_async_call_tool_captures_raw_arguments_and_result_when_enabled(
        self,
    ) -> None:
        client = self._make_client()

        class FakeResult:
            def model_dump(self, mode="json"):
                return {"content": [{"type": "text", "text": "prepared"}]}

        class FakeSession:
            server_name = "declarai"

            async def call_tool(self, name, arguments=None):
                return FakeResult()

        mcp._wrap_call_tool(FakeSession)
        raw_channel.enable()

        result = asyncio.run(
            FakeSession().call_tool(
                "prepare_action",
                {"direct": False, "query": "x" * (MAX_TEXT_ATTR_BYTES + 100)},
            )
        )

        self.assertIsInstance(result, FakeResult)
        attrs = client._buffer[-1].attributes
        self.assertEqual(attrs["gen_ai.tool.name"], "prepare_action")
        raw_input = attrs["prometa.raw.input"]
        raw_output = attrs["prometa.raw.output"]
        self.assertLessEqual(len(raw_input), MAX_TEXT_ATTR_BYTES)
        self.assertTrue(raw_input.endswith("...[truncated]"))
        self.assertEqual(
            json.loads(raw_output),
            {"content": [{"type": "text", "text": "prepared"}]},
        )


if __name__ == "__main__":
    unittest.main()
