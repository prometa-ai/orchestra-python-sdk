"""Tests for Prometa's LangChain instrumentation.

Focus: tool spans must be named after the *tool*, not its class, and must
carry the canonical tool-name attributes the MCP integration already sets.
"""

from __future__ import annotations

import asyncio
import unittest
from unittest import mock

from prometa import Prometa
from prometa.integrations import langchain as prometa_langchain


try:  # LangChain is an optional extra; CI installs only ".[dev]".
    from langchain_core.tools import BaseTool  # type: ignore

    HAS_LANGCHAIN = True
except Exception:  # pragma: no cover - exercised on the dependency-free CI path
    BaseTool = None  # type: ignore[assignment]
    HAS_LANGCHAIN = False


class _FakeTool:
    """Duck-typed stand-in for ``BaseTool`` (name + run/arun)."""

    def __init__(self, name: str) -> None:
        self.name = name

    def run(self, *args, **kwargs):
        return f"ran:{self.name}"

    async def arun(self, *args, **kwargs):
        return f"aran:{self.name}"


class _FakeChatModel:
    """Stand-in for a chat model: exposes ``model``, but no run/arun."""

    def __init__(self, model: str) -> None:
        self.model = model

    def invoke(self, *args, **kwargs):
        return "hello"


def _make_client() -> Prometa:
    return Prometa(
        endpoint="http://localhost:0/never-flushed",
        api_key=None,
        solution_id="sol-test",
        agent_name="test-agent",
        agent_id="agt-test-agent-id",
        stage="test",
        flush_interval_seconds=3600,
    )


class LangChainToolSpanIdentityTest(unittest.TestCase):
    """The dependency-free path: no ``langchain_core`` importable."""

    def setUp(self) -> None:
        # Force the duck-typed fallback so these run identically in CI,
        # where langchain-core is not installed.
        patcher = mock.patch.object(prometa_langchain, "_BASE_TOOL_CLS", None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _wrapped_tool_cls(self, method: str, span_name: str) -> type:
        cls = type("StructuredTool", (_FakeTool,), dict(_FakeTool.__dict__))
        prometa_langchain._wrap_method(cls, method, span_name)
        return cls

    def test_sync_tool_span_named_for_tool_with_canonical_attrs(self) -> None:
        client = _make_client()
        cls = self._wrapped_tool_cls("run", "tool.run")

        result = cls("search_products").run({"query": "shoes"})

        self.assertEqual(result, "ran:search_products")
        span = client._buffer[-1]
        self.assertEqual(span.kind, "tool")
        # Named after the tool, not the class (was "tool.run:StructuredTool").
        self.assertEqual(span.name, "tool.run:search_products")
        attrs = span.attributes
        self.assertEqual(attrs["gen_ai.tool.name"], "search_products")
        self.assertEqual(attrs["prometa.tool_name"], "search_products")
        self.assertEqual(attrs["tool.name"], "search_products")
        self.assertEqual(attrs["langchain.name"], "search_products")
        self.assertEqual(attrs["gen_ai.framework"], "langchain")

    def test_async_tool_span_named_for_tool_with_canonical_attrs(self) -> None:
        client = _make_client()
        cls = self._wrapped_tool_cls("arun", "tool.arun")

        result = asyncio.run(cls("payment_execute").arun({"amount": "10"}))

        self.assertEqual(result, "aran:payment_execute")
        span = client._buffer[-1]
        self.assertEqual(span.kind, "tool")
        self.assertEqual(span.name, "tool.arun:payment_execute")
        attrs = span.attributes
        self.assertEqual(attrs["gen_ai.tool.name"], "payment_execute")
        self.assertEqual(attrs["prometa.tool_name"], "payment_execute")
        self.assertEqual(attrs["tool.name"], "payment_execute")

    def test_distinct_tools_produce_distinct_span_names(self) -> None:
        """The issue's core symptom: two tools collapsing to one span name."""
        client = _make_client()
        cls = self._wrapped_tool_cls("arun", "tool.arun")

        async def main() -> None:
            await cls("search_products").arun({"query": "shoes"})
            await cls("payment_execute").arun({"amount": "10"})

        asyncio.run(main())

        names = [s.name for s in client._buffer[-2:]]
        self.assertEqual(names, ["tool.arun:search_products", "tool.arun:payment_execute"])
        self.assertEqual(len(set(names)), 2)

    def test_tool_error_still_records_identity(self) -> None:
        client = _make_client()

        class Boom(_FakeTool):
            def run(self, *args, **kwargs):
                raise ValueError("nope")

        prometa_langchain._wrap_method(Boom, "run", "tool.run")
        with self.assertRaises(ValueError):
            Boom("payment_execute").run({})

        span = client._buffer[-1]
        self.assertEqual(span.status, "error")
        self.assertEqual(span.name, "tool.run:payment_execute")
        self.assertEqual(span.attributes["prometa.tool_name"], "payment_execute")

    def test_chat_model_attributes_are_unchanged(self) -> None:
        """Regression guard: non-tools must keep the model-name behaviour."""
        attrs = prometa_langchain._attrs_for_object(_FakeChatModel("gpt-4o"))
        self.assertEqual(attrs["gen_ai.request.model"], "gpt-4o")
        self.assertEqual(attrs["langchain.model"], "gpt-4o")
        for key in ("tool.name", "gen_ai.tool.name", "prometa.tool_name"):
            self.assertNotIn(key, attrs)


@unittest.skipUnless(HAS_LANGCHAIN, "langchain-core not installed")
class LangChainRealToolTest(unittest.TestCase):
    """The ``isinstance(obj, BaseTool)`` path, when LangChain is present."""

    def _make_tool(self, tool_name: str, cls_name: str = "SearchTool"):
        namespace = {
            "name": tool_name,
            "description": "test tool",
            "_run": lambda self, *a, **k: "ok",
            "__annotations__": {"name": str, "description": str},
        }
        return type(cls_name, (BaseTool,), namespace)()

    def test_real_tool_yields_canonical_attributes(self) -> None:
        tool = self._make_tool("search_products")

        self.assertEqual(prometa_langchain._tool_name_of(tool), "search_products")
        self.assertEqual(prometa_langchain._kind_for_object(tool), "tool")

        attrs = prometa_langchain._attrs_for_object(tool)
        self.assertEqual(attrs["gen_ai.tool.name"], "search_products")
        self.assertEqual(attrs["prometa.tool_name"], "search_products")
        self.assertEqual(attrs["tool.name"], "search_products")

    def test_llm_named_tool_subclass_is_still_tool_kind(self) -> None:
        """``LLMMathTool`` matches the "llm" substring but is a tool."""
        tool = self._make_tool("llm_math", cls_name="LLMMathTool")
        self.assertEqual(prometa_langchain._kind_for_object(tool), "tool")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
