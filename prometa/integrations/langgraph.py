"""LangGraph auto-instrumentation.

Patches `langgraph.graph.StateGraph` (and CompiledGraph) so each graph
invocation becomes a Prometa workflow span. Individual node executions
inherit as child spans through the LangChain Runnable patch.

Usage::

    from prometa import Prometa
    from prometa.integrations import langgraph as prometa_lg, langchain as prometa_lc

    prometa = Prometa(endpoint=..., agent_name="my-graph")
    prometa_lc.install()   # for Runnable.invoke
    prometa_lg.install()   # for StateGraph.compile()-returned graphs
"""

from __future__ import annotations

import asyncio
import functools
from typing import Callable, Optional

from ..client import Prometa


_INSTALLED = False


def _client() -> Optional[Prometa]:
    return Prometa._current


def _wrap_graph_method(cls: type, method_name: str, span_label: str) -> None:
    if method_name not in cls.__dict__:
        return
    original = getattr(cls, method_name, None)
    if original is None or getattr(original, "__prometa_wrapped__", False):
        return
    is_async = asyncio.iscoroutinefunction(original)

    if is_async:

        @functools.wraps(original)
        async def aw(self, *args, **kwargs):  # type: ignore[no-redef]
            client = _client()
            if client is None:
                return await original(self, *args, **kwargs)
            with client._span("workflow", f"{span_label}:{type(self).__name__}") as span:
                span.attributes["gen_ai.framework"] = "langgraph"
                span.attributes["langgraph.class"] = type(self).__name__
                try:
                    return await original(self, *args, **kwargs)
                except Exception as e:
                    span.status = "error"
                    span.attributes["error.message"] = str(e)
                    raise

        wrapped: Callable = aw
    else:

        @functools.wraps(original)
        def sw(self, *args, **kwargs):  # type: ignore[no-redef]
            client = _client()
            if client is None:
                return original(self, *args, **kwargs)
            with client._span("workflow", f"{span_label}:{type(self).__name__}") as span:
                span.attributes["gen_ai.framework"] = "langgraph"
                span.attributes["langgraph.class"] = type(self).__name__
                try:
                    return original(self, *args, **kwargs)
                except Exception as e:
                    span.status = "error"
                    span.attributes["error.message"] = str(e)
                    raise

        wrapped = sw

    wrapped.__prometa_wrapped__ = True  # type: ignore[attr-defined]
    setattr(cls, method_name, wrapped)


def install() -> bool:
    global _INSTALLED
    if _INSTALLED:
        return True
    try:
        from langgraph.graph.state import CompiledStateGraph  # type: ignore
    except Exception:
        try:
            # Older LangGraph layouts
            from langgraph.graph import CompiledGraph as CompiledStateGraph  # type: ignore
        except Exception:  # pragma: no cover
            return False

    def all_subclasses(cls: type) -> set[type]:
        out: set[type] = set()
        stack = [cls]
        while stack:
            c = stack.pop()
            for sub in c.__subclasses__():
                if sub not in out:
                    out.add(sub)
                    stack.append(sub)
        return out

    for klass in [CompiledStateGraph, *all_subclasses(CompiledStateGraph)]:
        for m, label in (
            ("invoke", "langgraph.invoke"),
            ("ainvoke", "langgraph.ainvoke"),
            ("stream", "langgraph.stream"),
            ("astream", "langgraph.astream"),
        ):
            try:
                _wrap_graph_method(klass, m, label)
            except Exception:
                continue

    _INSTALLED = True
    return True
