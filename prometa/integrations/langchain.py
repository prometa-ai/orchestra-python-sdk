"""LangChain / LangGraph auto-instrumentation.

Calling :func:`install()` patches the LangChain core primitives so every
chain / runnable / tool / LLM call automatically becomes a Prometa span,
nested under the current trace if one is active.

This is intentionally minimal — it does not depend on
``langchain-instrumentor`` or any third-party instrumentation. It only
wraps the canonical entry points that virtually every LangChain
program goes through:

- ``Runnable.invoke`` / ``Runnable.ainvoke``
- ``Runnable.batch`` / ``Runnable.abatch``
- ``BaseChatModel.invoke`` / ``BaseChatModel.ainvoke``
- ``BaseTool.run`` / ``BaseTool.arun``

Usage::

    from prometa import Prometa
    from prometa.integrations import langchain as prometa_langchain

    prometa = Prometa(endpoint=..., agent_name="my-agent")
    prometa_langchain.install()

    # ...your normal LangChain code emits Prometa spans...
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from typing import Any, Callable, Optional

from ..client import Prometa


_INSTALLED = False


def _client() -> Optional[Prometa]:
    return Prometa._current


def _kind_for_object(obj: Any, default: str = "tool") -> str:
    """Map a LangChain object class to a Prometa span kind."""
    cls_name = type(obj).__name__.lower()
    if "chatmodel" in cls_name or "llm" in cls_name:
        return "agent"  # Chat/LLM span types are surfaced as agents
    if "tool" in cls_name:
        return "tool"
    if "chain" in cls_name or "runnable" in cls_name or "graph" in cls_name:
        return "workflow"
    return default


def _attrs_for_object(obj: Any) -> dict:
    out: dict = {
        "gen_ai.framework": "langchain",
        "langchain.class": type(obj).__name__,
    }
    # Best-effort: surface the model name if we're wrapping a chat model.
    for attr in ("model", "model_name", "name"):
        if hasattr(obj, attr):
            v = getattr(obj, attr, None)
            if isinstance(v, str):
                out[f"langchain.{attr}"] = v
                if attr in ("model", "model_name"):
                    out["gen_ai.request.model"] = v
                break
    return out


def _wrap_method(cls: type, method_name: str, span_name: str) -> None:
    """Replace ``cls.method_name`` with a Prometa-instrumented wrapper.

    Only patches if the method is actually defined on ``cls`` — inherited
    methods are skipped because patching the base class already covers them.
    Idempotent: a method already wrapped exposes ``__prometa_wrapped__``
    so repeated calls to ``install()`` are no-ops.
    """
    if method_name not in cls.__dict__:
        return
    original = getattr(cls, method_name, None)
    if original is None or getattr(original, "__prometa_wrapped__", False):
        return

    is_async = asyncio.iscoroutinefunction(original)

    if is_async:

        @functools.wraps(original)
        async def async_wrapper(self, *args, **kwargs):  # type: ignore[no-redef]
            client = _client()
            if client is None:
                return await original(self, *args, **kwargs)
            kind = _kind_for_object(self)
            with client._span(kind, f"{span_name}:{type(self).__name__}") as span:
                span.attributes.update(_attrs_for_object(self))
                try:
                    return await original(self, *args, **kwargs)
                except Exception as e:
                    span.status = "error"
                    span.attributes["error.message"] = str(e)
                    raise

        wrapped: Callable = async_wrapper

    else:

        @functools.wraps(original)
        def sync_wrapper(self, *args, **kwargs):  # type: ignore[no-redef]
            client = _client()
            if client is None:
                return original(self, *args, **kwargs)
            kind = _kind_for_object(self)
            with client._span(kind, f"{span_name}:{type(self).__name__}") as span:
                span.attributes.update(_attrs_for_object(self))
                try:
                    return original(self, *args, **kwargs)
                except Exception as e:
                    span.status = "error"
                    span.attributes["error.message"] = str(e)
                    raise

        wrapped = sync_wrapper

    wrapped.__prometa_wrapped__ = True  # type: ignore[attr-defined]
    setattr(cls, method_name, wrapped)


def install() -> bool:
    """Patch LangChain entry points. Returns True if patching was applied,
    False if LangChain isn't importable.
    """
    global _INSTALLED
    if _INSTALLED:
        return True

    try:
        from langchain_core.runnables.base import Runnable  # type: ignore
    except Exception:  # pragma: no cover - LangChain not installed
        return False

    targets: list[tuple[type, list[tuple[str, str]]]] = []
    targets.append(
        (
            Runnable,
            [
                ("invoke", "runnable.invoke"),
                ("ainvoke", "runnable.ainvoke"),
                ("batch", "runnable.batch"),
                ("abatch", "runnable.abatch"),
            ],
        )
    )

    try:
        from langchain_core.language_models.chat_models import BaseChatModel  # type: ignore

        targets.append(
            (
                BaseChatModel,
                [
                    ("invoke", "chat.invoke"),
                    ("ainvoke", "chat.ainvoke"),
                ],
            )
        )
    except Exception:
        pass

    try:
        from langchain_core.tools import BaseTool  # type: ignore

        targets.append(
            (
                BaseTool,
                [
                    ("run", "tool.run"),
                    ("arun", "tool.arun"),
                ],
            )
        )
    except Exception:
        pass

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

    for cls, methods in targets:
        for klass in [cls, *all_subclasses(cls)]:
            for method_name, span_name in methods:
                try:
                    _wrap_method(klass, method_name, span_name)
                except Exception:
                    # Never fail user code because of instrumentation.
                    pass

    _INSTALLED = True
    return True


def uninstall() -> None:  # pragma: no cover - test convenience only
    """No-op placeholder. Monkey-patches are not currently reverted; restart
    the process to remove instrumentation."""
    return None


# Silence "unused import" warnings.
_ = inspect
