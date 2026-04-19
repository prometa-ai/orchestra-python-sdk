"""Microsoft Semantic Kernel auto-instrumentation.

Patches the canonical invocation points in the Python Semantic Kernel
SDK (`pip install semantic-kernel`) so every Kernel function invocation,
agent run, and plugin call becomes a Prometa span.

Usage::

    from prometa import Prometa
    from prometa.integrations import semantic_kernel as prometa_sk

    prometa = Prometa(endpoint=..., agent_name="my-sk-app")
    prometa_sk.install()
"""

from __future__ import annotations

import asyncio
import functools
from typing import Any, Callable, Optional

from ..client import Prometa


_INSTALLED = False


def _client() -> Optional[Prometa]:
    return Prometa._current


def _attrs(obj: Any, kind: str) -> dict:
    out: dict = {
        "gen_ai.framework": "semantic-kernel",
        "sk.kind": kind,
        "sk.class": type(obj).__name__,
    }
    for attr in ("name", "plugin_name", "description"):
        v = getattr(obj, attr, None)
        if isinstance(v, str):
            out[f"sk.{attr}"] = v[:200]
    return out


def _wrap(cls: type, method_name: str, kind: str, label: str) -> None:
    if method_name not in cls.__dict__:
        return
    original = getattr(cls, method_name, None)
    if original is None or getattr(original, "__prometa_wrapped__", False):
        return
    is_async = asyncio.iscoroutinefunction(original)

    span_kind = {
        "kernel": "workflow",
        "function": "task",
        "agent": "agent",
        "plugin": "tool",
    }.get(kind, "task")

    if is_async:

        @functools.wraps(original)
        async def aw(self, *args, **kwargs):  # type: ignore[no-redef]
            client = _client()
            if client is None:
                return await original(self, *args, **kwargs)
            with client._span(
                span_kind,
                f"{label}:{getattr(self, 'name', type(self).__name__)}",
            ) as span:
                span.attributes.update(_attrs(self, kind))
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
            with client._span(
                span_kind,
                f"{label}:{getattr(self, 'name', type(self).__name__)}",
            ) as span:
                span.attributes.update(_attrs(self, kind))
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
        import semantic_kernel as sk  # type: ignore
    except Exception:  # pragma: no cover
        return False

    # Kernel
    Kernel = getattr(sk, "Kernel", None)
    if Kernel is not None:
        for m in ("invoke", "invoke_async", "invoke_prompt", "invoke_prompt_async"):
            try:
                _wrap(Kernel, m, "kernel", f"kernel.{m}")
            except Exception:
                continue

    # KernelFunction
    try:
        from semantic_kernel.functions.kernel_function import KernelFunction  # type: ignore
        for m in ("invoke", "invoke_async"):
            _wrap(KernelFunction, m, "function", f"kernel_function.{m}")
    except Exception:
        pass

    # Agent API (newer SK releases)
    try:
        from semantic_kernel.agents import Agent  # type: ignore
        for m in ("invoke", "invoke_async", "get_response", "get_response_async"):
            _wrap(Agent, m, "agent", f"agent.{m}")
    except Exception:
        pass

    _INSTALLED = True
    return True
