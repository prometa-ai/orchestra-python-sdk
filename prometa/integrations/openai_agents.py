"""OpenAI Agents SDK auto-instrumentation.

Patches the canonical entry points of the OpenAI Agents SDK
(`pip install openai-agents`) so every ``Agent`` run and ``Runner`` call
becomes a Prometa span, nested under the current trace.

Usage::

    from prometa import Prometa
    from prometa.integrations import openai_agents as prometa_oa

    prometa = Prometa(endpoint=..., agent_name="my-agent")
    prometa_oa.install()

    from agents import Agent, Runner
    agent = Agent(name="triage", instructions="...")
    result = Runner.run_sync(agent, "hello")

Targets in order of preference (we patch whichever exist):

- ``agents.Runner.run_sync``
- ``agents.Runner.run``  (async)
- ``agents.Agent.run_sync``
- ``agents.Agent.run``   (async)
"""

from __future__ import annotations

import asyncio
import functools
from typing import Any, Callable, Optional

from ..client import Prometa


_INSTALLED = False


def _client() -> Optional[Prometa]:
    return Prometa._current


def _attrs_for_agent(agent: Any) -> dict:
    out: dict = {"gen_ai.framework": "openai-agents"}
    for attr in ("name", "model", "instructions"):
        val = getattr(agent, attr, None)
        if isinstance(val, str):
            out[f"openai.agent.{attr}"] = val[:200]
            if attr == "model":
                out["gen_ai.request.model"] = val
    return out


def _extract_agent(args: tuple, kwargs: dict) -> Any:
    """The Agent is either the first positional arg (Runner.run(agent, ...)) or
    `self` (agent.run(...))."""
    if args and hasattr(args[0], "name"):
        return args[0]
    if "agent" in kwargs and hasattr(kwargs["agent"], "name"):
        return kwargs["agent"]
    return None


def _wrap_method(cls: type, method_name: str, span_label: str) -> None:
    if method_name not in cls.__dict__:
        return
    original = getattr(cls, method_name, None)
    if original is None or getattr(original, "__prometa_wrapped__", False):
        return

    is_async = asyncio.iscoroutinefunction(original)

    if is_async:

        @functools.wraps(original)
        async def aw(self_or_cls, *args, **kwargs):  # type: ignore[no-redef]
            client = _client()
            if client is None:
                return await original(self_or_cls, *args, **kwargs)
            agent = _extract_agent(args, kwargs)
            if agent is None and hasattr(self_or_cls, "name"):
                agent = self_or_cls
            agent_name = getattr(agent, "name", "unknown")
            with client._span("agent", f"{span_label}:{agent_name}") as span:
                if agent is not None:
                    span.attributes.update(_attrs_for_agent(agent))
                try:
                    return await original(self_or_cls, *args, **kwargs)
                except Exception as e:
                    span.status = "error"
                    span.attributes["error.message"] = str(e)
                    raise

        wrapped: Callable = aw

    else:

        @functools.wraps(original)
        def sw(self_or_cls, *args, **kwargs):  # type: ignore[no-redef]
            client = _client()
            if client is None:
                return original(self_or_cls, *args, **kwargs)
            agent = _extract_agent(args, kwargs)
            if agent is None and hasattr(self_or_cls, "name"):
                agent = self_or_cls
            agent_name = getattr(agent, "name", "unknown")
            with client._span("agent", f"{span_label}:{agent_name}") as span:
                if agent is not None:
                    span.attributes.update(_attrs_for_agent(agent))
                try:
                    return original(self_or_cls, *args, **kwargs)
                except Exception as e:
                    span.status = "error"
                    span.attributes["error.message"] = str(e)
                    raise

        wrapped = sw

    wrapped.__prometa_wrapped__ = True  # type: ignore[attr-defined]
    setattr(cls, method_name, wrapped)


def install() -> bool:
    """Patch openai-agents entry points. Returns True if patching applied,
    False if the SDK isn't importable."""
    global _INSTALLED
    if _INSTALLED:
        return True

    try:
        import agents  # type: ignore
    except Exception:  # pragma: no cover - SDK not installed
        return False

    targets: list[tuple[type, list[tuple[str, str]]]] = []

    Runner = getattr(agents, "Runner", None)
    if Runner is not None:
        targets.append(
            (
                Runner,
                [
                    ("run_sync", "runner.run_sync"),
                    ("run", "runner.run"),
                ],
            )
        )

    Agent = getattr(agents, "Agent", None)
    if Agent is not None:
        targets.append(
            (
                Agent,
                [
                    ("run_sync", "agent.run_sync"),
                    ("run", "agent.run"),
                ],
            )
        )

    patched_any = False
    for cls, methods in targets:
        for method_name, span_label in methods:
            before = getattr(cls, method_name, None)
            try:
                _wrap_method(cls, method_name, span_label)
            except Exception:
                continue
            after = getattr(cls, method_name, None)
            if after is not before:
                patched_any = True

    _INSTALLED = True
    return patched_any or True
