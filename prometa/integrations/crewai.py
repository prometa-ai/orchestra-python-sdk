"""CrewAI auto-instrumentation.

Patches the canonical execution entry points of the CrewAI framework
(`pip install crewai`) so every Crew run, Agent execution, and Task
becomes a Prometa span.

Usage::

    from prometa import Prometa
    from prometa.integrations import crewai as prometa_crewai

    prometa = Prometa(endpoint=..., agent_name="my-crew")
    prometa_crewai.install()

    # Normal CrewAI code emits Prometa spans automatically.
"""

from __future__ import annotations

import asyncio
import functools
from typing import Any, Callable, Optional

from ..client import Prometa


_INSTALLED = False


def _client() -> Optional[Prometa]:
    return Prometa._current


def _attrs_for(obj: Any, kind: str) -> dict:
    out: dict = {
        "gen_ai.framework": "crewai",
        "crewai.kind": kind,
        "crewai.class": type(obj).__name__,
    }
    for attr in ("role", "goal", "name", "description"):
        v = getattr(obj, attr, None)
        if isinstance(v, str):
            out[f"crewai.{attr}"] = v[:200]
    return out


def _wrap_method(
    cls: type, method_name: str, kind: str, span_label: str
) -> None:
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
            with client._span(
                "agent" if kind == "agent" else ("workflow" if kind == "crew" else "task"),
                f"{span_label}:{getattr(self, 'role', getattr(self, 'name', type(self).__name__))}",
            ) as span:
                span.attributes.update(_attrs_for(self, kind))
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
                "agent" if kind == "agent" else ("workflow" if kind == "crew" else "task"),
                f"{span_label}:{getattr(self, 'role', getattr(self, 'name', type(self).__name__))}",
            ) as span:
                span.attributes.update(_attrs_for(self, kind))
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
    """Patch CrewAI entry points. Returns True if patched, False if CrewAI
    isn't importable."""
    global _INSTALLED
    if _INSTALLED:
        return True

    try:
        import crewai  # type: ignore
    except Exception:  # pragma: no cover
        return False

    # Walk all subclasses at install time — CrewAI users frequently subclass.
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

    targets: list[tuple[type, list[tuple[str, str, str]]]] = []
    Crew = getattr(crewai, "Crew", None)
    if Crew is not None:
        targets.append(
            (
                Crew,
                [
                    ("kickoff", "crew", "crew.kickoff"),
                    ("kickoff_async", "crew", "crew.kickoff_async"),
                ],
            )
        )
    Agent = getattr(crewai, "Agent", None)
    if Agent is not None:
        targets.append(
            (
                Agent,
                [
                    ("execute_task", "agent", "agent.execute_task"),
                ],
            )
        )
    Task = getattr(crewai, "Task", None)
    if Task is not None:
        targets.append(
            (
                Task,
                [
                    ("execute_sync", "task", "task.execute_sync"),
                    ("execute_async", "task", "task.execute_async"),
                ],
            )
        )

    for cls, methods in targets:
        for klass in [cls, *all_subclasses(cls)]:
            for method_name, kind, label in methods:
                try:
                    _wrap_method(klass, method_name, kind, label)
                except Exception:
                    continue

    _INSTALLED = True
    return True
