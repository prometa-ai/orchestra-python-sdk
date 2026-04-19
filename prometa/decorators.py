"""Module-level decorator shortcuts.

Resolve to whichever Prometa client was last constructed. Keeps the
ergonomic `@prometa.workflow` form working alongside `from prometa import workflow`.
"""

from __future__ import annotations

from typing import Callable, Optional

from .client import Prometa


def _bound(method_name: str, name: Optional[str]) -> Callable:
    def wrap(fn: Callable) -> Callable:
        client = Prometa._current
        if client is None:
            # No client configured — return fn unchanged so app still works.
            return fn
        return getattr(client, method_name)(name)(fn)

    return wrap


def workflow(name: Optional[str] = None) -> Callable:
    return _bound("workflow", name)


def agent(name: Optional[str] = None) -> Callable:
    return _bound("agent", name)


def tool(name: Optional[str] = None) -> Callable:
    return _bound("tool", name)


def task(name: Optional[str] = None) -> Callable:
    return _bound("task", name)
