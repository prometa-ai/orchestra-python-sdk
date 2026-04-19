"""Per-task span stack. Uses contextvars so spans nest correctly across
async/await boundaries and sync calls alike.
"""

from __future__ import annotations

from contextvars import ContextVar, Token
from typing import List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .client import _Span

_stack: ContextVar[List["_Span"]] = ContextVar("_prometa_span_stack", default=[])


def current_span() -> Optional["_Span"]:
    stack = _stack.get()
    return stack[-1] if stack else None


def push(span: "_Span") -> Token:
    stack = list(_stack.get())
    stack.append(span)
    return _stack.set(stack)


def pop(token: Token) -> None:
    _stack.reset(token)
