"""Advanced Cost Optimization — SDK-side token budget throttling
(V1 §6 Q8).

Wrap LLM calls with :func:`check_budget(tokens)` to enforce a per-window
cap. Exceeding the budget raises :class:`BudgetExceededError` unless
``mode="soft"``, in which case the call is allowed but a warning span
attribute is set.

Usage::

    from prometa import Prometa
    from prometa.budget import TokenBudget, BudgetExceededError

    prometa = Prometa(endpoint=..., agent_name="my-app")
    budget = TokenBudget(limit_tokens=1_000_000, window_seconds=86_400, mode="hard")

    try:
        budget.check(approx_input_tokens + approx_output_tokens)
        resp = llm.chat(...)
    except BudgetExceededError:
        resp = "(request throttled: token budget exceeded)"
"""

from __future__ import annotations

import threading
import time
from typing import Optional

from .client import Prometa


class BudgetExceededError(RuntimeError):
    """Raised when a hard token budget is exhausted."""


class TokenBudget:
    """A sliding-window token budget shared across threads within one
    process. Not distributed — for cross-instance budgets call the
    platform API at runtime."""

    def __init__(
        self,
        limit_tokens: int,
        window_seconds: int = 86_400,
        mode: str = "hard",
        label: str = "default",
    ) -> None:
        if limit_tokens <= 0:
            raise ValueError("limit_tokens must be positive")
        if mode not in ("hard", "soft"):
            raise ValueError("mode must be 'hard' or 'soft'")
        self._limit = limit_tokens
        self._window = window_seconds
        self._mode = mode
        self._label = label
        self._lock = threading.Lock()
        # (timestamp_seconds, tokens)
        self._entries: list[tuple[float, int]] = []

    def _evict(self, now: Optional[float] = None) -> int:
        now = now or time.time()
        cutoff = now - self._window
        while self._entries and self._entries[0][0] < cutoff:
            self._entries.pop(0)
        return sum(tokens for _, tokens in self._entries)

    def used(self) -> int:
        with self._lock:
            return self._evict()

    def remaining(self) -> int:
        return max(0, self._limit - self.used())

    def check(self, tokens: int) -> None:
        """Attempt to consume ``tokens``. In ``hard`` mode, raises
        :class:`BudgetExceededError` if it would exceed the limit. In
        ``soft`` mode, records the consumption and tags the current
        Prometa span with ``budget.exceeded=true`` but does not raise."""
        with self._lock:
            total = self._evict() + max(0, tokens)
            exceeded = total > self._limit
            self._entries.append((time.time(), int(tokens)))
        client = Prometa._current
        if client is not None:
            # Find current top-of-stack span and tag it
            try:
                from . import _context
                span = _context.current_span()
                if span is not None:
                    span.attributes.update(
                        {
                            "budget.label": self._label,
                            "budget.limit": self._limit,
                            "budget.used": total,
                            "budget.remaining": max(0, self._limit - total),
                            "budget.exceeded": exceeded,
                        }
                    )
            except Exception:
                pass
        if exceeded and self._mode == "hard":
            raise BudgetExceededError(
                f"Token budget '{self._label}' exceeded: used {total}/{self._limit} "
                f"in last {self._window}s"
            )
