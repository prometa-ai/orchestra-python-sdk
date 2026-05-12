"""AML instrumentation helpers — memory.read and memory.write spans.

These map to the SDK v0.4 contract for AML features:

  B3  Working Memory (state + conversational context)
  B4  Personalization Memory (profile + episodic + procedural)
  C6  Dynamic Context Assembly
  E3  Goal Persistence & Session Continuity
  E4  Channel & Session Handoff

See ``@prometa/aml-core`` ``src/data/instrumentation-spec.yaml`` for the
attribute contract.

The SDK does not implement the memory store itself — that's the customer's
KV / vector DB / SQL backend. These helpers just RECORD reads and writes
so the AML detectors can prove entity carry-over (B3), personalization
grounding (B4), and goal continuity (E3) actually happened.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Optional

from .client import Prometa


# Scope vocabulary per the AML contract. Anything else raises at call
# time so typos surface immediately rather than producing un-evaluable
# spans the platform silently drops.
_SCOPES = frozenset({"working", "episodic", "profile", "procedural", "goal"})


def _client() -> Optional[Prometa]:
    return Prometa._current


# =============================================================================
# memory.read
# =============================================================================

class _MemoryReadHandle:
    """Yielded inside :func:`memory_read`. Records hit/miss + source id."""

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def hit(
        self,
        *,
        source_record_id: Optional[str] = None,
        user_visible: Optional[bool] = None,
    ) -> None:
        """Mark this read as a cache/store hit.

        ``source_record_id`` is the stable id of the underlying memory
        record. Required for B4 grounding: the AML engine cross-checks
        that personalization references resolve to real stored records,
        not the model's training data.
        """
        if self._span is None:
            return
        a = self._span.attributes
        a["memory.hit"] = True
        if source_record_id:
            a["memory.source_record_id"] = source_record_id
        if user_visible is not None:
            a["memory.user_visible"] = bool(user_visible)

    def miss(self) -> None:
        """Mark this read as a miss (no record found)."""
        if self._span is None:
            return
        self._span.attributes["memory.hit"] = False


@contextmanager
def memory_read(scope: str, key: str) -> Iterator[_MemoryReadHandle]:
    """Emit a ``memory.read`` span.

    ``scope`` ∈ ``{working, episodic, profile, procedural, goal}`` per the
    AML contract.

    Usage::

        with prometa.memory_read("profile", key=f"user:{user_id}") as m:
            record = profile_store.get(user_id)
            if record:
                m.hit(source_record_id=record.id, user_visible=True)
            else:
                m.miss()
    """
    if scope not in _SCOPES:
        raise ValueError(
            f"memory_read: scope must be one of {sorted(_SCOPES)}, got {scope!r}"
        )
    c = _client()
    if c is None:
        yield _MemoryReadHandle(None)
        return
    with c._span("memory", "memory.read") as span:
        span.attributes["memory.scope"] = scope
        span.attributes["memory.key"] = key
        yield _MemoryReadHandle(span)


# =============================================================================
# memory.write
# =============================================================================

def memory_write(
    scope: str,
    key: str,
    *,
    consent_id: Optional[str] = None,
    ttl_seconds: Optional[int] = None,
) -> bool:
    """Emit a one-shot ``memory.write`` span. No body needed — the write
    operation is the event, not its result.

    ``consent_id`` is REQUIRED for cross-session writes (B4 personalization
    memory, E3 goal persistence). Writes without a consent reference are
    accepted but the AML A8 detector flags them.

    Returns ``True`` if a span was emitted, ``False`` if no client active.

    Usage::

        prometa.memory_write(
            "procedural",
            key=f"user:{user_id}:prefs:meal",
            consent_id="cns-marketing-2026-q2",
            ttl_seconds=86400 * 365,
        )
    """
    if scope not in _SCOPES:
        raise ValueError(
            f"memory_write: scope must be one of {sorted(_SCOPES)}, got {scope!r}"
        )
    c = _client()
    if c is None:
        return False
    with c._span("memory", "memory.write") as span:
        a = span.attributes
        a["memory.scope"] = scope
        a["memory.key"] = key
        if consent_id:
            a["memory.consent_id"] = consent_id
        if ttl_seconds is not None:
            a["memory.ttl_seconds"] = int(ttl_seconds)
    return True
