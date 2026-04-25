"""Session id helpers — group multiple traces into a single conversational
session in the Prometa Trace / Session Explorer.

A session is the natural unit of analysis for chat-style agents: one chat
thread, one customer support conversation, one user task. Without it,
the platform's Trace Explorer shows a flat list keyed only by trace id
and an operator can't answer "show me the whole conversation, not just
this single API call."

Usage:

    from prometa import set_session_id

    # Anywhere inside a Prometa-traced workflow / agent / tool span:
    set_session_id("chat-conv-abc123")

    # Or at decorator time, when the session id is known up front:
    @prometa.workflow(name="handle-turn", session_id=conversation_id)
    def handle_turn(...):
        ...

The helper writes the OTel-standard ``gen_ai.conversation.id`` attribute
onto the *currently active* span. The platform ingest reads that
attribute (with ``session.id`` and ``prometa.session_id`` accepted as
fallbacks) and propagates it onto every span + the trace row at write
time, so any nested span automatically inherits the grouping.

Calling ``set_session_id`` outside a span context is a no-op (returns
``False``) — there's no current span to stamp. This is intentional:
session identity belongs to the trace it scopes, so calling it before
your first ``@prometa.workflow`` decorator fires has no defined target.
"""

from __future__ import annotations

from typing import Optional


# Canonical attribute key — OTel GenAI semantic convention. Read by the
# platform's OTLP ingest, also exposed in span.attributes for any
# downstream tooling (judge runs, replay, etc.) that wants to filter by
# conversation.
_SESSION_ATTR = "gen_ai.conversation.id"


def set_session_id(session_id: str) -> bool:
    """Stamp ``session_id`` on the current span.

    Returns ``True`` if a span was found and updated, ``False`` if there
    was no active span context (call happened outside a Prometa-traced
    block). Empty / falsy values are accepted and clear the attribute.

    The id is opaque — any string the calling app uses as the natural
    key for one chat / task / user-session. Avoid stuffing PII (email,
    user-identifying values) in here; the field is indexed and may be
    visible to anyone with ``traces:read`` permission.
    """
    from . import _context  # local import — avoid module-load circular

    span = _context.current_span()
    if span is None:
        return False
    if session_id:
        span.attributes[_SESSION_ATTR] = str(session_id)
    else:
        span.attributes.pop(_SESSION_ATTR, None)
    return True


def get_session_id() -> Optional[str]:
    """Return the session id on the current span, or ``None`` if unset
    or there's no active span."""
    from . import _context

    span = _context.current_span()
    if span is None:
        return None
    val = span.attributes.get(_SESSION_ATTR)
    return val if val else None
