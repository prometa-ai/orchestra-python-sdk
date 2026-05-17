"""Correlation-chain helpers — stamp the platform's
identity-horizontal attributes onto the current span.

The platform's correlation-id resolver reads these resource / span
attributes at OTLP ingest and resolves them to canonical Postgres
ids (Agent, Solution, Tool, Session, Customer). With them set, the
platform's chain materialisation reaches its full form
(`org:sol:agent:tool:cus:user::session:trace:span`); without them,
chain segments stay empty but the platform still works — the values
are purely additive.

See the correlation-id design doc on the platform side:
https://github.com/prometa-ai/agent-hook-v2/blob/main/resources/correlation/correlation-id-design.md

Each helper mirrors the shape of :func:`prometa.session.set_session_id`
— synchronous, no-op outside an active span context, returns ``True``
when the attribute was written.

Usage::

    from prometa import (
        set_customer_id,
        set_user_id,
        set_conversation_id,
        set_request_model,
        set_tool_name,
    )

    @prometa.workflow(name="handle-ticket")
    def handle(ticket):
        # Org-supplied customer key — bridges Prometa telemetry to
        # the org's internal CRM. Stamped once on the workflow root;
        # all nested spans inherit via the platform-side propagation.
        set_customer_id(ticket.customer_external_id)
        set_user_id(ticket.agent_email)
        set_conversation_id(ticket.thread_id)
        ...

        @prometa.tool(name="search-kb")
        def lookup():
            # Span kind is "tool" → prometa.tool_name lights up
            # the platform's Tool entity registration.
            set_tool_name("knowledge-base-search")
            ...

Helper coverage:

    ``set_customer_id(id)``      → ``prometa.customer_id``
    ``set_user_id(id)``          → ``gen_ai.user.id`` (+ ``prometa.user.id``
                                   for fallback by older platform builds)
    ``set_conversation_id(id)``  → ``gen_ai.conversation.id`` (alias of
                                   :func:`prometa.session.set_session_id` —
                                   kept as a separate import for SDK
                                   ergonomics; the canonical helper is
                                   ``set_session_id`` and this one
                                   delegates)
    ``set_request_model(name)``  → ``gen_ai.request.model``
    ``set_tool_name(name)``      → ``prometa.tool_name``

All helpers accept empty / falsy values as a clear-attribute signal.
"""

from __future__ import annotations

from typing import Optional


# Canonical attribute keys — match the platform's resolver
# (`src/lib/ingest/correlation-resolver.ts`).
_CUSTOMER_ATTR = "prometa.customer_id"
_USER_OTEL_ATTR = "gen_ai.user.id"
_USER_PROMETA_FALLBACK_ATTR = "prometa.user.id"
_CONVERSATION_ATTR = "gen_ai.conversation.id"
_MODEL_ATTR = "gen_ai.request.model"
_TOOL_NAME_ATTR = "prometa.tool_name"


def _set_attr(attr: str, value: Optional[str]) -> bool:
    """Internal: stamp ``value`` at ``attr`` on the active span.

    Returns ``True`` if a span was found, ``False`` if there was no
    active span context. Empty / falsy values pop the attribute.
    """
    from . import _context

    span = _context.current_span()
    if span is None:
        return False
    if value:
        span.attributes[attr] = str(value)
    else:
        span.attributes.pop(attr, None)
    return True


def set_customer_id(customer_id: str) -> bool:
    """Stamp the org-supplied customer key on the current span.

    The platform's correlation-id resolver maps this to the
    ``customer_id`` identity horizontal in the canonical chain. Values
    are validated against ``Organization.customerNamespace`` regex at
    ingest — a malformed customer id is rejected before any
    ClickHouse row is written.

    Typical use: stamp once at the workflow root with the customer's
    id in your CRM / data warehouse (``cus_abc123``,
    ``acme-corp:11042``, etc.). All nested spans inherit via the
    platform-side propagation; explicit re-stamping on nested spans
    is permitted but rarely needed.
    """
    return _set_attr(_CUSTOMER_ATTR, customer_id)


def set_user_id(user_id: str) -> bool:
    """Stamp the end-user identifier on the current span.

    Writes the OTel-standard ``gen_ai.user.id`` attribute and a
    Prometa-only fallback ``prometa.user.id`` for older platform
    builds that haven't picked up the OTel-standard key yet. Both
    point at the same value; the platform's resolver reads the first
    one it finds.

    Avoid stuffing PII (email, phone) in this field — it's indexed
    and visible to anyone with ``traces:read``. Use a stable internal
    user id instead.
    """
    from . import _context

    span = _context.current_span()
    if span is None:
        return False
    if user_id:
        s = str(user_id)
        span.attributes[_USER_OTEL_ATTR] = s
        span.attributes[_USER_PROMETA_FALLBACK_ATTR] = s
    else:
        span.attributes.pop(_USER_OTEL_ATTR, None)
        span.attributes.pop(_USER_PROMETA_FALLBACK_ATTR, None)
    return True


def set_conversation_id(conversation_id: str) -> bool:
    """Stamp the conversation / thread id on the current span.

    Functionally equivalent to :func:`prometa.session.set_session_id`
    — both write ``gen_ai.conversation.id``. Exposed as a separate
    helper for callers whose mental model is "conversation" rather
    than "session"; pick whichever reads better in your code.
    """
    from .session import set_session_id

    return set_session_id(conversation_id)


def set_request_model(model: str) -> bool:
    """Stamp the LLM model name on the current span.

    Use when your LLM client doesn't auto-emit
    ``gen_ai.request.model`` — most LLM-instrumentation libraries
    (OpenInference, Traceloop) set it automatically on the LLM-typed
    span they create, in which case this helper is unnecessary.
    """
    return _set_attr(_MODEL_ATTR, model)


def set_tool_name(tool_name: str) -> bool:
    """Stamp the logical tool name on the current span.

    Intended for tool-typed spans (``@prometa.tool``). The platform's
    correlation-id resolver auto-registers a ``Tool`` row in Postgres
    on first sighting, scoped to the current (org, solution) pair. The
    ``prometa.tool_name`` attribute is the lookup key; the canonical
    ``tool_id`` flows into ClickHouse via the resolver, not via this
    helper.

    Calling this helper on a non-tool span is permitted but the
    platform-side promotion to a ``tool_id`` identity-horizontal
    only fires when the span's kind is ``tool``.
    """
    return _set_attr(_TOOL_NAME_ATTR, tool_name)


__all__ = [
    "set_customer_id",
    "set_user_id",
    "set_conversation_id",
    "set_request_model",
    "set_tool_name",
]
