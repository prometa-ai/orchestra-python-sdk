"""Explicit data-flow refs between sibling spans.

OTel's ``parent_span_id`` captures the call stack — who *invoked* whom.
But for agent traces the more interesting question is often "whose
*output* did this span consume?", and that's almost always a sibling
relationship: an LLM emits a tool_call, the agent dispatches a sibling
tool span. Without an explicit ref the platform can only infer the
link from temporal proximity, which is brittle and opaque to
LLM-as-a-Judge evaluators that want to reason about flow rather than
timing.

These helpers let a span declare:

    set_input_ref(other_span_id)   # "I consumed the output of <id>"
    set_output_ref(other_span_id)  # "<id> consumed my output"

The platform reads ``prometa.input_ref`` / ``prometa.output_ref`` at
ingest, persists them as dedicated columns, and the trace UI's
Causal-context block surfaces them as clickable "Input from" / "Output
to" rows alongside parent / siblings.

Usage — capture-the-span pattern (the common case)::

    from prometa import current_span_id, set_input_ref

    @prometa.agent(name="openai.chat")
    async def call_llm(prompt: str) -> dict:
        # Snapshot the active span so the orchestrator can reference it
        # downstream. Stash on a closure / dict / contextvar — anywhere
        # the next span can find it.
        nonlocal_state["last_llm_span_id"] = current_span_id()
        ...

    @prometa.tool(name="kb-search")
    async def kb_search(q: str) -> list[str]:
        # Declare: this tool span consumed the LLM's output.
        set_input_ref(nonlocal_state["last_llm_span_id"])
        ...

Calling these helpers outside an active span context is a no-op
(returns ``False``) — there's no current span to stamp. Same contract
as ``set_session_id``.
"""

from __future__ import annotations

from typing import Optional


# Canonical attribute keys — Prometa-extension semconv. Read by the
# platform's OTLP ingest, also exposed in span.attributes for any
# downstream tooling (replay, judge runs) that wants to walk the
# data-flow DAG without joining tables.
_INPUT_REF_ATTR = "prometa.input_ref"
_OUTPUT_REF_ATTR = "prometa.output_ref"


def set_input_ref(ref_span_id: str) -> bool:
    """Declare that the currently-active span consumed ``ref_span_id``'s output.

    Returns ``True`` if a span was found and updated, ``False`` if there
    was no active span context (call happened outside a Prometa-traced
    block) or ``ref_span_id`` is empty/falsy. Empty value clears the
    attribute — symmetric with ``set_session_id``.

    The id is opaque to the SDK; pass whatever the producer's
    ``current_span_id()`` returned. No validation that the target span
    actually exists in the same trace — the platform tolerates dangling
    refs (renders nothing rather than throwing) so cross-batch races
    and sampling drops don't surface as user-facing errors.
    """
    from . import _context  # local import — avoid module-load circular

    span = _context.current_span()
    if span is None:
        return False
    if ref_span_id:
        span.attributes[_INPUT_REF_ATTR] = str(ref_span_id)
    else:
        span.attributes.pop(_INPUT_REF_ATTR, None)
    return True


def set_output_ref(ref_span_id: str) -> bool:
    """Symmetric counterpart to :func:`set_input_ref`.

    Declare that the currently-active span's output was consumed by
    ``ref_span_id``. Rarely the right direction — usually only the
    consumer knows its source — but included for the cases where a
    producer DOES know its consumer up front (e.g. a planner span
    scheduling a specific downstream call by id).
    """
    from . import _context

    span = _context.current_span()
    if span is None:
        return False
    if ref_span_id:
        span.attributes[_OUTPUT_REF_ATTR] = str(ref_span_id)
    else:
        span.attributes.pop(_OUTPUT_REF_ATTR, None)
    return True


def get_input_ref() -> Optional[str]:
    """Return the input-ref id stamped on the current span, or ``None``."""
    from . import _context

    span = _context.current_span()
    if span is None:
        return None
    val = span.attributes.get(_INPUT_REF_ATTR)
    return str(val) if val else None


def get_output_ref() -> Optional[str]:
    """Return the output-ref id stamped on the current span, or ``None``."""
    from . import _context

    span = _context.current_span()
    if span is None:
        return None
    val = span.attributes.get(_OUTPUT_REF_ATTR)
    return str(val) if val else None


def current_span_id() -> Optional[str]:
    """Return the ``span_id`` of the currently-active span, or ``None``.

    Convenience entrypoint for the capture-the-span pattern: callers
    that need to pass their span id to a downstream
    :func:`set_input_ref` use this instead of poking at the private
    ``_context`` module.
    """
    from . import _context

    span = _context.current_span()
    return span.span_id if span is not None else None
