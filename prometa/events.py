"""AML instrumentation helper — ``event.trigger`` span.

Maps to the SDK v0.4 contract bundled in the platform repo at
``resources/aml/phase-0/instrumentation-spec.yaml`` (span name
``event.trigger``). Feeds the platform's AML scoring engine for
features E1 (event-driven orchestration), E2 (proactive context), E4
(channel handoff), F7 (proactive optimization). The contract requires
``event.consent_id`` whenever the source is ``agent_initiated`` —
that's how the auditor verifies proactive actions have explicit consent.

ADDITIVE only. No raw kwargs — event metadata is non-sensitive.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Optional

from .client import Prometa


def _client() -> Optional[Prometa]:
    return Prometa._current


_EVENT_SOURCES = {
    "user_message",
    "webhook",
    "scheduler",
    "market_trigger",
    "iot_event",
    "agent_initiated",
    "channel_switch",
}


class _EventTriggerHandle:
    """Yielded inside :func:`event_trigger`. Records FSM transition."""

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def fsm_transition(
        self,
        *,
        from_state: str,
        to_state: str,
    ) -> None:
        """Record the FSM state transition the event drives.

        E1 reads these to verify the agent's orchestration is genuinely
        event-driven rather than naively single-turn.
        """
        if self._span is None:
            return
        a = self._span.attributes
        a["event.fsm_from_state"] = from_state
        a["event.fsm_to_state"] = to_state


@contextmanager
def event_trigger(
    source: str,
    *,
    consent_id: Optional[str] = None,
) -> Iterator[_EventTriggerHandle]:
    """Emit an ``event.trigger`` span around a non-user-initiated turn.

    ``source`` ∈ ``{user_message, webhook, scheduler, market_trigger,
    iot_event, agent_initiated, channel_switch}``.

    ``consent_id`` IS REQUIRED when ``source == "agent_initiated"`` —
    the AML A8 detector flags missing consent on proactive actions as
    a compliance-critical signal.

    Usage::

        # Scheduled job that fires a proactive notification:
        with prometa.event_trigger(
            "agent_initiated",
            consent_id=user_consent_id,
        ) as ev:
            ev.fsm_transition(from_state="idle", to_state="notifying")
            send_proactive_message(user)
    """
    if source not in _EVENT_SOURCES:
        raise ValueError(
            f"event_trigger: source must be one of {sorted(_EVENT_SOURCES)}, got {source!r}"
        )
    if source == "agent_initiated" and not consent_id:
        raise ValueError(
            "event_trigger: consent_id is required when source='agent_initiated' "
            "(AML A8 contract — proactive actions must carry consent)"
        )
    c = _client()
    if c is None:
        yield _EventTriggerHandle(None)
        return
    with c._span("event", "event.trigger") as span:
        span.attributes["event.source"] = source
        if consent_id is not None:
            span.attributes["event.consent_id"] = consent_id
        yield _EventTriggerHandle(span)
