"""AML instrumentation helpers — retry.attempt and circuit_breaker.state.

Maps to AML feature E6 (Resilience & Self-Healing) per the SDK v0.4
contract bundled in @prometa/aml-core ``src/data/instrumentation-spec.yaml``.

These helpers do NOT implement retry or circuit-breaker logic — that's
the customer's responsibility (Tenacity, custom code, framework primitives).
The SDK just RECORDS attempts and state transitions so the AML detector
can prove that retries happened with idempotency keys, that the circuit
breaker actually trips, and that runaway retry loops are absent.
"""

from __future__ import annotations

from typing import Optional

from .client import Prometa


_RETRY_OUTCOMES = frozenset({"success", "fail", "exhausted"})
_BREAKER_STATES = frozenset({"closed", "open", "half_open"})


def _client() -> Optional[Prometa]:
    return Prometa._current


def record_retry_attempt(
    *,
    target_span_id: Optional[str] = None,
    attempt_number: int,
    backoff_ms: int = 0,
    idempotency_key: Optional[str] = None,
    outcome: str,
) -> bool:
    """Emit a ``retry.attempt`` span.

    ``outcome`` ∈ ``{success, fail, exhausted}``. ``exhausted`` means the
    retry budget was used up without success — distinct from ``fail`` so
    the AML detector can count runaway-loop incidents (a critical signal
    for E6 scoring).

    ``idempotency_key`` SHOULD be set on writes (transfers, bookings,
    state changes). Without one, the AML engine reports E6 as ``partial``
    rather than ``present`` because retried writes risk double-execution.

    Returns ``True`` if a span was emitted, ``False`` if no client active.

    Usage::

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                result = external.call(idempotency_key=idem_key)
                prometa.record_retry_attempt(
                    attempt_number=attempt,
                    backoff_ms=0,
                    idempotency_key=idem_key,
                    outcome="success",
                )
                return result
            except TransientError:
                prometa.record_retry_attempt(
                    attempt_number=attempt,
                    backoff_ms=backoff,
                    idempotency_key=idem_key,
                    outcome="exhausted" if attempt == MAX_ATTEMPTS else "fail",
                )
                time.sleep(backoff / 1000)
                backoff *= 2
    """
    if outcome not in _RETRY_OUTCOMES:
        raise ValueError(
            "record_retry_attempt: outcome must be one of "
            f"{sorted(_RETRY_OUTCOMES)}, got {outcome!r}"
        )
    c = _client()
    if c is None:
        return False
    with c._span("retry", "retry.attempt") as span:
        a = span.attributes
        if target_span_id:
            a["retry.target_span_id"] = target_span_id
        a["retry.attempt_number"] = int(attempt_number)
        a["retry.backoff_ms"] = int(backoff_ms)
        if idempotency_key:
            a["retry.idempotency_key"] = idempotency_key
        a["retry.outcome"] = outcome
    return True


def record_circuit_breaker_state(
    *,
    target: str,
    from_state: str,
    to_state: str,
    failure_count: int = 0,
) -> bool:
    """Emit a ``circuit_breaker.state`` span on a state transition.

    States ∈ ``{closed, open, half_open}``. Emit only on TRANSITIONS, not
    on every guarded call — otherwise the span volume swamps the trace
    view and the AML detector's signal-to-noise crashes.

    ``target`` is the tool / API name guarded by the breaker
    (e.g. ``"payments-api"``). ``failure_count`` is the cumulative
    consecutive-failure tally at the time of the transition.

    Usage::

        breaker.on_transition(
            lambda old, new, fails: prometa.record_circuit_breaker_state(
                target="payments-api",
                from_state=old,
                to_state=new,
                failure_count=fails,
            )
        )
    """
    if from_state not in _BREAKER_STATES or to_state not in _BREAKER_STATES:
        raise ValueError(
            "record_circuit_breaker_state: states must be one of "
            f"{sorted(_BREAKER_STATES)}; got from={from_state!r} to={to_state!r}"
        )
    c = _client()
    if c is None:
        return False
    with c._span("circuit_breaker", "circuit_breaker.state") as span:
        a = span.attributes
        a["circuit_breaker.target"] = target
        a["circuit_breaker.from_state"] = from_state
        a["circuit_breaker.to_state"] = to_state
        a["circuit_breaker.failure_count"] = int(failure_count)
    return True
