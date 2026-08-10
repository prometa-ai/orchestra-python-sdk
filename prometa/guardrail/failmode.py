"""Client-side fail-mode resolution shared by both guardrail bindings.

Nothing here converts a timeout, a 5xx, or an unreachable service into a
verdict. A missing verdict resolves through the profile fail mode and only
through the profile fail mode, which is why the resolution lives in one place
that both the in-process and HTTP bindings call.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional, Protocol, Sequence, Tuple

from ..runtime.admission import RuntimeGuardrail, RuntimeTool
from ..runtime.kernel import GuardDecision
from .contract import (
    REASON_CODE_FAIL_OPEN,
    REASON_CODE_FAIL_OPEN_EXHAUSTED,
    applicable_guardrail_names,
    unavailable_assessment,
)
from .profiles import (
    GuardrailProfile,
    GuardrailProfileError,
    guardrail_is_observe_only,
)


CERTIFIED_MODEL_WORKLOAD_SURFACE = "certified"


class GuardrailUnavailableError(RuntimeError):
    """No usable verdict was produced and the profile fails closed."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class FailOpenObserver(Protocol):
    """Sink for the payload-free record every fail-open must leave behind."""

    def record(self, event: Mapping[str, Any]) -> None:
        """Persist one fail-open observation; never raise into the caller."""


@dataclass(frozen=True)
class FailModeOutcome:
    """What the client did when the service produced no usable verdict."""

    fail_mode: str
    reason_code: str
    decision: Optional[GuardDecision]


def resolve_fail_mode(profile_fail_mode: Any, *, workload_surface: str = "") -> str:
    """Resolve the effective fail mode, defaulting anything unknown to closed.

    The certified model-workload surface has no fail-open, matching every other
    rule on that surface.
    """

    if workload_surface == CERTIFIED_MODEL_WORKLOAD_SURFACE:
        return "closed"
    return "open" if profile_fail_mode == "open" else "closed"


def assert_workload_surface_fail_mode(
    profile: GuardrailProfile, workload_surface: str
) -> None:
    """Refuse a fail-open profile on the certified model-workload surface.

    Raised at evaluator construction rather than at request time so a profile
    the certified surface cannot honour never reaches production traffic.
    """

    if (
        workload_surface == CERTIFIED_MODEL_WORKLOAD_SURFACE
        and profile.fail_mode == "open"
    ):
        raise GuardrailProfileError("guardrail_profile_fail_open_certified_surface")


class FailOpenBudget:
    """Bounded consecutive fail-opens, so a dead service cannot silence a fleet.

    The trip is sticky and only ``record_success`` clears it. Time may not
    refill the allowance: a budget that recovers on elapsed time alone hands a
    permanently-down service a permanently-unguarded fleet, and an attacker who
    can keep the service down can then pace requests to stay under any
    per-window ceiling forever.

    ``window_seconds`` therefore tightens rather than relaxes. A run of
    fail-opens that outlives the window trips even before the count is reached,
    because a failure lasting longer than the window is worse than a burst
    inside it, not better.
    """

    def __init__(
        self,
        max_consecutive: int,
        window_seconds: float,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_consecutive < 1 or window_seconds <= 0:
            raise ValueError("fail-open budget bounds must be positive")
        self._max_consecutive = max_consecutive
        self._window_seconds = window_seconds
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._count = 0
        self._window_start = 0.0
        self._tripped = False

    def record_success(self) -> None:
        with self._lock:
            self._count = 0
            self._window_start = 0.0
            self._tripped = False

    def consume(self) -> bool:
        """Take one fail-open allowance; false once the budget is exhausted."""

        now = self._monotonic()
        with self._lock:
            if self._tripped:
                return False
            if not self._count:
                self._window_start = now
            if (
                self._count >= self._max_consecutive
                or now - self._window_start > self._window_seconds
            ):
                self._tripped = True
                return False
            self._count += 1
            return True

    @property
    def consecutive(self) -> int:
        with self._lock:
            return self._count

    @property
    def tripped(self) -> bool:
        with self._lock:
            return self._tripped


def coverage_identifiers(
    guardrails: Sequence[RuntimeGuardrail],
    tool: Optional[RuntimeTool],
    wire_stage_name: str,
) -> Tuple[str, ...]:
    """Every identifier a decision must report so the kernel accepts it."""

    names = set(applicable_guardrail_names(guardrails, wire_stage_name))
    if tool is not None:
        names |= set(tool.required_guardrails)
    return tuple(sorted(names))


def security_assurance_names(
    guardrails: Sequence[RuntimeGuardrail], wire_stage_name: str
) -> Tuple[str, ...]:
    applicable = applicable_guardrail_names(guardrails, wire_stage_name)
    return tuple(
        sorted(
            guardrail.name
            for guardrail in guardrails
            if guardrail.name in applicable and guardrail.security_assurance_enabled
        )
    )


def build_fail_open_decision(
    guardrails: Sequence[RuntimeGuardrail],
    tool: Optional[RuntimeTool],
    wire_stage_name: str,
    reason_code: str,
) -> GuardDecision:
    """Allow, but carry evidence that names the absence of any real scan."""

    assessments = tuple(
        unavailable_assessment(name, reason_code=reason_code)
        for name in security_assurance_names(guardrails, wire_stage_name)
    )
    return GuardDecision(
        allowed=True,
        action="allow",
        reason=reason_code,
        evaluated_guardrails=coverage_identifiers(guardrails, tool, wire_stage_name),
        transformed_payload=None,
        security_assessments=assessments,
    )


def fail_open_event(
    *,
    request_id: str,
    wire_stage_name: str,
    profile_id: str,
    reason_code: str,
    tenant: str,
) -> Dict[str, Any]:
    """The payload-free observation a fail-open leaves for the control plane."""

    return {
        "requestId": request_id,
        "stage": wire_stage_name,
        "profile": profile_id,
        "tenant": tenant,
        "appliedAction": "allow",
        "reasonCode": reason_code,
        "severity": "high",
        "reviewRequired": True,
    }


def apply_fail_mode(
    profile: GuardrailProfile,
    *,
    request_id: str,
    wire_stage_name: str,
    guardrails: Sequence[RuntimeGuardrail],
    tool: Optional[RuntimeTool],
    tenant: str,
    reason_code: str,
    budget: FailOpenBudget,
    observer: Optional[FailOpenObserver] = None,
    workload_surface: str = "",
) -> FailModeOutcome:
    """Resolve one unusable verdict into either a raise or a recorded allow."""

    fail_mode = resolve_fail_mode(profile.fail_mode, workload_surface=workload_surface)
    if fail_mode == "closed":
        return FailModeOutcome(fail_mode="closed", reason_code=reason_code, decision=None)
    if any(not guardrail_is_observe_only(guardrail) for guardrail in guardrails):
        raise GuardrailUnavailableError("guardrail_profile_fail_open_enforcing")
    if not budget.consume():
        return FailModeOutcome(
            fail_mode="closed",
            reason_code=REASON_CODE_FAIL_OPEN_EXHAUSTED,
            decision=None,
        )
    if observer is not None:
        try:
            observer.record(
                fail_open_event(
                    request_id=request_id,
                    wire_stage_name=wire_stage_name,
                    profile_id=profile.profile_id,
                    reason_code=REASON_CODE_FAIL_OPEN,
                    tenant=tenant,
                )
            )
        except Exception:
            # Evidence is not a gate on enforcement; the reverse of
            # ``security_decision_emit_failed``, which is retryable by design.
            pass
    return FailModeOutcome(
        fail_mode="open",
        reason_code=REASON_CODE_FAIL_OPEN,
        decision=build_fail_open_decision(
            guardrails, tool, wire_stage_name, REASON_CODE_FAIL_OPEN
        ),
    )


__all__ = [
    "CERTIFIED_MODEL_WORKLOAD_SURFACE",
    "FailModeOutcome",
    "FailOpenBudget",
    "FailOpenObserver",
    "GuardrailUnavailableError",
    "apply_fail_mode",
    "assert_workload_surface_fail_mode",
    "build_fail_open_decision",
    "coverage_identifiers",
    "fail_open_event",
    "resolve_fail_mode",
    "security_assurance_names",
]
