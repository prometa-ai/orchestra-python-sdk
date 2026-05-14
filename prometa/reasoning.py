"""AML instrumentation helpers — plan / confidence / schema validation spans.

Maps to the SDK v0.4 contract bundled in the platform repo at
``resources/aml/phase-0/instrumentation-spec.yaml``. Together they feed
the platform's AML scoring engine for features C2 (task decomposition),
C3 (confidence & uncertainty), C4 (explainability), D3 (parallel
execution), D4 (output validation).

ADDITIVE only — existing v0.3.x decorators and integrations are unchanged.
No raw kwargs — plan / confidence / schema attributes are non-sensitive
metadata about the agent's reasoning shape.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence

from .client import Prometa


def _client() -> Optional[Prometa]:
    return Prometa._current


# =============================================================================
# Plan generation (AML C2, D3, C4)
# =============================================================================


class _PlanGenerateHandle:
    """Yielded inside :func:`plan_generate`. Records steps + replan link."""

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def emitted(
        self,
        *,
        steps: Sequence[dict],
        replanned_from: Optional[str] = None,
        complexity_estimate: Optional[int] = None,
    ) -> None:
        """Record the plan structure.

        ``steps`` is a list of ``{"order", "action", "tool", "depends_on",
        "parallel_with"}`` dicts — C2 reads it for decomposition shape,
        D3 reads ``parallel_with`` for parallel-execution evidence.
        ``replanned_from`` is the prior plan id when this is a re-plan
        (E5 reviewer-critique loop reads it). Empty / None for the
        original plan.
        """
        if self._span is None:
            return
        a = self._span.attributes
        if steps:
            a["plan.steps"] = json.dumps(list(steps), separators=(",", ":"))
        if replanned_from is not None:
            a["plan.replanned_from"] = replanned_from
        if complexity_estimate is not None:
            a["plan.complexity_estimate"] = int(complexity_estimate)


@contextmanager
def plan_generate(plan_id: str) -> Iterator[_PlanGenerateHandle]:
    """Emit a ``plan.generate`` span around a plan-generation step.

    Usage::

        with prometa.plan_generate(plan_id="plan-abc-1") as p:
            steps = my_planner.generate(goal)
            p.emitted(
                steps=[
                    {"order": 1, "action": "search", "tool": "vector"},
                    {"order": 2, "action": "summarize", "tool": "llm",
                     "depends_on": [1]},
                ],
                complexity_estimate=3,
            )
    """
    c = _client()
    if c is None:
        yield _PlanGenerateHandle(None)
        return
    with c._span("plan", "plan.generate") as span:
        span.attributes["plan.id"] = plan_id
        yield _PlanGenerateHandle(span)


# =============================================================================
# Confidence score (AML C3)
# =============================================================================

_CONFIDENCE_BASES = {
    "retrieval_score",
    "self_consistency",
    "rule_check",
    "ensemble",
    "judge",
}
_CONFIDENCE_ACTIONS = {"respond", "hedge", "escalate", "decline"}


class _ConfidenceScoreHandle:
    """Yielded inside :func:`confidence_score`. Records the action taken."""

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def action(
        self,
        outcome: str,
        *,
        threshold_used: Optional[float] = None,
    ) -> None:
        """Record what the agent did with the confidence value.

        ``outcome`` ∈ ``{respond, hedge, escalate, decline}``.
        ``threshold_used`` is the cutoff that drove the decision — what
        C3 reads to verify calibration (low confidence + respond is
        flagged as miscalibrated).
        """
        if outcome not in _CONFIDENCE_ACTIONS:
            raise ValueError(
                f"confidence_score.action: outcome must be one of {sorted(_CONFIDENCE_ACTIONS)}, got {outcome!r}"
            )
        if self._span is None:
            return
        a = self._span.attributes
        a["confidence.action"] = outcome
        if threshold_used is not None:
            a["confidence.threshold_used"] = float(threshold_used)


@contextmanager
def confidence_score(
    value: float,
    *,
    calibration_basis: str,
) -> Iterator[_ConfidenceScoreHandle]:
    """Emit a ``confidence.score`` span around a confidence estimate.

    ``value`` ∈ ``[0.0, 1.0]``. ``calibration_basis`` records HOW the
    confidence was derived (retrieval scores, self-consistency, rule
    check, ensemble, or LLM-as-judge).

    Usage::

        with prometa.confidence_score(
            value=0.42, calibration_basis="self_consistency"
        ) as cs:
            if value < 0.6:
                cs.action("hedge", threshold_used=0.6)
            else:
                cs.action("respond", threshold_used=0.6)
    """
    if not 0.0 <= value <= 1.0:
        raise ValueError(
            f"confidence_score: value must be in [0.0, 1.0], got {value!r}"
        )
    if calibration_basis not in _CONFIDENCE_BASES:
        raise ValueError(
            f"confidence_score: calibration_basis must be one of {sorted(_CONFIDENCE_BASES)}, got {calibration_basis!r}"
        )
    c = _client()
    if c is None:
        yield _ConfidenceScoreHandle(None)
        return
    with c._span("confidence", "confidence.score") as span:
        a = span.attributes
        a["confidence.value"] = float(value)
        a["confidence.calibration_basis"] = calibration_basis
        yield _ConfidenceScoreHandle(span)


# =============================================================================
# Schema validation (AML D4)
# =============================================================================


class _SchemaValidateHandle:
    """Yielded inside :func:`schema_validate`. Records pass/fail + repairs."""

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def result(
        self,
        *,
        passed: bool,
        errors: Optional[Sequence[str]] = None,
        repair_attempt: int = 0,
        downstream_blocked: bool = False,
    ) -> None:
        """Record the validation outcome.

        ``repair_attempt`` is 0 for the original output, ≥1 for a
        repair pass. ``downstream_blocked`` flags whether the failure
        prevented a subsequent tool call (which is what D4 wants — a
        validator that's wired but doesn't actually gate the call
        downstream produces auditable false reassurance).
        """
        if self._span is None:
            return
        a = self._span.attributes
        a["schema.passed"] = bool(passed)
        a["schema.repair_attempt"] = int(repair_attempt)
        a["schema.downstream_blocked"] = bool(downstream_blocked)
        if errors:
            a["schema.errors"] = json.dumps(list(errors), separators=(",", ":"))


@contextmanager
def schema_validate(
    schema_id: str,
) -> Iterator[_SchemaValidateHandle]:
    """Emit a ``schema.validate`` span around an output-validation event.

    ``schema_id`` references the schema (e.g. ``"pyd:ClaimPayout@v3"``).

    Usage::

        with prometa.schema_validate("pyd:ClaimPayout@v3") as sv:
            try:
                ClaimPayout.model_validate(llm_output)
                sv.result(passed=True)
            except ValidationError as e:
                sv.result(
                    passed=False,
                    errors=[err["msg"] for err in e.errors()],
                    repair_attempt=0,
                    downstream_blocked=True,
                )
    """
    c = _client()
    if c is None:
        yield _SchemaValidateHandle(None)
        return
    with c._span("schema", "schema.validate") as span:
        span.attributes["schema.id"] = schema_id
        yield _SchemaValidateHandle(span)
