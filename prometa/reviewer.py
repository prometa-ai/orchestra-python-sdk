"""AML instrumentation helper — ``reviewer.invoke`` span.

Maps to the SDK v0.4 contract bundled in the platform repo at
``resources/aml/phase-0/instrumentation-spec.yaml`` (span name
``reviewer.invoke``). Feeds the platform's AML scoring engine for
feature E5 (reviewer / critique loop) — a secondary agent reviewing the
output of a primary agent before downstream execution.

ADDITIVE only. No raw kwargs.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence

from .client import Prometa


def _client() -> Optional[Prometa]:
    return Prometa._current


_REVIEWER_VERDICTS = {"approve", "request_fix", "block"}


class _ReviewerInvokeHandle:
    """Yielded inside :func:`reviewer_invoke`. Records verdict + rationale."""

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def verdict(
        self,
        outcome: str,
        *,
        rationale: Optional[str] = None,
        policy_violations: Optional[Sequence[str]] = None,
    ) -> None:
        """Record the reviewer's decision.

        ``outcome`` ∈ ``{approve, request_fix, block}``.
        ``policy_violations`` is the list of specific policy labels
        the reviewer flagged (e.g. ``["pricing_floor", "kvkk_consent"]``).
        """
        if outcome not in _REVIEWER_VERDICTS:
            raise ValueError(
                f"reviewer_invoke.verdict: outcome must be one of {sorted(_REVIEWER_VERDICTS)}, got {outcome!r}"
            )
        if self._span is None:
            return
        a = self._span.attributes
        a["reviewer.verdict"] = outcome
        if rationale is not None:
            a["reviewer.rationale"] = rationale
        if policy_violations:
            a["reviewer.policy_violations"] = ",".join(policy_violations)


@contextmanager
def reviewer_invoke(
    reviewer_id: str,
    *,
    target_span_id: str,
) -> Iterator[_ReviewerInvokeHandle]:
    """Emit a ``reviewer.invoke`` span around a secondary-agent review.

    ``reviewer_id`` identifies the reviewing agent (display name or id).
    ``target_span_id`` is the span_id of the action being reviewed —
    typically the primary agent's last tool call or output. The platform
    promotes it to a dedicated column so the trace UI can render the
    reviewer→target edge.

    Usage::

        with prometa.reviewer_invoke(
            "policy-reviewer-v2", target_span_id=primary_span_id
        ) as r:
            opinion = my_reviewer.evaluate(primary_output)
            r.verdict(
                "block" if opinion.violates else "approve",
                rationale=opinion.rationale,
                policy_violations=opinion.flags,
            )
    """
    c = _client()
    if c is None:
        yield _ReviewerInvokeHandle(None)
        return
    with c._span("reviewer", "reviewer.invoke") as span:
        a = span.attributes
        a["reviewer.id"] = reviewer_id
        a["reviewer.target_span_id"] = target_span_id
        yield _ReviewerInvokeHandle(span)
