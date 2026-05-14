"""AML instrumentation helper — ``model.route`` span.

Maps to the SDK v0.4 contract bundled in the platform repo at
``resources/aml/phase-0/instrumentation-spec.yaml`` (span name
``model.route``). Feeds the platform's AML scoring engine for feature
F1 (cost-aware routing) — agents that cascade between models based on
complexity, cost, or budget caps.

ADDITIVE only. No raw kwargs.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence

from .client import Prometa


def _client() -> Optional[Prometa]:
    return Prometa._current


class _ModelRouteHandle:
    """Yielded inside :func:`model_route`. Records cost / budget."""

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def cost(
        self,
        *,
        cost_estimate_usd: float,
        budget_cap_usd: Optional[float] = None,
    ) -> None:
        """Record the routing decision's cost economics.

        ``cost_estimate_usd`` is the routing layer's pre-call estimate
        (token budget × per-token price for the chosen model).
        ``budget_cap_usd`` is the per-request cap when one is set —
        F1 reads it to verify the agent actually respects budget caps
        rather than just emitting them as metadata.
        """
        if self._span is None:
            return
        a = self._span.attributes
        a["model.cost_estimate_usd"] = float(cost_estimate_usd)
        if budget_cap_usd is not None:
            a["model.budget_cap_usd"] = float(budget_cap_usd)


@contextmanager
def model_route(
    chosen: str,
    *,
    candidates_considered: Sequence[str],
    routing_reason: str,
) -> Iterator[_ModelRouteHandle]:
    """Emit a ``model.route`` span around a model-cascade decision.

    ``chosen`` is the model id selected (e.g. ``"claude-opus-4-7"`` or
    ``"gpt-4o-mini"``). ``candidates_considered`` is the list of models
    the router looked at before picking. ``routing_reason`` is a
    free-form label — common values include ``"low_complexity"``,
    ``"high_complexity"``, ``"fallback_after_failure"``, ``"budget_capped"``.

    Usage::

        with prometa.model_route(
            chosen="gpt-4o-mini",
            candidates_considered=["gpt-4o", "gpt-4o-mini", "claude-haiku"],
            routing_reason="low_complexity",
        ) as r:
            r.cost(cost_estimate_usd=0.0023, budget_cap_usd=0.05)
            response = openai.chat.completions.create(model="gpt-4o-mini", ...)
    """
    c = _client()
    if c is None:
        yield _ModelRouteHandle(None)
        return
    with c._span("model", "model.route") as span:
        a = span.attributes
        a["model.chosen"] = chosen
        if candidates_considered:
            a["model.candidates_considered"] = ",".join(candidates_considered)
        a["model.routing_reason"] = routing_reason
        yield _ModelRouteHandle(span)
