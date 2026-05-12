"""AML instrumentation helpers — guardrail.check and pii.filter spans.

These map to the SDK v0.4 contract bundled in @prometa/aml-core under
``src/data/instrumentation-spec.yaml`` (spans ``guardrail.check`` and
``pii.filter``). The contract feeds the platform's AML scoring engine
(features A1 PII filtering, A2 ethical guardrailing, A3 prompt injection
defense) so an instrumented agent becomes auditable for those features.

ADDITIVE only — existing v0.3.x decorators (``@workflow``, ``@agent``,
``@tool``, ``@task``) and the LLM / framework integrations are unchanged.

When no Prometa client is configured (testing, local dev, customer
hasn't called ``Prometa(...)`` yet), every helper is a silent no-op so
customer code can use them unconditionally without ``if prometa:`` guards.

Dual-channel: when :func:`prometa.raw_channel.is_enabled` is True, the
``raw_input`` / ``raw_retrieved`` / ``raw_output`` keyword arguments are
stamped on the span as ``prometa.raw.*`` attributes so the platform can
route them to ``prometa.spans_raw`` (short-TTL, access-gated). When False
(default), those kwargs are silently dropped at the SDK boundary.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence

from . import _raw_channel
from .client import Prometa


def _client() -> Optional[Prometa]:
    return Prometa._current


# =============================================================================
# Guardrail check (AML A2 ethical + A3 injection)
# =============================================================================

class _GuardrailHandle:
    """Yielded inside :func:`guardrail`. Stamps the verdict on exit.

    The handle is returned even when no Prometa client is active; its
    methods are no-ops in that case so caller code stays unconditional.
    """

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def verdict(
        self,
        outcome: str,
        *,
        confidence: Optional[float] = None,
        classifier: Optional[str] = None,
        categories: Optional[Sequence[str]] = None,
    ) -> None:
        """Record the classifier's decision on the span.

        ``outcome`` ∈ ``{pass, block, flag}`` per the AML contract.
        ``categories`` is a list of policy-specific labels (e.g.
        ``["self_harm"]`` for ethical, ``["prompt_injection"]`` for A3).
        """
        if self._span is None:
            return
        a = self._span.attributes
        a["guardrail.verdict"] = outcome
        if confidence is not None:
            a["guardrail.confidence"] = float(confidence)
        if classifier:
            a["guardrail.classifier"] = classifier
        if categories:
            # OTel Map(String, String) export wants string values; comma-
            # join keeps it greppable at the ClickHouse level. The platform
            # splits it back on read.
            a["guardrail.category"] = ",".join(categories)


@contextmanager
def guardrail(
    type_: str,
    *,
    raw_input: Optional[str] = None,
    raw_retrieved: Optional[str] = None,
) -> Iterator[_GuardrailHandle]:
    """Emit a ``guardrail.check`` span for an ethical / injection check.

    ``type_`` ∈ ``{ethical, injection, jailbreak, output_policy, bias}``
    per the AML contract.

    Usage::

        with prometa.guardrail("ethical", raw_input=user_query) as g:
            verdict = my_classifier.check(user_query)
            g.verdict(
                "block" if verdict.harmful else "pass",
                confidence=verdict.score,
                classifier="openai-moderation",
                categories=verdict.tags,
            )
    """
    c = _client()
    if c is None:
        yield _GuardrailHandle(None)
        return
    with c._span("guardrail", f"guardrail.{type_}") as span:
        span.attributes["guardrail.type"] = type_
        if _raw_channel.is_enabled():
            if raw_input is not None:
                span.attributes["prometa.raw.input"] = raw_input
            if raw_retrieved is not None:
                span.attributes["prometa.raw.retrieved_content"] = raw_retrieved
        yield _GuardrailHandle(span)


# =============================================================================
# PII filter (AML A1)
# =============================================================================

class _PIIFilterHandle:
    """Result-setter for the ``pii.filter`` span."""

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def result(
        self,
        *,
        matches_found: int,
        match_categories: Optional[Sequence[str]] = None,
        redacted: bool = True,
    ) -> None:
        """Record the filter's outcome.

        ``matches_found`` is the number of distinct PII tokens detected.
        ``match_categories`` is a list like ``["tckn", "iban", "email"]``.
        Set ``redacted=False`` to record a detection-only pass that did
        not modify the text (useful for analytics-only filters).
        """
        if self._span is None:
            return
        a = self._span.attributes
        a["pii.matches_found"] = int(matches_found)
        a["pii.redacted"] = bool(redacted)
        if match_categories:
            a["pii.match_categories"] = ",".join(match_categories)


@contextmanager
def pii_filter(
    direction: str,
    *,
    raw_input: Optional[str] = None,
    raw_output: Optional[str] = None,
) -> Iterator[_PIIFilterHandle]:
    """Emit a ``pii.filter`` span.

    ``direction`` ∈ ``{input, output, retrieved}`` — describes which
    channel the filter ran on. The AML A1 detector verifies that for
    every match in the raw stream, the sanitized log carries a placeholder
    at the corresponding position — so emitting BOTH raw and the post-
    filter stats is what makes the feature auditable.

    Usage::

        with prometa.pii_filter("input", raw_input=user_text) as pii:
            cleaned, matches = my_redactor.scrub(user_text)
            pii.result(matches_found=len(matches),
                       match_categories=[m.kind for m in matches])
    """
    if direction not in {"input", "output", "retrieved"}:
        raise ValueError(
            "pii_filter: direction must be one of input|output|retrieved, "
            f"got {direction!r}"
        )
    c = _client()
    if c is None:
        yield _PIIFilterHandle(None)
        return
    with c._span("pii", "pii.filter") as span:
        span.attributes["pii.direction"] = direction
        if _raw_channel.is_enabled():
            if raw_input is not None:
                span.attributes["prometa.raw.input"] = raw_input
            if raw_output is not None:
                span.attributes["prometa.raw.output"] = raw_output
        yield _PIIFilterHandle(span)
