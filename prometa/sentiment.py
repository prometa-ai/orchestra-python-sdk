"""AML instrumentation helper — ``sentiment.classify`` span.

Maps to the SDK v0.4 contract bundled in the platform repo at
``resources/aml/phase-0/instrumentation-spec.yaml`` (span name
``sentiment.classify``). Feeds the platform's AML scoring engine for
feature C5 (sentiment awareness) — agents that detect distressed /
frustrated / urgent users and adapt tone or escalate.

Dual-channel: ``raw_input`` is the pre-normalization user text — keeps
the caps / punctuation / profanity signals that sanitization typically
strips. C5 cannot be scored from the sanitized log alone.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Optional

from . import _raw_channel
from .client import Prometa


def _client() -> Optional[Prometa]:
    return Prometa._current


_SENTIMENT_LABELS = {"neutral", "frustrated", "distressed", "urgent", "confused"}
_SENTIMENT_ACTIONS = {
    "none",
    "tone_softened",
    "escalated_human",
    "crisis_resource_surfaced",
}


class _SentimentClassifyHandle:
    """Yielded inside :func:`sentiment_classify`. Records the action."""

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def action_taken(self, outcome: str) -> None:
        """Record what the agent did about the detected sentiment.

        ``outcome`` ∈ ``{none, tone_softened, escalated_human,
        crisis_resource_surfaced}``. C5 specifically rewards
        ``crisis_resource_surfaced`` on ``distressed`` classifications.
        """
        if outcome not in _SENTIMENT_ACTIONS:
            raise ValueError(
                f"sentiment_classify.action_taken: outcome must be one of {sorted(_SENTIMENT_ACTIONS)}, got {outcome!r}"
            )
        if self._span is None:
            return
        self._span.attributes["sentiment.action_taken"] = outcome


@contextmanager
def sentiment_classify(
    label: str,
    *,
    confidence: float,
    raw_input: Optional[str] = None,
) -> Iterator[_SentimentClassifyHandle]:
    """Emit a ``sentiment.classify`` span around an emotion-classification call.

    ``label`` ∈ ``{neutral, frustrated, distressed, urgent, confused}``.
    ``confidence`` ∈ ``[0.0, 1.0]``.

    Dual-channel: when :func:`prometa.raw_channel.is_enabled` is True,
    ``raw_input`` is stamped as ``prometa.raw.input`` — preserves caps /
    exclamation density / profanity that C5 reads to verify whether the
    classifier looked at the right signals.

    Usage::

        with prometa.sentiment_classify(
            label="distressed", confidence=0.86, raw_input=user_text
        ) as s:
            # ... assistant generates response ...
            s.action_taken("crisis_resource_surfaced")
    """
    if label not in _SENTIMENT_LABELS:
        raise ValueError(
            f"sentiment_classify: label must be one of {sorted(_SENTIMENT_LABELS)}, got {label!r}"
        )
    if not 0.0 <= confidence <= 1.0:
        raise ValueError(
            f"sentiment_classify: confidence must be in [0.0, 1.0], got {confidence!r}"
        )
    c = _client()
    if c is None:
        yield _SentimentClassifyHandle(None)
        return
    with c._span("sentiment", "sentiment.classify") as span:
        a = span.attributes
        a["sentiment.label"] = label
        a["sentiment.confidence"] = float(confidence)
        if _raw_channel.is_enabled() and raw_input is not None:
            a["prometa.raw.input"] = raw_input
        yield _SentimentClassifyHandle(span)
