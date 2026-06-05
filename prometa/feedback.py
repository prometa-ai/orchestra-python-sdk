"""User feedback helpers for trace-level Prometa indexing.

Applications can collect thumbs-up / thumbs-down, 1-5 star ratings, and
open-text comments after an assistant turn and feed the result to
Prometa as generic ``prometa.feedback.*`` trace attributes.

Two entrypoints cover the common delivery shapes:

- :func:`set_user_feedback` stamps feedback onto the currently-active
  Prometa span. Use this when feedback is collected before the traced
  workflow exits.
- :func:`record_user_feedback` emits a dedicated ``feedback.record``
  span. Use this when feedback arrives later from a UI callback or API
  endpoint; pass target ids so the platform can attach it to the
  original trace/session/span.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from .client import Prometa


MAX_COMMENT_CHARS = 4096

PROMETA_FEEDBACK_SIGNAL_ATTR = "prometa.feedback.signal"
PROMETA_FEEDBACK_LIKED_ATTR = "prometa.feedback.liked"
PROMETA_FEEDBACK_RATING_ATTR = "prometa.feedback.rating"
PROMETA_FEEDBACK_SCORE_ATTR = "prometa.feedback.score"
PROMETA_FEEDBACK_SENTIMENT_ATTR = "prometa.feedback.sentiment"
PROMETA_FEEDBACK_COMMENT_ATTR = "prometa.feedback.comment"
PROMETA_FEEDBACK_COMMENT_TRUNCATED_ATTR = "prometa.feedback.comment.truncated"
PROMETA_FEEDBACK_SOURCE_ATTR = "prometa.feedback.source"
PROMETA_FEEDBACK_ID_ATTR = "prometa.feedback.id"
PROMETA_FEEDBACK_USER_ID_ATTR = "prometa.feedback.user_id"
PROMETA_FEEDBACK_SUBMITTED_AT_ATTR = "prometa.feedback.submitted_at"
PROMETA_FEEDBACK_TARGET_TRACE_ID_ATTR = "prometa.feedback.target.trace_id"
PROMETA_FEEDBACK_TARGET_SPAN_ID_ATTR = "prometa.feedback.target.span_id"
PROMETA_FEEDBACK_TARGET_SESSION_ID_ATTR = "prometa.feedback.target.session_id"

FEEDBACK_ATTRIBUTE_KEYS = (
    PROMETA_FEEDBACK_SIGNAL_ATTR,
    PROMETA_FEEDBACK_LIKED_ATTR,
    PROMETA_FEEDBACK_RATING_ATTR,
    PROMETA_FEEDBACK_SCORE_ATTR,
    PROMETA_FEEDBACK_SENTIMENT_ATTR,
    PROMETA_FEEDBACK_COMMENT_ATTR,
    PROMETA_FEEDBACK_COMMENT_TRUNCATED_ATTR,
    PROMETA_FEEDBACK_SOURCE_ATTR,
    PROMETA_FEEDBACK_ID_ATTR,
    PROMETA_FEEDBACK_USER_ID_ATTR,
    PROMETA_FEEDBACK_SUBMITTED_AT_ATTR,
    PROMETA_FEEDBACK_TARGET_TRACE_ID_ATTR,
    PROMETA_FEEDBACK_TARGET_SPAN_ID_ATTR,
    PROMETA_FEEDBACK_TARGET_SESSION_ID_ATTR,
)


def build_user_feedback_attrs(
    *,
    liked: Optional[bool] = None,
    rating: Optional[int] = None,
    comment: Optional[str] = None,
    source: str = "user",
    feedback_id: Optional[str] = None,
    user_id: Optional[str] = None,
    submitted_at: Optional[Any] = None,
    target_trace_id: Optional[str] = None,
    target_span_id: Optional[str] = None,
    target_session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Build platform-indexable ``prometa.feedback.*`` attributes.

    ``liked`` is a thumbs-style signal: ``True`` for like, ``False`` for
    dislike. ``rating`` is an integer from 1 to 5. ``comment`` is
    open-text feedback and is truncated to ``MAX_COMMENT_CHARS`` to keep
    span payloads bounded.

    At least one of ``liked``, ``rating``, or ``comment`` must be
    supplied. Target ids are optional, but recommended when feedback is
    recorded after the original trace has already ended.
    """
    normalized_rating = _normalize_rating(rating)
    normalized_comment = _normalize_comment(comment)
    has_comment = normalized_comment is not None and normalized_comment != ""

    if liked is not None and not isinstance(liked, bool):
        raise ValueError("liked must be a bool when supplied")
    if liked is None and normalized_rating is None and not has_comment:
        raise ValueError("feedback requires liked, rating, or comment")

    attrs: Dict[str, Any] = {
        PROMETA_FEEDBACK_SIGNAL_ATTR: _feedback_signal(
            liked=liked,
            rating=normalized_rating,
            has_comment=has_comment,
        ),
        PROMETA_FEEDBACK_SOURCE_ATTR: str(source or "user"),
    }

    if liked is not None:
        attrs[PROMETA_FEEDBACK_LIKED_ATTR] = liked
    if normalized_rating is not None:
        attrs[PROMETA_FEEDBACK_RATING_ATTR] = normalized_rating

    score = _feedback_score(liked=liked, rating=normalized_rating)
    if score is not None:
        attrs[PROMETA_FEEDBACK_SCORE_ATTR] = score
        attrs[PROMETA_FEEDBACK_SENTIMENT_ATTR] = _feedback_sentiment(score)

    if normalized_comment is not None:
        truncated, was_truncated = _truncate_comment(normalized_comment)
        attrs[PROMETA_FEEDBACK_COMMENT_ATTR] = truncated
        attrs[PROMETA_FEEDBACK_COMMENT_TRUNCATED_ATTR] = was_truncated

    optional_values = {
        PROMETA_FEEDBACK_ID_ATTR: feedback_id,
        PROMETA_FEEDBACK_USER_ID_ATTR: user_id,
        PROMETA_FEEDBACK_SUBMITTED_AT_ATTR: _normalize_submitted_at(submitted_at),
        PROMETA_FEEDBACK_TARGET_TRACE_ID_ATTR: target_trace_id,
        PROMETA_FEEDBACK_TARGET_SPAN_ID_ATTR: target_span_id,
        PROMETA_FEEDBACK_TARGET_SESSION_ID_ATTR: target_session_id,
    }
    for key, value in optional_values.items():
        if value:
            attrs[key] = str(value)

    return attrs


def set_user_feedback(
    *,
    liked: Optional[bool] = None,
    rating: Optional[int] = None,
    comment: Optional[str] = None,
    source: str = "user",
    feedback_id: Optional[str] = None,
    user_id: Optional[str] = None,
    submitted_at: Optional[Any] = None,
    target_trace_id: Optional[str] = None,
    target_span_id: Optional[str] = None,
    target_session_id: Optional[str] = None,
) -> bool:
    """Stamp user feedback onto the currently-active span.

    Returns ``False`` when called outside a Prometa span context. Use
    :func:`record_user_feedback` when feedback is collected after the
    original trace has already completed.
    """
    from . import _context

    span = _context.current_span()
    if span is None:
        return False
    span.attributes.update(
        build_user_feedback_attrs(
            liked=liked,
            rating=rating,
            comment=comment,
            source=source,
            feedback_id=feedback_id,
            user_id=user_id,
            submitted_at=submitted_at,
            target_trace_id=target_trace_id,
            target_span_id=target_span_id,
            target_session_id=target_session_id,
        )
    )
    return True


def record_user_feedback(
    *,
    liked: Optional[bool] = None,
    rating: Optional[int] = None,
    comment: Optional[str] = None,
    source: str = "user",
    feedback_id: Optional[str] = None,
    user_id: Optional[str] = None,
    submitted_at: Optional[Any] = None,
    target_trace_id: Optional[str] = None,
    target_span_id: Optional[str] = None,
    target_session_id: Optional[str] = None,
) -> bool:
    """Emit a ``feedback.record`` span carrying user feedback.

    If an application calls this inside an active Prometa span, the
    feedback span is nested in the same trace. If it is called later,
    the span becomes a standalone feedback event and the optional
    target ids tell Prometa which trace/session/span should receive the
    feedback.
    """
    attrs = build_user_feedback_attrs(
        liked=liked,
        rating=rating,
        comment=comment,
        source=source,
        feedback_id=feedback_id,
        user_id=user_id,
        submitted_at=submitted_at,
        target_trace_id=target_trace_id,
        target_span_id=target_span_id,
        target_session_id=target_session_id,
    )
    client = Prometa._current
    if client is None:
        return False
    with client._span("feedback", "feedback.record") as span:
        span.attributes.update(attrs)
    return True


def _normalize_rating(rating: Optional[int]) -> Optional[int]:
    if rating is None:
        return None
    if isinstance(rating, bool):
        raise ValueError("rating must be an integer from 1 to 5")
    if isinstance(rating, float) and not rating.is_integer():
        raise ValueError("rating must be an integer from 1 to 5")
    try:
        normalized = int(rating)
    except (TypeError, ValueError) as exc:
        raise ValueError("rating must be an integer from 1 to 5") from exc
    if normalized < 1 or normalized > 5:
        raise ValueError("rating must be an integer from 1 to 5")
    return normalized


def _normalize_comment(comment: Optional[str]) -> Optional[str]:
    if comment is None:
        return None
    return str(comment)


def _normalize_submitted_at(value: Optional[Any]) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _truncate_comment(comment: str) -> tuple[str, bool]:
    if len(comment) <= MAX_COMMENT_CHARS:
        return comment, False
    return comment[: MAX_COMMENT_CHARS - 16] + "...[truncated]", True


def _feedback_signal(
    *,
    liked: Optional[bool],
    rating: Optional[int],
    has_comment: bool,
) -> str:
    parts = []
    if liked is not None:
        parts.append("like" if liked else "dislike")
    if rating is not None:
        parts.append("rating")
    if has_comment:
        parts.append("comment")
    return ",".join(parts)


def _feedback_score(
    *,
    liked: Optional[bool],
    rating: Optional[int],
) -> Optional[float]:
    if rating is not None:
        return (rating - 3) / 2
    if liked is not None:
        return 1.0 if liked else -1.0
    return None


def _feedback_sentiment(score: float) -> str:
    if score > 0:
        return "positive"
    if score < 0:
        return "negative"
    return "neutral"


__all__ = [
    "MAX_COMMENT_CHARS",
    "FEEDBACK_ATTRIBUTE_KEYS",
    "build_user_feedback_attrs",
    "set_user_feedback",
    "record_user_feedback",
]
