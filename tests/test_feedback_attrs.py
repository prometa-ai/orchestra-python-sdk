"""Tests for platform user feedback trace attributes."""

from __future__ import annotations

import unittest

from prometa import (
    Prometa,
    build_user_feedback_attrs,
    record_user_feedback,
    set_user_feedback,
)


class UserFeedbackAttrsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.prometa = Prometa(
            endpoint="http://localhost:0/never-flushed",
            api_key=None,
            solution_id="sol-test",
            agent_name="test-agent",
            agent_id="test-agent-id",
            stage="test",
        )

    def test_set_user_feedback_stamps_like_comment_attrs(self) -> None:
        @self.prometa.workflow(name="root")
        def handle():
            set_user_feedback(
                liked=False,
                comment="The answer missed the budget constraint.",
                source="thumbs_down",
                feedback_id="fb_123",
                user_id="user_456",
            )

        handle()

        attrs = self.prometa._buffer[-1].attributes
        self.assertEqual(attrs["prometa.feedback.signal"], "dislike,comment")
        self.assertFalse(attrs["prometa.feedback.liked"])
        self.assertEqual(attrs["prometa.feedback.score"], -1.0)
        self.assertEqual(attrs["prometa.feedback.sentiment"], "negative")
        self.assertEqual(
            attrs["prometa.feedback.comment"],
            "The answer missed the budget constraint.",
        )
        self.assertFalse(attrs["prometa.feedback.comment.truncated"])
        self.assertEqual(attrs["prometa.feedback.source"], "thumbs_down")
        self.assertEqual(attrs["prometa.feedback.id"], "fb_123")
        self.assertEqual(attrs["prometa.feedback.user_id"], "user_456")
        self.assertTrue(_feedback_attrs_are_prometa_only(attrs))

    def test_record_user_feedback_emits_targeted_feedback_span(self) -> None:
        recorded = record_user_feedback(
            rating=5,
            comment="Exactly what I needed.",
            source="stars",
            target_trace_id="trace_abc",
            target_span_id="span_def",
            target_session_id="session_ghi",
            submitted_at="2026-06-05T09:30:00Z",
        )

        self.assertTrue(recorded)
        span = self.prometa._buffer[-1]
        attrs = span.attributes
        self.assertEqual(span.name, "feedback.record")
        self.assertEqual(span.kind, "feedback")
        self.assertEqual(attrs["prometa.feedback.signal"], "rating,comment")
        self.assertEqual(attrs["prometa.feedback.rating"], 5)
        self.assertEqual(attrs["prometa.feedback.score"], 1.0)
        self.assertEqual(attrs["prometa.feedback.sentiment"], "positive")
        self.assertEqual(attrs["prometa.feedback.target.trace_id"], "trace_abc")
        self.assertEqual(attrs["prometa.feedback.target.span_id"], "span_def")
        self.assertEqual(attrs["prometa.feedback.target.session_id"], "session_ghi")
        self.assertEqual(
            attrs["prometa.feedback.submitted_at"],
            "2026-06-05T09:30:00Z",
        )
        self.assertTrue(_feedback_attrs_are_prometa_only(attrs))

    def test_record_user_feedback_inside_workflow_nests_in_active_trace(self) -> None:
        @self.prometa.workflow(name="root")
        def handle():
            record_user_feedback(liked=True, source="inline_ui")

        handle()

        feedback_span, root_span = self.prometa._buffer
        self.assertEqual(feedback_span.name, "feedback.record")
        self.assertEqual(feedback_span.parent_span_id, root_span.span_id)
        self.assertEqual(feedback_span.trace_id, root_span.trace_id)
        self.assertTrue(feedback_span.attributes["prometa.feedback.liked"])

    def test_build_user_feedback_attrs_accepts_comment_only(self) -> None:
        attrs = build_user_feedback_attrs(comment="Please include citations.")

        self.assertEqual(attrs["prometa.feedback.signal"], "comment")
        self.assertNotIn("prometa.feedback.score", attrs)
        self.assertEqual(attrs["prometa.feedback.comment"], "Please include citations.")

    def test_invalid_feedback_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "liked, rating, or comment"):
            build_user_feedback_attrs()
        with self.assertRaisesRegex(ValueError, "rating must be an integer"):
            build_user_feedback_attrs(rating=6)
        with self.assertRaisesRegex(ValueError, "liked must be a bool"):
            build_user_feedback_attrs(liked="yes")

    def test_helper_returns_false_outside_span(self) -> None:
        self.assertFalse(set_user_feedback(liked=True))


def _feedback_attrs_are_prometa_only(attrs: dict) -> bool:
    return all(
        key.startswith("prometa.feedback.")
        for key in attrs
        if ".feedback." in key
    )


if __name__ == "__main__":
    unittest.main()
