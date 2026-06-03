"""Tests for platform assistant intent trace attributes."""

from __future__ import annotations

import unittest

from prometa import (
    Prometa,
    classify_assistant_intent,
    set_assistant_intent,
    set_assistant_intent_from_text,
)
from prometa.integrations import _llm_common
from prometa.integrations import openai as prometa_openai


class AssistantIntentClassifierTest(unittest.TestCase):
    def test_general_question_is_label_a(self) -> None:
        self.assertEqual(
            classify_assistant_intent("What does this assistant do?"),
            ("A",),
        )

    def test_pipeline_status_question_is_labels_b_c(self) -> None:
        self.assertEqual(
            classify_assistant_intent("What is the current status of the pipeline flow?"),
            ("B", "C"),
        )

    def test_config_edit_plus_flow_execution_emits_multiple_labels(self) -> None:
        self.assertEqual(
            classify_assistant_intent(
                "Change the settings to use Gemma and then run the flow"
            ),
            ("D", "E"),
        )


class AssistantIntentSpanAttrsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.prometa = Prometa(
            endpoint="http://localhost:0/never-flushed",
            api_key=None,
            solution_id="sol-test",
            agent_name="test-agent",
            agent_id="test-agent-id",
            stage="test",
        )

    def test_set_assistant_intent_stamps_prometa_attrs(self) -> None:
        @self.prometa.workflow(name="root")
        def handle():
            set_assistant_intent(
                "D,E",
                source="quick_action",
                preclassified=True,
            )

        handle()

        attrs = self.prometa._buffer[-1].attributes
        self.assertEqual(attrs["prometa.intent.labels"], "D,E")
        self.assertEqual(
            attrs["prometa.intent.label_names"],
            "configuration_editing_execution,flow_process_execution",
        )
        self.assertEqual(attrs["prometa.intent.count"], 2)
        self.assertEqual(attrs["prometa.intent.source"], "quick_action")
        self.assertTrue(attrs["prometa.intent.preclassified"])
        self.assertEqual(attrs["prometa.intent.classifier_version"], "preclassified")
        self.assertTrue(_intent_attrs_are_prometa_only(attrs))

    def test_set_assistant_intent_from_text_uses_deterministic_classifier(self) -> None:
        @self.prometa.workflow(name="root")
        def handle():
            set_assistant_intent_from_text(
                "Update the model configuration and start the replay"
            )

        handle()

        attrs = self.prometa._buffer[-1].attributes
        self.assertEqual(attrs["prometa.intent.labels"], "D,E")
        self.assertFalse(attrs["prometa.intent.preclassified"])
        self.assertEqual(
            attrs["prometa.intent.classifier_version"],
            "deterministic_clause_v1",
        )

    def test_child_spans_inherit_parent_intent_attrs(self) -> None:
        @self.prometa.workflow(name="outer")
        def outer():
            set_assistant_intent("C", source="user_turn", preclassified=False)

            @self.prometa.tool(name="inner")
            def inner():
                return None

            inner()

        outer()

        self.assertEqual(len(self.prometa._buffer), 2)
        for span in self.prometa._buffer:
            self.assertEqual(span.attributes["prometa.intent.labels"], "C")
            self.assertEqual(span.attributes["prometa.intent.source"], "user_turn")
            self.assertTrue(_intent_attrs_are_prometa_only(span.attributes))

    def test_helper_returns_false_outside_span(self) -> None:
        self.assertFalse(set_assistant_intent("A"))
        self.assertFalse(set_assistant_intent_from_text("What is this?"))


class AssistantIntentLlmAttrsTest(unittest.TestCase):
    def test_openai_request_attrs_classify_latest_user_text(self) -> None:
        attrs = prometa_openai._request_attrs(
            {
                "model": "gpt-test",
                "messages": [
                    {"role": "system", "content": "You are helpful."},
                    {
                        "role": "user",
                        "content": "Change the settings and then run the flow",
                    },
                ],
            }
        )

        self.assertEqual(attrs["prometa.intent.labels"], "D,E")
        self.assertFalse(attrs["prometa.intent.preclassified"])
        self.assertTrue(_intent_attrs_are_prometa_only(attrs))

    def test_openai_request_attrs_strip_and_use_preclassified_intent_kwargs(self) -> None:
        kwargs = {
            "model": "gpt-test",
            "messages": [
                {
                    "role": "user",
                    "content": "This text would otherwise classify differently.",
                }
            ],
            "prometa_intent_labels": "D,E",
            "prometa_intent_source": "quick_action",
            "prometa_intent_preclassified": True,
        }

        attrs = prometa_openai._request_attrs(kwargs)

        self.assertNotIn("prometa_intent_labels", kwargs)
        self.assertNotIn("prometa_intent_source", kwargs)
        self.assertNotIn("prometa_intent_preclassified", kwargs)
        self.assertEqual(attrs["prometa.intent.labels"], "D,E")
        self.assertEqual(attrs["prometa.intent.source"], "quick_action")
        self.assertTrue(attrs["prometa.intent.preclassified"])
        self.assertTrue(_intent_attrs_are_prometa_only(attrs))

    def test_intent_kwarg_pop_strips_metadata_without_labels(self) -> None:
        kwargs = {
            "prometa_intent_source": "quick_action",
            "prometa_intent_preclassified": True,
        }

        attrs = _llm_common.pop_assistant_intent_attrs(kwargs)

        self.assertEqual(attrs, {})
        self.assertEqual(kwargs, {})


def _intent_attrs_are_prometa_only(attrs: dict) -> bool:
    return all(
        key.startswith("prometa.intent.")
        for key in attrs
        if ".intent." in key
    )


if __name__ == "__main__":
    unittest.main()
