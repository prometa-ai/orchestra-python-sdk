"""Tests for the optional OpenLLMetry bridge.

The bridge must preserve Prometa's existing span contract even though
OpenLLMetry emits standard OpenTelemetry spans with newer GenAI
attribute names.
"""

from __future__ import annotations

import json
import unittest

from prometa import Prometa
from prometa.integrations import openllmetry


class _FakeSpanContext:
    def __init__(self, trace_id: int, span_id: int) -> None:
        self.trace_id = trace_id
        self.span_id = span_id
        self.is_valid = True


class _FakeResource:
    def __init__(self, attributes=None) -> None:
        self.attributes = attributes or {}


class _FakeStatusCode:
    name = "OK"


class _FakeStatus:
    status_code = _FakeStatusCode()


class _FakeReadableSpan:
    def __init__(
        self,
        *,
        name: str,
        trace_id: int,
        span_id: int,
        parent=None,
        attributes=None,
    ) -> None:
        self.name = name
        self.context = _FakeSpanContext(trace_id, span_id)
        self.parent = parent
        self.attributes = attributes or {}
        self.resource = _FakeResource()
        self.start_time = 100
        self.end_time = 200
        self.status = _FakeStatus()


class OpenLLMetryBridgeTest(unittest.TestCase):
    def test_normalizes_openllmetry_genai_messages_for_prometa_ui(self) -> None:
        input_messages = json.dumps(
            [
                {"role": "system", "content": "Be brief."},
                {"role": "user", "content": [{"type": "text", "text": "hello"}]},
            ]
        )
        output_messages = json.dumps(
            [
                {
                    "role": "assistant",
                    "content": [{"type": "text", "text": "hi there"}],
                }
            ]
        )

        attrs = openllmetry._normalize_attributes(
            {
                "gen_ai.provider.name": "openai",
                "gen_ai.input.messages": input_messages,
                "gen_ai.output.messages": output_messages,
                "gen_ai.response.finish_reasons": ("stop",),
            }
        )

        self.assertEqual(attrs["gen_ai.system"], "openai")
        self.assertEqual(attrs["gen_ai.prompt"], input_messages)
        self.assertEqual(attrs["gen_ai.prompt.user"], "hello")
        self.assertEqual(attrs["gen_ai.completion"], "hi there")
        self.assertEqual(attrs["gen_ai.response.finish_reasons"], "stop")

    def test_processor_parents_openllmetry_span_under_active_prometa_span(self) -> None:
        prometa = Prometa(
            endpoint="http://localhost:0/never-flushed",
            api_key=None,
            solution_id="sol-test",
            agent_name="test-agent",
            stage="test",
            customer_id="cus-default",
        )
        processor = openllmetry._PrometaSpanProcessor(lambda: prometa)

        @prometa.workflow(name="root", session_id="conv-1")
        def root():
            span = _FakeReadableSpan(
                name="chat gpt-4o",
                trace_id=0x11111111111111111111111111111111,
                span_id=0x2222222222222222,
                attributes={
                    "gen_ai.provider.name": "openai",
                    "gen_ai.request.model": "gpt-4o",
                },
            )
            processor.on_start(span)
            processor.on_end(span)

        root()

        self.assertEqual(len(prometa._buffer), 2)
        openllmetry_span = prometa._buffer[0]
        root_span = prometa._buffer[1]

        self.assertEqual(openllmetry_span.trace_id, root_span.trace_id)
        self.assertEqual(openllmetry_span.parent_span_id, root_span.span_id)
        self.assertEqual(openllmetry_span.kind, "agent")
        self.assertEqual(
            openllmetry_span.attributes["prometa.instrumentation.provider"],
            "openllmetry",
        )
        self.assertEqual(openllmetry_span.attributes["prometa.solution_id"], "sol-test")
        self.assertEqual(
            openllmetry_span.attributes["gen_ai.conversation.id"],
            "conv-1",
        )
        self.assertEqual(
            openllmetry_span.attributes["prometa.customer_id"],
            "cus-default",
        )


if __name__ == "__main__":
    unittest.main()
