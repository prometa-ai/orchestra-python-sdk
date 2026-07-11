"""OpenAI-compatible tenant model-plane adapter tests."""

from __future__ import annotations

import asyncio
import io
import json
import urllib.error
import urllib.request

import pytest

from prometa.runtime import (
    ModelAdapterError,
    ModelInvocationRequest,
    OpenAICompatibleModelAdapter,
    RuntimeModel,
    RuntimeTool,
)


MODEL = RuntimeModel(
    name="Primary",
    provider="inference-engine",
    model_name="model-v1",
    role="primary",
    temperature=0.2,
    max_output_tokens=256,
    structured_output=True,
)
TOOL = RuntimeTool(
    name="Lookup order",
    source="mcp",
    operation="orders.lookup",
    input_schema={
        "type": "object",
        "properties": {"orderId": {"type": "string"}},
        "required": ["orderId"],
    },
    mcp_server="Orders",
    side_effects="read-only",
    risk_level="low",
    auth_binding="service-account",
    scopes=("orders.read",),
    approval_required=False,
    required_guardrails=(),
)
OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
}


def _request(*, tools=(), output_schema=None):
    return ModelInvocationRequest(
        request_id="request-1",
        model=MODEL,
        messages=(
            {"role": "system", "content": "Be useful."},
            {"role": "user", "content": "Where is order 42?"},
        ),
        tools=tuple(tools),
        output_schema=output_schema,
        attempt=1,
    )


class Response:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self, amount: int) -> bytes:
        return self.body[:amount]


def test_builds_bounded_openai_request_without_leaking_credentials(monkeypatch) -> None:
    captured = {}
    body = json.dumps(
        {
            "model": "model-v1@sha256:test",
            "choices": [
                {
                    "finish_reason": "stop",
                    "message": {"content": '{"answer":"ready"}'},
                }
            ],
        }
    ).encode()

    def fake_urlopen(request, timeout):
        captured["request"] = request
        captured["timeout"] = timeout
        return Response(body)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    adapter = OpenAICompatibleModelAdapter(
        "https://models.tenant.example",
        api_key="tenant-secret",
        timeout_seconds=7,
        headers={
            "x-tenant": "org-1",
            "x-orchestra-runtime-request-id": "must-not-win",
            "content-type": "text/plain",
        },
    )
    response = asyncio.run(
        adapter.invoke(_request(tools=(TOOL,), output_schema=OUTPUT_SCHEMA))
    )

    assert response.content == '{"answer":"ready"}'
    assert response.provider_model == "model-v1@sha256:test"
    request = captured["request"]
    payload = json.loads(request.data)
    assert request.full_url == "https://models.tenant.example/v1/chat/completions"
    assert request.get_header("Authorization") == "Bearer tenant-secret"
    assert request.get_header("X-orchestra-runtime-request-id") == "request-1"
    assert request.get_header("Content-type") == "application/json"
    assert payload["model"] == "model-v1"
    assert payload["tools"][0]["function"]["parameters"] == TOOL.input_schema
    assert payload["response_format"]["json_schema"]["strict"] is True
    assert captured["timeout"] == 7


def test_parses_strict_tool_calls() -> None:
    body = json.dumps(
        {
            "model": "model-v1",
            "choices": [
                {
                    "finish_reason": "tool_calls",
                    "message": {
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call-1",
                                "type": "function",
                                "function": {
                                    "name": "orders.lookup",
                                    "arguments": '{"orderId":"42"}',
                                },
                            }
                        ],
                    },
                }
            ],
        }
    ).encode()
    response = OpenAICompatibleModelAdapter._parse_response(body)
    assert response.tool_calls[0].name == "orders.lookup"
    assert response.tool_calls[0].arguments == {"orderId": "42"}


@pytest.mark.parametrize(
    ("body", "code"),
    [
        (b"not-json", "model_response_invalid_json"),
        (b'{"choices":[],"choices":[]}', "model_response_invalid_json"),
        (b"[]", "model_response_invalid"),
        (b'{"choices":[]}', "model_response_invalid_choices"),
        (
            json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call-1",
                                        "function": {
                                            "name": "orders.lookup",
                                            "arguments": "[]",
                                        },
                                    }
                                ],
                            }
                        }
                    ]
                }
            ).encode(),
            "model_tool_arguments_not_object",
        ),
    ],
)
def test_malformed_model_responses_fail_closed(body, code) -> None:
    with pytest.raises(ModelAdapterError) as caught:
        OpenAICompatibleModelAdapter._parse_response(body)
    assert caught.value.code == code


def test_http_statuses_preserve_retry_semantics_and_hide_token(monkeypatch) -> None:
    def throttled(request, timeout):
        raise urllib.error.HTTPError(
            request.full_url,
            429,
            "throttled",
            {},
            io.BytesIO(b'{"error":"slow down"}'),
        )

    monkeypatch.setattr(urllib.request, "urlopen", throttled)
    adapter = OpenAICompatibleModelAdapter(
        "https://models.tenant.example", api_key="do-not-print"
    )
    with pytest.raises(ModelAdapterError) as caught:
        asyncio.run(adapter.invoke(_request()))
    assert caught.value.code == "model_http_429"
    assert caught.value.retryable is True
    assert "do-not-print" not in str(caught.value)


def test_response_size_limit_and_url_configuration_are_strict(monkeypatch) -> None:
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda request, timeout: Response(b"x" * 10),
    )
    adapter = OpenAICompatibleModelAdapter(
        "http://127.0.0.1:9000", max_response_bytes=4
    )
    with pytest.raises(ModelAdapterError) as caught:
        asyncio.run(adapter.invoke(_request()))
    assert caught.value.code == "model_response_too_large"

    with pytest.raises(ValueError):
        OpenAICompatibleModelAdapter("models.internal")
    with pytest.raises(ValueError):
        OpenAICompatibleModelAdapter("https://user:secret@models.internal")
    with pytest.raises(ValueError):
        OpenAICompatibleModelAdapter("https://models.internal?token=secret")
    with pytest.raises(ValueError):
        OpenAICompatibleModelAdapter(
            "https://models.internal", endpoint_path="v1/chat/completions"
        )
