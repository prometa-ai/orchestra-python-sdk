"""OpenAI-compatible tenant model-plane adapter tests."""

from __future__ import annotations

import asyncio
import http.client
import io
import json
import sys
import urllib.error
import urllib.request
from dataclasses import asdict, fields, replace
from datetime import datetime, timedelta, timezone
from itertools import product
from types import ModuleType

import pytest
import prometa.runtime.model_gateway as model_gateway
from prometa import Prometa

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
ENGINE_REQUEST_ID = "req_" + "a" * 32
USAGE_RECORD_ID = "usage_" + "b" * 32


def _request(*, tools=(), output_schema=None, runtime_request_id="runtime-request-1"):
    return ModelInvocationRequest(
        runtime_request_id=runtime_request_id,
        model_invocation_id="model-invocation-1",
        model_attempt_id="model-attempt-1",
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
    def __init__(self, body: bytes, headers=None) -> None:
        self.body = body
        self.headers = dict(headers or {})

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self, amount: int) -> bytes:
        return self.body[:amount]


def _exception_chain(error):
    seen = set()
    current = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def test_optional_w3c_propagator_forwards_trace_context_only(monkeypatch) -> None:
    module = ModuleType("opentelemetry.trace.propagation.tracecontext")

    class Propagator:
        def inject(self, carrier):
            carrier.update(
                {
                    "traceparent": (
                        "00-4bf92f3577b34da6a3ce929d0e0e4736-"
                        "00f067aa0ba902b7-01"
                    ),
                    "tracestate": "vendor=value",
                    "baggage": "secret=must-not-cross",
                }
            )

    module.TraceContextTextMapPropagator = Propagator
    monkeypatch.setitem(
        sys.modules,
        "opentelemetry.trace.propagation.tracecontext",
        module,
    )
    monkeypatch.setattr(model_gateway, "_native_prometa_trace_headers", lambda: {})

    assert model_gateway._w3c_trace_headers() == {
        "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "tracestate": "vendor=value",
    }


def test_native_prometa_span_propagates_before_optional_otel(monkeypatch) -> None:
    captured = {}
    body = json.dumps(
        {"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]}
    ).encode()

    def fake_urlopen(request, timeout):
        captured["request"] = request
        return Response(
            body,
            {
                "x-request-id": ENGINE_REQUEST_ID,
                "x-orchestra-usage-record-id": USAGE_RECORD_ID,
            },
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(
        model_gateway,
        "_otel_w3c_trace_headers",
        lambda: {
            "traceparent": (
                "00-ffffffffffffffffffffffffffffffff-ffffffffffffffff-01"
            )
        },
    )
    adapter = OpenAICompatibleModelAdapter("https://models.tenant.example")
    client = Prometa(
        endpoint="http://localhost:0/never-flushed",
        agent_name="runtime-identity-test",
    )

    with client._span("task", "runtime-model-call") as native_span:
        asyncio.run(adapter.invoke(_request()))

    assert captured["request"].get_header("Traceparent") == (
        "00-%s-%s-01" % (native_span.trace_id, native_span.span_id)
    )


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
        return Response(
            body,
            {
                "x-request-id": ENGINE_REQUEST_ID,
                "x-orchestra-usage-record-id": USAGE_RECORD_ID,
            },
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(
        model_gateway,
        "_w3c_trace_headers",
        lambda: {
            "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "tracestate": "vendor=value",
        },
    )
    adapter = OpenAICompatibleModelAdapter(
        "https://models.tenant.example",
        api_key="tenant-secret",
        timeout_seconds=7,
        headers={
            "x-tenant": "org-1",
            "x-orchestra-runtime-request-id": "must-not-win",
            "X-Orchestra-Model-Invocation-Id": "must-not-win",
            "x-orchestra-model-attempt-id": "must-not-win",
            "x-request-id": "must-not-be-sent",
            "x-orchestra-usage-record-id": "must-not-be-sent",
            "Traceparent": (
                "00-ffffffffffffffffffffffffffffffff-ffffffffffffffff-01"
            ),
            "tracestate": "caller=must-not-win",
            "baggage": "secret=must-not-cross",
            "content-type": "text/plain",
        },
    )
    response = asyncio.run(
        adapter.invoke(_request(tools=(TOOL,), output_schema=OUTPUT_SCHEMA))
    )

    assert response.content == '{"answer":"ready"}'
    assert response.provider_model == "model-v1@sha256:test"
    assert response.engine_request_id == ENGINE_REQUEST_ID
    assert response.usage_record_id == USAGE_RECORD_ID
    request = captured["request"]
    payload = json.loads(request.data)
    assert request.full_url == "https://models.tenant.example/v1/chat/completions"
    assert request.get_header("Authorization") == "Bearer tenant-secret"
    assert (
        request.get_header("X-orchestra-runtime-request-id")
        == "runtime-request-1"
    )
    assert (
        request.get_header("X-orchestra-model-invocation-id")
        == "model-invocation-1"
    )
    assert (
        request.get_header("X-orchestra-model-attempt-id") == "model-attempt-1"
    )
    assert request.get_header("X-request-id") is None
    assert request.get_header("X-orchestra-usage-record-id") is None
    assert request.get_header("Baggage") is None
    assert request.get_header("Traceparent") == (
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    )
    assert request.get_header("Tracestate") == "vendor=value"
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
            {
                "x-request-id": ENGINE_REQUEST_ID,
                "x-orchestra-usage-record-id": USAGE_RECORD_ID,
                "authorization": "must-never-be-captured",
            },
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
    assert caught.value.retry_after_seconds is None
    assert caught.value.engine_request_id == ENGINE_REQUEST_ID
    assert caught.value.usage_record_id == USAGE_RECORD_ID
    assert "do-not-print" not in str(caught.value)
    assert "must-never-be-captured" not in str(caught.value)


@pytest.mark.parametrize(
    (
        "headers",
        "runtime_request_id",
        "expected_engine_id",
        "expected_usage_id",
    ),
    [
        ({"x-request-id": "runtime-request-1"}, "runtime-request-1", None, None),
        ({"x-request-id": ENGINE_REQUEST_ID}, ENGINE_REQUEST_ID, None, None),
        ({"x-request-id": "contains space"}, "runtime-request-1", None, None),
        (
            {"x-orchestra-usage-record-id": "x" * 257},
            "runtime-request-1",
            None,
            None,
        ),
        (
            {
                "x-request-id": ENGINE_REQUEST_ID,
                "x-orchestra-usage-record-id": USAGE_RECORD_ID,
            },
            "runtime-request-1",
            ENGINE_REQUEST_ID,
            USAGE_RECORD_ID,
        ),
    ],
)
def test_server_identity_compatibility_matrix(
    monkeypatch,
    headers,
    runtime_request_id,
    expected_engine_id,
    expected_usage_id,
) -> None:
    body = json.dumps(
        {"choices": [{"message": {"content": "ok"}, "finish_reason": "stop"}]}
    ).encode()
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda request, timeout: Response(body, headers),
    )
    adapter = OpenAICompatibleModelAdapter("https://models.tenant.example")

    response = asyncio.run(
        adapter.invoke(_request(runtime_request_id=runtime_request_id))
    )

    assert response.engine_request_id == expected_engine_id
    assert response.usage_record_id == expected_usage_id


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("runtime_request_id", "contains space"),
        ("model_invocation_id", "line\nbreak"),
        ("model_attempt_id", "x" * 257),
    ],
)
def test_outbound_identity_values_are_rejected_instead_of_rewritten(
    field, value
) -> None:
    values = {
        "runtime_request_id": "r" * 256,
        "model_invocation_id": "i" * 256,
        "model_attempt_id": "a" * 256,
        "model": MODEL,
        "messages": (),
        "tools": (),
        "output_schema": None,
        "attempt": 1,
    }
    # The exact upper bound is accepted and exposed through the compatibility
    # alias without mutation.
    valid = ModelInvocationRequest(**values)
    assert valid.request_id == "r" * 256

    values[field] = value
    with pytest.raises(ValueError, match=field):
        ModelInvocationRequest(**values)


def test_all_null_sentinel_case_variants_are_rejected_for_external_ids() -> None:
    base = {
        "runtime_request_id": "runtime-request",
        "model_invocation_id": "model-invocation",
        "model_attempt_id": "model-attempt",
        "model": MODEL,
        "messages": (),
        "tools": (),
        "output_schema": None,
        "attempt": 1,
    }
    for field in (
        "runtime_request_id",
        "model_invocation_id",
        "model_attempt_id",
    ):
        for sentinel in ("null", "none", "nil", "undefined"):
            variants = {
                "".join(characters)
                for characters in product(
                    *((character.lower(), character.upper()) for character in sentinel)
                )
            }
            for variant in variants:
                values = dict(base)
                values[field] = variant
                with pytest.raises(ValueError, match="reserved null identity sentinel"):
                    ModelInvocationRequest(**values)


@pytest.mark.parametrize(
    "value",
    ["!", "~" * 256, "nullx", "none-", "nil0", "undefined_", "NULLX"],
)
def test_ordinary_external_identity_boundaries_remain_valid(value) -> None:
    request = ModelInvocationRequest(
        runtime_request_id=value,
        model_invocation_id=value,
        model_attempt_id=value,
        model=MODEL,
        messages=(),
        tools=(),
        output_schema=None,
        attempt=1,
    )

    assert request.runtime_request_id == value
    assert request.model_invocation_id == value
    assert request.model_attempt_id == value


def test_model_invocation_request_preserves_legacy_keyword_and_positional_api() -> None:
    messages = ({"role": "user", "content": "hello"},)
    keyword = ModelInvocationRequest(
        request_id="legacy-keyword-request",
        model=MODEL,
        messages=messages,
        tools=(),
        output_schema=None,
        attempt=1,
    )
    positional = ModelInvocationRequest(
        "legacy-positional-request",
        MODEL,
        messages,
        (),
        None,
        2,
    )

    assert keyword.request_id == "legacy-keyword-request"
    assert keyword.runtime_request_id == keyword.request_id
    assert positional.request_id == "legacy-positional-request"
    assert positional.runtime_request_id == positional.request_id
    assert keyword.model_invocation_id != positional.model_invocation_id
    assert keyword.model_attempt_id != positional.model_attempt_id
    assert len(keyword.model_invocation_id) == 36
    assert len(keyword.model_attempt_id) == 36
    legacy_fields = (
        "request_id",
        "model",
        "messages",
        "tools",
        "output_schema",
        "attempt",
    )
    assert tuple(field.name for field in fields(keyword)) == legacy_fields
    if sys.version_info >= (3, 10):
        assert ModelInvocationRequest.__match_args__ == legacy_fields
    assert tuple(asdict(keyword)) == legacy_fields
    assert asdict(keyword)["request_id"] == "legacy-keyword-request"
    assert "runtime_request_id" not in repr(keyword)
    assert "model_invocation_id" not in repr(keyword)
    assert "model_attempt_id" not in repr(keyword)

    cloned = replace(keyword, attempt=2)
    assert cloned.attempt == 2
    assert cloned.model_invocation_id == keyword.model_invocation_id
    assert cloned.model_attempt_id == keyword.model_attempt_id
    rotated_attempt = replace(keyword, model_attempt_id="replacement-attempt")
    assert rotated_attempt.model_invocation_id == keyword.model_invocation_id
    assert rotated_attempt.model_attempt_id == "replacement-attempt"
    rotated_turn = replace(
        keyword,
        model_invocation_id="replacement-invocation",
        model_attempt_id="replacement-turn-attempt",
    )
    assert rotated_turn.model_invocation_id == "replacement-invocation"
    assert rotated_turn.model_attempt_id == "replacement-turn-attempt"

    with pytest.raises(ValueError, match="must match"):
        ModelInvocationRequest(
            request_id="legacy-request",
            runtime_request_id="new-request",
            model=MODEL,
            messages=messages,
            tools=(),
            output_schema=None,
            attempt=1,
        )


def test_retry_after_is_normalized_without_permitting_fail_open_values(
    monkeypatch,
) -> None:
    def throttled(request, timeout):
        raise urllib.error.HTTPError(
            request.full_url,
            429,
            "throttled",
            {"Retry-After": "42"},
            io.BytesIO(b'{}'),
        )

    monkeypatch.setattr(urllib.request, "urlopen", throttled)
    adapter = OpenAICompatibleModelAdapter("https://models.tenant.example")
    with pytest.raises(ModelAdapterError) as caught:
        asyncio.run(adapter.invoke(_request()))
    assert caught.value.retry_after_seconds == 42

    now = datetime(2026, 7, 20, tzinfo=timezone.utc)
    retry_at = now + timedelta(seconds=17)
    assert adapter._retry_after_seconds(
        {"Retry-After": retry_at.strftime("%a, %d %b %Y %H:%M:%S GMT")},
        now=now,
    ) == 17
    assert adapter._retry_after_seconds({"Retry-After": "invalid"}, now=now) is None
    assert adapter._retry_after_seconds({"Retry-After": "999999"}) == 86_400


def test_response_size_limit_and_url_configuration_are_strict(monkeypatch) -> None:
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda request, timeout: Response(
            b"x" * 10,
            {
                "x-request-id": ENGINE_REQUEST_ID,
                "x-orchestra-usage-record-id": USAGE_RECORD_ID,
            },
        ),
    )
    adapter = OpenAICompatibleModelAdapter(
        "http://127.0.0.1:9000", max_response_bytes=4
    )
    with pytest.raises(ModelAdapterError) as caught:
        asyncio.run(adapter.invoke(_request()))
    assert caught.value.code == "model_response_too_large"
    assert caught.value.engine_request_id == ENGINE_REQUEST_ID
    assert caught.value.usage_record_id == USAGE_RECORD_ID

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


def test_success_status_parse_failure_preserves_server_identities(monkeypatch) -> None:
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda request, timeout: Response(
            b"not-json",
            {
                "x-request-id": ENGINE_REQUEST_ID,
                "x-orchestra-usage-record-id": USAGE_RECORD_ID,
                "set-cookie": "secret-cookie",
            },
        ),
    )
    adapter = OpenAICompatibleModelAdapter("https://models.tenant.example")

    with pytest.raises(ModelAdapterError) as caught:
        asyncio.run(adapter.invoke(_request()))

    assert caught.value.code == "model_response_invalid_json"
    assert caught.value.engine_request_id == ENGINE_REQUEST_ID
    assert caught.value.usage_record_id == USAGE_RECORD_ID
    assert "secret-cookie" not in str(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert b"not-json" not in repr(caught.value).encode()


@pytest.mark.parametrize(
    "read_error",
    [
        http.client.IncompleteRead(b"partial", 10),
        http.client.HTTPException("response framing failed"),
    ],
)
def test_response_read_http_failures_preserve_canonical_headers(
    monkeypatch, read_error
) -> None:
    class ReadFailure(Response):
        def read(self, amount):
            raise read_error

    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda request, timeout: ReadFailure(
            b"",
            {
                "x-request-id": ENGINE_REQUEST_ID,
                "x-orchestra-usage-record-id": USAGE_RECORD_ID,
            },
        ),
    )
    adapter = OpenAICompatibleModelAdapter("https://models.tenant.example")

    with pytest.raises(ModelAdapterError) as caught:
        asyncio.run(adapter.invoke(_request()))

    assert caught.value.code == "model_transport_failed"
    assert caught.value.retryable is True
    assert caught.value.engine_request_id == ENGINE_REQUEST_ID
    assert caught.value.usage_record_id == USAGE_RECORD_ID
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert [caught.value] == list(_exception_chain(caught.value))
    if isinstance(read_error, http.client.IncompleteRead):
        assert read_error.partial == b"partial"
        assert b"partial" not in repr(caught.value).encode()


# ---------------------------------------------------------------------------
# Token usage — the engine reports real counts; the adapter must carry them
# through instead of leaving the runtime to estimate.
# ---------------------------------------------------------------------------


def _response_with(usage) -> bytes:
    document = {
        "model": "model-v1@sha256:test",
        "choices": [
            {"message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}
        ],
    }
    if usage is not None:
        document["usage"] = usage
    return json.dumps(document).encode("utf-8")


def test_usage_is_captured_from_the_provider_response() -> None:
    parsed = OpenAICompatibleModelAdapter._parse_response(
        _response_with({"prompt_tokens": 41, "completion_tokens": 7})
    )
    assert parsed.usage is not None
    assert parsed.usage.input_tokens == 41
    assert parsed.usage.output_tokens == 7
    assert parsed.usage.total_tokens == 48
    assert parsed.usage.cached_input_tokens == 0


def test_cached_prompt_tokens_are_captured() -> None:
    parsed = OpenAICompatibleModelAdapter._parse_response(
        _response_with(
            {
                "prompt_tokens": 100,
                "completion_tokens": 4,
                "prompt_tokens_details": {"cached_tokens": 64},
            }
        )
    )
    assert parsed.usage.cached_input_tokens == 64


def test_cached_tokens_are_clamped_to_prompt_tokens() -> None:
    """cached_tokens is a sub-count of prompt_tokens. A provider reporting more
    cached than prompt is incoherent and must not reach budget accounting."""
    parsed = OpenAICompatibleModelAdapter._parse_response(
        _response_with(
            {
                "prompt_tokens": 39,
                "completion_tokens": 2,
                "prompt_tokens_details": {"cached_tokens": 43},
            }
        )
    )
    assert parsed.usage.cached_input_tokens == 39


def test_missing_usage_reports_none_rather_than_zero() -> None:
    """None means 'this adapter cannot say'; zero would claim no tokens were spent."""
    assert OpenAICompatibleModelAdapter._parse_response(_response_with(None)).usage is None
    assert OpenAICompatibleModelAdapter._parse_response(_response_with({})).usage is None


@pytest.mark.parametrize(
    "usage",
    [
        {"prompt_tokens": -5, "completion_tokens": -1},
        {"prompt_tokens": "41", "completion_tokens": "7"},
        {"prompt_tokens": True, "completion_tokens": False},
        {"prompt_tokens": 1.5, "completion_tokens": 2.5},
    ],
)
def test_implausible_token_counts_are_refused_not_trusted(usage) -> None:
    """Usage is provider-controlled; nonsense must not become budget input."""
    assert OpenAICompatibleModelAdapter._parse_response(_response_with(usage)).usage is None


def test_malformed_prompt_tokens_details_does_not_fail_the_response() -> None:
    parsed = OpenAICompatibleModelAdapter._parse_response(
        _response_with(
            {"prompt_tokens": 10, "completion_tokens": 2, "prompt_tokens_details": "nope"}
        )
    )
    assert parsed.usage.input_tokens == 10
    assert parsed.usage.cached_input_tokens == 0


# ---------------------------------------------------------------------------
# Typed error codes — the engine's `error` envelope decides retryability that
# the status code alone gets wrong.
# ---------------------------------------------------------------------------


def _raise_http(monkeypatch, status: int, body: bytes) -> None:
    def failing(request, timeout):
        raise urllib.error.HTTPError(
            request.full_url, status, "err", {}, io.BytesIO(body)
        )

    monkeypatch.setattr(urllib.request, "urlopen", failing)


def _invoke_expecting_error(monkeypatch, status: int, body: bytes) -> ModelAdapterError:
    _raise_http(monkeypatch, status, body)
    adapter = OpenAICompatibleModelAdapter("https://models.tenant.example")
    with pytest.raises(ModelAdapterError) as caught:
        asyncio.run(adapter.invoke(_request()))
    error = caught.value
    assert error.__cause__ is None
    assert error.__context__ is None
    assert [error] == list(_exception_chain(error))
    if body:
        assert body not in repr(error).encode()
    return error


def test_structured_output_invalid_is_not_retried_despite_being_a_502(
    monkeypatch,
) -> None:
    """The engine already retried internally before answering; re-running an
    identical request cannot change the outcome."""
    error = _invoke_expecting_error(
        monkeypatch,
        502,
        b'{"error":{"code":"structured_output_invalid","message":"schema"}}',
    )
    assert error.retryable is False
    assert error.provider_code == "structured_output_invalid"


def test_transport_level_error_code_is_unchanged(monkeypatch) -> None:
    """`code` stays model_http_<status> — callers branch on it."""
    error = _invoke_expecting_error(
        monkeypatch, 502, b'{"error":{"code":"structured_output_invalid"}}'
    )
    assert error.code == "model_http_502"


def test_transient_upstream_error_stays_retryable(monkeypatch) -> None:
    error = _invoke_expecting_error(
        monkeypatch, 502, b'{"error":{"code":"upstream_error"}}'
    )
    assert error.retryable is True
    assert error.provider_code == "upstream_error"


def test_error_type_is_used_when_code_is_absent(monkeypatch) -> None:
    error = _invoke_expecting_error(
        monkeypatch, 400, b'{"error":{"type":"context_length_exceeded"}}'
    )
    assert error.provider_code == "context_length_exceeded"
    assert error.retryable is False


@pytest.mark.parametrize(
    "body",
    [
        b"not json at all",
        b'{"error":"a bare string"}',
        b'{"detail":{"code":"something"}}',
        b"",
        b"[]",
    ],
)
def test_unparseable_error_bodies_fall_back_to_status_semantics(
    monkeypatch, body
) -> None:
    """An error path must never raise a second error."""
    error = _invoke_expecting_error(monkeypatch, 503, body)
    assert error.code == "model_http_503"
    assert error.retryable is True
    assert error.provider_code is None


def test_error_body_read_failure_does_not_mask_the_http_error(monkeypatch) -> None:
    class Exploding(io.BytesIO):
        def read(self, *args):  # noqa: ANN002
            raise OSError("socket died mid-body")

    def failing(request, timeout):
        raise urllib.error.HTTPError(
            request.full_url, 500, "err", {}, Exploding(b"")
        )

    monkeypatch.setattr(urllib.request, "urlopen", failing)
    adapter = OpenAICompatibleModelAdapter("https://models.tenant.example")
    with pytest.raises(ModelAdapterError) as caught:
        asyncio.run(adapter.invoke(_request()))
    assert caught.value.code == "model_http_500"
    assert caught.value.provider_code is None
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_error_body_does_not_leak_the_api_key(monkeypatch) -> None:
    _raise_http(monkeypatch, 502, b'{"error":{"code":"upstream_error"}}')
    adapter = OpenAICompatibleModelAdapter(
        "https://models.tenant.example", api_key="do-not-print"
    )
    with pytest.raises(ModelAdapterError) as caught:
        asyncio.run(adapter.invoke(_request()))
    assert "do-not-print" not in str(caught.value)


def test_usage_with_only_cached_tokens_is_treated_as_absent() -> None:
    """cached_tokens is a sub-count of a prompt total; alone it says nothing."""
    parsed = OpenAICompatibleModelAdapter._parse_response(
        _response_with({"prompt_tokens_details": {"cached_tokens": 12}})
    )
    assert parsed.usage is None
