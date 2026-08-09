"""OpenAI-compatible tenant model-plane adapter for the runtime kernel."""

from __future__ import annotations

import asyncio
import http.client
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Mapping, Optional, Tuple
from urllib.parse import urlparse

from .kernel import (
    ModelAdapterError,
    ModelInvocationRequest,
    ModelInvocationResponse,
    ModelTokenUsage,
    ModelToolCall,
    _canonical_server_identity,
)


_CLIENT_IDENTITY_HEADERS = frozenset(
    {
        "x-orchestra-runtime-request-id",
        "x-orchestra-model-invocation-id",
        "x-orchestra-model-attempt-id",
    }
)
_SERVER_IDENTITY_HEADERS = frozenset(
    {"x-request-id", "x-orchestra-usage-record-id"}
)
_TRACE_CONTEXT_HEADERS = frozenset({"traceparent", "tracestate", "baggage"})
_RESERVED_REQUEST_HEADERS = (
    _CLIENT_IDENTITY_HEADERS | _SERVER_IDENTITY_HEADERS | _TRACE_CONTEXT_HEADERS
)


def _native_prometa_trace_headers() -> Mapping[str, str]:
    """Translate the active native Prometa span into W3C trace context."""

    try:
        from .. import _context
    except ImportError:
        return {}
    try:
        span = _context.current_span()
    except Exception:  # noqa: BLE001 - telemetry must not break execution
        return {}
    if span is None:
        return {}
    trace_id = getattr(span, "trace_id", None)
    span_id = getattr(span, "span_id", None)
    if (
        not isinstance(trace_id, str)
        or len(trace_id) != 32
        or trace_id == "0" * 32
        or any(character not in "0123456789abcdef" for character in trace_id)
        or not isinstance(span_id, str)
        or len(span_id) != 16
        or span_id == "0" * 16
        or any(character not in "0123456789abcdef" for character in span_id)
    ):
        return {}
    return {"traceparent": "00-%s-%s-01" % (trace_id, span_id)}


def _otel_w3c_trace_headers() -> Mapping[str, str]:
    """Inject active W3C trace context when OpenTelemetry is installed.

    The runtime extra intentionally does not depend on OpenTelemetry.  When an
    application has enabled the optional tracing stack, its current context is
    the fallback when there is no native Prometa span.
    """

    try:
        from opentelemetry.trace.propagation.tracecontext import (  # type: ignore
            TraceContextTextMapPropagator,
        )
    except ImportError:
        return {}
    carrier: Dict[str, str] = {}
    try:
        TraceContextTextMapPropagator().inject(carrier)
    except Exception:  # noqa: BLE001 - tracing must not break model execution
        return {}
    return {
        key.lower(): value
        for key, value in carrier.items()
        if key.lower() in {"traceparent", "tracestate"}
        and isinstance(value, str)
        and "\r" not in value
        and "\n" not in value
    }


def _w3c_trace_headers() -> Mapping[str, str]:
    """Prefer native Prometa context, then optional OpenTelemetry context."""

    return _native_prometa_trace_headers() or _otel_w3c_trace_headers()


def _response_identity_header(
    headers: Any, name: str, field_name: str
) -> Optional[str]:
    """Read one canonical server identity; legacy echoes become absent."""

    if headers is None:
        return None
    get_all = getattr(headers, "get_all", None)
    if callable(get_all):
        values = list(get_all(name) or ())
    else:
        items = getattr(headers, "items", None)
        values = (
            [value for key, value in items() if str(key).lower() == name]
            if callable(items)
            else []
        )
    if not values:
        return None
    if len(values) != 1 or not isinstance(values[0], str):
        return None
    return _canonical_server_identity(values[0], field_name)


def _response_identities(
    headers: Any, runtime_request_id: str
) -> Tuple[Optional[str], Optional[str]]:
    engine_request_id = _response_identity_header(
        headers, "x-request-id", "engine_request_id"
    )
    if engine_request_id == runtime_request_id:
        # Old engines echoed the caller's runtime identity in x-request-id.
        # Even a caller-selected value that resembles a canonical engine ID
        # remains caller-owned and must never be relabeled as server-owned.
        engine_request_id = None
    return (
        engine_request_id,
        _response_identity_header(
            headers, "x-orchestra-usage-record-id", "usage_record_id"
        ),
    )


def _reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_json_constant(value):
    raise ValueError("non-finite JSON number: %s" % value)


def _strict_json_loads(value, code):
    try:
        return json.loads(
            value,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, TypeError, ValueError, json.JSONDecodeError):
        pass
    # Raise after leaving the parser exception handler. JSONDecodeError keeps
    # the complete source document, which must not remain reachable through a
    # public adapter error's cause/context chain.
    raise ModelAdapterError(code)


def _non_negative_int(value: Any) -> int:
    """Coerce a provider-reported token count, refusing anything implausible.

    Usage blocks are provider-controlled, so a bool, float, string, or negative
    is treated as absent rather than trusted into the runtime's budget maths.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return value if value > 0 else 0


def _parse_usage(usage: Any) -> Optional[ModelTokenUsage]:
    """Read an OpenAI-shaped ``usage`` block into ``ModelTokenUsage``.

    Returns ``None`` when the provider sent nothing usable, so callers can tell
    "no usage reported" apart from "zero tokens". ``cached_tokens`` lives under
    ``prompt_tokens_details`` and is clamped to ``input_tokens``: it is defined
    as a sub-count, and a provider reporting more cached than prompt tokens is
    stating something incoherent that must not reach budget accounting.
    """
    if not isinstance(usage, dict):
        return None

    input_tokens = _non_negative_int(usage.get("prompt_tokens"))
    output_tokens = _non_negative_int(usage.get("completion_tokens"))

    # Guard on the real counts only. A block carrying nothing but
    # ``cached_tokens`` says nothing usable — cached is a sub-count of a
    # prompt total we don't have — and must not become an all-zero object that
    # reads as "no tokens were spent".
    if not (input_tokens or output_tokens):
        return None

    details = usage.get("prompt_tokens_details")
    cached = (
        _non_negative_int(details.get("cached_tokens"))
        if isinstance(details, dict)
        else 0
    )
    return ModelTokenUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_input_tokens=min(cached, input_tokens),
    )


# Engine error codes that describe the *request*, not a transient condition.
# The engine returns some of these with a 5xx status — ``structured_output_invalid``
# is a 502 because the failure happened upstream of the response — but retrying
# an identical request cannot change the outcome, and the engine has already
# retried internally before answering. Without this, the runtime spends its
# whole ``max_attempts_per_model`` budget re-running a request that is
# deterministically going to fail.
_NON_RETRYABLE_ERROR_CODES = frozenset(
    {
        "context_length_exceeded",
        "structured_output_invalid",
        "tokenization_not_supported",
        "model_not_found",
    }
)


def _error_code(body: bytes) -> Optional[str]:
    """Extract ``error.code`` from an OpenAI-shaped error body.

    Best-effort by design: an error path must never raise a second error, so
    any malformed or oversized body simply yields ``None`` and the caller falls
    back to status-code semantics.
    """
    try:
        document = json.loads(body)
    except (TypeError, ValueError, UnicodeDecodeError):
        return None
    if not isinstance(document, dict):
        return None
    error = document.get("error")
    if not isinstance(error, dict):
        return None
    code = error.get("code") or error.get("type")
    return code if isinstance(code, str) and code else None


class OpenAICompatibleModelAdapter:
    """Call a tenant-owned OpenAI-compatible chat-completions endpoint.

    The adapter is standard-library-only and works with the Orchestra inference
    engine or another tenant gateway. It never contacts the Orchestra control
    plane and never includes the bearer token in an exception.
    """

    def __init__(
        self,
        base_url: str,
        *,
        api_key: Optional[str] = None,
        endpoint_path: str = "/v1/chat/completions",
        timeout_seconds: float = 30.0,
        max_response_bytes: int = 4 * 1024 * 1024,
        headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        parsed = urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("base_url must be an absolute http(s) URL")
        if parsed.username or parsed.password:
            raise ValueError("base_url must not contain credentials")
        if parsed.query or parsed.fragment:
            raise ValueError("base_url must not contain a query or fragment")
        if not endpoint_path.startswith("/"):
            raise ValueError("endpoint_path must begin with /")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if type(max_response_bytes) is not int or max_response_bytes < 1:
            raise ValueError("max_response_bytes must be a positive integer")
        self.endpoint = base_url.rstrip("/") + endpoint_path
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.max_response_bytes = max_response_bytes
        self.headers = dict(headers or {})

    @staticmethod
    def _payload(request: ModelInvocationRequest) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "model": request.model.model_name,
            "messages": [dict(message) for message in request.messages],
        }
        if request.model.temperature is not None:
            payload["temperature"] = request.model.temperature
        if request.model.max_output_tokens is not None:
            payload["max_tokens"] = request.model.max_output_tokens
        if request.tools:
            payload["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": tool.operation,
                        "description": tool.name,
                        "parameters": dict(tool.input_schema),
                    },
                }
                for tool in request.tools
            ]
        if request.output_schema is not None:
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "orchestra_runtime_output",
                    "strict": True,
                    "schema": dict(request.output_schema),
                },
            }
        return payload

    @staticmethod
    def _retryable_status(status: int) -> bool:
        return status in {408, 409, 425, 429} or status >= 500

    @staticmethod
    def _retry_after_seconds(
        headers: Mapping[str, str],
        *,
        now: Optional[datetime] = None,
    ) -> Optional[float]:
        value = headers.get("Retry-After") or headers.get("retry-after")
        if not isinstance(value, str) or not value.strip():
            return None
        candidate = value.strip()
        if candidate.isdigit():
            seconds = float(candidate)
        else:
            try:
                retry_at = parsedate_to_datetime(candidate)
            except (TypeError, ValueError, OverflowError):
                return None
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            current = now or datetime.now(timezone.utc)
            seconds = max(0.0, (retry_at - current).total_seconds())
        # The runtime policy applies its much smaller retry budget. Preserve
        # oversized delays as a bounded value so they fail closed instead of
        # becoming an immediate retry.
        return min(seconds, 86_400.0)

    @staticmethod
    def _parse_response(
        data: bytes,
        *,
        engine_request_id: Optional[str] = None,
        usage_record_id: Optional[str] = None,
    ) -> ModelInvocationResponse:
        document = _strict_json_loads(data, "model_response_invalid_json")
        if not isinstance(document, dict):
            raise ModelAdapterError("model_response_invalid")
        choices = document.get("choices")
        if not isinstance(choices, list) or len(choices) != 1:
            raise ModelAdapterError("model_response_invalid_choices")
        choice = choices[0]
        if not isinstance(choice, dict) or not isinstance(choice.get("message"), dict):
            raise ModelAdapterError("model_response_invalid_message")
        message = choice["message"]
        content = message.get("content")
        if content is not None and not isinstance(content, (str, dict, list)):
            raise ModelAdapterError("model_response_invalid_content")

        raw_calls = message.get("tool_calls", [])
        if not isinstance(raw_calls, list) or len(raw_calls) > 32:
            raise ModelAdapterError("model_response_invalid_tool_calls")
        calls = []
        for item in raw_calls:
            if not isinstance(item, dict) or not isinstance(item.get("function"), dict):
                raise ModelAdapterError("model_response_invalid_tool_call")
            function = item["function"]
            call_id = item.get("id")
            name = function.get("name")
            arguments_raw = function.get("arguments")
            if (
                not isinstance(call_id, str)
                or not call_id
                or not isinstance(name, str)
                or not name
                or not isinstance(arguments_raw, str)
            ):
                raise ModelAdapterError("model_response_invalid_tool_call")
            arguments = _strict_json_loads(
                arguments_raw, "model_tool_arguments_invalid_json"
            )
            if not isinstance(arguments, dict):
                raise ModelAdapterError("model_tool_arguments_not_object")
            calls.append(ModelToolCall(call_id=call_id, name=name, arguments=arguments))
        if content is None and not calls:
            raise ModelAdapterError("model_response_empty")
        finish_reason = choice.get("finish_reason")
        provider_model = document.get("model")
        return ModelInvocationResponse(
            content=content,
            tool_calls=tuple(calls),
            finish_reason=(finish_reason if isinstance(finish_reason, str) else None),
            provider_model=(
                provider_model if isinstance(provider_model, str) else None
            ),
            usage=_parse_usage(document.get("usage")),
            engine_request_id=engine_request_id,
            usage_record_id=usage_record_id,
        )

    def _invoke_sync(self, request: ModelInvocationRequest) -> ModelInvocationResponse:
        body = json.dumps(
            self._payload(request),
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        headers = {
            **{
                key: value
                for key, value in self.headers.items()
                if key.lower() not in _RESERVED_REQUEST_HEADERS
            },
            "content-type": "application/json",
            "accept": "application/json",
            "x-orchestra-runtime-request-id": request.runtime_request_id,
            "x-orchestra-model-invocation-id": request.model_invocation_id,
            "x-orchestra-model-attempt-id": request.model_attempt_id,
            **_w3c_trace_headers(),
        }
        if self.api_key:
            headers["authorization"] = "Bearer %s" % self.api_key
        http_request = urllib.request.Request(
            self.endpoint,
            data=body,
            headers=headers,
            method="POST",
        )
        engine_request_id: Optional[str] = None
        usage_record_id: Optional[str] = None
        request_failure: Optional[ModelAdapterError] = None
        try:
            with urllib.request.urlopen(
                http_request, timeout=self.timeout_seconds
            ) as response:
                engine_request_id, usage_record_id = _response_identities(
                    getattr(response, "headers", None), request.runtime_request_id
                )
                data = response.read(self.max_response_bytes + 1)
        except urllib.error.HTTPError as exc:
            # Read the OpenAI-shaped ``error`` object when there is one: the
            # status code alone cannot distinguish "the upstream hiccuped"
            # from "this request is invalid and will stay invalid". Bounded by
            # max_response_bytes like any other body, and never allowed to
            # raise — a failure to parse the failure just falls through to
            # status-code semantics.
            code: Optional[str] = None
            engine_request_id, usage_record_id = _response_identities(
                exc.headers, request.runtime_request_id
            )
            try:
                code = _error_code(exc.read(self.max_response_bytes))
            except Exception:  # noqa: BLE001 — never mask the original HTTPError
                code = None

            retryable = self._retryable_status(exc.code)
            if code in _NON_RETRYABLE_ERROR_CODES:
                retryable = False

            # ``code`` stays ``model_http_<status>`` — callers already branch on
            # it and renaming it would break them. The provider's own code is
            # additive, on ``provider_code``.
            request_failure = ModelAdapterError(
                "model_http_%s" % exc.code,
                "Model gateway returned HTTP %s%s"
                % (exc.code, " (%s)" % code if code else ""),
                retryable=retryable,
                retry_after_seconds=(
                    self._retry_after_seconds(exc.headers) if retryable else None
                ),
                provider_code=code,
                engine_request_id=engine_request_id,
                usage_record_id=usage_record_id,
            )
        except (
            urllib.error.URLError,
            TimeoutError,
            OSError,
            http.client.HTTPException,
        ):
            request_failure = ModelAdapterError(
                "model_transport_failed",
                retryable=True,
                engine_request_id=engine_request_id,
                usage_record_id=usage_record_id,
            )
        # Raise outside the source exception handler: HTTPError owns its body
        # stream and IncompleteRead owns ``partial`` bytes. Neither may remain
        # reachable through ModelAdapterError.__cause__ or __context__.
        if request_failure is not None:
            raise request_failure
        if len(data) > self.max_response_bytes:
            raise ModelAdapterError(
                "model_response_too_large",
                engine_request_id=engine_request_id,
                usage_record_id=usage_record_id,
            )
        try:
            return self._parse_response(
                data,
                engine_request_id=engine_request_id,
                usage_record_id=usage_record_id,
            )
        except ModelAdapterError as exc:
            # Parsing and semantic response validation happen after a priced
            # engine request. Preserve only the two allowlisted server IDs so
            # the runtime's failure evidence remains joinable to that ledger
            # record without copying response headers wholesale.
            if exc.engine_request_id is not None or exc.usage_record_id is not None:
                response_failure = exc
            else:
                response_failure = ModelAdapterError(
                    exc.code,
                    str(exc),
                    retryable=exc.retryable,
                    retry_after_seconds=exc.retry_after_seconds,
                    provider_code=exc.provider_code,
                    engine_request_id=engine_request_id,
                    usage_record_id=usage_record_id,
                )
        # The parser exception can retain the full response document. Raise the
        # allowlisted replacement only after leaving that exception handler.
        raise response_failure

    async def invoke(self, request: ModelInvocationRequest) -> ModelInvocationResponse:
        return await asyncio.to_thread(self._invoke_sync, request)


__all__ = ["OpenAICompatibleModelAdapter"]
