"""OpenAI-compatible tenant model-plane adapter for the runtime kernel."""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Mapping, Optional
from urllib.parse import urlparse

from .kernel import (
    ModelAdapterError,
    ModelInvocationRequest,
    ModelInvocationResponse,
    ModelToolCall,
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
    except (UnicodeDecodeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ModelAdapterError(code) from exc


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
    def _parse_response(data: bytes) -> ModelInvocationResponse:
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
        )

    def _invoke_sync(self, request: ModelInvocationRequest) -> ModelInvocationResponse:
        body = json.dumps(
            self._payload(request),
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        headers = {
            **self.headers,
            "content-type": "application/json",
            "accept": "application/json",
            "x-orchestra-runtime-request-id": request.request_id,
        }
        if self.api_key:
            headers["authorization"] = "Bearer %s" % self.api_key
        http_request = urllib.request.Request(
            self.endpoint,
            data=body,
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                http_request, timeout=self.timeout_seconds
            ) as response:
                data = response.read(self.max_response_bytes + 1)
        except urllib.error.HTTPError as exc:
            retryable = self._retryable_status(exc.code)
            raise ModelAdapterError(
                "model_http_%s" % exc.code,
                "Model gateway returned HTTP %s" % exc.code,
                retryable=retryable,
                retry_after_seconds=(
                    self._retry_after_seconds(exc.headers) if retryable else None
                ),
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise ModelAdapterError("model_transport_failed", retryable=True) from exc
        if len(data) > self.max_response_bytes:
            raise ModelAdapterError("model_response_too_large")
        return self._parse_response(data)

    async def invoke(self, request: ModelInvocationRequest) -> ModelInvocationResponse:
        return await asyncio.to_thread(self._invoke_sync, request)


__all__ = ["OpenAICompatibleModelAdapter"]
