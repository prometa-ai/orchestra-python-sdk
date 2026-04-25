"""OpenAI client auto-instrumentation.

Patches ``openai.resources.chat.completions.Completions.create`` and the
async / streaming variants so every direct call to the OpenAI Python
client emits a Prometa span carrying:

- ``gen_ai.system`` = ``openai``
- ``gen_ai.request.model`` and request params
- ``gen_ai.usage.input_tokens`` / ``gen_ai.usage.output_tokens``
- ``gen_ai.prompt`` (truncated JSON of input messages)
- ``gen_ai.completion`` (truncated assistant reply)

Streaming is supported transparently: the wrapper returns a proxy
iterator that finalizes the span when the stream is exhausted, with
usage attributes populated from the final chunk (when the caller passes
``stream_options={"include_usage": True}``; otherwise the span carries
prompt/completion text but no token counts).

Usage::

    from prometa import Prometa
    from prometa.integrations import openai as prometa_openai

    Prometa(endpoint=..., agent_name="my-agent")
    prometa_openai.install()

    # ...your normal openai code emits Prometa spans...
"""

from __future__ import annotations

import functools
from typing import Any, Optional

from ..client import _Span
from . import _llm_common as _c


_INSTALLED = False
SYSTEM = "openai"


# ---------------------------------------------------------------------------
# Request / response extraction
# ---------------------------------------------------------------------------


def _request_attrs(kwargs: dict) -> dict:
    """Pull request metadata off the kwargs dict passed to ``.create()``."""
    out: dict = {
        "gen_ai.system": SYSTEM,
        "gen_ai.framework": SYSTEM,
        "gen_ai.operation.name": "chat",
    }
    model = kwargs.get("model")
    if isinstance(model, str):
        out["gen_ai.request.model"] = model
    for src, dst in (
        ("temperature", "gen_ai.request.temperature"),
        ("top_p", "gen_ai.request.top_p"),
        ("max_tokens", "gen_ai.request.max_tokens"),
        ("max_completion_tokens", "gen_ai.request.max_tokens"),
        ("stream", "gen_ai.request.stream"),
    ):
        if src in kwargs and kwargs[src] is not None:
            out[dst] = kwargs[src]
    messages = kwargs.get("messages")
    if messages is not None:
        out["gen_ai.prompt"] = _c.truncate(_c.safe_json(messages))
    elif "input" in kwargs:  # responses API
        out["gen_ai.prompt"] = _c.truncate(_c.safe_json(kwargs["input"]))
        out["gen_ai.operation.name"] = "responses"
    return out


def _apply_response_attrs(span: _Span, response: Any) -> None:
    """Populate span attributes from a non-streaming chat response."""
    usage = getattr(response, "usage", None)
    if usage is not None:
        # New SDK shape uses ``.prompt_tokens`` / ``.completion_tokens``;
        # responses API uses ``.input_tokens`` / ``.output_tokens``.
        in_tok = (
            getattr(usage, "prompt_tokens", None)
            or getattr(usage, "input_tokens", None)
        )
        out_tok = (
            getattr(usage, "completion_tokens", None)
            or getattr(usage, "output_tokens", None)
        )
        if in_tok is not None:
            span.attributes["gen_ai.usage.input_tokens"] = int(in_tok)
        if out_tok is not None:
            span.attributes["gen_ai.usage.output_tokens"] = int(out_tok)
    response_id = getattr(response, "id", None)
    if response_id:
        span.attributes["gen_ai.response.id"] = str(response_id)
    response_model = getattr(response, "model", None)
    if response_model:
        span.attributes["gen_ai.response.model"] = str(response_model)
    text = _extract_text(response)
    if text:
        span.attributes["gen_ai.completion"] = _c.truncate(text)
    finishes = _extract_finish_reasons(response)
    if finishes:
        span.attributes["gen_ai.response.finish_reasons"] = ",".join(finishes)


def _extract_text(response: Any) -> str:
    """Best-effort extraction of the assistant's text reply across both
    Chat Completions and the newer Responses API."""
    # Chat Completions: response.choices[i].message.content
    choices = getattr(response, "choices", None)
    if choices:
        parts: list[str] = []
        for ch in choices:
            msg = getattr(ch, "message", None)
            content = getattr(msg, "content", None) if msg is not None else None
            if isinstance(content, str):
                parts.append(content)
            elif isinstance(content, list):
                for item in content:
                    text = getattr(item, "text", None) or (
                        item.get("text") if isinstance(item, dict) else None
                    )
                    if isinstance(text, str):
                        parts.append(text)
        if parts:
            return "\n".join(parts)
    # Responses API: response.output_text (convenience accessor)
    text = getattr(response, "output_text", None)
    if isinstance(text, str):
        return text
    return ""


def _extract_finish_reasons(response: Any) -> list[str]:
    out: list[str] = []
    for ch in getattr(response, "choices", None) or []:
        reason = getattr(ch, "finish_reason", None)
        if isinstance(reason, str):
            out.append(reason)
    return out


# ---------------------------------------------------------------------------
# Streaming chunk handling
# ---------------------------------------------------------------------------


class _StreamAccumulator:
    """Tracks state across streamed chunks: assembled text + final usage.

    OpenAI's stream emits one usage-bearing chunk at the end *if the
    caller asked for it* via ``stream_options={"include_usage": True}``.
    We capture whatever shows up; if usage is absent, the span still
    carries prompt + accumulated completion text but no token counts.
    """

    def __init__(self) -> None:
        self.text_parts: list[str] = []
        self.input_tokens: Optional[int] = None
        self.output_tokens: Optional[int] = None
        self.response_id: Optional[str] = None
        self.response_model: Optional[str] = None
        self.finish_reasons: list[str] = []

    def absorb(self, chunk: Any) -> None:
        if not self.response_id:
            cid = getattr(chunk, "id", None)
            if cid:
                self.response_id = str(cid)
        if not self.response_model:
            cm = getattr(chunk, "model", None)
            if cm:
                self.response_model = str(cm)
        # Chat Completions stream chunk shape
        for ch in getattr(chunk, "choices", None) or []:
            delta = getattr(ch, "delta", None)
            content = getattr(delta, "content", None) if delta is not None else None
            if isinstance(content, str):
                self.text_parts.append(content)
            reason = getattr(ch, "finish_reason", None)
            if isinstance(reason, str) and reason not in self.finish_reasons:
                self.finish_reasons.append(reason)
        # Usage may appear on the final chunk
        usage = getattr(chunk, "usage", None)
        if usage is not None:
            in_tok = (
                getattr(usage, "prompt_tokens", None)
                or getattr(usage, "input_tokens", None)
            )
            out_tok = (
                getattr(usage, "completion_tokens", None)
                or getattr(usage, "output_tokens", None)
            )
            if in_tok is not None:
                self.input_tokens = int(in_tok)
            if out_tok is not None:
                self.output_tokens = int(out_tok)
        # Responses API streaming uses event-typed chunks; the
        # ``response.completed`` event carries the final aggregated
        # response object.
        evt_type = getattr(chunk, "type", None)
        if isinstance(evt_type, str) and evt_type.endswith(".completed"):
            final = getattr(chunk, "response", None)
            if final is not None:
                u = getattr(final, "usage", None)
                if u is not None:
                    in_tok = getattr(u, "input_tokens", None) or getattr(
                        u, "prompt_tokens", None
                    )
                    out_tok = getattr(u, "output_tokens", None) or getattr(
                        u, "completion_tokens", None
                    )
                    if in_tok is not None:
                        self.input_tokens = int(in_tok)
                    if out_tok is not None:
                        self.output_tokens = int(out_tok)
                final_text = getattr(final, "output_text", None)
                if isinstance(final_text, str) and final_text:
                    self.text_parts = [final_text]

    def write_to(self, span: _Span) -> None:
        if self.input_tokens is not None:
            span.attributes["gen_ai.usage.input_tokens"] = self.input_tokens
        if self.output_tokens is not None:
            span.attributes["gen_ai.usage.output_tokens"] = self.output_tokens
        if self.response_id:
            span.attributes["gen_ai.response.id"] = self.response_id
        if self.response_model:
            span.attributes["gen_ai.response.model"] = self.response_model
        if self.finish_reasons:
            span.attributes["gen_ai.response.finish_reasons"] = ",".join(
                self.finish_reasons
            )
        if self.text_parts:
            span.attributes["gen_ai.completion"] = _c.truncate(
                "".join(self.text_parts)
            )


# ---------------------------------------------------------------------------
# Method wrapping
# ---------------------------------------------------------------------------


def _make_span_name(operation: str, kwargs: dict) -> str:
    model = kwargs.get("model")
    return f"openai.{operation}:{model}" if isinstance(model, str) else f"openai.{operation}"


def _wrap_sync_create(cls: type, operation: str) -> None:
    if "create" not in cls.__dict__:
        return
    original = cls.__dict__["create"]
    if getattr(original, "__prometa_wrapped__", False):
        return

    @functools.wraps(original)
    def wrapper(self, *args, **kwargs):
        client = _c._client()
        if client is None:
            return original(self, *args, **kwargs)
        attrs = _request_attrs(kwargs)
        span_name = _make_span_name(operation, kwargs)
        is_stream = bool(kwargs.get("stream"))
        if is_stream:
            span = _c.open_manual_span("agent", span_name, attrs)
            if span is None:
                return original(self, *args, **kwargs)
            try:
                stream = original(self, *args, **kwargs)
            except Exception as e:
                _c.finalize_span(span, error=e)
                raise
            acc = _StreamAccumulator()
            return _c._StreamProxy(
                stream,
                span,
                on_chunk=lambda c, _s: acc.absorb(c),
                on_finalize=lambda s: acc.write_to(s),
            )
        # Non-streaming: standard context-manager span
        with client._span("agent", span_name) as span:
            span.attributes.update(attrs)
            try:
                response = original(self, *args, **kwargs)
            except Exception as e:
                span.status = "error"
                span.attributes["error.message"] = str(e)
                raise
            try:
                _apply_response_attrs(span, response)
            except Exception:
                pass
            return response

    wrapper.__prometa_wrapped__ = True  # type: ignore[attr-defined]
    setattr(cls, "create", wrapper)


def _wrap_async_create(cls: type, operation: str) -> None:
    if "create" not in cls.__dict__:
        return
    original = cls.__dict__["create"]
    if getattr(original, "__prometa_wrapped__", False):
        return

    @functools.wraps(original)
    async def wrapper(self, *args, **kwargs):
        client = _c._client()
        if client is None:
            return await original(self, *args, **kwargs)
        attrs = _request_attrs(kwargs)
        span_name = _make_span_name(operation, kwargs)
        is_stream = bool(kwargs.get("stream"))
        if is_stream:
            span = _c.open_manual_span("agent", span_name, attrs)
            if span is None:
                return await original(self, *args, **kwargs)
            try:
                stream = await original(self, *args, **kwargs)
            except Exception as e:
                _c.finalize_span(span, error=e)
                raise
            acc = _StreamAccumulator()
            return _c._AsyncStreamProxy(
                stream,
                span,
                on_chunk=lambda c, _s: acc.absorb(c),
                on_finalize=lambda s: acc.write_to(s),
            )
        with client._span("agent", span_name) as span:
            span.attributes.update(attrs)
            try:
                response = await original(self, *args, **kwargs)
            except Exception as e:
                span.status = "error"
                span.attributes["error.message"] = str(e)
                raise
            try:
                _apply_response_attrs(span, response)
            except Exception:
                pass
            return response

    wrapper.__prometa_wrapped__ = True  # type: ignore[attr-defined]
    setattr(cls, "create", wrapper)


# ---------------------------------------------------------------------------
# install / uninstall
# ---------------------------------------------------------------------------


def install() -> bool:
    """Patch the openai client's chat + responses ``.create`` methods.

    Returns True if patching applied, False if the openai library isn't
    importable.
    """
    global _INSTALLED
    if _INSTALLED:
        return True

    try:
        import openai  # type: ignore  # noqa: F401
    except Exception:  # pragma: no cover - openai not installed
        return False

    patched_any = False

    # Chat Completions: openai.resources.chat.completions
    try:
        from openai.resources.chat.completions import (  # type: ignore
            Completions,
            AsyncCompletions,
        )

        _wrap_sync_create(Completions, "chat")
        _wrap_async_create(AsyncCompletions, "chat")
        patched_any = True
    except Exception:
        pass

    # Responses API (openai>=1.40): openai.resources.responses
    try:
        from openai.resources.responses import (  # type: ignore
            Responses,
            AsyncResponses,
        )

        _wrap_sync_create(Responses, "responses")
        _wrap_async_create(AsyncResponses, "responses")
        patched_any = True
    except Exception:
        pass

    # Legacy Completions (text-davinci-003 era — still present in v1)
    try:
        from openai.resources.completions import (  # type: ignore
            Completions as LegacyCompletions,
            AsyncCompletions as AsyncLegacyCompletions,
        )

        _wrap_sync_create(LegacyCompletions, "completion")
        _wrap_async_create(AsyncLegacyCompletions, "completion")
    except Exception:
        pass

    _INSTALLED = patched_any
    return patched_any


def uninstall() -> None:  # pragma: no cover - test convenience only
    """No-op placeholder. Restart the process to remove instrumentation."""
    return None
