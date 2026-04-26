"""Google Gemini client auto-instrumentation.

Targets the modern `google-genai` SDK (``from google import genai``),
which exposes:

- ``client.models.generate_content`` (sync, blocking)
- ``client.models.generate_content_stream`` (sync, generator)
- ``client.aio.models.generate_content`` (async coroutine)
- ``client.aio.models.generate_content_stream`` (async generator)

Each call emits a Prometa span carrying:

- ``gen_ai.system`` = ``google``
- ``gen_ai.request.model`` and request params
- ``gen_ai.usage.input_tokens`` / ``gen_ai.usage.output_tokens``
- ``gen_ai.prompt`` (truncated JSON of contents)
- ``gen_ai.completion`` (truncated assistant reply)

Token usage comes off ``response.usage_metadata`` for non-streaming
calls; for streaming, the *final* chunk carries ``usage_metadata`` —
we capture it as it passes through.

Usage::

    from prometa import Prometa
    from prometa.integrations import google as prometa_google

    Prometa(endpoint=..., agent_name="my-agent")
    prometa_google.install()
"""

from __future__ import annotations

import functools
import inspect
from typing import Any, Optional

from ..client import _Span
from . import _llm_common as _c


_INSTALLED = False
SYSTEM = "google"


# ---------------------------------------------------------------------------
# Request / response extraction
# ---------------------------------------------------------------------------


def _request_attrs(args: tuple, kwargs: dict) -> dict:
    out: dict = {
        "gen_ai.system": SYSTEM,
        "gen_ai.framework": SYSTEM,
        "gen_ai.operation.name": "generate_content",
    }
    model = kwargs.get("model")
    if isinstance(model, str):
        out["gen_ai.request.model"] = model
    contents = kwargs.get("contents")
    if contents is not None:
        out["gen_ai.prompt"] = _c.truncate(_c.safe_json(contents))
        # Pre-extract the latest user-role text. Gemini accepts both a
        # bare string (single-turn convenience) and a list of Content
        # objects with role+parts; extract_last_user_text handles both.
        # See openai.py for the rationale on why we stamp this as a
        # separate attribute.
        user_text = _c.extract_last_user_text(contents)
        if user_text:
            out["gen_ai.prompt.user"] = _c.truncate(user_text)
    config = kwargs.get("config") or kwargs.get("generation_config")
    if config is not None:
        # config can be a dict or a pydantic model — extract a few common
        # fields without coupling to its exact type.
        for src, dst in (
            ("temperature", "gen_ai.request.temperature"),
            ("top_p", "gen_ai.request.top_p"),
            ("max_output_tokens", "gen_ai.request.max_tokens"),
        ):
            val = (
                config.get(src)
                if isinstance(config, dict)
                else getattr(config, src, None)
            )
            if val is not None:
                out[dst] = val
    return out


def _apply_response_attrs(span: _Span, response: Any) -> None:
    usage = getattr(response, "usage_metadata", None)
    if usage is not None:
        in_tok = getattr(usage, "prompt_token_count", None) or (
            usage.get("prompt_token_count") if isinstance(usage, dict) else None
        )
        out_tok = getattr(usage, "candidates_token_count", None) or (
            usage.get("candidates_token_count") if isinstance(usage, dict) else None
        )
        if in_tok is not None:
            span.attributes["gen_ai.usage.input_tokens"] = int(in_tok)
        if out_tok is not None:
            span.attributes["gen_ai.usage.output_tokens"] = int(out_tok)
    rmodel = getattr(response, "model_version", None)
    if rmodel:
        span.attributes["gen_ai.response.model"] = str(rmodel)
    text = _extract_text(response)
    if text:
        span.attributes["gen_ai.completion"] = _c.truncate(text)
    finishes = _extract_finish_reasons(response)
    if finishes:
        span.attributes["gen_ai.response.finish_reasons"] = ",".join(finishes)


def _extract_text(response: Any) -> str:
    # The SDK exposes a ``.text`` accessor that concatenates all text
    # parts across candidates — use it when present.
    text = getattr(response, "text", None)
    if isinstance(text, str) and text:
        return text
    # Fallback: walk candidates → content → parts manually.
    parts: list[str] = []
    for cand in getattr(response, "candidates", None) or []:
        content = getattr(cand, "content", None)
        for part in getattr(content, "parts", None) or []:
            t = getattr(part, "text", None)
            if isinstance(t, str):
                parts.append(t)
    return "".join(parts)


def _extract_finish_reasons(response: Any) -> list[str]:
    out: list[str] = []
    for cand in getattr(response, "candidates", None) or []:
        reason = getattr(cand, "finish_reason", None)
        if reason is None:
            continue
        # Enum-valued in the SDK; coerce to string for the attribute.
        out.append(getattr(reason, "name", str(reason)))
    return out


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------


class _StreamAccumulator:
    """Aggregates state across Gemini stream chunks. Each chunk has the
    same shape as a non-streaming response (usage_metadata + candidates),
    but only the final chunk carries the populated usage values."""

    def __init__(self) -> None:
        self.text_parts: list[str] = []
        self.input_tokens: Optional[int] = None
        self.output_tokens: Optional[int] = None
        self.response_model: Optional[str] = None
        self.finish_reasons: list[str] = []

    def absorb(self, chunk: Any) -> None:
        text = getattr(chunk, "text", None)
        if isinstance(text, str) and text:
            self.text_parts.append(text)
        else:
            # Fall back to walking parts manually — useful when ``.text``
            # accessor raises (e.g., when a candidate has no text part).
            for cand in getattr(chunk, "candidates", None) or []:
                content = getattr(cand, "content", None)
                for part in getattr(content, "parts", None) or []:
                    t = getattr(part, "text", None)
                    if isinstance(t, str):
                        self.text_parts.append(t)
        usage = getattr(chunk, "usage_metadata", None)
        if usage is not None:
            in_tok = getattr(usage, "prompt_token_count", None) or (
                usage.get("prompt_token_count") if isinstance(usage, dict) else None
            )
            out_tok = getattr(usage, "candidates_token_count", None) or (
                usage.get("candidates_token_count") if isinstance(usage, dict) else None
            )
            if in_tok is not None:
                self.input_tokens = int(in_tok)
            if out_tok is not None:
                self.output_tokens = int(out_tok)
        rmodel = getattr(chunk, "model_version", None)
        if rmodel and not self.response_model:
            self.response_model = str(rmodel)
        for cand in getattr(chunk, "candidates", None) or []:
            reason = getattr(cand, "finish_reason", None)
            if reason is None:
                continue
            name = getattr(reason, "name", str(reason))
            if name not in self.finish_reasons:
                self.finish_reasons.append(name)

    def write_to(self, span: _Span) -> None:
        if self.input_tokens is not None:
            span.attributes["gen_ai.usage.input_tokens"] = self.input_tokens
        if self.output_tokens is not None:
            span.attributes["gen_ai.usage.output_tokens"] = self.output_tokens
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
    return (
        f"google.{operation}:{model}"
        if isinstance(model, str)
        else f"google.{operation}"
    )


def _wrap_generate_content(cls: type, *, is_async: bool) -> None:
    if "generate_content" not in cls.__dict__:
        return
    original = cls.__dict__["generate_content"]
    if getattr(original, "__prometa_wrapped__", False):
        return

    if is_async:

        @functools.wraps(original)
        async def wrapper(self, *args, **kwargs):
            client = _c._client()
            if client is None:
                return await original(self, *args, **kwargs)
            attrs = _request_attrs(args, kwargs)
            span_name = _make_span_name("generate_content", kwargs)
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

    else:

        @functools.wraps(original)
        def wrapper(self, *args, **kwargs):
            client = _c._client()
            if client is None:
                return original(self, *args, **kwargs)
            attrs = _request_attrs(args, kwargs)
            span_name = _make_span_name("generate_content", kwargs)
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
    setattr(cls, "generate_content", wrapper)


def _wrap_generate_content_stream(cls: type, *, is_async: bool) -> None:
    if "generate_content_stream" not in cls.__dict__:
        return
    original = cls.__dict__["generate_content_stream"]
    if getattr(original, "__prometa_wrapped__", False):
        return

    if is_async:

        @functools.wraps(original)
        async def wrapper(self, *args, **kwargs):
            # The async streaming entry point may itself be a coroutine
            # that returns an async iterator, OR it may be a regular
            # function returning the iterator directly. Handle both.
            client = _c._client()
            if client is None:
                result = original(self, *args, **kwargs)
                return await result if inspect.iscoroutine(result) else result
            attrs = _request_attrs(args, kwargs)
            attrs["gen_ai.request.stream"] = True
            span_name = _make_span_name("generate_content_stream", kwargs)
            span = _c.open_manual_span("agent", span_name, attrs)
            if span is None:
                result = original(self, *args, **kwargs)
                return await result if inspect.iscoroutine(result) else result
            try:
                result = original(self, *args, **kwargs)
                stream = await result if inspect.iscoroutine(result) else result
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

    else:

        @functools.wraps(original)
        def wrapper(self, *args, **kwargs):
            client = _c._client()
            if client is None:
                return original(self, *args, **kwargs)
            attrs = _request_attrs(args, kwargs)
            attrs["gen_ai.request.stream"] = True
            span_name = _make_span_name("generate_content_stream", kwargs)
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

    wrapper.__prometa_wrapped__ = True  # type: ignore[attr-defined]
    setattr(cls, "generate_content_stream", wrapper)


# ---------------------------------------------------------------------------
# install / uninstall
# ---------------------------------------------------------------------------


def install() -> bool:
    """Patch the google-genai client's models methods.

    Returns True if patching applied, False if the google-genai library
    isn't importable.
    """
    global _INSTALLED
    if _INSTALLED:
        return True

    patched_any = False

    # Modern SDK: ``google-genai`` (pip install google-genai)
    try:
        from google.genai import models as _models  # type: ignore

        Models = getattr(_models, "Models", None)
        AsyncModels = getattr(_models, "AsyncModels", None)
        if Models is not None:
            _wrap_generate_content(Models, is_async=False)
            _wrap_generate_content_stream(Models, is_async=False)
            patched_any = True
        if AsyncModels is not None:
            _wrap_generate_content(AsyncModels, is_async=True)
            _wrap_generate_content_stream(AsyncModels, is_async=True)
            patched_any = True
    except Exception:
        pass

    # Legacy SDK: ``google-generativeai`` exposes ``GenerativeModel``
    # with ``generate_content`` / ``generate_content_async``. We support
    # both for users still on the older library.
    try:
        from google.generativeai.generative_models import (  # type: ignore
            GenerativeModel,
        )

        _wrap_generate_content(GenerativeModel, is_async=False)
        # Older SDK uses ``generate_content_async`` instead of an async
        # variant on a separate class. Wrap it under the same machinery.
        if "generate_content_async" in GenerativeModel.__dict__:
            original = GenerativeModel.__dict__["generate_content_async"]
            if not getattr(original, "__prometa_wrapped__", False):

                @functools.wraps(original)
                async def wrapper(self, *args, **kwargs):
                    client = _c._client()
                    if client is None:
                        return await original(self, *args, **kwargs)
                    attrs = _request_attrs(args, kwargs)
                    span_name = _make_span_name("generate_content_async", kwargs)
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
                setattr(GenerativeModel, "generate_content_async", wrapper)
        patched_any = True
    except Exception:
        pass

    _INSTALLED = patched_any
    return patched_any


def uninstall() -> None:  # pragma: no cover - test convenience only
    return None
