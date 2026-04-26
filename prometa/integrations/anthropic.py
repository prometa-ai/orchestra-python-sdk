"""Anthropic client auto-instrumentation.

Patches ``anthropic.resources.messages.Messages.create`` and the async /
streaming variants so every direct call to the Anthropic Python client
emits a Prometa span carrying:

- ``gen_ai.system`` = ``anthropic``
- ``gen_ai.request.model`` and request params
- ``gen_ai.usage.input_tokens`` / ``gen_ai.usage.output_tokens``
- ``gen_ai.prompt`` (truncated JSON of input messages, plus system prompt)
- ``gen_ai.completion`` (truncated assistant reply)

Streaming covers both APIs:

- ``client.messages.create(stream=True)`` — returns a raw stream of
  events; we wrap it with our proxy iterator.
- ``client.messages.stream(...)`` — returns a context manager whose
  ``__enter__`` yields a richer ``MessageStream``; we wrap the returned
  manager so the span finalizes on ``__exit__``.

Token counts are aggregated from the ``message_start`` / ``message_delta``
events that Anthropic emits before / after the content stream.

Usage::

    from prometa import Prometa
    from prometa.integrations import anthropic as prometa_anthropic

    Prometa(endpoint=..., agent_name="my-agent")
    prometa_anthropic.install()
"""

from __future__ import annotations

import functools
from typing import Any, Optional

from ..client import _Span
from . import _llm_common as _c


_INSTALLED = False
SYSTEM = "anthropic"


# ---------------------------------------------------------------------------
# Request / response extraction
# ---------------------------------------------------------------------------


def _request_attrs(kwargs: dict) -> dict:
    out: dict = {
        "gen_ai.system": SYSTEM,
        "gen_ai.framework": SYSTEM,
        "gen_ai.operation.name": "messages",
    }
    model = kwargs.get("model")
    if isinstance(model, str):
        out["gen_ai.request.model"] = model
    for src, dst in (
        ("temperature", "gen_ai.request.temperature"),
        ("top_p", "gen_ai.request.top_p"),
        ("max_tokens", "gen_ai.request.max_tokens"),
        ("stream", "gen_ai.request.stream"),
    ):
        if src in kwargs and kwargs[src] is not None:
            out[dst] = kwargs[src]
    # Anthropic separates the system prompt from the message list; bundle
    # both into a single JSON blob so the conversation panel sees the
    # full context the model received.
    prompt_payload: dict = {}
    system = kwargs.get("system")
    if system is not None:
        prompt_payload["system"] = system
    messages = kwargs.get("messages")
    if messages is not None:
        prompt_payload["messages"] = messages
    if prompt_payload:
        out["gen_ai.prompt"] = _c.truncate(_c.safe_json(prompt_payload))
    # Pre-extract the latest user-role text from the messages array
    # (system prompts go in a separate kwarg, never in `messages`, so
    # only the messages list matters for "what did the user say"). See
    # the openai integration's matching block for full rationale.
    if messages:
        user_text = _c.extract_last_user_text(messages)
        if user_text:
            out["gen_ai.prompt.user"] = _c.truncate(user_text)
    return out


def _apply_response_attrs(span: _Span, response: Any) -> None:
    usage = getattr(response, "usage", None)
    if usage is not None:
        in_tok = getattr(usage, "input_tokens", None)
        out_tok = getattr(usage, "output_tokens", None)
        if in_tok is not None:
            span.attributes["gen_ai.usage.input_tokens"] = int(in_tok)
        if out_tok is not None:
            span.attributes["gen_ai.usage.output_tokens"] = int(out_tok)
    rid = getattr(response, "id", None)
    if rid:
        span.attributes["gen_ai.response.id"] = str(rid)
    rmodel = getattr(response, "model", None)
    if rmodel:
        span.attributes["gen_ai.response.model"] = str(rmodel)
    stop = getattr(response, "stop_reason", None)
    if stop:
        span.attributes["gen_ai.response.finish_reasons"] = str(stop)
    text = _extract_text(response)
    if text:
        span.attributes["gen_ai.completion"] = _c.truncate(text)


def _extract_text(response: Any) -> str:
    """Concatenate all text blocks from a Message response."""
    content = getattr(response, "content", None)
    if not content:
        return ""
    parts: list[str] = []
    for block in content:
        # SDK models expose ``.type == "text"`` and ``.text``; raw dict
        # responses use the same field names.
        text = getattr(block, "text", None) or (
            block.get("text") if isinstance(block, dict) else None
        )
        if isinstance(text, str):
            parts.append(text)
    return "".join(parts)


# ---------------------------------------------------------------------------
# Streaming chunk handling
# ---------------------------------------------------------------------------


class _StreamAccumulator:
    """Aggregates state across Anthropic stream events.

    Anthropic's event model (works for both raw ``stream=True`` events
    and the higher-level ``messages.stream`` accumulator):

    - ``message_start`` — carries the initial ``message`` with
      ``usage.input_tokens`` already populated.
    - ``content_block_delta`` — incremental text chunks.
    - ``message_delta`` — final event with ``usage.output_tokens``.
    """

    def __init__(self) -> None:
        self.text_parts: list[str] = []
        self.input_tokens: Optional[int] = None
        self.output_tokens: Optional[int] = None
        self.response_id: Optional[str] = None
        self.response_model: Optional[str] = None
        self.stop_reason: Optional[str] = None

    def absorb(self, event: Any) -> None:
        evt_type = getattr(event, "type", None) or (
            event.get("type") if isinstance(event, dict) else None
        )
        if evt_type == "message_start":
            msg = getattr(event, "message", None) or (
                event.get("message") if isinstance(event, dict) else None
            )
            if msg is not None:
                rid = getattr(msg, "id", None) or (
                    msg.get("id") if isinstance(msg, dict) else None
                )
                if rid:
                    self.response_id = str(rid)
                rmodel = getattr(msg, "model", None) or (
                    msg.get("model") if isinstance(msg, dict) else None
                )
                if rmodel:
                    self.response_model = str(rmodel)
                u = getattr(msg, "usage", None) or (
                    msg.get("usage") if isinstance(msg, dict) else None
                )
                if u is not None:
                    in_tok = getattr(u, "input_tokens", None) or (
                        u.get("input_tokens") if isinstance(u, dict) else None
                    )
                    if in_tok is not None:
                        self.input_tokens = int(in_tok)
        elif evt_type == "content_block_delta":
            delta = getattr(event, "delta", None) or (
                event.get("delta") if isinstance(event, dict) else None
            )
            if delta is not None:
                text = getattr(delta, "text", None) or (
                    delta.get("text") if isinstance(delta, dict) else None
                )
                if isinstance(text, str):
                    self.text_parts.append(text)
        elif evt_type == "message_delta":
            u = getattr(event, "usage", None) or (
                event.get("usage") if isinstance(event, dict) else None
            )
            if u is not None:
                out_tok = getattr(u, "output_tokens", None) or (
                    u.get("output_tokens") if isinstance(u, dict) else None
                )
                if out_tok is not None:
                    self.output_tokens = int(out_tok)
            delta = getattr(event, "delta", None) or (
                event.get("delta") if isinstance(event, dict) else None
            )
            if delta is not None:
                stop = getattr(delta, "stop_reason", None) or (
                    delta.get("stop_reason") if isinstance(delta, dict) else None
                )
                if stop:
                    self.stop_reason = str(stop)

    def write_to(self, span: _Span) -> None:
        if self.input_tokens is not None:
            span.attributes["gen_ai.usage.input_tokens"] = self.input_tokens
        if self.output_tokens is not None:
            span.attributes["gen_ai.usage.output_tokens"] = self.output_tokens
        if self.response_id:
            span.attributes["gen_ai.response.id"] = self.response_id
        if self.response_model:
            span.attributes["gen_ai.response.model"] = self.response_model
        if self.stop_reason:
            span.attributes["gen_ai.response.finish_reasons"] = self.stop_reason
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
        f"anthropic.{operation}:{model}"
        if isinstance(model, str)
        else f"anthropic.{operation}"
    )


def _wrap_sync_create(cls: type) -> None:
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
        span_name = _make_span_name("messages", kwargs)
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


def _wrap_async_create(cls: type) -> None:
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
        span_name = _make_span_name("messages", kwargs)
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


def _wrap_sync_stream(cls: type) -> None:
    """Wrap ``Messages.stream`` (the context-manager API).

    Returns the original stream manager wrapped in our sync proxy. We
    rely on the proxy's ``__enter__`` / ``__exit__`` forwarding to drive
    the underlying manager, while our ``__next__`` taps each event for
    metadata extraction.
    """
    if "stream" not in cls.__dict__:
        return
    original = cls.__dict__["stream"]
    if getattr(original, "__prometa_wrapped__", False):
        return

    @functools.wraps(original)
    def wrapper(self, *args, **kwargs):
        client = _c._client()
        if client is None:
            return original(self, *args, **kwargs)
        attrs = _request_attrs(kwargs)
        attrs["gen_ai.request.stream"] = True
        span_name = _make_span_name("messages.stream", kwargs)
        span = _c.open_manual_span("agent", span_name, attrs)
        if span is None:
            return original(self, *args, **kwargs)
        try:
            stream_mgr = original(self, *args, **kwargs)
        except Exception as e:
            _c.finalize_span(span, error=e)
            raise
        acc = _StreamAccumulator()
        return _c._StreamProxy(
            stream_mgr,
            span,
            on_chunk=lambda c, _s: acc.absorb(c),
            on_finalize=lambda s: acc.write_to(s),
        )

    wrapper.__prometa_wrapped__ = True  # type: ignore[attr-defined]
    setattr(cls, "stream", wrapper)


def _wrap_async_stream(cls: type) -> None:
    if "stream" not in cls.__dict__:
        return
    original = cls.__dict__["stream"]
    if getattr(original, "__prometa_wrapped__", False):
        return

    @functools.wraps(original)
    def wrapper(self, *args, **kwargs):
        # NOTE: ``AsyncMessages.stream`` is not itself a coroutine — it
        # synchronously returns an async context manager. Mirror that.
        client = _c._client()
        if client is None:
            return original(self, *args, **kwargs)
        attrs = _request_attrs(kwargs)
        attrs["gen_ai.request.stream"] = True
        span_name = _make_span_name("messages.stream", kwargs)
        span = _c.open_manual_span("agent", span_name, attrs)
        if span is None:
            return original(self, *args, **kwargs)
        try:
            stream_mgr = original(self, *args, **kwargs)
        except Exception as e:
            _c.finalize_span(span, error=e)
            raise
        acc = _StreamAccumulator()
        return _c._AsyncStreamProxy(
            stream_mgr,
            span,
            on_chunk=lambda c, _s: acc.absorb(c),
            on_finalize=lambda s: acc.write_to(s),
        )

    wrapper.__prometa_wrapped__ = True  # type: ignore[attr-defined]
    setattr(cls, "stream", wrapper)


# ---------------------------------------------------------------------------
# install / uninstall
# ---------------------------------------------------------------------------


def install() -> bool:
    """Patch the anthropic client's messages methods.

    Returns True if patching applied, False if the anthropic library
    isn't importable.
    """
    global _INSTALLED
    if _INSTALLED:
        return True

    try:
        import anthropic  # type: ignore  # noqa: F401
    except Exception:  # pragma: no cover - anthropic not installed
        return False

    patched_any = False

    try:
        from anthropic.resources.messages import (  # type: ignore
            Messages,
            AsyncMessages,
        )

        _wrap_sync_create(Messages)
        _wrap_async_create(AsyncMessages)
        _wrap_sync_stream(Messages)
        _wrap_async_stream(AsyncMessages)
        patched_any = True
    except Exception:
        # Newer anthropic SDK reorganized resources under
        # `anthropic.resources.messages.messages`. Try that as a fallback.
        try:
            from anthropic.resources.messages.messages import (  # type: ignore
                Messages,
                AsyncMessages,
            )

            _wrap_sync_create(Messages)
            _wrap_async_create(AsyncMessages)
            _wrap_sync_stream(Messages)
            _wrap_async_stream(AsyncMessages)
            patched_any = True
        except Exception:
            pass

    _INSTALLED = patched_any
    return patched_any


def uninstall() -> None:  # pragma: no cover - test convenience only
    return None
