"""Shared helpers for LLM-client auto-instrumentation.

Used by :mod:`prometa.integrations.openai`,
:mod:`prometa.integrations.anthropic`, and
:mod:`prometa.integrations.google`.

Each provider integration is responsible for:
- Locating the right method on the right resource class
- Extracting request metadata (model, temperature, etc.)
- Extracting prompt text and response/completion text
- Pulling token usage out of the response (and the final streaming chunk)

This module owns the cross-cutting concerns:
- Manual span lifecycle for streaming responses (since the stream is
  consumed *after* the wrapper returns, the standard ``client._span``
  context manager can't span the iteration)
- Stream proxy classes for sync/async iterators
- Truncated JSON serialization of prompts/completions so we don't blow up
  span size limits with megabyte-sized chat histories
"""

from __future__ import annotations

import json
import time
from typing import Any, AsyncIterator, Iterator, Optional

from .. import _context
from ..client import Prometa, _Span, _new_id, _now_unix_nano

# Cap individual prompt/completion attribute payloads. Real chat histories
# can be tens of KB; the OTLP envelope and ClickHouse string columns both
# tolerate that, but very long values inflate every trace fetch in the UI.
MAX_TEXT_ATTR_BYTES = 8000


def _client() -> Optional[Prometa]:
    return Prometa._current


def truncate(text: str, limit: int = MAX_TEXT_ATTR_BYTES) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 16] + "...[truncated]"


def safe_json(value: Any) -> str:
    """Best-effort JSON dump. Falls back to ``str()`` on non-serializable
    objects so we never raise inside instrumentation."""
    try:
        return json.dumps(value, default=str, ensure_ascii=False)
    except Exception:
        return str(value)


def open_manual_span(kind: str, name: str, base_attrs: dict) -> Optional[_Span]:
    """Create a span without using the contextvar stack.

    Streaming responses are consumed *after* the wrapper returns, so we
    cannot rely on ``client._span()`` (which is a contextmanager that
    closes when its ``with`` block exits). Instead we mint the span here,
    parent it to whatever's active, and hand finalization to the stream
    proxy. The span never appears in ``_context._stack``, which is fine —
    nothing nested will run inside the stream callback.
    """
    client = _client()
    if client is None:
        return None
    parent = _context.current_span()
    trace_id = parent.trace_id if parent else _new_id(32)
    span = _Span(
        name=name,
        kind=kind,
        trace_id=trace_id,
        span_id=_new_id(16),
        parent_span_id=parent.span_id if parent else None,
        start_ns=_now_unix_nano(),
        attributes={
            "prometa.kind": kind,
            "prometa.solution_id": client.solution_id or "",
            "prometa.stage": client.stage,
            "gen_ai.agent.name": client.agent_name,
            "gen_ai.agent.id": client.agent_id,
            **base_attrs,
        },
    )
    return span


def finalize_span(span: _Span, *, error: Optional[BaseException] = None) -> None:
    """End and buffer a manually-opened span. Idempotent."""
    if span.end_ns:
        return
    span.end_ns = _now_unix_nano()
    if error is not None:
        span.status = "error"
        span.attributes.setdefault("error.message", str(error))
    client = _client()
    if client is None:
        return
    with client._lock:
        client._buffer.append(span)


class _StreamProxy:
    """Wraps a sync iterator/iterable; finalizes the span on exhaustion or
    explicit ``.close()`` / ``__exit__``.

    ``on_chunk(chunk, span)`` is called for each chunk as it passes
    through. It should mutate ``span.attributes`` in place.
    ``on_finalize(span)`` is called once the stream ends; useful for any
    cleanup like aggregating accumulated text into a single attribute.

    Async-context propagation: when iteration / context-manager entry
    begins, the LLM span is pushed onto the per-task span stack via
    ``_context.push``. Anything decorated with ``@prometa.tool`` /
    ``@agent`` that runs while the consumer processes chunks will nest
    under the LLM span — so a tool call triggered from inside a streamed
    response gets the right ``parent_span_id`` instead of attaching to
    whatever was active when ``.create(stream=True)`` returned.

    The push is idempotent: ``__iter__`` and ``__enter__`` both attempt
    it but a flag prevents double-push if the user does
    ``with stream:\\n  for x in stream: ...``.
    """

    def __init__(self, inner: Iterator, span: _Span, on_chunk, on_finalize) -> None:
        self._inner = inner
        self._span = span
        self._on_chunk = on_chunk
        self._on_finalize = on_finalize
        self._closed = False
        self._ctx_token = None  # contextvar Token from _context.push, or None

    def _activate_context(self) -> None:
        if self._ctx_token is not None or self._closed:
            return
        try:
            self._ctx_token = _context.push(self._span)
        except Exception:
            self._ctx_token = None

    def _deactivate_context(self) -> None:
        if self._ctx_token is None:
            return
        try:
            _context.pop(self._ctx_token)
        except (LookupError, ValueError):
            # Token was set in a different contextvars context (rare —
            # happens if user iterates the stream from a different task
            # than the one that opened it). Best-effort: drop the token
            # and let the contextvars context die with its task.
            pass
        finally:
            self._ctx_token = None

    def __iter__(self):
        self._activate_context()
        return self

    def __next__(self):
        try:
            chunk = next(self._inner)
        except StopIteration:
            self._finish()
            raise
        except BaseException as e:
            self._finish(error=e)
            raise
        try:
            self._on_chunk(chunk, self._span)
        except Exception:
            pass
        return chunk

    # Many SDK stream objects are also context managers. Forward both
    # patterns so user code that does ``with stream as s:`` still works.
    def __enter__(self):
        inner_enter = getattr(self._inner, "__enter__", None)
        if inner_enter is not None:
            self._inner = inner_enter()
        self._activate_context()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            inner_exit = getattr(self._inner, "__exit__", None)
            if inner_exit is not None:
                inner_exit(exc_type, exc, tb)
        finally:
            self._finish(error=exc if exc_type else None)
        return False

    def close(self) -> None:
        inner_close = getattr(self._inner, "close", None)
        if inner_close is not None:
            try:
                inner_close()
            except Exception:
                pass
        self._finish()

    def __getattr__(self, item):
        # Forward unknown attributes to the wrapped stream (e.g.
        # `text_stream`, `current_message_snapshot` on Anthropic streams).
        return getattr(self._inner, item)

    def _finish(self, error: Optional[BaseException] = None) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._on_finalize(self._span)
        except Exception:
            pass
        # Pop the contextvar BEFORE buffering the span, so any
        # finalization side-effects (logs, etc.) don't see a stale
        # current_span pointing at a span we're about to ship.
        self._deactivate_context()
        finalize_span(self._span, error=error)


class _AsyncStreamProxy:
    """Async counterpart of :class:`_StreamProxy`. See that class's
    docstring for the async-context propagation contract."""

    def __init__(self, inner: AsyncIterator, span: _Span, on_chunk, on_finalize) -> None:
        self._inner = inner
        self._span = span
        self._on_chunk = on_chunk
        self._on_finalize = on_finalize
        self._closed = False
        self._ctx_token = None

    def _activate_context(self) -> None:
        if self._ctx_token is not None or self._closed:
            return
        try:
            self._ctx_token = _context.push(self._span)
        except Exception:
            self._ctx_token = None

    def _deactivate_context(self) -> None:
        if self._ctx_token is None:
            return
        try:
            _context.pop(self._ctx_token)
        except (LookupError, ValueError):
            pass
        finally:
            self._ctx_token = None

    def __aiter__(self):
        self._activate_context()
        return self

    async def __anext__(self):
        try:
            chunk = await self._inner.__anext__()
        except StopAsyncIteration:
            await self._finish()
            raise
        except BaseException as e:
            await self._finish(error=e)
            raise
        try:
            self._on_chunk(chunk, self._span)
        except Exception:
            pass
        return chunk

    async def __aenter__(self):
        inner_enter = getattr(self._inner, "__aenter__", None)
        if inner_enter is not None:
            self._inner = await inner_enter()
        self._activate_context()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            inner_exit = getattr(self._inner, "__aexit__", None)
            if inner_exit is not None:
                await inner_exit(exc_type, exc, tb)
        finally:
            await self._finish(error=exc if exc_type else None)
        return False

    async def aclose(self) -> None:
        inner_close = getattr(self._inner, "aclose", None)
        if inner_close is not None:
            try:
                await inner_close()
            except Exception:
                pass
        await self._finish()

    def __getattr__(self, item):
        return getattr(self._inner, item)

    async def _finish(self, error: Optional[BaseException] = None) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._on_finalize(self._span)
        except Exception:
            pass
        self._deactivate_context()
        finalize_span(self._span, error=error)


# Re-export for convenience so per-provider modules can `from ._llm_common
# import time` without needing a separate import line.
_ = time
