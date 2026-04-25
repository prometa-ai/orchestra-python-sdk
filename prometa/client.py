"""Prometa client — OTLP/JSON HTTP shipper.

Buffers spans and flushes them to the configured `/api/v2/otlp/v1/traces`
endpoint. No proto deps; pure-Python json + urllib.
"""

from __future__ import annotations

import atexit
import json
import os
import threading
import time
import urllib.request
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional


# Kept in sync with prometa.__version__ — surfaced as the OTLP
# instrumentation-scope version so the platform can group spans by SDK
# release for compatibility tracking.
_SCOPE_VERSION = "0.2.2"


def _now_unix_nano() -> int:
    return time.time_ns()


def _new_id(length: int = 16) -> str:
    return uuid.uuid4().hex[:length]


@dataclass
class _Span:
    name: str
    kind: str  # "workflow" | "agent" | "tool" | "task"
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    start_ns: int
    end_ns: int = 0
    status: str = "ok"
    attributes: Dict[str, Any] = field(default_factory=dict)


def _attr_kv(key: str, value: Any) -> Dict[str, Any]:
    """Convert a Python value to OTLP/JSON AnyValue shape."""
    if isinstance(value, bool):
        return {"key": key, "value": {"boolValue": value}}
    if isinstance(value, int):
        return {"key": key, "value": {"intValue": str(value)}}
    if isinstance(value, float):
        return {"key": key, "value": {"doubleValue": value}}
    return {"key": key, "value": {"stringValue": str(value)}}


class Prometa:
    """Singleton-style client. Construct once at app start.

    All decorators read the most recently constructed instance.
    """

    _current: Optional["Prometa"] = None

    def __init__(
        self,
        endpoint: str,
        api_key: Optional[str] = None,
        *,
        solution_id: Optional[str] = None,
        agent_name: str = "prometa-agent",
        agent_id: Optional[str] = None,
        stage: str = "development",
        flush_interval_seconds: float = 2.0,
        timeout_seconds: float = 5.0,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.api_key = api_key or os.environ.get("PROMETA_API_KEY")
        self.solution_id = solution_id
        self.agent_name = agent_name
        self.agent_id = agent_id or _new_id()
        self.stage = stage
        self._flush_interval = flush_interval_seconds
        self._timeout = timeout_seconds

        self._buffer: List[_Span] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._thread.start()
        atexit.register(self.flush)

        Prometa._current = self

    # ------------------------------------------------------------------
    # Decorator entry points (also exported as module-level helpers)
    # ------------------------------------------------------------------

    def workflow(self, name: Optional[str] = None) -> Callable:
        return self._decorator("workflow", name)

    def agent(self, name: Optional[str] = None) -> Callable:
        return self._decorator("agent", name)

    def tool(self, name: Optional[str] = None) -> Callable:
        return self._decorator("tool", name)

    def task(self, name: Optional[str] = None) -> Callable:
        return self._decorator("task", name)

    # ------------------------------------------------------------------
    # Internal: decorator factory & manual span context
    # ------------------------------------------------------------------

    def _decorator(self, kind: str, name: Optional[str]) -> Callable:
        def wrap(fn: Callable) -> Callable:
            span_name = name or fn.__name__
            import asyncio
            import functools

            if asyncio.iscoroutinefunction(fn):

                @functools.wraps(fn)
                async def aw(*args, **kwargs):
                    with self._span(kind, span_name) as span:
                        try:
                            return await fn(*args, **kwargs)
                        except Exception as e:
                            span.status = "error"
                            span.attributes["error.message"] = str(e)
                            raise

                return aw

            @functools.wraps(fn)
            def sw(*args, **kwargs):
                with self._span(kind, span_name) as span:
                    try:
                        return fn(*args, **kwargs)
                    except Exception as e:
                        span.status = "error"
                        span.attributes["error.message"] = str(e)
                        raise

            return sw

        return wrap

    @contextmanager
    def _span(self, kind: str, name: str) -> Iterator[_Span]:
        from . import _context  # local to avoid circular at import time

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
                "prometa.solution_id": self.solution_id or "",
                "prometa.stage": self.stage,
                "gen_ai.agent.name": self.agent_name,
                "gen_ai.agent.id": self.agent_id,
            },
        )
        token = _context.push(span)
        try:
            yield span
        finally:
            _context.pop(token)
            span.end_ns = _now_unix_nano()
            with self._lock:
                self._buffer.append(span)

    # ------------------------------------------------------------------
    # Flush
    # ------------------------------------------------------------------

    def _flush_loop(self) -> None:
        while not self._stop.wait(self._flush_interval):
            try:
                self.flush()
            except Exception:
                # Telemetry must never crash the host process.
                pass

    def flush(self) -> int:
        with self._lock:
            spans = list(self._buffer)
            self._buffer.clear()
        if not spans:
            return 0

        payload = self._build_otlp_payload(spans)
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.endpoint,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                **({"x-api-key": self.api_key} if self.api_key else {}),
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                resp.read()
        except Exception:
            # Re-buffer on failure with cap so we don't grow unboundedly.
            with self._lock:
                if len(self._buffer) < 10_000:
                    self._buffer.extend(spans[: 10_000 - len(self._buffer)])
        return len(spans)

    def shutdown(self) -> None:
        self._stop.set()
        self._thread.join(timeout=1)
        self.flush()

    def _build_otlp_payload(self, spans: List[_Span]) -> Dict[str, Any]:
        return {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            _attr_kv("service.name", self.agent_name),
                            _attr_kv("gen_ai.agent.name", self.agent_name),
                            _attr_kv("gen_ai.agent.id", self.agent_id),
                            _attr_kv("prometa.solution.name", self.solution_id or ""),
                            _attr_kv("prometa.stage", self.stage),
                        ]
                    },
                    "scopeSpans": [
                        {
                            "scope": {"name": "prometa-sdk", "version": _SCOPE_VERSION},
                            "spans": [
                                {
                                    "traceId": s.trace_id,
                                    "spanId": s.span_id,
                                    **(
                                        {"parentSpanId": s.parent_span_id}
                                        if s.parent_span_id
                                        else {}
                                    ),
                                    "name": s.name,
                                    "startTimeUnixNano": str(s.start_ns),
                                    "endTimeUnixNano": str(s.end_ns or s.start_ns),
                                    "status": {
                                        "code": 2 if s.status == "error" else 1
                                    },
                                    "attributes": [
                                        _attr_kv(k, v) for k, v in s.attributes.items()
                                    ],
                                }
                                for s in spans
                            ],
                        }
                    ],
                }
            ]
        }
