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
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional

from .intent import inherited_assistant_intent_attrs


# OTLP instrumentation-scope version, surfaced so the platform can group
# spans by SDK release for compatibility tracking. Derived at import
# time from the installed package metadata (i.e. pyproject.toml's
# version field) so it can never drift out of sync with the published
# package — the prior hardcoded mirror was missed by the release
# workflow's sed pass and got out of step on every release.
#
# Source-checkout fallback: when the package isn't pip-installed
# (typical during development / running tests against the source
# tree), `version("prometa-sdk")` raises PackageNotFoundError. Spans
# emitted in that mode get scope.version="0.0.0+source", which is
# informative on the platform side ("this came from a dev checkout,
# not a release").
try:
    from importlib.metadata import version as _pkg_version, PackageNotFoundError
    try:
        _SCOPE_VERSION = _pkg_version("prometa-sdk")
    except PackageNotFoundError:
        _SCOPE_VERSION = "0.0.0+source"
    del _pkg_version, PackageNotFoundError
except ImportError:  # pragma: no cover — Python <3.8 fallback, not really reachable
    _SCOPE_VERSION = "0.0.0+source"


def _now_unix_nano() -> int:
    return time.time_ns()


def _new_id(length: int = 16) -> str:
    return uuid.uuid4().hex[:length]


def _clean_agent_id(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _resolve_agent_id(passed: Optional[str]) -> Optional[str]:
    """Resolve optional agent_id: explicit kwarg > PROMETA_AGENT_ID env > None.

    Agents now mirror Tool registration semantics: customers identify
    an agent by the customer-owned ``(solution_id, agent_name)`` tuple,
    and the platform can auto-register/attach the canonical Agent row
    on first sighting. A supplied ``agent_id`` is still honored for
    advanced users who need to pin an ID, but the SDK no longer invents
    a random per-process fallback.
    """
    return _clean_agent_id(passed) or _clean_agent_id(os.environ.get("PROMETA_AGENT_ID"))


# Sentinel marking "caller did not pass agent_name". We can't just check
# `agent_name == DEFAULT_AGENT_NAME` because a caller may deliberately
# pass the literal "prometa-agent" — in which case we should not warn
# (they made an explicit choice). The sentinel preserves call-site
# intent without changing the public type signature.
_AGENT_NAME_UNSET = object()
DEFAULT_AGENT_NAME = "prometa-agent"


def _resolve_agent_name(passed: Any) -> str:
    """Resolve agent_name: explicit kwarg > PROMETA_AGENT_NAME env > default.

    ``agent_name`` is the customer-owned identity of the instrumented
    app. The platform keys the Agent registry on
    ``(orgId, solutionId, agent_name)``, so two apps in the same solution
    that both fall back to the default collapse into a single Agent row
    in the registry — every read after that fans out across the wrong
    population.

    Resolution precedence:
      1. Explicit kwarg passed to ``Prometa(agent_name=...)``.
      2. ``PROMETA_AGENT_NAME`` environment variable.
      3. The literal ``"prometa-agent"`` fallback, with a ``UserWarning``
         pointing the operator at the env var. The fallback exists so
         that a forgotten ``agent_name`` doesn't crash the process —
         the warning makes the collision risk visible at startup
         instead of surfacing later as "everyone shares one Agent row."
    """
    if passed is not _AGENT_NAME_UNSET:
        cleaned = str(passed).strip()
        if cleaned:
            return cleaned

    env_value = os.environ.get("PROMETA_AGENT_NAME", "").strip()
    if env_value:
        return env_value

    warnings.warn(
        "Prometa(): no agent_name supplied and PROMETA_AGENT_NAME is unset; "
        "falling back to \"prometa-agent\". Apps in the same solution that "
        "share this default collapse into one Agent row in the registry. "
        "Set agent_name=... (or export PROMETA_AGENT_NAME) to disambiguate.",
        UserWarning,
        stacklevel=3,
    )
    return DEFAULT_AGENT_NAME


def _agent_identity_attrs(agent_name: str, agent_id: Optional[str]) -> Dict[str, str]:
    attrs = {"gen_ai.agent.name": agent_name}
    if agent_id:
        attrs["prometa.agent_id"] = agent_id
        # Legacy compatibility: the platform's canonical correlation key is
        # prometa.agent_id, but older consumers may still inspect this OTel key.
        attrs["gen_ai.agent.id"] = agent_id
    return attrs


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
        # Sentinel default so we can distinguish "caller omitted" (warn
        # + fall back) from "caller explicitly passed 'prometa-agent'"
        # (no warn). See _resolve_agent_name for the resolution rules.
        agent_name: Any = _AGENT_NAME_UNSET,
        agent_id: Optional[str] = None,
        stage: str = "development",
        # Correlation-chain identity horizontal — the org's external
        # customer key (their CRM / data-warehouse id for the
        # end-customer this Prometa-instrumented app serves). Stamped
        # on every span's resource attributes when set; the platform's
        # correlation-id resolver validates against
        # `Organization.customerNamespace` at ingest. Leave None for
        # service-to-service or single-tenant deployments where every
        # span trivially belongs to the same customer scope.
        customer_id: Optional[str] = None,
        flush_interval_seconds: float = 2.0,
        timeout_seconds: float = 5.0,
    ) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.api_key = api_key or os.environ.get("PROMETA_API_KEY")
        self.solution_id = solution_id
        self.agent_name = _resolve_agent_name(agent_name)
        self.agent_id: Optional[str] = _resolve_agent_id(agent_id)
        self.stage = stage
        self.customer_id = customer_id
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

    def workflow(
        self, name: Optional[str] = None, *, session_id: Optional[str] = None
    ) -> Callable:
        return self._decorator("workflow", name, session_id=session_id)

    def agent(
        self, name: Optional[str] = None, *, session_id: Optional[str] = None
    ) -> Callable:
        return self._decorator("agent", name, session_id=session_id)

    def tool(
        self, name: Optional[str] = None, *, session_id: Optional[str] = None
    ) -> Callable:
        return self._decorator("tool", name, session_id=session_id)

    def task(
        self, name: Optional[str] = None, *, session_id: Optional[str] = None
    ) -> Callable:
        return self._decorator("task", name, session_id=session_id)

    # ------------------------------------------------------------------
    # Internal: decorator factory & manual span context
    # ------------------------------------------------------------------

    def _decorator(
        self,
        kind: str,
        name: Optional[str],
        *,
        session_id: Optional[str] = None,
    ) -> Callable:
        def wrap(fn: Callable) -> Callable:
            span_name = name or fn.__name__
            import asyncio
            import functools

            if asyncio.iscoroutinefunction(fn):

                @functools.wraps(fn)
                async def aw(*args, **kwargs):
                    with self._span(kind, span_name, session_id=session_id) as span:
                        try:
                            return await fn(*args, **kwargs)
                        except Exception as e:
                            span.status = "error"
                            span.attributes["error.message"] = str(e)
                            raise

                return aw

            @functools.wraps(fn)
            def sw(*args, **kwargs):
                with self._span(kind, span_name, session_id=session_id) as span:
                    try:
                        return fn(*args, **kwargs)
                    except Exception as e:
                        span.status = "error"
                        span.attributes["error.message"] = str(e)
                        raise

            return sw

        return wrap

    @contextmanager
    def _span(
        self,
        kind: str,
        name: str,
        *,
        session_id: Optional[str] = None,
    ) -> Iterator[_Span]:
        from . import _context  # local to avoid circular at import time

        parent = _context.current_span()
        trace_id = parent.trace_id if parent else _new_id(32)
        # Session id resolution order:
        #   1. Explicit session_id passed to the decorator (most specific).
        #   2. Inherit from parent span (set by an outer @workflow or
        #      set_session_id() call on a higher span).
        # If neither, the attribute stays empty and the trace gets no
        # session grouping — which is the documented opt-out.
        inherited_session = ""
        if parent is not None:
            inherited_session = parent.attributes.get(
                "gen_ai.conversation.id", ""
            )
        effective_session = session_id or inherited_session
        # Inherit customer_id from the parent span so a per-span
        # `set_customer_id` override on the workflow root flows into
        # every nested span. The constructor-time `self.customer_id`
        # is the org-wide default; per-span override wins.
        inherited_customer = ""
        if parent is not None:
            inherited_customer = parent.attributes.get(
                "prometa.customer_id", ""
            )
        effective_customer = inherited_customer or (self.customer_id or "")
        inherited_intent: Dict[str, Any] = {}
        if parent is not None:
            inherited_intent = inherited_assistant_intent_attrs(parent.attributes)
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
                **_agent_identity_attrs(self.agent_name, self.agent_id),
                **(
                    {"gen_ai.conversation.id": effective_session}
                    if effective_session
                    else {}
                ),
                **(
                    {"prometa.customer_id": effective_customer}
                    if effective_customer
                    else {}
                ),
                **inherited_intent,
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
                            *[
                                _attr_kv(k, v)
                                for k, v in _agent_identity_attrs(
                                    self.agent_name,
                                    self.agent_id,
                                ).items()
                            ],
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
