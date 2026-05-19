"""OpenLLMetry-backed auto-instrumentation bridge.

OpenLLMetry's instrumentors are standard OpenTelemetry instrumentations.
This module wires them into Prometa without replacing the SDK's existing
pure-Python shipper:

1. install the requested OpenLLMetry instrumentors when their optional
   packages are available;
2. convert finished OpenTelemetry spans into Prometa's in-memory span
   model; and
3. fall back to Prometa-native integrations for targets we already
   support when OpenLLMetry is not installed.

The integration is intentionally opt-in. Existing users who call
``prometa.integrations.openai.install()`` keep the current behavior and
span contract.
"""

from __future__ import annotations

import importlib
import json
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional

from .. import _context
from ..client import Prometa, _Span, _agent_identity_attrs
from . import _llm_common as _llm


DEFAULT_TARGETS = (
    "openai",
    "anthropic",
    "langchain",
    "chromadb",
    "pinecone",
)


@dataclass(frozen=True)
class _InstrumentorSpec:
    module: str
    class_name: str
    fallback: Optional[str] = None
    constructor_kwargs: dict[str, Any] = field(default_factory=dict)


_SPECS: dict[str, _InstrumentorSpec] = {
    "openai": _InstrumentorSpec(
        "opentelemetry.instrumentation.openai",
        "OpenAIInstrumentor",
        fallback="openai",
        constructor_kwargs={"use_legacy_attributes": True},
    ),
    "anthropic": _InstrumentorSpec(
        "opentelemetry.instrumentation.anthropic",
        "AnthropicInstrumentor",
        fallback="anthropic",
        constructor_kwargs={"use_legacy_attributes": True},
    ),
    "langchain": _InstrumentorSpec(
        "opentelemetry.instrumentation.langchain",
        "LangchainInstrumentor",
        fallback="langchain",
        constructor_kwargs={"use_legacy_attributes": True},
    ),
    "langgraph": _InstrumentorSpec(
        "opentelemetry.instrumentation.langchain",
        "LangchainInstrumentor",
        fallback="langgraph",
        constructor_kwargs={"use_legacy_attributes": True},
    ),
    "chromadb": _InstrumentorSpec(
        "opentelemetry.instrumentation.chromadb",
        "ChromaInstrumentor",
        fallback="chroma",
    ),
    "chroma": _InstrumentorSpec(
        "opentelemetry.instrumentation.chromadb",
        "ChromaInstrumentor",
        fallback="chroma",
    ),
    "pinecone": _InstrumentorSpec(
        "opentelemetry.instrumentation.pinecone",
        "PineconeInstrumentor",
        fallback="pinecone",
    ),
    # Extra OpenLLMetry targets with no Prometa-native fallback yet.
    # They are installed when callers have the corresponding packages.
    "bedrock": _InstrumentorSpec(
        "opentelemetry.instrumentation.bedrock",
        "BedrockInstrumentor",
    ),
    "cohere": _InstrumentorSpec(
        "opentelemetry.instrumentation.cohere",
        "CohereInstrumentor",
    ),
    "haystack": _InstrumentorSpec(
        "opentelemetry.instrumentation.haystack",
        "HaystackInstrumentor",
    ),
    "llamaindex": _InstrumentorSpec(
        "opentelemetry.instrumentation.llamaindex",
        "LlamaIndexInstrumentor",
    ),
}


@dataclass
class _MappedSpan:
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    inherited_attributes: dict[str, Any] = field(default_factory=dict)


_PROVIDER_LOCK = threading.Lock()
_BRIDGED_PROVIDERS: set[int] = set()
_INSTRUMENTORS: dict[str, Any] = {}


def install(
    targets: Optional[Iterable[str]] = None,
    *,
    fallback: bool = True,
    tracer_provider: Any = None,
    set_global_tracer_provider: bool = False,
    use_legacy_attributes: bool = True,
) -> dict[str, bool]:
    """Install OpenLLMetry instrumentors and bridge their spans to Prometa.

    Parameters
    ----------
    targets:
        Instrumentation names to attempt. Defaults to the targets that
        overlap with Prometa's existing native integrations:
        ``openai``, ``anthropic``, ``langchain``, ``chromadb``, and
        ``pinecone``.
    fallback:
        When ``True`` (default), use the existing Prometa-native
        integration if the matching OpenLLMetry instrumentor package is
        missing or fails to install.
    tracer_provider:
        Optional OpenTelemetry tracer provider. If omitted, this module
        creates a private provider and passes it directly to each
        OpenLLMetry instrumentor.
    set_global_tracer_provider:
        Set the created provider as OpenTelemetry's global provider.
        Leave this ``False`` unless the host app wants Prometa to own
        OpenTelemetry setup.
    use_legacy_attributes:
        Ask OpenLLMetry to keep prompt/completion payloads in span
        attributes when supported. Prometa's trace UI currently reads
        those attributes for the Conversation panel.
    """

    selected = _normalize_targets(targets)
    provider = _ensure_bridge_provider(
        tracer_provider=tracer_provider,
        set_global=set_global_tracer_provider,
    )

    results: dict[str, bool] = {}
    for target in selected:
        spec = _SPECS[target]
        installed = False
        if provider is not None:
            installed = _install_openllmetry_target(
                target,
                spec,
                provider,
                use_legacy_attributes=use_legacy_attributes,
            )
        if not installed and fallback:
            installed = _install_native_fallback(spec.fallback)
        results[target] = installed
    return results


def uninstall() -> None:  # pragma: no cover - mostly a test/dev convenience
    """Uninstrument OpenLLMetry instrumentors installed by this module."""

    for instrumentor in list(_INSTRUMENTORS.values()):
        try:
            instrumentor.uninstrument()
        except Exception:
            pass
    _INSTRUMENTORS.clear()


def _normalize_targets(targets: Optional[Iterable[str]]) -> tuple[str, ...]:
    raw = DEFAULT_TARGETS if targets is None else tuple(targets)
    out: list[str] = []
    for target in raw:
        key = str(target).strip().lower().replace("-", "")
        if key == "chroma":
            key = "chromadb"
        if key not in _SPECS:
            raise ValueError(f"unknown OpenLLMetry instrumentation target: {target}")
        if key not in out:
            out.append(key)
    return tuple(out)


def _ensure_bridge_provider(
    *,
    tracer_provider: Any = None,
    set_global: bool = False,
) -> Any:
    try:
        from opentelemetry import trace as trace_api  # type: ignore
        from opentelemetry.sdk.resources import Resource  # type: ignore
        from opentelemetry.sdk.trace import TracerProvider  # type: ignore
    except Exception:
        return None

    provider = tracer_provider
    if provider is None:
        provider = TracerProvider(resource=Resource.create(_resource_attributes()))
        if set_global:
            try:
                trace_api.set_tracer_provider(provider)
            except Exception:
                pass

    if not hasattr(provider, "add_span_processor"):
        return None

    with _PROVIDER_LOCK:
        key = id(provider)
        if key not in _BRIDGED_PROVIDERS:
            provider.add_span_processor(_PrometaSpanProcessor())
            _BRIDGED_PROVIDERS.add(key)
    return provider


def _resource_attributes() -> dict[str, str]:
    client = Prometa._current
    if client is None:
        return {"service.name": "prometa-agent", "telemetry.sdk.name": "prometa-sdk"}
    return {
        "service.name": client.agent_name,
        **_agent_identity_attrs(client.agent_name, client.agent_id),
        "prometa.solution.name": client.solution_id or "",
        "prometa.stage": client.stage,
        "telemetry.sdk.name": "prometa-sdk",
    }


def _install_openllmetry_target(
    target: str,
    spec: _InstrumentorSpec,
    tracer_provider: Any,
    *,
    use_legacy_attributes: bool,
) -> bool:
    try:
        module = importlib.import_module(spec.module)
        instrumentor_cls = getattr(module, spec.class_name)
    except Exception:
        return False

    kwargs = dict(spec.constructor_kwargs)
    if "use_legacy_attributes" in kwargs:
        kwargs["use_legacy_attributes"] = use_legacy_attributes

    try:
        instrumentor = instrumentor_cls(**kwargs)
    except TypeError:
        try:
            instrumentor = instrumentor_cls()
        except Exception:
            return False
    except Exception:
        return False

    try:
        instrumentor.instrument(tracer_provider=tracer_provider)
    except Exception:
        return False

    _INSTRUMENTORS[target] = instrumentor
    return True


def _install_native_fallback(name: Optional[str]) -> bool:
    if not name:
        return False
    try:
        if name == "openai":
            from . import openai

            return openai.install()
        if name == "anthropic":
            from . import anthropic

            return anthropic.install()
        if name == "langchain":
            from . import langchain
            from . import langgraph

            installed = langchain.install()
            try:
                langgraph.install()
            except Exception:
                pass
            return installed
        if name == "langgraph":
            from . import langgraph
            from . import langchain

            try:
                langchain.install()
            except Exception:
                pass
            return langgraph.install()
        if name == "chroma":
            from . import vector

            return vector.install_chroma()
        if name == "pinecone":
            from . import vector

            return vector.install_pinecone()
    except Exception:
        return False
    return False


class _PrometaSpanProcessor:
    """OpenTelemetry SpanProcessor that buffers Prometa-compatible spans."""

    def __init__(
        self,
        get_client: Optional[Callable[[], Optional[Prometa]]] = None,
    ) -> None:
        self._get_client = get_client or (lambda: Prometa._current)
        self._mapped: dict[tuple[str, str], _MappedSpan] = {}
        self._lock = threading.Lock()

    def on_start(self, span: Any, parent_context: Any = None) -> None:
        ctx = _span_context(span)
        if ctx is None:
            return

        trace_id = _trace_id(ctx)
        span_id = _span_id(ctx)
        parent_span_id: Optional[str] = None
        mapped_trace_id = trace_id
        inherited: dict[str, Any] = {}
        parent_was_mapped = False

        parent_ctx = _parent_context(span, parent_context)
        if parent_ctx is not None and _context_is_valid(parent_ctx):
            parent_key = (_trace_id(parent_ctx), _span_id(parent_ctx))
            with self._lock:
                parent_mapped = self._mapped.get(parent_key)
            if parent_mapped is not None:
                parent_was_mapped = True
                mapped_trace_id = parent_mapped.trace_id
                parent_span_id = parent_mapped.span_id
                inherited.update(parent_mapped.inherited_attributes)
            else:
                parent_span_id = _span_id(parent_ctx)

        current = _context.current_span()
        if current is not None and not parent_was_mapped:
            mapped_trace_id = current.trace_id
            parent_span_id = current.span_id
            inherited.update(_inheritable_attrs(current.attributes))

        with self._lock:
            self._mapped[(trace_id, span_id)] = _MappedSpan(
                trace_id=mapped_trace_id,
                span_id=span_id,
                parent_span_id=parent_span_id,
                inherited_attributes=inherited,
            )

    def on_end(self, span: Any) -> None:
        ctx = _span_context(span)
        if ctx is None:
            return

        key = (_trace_id(ctx), _span_id(ctx))
        with self._lock:
            mapped = self._mapped.pop(key, None)
        if mapped is None:
            mapped = _map_without_start(span)

        client = self._get_client()
        if client is None:
            return

        attrs = _normalize_attributes(_span_attributes(span))
        inherited = dict(mapped.inherited_attributes)
        inherited.update(
            {
                key: value
                for key, value in attrs.items()
                if key in ("gen_ai.conversation.id", "prometa.customer_id")
            }
        )

        kind = attrs.get("prometa.kind") or _infer_prometa_kind(
            getattr(span, "name", ""), attrs
        )
        base_attrs = {
            "prometa.kind": kind,
            "prometa.solution_id": client.solution_id or "",
            "prometa.stage": client.stage,
            **_agent_identity_attrs(client.agent_name, client.agent_id),
            "prometa.instrumentation.provider": "openllmetry",
        }
        if client.customer_id:
            base_attrs["prometa.customer_id"] = client.customer_id

        base_attrs.update(inherited)
        base_attrs.update(attrs)
        base_attrs["prometa.kind"] = kind

        prometa_span = _Span(
            name=str(getattr(span, "name", "") or "openllmetry.span"),
            kind=str(kind),
            trace_id=mapped.trace_id,
            span_id=mapped.span_id,
            parent_span_id=mapped.parent_span_id,
            start_ns=int(getattr(span, "start_time", 0) or 0),
            end_ns=int(getattr(span, "end_time", 0) or 0),
            status="error" if _span_is_error(span) else "ok",
            attributes=base_attrs,
        )
        if prometa_span.start_ns <= 0:
            prometa_span.start_ns = prometa_span.end_ns

        with client._lock:
            client._buffer.append(prometa_span)

    def shutdown(self) -> None:
        with self._lock:
            self._mapped.clear()

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        return True


def _span_context(span: Any) -> Any:
    ctx = getattr(span, "context", None)
    if ctx is None and hasattr(span, "get_span_context"):
        try:
            ctx = span.get_span_context()
        except Exception:
            ctx = None
    if ctx is None or not _context_is_valid(ctx):
        return None
    return ctx


def _parent_context(span: Any, parent_context: Any = None) -> Any:
    parent = getattr(span, "parent", None)
    if parent is not None:
        return parent
    if parent_context is None:
        return None
    try:
        from opentelemetry import trace as trace_api  # type: ignore

        parent_span = trace_api.get_current_span(parent_context)
        return parent_span.get_span_context()
    except Exception:
        return None


def _context_is_valid(ctx: Any) -> bool:
    if ctx is None:
        return False
    valid = getattr(ctx, "is_valid", True)
    if callable(valid):
        try:
            valid = valid()
        except Exception:
            valid = True
    return bool(valid) and bool(getattr(ctx, "trace_id", 0)) and bool(
        getattr(ctx, "span_id", 0)
    )


def _trace_id(ctx: Any) -> str:
    return f"{int(getattr(ctx, 'trace_id')):032x}"


def _span_id(ctx: Any) -> str:
    return f"{int(getattr(ctx, 'span_id')):016x}"


def _map_without_start(span: Any) -> _MappedSpan:
    ctx = _span_context(span)
    if ctx is None:
        return _MappedSpan("", "", None)
    parent = _parent_context(span)
    return _MappedSpan(
        trace_id=_trace_id(ctx),
        span_id=_span_id(ctx),
        parent_span_id=_span_id(parent)
        if parent is not None and _context_is_valid(parent)
        else None,
    )


def _span_attributes(span: Any) -> dict[str, Any]:
    attrs: dict[str, Any] = {}
    resource = getattr(span, "resource", None)
    resource_attrs = getattr(resource, "attributes", None)
    if resource_attrs:
        attrs.update(dict(resource_attrs))
    span_attrs = getattr(span, "attributes", None)
    if span_attrs:
        attrs.update(dict(span_attrs))
    return attrs


def _normalize_attributes(attrs: dict[str, Any]) -> dict[str, Any]:
    out = {str(key): _coerce_attribute(value) for key, value in attrs.items()}

    provider = (
        out.get("gen_ai.system")
        or out.get("gen_ai.provider.name")
        or out.get("llm.system")
    )
    if provider and "gen_ai.system" not in out:
        out["gen_ai.system"] = str(provider)

    finish_reasons = out.get("gen_ai.response.finish_reasons")
    if isinstance(finish_reasons, (list, tuple)):
        out["gen_ai.response.finish_reasons"] = ",".join(
            str(item) for item in finish_reasons
        )

    input_messages = _first_present(
        out,
        (
            "gen_ai.input.messages",
            "llm.prompts",
            "traceloop.entity.input",
            "gen_ai.task.input",
        ),
    )
    if input_messages is not None:
        out.setdefault("gen_ai.prompt", _text_attr(input_messages))
        user_text = _llm.extract_last_user_text(_maybe_json(input_messages))
        if user_text:
            out.setdefault("gen_ai.prompt.user", _llm.truncate(user_text))

    output_messages = _first_present(
        out,
        (
            "gen_ai.output.messages",
            "llm.completions",
            "traceloop.entity.output",
            "gen_ai.task.output",
        ),
    )
    if output_messages is not None:
        completion = _extract_assistant_text(_maybe_json(output_messages))
        out.setdefault("gen_ai.completion", _llm.truncate(completion))

    return out


def _coerce_attribute(value: Any) -> Any:
    if isinstance(value, tuple):
        return list(value)
    return value


def _first_present(attrs: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = attrs.get(key)
        if value is not None and value != "":
            return value
    return None


def _text_attr(value: Any) -> str:
    if isinstance(value, str):
        return _llm.truncate(value)
    return _llm.truncate(_llm.safe_json(value))


def _maybe_json(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:
        return value


def _extract_assistant_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        if "messages" in value:
            return _extract_assistant_text(value.get("messages"))
        if "message" in value:
            return _extract_assistant_text(value.get("message"))
        if "output_text" in value and isinstance(value["output_text"], str):
            return value["output_text"]
        text = _flatten_content(value.get("content"))
        if text:
            return text
        text = _flatten_content(value.get("parts"))
        if text:
            return text
    if isinstance(value, list):
        for item in reversed(value):
            role = _attr_or_key(item, "role")
            if role in ("assistant", "model"):
                text = _flatten_content(_attr_or_key(item, "content"))
                if text:
                    return text
                text = _flatten_content(_attr_or_key(item, "parts"))
                if text:
                    return text
            text = _extract_assistant_text(item)
            if text:
                return text
    return _text_attr(value)


def _flatten_content(content: Any) -> Optional[str]:
    if content is None:
        return None
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
                continue
            text = _attr_or_key(part, "text")
            if isinstance(text, str):
                parts.append(text)
        return "\n".join(parts) if parts else None
    return None


def _attr_or_key(obj: Any, name: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def _infer_prometa_kind(name: str, attrs: dict[str, Any]) -> str:
    span_kind = str(attrs.get("traceloop.span.kind", "")).lower()
    if span_kind in {"workflow", "agent", "tool", "task"}:
        return span_kind

    lower_name = str(name).lower()
    if (
        "vector.db.vendor" in attrs
        or "db.system" in attrs
        or lower_name.startswith(("pinecone.", "chroma.", "weaviate.", "qdrant."))
    ):
        return "retrieval"
    if (
        "gen_ai.tool.name" in attrs
        or "gen_ai.tool.call.id" in attrs
        or "gen_ai.tool.call.arguments" in attrs
    ):
        return "tool"
    if any(key.startswith("gen_ai.") for key in attrs):
        return "agent"
    return "task"


def _span_is_error(span: Any) -> bool:
    status = getattr(span, "status", None)
    code = getattr(status, "status_code", None)
    if code is None:
        return False
    name = str(getattr(code, "name", code)).lower()
    return "error" in name or name.endswith(".error")


def _inheritable_attrs(attrs: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in attrs.items()
        if key in ("gen_ai.conversation.id", "prometa.customer_id")
    }


__all__ = ["install", "uninstall", "DEFAULT_TARGETS"]
