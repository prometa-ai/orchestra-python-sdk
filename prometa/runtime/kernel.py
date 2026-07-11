"""Tenant-owned execution kernel for admitted Orchestra runtime releases.

The kernel is intentionally protocol-first. It owns request validation,
guard/tool admission, bounded model execution, resilience, and evidence. Model,
tool, state, and human-review implementations stay tenant supplied; the
Orchestra control plane is never called from the synchronous request path.
"""

from __future__ import annotations

import asyncio
import json
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    FrozenSet,
    List,
    Mapping,
    Optional,
    Protocol,
    Tuple,
)

from .admission import (
    AdmittedRuntimeRelease,
    CAPABILITY_EVIDENCE_EMIT,
    CAPABILITY_GUARD_EVALUATE,
    CAPABILITY_HUMAN_ESCALATION,
    CAPABILITY_MODEL_INVOKE,
    CAPABILITY_SCHEMA_VALIDATE,
    CAPABILITY_TOOL_BROKER,
    RuntimeGuardrail,
    RuntimeModel,
    RuntimeTool,
)


class RuntimeExecutionError(RuntimeError):
    """Stable fail-closed execution error."""

    def __init__(
        self,
        code: str,
        message: Optional[str] = None,
        *,
        retryable: bool = False,
    ) -> None:
        self.code = code
        self.retryable = retryable
        super().__init__(message or code.replace("_", " "))


class ModelAdapterError(RuntimeExecutionError):
    """Normalized model-plane failure raised by a model adapter."""


def _strict_json_loads(value: str) -> Any:
    def reject_duplicates(pairs):
        result = {}
        for key, child in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = child
        return result

    def reject_constant(constant):
        raise ValueError("non-finite JSON number: %s" % constant)

    return json.loads(
        value,
        object_pairs_hook=reject_duplicates,
        parse_constant=reject_constant,
    )


@dataclass(frozen=True)
class RuntimeEvidenceEvent:
    name: str
    outcome: str
    occurred_at: str
    attributes: Mapping[str, Any]


class EvidenceEmitter(Protocol):
    def emit(self, event: RuntimeEvidenceEvent) -> None:
        """Persist or buffer one evidence event; failures must raise."""


class InMemoryEvidenceEmitter:
    """Thread-safe evidence collector for conformance tests and local hosts."""

    def __init__(self) -> None:
        self._events: List[RuntimeEvidenceEvent] = []
        self._lock = threading.Lock()

    def emit(self, event: RuntimeEvidenceEvent) -> None:
        with self._lock:
            self._events.append(event)

    @property
    def events(self) -> Tuple[RuntimeEvidenceEvent, ...]:
        with self._lock:
            return tuple(self._events)


class PrometaEvidenceEmitter:
    """Emit kernel decisions through an existing ``Prometa`` OTLP client."""

    def __init__(self, client: Optional[Any] = None) -> None:
        self._client = client

    def emit(self, event: RuntimeEvidenceEvent) -> None:
        if self._client is None:
            from prometa.client import Prometa

            client = Prometa._current
        else:
            client = self._client
        if client is None:
            raise RuntimeExecutionError(
                "evidence_client_unavailable",
                "Construct Prometa before using PrometaEvidenceEmitter",
            )
        with client._span("task", event.name) as span:
            span.attributes["prometa.runtime.event"] = event.name
            span.attributes["prometa.runtime.outcome"] = event.outcome
            span.attributes["prometa.runtime.occurred_at"] = event.occurred_at
            for key, value in event.attributes.items():
                if value is not None:
                    span.attributes[key] = value


@dataclass(frozen=True)
class ModelToolCall:
    call_id: str
    name: str
    arguments: Mapping[str, Any]


@dataclass(frozen=True)
class ModelInvocationRequest:
    request_id: str
    model: RuntimeModel
    messages: Tuple[Mapping[str, Any], ...]
    tools: Tuple[RuntimeTool, ...]
    output_schema: Optional[Mapping[str, Any]]
    attempt: int


@dataclass(frozen=True)
class ModelInvocationResponse:
    content: Any
    tool_calls: Tuple[ModelToolCall, ...] = ()
    finish_reason: Optional[str] = None
    provider_model: Optional[str] = None


class ModelAdapter(Protocol):
    async def invoke(self, request: ModelInvocationRequest) -> ModelInvocationResponse:
        """Invoke one declared model without consulting Orchestra."""


@dataclass(frozen=True)
class GuardRequest:
    request_id: str
    stage: str
    payload: Any
    guardrails: Tuple[RuntimeGuardrail, ...]
    tool: Optional[RuntimeTool] = None


@dataclass(frozen=True)
class GuardDecision:
    allowed: bool
    action: str
    reason: str = ""
    evaluated_guardrails: Tuple[str, ...] = ()
    transformed_payload: Any = None


class GuardEvaluator(Protocol):
    async def evaluate(self, request: GuardRequest) -> GuardDecision:
        """Evaluate signed guard declarations in the tenant plane."""


@dataclass(frozen=True)
class HumanEscalationRequest:
    request_id: str
    reason: str
    stage: str
    payload: Any
    tool: Optional[RuntimeTool] = None


@dataclass(frozen=True)
class HumanEscalationDecision:
    approved: bool
    reviewer_reference: str
    reason: str = ""


class HumanEscalation(Protocol):
    async def request_review(
        self, request: HumanEscalationRequest
    ) -> HumanEscalationDecision:
        """Resolve a tenant-owned human decision; no platform callback implied."""


@dataclass(frozen=True)
class ToolInvocationRequest:
    request_id: str
    call_id: str
    tool: RuntimeTool
    arguments: Mapping[str, Any]


@dataclass(frozen=True)
class ToolInvocationResult:
    output: Any
    audit_reference: Optional[str] = None


class ToolBroker(Protocol):
    async def invoke(self, request: ToolInvocationRequest) -> ToolInvocationResult:
        """Execute an already-declared tool inside the tenant boundary."""


class DenyAllToolBroker:
    async def invoke(self, request: ToolInvocationRequest) -> ToolInvocationResult:
        raise RuntimeExecutionError(
            "tool_broker_denied",
            "No tenant tool broker was configured for %s" % request.tool.operation,
        )


class RuntimeStateStore(Protocol):
    async def save(self, request_id: str, state: Mapping[str, Any]) -> None:
        """Persist request lifecycle state in a tenant-owned store."""


class InMemoryRuntimeStateStore:
    def __init__(self) -> None:
        self.states: Dict[str, Mapping[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def save(self, request_id: str, state: Mapping[str, Any]) -> None:
        async with self._lock:
            self.states[request_id] = dict(state)


@dataclass(frozen=True)
class RuntimeExecutionPolicy:
    max_attempts_per_model: int = 2
    timeout_seconds: float = 30.0
    tool_timeout_seconds: float = 30.0
    initial_backoff_seconds: float = 0.1
    max_steps: int = 8
    fallback_model_names: Tuple[str, ...] = ()
    circuit_failure_threshold: int = 3
    circuit_reset_seconds: float = 30.0

    def __post_init__(self) -> None:
        if not 1 <= self.max_attempts_per_model <= 5:
            raise ValueError("max_attempts_per_model must be between 1 and 5")
        if self.timeout_seconds <= 0 or self.tool_timeout_seconds <= 0:
            raise ValueError("timeouts must be positive")
        if self.initial_backoff_seconds < 0:
            raise ValueError("initial_backoff_seconds must not be negative")
        if not 1 <= self.max_steps <= 64:
            raise ValueError("max_steps must be between 1 and 64")
        if not 1 <= self.circuit_failure_threshold <= 100:
            raise ValueError("circuit_failure_threshold must be between 1 and 100")
        if self.circuit_reset_seconds <= 0:
            raise ValueError("circuit_reset_seconds must be positive")
        if any(
            not isinstance(name, str)
            or not name.strip()
            or name != name.strip()
            or len(name) > 128
            for name in self.fallback_model_names
        ):
            raise ValueError("fallback_model_names must contain trimmed model names")
        if len(set(self.fallback_model_names)) != len(self.fallback_model_names):
            raise ValueError("fallback_model_names must be unique")


@dataclass(frozen=True)
class RuntimeExecutionResult:
    request_id: str
    output: Any
    model_name: str
    attempts: int
    tool_calls: int
    used_fallback: bool


@dataclass
class _CircuitState:
    failures: int = 0
    opened_at: Optional[float] = None


def available_runtime_capabilities(
    *,
    guard_evaluator: Optional[GuardEvaluator] = None,
    tool_broker: Optional[ToolBroker] = None,
    human_escalation: Optional[HumanEscalation] = None,
) -> FrozenSet[str]:
    capabilities = {
        CAPABILITY_MODEL_INVOKE,
        CAPABILITY_EVIDENCE_EMIT,
    }
    try:
        import jsonschema  # noqa: F401

        capabilities.add(CAPABILITY_SCHEMA_VALIDATE)
    except ImportError:
        pass
    if guard_evaluator is not None:
        capabilities.add(CAPABILITY_GUARD_EVALUATE)
    if tool_broker is not None and not isinstance(tool_broker, DenyAllToolBroker):
        capabilities.add(CAPABILITY_TOOL_BROKER)
    if human_escalation is not None:
        capabilities.add(CAPABILITY_HUMAN_ESCALATION)
    return frozenset(capabilities)


class RuntimeKernel:
    """Bounded executor for one already-admitted runtime release."""

    def __init__(
        self,
        admission: AdmittedRuntimeRelease,
        *,
        model_adapter: ModelAdapter,
        evidence_emitter: EvidenceEmitter,
        runtime_id: str,
        runtime_version: str,
        execution_policy: Optional[RuntimeExecutionPolicy] = None,
        guard_evaluator: Optional[GuardEvaluator] = None,
        tool_broker: Optional[ToolBroker] = None,
        human_escalation: Optional[HumanEscalation] = None,
        state_store: Optional[RuntimeStateStore] = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        for field_name, field_value in (
            ("runtime_id", runtime_id),
            ("runtime_version", runtime_version),
        ):
            if (
                not isinstance(field_value, str)
                or not field_value.strip()
                or field_value != field_value.strip()
                or len(field_value) > 128
            ):
                raise ValueError(
                    "%s must be a trimmed string of 1-128 characters" % field_name
                )
        self.admission = admission
        self.model_adapter = model_adapter
        self.evidence_emitter = evidence_emitter
        self.runtime_id = runtime_id
        self.runtime_version = runtime_version
        self.policy = execution_policy or RuntimeExecutionPolicy()
        self.guard_evaluator = guard_evaluator
        self.tool_broker = tool_broker or DenyAllToolBroker()
        self.human_escalation = human_escalation
        self.state_store = state_store
        self._sleep = sleep
        self._circuits: Dict[str, _CircuitState] = {}

        available = available_runtime_capabilities(
            guard_evaluator=guard_evaluator,
            tool_broker=tool_broker,
            human_escalation=human_escalation,
        )
        missing = admission.config.contract.required_capabilities - available
        if missing:
            raise RuntimeExecutionError(
                "runtime_component_missing",
                "Missing runtime components: %s" % ", ".join(sorted(missing)),
            )
        self._models = self._resolve_models()
        self._emit(
            "runtime.release.admitted",
            "accepted",
            None,
            {
                "prometa.runtime.contract_version": admission.config.contract.contract_version
            },
        )

    def _resolve_models(self) -> Tuple[RuntimeModel, ...]:
        by_name = {model.name: model for model in self.admission.config.models}
        ordered = [self.admission.config.primary_model]
        for name in self.policy.fallback_model_names:
            model = by_name.get(name)
            if model is None:
                raise ValueError(
                    "Fallback model %r is not declared in the bundle" % name
                )
            if model.role == "embedding":
                raise ValueError("Embedding model %r cannot be a fallback" % name)
            if model in ordered:
                raise ValueError(
                    "Fallback model %r duplicates the primary model" % name
                )
            ordered.append(model)
        return tuple(ordered)

    def _identity_attributes(self, request_id: Optional[str]) -> Dict[str, Any]:
        config = self.admission.config
        bundle_claims = self.admission.bundle.claims
        promotion_claims = self.admission.promotion.claims
        attributes: Dict[str, Any] = {
            "prometa.agent_id": config.manifest.agent_id,
            "gen_ai.agent.name": config.manifest.name,
            "prometa.bundle.digest": self.admission.bundle.artifact_digest,
            "prometa.bundle.jti": self.admission.bundle.jti,
            "prometa.environment": bundle_claims["targetEnvironment"],
            "prometa.attestation.id": self.admission.promotion.attestation_id,
            "prometa.attestation.jti": self.admission.promotion.jti,
            "prometa.policy.decision_id": promotion_claims["decisionId"],
            "prometa.policy.set_digest": promotion_claims["policySetDigest"],
            "prometa.release.id": promotion_claims["releaseId"],
            "prometa.deployment.id": promotion_claims["deploymentId"],
            "prometa.runtime.target": promotion_claims["requestedRuntime"],
            "prometa.runtime.id": self.runtime_id,
            "prometa.runtime.version": self.runtime_version,
            "prometa.manifest.id": config.manifest.manifest_id,
            "prometa.manifest.version": config.manifest.version,
        }
        if config.manifest.solution_name:
            attributes["prometa.solution_id"] = config.manifest.solution_name
        if request_id:
            attributes["prometa.runtime.request_id"] = request_id
        return attributes

    def _emit(
        self,
        name: str,
        outcome: str,
        request_id: Optional[str],
        attributes: Optional[Mapping[str, Any]] = None,
    ) -> None:
        merged = self._identity_attributes(request_id)
        if attributes:
            merged.update(attributes)
        event = RuntimeEvidenceEvent(
            name=name,
            outcome=outcome,
            occurred_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            attributes=merged,
        )
        try:
            self.evidence_emitter.emit(event)
        except RuntimeExecutionError:
            raise
        except Exception as exc:
            raise RuntimeExecutionError("evidence_emit_failed") from exc

    async def _save_state(self, request_id: str, status: str, **extra: Any) -> None:
        if self.state_store is not None:
            try:
                await self.state_store.save(
                    request_id,
                    {
                        "status": status,
                        "artifactDigest": self.admission.artifact_digest,
                        "releaseId": self.admission.promotion.claims["releaseId"],
                        **extra,
                    },
                )
            except asyncio.CancelledError:
                raise
            except RuntimeExecutionError:
                raise
            except Exception as exc:
                raise RuntimeExecutionError("state_store_failed") from exc

    def _validate_schema(
        self,
        stage: str,
        payload: Any,
        schema: Optional[Mapping[str, Any]],
        request_id: str,
    ) -> Any:
        if schema is None:
            return payload
        candidate = payload
        if stage == "output" and isinstance(payload, str):
            schema_type = schema.get("type")
            expects_structured = schema_type in {"object", "array"} or (
                isinstance(schema_type, list)
                and bool({"object", "array"}.intersection(schema_type))
            )
            if expects_structured:
                try:
                    candidate = _strict_json_loads(payload)
                except (TypeError, ValueError, json.JSONDecodeError):
                    self._emit(
                        "runtime.schema.%s" % stage,
                        "denied",
                        request_id,
                        {"prometa.runtime.reason": "invalid_json_output"},
                    )
                    raise RuntimeExecutionError("output_json_invalid")
        try:
            from jsonschema import Draft202012Validator

            errors = sorted(
                Draft202012Validator(schema).iter_errors(candidate),
                key=lambda error: tuple(str(part) for part in error.absolute_path),
            )
        except ImportError as exc:
            raise RuntimeExecutionError("runtime_dependency_missing") from exc
        except Exception as exc:
            raise RuntimeExecutionError("schema_validation_failed") from exc
        if errors:
            first = errors[0]
            path = "/".join(str(part) for part in first.absolute_path) or "$"
            self._emit(
                "runtime.schema.%s" % stage,
                "denied",
                request_id,
                {
                    "prometa.runtime.reason": "schema_mismatch",
                    "prometa.schema.path": path,
                    "prometa.schema.error_count": len(errors),
                },
            )
            raise RuntimeExecutionError("%s_schema_invalid" % stage)
        self._emit(
            "runtime.schema.%s" % stage,
            "accepted",
            request_id,
            {"prometa.schema.error_count": 0},
        )
        return candidate

    async def _guard(
        self,
        stage: str,
        payload: Any,
        request_id: str,
        *,
        tool: Optional[RuntimeTool] = None,
    ) -> Any:
        guardrails = self.admission.config.guardrails
        required = set(tool.required_guardrails if tool else ())
        if not guardrails and not required:
            return payload
        if self.guard_evaluator is None:
            raise RuntimeExecutionError("guard_evaluator_missing")
        try:
            decision = await self.guard_evaluator.evaluate(
                GuardRequest(
                    request_id=request_id,
                    stage=stage,
                    payload=payload,
                    guardrails=guardrails,
                    tool=tool,
                )
            )
        except asyncio.CancelledError:
            raise
        except RuntimeExecutionError:
            raise
        except Exception as exc:
            raise RuntimeExecutionError("guard_evaluation_failed") from exc
        if not isinstance(decision, GuardDecision):
            raise RuntimeExecutionError("invalid_guard_decision")
        declared = {guardrail.name for guardrail in guardrails}
        evaluated = set(decision.evaluated_guardrails)
        applies_to = "tool-calls" if stage == "tool" else stage
        applicable = {
            guardrail.name
            for guardrail in guardrails
            if guardrail.applies_to in {None, "all", applies_to}
        }
        declared_identifiers = (
            declared | required | {guardrail.guardrail_type for guardrail in guardrails}
        )
        if (
            not evaluated.issubset(declared_identifiers)
            or not applicable.issubset(evaluated)
            or not required.issubset(evaluated)
        ):
            raise RuntimeExecutionError("guardrail_evidence_incomplete")
        self._emit(
            "runtime.guard.%s" % stage,
            "accepted" if decision.allowed else "denied",
            request_id,
            {
                "prometa.guardrail.action": decision.action,
                "prometa.guardrail.reason": decision.reason,
                "prometa.guardrail.evaluated": ",".join(sorted(evaluated)),
            },
        )
        if decision.allowed:
            guarded_payload = (
                decision.transformed_payload
                if decision.transformed_payload is not None
                else payload
            )
            requires_human = any(
                guardrail.guardrail_type == "human-approval"
                and guardrail.name in applicable
                for guardrail in guardrails
            )
            if requires_human:
                return await self._human_review(
                    request_id,
                    stage,
                    "Signed human-approval guardrail",
                    tool,
                    guarded_payload,
                )
            return guarded_payload
        if decision.action == "escalate":
            return await self._human_review(
                request_id,
                stage,
                decision.reason or "guardrail escalation",
                tool,
                payload,
            )
        raise RuntimeExecutionError("guard_denied")

    async def _human_review(
        self,
        request_id: str,
        stage: str,
        reason: str,
        tool: Optional[RuntimeTool],
        payload: Any,
    ) -> Any:
        if self.human_escalation is None:
            self._emit(
                "runtime.human_review",
                "denied",
                request_id,
                {"prometa.runtime.reason": "human_escalation_unavailable"},
            )
            raise RuntimeExecutionError("human_escalation_unavailable")
        try:
            decision = await self.human_escalation.request_review(
                HumanEscalationRequest(
                    request_id=request_id,
                    reason=reason,
                    stage=stage,
                    payload=payload,
                    tool=tool,
                )
            )
        except asyncio.CancelledError:
            raise
        except RuntimeExecutionError:
            raise
        except Exception as exc:
            raise RuntimeExecutionError("human_review_failed") from exc
        if (
            not isinstance(decision, HumanEscalationDecision)
            or type(decision.approved) is not bool
            or not isinstance(decision.reviewer_reference, str)
            or not decision.reviewer_reference.strip()
        ):
            raise RuntimeExecutionError("invalid_human_review_decision")
        self._emit(
            "runtime.human_review",
            "accepted" if decision.approved else "denied",
            request_id,
            {
                "prometa.review.reference": decision.reviewer_reference,
                "prometa.review.reason": decision.reason,
            },
        )
        if not decision.approved:
            raise RuntimeExecutionError("human_review_denied")
        return payload

    def _tool_by_name(self, name: str) -> RuntimeTool:
        matches = [
            tool
            for tool in self.admission.config.tools
            if name in {tool.name, tool.operation}
        ]
        if len(matches) != 1:
            raise RuntimeExecutionError("undeclared_or_ambiguous_tool")
        return matches[0]

    async def _invoke_tool(
        self, call: ModelToolCall, request_id: str
    ) -> ToolInvocationResult:
        tool = self._tool_by_name(call.name)
        arguments = self._validate_schema(
            "tool_input", call.arguments, tool.input_schema, request_id
        )
        arguments = await self._guard("tool", arguments, request_id, tool=tool)
        if self.admission.config.guardrails or tool.required_guardrails:
            arguments = self._validate_schema(
                "tool_input", arguments, tool.input_schema, request_id
            )
        if tool.approval_required:
            await self._human_review(
                request_id,
                "tool",
                "Tool %s requires human approval" % tool.operation,
                tool,
                arguments,
            )
        self._emit(
            "runtime.tool.call",
            "started",
            request_id,
            {
                "gen_ai.tool.name": tool.operation,
                "prometa.tool.risk": tool.risk_level,
                "prometa.tool.side_effects": tool.side_effects,
            },
        )
        try:
            result = await asyncio.wait_for(
                self.tool_broker.invoke(
                    ToolInvocationRequest(
                        request_id=request_id,
                        call_id=call.call_id,
                        tool=tool,
                        arguments=arguments,
                    )
                ),
                timeout=self.policy.tool_timeout_seconds,
            )
            if not isinstance(result, ToolInvocationResult):
                raise RuntimeExecutionError("invalid_tool_result")
        except asyncio.TimeoutError as exc:
            self._emit(
                "runtime.tool.call",
                "failed",
                request_id,
                {
                    "gen_ai.tool.name": tool.operation,
                    "prometa.runtime.reason": "timeout",
                },
            )
            raise RuntimeExecutionError("tool_timeout") from exc
        except asyncio.CancelledError:
            raise
        except RuntimeExecutionError as exc:
            self._emit(
                "runtime.tool.call",
                "failed",
                request_id,
                {
                    "gen_ai.tool.name": tool.operation,
                    "prometa.runtime.reason": exc.code,
                },
            )
            raise
        except Exception as exc:
            self._emit(
                "runtime.tool.call",
                "failed",
                request_id,
                {
                    "gen_ai.tool.name": tool.operation,
                    "prometa.runtime.reason": "tool_call_failed",
                },
            )
            raise RuntimeExecutionError("tool_call_failed") from exc
        self._emit(
            "runtime.tool.call",
            "completed",
            request_id,
            {
                "gen_ai.tool.name": tool.operation,
                "prometa.tool.audit_reference": result.audit_reference,
            },
        )
        return result

    def _circuit_available(self, model: RuntimeModel, request_id: str) -> bool:
        state = self._circuits.setdefault(model.name, _CircuitState())
        if state.opened_at is None:
            return True
        now = asyncio.get_running_loop().time()
        if now - state.opened_at < self.policy.circuit_reset_seconds:
            self._emit(
                "runtime.circuit_breaker",
                "denied",
                request_id,
                {
                    "gen_ai.request.model": model.model_name,
                    "circuit_breaker.state": "open",
                },
            )
            return False
        self._emit(
            "runtime.circuit_breaker",
            "probe",
            request_id,
            {
                "gen_ai.request.model": model.model_name,
                "circuit_breaker.from_state": "open",
                "circuit_breaker.to_state": "half_open",
            },
        )
        return True

    def _model_succeeded(self, model: RuntimeModel, request_id: str) -> None:
        state = self._circuits.setdefault(model.name, _CircuitState())
        if state.failures or state.opened_at is not None:
            self._emit(
                "runtime.circuit_breaker",
                "closed",
                request_id,
                {
                    "gen_ai.request.model": model.model_name,
                    "circuit_breaker.from_state": (
                        "half_open" if state.opened_at is not None else "closed"
                    ),
                    "circuit_breaker.to_state": "closed",
                },
            )
        state.failures = 0
        state.opened_at = None

    def _model_failed(self, model: RuntimeModel, request_id: str) -> None:
        state = self._circuits.setdefault(model.name, _CircuitState())
        state.failures += 1
        if state.failures >= self.policy.circuit_failure_threshold:
            if state.opened_at is None:
                state.opened_at = asyncio.get_running_loop().time()
                self._emit(
                    "runtime.circuit_breaker",
                    "opened",
                    request_id,
                    {
                        "gen_ai.request.model": model.model_name,
                        "circuit_breaker.from_state": "closed",
                        "circuit_breaker.to_state": "open",
                        "circuit_breaker.failure_count": state.failures,
                    },
                )

    @staticmethod
    def _user_message(payload: Any) -> str:
        if isinstance(payload, str):
            return payload
        try:
            return json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeExecutionError("request_payload_not_json") from exc

    async def execute(
        self,
        payload: Any,
        *,
        request_id: Optional[str] = None,
    ) -> RuntimeExecutionResult:
        request_id = request_id or str(uuid.uuid4())
        if (
            not isinstance(request_id, str)
            or not request_id.strip()
            or request_id != request_id.strip()
            or len(request_id) > 256
        ):
            raise ValueError("request_id must be a trimmed string of 1-256 characters")
        attempts = 0
        tool_calls = 0
        executed_tool = False
        seen_tool_call_ids = set()
        self._emit("runtime.request", "started", request_id)
        try:
            await self._save_state(request_id, "running")
            runtime_input = self._validate_schema(
                "input",
                payload,
                self.admission.config.contract.input_schema,
                request_id,
            )
            runtime_input = await self._guard("input", runtime_input, request_id)
            if self.admission.config.guardrails:
                runtime_input = self._validate_schema(
                    "input",
                    runtime_input,
                    self.admission.config.contract.input_schema,
                    request_id,
                )
            initial_messages: Tuple[Mapping[str, Any], ...] = (
                {"role": "system", "content": self.admission.config.system_prompt},
                {"role": "user", "content": self._user_message(runtime_input)},
            )
            last_error: Optional[RuntimeExecutionError] = None

            for model_index, model in enumerate(self._models):
                if model_index > 0:
                    if executed_tool:
                        raise RuntimeExecutionError(
                            "fallback_after_tool_denied",
                            "Model fallback is unsafe after a tool side effect",
                        )
                    self._emit(
                        "runtime.model.fallback",
                        "selected",
                        request_id,
                        {"gen_ai.request.model": model.model_name},
                    )
                if not self._circuit_available(model, request_id):
                    last_error = RuntimeExecutionError("circuit_open", retryable=True)
                    continue

                messages = initial_messages
                steps = 0
                retry_attempt = 1
                while retry_attempt <= self.policy.max_attempts_per_model:
                    attempts += 1
                    self._emit(
                        "runtime.model.attempt",
                        "started",
                        request_id,
                        {
                            "gen_ai.request.model": model.model_name,
                            "prometa.model.provider": model.provider,
                            "retry.attempt_number": retry_attempt,
                        },
                    )
                    try:
                        response = await asyncio.wait_for(
                            self.model_adapter.invoke(
                                ModelInvocationRequest(
                                    request_id=request_id,
                                    model=model,
                                    messages=messages,
                                    tools=self.admission.config.tools,
                                    output_schema=self.admission.config.contract.output_schema,
                                    attempt=retry_attempt,
                                )
                            ),
                            timeout=self.policy.timeout_seconds,
                        )
                        if not isinstance(response, ModelInvocationResponse):
                            raise RuntimeExecutionError("invalid_model_response")
                        self._model_succeeded(model, request_id)
                        self._emit(
                            "runtime.model.attempt",
                            "completed",
                            request_id,
                            {
                                "gen_ai.request.model": model.model_name,
                                "gen_ai.response.model": response.provider_model
                                or model.model_name,
                                "gen_ai.response.finish_reasons": response.finish_reason,
                                "retry.attempt_number": retry_attempt,
                            },
                        )

                        if response.tool_calls:
                            response_call_ids = [
                                call.call_id for call in response.tool_calls
                            ]
                            if (
                                any(
                                    not isinstance(call.call_id, str)
                                    or not call.call_id
                                    or not isinstance(call.name, str)
                                    or not call.name
                                    for call in response.tool_calls
                                )
                                or len(set(response_call_ids)) != len(response_call_ids)
                                or bool(
                                    seen_tool_call_ids.intersection(response_call_ids)
                                )
                            ):
                                raise RuntimeExecutionError(
                                    "duplicate_or_invalid_tool_call"
                                )
                            seen_tool_call_ids.update(response_call_ids)
                            steps += 1
                            if steps > min(
                                self.policy.max_steps,
                                self.admission.config.max_iterations,
                            ):
                                raise RuntimeExecutionError(
                                    "runtime_step_limit_exceeded"
                                )
                            assistant_calls = []
                            tool_messages = []
                            for call in response.tool_calls:
                                result = await self._invoke_tool(call, request_id)
                                tool_calls += 1
                                executed_tool = True
                                assistant_calls.append(
                                    {
                                        "id": call.call_id,
                                        "type": "function",
                                        "function": {
                                            "name": call.name,
                                            "arguments": json.dumps(
                                                call.arguments,
                                                sort_keys=True,
                                                separators=(",", ":"),
                                                ensure_ascii=False,
                                            ),
                                        },
                                    }
                                )
                                tool_messages.append(
                                    {
                                        "role": "tool",
                                        "tool_call_id": call.call_id,
                                        "content": self._user_message(result.output),
                                    }
                                )
                            messages = messages + (
                                {
                                    "role": "assistant",
                                    "content": response.content
                                    if isinstance(response.content, str)
                                    else None,
                                    "tool_calls": assistant_calls,
                                },
                                *tool_messages,
                            )
                            continue

                        output = self._validate_schema(
                            "output",
                            response.content,
                            self.admission.config.contract.output_schema,
                            request_id,
                        )
                        output = await self._guard("output", output, request_id)
                        if self.admission.config.guardrails:
                            output = self._validate_schema(
                                "output",
                                output,
                                self.admission.config.contract.output_schema,
                                request_id,
                            )
                        result = RuntimeExecutionResult(
                            request_id=request_id,
                            output=output,
                            model_name=response.provider_model or model.model_name,
                            attempts=attempts,
                            tool_calls=tool_calls,
                            used_fallback=model_index > 0,
                        )
                        await self._save_state(
                            request_id,
                            "completed",
                            model=result.model_name,
                            attempts=attempts,
                            toolCalls=tool_calls,
                        )
                        self._emit(
                            "runtime.request",
                            "completed",
                            request_id,
                            {
                                "gen_ai.response.model": result.model_name,
                                "prometa.runtime.attempts": attempts,
                                "prometa.runtime.tool_calls": tool_calls,
                                "prometa.runtime.used_fallback": result.used_fallback,
                            },
                        )
                        return result
                    except asyncio.TimeoutError:
                        failure = ModelAdapterError("model_timeout", retryable=True)
                    except asyncio.CancelledError:
                        await self._save_state(request_id, "cancelled")
                        self._emit("runtime.request", "cancelled", request_id)
                        raise
                    except ModelAdapterError as caught:
                        failure = caught
                    except RuntimeExecutionError:
                        raise
                    except Exception as exc:
                        failure = ModelAdapterError(
                            "model_adapter_failed", retryable=True
                        )
                        failure.__cause__ = exc

                    last_error = failure
                    self._model_failed(model, request_id)
                    self._emit(
                        "runtime.model.attempt",
                        "failed",
                        request_id,
                        {
                            "gen_ai.request.model": model.model_name,
                            "retry.attempt_number": retry_attempt,
                            "prometa.runtime.reason": failure.code,
                            "prometa.runtime.retryable": failure.retryable,
                        },
                    )
                    if executed_tool and failure.retryable:
                        raise RuntimeExecutionError(
                            "retry_after_tool_denied",
                            "Model retry is unsafe after a tool call",
                        ) from failure
                    if not failure.retryable:
                        raise failure
                    if retry_attempt < self.policy.max_attempts_per_model:
                        backoff = self.policy.initial_backoff_seconds * (
                            2 ** (retry_attempt - 1)
                        )
                        self._emit(
                            "runtime.retry",
                            "scheduled",
                            request_id,
                            {
                                "gen_ai.request.model": model.model_name,
                                "retry.attempt_number": retry_attempt + 1,
                                "retry.backoff_ms": int(backoff * 1000),
                            },
                        )
                        await self._sleep(backoff)
                    retry_attempt += 1

            raise last_error or RuntimeExecutionError("no_model_available")
        except asyncio.CancelledError:
            raise
        except RuntimeExecutionError as error:
            state_error: Optional[RuntimeExecutionError] = None
            try:
                await self._save_state(request_id, "failed", reason=error.code)
            except RuntimeExecutionError as caught:
                state_error = caught
            final_error = state_error or error
            self._emit(
                "runtime.request",
                "failed",
                request_id,
                {
                    "prometa.runtime.reason": final_error.code,
                    "prometa.runtime.original_reason": error.code,
                },
            )
            if state_error is not None and state_error is not error:
                raise state_error from error
            raise


__all__ = [
    "RuntimeExecutionError",
    "ModelAdapterError",
    "RuntimeEvidenceEvent",
    "EvidenceEmitter",
    "InMemoryEvidenceEmitter",
    "PrometaEvidenceEmitter",
    "ModelToolCall",
    "ModelInvocationRequest",
    "ModelInvocationResponse",
    "ModelAdapter",
    "GuardRequest",
    "GuardDecision",
    "GuardEvaluator",
    "HumanEscalationRequest",
    "HumanEscalationDecision",
    "HumanEscalation",
    "ToolInvocationRequest",
    "ToolInvocationResult",
    "ToolBroker",
    "DenyAllToolBroker",
    "RuntimeStateStore",
    "InMemoryRuntimeStateStore",
    "RuntimeExecutionPolicy",
    "RuntimeExecutionResult",
    "available_runtime_capabilities",
    "RuntimeKernel",
]
