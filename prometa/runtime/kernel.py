"""Tenant-owned execution kernel for admitted Orchestra runtime releases.

The kernel is intentionally protocol-first. It owns request validation,
guard/tool admission, bounded model execution, resilience, and evidence. Model,
tool, state, and human-review implementations stay tenant supplied; the
Orchestra control plane is never called from the synchronous request path.
"""

from __future__ import annotations

import asyncio
import json
import re
import threading
import uuid
from dataclasses import InitVar, dataclass
from datetime import datetime, timezone
from math import isfinite
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
    CAPABILITY_SECURITY_DECISION_EMIT,
    CAPABILITY_SCHEMA_VALIDATE,
    CAPABILITY_TOOL_BROKER,
    CAPABILITY_WORKFLOW_CONTEXT_RESOLVE,
    CAPABILITY_WORKFLOW_DECISION_EMIT,
    CAPABILITY_WORKFLOW_POLICY_EVALUATE,
    CAPABILITY_WORKFLOW_STATE_PERSIST,
    RuntimeGuardrail,
    RuntimeModel,
    RuntimeTool,
)
from .security_assurance import (
    SecurityDecisionCorrelation,
    SecurityDecisionEmitter,
    SecurityGuardAssessment,
    build_security_decision,
    security_policy_identifier,
)
from .tasks import RuntimeTaskClaim
from .workflow_ontology import (
    RuntimeWorkflowOntology,
    VerifiedWorkflowContext,
    WorkflowContextRequest,
    WorkflowContextResolver,
    WorkflowDecisionEmitter,
    WorkflowDecisionEvidence,
    WorkflowExecutionContext,
    WorkflowIndeterminateRequest,
    WorkflowOntologyError,
    WorkflowPolicyDecision,
    WorkflowPostconditionRequest,
    WorkflowPostconditionValidator,
    WorkflowStateCommitRequest,
    WorkflowStateStore,
    evaluate_workflow_policy,
    workflow_fact_set_digest,
)


RUNTIME_EDGE_OVERLOAD_CONTRACT = "orchestra-runtime-edge-overload-v1"
MODEL_IDENTITY_MAX_LENGTH = 256
_RESERVED_EXTERNAL_IDENTITIES = frozenset({"null", "none", "nil", "undefined"})
_ENGINE_REQUEST_ID_PATTERN = re.compile(r"req_[0-9a-f]{32}\Z")
_USAGE_RECORD_ID_PATTERN = re.compile(r"usage_[0-9a-f]{32}\Z")
_MODEL_INVOCATION_MISSING = object()


def _validate_model_identity(value: Any, field_name: str) -> str:
    """Validate one cross-plane identity without rewriting it.

    The inference-engine contract accepts visible ASCII only.  Keeping the
    validation byte-for-byte (rather than stripping or truncating) prevents
    two distinct upstream identities from collapsing onto one ledger key.
    """

    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= MODEL_IDENTITY_MAX_LENGTH
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in value)
    ):
        raise ValueError(
            "%s must be 1-256 visible ASCII characters" % field_name
        )
    if value.lower() in _RESERVED_EXTERNAL_IDENTITIES:
        raise ValueError("%s uses a reserved null identity sentinel" % field_name)
    return value


def _canonical_server_identity(value: Any, field_name: str) -> Optional[str]:
    """Return a canonical server-owned ID, or ``None`` for legacy input.

    Older inference-engine releases echoed the runtime request identity in
    ``x-request-id``.  Treating a non-canonical value as absent prevents that
    legacy echo from being mislabeled as an engine-owned request identity.
    """

    pattern = {
        "engine_request_id": _ENGINE_REQUEST_ID_PATTERN,
        "usage_record_id": _USAGE_RECORD_ID_PATTERN,
    }.get(field_name)
    if pattern is None:
        raise ValueError("unsupported server identity field")
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        return None
    return value


def _validate_optional_server_identity(
    value: Optional[str], field_name: str
) -> Optional[str]:
    if value is None:
        return None
    canonical = _canonical_server_identity(value, field_name)
    if canonical is None:
        expected = (
            "req_[0-9a-f]{32}"
            if field_name == "engine_request_id"
            else "usage_[0-9a-f]{32}"
        )
        raise ValueError("%s must match %s" % (field_name, expected))
    return canonical


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

    def __init__(
        self,
        code: str,
        message: Optional[str] = None,
        *,
        retryable: bool = False,
        retry_after_seconds: Optional[float] = None,
        provider_code: Optional[str] = None,
        engine_request_id: Optional[str] = None,
        usage_record_id: Optional[str] = None,
    ) -> None:
        if retry_after_seconds is not None and (
            not retryable
            or not isinstance(retry_after_seconds, (int, float))
            or isinstance(retry_after_seconds, bool)
            or not isfinite(retry_after_seconds)
            or retry_after_seconds < 0
        ):
            raise ValueError(
                "retry_after_seconds requires a retryable non-negative number"
            )
        self.retry_after_seconds = (
            float(retry_after_seconds) if retry_after_seconds is not None else None
        )
        # The provider's own error identifier (OpenAI ``error.code``), when the
        # response carried one. ``code`` stays the transport-level
        # ``model_http_<status>`` so existing callers keep working; this is the
        # additive, more specific signal.
        self.provider_code = (
            provider_code if isinstance(provider_code, str) and provider_code else None
        )
        self.engine_request_id = _validate_optional_server_identity(
            engine_request_id, "engine_request_id"
        )
        self.usage_record_id = _validate_optional_server_identity(
            usage_record_id, "usage_record_id"
        )
        super().__init__(code, message, retryable=retryable)


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


@dataclass(frozen=True, init=False)
class ModelInvocationRequest:
    request_id: str
    model: RuntimeModel
    messages: Tuple[Mapping[str, Any], ...]
    tools: Tuple[RuntimeTool, ...]
    output_schema: Optional[Mapping[str, Any]]
    attempt: int
    # InitVars remain outside fields/asdict/repr/equality while teaching
    # dataclasses.replace() to carry the two non-legacy identities forward.
    _clone_model_invocation_id: InitVar[Optional[str]] = None
    _clone_model_attempt_id: InitVar[Optional[str]] = None
    __match_args__ = (
        "request_id",
        "model",
        "messages",
        "tools",
        "output_schema",
        "attempt",
    )

    def __init__(
        self,
        request_id: Any = _MODEL_INVOCATION_MISSING,
        model: Any = _MODEL_INVOCATION_MISSING,
        messages: Any = _MODEL_INVOCATION_MISSING,
        tools: Any = _MODEL_INVOCATION_MISSING,
        output_schema: Any = _MODEL_INVOCATION_MISSING,
        attempt: Any = _MODEL_INVOCATION_MISSING,
        *,
        runtime_request_id: Any = _MODEL_INVOCATION_MISSING,
        model_invocation_id: Optional[str] = None,
        model_attempt_id: Optional[str] = None,
        _clone_model_invocation_id: Optional[str] = None,
        _clone_model_attempt_id: Optional[str] = None,
    ) -> None:
        """Build a request while retaining the public v1 call signature.

        The declared dataclass fields and first six parameters deliberately
        match the former public shape. New callers can use
        ``runtime_request_id``; kernel callers additionally supply the semantic
        invocation and unique attempt identities. Those identities are exposed
        as properties without changing legacy ``fields()``, ``asdict()``, repr,
        equality, or positional pattern matching. Legacy construction gets safe
        UUID identities.
        """

        if runtime_request_id is _MODEL_INVOCATION_MISSING:
            if request_id is _MODEL_INVOCATION_MISSING:
                raise TypeError("request_id or runtime_request_id is required")
            resolved_runtime_request_id = request_id
        else:
            resolved_runtime_request_id = runtime_request_id
            if (
                request_id is not _MODEL_INVOCATION_MISSING
                and request_id != runtime_request_id
            ):
                raise ValueError("request_id and runtime_request_id must match")
        missing_fields = [
            name
            for name, value in (
                ("model", model),
                ("messages", messages),
                ("tools", tools),
                ("output_schema", output_schema),
                ("attempt", attempt),
            )
            if value is _MODEL_INVOCATION_MISSING
        ]
        if missing_fields:
            raise TypeError(
                "missing required ModelInvocationRequest fields: %s"
                % ", ".join(missing_fields)
            )
        object.__setattr__(self, "request_id", resolved_runtime_request_id)
        resolved_model_invocation_id = (
            model_invocation_id
            if model_invocation_id is not None
            else _clone_model_invocation_id
        )
        resolved_model_attempt_id = (
            model_attempt_id
            if model_attempt_id is not None
            else _clone_model_attempt_id
        )
        object.__setattr__(
            self,
            "_clone_model_invocation_id",
            (
                resolved_model_invocation_id
                if resolved_model_invocation_id is not None
                else str(uuid.uuid4())
            ),
        )
        object.__setattr__(
            self,
            "_clone_model_attempt_id",
            (
                resolved_model_attempt_id
                if resolved_model_attempt_id is not None
                else str(uuid.uuid4())
            ),
        )
        object.__setattr__(self, "model", model)
        object.__setattr__(self, "messages", messages)
        object.__setattr__(self, "tools", tools)
        object.__setattr__(self, "output_schema", output_schema)
        object.__setattr__(self, "attempt", attempt)
        self.__post_init__()

    def __post_init__(self) -> None:
        _validate_model_identity(self.runtime_request_id, "runtime_request_id")
        _validate_model_identity(self.model_invocation_id, "model_invocation_id")
        _validate_model_identity(self.model_attempt_id, "model_attempt_id")
        if type(self.attempt) is not int or self.attempt < 1:
            raise ValueError("attempt must be a positive integer")

    @property
    def runtime_request_id(self) -> str:
        """Cross-plane alias for the legacy ``request_id`` field."""

        return self.request_id

    @property
    def model_invocation_id(self) -> str:
        """Semantic model-turn identity, stable across retry and fallback."""

        return self._clone_model_invocation_id

    @property
    def model_attempt_id(self) -> str:
        """Unique identity for this outbound adapter call."""

        return self._clone_model_attempt_id


@dataclass(frozen=True)
class ModelTokenUsage:
    """Token accounting reported by the model plane for one invocation.

    Adapters populate this from the provider's own ``usage`` block rather than
    estimating: the runtime bills, budgets, and traces against these numbers,
    and an approximation that drifts is worse than an absent value. Adapters
    that cannot obtain real counts leave ``ModelInvocationResponse.usage`` as
    ``None``.

    ``cached_input_tokens`` is the portion of ``input_tokens`` served from a
    provider-side prefix cache. It is a *sub-count* of ``input_tokens``, not an
    addition to it.
    """

    input_tokens: int = 0
    output_tokens: int = 0
    cached_input_tokens: int = 0

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens


@dataclass(frozen=True)
class ModelInvocationResponse:
    content: Any
    tool_calls: Tuple[ModelToolCall, ...] = ()
    finish_reason: Optional[str] = None
    provider_model: Optional[str] = None
    # ``None`` when the adapter could not obtain real counts. Kept optional so
    # third-party adapters implementing ModelAdapter stay source-compatible.
    usage: Optional[ModelTokenUsage] = None
    # Server-owned correlation and ledger identities returned by the tenant
    # model plane.  Only these explicitly allowlisted values enter evidence.
    engine_request_id: Optional[str] = None
    usage_record_id: Optional[str] = None

    def __post_init__(self) -> None:
        _validate_optional_server_identity(
            self.engine_request_id, "engine_request_id"
        )
        _validate_optional_server_identity(self.usage_record_id, "usage_record_id")


def _usage_attributes(usage: Optional[ModelTokenUsage]) -> Mapping[str, Any]:
    """GenAI semantic-convention token attributes for an evidence event.

    Returns an empty mapping when the adapter reported no usage, so a span
    never carries a zero that reads as "no tokens were spent" when the truth is
    "this adapter cannot say". ``cached_input_tokens`` is only emitted when
    non-zero — most providers have no prefix cache, and a constant 0 on every
    span is noise.
    """
    if usage is None:
        return {}
    attributes: Dict[str, Any] = {
        "gen_ai.usage.input_tokens": usage.input_tokens,
        "gen_ai.usage.output_tokens": usage.output_tokens,
    }
    if usage.cached_input_tokens:
        attributes["gen_ai.usage.cached_input_tokens"] = usage.cached_input_tokens
    return attributes


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
    security_assessments: Tuple[SecurityGuardAssessment, ...] = ()


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
class _GuardOutcome:
    payload: Any
    approval_references: Tuple[str, ...] = ()


@dataclass(frozen=True)
class ToolInvocationRequest:
    request_id: str
    call_id: str
    tool: RuntimeTool
    arguments: Mapping[str, Any]
    agent_id: Optional[str] = None
    release_id: Optional[str] = None
    deployment_id: Optional[str] = None
    environment: Optional[str] = None
    granted_scopes: Tuple[str, ...] = ()
    approval_references: Tuple[str, ...] = ()


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
        self._lock = threading.Lock()

    async def save(self, request_id: str, state: Mapping[str, Any]) -> None:
        with self._lock:
            self.states[request_id] = dict(state)


@dataclass(frozen=True)
class RuntimeExecutionPolicy:
    max_attempts_per_model: int = 2
    timeout_seconds: float = 30.0
    tool_timeout_seconds: float = 30.0
    initial_backoff_seconds: float = 0.1
    max_backoff_seconds: float = 30.0
    max_retry_after_seconds: float = 30.0
    max_steps: int = 8
    fallback_model_names: Tuple[str, ...] = ()
    circuit_failure_threshold: int = 3
    circuit_reset_seconds: float = 30.0
    overload_contract_id: Optional[str] = None

    def __post_init__(self) -> None:
        if not 1 <= self.max_attempts_per_model <= 5:
            raise ValueError("max_attempts_per_model must be between 1 and 5")
        if self.timeout_seconds <= 0 or self.tool_timeout_seconds <= 0:
            raise ValueError("timeouts must be positive")
        if self.initial_backoff_seconds < 0:
            raise ValueError("initial_backoff_seconds must not be negative")
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError(
                "max_backoff_seconds must be at least initial_backoff_seconds"
            )
        if self.max_retry_after_seconds < 0:
            raise ValueError("max_retry_after_seconds must not be negative")
        if not 1 <= self.max_steps <= 64:
            raise ValueError("max_steps must be between 1 and 64")
        if not 1 <= self.circuit_failure_threshold <= 100:
            raise ValueError("circuit_failure_threshold must be between 1 and 100")
        if self.circuit_reset_seconds <= 0:
            raise ValueError("circuit_reset_seconds must be positive")
        if self.overload_contract_id not in {
            None,
            RUNTIME_EDGE_OVERLOAD_CONTRACT,
        }:
            raise ValueError("overload_contract_id is unsupported")
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


@dataclass(frozen=True)
class _WorkflowEvaluation:
    artifact: RuntimeWorkflowOntology
    task: Mapping[str, Any]
    context_request: WorkflowContextRequest
    verified_context: VerifiedWorkflowContext
    decision: WorkflowPolicyDecision
    runtime_input: Mapping[str, Any]


def available_runtime_capabilities(
    *,
    guard_evaluator: Optional[GuardEvaluator] = None,
    security_decision_emitter: Optional[SecurityDecisionEmitter] = None,
    tool_broker: Optional[ToolBroker] = None,
    human_escalation: Optional[HumanEscalation] = None,
    workflow_context_resolver: Optional[WorkflowContextResolver] = None,
    workflow_state_store: Optional[WorkflowStateStore] = None,
    workflow_decision_emitter: Optional[WorkflowDecisionEmitter] = None,
) -> FrozenSet[str]:
    capabilities = {
        CAPABILITY_MODEL_INVOKE,
        CAPABILITY_EVIDENCE_EMIT,
        CAPABILITY_WORKFLOW_POLICY_EVALUATE,
    }
    try:
        import jsonschema  # noqa: F401

        capabilities.add(CAPABILITY_SCHEMA_VALIDATE)
    except ImportError:
        pass
    if guard_evaluator is not None:
        capabilities.add(CAPABILITY_GUARD_EVALUATE)
    if security_decision_emitter is not None:
        capabilities.add(CAPABILITY_SECURITY_DECISION_EMIT)
    if tool_broker is not None and not isinstance(tool_broker, DenyAllToolBroker):
        capabilities.add(CAPABILITY_TOOL_BROKER)
    if human_escalation is not None:
        capabilities.add(CAPABILITY_HUMAN_ESCALATION)
    if workflow_context_resolver is not None:
        capabilities.add(CAPABILITY_WORKFLOW_CONTEXT_RESOLVE)
    if workflow_state_store is not None:
        capabilities.add(CAPABILITY_WORKFLOW_STATE_PERSIST)
    if workflow_decision_emitter is not None:
        capabilities.add(CAPABILITY_WORKFLOW_DECISION_EMIT)
    return frozenset(capabilities)


def runtime_release_identity_attributes(
    admission: AdmittedRuntimeRelease,
    *,
    runtime_id: str,
    runtime_version: str,
    request_id: Optional[str] = None,
    release_source: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the payload-free release identity shared by runtime evidence."""

    config = admission.config
    bundle_claims = admission.bundle.claims
    promotion_claims = admission.promotion.claims
    attributes: Dict[str, Any] = {
        "prometa.agent_id": config.manifest.agent_id,
        "gen_ai.agent.name": config.manifest.name,
        "prometa.artifact.type": "agent-bundle",
        "prometa.artifact.digest": admission.artifact_digest,
        "prometa.bundle.digest": admission.artifact_digest,
        "prometa.bundle.jti": admission.bundle.jti,
        "prometa.environment": bundle_claims["targetEnvironment"],
        "prometa.attestation.id": admission.promotion.attestation_id,
        "prometa.attestation.jti": admission.promotion.jti,
        "prometa.policy.decision_id": promotion_claims["decisionId"],
        "prometa.policy.set_digest": promotion_claims["policySetDigest"],
        "prometa.release.id": promotion_claims["releaseId"],
        "prometa.deployment.id": promotion_claims["deploymentId"],
        "prometa.runtime.target": promotion_claims["requestedRuntime"],
        "prometa.runtime.id": runtime_id,
        "prometa.runtime.version": runtime_version,
        "prometa.manifest.id": config.manifest.manifest_id,
        "prometa.manifest.version": config.manifest.version,
    }
    if config.contract.policy_digest is not None:
        attributes["prometa.policy.digest"] = config.contract.policy_digest
    if config.contract.configuration_digest is not None:
        attributes["prometa.configuration.digest"] = (
            config.contract.configuration_digest
        )
    if config.manifest.solution_name:
        attributes["prometa.solution_id"] = config.manifest.solution_name
    if request_id:
        attributes["prometa.runtime.request_id"] = request_id
    if release_source:
        attributes["prometa.release.source"] = release_source
    return attributes


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
        security_decision_emitter: Optional[SecurityDecisionEmitter] = None,
        tool_broker: Optional[ToolBroker] = None,
        human_escalation: Optional[HumanEscalation] = None,
        state_store: Optional[RuntimeStateStore] = None,
        workflow_context_resolver: Optional[WorkflowContextResolver] = None,
        workflow_state_store: Optional[WorkflowStateStore] = None,
        workflow_decision_emitter: Optional[WorkflowDecisionEmitter] = None,
        workflow_postcondition_validator: Optional[
            WorkflowPostconditionValidator
        ] = None,
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
        self.security_decision_emitter = security_decision_emitter
        self.tool_broker = tool_broker or DenyAllToolBroker()
        self.human_escalation = human_escalation
        self.state_store = state_store
        self.workflow_context_resolver = workflow_context_resolver
        self.workflow_state_store = workflow_state_store
        self.workflow_decision_emitter = workflow_decision_emitter
        self.workflow_postcondition_validator = workflow_postcondition_validator
        self._sleep = sleep
        self._circuits: Dict[str, _CircuitState] = {}

        available = available_runtime_capabilities(
            guard_evaluator=guard_evaluator,
            security_decision_emitter=security_decision_emitter,
            tool_broker=tool_broker,
            human_escalation=human_escalation,
            workflow_context_resolver=workflow_context_resolver,
            workflow_state_store=workflow_state_store,
            workflow_decision_emitter=workflow_decision_emitter,
        )
        missing = admission.config.contract.required_capabilities - available
        if missing:
            raise RuntimeExecutionError(
                "runtime_component_missing",
                "Missing runtime components: %s" % ", ".join(sorted(missing)),
            )
        if (
            admission.config.workflow_ontologies
            and workflow_postcondition_validator is None
        ):
            raise RuntimeExecutionError(
                "runtime_component_missing",
                "Missing runtime component: workflow postcondition validator",
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
        return runtime_release_identity_attributes(
            self.admission,
            runtime_id=self.runtime_id,
            runtime_version=self.runtime_version,
            request_id=request_id,
        )

    def _emit(
        self,
        name: str,
        outcome: str,
        request_id: Optional[str],
        attributes: Optional[Mapping[str, Any]] = None,
    ) -> None:
        merged = self._identity_attributes(request_id)
        if self.policy.overload_contract_id is not None:
            merged["prometa.runtime.edge_overload_contract"] = (
                self.policy.overload_contract_id
            )
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

    def emit_task_claim(self, claim: RuntimeTaskClaim) -> None:
        """Emit joinable evidence for a durable claim or recovery decision."""

        if (
            not isinstance(claim, RuntimeTaskClaim)
            or claim.attempt < 1
            or claim.sequence < 1
            or claim.transition not in {"claimed", "retried", "recovered"}
            or not isinstance(claim.lease_expires_at, datetime)
            or claim.lease_expires_at.tzinfo is None
        ):
            raise ValueError("claim must be a valid RuntimeTaskClaim")
        self._emit(
            "runtime.task.claim",
            claim.transition,
            claim.request_id,
            {
                "prometa.task.attempt": claim.attempt,
                "prometa.task.sequence": claim.sequence,
                "prometa.task.lease_expires_at": claim.lease_expires_at.astimezone(
                    timezone.utc
                )
                .isoformat()
                .replace("+00:00", "Z"),
            },
        )

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
        correlation: Optional[SecurityDecisionCorrelation] = None,
    ) -> _GuardOutcome:
        guardrails = self.admission.config.guardrails
        required = set(tool.required_guardrails if tool else ())
        if not guardrails and not required:
            return _GuardOutcome(payload)
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
        security_guardrails = tuple(
            guardrail
            for guardrail in guardrails
            if guardrail.name in applicable and guardrail.security_assurance_enabled
        )
        if security_guardrails:
            guarded_payload = self._apply_security_assurance(
                decision,
                security_guardrails,
                stage=stage,
                request_id=request_id,
                correlation=correlation,
                original_payload=payload,
            )
            requires_human = any(
                guardrail.guardrail_type == "human-approval"
                for guardrail in security_guardrails
            )
            if requires_human:
                human_decision = await self._human_review(
                    request_id,
                    stage,
                    "Signed human-approval guardrail",
                    tool,
                    guarded_payload,
                )
                return _GuardOutcome(
                    guarded_payload,
                    (human_decision.reviewer_reference,),
                )
            return _GuardOutcome(guarded_payload)
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
                human_decision = await self._human_review(
                    request_id,
                    stage,
                    "Signed human-approval guardrail",
                    tool,
                    guarded_payload,
                )
                return _GuardOutcome(
                    guarded_payload,
                    (human_decision.reviewer_reference,),
                )
            return _GuardOutcome(guarded_payload)
        if decision.action == "escalate":
            human_decision = await self._human_review(
                request_id,
                stage,
                decision.reason or "guardrail escalation",
                tool,
                payload,
            )
            return _GuardOutcome(payload, (human_decision.reviewer_reference,))
        raise RuntimeExecutionError("guard_denied")

    def _apply_security_assurance(
        self,
        decision: GuardDecision,
        guardrails: Tuple[RuntimeGuardrail, ...],
        *,
        stage: str,
        request_id: str,
        correlation: Optional[SecurityDecisionCorrelation],
        original_payload: Any,
    ) -> Any:
        emitter = self.security_decision_emitter
        policy_digest = self.admission.config.contract.policy_digest
        if emitter is None or policy_digest is None:
            raise RuntimeExecutionError("security_decision_emitter_missing")
        assessments = {
            assessment.guardrail_name: assessment
            for assessment in decision.security_assessments
            if isinstance(assessment, SecurityGuardAssessment)
        }
        if len(assessments) != len(decision.security_assessments) or set(
            assessments
        ) != {guardrail.name for guardrail in guardrails}:
            raise RuntimeExecutionError("security_decision_evidence_incomplete")

        surface = {
            "input": "input",
            "output": "output",
            "tool": "tool_request",
        }.get(stage)
        if surface is None:
            raise RuntimeExecutionError("security_decision_surface_unsupported")
        applied_actions = []
        for guardrail in guardrails:
            assessment = assessments[guardrail.name]
            mode = guardrail.enforcement_mode or "observe"
            review_threshold = guardrail.review_threshold
            enforce_threshold = guardrail.enforce_threshold
            recommended = guardrail.decision_action or "allow"
            if review_threshold is None or enforce_threshold is None:
                raise RuntimeExecutionError("security_decision_policy_incomplete")
            if not assessment.violated:
                recommended = "allow"
                applied = "allow"
            elif mode == "enforce" and (
                assessment.confidence_score >= enforce_threshold
            ):
                applied = recommended
                if (
                    applied in {"mask", "rewrite"}
                    and decision.transformed_payload is None
                ):
                    # A transformation policy must never release the original
                    # payload when its adapter omitted the transformed value.
                    applied = "deny"
            else:
                applied = "allow"
            review_required = (
                assessment.violated
                and mode == "review"
                and assessment.confidence_score >= review_threshold
            )
            runtime_decision = build_security_decision(
                request_id=request_id,
                agent_id=self.admission.config.manifest.agent_id,
                environment=self.admission.bundle.claims["targetEnvironment"],
                release_id=self.admission.promotion.claims["releaseId"],
                deployment_id=self.admission.promotion.claims["deploymentId"],
                surface=surface,
                policy_id=security_policy_identifier(guardrail.name),
                policy_version=str(self.admission.config.manifest.version),
                policy_digest=policy_digest,
                enforcement_mode=mode,
                recommended_action=recommended,
                applied_action=applied,
                review_required=review_required,
                assessment=assessment,
                correlation=correlation,
            )
            try:
                emitter.emit(runtime_decision)
            except RuntimeExecutionError:
                raise
            except Exception as exc:
                raise RuntimeExecutionError(
                    "security_decision_emit_failed", retryable=True
                ) from exc
            applied_actions.append(applied)
        if "deny" in applied_actions:
            raise RuntimeExecutionError("guard_denied")
        if any(action in {"mask", "rewrite"} for action in applied_actions):
            if decision.transformed_payload is None:
                raise RuntimeExecutionError("guard_transform_missing")
            return decision.transformed_payload
        return original_payload

    async def _human_review(
        self,
        request_id: str,
        stage: str,
        reason: str,
        tool: Optional[RuntimeTool],
        payload: Any,
    ) -> HumanEscalationDecision:
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
        return decision

    def _tool_by_name(self, name: str) -> RuntimeTool:
        matches = [
            tool
            for tool in self.admission.config.tools
            if name in {tool.name, tool.operation}
        ]
        if len(matches) != 1:
            raise RuntimeExecutionError("undeclared_or_ambiguous_tool")
        return matches[0]

    def _workflow_artifact(
        self, context: WorkflowExecutionContext
    ) -> RuntimeWorkflowOntology:
        matches = [
            artifact
            for artifact in self.admission.config.workflow_ontologies
            if artifact.ontology_id == context.ontology_id
            and artifact.version == context.version
        ]
        if len(matches) != 1:
            raise RuntimeExecutionError("workflow_context_binding_mismatch")
        return matches[0]

    @staticmethod
    def _workflow_task(
        artifact: RuntimeWorkflowOntology, tool: RuntimeTool
    ) -> Mapping[str, Any]:
        matches = []
        for task in artifact.compiled_policy["spec"].get("tasks", []):
            binding = task.get("tool")
            if not isinstance(binding, Mapping):
                continue
            if binding.get("name") not in {tool.name, tool.operation}:
                continue
            if tool.mcp_server is not None and binding.get("server") != tool.mcp_server:
                continue
            matches.append(task)
        if len(matches) != 1:
            raise RuntimeExecutionError("workflow_tool_mapping_ambiguous")
        return matches[0]

    @staticmethod
    def _validate_verified_workflow_context(
        context: Any,
    ) -> VerifiedWorkflowContext:
        if (
            not isinstance(context, VerifiedWorkflowContext)
            or not isinstance(context.current_state, str)
            or not context.current_state
            or type(context.state_version) is not int
            or context.state_version < 0
            or not isinstance(context.purpose, str)
            or not context.purpose
            or any(
                not isinstance(role, str) or not role for role in context.actor_role_ids
            )
            or len(set(context.actor_role_ids)) != len(context.actor_role_ids)
            or not isinstance(context.facts, Mapping)
            or any(
                not isinstance(fact_id, str)
                or not fact_id
                or not isinstance(fact, Mapping)
                for fact_id, fact in context.facts.items()
            )
        ):
            raise RuntimeExecutionError("invalid_verified_workflow_context")
        return context

    @staticmethod
    def _workflow_runtime_input(
        artifact: RuntimeWorkflowOntology,
        context_request: WorkflowContextRequest,
        verified: VerifiedWorkflowContext,
        *,
        phase: str,
        transition_id: Optional[str] = None,
    ) -> Mapping[str, Any]:
        request = dict(context_request.request_attributes)
        request.update(
            {
                "taskId": context_request.task_id,
                "purpose": verified.purpose,
            }
        )
        selected_transition = transition_id or context_request.transition_id
        if selected_transition is not None:
            request["transitionId"] = selected_transition
        return {
            "mode": artifact.mode,
            "phase": phase,
            "now": datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "state": {
                "current": verified.current_state,
                "instanceId": context_request.workflow.instance_id,
                "version": verified.state_version,
            },
            "request": request,
            "actor": {
                "opaqueRef": context_request.workflow.actor_ref,
                "roleIds": list(verified.actor_role_ids),
                "purpose": verified.purpose,
            },
            "facts": verified.facts,
            "approvals": list(verified.approvals),
            "evidenceRefs": list(verified.evidence_references),
            "usedIdempotencyKeys": list(verified.used_idempotency_keys),
        }

    def _emit_workflow_decision(
        self,
        evaluation: _WorkflowEvaluation,
        decision: WorkflowPolicyDecision,
    ) -> None:
        emitter = self.workflow_decision_emitter
        if emitter is None:
            raise RuntimeExecutionError("workflow_decision_emitter_missing")
        approval_references = tuple(
            sorted(
                {
                    approval.get("reference")
                    for approval in evaluation.verified_context.approvals
                    if isinstance(approval, Mapping)
                    and isinstance(approval.get("reference"), str)
                    and approval.get("reference")
                }
            )
        )
        try:
            evidence = WorkflowDecisionEvidence(
                request_id=evaluation.context_request.request_id,
                workflow_id=evaluation.artifact.ontology_id,
                workflow_version=evaluation.artifact.version,
                workflow_instance_id=(evaluation.context_request.workflow.instance_id),
                ontology_digest=evaluation.artifact.ontology_digest,
                policy_digest=evaluation.artifact.policy_digest,
                sector_snapshot_digest=evaluation.artifact.sector_snapshot_digest,
                state=evaluation.verified_context.current_state,
                state_version=evaluation.verified_context.state_version,
                task_id=evaluation.context_request.task_id,
                transition_id=decision.proposed_transition_id
                or evaluation.context_request.transition_id,
                recommended_outcome=decision.recommended_outcome,
                applied_outcome=decision.applied_outcome,
                reason_codes=decision.reason_codes,
                control_ids=decision.matched_control_ids,
                obligation_ids=decision.obligation_ids,
                fact_set_digest=workflow_fact_set_digest(
                    evaluation.verified_context.facts
                ),
                missing_fact_ids=decision.missing_fact_ids,
                stale_fact_ids=decision.stale_fact_ids,
                approval_references=approval_references,
                evidence_references=tuple(
                    sorted(evaluation.verified_context.evidence_references)
                ),
                occurred_at=datetime.now(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
            )
            emitter.emit(evidence)
        except (RuntimeExecutionError, WorkflowOntologyError):
            if evaluation.artifact.mode == "enforce":
                raise
        except Exception as exc:
            if evaluation.artifact.mode == "enforce":
                raise RuntimeExecutionError("workflow_decision_emit_failed") from exc

    async def _evaluate_workflow_before_tool(
        self,
        context: Optional[WorkflowExecutionContext],
        tool: RuntimeTool,
        arguments: Mapping[str, Any],
        request_id: str,
    ) -> Optional[_WorkflowEvaluation]:
        if not self.admission.config.workflow_ontologies:
            if context is not None:
                raise RuntimeExecutionError("workflow_context_not_admitted")
            return None
        if context is None:
            raise RuntimeExecutionError("workflow_context_required")
        artifact = self._workflow_artifact(context)
        task = self._workflow_task(artifact, tool)
        resolver = self.workflow_context_resolver
        if resolver is None:
            raise RuntimeExecutionError("workflow_context_resolver_missing")
        context_request = WorkflowContextRequest(
            request_id=request_id,
            workflow=context,
            task_id=task["id"],
            transition_id=None,
            request_attributes=dict(arguments),
        )
        try:
            verified = self._validate_verified_workflow_context(
                await resolver.resolve(context_request)
            )
        except asyncio.CancelledError:
            raise
        except RuntimeExecutionError:
            raise
        except Exception as exc:
            raise RuntimeExecutionError("workflow_context_resolve_failed") from exc
        runtime_input = self._workflow_runtime_input(
            artifact,
            context_request,
            verified,
            phase="pre_action",
        )
        decision = evaluate_workflow_policy(artifact.compiled_policy, runtime_input)
        evaluation = _WorkflowEvaluation(
            artifact=artifact,
            task=task,
            context_request=context_request,
            verified_context=verified,
            decision=decision,
            runtime_input=runtime_input,
        )
        self._emit_workflow_decision(evaluation, decision)
        if decision.applied_outcome != "allow":
            raise RuntimeExecutionError(
                "workflow_policy_%s" % decision.recommended_outcome
            )
        return evaluation

    async def _mark_workflow_indeterminate(
        self,
        evaluation: _WorkflowEvaluation,
        reason_code: str,
    ) -> None:
        store = self.workflow_state_store
        if store is None:
            raise RuntimeExecutionError("workflow_state_store_missing")
        indeterminate = WorkflowPolicyDecision(
            recommended_outcome="indeterminate",
            applied_outcome="deny",
            reason_codes=(reason_code,),
            matched_control_ids=evaluation.decision.matched_control_ids,
            missing_fact_ids=(),
            stale_fact_ids=(),
            obligation_ids=evaluation.decision.obligation_ids,
            evidence_requirement_ids=(),
            proposed_transition_id=evaluation.decision.proposed_transition_id,
            proposed_state=None,
            counterfactual_reason_codes=("reconcile_before_retry",),
        )
        try:
            await store.mark_indeterminate(
                WorkflowIndeterminateRequest(
                    request_id=evaluation.context_request.request_id,
                    workflow=evaluation.context_request.workflow,
                    state=evaluation.verified_context.current_state,
                    state_version=evaluation.verified_context.state_version,
                    task_id=evaluation.context_request.task_id,
                    transition_id=evaluation.decision.proposed_transition_id,
                    reason_code=reason_code,
                    ontology_digest=evaluation.artifact.ontology_digest,
                    policy_digest=evaluation.artifact.policy_digest,
                    sector_snapshot_digest=(evaluation.artifact.sector_snapshot_digest),
                )
            )
        except asyncio.CancelledError:
            raise
        except RuntimeExecutionError:
            raise
        except Exception as exc:
            raise RuntimeExecutionError("workflow_state_store_failed") from exc
        self._emit_workflow_decision(evaluation, indeterminate)

    async def _commit_workflow_after_tool(
        self,
        evaluation: Optional[_WorkflowEvaluation],
        tool: RuntimeTool,
        result: ToolInvocationResult,
    ) -> None:
        if evaluation is None or evaluation.decision.proposed_state is None:
            return
        validator = self.workflow_postcondition_validator
        store = self.workflow_state_store
        if validator is None or store is None:
            raise RuntimeExecutionError("runtime_component_missing")
        try:
            refreshed = self._validate_verified_workflow_context(
                await validator.validate(
                    WorkflowPostconditionRequest(
                        context_request=evaluation.context_request,
                        prior_context=evaluation.verified_context,
                        proposed_state=evaluation.decision.proposed_state,
                        tool_audit_reference=result.audit_reference,
                        tool_result=result.output,
                    )
                )
            )
        except asyncio.CancelledError:
            raise
        except RuntimeExecutionError:
            if tool.side_effects != "read-only":
                await self._mark_workflow_indeterminate(
                    evaluation, "workflow_postcondition_indeterminate"
                )
            raise
        except Exception as exc:
            if tool.side_effects != "read-only":
                await self._mark_workflow_indeterminate(
                    evaluation, "workflow_postcondition_unavailable"
                )
            raise RuntimeExecutionError(
                "workflow_postcondition_validation_failed"
            ) from exc
        if (
            refreshed.current_state != evaluation.verified_context.current_state
            or refreshed.state_version != evaluation.verified_context.state_version
        ):
            if tool.side_effects != "read-only":
                await self._mark_workflow_indeterminate(
                    evaluation, "workflow_state_changed_after_side_effect"
                )
            raise RuntimeExecutionError("workflow_state_conflict")
        runtime_input = self._workflow_runtime_input(
            evaluation.artifact,
            evaluation.context_request,
            refreshed,
            phase="state_commit",
            transition_id=evaluation.decision.proposed_transition_id,
        )
        decision = evaluate_workflow_policy(
            evaluation.artifact.compiled_policy, runtime_input
        )
        refreshed_evaluation = _WorkflowEvaluation(
            artifact=evaluation.artifact,
            task=evaluation.task,
            context_request=evaluation.context_request,
            verified_context=refreshed,
            decision=decision,
            runtime_input=runtime_input,
        )
        self._emit_workflow_decision(refreshed_evaluation, decision)
        if decision.applied_outcome != "allow" or decision.proposed_state is None:
            if (
                evaluation.artifact.mode == "observe"
                and decision.applied_outcome == "allow"
            ):
                return
            if tool.side_effects != "read-only":
                await self._mark_workflow_indeterminate(
                    refreshed_evaluation,
                    "workflow_postcondition_indeterminate",
                )
            raise RuntimeExecutionError("workflow_postcondition_denied")
        idempotency_key = runtime_input["request"].get("idempotencyKey")
        try:
            committed = await store.compare_and_set(
                WorkflowStateCommitRequest(
                    request_id=evaluation.context_request.request_id,
                    workflow=evaluation.context_request.workflow,
                    expected_state=refreshed.current_state,
                    expected_version=refreshed.state_version,
                    next_state=decision.proposed_state,
                    transition_id=decision.proposed_transition_id or "",
                    ontology_digest=evaluation.artifact.ontology_digest,
                    policy_digest=evaluation.artifact.policy_digest,
                    sector_snapshot_digest=(evaluation.artifact.sector_snapshot_digest),
                    approval_references=tuple(
                        sorted(
                            {
                                approval.get("reference")
                                for approval in refreshed.approvals
                                if isinstance(approval, Mapping)
                                and isinstance(approval.get("reference"), str)
                            }
                        )
                    ),
                    evidence_references=tuple(sorted(refreshed.evidence_references)),
                    idempotency_key=(
                        idempotency_key if isinstance(idempotency_key, str) else None
                    ),
                )
            )
        except asyncio.CancelledError:
            raise
        except RuntimeExecutionError:
            raise
        except Exception as exc:
            if tool.side_effects != "read-only":
                await self._mark_workflow_indeterminate(
                    refreshed_evaluation, "workflow_state_store_unavailable"
                )
            raise RuntimeExecutionError("workflow_state_store_failed") from exc
        if not committed:
            if tool.side_effects != "read-only":
                await self._mark_workflow_indeterminate(
                    refreshed_evaluation, "workflow_state_compare_and_set_failed"
                )
            raise RuntimeExecutionError("workflow_state_conflict")

    async def _invoke_tool(
        self,
        call: ModelToolCall,
        request_id: str,
        correlation: Optional[SecurityDecisionCorrelation] = None,
        workflow_context: Optional[WorkflowExecutionContext] = None,
    ) -> ToolInvocationResult:
        tool = self._tool_by_name(call.name)
        arguments = self._validate_schema(
            "tool_input", call.arguments, tool.input_schema, request_id
        )
        workflow_evaluation = await self._evaluate_workflow_before_tool(
            workflow_context,
            tool,
            arguments,
            request_id,
        )
        guard_outcome = await self._guard(
            "tool",
            arguments,
            request_id,
            tool=tool,
            correlation=correlation,
        )
        arguments = guard_outcome.payload
        approval_references = list(guard_outcome.approval_references)
        if self.admission.config.guardrails or tool.required_guardrails:
            arguments = self._validate_schema(
                "tool_input", arguments, tool.input_schema, request_id
            )
        if tool.approval_required:
            approval = await self._human_review(
                request_id,
                "tool",
                "Tool %s requires human approval" % tool.operation,
                tool,
                arguments,
            )
            approval_references.append(approval.reviewer_reference)
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
                        agent_id=self.admission.config.manifest.agent_id,
                        release_id=self.admission.promotion.claims["releaseId"],
                        deployment_id=self.admission.promotion.claims["deploymentId"],
                        environment=self.admission.bundle.claims["targetEnvironment"],
                        granted_scopes=self.admission.config.granted_scopes,
                        approval_references=tuple(approval_references),
                    )
                ),
                timeout=self.policy.tool_timeout_seconds,
            )
            if not isinstance(result, ToolInvocationResult):
                raise RuntimeExecutionError("invalid_tool_result")
        except asyncio.TimeoutError as exc:
            if workflow_evaluation is not None and tool.side_effects != "read-only":
                await self._mark_workflow_indeterminate(
                    workflow_evaluation, "tool_outcome_indeterminate"
                )
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
            if workflow_evaluation is not None and tool.side_effects != "read-only":
                await self._mark_workflow_indeterminate(
                    workflow_evaluation, "tool_outcome_indeterminate"
                )
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
            if workflow_evaluation is not None and tool.side_effects != "read-only":
                await self._mark_workflow_indeterminate(
                    workflow_evaluation, "tool_outcome_indeterminate"
                )
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
        await self._commit_workflow_after_tool(
            workflow_evaluation,
            tool,
            result,
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
        security_correlation: Optional[SecurityDecisionCorrelation] = None,
        workflow_context: Optional[WorkflowExecutionContext] = None,
    ) -> RuntimeExecutionResult:
        if request_id is None:
            request_id = str(uuid.uuid4())
        _validate_model_identity(request_id, "request_id")
        if self.admission.config.workflow_ontologies:
            if not isinstance(workflow_context, WorkflowExecutionContext):
                raise RuntimeExecutionError("workflow_context_required")
            self._workflow_artifact(workflow_context)
        elif workflow_context is not None:
            raise RuntimeExecutionError("workflow_context_not_admitted")
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
            runtime_input = (
                await self._guard(
                    "input",
                    runtime_input,
                    request_id,
                    correlation=security_correlation,
                )
            ).payload
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
            # One invocation identifies one semantic model turn.  Retries and
            # model fallback preserve it; a successful tool-result continuation
            # rotates it below because that continuation is a new model turn.
            model_invocation_id = str(uuid.uuid4())

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
                        {
                            "gen_ai.request.model": model.model_name,
                            "prometa.model.invocation_id": model_invocation_id,
                        },
                    )
                if not self._circuit_available(model, request_id):
                    last_error = RuntimeExecutionError("circuit_open", retryable=True)
                    continue

                messages = initial_messages
                steps = 0
                retry_attempt = 1
                while retry_attempt <= self.policy.max_attempts_per_model:
                    attempts += 1
                    model_attempt_id = str(uuid.uuid4())
                    self._emit(
                        "runtime.model.attempt",
                        "started",
                        request_id,
                        {
                            "gen_ai.request.model": model.model_name,
                            "prometa.model.provider": model.provider,
                            "prometa.model.invocation_id": model_invocation_id,
                            "prometa.model.attempt_id": model_attempt_id,
                            "retry.attempt_number": retry_attempt,
                        },
                    )
                    try:
                        response = await asyncio.wait_for(
                            self.model_adapter.invoke(
                                ModelInvocationRequest(
                                    runtime_request_id=request_id,
                                    model_invocation_id=model_invocation_id,
                                    model_attempt_id=model_attempt_id,
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
                                "prometa.model.invocation_id": model_invocation_id,
                                "prometa.model.attempt_id": model_attempt_id,
                                "prometa.model.engine_request_id": (
                                    response.engine_request_id
                                ),
                                "prometa.model.usage_record_id": (
                                    response.usage_record_id
                                ),
                                **_usage_attributes(response.usage),
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
                                result = await self._invoke_tool(
                                    call,
                                    request_id,
                                    correlation=security_correlation,
                                    workflow_context=workflow_context,
                                )
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
                            model_invocation_id = str(uuid.uuid4())
                            continue

                        output = self._validate_schema(
                            "output",
                            response.content,
                            self.admission.config.contract.output_schema,
                            request_id,
                        )
                        output = (
                            await self._guard(
                                "output",
                                output,
                                request_id,
                                correlation=security_correlation,
                            )
                        ).payload
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
                    failure_attributes = {
                        "gen_ai.request.model": model.model_name,
                        "retry.attempt_number": retry_attempt,
                        "prometa.runtime.reason": failure.code,
                        "prometa.runtime.retryable": failure.retryable,
                        "prometa.model.invocation_id": model_invocation_id,
                        "prometa.model.attempt_id": model_attempt_id,
                        "prometa.model.engine_request_id": (
                            failure.engine_request_id
                        ),
                        "prometa.model.usage_record_id": failure.usage_record_id,
                    }
                    if failure.retry_after_seconds is not None:
                        failure_attributes["retry.server_retry_after_ms"] = int(
                            failure.retry_after_seconds * 1000
                        )
                    self._emit(
                        "runtime.model.attempt",
                        "failed",
                        request_id,
                        failure_attributes,
                    )
                    if executed_tool and failure.retryable:
                        raise RuntimeExecutionError(
                            "retry_after_tool_denied",
                            "Model retry is unsafe after a tool call",
                        ) from failure
                    if not failure.retryable:
                        raise failure
                    if retry_attempt < self.policy.max_attempts_per_model:
                        retry_after = failure.retry_after_seconds
                        if (
                            retry_after is not None
                            and retry_after > self.policy.max_retry_after_seconds
                        ):
                            self._emit(
                                "runtime.retry",
                                "skipped",
                                request_id,
                                {
                                    "gen_ai.request.model": model.model_name,
                                    "retry.attempt_number": retry_attempt + 1,
                                    "retry.server_retry_after_ms": int(
                                        retry_after * 1000
                                    ),
                                    "prometa.runtime.reason": (
                                        "retry_after_exceeds_budget"
                                    ),
                                },
                            )
                            break
                        exponential_backoff = min(
                            self.policy.max_backoff_seconds,
                            self.policy.initial_backoff_seconds
                            * (2 ** (retry_attempt - 1)),
                        )
                        backoff = max(exponential_backoff, retry_after or 0.0)
                        self._emit(
                            "runtime.retry",
                            "scheduled",
                            request_id,
                            {
                                "gen_ai.request.model": model.model_name,
                                "retry.attempt_number": retry_attempt + 1,
                                "retry.backoff_ms": int(backoff * 1000),
                                "retry.backoff_source": (
                                    "server"
                                    if retry_after is not None
                                    and retry_after >= exponential_backoff
                                    else "runtime"
                                ),
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
    "RUNTIME_EDGE_OVERLOAD_CONTRACT",
    "RuntimeExecutionResult",
    "available_runtime_capabilities",
    "RuntimeKernel",
]
