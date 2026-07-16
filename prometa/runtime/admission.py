"""Combined admission and typed configuration for tenant runtime bundles.

Bundle integrity and promotion authorization remain separate signatures. This
module verifies both, cross-checks their shared identity, negotiates the
versioned runtime contract, and reserves both replay identities atomically.
It performs no network call to the Orchestra control plane.
"""

from __future__ import annotations

import json
import hashlib
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (
    Any,
    FrozenSet,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)
from .trust import (
    BundleTrustStore,
    BundleVerificationError,
    VerifiedBundle,
    VerifiedPromotionAttestation,
    verify_bundle_envelope,
    verify_promotion_attestation,
)


RUNTIME_CONTRACT_VERSION = 2
SUPPORTED_RUNTIME_CONTRACT_VERSIONS = frozenset({1, RUNTIME_CONTRACT_VERSION})
CAPABILITY_MODEL_INVOKE = "model.invoke.v1"
CAPABILITY_EVIDENCE_EMIT = "evidence.emit.v1"
CAPABILITY_SCHEMA_VALIDATE = "schema.validate.v1"
CAPABILITY_GUARD_EVALUATE = "guard.evaluate.v1"
CAPABILITY_TOOL_BROKER = "tool.broker.v1"
CAPABILITY_HUMAN_ESCALATION = "human.escalation.v1"
BASE_RUNTIME_CAPABILITIES = frozenset(
    {CAPABILITY_MODEL_INVOKE, CAPABILITY_EVIDENCE_EMIT}
)
KNOWN_RUNTIME_CAPABILITIES = frozenset(
    {
        CAPABILITY_MODEL_INVOKE,
        CAPABILITY_EVIDENCE_EMIT,
        CAPABILITY_SCHEMA_VALIDATE,
        CAPABILITY_GUARD_EVALUATE,
        CAPABILITY_TOOL_BROKER,
        CAPABILITY_HUMAN_ESCALATION,
    }
)


@dataclass(frozen=True)
class RuntimeManifest:
    manifest_id: str
    name: str
    version: int
    agent_id: str
    solution_name: Optional[str]


@dataclass(frozen=True)
class RuntimeModel:
    name: str
    provider: str
    model_name: str
    role: str
    temperature: Optional[float]
    max_output_tokens: Optional[int]
    structured_output: bool


@dataclass(frozen=True)
class RuntimeTool:
    name: str
    source: str
    operation: str
    input_schema: Mapping[str, Any]
    mcp_server: Optional[str]
    side_effects: str
    risk_level: str
    auth_binding: str
    scopes: Tuple[str, ...]
    approval_required: bool
    required_guardrails: Tuple[str, ...]


@dataclass(frozen=True)
class RuntimeGuardrail:
    name: str
    guardrail_type: str
    on_violation: str
    applies_to: Optional[str]


@dataclass(frozen=True)
class RuntimeCapabilityRequirement:
    name: str
    min_version: int
    max_version: int


@dataclass(frozen=True)
class RuntimeSecretReference:
    reference: str
    purpose: str
    provider: str
    required: bool


@dataclass(frozen=True)
class RuntimeContract:
    contract_version: int
    required_capabilities: FrozenSet[str]
    input_schema: Optional[Mapping[str, Any]]
    output_schema: Optional[Mapping[str, Any]]
    capability_requirements: Tuple[RuntimeCapabilityRequirement, ...] = ()
    policy_digest: Optional[str] = None
    configuration_digest: Optional[str] = None
    secret_references: Tuple[RuntimeSecretReference, ...] = ()


@dataclass(frozen=True)
class RuntimeBundleConfig:
    manifest: RuntimeManifest
    system_prompt: str
    models: Tuple[RuntimeModel, ...]
    primary_model: RuntimeModel
    topology: Mapping[str, Any]
    tools: Tuple[RuntimeTool, ...]
    guardrails: Tuple[RuntimeGuardrail, ...]
    contract: RuntimeContract
    mcp_servers: Tuple[str, ...] = ()
    required_scopes: Tuple[str, ...] = ()
    granted_scopes: Tuple[str, ...] = ()

    @property
    def max_iterations(self) -> int:
        value = self.topology.get("maxIterations", 1)
        return value if type(value) is int and 1 <= value <= 64 else 1


@dataclass(frozen=True)
class RuntimeAdmissionPolicy:
    expected_org_id: str
    expected_environment: str
    expected_release_id: str
    expected_deployment_id: str
    expected_runtime: str
    supported_capabilities: FrozenSet[str]
    expected_bundle_audience: str = "prometa-runtime"
    expected_promotion_audience: str = "prometa-runtime-admission"
    minimum_approvals: int = 0
    required_approval_roles: Optional[Mapping[str, int]] = None
    max_clock_skew_seconds: int = 60
    enforce_offline_lease: bool = True
    require_runtime_contract: bool = True


@dataclass(frozen=True)
class AdmittedRuntimeRelease:
    bundle: VerifiedBundle
    promotion: VerifiedPromotionAttestation
    config: RuntimeBundleConfig

    @property
    def artifact_digest(self) -> str:
        return self.bundle.artifact_digest


@dataclass(frozen=True)
class RuntimeActivationResult:
    """Durable rollout activation result for a tenant runtime replica."""

    created: bool
    activated_at: Optional[datetime] = None


class AdmissionReplayStore(Protocol):
    """Atomic replay reservation boundary for a bundle/attestation pair."""

    def reserve_pair(self, bundle_jti: str, promotion_jti: str) -> bool:
        """Return true only when both identities were newly reserved."""


class RuntimeActivationStore(Protocol):
    """Restart-safe activation boundary for a deployment's runtime replicas."""

    def activate_or_join(
        self,
        *,
        runtime_id: str,
        deployment_id: str,
        release_id: str,
        artifact_digest: str,
        bundle_jti: str,
        promotion_jti: str,
    ) -> RuntimeActivationResult:
        """Create one activation or join its exact immutable identity."""


class InMemoryAdmissionReplayStore:
    """Thread-safe single-process replay store for tests and small hosts.

    Multi-replica production hosts should implement ``AdmissionReplayStore``
    with a durable unique transaction in their tenant-owned state store.
    """

    def __init__(self) -> None:
        self._bundle_jtis = set()
        self._promotion_jtis = set()
        self._lock = threading.Lock()

    def reserve_pair(self, bundle_jti: str, promotion_jti: str) -> bool:
        with self._lock:
            if bundle_jti in self._bundle_jtis or promotion_jti in self._promotion_jtis:
                return False
            self._bundle_jtis.add(bundle_jti)
            self._promotion_jtis.add(promotion_jti)
            return True


class InMemoryRuntimeActivationStore:
    """Thread-safe activation store for tests and single-process hosts."""

    def __init__(self) -> None:
        self._activations = {}
        self._bundle_jtis = {}
        self._promotion_jtis = {}
        self._lock = threading.Lock()

    def activate_or_join(
        self,
        *,
        runtime_id: str,
        deployment_id: str,
        release_id: str,
        artifact_digest: str,
        bundle_jti: str,
        promotion_jti: str,
    ) -> RuntimeActivationResult:
        activation_key = (runtime_id, deployment_id)
        identity = (
            release_id,
            artifact_digest,
            bundle_jti,
            promotion_jti,
        )
        with self._lock:
            existing = self._activations.get(activation_key)
            if existing is not None:
                existing_identity, activated_at = existing
                if existing_identity != identity:
                    raise BundleVerificationError("runtime_activation_conflict")
                return RuntimeActivationResult(
                    created=False, activated_at=activated_at
                )
            known_digest = self._bundle_jtis.get(bundle_jti)
            if known_digest is not None and known_digest != artifact_digest:
                raise BundleVerificationError("runtime_activation_conflict")
            if promotion_jti in self._promotion_jtis:
                raise BundleVerificationError("runtime_activation_conflict")
            activated_at = datetime.now(timezone.utc)
            self._activations[activation_key] = (identity, activated_at)
            self._bundle_jtis[bundle_jti] = artifact_digest
            self._promotion_jtis[promotion_jti] = activation_key
            return RuntimeActivationResult(created=True, activated_at=activated_at)


def _mapping(value: Any, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BundleVerificationError(code)
    return value


def _sequence(value: Any, code: str, maximum: int = 128) -> Sequence[Any]:
    if not isinstance(value, list) or len(value) > maximum:
        raise BundleVerificationError(code)
    return value


def _string(value: Mapping[str, Any], key: str, code: str) -> str:
    candidate = value.get(key)
    if not isinstance(candidate, str) or not candidate.strip():
        raise BundleVerificationError(code, "Missing or invalid %s" % key)
    if candidate != candidate.strip():
        raise BundleVerificationError(code, "Invalid whitespace in %s" % key)
    return candidate


def _optional_string(value: Mapping[str, Any], key: str, code: str) -> Optional[str]:
    candidate = value.get(key)
    if candidate is None:
        return None
    if not isinstance(candidate, str) or not candidate.strip():
        raise BundleVerificationError(code)
    return candidate


def _optional_number(value: Mapping[str, Any], key: str, code: str) -> Optional[float]:
    candidate = value.get(key)
    if candidate is None:
        return None
    if isinstance(candidate, bool) or not isinstance(candidate, (int, float)):
        raise BundleVerificationError(code)
    numeric = float(candidate)
    if not 0 <= numeric <= 2:
        raise BundleVerificationError(code)
    return numeric


def _optional_positive_int(
    value: Mapping[str, Any], key: str, code: str
) -> Optional[int]:
    candidate = value.get(key)
    if candidate is None:
        return None
    if type(candidate) is not int or candidate < 1 or candidate > 1_000_000:
        raise BundleVerificationError(code)
    return candidate


def _string_tuple(value: Any, code: str, maximum: int = 128) -> Tuple[str, ...]:
    entries = _sequence(value, code, maximum)
    if any(not isinstance(entry, str) or not entry for entry in entries):
        raise BundleVerificationError(code)
    if len(set(entries)) != len(entries):
        raise BundleVerificationError(code)
    return tuple(entries)


def _json_copy(value: Mapping[str, Any], code: str) -> Mapping[str, Any]:
    try:
        return json.loads(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            )
        )
    except (TypeError, ValueError) as exc:
        raise BundleVerificationError(code) from exc


def _canonical_digest(value: Any) -> str:
    try:
        canonical = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise BundleVerificationError("invalid_runtime_digest_projection") from exc
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _selected(value: Mapping[str, Any], keys: Sequence[str]) -> Mapping[str, Any]:
    return {key: value[key] for key in keys if key in value}


_CAPABILITY_PATTERN = re.compile(r"^(.*)\.v([1-9][0-9]*)$")
_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
_TOOL_POLICY_KEYS = (
    "name",
    "source",
    "mcpServer",
    "operation",
    "sideEffects",
    "riskLevel",
    "authBinding",
    "scopes",
    "approvalRequired",
    "requiredGuardrails",
)
_TOOL_CONFIGURATION_KEYS = (
    "name",
    "source",
    "mcpServer",
    "operation",
    "inputSchema",
    "rateLimitPerMin",
)


def _capability_parts(value: str) -> Tuple[str, int]:
    match = _CAPABILITY_PATTERN.fullmatch(value)
    if match is None or not match.group(1):
        raise BundleVerificationError("invalid_runtime_capabilities")
    return match.group(1), int(match.group(2))


def _parse_capability_requirements(
    value: Any, required_capabilities: FrozenSet[str]
) -> Tuple[RuntimeCapabilityRequirement, ...]:
    entries = _sequence(value, "invalid_runtime_capability_requirements", 64)
    parsed = []
    for entry in entries:
        requirement = _mapping(entry, "invalid_runtime_capability_requirements")
        name = _string(requirement, "name", "invalid_runtime_capability_requirements")
        minimum = requirement.get("minVersion")
        maximum = requirement.get("maxVersion")
        if (
            type(minimum) is not int
            or type(maximum) is not int
            or minimum < 1
            or maximum < minimum
        ):
            raise BundleVerificationError("invalid_runtime_capability_requirements")
        parsed.append(RuntimeCapabilityRequirement(name, minimum, maximum))
    if len({requirement.name for requirement in parsed}) != len(parsed):
        raise BundleVerificationError("invalid_runtime_capability_requirements")

    exact_by_name = {}
    for capability in required_capabilities:
        name, version = _capability_parts(capability)
        if name in exact_by_name:
            raise BundleVerificationError("invalid_runtime_capability_requirements")
        exact_by_name[name] = version
    ranges_by_name = {requirement.name: requirement for requirement in parsed}
    if set(exact_by_name) != set(ranges_by_name):
        raise BundleVerificationError("runtime_capability_requirement_mismatch")
    for name, version in exact_by_name.items():
        requirement = ranges_by_name[name]
        if not requirement.min_version <= version <= requirement.max_version:
            raise BundleVerificationError("runtime_capability_requirement_mismatch")
    return tuple(parsed)


def _parse_secret_references(value: Any) -> Tuple[RuntimeSecretReference, ...]:
    entries = _sequence(value, "invalid_runtime_secret_references", 64)
    parsed = []
    for entry in entries:
        secret = _mapping(entry, "invalid_runtime_secret_references")
        reference = _string(secret, "reference", "invalid_runtime_secret_references")
        purpose = _string(secret, "purpose", "invalid_runtime_secret_references")
        provider = _string(secret, "provider", "invalid_runtime_secret_references")
        required = secret.get("required")
        if (
            purpose != "agent-identity"
            or provider not in {"platform-vault", "environment", "external-vault"}
            or required is not True
        ):
            raise BundleVerificationError("invalid_runtime_secret_references")
        parsed.append(RuntimeSecretReference(reference, purpose, provider, True))
    if len({secret.reference for secret in parsed}) != len(parsed):
        raise BundleVerificationError("invalid_runtime_secret_references")
    return tuple(parsed)


def _verify_runtime_contract_digests(
    content: Mapping[str, Any],
    contract: Mapping[str, Any],
    input_schema: Optional[Mapping[str, Any]],
    output_schema: Optional[Mapping[str, Any]],
) -> Tuple[str, str]:
    policy_digest = contract.get("policyDigest")
    configuration_digest = contract.get("configurationDigest")
    if (
        not isinstance(policy_digest, str)
        or _DIGEST_PATTERN.fullmatch(policy_digest) is None
        or not isinstance(configuration_digest, str)
        or _DIGEST_PATTERN.fullmatch(configuration_digest) is None
    ):
        raise BundleVerificationError("invalid_runtime_contract_digest")

    raw_tools = _sequence(content.get("tools", []), "invalid_runtime_tools", 128)
    tool_mappings = [
        _mapping(tool, "invalid_runtime_tool") for tool in raw_tools
    ]
    expected_policy = _canonical_digest(
        {
            "guardrails": content.get("guardrails", []),
            "identity": content.get("identity"),
            "tools": [_selected(tool, _TOOL_POLICY_KEYS) for tool in tool_mappings],
            "requiredScopes": content.get("requiredScopes", []),
            "grantedScopes": content.get("grantedScopes", []),
        }
    )
    if policy_digest != expected_policy:
        raise BundleVerificationError("runtime_policy_digest_mismatch")

    expected_configuration = _canonical_digest(
        {
            "manifest": content.get("manifest"),
            "systemPrompt": content.get("systemPrompt"),
            "models": content.get("models"),
            "primaryModel": content.get("primaryModel"),
            "topology": content.get("topology"),
            "tools": [
                _selected(tool, _TOOL_CONFIGURATION_KEYS) for tool in tool_mappings
            ],
            "skills": content.get("skills", []),
            "knowledge": content.get("knowledge", []),
            "memory": content.get("memory", []),
            "subAgents": content.get("subAgents", []),
            "workflows": content.get("workflows", []),
            "triggers": content.get("triggers", []),
            "evaluation": content.get("evaluation", []),
            "inputSchema": input_schema,
            "outputSchema": output_schema,
            "mcpServers": content.get("mcpServers", []),
        }
    )
    if configuration_digest != expected_configuration:
        raise BundleVerificationError("runtime_configuration_digest_mismatch")
    return policy_digest, configuration_digest


def _reject_remote_refs(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if key == "$ref" and isinstance(child, str) and not child.startswith("#"):
                raise BundleVerificationError("remote_schema_ref_denied")
            _reject_remote_refs(child)
    elif isinstance(value, list):
        for child in value:
            _reject_remote_refs(child)


def _validate_json_schema(value: Any, code: str) -> Optional[Mapping[str, Any]]:
    if value is None:
        return None
    schema = _json_copy(_mapping(value, code), code)
    _reject_remote_refs(schema)
    try:
        from jsonschema import Draft202012Validator
        from jsonschema.exceptions import SchemaError
    except ImportError as exc:  # pragma: no cover - core-only smoke owns this path
        raise BundleVerificationError(
            "runtime_dependency_missing",
            "Install prometa-sdk[runtime] to validate runtime schemas",
        ) from exc
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise BundleVerificationError(code, "Invalid JSON Schema") from exc
    return schema


def _parse_model(value: Any) -> RuntimeModel:
    model = _mapping(value, "invalid_runtime_model")
    role = _string(model, "role", "invalid_runtime_model")
    if role not in {"primary", "router", "embedding"}:
        raise BundleVerificationError("invalid_runtime_model")
    structured = model.get("structuredOutput", False)
    if type(structured) is not bool:
        raise BundleVerificationError("invalid_runtime_model")
    return RuntimeModel(
        name=_string(model, "name", "invalid_runtime_model"),
        provider=_string(model, "provider", "invalid_runtime_model"),
        model_name=_string(model, "modelName", "invalid_runtime_model"),
        role=role,
        temperature=_optional_number(model, "temperature", "invalid_runtime_model"),
        max_output_tokens=_optional_positive_int(
            model, "maxOutputTokens", "invalid_runtime_model"
        ),
        structured_output=structured,
    )


def _parse_tool(value: Any) -> RuntimeTool:
    tool = _mapping(value, "invalid_runtime_tool")
    schema = _validate_json_schema(tool.get("inputSchema"), "invalid_tool_input_schema")
    if schema is None:
        raise BundleVerificationError("tool_input_schema_missing")
    approval = tool.get("approvalRequired", False)
    if type(approval) is not bool:
        raise BundleVerificationError("invalid_runtime_tool")
    scopes = _string_tuple(tool.get("scopes", []), "invalid_runtime_tool")
    required_guardrails = _string_tuple(
        tool.get("requiredGuardrails", []), "invalid_runtime_tool"
    )
    source = _string(tool, "source", "invalid_runtime_tool")
    side_effects = _string(tool, "sideEffects", "invalid_runtime_tool")
    risk_level = _string(tool, "riskLevel", "invalid_runtime_tool")
    auth_binding = _string(tool, "authBinding", "invalid_runtime_tool")
    mcp_server = _optional_string(tool, "mcpServer", "invalid_runtime_tool")
    if source not in {"mcp", "native", "rest", "graphql"}:
        raise BundleVerificationError("invalid_runtime_tool")
    if side_effects not in {"read-only", "write", "destructive"}:
        raise BundleVerificationError("invalid_runtime_tool")
    if risk_level not in {"low", "medium", "high", "critical"}:
        raise BundleVerificationError("invalid_runtime_tool")
    if auth_binding not in {"none", "api-key", "oauth", "service-account"}:
        raise BundleVerificationError("invalid_runtime_tool")
    if (source == "mcp") != (mcp_server is not None):
        raise BundleVerificationError("invalid_runtime_tool")
    return RuntimeTool(
        name=_string(tool, "name", "invalid_runtime_tool"),
        source=source,
        operation=_string(tool, "operation", "invalid_runtime_tool"),
        input_schema=schema,
        mcp_server=mcp_server,
        side_effects=side_effects,
        risk_level=risk_level,
        auth_binding=auth_binding,
        scopes=scopes,
        approval_required=approval,
        required_guardrails=required_guardrails,
    )


def _parse_guardrail(value: Any) -> RuntimeGuardrail:
    guardrail = _mapping(value, "invalid_runtime_guardrail")
    guardrail_type = _string(guardrail, "guardrailType", "invalid_runtime_guardrail")
    if guardrail_type not in {
        "input-filter",
        "output-filter",
        "pii-dlp",
        "secret-dlp",
        "mcp-risk-gate",
        "content-policy",
        "eval-gate",
        "cost-budget",
        "human-approval",
    }:
        raise BundleVerificationError("invalid_runtime_guardrail")
    on_violation = _string(guardrail, "onViolation", "invalid_runtime_guardrail")
    if on_violation not in {"block", "redact", "escalate", "log"}:
        raise BundleVerificationError("invalid_runtime_guardrail")
    applies_to = _optional_string(guardrail, "appliesTo", "invalid_runtime_guardrail")
    if applies_to not in {None, "input", "output", "tool-calls", "all"}:
        raise BundleVerificationError("invalid_runtime_guardrail")
    return RuntimeGuardrail(
        name=_string(guardrail, "name", "invalid_runtime_guardrail"),
        guardrail_type=guardrail_type,
        on_violation=on_violation,
        applies_to=applies_to,
    )


def parse_runtime_bundle(
    bundle: VerifiedBundle,
    *,
    supported_capabilities: Iterable[str],
    require_runtime_contract: bool = True,
) -> RuntimeBundleConfig:
    """Parse verified bytes into strict immutable execution configuration."""

    content = _mapping(bundle.content, "invalid_runtime_bundle")
    manifest_value = _mapping(content.get("manifest"), "invalid_runtime_manifest")
    agent_id = _string(manifest_value, "agentId", "invalid_runtime_manifest")
    version = manifest_value.get("version")
    if type(version) is not int or version < 1:
        raise BundleVerificationError("invalid_runtime_manifest")
    manifest = RuntimeManifest(
        manifest_id=_string(manifest_value, "id", "invalid_runtime_manifest"),
        name=_string(manifest_value, "name", "invalid_runtime_manifest"),
        version=version,
        agent_id=agent_id,
        solution_name=_optional_string(
            manifest_value, "solutionName", "invalid_runtime_manifest"
        ),
    )

    system_prompt = _string(content, "systemPrompt", "invalid_system_prompt")
    models = tuple(
        _parse_model(value)
        for value in _sequence(content.get("models"), "invalid_runtime_models", 32)
    )
    primaries = tuple(model for model in models if model.role == "primary")
    if len(primaries) != 1:
        raise BundleVerificationError("invalid_primary_model")
    if len({model.name for model in models}) != len(models):
        raise BundleVerificationError("ambiguous_runtime_model")
    if _parse_model(content.get("primaryModel")) != primaries[0]:
        raise BundleVerificationError("primary_model_mismatch")

    tools = tuple(
        _parse_tool(value)
        for value in _sequence(content.get("tools", []), "invalid_runtime_tools", 128)
    )
    mcp_servers = _string_tuple(
        content.get("mcpServers", []), "invalid_runtime_mcp_servers", 128
    )
    required_scopes = _string_tuple(
        content.get("requiredScopes", []), "invalid_runtime_scopes", 256
    )
    granted_scopes = _string_tuple(
        content.get("grantedScopes", []), "invalid_runtime_scopes", 256
    )
    guardrails = tuple(
        _parse_guardrail(value)
        for value in _sequence(
            content.get("guardrails", []), "invalid_runtime_guardrails", 128
        )
    )
    tool_identifiers = {}
    for index, tool in enumerate(tools):
        for identifier in {tool.name, tool.operation}:
            owner = tool_identifiers.setdefault(identifier, index)
            if owner != index:
                raise BundleVerificationError("ambiguous_runtime_tool")
    derived_mcp_servers = {
        tool.mcp_server for tool in tools if tool.mcp_server is not None
    }
    if set(mcp_servers) != derived_mcp_servers:
        raise BundleVerificationError("runtime_mcp_server_manifest_mismatch")
    derived_required_scopes = {scope for tool in tools for scope in tool.scopes}
    if set(required_scopes) != derived_required_scopes:
        raise BundleVerificationError("runtime_required_scope_manifest_mismatch")
    if not derived_required_scopes.issubset(set(granted_scopes)):
        raise BundleVerificationError("runtime_tool_scope_not_granted")
    if len({guardrail.name for guardrail in guardrails}) != len(guardrails):
        raise BundleVerificationError("ambiguous_runtime_guardrail")
    topology = _json_copy(
        _mapping(content.get("topology"), "invalid_runtime_topology"),
        "invalid_runtime_topology",
    )
    max_iterations = topology.get("maxIterations", 1)
    if type(max_iterations) is not int or not 1 <= max_iterations <= 64:
        raise BundleVerificationError("invalid_runtime_topology")
    if _string(topology, "pattern", "invalid_runtime_topology") != "single-react":
        raise BundleVerificationError("unsupported_runtime_topology")

    raw_contract = content.get("runtimeContract")
    if raw_contract is None:
        if require_runtime_contract:
            raise BundleVerificationError("runtime_contract_missing")
        legacy_required = set(BASE_RUNTIME_CAPABILITIES)
        if tools:
            legacy_required.add(CAPABILITY_SCHEMA_VALIDATE)
            legacy_required.add(CAPABILITY_TOOL_BROKER)
        if guardrails or any(tool.required_guardrails for tool in tools):
            legacy_required.add(CAPABILITY_GUARD_EVALUATE)
        if any(tool.approval_required for tool in tools) or any(
            guardrail.on_violation == "escalate"
            or guardrail.guardrail_type == "human-approval"
            for guardrail in guardrails
        ):
            legacy_required.add(CAPABILITY_HUMAN_ESCALATION)
        contract = RuntimeContract(
            contract_version=0,
            required_capabilities=frozenset(legacy_required),
            input_schema=None,
            output_schema=None,
        )
    else:
        contract_value = _mapping(raw_contract, "invalid_runtime_contract")
        contract_version = contract_value.get("contractVersion")
        if contract_version not in SUPPORTED_RUNTIME_CONTRACT_VERSIONS:
            raise BundleVerificationError("unsupported_runtime_contract")
        requirements = _string_tuple(
            contract_value.get("requiredCapabilities"),
            "invalid_runtime_capabilities",
            64,
        )
        required = frozenset(requirements)
        if not BASE_RUNTIME_CAPABILITIES.issubset(required):
            raise BundleVerificationError("runtime_capability_downgrade")
        input_schema = _validate_json_schema(
            contract_value.get("inputSchema"), "invalid_input_schema"
        )
        output_schema = _validate_json_schema(
            contract_value.get("outputSchema"), "invalid_output_schema"
        )
        inferred = set(BASE_RUNTIME_CAPABILITIES)
        if input_schema is not None or output_schema is not None or tools:
            inferred.add(CAPABILITY_SCHEMA_VALIDATE)
        if tools:
            inferred.add(CAPABILITY_TOOL_BROKER)
        if guardrails or any(tool.required_guardrails for tool in tools):
            inferred.add(CAPABILITY_GUARD_EVALUATE)
        if any(tool.approval_required for tool in tools) or any(
            guardrail.on_violation == "escalate"
            or guardrail.guardrail_type == "human-approval"
            for guardrail in guardrails
        ):
            inferred.add(CAPABILITY_HUMAN_ESCALATION)
        if not inferred.issubset(required):
            raise BundleVerificationError("runtime_capability_downgrade")
        capability_requirements = ()
        policy_digest = None
        configuration_digest = None
        secret_references = ()
        if contract_version == RUNTIME_CONTRACT_VERSION:
            capability_requirements = _parse_capability_requirements(
                contract_value.get("capabilityRequirements"), required
            )
            secret_references = _parse_secret_references(
                contract_value.get("secretReferences")
            )
            policy_digest, configuration_digest = _verify_runtime_contract_digests(
                content,
                contract_value,
                input_schema,
                output_schema,
            )
        contract = RuntimeContract(
            contract_version=contract_version,
            required_capabilities=required,
            input_schema=input_schema,
            output_schema=output_schema,
            capability_requirements=capability_requirements,
            policy_digest=policy_digest,
            configuration_digest=configuration_digest,
            secret_references=secret_references,
        )

    supported = frozenset(supported_capabilities)
    unknown_supported = supported - KNOWN_RUNTIME_CAPABILITIES
    if unknown_supported:
        raise BundleVerificationError("unknown_local_runtime_capability")
    missing = contract.required_capabilities - supported
    if contract.contract_version < RUNTIME_CONTRACT_VERSION and missing:
        raise BundleVerificationError(
            "unsupported_runtime_capability",
            "Unsupported runtime capabilities: %s" % ", ".join(sorted(missing)),
        )
    if contract.contract_version == RUNTIME_CONTRACT_VERSION:
        supported_versions = {}
        for capability in supported:
            name, version = _capability_parts(capability)
            supported_versions.setdefault(name, set()).add(version)
        missing_requirements = [
            requirement.name
            for requirement in contract.capability_requirements
            if not any(
                requirement.min_version <= version <= requirement.max_version
                for version in supported_versions.get(requirement.name, set())
            )
        ]
        if missing_requirements:
            raise BundleVerificationError(
                "unsupported_runtime_capability",
                "Unsupported runtime capabilities: %s"
                % ", ".join(sorted(missing_requirements)),
            )

    return RuntimeBundleConfig(
        manifest=manifest,
        system_prompt=system_prompt,
        models=models,
        primary_model=primaries[0],
        topology=topology,
        tools=tools,
        mcp_servers=mcp_servers,
        required_scopes=required_scopes,
        granted_scopes=granted_scopes,
        guardrails=guardrails,
        contract=contract,
    )


def _cross_check_release_identity(
    bundle: VerifiedBundle,
    promotion: VerifiedPromotionAttestation,
) -> None:
    manifest = _mapping(bundle.content.get("manifest"), "invalid_runtime_manifest")
    claims = promotion.claims
    if claims.get("manifestId") != manifest.get("id"):
        raise BundleVerificationError("promotion_manifest_mismatch")
    if claims.get("manifestVersion") != manifest.get("version"):
        raise BundleVerificationError("promotion_manifest_version_mismatch")
    if claims.get("agentId") != manifest.get("agentId"):
        raise BundleVerificationError("promotion_agent_mismatch")


def _verify_runtime_release(
    bundle: Mapping[str, Any],
    promotion_attestation: Mapping[str, Any],
    *,
    bundle_trust_store: BundleTrustStore,
    promotion_trust_store: BundleTrustStore,
    policy: RuntimeAdmissionPolicy,
    now: Optional[datetime] = None,
    revoked_bundle_key_ids: Iterable[str] = (),
    revoked_bundle_jtis: Iterable[str] = (),
    revoked_promotion_key_ids: Iterable[str] = (),
    revoked_promotion_jtis: Iterable[str] = (),
    revoked_attestation_ids: Iterable[str] = (),
) -> AdmittedRuntimeRelease:
    """Verify, bind, and negotiate a release before a replay/activation write."""

    verified_bundle = verify_bundle_envelope(
        bundle,
        bundle_trust_store,
        expected_org_id=policy.expected_org_id,
        expected_audience=policy.expected_bundle_audience,
        expected_environment=policy.expected_environment,
        now=now,
        revoked_key_ids=revoked_bundle_key_ids,
        revoked_jtis=revoked_bundle_jtis,
        max_clock_skew_seconds=policy.max_clock_skew_seconds,
        enforce_offline_lease=policy.enforce_offline_lease,
    )
    verified_promotion = verify_promotion_attestation(
        promotion_attestation,
        promotion_trust_store,
        expected_org_id=policy.expected_org_id,
        expected_audience=policy.expected_promotion_audience,
        expected_environment=policy.expected_environment,
        expected_artifact_digest=verified_bundle.artifact_digest,
        expected_release_id=policy.expected_release_id,
        expected_deployment_id=policy.expected_deployment_id,
        expected_runtime=policy.expected_runtime,
        minimum_approvals=policy.minimum_approvals,
        required_approval_roles=policy.required_approval_roles,
        now=now,
        revoked_key_ids=revoked_promotion_key_ids,
        revoked_jtis=revoked_promotion_jtis,
        revoked_attestation_ids=revoked_attestation_ids,
        max_clock_skew_seconds=policy.max_clock_skew_seconds,
        enforce_offline_lease=policy.enforce_offline_lease,
    )
    _cross_check_release_identity(verified_bundle, verified_promotion)
    config = parse_runtime_bundle(
        verified_bundle,
        supported_capabilities=policy.supported_capabilities,
        require_runtime_contract=policy.require_runtime_contract,
    )
    return AdmittedRuntimeRelease(
        bundle=verified_bundle,
        promotion=verified_promotion,
        config=config,
    )


def admit_runtime_release(
    bundle: Mapping[str, Any],
    promotion_attestation: Mapping[str, Any],
    *,
    bundle_trust_store: BundleTrustStore,
    promotion_trust_store: BundleTrustStore,
    replay_store: AdmissionReplayStore,
    policy: RuntimeAdmissionPolicy,
    now: Optional[datetime] = None,
    revoked_bundle_key_ids: Iterable[str] = (),
    revoked_bundle_jtis: Iterable[str] = (),
    revoked_promotion_key_ids: Iterable[str] = (),
    revoked_promotion_jtis: Iterable[str] = (),
    revoked_attestation_ids: Iterable[str] = (),
) -> AdmittedRuntimeRelease:
    """Verify, bind, negotiate, and atomically reserve one runtime release."""

    if replay_store is None:
        raise BundleVerificationError("replay_store_required")
    admitted = _verify_runtime_release(
        bundle,
        promotion_attestation,
        bundle_trust_store=bundle_trust_store,
        promotion_trust_store=promotion_trust_store,
        policy=policy,
        now=now,
        revoked_bundle_key_ids=revoked_bundle_key_ids,
        revoked_bundle_jtis=revoked_bundle_jtis,
        revoked_promotion_key_ids=revoked_promotion_key_ids,
        revoked_promotion_jtis=revoked_promotion_jtis,
        revoked_attestation_ids=revoked_attestation_ids,
    )
    if not replay_store.reserve_pair(admitted.bundle.jti, admitted.promotion.jti):
        raise BundleVerificationError("replayed_runtime_release")
    return admitted


def activate_runtime_release(
    bundle: Mapping[str, Any],
    promotion_attestation: Mapping[str, Any],
    *,
    bundle_trust_store: BundleTrustStore,
    promotion_trust_store: BundleTrustStore,
    activation_store: RuntimeActivationStore,
    runtime_id: str,
    policy: RuntimeAdmissionPolicy,
    now: Optional[datetime] = None,
    revoked_bundle_key_ids: Iterable[str] = (),
    revoked_bundle_jtis: Iterable[str] = (),
    revoked_promotion_key_ids: Iterable[str] = (),
    revoked_promotion_jtis: Iterable[str] = (),
    revoked_attestation_ids: Iterable[str] = (),
) -> Tuple[AdmittedRuntimeRelease, RuntimeActivationResult]:
    """Verify a release and create or join its exact deployment activation.

    Unlike one-shot admission, exact replicas and restarts may join an existing
    activation. The store must reject changed activation identity, promotion
    JTI reuse, and a bundle JTI bound to different artifact bytes.
    """

    if activation_store is None:
        raise BundleVerificationError("activation_store_required")
    if (
        not isinstance(runtime_id, str)
        or not runtime_id.strip()
        or runtime_id != runtime_id.strip()
        or len(runtime_id) > 128
    ):
        raise ValueError("runtime_id must be a trimmed string of 1-128 characters")
    admitted = _verify_runtime_release(
        bundle,
        promotion_attestation,
        bundle_trust_store=bundle_trust_store,
        promotion_trust_store=promotion_trust_store,
        policy=policy,
        now=now,
        revoked_bundle_key_ids=revoked_bundle_key_ids,
        revoked_bundle_jtis=revoked_bundle_jtis,
        revoked_promotion_key_ids=revoked_promotion_key_ids,
        revoked_promotion_jtis=revoked_promotion_jtis,
        revoked_attestation_ids=revoked_attestation_ids,
    )
    result = activation_store.activate_or_join(
        runtime_id=runtime_id,
        deployment_id=policy.expected_deployment_id,
        release_id=policy.expected_release_id,
        artifact_digest=admitted.artifact_digest,
        bundle_jti=admitted.bundle.jti,
        promotion_jti=admitted.promotion.jti,
    )
    if not isinstance(result, RuntimeActivationResult):
        raise BundleVerificationError("activation_store_invalid")
    return admitted, result


__all__ = [
    "RUNTIME_CONTRACT_VERSION",
    "SUPPORTED_RUNTIME_CONTRACT_VERSIONS",
    "CAPABILITY_MODEL_INVOKE",
    "CAPABILITY_EVIDENCE_EMIT",
    "CAPABILITY_SCHEMA_VALIDATE",
    "CAPABILITY_GUARD_EVALUATE",
    "CAPABILITY_TOOL_BROKER",
    "CAPABILITY_HUMAN_ESCALATION",
    "BASE_RUNTIME_CAPABILITIES",
    "KNOWN_RUNTIME_CAPABILITIES",
    "RuntimeManifest",
    "RuntimeModel",
    "RuntimeTool",
    "RuntimeGuardrail",
    "RuntimeCapabilityRequirement",
    "RuntimeSecretReference",
    "RuntimeContract",
    "RuntimeBundleConfig",
    "RuntimeAdmissionPolicy",
    "AdmittedRuntimeRelease",
    "RuntimeActivationResult",
    "AdmissionReplayStore",
    "RuntimeActivationStore",
    "InMemoryAdmissionReplayStore",
    "InMemoryRuntimeActivationStore",
    "parse_runtime_bundle",
    "admit_runtime_release",
    "activate_runtime_release",
]
