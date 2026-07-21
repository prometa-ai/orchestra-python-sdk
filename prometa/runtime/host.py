"""Tenant-deployed reference HTTP host for the optional runtime kernel.

The host activates one signed release against a tenant PostgreSQL database,
serves a bounded authenticated request API, and calls tenant-owned model and
persistence planes. Optional bootstrap pull reads one tenant-selected handoff,
and optional lifecycle receipts use a durable asynchronous outbox; the
Orchestra control plane is never in the request path.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import hashlib
import hmac
import json
import os
import signal
import ssl
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, TextIO, Tuple

from .admission import (
    AdmittedRuntimeRelease,
    RuntimeAdmissionPolicy,
    activate_runtime_release,
)
from .control_plane import (
    RuntimeControlPlaneClient,
    RuntimeControlPlaneError,
    RuntimeReleaseHandoff,
)
from .kernel import (
    EvidenceEmitter,
    GuardEvaluator,
    HumanEscalation,
    RUNTIME_EDGE_OVERLOAD_CONTRACT,
    RuntimeEvidenceEvent,
    RuntimeExecutionError,
    RuntimeExecutionPolicy,
    RuntimeExecutionResult,
    RuntimeKernel,
    available_runtime_capabilities,
    runtime_release_identity_attributes,
)
from .model_gateway import OpenAICompatibleModelAdapter
from .mcp import (
    MCP_RISK_LEVELS,
    EnvironmentMcpCredentialProvider,
    ExplicitMcpEgressPolicy,
    GovernedMcpToolBroker,
    McpBrokerPolicy,
    McpCredentialBinding,
    McpServerConfig,
    McpTransportClient,
    McpToolGrant,
    OfficialMcpTransportClient,
    official_mcp_transport_available,
)
from .postgres import (
    PostgresMcpAuditSink,
    PostgresMcpIdempotencyStore,
    PostgresRuntimeActivationStore,
    PostgresRuntimeReceiptOutbox,
    PostgresRuntimeReleaseCache,
    PostgresRuntimeStateStore,
    PostgresRuntimeTaskStore,
    RuntimePersistenceError,
    check_postgres_runtime_compatibility,
)
from .receipts import (
    RuntimeReceiptClient,
    RuntimeReceiptDispatcher,
    build_runtime_receipt,
)
from .tasks import (
    RUNTIME_TASK_LIFECYCLE_VERSION,
    RuntimeTaskClaim,
    RuntimeTaskError,
    RuntimeTaskSnapshot,
    RuntimeTaskStore,
    canonical_payload_digest,
)
from .trust import BundleTrustEntry, BundleTrustStore, BundleVerificationError


HOST_CONFIG_VERSION = 1
DEFAULT_MAX_REQUEST_BYTES = 1_048_576
DEFAULT_REQUEST_TIMEOUT_SECONDS = 60.0
_MAX_CONFIG_BYTES = 4_194_304
_MAX_EVIDENCE_BYTES = 65_536
_IDENTIFIER_CHARACTERS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:/@+-"
)
_ENVIRONMENT_NAME_FIRST = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_"
)
_ENVIRONMENT_NAME_CHARACTERS = _ENVIRONMENT_NAME_FIRST.union("0123456789")
_RUNTIME_EDGE_OVERLOAD_CONTRACT_ENV = "PROMETA_RUNTIME_EDGE_OVERLOAD_CONTRACT"


class RuntimeHostError(RuntimeError):
    """Stable host bootstrap or request-boundary failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code.replace("_", " "))


@dataclass(frozen=True)
class RuntimeHostMcpConfig:
    servers: Tuple[McpServerConfig, ...]
    grants: Tuple[McpToolGrant, ...]
    policy: McpBrokerPolicy
    egress_policy: ExplicitMcpEgressPolicy
    credential_bindings: Tuple[McpCredentialBinding, ...]
    tool_timeout_seconds: float
    reservation_timeout_seconds: float


@dataclass(frozen=True)
class RuntimeServerTlsConfig:
    """Tenant-owned server certificate and optional client-authentication trust."""

    certificate_file: Path
    private_key_file: Path
    client_ca_file: Optional[Path] = None
    require_client_certificate: bool = False


@dataclass(frozen=True)
class RuntimeHostConfig:
    tenant_id: str
    runtime_id: str
    runtime_version: str
    org_id: str
    environment: str
    release_id: str
    deployment_id: str
    runtime_target: str
    bundle: Optional[Mapping[str, Any]]
    promotion_attestation: Optional[Mapping[str, Any]]
    bundle_trust_store: BundleTrustStore
    promotion_trust_store: BundleTrustStore
    model_gateway_base_url: str
    model_gateway_api_key_env: Optional[str]
    model_gateway_endpoint_path: str
    model_gateway_timeout_seconds: float
    model_gateway_max_response_bytes: int
    database_dsn_env: str
    api_token_env: str
    request_timeout_seconds: float
    max_request_bytes: int
    control_plane_base_url: Optional[str] = None
    control_plane_attestation_id: Optional[str] = None
    control_plane_api_key_env: Optional[str] = None
    control_plane_allow_insecure_http: bool = False
    control_plane_timeout_seconds: float = 5.0
    control_plane_max_response_bytes: int = 12 * 1024 * 1024
    control_plane_max_clock_skew_seconds: int = 300
    control_plane_max_cache_age_seconds: float = 300.0
    receipt_base_url: Optional[str] = None
    receipt_api_key_env: Optional[str] = None
    receipt_timeout_seconds: float = 5.0
    receipt_poll_interval_seconds: float = 2.0
    receipt_lease_seconds: float = 30.0
    receipt_initial_backoff_seconds: float = 1.0
    receipt_max_backoff_seconds: float = 300.0
    task_recovery_enabled: bool = False
    task_recovery_lease_seconds: float = 90.0
    task_recovery_max_attempts: int = 3
    task_recovery_history_limit: int = 50
    mcp_broker: Optional[RuntimeHostMcpConfig] = None


@dataclass(frozen=True)
class RuntimeHostResponse:
    status: int
    body: Mapping[str, Any]


def _strict_json_loads(data: bytes, code: str) -> Any:
    def reject_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def reject_constant(value):
        raise ValueError("non-finite number")

    try:
        return json.loads(
            data.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        raise RuntimeHostError(code) from None


def _mapping(value: Any, code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RuntimeHostError(code)
    return value


def _exact_keys(
    value: Mapping[str, Any],
    *,
    required: Sequence[str],
    optional: Sequence[str] = (),
    code: str,
) -> None:
    keys = set(value)
    if not set(required).issubset(keys) or not keys.issubset(
        set(required).union(optional)
    ):
        raise RuntimeHostError(code)


def _identifier(name: str, value: Any, maximum: int = 128) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(character not in _IDENTIFIER_CHARACTERS for character in value)
    ):
        raise RuntimeHostError("invalid_%s" % name)
    return value


def _bounded_string(name: str, value: Any, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
    ):
        raise RuntimeHostError("invalid_%s" % name)
    return value


def _environment_name(name: str, value: Any) -> str:
    candidate = _bounded_string(name, value, 128)
    if candidate[0] not in _ENVIRONMENT_NAME_FIRST or any(
        character not in _ENVIRONMENT_NAME_CHARACTERS for character in candidate[1:]
    ):
        raise RuntimeHostError("invalid_%s" % name)
    return candidate


def _positive_number(name: str, value: Any, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeHostError("invalid_%s" % name)
    result = float(value)
    if result <= 0 or result > maximum:
        raise RuntimeHostError("invalid_%s" % name)
    return result


def _positive_integer(name: str, value: Any, maximum: int) -> int:
    if type(value) is not int or value <= 0 or value > maximum:
        raise RuntimeHostError("invalid_%s" % name)
    return value


def _boolean(name: str, value: Any) -> bool:
    if type(value) is not bool:
        raise RuntimeHostError("invalid_%s" % name)
    return value


def _service_base_url(
    name: str, value: Any, allow_insecure_http: bool
) -> str:
    candidate = _bounded_string("%s_base_url" % name, value, 2048)
    parsed = urllib.parse.urlsplit(candidate)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise RuntimeHostError("invalid_%s_base_url" % name)
    if parsed.scheme != "https" and not allow_insecure_http:
        raise RuntimeHostError("insecure_%s_base_url" % name)
    return candidate.rstrip("/")


def _string_set(name: str, value: Any) -> Optional[frozenset]:
    if value is None:
        return None
    if not isinstance(value, list) or not value or len(value) > 128:
        raise RuntimeHostError("invalid_%s" % name)
    return frozenset(_identifier(name, child, 512) for child in value)


def _string_tuple_value(
    name: str,
    value: Any,
    *,
    maximum_items: int = 256,
    maximum_length: int = 512,
) -> Tuple[str, ...]:
    if not isinstance(value, list) or len(value) > maximum_items:
        raise RuntimeHostError("invalid_%s" % name)
    result = tuple(
        _bounded_string(name, child, maximum_length) for child in value
    )
    if len(set(result)) != len(result):
        raise RuntimeHostError("invalid_%s" % name)
    return result


def _parse_trust_store(value: Any, code: str) -> BundleTrustStore:
    if not isinstance(value, list) or not value or len(value) > 32:
        raise RuntimeHostError(code)
    entries = []
    for raw in value:
        item = _mapping(raw, code)
        _exact_keys(
            item,
            required=("issuer", "keyId", "publicKeySpkiDerBase64"),
            optional=("allowedOrgIds", "allowedAudiences", "allowedEnvironments"),
            code=code,
        )
        entries.append(
            BundleTrustEntry(
                issuer=_bounded_string("trust_issuer", item["issuer"], 512),
                key_id=_identifier("trust_key_id", item["keyId"], 256),
                public_key_spki_der_base64=_bounded_string(
                    "trust_public_key", item["publicKeySpkiDerBase64"], 4096
                ),
                allowed_org_ids=_string_set("trust_org_id", item.get("allowedOrgIds")),
                allowed_audiences=_string_set(
                    "trust_audience", item.get("allowedAudiences")
                ),
                allowed_environments=_string_set(
                    "trust_environment", item.get("allowedEnvironments")
                ),
            )
        )
    try:
        return BundleTrustStore(entries)
    except ValueError:
        raise RuntimeHostError(code) from None


def _parse_mcp_host_config(value: Any) -> RuntimeHostMcpConfig:
    document = _mapping(value, "mcp_broker_config_invalid")
    _exact_keys(
        document,
        required=("servers", "grants", "policy", "egress"),
        optional=(
            "credentialBindings",
            "toolTimeoutSeconds",
            "reservationTimeoutSeconds",
        ),
        code="mcp_broker_config_invalid",
    )
    raw_servers = document["servers"]
    if not isinstance(raw_servers, list) or not 1 <= len(raw_servers) <= 64:
        raise RuntimeHostError("mcp_servers_invalid")
    servers = []
    for raw_server in raw_servers:
        item = _mapping(raw_server, "mcp_server_config_invalid")
        _exact_keys(
            item,
            required=(
                "name",
                "connectionId",
                "transport",
                "environment",
                "authMode",
                "scopes",
                "riskLevel",
            ),
            optional=(
                "endpoint",
                "command",
                "arguments",
                "workingDirectory",
                "enabled",
                "allowInsecureHttp",
                "timeoutSeconds",
                "maxResponseBytes",
            ),
            code="mcp_server_config_invalid",
        )
        try:
            servers.append(
                McpServerConfig(
                    name=_bounded_string("mcp_server_name", item["name"], 120),
                    connection_id=_identifier(
                        "mcp_server_connection_id", item["connectionId"], 200
                    ),
                    transport=_identifier(
                        "mcp_server_transport", item["transport"], 64
                    ),
                    environment=_identifier(
                        "mcp_server_environment", item["environment"], 64
                    ),
                    auth_mode=_identifier(
                        "mcp_server_auth_mode", item["authMode"], 64
                    ),
                    scopes=_string_tuple_value(
                        "mcp_server_scopes", item["scopes"], maximum_length=256
                    ),
                    risk_level=_identifier(
                        "mcp_server_risk_level", item["riskLevel"], 64
                    ),
                    endpoint=(
                        _bounded_string("mcp_server_endpoint", item["endpoint"], 2048)
                        if item.get("endpoint") is not None
                        else None
                    ),
                    command=(
                        _bounded_string("mcp_server_command", item["command"], 500)
                        if item.get("command") is not None
                        else None
                    ),
                    arguments=_string_tuple_value(
                        "mcp_server_arguments",
                        item.get("arguments", []),
                        maximum_items=128,
                    ),
                    working_directory=(
                        _bounded_string(
                            "mcp_server_working_directory",
                            item["workingDirectory"],
                            1024,
                        )
                        if item.get("workingDirectory") is not None
                        else None
                    ),
                    enabled=_boolean(
                        "mcp_server_enabled", item.get("enabled", True)
                    ),
                    allow_insecure_http=_boolean(
                        "mcp_server_allow_insecure_http",
                        item.get("allowInsecureHttp", False),
                    ),
                    timeout_seconds=_positive_number(
                        "mcp_server_timeout_seconds",
                        item.get("timeoutSeconds", 30),
                        300,
                    ),
                    max_response_bytes=_positive_integer(
                        "mcp_server_max_response_bytes",
                        item.get("maxResponseBytes", 1_048_576),
                        10_485_760,
                    ),
                )
            )
        except ValueError:
            raise RuntimeHostError("mcp_server_config_invalid") from None
    server_names = {server.name for server in servers}
    server_ids = {server.connection_id for server in servers}
    if len(server_names) != len(servers) or len(server_ids) != len(servers):
        raise RuntimeHostError("mcp_server_identity_duplicate")

    raw_grants = document["grants"]
    if not isinstance(raw_grants, list) or not 1 <= len(raw_grants) <= 1024:
        raise RuntimeHostError("mcp_grants_invalid")
    grants = []
    for raw_grant in raw_grants:
        item = _mapping(raw_grant, "mcp_grant_config_invalid")
        _exact_keys(
            item,
            required=("toolName",),
            optional=(
                "agentIds",
                "permission",
                "riskLevel",
                "serverConnectionId",
            ),
            code="mcp_grant_config_invalid",
        )
        try:
            grant = McpToolGrant(
                tool_name=_bounded_string(
                    "mcp_grant_tool_name", item["toolName"], 200
                ),
                agent_ids=_string_tuple_value(
                    "mcp_grant_agent_ids",
                    item.get("agentIds", []),
                    maximum_items=1024,
                    maximum_length=256,
                ),
                permission=_identifier(
                    "mcp_grant_permission", item.get("permission", "read"), 64
                ),
                risk_level=_identifier(
                    "mcp_grant_risk_level", item.get("riskLevel", "low"), 64
                ),
                server_connection_id=(
                    _identifier(
                        "mcp_grant_server_connection_id",
                        item["serverConnectionId"],
                        200,
                    )
                    if item.get("serverConnectionId") is not None
                    else None
                ),
            )
        except ValueError:
            raise RuntimeHostError("mcp_grant_config_invalid") from None
        if (
            grant.server_connection_id is not None
            and grant.server_connection_id not in server_ids
        ):
            raise RuntimeHostError("mcp_grant_server_unknown")
        grants.append(grant)
    if len(set(grants)) != len(grants):
        raise RuntimeHostError("mcp_grant_duplicate")

    raw_policy = _mapping(document["policy"], "mcp_policy_config_invalid")
    _exact_keys(
        raw_policy,
        required=("maxRiskLevel",),
        optional=("requireApprovalFor", "requireIdempotencyFor"),
        code="mcp_policy_config_invalid",
    )
    approval_classes = frozenset(
        _string_tuple_value(
            "mcp_require_approval_for",
            raw_policy.get("requireApprovalFor", ["write", "destructive"]),
            maximum_items=3,
            maximum_length=64,
        )
    )
    idempotency_classes = frozenset(
        _string_tuple_value(
            "mcp_require_idempotency_for",
            raw_policy.get("requireIdempotencyFor", ["write", "destructive"]),
            maximum_items=3,
            maximum_length=64,
        )
    )
    minimum_side_effect_policy = frozenset({"write", "destructive"})
    if not minimum_side_effect_policy.issubset(approval_classes) or not (
        minimum_side_effect_policy.issubset(idempotency_classes)
    ):
        raise RuntimeHostError("mcp_policy_weakened")
    try:
        policy = McpBrokerPolicy(
            max_risk_level=_identifier(
                "mcp_max_risk_level", raw_policy["maxRiskLevel"], 64
            ),
            require_approval_for=approval_classes,
            require_idempotency_for=idempotency_classes,
        )
    except ValueError:
        raise RuntimeHostError("mcp_policy_config_invalid") from None

    raw_egress = _mapping(document["egress"], "mcp_egress_config_invalid")
    _exact_keys(
        raw_egress,
        required=(),
        optional=("allowedHttpOrigins", "allowedStdioCommands"),
        code="mcp_egress_config_invalid",
    )
    try:
        egress_policy = ExplicitMcpEgressPolicy(
            allowed_http_origins=frozenset(
                _string_tuple_value(
                    "mcp_allowed_http_origins",
                    raw_egress.get("allowedHttpOrigins", []),
                    maximum_items=64,
                    maximum_length=2048,
                )
            ),
            allowed_stdio_commands=frozenset(
                _string_tuple_value(
                    "mcp_allowed_stdio_commands",
                    raw_egress.get("allowedStdioCommands", []),
                    maximum_items=64,
                    maximum_length=500,
                )
            ),
        )
    except ValueError:
        raise RuntimeHostError("mcp_egress_config_invalid") from None
    if any(not egress_policy.allows(server) for server in servers):
        raise RuntimeHostError("mcp_egress_binding_missing")

    raw_bindings = document.get("credentialBindings", [])
    if not isinstance(raw_bindings, list) or len(raw_bindings) > 64:
        raise RuntimeHostError("mcp_credential_bindings_invalid")
    bindings = []
    for raw_binding in raw_bindings:
        item = _mapping(raw_binding, "mcp_credential_binding_invalid")
        _exact_keys(
            item,
            required=("serverName", "authMode"),
            optional=("httpHeaders", "stdioEnvironment"),
            code="mcp_credential_binding_invalid",
        )
        http_headers = _mapping(
            item.get("httpHeaders", {}), "mcp_credential_binding_invalid"
        )
        stdio_environment = _mapping(
            item.get("stdioEnvironment", {}), "mcp_credential_binding_invalid"
        )
        try:
            binding = McpCredentialBinding(
                server_name=_bounded_string(
                    "mcp_credential_server_name", item["serverName"], 120
                ),
                auth_mode=_identifier(
                    "mcp_credential_auth_mode", item["authMode"], 64
                ),
                http_headers=dict(http_headers),
                stdio_environment=dict(stdio_environment),
            )
        except ValueError:
            raise RuntimeHostError("mcp_credential_binding_invalid") from None
        if binding.server_name not in server_names:
            raise RuntimeHostError("mcp_credential_server_unknown")
        bindings.append(binding)
    binding_by_server = {binding.server_name: binding for binding in bindings}
    if len(binding_by_server) != len(bindings):
        raise RuntimeHostError("mcp_credential_binding_duplicate")
    for server in servers:
        binding = binding_by_server.get(server.name)
        if server.auth_mode == "none":
            if binding is not None and binding.auth_mode != "none":
                raise RuntimeHostError("mcp_credential_auth_mismatch")
            continue
        if binding is None or binding.auth_mode != server.auth_mode:
            raise RuntimeHostError("mcp_credential_binding_missing")
        if server.transport == "streamable-http":
            if not binding.http_headers or binding.stdio_environment:
                raise RuntimeHostError("mcp_credential_transport_mismatch")
        elif not binding.stdio_environment or binding.http_headers:
            raise RuntimeHostError("mcp_credential_transport_mismatch")

    maximum_server_timeout = max(server.timeout_seconds for server in servers)
    tool_timeout_seconds = _positive_number(
        "mcp_tool_timeout_seconds",
        document.get("toolTimeoutSeconds", maximum_server_timeout),
        600,
    )
    if tool_timeout_seconds < maximum_server_timeout:
        raise RuntimeHostError("mcp_tool_timeout_too_short")
    reservation_timeout_seconds = _positive_number(
        "mcp_reservation_timeout_seconds",
        document.get(
            "reservationTimeoutSeconds", max(300, tool_timeout_seconds + 30)
        ),
        86_400,
    )
    if reservation_timeout_seconds <= tool_timeout_seconds:
        raise RuntimeHostError("mcp_reservation_timeout_too_short")
    return RuntimeHostMcpConfig(
        servers=tuple(servers),
        grants=tuple(grants),
        policy=policy,
        egress_policy=egress_policy,
        credential_bindings=tuple(bindings),
        tool_timeout_seconds=tool_timeout_seconds,
        reservation_timeout_seconds=reservation_timeout_seconds,
    )


def load_runtime_host_config(path: Path) -> RuntimeHostConfig:
    """Load strict non-secret host configuration from a mounted JSON file."""

    try:
        raw = path.read_bytes()
    except OSError:
        raise RuntimeHostError("host_config_unavailable") from None
    if len(raw) > _MAX_CONFIG_BYTES:
        raise RuntimeHostError("host_config_too_large")
    document = _mapping(
        _strict_json_loads(raw, "host_config_invalid_json"), "host_config_invalid"
    )
    _exact_keys(
        document,
        required=(
            "configVersion",
            "tenantId",
            "runtimeId",
            "runtimeVersion",
            "orgId",
            "environment",
            "releaseId",
            "deploymentId",
            "runtimeTarget",
            "bundleTrust",
            "promotionTrust",
            "modelGateway",
        ),
        optional=(
            "databaseDsnEnv",
            "apiTokenEnv",
            "requestTimeoutSeconds",
            "maxRequestBytes",
            "bundle",
            "promotionAttestation",
            "controlPlanePull",
            "receiptDelivery",
            "taskRecovery",
            "mcpBroker",
        ),
        code="host_config_invalid",
    )
    if document["configVersion"] != HOST_CONFIG_VERSION:
        raise RuntimeHostError("host_config_version_unsupported")
    mcp_broker = (
        _parse_mcp_host_config(document["mcpBroker"])
        if document.get("mcpBroker") is not None
        else None
    )
    has_bundle = "bundle" in document
    has_promotion = "promotionAttestation" in document
    has_embedded_release = has_bundle and has_promotion
    has_control_plane_pull = "controlPlanePull" in document
    if (
        has_bundle != has_promotion
        or has_embedded_release == has_control_plane_pull
    ):
        raise RuntimeHostError("release_source_invalid")
    model = _mapping(document["modelGateway"], "model_gateway_config_invalid")
    _exact_keys(
        model,
        required=("baseUrl",),
        optional=("apiKeyEnv", "endpointPath", "timeoutSeconds", "maxResponseBytes"),
        code="model_gateway_config_invalid",
    )
    api_key_env = model.get("apiKeyEnv")
    if api_key_env is not None:
        api_key_env = _environment_name("model_api_key_env", api_key_env)
    request_timeout_seconds = _positive_number(
        "request_timeout_seconds",
        document.get("requestTimeoutSeconds", DEFAULT_REQUEST_TIMEOUT_SECONDS),
        600,
    )
    max_request_bytes = _positive_integer(
        "max_request_bytes",
        document.get("maxRequestBytes", DEFAULT_MAX_REQUEST_BYTES),
        16 * 1024 * 1024,
    )
    control_plane_base_url = None
    control_plane_attestation_id = None
    control_plane_api_key_env = None
    control_plane_allow_insecure_http = False
    control_plane_timeout_seconds = 5.0
    control_plane_max_response_bytes = 12 * 1024 * 1024
    control_plane_max_clock_skew_seconds = 300
    control_plane_max_cache_age_seconds = 300.0
    if has_control_plane_pull:
        control = _mapping(
            document["controlPlanePull"], "control_plane_pull_config_invalid"
        )
        _exact_keys(
            control,
            required=("baseUrl", "attestationId", "apiKeyEnv"),
            optional=(
                "allowInsecureHttp",
                "timeoutSeconds",
                "maxResponseBytes",
                "maxClockSkewSeconds",
                "maxCacheAgeSeconds",
            ),
            code="control_plane_pull_config_invalid",
        )
        control_plane_allow_insecure_http = _boolean(
            "control_plane_allow_insecure_http",
            control.get("allowInsecureHttp", False),
        )
        control_plane_base_url = _service_base_url(
            "control_plane",
            control["baseUrl"],
            control_plane_allow_insecure_http,
        )
        control_plane_attestation_id = _identifier(
            "control_plane_attestation_id", control["attestationId"], 200
        )
        control_plane_api_key_env = _environment_name(
            "control_plane_api_key_env", control["apiKeyEnv"]
        )
        control_plane_timeout_seconds = _positive_number(
            "control_plane_timeout_seconds",
            control.get("timeoutSeconds", 5),
            60,
        )
        control_plane_max_response_bytes = _positive_integer(
            "control_plane_max_response_bytes",
            control.get("maxResponseBytes", 12 * 1024 * 1024),
            16 * 1024 * 1024,
        )
        if control_plane_max_response_bytes < 1024:
            raise RuntimeHostError("invalid_control_plane_max_response_bytes")
        control_plane_max_clock_skew_seconds = _positive_integer(
            "control_plane_max_clock_skew_seconds",
            control.get("maxClockSkewSeconds", 300),
            3600,
        )
        control_plane_max_cache_age_seconds = _positive_number(
            "control_plane_max_cache_age_seconds",
            control.get("maxCacheAgeSeconds", 300),
            7 * 86_400,
        )
    task_recovery_enabled = "taskRecovery" in document
    task_recovery_lease_seconds = max(60.0, request_timeout_seconds + 30.0)
    task_recovery_max_attempts = 3
    task_recovery_history_limit = 50
    if task_recovery_enabled:
        task_recovery = _mapping(
            document["taskRecovery"], "task_recovery_config_invalid"
        )
        _exact_keys(
            task_recovery,
            required=(),
            optional=("leaseSeconds", "maxAttempts", "historyLimit"),
            code="task_recovery_config_invalid",
        )
        task_recovery_lease_seconds = _positive_number(
            "task_recovery_lease_seconds",
            task_recovery.get("leaseSeconds", task_recovery_lease_seconds),
            3600,
        )
        task_recovery_max_attempts = _positive_integer(
            "task_recovery_max_attempts",
            task_recovery.get("maxAttempts", 3),
            20,
        )
        task_recovery_history_limit = _positive_integer(
            "task_recovery_history_limit",
            task_recovery.get("historyLimit", 50),
            100,
        )
        if task_recovery_lease_seconds <= request_timeout_seconds:
            raise RuntimeHostError("task_recovery_lease_too_short")
    receipt_base_url = None
    receipt_api_key_env = None
    receipt_timeout_seconds = 5.0
    receipt_poll_interval_seconds = 2.0
    receipt_lease_seconds = 30.0
    receipt_initial_backoff_seconds = 1.0
    receipt_max_backoff_seconds = 300.0
    if document.get("receiptDelivery") is not None:
        receipt = _mapping(
            document["receiptDelivery"], "receipt_delivery_config_invalid"
        )
        _exact_keys(
            receipt,
            required=("baseUrl", "apiKeyEnv"),
            optional=(
                "allowInsecureHttp",
                "timeoutSeconds",
                "pollIntervalSeconds",
                "leaseSeconds",
                "initialBackoffSeconds",
                "maxBackoffSeconds",
            ),
            code="receipt_delivery_config_invalid",
        )
        allow_insecure_http = _boolean(
            "receipt_allow_insecure_http",
            receipt.get("allowInsecureHttp", False),
        )
        receipt_base_url = _service_base_url(
            "receipt", receipt["baseUrl"], allow_insecure_http
        )
        receipt_api_key_env = _environment_name(
            "receipt_api_key_env", receipt["apiKeyEnv"]
        )
        receipt_timeout_seconds = _positive_number(
            "receipt_timeout_seconds", receipt.get("timeoutSeconds", 5), 60
        )
        receipt_poll_interval_seconds = _positive_number(
            "receipt_poll_interval_seconds",
            receipt.get("pollIntervalSeconds", 2),
            300,
        )
        receipt_lease_seconds = _positive_number(
            "receipt_lease_seconds", receipt.get("leaseSeconds", 30), 3600
        )
        receipt_initial_backoff_seconds = _positive_number(
            "receipt_initial_backoff_seconds",
            receipt.get("initialBackoffSeconds", 1),
            3600,
        )
        receipt_max_backoff_seconds = _positive_number(
            "receipt_max_backoff_seconds",
            receipt.get("maxBackoffSeconds", 300),
            86_400,
        )
        if receipt_lease_seconds <= receipt_timeout_seconds:
            raise RuntimeHostError("receipt_lease_too_short")
        if receipt_max_backoff_seconds < receipt_initial_backoff_seconds:
            raise RuntimeHostError("receipt_backoff_invalid")
    return RuntimeHostConfig(
        tenant_id=_identifier("tenant_id", document["tenantId"]),
        runtime_id=_identifier("runtime_id", document["runtimeId"]),
        runtime_version=_identifier("runtime_version", document["runtimeVersion"]),
        org_id=_identifier("org_id", document["orgId"]),
        environment=_identifier("environment", document["environment"]),
        release_id=_identifier("release_id", document["releaseId"]),
        deployment_id=_identifier("deployment_id", document["deploymentId"]),
        runtime_target=_identifier("runtime_target", document["runtimeTarget"]),
        bundle=(
            _mapping(document["bundle"], "bundle_config_invalid")
            if has_embedded_release
            else None
        ),
        promotion_attestation=(
            _mapping(
                document["promotionAttestation"], "promotion_config_invalid"
            )
            if has_embedded_release
            else None
        ),
        bundle_trust_store=_parse_trust_store(
            document["bundleTrust"], "bundle_trust_invalid"
        ),
        promotion_trust_store=_parse_trust_store(
            document["promotionTrust"], "promotion_trust_invalid"
        ),
        model_gateway_base_url=_bounded_string(
            "model_gateway_base_url", model["baseUrl"], 2048
        ),
        model_gateway_api_key_env=api_key_env,
        model_gateway_endpoint_path=_bounded_string(
            "model_gateway_endpoint_path",
            model.get("endpointPath", "/v1/chat/completions"),
            512,
        ),
        model_gateway_timeout_seconds=_positive_number(
            "model_gateway_timeout_seconds",
            model.get("timeoutSeconds", 30),
            300,
        ),
        model_gateway_max_response_bytes=_positive_integer(
            "model_gateway_max_response_bytes",
            model.get("maxResponseBytes", 4 * 1024 * 1024),
            16 * 1024 * 1024,
        ),
        database_dsn_env=_environment_name(
            "database_dsn_env",
            document.get("databaseDsnEnv", "PROMETA_RUNTIME_DATABASE_URL"),
        ),
        api_token_env=_environment_name(
            "api_token_env",
            document.get("apiTokenEnv", "PROMETA_RUNTIME_API_TOKEN"),
        ),
        request_timeout_seconds=request_timeout_seconds,
        max_request_bytes=max_request_bytes,
        control_plane_base_url=control_plane_base_url,
        control_plane_attestation_id=control_plane_attestation_id,
        control_plane_api_key_env=control_plane_api_key_env,
        control_plane_allow_insecure_http=control_plane_allow_insecure_http,
        control_plane_timeout_seconds=control_plane_timeout_seconds,
        control_plane_max_response_bytes=control_plane_max_response_bytes,
        control_plane_max_clock_skew_seconds=(
            control_plane_max_clock_skew_seconds
        ),
        control_plane_max_cache_age_seconds=(
            control_plane_max_cache_age_seconds
        ),
        receipt_base_url=receipt_base_url,
        receipt_api_key_env=receipt_api_key_env,
        receipt_timeout_seconds=receipt_timeout_seconds,
        receipt_poll_interval_seconds=receipt_poll_interval_seconds,
        receipt_lease_seconds=receipt_lease_seconds,
        receipt_initial_backoff_seconds=receipt_initial_backoff_seconds,
        receipt_max_backoff_seconds=receipt_max_backoff_seconds,
        task_recovery_enabled=task_recovery_enabled,
        task_recovery_lease_seconds=task_recovery_lease_seconds,
        task_recovery_max_attempts=task_recovery_max_attempts,
        task_recovery_history_limit=task_recovery_history_limit,
        mcp_broker=mcp_broker,
    )


class JsonLineEvidenceEmitter:
    """Write payload-free kernel evidence as bounded JSON lines."""

    def __init__(self, stream: Optional[TextIO] = None) -> None:
        self._stream = stream or sys.stdout
        self._lock = threading.Lock()

    def emit(self, event: RuntimeEvidenceEvent) -> None:
        body = json.dumps(
            {
                "type": "prometa.runtime.evidence",
                "name": event.name,
                "outcome": event.outcome,
                "occurredAt": event.occurred_at,
                "attributes": dict(event.attributes),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        if len(body.encode("utf-8")) > _MAX_EVIDENCE_BYTES:
            raise RuntimeHostError("evidence_event_too_large")
        with self._lock:
            self._stream.write(body + "\n")
            self._stream.flush()


class _KernelLoop:
    def __init__(self, kernel: RuntimeKernel) -> None:
        self._kernel = kernel
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run,
            name="prometa-runtime-kernel",
            daemon=True,
        )
        self._closed = False
        self._thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()
        pending = asyncio.all_tasks(self._loop)
        for task in pending:
            task.cancel()
        if pending:
            self._loop.run_until_complete(
                asyncio.gather(*pending, return_exceptions=True)
            )
        self._loop.close()

    def execute(
        self, payload: Any, request_id: str, timeout_seconds: float
    ) -> RuntimeExecutionResult:
        if self._closed:
            raise RuntimeHostError("runtime_host_stopped")
        future = asyncio.run_coroutine_threadsafe(
            self._kernel.execute(payload, request_id=request_id),
            self._loop,
        )
        try:
            return future.result(timeout=timeout_seconds)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise RuntimeHostError("runtime_request_timeout") from None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            raise RuntimeHostError("runtime_shutdown_timeout")


class ReferenceRuntimeHost:
    """Authenticated HTTP application around one activated runtime kernel."""

    def __init__(
        self,
        kernel: RuntimeKernel,
        *,
        api_token: str,
        request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
        max_request_bytes: int = DEFAULT_MAX_REQUEST_BYTES,
        receipt_dispatcher: Optional[RuntimeReceiptDispatcher] = None,
        release_source: str = "embedded",
        task_store: Optional[RuntimeTaskStore] = None,
        task_lease_seconds: float = 90.0,
        task_max_attempts: int = 3,
        task_history_limit: int = 50,
    ) -> None:
        if not isinstance(api_token, str) or len(api_token.encode("utf-8")) < 32:
            raise RuntimeHostError("api_token_too_short")
        self.kernel = kernel
        self._token_digest = hashlib.sha256(api_token.encode("utf-8")).digest()
        self.request_timeout_seconds = _positive_number(
            "request_timeout_seconds", request_timeout_seconds, 600
        )
        self.max_request_bytes = _positive_integer(
            "max_request_bytes", max_request_bytes, 16 * 1024 * 1024
        )
        self._inflight = set()
        self._inflight_condition = threading.Condition()
        self._closing = False
        self._receipt_dispatcher = receipt_dispatcher
        if release_source not in {"embedded", "control_plane", "cache"}:
            raise RuntimeHostError("release_source_invalid")
        self.release_source = release_source
        self._task_store = task_store
        self.task_recovery_enabled = task_store is not None
        self.task_lease_seconds = _positive_number(
            "task_lease_seconds", task_lease_seconds, 3600
        )
        self.task_max_attempts = _positive_integer(
            "task_max_attempts", task_max_attempts, 20
        )
        self.task_history_limit = _positive_integer(
            "task_history_limit", task_history_limit, 100
        )
        if self.task_recovery_enabled:
            if kernel.admission.config.tools:
                raise RuntimeHostError("task_recovery_side_effects_unsupported")
            if self.task_lease_seconds <= self.request_timeout_seconds:
                raise RuntimeHostError("task_recovery_lease_too_short")
        self._runner = _KernelLoop(kernel)

    def _authorized(self, headers: Mapping[str, str]) -> bool:
        value = headers.get("authorization", "")
        if not value.startswith("Bearer "):
            return False
        candidate = value[7:]
        digest = hashlib.sha256(candidate.encode("utf-8")).digest()
        return hmac.compare_digest(digest, self._token_digest)

    @staticmethod
    def _error(status: int, code: str) -> RuntimeHostResponse:
        return RuntimeHostResponse(status=status, body={"error": {"code": code}})

    def _readiness(self) -> RuntimeHostResponse:
        return RuntimeHostResponse(status=200, body={"status": "ready"})

    @staticmethod
    def _iso(value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        return (
            value.astimezone(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

    @classmethod
    def _task_error(cls, error: Exception) -> RuntimeHostResponse:
        code = getattr(error, "code", "task_store_unavailable")
        if code == "task_payload_too_large":
            return cls._error(413, code)
        if code == "task_payload_not_json":
            return cls._error(422, code)
        conflicts = {
            "task_identity_conflict",
            "task_in_progress",
            "task_already_completed",
            "task_terminal",
            "task_recovery_blocked",
            "task_attempts_exhausted",
            "task_lease_lost",
        }
        return cls._error(409 if code in conflicts else 503, code)

    @classmethod
    def _task_snapshot(cls, snapshot: RuntimeTaskSnapshot) -> RuntimeHostResponse:
        record = snapshot.record
        lifecycle = []
        for event in snapshot.events:
            item = {
                "sequence": event.sequence,
                "transition": event.transition,
                "status": event.status,
                "attempt": event.attempt,
                "occurredAt": cls._iso(event.occurred_at),
            }
            if event.reason is not None:
                item["reason"] = event.reason
            lifecycle.append(item)
        return RuntimeHostResponse(
            status=200,
            body={
                "taskLifecycleVersion": RUNTIME_TASK_LIFECYCLE_VERSION,
                "requestId": record.request_id,
                "artifactDigest": record.artifact_digest,
                "releaseId": record.release_id,
                "deploymentId": record.deployment_id,
                "status": record.status,
                "attempt": record.attempt,
                "maxAttempts": record.max_attempts,
                "recoverable": record.recoverable,
                "sequence": record.sequence,
                "leaseExpiresAt": cls._iso(record.lease_expires_at),
                "lastErrorCode": record.last_error_code,
                "outputDigest": record.output_digest,
                "modelName": record.model_name,
                "modelAttempts": record.model_attempts,
                "toolCalls": record.tool_calls,
                "usedFallback": record.used_fallback,
                "createdAt": cls._iso(record.created_at),
                "updatedAt": cls._iso(record.updated_at),
                "completedAt": cls._iso(record.completed_at),
                "historyTruncated": snapshot.history_truncated,
                "lifecycle": lifecycle,
            },
        )

    def _task_status(self, encoded_request_id: str) -> RuntimeHostResponse:
        if self._task_store is None:
            return self._error(404, "not_found")
        try:
            request_id = _identifier(
                "request_id",
                urllib.parse.unquote(encoded_request_id, errors="strict"),
                256,
            )
        except (RuntimeHostError, UnicodeError):
            return self._error(400, "invalid_request_id")
        try:
            snapshot = self._task_store.get(
                request_id, history_limit=self.task_history_limit
            )
        except (RuntimePersistenceError, RuntimeTaskError) as exc:
            return self._task_error(exc)
        except Exception:
            return self._error(503, "task_store_unavailable")
        if snapshot is None:
            return self._error(404, "task_not_found")
        return self._task_snapshot(snapshot)

    def _claim_task(self, request_id: str, payload: Any) -> Optional[RuntimeTaskClaim]:
        if self._task_store is None:
            return None
        promotion = self.kernel.admission.promotion.claims
        return self._task_store.claim(
            request_id,
            input_digest=canonical_payload_digest(payload),
            artifact_digest=self.kernel.admission.artifact_digest,
            release_id=promotion["releaseId"],
            deployment_id=promotion["deploymentId"],
            recoverable=True,
            max_attempts=self.task_max_attempts,
            lease_seconds=self.task_lease_seconds,
        )

    def _record_task_failure(
        self,
        claim: Optional[RuntimeTaskClaim],
        *,
        reason: str,
        retryable: bool,
    ) -> Optional[RuntimeHostResponse]:
        if claim is None or self._task_store is None:
            return None
        try:
            self._task_store.fail(
                claim,
                reason=reason,
                retryable=retryable,
            )
        except (RuntimePersistenceError, RuntimeTaskError) as exc:
            return self._task_error(exc)
        except Exception:
            return self._error(503, "task_store_unavailable")
        return None

    def handle(
        self,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes = b"",
    ) -> RuntimeHostResponse:
        normalized_headers = {
            str(key).lower(): str(value) for key, value in headers.items()
        }
        if method == "GET" and path == "/healthz":
            return RuntimeHostResponse(status=200, body={"status": "ok"})
        if method == "GET" and path == "/readyz":
            return self._readiness()
        task_prefix = "/v1/runtime/tasks/"
        if path.startswith(task_prefix):
            if method != "GET":
                return self._error(405, "method_not_allowed")
            if not self._authorized(normalized_headers):
                return self._error(401, "unauthorized")
            return self._task_status(path[len(task_prefix) :])
        if path != "/v1/runtime/execute":
            return self._error(404, "not_found")
        if method != "POST":
            return self._error(405, "method_not_allowed")
        if not self._authorized(normalized_headers):
            return self._error(401, "unauthorized")
        content_type = normalized_headers.get("content-type", "").split(";", 1)[0]
        if content_type.strip().lower() != "application/json":
            return self._error(415, "content_type_unsupported")
        if len(body) > self.max_request_bytes:
            return self._error(413, "request_too_large")
        try:
            request = _mapping(
                _strict_json_loads(body, "request_invalid_json"), "request_invalid"
            )
            _exact_keys(
                request,
                required=("requestId", "input"),
                code="request_invalid",
            )
            request_id = _identifier("request_id", request["requestId"], 256)
        except RuntimeHostError as exc:
            return self._error(400, exc.code)
        with self._inflight_condition:
            if self._closing:
                return self._error(503, "runtime_host_stopping")
            if request_id in self._inflight:
                return self._error(409, "request_in_progress")
            self._inflight.add(request_id)
        claim: Optional[RuntimeTaskClaim] = None
        result: Optional[RuntimeExecutionResult] = None
        failure: Optional[RuntimeHostResponse] = None
        failure_code: Optional[str] = None
        failure_retryable = False
        try:
            claim = self._claim_task(request_id, request["input"])
            if claim is not None:
                self.kernel.emit_task_claim(claim)
            result = self._runner.execute(
                request["input"], request_id, self.request_timeout_seconds
            )
        except (RuntimePersistenceError, RuntimeTaskError) as exc:
            failure = self._task_error(exc)
            failure_code = exc.code
        except RuntimeHostError as exc:
            status = 504 if exc.code == "runtime_request_timeout" else 503
            failure = self._error(status, exc.code)
            failure_code = exc.code
            failure_retryable = True
        except RuntimeExecutionError as exc:
            if exc.code in {"input_schema_invalid", "request_payload_not_json"}:
                status = 422
            elif exc.retryable or exc.code in {
                "state_store_failed",
                "evidence_emit_failed",
                "gateway_unavailable",
                "model_transport_failed",
                "circuit_open",
            }:
                status = 503
            else:
                status = 500
            failure = self._error(status, exc.code)
            failure_code = exc.code
            failure_retryable = status == 503
        except Exception:
            failure = self._error(500, "runtime_internal_error")
            failure_code = "runtime_internal_error"
            failure_retryable = True
        finally:
            with self._inflight_condition:
                self._inflight.discard(request_id)
                self._inflight_condition.notify_all()
        if failure is not None:
            if claim is not None and failure_code is not None:
                task_failure = self._record_task_failure(
                    claim,
                    reason=failure_code,
                    retryable=failure_retryable,
                )
                if task_failure is not None:
                    return task_failure
            return failure
        if result is None:
            return self._error(500, "runtime_internal_error")
        if claim is not None and self._task_store is not None:
            try:
                output_digest = canonical_payload_digest(result.output)
            except RuntimeTaskError as exc:
                task_failure = self._record_task_failure(
                    claim,
                    reason=exc.code,
                    retryable=False,
                )
                return task_failure or self._error(500, exc.code)
            try:
                self._task_store.complete(
                    claim,
                    output_digest=output_digest,
                    model_name=result.model_name,
                    model_attempts=result.attempts,
                    tool_calls=result.tool_calls,
                    used_fallback=result.used_fallback,
                )
            except (RuntimePersistenceError, RuntimeTaskError) as exc:
                return self._task_error(exc)
            except Exception:
                return self._error(503, "task_store_unavailable")
        return RuntimeHostResponse(
            status=200,
            body={
                "requestId": result.request_id,
                "output": result.output,
                "modelName": result.model_name,
                "attempts": result.attempts,
                "toolCalls": result.tool_calls,
                "usedFallback": result.used_fallback,
            },
        )

    def close(self, drain_timeout_seconds: float = 30.0) -> None:
        timeout = _positive_number("drain_timeout_seconds", drain_timeout_seconds, 600)
        deadline = time.monotonic() + timeout
        with self._inflight_condition:
            self._closing = True
            while self._inflight:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._inflight_condition.wait(timeout=remaining)
            drained = not self._inflight
        try:
            self._runner.close()
        finally:
            if self._receipt_dispatcher is not None:
                self._receipt_dispatcher.close()
        if not drained:
            raise RuntimeHostError("runtime_shutdown_timeout")


def _lifecycle_receipt_id(
    config: RuntimeHostConfig, attestation_id: str, transition: str
) -> str:
    identity = "\x00".join(
        (
            config.tenant_id,
            config.runtime_id,
            config.deployment_id,
            config.release_id,
            attestation_id,
            transition,
        )
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
    return "runtime-%s-%s" % (transition, digest)


@dataclass(frozen=True)
class _ResolvedReleaseMaterial:
    bundle: Mapping[str, Any]
    promotion_attestation: Mapping[str, Any]
    source: str
    pulled_handoff: Optional[RuntimeReleaseHandoff] = None
    cache: Optional[PostgresRuntimeReleaseCache] = None


def _resolve_release_material(
    config: RuntimeHostConfig,
    *,
    environment: Mapping[str, str],
    dsn: str,
    now: datetime,
) -> _ResolvedReleaseMaterial:
    if config.bundle is not None and config.promotion_attestation is not None:
        return _ResolvedReleaseMaterial(
            bundle=config.bundle,
            promotion_attestation=config.promotion_attestation,
            source="embedded",
        )
    if (
        config.control_plane_base_url is None
        or config.control_plane_attestation_id is None
        or config.control_plane_api_key_env is None
    ):
        raise RuntimeHostError("release_source_invalid")
    api_key = environment.get(config.control_plane_api_key_env, "")
    if not api_key:
        raise RuntimeHostError("control_plane_api_key_missing")
    if len(api_key.encode("utf-8")) < 16:
        raise RuntimeHostError("control_plane_api_key_too_short")

    cache = PostgresRuntimeReleaseCache(dsn, tenant_id=config.tenant_id)
    try:
        client = RuntimeControlPlaneClient(
            config.control_plane_base_url,
            api_key,
            timeout_seconds=config.control_plane_timeout_seconds,
            max_response_bytes=config.control_plane_max_response_bytes,
            max_clock_skew_seconds=(
                config.control_plane_max_clock_skew_seconds
            ),
            allow_insecure_http=config.control_plane_allow_insecure_http,
        )
        handoff = client.fetch_release(
            config.control_plane_attestation_id,
            expected_release_id=config.release_id,
            expected_deployment_id=config.deployment_id,
            expected_environment=config.environment,
            expected_runtime=config.runtime_target,
            now=now,
        )
    except ValueError:
        raise RuntimeHostError("control_plane_pull_config_invalid") from None
    except RuntimeControlPlaneError as exc:
        if not exc.retryable:
            raise RuntimeHostError("control_plane_pull_rejected") from None
        cached = cache.load(
            config.control_plane_attestation_id,
            max_age_seconds=config.control_plane_max_cache_age_seconds,
            now=now,
        )
        if cached is None:
            raise RuntimeHostError("control_plane_pull_unavailable") from None
        if (
            cached.release_id != config.release_id
            or cached.deployment_id != config.deployment_id
            or cached.target_environment != config.environment
            or cached.runtime_target != config.runtime_target
        ):
            raise RuntimeHostError("control_plane_cache_binding_mismatch")
        return _ResolvedReleaseMaterial(
            bundle=cached.bundle,
            promotion_attestation=cached.promotion_attestation,
            source="cache",
            cache=cache,
        )
    return _ResolvedReleaseMaterial(
        bundle=handoff.bundle,
        promotion_attestation=handoff.promotion_attestation,
        source="control_plane",
        pulled_handoff=handoff,
        cache=cache,
    )


_MCP_TARGET_ENVIRONMENT = {
    "prod": "production",
    "production": "production",
    "staging": "staging",
    "dev": "development",
    "development": "development",
    "test": "test",
}


def _validate_mcp_release_binding(
    config: RuntimeHostConfig, admitted: AdmittedRuntimeRelease
) -> None:
    mcp_tools = tuple(
        tool for tool in admitted.config.tools if tool.source == "mcp"
    )
    local = config.mcp_broker
    if local is None:
        if mcp_tools:
            raise RuntimeHostError("mcp_broker_config_missing")
        return
    if not mcp_tools:
        raise RuntimeHostError("mcp_release_binding_mismatch")
    configured_names = {server.name for server in local.servers}
    if configured_names != set(admitted.config.mcp_servers):
        raise RuntimeHostError("mcp_server_manifest_mismatch")
    target_environment = _MCP_TARGET_ENVIRONMENT.get(config.environment)
    if target_environment is None:
        raise RuntimeHostError("mcp_environment_invalid")
    servers = {server.name: server for server in local.servers}
    signed_operations = {tool.operation for tool in mcp_tools}
    if any(grant.tool_name not in signed_operations for grant in local.grants):
        raise RuntimeHostError("mcp_grant_tool_unknown")
    risk_rank = {
        name: index for index, name in enumerate(MCP_RISK_LEVELS, start=1)
    }
    max_risk = risk_rank[local.policy.max_risk_level]
    agent_id = admitted.config.manifest.agent_id
    for tool in mcp_tools:
        if tool.side_effects in {"write", "destructive"} and not (
            tool.approval_required
        ):
            raise RuntimeHostError("mcp_side_effect_approval_contract_missing")
        server = servers.get(tool.mcp_server or "")
        if server is None:
            raise RuntimeHostError("mcp_server_manifest_mismatch")
        if server.environment != target_environment:
            raise RuntimeHostError("mcp_environment_mismatch")
        if tool.auth_binding != server.auth_mode:
            raise RuntimeHostError("mcp_auth_binding_mismatch")
        if not set(tool.scopes).issubset(set(server.scopes)):
            raise RuntimeHostError("mcp_server_scope_mismatch")
        candidates = []
        for grant in local.grants:
            if grant.tool_name != tool.operation:
                continue
            if grant.server_connection_id not in {
                None,
                server.connection_id,
            }:
                continue
            if grant.agent_ids and agent_id not in grant.agent_ids:
                continue
            score = (
                1 if grant.server_connection_id is not None else 0,
                1 if grant.agent_ids else 0,
            )
            candidates.append((score, grant))
        if not candidates:
            raise RuntimeHostError("mcp_tool_not_granted")
        best_score = max(score for score, _grant in candidates)
        best = [grant for score, grant in candidates if score == best_score]
        if len(best) != 1:
            raise RuntimeHostError("mcp_tool_grant_ambiguous")
        effective_risk = max(
            risk_rank[tool.risk_level],
            risk_rank[server.risk_level],
            risk_rank[best[0].risk_level],
        )
        if effective_risk > max_risk:
            raise RuntimeHostError("mcp_risk_ceiling_exceeded")


def build_reference_runtime_host(
    config: RuntimeHostConfig,
    *,
    environment: Optional[Mapping[str, str]] = None,
    evidence_emitter: Optional[EvidenceEmitter] = None,
    guard_evaluator: Optional[GuardEvaluator] = None,
    human_escalation: Optional[HumanEscalation] = None,
    mcp_transport_client: Optional[McpTransportClient] = None,
    now: Optional[datetime] = None,
) -> Tuple[ReferenceRuntimeHost, bool]:
    """Activate configured artifacts and construct the tenant request host."""

    env = environment if environment is not None else os.environ
    overload_contract = env.get(_RUNTIME_EDGE_OVERLOAD_CONTRACT_ENV, "").strip()
    if overload_contract and overload_contract != RUNTIME_EDGE_OVERLOAD_CONTRACT:
        raise RuntimeHostError("runtime_edge_overload_contract_unsupported")
    if (
        overload_contract == RUNTIME_EDGE_OVERLOAD_CONTRACT
        and config.model_gateway_endpoint_path != "/v1/chat/completions"
    ):
        raise RuntimeHostError("runtime_edge_model_endpoint_unsupported")
    dsn = env.get(config.database_dsn_env, "").strip()
    if not dsn:
        raise RuntimeHostError("runtime_database_url_missing")
    api_token = env.get(config.api_token_env, "")
    if not api_token:
        raise RuntimeHostError("runtime_api_token_missing")
    if len(api_token.encode("utf-8")) < 32:
        raise RuntimeHostError("api_token_too_short")
    try:
        check_postgres_runtime_compatibility(dsn)
    except RuntimePersistenceError as exc:
        raise RuntimeHostError(exc.code) from None
    receipt_api_key = None
    if config.receipt_base_url is not None:
        if config.receipt_api_key_env is None:
            raise RuntimeHostError("receipt_api_key_env_missing")
        receipt_api_key = env.get(config.receipt_api_key_env, "")
        if not receipt_api_key:
            raise RuntimeHostError("receipt_api_key_missing")
        if config.receipt_lease_seconds <= config.receipt_timeout_seconds:
            raise RuntimeHostError("receipt_lease_too_short")
    model_api_key = None
    if config.model_gateway_api_key_env is not None:
        model_api_key = env.get(config.model_gateway_api_key_env, "")
        if not model_api_key:
            raise RuntimeHostError("model_gateway_api_key_missing")
    model_adapter = OpenAICompatibleModelAdapter(
        config.model_gateway_base_url,
        api_key=model_api_key,
        endpoint_path=config.model_gateway_endpoint_path,
        timeout_seconds=config.model_gateway_timeout_seconds,
        max_response_bytes=config.model_gateway_max_response_bytes,
    )
    tool_broker = None
    if config.mcp_broker is not None:
        if (
            mcp_transport_client is None
            and not official_mcp_transport_available()
        ):
            raise RuntimeHostError("mcp_dependency_missing")
        tool_broker = GovernedMcpToolBroker(
            servers=config.mcp_broker.servers,
            grants=config.mcp_broker.grants,
            policy=config.mcp_broker.policy,
            egress_policy=config.mcp_broker.egress_policy,
            credential_provider=EnvironmentMcpCredentialProvider(
                config.mcp_broker.credential_bindings,
                environ=env,
            ),
            transport_client=(
                mcp_transport_client or OfficialMcpTransportClient()
            ),
            audit_sink=PostgresMcpAuditSink(
                dsn,
                tenant_id=config.tenant_id,
                runtime_id=config.runtime_id,
            ),
            idempotency_store=PostgresMcpIdempotencyStore(
                dsn,
                tenant_id=config.tenant_id,
                runtime_id=config.runtime_id,
                reservation_timeout_seconds=(
                    config.mcp_broker.reservation_timeout_seconds
                ),
            ),
        )
    admission_now = now or datetime.now(timezone.utc)
    material = _resolve_release_material(
        config,
        environment=env,
        dsn=dsn,
        now=admission_now,
    )
    policy = RuntimeAdmissionPolicy(
        expected_org_id=config.org_id,
        expected_environment=config.environment,
        expected_release_id=config.release_id,
        expected_deployment_id=config.deployment_id,
        expected_runtime=config.runtime_target,
        supported_capabilities=available_runtime_capabilities(
            guard_evaluator=guard_evaluator,
            tool_broker=tool_broker,
            human_escalation=human_escalation,
        ),
    )
    admitted, activation = activate_runtime_release(
        material.bundle,
        material.promotion_attestation,
        bundle_trust_store=config.bundle_trust_store,
        promotion_trust_store=config.promotion_trust_store,
        activation_store=PostgresRuntimeActivationStore(
            dsn,
            tenant_id=config.tenant_id,
        ),
        runtime_id=config.runtime_id,
        policy=policy,
        now=admission_now,
    )
    _validate_mcp_release_binding(config, admitted)
    if material.pulled_handoff is not None:
        if material.cache is None:
            raise RuntimeHostError("control_plane_cache_unavailable")
        material.cache.save(material.pulled_handoff)
    emitter = evidence_emitter or JsonLineEvidenceEmitter()
    receipt_outbox = None
    receipt_dispatcher = None
    if config.receipt_base_url is not None and receipt_api_key is not None:
        receipt_outbox = PostgresRuntimeReceiptOutbox(
            dsn,
            tenant_id=config.tenant_id,
        )
        identity_attributes = runtime_release_identity_attributes(
            admitted,
            runtime_id=config.runtime_id,
            runtime_version=config.runtime_version,
            release_source=material.source,
        )

        def receipt_status(outcome: str, details: Mapping[str, str]) -> None:
            attributes = dict(identity_attributes)
            attributes.update(
                {
                    "prometa.receipt.id": details["receiptId"],
                    "prometa.receipt.transition": details["transition"],
                }
            )
            if "errorCode" in details:
                attributes["prometa.receipt.error_code"] = details["errorCode"]
            emitter.emit(
                RuntimeEvidenceEvent(
                    name="runtime.receipt.delivery",
                    outcome=outcome,
                    occurred_at=datetime.now(timezone.utc)
                    .isoformat(timespec="milliseconds")
                    .replace("+00:00", "Z"),
                    attributes=attributes,
                )
            )

        receipt_dispatcher = RuntimeReceiptDispatcher(
            receipt_outbox,
            RuntimeReceiptClient(
                config.receipt_base_url,
                receipt_api_key,
                timeout=config.receipt_timeout_seconds,
            ),
            poll_interval_seconds=config.receipt_poll_interval_seconds,
            lease_seconds=config.receipt_lease_seconds,
            initial_backoff_seconds=config.receipt_initial_backoff_seconds,
            max_backoff_seconds=config.receipt_max_backoff_seconds,
            shutdown_timeout_seconds=min(
                300, config.receipt_timeout_seconds + 2
            ),
            on_status=receipt_status,
        )
    kernel = RuntimeKernel(
        admitted,
        model_adapter=model_adapter,
        evidence_emitter=emitter,
        runtime_id=config.runtime_id,
        runtime_version=config.runtime_version,
        execution_policy=RuntimeExecutionPolicy(
            timeout_seconds=config.model_gateway_timeout_seconds,
            tool_timeout_seconds=(
                config.mcp_broker.tool_timeout_seconds
                if config.mcp_broker is not None
                else 30.0
            ),
            overload_contract_id=overload_contract or None,
        ),
        guard_evaluator=guard_evaluator,
        tool_broker=tool_broker,
        human_escalation=human_escalation,
        state_store=PostgresRuntimeStateStore(
            dsn,
            tenant_id=config.tenant_id,
            runtime_id=config.runtime_id,
        ),
    )
    emitter.emit(
        RuntimeEvidenceEvent(
            name="runtime.release.material",
            outcome="verified",
            occurred_at=datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            attributes=runtime_release_identity_attributes(
                admitted,
                runtime_id=config.runtime_id,
                runtime_version=config.runtime_version,
                release_source=material.source,
            ),
        )
    )
    host = ReferenceRuntimeHost(
        kernel,
        api_token=api_token,
        request_timeout_seconds=config.request_timeout_seconds,
        max_request_bytes=config.max_request_bytes,
        receipt_dispatcher=receipt_dispatcher,
        release_source=material.source,
        task_store=(
            PostgresRuntimeTaskStore(
                dsn,
                tenant_id=config.tenant_id,
                runtime_id=config.runtime_id,
            )
            if config.task_recovery_enabled
            else None
        ),
        task_lease_seconds=config.task_recovery_lease_seconds,
        task_max_attempts=config.task_recovery_max_attempts,
        task_history_limit=config.task_recovery_history_limit,
    )
    if receipt_outbox is not None and receipt_dispatcher is not None:
        activation_at = activation.activated_at or admission_now
        admitted_receipt = build_runtime_receipt(
            attestation_id=admitted.promotion.attestation_id,
            artifact_digest=admitted.artifact_digest,
            release_id=config.release_id,
            deployment_id=config.deployment_id,
            target_environment=config.environment,
            runtime_target=config.runtime_target,
            runtime_id=config.runtime_id,
            runtime_version=config.runtime_version,
            transition="admitted",
            outcome="accepted",
            policy_digest=admitted.config.contract.policy_digest,
            configuration_digest=admitted.config.contract.configuration_digest,
            receipt_id=_lifecycle_receipt_id(
                config, admitted.promotion.attestation_id, "admitted"
            ),
            event_at=activation_at,
        )
        active_receipt = build_runtime_receipt(
            attestation_id=admitted.promotion.attestation_id,
            artifact_digest=admitted.artifact_digest,
            release_id=config.release_id,
            deployment_id=config.deployment_id,
            target_environment=config.environment,
            runtime_target=config.runtime_target,
            runtime_id=config.runtime_id,
            runtime_version=config.runtime_version,
            transition="active",
            outcome="succeeded",
            policy_digest=admitted.config.contract.policy_digest,
            configuration_digest=admitted.config.contract.configuration_digest,
            receipt_id=_lifecycle_receipt_id(
                config, admitted.promotion.attestation_id, "active"
            ),
            event_at=datetime.now(timezone.utc),
        )
        try:
            receipt_outbox.enqueue(admitted_receipt)
            receipt_outbox.enqueue(active_receipt)
        except Exception:
            host.close()
            raise
        receipt_dispatcher.start()
        receipt_dispatcher.wake()
    return host, activation.created


class _RuntimeHttpServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: Tuple[str, int],
        application: ReferenceRuntimeHost,
        ssl_context: Optional[ssl.SSLContext] = None,
    ):
        self.application = application
        super().__init__(address, _RuntimeRequestHandler)
        if ssl_context is not None:
            self.socket = ssl_context.wrap_socket(self.socket, server_side=True)


class _RuntimeRequestHandler(BaseHTTPRequestHandler):
    server: _RuntimeHttpServer
    protocol_version = "HTTP/1.1"
    server_version = "prometa-runtime"
    sys_version = ""

    def log_message(self, format: str, *args: Any) -> None:
        return None

    def _body(self) -> Tuple[bytes, Optional[RuntimeHostResponse]]:
        if self.headers.get("transfer-encoding") is not None:
            return b"", ReferenceRuntimeHost._error(400, "request_framing_invalid")
        values = self.headers.get_all("content-length", [])
        if not values:
            return b"", ReferenceRuntimeHost._error(411, "content_length_required")
        if len(values) != 1:
            return b"", ReferenceRuntimeHost._error(400, "request_framing_invalid")
        try:
            length = int(values[0])
        except ValueError:
            return b"", ReferenceRuntimeHost._error(400, "request_framing_invalid")
        if length < 0:
            return b"", ReferenceRuntimeHost._error(400, "request_framing_invalid")
        if length > self.server.application.max_request_bytes:
            return b"", ReferenceRuntimeHost._error(413, "request_too_large")
        body = self.rfile.read(length)
        if len(body) != length:
            return b"", ReferenceRuntimeHost._error(400, "request_body_incomplete")
        return body, None

    def _dispatch(self) -> None:
        path = self.path.split("?", 1)[0]
        headers = dict(self.headers.items())
        body = b""
        response = None
        protected_request = (
            self.command == "POST" and path == "/v1/runtime/execute"
        ) or (
            self.command == "GET"
            and path.startswith("/v1/runtime/tasks/")
        )
        if protected_request:
            authorizations = self.headers.get_all("authorization", [])
            if len(authorizations) != 1 or not self.server.application._authorized(
                {"authorization": authorizations[0] if authorizations else ""}
            ):
                response = ReferenceRuntimeHost._error(401, "unauthorized")
            elif self.command == "POST":
                body, response = self._body()
        if response is None:
            response = self.server.application.handle(
                self.command,
                path,
                headers,
                body,
            )
        try:
            encoded = json.dumps(
                dict(response.body),
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError):
            response = ReferenceRuntimeHost._error(500, "runtime_response_invalid")
            encoded = b'{"error":{"code":"runtime_response_invalid"}}'
        self.close_connection = True
        self.send_response(response.status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(encoded)))
        self.send_header("cache-control", "no-store")
        self.send_header("x-content-type-options", "nosniff")
        if response.status == 401:
            self.send_header("www-authenticate", "Bearer")
        self.send_header("connection", "close")
        self.end_headers()
        self.wfile.write(encoded)

    do_GET = _dispatch
    do_POST = _dispatch
    do_PUT = _dispatch
    do_PATCH = _dispatch
    do_DELETE = _dispatch


def build_runtime_server_ssl_context(
    config: RuntimeServerTlsConfig,
) -> ssl.SSLContext:
    """Build the fail-closed TLS context without exposing certificate paths."""

    if config.require_client_certificate and config.client_ca_file is None:
        raise RuntimeHostError("server_tls_client_ca_required")
    try:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.options |= ssl.OP_NO_COMPRESSION
        context.load_cert_chain(
            certfile=str(config.certificate_file),
            keyfile=str(config.private_key_file),
        )
        if config.client_ca_file is not None:
            context.load_verify_locations(cafile=str(config.client_ca_file))
        context.verify_mode = (
            ssl.CERT_REQUIRED if config.require_client_certificate else ssl.CERT_NONE
        )
        return context
    except (OSError, ssl.SSLError, ValueError):
        raise RuntimeHostError("server_tls_material_invalid") from None


def serve_reference_runtime_host(
    application: ReferenceRuntimeHost,
    *,
    bind_host: str = "0.0.0.0",
    port: int = 8080,
    tls_config: Optional[RuntimeServerTlsConfig] = None,
) -> None:
    """Serve until SIGINT/SIGTERM, then drain HTTP and stop the kernel loop."""

    if not bind_host or not 1 <= port <= 65535:
        raise RuntimeHostError("listen_address_invalid")
    ssl_context = (
        build_runtime_server_ssl_context(tls_config)
        if tls_config is not None
        else None
    )
    server = _RuntimeHttpServer((bind_host, port), application, ssl_context)
    stopping = threading.Event()

    def stop(signum=None, frame=None):
        if stopping.is_set():
            return
        stopping.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    previous = {}
    if threading.current_thread() is threading.main_thread():
        for selected in (signal.SIGINT, signal.SIGTERM):
            previous[selected] = signal.signal(selected, stop)
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        server.server_close()
        try:
            application.close()
        finally:
            for selected, handler in previous.items():
                signal.signal(selected, handler)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="prometa-runtime-host",
        description="Serve one activated tenant-owned Orchestra runtime release.",
    )
    parser.add_argument(
        "--config",
        default=os.environ.get(
            "PROMETA_RUNTIME_CONFIG", "/etc/prometa-runtime/config.json"
        ),
    )
    parser.add_argument(
        "--host", default=os.environ.get("PROMETA_RUNTIME_HOST", "0.0.0.0")
    )
    parser.add_argument("--port", type=int, default=os.environ.get("PORT", "8080"))
    parser.add_argument(
        "--tls-cert-file",
        default=os.environ.get("PROMETA_RUNTIME_SERVER_TLS_CERT_FILE", ""),
    )
    parser.add_argument(
        "--tls-key-file",
        default=os.environ.get("PROMETA_RUNTIME_SERVER_TLS_KEY_FILE", ""),
    )
    parser.add_argument(
        "--tls-client-ca-file",
        default=os.environ.get("PROMETA_RUNTIME_SERVER_TLS_CLIENT_CA_FILE", ""),
    )
    client_certificate_group = parser.add_mutually_exclusive_group()
    client_certificate_group.add_argument(
        "--tls-require-client-certificate",
        dest="tls_require_client_certificate",
        action="store_true",
    )
    client_certificate_group.add_argument(
        "--tls-no-require-client-certificate",
        dest="tls_require_client_certificate",
        action="store_false",
    )
    parser.set_defaults(tls_require_client_certificate=None)
    args = parser.parse_args(argv)
    application = None
    try:
        require_client_certificate = args.tls_require_client_certificate
        if require_client_certificate is None:
            raw_require_client_certificate = os.environ.get(
                "PROMETA_RUNTIME_SERVER_TLS_REQUIRE_CLIENT_CERTIFICATE", "false"
            ).strip().lower()
            if raw_require_client_certificate not in {"true", "false"}:
                raise RuntimeHostError("server_tls_configuration_invalid")
            require_client_certificate = raw_require_client_certificate == "true"
        if bool(args.tls_cert_file) != bool(args.tls_key_file):
            raise RuntimeHostError("server_tls_configuration_invalid")
        if (args.tls_client_ca_file or require_client_certificate) and not args.tls_cert_file:
            raise RuntimeHostError("server_tls_configuration_invalid")
        tls_config = (
            RuntimeServerTlsConfig(
                certificate_file=Path(args.tls_cert_file),
                private_key_file=Path(args.tls_key_file),
                client_ca_file=(
                    Path(args.tls_client_ca_file)
                    if args.tls_client_ca_file
                    else None
                ),
                require_client_certificate=require_client_certificate,
            )
            if args.tls_cert_file
            else None
        )
        if tls_config is not None:
            # Validate before release activation or the ready record. The
            # serving path rebuilds the context so a last-moment projection
            # replacement is also checked before accepting connections.
            build_runtime_server_ssl_context(tls_config)
        config = load_runtime_host_config(Path(args.config))
        application, created = build_reference_runtime_host(config)
        print(
            json.dumps(
                {
                    "type": "prometa.runtime.host",
                    "status": "ready",
                    "activation": "created" if created else "joined",
                    "runtimeId": config.runtime_id,
                    "releaseId": config.release_id,
                    "deploymentId": config.deployment_id,
                    "releaseSource": application.release_source,
                    "taskRecovery": application.task_recovery_enabled,
                    "serverTls": tls_config is not None,
                    "clientCertificateRequired": require_client_certificate,
                },
                separators=(",", ":"),
            ),
            flush=True,
        )
        serve_reference_runtime_host(
            application,
            bind_host=args.host,
            port=args.port,
            tls_config=tls_config,
        )
        return 0
    except (
        BundleVerificationError,
        RuntimeExecutionError,
        RuntimeHostError,
        RuntimePersistenceError,
    ) as exc:
        if application is not None:
            application.close()
        print(
            json.dumps(
                {"type": "prometa.runtime.host", "status": "failed", "code": exc.code},
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 2
    except Exception:
        if application is not None:
            application.close()
        print(
            '{"type":"prometa.runtime.host","status":"failed","code":"host_bootstrap_failed"}',
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "HOST_CONFIG_VERSION",
    "DEFAULT_MAX_REQUEST_BYTES",
    "DEFAULT_REQUEST_TIMEOUT_SECONDS",
    "RuntimeHostError",
    "RuntimeHostConfig",
    "RuntimeHostResponse",
    "RuntimeServerTlsConfig",
    "build_runtime_server_ssl_context",
    "JsonLineEvidenceEmitter",
    "ReferenceRuntimeHost",
    "load_runtime_host_config",
    "build_reference_runtime_host",
    "serve_reference_runtime_host",
    "main",
]
