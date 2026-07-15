"""Governed tenant-side MCP tool execution.

This module is an optional runtime adapter. It intersects signed bundle intent
with tenant-local connection and permission projections before invoking an MCP
server. It never consults the Orchestra control plane in the request path.
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import os
import re
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    FrozenSet,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)
from urllib.parse import urlsplit

from .kernel import (
    RuntimeExecutionError,
    ToolBroker,
    ToolInvocationRequest,
    ToolInvocationResult,
)


MCP_TRANSPORTS = frozenset({"stdio", "streamable-http"})
MCP_AUTH_MODES = frozenset({"none", "api-key", "oauth", "service-account"})
MCP_RISK_LEVELS = ("low", "medium", "high", "critical")
MCP_ENVIRONMENTS = frozenset({"production", "staging", "development", "test"})
MCP_PERMISSION_LEVELS = frozenset({"read", "read-write", "write", "execute"})
MCP_SIDE_EFFECTS = frozenset({"read-only", "write", "destructive"})

_RISK_RANK = {name: index for index, name in enumerate(MCP_RISK_LEVELS, start=1)}
_TARGET_ENVIRONMENT = {
    "prod": "production",
    "production": "production",
    "staging": "staging",
    "dev": "development",
    "development": "development",
    "test": "test",
}
_HEADER_NAME = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
_ENVIRONMENT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_FORBIDDEN_CREDENTIAL_HEADERS = frozenset(
    {
        "connection",
        "content-length",
        "host",
        "mcp-protocol-version",
        "mcp-session-id",
        "transfer-encoding",
    }
)


def _trimmed(value: str, field: str, maximum: int = 256) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or "\x00" in value
    ):
        raise ValueError("%s must be a trimmed string of 1-%d characters" % (field, maximum))
    return value


def _string_tuple(values: Sequence[str], field: str, maximum: int = 256) -> Tuple[str, ...]:
    if isinstance(values, (str, bytes)) or len(values) > maximum:
        raise ValueError("%s must be a sequence of at most %d strings" % (field, maximum))
    result = tuple(_trimmed(value, field) for value in values)
    if len(set(result)) != len(result):
        raise ValueError("%s must not contain duplicates" % field)
    return result


def _canonical_json(value: Any, *, maximum: Optional[int] = None) -> bytes:
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise RuntimeExecutionError("mcp_payload_not_json") from exc
    if maximum is not None and len(encoded) > maximum:
        raise RuntimeExecutionError("mcp_response_too_large")
    return encoded


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _is_internal_host(host: str) -> bool:
    normalized = host.lower().rstrip(".")
    if normalized == "localhost" or normalized.endswith(
        (".internal", ".local", ".svc")
    ):
        return True
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv4Address):
        return bool(
            address.is_loopback
            or address.is_link_local
            or address in ipaddress.ip_network("10.0.0.0/8")
            or address in ipaddress.ip_network("172.16.0.0/12")
            or address in ipaddress.ip_network("192.168.0.0/16")
        )
    return bool(
        address == ipaddress.ip_address("::1")
        or address in ipaddress.ip_network("fc00::/7")
        or address in ipaddress.ip_network("fe80::/10")
    )


def _endpoint_origin(endpoint: str) -> str:
    parsed = urlsplit(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("endpoint must be an http(s) URL")
    try:
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except ValueError as exc:
        raise ValueError("endpoint port is invalid") from exc
    host = parsed.hostname.lower().rstrip(".")
    if ":" in host:
        host = "[%s]" % host
    return "%s://%s:%d" % (parsed.scheme, host, port)


@dataclass(frozen=True)
class McpServerConfig:
    """Tenant-local binding for one logical server in a signed bundle."""

    name: str
    connection_id: str
    transport: str
    environment: str
    auth_mode: str
    scopes: Tuple[str, ...]
    risk_level: str
    endpoint: Optional[str] = None
    command: Optional[str] = None
    arguments: Tuple[str, ...] = ()
    working_directory: Optional[str] = None
    enabled: bool = True
    allow_insecure_http: bool = False
    timeout_seconds: float = 30.0
    max_response_bytes: int = 1_048_576

    def __post_init__(self) -> None:
        _trimmed(self.name, "name", 120)
        _trimmed(self.connection_id, "connection_id", 200)
        if self.transport not in MCP_TRANSPORTS:
            raise ValueError("unsupported MCP transport")
        if self.environment not in MCP_ENVIRONMENTS:
            raise ValueError("unsupported MCP environment")
        if self.auth_mode not in MCP_AUTH_MODES:
            raise ValueError("unsupported MCP auth mode")
        if self.risk_level not in _RISK_RANK:
            raise ValueError("unsupported MCP risk level")
        object.__setattr__(self, "scopes", _string_tuple(self.scopes, "scopes"))
        object.__setattr__(
            self, "arguments", _string_tuple(self.arguments, "arguments", 128)
        )
        if type(self.enabled) is not bool or type(self.allow_insecure_http) is not bool:
            raise ValueError("MCP boolean options must be booleans")
        if not isinstance(self.timeout_seconds, (int, float)) or not (
            0 < self.timeout_seconds <= 300
        ):
            raise ValueError("timeout_seconds must be between 0 and 300")
        if type(self.max_response_bytes) is not int or not (
            1 <= self.max_response_bytes <= 10_485_760
        ):
            raise ValueError("max_response_bytes must be between 1 and 10485760")

        if self.transport == "streamable-http":
            endpoint = _trimmed(self.endpoint or "", "endpoint", 2048)
            parsed = urlsplit(endpoint)
            _endpoint_origin(endpoint)
            if parsed.username is not None or parsed.password is not None:
                raise ValueError("endpoint userinfo is forbidden")
            if parsed.fragment:
                raise ValueError("endpoint fragments are forbidden")
            if parsed.scheme == "http" and not (
                self.allow_insecure_http
                and parsed.hostname is not None
                and _is_internal_host(parsed.hostname)
            ):
                raise ValueError("plain HTTP requires an explicit internal-host opt-in")
            if self.command is not None or self.arguments or self.working_directory:
                raise ValueError("HTTP MCP servers cannot declare stdio options")
        else:
            _trimmed(self.command or "", "command", 500)
            if self.endpoint is not None:
                raise ValueError("stdio MCP servers cannot declare an endpoint")
            if self.allow_insecure_http:
                raise ValueError("allow_insecure_http is only valid for HTTP")
            if self.working_directory is not None:
                _trimmed(self.working_directory, "working_directory", 1024)


@dataclass(frozen=True)
class McpToolGrant:
    """Tenant projection of one platform MCP permission-matrix row."""

    tool_name: str
    agent_ids: Tuple[str, ...] = ()
    permission: str = "read"
    risk_level: str = "low"
    server_connection_id: Optional[str] = None

    def __post_init__(self) -> None:
        _trimmed(self.tool_name, "tool_name", 200)
        object.__setattr__(
            self, "agent_ids", _string_tuple(self.agent_ids, "agent_ids", 1024)
        )
        if self.permission not in MCP_PERMISSION_LEVELS:
            raise ValueError("unsupported MCP permission level")
        if self.risk_level not in _RISK_RANK:
            raise ValueError("unsupported MCP risk level")
        if self.server_connection_id is not None:
            _trimmed(self.server_connection_id, "server_connection_id", 200)


@dataclass(frozen=True)
class McpBrokerPolicy:
    """Tenant-local restrictions that may only narrow signed bundle intent."""

    max_risk_level: str
    require_approval_for: FrozenSet[str] = frozenset({"write", "destructive"})
    require_idempotency_for: FrozenSet[str] = frozenset({"write", "destructive"})

    def __post_init__(self) -> None:
        if self.max_risk_level not in _RISK_RANK:
            raise ValueError("unsupported MCP risk ceiling")
        for field_name, values in (
            ("require_approval_for", self.require_approval_for),
            ("require_idempotency_for", self.require_idempotency_for),
        ):
            if not isinstance(values, frozenset) or not values.issubset(MCP_SIDE_EFFECTS):
                raise ValueError("%s contains an unsupported side-effect class" % field_name)


class McpEgressPolicy(Protocol):
    def allows(self, server: McpServerConfig) -> bool:
        """Return true only when this exact tenant target is permitted."""


@dataclass(frozen=True)
class ExplicitMcpEgressPolicy:
    """Exact origin/command allowlist; no wildcards or implicit public egress."""

    allowed_http_origins: FrozenSet[str] = frozenset()
    allowed_stdio_commands: FrozenSet[str] = frozenset()

    def __post_init__(self) -> None:
        origins = frozenset(_endpoint_origin(value) for value in self.allowed_http_origins)
        commands = frozenset(
            _trimmed(value, "allowed_stdio_commands", 500)
            for value in self.allowed_stdio_commands
        )
        object.__setattr__(self, "allowed_http_origins", origins)
        object.__setattr__(self, "allowed_stdio_commands", commands)

    def allows(self, server: McpServerConfig) -> bool:
        if server.transport == "streamable-http":
            return _endpoint_origin(server.endpoint or "") in self.allowed_http_origins
        return (server.command or "") in self.allowed_stdio_commands


@dataclass(frozen=True)
class McpTransportCredentials:
    headers: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({})
    )
    environment: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        headers = dict(self.headers)
        environment = dict(self.environment)
        for name, value in headers.items():
            if (
                not isinstance(name, str)
                or not _HEADER_NAME.fullmatch(name)
                or name.lower() in _FORBIDDEN_CREDENTIAL_HEADERS
            ):
                raise ValueError("invalid MCP credential header")
            if not isinstance(value, str) or not value or "\r" in value or "\n" in value:
                raise ValueError("invalid MCP credential value")
        for name, value in environment.items():
            if not isinstance(name, str) or not _ENVIRONMENT_NAME.fullmatch(name):
                raise ValueError("invalid MCP credential environment name")
            if not isinstance(value, str) or not value or "\x00" in value:
                raise ValueError("invalid MCP credential environment value")
        object.__setattr__(self, "headers", MappingProxyType(headers))
        object.__setattr__(self, "environment", MappingProxyType(environment))


class McpCredentialProvider(Protocol):
    async def resolve(self, server: McpServerConfig) -> McpTransportCredentials:
        """Resolve tenant credentials without exposing them to audit events."""


@dataclass(frozen=True)
class McpCredentialBinding:
    """Secret-free mapping from transport fields to source environment names."""

    server_name: str
    auth_mode: str
    http_headers: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({})
    )
    stdio_environment: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        _trimmed(self.server_name, "server_name", 120)
        if self.auth_mode not in MCP_AUTH_MODES:
            raise ValueError("unsupported MCP auth mode")
        headers = dict(self.http_headers)
        environment = dict(self.stdio_environment)
        for header, source in headers.items():
            if (
                not isinstance(header, str)
                or not _HEADER_NAME.fullmatch(header)
                or header.lower() in _FORBIDDEN_CREDENTIAL_HEADERS
            ):
                raise ValueError("invalid MCP credential header binding")
            if not isinstance(source, str) or not _ENVIRONMENT_NAME.fullmatch(source):
                raise ValueError("invalid MCP credential source environment name")
        for target, source in environment.items():
            if (
                not isinstance(target, str)
                or not _ENVIRONMENT_NAME.fullmatch(target)
                or not isinstance(source, str)
                or not _ENVIRONMENT_NAME.fullmatch(source)
            ):
                raise ValueError("invalid MCP stdio credential binding")
        if self.auth_mode == "none" and (headers or environment):
            raise ValueError("auth_mode none cannot bind credentials")
        object.__setattr__(self, "http_headers", MappingProxyType(headers))
        object.__setattr__(self, "stdio_environment", MappingProxyType(environment))


class EnvironmentMcpCredentialProvider:
    """Resolve named secrets at invocation time from an environment mapping."""

    def __init__(
        self,
        bindings: Sequence[McpCredentialBinding],
        *,
        environ: Optional[Mapping[str, str]] = None,
    ) -> None:
        by_name = {binding.server_name: binding for binding in bindings}
        if len(by_name) != len(bindings):
            raise ValueError("MCP credential bindings must have unique server names")
        self._bindings = by_name
        self._environ = environ if environ is not None else os.environ

    async def resolve(self, server: McpServerConfig) -> McpTransportCredentials:
        binding = self._bindings.get(server.name)
        if binding is None:
            if server.auth_mode == "none":
                return McpTransportCredentials()
            raise RuntimeExecutionError("mcp_credentials_missing")
        if binding.auth_mode != server.auth_mode:
            raise RuntimeExecutionError("mcp_credential_auth_mismatch")

        def read(source: str) -> str:
            value = self._environ.get(source)
            if not isinstance(value, str) or not value:
                raise RuntimeExecutionError("mcp_credentials_missing")
            return value

        return McpTransportCredentials(
            headers={target: read(source) for target, source in binding.http_headers.items()},
            environment={
                target: read(source)
                for target, source in binding.stdio_environment.items()
            },
        )


@dataclass(frozen=True)
class McpAuditEvent:
    audit_reference: str
    phase: str
    outcome: str
    occurred_at: str
    request_id: str
    call_id: str
    agent_id: Optional[str]
    release_id: Optional[str]
    deployment_id: Optional[str]
    environment: Optional[str]
    server_name: Optional[str]
    server_connection_id: Optional[str]
    transport: Optional[str]
    operation: str
    permission: Optional[str]
    effective_risk: Optional[str]
    side_effects: str
    scopes: Tuple[str, ...]
    approval_references: Tuple[str, ...]
    argument_digest: Optional[str]
    output_digest: Optional[str]
    idempotency_key: Optional[str]
    reason: Optional[str] = None


class McpAuditSink(Protocol):
    async def record(self, event: McpAuditEvent) -> None:
        """Persist one payload-free MCP decision event; failures must raise."""


class InMemoryMcpAuditSink:
    def __init__(self) -> None:
        self._events = []
        self._lock = threading.Lock()

    async def record(self, event: McpAuditEvent) -> None:
        with self._lock:
            self._events.append(event)

    @property
    def events(self) -> Tuple[McpAuditEvent, ...]:
        with self._lock:
            return tuple(self._events)


@dataclass(frozen=True)
class McpIdempotencyRecord:
    request_digest: str
    status: str
    output_digest: Optional[str] = None


class McpIdempotencyStore(Protocol):
    async def reserve(self, key: str, request_digest: str) -> str:
        """Atomically return acquired, reserved, completed, indeterminate, or conflict."""

    async def complete(self, key: str, request_digest: str, output_digest: str) -> None:
        """Record a completed tool call."""

    async def release(self, key: str, request_digest: str) -> None:
        """Release a reservation only when no transport call was attempted."""

    async def mark_indeterminate(self, key: str, request_digest: str) -> None:
        """Block automatic replay when the side-effect outcome is uncertain."""


class InMemoryMcpIdempotencyStore:
    """Single-process test/development store; not safe across runtime replicas."""

    def __init__(self) -> None:
        self._records: Dict[str, McpIdempotencyRecord] = {}
        self._lock = threading.Lock()

    async def reserve(self, key: str, request_digest: str) -> str:
        with self._lock:
            record = self._records.get(key)
            if record is None:
                self._records[key] = McpIdempotencyRecord(request_digest, "reserved")
                return "acquired"
            if record.request_digest != request_digest:
                return "conflict"
            return record.status

    async def complete(self, key: str, request_digest: str, output_digest: str) -> None:
        with self._lock:
            record = self._records.get(key)
            if record is None or record.request_digest != request_digest:
                raise ValueError("MCP idempotency reservation is missing")
            if record.status != "reserved":
                raise ValueError("MCP idempotency reservation is not active")
            self._records[key] = McpIdempotencyRecord(
                request_digest, "completed", output_digest
            )

    async def release(self, key: str, request_digest: str) -> None:
        with self._lock:
            record = self._records.get(key)
            if record is None:
                return
            if record.request_digest != request_digest or record.status != "reserved":
                raise ValueError("MCP idempotency reservation cannot be released")
            del self._records[key]

    async def mark_indeterminate(self, key: str, request_digest: str) -> None:
        with self._lock:
            record = self._records.get(key)
            if record is not None and record.request_digest != request_digest:
                raise ValueError("MCP idempotency digest mismatch")
            self._records[key] = McpIdempotencyRecord(
                request_digest, "indeterminate"
            )

    def get(self, key: str) -> Optional[McpIdempotencyRecord]:
        with self._lock:
            return self._records.get(key)


class McpTransportClient(Protocol):
    async def call_tool(
        self,
        server: McpServerConfig,
        operation: str,
        arguments: Mapping[str, Any],
        credentials: McpTransportCredentials,
        metadata: Mapping[str, Any],
    ) -> Any:
        """Call one MCP tool using a tenant-owned transport."""


class McpTransportError(RuntimeExecutionError):
    def __init__(
        self,
        code: str,
        message: Optional[str] = None,
        *,
        outcome_unknown: bool,
    ) -> None:
        super().__init__(code, message, retryable=False)
        self.outcome_unknown = outcome_unknown


class OfficialMcpTransportClient:
    """Optional adapter for the official MCP Python SDK v1 transports."""

    async def call_tool(
        self,
        server: McpServerConfig,
        operation: str,
        arguments: Mapping[str, Any],
        credentials: McpTransportCredentials,
        metadata: Mapping[str, Any],
    ) -> Any:
        try:
            import httpx
            from mcp import ClientSession, StdioServerParameters
            from mcp.client.stdio import stdio_client
            from mcp.client.streamable_http import streamable_http_client
        except ImportError as exc:
            raise McpTransportError(
                "mcp_dependency_missing",
                "Install prometa-sdk[runtime-mcp] on Python 3.10 or newer",
                outcome_unknown=False,
            ) from exc

        async def invoke(read_stream, write_stream):
            async with ClientSession(
                read_stream,
                write_stream,
                read_timeout_seconds=timedelta(seconds=server.timeout_seconds),
            ) as session:
                await session.initialize()
                return await session.call_tool(
                    operation,
                    arguments=dict(arguments),
                    meta=dict(metadata),
                )

        try:
            if server.transport == "streamable-http":
                async with httpx.AsyncClient(
                    headers=dict(credentials.headers),
                    timeout=httpx.Timeout(server.timeout_seconds),
                    follow_redirects=False,
                    trust_env=False,
                ) as http_client:
                    async with streamable_http_client(
                        server.endpoint or "",
                        http_client=http_client,
                        terminate_on_close=True,
                    ) as streams:
                        result = await invoke(streams[0], streams[1])
            else:
                parameters = StdioServerParameters(
                    command=server.command or "",
                    args=list(server.arguments),
                    env=dict(credentials.environment) or None,
                    cwd=server.working_directory,
                )
                async with stdio_client(parameters) as streams:
                    result = await invoke(streams[0], streams[1])
        except asyncio.CancelledError:
            raise
        except McpTransportError:
            raise
        except Exception as exc:
            raise McpTransportError(
                "mcp_transport_failed", outcome_unknown=True
            ) from exc

        if bool(getattr(result, "isError", False)):
            raise McpTransportError(
                "mcp_tool_reported_error", outcome_unknown=True
            )
        structured = getattr(result, "structuredContent", None)
        if structured is not None:
            return structured
        content = list(getattr(result, "content", ()))
        if len(content) == 1 and isinstance(getattr(content[0], "text", None), str):
            return content[0].text
        rendered = []
        for item in content:
            if hasattr(item, "model_dump"):
                rendered.append(item.model_dump(mode="json", exclude_none=True))
            elif isinstance(item, Mapping):
                rendered.append(dict(item))
            else:
                raise McpTransportError(
                    "mcp_response_invalid", outcome_unknown=True
                )
        return rendered


@dataclass(frozen=True)
class _Authorization:
    server: McpServerConfig
    grant: McpToolGrant
    effective_risk: str
    credentials: McpTransportCredentials


class GovernedMcpToolBroker(ToolBroker):
    """Fail-closed MCP broker for the tenant-owned runtime kernel."""

    def __init__(
        self,
        *,
        servers: Sequence[McpServerConfig],
        grants: Sequence[McpToolGrant],
        policy: McpBrokerPolicy,
        egress_policy: McpEgressPolicy,
        transport_client: McpTransportClient,
        audit_sink: McpAuditSink,
        credential_provider: Optional[McpCredentialProvider] = None,
        idempotency_store: Optional[McpIdempotencyStore] = None,
    ) -> None:
        by_name = {server.name: server for server in servers}
        by_id = {server.connection_id: server for server in servers}
        if len(by_name) != len(servers) or len(by_id) != len(servers):
            raise ValueError("MCP servers must have unique names and connection ids")
        if not servers:
            raise ValueError("at least one MCP server is required")
        if not grants:
            raise ValueError("at least one MCP tool grant is required")
        self._servers = by_name
        self._grants = tuple(grants)
        self._policy = policy
        self._egress_policy = egress_policy
        self._transport = transport_client
        self._audit_sink = audit_sink
        self._credential_provider = credential_provider
        self._idempotency = idempotency_store

    @staticmethod
    def _required_identity(value: Optional[str], code: str) -> str:
        if (
            not isinstance(value, str)
            or not value
            or value != value.strip()
            or len(value) > 256
        ):
            raise RuntimeExecutionError(code)
        return value

    def _match_grant(
        self, operation: str, agent_id: str, connection_id: str
    ) -> McpToolGrant:
        candidates = []
        for grant in self._grants:
            if grant.tool_name != operation:
                continue
            if grant.server_connection_id not in {None, connection_id}:
                continue
            if grant.agent_ids and agent_id not in grant.agent_ids:
                continue
            score = (
                1 if grant.server_connection_id is not None else 0,
                1 if grant.agent_ids else 0,
            )
            candidates.append((score, grant))
        if not candidates:
            raise RuntimeExecutionError("mcp_tool_not_granted")
        best_score = max(score for score, _grant in candidates)
        best = [grant for score, grant in candidates if score == best_score]
        if len(best) != 1:
            raise RuntimeExecutionError("mcp_tool_grant_ambiguous")
        return best[0]

    async def _credentials(self, server: McpServerConfig) -> McpTransportCredentials:
        if self._credential_provider is None:
            credentials = McpTransportCredentials()
        else:
            try:
                credentials = await self._credential_provider.resolve(server)
            except asyncio.CancelledError:
                raise
            except RuntimeExecutionError:
                raise
            except Exception as exc:
                raise RuntimeExecutionError("mcp_credential_resolution_failed") from exc
        if not isinstance(credentials, McpTransportCredentials):
            raise RuntimeExecutionError("mcp_credentials_invalid")
        populated = bool(credentials.headers or credentials.environment)
        if server.auth_mode == "none" and populated:
            raise RuntimeExecutionError("mcp_unexpected_credentials")
        if server.auth_mode != "none":
            transport_credentials = (
                credentials.headers
                if server.transport == "streamable-http"
                else credentials.environment
            )
            if not transport_credentials:
                raise RuntimeExecutionError("mcp_credentials_missing")
        return credentials

    async def _authorize(self, request: ToolInvocationRequest) -> _Authorization:
        tool = request.tool
        if tool.source != "mcp" or tool.mcp_server is None:
            raise RuntimeExecutionError("mcp_tool_source_invalid")
        agent_id = self._required_identity(request.agent_id, "mcp_agent_identity_missing")
        self._required_identity(request.release_id, "mcp_release_identity_missing")
        self._required_identity(request.deployment_id, "mcp_deployment_identity_missing")
        target_environment = _TARGET_ENVIRONMENT.get(request.environment or "")
        if target_environment is None:
            raise RuntimeExecutionError("mcp_environment_invalid")
        server = self._servers.get(tool.mcp_server)
        if server is None:
            raise RuntimeExecutionError("mcp_server_not_configured")
        if not server.enabled:
            raise RuntimeExecutionError("mcp_server_disabled")
        if server.environment != target_environment:
            raise RuntimeExecutionError("mcp_environment_mismatch")
        if tool.auth_binding != server.auth_mode:
            raise RuntimeExecutionError("mcp_auth_binding_mismatch")
        signed_scopes = set(tool.scopes)
        if not signed_scopes.issubset(set(request.granted_scopes)):
            raise RuntimeExecutionError("mcp_scope_not_granted")
        if not signed_scopes.issubset(set(server.scopes)):
            raise RuntimeExecutionError("mcp_server_scope_mismatch")
        try:
            egress_allowed = self._egress_policy.allows(server)
        except Exception as exc:
            raise RuntimeExecutionError("mcp_egress_policy_failed") from exc
        if egress_allowed is not True:
            raise RuntimeExecutionError("mcp_egress_denied")
        grant = self._match_grant(tool.operation, agent_id, server.connection_id)
        effective_risk = max(
            (tool.risk_level, server.risk_level, grant.risk_level),
            key=lambda value: _RISK_RANK.get(value, _RISK_RANK["critical"]),
        )
        if _RISK_RANK.get(effective_risk, _RISK_RANK["critical"]) > _RISK_RANK[
            self._policy.max_risk_level
        ]:
            raise RuntimeExecutionError("mcp_risk_ceiling_exceeded")
        approval_required = bool(
            tool.approval_required
            or tool.side_effects in self._policy.require_approval_for
        )
        valid_approvals = tuple(
            reference
            for reference in request.approval_references
            if isinstance(reference, str) and reference and reference == reference.strip()
        )
        if len(valid_approvals) != len(request.approval_references):
            raise RuntimeExecutionError("mcp_approval_reference_invalid")
        if approval_required and not valid_approvals:
            raise RuntimeExecutionError("mcp_approval_required")
        if (
            tool.side_effects in self._policy.require_idempotency_for
            and self._idempotency is None
        ):
            raise RuntimeExecutionError("mcp_idempotency_store_required")
        credentials = await self._credentials(server)
        return _Authorization(server, grant, effective_risk, credentials)

    @staticmethod
    def _idempotency_key(
        request: ToolInvocationRequest, authorization: _Authorization
    ) -> str:
        identity = {
            "version": 1,
            "releaseId": request.release_id,
            "deploymentId": request.deployment_id,
            "requestId": request.request_id,
            "callId": request.call_id,
            "agentId": request.agent_id,
            "serverConnectionId": authorization.server.connection_id,
            "operation": request.tool.operation,
        }
        return "mcp1:" + hashlib.sha256(_canonical_json(identity)).hexdigest()

    def _event(
        self,
        request: ToolInvocationRequest,
        audit_reference: str,
        phase: str,
        outcome: str,
        *,
        authorization: Optional[_Authorization] = None,
        argument_digest: Optional[str] = None,
        output_digest: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> McpAuditEvent:
        server = authorization.server if authorization else None
        grant = authorization.grant if authorization else None
        return McpAuditEvent(
            audit_reference=audit_reference,
            phase=phase,
            outcome=outcome,
            occurred_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            request_id=request.request_id,
            call_id=request.call_id,
            agent_id=request.agent_id,
            release_id=request.release_id,
            deployment_id=request.deployment_id,
            environment=request.environment,
            server_name=server.name if server else request.tool.mcp_server,
            server_connection_id=server.connection_id if server else None,
            transport=server.transport if server else None,
            operation=request.tool.operation,
            permission=grant.permission if grant else None,
            effective_risk=authorization.effective_risk if authorization else None,
            side_effects=request.tool.side_effects,
            scopes=tuple(request.tool.scopes),
            approval_references=tuple(request.approval_references),
            argument_digest=argument_digest,
            output_digest=output_digest,
            idempotency_key=idempotency_key,
            reason=reason,
        )

    async def _record(self, event: McpAuditEvent) -> None:
        try:
            await self._audit_sink.record(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise RuntimeExecutionError("mcp_audit_failed") from exc

    async def _store_call(self, operation, *args):
        try:
            return await operation(*args)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise RuntimeExecutionError("mcp_idempotency_store_failed") from exc

    async def invoke(self, request: ToolInvocationRequest) -> ToolInvocationResult:
        audit_reference = "mcp-audit-" + str(uuid.uuid4())
        argument_digest: Optional[str] = None
        try:
            argument_digest = _digest(_canonical_json(request.arguments))
            authorization = await self._authorize(request)
        except RuntimeExecutionError as exc:
            await self._record(
                self._event(
                    request,
                    audit_reference,
                    "authorization",
                    "denied",
                    argument_digest=argument_digest,
                    reason=exc.code,
                )
            )
            raise

        idempotency_key = self._idempotency_key(request, authorization)
        request_digest = argument_digest or ""
        if self._idempotency is not None:
            try:
                status = await self._store_call(
                    self._idempotency.reserve,
                    idempotency_key,
                    request_digest,
                )
            except RuntimeExecutionError as exc:
                await self._record(
                    self._event(
                        request,
                        audit_reference,
                        "idempotency",
                        "failed",
                        authorization=authorization,
                        argument_digest=argument_digest,
                        idempotency_key=idempotency_key,
                        reason=exc.code,
                    )
                )
                raise
            if status != "acquired":
                code = {
                    "reserved": "mcp_tool_call_in_progress",
                    "completed": "mcp_duplicate_tool_call",
                    "indeterminate": "mcp_tool_call_indeterminate",
                    "conflict": "mcp_idempotency_conflict",
                }.get(status, "mcp_idempotency_state_invalid")
                await self._record(
                    self._event(
                        request,
                        audit_reference,
                        "idempotency",
                        "denied",
                        authorization=authorization,
                        argument_digest=argument_digest,
                        idempotency_key=idempotency_key,
                        reason=code,
                    )
                )
                raise RuntimeExecutionError(code)

        try:
            await self._record(
                self._event(
                    request,
                    audit_reference,
                    "authorization",
                    "accepted",
                    authorization=authorization,
                    argument_digest=argument_digest,
                    idempotency_key=idempotency_key,
                )
            )
        except RuntimeExecutionError:
            if self._idempotency is not None:
                await self._store_call(
                    self._idempotency.release,
                    idempotency_key,
                    request_digest,
                )
            raise

        metadata = {
            "prometa.io/idempotency-key": idempotency_key,
            "prometa.io/request-id": request.request_id,
            "prometa.io/call-id": request.call_id,
        }
        try:
            output = await self._transport.call_tool(
                authorization.server,
                request.tool.operation,
                request.arguments,
                authorization.credentials,
                metadata,
            )
            output_bytes = _canonical_json(
                output, maximum=authorization.server.max_response_bytes
            )
            output = json.loads(output_bytes.decode("utf-8"))
            output_digest = _digest(output_bytes)
        except asyncio.CancelledError:
            if self._idempotency is not None:
                await self._store_call(
                    self._idempotency.mark_indeterminate,
                    idempotency_key,
                    request_digest,
                )
            raise
        except RuntimeExecutionError as exc:
            if self._idempotency is not None:
                should_release = isinstance(exc, McpTransportError) and not (
                    exc.outcome_unknown
                )
                operation = (
                    self._idempotency.release
                    if should_release
                    else self._idempotency.mark_indeterminate
                )
                await self._store_call(operation, idempotency_key, request_digest)
            await self._record(
                self._event(
                    request,
                    audit_reference,
                    "execution",
                    "failed",
                    authorization=authorization,
                    argument_digest=argument_digest,
                    idempotency_key=idempotency_key,
                    reason=exc.code,
                )
            )
            raise
        except Exception as exc:
            if self._idempotency is not None:
                await self._store_call(
                    self._idempotency.mark_indeterminate,
                    idempotency_key,
                    request_digest,
                )
            failure = McpTransportError("mcp_transport_failed", outcome_unknown=True)
            await self._record(
                self._event(
                    request,
                    audit_reference,
                    "execution",
                    "failed",
                    authorization=authorization,
                    argument_digest=argument_digest,
                    idempotency_key=idempotency_key,
                    reason=failure.code,
                )
            )
            raise failure from exc

        if self._idempotency is not None:
            try:
                await self._store_call(
                    self._idempotency.complete,
                    idempotency_key,
                    request_digest,
                    output_digest,
                )
            except RuntimeExecutionError as exc:
                await self._store_call(
                    self._idempotency.mark_indeterminate,
                    idempotency_key,
                    request_digest,
                )
                await self._record(
                    self._event(
                        request,
                        audit_reference,
                        "idempotency",
                        "failed",
                        authorization=authorization,
                        argument_digest=argument_digest,
                        output_digest=output_digest,
                        idempotency_key=idempotency_key,
                        reason=exc.code,
                    )
                )
                raise

        try:
            await self._record(
                self._event(
                    request,
                    audit_reference,
                    "execution",
                    "completed",
                    authorization=authorization,
                    argument_digest=argument_digest,
                    output_digest=output_digest,
                    idempotency_key=idempotency_key,
                )
            )
        except RuntimeExecutionError:
            if self._idempotency is not None:
                await self._store_call(
                    self._idempotency.mark_indeterminate,
                    idempotency_key,
                    request_digest,
                )
            raise
        return ToolInvocationResult(output=output, audit_reference=audit_reference)


__all__ = [
    "MCP_TRANSPORTS",
    "MCP_AUTH_MODES",
    "MCP_RISK_LEVELS",
    "MCP_ENVIRONMENTS",
    "MCP_PERMISSION_LEVELS",
    "McpServerConfig",
    "McpToolGrant",
    "McpBrokerPolicy",
    "McpEgressPolicy",
    "ExplicitMcpEgressPolicy",
    "McpTransportCredentials",
    "McpCredentialProvider",
    "McpCredentialBinding",
    "EnvironmentMcpCredentialProvider",
    "McpAuditEvent",
    "McpAuditSink",
    "InMemoryMcpAuditSink",
    "McpIdempotencyRecord",
    "McpIdempotencyStore",
    "InMemoryMcpIdempotencyStore",
    "McpTransportClient",
    "McpTransportError",
    "OfficialMcpTransportClient",
    "GovernedMcpToolBroker",
]
