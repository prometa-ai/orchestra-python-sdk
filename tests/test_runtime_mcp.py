"""Phase 2B governed tenant-side MCP broker tests."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from prometa.runtime import (
    EnvironmentMcpCredentialProvider,
    ExplicitMcpEgressPolicy,
    GovernedMcpToolBroker,
    InMemoryMcpAuditSink,
    InMemoryMcpIdempotencyStore,
    McpBrokerPolicy,
    McpCredentialBinding,
    McpServerConfig,
    McpToolGrant,
    McpTransportCredentials,
    McpTransportError,
    RuntimeExecutionError,
    RuntimeTool,
    ToolInvocationRequest,
)


TOOL = RuntimeTool(
    name="Lookup order",
    source="mcp",
    operation="orders.lookup",
    input_schema={"type": "object"},
    mcp_server="Orders",
    side_effects="read-only",
    risk_level="low",
    auth_binding="api-key",
    scopes=("orders.read",),
    approval_required=False,
    required_guardrails=(),
)

SERVER = McpServerConfig(
    name="Orders",
    connection_id="conn-orders-prod",
    transport="streamable-http",
    environment="production",
    auth_mode="api-key",
    scopes=("orders.read", "orders.write"),
    risk_level="low",
    endpoint="https://orders.internal/mcp",
)

GRANT = McpToolGrant(
    tool_name="orders.lookup",
    permission="read",
    risk_level="low",
    server_connection_id="conn-orders-prod",
)


class RecordingTransport:
    def __init__(self, output=None, error=None) -> None:
        self.output = output if output is not None else {"status": "found"}
        self.error = error
        self.calls = []

    async def call_tool(
        self, server, operation, arguments, credentials, metadata
    ):
        self.calls.append(
            {
                "server": server,
                "operation": operation,
                "arguments": arguments,
                "credentials": credentials,
                "metadata": metadata,
            }
        )
        if self.error is not None:
            raise self.error
        return self.output


class StaticCredentialProvider:
    def __init__(self, credentials) -> None:
        self.credentials = credentials

    async def resolve(self, server):
        return self.credentials


class FailingAuditSink:
    async def record(self, event) -> None:
        raise OSError("audit store unavailable")


class FailAfterAuditSink:
    def __init__(self, accepted=1) -> None:
        self.accepted = accepted
        self.events = []

    async def record(self, event) -> None:
        if len(self.events) >= self.accepted:
            raise OSError("audit store unavailable")
        self.events.append(event)


class FailingIdempotencyStore:
    async def reserve(self, key, request_digest):
        raise OSError("idempotency store unavailable")

    async def complete(self, key, request_digest, output_digest):
        raise AssertionError("complete should not be called")

    async def release(self, key, request_digest):
        raise AssertionError("release should not be called")

    async def mark_indeterminate(self, key, request_digest):
        raise AssertionError("mark_indeterminate should not be called")


def _credentials(environ=None):
    return EnvironmentMcpCredentialProvider(
        [
            McpCredentialBinding(
                server_name="Orders",
                auth_mode="api-key",
                http_headers={"Authorization": "ORDERS_MCP_TOKEN"},
            )
        ],
        environ=(
            {"ORDERS_MCP_TOKEN": "Bearer tenant-secret"}
            if environ is None
            else environ
        ),
    )


def _request(tool=TOOL, **overrides):
    values = {
        "request_id": "request-1",
        "call_id": "call-1",
        "tool": tool,
        "arguments": {"orderId": "order-secret-42"},
        "agent_id": "agent-orders",
        "release_id": "release-7",
        "deployment_id": "deployment-prod",
        "environment": "prod",
        "granted_scopes": ("orders.read", "orders.write"),
        "approval_references": (),
    }
    values.update(overrides)
    return ToolInvocationRequest(**values)


def _broker(
    *,
    servers=(SERVER,),
    grants=(GRANT,),
    policy=None,
    egress_policy=None,
    transport=None,
    audit_sink=None,
    credential_provider=None,
    idempotency_store=None,
):
    transport = transport or RecordingTransport()
    audit_sink = audit_sink or InMemoryMcpAuditSink()
    broker = GovernedMcpToolBroker(
        servers=servers,
        grants=grants,
        policy=policy or McpBrokerPolicy(max_risk_level="medium"),
        egress_policy=egress_policy
        or ExplicitMcpEgressPolicy(
            allowed_http_origins=frozenset({"https://orders.internal"})
        ),
        transport_client=transport,
        audit_sink=audit_sink,
        credential_provider=credential_provider or _credentials(),
        idempotency_store=idempotency_store,
    )
    return broker, transport, audit_sink


def _error(broker, request):
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(broker.invoke(request))
    return caught.value.code


def test_governed_broker_executes_with_identity_and_payload_free_audit() -> None:
    audit = InMemoryMcpAuditSink()
    transport = RecordingTransport(
        {"status": "found", "privateResult": "result-secret-99"}
    )
    broker, _, _ = _broker(transport=transport, audit_sink=audit)

    result = asyncio.run(broker.invoke(_request()))

    assert result.output["status"] == "found"
    assert result.audit_reference.startswith("mcp-audit-")
    assert len(transport.calls) == 1
    call = transport.calls[0]
    assert call["operation"] == "orders.lookup"
    assert call["credentials"].headers["Authorization"] == "Bearer tenant-secret"
    assert call["metadata"]["prometa.io/request-id"] == "request-1"
    assert call["metadata"]["prometa.io/idempotency-key"].startswith("mcp1:")
    assert [(event.phase, event.outcome) for event in audit.events] == [
        ("authorization", "accepted"),
        ("execution", "completed"),
    ]
    completed = audit.events[-1]
    assert completed.agent_id == "agent-orders"
    assert completed.permission == "read"
    assert completed.effective_risk == "low"
    rendered_audit = repr(audit.events)
    assert "order-secret-42" not in rendered_audit
    assert "result-secret-99" not in rendered_audit
    assert "tenant-secret" not in rendered_audit
    assert completed.argument_digest.startswith("sha256:")
    assert completed.output_digest.startswith("sha256:")


@pytest.mark.parametrize(
    ("servers", "grants", "policy", "egress", "invocation", "code"),
    [
        (
            (replace(SERVER, enabled=False),),
            (GRANT,),
            McpBrokerPolicy(max_risk_level="medium"),
            None,
            _request(),
            "mcp_server_disabled",
        ),
        (
            (replace(SERVER, environment="staging"),),
            (GRANT,),
            McpBrokerPolicy(max_risk_level="medium"),
            None,
            _request(),
            "mcp_environment_mismatch",
        ),
        (
            (SERVER,),
            (GRANT,),
            McpBrokerPolicy(max_risk_level="medium"),
            None,
            _request(granted_scopes=()),
            "mcp_scope_not_granted",
        ),
        (
            (replace(SERVER, scopes=("orders.write",)),),
            (GRANT,),
            McpBrokerPolicy(max_risk_level="medium"),
            None,
            _request(),
            "mcp_server_scope_mismatch",
        ),
        (
            (SERVER,),
            (replace(GRANT, agent_ids=("other-agent",)),),
            McpBrokerPolicy(max_risk_level="medium"),
            None,
            _request(),
            "mcp_tool_not_granted",
        ),
        (
            (SERVER,),
            (GRANT,),
            McpBrokerPolicy(max_risk_level="low"),
            None,
            _request(tool=replace(TOOL, risk_level="high")),
            "mcp_risk_ceiling_exceeded",
        ),
        (
            (SERVER,),
            (GRANT,),
            McpBrokerPolicy(max_risk_level="medium"),
            ExplicitMcpEgressPolicy(),
            _request(),
            "mcp_egress_denied",
        ),
        (
            (SERVER,),
            (GRANT,),
            McpBrokerPolicy(max_risk_level="medium"),
            None,
            _request(tool=replace(TOOL, auth_binding="oauth")),
            "mcp_auth_binding_mismatch",
        ),
    ],
)
def test_governance_denials_are_audited_before_transport(
    servers, grants, policy, egress, invocation, code
) -> None:
    audit = InMemoryMcpAuditSink()
    transport = RecordingTransport()
    broker, _, _ = _broker(
        servers=servers,
        grants=grants,
        policy=policy,
        egress_policy=egress,
        transport=transport,
        audit_sink=audit,
    )

    assert _error(broker, invocation) == code
    assert transport.calls == []
    assert audit.events[-1].outcome == "denied"
    assert audit.events[-1].reason == code


def test_specific_grants_win_and_equal_specificity_is_fail_closed() -> None:
    global_grant = replace(
        GRANT,
        permission="read",
        risk_level="medium",
        server_connection_id=None,
    )
    agent_grant = replace(
        GRANT,
        agent_ids=("agent-orders",),
        permission="execute",
        risk_level="low",
    )
    audit = InMemoryMcpAuditSink()
    broker, _, _ = _broker(grants=(global_grant, agent_grant), audit_sink=audit)
    asyncio.run(broker.invoke(_request()))
    assert audit.events[-1].permission == "execute"
    assert audit.events[-1].effective_risk == "low"

    ambiguous = replace(agent_grant, agent_ids=("agent-orders", "agent-two"))
    broker, transport, _ = _broker(grants=(agent_grant, ambiguous))
    assert _error(broker, _request()) == "mcp_tool_grant_ambiguous"
    assert transport.calls == []


def test_write_calls_require_approval_and_a_shared_idempotency_boundary() -> None:
    write_tool = replace(
        TOOL,
        operation="orders.update",
        side_effects="write",
        scopes=("orders.write",),
    )
    write_grant = replace(GRANT, tool_name="orders.update", permission="write")
    request = _request(tool=write_tool)

    broker, transport, _ = _broker(grants=(write_grant,))
    assert _error(broker, request) == "mcp_approval_required"
    assert transport.calls == []

    approved = replace(request, approval_references=("tenant-review-7",))
    broker, transport, _ = _broker(grants=(write_grant,))
    assert _error(broker, approved) == "mcp_idempotency_store_required"
    assert transport.calls == []

    store = InMemoryMcpIdempotencyStore()
    broker, transport, audit = _broker(
        grants=(write_grant,), idempotency_store=store
    )
    first = asyncio.run(broker.invoke(approved))
    assert first.output == {"status": "found"}
    assert len(transport.calls) == 1
    assert audit.events[-1].approval_references == ("tenant-review-7",)

    assert _error(broker, approved) == "mcp_duplicate_tool_call"
    assert len(transport.calls) == 1
    conflict = replace(approved, arguments={"orderId": "different"})
    assert _error(broker, conflict) == "mcp_idempotency_conflict"
    assert len(transport.calls) == 1


def test_unknown_transport_outcome_blocks_automatic_replay() -> None:
    write_tool = replace(
        TOOL,
        operation="orders.update",
        side_effects="write",
        scopes=("orders.write",),
    )
    grant = replace(GRANT, tool_name="orders.update", permission="write")
    request = _request(
        tool=write_tool, approval_references=("tenant-review-8",)
    )
    store = InMemoryMcpIdempotencyStore()
    transport = RecordingTransport(
        error=McpTransportError("socket_closed", outcome_unknown=True)
    )
    broker, _, audit = _broker(
        grants=(grant,), transport=transport, idempotency_store=store
    )

    assert _error(broker, request) == "socket_closed"
    assert len(transport.calls) == 1
    assert audit.events[-1].reason == "socket_closed"
    assert _error(broker, request) == "mcp_tool_call_indeterminate"
    assert len(transport.calls) == 1


def test_idempotency_store_outage_is_audited_and_prevents_transport() -> None:
    audit = InMemoryMcpAuditSink()
    transport = RecordingTransport()
    broker, _, _ = _broker(
        audit_sink=audit,
        transport=transport,
        idempotency_store=FailingIdempotencyStore(),
    )

    assert _error(broker, _request()) == "mcp_idempotency_store_failed"
    assert transport.calls == []
    assert audit.events[-1].phase == "idempotency"
    assert audit.events[-1].outcome == "failed"
    assert audit.events[-1].reason == "mcp_idempotency_store_failed"


def test_audit_failures_are_fail_closed_before_and_after_execution() -> None:
    store = InMemoryMcpIdempotencyStore()
    first_transport = RecordingTransport()
    broker, _, _ = _broker(
        transport=first_transport,
        audit_sink=FailingAuditSink(),
        idempotency_store=store,
    )
    assert _error(broker, _request()) == "mcp_audit_failed"
    assert first_transport.calls == []

    recovery_transport = RecordingTransport()
    recovery, _, _ = _broker(
        transport=recovery_transport,
        idempotency_store=store,
    )
    asyncio.run(recovery.invoke(_request()))
    assert len(recovery_transport.calls) == 1

    post_store = InMemoryMcpIdempotencyStore()
    post_transport = RecordingTransport()
    post, _, _ = _broker(
        transport=post_transport,
        audit_sink=FailAfterAuditSink(accepted=1),
        idempotency_store=post_store,
    )
    assert _error(post, _request(call_id="call-after-audit")) == "mcp_audit_failed"
    assert len(post_transport.calls) == 1

    replay, _, _ = _broker(
        transport=post_transport,
        idempotency_store=post_store,
    )
    assert (
        _error(replay, _request(call_id="call-after-audit"))
        == "mcp_tool_call_indeterminate"
    )
    assert len(post_transport.calls) == 1


def test_credentials_are_resolved_late_and_must_match_transport_auth() -> None:
    missing = EnvironmentMcpCredentialProvider(
        [
            McpCredentialBinding(
                server_name="Orders",
                auth_mode="api-key",
                http_headers={"Authorization": "ORDERS_MCP_TOKEN"},
            )
        ],
        environ={},
    )
    broker, transport, _ = _broker(credential_provider=missing)
    assert _error(broker, _request()) == "mcp_credentials_missing"
    assert transport.calls == []

    no_auth_server = replace(SERVER, auth_mode="none")
    no_auth_tool = replace(TOOL, auth_binding="none")
    provider = StaticCredentialProvider(
        McpTransportCredentials(headers={"Authorization": "should-not-leak"})
    )
    broker, transport, _ = _broker(
        servers=(no_auth_server,), credential_provider=provider
    )
    assert _error(broker, _request(tool=no_auth_tool)) == "mcp_unexpected_credentials"
    assert transport.calls == []

    rotating_environment = {"ORDERS_MCP_TOKEN": "Bearer key-one"}
    rotating_transport = RecordingTransport()
    broker, _, _ = _broker(
        transport=rotating_transport,
        credential_provider=_credentials(rotating_environment),
    )
    asyncio.run(broker.invoke(_request(call_id="rotation-one")))
    rotating_environment["ORDERS_MCP_TOKEN"] = "Bearer key-two"
    asyncio.run(broker.invoke(_request(call_id="rotation-two")))
    assert [
        call["credentials"].headers["Authorization"]
        for call in rotating_transport.calls
    ] == ["Bearer key-one", "Bearer key-two"]


def test_response_bounds_and_non_finite_values_become_indeterminate() -> None:
    store = InMemoryMcpIdempotencyStore()
    tiny_server = replace(SERVER, max_response_bytes=10)
    transport = RecordingTransport({"result": "far too large"})
    broker, _, audit = _broker(
        servers=(tiny_server,),
        transport=transport,
        idempotency_store=store,
    )
    assert _error(broker, _request()) == "mcp_response_too_large"
    assert audit.events[-1].reason == "mcp_response_too_large"
    assert _error(broker, _request()) == "mcp_tool_call_indeterminate"

    nan_transport = RecordingTransport({"score": float("nan")})
    broker, _, _ = _broker(transport=nan_transport)
    assert _error(broker, _request(call_id="nan-call")) == "mcp_payload_not_json"


def test_server_and_credential_configuration_rejects_unsafe_shapes() -> None:
    with pytest.raises(ValueError, match="plain HTTP"):
        replace(SERVER, endpoint="http://orders.example.com/mcp")
    with pytest.raises(ValueError, match="userinfo"):
        replace(SERVER, endpoint="https://user:pass@orders.internal/mcp")
    with pytest.raises(ValueError, match="plain HTTP"):
        replace(
            SERVER,
            endpoint="http://192.0.2.10/mcp",
            allow_insecure_http=True,
        )
    private_http = replace(
        SERVER,
        endpoint="http://10.0.0.10/mcp",
        allow_insecure_http=True,
    )
    assert private_http.endpoint == "http://10.0.0.10/mcp"
    with pytest.raises(ValueError, match="credential header"):
        McpCredentialBinding(
            server_name="Orders",
            auth_mode="api-key",
            http_headers={"Mcp-Session-Id": "ORDERS_MCP_TOKEN"},
        )
    policy = ExplicitMcpEgressPolicy(
        allowed_http_origins=frozenset({"https://orders.internal:443"})
    )
    assert policy.allows(SERVER)
    assert not policy.allows(
        replace(SERVER, endpoint="https://other.internal/mcp")
    )
