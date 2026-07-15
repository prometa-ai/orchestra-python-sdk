"""Official MCP Python SDK transport compatibility test."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("mcp")

from prometa.runtime import (
    ExplicitMcpEgressPolicy,
    GovernedMcpToolBroker,
    InMemoryMcpAuditSink,
    McpBrokerPolicy,
    McpServerConfig,
    McpToolGrant,
    McpTransportCredentials,
    OfficialMcpTransportClient,
    RuntimeTool,
    ToolInvocationRequest,
)


def test_official_stdio_transport_initializes_and_calls_tool() -> None:
    server_script = Path(__file__).with_name("runtime_mcp_stdio_server.py")
    server = McpServerConfig(
        name="Runtime Echo",
        connection_id="conn-runtime-echo",
        transport="stdio",
        environment="development",
        auth_mode="none",
        scopes=(),
        risk_level="low",
        command=sys.executable,
        arguments=(str(server_script),),
        timeout_seconds=10,
    )
    tool = RuntimeTool(
        name="Runtime echo",
        source="mcp",
        operation="echo_runtime",
        input_schema={"type": "object"},
        mcp_server="Runtime Echo",
        side_effects="read-only",
        risk_level="low",
        auth_binding="none",
        scopes=(),
        approval_required=False,
        required_guardrails=(),
    )
    audit = InMemoryMcpAuditSink()
    broker = GovernedMcpToolBroker(
        servers=(server,),
        grants=(McpToolGrant(tool_name="echo_runtime"),),
        policy=McpBrokerPolicy(max_risk_level="low"),
        egress_policy=ExplicitMcpEgressPolicy(
            allowed_stdio_commands=frozenset({sys.executable})
        ),
        transport_client=OfficialMcpTransportClient(),
        audit_sink=audit,
    )
    request = ToolInvocationRequest(
        request_id="stdio-request-1",
        call_id="stdio-call-1",
        tool=tool,
        arguments={"value": "official-sdk-ok"},
        agent_id="agent-runtime-echo",
        release_id="release-runtime-echo",
        deployment_id="deployment-runtime-echo",
        environment="dev",
    )

    result = asyncio.run(broker.invoke(request))

    assert "official-sdk-ok" in json.dumps(result.output, sort_keys=True)
    assert [(event.phase, event.outcome) for event in audit.events] == [
        ("authorization", "accepted"),
        ("execution", "completed"),
    ]


def test_streamable_http_refuses_redirects_and_ambient_proxy_state(
    monkeypatch,
) -> None:
    import httpx
    import mcp
    import mcp.client.streamable_http as streamable_http

    observed = {}

    class FakeHttpClient:
        def __init__(self, **kwargs) -> None:
            observed["http"] = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeStreams:
        async def __aenter__(self):
            return "read", "write", lambda: None

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeResult:
        isError = False
        structuredContent = {"status": "ok"}
        content = ()

    class FakeSession:
        def __init__(self, read, write, **kwargs) -> None:
            observed["session"] = (read, write, kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def initialize(self):
            observed["initialized"] = True

        async def call_tool(self, name, arguments=None, *, meta=None, **kwargs):
            observed["call"] = (name, arguments, meta)
            return FakeResult()

    def fake_streamable_http_client(url, **kwargs):
        observed["url"] = url
        observed["stream"] = kwargs
        return FakeStreams()

    monkeypatch.setattr(httpx, "AsyncClient", FakeHttpClient)
    monkeypatch.setattr(mcp, "ClientSession", FakeSession)
    monkeypatch.setattr(
        streamable_http, "streamable_http_client", fake_streamable_http_client
    )

    server = McpServerConfig(
        name="HTTP Tools",
        connection_id="conn-http-tools",
        transport="streamable-http",
        environment="development",
        auth_mode="api-key",
        scopes=(),
        risk_level="low",
        endpoint="https://mcp.integration.example.com/mcp",
    )
    output = asyncio.run(
        OfficialMcpTransportClient().call_tool(
            server,
            "health_check",
            {"probe": True},
            McpTransportCredentials(headers={"Authorization": "Bearer secret"}),
            {"prometa.io/request-id": "request-http"},
        )
    )

    assert output == {"status": "ok"}
    assert observed["http"]["follow_redirects"] is False
    assert observed["http"]["trust_env"] is False
    assert observed["http"]["headers"] == {"Authorization": "Bearer secret"}
    assert observed["url"] == "https://mcp.integration.example.com/mcp"
    assert observed["stream"]["terminate_on_close"] is True
    assert observed["initialized"] is True
    assert observed["call"] == (
        "health_check",
        {"probe": True},
        {"prometa.io/request-id": "request-http"},
    )
