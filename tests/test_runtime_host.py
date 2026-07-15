"""Tenant reference-runtime host boundary and HTTP behavior tests."""

from __future__ import annotations

import asyncio
import http.client
import io
import json
import os
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

pytest.importorskip("cryptography")
pytest.importorskip("jsonschema")

from prometa.runtime import (
    BASE_RUNTIME_CAPABILITIES,
    CAPABILITY_SCHEMA_VALIDATE,
    BundleTrustEntry,
    BundleTrustStore,
    InMemoryAdmissionReplayStore,
    InMemoryEvidenceEmitter,
    InMemoryRuntimeTaskStore,
    GovernedMcpToolBroker,
    HumanEscalationDecision,
    JsonLineEvidenceEmitter,
    ModelAdapterError,
    ModelInvocationResponse,
    ModelToolCall,
    PostgresRuntimeTaskStore,
    ReferenceRuntimeHost,
    RuntimeAdmissionPolicy,
    RuntimeActivationResult,
    RuntimeEvidenceEvent,
    RuntimeHostError,
    RuntimeHostConfig,
    RuntimeKernel,
    RuntimePersistenceError,
    RuntimeTool,
    admit_runtime_release,
    build_reference_runtime_host,
    canonical_payload_digest,
    install_postgres_runtime_schema,
    load_runtime_host_config,
)
from prometa.runtime.host import _RuntimeHttpServer


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "runtime-kernel-v1.json"
API_TOKEN = "runtime-test-token-0123456789abcdef"


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _trust(value) -> BundleTrustStore:
    return BundleTrustStore(
        [
            BundleTrustEntry(
                issuer=value["issuer"],
                key_id=value["keyId"],
                public_key_spki_der_base64=value["publicKeySpkiDerBase64"],
            )
        ]
    )


def _vector():
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _admitted():
    vector = _vector()
    verification = vector["verification"]
    admitted = admit_runtime_release(
        vector["bundle"],
        vector["attestation"],
        bundle_trust_store=_trust(vector["bundleTrust"]),
        promotion_trust_store=_trust(vector["promotionTrust"]),
        replay_store=InMemoryAdmissionReplayStore(),
        policy=RuntimeAdmissionPolicy(
            expected_org_id=verification["expectedOrgId"],
            expected_environment=verification["expectedEnvironment"],
            expected_release_id=verification["expectedReleaseId"],
            expected_deployment_id=verification["expectedDeploymentId"],
            expected_runtime=verification["expectedRuntime"],
            supported_capabilities=frozenset(
                {*BASE_RUNTIME_CAPABILITIES, CAPABILITY_SCHEMA_VALIDATE}
            ),
        ),
        now=_instant(verification["now"]),
    )
    return vector, admitted


def _mcp_admitted():
    vector, admitted = _admitted()
    tool = RuntimeTool(
        name="Write order",
        source="mcp",
        operation="orders.write",
        input_schema={"type": "object"},
        mcp_server="Orders",
        side_effects="write",
        risk_level="medium",
        auth_binding="service-account",
        scopes=("orders.write",),
        approval_required=True,
        required_guardrails=(),
    )
    config = replace(
        admitted.config,
        tools=(tool,),
        mcp_servers=("Orders",),
        required_scopes=("orders.write",),
        granted_scopes=("orders.write",),
    )
    return vector, replace(admitted, config=config)


class RecordingModelAdapter:
    def __init__(self, output):
        self.output = output
        self.requests = []

    async def invoke(self, request):
        self.requests.append(request)
        return ModelInvocationResponse(content=self.output)


class SleepingModelAdapter:
    async def invoke(self, request):
        await asyncio.sleep(60)
        raise AssertionError("cancelled request resumed")


def _host(adapter, *, timeout=2.0, task_store=None):
    _, admitted = _admitted()
    kernel = RuntimeKernel(
        admitted,
        model_adapter=adapter,
        evidence_emitter=InMemoryEvidenceEmitter(),
        runtime_id="reference-host-test",
        runtime_version="0.18.0",
    )
    return ReferenceRuntimeHost(
        kernel,
        api_token=API_TOKEN,
        request_timeout_seconds=timeout,
        max_request_bytes=1024,
        task_store=task_store,
        task_lease_seconds=max(10.0, timeout + 1),
    )


def _execute(host, payload, *, token=API_TOKEN, content_type="application/json"):
    return host.handle(
        "POST",
        "/v1/runtime/execute",
        {
            "authorization": "Bearer %s" % token,
            "content-type": content_type,
        },
        json.dumps(payload).encode("utf-8"),
    )


def test_reference_host_health_auth_schema_and_success_contract() -> None:
    vector = _vector()
    adapter = RecordingModelAdapter(vector["sampleOutput"])
    host = _host(adapter)
    try:
        assert host.handle("GET", "/healthz", {}).body == {"status": "ok"}
        ready = host.handle("GET", "/readyz", {})
        assert ready.status == 200
        assert ready.body == {"status": "ready"}

        unauthorized = _execute(
            host,
            {"requestId": "request-1", "input": vector["sampleInput"]},
            token="wrong-token-0123456789abcdefghijkl",
        )
        assert unauthorized.status == 401
        assert unauthorized.body == {"error": {"code": "unauthorized"}}
        assert adapter.requests == []

        unsupported = _execute(
            host,
            {"requestId": "request-1", "input": vector["sampleInput"]},
            content_type="text/plain",
        )
        assert unsupported.status == 415
        assert adapter.requests == []

        invalid = _execute(
            host,
            {"requestId": "request-invalid", "input": {"unexpected": True}},
        )
        assert invalid.status == 422
        assert invalid.body == {"error": {"code": "input_schema_invalid"}}
        assert adapter.requests == []

        response = _execute(
            host,
            {"requestId": "request-valid", "input": vector["sampleInput"]},
        )
        assert response.status == 200
        assert response.body == {
            "requestId": "request-valid",
            "output": vector["sampleOutput"],
            "modelName": "golden-model",
            "attempts": 1,
            "toolCalls": 0,
            "usedFallback": False,
        }
        assert len(adapter.requests) == 1

        duplicate_json = host.handle(
            "POST",
            "/v1/runtime/execute",
            {
                "authorization": "Bearer %s" % API_TOKEN,
                "content-type": "application/json",
            },
            b'{"requestId":"one","requestId":"two","input":{}}',
        )
        assert duplicate_json.status == 400
        assert duplicate_json.body == {"error": {"code": "request_invalid_json"}}
        assert host.handle("GET", "/v1/runtime/execute", {}).status == 405
        assert host.handle("GET", "/missing", {}).status == 404
    finally:
        host.close()


def test_reference_host_timeout_cancels_work_without_leaking_details() -> None:
    host = _host(SleepingModelAdapter(), timeout=0.02)
    try:
        response = _execute(
            host,
            {"requestId": "request-timeout", "input": _vector()["sampleInput"]},
        )
        assert response.status == 504
        assert response.body == {"error": {"code": "runtime_request_timeout"}}
    finally:
        host.close()


def test_reference_host_rejects_parallel_duplicate_request_ids() -> None:
    entered = threading.Event()
    release = threading.Event()

    class BlockingAdapter:
        async def invoke(self, request):
            entered.set()
            await asyncio.to_thread(release.wait)
            return ModelInvocationResponse(content=_vector()["sampleOutput"])

    host = _host(BlockingAdapter())
    first = []

    def run_first():
        first.append(
            _execute(
                host,
                {"requestId": "request-shared", "input": _vector()["sampleInput"]},
            )
        )

    thread = threading.Thread(target=run_first)
    thread.start()
    assert entered.wait(timeout=1)
    try:
        duplicate = _execute(
            host,
            {"requestId": "request-shared", "input": _vector()["sampleInput"]},
        )
        assert duplicate.status == 409
        assert duplicate.body == {"error": {"code": "request_in_progress"}}
    finally:
        release.set()
        thread.join(timeout=2)
        host.close()
    assert first[0].status == 200


def test_reference_hosts_coordinate_durable_tasks_and_replay_status() -> None:
    store = InMemoryRuntimeTaskStore()
    entered = threading.Event()
    release = threading.Event()

    class BlockingAdapter:
        async def invoke(self, request):
            entered.set()
            await asyncio.to_thread(release.wait)
            return ModelInvocationResponse(content=_vector()["sampleOutput"])

    first_host = _host(BlockingAdapter(), task_store=store)
    second_host = _host(
        RecordingModelAdapter(_vector()["sampleOutput"]), task_store=store
    )
    responses = []
    thread = threading.Thread(
        target=lambda: responses.append(
            _execute(
                first_host,
                {
                    "requestId": "request-durable",
                    "input": _vector()["sampleInput"],
                },
            )
        )
    )
    thread.start()
    assert entered.wait(timeout=1)
    try:
        duplicate = _execute(
            second_host,
            {
                "requestId": "request-durable",
                "input": _vector()["sampleInput"],
            },
        )
        assert duplicate.status == 409
        assert duplicate.body == {"error": {"code": "task_in_progress"}}
    finally:
        release.set()
        thread.join(timeout=2)
    try:
        assert responses[0].status == 200
        unauthorized = second_host.handle(
            "GET", "/v1/runtime/tasks/request-durable", {}, b""
        )
        assert unauthorized.status == 401
        status = second_host.handle(
            "GET",
            "/v1/runtime/tasks/request-durable",
            {"authorization": "Bearer %s" % API_TOKEN},
        )
        assert status.status == 200
        assert status.body["taskLifecycleVersion"] == 1
        assert status.body["status"] == "completed"
        assert status.body["attempt"] == 1
        assert status.body["outputDigest"].startswith("sha256:")
        assert [item["transition"] for item in status.body["lifecycle"]] == [
            "claimed",
            "completed",
        ]
        assert "input" not in status.body
        assert "output" not in status.body
        repeated = _execute(
            second_host,
            {
                "requestId": "request-durable",
                "input": _vector()["sampleInput"],
            },
        )
        assert repeated.status == 409
        assert repeated.body == {
            "error": {"code": "task_already_completed"}
        }
        changed = _execute(
            second_host,
            {
                "requestId": "request-durable",
                "input": {"question": "changed"},
            },
        )
        assert changed.status == 409
        assert changed.body == {"error": {"code": "task_identity_conflict"}}
        missing = second_host.handle(
            "GET",
            "/v1/runtime/tasks/missing",
            {"authorization": "Bearer %s" % API_TOKEN},
        )
        assert missing.status == 404
        claim_events = [
            event
            for event in first_host.kernel.evidence_emitter.events
            if event.name == "runtime.task.claim"
        ]
        assert claim_events[0].outcome == "claimed"
        assert "question" not in repr(claim_events)
    finally:
        first_host.close()
        second_host.close()


def test_reference_host_retries_payload_free_task_after_retryable_failure() -> None:
    store = InMemoryRuntimeTaskStore()

    class RecoveringAdapter:
        def __init__(self):
            self.calls = 0

        async def invoke(self, request):
            self.calls += 1
            if self.calls <= 2:
                raise ModelAdapterError("gateway_unavailable", retryable=True)
            return ModelInvocationResponse(content=_vector()["sampleOutput"])

    adapter = RecoveringAdapter()
    host = _host(adapter, task_store=store)
    request = {
        "requestId": "request-retry",
        "input": _vector()["sampleInput"],
    }
    try:
        first = _execute(host, request)
        assert first.status == 503
        assert first.body == {"error": {"code": "gateway_unavailable"}}
        retryable = store.get("request-retry")
        assert retryable is not None
        assert retryable.record.status == "retryable"

        second = _execute(host, request)
        assert second.status == 200
        completed = store.get("request-retry")
        assert completed is not None
        assert completed.record.attempt == 2
        assert [event.transition for event in completed.events] == [
            "claimed",
            "retry_scheduled",
            "retried",
            "completed",
        ]
    finally:
        host.close()


def test_reference_host_reclaims_an_expired_model_only_attempt() -> None:
    store = InMemoryRuntimeTaskStore()
    vector, admitted = _admitted()
    promotion = admitted.promotion.claims
    store.claim(
        "request-orphan",
        input_digest=canonical_payload_digest(vector["sampleInput"]),
        artifact_digest=admitted.artifact_digest,
        release_id=promotion["releaseId"],
        deployment_id=promotion["deploymentId"],
        recoverable=True,
        max_attempts=3,
        lease_seconds=10,
        now=datetime.now(timezone.utc) - timedelta(seconds=11),
    )
    host = _host(
        RecordingModelAdapter(vector["sampleOutput"]),
        task_store=store,
    )
    try:
        response = _execute(
            host,
            {"requestId": "request-orphan", "input": vector["sampleInput"]},
        )
        assert response.status == 200
        snapshot = store.get("request-orphan")
        assert snapshot is not None
        assert snapshot.record.attempt == 2
        assert [event.transition for event in snapshot.events] == [
            "claimed",
            "recovered",
            "completed",
        ]
        claim_events = [
            event
            for event in host.kernel.evidence_emitter.events
            if event.name == "runtime.task.claim"
        ]
        assert claim_events[0].outcome == "recovered"
    finally:
        host.close()


def test_reference_host_fails_closed_when_task_store_is_unavailable() -> None:
    def unavailable(dsn):
        raise OSError("database unavailable at %s" % dsn)

    adapter = RecordingModelAdapter(_vector()["sampleOutput"])
    store = PostgresRuntimeTaskStore(
        "postgresql://secret:password@db.example/runtime",
        tenant_id="tenant-1",
        runtime_id="runtime-1",
        connect=unavailable,
    )
    host = _host(adapter, task_store=store)
    try:
        response = _execute(
            host,
            {"requestId": "request-outage", "input": _vector()["sampleInput"]},
        )
        assert response.status == 503
        assert response.body == {"error": {"code": "task_store_unavailable"}}
        assert adapter.requests == []
        status = host.handle(
            "GET",
            "/v1/runtime/tasks/request-outage",
            {"authorization": "Bearer %s" % API_TOKEN},
        )
        assert status.status == 503
        assert "secret" not in repr(status.body)
        assert "password" not in repr(status.body)
    finally:
        host.close()


def test_reference_host_shutdown_drains_inflight_and_refuses_new_work() -> None:
    entered = threading.Event()
    release = threading.Event()

    class BlockingAdapter:
        async def invoke(self, request):
            entered.set()
            await asyncio.to_thread(release.wait)
            return ModelInvocationResponse(content=_vector()["sampleOutput"])

    host = _host(BlockingAdapter())
    request_thread = threading.Thread(
        target=lambda: _execute(
            host,
            {"requestId": "request-drain", "input": _vector()["sampleInput"]},
        )
    )
    request_thread.start()
    assert entered.wait(timeout=1)
    close_thread = threading.Thread(target=host.close)
    close_thread.start()
    try:
        close_thread.join(timeout=0.05)
        assert close_thread.is_alive()
        refused = _execute(
            host,
            {"requestId": "request-new", "input": _vector()["sampleInput"]},
        )
        assert refused.status == 503
        assert refused.body == {"error": {"code": "runtime_host_stopping"}}
    finally:
        release.set()
        request_thread.join(timeout=2)
        close_thread.join(timeout=2)
    assert not request_thread.is_alive()
    assert not close_thread.is_alive()


def test_reference_host_serves_real_http_and_bounds_content_length() -> None:
    vector = _vector()
    host = _host(
        RecordingModelAdapter(vector["sampleOutput"]),
        task_store=InMemoryRuntimeTaskStore(),
    )
    server = _RuntimeHttpServer(("127.0.0.1", 0), host)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = "http://127.0.0.1:%d" % server.server_address[1]
    try:
        with urllib.request.urlopen(base_url + "/healthz", timeout=2) as response:
            assert json.loads(response.read()) == {"status": "ok"}
            assert "Python" not in response.headers.get("server", "")
            assert response.headers["x-content-type-options"] == "nosniff"

        body = json.dumps(
            {"requestId": "request-http", "input": vector["sampleInput"]}
        ).encode("utf-8")
        request = urllib.request.Request(
            base_url + "/v1/runtime/execute",
            data=body,
            method="POST",
            headers={
                "authorization": "Bearer %s" % API_TOKEN,
                "content-type": "application/json",
            },
        )
        with urllib.request.urlopen(request, timeout=2) as response:
            assert json.loads(response.read())["output"] == vector["sampleOutput"]

        status_request = urllib.request.Request(
            base_url + "/v1/runtime/tasks/request-http",
            headers={"authorization": "Bearer %s" % API_TOKEN},
        )
        with urllib.request.urlopen(status_request, timeout=2) as response:
            task_status = json.loads(response.read())
            assert task_status["taskLifecycleVersion"] == 1
            assert task_status["status"] == "completed"
            assert response.headers["cache-control"] == "no-store"

        duplicate_auth = http.client.HTTPConnection(
            "127.0.0.1", server.server_address[1], timeout=2
        )
        duplicate_auth.putrequest("GET", "/v1/runtime/tasks/request-http")
        duplicate_auth.putheader("authorization", "Bearer %s" % API_TOKEN)
        duplicate_auth.putheader("authorization", "Bearer %s" % API_TOKEN)
        duplicate_auth.endheaders()
        duplicate_response = duplicate_auth.getresponse()
        assert duplicate_response.status == 401
        duplicate_response.read()
        duplicate_auth.close()

        oversized = urllib.request.Request(
            base_url + "/v1/runtime/execute",
            data=b"x" * 1025,
            method="POST",
            headers={
                "authorization": "Bearer %s" % API_TOKEN,
                "content-type": "application/json",
            },
        )
        with pytest.raises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(oversized, timeout=2)
        assert caught.value.code == 413
        assert json.loads(caught.value.read()) == {
            "error": {"code": "request_too_large"}
        }

        unauthenticated = urllib.request.Request(
            base_url + "/v1/runtime/execute",
            data=b"x" * 1025,
            method="POST",
            headers={
                "authorization": "Bearer invalid",
                "content-type": "application/json",
            },
        )
        with pytest.raises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(unauthenticated, timeout=2)
        assert caught.value.code == 401
        assert caught.value.headers["www-authenticate"] == "Bearer"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        host.close()


def _config_document():
    vector = _vector()
    verification = vector["verification"]
    return {
        "configVersion": 1,
        "tenantId": "tenant-golden",
        "runtimeId": "runtime-golden",
        "runtimeVersion": "0.18.0",
        "orgId": verification["expectedOrgId"],
        "environment": verification["expectedEnvironment"],
        "releaseId": verification["expectedReleaseId"],
        "deploymentId": verification["expectedDeploymentId"],
        "runtimeTarget": verification["expectedRuntime"],
        "bundle": vector["bundle"],
        "promotionAttestation": vector["attestation"],
        "bundleTrust": [
            {
                "issuer": vector["bundleTrust"]["issuer"],
                "keyId": vector["bundleTrust"]["keyId"],
                "publicKeySpkiDerBase64": vector["bundleTrust"][
                    "publicKeySpkiDerBase64"
                ],
                "allowedOrgIds": [verification["expectedOrgId"]],
                "allowedAudiences": ["prometa-runtime"],
                "allowedEnvironments": [verification["expectedEnvironment"]],
            }
        ],
        "promotionTrust": [
            {
                "issuer": vector["promotionTrust"]["issuer"],
                "keyId": vector["promotionTrust"]["keyId"],
                "publicKeySpkiDerBase64": vector["promotionTrust"][
                    "publicKeySpkiDerBase64"
                ],
                "allowedOrgIds": [verification["expectedOrgId"]],
                "allowedAudiences": ["prometa-runtime-admission"],
                "allowedEnvironments": [verification["expectedEnvironment"]],
            }
        ],
        "modelGateway": {
            "baseUrl": "http://model-gateway:8000",
            "apiKeyEnv": "MODEL_GATEWAY_API_KEY",
        },
    }


def _mcp_broker_document():
    return {
        "servers": [
            {
                "name": "Orders",
                "connectionId": "orders-prod",
                "transport": "streamable-http",
                "environment": "production",
                "authMode": "service-account",
                "scopes": ["orders.write"],
                "riskLevel": "medium",
                "endpoint": "http://mcp-orders.default.svc:8000/mcp",
                "allowInsecureHttp": True,
                "timeoutSeconds": 20,
                "maxResponseBytes": 1048576,
            }
        ],
        "grants": [
            {
                "toolName": "orders.write",
                "agentIds": ["agent-golden-1"],
                "permission": "write",
                "riskLevel": "medium",
                "serverConnectionId": "orders-prod",
            }
        ],
        "policy": {
            "maxRiskLevel": "medium",
            "requireApprovalFor": ["write", "destructive"],
            "requireIdempotencyFor": ["write", "destructive"],
        },
        "egress": {
            "allowedHttpOrigins": ["http://mcp-orders.default.svc:8000"],
            "allowedStdioCommands": [],
        },
        "credentialBindings": [
            {
                "serverName": "Orders",
                "authMode": "service-account",
                "httpHeaders": {
                    "Authorization": "MCP_ORDERS_AUTHORIZATION"
                },
            }
        ],
        "toolTimeoutSeconds": 25,
        "reservationTimeoutSeconds": 90,
    }


def test_host_config_is_strict_bounded_and_keeps_secrets_in_environment(
    tmp_path,
) -> None:
    path = tmp_path / "config.json"
    path.write_text(json.dumps(_config_document()), encoding="utf-8")
    config = load_runtime_host_config(path)
    assert config.runtime_id == "runtime-golden"
    assert config.model_gateway_api_key_env == "MODEL_GATEWAY_API_KEY"
    assert config.database_dsn_env == "PROMETA_RUNTIME_DATABASE_URL"
    assert config.api_token_env == "PROMETA_RUNTIME_API_TOKEN"
    assert config.receipt_base_url is None
    assert config.control_plane_base_url is None
    assert config.task_recovery_enabled is False

    invalid = _config_document()
    invalid["unknown"] = True
    path.write_text(json.dumps(invalid), encoding="utf-8")
    with pytest.raises(RuntimeHostError) as caught:
        load_runtime_host_config(path)
    assert caught.value.code == "host_config_invalid"

    invalid = _config_document()
    invalid["apiTokenEnv"] = "INVALID-NAME"
    path.write_text(json.dumps(invalid), encoding="utf-8")
    with pytest.raises(RuntimeHostError) as caught:
        load_runtime_host_config(path)
    assert caught.value.code == "invalid_api_token_env"

    configured = _config_document()
    configured["receiptDelivery"] = {
        "baseUrl": "https://orchestra.example.test/control",
        "apiKeyEnv": "ORCHESTRA_RUNTIME_RECEIPT_API_KEY",
        "timeoutSeconds": 3,
        "pollIntervalSeconds": 1,
        "leaseSeconds": 10,
        "initialBackoffSeconds": 2,
        "maxBackoffSeconds": 20,
    }
    path.write_text(json.dumps(configured), encoding="utf-8")
    receipt_config = load_runtime_host_config(path)
    assert receipt_config.receipt_base_url == "https://orchestra.example.test/control"
    assert (
        receipt_config.receipt_api_key_env
        == "ORCHESTRA_RUNTIME_RECEIPT_API_KEY"
    )
    assert receipt_config.receipt_timeout_seconds == 3
    assert receipt_config.receipt_lease_seconds == 10

    pulled = _config_document()
    pulled.pop("bundle")
    pulled.pop("promotionAttestation")
    pulled["controlPlanePull"] = {
        "baseUrl": "https://orchestra.example.test",
        "attestationId": "runtime-kernel-attestation-v1",
        "apiKeyEnv": "ORCHESTRA_RUNTIME_CONTROL_PLANE_API_KEY",
        "timeoutSeconds": 4,
        "maxResponseBytes": 2 * 1024 * 1024,
        "maxClockSkewSeconds": 120,
        "maxCacheAgeSeconds": 600,
    }
    path.write_text(json.dumps(pulled), encoding="utf-8")
    pull_config = load_runtime_host_config(path)
    assert pull_config.bundle is None
    assert pull_config.promotion_attestation is None
    assert pull_config.control_plane_base_url == "https://orchestra.example.test"
    assert (
        pull_config.control_plane_api_key_env
        == "ORCHESTRA_RUNTIME_CONTROL_PLANE_API_KEY"
    )
    assert pull_config.control_plane_max_cache_age_seconds == 600
    assert pull_config.control_plane_max_clock_skew_seconds == 120

    durable = _config_document()
    durable["taskRecovery"] = {
        "leaseSeconds": 90,
        "maxAttempts": 4,
        "historyLimit": 25,
    }
    path.write_text(json.dumps(durable), encoding="utf-8")
    durable_config = load_runtime_host_config(path)
    assert durable_config.task_recovery_enabled is True
    assert durable_config.task_recovery_lease_seconds == 90
    assert durable_config.task_recovery_max_attempts == 4
    assert durable_config.task_recovery_history_limit == 25

    mcp_enabled = _config_document()
    mcp_enabled["mcpBroker"] = _mcp_broker_document()
    path.write_text(json.dumps(mcp_enabled), encoding="utf-8")
    mcp_config = load_runtime_host_config(path)
    assert mcp_config.mcp_broker is not None
    assert mcp_config.mcp_broker.servers[0].name == "Orders"
    assert mcp_config.mcp_broker.servers[0].connection_id == "orders-prod"
    assert mcp_config.mcp_broker.grants[0].tool_name == "orders.write"
    assert mcp_config.mcp_broker.tool_timeout_seconds == 25
    assert mcp_config.mcp_broker.reservation_timeout_seconds == 90
    assert (
        mcp_config.mcp_broker.credential_bindings[0].http_headers[
            "Authorization"
        ]
        == "MCP_ORDERS_AUTHORIZATION"
    )

    weakened = json.loads(json.dumps(mcp_enabled))
    weakened["mcpBroker"]["policy"]["requireIdempotencyFor"] = ["write"]
    path.write_text(json.dumps(weakened), encoding="utf-8")
    with pytest.raises(RuntimeHostError) as caught:
        load_runtime_host_config(path)
    assert caught.value.code == "mcp_policy_weakened"

    missing_credential_binding = json.loads(json.dumps(mcp_enabled))
    missing_credential_binding["mcpBroker"]["credentialBindings"] = []
    path.write_text(json.dumps(missing_credential_binding), encoding="utf-8")
    with pytest.raises(RuntimeHostError) as caught:
        load_runtime_host_config(path)
    assert caught.value.code == "mcp_credential_binding_missing"

    missing_egress = json.loads(json.dumps(mcp_enabled))
    missing_egress["mcpBroker"]["egress"]["allowedHttpOrigins"] = []
    path.write_text(json.dumps(missing_egress), encoding="utf-8")
    with pytest.raises(RuntimeHostError) as caught:
        load_runtime_host_config(path)
    assert caught.value.code == "mcp_egress_binding_missing"

    short_reservation = json.loads(json.dumps(mcp_enabled))
    short_reservation["mcpBroker"]["reservationTimeoutSeconds"] = 25
    path.write_text(json.dumps(short_reservation), encoding="utf-8")
    with pytest.raises(RuntimeHostError) as caught:
        load_runtime_host_config(path)
    assert caught.value.code == "mcp_reservation_timeout_too_short"

    durable["taskRecovery"]["leaseSeconds"] = 60
    path.write_text(json.dumps(durable), encoding="utf-8")
    with pytest.raises(RuntimeHostError) as caught:
        load_runtime_host_config(path)
    assert caught.value.code == "task_recovery_lease_too_short"

    invalid_source = _config_document()
    invalid_source["controlPlanePull"] = pulled["controlPlanePull"]
    path.write_text(json.dumps(invalid_source), encoding="utf-8")
    with pytest.raises(RuntimeHostError) as caught:
        load_runtime_host_config(path)
    assert caught.value.code == "release_source_invalid"

    pulled["controlPlanePull"]["baseUrl"] = "http://orchestra.example.test"
    path.write_text(json.dumps(pulled), encoding="utf-8")
    with pytest.raises(RuntimeHostError) as caught:
        load_runtime_host_config(path)
    assert caught.value.code == "insecure_control_plane_base_url"

    configured["receiptDelivery"]["baseUrl"] = "http://orchestra.example.test"
    path.write_text(json.dumps(configured), encoding="utf-8")
    with pytest.raises(RuntimeHostError) as caught:
        load_runtime_host_config(path)
    assert caught.value.code == "insecure_receipt_base_url"

    configured["receiptDelivery"]["allowInsecureHttp"] = True
    configured["receiptDelivery"]["leaseSeconds"] = 2
    path.write_text(json.dumps(configured), encoding="utf-8")
    with pytest.raises(RuntimeHostError) as caught:
        load_runtime_host_config(path)
    assert caught.value.code == "receipt_lease_too_short"

    path.write_text('{"configVersion":1,"configVersion":1}', encoding="utf-8")
    with pytest.raises(RuntimeHostError) as caught:
        load_runtime_host_config(path)
    assert caught.value.code == "host_config_invalid_json"


def test_host_rejects_incompatible_database_before_release_activation(
    tmp_path, monkeypatch
) -> None:
    import prometa.runtime.host as host_module

    path = tmp_path / "config.json"
    path.write_text(json.dumps(_config_document()), encoding="utf-8")
    config = load_runtime_host_config(path)

    def incompatible(_dsn):
        raise RuntimePersistenceError("runtime_schema_too_new")

    monkeypatch.setattr(
        host_module, "check_postgres_runtime_compatibility", incompatible
    )
    with pytest.raises(RuntimeHostError) as caught:
        build_reference_runtime_host(
            config,
            environment={
                "PROMETA_RUNTIME_DATABASE_URL": "postgresql://unused",
                "PROMETA_RUNTIME_API_TOKEN": (
                    "runtime-token-0123456789abcdefghijklmnop"
                ),
            },
        )
    assert caught.value.code == "runtime_schema_too_new"


def test_reference_host_wires_mcp_only_for_an_exact_signed_release_binding(
    tmp_path, monkeypatch
) -> None:
    import prometa.runtime.host as host_module

    document = _config_document()
    document["mcpBroker"] = _mcp_broker_document()
    path = tmp_path / "config.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    config = load_runtime_host_config(path)
    _vector_value, admitted = _mcp_admitted()

    monkeypatch.setattr(
        host_module, "check_postgres_runtime_compatibility", lambda _dsn: None
    )
    monkeypatch.setattr(
        host_module,
        "activate_runtime_release",
        lambda *args, **kwargs: (
            admitted,
            RuntimeActivationResult(created=True),
        ),
    )
    environment = {
        "PROMETA_RUNTIME_DATABASE_URL": "postgresql://unused",
        "PROMETA_RUNTIME_API_TOKEN": API_TOKEN,
        "MODEL_GATEWAY_API_KEY": "model-key",
        "MCP_ORDERS_AUTHORIZATION": "Bearer tenant-mcp-key",
    }

    class TenantTransport:
        async def call_tool(self, *args, **kwargs):
            return {"ok": True}

    monkeypatch.setattr(
        host_module, "official_mcp_transport_available", lambda: False
    )
    with pytest.raises(RuntimeHostError) as caught:
        build_reference_runtime_host(
            config,
            environment=environment,
            evidence_emitter=InMemoryEvidenceEmitter(),
        )
    assert caught.value.code == "mcp_dependency_missing"

    transport = TenantTransport()
    host, created = build_reference_runtime_host(
        config,
        environment=environment,
        evidence_emitter=InMemoryEvidenceEmitter(),
        mcp_transport_client=transport,
    )
    try:
        assert created is True
        assert isinstance(host.kernel.tool_broker, GovernedMcpToolBroker)
        assert host.kernel.policy.tool_timeout_seconds == 25
    finally:
        host.close()

    _vector_value, model_only = _admitted()
    monkeypatch.setattr(
        host_module,
        "activate_runtime_release",
        lambda *args, **kwargs: (
            model_only,
            RuntimeActivationResult(created=True),
        ),
    )
    with pytest.raises(RuntimeHostError) as caught:
        build_reference_runtime_host(
            config,
            environment=environment,
            evidence_emitter=InMemoryEvidenceEmitter(),
            mcp_transport_client=transport,
        )
    assert caught.value.code == "mcp_release_binding_mismatch"

    unsafe_tool = replace(admitted.config.tools[0], approval_required=False)
    unsafe_release = replace(
        admitted,
        config=replace(admitted.config, tools=(unsafe_tool,)),
    )
    monkeypatch.setattr(
        host_module,
        "activate_runtime_release",
        lambda *args, **kwargs: (
            unsafe_release,
            RuntimeActivationResult(created=True),
        ),
    )
    with pytest.raises(RuntimeHostError) as caught:
        build_reference_runtime_host(
            config,
            environment=environment,
            evidence_emitter=InMemoryEvidenceEmitter(),
            mcp_transport_client=transport,
        )
    assert caught.value.code == "mcp_side_effect_approval_contract_missing"

    path.write_text(json.dumps(_config_document()), encoding="utf-8")
    model_only_config = load_runtime_host_config(path)
    monkeypatch.setattr(
        host_module,
        "activate_runtime_release",
        lambda *args, **kwargs: (
            admitted,
            RuntimeActivationResult(created=True),
        ),
    )
    with pytest.raises(RuntimeHostError) as caught:
        build_reference_runtime_host(
            model_only_config,
            environment=environment,
            evidence_emitter=InMemoryEvidenceEmitter(),
            mcp_transport_client=transport,
        )
    assert caught.value.code == "mcp_broker_config_missing"


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN"),
    reason="PROMETA_RUNTIME_TEST_POSTGRES_DSN is not configured",
)
def test_reference_host_executes_mcp_with_explicit_tenant_human_adapter(
    tmp_path, monkeypatch
) -> None:
    import prometa.runtime.host as host_module

    dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    install_postgres_runtime_schema(dsn)
    document = _config_document()
    document["tenantId"] = "host-mcp-%s" % uuid.uuid4().hex
    document["runtimeId"] = "runtime-host-mcp"
    document["mcpBroker"] = _mcp_broker_document()
    path = tmp_path / "config.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    config = load_runtime_host_config(path)
    vector, admitted = _mcp_admitted()

    monkeypatch.setattr(
        host_module,
        "activate_runtime_release",
        lambda *args, **kwargs: (
            admitted,
            RuntimeActivationResult(created=True),
        ),
    )

    class TenantHumanEscalation:
        async def request_review(self, request):
            return HumanEscalationDecision(
                approved=True,
                reviewer_reference="tenant-review-1",
            )

    class TenantTransport:
        def __init__(self):
            self.calls = []

        async def call_tool(
            self, server, operation, arguments, credentials, metadata
        ):
            self.calls.append(
                (server.name, operation, arguments, credentials, metadata)
            )
            return {"recorded": True}

    class ToolCallingModel:
        def __init__(self):
            self.responses = [
                ModelInvocationResponse(
                    content=None,
                    tool_calls=(
                        ModelToolCall(
                            call_id="call-host-mcp",
                            name="orders.write",
                            arguments={"orderId": "order-host-mcp"},
                        ),
                    ),
                ),
                ModelInvocationResponse(content=vector["sampleOutput"]),
            ]

        async def invoke(self, request):
            return self.responses.pop(0)

    transport = TenantTransport()
    host, created = build_reference_runtime_host(
        config,
        environment={
            "PROMETA_RUNTIME_DATABASE_URL": dsn,
            "PROMETA_RUNTIME_API_TOKEN": API_TOKEN,
            "MODEL_GATEWAY_API_KEY": "model-key",
            "MCP_ORDERS_AUTHORIZATION": "Bearer tenant-mcp-key",
        },
        evidence_emitter=InMemoryEvidenceEmitter(),
        human_escalation=TenantHumanEscalation(),
        mcp_transport_client=transport,
    )
    try:
        assert created is True
        host.kernel.model_adapter = ToolCallingModel()
        response = _execute(
            host,
            {"requestId": "request-host-mcp", "input": vector["sampleInput"]},
        )
        assert response.status == 200
        assert response.body["output"] == vector["sampleOutput"]
        assert response.body["toolCalls"] == 1
        assert len(transport.calls) == 1
        server_name, operation, arguments, credentials, metadata = (
            transport.calls[0]
        )
        assert server_name == "Orders"
        assert operation == "orders.write"
        assert arguments == {"orderId": "order-host-mcp"}
        assert credentials.headers == {
            "Authorization": "Bearer tenant-mcp-key"
        }
        assert metadata["prometa.io/request-id"] == "request-host-mcp"
    finally:
        host.close()

    import psycopg

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, output_digest
                FROM prometa_runtime_mcp_idempotency
                WHERE tenant_id = %s AND runtime_id = %s
                """,
                (document["tenantId"], document["runtimeId"]),
            )
            idempotency = cursor.fetchone()
            cursor.execute(
                """
                SELECT event
                FROM prometa_runtime_mcp_audit
                WHERE tenant_id = %s AND runtime_id = %s
                ORDER BY occurred_at, event_id
                """,
                (document["tenantId"], document["runtimeId"]),
            )
            audit_events = [row[0] for row in cursor.fetchall()]
    assert idempotency is not None
    assert idempotency[0] == "completed"
    assert idempotency[1].startswith("sha256:")
    assert [event["outcome"] for event in audit_events] == [
        "accepted",
        "completed",
    ]
    encoded = json.dumps(audit_events, sort_keys=True)
    assert "order-host-mcp" not in encoded
    assert "tenant-mcp-key" not in encoded


def test_json_line_evidence_is_bounded_and_contains_no_request_payload() -> None:
    stream = io.StringIO()
    emitter = JsonLineEvidenceEmitter(stream)
    emitter.emit(
        RuntimeEvidenceEvent(
            name="runtime.request",
            outcome="completed",
            occurred_at="2026-07-12T00:00:00Z",
            attributes={"prometa.runtime.request_id": "request-1"},
        )
    )
    output = stream.getvalue()
    assert json.loads(output)["name"] == "runtime.request"
    assert _vector()["sampleInput"]["question"] not in output

    with pytest.raises(RuntimeHostError) as caught:
        emitter.emit(
            RuntimeEvidenceEvent(
                name="runtime.request",
                outcome="failed",
                occurred_at="2026-07-12T00:00:00Z",
                attributes={"large": "x" * 70_000},
            )
        )
    assert caught.value.code == "evidence_event_too_large"


def test_reference_host_rejects_short_api_tokens() -> None:
    _, admitted = _admitted()
    kernel = RuntimeKernel(
        admitted,
        model_adapter=RecordingModelAdapter(_vector()["sampleOutput"]),
        evidence_emitter=InMemoryEvidenceEmitter(),
        runtime_id="reference-host-test",
        runtime_version="0.18.0",
    )
    with pytest.raises(RuntimeHostError) as caught:
        ReferenceRuntimeHost(kernel, api_token="too-short")
    assert caught.value.code == "api_token_too_short"


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN"),
    reason="PROMETA_RUNTIME_TEST_POSTGRES_DSN is not configured",
)
def test_reference_host_bootstrap_joins_activation_and_executes_with_postgres() -> None:
    dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    install_postgres_runtime_schema(dsn)
    vector = _vector()

    class GatewayHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return None

        def do_POST(self):
            length = int(self.headers["content-length"])
            request = json.loads(self.rfile.read(length))
            assert request["model"] == "golden-model"
            response = json.dumps(
                {
                    "model": "golden-model",
                    "choices": [
                        {
                            "message": {"content": vector["sampleOutput"]},
                            "finish_reason": "stop",
                        }
                    ],
                }
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

    gateway = ThreadingHTTPServer(("127.0.0.1", 0), GatewayHandler)
    gateway_thread = threading.Thread(target=gateway.serve_forever, daemon=True)
    gateway_thread.start()
    received_receipts = []
    receipt_entered = threading.Event()
    release_receipts = threading.Event()

    class ReceiptHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return None

        def do_POST(self):
            assert self.path == "/api/runtime-receipts"
            assert self.headers["x-api-key"] == "runtime-receipt-key"
            length = int(self.headers["content-length"])
            receipt = json.loads(self.rfile.read(length))
            received_receipts.append(receipt)
            receipt_entered.set()
            assert release_receipts.wait(timeout=3)
            response = json.dumps(
                {"receiptId": receipt["receiptId"], "status": "recorded"}
            ).encode("utf-8")
            self.send_response(201)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

    receipt_server = ThreadingHTTPServer(("127.0.0.1", 0), ReceiptHandler)
    receipt_thread = threading.Thread(
        target=receipt_server.serve_forever, daemon=True
    )
    receipt_thread.start()
    verification = vector["verification"]
    config = RuntimeHostConfig(
        tenant_id="host-test-%s" % uuid.uuid4().hex,
        runtime_id="runtime-host-shared",
        runtime_version="0.18.0",
        org_id=verification["expectedOrgId"],
        environment=verification["expectedEnvironment"],
        release_id=verification["expectedReleaseId"],
        deployment_id=verification["expectedDeploymentId"],
        runtime_target=verification["expectedRuntime"],
        bundle=vector["bundle"],
        promotion_attestation=vector["attestation"],
        bundle_trust_store=_trust(vector["bundleTrust"]),
        promotion_trust_store=_trust(vector["promotionTrust"]),
        model_gateway_base_url="http://127.0.0.1:%d" % gateway.server_address[1],
        model_gateway_api_key_env=None,
        model_gateway_endpoint_path="/v1/chat/completions",
        model_gateway_timeout_seconds=2,
        model_gateway_max_response_bytes=1024 * 1024,
        database_dsn_env="RUNTIME_DSN",
        api_token_env="RUNTIME_TOKEN",
        request_timeout_seconds=3,
        max_request_bytes=1024,
        receipt_base_url="http://127.0.0.1:%d" % receipt_server.server_address[1],
        receipt_api_key_env="RUNTIME_RECEIPT_KEY",
        receipt_timeout_seconds=2,
        receipt_poll_interval_seconds=0.01,
        receipt_lease_seconds=5,
        receipt_initial_backoff_seconds=0.01,
        receipt_max_backoff_seconds=0.1,
        task_recovery_enabled=True,
        task_recovery_lease_seconds=10,
        task_recovery_max_attempts=3,
        task_recovery_history_limit=20,
    )
    environment = {
        "RUNTIME_DSN": dsn,
        "RUNTIME_TOKEN": API_TOKEN,
        "RUNTIME_RECEIPT_KEY": "runtime-receipt-key",
    }
    first = second = None
    first_evidence = InMemoryEvidenceEmitter()
    try:
        with pytest.raises(RuntimeHostError) as caught:
            build_reference_runtime_host(
                config,
                environment={
                    "RUNTIME_DSN": dsn,
                    "RUNTIME_TOKEN": API_TOKEN,
                },
                evidence_emitter=InMemoryEvidenceEmitter(),
                now=_instant(verification["now"]),
            )
        assert caught.value.code == "receipt_api_key_missing"

        first, created = build_reference_runtime_host(
            config,
            environment=environment,
            evidence_emitter=first_evidence,
            now=_instant(verification["now"]),
        )
        assert created is True
        assert receipt_entered.wait(timeout=2)
        first_response = _execute(
            first,
            {"requestId": "request-first", "input": vector["sampleInput"]},
        )
        assert first_response.status == 200
        first_status = first.handle(
            "GET",
            "/v1/runtime/tasks/request-first",
            {"authorization": "Bearer %s" % API_TOKEN},
        )
        assert first_status.status == 200
        assert first_status.body["status"] == "completed"
        assert first_status.body["historyTruncated"] is False
        release_receipts.set()
        deadline = time.monotonic() + 3
        while len(received_receipts) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert [item["transition"] for item in received_receipts] == [
            "admitted",
            "active",
        ]
        assert len({item["receiptId"] for item in received_receipts}) == 2

        second, joined_created = build_reference_runtime_host(
            config,
            environment=environment,
            evidence_emitter=InMemoryEvidenceEmitter(),
            now=_instant(verification["now"]),
        )
        assert joined_created is False
        second_response = _execute(
            second,
            {"requestId": "request-second", "input": vector["sampleInput"]},
        )
        assert second_response.status == 200
        duplicate = _execute(
            second,
            {"requestId": "request-first", "input": vector["sampleInput"]},
        )
        assert duplicate.status == 409
        assert duplicate.body == {
            "error": {"code": "task_already_completed"}
        }
        assert first_response.body["output"] == vector["sampleOutput"]
        time.sleep(0.1)
        assert len(received_receipts) == 2
        delivery_events = [
            event
            for event in first_evidence.events
            if event.name == "runtime.receipt.delivery"
        ]
        assert {event.outcome for event in delivery_events} == {"delivered"}
        assert "runtime-receipt-key" not in repr(delivery_events)
    finally:
        release_receipts.set()
        if first is not None:
            first.close()
        if second is not None:
            second.close()
        receipt_server.shutdown()
        receipt_server.server_close()
        receipt_thread.join(timeout=2)
        gateway.shutdown()
        gateway.server_close()
        gateway_thread.join(timeout=2)


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN"),
    reason="PROMETA_RUNTIME_TEST_POSTGRES_DSN is not configured",
)
def test_reference_host_pulls_and_uses_bounded_cache_when_platform_is_down() -> None:
    dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    install_postgres_runtime_schema(dsn)
    vector = _vector()
    verification = vector["verification"]
    promotion_claims = json.loads(vector["attestation"]["signedPayload"])

    class GatewayHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return None

        def do_POST(self):
            length = int(self.headers["content-length"])
            self.rfile.read(length)
            response = json.dumps(
                {
                    "model": "golden-model",
                    "choices": [
                        {
                            "message": {"content": vector["sampleOutput"]},
                            "finish_reason": "stop",
                        }
                    ],
                }
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

    gateway = ThreadingHTTPServer(("127.0.0.1", 0), GatewayHandler)
    gateway_thread = threading.Thread(target=gateway.serve_forever, daemon=True)
    gateway_thread.start()
    pulls = []
    control_status = {"value": 200}

    class ControlPlaneHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return None

        def do_GET(self):
            pulls.append((self.path, self.headers.get("x-api-key")))
            if control_status["value"] != 200:
                response = json.dumps(
                    {
                        "error": "Promotion attestation has been revoked",
                        "code": "release_revoked",
                    }
                ).encode("utf-8")
                self.send_response(control_status["value"])
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)
                return
            handoff = {
                "handoffVersion": 1,
                "attestationId": vector["attestation"]["attestationId"],
                "artifactId": promotion_claims["artifactId"],
                "artifactDigest": vector["bundle"]["artifactDigest"],
                "releaseId": verification["expectedReleaseId"],
                "deploymentId": verification["expectedDeploymentId"],
                "targetEnvironment": verification["expectedEnvironment"],
                "runtimeTarget": verification["expectedRuntime"],
                "checkedAt": verification["now"],
                "bundle": vector["bundle"],
                "promotionAttestation": vector["attestation"],
            }
            response = json.dumps(handoff).encode("utf-8")
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

    control = ThreadingHTTPServer(("127.0.0.1", 0), ControlPlaneHandler)
    control_thread = threading.Thread(target=control.serve_forever, daemon=True)
    control_thread.start()
    control_closed = False
    config = RuntimeHostConfig(
        tenant_id="host-pull-%s" % uuid.uuid4().hex,
        runtime_id="runtime-host-pull",
        runtime_version="0.18.0",
        org_id=verification["expectedOrgId"],
        environment=verification["expectedEnvironment"],
        release_id=verification["expectedReleaseId"],
        deployment_id=verification["expectedDeploymentId"],
        runtime_target=verification["expectedRuntime"],
        bundle=None,
        promotion_attestation=None,
        bundle_trust_store=_trust(vector["bundleTrust"]),
        promotion_trust_store=_trust(vector["promotionTrust"]),
        model_gateway_base_url="http://127.0.0.1:%d" % gateway.server_address[1],
        model_gateway_api_key_env=None,
        model_gateway_endpoint_path="/v1/chat/completions",
        model_gateway_timeout_seconds=2,
        model_gateway_max_response_bytes=1024 * 1024,
        database_dsn_env="RUNTIME_DSN",
        api_token_env="RUNTIME_TOKEN",
        request_timeout_seconds=3,
        max_request_bytes=1024,
        control_plane_base_url="http://127.0.0.1:%d"
        % control.server_address[1],
        control_plane_attestation_id=vector["attestation"]["attestationId"],
        control_plane_api_key_env="CONTROL_PLANE_KEY",
        control_plane_allow_insecure_http=True,
        control_plane_timeout_seconds=0.2,
        control_plane_max_response_bytes=1024 * 1024,
        control_plane_max_clock_skew_seconds=60,
        control_plane_max_cache_age_seconds=60,
    )
    environment = {
        "RUNTIME_DSN": dsn,
        "RUNTIME_TOKEN": API_TOKEN,
        "CONTROL_PLANE_KEY": "runtime-read-key-0123456789abcdef",
    }
    first = second = None
    try:
        with pytest.raises(RuntimeHostError) as caught:
            build_reference_runtime_host(
                config,
                environment={
                    "RUNTIME_DSN": dsn,
                    "RUNTIME_TOKEN": API_TOKEN,
                },
                now=_instant(verification["now"]),
            )
        assert caught.value.code == "control_plane_api_key_missing"

        first_evidence = InMemoryEvidenceEmitter()
        first, created = build_reference_runtime_host(
            config,
            environment=environment,
            evidence_emitter=first_evidence,
            now=_instant(verification["now"]),
        )
        assert created is True
        assert first.release_source == "control_plane"
        assert pulls == [
            (
                "/api/runtime-releases/%s"
                % vector["attestation"]["attestationId"],
                "runtime-read-key-0123456789abcdef",
            )
        ]
        assert _execute(
            first,
            {"requestId": "request-pulled", "input": vector["sampleInput"]},
        ).status == 200
        assert any(
            event.attributes["prometa.release.source"] == "control_plane"
            for event in first_evidence.events
            if event.name == "runtime.release.material"
        )
        first.close()
        first = None

        control_status["value"] = 409
        with pytest.raises(RuntimeHostError) as caught:
            build_reference_runtime_host(
                config,
                environment=environment,
                evidence_emitter=InMemoryEvidenceEmitter(),
                now=_instant(verification["now"]),
            )
        assert caught.value.code == "control_plane_pull_rejected"

        control.shutdown()
        control.server_close()
        control_thread.join(timeout=2)
        control_closed = True

        second_evidence = InMemoryEvidenceEmitter()
        second, joined = build_reference_runtime_host(
            config,
            environment=environment,
            evidence_emitter=second_evidence,
            now=_instant(verification["now"]),
        )
        assert joined is False
        assert second.release_source == "cache"
        assert _execute(
            second,
            {"requestId": "request-cached", "input": vector["sampleInput"]},
        ).status == 200
        assert any(
            event.attributes["prometa.release.source"] == "cache"
            for event in second_evidence.events
            if event.name == "runtime.release.material"
        )
    finally:
        if first is not None:
            first.close()
        if second is not None:
            second.close()
        if not control_closed:
            control.shutdown()
            control.server_close()
            control_thread.join(timeout=2)
        gateway.shutdown()
        gateway.server_close()
        gateway_thread.join(timeout=2)
