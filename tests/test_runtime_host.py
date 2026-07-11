"""Tenant reference-runtime host boundary and HTTP behavior tests."""

from __future__ import annotations

import asyncio
import io
import json
import os
import threading
import urllib.error
import urllib.request
import uuid
from datetime import datetime
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
    JsonLineEvidenceEmitter,
    ModelInvocationResponse,
    ReferenceRuntimeHost,
    RuntimeAdmissionPolicy,
    RuntimeEvidenceEvent,
    RuntimeHostError,
    RuntimeHostConfig,
    RuntimeKernel,
    admit_runtime_release,
    build_reference_runtime_host,
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


def _host(adapter, *, timeout=2.0):
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
    host = _host(RecordingModelAdapter(vector["sampleOutput"]))
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

    path.write_text('{"configVersion":1,"configVersion":1}', encoding="utf-8")
    with pytest.raises(RuntimeHostError) as caught:
        load_runtime_host_config(path)
    assert caught.value.code == "host_config_invalid_json"


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
    )
    environment = {"RUNTIME_DSN": dsn, "RUNTIME_TOKEN": API_TOKEN}
    first = second = None
    try:
        first, created = build_reference_runtime_host(
            config,
            environment=environment,
            evidence_emitter=InMemoryEvidenceEmitter(),
            now=_instant(verification["now"]),
        )
        second, joined_created = build_reference_runtime_host(
            config,
            environment=environment,
            evidence_emitter=InMemoryEvidenceEmitter(),
            now=_instant(verification["now"]),
        )
        assert created is True
        assert joined_created is False
        first_response = _execute(
            first,
            {"requestId": "request-first", "input": vector["sampleInput"]},
        )
        second_response = _execute(
            second,
            {"requestId": "request-second", "input": vector["sampleInput"]},
        )
        assert first_response.status == 200
        assert second_response.status == 200
        assert first_response.body["output"] == vector["sampleOutput"]
    finally:
        if first is not None:
            first.close()
        if second is not None:
            second.close()
        gateway.shutdown()
        gateway.server_close()
        gateway_thread.join(timeout=2)
