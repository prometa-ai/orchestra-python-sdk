"""Tenant-initiated runtime release pull contract tests."""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from prometa.runtime.control_plane import (
    RuntimeControlPlaneClient,
    RuntimeControlPlaneError,
)


API_KEY = "runtime-read-key-0123456789abcdef"
DIGEST = "sha256:" + "a" * 64
FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "runtime-release-handoff-v1.json"
)


def _fixture():
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _handoff(**overrides):
    document = {
        "handoffVersion": 1,
        "attestationId": "attestation-1",
        "artifactId": "artifact-1",
        "artifactDigest": DIGEST,
        "releaseId": "release-1",
        "deploymentId": "deployment-1",
        "targetEnvironment": "prod",
        "runtimeTarget": "tenant-runtime",
        "checkedAt": datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        "bundle": {"signed": True, "artifactDigest": DIGEST},
        "promotionAttestation": {
            "signed": True,
            "attestationId": "attestation-1",
        },
    }
    document.update(overrides)
    return document


class _Server:
    def __init__(self, status=200, document=None, headers=None):
        state = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                state.path = self.path
                state.api_key = self.headers.get("x-api-key")
                body = (
                    document
                    if isinstance(document, bytes)
                    else json.dumps(document or _handoff()).encode("utf-8")
                )
                self.send_response(status)
                for name, value in (headers or {}).items():
                    self.send_header(name, value)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                return None

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.path = None
        self.api_key = None

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    @property
    def base_url(self):
        return "http://127.0.0.1:%d" % self.server.server_port


def _client(server, **kwargs):
    return RuntimeControlPlaneClient(
        server.base_url,
        API_KEY,
        allow_insecure_http=True,
        **kwargs,
    )


def test_fetch_release_is_bounded_bound_and_no_redirect() -> None:
    fixture = _fixture()
    with _Server(document=fixture) as server:
        result = _client(server).fetch_release(
            "attestation-1",
            expected_release_id="release-1",
            expected_deployment_id="deployment-1",
            expected_environment="prod",
            expected_runtime="tenant-runtime",
            now=datetime.fromisoformat(
                fixture["checkedAt"].replace("Z", "+00:00")
            ),
        )
    assert server.path == "/api/runtime-releases/attestation-1"
    assert server.api_key == API_KEY
    assert result.artifact_digest == DIGEST
    assert result.bundle["signed"] is True
    assert result.checked_at.isoformat() == "2026-07-12T08:00:00+00:00"

    with _Server(
        status=302,
        headers={"location": "http://127.0.0.1:1/credential-sink"},
    ) as redirect:
        with pytest.raises(RuntimeControlPlaneError) as caught:
            _client(redirect).fetch_release("attestation-1")
    assert caught.value.status == 302
    assert caught.value.retryable is False


def test_fetch_release_classifies_remote_failures() -> None:
    with _Server(
        status=409,
        document={"error": "revoked", "code": "release_revoked"},
    ) as rejected:
        with pytest.raises(RuntimeControlPlaneError) as caught:
            _client(rejected).fetch_release("attestation-1")
    assert caught.value.code == "release_revoked"
    assert caught.value.status == 409
    assert caught.value.retryable is False

    with _Server(status=503, document={"error": "unavailable"}) as unavailable:
        with pytest.raises(RuntimeControlPlaneError) as caught:
            _client(unavailable).fetch_release("attestation-1")
    assert caught.value.code == "control_plane_http_503"
    assert caught.value.retryable is True


@pytest.mark.parametrize(
    "document,code",
    [
        (b'{"handoffVersion":1,"handoffVersion":1}', "control_plane_response_invalid"),
        (_handoff(handoffVersion=2), "control_plane_version_unsupported"),
        (_handoff(releaseId="release-other"), "control_plane_binding_mismatch"),
        (
            _handoff(checkedAt="2026-01-01T00:00:00.000Z"),
            "control_plane_response_stale",
        ),
        (
            _handoff(bundle={"signed": True, "artifactDigest": "sha256:" + "b" * 64}),
            "control_plane_binding_mismatch",
        ),
    ],
)
def test_fetch_release_rejects_malformed_or_mismatched_responses(document, code) -> None:
    with _Server(document=document) as server:
        with pytest.raises(RuntimeControlPlaneError) as caught:
            _client(server).fetch_release(
                "attestation-1", expected_release_id="release-1"
            )
    assert caught.value.code == code
    assert caught.value.retryable is False


def test_client_requires_https_and_enforces_response_limit() -> None:
    with pytest.raises(ValueError, match="HTTPS"):
        RuntimeControlPlaneClient("http://orchestra.example", API_KEY)

    with _Server(document=b"{" + b" " * 2048 + b"}") as server:
        with pytest.raises(RuntimeControlPlaneError) as caught:
            _client(server, max_response_bytes=1024).fetch_release("attestation-1")
    assert caught.value.code == "control_plane_response_too_large"
