"""Runtime receipt construction and stdlib transport tests."""

from __future__ import annotations

import json
import urllib.error
from datetime import datetime, timezone

import pytest

from prometa.runtime import (
    RuntimeReceiptClient,
    RuntimeReceiptError,
    RuntimeReceiptSubmissionError,
    build_runtime_receipt,
)


DIGEST = "sha256:" + "a" * 64


def _receipt(**overrides):
    values = {
        "attestation_id": "attestation-1",
        "artifact_digest": DIGEST,
        "release_id": "release-1",
        "deployment_id": "deployment-1",
        "target_environment": "prod",
        "runtime_target": "tenant-runtime",
        "runtime_id": "tenant-runtime-01",
        "runtime_version": "1.2.3",
        "transition": "admitted",
        "outcome": "accepted",
        "receipt_id": "receipt-1",
        "event_at": datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return build_runtime_receipt(**values)


def test_builds_the_platform_contract_with_stable_retry_identity() -> None:
    receipt = _receipt()
    assert receipt == {
        "receiptId": "receipt-1",
        "attestationId": "attestation-1",
        "artifactDigest": DIGEST,
        "releaseId": "release-1",
        "deploymentId": "deployment-1",
        "targetEnvironment": "prod",
        "runtimeTarget": "tenant-runtime",
        "runtimeId": "tenant-runtime-01",
        "runtimeVersion": "1.2.3",
        "transition": "admitted",
        "outcome": "accepted",
        "reason": None,
        "eventAt": "2026-07-10T12:00:00.000Z",
    }


def test_accepts_semver_build_metadata_and_digest_style_runtime_ids() -> None:
    receipt = _receipt(
        runtime_target="registry.example/runtime@sha256:abc",
        runtime_version="1.2.3+build.7",
    )
    assert receipt["runtimeVersion"] == "1.2.3+build.7"


def test_rejects_unsafe_local_receipts_before_network_io() -> None:
    with pytest.raises(RuntimeReceiptError, match="outcome"):
        _receipt(transition="active", outcome="failed")
    with pytest.raises(RuntimeReceiptError, match="timezone-aware"):
        _receipt(event_at=datetime(2026, 7, 10, 12, 0))
    with pytest.raises(RuntimeReceiptError, match="sha256"):
        _receipt(artifact_digest="not-a-digest")


class _Response:
    def __init__(self, value):
        self._value = value

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self):
        return json.dumps(self._value).encode("utf-8")

    def close(self):
        return None


def test_client_submits_with_runtime_scope_key_and_no_extra_dependency(monkeypatch) -> None:
    captured = {}

    def fake_urlopen(request, timeout):
        captured["request"] = request
        captured["timeout"] = timeout
        return _Response({"receiptId": "receipt-1", "status": "recorded"})

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    result = RuntimeReceiptClient(
        "https://orchestra.example.test/", "pk_runtime", timeout=3.0
    ).submit(_receipt())
    assert result["status"] == "recorded"
    assert captured["timeout"] == 3.0
    assert captured["request"].full_url.endswith("/api/runtime-receipts")
    assert captured["request"].get_header("X-api-key") == "pk_runtime"
    sent = json.loads(captured["request"].data.decode("utf-8"))
    assert sent["receiptId"] == "receipt-1"


def test_client_surfaces_http_status_without_exposing_the_key(monkeypatch) -> None:
    def denied(_request, timeout):
        assert timeout == 10.0
        raise urllib.error.HTTPError(
            "https://orchestra.example.test/api/runtime-receipts",
            403,
            "Forbidden",
            {},
            _Response({"error": "Forbidden"}),
        )

    monkeypatch.setattr("urllib.request.urlopen", denied)
    client = RuntimeReceiptClient("https://orchestra.example.test", "secret-key")
    with pytest.raises(RuntimeReceiptSubmissionError) as caught:
        client.submit(_receipt())
    assert caught.value.status == 403
    assert "secret-key" not in str(caught.value)
