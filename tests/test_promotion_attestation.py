"""Fail-closed promotion-attestation v1 admission tests."""

from __future__ import annotations

import base64
import copy
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("cryptography")

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from prometa.runtime import (
    BundleTrustEntry,
    BundleTrustStore,
    BundleVerificationError,
    verify_promotion_attestation,
)


NOW = datetime(2026, 7, 10, 12, 5, tzinfo=timezone.utc)
DIGEST = "sha256:" + "a" * 64
FIXTURE_PATH = Path(__file__).parent / "fixtures" / "promotion-attestation-v1.json"


def _public_key(private_key) -> str:
    der = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return base64.b64encode(der).decode("ascii")


def _signed(private_key, mutate=None):
    claims = {
        "artifactType": "orchestra.promotion-attestation",
        "attestationVersion": 1,
        "issuer": "https://orchestra.example.test/promotion",
        "keyId": "promotion-key-v1",
        "subject": "promotion-attestation:attestation-1",
        "orgId": "org-golden",
        "audience": "prometa-runtime-admission",
        "targetEnvironment": "prod",
        "artifactId": "artifact-1",
        "artifactDigest": DIGEST,
        "manifestId": "manifest-1",
        "manifestVersion": 4,
        "agentId": "agent-1",
        "decisionId": "decision-1",
        "decisionAllow": True,
        "gateStage": "prod",
        "policySetDigest": "sha256:" + "b" * 64,
        "evidenceDigest": "sha256:" + "c" * 64,
        "decisionEvaluatedAt": "2026-07-10T12:00:00.000Z",
        "decisionValidUntil": "2026-07-10T12:15:00.000Z",
        "requestedRuntime": "tenant-runtime",
        "releaseId": "release-1",
        "deploymentId": "deployment-1",
        "approvals": [],
        "issuedAt": "2026-07-10T12:01:00.000Z",
        "notBefore": "2026-07-10T12:01:00.000Z",
        "expiresAt": "2026-07-10T12:11:00.000Z",
        "offlineLeaseExpiresAt": "2026-07-10T12:08:00.000Z",
        "jti": "promotion-jti-1",
        "revocationRef": "urn:prometa:promotion-attestation:attestation-1",
    }
    if mutate:
        mutate(claims)
    payload = json.dumps(
        claims, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    envelope = {
        "attestationId": "attestation-1",
        "attestationVersion": 1,
        "algorithm": "ed25519",
        "canonicalization": "signed-payload-json-v1",
        "issuer": claims["issuer"],
        "keyId": claims["keyId"],
        "signedPayload": payload,
        "signature": base64.b64encode(
            private_key.sign(payload.encode("utf-8"))
        ).decode("ascii"),
        "signed": True,
        "authorization": {
            "artifactId": claims["artifactId"],
            "artifactDigest": claims["artifactDigest"],
            "decisionId": claims["decisionId"],
            "releaseId": claims["releaseId"],
            "deploymentId": claims["deploymentId"],
            "targetEnvironment": claims["targetEnvironment"],
            "requestedRuntime": claims["requestedRuntime"],
            "expiresAt": claims["expiresAt"],
            "offlineLeaseExpiresAt": claims["offlineLeaseExpiresAt"],
        },
    }
    return envelope


@pytest.fixture()
def signed_attestation():
    private_key = Ed25519PrivateKey.generate()
    envelope = _signed(private_key)
    entry = BundleTrustEntry(
        issuer=envelope["issuer"],
        key_id=envelope["keyId"],
        public_key_spki_der_base64=_public_key(private_key),
        allowed_org_ids=frozenset({"org-golden"}),
        allowed_audiences=frozenset({"prometa-runtime-admission"}),
        allowed_environments=frozenset({"prod"}),
    )
    return private_key, envelope, BundleTrustStore([entry])


def _verify(envelope, store, **overrides):
    options = {
        "expected_org_id": "org-golden",
        "expected_audience": "prometa-runtime-admission",
        "expected_environment": "prod",
        "expected_artifact_digest": DIGEST,
        "expected_release_id": "release-1",
        "expected_deployment_id": "deployment-1",
        "expected_runtime": "tenant-runtime",
        "now": NOW,
    }
    options.update(overrides)
    return verify_promotion_attestation(envelope, store, **options)


def _assert_code(code, callback) -> None:
    with pytest.raises(BundleVerificationError) as caught:
        callback()
    assert caught.value.code == code


def test_valid_attestation_binds_the_exact_release(signed_attestation) -> None:
    _, envelope, store = signed_attestation
    verified = _verify(envelope, store)
    assert verified.attestation_id == "attestation-1"
    assert verified.artifact_digest == DIGEST
    assert verified.jti == "promotion-jti-1"


def test_typescript_golden_vector_verifies_exact_payload_bytes() -> None:
    vector = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    trust = vector["trust"]
    expected = vector["verification"]
    store = BundleTrustStore(
        [
            BundleTrustEntry(
                issuer=trust["issuer"],
                key_id=trust["keyId"],
                public_key_spki_der_base64=trust["publicKeySpkiDerBase64"],
                allowed_org_ids=frozenset({expected["expectedOrgId"]}),
                allowed_audiences=frozenset({expected["expectedAudience"]}),
                allowed_environments=frozenset(
                    {expected["expectedEnvironment"]}
                ),
            )
        ]
    )
    verified = verify_promotion_attestation(
        vector["attestation"],
        store,
        expected_org_id=expected["expectedOrgId"],
        expected_audience=expected["expectedAudience"],
        expected_environment=expected["expectedEnvironment"],
        expected_artifact_digest=expected["expectedArtifactDigest"],
        expected_release_id=expected["expectedReleaseId"],
        expected_deployment_id=expected["expectedDeploymentId"],
        expected_runtime=expected["expectedRuntime"],
        now=datetime.fromisoformat(expected["now"].replace("Z", "+00:00")),
    )
    assert verified.attestation_id == "attestation-golden-v1"
    assert verified.jti == "promotion-golden-v1"


@pytest.mark.parametrize(
    ("option", "value", "code"),
    [
        ("expected_org_id", "other", "wrong_org"),
        ("expected_audience", "other", "wrong_audience"),
        ("expected_environment", "staging", "wrong_environment"),
        ("expected_artifact_digest", "sha256:" + "d" * 64, "wrong_artifact_digest"),
        ("expected_release_id", "release-2", "wrong_release"),
        ("expected_deployment_id", "deployment-2", "wrong_deployment"),
        ("expected_runtime", "other-runtime", "wrong_runtime"),
    ],
)
def test_expected_release_bindings_are_mandatory(
    signed_attestation, option, value, code
) -> None:
    _, envelope, store = signed_attestation
    _assert_code(code, lambda: _verify(envelope, store, **{option: value}))


def test_signature_and_purpose_are_both_required(signed_attestation) -> None:
    private_key, envelope, store = signed_attestation
    tampered = copy.deepcopy(envelope)
    tampered["signedPayload"] = tampered["signedPayload"].replace(
        "release-1", "release-2"
    )
    _assert_code("invalid_signature", lambda: _verify(tampered, store))

    wrong_type = _signed(
        private_key, lambda claims: claims.update({"artifactType": "bundle"})
    )
    _assert_code("wrong_artifact_type", lambda: _verify(wrong_type, store))

    blocked = _signed(
        private_key, lambda claims: claims.update({"decisionAllow": False})
    )
    _assert_code("gate_not_allowed", lambda: _verify(blocked, store))

    wrong_revocation_ref = _signed(
        private_key,
        lambda claims: claims.update(
            {"revocationRef": "urn:prometa:promotion-attestation:other"}
        ),
    )
    _assert_code(
        "revocation_ref_mismatch",
        lambda: _verify(wrong_revocation_ref, store),
    )


def test_approval_timestamps_must_follow_the_bound_decision(
    signed_attestation,
) -> None:
    private_key, _, store = signed_attestation
    stale_approval = _signed(
        private_key,
        lambda claims: claims.update(
            {
                "approvals": [
                    {
                        "identity": "reviewer-1",
                        "method": "manual",
                        "approvedAt": "2026-07-10T11:59:00.000Z",
                    }
                ]
            }
        ),
    )
    _assert_code("invalid_approvals", lambda: _verify(stale_approval, store))


def test_revocation_replay_expiry_and_offline_lease(signed_attestation) -> None:
    _, envelope, store = signed_attestation
    _assert_code(
        "revoked_attestation",
        lambda: _verify(envelope, store, revoked_attestation_ids={"attestation-1"}),
    )
    _assert_code(
        "offline_lease_expired",
        lambda: _verify(
            envelope,
            store,
            now=datetime(2026, 7, 10, 12, 9, tzinfo=timezone.utc),
        ),
    )
    _assert_code(
        "expired_attestation",
        lambda: _verify(
            envelope,
            store,
            now=datetime(2026, 7, 10, 12, 11, tzinfo=timezone.utc),
        ),
    )
    seen = set()
    assert _verify(envelope, store, seen_jtis=seen).jti == "promotion-jti-1"
    _assert_code(
        "replayed_attestation", lambda: _verify(envelope, store, seen_jtis=seen)
    )


def test_unsigned_transport_mirror_cannot_disagree(signed_attestation) -> None:
    _, envelope, store = signed_attestation
    mismatched = copy.deepcopy(envelope)
    mismatched["authorization"]["deploymentId"] = "deployment-attacker"
    _assert_code(
        "transport_authorization_mismatch", lambda: _verify(mismatched, store)
    )
