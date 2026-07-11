"""Fail-closed promotion-attestation v1 admission tests."""

from __future__ import annotations

import base64
import copy
import hashlib
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
APPROVAL_SCOPE_DIGEST = (
    "sha256:0ed1f9a9663483de64301e1124424a21354e62a91dc5aa2a0536eb3214a34c03"
)
FIXTURE_PATH = Path(__file__).parent / "fixtures" / "promotion-attestation-v1.json"
REVIEW_FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "promotion-attestation-review-v1.json"
)


def _public_key(private_key) -> str:
    der = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return base64.b64encode(der).decode("ascii")


def _approval_scope_digest(claims) -> str:
    scope = {
        "artifactId": claims["artifactId"],
        "artifactDigest": claims["artifactDigest"],
        "decisionId": claims["decisionId"],
        "targetEnvironment": claims["targetEnvironment"],
        "agentId": claims["agentId"],
        "requestedRuntime": claims["requestedRuntime"],
        "releaseId": claims["releaseId"],
        "deploymentId": claims["deploymentId"],
    }
    canonical = json.dumps(
        scope, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _approval(claims, **overrides):
    evidence = {
        "approvalId": "approval-1",
        "identity": "user:approver-1",
        "approvedAt": "2026-07-10T12:00:30.000Z",
        "expiresAt": "2026-07-10T12:12:00.000Z",
        "method": "prometa-session",
        "role": "Compliance Officer",
        "scopeDigest": _approval_scope_digest(claims),
    }
    evidence.update(overrides)
    return evidence


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
        "approvalRequirement": {"minimum": 0, "policy": "platform-default-v1"},
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


def test_typescript_review_vector_verifies_role_and_request_evidence() -> None:
    vector = json.loads(REVIEW_FIXTURE_PATH.read_text(encoding="utf-8"))
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
        minimum_approvals=expected["minimumApprovals"],
        required_approval_roles=expected["requiredApprovalRoles"],
        now=datetime.fromisoformat(expected["now"].replace("Z", "+00:00")),
    )
    assert verified.attestation_id == "attestation-review-golden-v1"
    assert verified.claims["approvalRequest"]["requestId"] == "review-golden-v1"


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


def test_scoped_human_approvals_and_minimums_are_enforced(signed_attestation) -> None:
    private_key, _, store = signed_attestation
    baseline_claims = json.loads(_signed(private_key)["signedPayload"])
    assert _approval_scope_digest(baseline_claims) == APPROVAL_SCOPE_DIGEST

    def with_approval(claims) -> None:
        claims["approvalRequirement"] = {
            "minimum": 1,
            "policy": "env:PROMETA_PROMOTION_REQUIRED_APPROVALS_PROD",
        }
        claims["approvals"] = [_approval(claims)]

    envelope = _signed(private_key, with_approval)
    verified = _verify(envelope, store, minimum_approvals=1)
    assert verified.claims["approvals"][0]["approvalId"] == "approval-1"
    _assert_code(
        "insufficient_approvals",
        lambda: _verify(envelope, store, minimum_approvals=2),
    )

    wrong_scope = _signed(
        private_key,
        lambda claims: (
            with_approval(claims),
            claims["approvals"][0].update({"scopeDigest": "sha256:" + "f" * 64}),
        ),
    )
    _assert_code("approval_scope_mismatch", lambda: _verify(wrong_scope, store))

    duplicate = _signed(
        private_key,
        lambda claims: (
            with_approval(claims),
            claims["approvals"].append(
                {**claims["approvals"][0], "approvalId": "approval-2"}
            ),
        ),
    )
    _assert_code("duplicate_approval_identity", lambda: _verify(duplicate, store))


def test_approval_policy_and_expiry_evidence_fail_closed(signed_attestation) -> None:
    private_key, _, store = signed_attestation

    invalid_requirement = _signed(
        private_key,
        lambda claims: claims.update(
            {"approvalRequirement": {"minimum": True, "policy": "test"}}
        ),
    )
    _assert_code("invalid_approvals", lambda: _verify(invalid_requirement, store))

    for field, value in (
        ("method", "api-key"),
        ("identity", "service:ci"),
        ("identity", "user:"),
    ):
        malformed_human = _signed(
            private_key,
            lambda claims, field=field, value=value: claims.update(
                {"approvals": [_approval(claims, **{field: value})]}
            ),
        )
        _assert_code(
            "invalid_approvals", lambda value=malformed_human: _verify(value, store)
        )

    def expired_approval(claims) -> None:
        claims["approvals"] = [
            _approval(claims, expiresAt="2026-07-10T12:10:00.000Z")
        ]

    invalid_expiry = _signed(private_key, expired_approval)
    _assert_code("invalid_approvals", lambda: _verify(invalid_expiry, store))

    beyond_decision = _signed(
        private_key,
        lambda claims: claims.update(
            {
                "approvals": [
                    _approval(claims, expiresAt="2026-07-10T12:16:00.000Z")
                ]
            }
        ),
    )
    _assert_code("invalid_approvals", lambda: _verify(beyond_decision, store))

    for invalid_minimum in (-1, True, 11):
        _assert_code(
            "invalid_minimum_approvals",
            lambda value=invalid_minimum: _verify(
                _signed(private_key), store, minimum_approvals=value
            ),
        )


def test_review_request_role_quorum_and_separation_are_enforced(
    signed_attestation,
) -> None:
    private_key, _, store = signed_attestation

    def with_review(claims) -> None:
        requirement = {
            "minimum": 2,
            "policy": "env:review-policy-v2",
            "roleRequirements": [
                {"role": "Compliance Officer", "minimum": 1},
                {"role": "Security", "minimum": 1},
            ],
            "separationOfDuties": True,
            "reviewRequestRequired": True,
        }
        claims["approvalRequirement"] = requirement
        claims["approvalRequest"] = {
            "requestId": "review-1",
            "requesterIdentity": "user:requester-1",
            "requestedAt": "2026-07-10T12:00:15.000Z",
            "expiresAt": "2026-07-10T12:12:00.000Z",
            "policyDigest": "sha256:"
            + hashlib.sha256(
                json.dumps(
                    requirement,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ).encode("utf-8")
            ).hexdigest(),
        }
        claims["approvals"] = [
            _approval(
                claims,
                approvalId="approval-compliance",
                requestId="review-1",
                identity="user:compliance-1",
                role="Compliance Officer",
            ),
            _approval(
                claims,
                approvalId="approval-security",
                requestId="review-1",
                identity="user:security-1",
                role="Security",
            ),
        ]

    envelope = _signed(private_key, with_review)
    verified = _verify(
        envelope,
        store,
        required_approval_roles={"Security": 1},
    )
    assert verified.claims["approvalRequest"]["requestId"] == "review-1"

    mirrored = copy.deepcopy(envelope)
    claims = json.loads(mirrored["signedPayload"])
    mirrored["authorization"].update(
        {
            "approvalRequirement": claims["approvalRequirement"],
            "approvalIds": [item["approvalId"] for item in claims["approvals"]],
            "approvalRequestId": "review-1",
        }
    )
    assert _verify(mirrored, store).attestation_id == "attestation-1"
    mirrored["authorization"]["approvalRequestId"] = "review-attacker"
    _assert_code(
        "transport_authorization_mismatch", lambda: _verify(mirrored, store)
    )

    def mutated_review(mutator):
        def apply(claims) -> None:
            with_review(claims)
            mutator(claims)

        return _signed(private_key, apply)

    _assert_code(
        "requester_approval_forbidden",
        lambda: _verify(
            mutated_review(
                lambda claims: claims["approvals"][0].update(
                    {"identity": "user:requester-1"}
                )
            ),
            store,
        ),
    )
    _assert_code(
        "approval_request_mismatch",
        lambda: _verify(
            mutated_review(
                lambda claims: claims["approvals"][0].update(
                    {"requestId": "review-2"}
                )
            ),
            store,
        ),
    )
    _assert_code(
        "invalid_approval_request",
        lambda: _verify(
            mutated_review(
                lambda claims: claims["approvalRequest"].update(
                    {"policyDigest": "sha256:" + "f" * 64}
                )
            ),
            store,
        ),
    )
    _assert_code(
        "approval_request_required",
        lambda: _verify(
            mutated_review(lambda claims: claims.pop("approvalRequest")), store
        ),
    )
    _assert_code(
        "insufficient_role_approvals",
        lambda: _verify(
            mutated_review(
                lambda claims: claims["approvals"][1].update(
                    {"role": "Compliance Officer"}
                )
            ),
            store,
        ),
    )
    _assert_code(
        "insufficient_role_approvals",
        lambda: _verify(
            envelope,
            store,
            required_approval_roles={"Security": 2},
        ),
    )
    _assert_code(
        "invalid_required_approval_roles",
        lambda: _verify(
            envelope,
            store,
            required_approval_roles={"Security": 0},
        ),
    )


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
