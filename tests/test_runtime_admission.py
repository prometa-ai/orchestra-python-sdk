"""Phase 2A combined runtime admission and contract-negotiation tests."""

from __future__ import annotations

import base64
import copy
import json
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("cryptography")
pytest.importorskip("jsonschema")

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from prometa.runtime import (
    BASE_RUNTIME_CAPABILITIES,
    CAPABILITY_GUARD_EVALUATE,
    CAPABILITY_HUMAN_ESCALATION,
    CAPABILITY_SCHEMA_VALIDATE,
    CAPABILITY_TOOL_BROKER,
    BundleTrustEntry,
    BundleTrustStore,
    BundleVerificationError,
    InMemoryAdmissionReplayStore,
    InMemoryRuntimeActivationStore,
    RuntimeAdmissionPolicy,
    VerifiedBundle,
    activate_runtime_release,
    admit_runtime_release,
    parse_runtime_bundle,
    verify_bundle_envelope,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "runtime-kernel-v1.json"
LEGACY_BUNDLE_PATH = Path(__file__).parent / "fixtures" / "bundle-envelope-v1.json"


@pytest.fixture()
def vector():
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


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


def _policy(vector, **overrides) -> RuntimeAdmissionPolicy:
    verification = vector["verification"]
    values = {
        "expected_org_id": verification["expectedOrgId"],
        "expected_environment": verification["expectedEnvironment"],
        "expected_release_id": verification["expectedReleaseId"],
        "expected_deployment_id": verification["expectedDeploymentId"],
        "expected_runtime": verification["expectedRuntime"],
        "supported_capabilities": frozenset(
            {*BASE_RUNTIME_CAPABILITIES, CAPABILITY_SCHEMA_VALIDATE}
        ),
    }
    values.update(overrides)
    return RuntimeAdmissionPolicy(**values)


def _admit(vector, **overrides):
    values = {
        "bundle": vector["bundle"],
        "promotion_attestation": vector["attestation"],
        "bundle_trust_store": _trust(vector["bundleTrust"]),
        "promotion_trust_store": _trust(vector["promotionTrust"]),
        "replay_store": InMemoryAdmissionReplayStore(),
        "policy": _policy(vector),
        "now": _instant(vector["verification"]["now"]),
    }
    values.update(overrides)
    return admit_runtime_release(**values)


def _assert_code(code, callback) -> None:
    with pytest.raises(BundleVerificationError) as caught:
        callback()
    assert caught.value.code == code


def _verified_bundle(vector) -> VerifiedBundle:
    verification = vector["verification"]
    return verify_bundle_envelope(
        vector["bundle"],
        _trust(vector["bundleTrust"]),
        expected_org_id=verification["expectedOrgId"],
        expected_audience="prometa-runtime",
        expected_environment=verification["expectedEnvironment"],
        now=_instant(verification["now"]),
    )


def _mutated_verified(vector, mutate) -> VerifiedBundle:
    verified = _verified_bundle(vector)
    content = copy.deepcopy(dict(verified.content))
    mutate(content)
    return VerifiedBundle(
        claims=verified.claims,
        content=content,
        signed_payload=verified.signed_payload,
        trust_entry=verified.trust_entry,
    )


def _public_key_base64(private_key) -> str:
    der = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return base64.b64encode(der).decode("ascii")


def test_signed_and_promoted_runtime_contract_admits_atomically(vector) -> None:
    replay = InMemoryAdmissionReplayStore()
    admitted = _admit(vector, replay_store=replay)

    assert admitted.config.manifest.agent_id == "agent-golden-1"
    assert admitted.config.primary_model.model_name == "golden-model"
    assert admitted.config.contract.contract_version == 1
    assert admitted.config.contract.input_schema["required"] == ["question"]
    assert admitted.config.contract.output_schema["required"] == ["answer"]
    assert admitted.artifact_digest == vector["bundle"]["artifactDigest"]
    _assert_code(
        "replayed_runtime_release",
        lambda: _admit(vector, replay_store=replay),
    )


def test_runtime_activation_allows_exact_replicas_and_rejects_identity_drift(
    vector,
) -> None:
    store = InMemoryRuntimeActivationStore()
    values = {
        "bundle": vector["bundle"],
        "promotion_attestation": vector["attestation"],
        "bundle_trust_store": _trust(vector["bundleTrust"]),
        "promotion_trust_store": _trust(vector["promotionTrust"]),
        "activation_store": store,
        "runtime_id": "runtime-host-1",
        "policy": _policy(vector),
        "now": _instant(vector["verification"]["now"]),
    }
    admitted, first = activate_runtime_release(**values)
    joined_admission, joined = activate_runtime_release(**values)

    assert first.created is True
    assert joined.created is False
    assert joined_admission.artifact_digest == admitted.artifact_digest

    redeployed = store.activate_or_join(
        runtime_id="runtime-host-2",
        deployment_id="deployment-redeploy",
        release_id=vector["verification"]["expectedReleaseId"],
        artifact_digest=admitted.artifact_digest,
        bundle_jti="runtime-kernel-bundle-v1",
        promotion_jti="runtime-kernel-promotion-redeploy",
    )
    assert redeployed.created is True

    changed_policy = _policy(vector, expected_release_id="release-other")
    _assert_code(
        "wrong_release",
        lambda: activate_runtime_release(**{**values, "policy": changed_policy}),
    )
    with pytest.raises(ValueError, match="runtime_id"):
        activate_runtime_release(**{**values, "runtime_id": " "})

    with pytest.raises(BundleVerificationError) as caught:
        store.activate_or_join(
            runtime_id="runtime-host-3",
            deployment_id="deployment-digest-conflict",
            release_id=vector["verification"]["expectedReleaseId"],
            artifact_digest="sha256:" + "a" * 64,
            bundle_jti="runtime-kernel-bundle-v1",
            promotion_jti="runtime-kernel-promotion-digest-conflict",
        )
    assert caught.value.code == "runtime_activation_conflict"


def test_failed_second_signature_does_not_partially_reserve_bundle(vector) -> None:
    replay = InMemoryAdmissionReplayStore()
    tampered = copy.deepcopy(vector["attestation"])
    tampered["signature"] = "AAAA"
    _assert_code(
        "invalid_signature",
        lambda: _admit(
            vector,
            replay_store=replay,
            promotion_attestation=tampered,
        ),
    )
    assert _admit(vector, replay_store=replay).bundle.jti == "runtime-kernel-bundle-v1"


def test_missing_or_unsupported_capabilities_fail_before_replay(vector) -> None:
    replay = InMemoryAdmissionReplayStore()
    _assert_code(
        "unsupported_runtime_capability",
        lambda: _admit(
            vector,
            replay_store=replay,
            policy=_policy(
                vector,
                supported_capabilities=BASE_RUNTIME_CAPABILITIES,
            ),
        ),
    )
    assert _admit(vector, replay_store=replay).config.contract.contract_version == 1


def test_runtime_contract_downgrade_and_remote_refs_fail_closed(vector) -> None:
    downgraded = _mutated_verified(
        vector,
        lambda content: content["runtimeContract"]["requiredCapabilities"].remove(
            CAPABILITY_SCHEMA_VALIDATE
        ),
    )
    _assert_code(
        "runtime_capability_downgrade",
        lambda: parse_runtime_bundle(
            downgraded,
            supported_capabilities={
                *BASE_RUNTIME_CAPABILITIES,
                CAPABILITY_SCHEMA_VALIDATE,
            },
        ),
    )

    def add_remote_ref(content):
        content["runtimeContract"]["inputSchema"] = {
            "$ref": "https://attacker.example/schema.json"
        }

    remote = _mutated_verified(vector, add_remote_ref)
    _assert_code(
        "remote_schema_ref_denied",
        lambda: parse_runtime_bundle(
            remote,
            supported_capabilities={
                *BASE_RUNTIME_CAPABILITIES,
                CAPABILITY_SCHEMA_VALIDATE,
            },
        ),
    )

    add_remote_ref_content = _mutated_verified(
        vector,
        lambda content: content["runtimeContract"].update(
            {"inputSchema": {"$ref": "schema.json"}}
        ),
    )
    _assert_code(
        "remote_schema_ref_denied",
        lambda: parse_runtime_bundle(
            add_remote_ref_content,
            supported_capabilities={
                *BASE_RUNTIME_CAPABILITIES,
                CAPABILITY_SCHEMA_VALIDATE,
            },
        ),
    )


def test_tool_and_human_guard_requirements_cannot_be_downgraded(vector) -> None:
    def add_governed_tool(content):
        content["tools"] = [
            {
                "name": "Lookup",
                "source": "mcp",
                "operation": "orders.lookup",
                "inputSchema": {"type": "object"},
                "mcpServer": "Orders",
                "sideEffects": "read-only",
                "riskLevel": "low",
                "authBinding": "service-account",
                "scopes": ["orders.read"],
                "requiredGuardrails": ["tenant_risk_gate"],
            }
        ]
        content["runtimeContract"]["requiredCapabilities"].append(
            CAPABILITY_TOOL_BROKER
        )

    governed_tool = _mutated_verified(vector, add_governed_tool)
    _assert_code(
        "runtime_capability_downgrade",
        lambda: parse_runtime_bundle(
            governed_tool,
            supported_capabilities={
                *BASE_RUNTIME_CAPABILITIES,
                CAPABILITY_SCHEMA_VALIDATE,
                CAPABILITY_TOOL_BROKER,
                CAPABILITY_GUARD_EVALUATE,
            },
        ),
    )

    def add_human_guard(content):
        content["guardrails"] = [
            {
                "name": "Human approval",
                "guardrailType": "human-approval",
                "onViolation": "block",
                "appliesTo": "input",
            }
        ]
        content["runtimeContract"]["requiredCapabilities"].append(
            CAPABILITY_GUARD_EVALUATE
        )

    human_guard = _mutated_verified(vector, add_human_guard)
    _assert_code(
        "runtime_capability_downgrade",
        lambda: parse_runtime_bundle(
            human_guard,
            supported_capabilities={
                *BASE_RUNTIME_CAPABILITIES,
                CAPABILITY_SCHEMA_VALIDATE,
                CAPABILITY_GUARD_EVALUATE,
                CAPABILITY_HUMAN_ESCALATION,
            },
        ),
    )


def test_primary_model_and_tool_schema_are_strict(vector) -> None:
    duplicate_primary = _mutated_verified(
        vector,
        lambda content: content["models"][1].update({"role": "primary"}),
    )
    _assert_code(
        "invalid_primary_model",
        lambda: parse_runtime_bundle(
            duplicate_primary,
            supported_capabilities={
                *BASE_RUNTIME_CAPABILITIES,
                CAPABILITY_SCHEMA_VALIDATE,
            },
        ),
    )

    duplicate_name = _mutated_verified(
        vector,
        lambda content: content["models"][1].update(
            {"name": content["models"][0]["name"]}
        ),
    )
    _assert_code(
        "ambiguous_runtime_model",
        lambda: parse_runtime_bundle(
            duplicate_name,
            supported_capabilities={
                *BASE_RUNTIME_CAPABILITIES,
                CAPABILITY_SCHEMA_VALIDATE,
            },
        ),
    )

    primary_mismatch = _mutated_verified(
        vector,
        lambda content: content["primaryModel"].update({"modelName": "other"}),
    )
    _assert_code(
        "primary_model_mismatch",
        lambda: parse_runtime_bundle(
            primary_mismatch,
            supported_capabilities={
                *BASE_RUNTIME_CAPABILITIES,
                CAPABILITY_SCHEMA_VALIDATE,
            },
        ),
    )

    def add_untyped_tool(content):
        content["tools"] = [
            {
                "name": "Lookup",
                "source": "native",
                "operation": "lookup",
                "sideEffects": "read-only",
                "riskLevel": "low",
                "authBinding": "none",
                "scopes": [],
            }
        ]

    untyped_tool = _mutated_verified(vector, add_untyped_tool)
    _assert_code(
        "tool_input_schema_missing",
        lambda: parse_runtime_bundle(
            untyped_tool,
            supported_capabilities={
                *BASE_RUNTIME_CAPABILITIES,
                CAPABILITY_SCHEMA_VALIDATE,
            },
        ),
    )


def test_phase_2a_kernel_rejects_unsupported_topology_semantics(vector) -> None:
    supervisor = _mutated_verified(
        vector,
        lambda content: content["topology"].update({"pattern": "supervisor-worker"}),
    )
    _assert_code(
        "unsupported_runtime_topology",
        lambda: parse_runtime_bundle(
            supervisor,
            supported_capabilities={
                *BASE_RUNTIME_CAPABILITIES,
                CAPABILITY_SCHEMA_VALIDATE,
            },
        ),
    )


def test_cross_artifact_manifest_identity_is_checked_after_both_signatures(
    vector,
) -> None:
    private_key = Ed25519PrivateKey.generate()
    attestation = copy.deepcopy(vector["attestation"])
    claims = json.loads(attestation["signedPayload"])
    claims["manifestId"] = "manifest-other"
    claims["issuer"] = "https://orchestra.example.test/rotated-promotion"
    claims["keyId"] = "rotated-promotion-key"
    payload = json.dumps(
        claims, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    attestation["issuer"] = claims["issuer"]
    attestation["keyId"] = claims["keyId"]
    attestation["signedPayload"] = payload
    attestation["signature"] = base64.b64encode(
        private_key.sign(payload.encode("utf-8"))
    ).decode("ascii")
    store = BundleTrustStore(
        [
            BundleTrustEntry(
                issuer=claims["issuer"],
                key_id=claims["keyId"],
                public_key_spki_der_base64=_public_key_base64(private_key),
            )
        ]
    )
    _assert_code(
        "promotion_manifest_mismatch",
        lambda: _admit(
            vector,
            promotion_attestation=attestation,
            promotion_trust_store=store,
        ),
    )


def test_legacy_bundle_remains_verifiable_but_not_executable_by_default(vector) -> None:
    legacy_doc = json.loads(LEGACY_BUNDLE_PATH.read_text(encoding="utf-8"))
    verification = legacy_doc["verification"]
    verified = verify_bundle_envelope(
        legacy_doc["bundle"],
        _trust(legacy_doc["trust"]),
        expected_org_id=verification["expectedOrgId"],
        expected_audience=verification["expectedAudience"],
        expected_environment=verification["expectedEnvironment"],
        now=_instant(verification["now"]),
    )
    _assert_code(
        "runtime_contract_missing",
        lambda: parse_runtime_bundle(
            verified,
            supported_capabilities={
                *BASE_RUNTIME_CAPABILITIES,
                CAPABILITY_GUARD_EVALUATE,
            },
        ),
    )
    parsed = parse_runtime_bundle(
        verified,
        supported_capabilities={
            *BASE_RUNTIME_CAPABILITIES,
            CAPABILITY_GUARD_EVALUATE,
        },
        require_runtime_contract=False,
    )
    assert parsed.contract.contract_version == 0
    assert CAPABILITY_GUARD_EVALUATE in parsed.contract.required_capabilities
