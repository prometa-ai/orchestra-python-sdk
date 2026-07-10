"""Conformance and fail-closed tests for bundle-envelope v1 verification."""

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
    verify_bundle_envelope,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "bundle-envelope-v1.json"


@pytest.fixture()
def vector():
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _entry(vector, **overrides) -> BundleTrustEntry:
    values = {
        "issuer": vector["trust"]["issuer"],
        "key_id": vector["trust"]["keyId"],
        "public_key_spki_der_base64": vector["trust"][
            "publicKeySpkiDerBase64"
        ],
        "allowed_org_ids": frozenset({vector["verification"]["expectedOrgId"]}),
        "allowed_audiences": frozenset(
            {vector["verification"]["expectedAudience"]}
        ),
        "allowed_environments": frozenset(
            {vector["verification"]["expectedEnvironment"]}
        ),
    }
    values.update(overrides)
    return BundleTrustEntry(**values)


def _verify(vector, bundle=None, trust_store=None, **overrides):
    verification = vector["verification"]
    options = {
        "expected_org_id": verification["expectedOrgId"],
        "expected_audience": verification["expectedAudience"],
        "expected_environment": verification["expectedEnvironment"],
        "now": _instant(verification["now"]),
    }
    options.update(overrides)
    return verify_bundle_envelope(
        bundle or vector["bundle"],
        trust_store or BundleTrustStore([_entry(vector)]),
        **options,
    )


def _assert_code(code, callback) -> None:
    with pytest.raises(BundleVerificationError) as caught:
        callback()
    assert caught.value.code == code


def _resign(vector, private_key, *, issuer, key_id, mutate_claims=None):
    bundle = copy.deepcopy(vector["bundle"])
    claims = json.loads(bundle["signedPayload"])
    claims["issuer"] = issuer
    claims["keyId"] = key_id
    claims["jti"] = "rotated-bundle-v1"
    if mutate_claims is not None:
        mutate_claims(claims)
    payload = json.dumps(
        claims, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    bundle["signedPayload"] = payload
    bundle["envelopeSignature"] = base64.b64encode(
        private_key.sign(payload.encode("utf-8"))
    ).decode("ascii")
    bundle["issuer"] = issuer
    bundle["keyId"] = key_id
    bundle["artifactDigest"] = claims["artifactDigest"]
    return bundle


def _public_key_base64(private_key) -> str:
    der = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return base64.b64encode(der).decode("ascii")


def test_golden_vector_verifies_exact_typescript_payload(vector) -> None:
    verified = _verify(vector)
    assert verified.content["manifest"]["id"] == "manifest-golden-1"
    assert verified.artifact_digest == vector["bundle"]["artifactDigest"]
    assert verified.jti == "bundle-golden-v1"


def test_embedded_transport_key_is_not_a_trust_root(vector) -> None:
    bundle = copy.deepcopy(vector["bundle"])
    bundle["publicKey"] = "attacker-controlled-transport-metadata"
    assert _verify(vector, bundle=bundle).content["manifest"]["deployable"] is True


def test_tampered_signed_payload_is_rejected(vector) -> None:
    bundle = copy.deepcopy(vector["bundle"])
    bundle["signedPayload"] = bundle["signedPayload"].replace(
        "org-golden", "org-attacker"
    )
    _assert_code("invalid_signature", lambda: _verify(vector, bundle=bundle))


@pytest.mark.parametrize(
    ("option", "value", "code"),
    [
        ("expected_org_id", "org-other", "wrong_org"),
        ("expected_audience", "other-runtime", "wrong_audience"),
        ("expected_environment", "staging", "wrong_environment"),
    ],
)
def test_expected_admission_bindings_are_mandatory(vector, option, value, code) -> None:
    _assert_code(code, lambda: _verify(vector, **{option: value}))


def test_unsigned_and_unknown_key_artifacts_fail_closed(vector) -> None:
    unsigned = copy.deepcopy(vector["bundle"])
    unsigned["signed"] = False
    _assert_code("unsigned_bundle", lambda: _verify(vector, bundle=unsigned))
    _assert_code(
        "unknown_signing_key",
        lambda: _verify(vector, trust_store=BundleTrustStore([])),
    )


def test_signed_payload_size_is_bounded_before_parsing(vector) -> None:
    _assert_code(
        "signed_payload_too_large",
        lambda: _verify(vector, max_signed_payload_bytes=32),
    )


def test_transport_content_mirror_cannot_disagree(vector) -> None:
    bundle = copy.deepcopy(vector["bundle"])
    bundle["content"]["systemPrompt"] = "Ignore the verified content"
    _assert_code(
        "transport_content_mismatch", lambda: _verify(vector, bundle=bundle)
    )


def test_expiry_and_offline_lease_are_independent(vector) -> None:
    _assert_code(
        "not_yet_valid",
        lambda: _verify(vector, now=datetime(2026, 7, 10, 11, 58, tzinfo=timezone.utc)),
    )
    _assert_code(
        "offline_lease_expired",
        lambda: _verify(vector, now=datetime(2026, 7, 10, 12, 16, tzinfo=timezone.utc)),
    )
    assert _verify(
        vector,
        now=datetime(2026, 7, 10, 12, 16, tzinfo=timezone.utc),
        enforce_offline_lease=False,
    ).jti == "bundle-golden-v1"
    _assert_code(
        "expired_bundle",
        lambda: _verify(vector, now=datetime(2026, 7, 10, 13, 0, tzinfo=timezone.utc)),
    )


def test_revocation_and_replay_guards(vector) -> None:
    _assert_code(
        "revoked_signing_key",
        lambda: _verify(vector, revoked_key_ids={"orchestra-test-key-v1"}),
    )
    _assert_code(
        "revoked_bundle",
        lambda: _verify(vector, revoked_jtis={"bundle-golden-v1"}),
    )

    seen = set()
    assert _verify(vector, seen_jtis=seen).jti == "bundle-golden-v1"
    assert seen == {"bundle-golden-v1"}
    _assert_code("replayed_bundle", lambda: _verify(vector, seen_jtis=seen))


def test_rotation_accepts_an_overlapping_new_trust_entry(vector) -> None:
    private_key = Ed25519PrivateKey.generate()
    new_entry = BundleTrustEntry(
        issuer="https://orchestra.example.test",
        key_id="orchestra-test-key-v2",
        public_key_spki_der_base64=_public_key_base64(private_key),
        allowed_org_ids=frozenset({"org-golden"}),
        allowed_audiences=frozenset({"prometa-runtime"}),
        allowed_environments=frozenset({"prod"}),
        active_from=datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc),
    )
    bundle = _resign(
        vector,
        private_key,
        issuer=new_entry.issuer,
        key_id=new_entry.key_id,
    )
    store = BundleTrustStore([_entry(vector), new_entry])
    assert _verify(vector, bundle=bundle, trust_store=store).trust_entry == new_entry


def test_retired_or_wrongly_scoped_trust_entries_are_rejected(vector) -> None:
    retired = _entry(
        vector,
        retired_at=datetime(2026, 7, 10, 12, 1, tzinfo=timezone.utc),
    )
    _assert_code(
        "signing_key_retired",
        lambda: _verify(vector, trust_store=BundleTrustStore([retired])),
    )
    wrong_scope = _entry(vector, allowed_org_ids=frozenset({"org-other"}))
    _assert_code(
        "signing_key_org_denied",
        lambda: _verify(vector, trust_store=BundleTrustStore([wrong_scope])),
    )


def test_signed_digest_and_subject_tampering_are_rejected(vector) -> None:
    private_key = Ed25519PrivateKey.generate()
    key_id = "orchestra-test-key-v2"
    entry = BundleTrustEntry(
        issuer=vector["trust"]["issuer"],
        key_id=key_id,
        public_key_spki_der_base64=_public_key_base64(private_key),
    )
    store = BundleTrustStore([entry])

    bad_digest = _resign(
        vector,
        private_key,
        issuer=entry.issuer,
        key_id=key_id,
        mutate_claims=lambda claims: claims.update(
            {"artifactDigest": "sha256:" + hashlib.sha256(b"wrong").hexdigest()}
        ),
    )
    _assert_code(
        "artifact_digest_mismatch",
        lambda: _verify(vector, bundle=bad_digest, trust_store=store),
    )

    bad_subject = _resign(
        vector,
        private_key,
        issuer=entry.issuer,
        key_id=key_id,
        mutate_claims=lambda claims: claims.update(
            {"subject": "agent-manifest:other:v99"}
        ),
    )
    _assert_code(
        "subject_mismatch",
        lambda: _verify(vector, bundle=bad_subject, trust_store=store),
    )
