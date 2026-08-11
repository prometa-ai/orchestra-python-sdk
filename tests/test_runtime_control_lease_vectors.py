"""The shared runtime-control lease conformance vectors (contract §8).

These fixtures are not this SDK's. They are the cross-implementation gate the
engine and the control plane run against the same bytes, which is the only
thing that would have caught the wire-shape divergence that produced three
mutually unverifiable implementations. ``tests/fixtures/lease-vectors/README.md``
describes the format; ``generate.py`` beside the shared copy reproduces it.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("cryptography")

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from prometa.runtime import (
    RUNTIME_CONTROL_ACK_KEY,
    RUNTIME_CONTROL_LEASE_TYPE,
    BundleTrustEntry,
    BundleTrustStore,
    BundleVerificationError,
    RuntimeControlGate,
    RuntimeControlRefresher,
    build_runtime_receipt,
)
from prometa.runtime.receipts import RuntimeReceiptError


VECTOR_ROOT = Path(__file__).parent / "fixtures" / "lease-vectors"
RUNTIME_ID = "sdk-host"


def _load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


MANIFEST = _load(VECTOR_ROOT / "manifest.json")
VECTORS = [entry["id"] for entry in MANIFEST["vectors"]]
ACK_VECTORS = [entry["id"] for entry in MANIFEST["acknowledgementVectors"]]

# The acknowledgement vectors pin a wire shape, not signed bytes, so the leases
# used to drive this runtime into each of those states are signed locally.
_ACK_KEY = Ed25519PrivateKey.generate()
_ACK_TRUST = {
    "issuer": "https://control.prometa.test",
    "keyId": "ack-fixture-key-1",
    "publicKeySpkiDerBase64": base64.b64encode(
        _ACK_KEY.public_key().public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    ).decode("ascii"),
    "allowedArtifactTypes": [RUNTIME_CONTROL_LEASE_TYPE],
}


def _instant(value) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _wire_instant(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _ack_lease(*, lease_id, revision, expires_at, controls, issued_at=None):
    expires = _instant(expires_at)
    issued = _instant(issued_at) if issued_at is not None else expires - timedelta(
        minutes=5
    )
    identities = MANIFEST["identities"]
    claims = {
        "artifactType": RUNTIME_CONTROL_LEASE_TYPE,
        "leaseVersion": 1,
        "issuer": _ACK_TRUST["issuer"],
        "keyId": _ACK_TRUST["keyId"],
        "orgId": identities["orgId"],
        "targetEnvironment": identities["targetEnvironment"],
        "leaseId": lease_id,
        "revision": revision,
        "issuedAt": _wire_instant(issued),
        "notBefore": _wire_instant(issued),
        "expiresAt": _wire_instant(expires),
        "mode": "enforcing",
        "staleAction": "continue",
        "controls": controls,
        "jti": "jti-%s-%d" % (lease_id, revision),
    }
    signed_payload = json.dumps(
        claims, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return {
        "artifactType": RUNTIME_CONTROL_LEASE_TYPE,
        "leaseVersion": 1,
        "algorithm": "ed25519",
        "canonicalization": "signed-payload-json-v1",
        "issuer": _ACK_TRUST["issuer"],
        "keyId": _ACK_TRUST["keyId"],
        "signedPayload": signed_payload,
        "signature": base64.b64encode(
            _ACK_KEY.sign(signed_payload.encode("utf-8"))
        ).decode("ascii"),
        "signed": True,
    }


def _optional_set(entry, key):
    value = entry.get(key)
    return frozenset(value) if value is not None else None


def _trust_store(entries) -> BundleTrustStore:
    return BundleTrustStore(
        [
            BundleTrustEntry(
                issuer=entry["issuer"],
                key_id=entry["keyId"],
                public_key_spki_der_base64=entry["publicKeySpkiDerBase64"],
                allowed_org_ids=_optional_set(entry, "allowedOrgIds"),
                allowed_environments=_optional_set(entry, "allowedEnvironments"),
                allowed_artifact_types=_optional_set(entry, "allowedArtifactTypes"),
            )
            for entry in entries
        ]
    )


def _gate(vector) -> RuntimeControlGate:
    context = vector["context"]
    profile = vector["runtimes"][RUNTIME_ID]
    subjects = profile["subjects"]
    return RuntimeControlGate(
        trust_store=_trust_store(vector["trust"]),
        expected_org_id=context["expectedOrgId"],
        expected_environment=context["expectedEnvironment"],
        tenant_id=subjects["tenant"],
        deployment_id=subjects["deployment"],
        solution_id=subjects.get("solution"),
        agent_id=subjects.get("agent"),
        enforceable_scopes=frozenset(profile["enforceableScopes"]),
        max_lease_seconds=context["maxLeaseSeconds"],
        max_clock_skew_seconds=context["maxClockSkewSeconds"],
    )


@pytest.mark.parametrize("vector_id", VECTORS)
def test_the_shared_conformance_vectors_all_pass(vector_id) -> None:
    vector = _load(VECTOR_ROOT / "vectors" / ("%s.json" % vector_id))
    gate = _gate(vector)

    for index, step in enumerate(vector["steps"]):
        at = _instant(step["at"])
        where = "%s step %d" % (vector_id, index)
        if step["action"] == "apply":
            expected = step["expect"]
            if expected["outcome"] == "refused":
                with pytest.raises(BundleVerificationError) as caught:
                    gate.apply(step["lease"], now=at)
                assert caught.value.code == expected["errorCode"], where
                continue
            gate.apply(step["lease"], now=at)
            lease = gate.current_lease()
            assert lease.digest == expected["digest"], where
            assert lease.lease_id == expected["leaseId"], where
            assert lease.revision == expected["revision"], where
            assert lease.mode == expected["mode"], where
            assert lease.stale_action == expected["staleAction"], where
            assert len(lease.controls) == expected["controlCount"], where
            continue

        assert step["action"] == "evaluate", where
        expected = step["expectByRuntime"][RUNTIME_ID]
        status = gate.status(at)
        assert status.admitting is expected["admitting"], where
        assert status.state == expected["state"], where
        assert status.stale is expected["stale"], where
        assert status.enforcement == expected["enforcement"], where
        assert status.enforced_control_count == expected["enforcedControlCount"], where


def test_every_vector_the_contract_names_is_present() -> None:
    assert {
        "lease-valid-v1",
        "lease-wrong-key-purpose",
        "lease-lower-revision",
        "lease-advisory-expired",
        "lease-agent-scope",
        "lease-tampered-signature",
    } <= set(VECTORS)


def _receipt_for(acknowledgement):
    """Emit the acknowledgement on the transition it belongs to.

    ``controls_stale`` is the per-refresh freshness acknowledgement and the
    only transition that may carry no lease, so it is what an acknowledgement
    without a ``leaseId`` has to travel on.
    """

    stale = bool(acknowledgement.get("stale")) if acknowledgement else False
    return build_runtime_receipt(
        attestation_id="attestation-1",
        artifact_digest="sha256:" + "a" * 64,
        release_id="release-golden",
        deployment_id="deployment-golden",
        target_environment="staging",
        runtime_target="prometa-runtime-host",
        runtime_id="runtime-1",
        runtime_version="0.20.2",
        transition="controls_stale",
        outcome="failed" if stale else "succeeded",
        runtime_control_ack=acknowledgement,
    )


@pytest.mark.parametrize("vector_id", ACK_VECTORS)
def test_the_acknowledgement_direction_vectors_all_pass(vector_id) -> None:
    # These run the other way round for a producer: every `accepted` case is a
    # shape this runtime must be able to emit unchanged, and no `rejected` case
    # may ever leave it. Nothing covered this direction before, which is how a
    # runtime came to emit a shape the control plane does not read.
    vector = _load(VECTOR_ROOT / "acks" / ("%s.json" % vector_id))
    acknowledgement = vector["acknowledgement"]
    outcome = vector["expect"]["outcome"]

    if outcome == "absent":
        assert acknowledgement is None
        # A control acknowledgement with no object at all is not "compliant",
        # so this runtime cannot emit one: the transition requires it.
        with pytest.raises(RuntimeReceiptError):
            _receipt_for(None)
        return
    if outcome == "rejected":
        with pytest.raises(RuntimeReceiptError):
            _receipt_for(acknowledgement)
        return

    assert outcome == "accepted", vector_id
    receipt = _receipt_for(acknowledgement)
    assert receipt[RUNTIME_CONTROL_ACK_KEY] == vector["expect"]["normalized"]


def _comparable(acknowledgement):
    """Scope order is not pinned by the contract; membership is."""

    return {
        **acknowledgement,
        "enforceableScopes": sorted(acknowledgement["enforceableScopes"]),
    }


def _ack_gate(scopes, **overrides):
    """A gate on the vectors' own identities, so leaseIds compare directly."""

    identities = MANIFEST["identities"]
    values = {
        "trust_store": _trust_store([_ACK_TRUST]),
        "expected_org_id": identities["orgId"],
        "expected_environment": identities["targetEnvironment"],
        "tenant_id": identities["tenantId"],
        "deployment_id": identities["deploymentId"],
        "enforceable_scopes": frozenset(scopes),
    }
    values.update(overrides)
    return RuntimeControlGate(**values)


def test_this_runtime_emits_the_acknowledgement_shape_while_enforcing() -> None:
    vector = _load(VECTOR_ROOT / "acks" / "ack-enforcing-quarantine.json")
    expected = vector["expect"]["normalized"]
    gate = _ack_gate(expected["enforceableScopes"])
    identities = MANIFEST["identities"]

    gate.apply(
        _ack_lease(
            lease_id=expected["leaseId"],
            revision=expected["revision"],
            expires_at=expected["leaseExpiresAt"],
            controls=[
                {
                    "scope": "org",
                    "subjectId": identities["orgId"],
                    "state": "quarantined",
                },
                {
                    "scope": "deployment",
                    "subjectId": identities["deploymentId"],
                    "state": "quarantined",
                },
                # A scope this gate never declared enforceable: reported as the
                # gap it is, not dropped.
                {
                    "scope": "agent",
                    "subjectId": identities["agentId"],
                    "state": "quarantined",
                },
            ],
        ),
        now=_instant(vector["observedAt"]),
    )

    acknowledgement = gate.status(_instant(vector["observedAt"])).to_acknowledgement()

    assert acknowledgement == {**expected, "enforceableScopes": sorted(
        expected["enforceableScopes"]
    )}
    # Order is not pinned by the contract; membership is.
    assert set(acknowledgement["enforceableScopes"]) == set(
        expected["enforceableScopes"]
    )
    assert _receipt_for(acknowledgement)[RUNTIME_CONTROL_ACK_KEY] == acknowledgement


def test_this_runtime_emits_the_acknowledgement_shape_when_stale_and_enforcing() -> None:
    vector = _load(VECTOR_ROOT / "acks" / "ack-stale-lease-still-enforcing.json")
    expected = vector["expect"]["normalized"]
    gate = _ack_gate(expected["enforceableScopes"])
    identities = MANIFEST["identities"]
    expires_at = _instant(expected["leaseExpiresAt"])

    gate.apply(
        _ack_lease(
            lease_id=expected["leaseId"],
            revision=expected["revision"],
            expires_at=expected["leaseExpiresAt"],
            issued_at=expires_at - timedelta(minutes=5),
            controls=[
                {
                    "scope": "deployment",
                    "subjectId": identities["deploymentId"],
                    "state": "quarantined",
                }
            ],
        ),
        now=expires_at - timedelta(minutes=1),
    )

    acknowledgement = gate.status(_instant(vector["observedAt"])).to_acknowledgement()

    # An expired lease under `continue` is still being enforced, and says so:
    # reporting 0 here would make an unreachable issuer look like a lifted
    # quarantine on the operator's surface.
    assert acknowledgement == {**expected, "enforceableScopes": sorted(
        expected["enforceableScopes"]
    )}


def test_a_replica_holding_no_lease_reports_it_and_claims_nothing() -> None:
    vector = _load(VECTOR_ROOT / "acks" / "ack-no-lease.json")
    expected = vector["expect"]["normalized"]
    gate = _ack_gate(expected["enforceableScopes"])

    acknowledgement = gate.status(_instant(vector["observedAt"])).to_acknowledgement()

    assert {
        key: value
        for key, value in _comparable(acknowledgement).items()
        if key != "stale"
    } == {
        key: value
        for key, value in _comparable(expected).items()
        if key != "stale"
    }
    # This runtime differs from the vector on one field, in the direction that
    # claims less: a replica holding no lease reports `stale: true`, because it
    # holds nothing current. §7b pins the shape and no acknowledgement vector
    # refuses that pairing, so the intake accepts it either way — which the
    # emitted receipt below proves against the same validator.
    assert acknowledgement["stale"] is True
    assert _receipt_for(acknowledgement)[RUNTIME_CONTROL_ACK_KEY] == acknowledgement


def test_a_lease_this_replica_could_not_read_is_reported_as_such() -> None:
    vector = _load(VECTOR_ROOT / "acks" / "ack-lease-parse-failed.json")
    expected = vector["expect"]["normalized"]
    gate = _ack_gate(expected["enforceableScopes"])

    gate.record_refresh_failure("control_lease_invalid", lease_parse_failed=True)
    acknowledgement = gate.status(_instant(vector["observedAt"])).to_acknowledgement()

    assert acknowledgement["leaseParseFailed"] is True
    assert {
        key: value
        for key, value in _comparable(acknowledgement).items()
        if key != "stale"
    } == {
        key: value
        for key, value in _comparable(expected).items()
        if key != "stale"
    }
    # An unreadable lease must never read as "no quarantine": nothing here
    # claims enforcement, and the refusal reason is kept for the operator view.
    assert acknowledgement["enforcement"] == "advisory"
    assert gate.status().last_refresh_error == "control_lease_invalid"


def test_an_out_of_order_refusal_is_not_reported_as_an_unreadable_lease() -> None:
    # A replay refused on ordering was understood, not unparseable. Reporting
    # it as a parse failure would hide a replay attempt behind a complaint
    # about the producer's bytes.
    gate = _ack_gate(["org", "tenant", "deployment"])
    identities = MANIFEST["identities"]
    # The refresher applies against the real clock, so anchor these to it.
    now = datetime.now(timezone.utc).replace(microsecond=0)
    quarantine = _ack_lease(
        lease_id="lease-ack-stream-a",
        revision=40,
        expires_at=now + timedelta(minutes=5),
        issued_at=now,
        controls=[
            {
                "scope": "deployment",
                "subjectId": identities["deploymentId"],
                "state": "quarantined",
            }
        ],
    )
    gate.apply(quarantine, now=now)
    replay = _ack_lease(
        lease_id="lease-ack-stream-b",
        revision=39,
        expires_at=now + timedelta(minutes=4),
        issued_at=now - timedelta(minutes=1),
        controls=[
            {
                "scope": "deployment",
                "subjectId": identities["deploymentId"],
                "state": "serving",
            }
        ],
    )
    refresher = RuntimeControlRefresher(gate, lambda: replay, interval_seconds=60)

    status = refresher.refresh_once()

    assert status.last_refresh_error == "control_lease_out_of_order"
    assert status.lease_parse_failed is False
    assert status.admitting is False


def test_a_quarantine_cannot_be_lifted_by_unplugging_the_control_plane() -> None:
    # Beyond the contract's six. `staleAction: "continue"` means keep enforcing
    # what the last lease said, not resume serving: the other reading lets an
    # operator lift a quarantine by making the control plane unreachable.
    vector = _load(VECTOR_ROOT / "vectors" / "lease-quarantine-expired.json")
    apply_step, evaluate_step = vector["steps"]

    assert apply_step["expect"]["staleAction"] == "continue"
    assert evaluate_step["expectByRuntime"][RUNTIME_ID] == {
        "admitting": False,
        "state": "quarantined",
        "stale": True,
        "enforcement": "enforcing",
        "enforcedControlCount": 1,
    }


def test_the_engine_profile_is_the_one_that_cannot_resolve_an_agent() -> None:
    # The vectors are shared, so this SDK asserts the *other* runtime's
    # declared scopes too: an engine that quietly grew "agent" would make
    # lease-agent-scope pass on both sides while enforcing nothing.
    vector = _load(VECTOR_ROOT / "vectors" / "lease-agent-scope.json")
    engine = vector["runtimes"]["engine"]

    assert "agent" not in engine["enforceableScopes"]
    assert "solution" not in engine["enforceableScopes"]
    assert vector["steps"][-1]["expectByRuntime"]["engine"] == {
        "admitting": True,
        "state": "serving",
        "stale": False,
        "enforcement": "advisory",
        "enforcedControlCount": 0,
    }
