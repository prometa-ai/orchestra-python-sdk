"""Runtime-control lease verification and the enforced kill-switch gate."""

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
    RuntimeControlDenied,
    RuntimeControlGate,
    RuntimeControlRefresher,
    build_runtime_receipt,
    verify_runtime_control_lease,
)
from prometa.runtime.receipts import RuntimeReceiptError


ISSUER = "https://control.prometa.test"
KEY_ID = "control-lease-key-1"
ORG_ID = "org-acme"
ENVIRONMENT = "prod"
TENANT_ID = "tenant-acme"
DEPLOYMENT_ID = "deployment-42"
SOLUTION_ID = "solution-42"
AGENT_ID = "agent-42"
RELEASE_ID = "release-42"
RUNTIME_TARGET = "prometa-runtime-host"
ROUTING_POLICY_TYPE = "orchestra.routing-policy"
# The refresher applies leases against the real clock, so anchor the
# fixtures to it rather than to a frozen instant.
NOW = datetime.now(timezone.utc).replace(microsecond=0)

_PRIVATE_KEY = Ed25519PrivateKey.generate()
_PUBLIC_KEY_DER = base64.b64encode(
    _PRIVATE_KEY.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
).decode("ascii")


def _instant(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _trust_store(**overrides) -> BundleTrustStore:
    values = {
        "issuer": ISSUER,
        "key_id": KEY_ID,
        "public_key_spki_der_base64": _PUBLIC_KEY_DER,
        "allowed_artifact_types": frozenset({RUNTIME_CONTROL_LEASE_TYPE}),
    }
    values.update(overrides)
    return BundleTrustStore([BundleTrustEntry(**values)])


def _control(
    scope="deployment", subject_id=DEPLOYMENT_ID, state="serving", reason_code=None
):
    control = {"scope": scope, "subjectId": subject_id, "state": state}
    if reason_code is not None:
        control["reasonCode"] = reason_code
    return control


def _lease(
    *,
    lease_id="lease-1",
    revision=1,
    controls=None,
    mode="enforcing",
    stale_action="continue",
    issued_at=NOW,
    not_before=None,
    expires_at=None,
    claim_overrides=None,
    private_key=None,
    issuer=ISSUER,
    key_id=KEY_ID,
    **envelope_overrides,
):
    not_before = not_before if not_before is not None else issued_at
    expires_at = (
        expires_at if expires_at is not None else not_before + timedelta(minutes=5)
    )
    claims = {
        "artifactType": RUNTIME_CONTROL_LEASE_TYPE,
        "leaseVersion": 1,
        "issuer": issuer,
        "keyId": key_id,
        "orgId": ORG_ID,
        "targetEnvironment": ENVIRONMENT,
        "leaseId": lease_id,
        "revision": revision,
        "issuedAt": _instant(issued_at),
        "notBefore": _instant(not_before),
        "expiresAt": _instant(expires_at),
        "mode": mode,
        "staleAction": stale_action,
        "controls": [_control()] if controls is None else controls,
        "jti": "jti-%s-%s" % (lease_id, revision),
    }
    if claim_overrides is not None:
        claims.update(claim_overrides)
    signed_payload = json.dumps(
        claims, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    signature = base64.b64encode(
        (private_key or _PRIVATE_KEY).sign(signed_payload.encode("utf-8"))
    ).decode("ascii")
    envelope = {
        "artifactType": RUNTIME_CONTROL_LEASE_TYPE,
        "leaseVersion": 1,
        "algorithm": "ed25519",
        "canonicalization": "signed-payload-json-v1",
        "issuer": issuer,
        "keyId": key_id,
        "signedPayload": signed_payload,
        "signature": signature,
        "signed": True,
    }
    envelope.update(envelope_overrides)
    return envelope


def _gate(**overrides) -> RuntimeControlGate:
    values = {
        "trust_store": _trust_store(),
        "expected_org_id": ORG_ID,
        "expected_environment": ENVIRONMENT,
        "tenant_id": TENANT_ID,
        "deployment_id": DEPLOYMENT_ID,
        "solution_id": SOLUTION_ID,
        "agent_id": AGENT_ID,
    }
    values.update(overrides)
    return RuntimeControlGate(**values)


def _verify(lease, **overrides):
    options = {
        "expected_org_id": ORG_ID,
        "expected_environment": ENVIRONMENT,
        "now": NOW,
    }
    options.update(overrides)
    trust_store = options.pop("trust_store", None) or _trust_store()
    return verify_runtime_control_lease(lease, trust_store, **options)


def _assert_code(code, callback) -> None:
    with pytest.raises(BundleVerificationError) as caught:
        callback()
    assert caught.value.code == code


def test_a_valid_quarantine_lease_verifies_with_its_controls_and_reason() -> None:
    verified = _verify(
        _lease(
            lease_id="lease-q",
            revision=7,
            controls=[
                _control(state="quarantined", reason_code="incident-4711"),
                _control(scope="agent", subject_id=AGENT_ID, state="serving"),
            ],
        )
    )

    assert verified.lease_id == "lease-q"
    assert verified.revision == 7
    assert verified.mode == "enforcing"
    assert verified.stale_action == "continue"
    assert [control.scope for control in verified.controls] == ["deployment", "agent"]
    assert verified.controls[0].quarantined is True
    assert verified.controls[0].reason_code == "incident-4711"
    assert verified.digest.startswith("sha256:")


@pytest.mark.parametrize(
    "code,lease_kwargs",
    [
        ("wrong_org", {"claim_overrides": {"orgId": "org-other"}}),
        ("wrong_environment", {"claim_overrides": {"targetEnvironment": "staging"}}),
        ("unsupported_control_state", {"controls": [_control(state="paused")]}),
        ("unsupported_control_scope", {"controls": [_control(scope="cluster")]}),
        ("unsupported_control_mode", {"mode": "observe"}),
        ("unsupported_stale_action", {"stale_action": "freeze"}),
        ("invalid_lease_revision", {"claim_overrides": {"revision": -1}}),
        ("invalid_lease_revision", {"claim_overrides": {"revision": "7"}}),
        ("invalid_controls", {"claim_overrides": {"controls": {}}}),
        (
            "ambiguous_control",
            {"controls": [_control(), _control(state="quarantined")]},
        ),
        (
            "wrong_artifact_type",
            {"claim_overrides": {"artifactType": "orchestra.promotion-attestation"}},
        ),
        (
            "invalid_time_claim",
            {"claim_overrides": {"expiresAt": "2026-08-11T09:05:00Z"}},
        ),
    ],
)
def test_a_lease_that_does_not_match_the_contract_shape_is_refused(
    code, lease_kwargs
) -> None:
    _assert_code(code, lambda: _verify(_lease(**lease_kwargs)))


def test_the_envelope_artifact_type_is_checked_before_the_signature() -> None:
    lease = _lease()
    lease["artifactType"] = "orchestra.routing-policy"

    _assert_code("wrong_artifact_type", lambda: _verify(lease))


def test_a_lease_signed_by_a_routing_key_is_refused_even_though_it_verifies() -> None:
    routing_key_store = _trust_store(
        allowed_artifact_types=frozenset({ROUTING_POLICY_TYPE})
    )

    _assert_code(
        "signing_key_purpose_denied",
        lambda: _verify(_lease(), trust_store=routing_key_store),
    )


def test_a_key_with_no_declared_purpose_cannot_stop_a_runtime() -> None:
    _assert_code(
        "signing_key_purpose_unknown",
        lambda: _verify(_lease(), trust_store=_trust_store(allowed_artifact_types=None)),
    )


def test_a_constraint_the_lease_cannot_carry_is_refused_not_ignored() -> None:
    # A lease has no audience claim. Silently skipping the constraint would let
    # an operator believe this key is narrowed when nothing narrows it.
    _assert_code(
        "invalid_trust_entry",
        lambda: _verify(
            _lease(),
            trust_store=_trust_store(allowed_audiences=frozenset({"anything"})),
        ),
    )


def test_a_lease_longer_than_the_ceiling_is_refused() -> None:
    long_lease = _lease(expires_at=NOW + timedelta(hours=6))

    _assert_code(
        "control_lease_too_long",
        lambda: _verify(long_lease, max_lease_seconds=900),
    )


def test_an_expired_or_unsigned_lease_never_verifies() -> None:
    expired = _lease(
        issued_at=NOW - timedelta(minutes=20),
        expires_at=NOW - timedelta(minutes=15),
    )
    _assert_code("expired_control_lease", lambda: _verify(expired))

    forged = _lease(private_key=Ed25519PrivateKey.generate())
    _assert_code("invalid_signature", lambda: _verify(forged))

    unsigned = _lease()
    unsigned["signed"] = False
    _assert_code("unsigned_control_lease", lambda: _verify(unsigned))


def test_an_unknown_signing_key_is_refused_by_the_shared_trust_store() -> None:
    _assert_code(
        "unknown_signing_key",
        lambda: _verify(_lease(issuer="https://attacker.test")),
    )


def test_a_quarantined_gate_refuses_work_and_names_the_reason() -> None:
    gate = _gate()
    status = gate.apply(
        _lease(controls=[_control(state="quarantined", reason_code="incident-4711")]),
        now=NOW,
    )

    assert status.state == "quarantined"
    assert status.admitting is False
    assert status.stale is False
    assert status.enforcement == "enforcing"
    assert status.enforced_control_count == 1
    assert status.reason_code == "incident-4711"
    with pytest.raises(RuntimeControlDenied) as caught:
        gate.admit(NOW)
    assert caught.value.code == "runtime_quarantined"


def test_a_control_this_replica_is_not_the_subject_of_is_not_enforcement() -> None:
    gate = _gate()

    status = gate.apply(
        _lease(
            controls=[
                _control(subject_id="deployment-other", state="quarantined"),
                _control(scope="agent", subject_id="agent-other", state="quarantined"),
            ]
        ),
        now=NOW,
    )

    assert status.admitting is True
    assert status.state == "serving"
    assert status.enforced_control_count == 0
    assert status.enforcement == "advisory"


def test_a_scope_this_replica_cannot_resolve_is_reported_as_not_enforced() -> None:
    engine_like = _gate(
        solution_id=None,
        agent_id=None,
        enforceable_scopes=frozenset({"org", "tenant", "deployment"}),
    )

    status = engine_like.apply(
        _lease(
            controls=[_control(scope="agent", subject_id=AGENT_ID, state="quarantined")]
        ),
        now=NOW,
    )

    assert status.admitting is True
    assert status.enforced_control_count == 0
    assert status.enforcement == "advisory"
    assert sorted(status.enforceable_scopes) == ["deployment", "org", "tenant"]


def test_a_scope_with_no_identity_behind_it_cannot_be_declared_enforceable() -> None:
    with pytest.raises(ValueError, match="agent"):
        _gate(agent_id=None)


def test_an_advisory_lease_never_refuses_traffic_however_stale_it_gets() -> None:
    gate = _gate()
    gate.apply(
        _lease(
            mode="advisory",
            stale_action="stop",
            controls=[_control(state="quarantined", reason_code="dry-run")],
        ),
        now=NOW,
    )

    for at in (NOW, NOW + timedelta(hours=4)):
        status = gate.status(at)
        assert status.admitting is True
        assert status.mode == "advisory"
        assert status.enforcement == "advisory"
        assert status.enforced_control_count == 0
        assert gate.admit(at).admitting is True


def test_a_serving_gate_keeps_serving_when_the_lease_goes_stale() -> None:
    gate = _gate()
    gate.apply(_lease(), now=NOW)

    later = NOW + timedelta(minutes=30)
    status = gate.status(later)

    assert status.state == "serving"
    assert status.stale is True
    assert status.admitting is True
    assert gate.admit(later).stale is True


def test_the_signed_stale_action_decides_and_not_the_local_default() -> None:
    permissive_local_default = _gate(stale_action="continue")
    permissive_local_default.apply(_lease(stale_action="stop"), now=NOW)

    assert permissive_local_default.admit(NOW + timedelta(minutes=1)).admitting is True
    with pytest.raises(RuntimeControlDenied) as caught:
        permissive_local_default.admit(NOW + timedelta(minutes=30))
    assert caught.value.code == "runtime_controls_stale"
    assert permissive_local_default.stale_action == "stop"


def test_quarantine_survives_expiry_so_it_cannot_be_waited_out() -> None:
    gate = _gate()
    gate.apply(_lease(controls=[_control(state="quarantined")]), now=NOW)

    later = NOW + timedelta(hours=4)
    status = gate.status(later)

    assert status.stale is True
    assert status.state == "quarantined"
    assert status.admitting is False


def test_a_replayed_lower_revision_cannot_lift_a_live_quarantine() -> None:
    gate = _gate()
    early_serving = _lease(lease_id="lease-fleet", revision=6)
    gate.apply(
        _lease(
            lease_id="lease-fleet",
            revision=7,
            controls=[_control(state="quarantined")],
            issued_at=NOW + timedelta(minutes=1),
        ),
        now=NOW + timedelta(minutes=1),
    )

    with pytest.raises(BundleVerificationError) as caught:
        gate.apply(early_serving, now=NOW + timedelta(minutes=2))

    assert caught.value.code == "control_lease_out_of_order"
    assert gate.status(NOW + timedelta(minutes=2)).admitting is False


def test_a_replay_from_another_lease_stream_cannot_lift_a_live_quarantine() -> None:
    # The replayed lease is genuine, correctly signed, unexpired and says
    # `serving`; the only thing it changes is the leaseId. An out-of-order
    # guard scoped to one leaseId has no recorded revision for a stream it has
    # never seen, so it accepts it and the quarantine is lifted by whoever can
    # replay a captured lease.
    gate = _gate()
    gate.apply(
        _lease(
            lease_id="lease-stream-a",
            revision=40,
            controls=[_control(state="quarantined")],
        ),
        now=NOW,
    )

    with pytest.raises(BundleVerificationError) as caught:
        gate.apply(
            _lease(
                lease_id="lease-stream-b",
                revision=39,
                controls=[_control(state="serving")],
                issued_at=NOW + timedelta(seconds=30),
            ),
            now=NOW + timedelta(seconds=40),
        )

    assert caught.value.code == "control_lease_out_of_order"
    status = gate.status(NOW + timedelta(seconds=40))
    assert status.admitting is False
    assert status.state == "quarantined"
    assert status.lease_id == "lease-stream-a"


def test_a_new_lease_stream_at_a_higher_revision_is_still_adopted() -> None:
    # Global ordering must not pin a replica to one leaseId: reissuing the
    # control set under a new lease is a normal operation.
    gate = _gate()
    gate.apply(
        _lease(
            lease_id="lease-stream-a",
            revision=40,
            controls=[_control(state="quarantined")],
        ),
        now=NOW,
    )

    status = gate.apply(
        _lease(
            lease_id="lease-stream-b",
            revision=41,
            controls=[_control(state="serving")],
            issued_at=NOW + timedelta(seconds=30),
        ),
        now=NOW + timedelta(seconds=40),
    )

    assert status.state == "serving"
    assert status.admitting is True


def test_a_matched_serving_control_is_not_counted_as_enforcement() -> None:
    gate = _gate()

    status = gate.apply(
        _lease(
            controls=[
                _control(state="serving", reason_code="incident-cleared"),
                _control(
                    scope="tenant", subject_id="tenant-someone-else", state="quarantined"
                ),
            ]
        ),
        now=NOW,
    )

    # This replica matched a control and enforced nothing: agreeing to serve is
    # not a control being applied, and the quarantine names somebody else.
    assert status.admitting is True
    assert status.state == "serving"
    assert status.enforced_control_count == 0
    assert status.enforcement == "advisory"


def test_a_hard_stop_reports_the_controls_it_is_refusing_for() -> None:
    gate = _gate()
    gate.apply(_lease(stale_action="stop", controls=[_control()]), now=NOW)

    status = gate.status(NOW + timedelta(hours=4))

    assert status.admitting is False
    assert status.denial_code == "runtime_controls_stale"
    assert status.enforced_control_count == 1
    assert status.enforcement == "enforcing"


def test_controls_at_a_scope_this_replica_cannot_resolve_are_reported_as_a_gap() -> None:
    engine_like = _gate(
        solution_id=None,
        agent_id=None,
        enforceable_scopes=frozenset({"org", "tenant", "deployment"}),
    )

    status = engine_like.apply(
        _lease(
            controls=[
                _control(state="quarantined"),
                _control(scope="agent", subject_id=AGENT_ID, state="quarantined"),
            ]
        ),
        now=NOW,
    )

    assert status.enforced_control_count == 1
    assert status.ignored_control_count == 1


def test_a_refreshed_lease_at_the_same_revision_is_adopted() -> None:
    # A control set that has not changed is re-signed with the same leaseId and
    # revision and a later expiry. Refusing it would starve the refresh.
    gate = _gate()
    gate.apply(
        _lease(lease_id="lease-fleet", revision=7, controls=[_control()]), now=NOW
    )

    status = gate.apply(
        _lease(
            lease_id="lease-fleet",
            revision=7,
            controls=[_control()],
            issued_at=NOW + timedelta(minutes=4),
        ),
        now=NOW + timedelta(minutes=4),
    )

    assert status.stale is False
    assert status.revision == 7


def test_a_gate_with_no_lease_yet_serves_but_reports_unknown_and_stale() -> None:
    gate = _gate()

    status = gate.status(NOW)

    assert status.state == "unknown"
    assert status.stale is True
    assert status.admitting is True
    assert status.enforced_control_count == 0
    assert _gate(stale_action="stop").status(NOW).admitting is False


def test_the_public_readiness_view_carries_no_identity_or_operator_text() -> None:
    gate = _gate()
    gate.apply(
        _lease(
            lease_id="lease-secret",
            controls=[_control(state="quarantined", reason_code="CEO ordered stop")],
        ),
        now=NOW,
    )

    public = gate.status(NOW).to_public_wire()

    assert public == {
        "admitting": False,
        "quarantined": True,
        "stale": False,
        "enforcing": True,
        "enforcedControlCount": 1,
    }
    assert "CEO ordered stop" not in json.dumps(public)
    assert "lease-secret" not in json.dumps(public)


def test_an_advisory_lease_does_not_report_an_enforced_quarantine() -> None:
    gate = _gate()
    gate.apply(
        _lease(mode="advisory", controls=[_control(state="quarantined")]), now=NOW
    )

    public = gate.status(NOW).to_public_wire()

    assert public["admitting"] is True
    assert public["quarantined"] is False
    assert public["enforcing"] is False


def test_a_refresh_failure_records_the_error_and_keeps_the_enforced_state() -> None:
    gate = _gate()
    gate.apply(_lease(controls=[_control(state="quarantined")]), now=NOW)

    def fetch():
        raise RuntimeError("control plane unreachable")

    observed = []
    refresher = RuntimeControlRefresher(
        gate,
        fetch,
        interval_seconds=60,
        on_status=lambda outcome, status: observed.append(outcome),
    )
    status = refresher.refresh_once()

    assert observed == ["refresh_failed"]
    assert status.state == "quarantined"
    assert status.admitting is False
    assert status.last_refresh_error == "control_refresh_failed"


def test_every_refresh_acknowledges_even_when_nothing_changed() -> None:
    gate = _gate()
    quarantine = _lease(
        lease_id="lease-q", revision=4, controls=[_control(state="quarantined")]
    )
    leases = [quarantine, quarantine]
    acknowledgements = []
    refresher = RuntimeControlRefresher(
        gate,
        lambda: leases.pop(0) if leases else None,
        interval_seconds=60,
        on_refresh=lambda status, previous: acknowledgements.append(
            (previous.state, status.state)
        ),
    )

    refresher.refresh_once()
    refresher.refresh_once()

    # A replica sitting steady in `quarantined` still speaks, or it drops out
    # of the enforcement count it is judged by.
    assert acknowledgements == [("unknown", "quarantined"), ("quarantined", "quarantined")]


def test_a_refresh_that_lifts_quarantine_reports_the_new_state() -> None:
    gate = _gate()
    gate.apply(
        _lease(lease_id="lease-q", revision=4, controls=[_control(state="quarantined")]),
        now=NOW,
    )
    leases = [_lease(lease_id="lease-q", revision=5, issued_at=NOW + timedelta(minutes=1))]
    changes = []
    refresher = RuntimeControlRefresher(
        gate,
        lambda: leases.pop(0) if leases else None,
        interval_seconds=60,
        on_refresh=lambda status, previous: changes.append(
            (previous.state, status.state)
        ),
    )

    refresher.refresh_once()
    refresher.refresh_once()

    assert changes == [("quarantined", "serving"), ("serving", "serving")]
    assert gate.status().last_refresh_error == "control_lease_absent"


def test_a_rejected_lease_leaves_the_previous_state_enforced() -> None:
    gate = _gate()
    gate.apply(_lease(controls=[_control(state="quarantined")]), now=NOW)
    forged = _lease(private_key=Ed25519PrivateKey.generate())
    refresher = RuntimeControlRefresher(gate, lambda: forged, interval_seconds=60)

    status = refresher.refresh_once()

    assert status.state == "quarantined"
    assert status.last_refresh_error == "invalid_signature"


def test_the_documented_refresh_cost_is_what_the_fixtures_say() -> None:
    # The README quotes this overhead as the reason `refreshIntervalSeconds`
    # exists. Recompute it from the repo's own fixtures so the number cannot
    # drift away from the artifacts it describes.
    fixtures = Path(__file__).parent / "fixtures"

    def _read(name):
        return json.loads((fixtures / name).read_text(encoding="utf-8"))

    def _bytes(document):
        return len(json.dumps(document, separators=(",", ":")).encode("utf-8"))

    handoff = _read("runtime-release-handoff-v1.json")
    handoff["bundle"] = _read("bundle-envelope-v1.json")
    handoff["promotionAttestation"] = _read("promotion-attestation-v1.json")
    lease = _read("lease-vectors/vectors/lease-valid-v1.json")["steps"][0]["lease"]
    handoff["runtimeControl"] = lease

    assert _bytes(handoff) == 8332
    assert _bytes(lease) == 977
    assert round(_bytes(handoff) / _bytes(lease), 1) == 8.5


def _ack(**overrides):
    values = {
        "leaseId": "lease-q",
        "revision": 7,
        "enforcement": "enforcing",
        "enforceableScopes": ["deployment", "org"],
        "enforcedControlCount": 1,
        "ignoredControlCount": 0,
        "stale": False,
        "leaseExpiresAt": "2026-08-11T09:05:00.000Z",
        "leaseParseFailed": False,
    }
    values.update(overrides)
    return values


def _receipt(**overrides):
    values = {
        "attestation_id": "attestation-1",
        "artifact_digest": "sha256:" + "a" * 64,
        "release_id": RELEASE_ID,
        "deployment_id": DEPLOYMENT_ID,
        "target_environment": ENVIRONMENT,
        "runtime_target": RUNTIME_TARGET,
        "runtime_id": "runtime-1",
        "runtime_version": "0.20.2",
        "transition": "quarantined",
        "outcome": "succeeded",
        "runtime_control_ack": _ack(),
    }
    values.update(overrides)
    return build_runtime_receipt(**values)


def test_a_quarantine_acknowledgement_travels_in_the_pinned_shape() -> None:
    receipt = _receipt()

    assert receipt["transition"] == "quarantined"
    assert receipt[RUNTIME_CONTROL_ACK_KEY] == _ack()


def test_the_acknowledgement_carries_the_pinned_keys_and_no_others() -> None:
    # `mode` is the issuer's instruction and is deliberately not echoed back:
    # a consumer reading it here would be told what the replica was asked to
    # do rather than what it did.
    receipt = _receipt(runtime_control_ack=_ack(mode="enforcing", leaseDigest="x"))

    assert sorted(receipt[RUNTIME_CONTROL_ACK_KEY]) == [
        "enforceableScopes",
        "enforcedControlCount",
        "enforcement",
        "ignoredControlCount",
        "leaseExpiresAt",
        "leaseId",
        "leaseParseFailed",
        "revision",
        "stale",
    ]
    assert "mode" not in receipt[RUNTIME_CONTROL_ACK_KEY]


def test_an_acknowledgement_cannot_name_a_lease_without_its_revision() -> None:
    with pytest.raises(RuntimeReceiptError):
        _receipt(runtime_control_ack=_ack(revision=None))
    with pytest.raises(RuntimeReceiptError):
        _receipt(
            transition="controls_stale",
            outcome="failed",
            runtime_control_ack=_ack(leaseId=None, stale=True),
        )


def test_an_acknowledgement_cannot_claim_enforcement_it_cannot_name() -> None:
    # §1: a surface may only claim enforcement it can prove.
    with pytest.raises(RuntimeReceiptError):
        _receipt(
            transition="controls_stale",
            outcome="failed",
            runtime_control_ack=_ack(
                leaseId=None,
                revision=None,
                leaseExpiresAt=None,
                stale=True,
                enforcement="enforcing",
            ),
        )


def test_an_acknowledgement_is_refused_when_a_pinned_member_is_wrong() -> None:
    for broken in (
        {"enforceableScopes": ["cluster"]},
        {"enforceableScopes": []},
        {"enforcement": "off"},
        {"enforcement": "partly"},
        {"enforcedControlCount": -1},
        {"ignoredControlCount": "1"},
        {"stale": "false"},
        {"leaseParseFailed": None},
    ):
        with pytest.raises(RuntimeReceiptError):
            _receipt(runtime_control_ack=_ack(**broken))
    for field in (
        "leaseId",
        "revision",
        "enforcement",
        "enforceableScopes",
        "enforcedControlCount",
        "ignoredControlCount",
        "stale",
        "leaseExpiresAt",
        "leaseParseFailed",
    ):
        # A missing member is fatal rather than defaulted: defaulting one would
        # rewrite what the replica said.
        incomplete = _ack()
        incomplete.pop(field)
        with pytest.raises(RuntimeReceiptError):
            _receipt(runtime_control_ack=incomplete)


def test_a_stale_acknowledgement_may_omit_a_lease_but_not_the_flag() -> None:
    stale = _receipt(
        transition="controls_stale",
        outcome="failed",
        runtime_control_ack=_ack(
            leaseId=None,
            revision=None,
            leaseExpiresAt=None,
            enforcement="advisory",
            enforcedControlCount=0,
            stale=True,
        ),
    )

    assert stale[RUNTIME_CONTROL_ACK_KEY]["leaseId"] is None
    assert stale[RUNTIME_CONTROL_ACK_KEY]["revision"] is None
    assert stale[RUNTIME_CONTROL_ACK_KEY]["stale"] is True
    with pytest.raises(RuntimeReceiptError):
        _receipt(transition="controls_stale", outcome="failed")
    with pytest.raises(RuntimeReceiptError):
        _receipt(
            transition="controls_stale",
            outcome="succeeded",
            runtime_control_ack=_ack(stale=True),
        )


def test_a_stale_replica_still_acknowledges_the_lease_it_is_enforcing() -> None:
    # `controls_stale` is the per-refresh freshness acknowledgement, so it
    # carries the lease and revision the replica is still applying — a
    # quarantine that outlives its lease is still being enforced.
    stale = _receipt(
        transition="controls_stale",
        outcome="failed",
        runtime_control_ack=_ack(stale=True),
    )

    assert stale[RUNTIME_CONTROL_ACK_KEY]["leaseId"] == "lease-q"
    assert stale[RUNTIME_CONTROL_ACK_KEY]["revision"] == 7
    assert stale[RUNTIME_CONTROL_ACK_KEY]["enforcedControlCount"] == 1


def test_a_lifecycle_receipt_cannot_pretend_to_acknowledge_a_control() -> None:
    with pytest.raises(RuntimeReceiptError):
        _receipt(transition="active", outcome="succeeded")
    with pytest.raises(RuntimeReceiptError):
        _receipt(transition="quarantined", outcome="succeeded",
                 runtime_control_ack=_ack(stale=True))
    with pytest.raises(RuntimeReceiptError):
        build_runtime_receipt(
            attestation_id="attestation-1",
            artifact_digest="sha256:" + "a" * 64,
            release_id=RELEASE_ID,
            deployment_id=DEPLOYMENT_ID,
            target_environment=ENVIRONMENT,
            runtime_target=RUNTIME_TARGET,
            runtime_id="runtime-1",
            runtime_version="0.20.2",
            transition="active",
            outcome="succeeded",
            runtime_control_ack=_ack(),
        )
