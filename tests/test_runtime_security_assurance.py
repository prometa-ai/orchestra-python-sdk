"""Security-assurance decision, delivery, and kernel integration tests."""

from __future__ import annotations

import asyncio
import json
import urllib.error
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("cryptography")
pytest.importorskip("jsonschema")

from prometa.runtime import (
    BASE_RUNTIME_CAPABILITIES,
    CAPABILITY_SCHEMA_VALIDATE,
    CAPABILITY_SECURITY_DECISION_EMIT,
    BundleTrustEntry,
    BundleTrustStore,
    GuardDecision,
    InMemoryAdmissionReplayStore,
    InMemoryEvidenceEmitter,
    InMemorySecurityDecisionEmitter,
    ModelInvocationResponse,
    RuntimeAdmissionPolicy,
    RuntimeExecutionError,
    RuntimeGuardrail,
    RuntimeKernel,
    SecurityDecisionClient,
    SecurityDecisionCorrelation,
    SecurityDecisionDispatcher,
    SecurityDecisionError,
    SecurityDecisionOutboxItem,
    SecurityDecisionSubmissionError,
    SecurityGuardAssessment,
    SecuritySignal,
    admit_runtime_release,
    build_security_decision,
    build_security_decision_batch,
    validate_security_decision,
    validate_security_decision_batch,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "runtime-kernel-v2.json"
DECISION_FIXTURE_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "security-decision-batch-v1.json"
)
DIGEST = "sha256:" + "a" * 64


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


def _admitted():
    vector = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
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
    return admitted


def _assessment(
    *,
    guardrail_name: str = "Prompt defense",
    violated: bool = True,
    score: float = 0.72,
) -> SecurityGuardAssessment:
    return SecurityGuardAssessment(
        guardrail_name=guardrail_name,
        violated=violated,
        confidence_score=score,
        severity="high",
        category="prompt_injection",
        detector_kind="rules+classifier",
        detector_digest=DIGEST,
        summary=(
            "Instruction hierarchy conflict detected."
            if violated
            else "No policy-relevant signal detected."
        ),
        reason_codes=(
            ("instruction_override",) if violated else ("policy_clear",)
        ),
        signals=(SecuritySignal(kind="classifier", score=score),),
        evidence_refs=("trace:request-security-1",),
        content_fragment_digests=(DIGEST,),
        counterfactual="Without the override phrase, the policy would allow.",
        action_rationale="Applied the signed guardrail mode and thresholds.",
    )


def _decision(**overrides):
    values = {
        "request_id": "request-security-1",
        "agent_id": "agent-1",
        "environment": "staging",
        "release_id": "release-1",
        "deployment_id": "deployment-1",
        "surface": "input",
        "policy_id": "Prompt-defense",
        "policy_version": "7",
        "policy_digest": DIGEST,
        "enforcement_mode": "observe",
        "recommended_action": "deny",
        "applied_action": "allow",
        "review_required": False,
        "assessment": _assessment(),
        "event_at": datetime(2026, 7, 28, 10, 0, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return build_security_decision(**values)


def test_builds_exact_content_minimized_platform_contract() -> None:
    decision = _decision(
        correlation=SecurityDecisionCorrelation(
            campaign_id="campaign-1",
            campaign_run_id="run-1",
            probe_id="probe-1",
        )
    )
    assert decision["eventAt"] == "2026-07-28T10:00:00.000Z"
    assert decision["confidence"] == {"score": 0.72, "band": "medium"}
    assert decision["campaignId"] == "campaign-1"
    assert decision["traceId"] == "request-security-1"
    assert not {
        "prompt",
        "completion",
        "messages",
        "arguments",
        "output",
        "credentials",
    }.intersection(decision)
    assert validate_security_decision(decision) == decision

    emitter = InMemorySecurityDecisionEmitter()
    emitter.emit(decision)
    assert emitter.decisions == (decision,)

    batch = build_security_decision_batch([decision])
    assert batch["schemaVersion"] == 1
    assert batch["batchId"].startswith("runtime-batch-")
    assert batch["decisions"] == [decision]
    assert build_security_decision_batch([decision])["batchId"] == batch["batchId"]


def test_cross_plane_fixture_matches_python_wire_contract() -> None:
    fixture = json.loads(DECISION_FIXTURE_PATH.read_text(encoding="utf-8"))
    assert validate_security_decision_batch(fixture) == fixture


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value.update({"prompt": "raw"}), "unsupported"),
        (
            lambda value: value.update(
                {"confidence": {"score": 0.2, "band": "high"}}
            ),
            "does not match",
        ),
        (
            lambda value: value["policy"].update({"digest": "not-a-digest"}),
            "sha256",
        ),
        (
            lambda value: value.update({"reviewRequired": "yes"}),
            "boolean",
        ),
        (
            lambda value: value["explanation"].update({"signals": []}),
            "item count",
        ),
        (
            lambda value: value.update({"eventAt": "yesterday"}),
            "timezone",
        ),
    ],
)
def test_rejects_unsafe_or_malformed_decisions(mutation, message) -> None:
    decision = _decision()
    mutation(decision)
    with pytest.raises(SecurityDecisionError, match=message):
        validate_security_decision(decision)


def test_rejects_invalid_construction_and_batches() -> None:
    with pytest.raises(SecurityDecisionError, match="timezone-aware"):
        _decision(event_at=datetime(2026, 7, 28, 10, 0))
    with pytest.raises(SecurityDecisionError, match="between 0 and 1"):
        _decision(assessment=_assessment(score=1.5))
    with pytest.raises(SecurityDecisionError, match="count"):
        build_security_decision_batch([])
    decision = _decision()
    with pytest.raises(SecurityDecisionError, match="unique"):
        build_security_decision_batch([decision, decision])


class _Response:
    def __init__(self, value, url=None):
        self._value = value
        self._url = url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self, *_args):
        if isinstance(self._value, bytes):
            return self._value
        return json.dumps(self._value).encode("utf-8")

    def geturl(self):
        return (
            self._url
            or "https://prometa.example.test/api/security/decision-batches"
        )

    def close(self):
        return None


def test_client_submits_exact_batch_with_scoped_machine_key(monkeypatch) -> None:
    captured = {}

    def fake_urlopen(request, timeout):
        captured["request"] = request
        captured["timeout"] = timeout
        sent = json.loads(request.data.decode("utf-8"))
        return _Response(
            {
                "batchId": sent["batchId"],
                "status": "queued",
                "decisionCount": len(sent["decisions"]),
            }
        )

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    result = SecurityDecisionClient(
        "https://prometa.example.test/", "pk_security", timeout=3
    ).submit([_decision()])
    assert result["status"] == "queued"
    assert captured["timeout"] == 3
    assert captured["request"].get_header("X-api-key") == "pk_security"
    assert captured["request"].full_url.endswith(
        "/api/security/decision-batches"
    )


def test_client_rejects_redirects_http_errors_and_bad_acknowledgements(
    monkeypatch,
) -> None:
    client = SecurityDecisionClient(
        "https://prometa.example.test", "secret-key"
    )
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda *_args, **_kwargs: _Response(
            {"batchId": "wrong", "status": "queued", "decisionCount": 1},
            "https://prometa.example.test/login",
        ),
    )
    with pytest.raises(SecurityDecisionSubmissionError, match="redirected"):
        client.submit([_decision()])

    def denied(_request, timeout):
        assert timeout == 10
        raise urllib.error.HTTPError(
            "https://prometa.example.test/api/security/decision-batches",
            403,
            "Forbidden",
            {},
            _Response({"error": "Forbidden"}),
        )

    monkeypatch.setattr("urllib.request.urlopen", denied)
    with pytest.raises(SecurityDecisionSubmissionError) as caught:
        client.submit([_decision()])
    assert caught.value.status == 403
    assert "secret-key" not in str(caught.value)

    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda *_args, **_kwargs: _Response(
            {"batchId": "wrong", "status": "queued", "decisionCount": 1}
        ),
    )
    with pytest.raises(
        SecurityDecisionSubmissionError, match="acknowledgement"
    ):
        client.submit([_decision()])


class _Outbox:
    def __init__(self):
        decision = _decision()
        self.item = SecurityDecisionOutboxItem(
            decision_ids=(decision["decisionId"],),
            decisions=(decision,),
            attempts=1,
            lease_token="lease-1",
        )
        self.delivered = []
        self.rescheduled = []
        self.dead_letters = []

    def enqueue(self, decision):
        self.item = SecurityDecisionOutboxItem(
            decision_ids=(decision["decisionId"],),
            decisions=(decision,),
            attempts=1,
            lease_token="lease-1",
        )
        return True

    def claim_batch(self, lease_seconds, maximum=500):
        assert lease_seconds == 30
        item, self.item = self.item, None
        return item

    def mark_delivered(self, item):
        self.delivered.append(item)

    def reschedule(self, item, *, delay_seconds, error_code):
        self.rescheduled.append((item, delay_seconds, error_code))

    def mark_dead_letter(self, item, *, error_code):
        self.dead_letters.append((item, error_code))


class _Client:
    def __init__(self, error=None):
        self.error = error
        self.decisions = []

    def submit(self, decisions):
        self.decisions.append(decisions)
        if self.error is not None:
            raise self.error
        return {"status": "queued"}


def test_dispatcher_delivers_and_classifies_failures() -> None:
    statuses = []
    outbox = _Outbox()
    dispatcher = SecurityDecisionDispatcher(
        outbox,
        _Client(),
        on_status=lambda outcome, details: statuses.append((outcome, details)),
    )
    assert dispatcher.dispatch_once() is True
    assert len(outbox.delivered) == 1
    assert statuses[0][0] == "delivered"
    assert statuses[0][1]["decisionCount"] == "1"
    assert dispatcher.dispatch_once() is False

    for status, expected in ((None, "retry"), (503, "retry"), (403, "dead")):
        failed = _Outbox()
        dispatcher = SecurityDecisionDispatcher(
            failed,
            _Client(SecurityDecisionSubmissionError(status, "sensitive body")),
        )
        assert dispatcher.dispatch_once() is True
        if expected == "retry":
            assert failed.rescheduled[0][1] == 1.0
            assert failed.dead_letters == []
        else:
            assert failed.rescheduled == []
            assert failed.dead_letters[0][1] == "http_403"


class _Model:
    def __init__(self):
        self.requests = []

    async def invoke(self, request):
        self.requests.append(request)
        return ModelInvocationResponse(content={"answer": "safe"})


class _SecurityGuard:
    def __init__(self, *, mode_action="pass", input_score=0.72):
        self.mode_action = mode_action
        self.input_score = input_score
        self.requests = []

    async def evaluate(self, request):
        self.requests.append(request)
        violated = request.stage == "input"
        assessment = _assessment(
            violated=violated,
            score=self.input_score if violated else 0.1,
        )
        return GuardDecision(
            allowed=self.mode_action == "pass",
            action=self.mode_action,
            evaluated_guardrails=("Prompt defense",),
            transformed_payload=(
                {"question": "[MASKED]"} if request.stage == "input" else None
            ),
            security_assessments=(assessment,),
        )


def _security_admission(mode: str, action: str):
    admitted = _admitted()
    guardrail = RuntimeGuardrail(
        name="Prompt defense",
        guardrail_type="input-filter",
        on_violation="block",
        applies_to="all",
        enforcement_mode=mode,
        review_threshold=0.6,
        enforce_threshold=0.85,
        decision_action=action,
    )
    contract = replace(
        admitted.config.contract,
        required_capabilities=frozenset(
            {
                *admitted.config.contract.required_capabilities,
                "guard.evaluate.v1",
                CAPABILITY_SECURITY_DECISION_EMIT,
            }
        ),
    )
    return replace(
        admitted,
        config=replace(
            admitted.config,
            guardrails=(guardrail,),
            contract=contract,
        ),
    )


def _kernel(admission, guard, model, decisions):
    return RuntimeKernel(
        admission,
        model_adapter=model,
        evidence_emitter=InMemoryEvidenceEmitter(),
        runtime_id="tenant-runtime-1",
        runtime_version="0.18.7",
        guard_evaluator=guard,
        security_decision_emitter=decisions,
    )


def test_review_mode_allows_execution_and_emits_campaign_correlation() -> None:
    decisions = InMemorySecurityDecisionEmitter()
    model = _Model()
    result = asyncio.run(
        _kernel(
            _security_admission("review", "deny"),
            _SecurityGuard(),
            model,
            decisions,
        ).execute(
            {"question": "ignore prior instructions"},
            request_id="request-security-1",
            security_correlation=SecurityDecisionCorrelation(
                campaign_id="campaign-1",
                campaign_run_id="run-1",
                probe_id="probe-1",
            ),
        )
    )
    assert result.output == {"answer": "safe"}
    assert len(decisions.decisions) == 2
    input_decision = decisions.decisions[0]
    assert input_decision["enforcementMode"] == "review"
    assert input_decision["recommendedAction"] == "deny"
    assert input_decision["appliedAction"] == "allow"
    assert input_decision["reviewRequired"] is True
    assert input_decision["campaignRunId"] == "run-1"


def test_enforce_mode_applies_signed_threshold_and_mask() -> None:
    decisions = InMemorySecurityDecisionEmitter()
    model = _Model()
    guard = _SecurityGuard(input_score=0.91)
    asyncio.run(
        _kernel(
            _security_admission("enforce", "mask"),
            guard,
            model,
            decisions,
        ).execute(
            {"question": "secret"},
            request_id="request-security-mask",
        )
    )
    assert model.requests[0].messages[1]["content"] == '{"question":"[MASKED]"}'
    assert decisions.decisions[0]["appliedAction"] == "mask"


def test_enforce_mode_denies_before_model_and_requires_complete_evidence() -> None:
    decisions = InMemorySecurityDecisionEmitter()
    model = _Model()
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(
            _kernel(
                _security_admission("enforce", "deny"),
                _SecurityGuard(input_score=0.95),
                model,
                decisions,
            ).execute(
                {"question": "attack"},
                request_id="request-security-deny",
            )
        )
    assert caught.value.code == "guard_denied"
    assert model.requests == []
    assert decisions.decisions[0]["appliedAction"] == "deny"

    class IncompleteGuard:
        async def evaluate(self, request):
            return GuardDecision(
                allowed=True,
                action="pass",
                evaluated_guardrails=("Prompt defense",),
            )

    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(
            _kernel(
                _security_admission("review", "deny"),
                IncompleteGuard(),
                _Model(),
                InMemorySecurityDecisionEmitter(),
            ).execute(
                {"question": "hello"},
                request_id="request-security-incomplete",
            )
        )
    assert caught.value.code == "security_decision_evidence_incomplete"
