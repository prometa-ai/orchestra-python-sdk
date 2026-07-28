"""Workflow-decision validation, delivery, and minimization tests."""

from __future__ import annotations

import json
import urllib.error
from pathlib import Path

import pytest

from prometa.runtime import (
    DurableWorkflowDecisionEmitter,
    WorkflowDecisionClient,
    WorkflowDecisionDispatcher,
    WorkflowDecisionError,
    WorkflowDecisionEvidence,
    WorkflowDecisionOutboxItem,
    WorkflowDecisionSubmissionError,
    build_workflow_decision_batch,
    validate_workflow_decision,
    validate_workflow_decision_batch,
    workflow_decision_from_evidence,
)


DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
DIGEST_C = "sha256:" + "c" * 64
DIGEST_D = "sha256:" + "d" * 64
FIXTURE_PATH = Path(__file__).parent / "fixtures" / "workflow-decision-batch-v1.json"


def _evidence(**overrides) -> WorkflowDecisionEvidence:
    values = {
        "request_id": "request-workflow-1",
        "workflow_id": "invoice-to-sap",
        "workflow_version": 1,
        "workflow_instance_id": "invoice-42",
        "ontology_digest": DIGEST_A,
        "policy_digest": DIGEST_B,
        "sector_snapshot_digest": DIGEST_C,
        "state": "matched",
        "state_version": 3,
        "task_id": "post-to-sap",
        "transition_id": "matched-to-ready",
        "recommended_outcome": "require_approval",
        "applied_outcome": "deny",
        "reason_codes": ("approval_required",),
        "control_ids": ("variance-approval",),
        "obligation_ids": ("reserve-idempotency",),
        "fact_set_digest": DIGEST_D,
        "missing_fact_ids": (),
        "stale_fact_ids": (),
        "approval_references": ("approval-42",),
        "evidence_references": ("po-match-42",),
        "occurred_at": "2026-07-28T10:00:00.000Z",
    }
    values.update(overrides)
    return WorkflowDecisionEvidence(**values)


def _decision(**overrides):
    decision = workflow_decision_from_evidence(_evidence())
    decision.update(overrides)
    return decision


def test_builds_deterministic_payload_free_decision_and_batch() -> None:
    decision = _decision()
    assert decision["decisionId"].startswith("workflow-decision-")
    assert decision["occurredAt"] == "2026-07-28T10:00:00.000Z"
    assert validate_workflow_decision(decision) == decision
    assert workflow_decision_from_evidence(_evidence()) == decision
    assert not {
        "prompt",
        "results",
        "messages",
        "arguments",
        "businessFacts",
        "actorRoles",
    }.intersection(decision)

    batch = build_workflow_decision_batch([decision])
    assert batch["schema"] == "prometa.workflow-decision.v1"
    assert batch["batchId"].startswith("workflow-batch-")
    assert batch["decisions"] == [decision]
    assert validate_workflow_decision_batch(batch) == batch


def test_cross_plane_fixture_matches_python_wire_contract() -> None:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    assert validate_workflow_decision_batch(fixture) == fixture
    assert build_workflow_decision_batch([_decision()]) == fixture


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value.update({"arguments": {"amount": 10}}), "fields"),
        (
            lambda value: value.update({"policyDigest": "not-a-digest"}),
            "sha256",
        ),
        (
            lambda value: value.update({"recommendedOutcome": "maybe"}),
            "unsupported",
        ),
        (
            lambda value: value.update({"occurredAt": "yesterday"}),
            "timezone",
        ),
        (
            lambda value: value.update({"reasonCodes": ["duplicate", "duplicate"]}),
            "unique",
        ),
    ],
)
def test_rejects_raw_unknown_or_malformed_fields(mutation, message) -> None:
    decision = _decision()
    mutation(decision)
    with pytest.raises(WorkflowDecisionError, match=message):
        validate_workflow_decision(decision)


def test_rejects_invalid_batch_and_evidence_types() -> None:
    with pytest.raises(WorkflowDecisionError, match="count"):
        build_workflow_decision_batch([])
    decision = _decision()
    with pytest.raises(WorkflowDecisionError, match="unique"):
        build_workflow_decision_batch([decision, decision])
    with pytest.raises(WorkflowDecisionError, match="WorkflowDecisionEvidence"):
        workflow_decision_from_evidence({})  # type: ignore[arg-type]

    tampered = _decision()
    tampered["state"] = "posted"
    with pytest.raises(WorkflowDecisionError, match="decisionId"):
        validate_workflow_decision(tampered)

    batch = build_workflow_decision_batch([_decision()])
    batch["batchId"] = "workflow-batch-" + "0" * 64
    with pytest.raises(WorkflowDecisionError, match="batchId"):
        validate_workflow_decision_batch(batch)


class _Response:
    def __init__(self, value, url=None):
        self.value = value
        self.url = url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self, *_args):
        if isinstance(self.value, bytes):
            return self.value
        return json.dumps(self.value).encode("utf-8")

    def geturl(self):
        return self.url or "https://prometa.example.test/api/workflow-decision-batches"

    def close(self):
        return None


def test_client_posts_exact_scoped_batch(monkeypatch) -> None:
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
    result = WorkflowDecisionClient(
        "https://prometa.example.test/", "pk_workflow", timeout=3
    ).submit([_decision()])
    assert result["status"] == "queued"
    assert captured["timeout"] == 3
    assert captured["request"].get_header("X-api-key") == "pk_workflow"
    assert captured["request"].full_url.endswith("/api/workflow-decision-batches")


def test_client_rejects_redirect_and_http_failure(monkeypatch) -> None:
    client = WorkflowDecisionClient("https://prometa.example.test", "workflow-secret")
    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda *_args, **_kwargs: _Response(
            {"batchId": "wrong", "status": "queued", "decisionCount": 1},
            "https://prometa.example.test/login",
        ),
    )
    with pytest.raises(WorkflowDecisionSubmissionError, match="redirected"):
        client.submit([_decision()])

    def denied(_request, timeout):
        assert timeout == 10
        raise urllib.error.HTTPError(
            "https://prometa.example.test/api/workflow-decision-batches",
            403,
            "Forbidden",
            {},
            _Response({"error": "Forbidden"}),
        )

    monkeypatch.setattr("urllib.request.urlopen", denied)
    with pytest.raises(WorkflowDecisionSubmissionError) as caught:
        client.submit([_decision()])
    assert caught.value.status == 403
    assert "workflow-secret" not in str(caught.value)


class _Outbox:
    def __init__(self):
        self.item = WorkflowDecisionOutboxItem(
            decision_ids=(_decision()["decisionId"],),
            decisions=(_decision(),),
            attempts=1,
            lease_token="lease-1",
        )
        self.enqueued = []
        self.delivered = []
        self.rescheduled = []
        self.dead_letters = []

    def enqueue(self, decision):
        self.enqueued.append(decision)
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

    def submit(self, _decisions):
        if self.error is not None:
            raise self.error
        return {"status": "queued"}


def test_durable_emitter_and_dispatcher_are_local_first() -> None:
    outbox = _Outbox()
    dispatcher = WorkflowDecisionDispatcher(outbox, _Client())
    emitter = DurableWorkflowDecisionEmitter(outbox, dispatcher)
    emitter.emit(_evidence())
    assert outbox.enqueued == [_decision()]
    assert dispatcher.dispatch_once() is True
    assert len(outbox.delivered) == 1

    for status, expected in ((None, "retry"), (503, "retry"), (403, "dead")):
        failed = _Outbox()
        dispatcher = WorkflowDecisionDispatcher(
            failed,
            _Client(WorkflowDecisionSubmissionError(status, "sensitive response")),
        )
        assert dispatcher.dispatch_once() is True
        if expected == "retry":
            assert failed.rescheduled[0][1:] == (
                1.0,
                "transport" if status is None else "http_503",
            )
            assert failed.dead_letters == []
        else:
            assert failed.rescheduled == []
            assert failed.dead_letters[0][1] == "http_403"
