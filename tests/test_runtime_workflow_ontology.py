from __future__ import annotations

import copy

import pytest

from prometa.runtime.workflow_ontology import (
    WorkflowOntologyError,
    canonical_digest,
    evaluate_workflow_policy,
    parse_workflow_ontology_artifact,
)


def _spec():
    return {
        "schemaVersion": 1,
        "name": "Invoice reference",
        "description": "Small cross-language policy fixture.",
        "sectorBinding": {
            "sector": "manufacturing",
            "snapshot": {"categories": [], "nodes": [], "relations": []},
            "snapshotDigest": canonical_digest(
                {"categories": [], "nodes": [], "relations": []}
            ),
        },
        "allowedConditionPaths": [
            "request.idempotencyKey",
            "fact.variance_percent",
        ],
        "roles": [],
        "businessObjects": [],
        "states": [
            {"id": "matched", "label": "Matched", "type": "initial"},
            {"id": "ready", "label": "Ready", "type": "terminal"},
        ],
        "tasks": [
            {
                "id": "assess",
                "label": "Assess",
                "kind": "decision",
                "requiredFactIds": ["variance_percent"],
            }
        ],
        "transitions": [
            {
                "id": "matched_to_ready",
                "fromStateId": "matched",
                "toStateId": "ready",
                "taskId": "assess",
                "priority": 0,
                "condition": {
                    "op": "lte",
                    "path": "fact.variance_percent",
                    "value": 2,
                },
                "requiredFactIds": ["variance_percent"],
                "obligationIds": ["reserve"],
            }
        ],
        "facts": [
            {
                "id": "variance_percent",
                "path": "fact.variance_percent",
                "label": "Variance",
                "authoritativeSource": "procurement",
                "maxAgeSeconds": 900,
            }
        ],
        "evidenceRequirements": [],
        "obligations": [
            {
                "id": "reserve",
                "kind": "reserve_idempotency",
                "description": "Reserve once.",
            }
        ],
        "controls": [
            {
                "id": "idempotency",
                "label": "Idempotency",
                "effect": "obligate",
                "reasonCode": "idempotency_required",
                "target": {"transitionIds": ["matched_to_ready"]},
                "obligationIds": ["reserve"],
                "idempotency": {
                    "scope": "workflow_instance",
                    "keyPath": "request.idempotencyKey",
                },
            }
        ],
    }


def _artifact():
    spec = _spec()
    ontology_digest = canonical_digest(spec)
    projection = {
        key: spec[key]
        for key in (
            "allowedConditionPaths",
            "states",
            "tasks",
            "transitions",
            "facts",
            "evidenceRequirements",
            "obligations",
            "controls",
        )
    }
    policy_digest = canonical_digest(projection)
    sector_digest = spec["sectorBinding"]["snapshotDigest"]
    compiled = {
        "schemaVersion": 1,
        "ontologyDigest": ontology_digest,
        "policyDigest": policy_digest,
        "sectorSnapshotDigest": sector_digest,
        "spec": spec,
        "stateIds": ["matched", "ready"],
        "terminalStateIds": ["ready"],
        "transitionIdsByStateAndTask": {"matched:assess": ["matched_to_ready"]},
    }
    return {
        "ontologyId": "ontology-1",
        "version": 1,
        "mode": "enforce",
        "ontologyDigest": ontology_digest,
        "policyDigest": policy_digest,
        "sectorSnapshotDigest": sector_digest,
        "compiledPolicy": compiled,
    }


def _input(**overrides):
    value = {
        "mode": "enforce",
        "now": "2026-07-28T12:00:00.000Z",
        "state": {"current": "matched", "instanceId": "invoice-1", "version": 0},
        "request": {"taskId": "assess", "idempotencyKey": "post-1"},
        "actor": {"opaqueRef": "agent-1", "roleIds": ["invoice_agent"]},
        "facts": {
            "variance_percent": {
                "value": 1.5,
                "observedAt": "2026-07-28T11:59:00.000Z",
                "authoritative": True,
                "source": "procurement",
            }
        },
        "approvals": [],
        "evidenceRefs": [],
        "usedIdempotencyKeys": [],
    }
    value.update(overrides)
    return value


def test_strict_artifact_parser_rederives_digests_and_indexes():
    artifact = parse_workflow_ontology_artifact(_artifact())
    assert artifact.ontology_id == "ontology-1"
    assert artifact.mode == "enforce"
    assert artifact.compiled_policy["terminalStateIds"] == ["ready"]

    for mutation in (
        lambda value: value.update({"unknown": True}),
        lambda value: value.update({"mode": "permissive"}),
        lambda value: value.update({"ontologyDigest": "sha256:" + "0" * 64}),
        lambda value: value["compiledPolicy"].update(
            {"transitionIdsByStateAndTask": {}}
        ),
    ):
        candidate = copy.deepcopy(_artifact())
        mutation(candidate)
        with pytest.raises(WorkflowOntologyError):
            parse_workflow_ontology_artifact(candidate)

    snapshot_drift = copy.deepcopy(_artifact())
    snapshot_drift["compiledPolicy"]["spec"]["sectorBinding"]["snapshot"] = {
        "categories": ["changed"],
        "nodes": [],
        "relations": [],
    }
    snapshot_drift["ontologyDigest"] = canonical_digest(
        snapshot_drift["compiledPolicy"]["spec"]
    )
    snapshot_drift["compiledPolicy"]["ontologyDigest"] = snapshot_drift[
        "ontologyDigest"
    ]
    with pytest.raises(WorkflowOntologyError):
        parse_workflow_ontology_artifact(snapshot_drift)


def test_policy_allows_with_obligations_and_proposed_transition():
    policy = _artifact()["compiledPolicy"]
    decision = evaluate_workflow_policy(policy, _input())
    assert decision.as_dict() == {
        "recommendedOutcome": "allow",
        "appliedOutcome": "allow",
        "reasonCodes": ["allowed_with_obligations"],
        "matchedControlIds": ["idempotency"],
        "missingFactIds": [],
        "staleFactIds": [],
        "obligationIds": ["reserve"],
        "evidenceRequirementIds": [],
        "counterfactualReasonCodes": [],
        "proposedTransitionId": "matched_to_ready",
        "proposedState": "ready",
    }


def test_policy_precedence_for_invalid_missing_stale_and_duplicate_inputs():
    policy = _artifact()["compiledPolicy"]
    assert evaluate_workflow_policy(
        policy,
        _input(state={"current": "unknown", "instanceId": "invoice-1", "version": 0}),
    ).reason_codes == ("invalid_current_state",)
    assert evaluate_workflow_policy(
        policy, _input(request={"taskId": "unknown"})
    ).reason_codes == ("unknown_task",)

    missing = _input(facts={})
    decision = evaluate_workflow_policy(policy, missing)
    assert decision.recommended_outcome == "indeterminate"
    assert decision.applied_outcome == "deny"
    assert decision.missing_fact_ids == ("variance_percent",)

    stale = copy.deepcopy(_input())
    stale["facts"]["variance_percent"]["observedAt"] = "2026-07-28T10:00:00Z"
    assert evaluate_workflow_policy(policy, stale).stale_fact_ids == (
        "variance_percent",
    )

    duplicate = _input(usedIdempotencyKeys=["post-1"])
    assert evaluate_workflow_policy(policy, duplicate).reason_codes == (
        "duplicate_idempotency_key",
    )


def test_observe_mode_logs_denial_but_applies_allow():
    policy = _artifact()["compiledPolicy"]
    decision = evaluate_workflow_policy(
        policy,
        _input(
            mode="observe",
            request={"taskId": "assess", "idempotencyKey": "post-1"},
            facts={
                "variance_percent": {
                    "value": 4,
                    "observedAt": "2026-07-28T11:59:00Z",
                    "authoritative": True,
                    "source": "procurement",
                }
            },
        ),
    )
    assert decision.recommended_outcome == "deny"
    assert decision.applied_outcome == "allow"
    assert decision.reason_codes == ("transition_condition_false",)
