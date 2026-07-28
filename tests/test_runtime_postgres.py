"""PostgreSQL durability tests for the optional tenant runtime."""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from prometa.runtime import (
    RUNTIME_POSTGRES_COMPATIBILITY_VERSION,
    RUNTIME_POSTGRES_MAX_SCHEMA_VERSION,
    RUNTIME_POSTGRES_MIN_SCHEMA_VERSION,
    RUNTIME_POSTGRES_SCHEMA_VERSION,
    ExplicitMcpEgressPolicy,
    GovernedMcpToolBroker,
    McpAuditEvent,
    McpBrokerPolicy,
    McpServerConfig,
    McpToolGrant,
    McpTransportError,
    PostgresMcpAuditSink,
    PostgresMcpIdempotencyStore,
    PostgresAdmissionReplayStore,
    PostgresRuntimeActivationStore,
    PostgresRuntimeReceiptOutbox,
    PostgresRuntimeReleaseCache,
    PostgresSecurityDecisionOutbox,
    PostgresRuntimeStateStore,
    PostgresRuntimeTaskStore,
    PostgresWorkflowDecisionOutbox,
    PostgresWorkflowStateStore,
    RuntimeReleaseHandoff,
    RuntimePersistenceError,
    RuntimeExecutionError,
    RuntimeTaskClaim,
    RuntimeTaskError,
    RuntimeTool,
    ToolInvocationRequest,
    WorkflowExecutionContext,
    WorkflowIndeterminateRequest,
    WorkflowStateCommitRequest,
    workflow_decision_from_evidence,
    WorkflowDecisionEvidence,
    SecurityGuardAssessment,
    SecuritySignal,
    build_security_decision,
    build_runtime_receipt,
    canonical_payload_digest,
    check_postgres_runtime_compatibility,
    install_postgres_runtime_schema,
    verify_postgres_runtime_integrity,
)
from prometa.runtime.postgres import (
    compatibility_main as postgres_compatibility_main,
    main as postgres_init_main,
    verify_main as postgres_verify_main,
)


def _unavailable(dsn):
    raise OSError("database unavailable at %s" % dsn)


def _handoff(
    *,
    attestation_id="attestation-cache",
    artifact_digest="sha256:" + "c" * 64,
    fetched_at=None,
):
    fetched = fetched_at or datetime.now(timezone.utc)
    return RuntimeReleaseHandoff(
        attestation_id=attestation_id,
        artifact_id="artifact-cache",
        artifact_digest=artifact_digest,
        release_id="release-cache",
        deployment_id="deployment-cache",
        target_environment="prod",
        runtime_target="tenant-runtime",
        bundle={"signed": True, "artifactDigest": artifact_digest},
        promotion_attestation={
            "signed": True,
            "attestationId": attestation_id,
        },
        checked_at=fetched,
        fetched_at=fetched,
    )


def _mcp_audit_event(**overrides):
    values = {
        "audit_reference": "mcp-audit-postgres",
        "phase": "execution",
        "outcome": "completed",
        "occurred_at": "2026-07-15T12:00:00.000Z",
        "request_id": "request-postgres",
        "call_id": "call-postgres",
        "agent_id": "agent-postgres",
        "release_id": "release-postgres",
        "deployment_id": "deployment-postgres",
        "environment": "prod",
        "server_name": "Orders",
        "server_connection_id": "orders-prod",
        "transport": "streamable-http",
        "operation": "orders.write",
        "permission": "write",
        "effective_risk": "medium",
        "side_effects": "write",
        "scopes": ("orders.write",),
        "approval_references": ("review-postgres",),
        "argument_digest": "sha256:" + "a" * 64,
        "output_digest": "sha256:" + "b" * 64,
        "idempotency_key": "mcp1:" + "c" * 64,
        "reason": None,
    }
    values.update(overrides)
    return McpAuditEvent(**values)


def _mcp_tool_request(*, request_id="request-postgres", call_id="call-postgres"):
    return ToolInvocationRequest(
        request_id=request_id,
        call_id=call_id,
        tool=RuntimeTool(
            name="Write order",
            source="mcp",
            operation="orders.write",
            input_schema={"type": "object"},
            mcp_server="Orders",
            side_effects="write",
            risk_level="medium",
            auth_binding="none",
            scopes=("orders.write",),
            approval_required=True,
            required_guardrails=(),
        ),
        arguments={"orderId": "order-postgres"},
        agent_id="agent-postgres",
        release_id="release-postgres",
        deployment_id="deployment-postgres",
        environment="prod",
        granted_scopes=("orders.write",),
        approval_references=("review-postgres",),
    )


def _security_decision(*, decision_id=None):
    decision = build_security_decision(
        request_id="request-security-postgres",
        agent_id="agent-postgres",
        environment="prod",
        release_id="release-postgres",
        deployment_id="deployment-postgres",
        surface="tool_request",
        policy_id="secret-policy",
        policy_version="7",
        policy_digest="sha256:" + "a" * 64,
        enforcement_mode="enforce",
        recommended_action="mask",
        applied_action="mask",
        review_required=False,
        assessment=SecurityGuardAssessment(
            guardrail_name="Secret policy",
            violated=True,
            confidence_score=0.96,
            severity="high",
            category="secret_exposure",
            detector_kind="rules",
            detector_digest="sha256:" + "b" * 64,
            summary="Credential-like value detected.",
            reason_codes=("credential_pattern",),
            signals=(SecuritySignal(kind="regex", score=1.0),),
            counterfactual="A non-secret identifier would be allowed.",
            action_rationale="Masked before the tool received it.",
        ),
    )
    if decision_id is not None:
        decision["decisionId"] = decision_id
    return decision


def _workflow_decision():
    return workflow_decision_from_evidence(
        WorkflowDecisionEvidence(
            request_id="request-workflow-postgres",
            workflow_id="workflow-postgres",
            workflow_version=1,
            workflow_instance_id="instance-postgres",
            ontology_digest="sha256:" + "a" * 64,
            policy_digest="sha256:" + "b" * 64,
            sector_snapshot_digest="sha256:" + "c" * 64,
            state="received",
            state_version=0,
            task_id="extract",
            transition_id="extract-transition",
            recommended_outcome="allow",
            applied_outcome="allow",
            reason_codes=("valid",),
            control_ids=(),
            obligation_ids=(),
            fact_set_digest="sha256:" + "d" * 64,
            missing_fact_ids=(),
            stale_fact_ids=(),
            approval_references=(),
            evidence_references=(),
            occurred_at="2026-07-28T10:00:00.000Z",
        )
    )


class _StaticCursor:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, statement, parameters=None):
        return None

    def fetchone(self):
        return self.row


class _StaticConnection:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return _StaticCursor(self.row)


class _CompatibilityCursor:
    def __init__(self, versions, tables):
        self.versions = versions
        self.tables = tables
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, statement, parameters=None):
        if "to_regclass" in statement:
            self.rows = [("prometa_runtime_schema_migrations",)]
        elif "SELECT version" in statement:
            self.rows = [(version,) for version in self.versions]
        elif "information_schema.tables" in statement:
            self.rows = [(table,) for table in self.tables]
        else:
            self.rows = []

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class _CompatibilityConnection:
    def __init__(self, versions, tables):
        self.versions = versions
        self.tables = tables

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return _CompatibilityCursor(self.versions, self.tables)


def test_postgres_adapters_validate_inputs_before_connecting() -> None:
    with pytest.raises(ValueError, match="tenant_id"):
        PostgresAdmissionReplayStore("postgresql://unused", tenant_id=" ")

    replay = PostgresAdmissionReplayStore(
        "postgresql://secret:password@db.example/runtime",
        tenant_id="tenant-1",
        connect=_unavailable,
    )
    with pytest.raises(RuntimePersistenceError) as caught:
        replay.reserve_pair("bundle-1", "promotion-1")
    assert caught.value.code == "replay_store_unavailable"
    assert "secret" not in str(caught.value)
    assert "password" not in str(caught.value)
    assert caught.value.__cause__ is None

    state = PostgresRuntimeStateStore(
        "postgresql://secret:password@db.example/runtime",
        tenant_id="tenant-1",
        runtime_id="runtime-1",
        connect=_unavailable,
    )
    with pytest.raises(RuntimePersistenceError) as caught:
        asyncio.run(state.save("request-1", {"status": "running"}))
    assert caught.value.code == "state_store_unavailable"
    assert "password" not in str(caught.value)

    activation = PostgresRuntimeActivationStore(
        "postgresql://secret:password@db.example/runtime",
        tenant_id="tenant-1",
        connect=_unavailable,
    )
    activation_values = {
        "runtime_id": "runtime-1",
        "deployment_id": "deployment-1",
        "release_id": "release-1",
        "artifact_digest": "sha256:" + "a" * 64,
        "bundle_jti": "bundle-1",
        "promotion_jti": "promotion-1",
    }
    with pytest.raises(RuntimePersistenceError) as caught:
        activation.activate_or_join(**activation_values)
    assert caught.value.code == "activation_store_unavailable"
    assert "password" not in str(caught.value)
    with pytest.raises(ValueError, match="artifact_digest"):
        activation.activate_or_join(
            **{**activation_values, "artifact_digest": "not-a-digest"}
        )

    outbox = PostgresRuntimeReceiptOutbox(
        "postgresql://secret:password@db.example/runtime",
        tenant_id="tenant-1",
        connect=_unavailable,
    )
    receipt = build_runtime_receipt(
        attestation_id="attestation-1",
        artifact_digest="sha256:" + "a" * 64,
        release_id="release-1",
        deployment_id="deployment-1",
        target_environment="prod",
        runtime_target="tenant-runtime",
        runtime_id="runtime-1",
        runtime_version="1",
        transition="admitted",
        outcome="accepted",
        receipt_id="receipt-1",
    )
    with pytest.raises(RuntimePersistenceError) as caught:
        outbox.enqueue(receipt)
    assert caught.value.code == "receipt_outbox_unavailable"
    assert "password" not in str(caught.value)

    security_outbox = PostgresSecurityDecisionOutbox(
        "postgresql://secret:password@db.example/runtime",
        tenant_id="tenant-1",
        connect=_unavailable,
    )
    with pytest.raises(RuntimePersistenceError) as caught:
        security_outbox.enqueue(_security_decision())
    assert caught.value.code == "security_decision_outbox_unavailable"
    assert "password" not in str(caught.value)

    workflow_outbox = PostgresWorkflowDecisionOutbox(
        "postgresql://secret:password@db.example/runtime",
        tenant_id="tenant-1",
        connect=_unavailable,
    )
    with pytest.raises(RuntimePersistenceError) as caught:
        workflow_outbox.enqueue(_workflow_decision())
    assert caught.value.code == "workflow_decision_outbox_unavailable"
    assert "password" not in str(caught.value)

    cache = PostgresRuntimeReleaseCache(
        "postgresql://secret:password@db.example/runtime",
        tenant_id="tenant-1",
        connect=_unavailable,
    )
    with pytest.raises(RuntimePersistenceError) as caught:
        cache.save(_handoff())
    assert caught.value.code == "release_cache_unavailable"
    assert "password" not in str(caught.value)
    with pytest.raises(RuntimePersistenceError) as caught:
        cache.load("attestation-cache", max_age_seconds=60)
    assert caught.value.code == "release_cache_unavailable"
    with pytest.raises(ValueError, match="bindings"):
        cache.save(
            RuntimeReleaseHandoff(
                **{
                    **_handoff().__dict__,
                    "bundle": {
                        "signed": True,
                        "artifactDigest": "sha256:" + "e" * 64,
                    },
                }
            )
        )

    tasks = PostgresRuntimeTaskStore(
        "postgresql://secret:password@db.example/runtime",
        tenant_id="tenant-1",
        runtime_id="runtime-1",
        connect=_unavailable,
    )
    with pytest.raises(RuntimePersistenceError) as caught:
        tasks.claim(
            "request-1",
            input_digest=canonical_payload_digest({"question": "hello"}),
            artifact_digest="sha256:" + "a" * 64,
            release_id="release-1",
            deployment_id="deployment-1",
            recoverable=True,
            max_attempts=3,
            lease_seconds=30,
        )
    assert caught.value.code == "task_store_unavailable"
    assert "password" not in str(caught.value)

    with pytest.raises(RuntimePersistenceError) as caught:
        check_postgres_runtime_compatibility(
            "postgresql://secret:password@db.example/runtime",
            connect=_unavailable,
        )
    assert caught.value.code == "runtime_schema_compatibility_failed"
    assert "password" not in str(caught.value)

    with pytest.raises(RuntimePersistenceError) as caught:
        verify_postgres_runtime_integrity(
            "postgresql://secret:password@db.example/runtime",
            connect=_unavailable,
        )
    assert caught.value.code == "runtime_schema_verification_failed"
    assert "password" not in str(caught.value)

    with pytest.raises(RuntimePersistenceError) as caught:
        check_postgres_runtime_compatibility(
            "postgresql://unused",
            connect=lambda dsn: _StaticConnection((None,)),
        )
    assert caught.value.code == "runtime_schema_uninitialized"

    with pytest.raises(ValueError, match="dsn"):
        check_postgres_runtime_compatibility(" ")

    with pytest.raises(RuntimePersistenceError) as caught:
        check_postgres_runtime_compatibility(
            "postgresql://unused",
            connect=lambda dsn: _CompatibilityConnection((), ()),
        )
    assert caught.value.code == "runtime_schema_uninitialized"

    with pytest.raises(RuntimePersistenceError) as caught:
        check_postgres_runtime_compatibility(
            "postgresql://unused",
            connect=lambda dsn: _CompatibilityConnection(
                tuple(range(1, RUNTIME_POSTGRES_SCHEMA_VERSION + 1)),
                ("prometa_runtime_schema_migrations",),
            ),
        )
    assert caught.value.code == "runtime_schema_incompatible"


def test_state_validation_is_finite_and_bounded() -> None:
    state = PostgresRuntimeStateStore(
        "postgresql://unused",
        tenant_id="tenant-1",
        runtime_id="runtime-1",
        connect=_unavailable,
    )
    with pytest.raises(ValueError, match="finite JSON"):
        asyncio.run(state.save("request-1", {"score": float("nan")}))
    with pytest.raises(ValueError, match="1 MiB"):
        asyncio.run(state.save("request-1", {"payload": "x" * 1_048_576}))


def test_workflow_state_store_validates_before_connecting() -> None:
    store = PostgresWorkflowStateStore(
        "postgresql://secret:password@db.example/runtime",
        tenant_id="tenant-1",
        runtime_id="runtime-1",
        connect=_unavailable,
    )
    workflow = WorkflowExecutionContext(
        ontology_id="workflow-1",
        version=1,
        instance_id="instance-1",
        actor_ref="actor-opaque",
    )
    commit = WorkflowStateCommitRequest(
        request_id="request-1",
        workflow=workflow,
        expected_state="received",
        expected_version=0,
        next_state="extracted",
        transition_id="extract",
        ontology_digest="sha256:" + "a" * 64,
        policy_digest="sha256:" + "b" * 64,
        sector_snapshot_digest="sha256:" + "c" * 64,
        approval_references=(),
        evidence_references=("document-ref-1",),
        idempotency_key="opaque-idempotency-key",
    )
    with pytest.raises(RuntimePersistenceError) as caught:
        asyncio.run(store.compare_and_set(commit))
    assert caught.value.code == "workflow_state_store_unavailable"
    assert "password" not in str(caught.value)

    with pytest.raises(ValueError, match="workflow digests"):
        asyncio.run(
            store.compare_and_set(
                WorkflowStateCommitRequest(
                    **{**commit.__dict__, "policy_digest": "not-a-digest"}
                )
            )
        )
    with pytest.raises(ValueError, match="reason_code"):
        asyncio.run(
            store.mark_indeterminate(
                WorkflowIndeterminateRequest(
                    request_id="request-1",
                    workflow=workflow,
                    state="received",
                    state_version=0,
                    task_id="extract-invoice",
                    transition_id="extract",
                    reason_code="Not Machine Readable",
                    ontology_digest="sha256:" + "a" * 64,
                    policy_digest="sha256:" + "b" * 64,
                    sector_snapshot_digest="sha256:" + "c" * 64,
                )
            )
        )


def test_malformed_state_rows_fail_with_a_stable_code() -> None:
    state = PostgresRuntimeStateStore(
        "postgresql://unused",
        tenant_id="tenant-1",
        runtime_id="runtime-1",
        connect=lambda dsn: _StaticConnection(("not-json", 1, None)),
    )
    with pytest.raises(RuntimePersistenceError) as caught:
        asyncio.run(state.load("request-1"))
    assert caught.value.code == "state_record_invalid"


def test_malformed_release_cache_rows_fail_with_a_stable_code() -> None:
    cache = PostgresRuntimeReleaseCache(
        "postgresql://unused",
        tenant_id="tenant-1",
        connect=lambda dsn: _StaticConnection(
            (
                "artifact-1",
                "sha256:" + "a" * 64,
                "release-1",
                "deployment-1",
                "prod",
                "tenant-runtime",
                "not-json",
                {},
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
            )
        ),
    )
    with pytest.raises(RuntimePersistenceError) as caught:
        cache.load("attestation-1", max_age_seconds=60)
    assert caught.value.code == "release_cache_record_invalid"


def test_schema_init_cli_reads_named_environment_without_printing_dsn(
    monkeypatch, capsys
) -> None:
    dsn = "postgresql://secret:password@db.example/runtime"
    observed = []
    monkeypatch.setenv("CUSTOM_RUNTIME_DSN", dsn)
    monkeypatch.setattr(
        "prometa.runtime.postgres.install_postgres_runtime_schema",
        observed.append,
    )
    assert postgres_init_main(["--dsn-env", "CUSTOM_RUNTIME_DSN"]) == 0
    assert observed == [dsn]
    output = capsys.readouterr().out
    assert "schema is ready" in output
    assert "secret" not in output
    assert "password" not in output


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN"),
    reason="PROMETA_RUNTIME_TEST_POSTGRES_DSN is not configured",
)
def test_postgres_replay_and_state_are_shared_across_replicas() -> None:
    dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    install_postgres_runtime_schema(dsn)
    install_postgres_runtime_schema(dsn)
    tenant_id = "conformance-%s" % uuid.uuid4().hex
    runtime_id = "runtime-shared"

    def reserve(_):
        store = PostgresAdmissionReplayStore(
            dsn,
            tenant_id=tenant_id,
        )
        return store.reserve_pair("bundle-shared", "promotion-shared")

    with ThreadPoolExecutor(max_workers=12) as executor:
        outcomes = list(executor.map(reserve, range(24)))
    assert outcomes.count(True) == 1
    assert outcomes.count(False) == 23

    replay = PostgresAdmissionReplayStore(
        dsn,
        tenant_id=tenant_id,
    )
    assert replay.reserve_pair("bundle-new", "promotion-shared") is False
    assert replay.reserve_pair("bundle-shared", "promotion-new") is False
    isolated = PostgresAdmissionReplayStore(
        dsn,
        tenant_id=tenant_id + "-other",
    )
    assert isolated.reserve_pair("bundle-shared", "promotion-shared") is True

    release_cache = PostgresRuntimeReleaseCache(dsn, tenant_id=tenant_id)
    cached_handoff = _handoff()
    release_cache.save(cached_handoff)
    release_cache.save(cached_handoff)
    loaded_handoff = release_cache.load(
        cached_handoff.attestation_id,
        max_age_seconds=60,
    )
    assert loaded_handoff is not None
    assert loaded_handoff.artifact_digest == cached_handoff.artifact_digest
    assert loaded_handoff.bundle == cached_handoff.bundle
    assert (
        release_cache.load(
            cached_handoff.attestation_id,
            max_age_seconds=1,
            now=cached_handoff.fetched_at + timedelta(seconds=2),
        )
        is None
    )
    conflicting_handoff = RuntimeReleaseHandoff(
        **{
            **cached_handoff.__dict__,
            "artifact_digest": "sha256:" + "d" * 64,
            "bundle": {
                "signed": True,
                "artifactDigest": "sha256:" + "d" * 64,
            },
        }
    )
    with pytest.raises(RuntimePersistenceError) as caught:
        release_cache.save(conflicting_handoff)
    assert caught.value.code == "release_cache_conflict"

    activations = PostgresRuntimeActivationStore(dsn, tenant_id=tenant_id)
    activation_values = {
        "runtime_id": runtime_id,
        "deployment_id": "deployment-shared",
        "release_id": "release-shared",
        "artifact_digest": "sha256:" + "a" * 64,
        "bundle_jti": "bundle-activation",
        "promotion_jti": "promotion-activation",
    }

    def activate(_):
        store = PostgresRuntimeActivationStore(dsn, tenant_id=tenant_id)
        return store.activate_or_join(**activation_values).created

    with ThreadPoolExecutor(max_workers=12) as executor:
        activation_outcomes = list(executor.map(activate, range(24)))
    assert activation_outcomes.count(True) == 1
    assert activation_outcomes.count(False) == 23
    joined_activation = activations.activate_or_join(**activation_values)
    assert joined_activation.created is False
    assert joined_activation.activated_at is not None
    assert joined_activation.activated_at.tzinfo is not None
    assert (
        activations.activate_or_join(**activation_values).activated_at
        == joined_activation.activated_at
    )

    redeployed = activations.activate_or_join(
        **{
            **activation_values,
            "runtime_id": "runtime-redeploy",
            "deployment_id": "deployment-redeploy",
            "promotion_jti": "promotion-redeploy",
        }
    )
    assert redeployed.created is True

    with pytest.raises(RuntimePersistenceError) as caught:
        activations.activate_or_join(
            **{**activation_values, "release_id": "release-conflict"}
        )
    assert caught.value.code == "runtime_activation_conflict"

    receipt = build_runtime_receipt(
        attestation_id="attestation-shared",
        artifact_digest=activation_values["artifact_digest"],
        release_id=activation_values["release_id"],
        deployment_id=activation_values["deployment_id"],
        target_environment="prod",
        runtime_target="tenant-runtime",
        runtime_id=runtime_id,
        runtime_version="1",
        transition="admitted",
        outcome="accepted",
        receipt_id="receipt-shared",
        event_at=datetime.now(timezone.utc),
    )
    first_outbox = PostgresRuntimeReceiptOutbox(dsn, tenant_id=tenant_id)
    second_outbox = PostgresRuntimeReceiptOutbox(dsn, tenant_id=tenant_id)
    assert first_outbox.enqueue(receipt) is True
    assert second_outbox.enqueue(receipt) is False
    ordered_receipt = build_runtime_receipt(
        **{
            "attestation_id": "attestation-shared",
            "artifact_digest": activation_values["artifact_digest"],
            "release_id": activation_values["release_id"],
            "deployment_id": activation_values["deployment_id"],
            "target_environment": "prod",
            "runtime_target": "tenant-runtime",
            "runtime_id": runtime_id,
            "runtime_version": "1",
            "transition": "active",
            "outcome": "succeeded",
            "receipt_id": "receipt-ordered-after-admission",
            "event_at": datetime.now(timezone.utc),
        }
    )
    assert first_outbox.enqueue(ordered_receipt) is True
    first_lease = first_outbox.claim_next(30)
    assert first_lease is not None
    assert first_lease.receipt_id == "receipt-shared"
    assert first_lease.attempts == 1
    # A second replica cannot skip the leased admission and deliver the later
    # active transition out of order for the same deployment.
    assert second_outbox.claim_next(30) is None
    first_outbox.reschedule(
        first_lease,
        delay_seconds=0,
        error_code="transport",
    )
    second_lease = second_outbox.claim_next(30)
    assert second_lease is not None
    assert second_lease.attempts == 2
    assert second_lease.lease_token != first_lease.lease_token
    with pytest.raises(RuntimePersistenceError) as caught:
        first_outbox.mark_delivered(first_lease)
    assert caught.value.code == "receipt_outbox_lease_lost"
    second_outbox.mark_delivered(second_lease)
    ordered_lease = first_outbox.claim_next(30)
    assert ordered_lease is not None
    assert ordered_lease.receipt_id == "receipt-ordered-after-admission"
    first_outbox.mark_delivered(ordered_lease)
    assert first_outbox.claim_next(30) is None

    dead_letter = build_runtime_receipt(
        **{
            "attestation_id": "attestation-shared",
            "artifact_digest": activation_values["artifact_digest"],
            "release_id": activation_values["release_id"],
            "deployment_id": activation_values["deployment_id"],
            "target_environment": "prod",
            "runtime_target": "tenant-runtime",
            "runtime_id": runtime_id,
            "runtime_version": "1",
            "transition": "active",
            "outcome": "succeeded",
            "receipt_id": "receipt-dead-letter",
            "event_at": datetime.now(timezone.utc),
        }
    )
    assert first_outbox.enqueue(dead_letter) is True
    dead_letter_lease = first_outbox.claim_next(30)
    assert dead_letter_lease is not None
    first_outbox.mark_dead_letter(dead_letter_lease, error_code="http_403")
    assert second_outbox.claim_next(30) is None

    first_security_outbox = PostgresSecurityDecisionOutbox(dsn, tenant_id=tenant_id)
    second_security_outbox = PostgresSecurityDecisionOutbox(dsn, tenant_id=tenant_id)
    decision_one = _security_decision(decision_id="decision-postgres-1")
    decision_two = _security_decision(decision_id="decision-postgres-2")
    assert first_security_outbox.enqueue(decision_one) is True
    assert second_security_outbox.enqueue(decision_one) is False
    assert first_security_outbox.enqueue(decision_two) is True
    security_lease = first_security_outbox.claim_batch(30)
    assert security_lease is not None
    assert set(security_lease.decision_ids) == {
        "decision-postgres-1",
        "decision-postgres-2",
    }
    assert second_security_outbox.claim_batch(30) is None
    first_security_outbox.reschedule(
        security_lease,
        delay_seconds=0,
        error_code="transport",
    )
    security_retry = second_security_outbox.claim_batch(30)
    assert security_retry is not None
    assert security_retry.attempts == 2
    with pytest.raises(RuntimePersistenceError) as caught:
        first_security_outbox.mark_delivered(security_lease)
    assert caught.value.code == "security_decision_outbox_lease_lost"
    second_security_outbox.mark_delivered(security_retry)
    assert first_security_outbox.claim_batch(30) is None

    first_workflow_outbox = PostgresWorkflowDecisionOutbox(dsn, tenant_id=tenant_id)
    second_workflow_outbox = PostgresWorkflowDecisionOutbox(dsn, tenant_id=tenant_id)
    workflow_decision = _workflow_decision()
    assert first_workflow_outbox.enqueue(workflow_decision) is True
    assert second_workflow_outbox.enqueue(workflow_decision) is False
    workflow_lease = first_workflow_outbox.claim_batch(30)
    assert workflow_lease is not None
    assert workflow_lease.decision_ids == (workflow_decision["decisionId"],)
    assert second_workflow_outbox.claim_batch(30) is None
    first_workflow_outbox.reschedule(
        workflow_lease,
        delay_seconds=0,
        error_code="transport",
    )
    workflow_retry = second_workflow_outbox.claim_batch(30)
    assert workflow_retry is not None
    assert workflow_retry.attempts == 2
    with pytest.raises(RuntimePersistenceError) as caught:
        first_workflow_outbox.mark_delivered(workflow_lease)
    assert caught.value.code == "workflow_decision_outbox_lease_lost"
    second_workflow_outbox.mark_delivered(workflow_retry)
    assert first_workflow_outbox.claim_batch(30) is None

    with pytest.raises(RuntimePersistenceError) as caught:
        activations.activate_or_join(
            **{
                **activation_values,
                "runtime_id": "runtime-other",
                "deployment_id": "deployment-other",
                "promotion_jti": "promotion-digest-conflict",
                "artifact_digest": "sha256:" + "b" * 64,
            }
        )
    assert caught.value.code == "runtime_activation_conflict"
    with pytest.raises(RuntimePersistenceError) as caught:
        activations.activate_or_join(
            **{
                **activation_values,
                "runtime_id": "runtime-promotion-replay",
                "deployment_id": "deployment-promotion-replay",
            }
        )
    assert caught.value.code == "runtime_activation_conflict"

    first = PostgresRuntimeStateStore(
        dsn,
        tenant_id=tenant_id,
        runtime_id=runtime_id,
    )
    second = PostgresRuntimeStateStore(
        dsn,
        tenant_id=tenant_id,
        runtime_id=runtime_id,
    )

    async def state_scenario():
        await first.save("request-shared", {"status": "running"})
        initial = await second.load("request-shared")
        assert initial is not None
        assert initial.state == {"status": "running"}
        assert initial.version == 1

        await second.save("request-shared", {"status": "completed", "attempts": 1})
        completed = await first.load("request-shared")
        assert completed is not None
        assert completed.state == {"status": "completed", "attempts": 1}
        assert completed.version == 2

        other_tenant = PostgresRuntimeStateStore(
            dsn,
            tenant_id=tenant_id + "-other",
            runtime_id=runtime_id,
        )
        assert await other_tenant.load("request-shared") is None
        other_runtime = PostgresRuntimeStateStore(
            dsn,
            tenant_id=tenant_id,
            runtime_id=runtime_id + "-other",
        )
        assert await other_runtime.load("request-shared") is None
        assert await first.delete("request-shared") is True
        assert await first.delete("request-shared") is False
        assert await second.load("request-shared") is None

    asyncio.run(state_scenario())


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN"),
    reason="PROMETA_RUNTIME_TEST_POSTGRES_DSN is not configured",
)
def test_postgres_mcp_side_effects_are_replica_safe_and_tenant_isolated() -> None:
    dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    install_postgres_runtime_schema(dsn)
    tenant_id = "mcp-%s" % uuid.uuid4().hex
    runtime_id = "runtime-mcp-shared"
    request = _mcp_tool_request()

    server = McpServerConfig(
        name="Orders",
        connection_id="orders-prod",
        transport="streamable-http",
        endpoint="https://orders.example.test/mcp",
        environment="production",
        auth_mode="none",
        scopes=("orders.write",),
        risk_level="medium",
    )
    grant = McpToolGrant(
        tool_name="orders.write",
        agent_ids=("agent-postgres",),
        permission="write",
        risk_level="medium",
        server_connection_id="orders-prod",
    )

    class BlockingTransport:
        def __init__(self):
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.calls = 0

        async def call_tool(self, server, operation, arguments, credentials, metadata):
            self.calls += 1
            self.started.set()
            await self.release.wait()
            return {"recorded": True}

    def broker(tenant, transport, *, reservation_timeout_seconds=30):
        return GovernedMcpToolBroker(
            servers=(server,),
            grants=(grant,),
            policy=McpBrokerPolicy(max_risk_level="medium"),
            egress_policy=ExplicitMcpEgressPolicy(
                allowed_http_origins=frozenset({"https://orders.example.test"})
            ),
            transport_client=transport,
            audit_sink=PostgresMcpAuditSink(
                dsn,
                tenant_id=tenant,
                runtime_id=runtime_id,
            ),
            idempotency_store=PostgresMcpIdempotencyStore(
                dsn,
                tenant_id=tenant,
                runtime_id=runtime_id,
                reservation_timeout_seconds=reservation_timeout_seconds,
            ),
        )

    async def scenario():
        transport = BlockingTransport()
        first = broker(tenant_id, transport)
        second = broker(tenant_id, transport)
        owner = asyncio.create_task(first.invoke(request))
        await asyncio.wait_for(transport.started.wait(), timeout=2)
        with pytest.raises(RuntimeExecutionError) as caught:
            await second.invoke(request)
        assert caught.value.code == "mcp_tool_call_in_progress"
        assert transport.calls == 1
        transport.release.set()
        result = await owner
        assert result.output == {"recorded": True}

        with pytest.raises(RuntimeExecutionError) as caught:
            await second.invoke(request)
        assert caught.value.code == "mcp_duplicate_tool_call"
        assert transport.calls == 1

        isolated_transport = BlockingTransport()
        isolated_transport.release.set()
        isolated = broker(tenant_id + "-other", isolated_transport)
        isolated_result = await isolated.invoke(request)
        assert isolated_result.output == {"recorded": True}
        assert isolated_transport.calls == 1

        stale_key = "mcp1:" + "d" * 64
        stale_digest = "sha256:" + "e" * 64
        stale = PostgresMcpIdempotencyStore(
            dsn,
            tenant_id=tenant_id,
            runtime_id=runtime_id,
            reservation_timeout_seconds=0.05,
        )
        assert await stale.reserve(stale_key, stale_digest) == "acquired"
        await asyncio.sleep(0.1)
        replacement = PostgresMcpIdempotencyStore(
            dsn,
            tenant_id=tenant_id,
            runtime_id=runtime_id,
            reservation_timeout_seconds=0.05,
        )
        assert await replacement.reserve(stale_key, stale_digest) == "indeterminate"
        stale_record = await replacement.get(stale_key)
        assert stale_record is not None
        assert stale_record.status == "indeterminate"

        uncertain_request = _mcp_tool_request(
            request_id="request-uncertain",
            call_id="call-uncertain",
        )

        class UncertainTransport:
            async def call_tool(self, *args, **kwargs):
                raise McpTransportError("mcp_transport_failed", outcome_unknown=True)

        uncertain = broker(tenant_id, UncertainTransport())
        with pytest.raises(RuntimeExecutionError) as caught:
            await uncertain.invoke(uncertain_request)
        assert caught.value.code == "mcp_transport_failed"
        with pytest.raises(RuntimeExecutionError) as caught:
            await second.invoke(uncertain_request)
        assert caught.value.code == "mcp_tool_call_indeterminate"

    asyncio.run(scenario())

    import psycopg

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT event
                FROM prometa_runtime_mcp_audit
                WHERE tenant_id = %s AND runtime_id = %s
                ORDER BY occurred_at, event_id
                """,
                (tenant_id, runtime_id),
            )
            events = [row[0] for row in cursor.fetchall()]
    assert events
    assert {event["outcome"] for event in events}.issuperset(
        {"accepted", "completed", "denied", "failed"}
    )
    encoded_events = json.dumps(events, sort_keys=True)
    assert "order-postgres" not in encoded_events
    assert '"arguments"' not in encoded_events
    assert '"output"' not in encoded_events
    assert '"credentials"' not in encoded_events


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN"),
    reason="PROMETA_RUNTIME_TEST_POSTGRES_DSN is not configured",
)
def test_postgres_mcp_audit_is_append_only_and_payload_free() -> None:
    dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    install_postgres_runtime_schema(dsn)
    tenant_id = "mcp-audit-%s" % uuid.uuid4().hex
    sink = PostgresMcpAuditSink(
        dsn,
        tenant_id=tenant_id,
        runtime_id="runtime-mcp-audit",
    )
    event = _mcp_audit_event()
    asyncio.run(sink.record(event))
    asyncio.run(sink.record(event))

    import psycopg

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT event
                FROM prometa_runtime_mcp_audit
                WHERE tenant_id = %s AND runtime_id = %s
                """,
                (tenant_id, "runtime-mcp-audit"),
            )
            rows = cursor.fetchall()
    assert len(rows) == 1
    stored = rows[0][0]
    assert stored["argumentDigest"] == "sha256:" + "a" * 64
    assert stored["outputDigest"] == "sha256:" + "b" * 64
    assert "arguments" not in stored
    assert "output" not in stored
    assert "credentials" not in stored


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN"),
    reason="PROMETA_RUNTIME_TEST_POSTGRES_DSN is not configured",
)
def test_postgres_task_leases_recover_and_replay_ordered_history() -> None:
    dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    install_postgres_runtime_schema(dsn)
    tenant_id = "task-%s" % uuid.uuid4().hex
    runtime_id = "runtime-task-shared"
    now = datetime(2026, 7, 12, 9, 0, tzinfo=timezone.utc)
    input_digest = canonical_payload_digest({"question": "hello"})
    artifact_digest = "sha256:" + "a" * 64

    def claim(store, request_id="request-shared", **overrides):
        values = {
            "input_digest": input_digest,
            "artifact_digest": artifact_digest,
            "release_id": "release-task",
            "deployment_id": "deployment-task",
            "recoverable": True,
            "max_attempts": 3,
            "lease_seconds": 30,
            "now": now,
        }
        values.update(overrides)
        return store.claim(request_id, **values)

    isolated_tenant = PostgresRuntimeTaskStore(
        dsn,
        tenant_id=tenant_id + "-other",
        runtime_id=runtime_id,
    )
    isolated_runtime = PostgresRuntimeTaskStore(
        dsn,
        tenant_id=tenant_id,
        runtime_id=runtime_id + "-other",
    )
    assert isolated_tenant.get("request-shared") is None
    assert isolated_runtime.get("request-shared") is None

    def concurrent_claim(_):
        store = PostgresRuntimeTaskStore(
            dsn,
            tenant_id=tenant_id,
            runtime_id=runtime_id,
        )
        try:
            return claim(store)
        except RuntimeTaskError as exc:
            return exc.code

    with ThreadPoolExecutor(max_workers=12) as executor:
        outcomes = list(executor.map(concurrent_claim, range(24)))
    claims = [item for item in outcomes if isinstance(item, RuntimeTaskClaim)]
    assert len(claims) == 1
    assert outcomes.count("task_in_progress") == 23
    first = claims[0]

    store = PostgresRuntimeTaskStore(
        dsn,
        tenant_id=tenant_id,
        runtime_id=runtime_id,
    )
    with pytest.raises(RuntimeTaskError) as caught:
        claim(
            store,
            input_digest=canonical_payload_digest({"question": "changed"}),
        )
    assert caught.value.code == "task_identity_conflict"

    retry_event = store.fail(
        first,
        reason="gateway_unavailable",
        retryable=True,
        now=now + timedelta(seconds=1),
    )
    assert retry_event.transition == "retry_scheduled"
    second = claim(store, now=now + timedelta(seconds=2))
    assert second.transition == "retried"
    assert second.attempt == 2
    with pytest.raises(RuntimeTaskError) as caught:
        store.complete(
            first,
            output_digest=canonical_payload_digest({"answer": "stale"}),
            model_name="golden-model",
            model_attempts=1,
            tool_calls=0,
            used_fallback=False,
            now=now + timedelta(seconds=3),
        )
    assert caught.value.code == "task_lease_lost"
    completed = store.complete(
        second,
        output_digest=canonical_payload_digest({"answer": "done"}),
        model_name="golden-model",
        model_attempts=2,
        tool_calls=0,
        used_fallback=False,
        now=now + timedelta(seconds=3),
    )
    assert completed.status == "completed"
    snapshot = store.get("request-shared")
    assert snapshot is not None
    assert snapshot.record.status == "completed"
    assert snapshot.record.attempt == 2
    assert [event.transition for event in snapshot.events] == [
        "claimed",
        "retry_scheduled",
        "retried",
        "completed",
    ]
    with pytest.raises(RuntimeTaskError) as caught:
        claim(store)
    assert caught.value.code == "task_already_completed"

    orphan = claim(
        store,
        request_id="request-orphan",
        lease_seconds=10,
        now=now,
    )
    recovered = claim(
        store,
        request_id="request-orphan",
        lease_seconds=10,
        now=now + timedelta(seconds=11),
    )
    assert recovered.transition == "recovered"
    assert recovered.attempt == 2
    assert recovered.claim_token != orphan.claim_token
    orphan_snapshot = store.get("request-orphan")
    assert orphan_snapshot is not None
    assert [event.transition for event in orphan_snapshot.events] == [
        "claimed",
        "recovered",
    ]


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN"),
    reason="PROMETA_RUNTIME_TEST_POSTGRES_DSN is not configured",
)
def test_postgres_workflow_state_cas_allows_one_replica_and_quarantines() -> None:
    import psycopg

    dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    install_postgres_runtime_schema(dsn)
    tenant_id = "workflow-cas-%s" % uuid.uuid4().hex
    runtime_id = "runtime-workflow"
    workflow = WorkflowExecutionContext(
        ontology_id="invoice-to-sap",
        version=1,
        instance_id="invoice-instance",
        actor_ref="opaque-actor",
    )
    stores = (
        PostgresWorkflowStateStore(
            dsn,
            tenant_id=tenant_id,
            runtime_id=runtime_id,
        ),
        PostgresWorkflowStateStore(
            dsn,
            tenant_id=tenant_id,
            runtime_id=runtime_id,
        ),
    )

    def commit(index: int) -> bool:
        return asyncio.run(
            stores[index].compare_and_set(
                WorkflowStateCommitRequest(
                    request_id="request-%d" % index,
                    workflow=workflow,
                    expected_state="received",
                    expected_version=0,
                    next_state="extracted-%d" % index,
                    transition_id="extract-%d" % index,
                    ontology_digest="sha256:" + "a" * 64,
                    policy_digest="sha256:" + "b" * 64,
                    sector_snapshot_digest="sha256:" + "c" * 64,
                    approval_references=("approval-ref",),
                    evidence_references=("evidence-ref",),
                    idempotency_key="private-idempotency-key",
                )
            )
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = tuple(executor.map(commit, range(2)))
        assert sorted(results) == [False, True]
        winner = results.index(True)
        winner_state = "extracted-%d" % winner

        asyncio.run(
            stores[winner].mark_indeterminate(
                WorkflowIndeterminateRequest(
                    request_id="request-indeterminate",
                    workflow=workflow,
                    state=winner_state,
                    state_version=1,
                    task_id="post-to-sap",
                    transition_id="post",
                    reason_code="tool_outcome_unknown",
                    ontology_digest="sha256:" + "a" * 64,
                    policy_digest="sha256:" + "b" * 64,
                    sector_snapshot_digest="sha256:" + "c" * 64,
                )
            )
        )

        with psycopg.connect(dsn) as connection:
            instance = connection.execute(
                """
                SELECT state, state_version, quarantined
                FROM prometa_runtime_workflow_instance
                WHERE tenant_id = %s AND runtime_id = %s
                  AND workflow_id = %s AND workflow_version = %s
                  AND instance_id = %s
                """,
                (
                    tenant_id,
                    runtime_id,
                    workflow.ontology_id,
                    workflow.version,
                    workflow.instance_id,
                ),
            ).fetchone()
            ledger = connection.execute(
                """
                SELECT outcome, idempotency_key_digest
                FROM prometa_runtime_workflow_ledger
                WHERE tenant_id = %s AND runtime_id = %s
                ORDER BY created_at, event_id
                """,
                (tenant_id, runtime_id),
            ).fetchall()
        assert instance == (winner_state, 1, True)
        assert sorted(row[0] for row in ledger) == [
            "committed",
            "indeterminate",
        ]
        committed_digest = next(row[1] for row in ledger if row[0] == "committed")
        assert committed_digest is not None
        assert "private-idempotency-key" not in repr(ledger)
    finally:
        with psycopg.connect(dsn) as connection:
            connection.execute(
                """
                DELETE FROM prometa_runtime_workflow_ledger
                WHERE tenant_id = %s AND runtime_id = %s
                """,
                (tenant_id, runtime_id),
            )
            connection.execute(
                """
                DELETE FROM prometa_runtime_workflow_instance
                WHERE tenant_id = %s AND runtime_id = %s
                """,
                (tenant_id, runtime_id),
            )


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN"),
    reason="PROMETA_RUNTIME_TEST_POSTGRES_DSN is not configured",
)
def test_postgres_restore_integrity_verifier_is_payload_free_and_fail_closed(
    monkeypatch, capsys
) -> None:
    import psycopg

    dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    install_postgres_runtime_schema(dsn)
    tenant_id = "verify-%s" % uuid.uuid4().hex
    runtime_id = "runtime-verify"
    request_id = "request-verify"
    store = PostgresRuntimeTaskStore(
        dsn,
        tenant_id=tenant_id,
        runtime_id=runtime_id,
    )
    claim = store.claim(
        request_id,
        input_digest=canonical_payload_digest({"question": "private input"}),
        artifact_digest="sha256:" + "d" * 64,
        release_id="release-verify",
        deployment_id="deployment-verify",
        recoverable=True,
        max_attempts=3,
        lease_seconds=30,
    )
    store.complete(
        claim,
        output_digest=canonical_payload_digest({"answer": "private output"}),
        model_name="tenant/model",
        model_attempts=1,
        tool_calls=0,
        used_fallback=False,
    )

    try:
        report = verify_postgres_runtime_integrity(dsn)
        assert report.schema_version == RUNTIME_POSTGRES_SCHEMA_VERSION
        assert report.migration_versions == tuple(
            range(1, RUNTIME_POSTGRES_SCHEMA_VERSION + 1)
        )
        assert report.table_counts["prometa_runtime_task"] >= 1
        assert report.table_counts["prometa_runtime_task_event"] >= 2
        assert "private input" not in repr(report)
        assert "private output" not in repr(report)

        monkeypatch.setenv("RUNTIME_VERIFY_DSN", dsn)
        assert postgres_verify_main(["--dsn-env", "RUNTIME_VERIFY_DSN"]) == 0
        output = json.loads(capsys.readouterr().out)
        assert output["integrity"] == "verified"
        assert output["schemaVersion"] == RUNTIME_POSTGRES_SCHEMA_VERSION
        assert "private" not in repr(output)

        with psycopg.connect(dsn) as connection:
            connection.execute(
                """
                UPDATE prometa_runtime_task
                SET sequence = sequence + 1
                WHERE tenant_id = %s AND runtime_id = %s AND request_id = %s
                """,
                (tenant_id, runtime_id, request_id),
            )
        with pytest.raises(RuntimePersistenceError) as caught:
            verify_postgres_runtime_integrity(dsn)
        assert caught.value.code == "runtime_schema_integrity_failed"
    finally:
        with psycopg.connect(dsn) as connection:
            connection.execute(
                """
                DELETE FROM prometa_runtime_task
                WHERE tenant_id = %s AND runtime_id = %s AND request_id = %s
                """,
                (tenant_id, runtime_id, request_id),
            )


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN"),
    reason="PROMETA_RUNTIME_TEST_POSTGRES_DSN is not configured",
)
def test_postgres_compatibility_contract_is_payload_free_and_fail_closed(
    monkeypatch, capsys
) -> None:
    import psycopg

    dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    install_postgres_runtime_schema(dsn)
    report = check_postgres_runtime_compatibility(dsn)
    assert report.schema_version == RUNTIME_POSTGRES_SCHEMA_VERSION
    assert report.migration_versions == tuple(
        range(1, RUNTIME_POSTGRES_SCHEMA_VERSION + 1)
    )
    assert report.minimum_schema_version == RUNTIME_POSTGRES_MIN_SCHEMA_VERSION
    assert report.maximum_schema_version == RUNTIME_POSTGRES_MAX_SCHEMA_VERSION
    assert report.as_dict()["compatibilityVersion"] == (
        RUNTIME_POSTGRES_COMPATIBILITY_VERSION
    )

    monkeypatch.setenv("RUNTIME_COMPATIBILITY_DSN", dsn)
    assert postgres_compatibility_main(["--dsn-env", "RUNTIME_COMPATIBILITY_DSN"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output == {
        "compatibility": "compatible",
        "compatibilityVersion": 1,
        "maximumSchemaVersion": RUNTIME_POSTGRES_MAX_SCHEMA_VERSION,
        "migrationVersions": list(range(1, RUNTIME_POSTGRES_SCHEMA_VERSION + 1)),
        "minimumSchemaVersion": RUNTIME_POSTGRES_MIN_SCHEMA_VERSION,
        "schemaVersion": RUNTIME_POSTGRES_SCHEMA_VERSION,
    }

    def expect_code(code: str) -> None:
        with pytest.raises(RuntimePersistenceError) as caught:
            check_postgres_runtime_compatibility(dsn)
        assert caught.value.code == code

    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(
                "INSERT INTO prometa_runtime_schema_migrations (version) VALUES (8)"
            )
        expect_code("runtime_schema_too_new")
    finally:
        with psycopg.connect(dsn) as connection:
            connection.execute(
                "DELETE FROM prometa_runtime_schema_migrations WHERE version = 8"
            )

    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(
                "DELETE FROM prometa_runtime_schema_migrations WHERE version = 7"
            )
        expect_code("runtime_schema_too_old")
    finally:
        with psycopg.connect(dsn) as connection:
            connection.execute(
                "INSERT INTO prometa_runtime_schema_migrations (version) VALUES (7)"
            )

    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(
                "DELETE FROM prometa_runtime_schema_migrations WHERE version = 3"
            )
        expect_code("runtime_schema_migration_gap")
    finally:
        with psycopg.connect(dsn) as connection:
            connection.execute(
                "INSERT INTO prometa_runtime_schema_migrations (version) VALUES (3)"
            )
