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
    RUNTIME_POSTGRES_SCHEMA_VERSION,
    PostgresAdmissionReplayStore,
    PostgresRuntimeActivationStore,
    PostgresRuntimeReceiptOutbox,
    PostgresRuntimeReleaseCache,
    PostgresRuntimeStateStore,
    PostgresRuntimeTaskStore,
    RuntimeReleaseHandoff,
    RuntimePersistenceError,
    RuntimeTaskClaim,
    RuntimeTaskError,
    build_runtime_receipt,
    canonical_payload_digest,
    install_postgres_runtime_schema,
    verify_postgres_runtime_integrity,
)
from prometa.runtime.postgres import (
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
        verify_postgres_runtime_integrity(
            "postgresql://secret:password@db.example/runtime",
            connect=_unavailable,
        )
    assert caught.value.code == "runtime_schema_verification_failed"
    assert "password" not in str(caught.value)


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
    first_lease = first_outbox.claim_next(30)
    assert first_lease is not None
    assert first_lease.attempts == 1
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
        assert report.migration_versions == (1, 2, 3, 4, 5)
        assert report.table_counts["prometa_runtime_task"] >= 1
        assert report.table_counts["prometa_runtime_task_event"] >= 2
        assert "private input" not in repr(report)
        assert "private output" not in repr(report)

        monkeypatch.setenv("RUNTIME_VERIFY_DSN", dsn)
        assert postgres_verify_main(["--dsn-env", "RUNTIME_VERIFY_DSN"]) == 0
        output = json.loads(capsys.readouterr().out)
        assert output["integrity"] == "verified"
        assert output["schemaVersion"] == 5
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
