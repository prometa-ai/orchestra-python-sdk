"""PostgreSQL durability tests for the optional tenant runtime."""

from __future__ import annotations

import asyncio
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest

from prometa.runtime import (
    PostgresAdmissionReplayStore,
    PostgresRuntimeActivationStore,
    PostgresRuntimeReceiptOutbox,
    PostgresRuntimeStateStore,
    RuntimePersistenceError,
    build_runtime_receipt,
    install_postgres_runtime_schema,
)
from prometa.runtime.postgres import main as postgres_init_main


def _unavailable(dsn):
    raise OSError("database unavailable at %s" % dsn)


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
