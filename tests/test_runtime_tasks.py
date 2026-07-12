"""Payload-free runtime task lifecycle contract tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from prometa.runtime.tasks import (
    InMemoryRuntimeTaskStore,
    RuntimeTaskError,
    canonical_payload_digest,
)


NOW = datetime(2026, 7, 12, 9, 0, tzinfo=timezone.utc)
ARTIFACT = "sha256:" + "a" * 64


def _claim(store, request_id="request-1", **overrides):
    values = {
        "input_digest": canonical_payload_digest({"question": "hello"}),
        "artifact_digest": ARTIFACT,
        "release_id": "release-1",
        "deployment_id": "deployment-1",
        "recoverable": True,
        "max_attempts": 3,
        "lease_seconds": 30,
        "now": NOW,
    }
    values.update(overrides)
    return store.claim(request_id, **values)


def _assert_code(code, operation):
    with pytest.raises(RuntimeTaskError) as caught:
        operation()
    assert caught.value.code == code


def test_payload_digest_is_canonical_bounded_and_finite() -> None:
    assert canonical_payload_digest({"b": 2, "a": 1}) == canonical_payload_digest(
        {"a": 1, "b": 2}
    )
    _assert_code("task_payload_not_json", lambda: canonical_payload_digest({1, 2}))
    _assert_code(
        "task_payload_not_json",
        lambda: canonical_payload_digest({"score": float("nan")}),
    )
    _assert_code(
        "task_payload_too_large",
        lambda: canonical_payload_digest("x" * (16 * 1024 * 1024)),
    )


def test_task_retry_completion_and_lifecycle_replay() -> None:
    store = InMemoryRuntimeTaskStore()
    first = _claim(store)
    assert first.transition == "claimed"
    assert first.attempt == 1

    _assert_code("task_in_progress", lambda: _claim(store, now=NOW + timedelta(seconds=1)))
    _assert_code(
        "task_identity_conflict",
        lambda: _claim(
            store,
            input_digest=canonical_payload_digest({"question": "changed"}),
        ),
    )

    retry = store.fail(
        first,
        reason="gateway_unavailable",
        retryable=True,
        now=NOW + timedelta(seconds=2),
    )
    assert retry.transition == "retry_scheduled"
    assert retry.status == "retryable"

    second = _claim(store, now=NOW + timedelta(seconds=3))
    assert second.transition == "retried"
    assert second.attempt == 2
    completed = store.complete(
        second,
        output_digest=canonical_payload_digest({"answer": "done"}),
        model_name="tenant/golden-model",
        model_attempts=2,
        tool_calls=0,
        used_fallback=False,
        now=NOW + timedelta(seconds=4),
    )
    assert completed.status == "completed"

    snapshot = store.get("request-1")
    assert snapshot is not None
    assert snapshot.record.status == "completed"
    assert snapshot.record.attempt == 2
    assert snapshot.record.model_name == "tenant/golden-model"
    assert snapshot.record.output_digest == canonical_payload_digest(
        {"answer": "done"}
    )
    assert [event.transition for event in snapshot.events] == [
        "claimed",
        "retry_scheduled",
        "retried",
        "completed",
    ]
    assert snapshot.history_truncated is False
    _assert_code("task_already_completed", lambda: _claim(store))


def test_expired_claim_recovery_is_bounded_and_lease_owned() -> None:
    store = InMemoryRuntimeTaskStore()
    first = _claim(store, max_attempts=2, lease_seconds=10)
    recovered = _claim(
        store,
        max_attempts=2,
        lease_seconds=10,
        now=NOW + timedelta(seconds=11),
    )
    assert recovered.transition == "recovered"
    assert recovered.attempt == 2
    _assert_code(
        "task_lease_lost",
        lambda: store.fail(
            first,
            reason="model_timeout",
            retryable=True,
            now=NOW + timedelta(seconds=12),
        ),
    )
    store.fail(
        recovered,
        reason="model_timeout",
        retryable=True,
        now=NOW + timedelta(seconds=12),
    )
    snapshot = store.get("request-1")
    assert snapshot is not None
    assert snapshot.record.status == "failed"
    assert snapshot.events[-1].transition == "failed"
    _assert_code("task_terminal", lambda: _claim(store, max_attempts=2))


def test_exhausted_and_nonrecoverable_tasks_fail_closed() -> None:
    exhausted = InMemoryRuntimeTaskStore()
    _claim(exhausted, max_attempts=1, lease_seconds=10)
    _assert_code(
        "task_attempts_exhausted",
        lambda: _claim(
            exhausted,
            max_attempts=1,
            lease_seconds=10,
            now=NOW + timedelta(seconds=11),
        ),
    )
    exhausted_snapshot = exhausted.get("request-1")
    assert exhausted_snapshot is not None
    assert exhausted_snapshot.record.status == "failed"
    assert exhausted_snapshot.events[-1].transition == "attempts_exhausted"

    blocked = InMemoryRuntimeTaskStore()
    _claim(blocked, recoverable=False, lease_seconds=10)
    _assert_code(
        "task_recovery_blocked",
        lambda: _claim(
            blocked,
            recoverable=False,
            lease_seconds=10,
            now=NOW + timedelta(seconds=11),
        ),
    )
    blocked_snapshot = blocked.get("request-1")
    assert blocked_snapshot is not None
    assert blocked_snapshot.record.status == "blocked"
    assert blocked_snapshot.events[-1].reason == "task_recovery_blocked"


def test_task_history_and_input_validation_are_bounded() -> None:
    store = InMemoryRuntimeTaskStore()
    first = _claim(store)
    store.fail(first, reason="model_timeout", retryable=True, now=NOW)
    second = _claim(store, now=NOW)
    store.fail(second, reason="model_timeout", retryable=True, now=NOW)
    snapshot = store.get("request-1", history_limit=2)
    assert snapshot is not None
    assert snapshot.history_truncated is True
    assert [event.sequence for event in snapshot.events] == [3, 4]
    assert store.get("missing") is None

    with pytest.raises(ValueError, match="history_limit"):
        store.get("request-1", history_limit=0)
    with pytest.raises(ValueError, match="max_attempts"):
        _claim(store, request_id="other", max_attempts=0)
    with pytest.raises(ValueError, match="timezone-aware"):
        _claim(store, request_id="other", now=datetime(2026, 7, 12))
