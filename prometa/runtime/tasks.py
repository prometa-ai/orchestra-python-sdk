"""Payload-free durable task lifecycle contracts for tenant runtimes.

The contract coordinates client-driven retries across runtime replicas without
persisting request or response bodies. A caller must replay the same request ID
and semantically identical finite JSON input after a retryable failure or
expired lease. Model calls remain at-least-once; exactly-once inference and
automatic background replay are explicitly outside this contract.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Protocol, Tuple


_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_TASK_STATUSES = frozenset(
    {"running", "retryable", "completed", "failed", "blocked"}
)
_MAX_DIGEST_PAYLOAD_BYTES = 16 * 1024 * 1024
RUNTIME_TASK_LIFECYCLE_VERSION = 1


class RuntimeTaskError(RuntimeError):
    """Stable task lifecycle failure safe to expose as an error code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code.replace("_", " "))


@dataclass(frozen=True)
class RuntimeTaskClaim:
    """Exclusive, bounded ownership of one task attempt."""

    request_id: str
    claim_token: str
    attempt: int
    sequence: int
    transition: str
    lease_expires_at: datetime


@dataclass(frozen=True)
class RuntimeTaskEvent:
    """One ordered, payload-free task lifecycle transition."""

    sequence: int
    transition: str
    status: str
    attempt: int
    occurred_at: datetime
    reason: Optional[str] = None


@dataclass(frozen=True)
class RuntimeTaskRecord:
    """Current payload-free task projection."""

    request_id: str
    artifact_digest: str
    release_id: str
    deployment_id: str
    status: str
    attempt: int
    max_attempts: int
    recoverable: bool
    sequence: int
    lease_expires_at: Optional[datetime]
    last_error_code: Optional[str]
    output_digest: Optional[str]
    model_name: Optional[str]
    model_attempts: Optional[int]
    tool_calls: Optional[int]
    used_fallback: Optional[bool]
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]


@dataclass(frozen=True)
class RuntimeTaskSnapshot:
    """Current task projection plus a bounded ordered event replay."""

    record: RuntimeTaskRecord
    events: Tuple[RuntimeTaskEvent, ...]
    history_truncated: bool


class RuntimeTaskStore(Protocol):
    def claim(
        self,
        request_id: str,
        *,
        input_digest: str,
        artifact_digest: str,
        release_id: str,
        deployment_id: str,
        recoverable: bool,
        max_attempts: int,
        lease_seconds: float,
        now: Optional[datetime] = None,
    ) -> RuntimeTaskClaim:
        """Create, retry, or recover one exclusive task attempt."""

    def complete(
        self,
        claim: RuntimeTaskClaim,
        *,
        output_digest: str,
        model_name: str,
        model_attempts: int,
        tool_calls: int,
        used_fallback: bool,
        now: Optional[datetime] = None,
    ) -> RuntimeTaskEvent:
        """Commit terminal success while the caller still owns the lease."""

    def fail(
        self,
        claim: RuntimeTaskClaim,
        *,
        reason: str,
        retryable: bool,
        now: Optional[datetime] = None,
    ) -> RuntimeTaskEvent:
        """Commit retryable or terminal failure for the owned attempt."""

    def get(
        self, request_id: str, *, history_limit: int = 50
    ) -> Optional[RuntimeTaskSnapshot]:
        """Read one payload-free task projection and ordered event history."""


def _identifier(name: str, value: Any, maximum: int = 256) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(
            "%s must be a trimmed string of 1-%d characters" % (name, maximum)
        )
    return value


def _digest(name: str, value: Any) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ValueError("%s must be sha256:<hex>" % name)
    return value


def _error_code(value: Any) -> str:
    if not isinstance(value, str) or _ERROR_CODE.fullmatch(value) is None:
        raise ValueError("reason must be a stable snake_case error code")
    return value


def _instant(value: Optional[datetime]) -> datetime:
    current = value or datetime.now(timezone.utc)
    if not isinstance(current, datetime) or current.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    return current.astimezone(timezone.utc)


def _claim_policy(max_attempts: Any, lease_seconds: Any) -> Tuple[int, float]:
    if type(max_attempts) is not int or not 1 <= max_attempts <= 20:
        raise ValueError("max_attempts must be between 1 and 20")
    if (
        isinstance(lease_seconds, bool)
        or not isinstance(lease_seconds, (int, float))
        or lease_seconds <= 0
        or lease_seconds > 3600
    ):
        raise ValueError("lease_seconds must be greater than 0 and at most 3600")
    return max_attempts, float(lease_seconds)


def _history_limit(value: Any) -> int:
    if type(value) is not int or not 1 <= value <= 100:
        raise ValueError("history_limit must be between 1 and 100")
    return value


def canonical_payload_digest(value: Any) -> str:
    """Return a bounded deterministic digest without retaining the payload."""

    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RuntimeTaskError("task_payload_not_json") from exc
    if len(encoded) > _MAX_DIGEST_PAYLOAD_BYTES:
        raise RuntimeTaskError("task_payload_too_large")
    return "sha256:%s" % hashlib.sha256(encoded).hexdigest()


@dataclass
class _InMemoryTask:
    request_id: str
    input_digest: str
    artifact_digest: str
    release_id: str
    deployment_id: str
    recoverable: bool
    max_attempts: int
    status: str
    attempt: int
    sequence: int
    claim_token: Optional[str]
    lease_expires_at: Optional[datetime]
    last_error_code: Optional[str]
    output_digest: Optional[str]
    model_name: Optional[str]
    model_attempts: Optional[int]
    tool_calls: Optional[int]
    used_fallback: Optional[bool]
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]


class InMemoryRuntimeTaskStore:
    """Thread-safe contract reference for tests and single-process hosts."""

    def __init__(self) -> None:
        self._tasks: Dict[str, _InMemoryTask] = {}
        self._events: Dict[str, list[RuntimeTaskEvent]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _event(
        task: _InMemoryTask,
        transition: str,
        occurred_at: datetime,
        reason: Optional[str] = None,
    ) -> RuntimeTaskEvent:
        return RuntimeTaskEvent(
            sequence=task.sequence,
            transition=transition,
            status=task.status,
            attempt=task.attempt,
            occurred_at=occurred_at,
            reason=reason,
        )

    @staticmethod
    def _record(task: _InMemoryTask) -> RuntimeTaskRecord:
        if task.status not in _TASK_STATUSES:
            raise RuntimeTaskError("task_record_invalid")
        return RuntimeTaskRecord(
            request_id=task.request_id,
            artifact_digest=task.artifact_digest,
            release_id=task.release_id,
            deployment_id=task.deployment_id,
            status=task.status,
            attempt=task.attempt,
            max_attempts=task.max_attempts,
            recoverable=task.recoverable,
            sequence=task.sequence,
            lease_expires_at=task.lease_expires_at,
            last_error_code=task.last_error_code,
            output_digest=task.output_digest,
            model_name=task.model_name,
            model_attempts=task.model_attempts,
            tool_calls=task.tool_calls,
            used_fallback=task.used_fallback,
            created_at=task.created_at,
            updated_at=task.updated_at,
            completed_at=task.completed_at,
        )

    def claim(
        self,
        request_id: str,
        *,
        input_digest: str,
        artifact_digest: str,
        release_id: str,
        deployment_id: str,
        recoverable: bool,
        max_attempts: int,
        lease_seconds: float,
        now: Optional[datetime] = None,
    ) -> RuntimeTaskClaim:
        request = _identifier("request_id", request_id)
        input_value = _digest("input_digest", input_digest)
        artifact = _digest("artifact_digest", artifact_digest)
        release = _identifier("release_id", release_id, 200)
        deployment = _identifier("deployment_id", deployment_id, 200)
        if type(recoverable) is not bool:
            raise ValueError("recoverable must be a boolean")
        attempts, lease = _claim_policy(max_attempts, lease_seconds)
        current = _instant(now)
        expires = current + timedelta(seconds=lease)
        token = uuid.uuid4().hex
        exhausted = False
        with self._lock:
            task = self._tasks.get(request)
            if task is None:
                task = _InMemoryTask(
                    request_id=request,
                    input_digest=input_value,
                    artifact_digest=artifact,
                    release_id=release,
                    deployment_id=deployment,
                    recoverable=recoverable,
                    max_attempts=attempts,
                    status="running",
                    attempt=1,
                    sequence=1,
                    claim_token=token,
                    lease_expires_at=expires,
                    last_error_code=None,
                    output_digest=None,
                    model_name=None,
                    model_attempts=None,
                    tool_calls=None,
                    used_fallback=None,
                    created_at=current,
                    updated_at=current,
                    completed_at=None,
                )
                self._tasks[request] = task
                event = self._event(task, "claimed", current)
                self._events[request] = [event]
                transition = event.transition
            else:
                identity = (
                    task.input_digest,
                    task.artifact_digest,
                    task.release_id,
                    task.deployment_id,
                    task.recoverable,
                    task.max_attempts,
                )
                expected = (
                    input_value,
                    artifact,
                    release,
                    deployment,
                    recoverable,
                    attempts,
                )
                if identity != expected:
                    raise RuntimeTaskError("task_identity_conflict")
                if task.status == "running" and task.lease_expires_at > current:
                    raise RuntimeTaskError("task_in_progress")
                if task.status == "completed":
                    raise RuntimeTaskError("task_already_completed")
                if task.status == "failed":
                    raise RuntimeTaskError("task_terminal")
                if task.status == "blocked":
                    raise RuntimeTaskError("task_recovery_blocked")
                if task.status not in {"running", "retryable"}:
                    raise RuntimeTaskError("task_record_invalid")
                if not task.recoverable:
                    task.status = "blocked"
                    task.sequence += 1
                    task.claim_token = None
                    task.lease_expires_at = None
                    task.last_error_code = "task_recovery_blocked"
                    task.updated_at = current
                    event = self._event(
                        task,
                        "recovery_blocked",
                        current,
                        "task_recovery_blocked",
                    )
                    self._events[request].append(event)
                    raise RuntimeTaskError("task_recovery_blocked")
                if task.attempt >= task.max_attempts:
                    task.status = "failed"
                    task.sequence += 1
                    task.claim_token = None
                    task.lease_expires_at = None
                    task.last_error_code = "task_attempts_exhausted"
                    task.updated_at = current
                    task.completed_at = current
                    event = self._event(
                        task,
                        "attempts_exhausted",
                        current,
                        "task_attempts_exhausted",
                    )
                    self._events[request].append(event)
                    exhausted = True
                    transition = event.transition
                else:
                    transition = (
                        "recovered" if task.status == "running" else "retried"
                    )
                    task.status = "running"
                    task.attempt += 1
                    task.sequence += 1
                    task.claim_token = token
                    task.lease_expires_at = expires
                    task.last_error_code = None
                    task.updated_at = current
                    event = self._event(task, transition, current)
                    self._events[request].append(event)
            claim = RuntimeTaskClaim(
                request_id=request,
                claim_token=token,
                attempt=task.attempt,
                sequence=task.sequence,
                transition=transition,
                lease_expires_at=expires,
            )
        if exhausted:
            raise RuntimeTaskError("task_attempts_exhausted")
        return claim

    def _owned(
        self, claim: RuntimeTaskClaim, current: datetime
    ) -> _InMemoryTask:
        if not isinstance(claim, RuntimeTaskClaim):
            raise ValueError("claim must be a RuntimeTaskClaim")
        task = self._tasks.get(claim.request_id)
        if (
            task is None
            or task.status != "running"
            or task.claim_token != claim.claim_token
            or task.attempt != claim.attempt
            or task.lease_expires_at is None
            or task.lease_expires_at <= current
        ):
            raise RuntimeTaskError("task_lease_lost")
        return task

    def complete(
        self,
        claim: RuntimeTaskClaim,
        *,
        output_digest: str,
        model_name: str,
        model_attempts: int,
        tool_calls: int,
        used_fallback: bool,
        now: Optional[datetime] = None,
    ) -> RuntimeTaskEvent:
        output = _digest("output_digest", output_digest)
        model = _identifier("model_name", model_name, 256)
        if type(model_attempts) is not int or model_attempts < 1:
            raise ValueError("model_attempts must be a positive integer")
        if type(tool_calls) is not int or tool_calls < 0:
            raise ValueError("tool_calls must be a non-negative integer")
        if type(used_fallback) is not bool:
            raise ValueError("used_fallback must be a boolean")
        current = _instant(now)
        with self._lock:
            task = self._owned(claim, current)
            task.status = "completed"
            task.sequence += 1
            task.claim_token = None
            task.lease_expires_at = None
            task.output_digest = output
            task.model_name = model
            task.model_attempts = model_attempts
            task.tool_calls = tool_calls
            task.used_fallback = used_fallback
            task.updated_at = current
            task.completed_at = current
            event = self._event(task, "completed", current)
            self._events[claim.request_id].append(event)
            return event

    def fail(
        self,
        claim: RuntimeTaskClaim,
        *,
        reason: str,
        retryable: bool,
        now: Optional[datetime] = None,
    ) -> RuntimeTaskEvent:
        error = _error_code(reason)
        if type(retryable) is not bool:
            raise ValueError("retryable must be a boolean")
        current = _instant(now)
        with self._lock:
            task = self._owned(claim, current)
            can_retry = retryable and task.recoverable and task.attempt < task.max_attempts
            task.status = "retryable" if can_retry else "failed"
            task.sequence += 1
            task.claim_token = None
            task.lease_expires_at = None
            task.last_error_code = error
            task.updated_at = current
            task.completed_at = None if can_retry else current
            transition = "retry_scheduled" if can_retry else "failed"
            event = self._event(task, transition, current, error)
            self._events[claim.request_id].append(event)
            return event

    def get(
        self, request_id: str, *, history_limit: int = 50
    ) -> Optional[RuntimeTaskSnapshot]:
        request = _identifier("request_id", request_id)
        limit = _history_limit(history_limit)
        with self._lock:
            task = self._tasks.get(request)
            if task is None:
                return None
            events = self._events[request]
            selected = tuple(events[-limit:])
            return RuntimeTaskSnapshot(
                record=self._record(task),
                events=selected,
                history_truncated=len(events) > len(selected),
            )


__all__ = [
    "RUNTIME_TASK_LIFECYCLE_VERSION",
    "RuntimeTaskError",
    "RuntimeTaskClaim",
    "RuntimeTaskEvent",
    "RuntimeTaskRecord",
    "RuntimeTaskSnapshot",
    "RuntimeTaskStore",
    "InMemoryRuntimeTaskStore",
    "canonical_payload_digest",
]
