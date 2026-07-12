"""PostgreSQL durability adapters for multi-replica tenant runtimes.

The adapters are optional and never contact the Orchestra control plane. They
scope replay and request state by tenant/runtime identity and require operators
to install the fixed schema explicitly before serving traffic.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple

from .admission import RuntimeActivationResult
from .control_plane import RuntimeReleaseHandoff
from .receipts import RuntimeReceiptOutboxItem
from .tasks import (
    RuntimeTaskClaim,
    RuntimeTaskError,
    RuntimeTaskEvent,
    RuntimeTaskRecord,
    RuntimeTaskSnapshot,
    _TASK_STATUSES,
    _claim_policy as _task_claim_policy,
    _digest as _task_digest,
    _error_code as _task_error_code,
    _history_limit as _task_history_limit,
    _identifier as _task_identifier,
    _instant as _task_instant,
)


_MAX_IDENTIFIER_LENGTH = 128
_MAX_STATE_BYTES = 1_048_576
_MAX_RECEIPT_BYTES = 64 * 1024
_MAX_RELEASE_DOCUMENT_BYTES = 12 * 1024 * 1024
_SHA256_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_RUNTIME_ENVIRONMENTS = frozenset({"dev", "test", "staging", "prod"})
_RECEIPT_SEQUENCE = {
    "admitted": 10,
    "rollout_started": 20,
    "active": 30,
    "paused": 40,
    "rollback_started": 50,
    "rolled_back": 60,
    "failed": 70,
    "stopped": 80,
}
RUNTIME_POSTGRES_SCHEMA_VERSION = 5
RUNTIME_POSTGRES_COMPATIBILITY_VERSION = 1
RUNTIME_POSTGRES_MIN_SCHEMA_VERSION = 5
RUNTIME_POSTGRES_MAX_SCHEMA_VERSION = RUNTIME_POSTGRES_SCHEMA_VERSION
_RUNTIME_TABLES = (
    "prometa_runtime_schema_migrations",
    "prometa_runtime_admission_replay",
    "prometa_runtime_request_state",
    "prometa_runtime_release_activation",
    "prometa_runtime_bundle_identity",
    "prometa_runtime_receipt_outbox",
    "prometa_runtime_release_cache",
    "prometa_runtime_task",
    "prometa_runtime_task_event",
)
_TASK_TABLE_COLUMNS = {
    "prometa_runtime_task": frozenset(
        {
            "tenant_id",
            "runtime_id",
            "request_id",
            "input_digest",
            "artifact_digest",
            "release_id",
            "deployment_id",
            "recoverable",
            "max_attempts",
            "status",
            "attempt",
            "sequence",
            "claim_token",
            "lease_expires_at",
            "last_error_code",
            "output_digest",
            "model_name",
            "model_attempts",
            "tool_calls",
            "used_fallback",
            "created_at",
            "updated_at",
            "completed_at",
        }
    ),
    "prometa_runtime_task_event": frozenset(
        {
            "tenant_id",
            "runtime_id",
            "request_id",
            "sequence",
            "transition",
            "status",
            "attempt",
            "reason",
            "occurred_at",
        }
    ),
}

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS prometa_runtime_schema_migrations (
    version INTEGER PRIMARY KEY,
    installed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS prometa_runtime_admission_replay (
    tenant_id TEXT NOT NULL,
    bundle_jti TEXT NOT NULL,
    promotion_jti TEXT NOT NULL,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, bundle_jti),
    UNIQUE (tenant_id, promotion_jti)
);

CREATE TABLE IF NOT EXISTS prometa_runtime_request_state (
    tenant_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    state JSONB NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, runtime_id, request_id)
);

CREATE INDEX IF NOT EXISTS prometa_runtime_request_state_updated_idx
    ON prometa_runtime_request_state (tenant_id, runtime_id, updated_at);

CREATE TABLE IF NOT EXISTS prometa_runtime_release_activation (
    tenant_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    release_id TEXT NOT NULL,
    artifact_digest TEXT NOT NULL,
    bundle_jti TEXT NOT NULL,
    promotion_jti TEXT NOT NULL,
    activated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_joined_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, runtime_id, deployment_id),
    UNIQUE (tenant_id, promotion_jti)
);

ALTER TABLE prometa_runtime_release_activation
    DROP CONSTRAINT IF EXISTS
    prometa_runtime_release_activation_tenant_id_bundle_jti_key;

CREATE TABLE IF NOT EXISTS prometa_runtime_bundle_identity (
    tenant_id TEXT NOT NULL,
    bundle_jti TEXT NOT NULL,
    artifact_digest TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, bundle_jti)
);

CREATE INDEX IF NOT EXISTS prometa_runtime_release_activation_release_idx
    ON prometa_runtime_release_activation (tenant_id, release_id);

CREATE TABLE IF NOT EXISTS prometa_runtime_receipt_outbox (
    tenant_id TEXT NOT NULL,
    receipt_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'delivered', 'dead_letter')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    leased_until TIMESTAMPTZ,
    lease_token TEXT,
    last_error_code TEXT,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, receipt_id)
);

CREATE INDEX IF NOT EXISTS prometa_runtime_receipt_outbox_pending_idx
    ON prometa_runtime_receipt_outbox (
        tenant_id, status, available_at, created_at, sequence
    );

CREATE TABLE IF NOT EXISTS prometa_runtime_release_cache (
    tenant_id TEXT NOT NULL,
    attestation_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    artifact_digest TEXT NOT NULL,
    release_id TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    target_environment TEXT NOT NULL,
    runtime_target TEXT NOT NULL,
    bundle JSONB NOT NULL,
    promotion_attestation JSONB NOT NULL,
    checked_at TIMESTAMPTZ NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    verified_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, attestation_id)
);

CREATE INDEX IF NOT EXISTS prometa_runtime_release_cache_deployment_idx
    ON prometa_runtime_release_cache (
        tenant_id, deployment_id, verified_at
    );

CREATE TABLE IF NOT EXISTS prometa_runtime_task (
    tenant_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    artifact_digest TEXT NOT NULL,
    release_id TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    recoverable BOOLEAN NOT NULL,
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 20),
    status TEXT NOT NULL CHECK (
        status IN ('running', 'retryable', 'completed', 'failed', 'blocked')
    ),
    attempt INTEGER NOT NULL CHECK (attempt BETWEEN 1 AND 20),
    sequence BIGINT NOT NULL CHECK (sequence >= 1),
    claim_token TEXT,
    lease_expires_at TIMESTAMPTZ,
    last_error_code TEXT,
    output_digest TEXT,
    model_name TEXT,
    model_attempts INTEGER CHECK (model_attempts IS NULL OR model_attempts >= 1),
    tool_calls INTEGER CHECK (tool_calls IS NULL OR tool_calls >= 0),
    used_fallback BOOLEAN,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, runtime_id, request_id),
    CHECK (
        (status = 'running' AND claim_token IS NOT NULL
            AND lease_expires_at IS NOT NULL)
        OR
        (status <> 'running' AND claim_token IS NULL
            AND lease_expires_at IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS prometa_runtime_task_recovery_idx
    ON prometa_runtime_task (
        tenant_id, runtime_id, status, lease_expires_at, updated_at
    );

CREATE TABLE IF NOT EXISTS prometa_runtime_task_event (
    tenant_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence >= 1),
    transition TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('running', 'retryable', 'completed', 'failed', 'blocked')
    ),
    attempt INTEGER NOT NULL CHECK (attempt BETWEEN 1 AND 20),
    reason TEXT,
    occurred_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, runtime_id, request_id, sequence),
    FOREIGN KEY (tenant_id, runtime_id, request_id)
        REFERENCES prometa_runtime_task (tenant_id, runtime_id, request_id)
        ON DELETE CASCADE
);

INSERT INTO prometa_runtime_schema_migrations (version)
VALUES (1)
ON CONFLICT (version) DO NOTHING;

INSERT INTO prometa_runtime_schema_migrations (version)
VALUES (2)
ON CONFLICT (version) DO NOTHING;

INSERT INTO prometa_runtime_schema_migrations (version)
VALUES (3)
ON CONFLICT (version) DO NOTHING;

INSERT INTO prometa_runtime_schema_migrations (version)
VALUES (4)
ON CONFLICT (version) DO NOTHING;

INSERT INTO prometa_runtime_schema_migrations (version)
VALUES (5)
ON CONFLICT (version) DO NOTHING;
"""


class RuntimePersistenceError(RuntimeError):
    """Stable persistence failure that does not expose database credentials."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code.replace("_", " "))


@dataclass(frozen=True)
class RuntimeStateRecord:
    """One durable request-state snapshot and its store-assigned version."""

    request_id: str
    state: Mapping[str, Any]
    version: int
    updated_at: datetime


@dataclass(frozen=True)
class RuntimePostgresVerificationReport:
    """Payload-free result of a tenant runtime restore integrity check."""

    schema_version: int
    migration_versions: Tuple[int, ...]
    table_counts: Mapping[str, int]

    def as_dict(self) -> Mapping[str, Any]:
        return {
            "integrity": "verified",
            "schemaVersion": self.schema_version,
            "migrationVersions": list(self.migration_versions),
            "tableCounts": dict(self.table_counts),
        }


@dataclass(frozen=True)
class RuntimePostgresCompatibilityReport:
    """Payload-free result of a target runtime/database compatibility check."""

    schema_version: int
    migration_versions: Tuple[int, ...]
    minimum_schema_version: int
    maximum_schema_version: int

    def as_dict(self) -> Mapping[str, Any]:
        return {
            "compatibility": "compatible",
            "compatibilityVersion": RUNTIME_POSTGRES_COMPATIBILITY_VERSION,
            "schemaVersion": self.schema_version,
            "migrationVersions": list(self.migration_versions),
            "minimumSchemaVersion": self.minimum_schema_version,
            "maximumSchemaVersion": self.maximum_schema_version,
        }


def _validate_identifier(
    name: str, value: str, max_length: int = _MAX_IDENTIFIER_LENGTH
) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or value != value.strip()
        or len(value) > max_length
    ):
        raise ValueError(
            "%s must be a trimmed string of 1-%d characters"
            % (name, max_length)
        )
    return value


def _validate_digest(value: str) -> str:
    if not isinstance(value, str) or _SHA256_DIGEST.fullmatch(value) is None:
        raise ValueError("artifact_digest must be sha256:<hex>")
    return value


def _default_connect(dsn: str) -> Any:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - core-only smoke owns this path
        raise RuntimePersistenceError("postgres_dependency_missing") from exc
    return psycopg.connect(dsn)


def _serialize_state(state: Mapping[str, Any]) -> str:
    if not isinstance(state, Mapping):
        raise ValueError("state must be a mapping")
    try:
        encoded = json.dumps(
            state,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("state must be finite JSON data") from exc
    if len(encoded.encode("utf-8")) > _MAX_STATE_BYTES:
        raise ValueError("state exceeds the 1 MiB limit")
    return encoded


def _serialize_receipt(receipt: Mapping[str, Any]) -> tuple[str, str, int]:
    if not isinstance(receipt, Mapping):
        raise ValueError("receipt must be a mapping")
    receipt_id = _validate_identifier(
        "receipt_id", receipt.get("receiptId"), max_length=200
    )
    transition = receipt.get("transition")
    if not isinstance(transition, str) or transition not in _RECEIPT_SEQUENCE:
        raise ValueError("receipt transition is unsupported")
    try:
        encoded = json.dumps(
            dict(receipt),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("receipt must be finite JSON data") from exc
    if len(encoded.encode("utf-8")) > _MAX_RECEIPT_BYTES:
        raise ValueError("receipt exceeds the 64 KiB limit")
    return receipt_id, encoded, _RECEIPT_SEQUENCE[transition]


def _serialize_release_document(name: str, value: Mapping[str, Any]) -> str:
    if not isinstance(value, Mapping):
        raise ValueError("%s must be a mapping" % name)
    try:
        encoded = json.dumps(
            dict(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("%s must be finite JSON data" % name) from exc
    if len(encoded.encode("utf-8")) > _MAX_RELEASE_DOCUMENT_BYTES:
        raise ValueError("%s exceeds the 12 MiB limit" % name)
    return encoded


def _release_document(value: Any) -> Mapping[str, Any]:
    try:
        if isinstance(value, str):
            value = json.loads(value)
    except json.JSONDecodeError:
        raise RuntimePersistenceError("release_cache_record_invalid") from None
    if not isinstance(value, Mapping):
        raise RuntimePersistenceError("release_cache_record_invalid")
    return dict(value)


def install_postgres_runtime_schema(
    dsn: str,
    *,
    connect: Optional[Callable[[str], Any]] = None,
) -> None:
    """Install the fixed runtime durability schema in a tenant-owned database."""

    if not isinstance(dsn, str) or not dsn.strip():
        raise ValueError("dsn must be a non-empty string")
    connector = connect or _default_connect
    try:
        with connector(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(_SCHEMA_SQL)
    except RuntimePersistenceError:
        raise
    except Exception:
        raise RuntimePersistenceError("runtime_schema_install_failed") from None


def check_postgres_runtime_compatibility(
    dsn: str,
    *,
    connect: Optional[Callable[[str], Any]] = None,
) -> RuntimePostgresCompatibilityReport:
    """Verify that this runtime can safely use an installed schema."""

    if not isinstance(dsn, str) or not dsn.strip():
        raise ValueError("dsn must be a non-empty string")
    connector = connect or _default_connect
    try:
        with connector(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                cursor.execute(
                    "SELECT to_regclass('public.prometa_runtime_schema_migrations')"
                )
                migration_table = cursor.fetchone()
                if migration_table is None or migration_table[0] is None:
                    raise RuntimePersistenceError("runtime_schema_uninitialized")

                cursor.execute(
                    """
                    SELECT version
                    FROM prometa_runtime_schema_migrations
                    ORDER BY version
                    """
                )
                versions = tuple(int(row[0]) for row in cursor.fetchall())
                if not versions:
                    raise RuntimePersistenceError("runtime_schema_uninitialized")
                if versions != tuple(range(1, versions[-1] + 1)):
                    raise RuntimePersistenceError("runtime_schema_migration_gap")
                schema_version = versions[-1]
                if schema_version < RUNTIME_POSTGRES_MIN_SCHEMA_VERSION:
                    raise RuntimePersistenceError("runtime_schema_too_old")
                if schema_version > RUNTIME_POSTGRES_MAX_SCHEMA_VERSION:
                    raise RuntimePersistenceError("runtime_schema_too_new")

                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = ANY(%s)
                    ORDER BY table_name
                    """,
                    (list(_RUNTIME_TABLES),),
                )
                present_tables = {str(row[0]) for row in cursor.fetchall()}
                if present_tables != set(_RUNTIME_TABLES):
                    raise RuntimePersistenceError("runtime_schema_incompatible")
    except RuntimePersistenceError:
        raise
    except Exception:
        raise RuntimePersistenceError(
            "runtime_schema_compatibility_failed"
        ) from None
    return RuntimePostgresCompatibilityReport(
        schema_version=schema_version,
        migration_versions=versions,
        minimum_schema_version=RUNTIME_POSTGRES_MIN_SCHEMA_VERSION,
        maximum_schema_version=RUNTIME_POSTGRES_MAX_SCHEMA_VERSION,
    )


def verify_postgres_runtime_integrity(
    dsn: str,
    *,
    connect: Optional[Callable[[str], Any]] = None,
) -> RuntimePostgresVerificationReport:
    """Verify a restored runtime database without returning tenant data."""

    if not isinstance(dsn, str) or not dsn.strip():
        raise ValueError("dsn must be a non-empty string")
    connector = connect or _default_connect
    try:
        with connector(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                cursor.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = ANY(%s)
                    ORDER BY table_name
                    """,
                    (list(_RUNTIME_TABLES),),
                )
                present_tables = {str(row[0]) for row in cursor.fetchall()}
                if present_tables != set(_RUNTIME_TABLES):
                    raise RuntimePersistenceError(
                        "runtime_schema_integrity_failed"
                    )

                cursor.execute(
                    """
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = ANY(%s)
                    ORDER BY table_name, ordinal_position
                    """,
                    (list(_TASK_TABLE_COLUMNS),),
                )
                columns = {
                    table_name: set()
                    for table_name in _TASK_TABLE_COLUMNS
                }
                for table_name, column_name in cursor.fetchall():
                    columns[str(table_name)].add(str(column_name))
                if any(
                    columns[table_name] != expected
                    for table_name, expected in _TASK_TABLE_COLUMNS.items()
                ):
                    raise RuntimePersistenceError(
                        "runtime_schema_integrity_failed"
                    )

                cursor.execute(
                    """
                    SELECT version
                    FROM prometa_runtime_schema_migrations
                    ORDER BY version
                    """
                )
                versions = tuple(int(row[0]) for row in cursor.fetchall())
                expected_versions = tuple(
                    range(1, RUNTIME_POSTGRES_SCHEMA_VERSION + 1)
                )
                if versions != expected_versions:
                    raise RuntimePersistenceError(
                        "runtime_schema_integrity_failed"
                    )

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM prometa_runtime_task AS task
                    LEFT JOIN LATERAL (
                        SELECT COUNT(*) AS event_count,
                               MAX(event.sequence) AS max_sequence
                        FROM prometa_runtime_task_event AS event
                        WHERE event.tenant_id = task.tenant_id
                          AND event.runtime_id = task.runtime_id
                          AND event.request_id = task.request_id
                    ) AS history ON TRUE
                    LEFT JOIN LATERAL (
                        SELECT event.status, event.attempt
                        FROM prometa_runtime_task_event AS event
                        WHERE event.tenant_id = task.tenant_id
                          AND event.runtime_id = task.runtime_id
                          AND event.request_id = task.request_id
                        ORDER BY event.sequence DESC
                        LIMIT 1
                    ) AS latest ON TRUE
                    WHERE history.event_count <> task.sequence
                       OR history.max_sequence IS DISTINCT FROM task.sequence
                       OR latest.status IS DISTINCT FROM task.status
                       OR latest.attempt IS DISTINCT FROM task.attempt
                       OR task.attempt > task.max_attempts
                       OR task.input_digest !~ '^sha256:[0-9a-f]{64}$'
                       OR task.artifact_digest !~ '^sha256:[0-9a-f]{64}$'
                       OR (task.output_digest IS NOT NULL AND
                           task.output_digest !~ '^sha256:[0-9a-f]{64}$')
                       OR ((task.status = 'running') IS DISTINCT FROM
                           (task.claim_token IS NOT NULL AND
                            task.lease_expires_at IS NOT NULL))
                       OR ((task.status = 'completed') IS DISTINCT FROM
                           (task.output_digest IS NOT NULL AND
                            task.model_name IS NOT NULL AND
                            task.model_attempts IS NOT NULL AND
                            task.tool_calls IS NOT NULL AND
                            task.used_fallback IS NOT NULL))
                       OR ((task.status IN ('completed', 'failed')) IS DISTINCT FROM
                           (task.completed_at IS NOT NULL))
                    """
                )
                row = cursor.fetchone()
                if row is None or int(row[0]) != 0:
                    raise RuntimePersistenceError(
                        "runtime_schema_integrity_failed"
                    )

                cursor.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM prometa_runtime_schema_migrations),
                        (SELECT COUNT(*) FROM prometa_runtime_admission_replay),
                        (SELECT COUNT(*) FROM prometa_runtime_request_state),
                        (SELECT COUNT(*) FROM prometa_runtime_release_activation),
                        (SELECT COUNT(*) FROM prometa_runtime_bundle_identity),
                        (SELECT COUNT(*) FROM prometa_runtime_receipt_outbox),
                        (SELECT COUNT(*) FROM prometa_runtime_release_cache),
                        (SELECT COUNT(*) FROM prometa_runtime_task),
                        (SELECT COUNT(*) FROM prometa_runtime_task_event)
                    """
                )
                count_row = cursor.fetchone()
                if count_row is None or len(count_row) != len(_RUNTIME_TABLES):
                    raise RuntimePersistenceError(
                        "runtime_schema_integrity_failed"
                    )
                counts = {
                    table_name: int(count)
                    for table_name, count in zip(_RUNTIME_TABLES, count_row)
                }
    except RuntimePersistenceError:
        raise
    except Exception:
        raise RuntimePersistenceError(
            "runtime_schema_verification_failed"
        ) from None
    return RuntimePostgresVerificationReport(
        schema_version=RUNTIME_POSTGRES_SCHEMA_VERSION,
        migration_versions=versions,
        table_counts=counts,
    )


class _PostgresTenantStore:
    def __init__(
        self,
        dsn: str,
        *,
        tenant_id: str,
        connect: Optional[Callable[[str], Any]] = None,
    ) -> None:
        if not isinstance(dsn, str) or not dsn.strip():
            raise ValueError("dsn must be a non-empty string")
        self._dsn = dsn
        self.tenant_id = _validate_identifier("tenant_id", tenant_id)
        self._connect = connect or _default_connect


class PostgresAdmissionReplayStore(_PostgresTenantStore):
    """Tenant-wide atomic replay reservation shared by all runtime replicas."""

    def reserve_pair(self, bundle_jti: str, promotion_jti: str) -> bool:
        bundle = _validate_identifier("bundle_jti", bundle_jti)
        promotion = _validate_identifier("promotion_jti", promotion_jti)
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO prometa_runtime_admission_replay (
                            tenant_id, bundle_jti, promotion_jti
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                        RETURNING bundle_jti
                        """,
                        (self.tenant_id, bundle, promotion),
                    )
                    return cursor.fetchone() is not None
        except RuntimePersistenceError:
            raise
        except Exception:
            raise RuntimePersistenceError("replay_store_unavailable") from None


class PostgresRuntimeActivationStore(_PostgresTenantStore):
    """Restart-safe immutable activation shared by tenant runtime replicas."""

    def activate_or_join(
        self,
        *,
        runtime_id: str,
        deployment_id: str,
        release_id: str,
        artifact_digest: str,
        bundle_jti: str,
        promotion_jti: str,
    ) -> RuntimeActivationResult:
        runtime = _validate_identifier("runtime_id", runtime_id)
        deployment = _validate_identifier("deployment_id", deployment_id)
        release = _validate_identifier("release_id", release_id)
        digest = _validate_digest(artifact_digest)
        bundle = _validate_identifier("bundle_jti", bundle_jti)
        promotion = _validate_identifier("promotion_jti", promotion_jti)
        identity = (release, digest, bundle, promotion)
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO prometa_runtime_bundle_identity (
                            tenant_id, bundle_jti, artifact_digest
                        ) VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (self.tenant_id, bundle, digest),
                    )
                    cursor.execute(
                        """
                        SELECT artifact_digest
                        FROM prometa_runtime_bundle_identity
                        WHERE tenant_id = %s AND bundle_jti = %s
                        FOR UPDATE
                        """,
                        (self.tenant_id, bundle),
                    )
                    bundle_identity = cursor.fetchone()
                    if bundle_identity is None or bundle_identity[0] != digest:
                        raise RuntimePersistenceError("runtime_activation_conflict")
                    cursor.execute(
                        """
                        INSERT INTO prometa_runtime_release_activation (
                            tenant_id, runtime_id, deployment_id, release_id,
                            artifact_digest, bundle_jti, promotion_jti
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        RETURNING activated_at
                        """,
                        (
                            self.tenant_id,
                            runtime,
                            deployment,
                            release,
                            digest,
                            bundle,
                            promotion,
                        ),
                    )
                    inserted = cursor.fetchone()
                    if inserted is not None:
                        return RuntimeActivationResult(
                            created=True, activated_at=inserted[0]
                        )
                    cursor.execute(
                        """
                        SELECT release_id, artifact_digest, bundle_jti,
                               promotion_jti, activated_at
                        FROM prometa_runtime_release_activation
                        WHERE tenant_id = %s AND runtime_id = %s AND deployment_id = %s
                        FOR UPDATE
                        """,
                        (self.tenant_id, runtime, deployment),
                    )
                    existing = cursor.fetchone()
                    if existing is None or tuple(existing[:4]) != identity:
                        raise RuntimePersistenceError("runtime_activation_conflict")
                    cursor.execute(
                        """
                        UPDATE prometa_runtime_release_activation
                        SET last_joined_at = CURRENT_TIMESTAMP
                        WHERE tenant_id = %s AND runtime_id = %s AND deployment_id = %s
                        """,
                        (self.tenant_id, runtime, deployment),
                    )
                    return RuntimeActivationResult(
                        created=False, activated_at=existing[4]
                    )
        except RuntimePersistenceError:
            raise
        except Exception:
            raise RuntimePersistenceError("activation_store_unavailable") from None


class PostgresRuntimeReceiptOutbox(_PostgresTenantStore):
    """Durable leased receipt queue shared by all runtime replicas."""

    def enqueue(self, receipt: Mapping[str, Any]) -> bool:
        receipt_id, encoded, sequence = _serialize_receipt(receipt)
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO prometa_runtime_receipt_outbox (
                            tenant_id, receipt_id, sequence, payload
                        ) VALUES (%s, %s, %s, %s::jsonb)
                        ON CONFLICT (tenant_id, receipt_id) DO NOTHING
                        RETURNING receipt_id
                        """,
                        (self.tenant_id, receipt_id, sequence, encoded),
                    )
                    return cursor.fetchone() is not None
        except RuntimePersistenceError:
            raise
        except Exception:
            raise RuntimePersistenceError("receipt_outbox_unavailable") from None

    def claim_next(self, lease_seconds: float) -> Optional[RuntimeReceiptOutboxItem]:
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or lease_seconds <= 0
            or lease_seconds > 3600
        ):
            raise ValueError("lease_seconds must be between 0 and 3600")
        lease_token = str(uuid.uuid4())
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        WITH candidate AS (
                            SELECT current_receipt.receipt_id
                            FROM prometa_runtime_receipt_outbox AS current_receipt
                            WHERE current_receipt.tenant_id = %s
                              AND current_receipt.status = 'pending'
                              AND current_receipt.available_at <= CURRENT_TIMESTAMP
                              AND (
                                  current_receipt.leased_until IS NULL
                                  OR current_receipt.leased_until
                                      <= CURRENT_TIMESTAMP
                              )
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM prometa_runtime_receipt_outbox AS earlier
                                  WHERE earlier.tenant_id
                                      = current_receipt.tenant_id
                                    AND earlier.status = 'pending'
                                    AND earlier.payload->>'deploymentId'
                                      = current_receipt.payload->>'deploymentId'
                                    AND (
                                      earlier.sequence,
                                      earlier.created_at,
                                      earlier.receipt_id
                                    ) < (
                                      current_receipt.sequence,
                                      current_receipt.created_at,
                                      current_receipt.receipt_id
                                    )
                              )
                            ORDER BY current_receipt.created_at,
                                     current_receipt.sequence,
                                     current_receipt.receipt_id
                            FOR UPDATE OF current_receipt SKIP LOCKED
                            LIMIT 1
                        )
                        UPDATE prometa_runtime_receipt_outbox AS outbox
                        SET leased_until = CURRENT_TIMESTAMP
                                + (%s * INTERVAL '1 second'),
                            lease_token = %s,
                            attempts = outbox.attempts + 1,
                            updated_at = CURRENT_TIMESTAMP
                        FROM candidate
                        WHERE outbox.tenant_id = %s
                          AND outbox.receipt_id = candidate.receipt_id
                        RETURNING outbox.receipt_id, outbox.payload,
                                  outbox.attempts
                        """,
                        (
                            self.tenant_id,
                            float(lease_seconds),
                            lease_token,
                            self.tenant_id,
                        ),
                    )
                    row = cursor.fetchone()
        except RuntimePersistenceError:
            raise
        except Exception:
            raise RuntimePersistenceError("receipt_outbox_unavailable") from None
        if row is None:
            return None
        try:
            receipt_id = _validate_identifier(
                "receipt_id", row[0], max_length=200
            )
            receipt = row[1]
            if isinstance(receipt, str):
                receipt = json.loads(receipt)
            attempts = int(row[2])
        except (TypeError, ValueError, json.JSONDecodeError, IndexError):
            raise RuntimePersistenceError("receipt_outbox_record_invalid") from None
        if (
            not isinstance(receipt, Mapping)
            or receipt.get("receiptId") != receipt_id
            or attempts < 1
        ):
            raise RuntimePersistenceError("receipt_outbox_record_invalid")
        return RuntimeReceiptOutboxItem(
            receipt_id=receipt_id,
            receipt=dict(receipt),
            attempts=attempts,
            lease_token=lease_token,
        )

    @staticmethod
    def _validate_item(item: RuntimeReceiptOutboxItem) -> tuple[str, str]:
        if not isinstance(item, RuntimeReceiptOutboxItem):
            raise ValueError("item must be a RuntimeReceiptOutboxItem")
        return (
            _validate_identifier("receipt_id", item.receipt_id, max_length=200),
            _validate_identifier("lease_token", item.lease_token, max_length=200),
        )

    @staticmethod
    def _validate_error_code(error_code: str) -> str:
        if not isinstance(error_code, str) or _ERROR_CODE.fullmatch(error_code) is None:
            raise ValueError("error_code must be a bounded machine code")
        return error_code

    def _complete_lease(
        self,
        item: RuntimeReceiptOutboxItem,
        *,
        status: str,
        error_code: Optional[str] = None,
        delay_seconds: float = 0,
    ) -> None:
        receipt_id, lease_token = self._validate_item(item)
        if status not in {"pending", "delivered", "dead_letter"}:
            raise ValueError("unsupported receipt outbox status")
        if error_code is not None:
            error_code = self._validate_error_code(error_code)
        if (
            isinstance(delay_seconds, bool)
            or not isinstance(delay_seconds, (int, float))
            or delay_seconds < 0
            or delay_seconds > 86_400
        ):
            raise ValueError("delay_seconds must be between 0 and 86400")
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE prometa_runtime_receipt_outbox
                        SET status = %s,
                            available_at = CASE
                                WHEN %s = 'pending' THEN CURRENT_TIMESTAMP
                                    + (%s * INTERVAL '1 second')
                                ELSE available_at
                            END,
                            leased_until = NULL,
                            lease_token = NULL,
                            last_error_code = %s,
                            delivered_at = CASE
                                WHEN %s = 'delivered' THEN CURRENT_TIMESTAMP
                                ELSE delivered_at
                            END,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE tenant_id = %s
                          AND receipt_id = %s
                          AND status = 'pending'
                          AND lease_token = %s
                        """,
                        (
                            status,
                            status,
                            float(delay_seconds),
                            error_code,
                            status,
                            self.tenant_id,
                            receipt_id,
                            lease_token,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise RuntimePersistenceError("receipt_outbox_lease_lost")
        except RuntimePersistenceError:
            raise
        except Exception:
            raise RuntimePersistenceError("receipt_outbox_unavailable") from None

    def mark_delivered(self, item: RuntimeReceiptOutboxItem) -> None:
        self._complete_lease(item, status="delivered")

    def reschedule(
        self,
        item: RuntimeReceiptOutboxItem,
        *,
        delay_seconds: float,
        error_code: str,
    ) -> None:
        self._complete_lease(
            item,
            status="pending",
            delay_seconds=delay_seconds,
            error_code=error_code,
        )

    def mark_dead_letter(
        self, item: RuntimeReceiptOutboxItem, *, error_code: str
    ) -> None:
        self._complete_lease(
            item,
            status="dead_letter",
            error_code=error_code,
        )


class PostgresRuntimeReleaseCache(_PostgresTenantStore):
    """Persist caller-verified release material for bounded offline restart."""

    def save(self, handoff: RuntimeReleaseHandoff) -> None:
        if not isinstance(handoff, RuntimeReleaseHandoff):
            raise ValueError("handoff must be a RuntimeReleaseHandoff")
        attestation_id = _validate_identifier(
            "attestation_id", handoff.attestation_id, max_length=200
        )
        artifact_id = _validate_identifier(
            "artifact_id", handoff.artifact_id, max_length=200
        )
        digest = _validate_digest(handoff.artifact_digest)
        release_id = _validate_identifier(
            "release_id", handoff.release_id, max_length=200
        )
        deployment_id = _validate_identifier(
            "deployment_id", handoff.deployment_id, max_length=200
        )
        environment = _validate_identifier(
            "target_environment", handoff.target_environment
        )
        runtime_target = _validate_identifier(
            "runtime_target", handoff.runtime_target
        )
        bundle = _serialize_release_document("bundle", handoff.bundle)
        promotion = _serialize_release_document(
            "promotion_attestation", handoff.promotion_attestation
        )
        if (
            environment not in _RUNTIME_ENVIRONMENTS
            or handoff.bundle.get("artifactDigest") != digest
            or handoff.promotion_attestation.get("attestationId")
            != attestation_id
        ):
            raise ValueError("handoff release bindings are inconsistent")
        if handoff.checked_at.tzinfo is None or handoff.fetched_at.tzinfo is None:
            raise ValueError("handoff timestamps must be timezone-aware")
        identity = (
            artifact_id,
            digest,
            release_id,
            deployment_id,
            environment,
            runtime_target,
            bundle,
            promotion,
        )
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO prometa_runtime_release_cache (
                            tenant_id, attestation_id, artifact_id,
                            artifact_digest, release_id, deployment_id,
                            target_environment, runtime_target, bundle,
                            promotion_attestation, checked_at, fetched_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s,
                            %s::jsonb, %s::jsonb, %s, %s
                        )
                        ON CONFLICT (tenant_id, attestation_id) DO NOTHING
                        RETURNING attestation_id
                        """,
                        (
                            self.tenant_id,
                            attestation_id,
                            artifact_id,
                            digest,
                            release_id,
                            deployment_id,
                            environment,
                            runtime_target,
                            bundle,
                            promotion,
                            handoff.checked_at,
                            handoff.fetched_at,
                        ),
                    )
                    if cursor.fetchone() is not None:
                        return
                    cursor.execute(
                        """
                        SELECT artifact_id, artifact_digest, release_id,
                               deployment_id, target_environment,
                               runtime_target, bundle, promotion_attestation
                        FROM prometa_runtime_release_cache
                        WHERE tenant_id = %s AND attestation_id = %s
                        FOR UPDATE
                        """,
                        (self.tenant_id, attestation_id),
                    )
                    existing = cursor.fetchone()
                    if existing is None:
                        raise RuntimePersistenceError("release_cache_conflict")
                    stored_identity = (
                        *tuple(existing[:6]),
                        _serialize_release_document(
                            "bundle", _release_document(existing[6])
                        ),
                        _serialize_release_document(
                            "promotion_attestation",
                            _release_document(existing[7]),
                        ),
                    )
                    if stored_identity != identity:
                        raise RuntimePersistenceError("release_cache_conflict")
                    cursor.execute(
                        """
                        UPDATE prometa_runtime_release_cache
                        SET checked_at = %s,
                            fetched_at = %s,
                            verified_at = CURRENT_TIMESTAMP
                        WHERE tenant_id = %s AND attestation_id = %s
                        """,
                        (
                            handoff.checked_at,
                            handoff.fetched_at,
                            self.tenant_id,
                            attestation_id,
                        ),
                    )
        except RuntimePersistenceError:
            raise
        except Exception:
            raise RuntimePersistenceError("release_cache_unavailable") from None

    def load(
        self,
        attestation_id: str,
        *,
        max_age_seconds: float,
        now: Optional[datetime] = None,
    ) -> Optional[RuntimeReleaseHandoff]:
        attestation = _validate_identifier(
            "attestation_id", attestation_id, max_length=200
        )
        if (
            isinstance(max_age_seconds, bool)
            or not isinstance(max_age_seconds, (int, float))
            or max_age_seconds <= 0
            or max_age_seconds > 7 * 86_400
        ):
            raise ValueError("max_age_seconds must be greater than 0 and at most 604800")
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        current = current.astimezone(timezone.utc)
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT artifact_id, artifact_digest, release_id,
                               deployment_id, target_environment,
                               runtime_target, bundle, promotion_attestation,
                               checked_at, fetched_at
                        FROM prometa_runtime_release_cache
                        WHERE tenant_id = %s AND attestation_id = %s
                        """,
                        (self.tenant_id, attestation),
                    )
                    row = cursor.fetchone()
        except RuntimePersistenceError:
            raise
        except Exception:
            raise RuntimePersistenceError("release_cache_unavailable") from None
        if row is None:
            return None
        try:
            checked_at = row[8]
            fetched_at = row[9]
            if (
                not isinstance(checked_at, datetime)
                or checked_at.tzinfo is None
                or not isinstance(fetched_at, datetime)
                or fetched_at.tzinfo is None
                or fetched_at.astimezone(timezone.utc)
                > current + timedelta(seconds=60)
            ):
                raise RuntimePersistenceError("release_cache_record_invalid")
            if (
                current - fetched_at.astimezone(timezone.utc)
            ).total_seconds() > float(max_age_seconds):
                return None
            environment = _validate_identifier("target_environment", row[4])
            if environment not in _RUNTIME_ENVIRONMENTS:
                raise ValueError("unsupported cached target environment")
            return RuntimeReleaseHandoff(
                attestation_id=attestation,
                artifact_id=_validate_identifier(
                    "artifact_id", row[0], max_length=200
                ),
                artifact_digest=_validate_digest(row[1]),
                release_id=_validate_identifier(
                    "release_id", row[2], max_length=200
                ),
                deployment_id=_validate_identifier(
                    "deployment_id", row[3], max_length=200
                ),
                target_environment=environment,
                runtime_target=_validate_identifier("runtime_target", row[5]),
                bundle=_release_document(row[6]),
                promotion_attestation=_release_document(row[7]),
                checked_at=checked_at.astimezone(timezone.utc),
                fetched_at=fetched_at.astimezone(timezone.utc),
            )
        except (IndexError, TypeError, ValueError):
            raise RuntimePersistenceError("release_cache_record_invalid") from None


class PostgresRuntimeTaskStore(_PostgresTenantStore):
    """Atomic cross-replica task leases and ordered payload-free history."""

    def __init__(
        self,
        dsn: str,
        *,
        tenant_id: str,
        runtime_id: str,
        connect: Optional[Callable[[str], Any]] = None,
    ) -> None:
        super().__init__(dsn, tenant_id=tenant_id, connect=connect)
        self.runtime_id = _validate_identifier("runtime_id", runtime_id)

    def _insert_event(
        self,
        cursor: Any,
        *,
        request_id: str,
        sequence: int,
        transition: str,
        status: str,
        attempt: int,
        occurred_at: datetime,
        reason: Optional[str] = None,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO prometa_runtime_task_event (
                tenant_id, runtime_id, request_id, sequence,
                transition, status, attempt, reason, occurred_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                self.tenant_id,
                self.runtime_id,
                request_id,
                sequence,
                transition,
                status,
                attempt,
                reason,
                occurred_at,
            ),
        )

    @staticmethod
    def _current_time(
        cursor: Any, supplied: Optional[datetime]
    ) -> datetime:
        if supplied is not None:
            return supplied
        cursor.execute("SELECT CURRENT_TIMESTAMP")
        row = cursor.fetchone()
        if (
            row is None
            or not isinstance(row[0], datetime)
            or row[0].tzinfo is None
        ):
            raise RuntimeTaskError("task_clock_invalid")
        return row[0].astimezone(timezone.utc)

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
        request = _task_identifier("request_id", request_id)
        input_value = _task_digest("input_digest", input_digest)
        artifact = _task_digest("artifact_digest", artifact_digest)
        release = _task_identifier("release_id", release_id, 200)
        deployment = _task_identifier("deployment_id", deployment_id, 200)
        if type(recoverable) is not bool:
            raise ValueError("recoverable must be a boolean")
        attempts, lease = _task_claim_policy(max_attempts, lease_seconds)
        supplied_current = None if now is None else _task_instant(now)
        token = uuid.uuid4().hex
        result: Optional[RuntimeTaskClaim] = None
        post_commit_error: Optional[str] = None
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    current = self._current_time(cursor, supplied_current)
                    expires = current + timedelta(seconds=lease)
                    cursor.execute(
                        """
                        INSERT INTO prometa_runtime_task (
                            tenant_id, runtime_id, request_id, input_digest,
                            artifact_digest, release_id, deployment_id,
                            recoverable, max_attempts, status, attempt,
                            sequence, claim_token, lease_expires_at,
                            created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            'running', 1, 1, %s, %s, %s, %s
                        )
                        ON CONFLICT (tenant_id, runtime_id, request_id)
                        DO NOTHING
                        RETURNING request_id
                        """,
                        (
                            self.tenant_id,
                            self.runtime_id,
                            request,
                            input_value,
                            artifact,
                            release,
                            deployment,
                            recoverable,
                            attempts,
                            token,
                            expires,
                            current,
                            current,
                        ),
                    )
                    inserted = cursor.fetchone()
                    if inserted is not None:
                        self._insert_event(
                            cursor,
                            request_id=request,
                            sequence=1,
                            transition="claimed",
                            status="running",
                            attempt=1,
                            occurred_at=current,
                        )
                        result = RuntimeTaskClaim(
                            request_id=request,
                            claim_token=token,
                            attempt=1,
                            sequence=1,
                            transition="claimed",
                            lease_expires_at=expires,
                        )
                    else:
                        cursor.execute(
                            """
                            SELECT input_digest, artifact_digest, release_id,
                                   deployment_id, recoverable, max_attempts,
                                   status, attempt, sequence, lease_expires_at
                            FROM prometa_runtime_task
                            WHERE tenant_id = %s AND runtime_id = %s
                              AND request_id = %s
                            FOR UPDATE
                            """,
                            (self.tenant_id, self.runtime_id, request),
                        )
                        row = cursor.fetchone()
                        try:
                            if row is None:
                                raise ValueError("missing task")
                            stored_max_attempts = int(row[5])
                            stored_identity = (
                                _task_digest("input_digest", row[0]),
                                _task_digest("artifact_digest", row[1]),
                                _task_identifier("release_id", row[2], 200),
                                _task_identifier("deployment_id", row[3], 200),
                                row[4],
                                stored_max_attempts,
                            )
                            status = str(row[6])
                            attempt = int(row[7])
                            sequence = int(row[8])
                            lease_expires_at = row[9]
                            if (
                                type(row[4]) is not bool
                                or not 1 <= stored_max_attempts <= 20
                                or status not in _TASK_STATUSES
                                or not 1 <= attempt <= 20
                                or not 1 <= sequence
                                or (status == "running")
                                != isinstance(lease_expires_at, datetime)
                                or (
                                    isinstance(lease_expires_at, datetime)
                                    and lease_expires_at.tzinfo is None
                                )
                            ):
                                raise ValueError("invalid task")
                        except (IndexError, TypeError, ValueError):
                            raise RuntimeTaskError("task_record_invalid") from None
                        expected_identity = (
                            input_value,
                            artifact,
                            release,
                            deployment,
                            recoverable,
                            attempts,
                        )
                        if stored_identity != expected_identity:
                            raise RuntimeTaskError("task_identity_conflict")
                        if (
                            status == "running"
                            and lease_expires_at.astimezone(timezone.utc) > current
                        ):
                            raise RuntimeTaskError("task_in_progress")
                        terminal_errors = {
                            "completed": "task_already_completed",
                            "failed": "task_terminal",
                            "blocked": "task_recovery_blocked",
                        }
                        if status in terminal_errors:
                            raise RuntimeTaskError(terminal_errors[status])
                        if status not in {"running", "retryable"}:
                            raise RuntimeTaskError("task_record_invalid")
                        next_sequence = sequence + 1
                        if not recoverable:
                            next_status = "blocked"
                            transition = "recovery_blocked"
                            reason = "task_recovery_blocked"
                            post_commit_error = reason
                        elif attempt >= attempts:
                            next_status = "failed"
                            transition = "attempts_exhausted"
                            reason = "task_attempts_exhausted"
                            post_commit_error = reason
                        else:
                            next_status = "running"
                            transition = (
                                "recovered" if status == "running" else "retried"
                            )
                            reason = None
                        if post_commit_error is not None:
                            cursor.execute(
                                """
                                UPDATE prometa_runtime_task
                                SET status = %s, sequence = %s,
                                    claim_token = NULL,
                                    lease_expires_at = NULL,
                                    last_error_code = %s,
                                    updated_at = %s,
                                    completed_at = %s
                                WHERE tenant_id = %s AND runtime_id = %s
                                  AND request_id = %s
                                """,
                                (
                                    next_status,
                                    next_sequence,
                                    reason,
                                    current,
                                    current if next_status == "failed" else None,
                                    self.tenant_id,
                                    self.runtime_id,
                                    request,
                                ),
                            )
                            self._insert_event(
                                cursor,
                                request_id=request,
                                sequence=next_sequence,
                                transition=transition,
                                status=next_status,
                                attempt=attempt,
                                occurred_at=current,
                                reason=reason,
                            )
                        else:
                            next_attempt = attempt + 1
                            cursor.execute(
                                """
                                UPDATE prometa_runtime_task
                                SET status = 'running', attempt = %s,
                                    sequence = %s, claim_token = %s,
                                    lease_expires_at = %s,
                                    last_error_code = NULL,
                                    updated_at = %s,
                                    completed_at = NULL
                                WHERE tenant_id = %s AND runtime_id = %s
                                  AND request_id = %s
                                """,
                                (
                                    next_attempt,
                                    next_sequence,
                                    token,
                                    expires,
                                    current,
                                    self.tenant_id,
                                    self.runtime_id,
                                    request,
                                ),
                            )
                            self._insert_event(
                                cursor,
                                request_id=request,
                                sequence=next_sequence,
                                transition=transition,
                                status="running",
                                attempt=next_attempt,
                                occurred_at=current,
                            )
                            result = RuntimeTaskClaim(
                                request_id=request,
                                claim_token=token,
                                attempt=next_attempt,
                                sequence=next_sequence,
                                transition=transition,
                                lease_expires_at=expires,
                            )
            if post_commit_error is not None:
                raise RuntimeTaskError(post_commit_error)
            if result is None:
                raise RuntimeTaskError("task_record_invalid")
            return result
        except (RuntimePersistenceError, RuntimeTaskError):
            raise
        except Exception:
            raise RuntimePersistenceError("task_store_unavailable") from None

    def _owned(
        self,
        cursor: Any,
        claim: RuntimeTaskClaim,
        current: datetime,
    ) -> tuple[int, int, int, bool]:
        if not isinstance(claim, RuntimeTaskClaim):
            raise ValueError("claim must be a RuntimeTaskClaim")
        request = _task_identifier("request_id", claim.request_id)
        token = _task_identifier("claim_token", claim.claim_token, 200)
        cursor.execute(
            """
            SELECT status, attempt, max_attempts, recoverable,
                   claim_token, lease_expires_at, sequence
            FROM prometa_runtime_task
            WHERE tenant_id = %s AND runtime_id = %s AND request_id = %s
            FOR UPDATE
            """,
            (self.tenant_id, self.runtime_id, request),
        )
        row = cursor.fetchone()
        try:
            if row is None:
                raise ValueError("missing task")
            status = str(row[0])
            attempt = int(row[1])
            max_attempts = int(row[2])
            recoverable = row[3]
            stored_token = str(row[4])
            lease_expires_at = row[5]
            sequence = int(row[6])
        except (IndexError, TypeError, ValueError):
            raise RuntimeTaskError("task_record_invalid") from None
        if (
            status != "running"
            or type(recoverable) is not bool
            or stored_token != token
            or attempt != claim.attempt
            or sequence != claim.sequence
            or not isinstance(lease_expires_at, datetime)
            or lease_expires_at.tzinfo is None
            or lease_expires_at.astimezone(timezone.utc) <= current
        ):
            raise RuntimeTaskError("task_lease_lost")
        return attempt, max_attempts, sequence, recoverable

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
        output = _task_digest("output_digest", output_digest)
        model = _task_identifier("model_name", model_name, 256)
        if type(model_attempts) is not int or model_attempts < 1:
            raise ValueError("model_attempts must be a positive integer")
        if type(tool_calls) is not int or tool_calls < 0:
            raise ValueError("tool_calls must be a non-negative integer")
        if type(used_fallback) is not bool:
            raise ValueError("used_fallback must be a boolean")
        supplied_current = None if now is None else _task_instant(now)
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    current = self._current_time(cursor, supplied_current)
                    attempt, _, sequence, _ = self._owned(cursor, claim, current)
                    next_sequence = sequence + 1
                    cursor.execute(
                        """
                        UPDATE prometa_runtime_task
                        SET status = 'completed', sequence = %s,
                            claim_token = NULL, lease_expires_at = NULL,
                            output_digest = %s, model_name = %s,
                            model_attempts = %s, tool_calls = %s,
                            used_fallback = %s, updated_at = %s,
                            completed_at = %s
                        WHERE tenant_id = %s AND runtime_id = %s
                          AND request_id = %s
                        """,
                        (
                            next_sequence,
                            output,
                            model,
                            model_attempts,
                            tool_calls,
                            used_fallback,
                            current,
                            current,
                            self.tenant_id,
                            self.runtime_id,
                            claim.request_id,
                        ),
                    )
                    event = RuntimeTaskEvent(
                        sequence=next_sequence,
                        transition="completed",
                        status="completed",
                        attempt=attempt,
                        occurred_at=current,
                    )
                    self._insert_event(
                        cursor,
                        request_id=claim.request_id,
                        sequence=event.sequence,
                        transition=event.transition,
                        status=event.status,
                        attempt=event.attempt,
                        occurred_at=event.occurred_at,
                    )
                    return event
        except (RuntimePersistenceError, RuntimeTaskError):
            raise
        except Exception:
            raise RuntimePersistenceError("task_store_unavailable") from None

    def fail(
        self,
        claim: RuntimeTaskClaim,
        *,
        reason: str,
        retryable: bool,
        now: Optional[datetime] = None,
    ) -> RuntimeTaskEvent:
        error = _task_error_code(reason)
        if type(retryable) is not bool:
            raise ValueError("retryable must be a boolean")
        supplied_current = None if now is None else _task_instant(now)
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    current = self._current_time(cursor, supplied_current)
                    attempt, max_attempts, sequence, recoverable = self._owned(
                        cursor, claim, current
                    )
                    can_retry = (
                        retryable and recoverable and attempt < max_attempts
                    )
                    status = "retryable" if can_retry else "failed"
                    transition = "retry_scheduled" if can_retry else "failed"
                    next_sequence = sequence + 1
                    cursor.execute(
                        """
                        UPDATE prometa_runtime_task
                        SET status = %s, sequence = %s,
                            claim_token = NULL, lease_expires_at = NULL,
                            last_error_code = %s, updated_at = %s,
                            completed_at = %s
                        WHERE tenant_id = %s AND runtime_id = %s
                          AND request_id = %s
                        """,
                        (
                            status,
                            next_sequence,
                            error,
                            current,
                            None if can_retry else current,
                            self.tenant_id,
                            self.runtime_id,
                            claim.request_id,
                        ),
                    )
                    event = RuntimeTaskEvent(
                        sequence=next_sequence,
                        transition=transition,
                        status=status,
                        attempt=attempt,
                        occurred_at=current,
                        reason=error,
                    )
                    self._insert_event(
                        cursor,
                        request_id=claim.request_id,
                        sequence=event.sequence,
                        transition=event.transition,
                        status=event.status,
                        attempt=event.attempt,
                        occurred_at=event.occurred_at,
                        reason=event.reason,
                    )
                    return event
        except (RuntimePersistenceError, RuntimeTaskError):
            raise
        except Exception:
            raise RuntimePersistenceError("task_store_unavailable") from None

    def get(
        self, request_id: str, *, history_limit: int = 50
    ) -> Optional[RuntimeTaskSnapshot]:
        request = _task_identifier("request_id", request_id)
        limit = _task_history_limit(history_limit)
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT artifact_digest, release_id, deployment_id,
                               status, attempt, max_attempts, recoverable,
                               sequence, lease_expires_at, last_error_code,
                               output_digest, model_name, model_attempts,
                               tool_calls, used_fallback, created_at,
                               updated_at, completed_at
                        FROM prometa_runtime_task
                        WHERE tenant_id = %s AND runtime_id = %s
                          AND request_id = %s
                        """,
                        (self.tenant_id, self.runtime_id, request),
                    )
                    row = cursor.fetchone()
                    if row is None:
                        return None
                    cursor.execute(
                        """
                        SELECT sequence, transition, status, attempt,
                               occurred_at, reason
                        FROM prometa_runtime_task_event
                        WHERE tenant_id = %s AND runtime_id = %s
                          AND request_id = %s
                        ORDER BY sequence DESC
                        LIMIT %s
                        """,
                        (self.tenant_id, self.runtime_id, request, limit),
                    )
                    event_rows = cursor.fetchall()
        except (RuntimePersistenceError, RuntimeTaskError):
            raise
        except Exception:
            raise RuntimePersistenceError("task_store_unavailable") from None
        try:
            status = str(row[3])
            recoverable = row[6]
            lease_expires_at = row[8]
            created_at = row[15]
            updated_at = row[16]
            completed_at = row[17]
            if (
                status not in _TASK_STATUSES
                or type(recoverable) is not bool
                or not isinstance(created_at, datetime)
                or created_at.tzinfo is None
                or not isinstance(updated_at, datetime)
                or updated_at.tzinfo is None
                or (
                    lease_expires_at is not None
                    and (
                        not isinstance(lease_expires_at, datetime)
                        or lease_expires_at.tzinfo is None
                    )
                )
                or (status == "running")
                != isinstance(lease_expires_at, datetime)
                or (
                    completed_at is not None
                    and (
                        not isinstance(completed_at, datetime)
                        or completed_at.tzinfo is None
                    )
                )
            ):
                raise ValueError("invalid timestamps")
            last_error = (
                None if row[9] is None else _task_error_code(row[9])
            )
            output = None if row[10] is None else _task_digest(
                "output_digest", row[10]
            )
            model = None if row[11] is None else _task_identifier(
                "model_name", row[11], 256
            )
            record = RuntimeTaskRecord(
                request_id=request,
                artifact_digest=_task_digest("artifact_digest", row[0]),
                release_id=_task_identifier("release_id", row[1], 200),
                deployment_id=_task_identifier("deployment_id", row[2], 200),
                status=status,
                attempt=int(row[4]),
                max_attempts=int(row[5]),
                recoverable=recoverable,
                sequence=int(row[7]),
                lease_expires_at=lease_expires_at,
                last_error_code=last_error,
                output_digest=output,
                model_name=model,
                model_attempts=None if row[12] is None else int(row[12]),
                tool_calls=None if row[13] is None else int(row[13]),
                used_fallback=row[14],
                created_at=created_at,
                updated_at=updated_at,
                completed_at=completed_at,
            )
            if (
                not 1 <= record.attempt <= 20
                or not 1 <= record.max_attempts <= 20
                or record.attempt > record.max_attempts
                or record.sequence < 1
                or (
                    record.used_fallback is not None
                    and type(record.used_fallback) is not bool
                )
                or (record.status == "completed")
                != (record.output_digest is not None)
                or (record.status in {"completed", "failed"})
                != (record.completed_at is not None)
            ):
                raise ValueError("invalid task projection")
            events = []
            for event_row in reversed(event_rows):
                occurred_at = event_row[4]
                reason = (
                    None
                    if event_row[5] is None
                    else _task_error_code(event_row[5])
                )
                if (
                    event_row[2] not in _TASK_STATUSES
                    or not isinstance(occurred_at, datetime)
                    or occurred_at.tzinfo is None
                ):
                    raise ValueError("invalid task event")
                events.append(
                    RuntimeTaskEvent(
                        sequence=int(event_row[0]),
                        transition=_task_identifier(
                            "task_transition", event_row[1], 64
                        ),
                        status=event_row[2],
                        attempt=int(event_row[3]),
                        occurred_at=occurred_at,
                        reason=reason,
                    )
                )
        except (IndexError, TypeError, ValueError):
            raise RuntimePersistenceError("task_record_invalid") from None
        if (
            not events
            or events[-1].sequence != record.sequence
            or any(
                event.sequence < 1
                or event.attempt < 1
                or event.attempt > record.max_attempts
                for event in events
            )
            or any(
                later.sequence <= earlier.sequence
                for earlier, later in zip(events, events[1:])
            )
        ):
            raise RuntimePersistenceError("task_record_invalid")
        return RuntimeTaskSnapshot(
            record=record,
            events=tuple(events),
            history_truncated=record.sequence > len(events),
        )


class PostgresRuntimeStateStore(_PostgresTenantStore):
    """Shared JSON request-state store for tenant runtime replicas.

    Each save is an atomic last-write-wins snapshot with a monotonic database
    version. The kernel remains responsible for request lifecycle ordering.
    """

    def __init__(
        self,
        dsn: str,
        *,
        tenant_id: str,
        runtime_id: str,
        connect: Optional[Callable[[str], Any]] = None,
    ) -> None:
        super().__init__(dsn, tenant_id=tenant_id, connect=connect)
        self.runtime_id = _validate_identifier("runtime_id", runtime_id)

    async def save(self, request_id: str, state: Mapping[str, Any]) -> None:
        request = _validate_identifier("request_id", request_id)
        encoded = _serialize_state(state)
        await asyncio.to_thread(self._save_sync, request, encoded)

    def _save_sync(self, request_id: str, encoded: str) -> None:
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO prometa_runtime_request_state (
                            tenant_id, runtime_id, request_id, state
                        ) VALUES (%s, %s, %s, %s::jsonb)
                        ON CONFLICT (tenant_id, runtime_id, request_id)
                        DO UPDATE SET
                            state = EXCLUDED.state,
                            version = prometa_runtime_request_state.version + 1,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (self.tenant_id, self.runtime_id, request_id, encoded),
                    )
        except RuntimePersistenceError:
            raise
        except Exception:
            raise RuntimePersistenceError("state_store_unavailable") from None

    async def load(self, request_id: str) -> Optional[RuntimeStateRecord]:
        request = _validate_identifier("request_id", request_id)
        return await asyncio.to_thread(self._load_sync, request)

    def _load_sync(self, request_id: str) -> Optional[RuntimeStateRecord]:
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT state, version, updated_at
                        FROM prometa_runtime_request_state
                        WHERE tenant_id = %s AND runtime_id = %s AND request_id = %s
                        """,
                        (self.tenant_id, self.runtime_id, request_id),
                    )
                    row = cursor.fetchone()
        except RuntimePersistenceError:
            raise
        except Exception:
            raise RuntimePersistenceError("state_store_unavailable") from None
        if row is None:
            return None
        state = row[0]
        try:
            if isinstance(state, str):
                state = json.loads(state)
            version = int(row[1])
            updated_at = row[2]
        except (TypeError, ValueError, json.JSONDecodeError, IndexError):
            raise RuntimePersistenceError("state_record_invalid") from None
        if (
            not isinstance(state, Mapping)
            or version < 1
            or not isinstance(updated_at, datetime)
        ):
            raise RuntimePersistenceError("state_record_invalid")
        return RuntimeStateRecord(
            request_id=request_id,
            state=dict(state),
            version=version,
            updated_at=updated_at,
        )

    async def delete(self, request_id: str) -> bool:
        request = _validate_identifier("request_id", request_id)
        return await asyncio.to_thread(self._delete_sync, request)

    def _delete_sync(self, request_id: str) -> bool:
        try:
            with self._connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        DELETE FROM prometa_runtime_request_state
                        WHERE tenant_id = %s AND runtime_id = %s AND request_id = %s
                        """,
                        (self.tenant_id, self.runtime_id, request_id),
                    )
                    return cursor.rowcount == 1
        except RuntimePersistenceError:
            raise
        except Exception:
            raise RuntimePersistenceError("state_store_unavailable") from None


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="prometa-runtime-postgres-init",
        description="Install the fixed tenant-runtime PostgreSQL schema.",
    )
    parser.add_argument(
        "--dsn-env",
        default="PROMETA_RUNTIME_DATABASE_URL",
        help="Environment variable containing a libpq PostgreSQL DSN",
    )
    args = parser.parse_args(argv)
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        parser.error("%s is not set" % args.dsn_env)
    try:
        install_postgres_runtime_schema(dsn)
    except RuntimePersistenceError as exc:
        parser.error(exc.code)
    print("Tenant runtime PostgreSQL schema is ready.")
    return 0


def verify_main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="prometa-runtime-postgres-verify",
        description="Verify restored tenant-runtime PostgreSQL integrity.",
    )
    parser.add_argument(
        "--dsn-env",
        default="PROMETA_RUNTIME_DATABASE_URL",
        help="Environment variable containing a libpq PostgreSQL DSN",
    )
    args = parser.parse_args(argv)
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        parser.error("%s is not set" % args.dsn_env)
    try:
        report = verify_postgres_runtime_integrity(dsn)
    except RuntimePersistenceError as exc:
        parser.error(exc.code)
    print(json.dumps(report.as_dict(), sort_keys=True, separators=(",", ":")))
    return 0


def compatibility_main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="prometa-runtime-postgres-compatibility",
        description="Check target-runtime PostgreSQL schema compatibility.",
    )
    parser.add_argument(
        "--dsn-env",
        default="PROMETA_RUNTIME_DATABASE_URL",
        help="Environment variable containing a libpq PostgreSQL DSN",
    )
    args = parser.parse_args(argv)
    dsn = os.environ.get(args.dsn_env)
    if not dsn:
        parser.error("%s is not set" % args.dsn_env)
    try:
        report = check_postgres_runtime_compatibility(dsn)
    except RuntimePersistenceError as exc:
        parser.error(exc.code)
    print(json.dumps(report.as_dict(), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "RUNTIME_POSTGRES_SCHEMA_VERSION",
    "RUNTIME_POSTGRES_COMPATIBILITY_VERSION",
    "RUNTIME_POSTGRES_MIN_SCHEMA_VERSION",
    "RUNTIME_POSTGRES_MAX_SCHEMA_VERSION",
    "RuntimePersistenceError",
    "RuntimePostgresCompatibilityReport",
    "RuntimePostgresVerificationReport",
    "RuntimeStateRecord",
    "install_postgres_runtime_schema",
    "check_postgres_runtime_compatibility",
    "verify_postgres_runtime_integrity",
    "PostgresAdmissionReplayStore",
    "PostgresRuntimeActivationStore",
    "PostgresRuntimeReceiptOutbox",
    "PostgresRuntimeReleaseCache",
    "PostgresRuntimeTaskStore",
    "PostgresRuntimeStateStore",
    "main",
    "compatibility_main",
    "verify_main",
]
