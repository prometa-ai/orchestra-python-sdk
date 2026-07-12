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
from typing import Any, Callable, Mapping, Optional, Sequence

from .admission import RuntimeActivationResult
from .control_plane import RuntimeReleaseHandoff
from .receipts import RuntimeReceiptOutboxItem


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
    """Install the fixed replay/state schema in a tenant-owned database."""

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
                            SELECT receipt_id
                            FROM prometa_runtime_receipt_outbox
                            WHERE tenant_id = %s
                              AND status = 'pending'
                              AND available_at <= CURRENT_TIMESTAMP
                              AND (
                                  leased_until IS NULL
                                  OR leased_until <= CURRENT_TIMESTAMP
                              )
                            ORDER BY created_at, sequence, receipt_id
                            FOR UPDATE SKIP LOCKED
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


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "RuntimePersistenceError",
    "RuntimeStateRecord",
    "install_postgres_runtime_schema",
    "PostgresAdmissionReplayStore",
    "PostgresRuntimeActivationStore",
    "PostgresRuntimeReceiptOutbox",
    "PostgresRuntimeReleaseCache",
    "PostgresRuntimeStateStore",
    "main",
]
