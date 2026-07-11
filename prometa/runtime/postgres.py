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
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Mapping, Optional, Sequence


_MAX_IDENTIFIER_LENGTH = 128
_MAX_STATE_BYTES = 1_048_576

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

INSERT INTO prometa_runtime_schema_migrations (version)
VALUES (1)
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


def _validate_identifier(name: str, value: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or value != value.strip()
        or len(value) > _MAX_IDENTIFIER_LENGTH
    ):
        raise ValueError(
            "%s must be a trimmed string of 1-%d characters"
            % (name, _MAX_IDENTIFIER_LENGTH)
        )
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
    "PostgresRuntimeStateStore",
    "main",
]
