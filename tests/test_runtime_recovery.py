"""Deployment-class recovery and logical restore proof."""

from __future__ import annotations

import json
import os
import re
import shutil
import socket
import socketserver
import stat
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional

import pytest

from prometa.runtime import (
    PostgresRuntimeTaskStore,
    canonical_payload_digest,
    install_postgres_runtime_schema,
    verify_postgres_runtime_integrity,
)


ROOT = Path(__file__).parent.parent
BACKUP_SCRIPT = (
    ROOT / "deploy/reference-runtime/operations/backup-postgres.sh"
)
RESTORE_SCRIPT = (
    ROOT / "deploy/reference-runtime/operations/restore-postgres.sh"
)
HOST_PROCESS = ROOT / "tests/runtime_recovery_host_process.py"
API_TOKEN = "runtime-recovery-token-0123456789abcdef"


def _postgres_client_directory() -> Optional[Path]:
    directories = set()
    for command in ("pg_dump", "pg_restore", "psql"):
        path = shutil.which(command)
        if path is not None:
            directories.add(Path(path).resolve().parent)
    directories.update(
        path.parent
        for pattern in (
            "/opt/homebrew/Cellar/postgresql@*/**/bin/pg_dump",
            "/usr/lib/postgresql/*/bin/pg_dump",
        )
        for path in Path("/").glob(pattern.lstrip("/"))
    )
    candidates = []
    for directory in directories:
        if not all((directory / command).is_file() for command in (
            "pg_dump",
            "pg_restore",
            "psql",
        )):
            continue
        version = subprocess.run(
            [str(directory / "pg_dump"), "--version"],
            check=False,
            capture_output=True,
            text=True,
        )
        match = re.search(r"PostgreSQL\)\s+(\d+)", version.stdout)
        if version.returncode == 0 and match is not None:
            candidates.append((int(match.group(1)), directory))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


POSTGRES_CLIENT_DIRECTORY = _postgres_client_directory()


def _pg_environment(parameters, database_name: str) -> dict[str, str]:
    environment = os.environ.copy()
    if POSTGRES_CLIENT_DIRECTORY is not None:
        environment["PATH"] = "%s%s%s" % (
            POSTGRES_CLIENT_DIRECTORY,
            os.pathsep,
            environment.get("PATH", ""),
        )
    mapping = {
        "host": "PGHOST",
        "port": "PGPORT",
        "user": "PGUSER",
        "password": "PGPASSWORD",
        "sslmode": "PGSSLMODE",
    }
    for source, target in mapping.items():
        value = parameters.get(source)
        if value is not None:
            environment[target] = str(value)
        else:
            environment.pop(target, None)
    environment["PGDATABASE"] = database_name
    environment["PGCONNECT_TIMEOUT"] = "5"
    return environment


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _start_host_process(
    *,
    dsn: str,
    tenant_id: str,
    runtime_id: str,
    model_gateway_url: str,
    port: int,
) -> subprocess.Popen:
    environment = os.environ.copy()
    environment.update(
        {
            "RECOVERY_DATABASE_DSN": dsn,
            "RECOVERY_TENANT_ID": tenant_id,
            "RECOVERY_RUNTIME_ID": runtime_id,
            "RECOVERY_MODEL_GATEWAY_URL": model_gateway_url,
            "RECOVERY_API_TOKEN": API_TOKEN,
            "RECOVERY_HOST_PORT": str(port),
        }
    )
    process = subprocess.Popen(
        [sys.executable, str(HOST_PROCESS)],
        cwd=ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 8
    health_url = "http://127.0.0.1:%d/healthz" % port
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(
                "runtime host exited during startup: %s"
                % (stdout + stderr)[-1000:]
            )
        try:
            with urllib.request.urlopen(health_url, timeout=0.2) as response:
                if response.status == 200:
                    return process
        except (OSError, urllib.error.URLError):
            time.sleep(0.05)
    process.kill()
    stdout, stderr = process.communicate(timeout=2)
    raise AssertionError(
        "runtime host did not become healthy: %s" % (stdout + stderr)[-1000:]
    )


def _stop_host_process(process: Optional[subprocess.Popen]) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.communicate(timeout=4)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=2)


def _host_request(port: int, path: str, payload=None):
    headers = {"authorization": "Bearer %s" % API_TOKEN}
    data = None
    method = "GET"
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["content-type"] = "application/json"
        method = "POST"
    request = urllib.request.Request(
        "http://127.0.0.1:%d%s" % (port, path),
        data=data,
        method=method,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=6) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def _start_model_gateway(*, block_first: bool):
    calls = []
    first_entered = threading.Event()
    release_first = threading.Event()

    class GatewayHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return None

        def do_POST(self):
            length = int(self.headers["content-length"])
            request = json.loads(self.rfile.read(length))
            calls.append(request)
            call_number = len(calls)
            if block_first and call_number == 1:
                first_entered.set()
                release_first.wait(timeout=8)
            response = json.dumps(
                {
                    "model": "golden-model",
                    "choices": [
                        {
                            "message": {
                                "content": {"answer": "Recovered safely."}
                            },
                            "finish_reason": "stop",
                        }
                    ],
                }
            ).encode("utf-8")
            try:
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)
            except (BrokenPipeError, ConnectionResetError):
                return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), GatewayHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, calls, first_entered, release_first


def _stop_model_gateway(server, thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def _cleanup_tenant(dsn: str, tenant_id: str) -> None:
    import psycopg
    from psycopg import sql

    tables = (
        "prometa_runtime_task",
        "prometa_runtime_request_state",
        "prometa_runtime_receipt_outbox",
        "prometa_runtime_release_cache",
        "prometa_runtime_release_activation",
        "prometa_runtime_bundle_identity",
        "prometa_runtime_admission_replay",
    )
    with psycopg.connect(dsn) as connection:
        for table in tables:
            connection.execute(
                sql.SQL("DELETE FROM {} WHERE tenant_id = %s").format(
                    sql.Identifier(table)
                ),
                (tenant_id,),
            )


class _DatabaseProxy(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, upstream):
        self.upstream = upstream
        self.enabled = threading.Event()
        self.enabled.set()
        super().__init__(address, _DatabaseProxyHandler)


class _DatabaseProxyHandler(socketserver.BaseRequestHandler):
    def handle(self):
        if not self.server.enabled.is_set():
            return None
        try:
            upstream = socket.create_connection(self.server.upstream, timeout=2)
        except OSError:
            return None

        def copy(source, target):
            try:
                while True:
                    data = source.recv(65_536)
                    if not data:
                        break
                    target.sendall(data)
            except OSError:
                pass
            try:
                target.shutdown(socket.SHUT_WR)
            except OSError:
                pass

        response = threading.Thread(
            target=copy,
            args=(upstream, self.request),
            daemon=True,
        )
        response.start()
        copy(self.request, upstream)
        response.join(timeout=2)
        upstream.close()


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN"),
    reason="PROMETA_RUNTIME_TEST_POSTGRES_DSN is not configured",
)
def test_killed_host_reclaims_expired_task_in_fresh_process() -> None:
    dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    install_postgres_runtime_schema(dsn)
    tenant_id = "kill-%s" % uuid.uuid4().hex
    runtime_id = "runtime-kill"
    request_id = "request-kill"
    port = _free_port()
    gateway, gateway_thread, calls, entered, release = _start_model_gateway(
        block_first=True
    )
    gateway_url = "http://127.0.0.1:%d" % gateway.server_address[1]
    first = replacement = None
    request_errors = []
    request_thread = None
    try:
        first = _start_host_process(
            dsn=dsn,
            tenant_id=tenant_id,
            runtime_id=runtime_id,
            model_gateway_url=gateway_url,
            port=port,
        )

        def invoke_first():
            try:
                _host_request(
                    port,
                    "/v1/runtime/execute",
                    {
                        "requestId": request_id,
                        "input": {"question": "Recover this request"},
                    },
                )
            except Exception as exc:
                request_errors.append(type(exc).__name__)

        request_thread = threading.Thread(target=invoke_first)
        request_thread.start()
        assert entered.wait(timeout=3)
        first.kill()
        first.communicate(timeout=3)
        release.set()
        request_thread.join(timeout=3)
        assert request_errors

        store = PostgresRuntimeTaskStore(
            dsn,
            tenant_id=tenant_id,
            runtime_id=runtime_id,
        )
        orphan = store.get(request_id)
        assert orphan is not None
        assert orphan.record.status == "running"
        assert orphan.record.attempt == 1

        replacement = _start_host_process(
            dsn=dsn,
            tenant_id=tenant_id,
            runtime_id=runtime_id,
            model_gateway_url=gateway_url,
            port=port,
        )
        assert orphan.record.lease_expires_at is not None
        remaining = (
            orphan.record.lease_expires_at - datetime.now(timezone.utc)
        ).total_seconds()
        if remaining > 0:
            time.sleep(remaining + 0.2)
        status, response = _host_request(
            port,
            "/v1/runtime/execute",
            {
                "requestId": request_id,
                "input": {"question": "Recover this request"},
            },
        )
        assert status == 200
        assert response["output"] == {"answer": "Recovered safely."}
        status, task = _host_request(
            port, "/v1/runtime/tasks/%s" % request_id
        )
        assert status == 200
        assert task["status"] == "completed"
        assert task["attempt"] == 2
        assert [event["transition"] for event in task["lifecycle"]] == [
            "claimed",
            "recovered",
            "completed",
        ]
        assert len(calls) == 2
        assert "Recover this request" not in repr(task)
    finally:
        release.set()
        if request_thread is not None:
            request_thread.join(timeout=1)
        _stop_host_process(first)
        _stop_host_process(replacement)
        _stop_model_gateway(gateway, gateway_thread)
        _cleanup_tenant(dsn, tenant_id)


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN"),
    reason="PROMETA_RUNTIME_TEST_POSTGRES_DSN is not configured",
)
def test_database_path_outage_denies_before_model_and_recovers() -> None:
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    install_postgres_runtime_schema(dsn)
    parameters = conninfo_to_dict(dsn)
    tenant_id = "db-outage-%s" % uuid.uuid4().hex
    runtime_id = "runtime-db-outage"
    request_id = "request-db-outage"
    gateway, gateway_thread, calls, _, release = _start_model_gateway(
        block_first=False
    )
    gateway_url = "http://127.0.0.1:%d" % gateway.server_address[1]
    upstream = (
        parameters.get("host", "127.0.0.1"),
        int(parameters.get("port", 5432)),
    )
    proxy = _DatabaseProxy(("127.0.0.1", 0), upstream)
    proxy_thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    proxy_thread.start()
    proxied_parameters = dict(parameters)
    proxied_parameters["host"] = "127.0.0.1"
    proxied_parameters["port"] = str(proxy.server_address[1])
    proxied_dsn = make_conninfo(**proxied_parameters)
    port = _free_port()
    host = None
    try:
        host = _start_host_process(
            dsn=proxied_dsn,
            tenant_id=tenant_id,
            runtime_id=runtime_id,
            model_gateway_url=gateway_url,
            port=port,
        )
        proxy.enabled.clear()
        denied_status, denied = _host_request(
            port,
            "/v1/runtime/execute",
            {
                "requestId": request_id,
                "input": {"question": "Do not invoke the model"},
            },
        )
        assert denied_status == 503
        assert denied == {"error": {"code": "task_store_unavailable"}}
        assert calls == []

        proxy.enabled.set()
        status, response = _host_request(
            port,
            "/v1/runtime/execute",
            {
                "requestId": request_id,
                "input": {"question": "Do not invoke the model"},
            },
        )
        assert status == 200
        assert response["output"] == {"answer": "Recovered safely."}
        assert len(calls) == 1
        task = PostgresRuntimeTaskStore(
            dsn,
            tenant_id=tenant_id,
            runtime_id=runtime_id,
        ).get(request_id)
        assert task is not None
        assert task.record.status == "completed"
        assert task.record.attempt == 1
        assert [event.transition for event in task.events] == [
            "claimed",
            "completed",
        ]
        assert "Do not invoke the model" not in repr(task)
    finally:
        release.set()
        _stop_host_process(host)
        proxy.shutdown()
        proxy.server_close()
        proxy_thread.join(timeout=2)
        _stop_model_gateway(gateway, gateway_thread)
        _cleanup_tenant(dsn, tenant_id)


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN")
    or POSTGRES_CLIENT_DIRECTORY is None,
    reason="PostgreSQL DSN and client tools are required",
)
def test_logical_backup_restores_task_ledger_into_fresh_database(tmp_path) -> None:
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict

    dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    install_postgres_runtime_schema(dsn)
    parameters = conninfo_to_dict(dsn)
    source_database = parameters.get("dbname")
    assert source_database
    restore_database = "runtime_restore_%s" % uuid.uuid4().hex[:20]
    tenant_id = "backup-%s" % uuid.uuid4().hex
    runtime_id = "runtime-backup"
    request_id = "request-backup"
    store = PostgresRuntimeTaskStore(
        dsn,
        tenant_id=tenant_id,
        runtime_id=runtime_id,
    )
    claim = store.claim(
        request_id,
        input_digest=canonical_payload_digest({"question": "sensitive input"}),
        artifact_digest="sha256:" + "e" * 64,
        release_id="release-backup",
        deployment_id="deployment-backup",
        recoverable=True,
        max_attempts=3,
        lease_seconds=30,
    )
    store.complete(
        claim,
        output_digest=canonical_payload_digest({"answer": "sensitive output"}),
        model_name="tenant/backup-model",
        model_attempts=1,
        tool_calls=0,
        used_fallback=False,
    )
    source_report = verify_postgres_runtime_integrity(dsn)
    archive = tmp_path / "runtime.dump"
    admin_parameters = dict(parameters)
    admin_parameters["dbname"] = "postgres"

    with psycopg.connect(**admin_parameters, autocommit=True) as connection:
        connection.execute(
            sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(restore_database)
            )
        )

    try:
        backup_environment = _pg_environment(parameters, source_database)
        backup_environment["PROMETA_RUNTIME_BACKUP_FILE"] = str(archive)
        backup = subprocess.run(
            [str(BACKUP_SCRIPT)],
            check=False,
            capture_output=True,
            text=True,
            env=backup_environment,
            timeout=60,
        )
        assert backup.returncode == 0, backup.stderr
        assert archive.is_file()
        assert Path("%s.sha256" % archive).is_file()
        assert stat.S_IMODE(archive.stat().st_mode) == 0o600
        assert "sensitive" not in backup.stdout + backup.stderr
        assert parameters.get("password", "") not in backup.stdout + backup.stderr

        checksum_path = Path("%s.sha256" % archive)
        checksum = checksum_path.read_text(encoding="utf-8")
        checksum_path.write_text("0" * 64 + "  runtime.dump\n", encoding="utf-8")
        restore_environment = _pg_environment(parameters, restore_database)
        restore_environment.update(
            {
                "PROMETA_RUNTIME_RESTORE_FILE": str(archive),
                "PROMETA_RUNTIME_RESTORE_CONFIRM": "restore-tenant-runtime",
            }
        )
        denied = subprocess.run(
            [str(RESTORE_SCRIPT)],
            check=False,
            capture_output=True,
            text=True,
            env=restore_environment,
            timeout=60,
        )
        assert denied.returncode == 2
        assert "restore checksum mismatch" in denied.stderr
        checksum_path.write_text(checksum, encoding="utf-8")

        restored = subprocess.run(
            [str(RESTORE_SCRIPT)],
            check=False,
            capture_output=True,
            text=True,
            env=restore_environment,
            timeout=60,
        )
        assert restored.returncode == 0, restored.stderr
        assert "sensitive" not in restored.stdout + restored.stderr
        assert parameters.get("password", "") not in restored.stdout + restored.stderr

        restore_parameters = dict(parameters)
        restore_parameters["dbname"] = restore_database
        restore_dsn = psycopg.conninfo.make_conninfo(**restore_parameters)
        restored_report = verify_postgres_runtime_integrity(restore_dsn)
        assert restored_report.table_counts == source_report.table_counts
        restored_task = PostgresRuntimeTaskStore(
            restore_dsn,
            tenant_id=tenant_id,
            runtime_id=runtime_id,
        ).get(request_id)
        assert restored_task is not None
        assert restored_task.record.status == "completed"
        assert [event.transition for event in restored_task.events] == [
            "claimed",
            "completed",
        ]

        repeated = subprocess.run(
            [str(RESTORE_SCRIPT)],
            check=False,
            capture_output=True,
            text=True,
            env=restore_environment,
            timeout=60,
        )
        assert repeated.returncode == 2
        assert "target database is not empty" in repeated.stderr
    finally:
        with psycopg.connect(dsn) as connection:
            connection.execute(
                """
                DELETE FROM prometa_runtime_task
                WHERE tenant_id = %s AND runtime_id = %s AND request_id = %s
                """,
                (tenant_id, runtime_id, request_id),
            )
        with psycopg.connect(**admin_parameters, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    sql.Identifier(restore_database)
                )
            )
