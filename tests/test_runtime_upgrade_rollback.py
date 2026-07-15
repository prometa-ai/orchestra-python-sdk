"""Real PostgreSQL source-baseline upgrade and prior-bundle rollback drill."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional, Tuple

import pytest

pytest.importorskip("cryptography")
pytest.importorskip("psycopg")

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from prometa.runtime import (
    RUNTIME_POSTGRES_SCHEMA_VERSION,
    check_postgres_runtime_compatibility,
    install_postgres_runtime_schema,
)


ROOT = Path(__file__).parent.parent
HOST_PROCESS = ROOT / "tests/runtime_upgrade_host_process.py"
API_TOKEN = "runtime-upgrade-token-0123456789abcdef"
ORG_ID = "org-upgrade-drill"
TENANT_ID = "tenant-upgrade-drill"
RUNTIME_ID = "runtime-upgrade-drill"
BUNDLE_ISSUER = "https://orchestra.example.test"
PROMOTION_ISSUER = "https://orchestra.example.test/promotion"
BUNDLE_KEY_ID = "upgrade-drill-bundle-key"
PROMOTION_KEY_ID = "upgrade-drill-promotion-key"


def _canonical(value) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _instant(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _public_key(private_key: Ed25519PrivateKey) -> str:
    der = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return base64.b64encode(der).decode("ascii")


def _sign(private_key: Ed25519PrivateKey, payload: str) -> str:
    return base64.b64encode(private_key.sign(payload.encode("utf-8"))).decode(
        "ascii"
    )


def _content(label: str, manifest_version: int) -> dict:
    primary = {
        "name": "Primary",
        "provider": "inference-engine",
        "modelName": "golden-model",
        "role": "primary",
        "temperature": 0.0,
        "maxOutputTokens": 128,
        "structuredOutput": True,
    }
    return {
        "schemaVersion": 1,
        "manifest": {
            "id": "manifest-upgrade-drill",
            "name": "Upgrade Drill",
            "description": "Tenant runtime compatibility drill",
            "version": manifest_version,
            "status": "published",
            "agentId": "agent-upgrade-drill",
            "solutionId": "solution-upgrade-drill",
            "solutionName": "Upgrade Drill",
            "deployable": True,
        },
        "systemPrompt": "Serve release %s." % label,
        "models": [primary],
        "primaryModel": primary,
        "topology": {"pattern": "single-react", "maxIterations": 1},
        "tools": [],
        "skills": [],
        "knowledge": [],
        "memory": [],
        "subAgents": [],
        "workflows": [],
        "guardrails": [],
        "identity": None,
        "triggers": [],
        "evaluation": [],
        "mcpServers": [],
        "requiredScopes": [],
        "grantedScopes": [],
        "readiness": {
            "quality": 100,
            "security": 100,
            "maturity": 80,
            "productivity": 60,
        },
        "runtimeContract": {
            "contractVersion": 1,
            "requiredCapabilities": [
                "evidence.emit.v1",
                "model.invoke.v1",
                "schema.validate.v1",
            ],
            "inputSchema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "question": {"type": "string", "minLength": 1}
                },
                "required": ["question"],
                "additionalProperties": False,
            },
            "outputSchema": {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {"answer": {"type": "string", "minLength": 1}},
                "required": ["answer"],
                "additionalProperties": False,
            },
        },
    }


def _bundle(
    label: str,
    manifest_version: int,
    private_key: Ed25519PrivateKey,
    now: datetime,
) -> dict:
    content = _content(label, manifest_version)
    canonical_content = _canonical(content)
    digest = "sha256:" + hashlib.sha256(
        canonical_content.encode("utf-8")
    ).hexdigest()
    claims = {
        "envelopeVersion": 1,
        "issuer": BUNDLE_ISSUER,
        "keyId": BUNDLE_KEY_ID,
        "orgId": ORG_ID,
        "audience": "prometa-runtime",
        "targetEnvironment": "prod",
        "subject": "agent-manifest:manifest-upgrade-drill:v%d" % manifest_version,
        "jti": "bundle-upgrade-%s" % label.lower(),
        "artifactDigest": digest,
        "contentCanonical": canonical_content,
        "issuedAt": _instant(now - timedelta(minutes=2)),
        "notBefore": _instant(now - timedelta(minutes=2)),
        "expiresAt": _instant(now + timedelta(hours=1)),
        "offlineLeaseExpiresAt": _instant(now + timedelta(minutes=30)),
    }
    payload = _canonical(claims)
    return {
        "content": content,
        "algorithm": "ed25519",
        "envelopeVersion": 1,
        "envelopeCanonicalization": "signed-payload-json-v1",
        "signedPayload": payload,
        "envelopeSignature": _sign(private_key, payload),
        "artifactDigest": digest,
        "issuer": BUNDLE_ISSUER,
        "keyId": BUNDLE_KEY_ID,
        "signed": True,
    }


def _attestation(
    bundle: dict,
    *,
    label: str,
    manifest_version: int,
    release_id: str,
    deployment_id: str,
    private_key: Ed25519PrivateKey,
    now: datetime,
) -> dict:
    normalized_label = label.lower()
    artifact_label = "a" if normalized_label == "a-rollback" else normalized_label
    attestation_id = "attestation-upgrade-%s" % normalized_label
    claims = {
        "artifactType": "orchestra.promotion-attestation",
        "attestationVersion": 1,
        "issuer": PROMOTION_ISSUER,
        "keyId": PROMOTION_KEY_ID,
        "subject": "promotion-attestation:%s" % attestation_id,
        "orgId": ORG_ID,
        "audience": "prometa-runtime-admission",
        "targetEnvironment": "prod",
        "artifactId": "artifact-upgrade-%s" % artifact_label,
        "artifactDigest": bundle["artifactDigest"],
        "manifestId": "manifest-upgrade-drill",
        "manifestVersion": manifest_version,
        "agentId": "agent-upgrade-drill",
        "decisionId": "decision-upgrade-%s" % label.lower(),
        "decisionAllow": True,
        "gateStage": "prod",
        "policySetDigest": "sha256:" + "b" * 64,
        "evidenceDigest": "sha256:" + "c" * 64,
        "decisionEvaluatedAt": _instant(now - timedelta(minutes=3)),
        "decisionValidUntil": _instant(now + timedelta(minutes=30)),
        "requestedRuntime": "tenant-runtime",
        "releaseId": release_id,
        "deploymentId": deployment_id,
        "approvals": [],
        "issuedAt": _instant(now - timedelta(minutes=1)),
        "notBefore": _instant(now - timedelta(minutes=1)),
        "expiresAt": _instant(now + timedelta(minutes=20)),
        "offlineLeaseExpiresAt": _instant(now + timedelta(minutes=15)),
        "jti": "promotion-upgrade-%s" % label.lower(),
        "revocationRef": "urn:prometa:promotion-attestation:%s"
        % attestation_id,
    }
    payload = _canonical(claims)
    return {
        "attestationId": attestation_id,
        "attestationVersion": 1,
        "algorithm": "ed25519",
        "canonicalization": "signed-payload-json-v1",
        "issuer": PROMOTION_ISSUER,
        "keyId": PROMOTION_KEY_ID,
        "signedPayload": payload,
        "signature": _sign(private_key, payload),
        "signed": True,
        "authorization": {
            "artifactId": claims["artifactId"],
            "artifactDigest": claims["artifactDigest"],
            "decisionId": claims["decisionId"],
            "releaseId": release_id,
            "deploymentId": deployment_id,
            "targetEnvironment": "prod",
            "requestedRuntime": "tenant-runtime",
            "expiresAt": claims["expiresAt"],
            "offlineLeaseExpiresAt": claims["offlineLeaseExpiresAt"],
        },
    }


def _config(
    bundle: dict,
    attestation: dict,
    *,
    release_id: str,
    deployment_id: str,
    gateway_url: str,
    bundle_public_key: str,
    promotion_public_key: str,
) -> dict:
    return {
        "configVersion": 1,
        "tenantId": TENANT_ID,
        "runtimeId": RUNTIME_ID,
        "runtimeVersion": "0.18.0",
        "orgId": ORG_ID,
        "environment": "prod",
        "releaseId": release_id,
        "deploymentId": deployment_id,
        "runtimeTarget": "tenant-runtime",
        "bundle": bundle,
        "promotionAttestation": attestation,
        "bundleTrust": [
            {
                "issuer": BUNDLE_ISSUER,
                "keyId": BUNDLE_KEY_ID,
                "publicKeySpkiDerBase64": bundle_public_key,
                "allowedOrgIds": [ORG_ID],
                "allowedAudiences": ["prometa-runtime"],
                "allowedEnvironments": ["prod"],
            }
        ],
        "promotionTrust": [
            {
                "issuer": PROMOTION_ISSUER,
                "keyId": PROMOTION_KEY_ID,
                "publicKeySpkiDerBase64": promotion_public_key,
                "allowedOrgIds": [ORG_ID],
                "allowedAudiences": ["prometa-runtime-admission"],
                "allowedEnvironments": ["prod"],
            }
        ],
        "modelGateway": {"baseUrl": gateway_url},
        "requestTimeoutSeconds": 5,
        "maxRequestBytes": 4096,
    }


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _start_gateway():
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return None

        def do_POST(self):
            length = int(self.headers["content-length"])
            request = json.loads(self.rfile.read(length))
            calls.append(request)
            response = json.dumps(
                {
                    "model": "golden-model",
                    "choices": [
                        {
                            "message": {
                                "content": {"answer": "served by upgrade drill"}
                            },
                            "finish_reason": "stop",
                        }
                    ],
                }
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, calls


def _start_host(
    source: Path,
    config_path: Path,
    *,
    dsn: str,
    now: datetime,
) -> Tuple[subprocess.Popen, int]:
    port = _free_port()
    environment = os.environ.copy()
    environment.update(
        {
            "PYTHONPATH": str(source),
            "DRILL_RUNTIME_CONFIG": str(config_path),
            "DRILL_RUNTIME_PORT": str(port),
            "DRILL_NOW": _instant(now),
            "PROMETA_RUNTIME_DATABASE_URL": dsn,
            "PROMETA_RUNTIME_API_TOKEN": API_TOKEN,
        }
    )
    process = subprocess.Popen(
        [sys.executable, str(HOST_PROCESS)],
        cwd=source,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(
                "runtime host exited during drill startup: %s"
                % (stdout + stderr)[-2000:]
            )
        try:
            with urllib.request.urlopen(
                "http://127.0.0.1:%d/readyz" % port, timeout=0.2
            ) as response:
                if response.status == 200:
                    return process, port
        except (OSError, urllib.error.URLError):
            time.sleep(0.05)
    process.kill()
    stdout, stderr = process.communicate(timeout=2)
    raise AssertionError(
        "runtime host did not become ready: %s" % (stdout + stderr)[-2000:]
    )


def _stop_host(process: Optional[subprocess.Popen]) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=2)


def _execute(port: int, request_id: str) -> dict:
    request = urllib.request.Request(
        "http://127.0.0.1:%d/v1/runtime/execute" % port,
        method="POST",
        data=json.dumps(
            {"requestId": request_id, "input": {"question": "status?"}}
        ).encode("utf-8"),
        headers={
            "authorization": "Bearer %s" % API_TOKEN,
            "content-type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=8) as response:
        assert response.status == 200
        return json.loads(response.read())


def _run_baseline_schema_install(source: Path, dsn: str) -> None:
    environment = os.environ.copy()
    environment.update(
        {
            "PYTHONPATH": str(source),
            "PROMETA_RUNTIME_DATABASE_URL": dsn,
        }
    )
    subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import os; from prometa.runtime import "
                "install_postgres_runtime_schema; "
                "install_postgres_runtime_schema("
                "os.environ['PROMETA_RUNTIME_DATABASE_URL'])"
            ),
        ],
        cwd=source,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )


def _temporary_database(base_dsn: str):
    import psycopg

    parameters = conninfo_to_dict(base_dsn)
    database = "runtime_upgrade_%s" % uuid.uuid4().hex
    admin = dict(parameters)
    admin_database = admin.get("dbname") or admin.get("database") or "postgres"
    admin["dbname"] = admin_database
    admin.pop("database", None)
    with psycopg.connect(make_conninfo(**admin), autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    target = dict(parameters)
    target["dbname"] = database
    target.pop("database", None)
    return database, make_conninfo(**target), make_conninfo(**admin)


def _drop_database(admin_dsn: str, database: str) -> None:
    import psycopg

    with psycopg.connect(admin_dsn, autocommit=True) as connection:
        connection.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (database,),
        )
        connection.execute(
            sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database))
        )


@pytest.mark.skipif(
    not os.environ.get("PROMETA_RUNTIME_TEST_POSTGRES_DSN")
    or not os.environ.get("PROMETA_RUNTIME_UPGRADE_BASELINE"),
    reason="upgrade baseline and PostgreSQL DSN are required",
)
def test_source_baseline_upgrade_and_fresh_prior_bundle_rollback(tmp_path) -> None:
    import psycopg

    baseline = Path(os.environ["PROMETA_RUNTIME_UPGRADE_BASELINE"]).resolve()
    assert (baseline / "prometa/runtime/host.py").is_file()
    base_dsn = os.environ["PROMETA_RUNTIME_TEST_POSTGRES_DSN"]
    database, dsn, admin_dsn = _temporary_database(base_dsn)
    gateway, gateway_thread, calls = _start_gateway()
    process = None
    now = datetime.now(timezone.utc).replace(microsecond=0)
    bundle_key = Ed25519PrivateKey.generate()
    promotion_key = Ed25519PrivateKey.generate()
    bundle_a = _bundle("A", 7, bundle_key, now)
    bundle_b = _bundle("B", 8, bundle_key, now)
    release_specs = [
        ("A", bundle_a, 7, "release-a", "deployment-a", baseline),
        ("B", bundle_b, 8, "release-b", "deployment-b", ROOT),
        (
            "A-rollback",
            deepcopy(bundle_a),
            7,
            "release-a-rollback",
            "deployment-a-rollback",
            baseline,
        ),
    ]
    try:
        _run_baseline_schema_install(baseline, dsn)
        with psycopg.connect(dsn) as connection:
            versions = tuple(
                row[0]
                for row in connection.execute(
                    "SELECT version FROM prometa_runtime_schema_migrations "
                    "ORDER BY version"
                ).fetchall()
            )
        assert versions == (1, 2)

        for index, (
            label,
            bundle,
            manifest_version,
            release_id,
            deployment_id,
            source,
        ) in enumerate(release_specs):
            if index == 1:
                install_postgres_runtime_schema(dsn)
                report = check_postgres_runtime_compatibility(dsn)
                assert report.migration_versions == (1, 2, 3, 4, 5, 6)
            attestation = _attestation(
                bundle,
                label=label,
                manifest_version=manifest_version,
                release_id=release_id,
                deployment_id=deployment_id,
                private_key=promotion_key,
                now=now,
            )
            config = _config(
                bundle,
                attestation,
                release_id=release_id,
                deployment_id=deployment_id,
                gateway_url="http://127.0.0.1:%d" % gateway.server_address[1],
                bundle_public_key=_public_key(bundle_key),
                promotion_public_key=_public_key(promotion_key),
            )
            config_path = tmp_path / ("config-%s.json" % label.lower())
            config_path.write_text(_canonical(config), encoding="utf-8")
            process, port = _start_host(source, config_path, dsn=dsn, now=now)
            response = _execute(port, "request-%s" % label.lower())
            assert response["output"] == {"answer": "served by upgrade drill"}
            _stop_host(process)
            process = None

        with psycopg.connect(dsn) as connection:
            rows = connection.execute(
                """
                SELECT deployment_id, release_id, artifact_digest,
                       bundle_jti, promotion_jti
                FROM prometa_runtime_release_activation
                WHERE tenant_id = %s AND runtime_id = %s
                ORDER BY deployment_id
                """,
                (TENANT_ID, RUNTIME_ID),
            ).fetchall()
        by_deployment = {row[0]: row for row in rows}
        assert set(by_deployment) == {
            "deployment-a",
            "deployment-b",
            "deployment-a-rollback",
        }
        first = by_deployment["deployment-a"]
        rollback = by_deployment["deployment-a-rollback"]
        assert first[2] == rollback[2] == bundle_a["artifactDigest"]
        assert first[3] == rollback[3] == "bundle-upgrade-a"
        assert first[4] != rollback[4]
        assert by_deployment["deployment-b"][2] == bundle_b["artifactDigest"]
        assert len(calls) == 3
        report_path = os.environ.get("PROMETA_RUNTIME_UPGRADE_REPORT")
        if report_path:
            report = {
                "contractVersion": 1,
                "passed": True,
                "baselineRef": os.environ.get(
                    "PROMETA_RUNTIME_UPGRADE_BASELINE_REF", "unspecified"
                ),
                "baselineSchemaVersion": 2,
                "targetSchemaVersion": RUNTIME_POSTGRES_SCHEMA_VERSION,
                "deploymentSequence": [
                    "deployment-a",
                    "deployment-b",
                    "deployment-a-rollback",
                ],
                "priorBundleDigestReused": first[2] == rollback[2],
                "freshPromotionIdentity": first[4] != rollback[4],
                "synchronousControlPlaneCalls": 0,
            }
            Path(report_path).write_text(
                _canonical(report) + "\n", encoding="utf-8"
            )
    finally:
        _stop_host(process)
        gateway.shutdown()
        gateway.server_close()
        gateway_thread.join(timeout=2)
        _drop_database(admin_dsn, database)
