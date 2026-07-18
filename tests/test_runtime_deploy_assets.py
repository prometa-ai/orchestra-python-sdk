import json
import re
import stat
import subprocess
from pathlib import Path


ROOT = Path(__file__).parent.parent


def _project_version() -> str:
    project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"$', project, re.MULTILINE)
    assert match is not None
    return match.group(1)


def test_runtime_deploy_assets_follow_package_version():
    version = _project_version()
    chart = (ROOT / "deploy/reference-runtime/chart/Chart.yaml").read_text(
        encoding="utf-8"
    )
    dockerfile = (ROOT / "deploy/reference-runtime/Dockerfile").read_text(
        encoding="utf-8"
    )
    ubi_dockerfile = (ROOT / "deploy/reference-runtime/Dockerfile.ubi").read_text(
        encoding="utf-8"
    )
    compose = (ROOT / "deploy/reference-runtime/compose.yaml").read_text(
        encoding="utf-8"
    )

    assert 'appVersion: "%s"' % version in chart
    assert "ARG IMAGE_VERSION=%s" % version in dockerfile
    assert '"prometa-sdk[runtime-host,runtime-mcp]==%s"' % version in dockerfile
    assert "ARG IMAGE_VERSION=%s" % version in ubi_dockerfile
    assert '"prometa-sdk[runtime-host,runtime-mcp]==%s"' % version in ubi_dockerfile
    assert "prometa-runtime-host:%s" % version in compose


def test_runtime_ubi_image_and_openshift_profile_are_explicitly_bounded():
    root = ROOT / "deploy/reference-runtime"
    dockerfile = (root / "Dockerfile.ubi").read_text(encoding="utf-8")
    chart = root / "chart"
    values = (chart / "values.yaml").read_text(encoding="utf-8")
    profile = (chart / "values.openshift-production.yaml").read_text(
        encoding="utf-8"
    )
    helpers = (chart / "templates/_helpers.tpl").read_text(encoding="utf-8")
    deployment = (chart / "templates/deployment.yaml").read_text(encoding="utf-8")

    assert "registry.access.redhat.com/ubi9/python-312@sha256:" in dockerfile
    assert "registry.access.redhat.com/ubi9/python-312-minimal@sha256:" in dockerfile
    assert 'io.prometa.image.variant="ubi9"' in dockerfile
    assert "HOME=/tmp" in dockerfile
    assert "USER 1001" in dockerfile
    assert "UBI is an image-family choice" in dockerfile

    assert 'digest: ""' in values
    assert "productionProfile:" in values
    assert "profileId: orchestra-ocp-4.20-amd64-v1" in profile
    assert "namespaceDefaultDenyAcknowledged: false" in profile
    assert "modelGatewayApiKeyOptional: false" in profile
    assert "receiptApiKeyOptional: false" in profile
    assert "backup:\n  enabled: false" in profile
    assert "the OpenShift runtime profile requires" in helpers
    assert "a separate migration credential Secret" in helpers
    assert "prometa.io/production-profile-id" in deployment


def test_runtime_topology_profiles_follow_current_chart_version():
    root = ROOT / "deploy/reference-runtime"
    chart = (root / "chart/Chart.yaml").read_text(encoding="utf-8")
    match = re.search(r"^version: ([^ ]+)$", chart, re.MULTILINE)
    assert match is not None

    for name in ("topology-profiles.json", "topology-profiles.mcp.json"):
        profile = json.loads((root / name).read_text(encoding="utf-8"))
        assert profile["profiles"][0]["chartVersion"] == match.group(1)


def test_runtime_chart_references_external_sensitive_objects():
    chart = ROOT / "deploy/reference-runtime/chart"
    rendered_sources = "\n".join(
        path.read_text(encoding="utf-8") for path in (chart / "templates").glob("*")
    )
    values = (chart / "values.yaml").read_text(encoding="utf-8")

    assert "kind: Secret" not in rendered_sources
    assert 'existingSecret: ""' in values
    assert "runtimeConfig.existingSecret" in rendered_sources
    assert "credentials.existingSecret" in rendered_sources
    assert "readOnlyRootFilesystem: true" in values
    assert "automountServiceAccountToken: false" in values
    assert "controlPlaneApiKeyKey: control-plane-api-key" in values
    assert "ORCHESTRA_RUNTIME_CONTROL_PLANE_API_KEY" in rendered_sources
    assert "prometa-runtime-postgres-compatibility" in rendered_sources
    assert "pre-install,pre-upgrade,pre-rollback" in rendered_sources
    assert "prometa.io/database-maintenance" in rendered_sources
    assert "prometa.io/runtime-config-rollout-id" in rendered_sources


def test_runtime_examples_enable_bounded_payload_free_task_recovery():
    for name in ("config.example.json", "config.pull.example.json"):
        document = (ROOT / "deploy/reference-runtime" / name).read_text(
            encoding="utf-8"
        )
        assert '"taskRecovery"' in document
        assert '"leaseSeconds": 90' in document
        assert '"maxAttempts": 3' in document
        assert '"historyLimit": 50' in document


def test_runtime_mcp_examples_keep_credentials_external_and_egress_explicit():
    root = ROOT / "deploy/reference-runtime"
    document = json.loads(
        (root / "config.mcp.example.json").read_text(encoding="utf-8")
    )
    broker = document["mcpBroker"]
    assert "taskRecovery" not in document
    assert broker["servers"][0]["environment"] == "production"
    assert broker["policy"]["requireApprovalFor"] == [
        "write",
        "destructive",
    ]
    assert broker["policy"]["requireIdempotencyFor"] == [
        "write",
        "destructive",
    ]
    assert broker["credentialBindings"][0]["httpHeaders"] == {
        "Authorization": "MCP_INTEGRATION_AUTHORIZATION"
    }
    assert broker["egress"]["allowedHttpOrigins"] == [
        "https://mcp-integration.tenant-tools.svc:8443"
    ]
    assert "tenant-mcp-key" not in json.dumps(document)

    values = (root / "chart/values.mcp.example.yaml").read_text(
        encoding="utf-8"
    )
    assert "MCP_INTEGRATION_AUTHORIZATION" in values
    assert "tenant-runtime-mcp-credentials" in values
    assert "port: 8443" in values


def test_runtime_backup_restore_assets_are_fail_closed_and_secret_safe():
    root = ROOT / "deploy/reference-runtime"
    backup_path = root / "operations/backup-postgres.sh"
    restore_path = root / "operations/restore-postgres.sh"
    backup = backup_path.read_text(encoding="utf-8")
    restore = restore_path.read_text(encoding="utf-8")
    compose = (root / "compose.yaml").read_text(encoding="utf-8")

    assert backup_path.stat().st_mode & stat.S_IXUSR
    assert restore_path.stat().st_mode & stat.S_IXUSR
    assert "PROMETA_RUNTIME_DATABASE_URL" not in backup + restore
    assert "PGPASSWORD" not in backup + restore
    assert "umask 077" in backup
    assert "pg_restore --list" in backup
    assert "backup basename contains unsupported characters" in backup
    assert "restore-tenant-runtime" in restore
    assert "target database is not empty" in restore
    assert "restore checksum mismatch" in restore
    assert "restore basename contains unsupported characters" in restore
    assert "PROMETA_RUNTIME_EXPECTED_SCHEMA_VERSION:-6" in restore
    assert 'profiles: ["operations"]' in compose
    assert "runtime-backups:/backups" in compose

    denied = subprocess.run(
        [str(restore_path)],
        check=False,
        capture_output=True,
        text=True,
        env={},
    )
    assert denied.returncode == 2
    assert "PROMETA_RUNTIME_RESTORE_FILE is required" in denied.stderr


def test_runtime_chart_backup_is_optional_external_and_fail_closed():
    chart = ROOT / "deploy/reference-runtime/chart"
    values = (chart / "values.yaml").read_text(encoding="utf-8")
    helpers = (chart / "templates/_helpers.tpl").read_text(encoding="utf-8")
    cronjob = (chart / "templates/backup-cronjob.yaml").read_text(
        encoding="utf-8"
    )
    policy = (chart / "templates/networkpolicy.yaml").read_text(
        encoding="utf-8"
    )

    assert "backup:\n  enabled: false" in values
    assert "acknowledgeSensitiveData: false" in values
    assert "backup.acknowledgeSensitiveData must be true" in helpers
    assert "backup.networkPolicy.egress is required" in helpers
    assert "kind: CronJob" in cronjob
    assert "persistentVolumeClaim:" in cronjob
    assert ".Values.backup.existingSecret" in cronjob
    assert "kind: Secret" not in cronjob
    assert "app.kubernetes.io/component: backup" in policy


def test_runtime_chart_compatibility_hook_uses_target_image_and_migration_identity():
    chart = ROOT / "deploy/reference-runtime/chart"
    values = (chart / "values.yaml").read_text(encoding="utf-8")
    compatibility = (chart / "templates/compatibility-job.yaml").read_text(
        encoding="utf-8"
    )
    migration = (chart / "templates/migration-job.yaml").read_text(
        encoding="utf-8"
    )
    policy = (chart / "templates/networkpolicy.yaml").read_text(
        encoding="utf-8"
    )

    assert "compatibilityCheck: true" in values
    assert 'helm.sh/hook: pre-install,pre-upgrade,pre-rollback' in compatibility
    assert 'helm.sh/hook-weight: "10"' in compatibility
    assert "if .Values.migration.compatibilityCheck" in compatibility
    assert 'include "prometa-runtime.image"' in compatibility
    assert 'command: ["prometa-runtime-postgres-compatibility"]' in compatibility
    assert 'prometa.io/database-maintenance: "true"' in compatibility
    assert 'prometa.io/database-maintenance: "true"' in migration
    assert "pre-install,pre-upgrade,pre-rollback" in policy
    assert "helm.sh/hook-delete-policy: before-hook-creation" in policy
    assert "before-hook-creation,hook-succeeded" not in policy
    assert "or .Values.migration.enabled .Values.migration.compatibilityCheck" in policy


def test_runtime_upgrade_baseline_is_explicit_and_drill_is_executable():
    root = ROOT / "deploy/reference-runtime"
    manifest = json.loads(
        (root / "compatibility-baselines.json").read_text(encoding="utf-8")
    )
    drill = root / "ci/upgrade-rollback-drill.sh"

    assert manifest == {
        "contractVersion": 1,
        "baselines": [
            {
                "name": "phase3-chart-0.1.0",
                "gitRef": "51e2faa",
                "chartVersion": "0.1.0",
                "runtimeVersion": "0.18.0",
                "postgresSchemaVersion": 2,
                "artifactStatus": "source-baseline-not-published-release",
            }
        ],
    }
    assert drill.stat().st_mode & stat.S_IXUSR
