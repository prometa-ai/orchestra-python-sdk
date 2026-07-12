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
    compose = (ROOT / "deploy/reference-runtime/compose.yaml").read_text(
        encoding="utf-8"
    )

    assert 'appVersion: "%s"' % version in chart
    assert 'org.opencontainers.image.version="%s"' % version in dockerfile
    assert '"prometa-sdk[runtime-host]==%s"' % version in dockerfile
    assert "prometa-runtime-host:%s" % version in compose


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


def test_runtime_examples_enable_bounded_payload_free_task_recovery():
    for name in ("config.example.json", "config.pull.example.json"):
        document = (ROOT / "deploy/reference-runtime" / name).read_text(
            encoding="utf-8"
        )
        assert '"taskRecovery"' in document
        assert '"leaseSeconds": 90' in document
        assert '"maxAttempts": 3' in document
        assert '"historyLimit": 50' in document


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
