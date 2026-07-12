import re
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
