import shutil
import stat
import subprocess
from pathlib import Path

from prometa import __version__ as RUNTIME_VERSION


ROOT = Path(__file__).parent.parent
VERIFIER = ROOT / "scripts/verify_runtime_release_contract.sh"
SYNCHRONIZER = ROOT / "scripts/sync_runtime_release_version.py"
ARTIFACT_WORKFLOW = ROOT / ".github/workflows/publish-runtime-artifacts.yml"
PUBLISHED_INSTALL_WORKFLOW = ROOT / ".github/workflows/runtime-published-install.yml"
PUBLISHED_UPGRADE_WORKFLOW = (
    ROOT / ".github/workflows/runtime-published-upgrade-rollback.yml"
)
RELEASE_WORKFLOW = ROOT / ".github/workflows/release.yml"
CHART_VERSION = next(
    line.split(":", 1)[1].strip()
    for line in (ROOT / "deploy/reference-runtime/chart/Chart.yaml")
    .read_text(encoding="utf-8")
    .splitlines()
    if line.startswith("version:")
)
CURRENT_TAG = "v" + RUNTIME_VERSION


def _run_verifier(tag: str, root: Path = ROOT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(VERIFIER), tag, str(root)],
        check=False,
        capture_output=True,
        text=True,
    )


def _release_fixture(tmp_path: Path) -> Path:
    for path in (
        Path("pyproject.toml"),
        Path("prometa/__init__.py"),
        Path("deploy/reference-runtime/Dockerfile"),
        Path("deploy/reference-runtime/Dockerfile.ubi"),
        Path("deploy/reference-runtime/compose.yaml"),
        Path("deploy/reference-runtime/chart/Chart.yaml"),
        Path("deploy/reference-runtime/chart/values.production.example.yaml"),
        Path("deploy/reference-runtime/chart/values.mcp.example.yaml"),
        Path("deploy/reference-runtime/config.example.json"),
        Path("deploy/reference-runtime/config.mcp.example.json"),
        Path("deploy/reference-runtime/config.pull.example.json"),
        Path("deploy/reference-runtime/topology-profiles.json"),
        Path("deploy/reference-runtime/topology-profiles.mcp.json"),
        Path("examples/runtime_kernel_quickstart.py"),
    ):
        destination = tmp_path / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / path, destination)
    return tmp_path


def test_runtime_release_contract_binds_current_tag_images_and_chart():
    assert VERIFIER.stat().st_mode & stat.S_IXUSR

    result = _run_verifier(CURRENT_TAG)

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "release_version=" + RUNTIME_VERSION,
        "release_tag=" + CURRENT_TAG,
        "chart_version=" + CHART_VERSION,
        "chart_app_version=" + RUNTIME_VERSION,
    ]


def test_runtime_release_contract_rejects_wrong_tag():
    major, minor, patch = (int(value) for value in RUNTIME_VERSION.split("."))
    wrong_tag = "v%d.%d.%d" % (major, minor, patch + 1)
    result = _run_verifier(wrong_tag)

    assert result.returncode == 2
    assert "does not match " + CURRENT_TAG in result.stderr


def test_runtime_release_contract_rejects_chart_app_drift(tmp_path):
    fixture = _release_fixture(tmp_path)
    chart = fixture / "deploy/reference-runtime/chart/Chart.yaml"
    chart.write_text(
        chart.read_text(encoding="utf-8").replace(
            f'appVersion: "{RUNTIME_VERSION}"', 'appVersion: "9.9.9"'
        ),
        encoding="utf-8",
    )

    result = _run_verifier(CURRENT_TAG, fixture)

    assert result.returncode == 2
    assert "release versions differ" in result.stderr


def test_runtime_release_contract_rejects_image_package_drift(tmp_path):
    fixture = _release_fixture(tmp_path)
    dockerfile = fixture / "deploy/reference-runtime/Dockerfile.ubi"
    dockerfile.write_text(
        dockerfile.read_text(encoding="utf-8").replace(
            f"prometa-sdk[runtime-host,runtime-mcp]=={RUNTIME_VERSION}",
            "prometa-sdk[runtime-host,runtime-mcp]==9.9.9",
        ),
        encoding="utf-8",
    )

    result = _run_verifier(CURRENT_TAG, fixture)

    assert result.returncode == 2
    assert "does not install" in result.stderr


def _use_legacy_debian_version_label(root: Path, version: str) -> None:
    dockerfile = root / "deploy/reference-runtime/Dockerfile"
    dockerfile.write_text(
        dockerfile.read_text(encoding="utf-8")
        .replace(f"ARG IMAGE_VERSION={version}\n", "")
        .replace(
            'org.opencontainers.image.version="${IMAGE_VERSION}"',
            f'org.opencontainers.image.version="{version}"',
        ),
        encoding="utf-8",
    )


def test_runtime_release_contract_accepts_only_v0180_legacy_debian_metadata(
    tmp_path,
):
    fixture = _release_fixture(tmp_path)
    synchronized = subprocess.run(
        [str(SYNCHRONIZER), "0.18.0", "--repository-root", str(fixture)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert synchronized.returncode == 0, synchronized.stderr
    _use_legacy_debian_version_label(fixture, "0.18.0")

    result = _run_verifier("v0.18.0", fixture)

    assert result.returncode == 0, result.stderr


def test_runtime_release_contract_rejects_legacy_metadata_for_future_tag(tmp_path):
    fixture = _release_fixture(tmp_path)
    synchronized = subprocess.run(
        [str(SYNCHRONIZER), "0.19.0", "--repository-root", str(fixture)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert synchronized.returncode == 0, synchronized.stderr
    _use_legacy_debian_version_label(fixture, "0.19.0")

    result = _run_verifier("v0.19.0", fixture)

    assert result.returncode == 2
    assert "does not default IMAGE_VERSION" in result.stderr


def test_runtime_release_version_synchronizer_updates_every_bound_surface(tmp_path):
    fixture = _release_fixture(tmp_path)

    result = subprocess.run(
        [str(SYNCHRONIZER), "0.19.0", "--repository-root", str(fixture)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    verified = _run_verifier("v0.19.0", fixture)
    assert verified.returncode == 0, verified.stderr
    assert "release_version=0.19.0" in verified.stdout


def test_runtime_release_version_synchronizer_advances_independent_chart(tmp_path):
    fixture = _release_fixture(tmp_path)

    result = subprocess.run(
        [
            str(SYNCHRONIZER),
            "0.18.1",
            "--chart-version",
            "0.3.2",
            "--repository-root",
            str(fixture),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    verified = _run_verifier("v0.18.1", fixture)
    assert verified.returncode == 0, verified.stderr
    assert "chart_version=0.3.2" in verified.stdout
    profile = (fixture / "deploy/reference-runtime/topology-profiles.json").read_text()
    assert '"runtimeVersion": "0.18.1"' in profile
    assert '"chartVersion": "0.3.2"' in profile


def test_runtime_release_version_synchronizer_rejects_partial_asset_shape(tmp_path):
    fixture = _release_fixture(tmp_path)
    compose = fixture / "deploy/reference-runtime/compose.yaml"
    compose.write_text(
        compose.read_text(encoding="utf-8").replace(
            f"prometa-runtime-host:{RUNTIME_VERSION}",
            "prometa-runtime-host:latest",
            1,
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [str(SYNCHRONIZER), "0.19.0", "--repository-root", str(fixture)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "expected 2 version match(es), found 1" in result.stderr


def test_runtime_artifact_workflow_is_exact_tag_signed_and_attested():
    workflow = ARTIFACT_WORKFLOW.read_text(encoding="utf-8")

    assert "source_tag:" in workflow
    assert workflow.count("ref: ${{ github.event.repository.default_branch }}") == 3
    assert workflow.count("ref: ${{ inputs.source_tag || github.ref }}") == 3
    assert workflow.count("git -C source describe --tags --exact-match HEAD") == 3
    assert "prometa-runtime-host-ubi9" in workflow
    assert "prometa-runtime-host" in workflow
    assert "platforms: linux/amd64" in workflow
    assert workflow.count("cosign sign") == 2
    assert workflow.count("--type cyclonedx") >= 4
    assert workflow.count("actions/attest-build-provenance@v2") == 2
    assert "pypi.org/pypi/prometa-sdk/%s/json" in workflow
    assert "exact PyPI package was unavailable after 10 minutes" in workflow
    assert "verify_run_id:" in workflow
    assert "actions: read" in workflow
    assert workflow.count("run-id: ${{ inputs.verify_run_id }}") == 2
    assert "inputs.verify_run_id != '' && needs.publish.result == 'skipped'" in workflow
    assert 'expected_tag="${{ inputs.source_tag || github.ref_name }}"' in workflow
    assert 'mkdir -p "$RUNNER_TEMP/pulled-chart"' in workflow
    assert "helm pull" in workflow
    assert "verify signed artifact set" in workflow


def test_published_install_workflow_consumes_immutable_release_artifacts():
    workflow = PUBLISHED_INSTALL_WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "packages: read" in workflow
    assert "ref: ${{ inputs.source_tag }}" in workflow
    assert "gh release download" in workflow
    assert "git -C source describe --tags --exact-match HEAD" in workflow
    assert "release-prometa-runtime-host-ubi9.json" in workflow
    assert "cosign verify" in workflow
    assert "cosign verify-attestation" in workflow
    assert "PROMETA_RUNTIME_TOPOLOGY_ARTIFACT_MODE: published" in workflow
    assert "PROMETA_RUNTIME_TOPOLOGY_CHART_SHA256" in workflow
    assert "PROMETA_RUNTIME_TOPOLOGY_REGISTRY_CONFIG" in workflow
    assert 'echo "DOCKER_CONFIG=$RUNNER_TEMP/docker-config"' in workflow
    assert "PROMETA_RUNTIME_TOPOLOGY_CLUSTER: runtime-published-install" in workflow
    assert "reference-profile-not-production-certification" in workflow
    assert "not OpenShift production certification" in workflow


def test_published_upgrade_workflow_consumes_two_immutable_release_sets():
    workflow = PUBLISHED_UPGRADE_WORKFLOW.read_text(encoding="utf-8")

    assert "baseline_tag:" in workflow
    assert "target_tag:" in workflow
    assert "resolve-published-release.sh" in workflow
    assert 'PROMETA_RUNTIME_KEEP_TOPOLOGY_CLUSTER: "true"' in workflow
    assert "PROMETA_RUNTIME_PUBLISHED_BASELINE_DESCRIPTOR" in workflow
    assert "PROMETA_RUNTIME_PUBLISHED_TARGET_DESCRIPTOR" in workflow
    assert "published-upgrade-rollback.sh" in workflow
    assert "published-runtime-upgrade-forward-rollback" in workflow
    assert "[$baseline, $target, $baseline]" in workflow
    assert "not OpenShift production certification" in workflow


def test_release_dispatches_package_and_runtime_artifacts_from_same_tag():
    workflow = RELEASE_WORKFLOW.read_text(encoding="utf-8")

    assert 'gh workflow run publish.yml --ref "v$NEW"' in workflow
    assert 'python scripts/sync_runtime_release_version.py "$NEW"' in workflow
    assert '--chart-version "$NEW_CHART"' in workflow
    assert "chart_version:" in workflow
    assert 'echo "chart_version=$NEW_CHART_VERSION"' in workflow
    assert "deploy/reference-runtime/topology-profiles.json \\" in workflow
    assert 'scripts/verify_runtime_release_contract.sh "v$NEW"' in workflow
    assert "gh workflow run publish-runtime-artifacts.yml" in workflow
    assert '--ref main \\\n            -f source_tag="v$NEW"' in workflow
