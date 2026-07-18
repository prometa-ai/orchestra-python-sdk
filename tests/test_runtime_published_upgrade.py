import importlib.util
import json
import stat
from pathlib import Path

import pytest


ROOT = Path(__file__).parent.parent
DEPLOY = ROOT / "deploy/reference-runtime"
FIXTURE_PATH = DEPLOY / "ci/published_upgrade_fixture.py"
HARNESS = DEPLOY / "ci/published-upgrade-rollback.sh"
RESOLVER = DEPLOY / "ci/resolve-published-release.sh"


def _load_fixture():
    spec = importlib.util.spec_from_file_location(
        "published_upgrade_fixture_test", FIXTURE_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _descriptor(
    path: Path,
    *,
    release_tag: str,
    revision: str,
    runtime_version: str,
    image_digit: str,
    chart_version: str,
    chart_digit: str,
) -> dict:
    value = {
        "contractVersion": 1,
        "releaseTag": release_tag,
        "releaseRevision": revision,
        "runtimeVersion": runtime_version,
        "runtimeImage": (
            "ghcr.io/prometa-ai/orchestra-python-sdk/"
            "prometa-runtime-host-ubi9@sha256:" + image_digit * 64
        ),
        "chartVersion": chart_version,
        "chartPackage": "/tmp/prometa-runtime-%s.tgz" % chart_version,
        "chartPackageSha256": "sha256:" + chart_digit * 64,
        "chartOciReference": (
            "ghcr.io/prometa-ai/orchestra-python-sdk/charts/"
            "prometa-runtime@sha256:" + chart_digit * 64
        ),
    }
    path.write_text(json.dumps(value), encoding="utf-8")
    return value


def _observation_lines(sequence: dict, baseline: dict, target: dict) -> list[str]:
    lines = []
    for stage in ("baseline", "target", "rollback"):
        descriptor = target if stage == "target" else baseline
        for tenant in sequence["tenants"]:
            expected = next(
                value for value in tenant["stages"] if value["stage"] == stage
            )
            fields = (
                stage,
                tenant["tenant"],
                descriptor["runtimeImage"],
                descriptor["chartVersion"],
                str(sequence["runtimeReplicasPerTenant"]),
                "1,2,3,4,5,6",
                expected["deploymentId"],
                expected["releaseId"],
                expected["artifactDigest"],
                expected["bundleJti"],
                expected["promotionJti"],
            )
            lines.append("\t".join(fields))
    return lines


def test_published_upgrade_fixture_builds_fresh_forward_rollback(tmp_path):
    pytest.importorskip("cryptography")
    fixture = _load_fixture()
    output = tmp_path / "sequence"

    fixture.prepare_sequence(output, "0.18.0", "0.18.1")

    sequence = json.loads((output / "sequence.json").read_text())
    assert sequence["baselineRuntimeVersion"] == "0.18.0"
    assert sequence["targetRuntimeVersion"] == "0.18.1"
    assert sequence["runtimeReplicasPerTenant"] == 2
    assert [tenant["tenant"] for tenant in sequence["tenants"]] == ["a", "b"]
    for tenant in sequence["tenants"]:
        stages = {stage["stage"]: stage for stage in tenant["stages"]}
        assert set(stages) == {"baseline", "target", "rollback"}
        assert (
            stages["baseline"]["artifactDigest"] == stages["rollback"]["artifactDigest"]
        )
        assert stages["baseline"]["bundleJti"] == stages["rollback"]["bundleJti"]
        assert stages["baseline"]["promotionJti"] != stages["rollback"]["promotionJti"]
        assert stages["baseline"]["deploymentId"] != stages["rollback"]["deploymentId"]
        assert (
            stages["target"]["artifactDigest"] != stages["baseline"]["artifactDigest"]
        )
        for stage in stages.values():
            config_path = output / stage["configFile"]
            config = json.loads(config_path.read_text())
            assert stat.S_IMODE(config_path.stat().st_mode) == 0o600
            assert config["deploymentId"] == stage["deploymentId"]
            assert config["runtimeVersion"] == stage["runtimeVersion"]
            assert "controlPlanePull" not in config


@pytest.mark.parametrize(
    ("baseline", "target"),
    (("0.18.0", "0.18.0"), ("0.18.1", "0.18.0"), ("bad", "0.18.1")),
)
def test_published_upgrade_fixture_rejects_non_forward_versions(
    tmp_path, baseline, target
):
    fixture = _load_fixture()

    with pytest.raises(ValueError):
        fixture.prepare_sequence(tmp_path / "sequence", baseline, target)


def test_published_upgrade_report_binds_both_release_sets_and_is_payload_free(
    tmp_path,
):
    pytest.importorskip("cryptography")
    fixture = _load_fixture()
    assets = tmp_path / "sequence"
    fixture.prepare_sequence(assets, "0.18.0", "0.18.1")
    sequence = json.loads((assets / "sequence.json").read_text())
    baseline_path = tmp_path / "baseline.json"
    target_path = tmp_path / "target.json"
    baseline = _descriptor(
        baseline_path,
        release_tag="v0.18.0",
        revision="a" * 40,
        runtime_version="0.18.0",
        image_digit="b",
        chart_version="0.3.1",
        chart_digit="c",
    )
    target = _descriptor(
        target_path,
        release_tag="v0.18.1",
        revision="d" * 40,
        runtime_version="0.18.1",
        image_digit="e",
        chart_version="0.3.2",
        chart_digit="f",
    )
    observations = tmp_path / "observations.tsv"
    observations.write_text(
        "\n".join(_observation_lines(sequence, baseline, target)) + "\n",
        encoding="utf-8",
    )
    report = tmp_path / "report.json"

    fixture.write_report(
        assets / "sequence.json",
        observations,
        baseline_path,
        target_path,
        "v1.34.8+k3s1",
        report,
    )

    evidence = json.loads(report.read_text())
    assert evidence["passed"] is True
    assert evidence["evidenceStatus"] == (
        "reference-profile-not-production-certification"
    )
    assert [stage["releaseTag"] for stage in evidence["artifactSequence"]] == [
        "v0.18.0",
        "v0.18.1",
        "v0.18.0",
    ]
    assert evidence["publishedChartAndImageExecution"] is True
    assert evidence["targetImageMigrationHookPassed"] is True
    assert evidence["baselineImageCompatibilityHookPassed"] is True
    assert evidence["postgresSchemaVersions"] == {
        "baseline": [1, 2, 3, 4, 5, 6],
        "target": [1, 2, 3, 4, 5, 6],
        "rollback": [1, 2, 3, 4, 5, 6],
    }
    assert evidence["priorBundleDigestReused"] is True
    assert evidence["freshRollbackPromotionIdentity"] is True
    assert evidence["synchronousControlPlaneCalls"] == 0
    lowered = report.read_text(encoding="utf-8").lower()
    for forbidden in (
        '"question"',
        '"answer"',
        '"token"',
        '"password"',
        '"signedpayload"',
        '"signature"',
    ):
        assert forbidden not in lowered

    tampered = _observation_lines(sequence, baseline, target)
    tampered[0] = tampered[0].replace(baseline["runtimeImage"], target["runtimeImage"])
    observations.write_text("\n".join(tampered) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="published_upgrade_observation_invalid"):
        fixture.write_report(
            assets / "sequence.json",
            observations,
            baseline_path,
            target_path,
            "v1.34.8+k3s1",
            report,
        )


def test_published_upgrade_shell_contracts_are_executable_and_fail_closed():
    assert HARNESS.stat().st_mode & stat.S_IXUSR
    assert RESOLVER.stat().st_mode & stat.S_IXUSR
    harness = HARNESS.read_text(encoding="utf-8")
    resolver = RESOLVER.read_text(encoding="utf-8")

    assert '"$helm_command" upgrade runtime' in harness
    assert "--reuse-values" in harness
    assert "runtimeConfig.rolloutId" in harness
    assert "published_upgrade_fixture.py" in harness
    assert "prometa_runtime_release_activation" in harness
    assert "cosign verify" in resolver
    assert "cosign verify-attestation" in resolver
    assert "verify_runtime_release_contract.sh" in resolver
    assert "gh release download" in resolver
    assert "chartPackageSha256" in resolver
