"""Reusable tenant-runtime conformance runner tests."""

from __future__ import annotations

import asyncio
import json
import shlex
import sys
from importlib import resources
from pathlib import Path

import pytest

pytest.importorskip("cryptography")
pytest.importorskip("jsonschema")

from prometa.runtime import (
    RuntimeConformanceCase,
    RuntimeConformanceObservation,
    RuntimeConformanceProtocolError,
    SubprocessRuntimeConformanceDriver,
    load_runtime_conformance_fixture,
    run_runtime_conformance,
)
from prometa.runtime.conformance import main


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "runtime-kernel-v1.json"
COMMAND_DRIVER_PATH = (
    Path(__file__).parent.parent / "examples" / "runtime_conformance_command_driver.py"
)


def test_packaged_fixture_is_the_cross_repo_golden_vector() -> None:
    packaged = (
        resources.files("prometa.runtime.fixtures")
        .joinpath("runtime-kernel-v1.json")
        .read_bytes()
    )
    assert packaged == FIXTURE_PATH.read_bytes()
    assert load_runtime_conformance_fixture()["fixtureVersion"] == 1


def test_sdk_runtime_passes_the_standard_conformance_suite() -> None:
    report = asyncio.run(run_runtime_conformance())

    assert report.passed is True
    assert report.driver_name == "prometa-sdk-runtime-kernel"
    assert report.profile == "core"
    assert report.fixture_digest.startswith("sha256:")
    assert [check.case_id for check in report.checks] == [
        "admission.valid",
        "admission.tampered-bundle",
        "admission.replay",
        "execution.valid",
        "execution.invalid-input",
        "execution.evidence-fail-closed",
    ]
    assert all(check.reason == "passed" for check in report.checks)

    serialized = json.dumps(report.as_dict(), sort_keys=True)
    fixture = load_runtime_conformance_fixture()
    assert fixture["sampleInput"]["question"] not in serialized
    assert fixture["sampleOutput"]["answer"] not in serialized
    assert "publicKeySpkiDerBase64" not in serialized


def test_resilience_and_deployment_profiles_cover_outage_boundaries() -> None:
    resilience = asyncio.run(run_runtime_conformance(profile="resilience"))
    assert resilience.passed is True
    assert [check.case_id for check in resilience.checks] == [
        "admission.control-plane-offline",
        "admission.offline-lease-expired",
        "admission.replay-store-unavailable",
        "execution.state-store-unavailable",
        "execution.model-plane-unavailable",
    ]
    assert all(
        check.details["controlPlaneInvocations"] == 0 for check in resilience.checks
    )
    assert resilience.checks[-1].details["modelInvocations"] == 2

    deployment = asyncio.run(run_runtime_conformance(profile="deployment"))
    assert deployment.passed is True
    assert deployment.profile == "deployment"
    assert len(deployment.checks) == 11


def test_subprocess_driver_passes_combined_deployment_profile() -> None:
    driver = SubprocessRuntimeConformanceDriver(
        (sys.executable, str(COMMAND_DRIVER_PATH)),
        name="external-sdk-process",
        timeout_seconds=10,
    )
    report = asyncio.run(run_runtime_conformance(driver, profile="deployment"))
    assert report.passed is True
    assert report.driver_name == "external-sdk-process"
    assert len(report.checks) == 11


def test_reference_host_process_passes_combined_deployment_profile() -> None:
    driver = SubprocessRuntimeConformanceDriver(
        (sys.executable, "-m", "prometa.runtime.host_conformance"),
        name="reference-host-process",
        timeout_seconds=10,
    )
    report = asyncio.run(run_runtime_conformance(driver, profile="deployment"))
    assert report.passed is True
    assert report.driver_name == "reference-host-process"
    assert len(report.checks) == 11


def test_subprocess_driver_bounds_timeout_output_and_errors() -> None:
    case = RuntimeConformanceCase(
        case_id="admission.valid",
        description="test",
        vector=load_runtime_conformance_fixture(),
    )
    timeout = SubprocessRuntimeConformanceDriver(
        (sys.executable, "-c", "import time; time.sleep(2)"),
        name="timeout",
        timeout_seconds=0.1,
    )
    with pytest.raises(RuntimeConformanceProtocolError) as caught:
        asyncio.run(timeout.run_case(case))
    assert caught.value.code == "driver_timeout"

    oversized = SubprocessRuntimeConformanceDriver(
        (sys.executable, "-c", "print('x' * 2048)"),
        name="oversized",
        max_output_bytes=1024,
    )
    with pytest.raises(RuntimeConformanceProtocolError) as caught:
        asyncio.run(oversized.run_case(case))
    assert caught.value.code == "driver_output_too_large"

    oversized_stderr = SubprocessRuntimeConformanceDriver(
        (
            sys.executable,
            "-c",
            "import sys; sys.stderr.write('x' * 20000)",
        ),
        name="oversized-stderr",
    )
    with pytest.raises(RuntimeConformanceProtocolError) as caught:
        asyncio.run(oversized_stderr.run_case(case))
    assert caught.value.code == "driver_stderr_too_large"

    nonzero = SubprocessRuntimeConformanceDriver(
        (
            sys.executable,
            "-c",
            "import sys; sys.stderr.write('secret-value'); raise SystemExit(3)",
        ),
        name="nonzero",
    )
    with pytest.raises(RuntimeConformanceProtocolError) as caught:
        asyncio.run(nonzero.run_case(case))
    assert caught.value.code == "driver_exit_nonzero"
    assert "secret-value" not in str(caught.value)

    malformed = SubprocessRuntimeConformanceDriver(
        (sys.executable, "-c", "print('{}')"),
        name="malformed",
    )
    with pytest.raises(RuntimeConformanceProtocolError) as caught:
        asyncio.run(malformed.run_case(case))
    assert caught.value.code == "driver_response_invalid"

    unsafe_response = json.dumps(
        {
            "protocolVersion": 1,
            "observation": {
                "accepted": False,
                "errorCode": "secret value",
                "output": None,
                "modelInvocations": 0,
                "controlPlaneInvocations": 0,
                "evidenceEvents": [],
            },
        }
    )
    unsafe = SubprocessRuntimeConformanceDriver(
        (sys.executable, "-c", "print(%r)" % unsafe_response),
        name="unsafe",
    )
    with pytest.raises(RuntimeConformanceProtocolError) as caught:
        asyncio.run(unsafe.run_case(case))
    assert caught.value.code == "driver_error_code_invalid"


class IncorrectDriver:
    name = "incorrect-runtime"

    async def run_case(self, case):
        return RuntimeConformanceObservation(accepted=True)


class UnsafeVisibleFieldDriver:
    name = "unsafe-visible-field"

    async def run_case(self, case):
        return RuntimeConformanceObservation(
            accepted=False,
            error_code="secret value",
        )


def test_driver_failures_are_reported_without_aborting_remaining_cases() -> None:
    report = asyncio.run(run_runtime_conformance(IncorrectDriver()))
    assert report.passed is False
    assert len(report.checks) == 6
    assert report.checks[0].passed is True
    assert report.checks[1].reason == "acceptance_mismatch,error_code_mismatch"
    assert report.checks[-1].passed is False

    unsafe = asyncio.run(run_runtime_conformance(UnsafeVisibleFieldDriver()))
    assert unsafe.passed is False
    assert all(
        check.reason == "driver_error:driver_error_code_invalid"
        for check in unsafe.checks
    )
    assert "secret value" not in json.dumps(unsafe.as_dict())


def test_cli_writes_machine_readable_report(tmp_path) -> None:
    output = tmp_path / "report.json"
    assert main(["--output", str(output), "--compact"]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["reportVersion"] == 2
    assert report["profile"] == "core"
    assert report["passed"] is True
    assert len(report["checks"]) == 6


def test_cli_runs_subprocess_deployment_profile(tmp_path) -> None:
    output = tmp_path / "deployment-report.json"
    command = shlex.join((sys.executable, str(COMMAND_DRIVER_PATH)))
    assert (
        main(
            [
                "--command",
                command,
                "--driver-name",
                "tenant-wrapper",
                "--profile",
                "deployment",
                "--output",
                str(output),
                "--compact",
            ]
        )
        == 0
    )
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["driverName"] == "tenant-wrapper"
    assert report["profile"] == "deployment"
    assert report["passed"] is True
    assert len(report["checks"]) == 11
