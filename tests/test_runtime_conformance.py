"""Reusable tenant-runtime conformance runner tests."""

from __future__ import annotations

import asyncio
import json
from importlib import resources
from pathlib import Path

import pytest

pytest.importorskip("cryptography")
pytest.importorskip("jsonschema")

from prometa.runtime import (
    RuntimeConformanceObservation,
    load_runtime_conformance_fixture,
    run_runtime_conformance,
)
from prometa.runtime.conformance import main


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "runtime-kernel-v1.json"


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


class IncorrectDriver:
    name = "incorrect-runtime"

    async def run_case(self, case):
        return RuntimeConformanceObservation(accepted=True)


def test_driver_failures_are_reported_without_aborting_remaining_cases() -> None:
    report = asyncio.run(run_runtime_conformance(IncorrectDriver()))
    assert report.passed is False
    assert len(report.checks) == 6
    assert report.checks[0].passed is True
    assert report.checks[1].reason == "acceptance_mismatch,error_code_mismatch"
    assert report.checks[-1].passed is False


def test_cli_writes_machine_readable_report(tmp_path) -> None:
    output = tmp_path / "report.json"
    assert main(["--output", str(output), "--compact"]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["reportVersion"] == 1
    assert report["passed"] is True
    assert len(report["checks"]) == 6
