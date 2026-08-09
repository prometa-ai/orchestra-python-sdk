"""The shipped guardrail conformance runner, run against the built-in service."""

from __future__ import annotations

import json
from pathlib import Path

from prometa.guardrail.conformance import (
    CHECK_IDS,
    CHECKS_BEYOND_CHECKLIST,
    DELEGATED_CHECKS,
    build_conformance_driver,
    main,
    run_guardrail_conformance,
)


REPO_ROOT = Path(__file__).resolve().parent.parent

# Every item in the contract's conformance checklist, transcribed from the
# document. A check that is deleted or never written fails this suite instead
# of quietly leaving the checklist aspirational.
CONTRACT_CHECKLIST = frozenset(
    # C12 and C13 in the document are the payload-kind pair; neither this
    # runner nor a delegated suite owns them yet, so they stay out of the
    # transcription rather than being asserted as covered.
    ["C%d" % index for index in range(1, 12)]
    + ["C14", "C15"]
    + ["K%d" % index for index in range(1, 10)]
    + ["M%d" % index for index in range(1, 8)]
    + ["L1", "L2", "L4", "L5", "L6"]
    + ["F%d" % index for index in range(1, 9)]
    + ["D%d" % index for index in range(1, 9)]
    + ["V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V11", "V12"]
    + ["E%d" % index for index in range(1, 6)]
)


def test_the_builtin_service_passes_every_check_the_runner_owns() -> None:
    report = run_guardrail_conformance(build_conformance_driver())

    assert report.failures == ()
    assert report.passed is True
    assert report.contract == "orchestra-guardrail-evaluate-v1"
    assert {check.check_id for check in report.checks} == set(CHECK_IDS)


def test_every_delegated_check_names_a_test_module_that_exists() -> None:
    missing = sorted(
        check_id
        for check_id, path in DELEGATED_CHECKS.items()
        if not (REPO_ROOT / path).is_file()
    )

    assert missing == []
    assert set(DELEGATED_CHECKS).isdisjoint(CHECK_IDS)


def test_no_checklist_item_is_unowned() -> None:
    owned = set(CHECK_IDS) | set(DELEGATED_CHECKS)

    assert sorted(CONTRACT_CHECKLIST - owned) == []
    assert len(CHECK_IDS) == len(set(CHECK_IDS))


def test_every_check_beyond_the_checklist_names_the_clause_it_enforces() -> None:
    """The checklist has gaps; a check filling one still has to be declared.

    §7 never covered §2.2, so an implementation with no authentication passed
    every listed item. Extra checks are allowed, but each one names its clause
    so nothing is added to the runner without saying what it is for.
    """

    owned = set(CHECK_IDS) | set(DELEGATED_CHECKS)
    extra = owned - CONTRACT_CHECKLIST

    assert sorted(extra) == sorted(CHECKS_BEYOND_CHECKLIST)
    assert all(clause.strip() for clause in CHECKS_BEYOND_CHECKLIST.values())


def test_an_implementation_that_always_allows_is_not_conformant() -> None:
    class _AlwaysAllows:
        name = "always-allows"

        def evaluate(self, document):
            return 200, {
                "contractVersion": 1,
                "requestId": document.get("requestId"),
                "verdict": "allow",
                "reason": "",
                "reasonCode": "guardrail_allowed",
                "evaluatedGuardrails": [],
                "transformedPayload": None,
                "assessments": [],
                "deferred": [],
                "detectorPack": {"id": "none", "version": 0, "digest": "sha256:"},
                "latencyMs": 0.0,
                "compat": {"unknownFieldsDropped": 0},
            }

    report = run_guardrail_conformance(_AlwaysAllows())

    assert report.passed is False
    assert {"C1", "C3", "M5"} <= {check.check_id for check in report.failures}


def test_the_runner_reports_a_machine_readable_document(tmp_path) -> None:
    output = tmp_path / "report.json"

    code = main(["--output", str(output), "--compact"])
    document = json.loads(output.read_text(encoding="utf-8"))

    assert code == 0
    assert document["passed"] is True
    assert document["contractVersion"] == 1
    assert document["delegated"] == dict(DELEGATED_CHECKS)
    assert len(document["checks"]) == len(CHECK_IDS)
