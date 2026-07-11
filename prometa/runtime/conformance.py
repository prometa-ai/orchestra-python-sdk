"""Reusable conformance runner for tenant-owned runtime implementations.

The runner operates on the public signed fixture and a narrow driver protocol.
It can validate the built-in SDK kernel or a tenant adapter without requiring a
Prometa-hosted runtime or a synchronous control-plane connection.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import importlib
import importlib.metadata
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import resources
from pathlib import Path
from typing import Any, Mapping, Optional, Protocol, Sequence, Tuple

from .admission import (
    BASE_RUNTIME_CAPABILITIES,
    CAPABILITY_SCHEMA_VALIDATE,
    InMemoryAdmissionReplayStore,
    RuntimeAdmissionPolicy,
    admit_runtime_release,
)
from .kernel import (
    InMemoryEvidenceEmitter,
    ModelInvocationRequest,
    ModelInvocationResponse,
    RuntimeEvidenceEvent,
    RuntimeExecutionError,
    RuntimeKernel,
)
from .trust import BundleTrustEntry, BundleTrustStore, BundleVerificationError


CONFORMANCE_REPORT_VERSION = 1
_REQUIRED_IDENTITY_KEYS = (
    "prometa.agent_id",
    "prometa.bundle.digest",
    "prometa.bundle.jti",
    "prometa.attestation.id",
    "prometa.policy.decision_id",
    "prometa.release.id",
    "prometa.deployment.id",
    "prometa.runtime.target",
    "prometa.runtime.id",
    "prometa.runtime.version",
)


@dataclass(frozen=True)
class RuntimeConformanceCase:
    case_id: str
    description: str
    vector: Mapping[str, Any]


@dataclass(frozen=True)
class RuntimeConformanceObservation:
    accepted: bool
    error_code: Optional[str] = None
    output: Any = None
    model_invocations: int = 0
    evidence_events: Tuple[RuntimeEvidenceEvent, ...] = ()


class RuntimeConformanceDriver(Protocol):
    """Adapter boundary for exercising a tenant runtime implementation."""

    name: str

    async def run_case(
        self, case: RuntimeConformanceCase
    ) -> RuntimeConformanceObservation:
        """Execute one isolated conformance case and return sanitized facts."""


@dataclass(frozen=True)
class RuntimeConformanceCheck:
    case_id: str
    description: str
    passed: bool
    reason: str
    details: Mapping[str, Any]

    def as_dict(self) -> Mapping[str, Any]:
        return {
            "caseId": self.case_id,
            "description": self.description,
            "passed": self.passed,
            "reason": self.reason,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class RuntimeConformanceReport:
    report_version: int
    generated_at: str
    sdk_version: str
    driver_name: str
    fixture_version: str
    fixture_digest: str
    passed: bool
    checks: Tuple[RuntimeConformanceCheck, ...]

    def as_dict(self) -> Mapping[str, Any]:
        return {
            "reportVersion": self.report_version,
            "generatedAt": self.generated_at,
            "sdkVersion": self.sdk_version,
            "driverName": self.driver_name,
            "fixtureVersion": self.fixture_version,
            "fixtureDigest": self.fixture_digest,
            "passed": self.passed,
            "checks": [check.as_dict() for check in self.checks],
        }


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _trust(value: Mapping[str, Any]) -> BundleTrustStore:
    return BundleTrustStore(
        [
            BundleTrustEntry(
                issuer=value["issuer"],
                key_id=value["keyId"],
                public_key_spki_der_base64=value["publicKeySpkiDerBase64"],
            )
        ]
    )


def _policy(vector: Mapping[str, Any]) -> RuntimeAdmissionPolicy:
    verification = vector["verification"]
    return RuntimeAdmissionPolicy(
        expected_org_id=verification["expectedOrgId"],
        expected_environment=verification["expectedEnvironment"],
        expected_release_id=verification["expectedReleaseId"],
        expected_deployment_id=verification["expectedDeploymentId"],
        expected_runtime=verification["expectedRuntime"],
        supported_capabilities=frozenset(
            {*BASE_RUNTIME_CAPABILITIES, CAPABILITY_SCHEMA_VALIDATE}
        ),
    )


def _admit(
    vector: Mapping[str, Any], replay_store: InMemoryAdmissionReplayStore
) -> Any:
    return admit_runtime_release(
        vector["bundle"],
        vector["attestation"],
        bundle_trust_store=_trust(vector["bundleTrust"]),
        promotion_trust_store=_trust(vector["promotionTrust"]),
        replay_store=replay_store,
        policy=_policy(vector),
        now=_instant(vector["verification"]["now"]),
    )


class _ConformanceModelAdapter:
    def __init__(self, output: Any) -> None:
        self.output = output
        self.invocations = 0

    async def invoke(self, request: ModelInvocationRequest) -> ModelInvocationResponse:
        self.invocations += 1
        return ModelInvocationResponse(
            content=json.dumps(self.output, sort_keys=True, separators=(",", ":")),
            finish_reason="stop",
            provider_model="conformance-model",
        )


class _FailingEvidenceEmitter:
    def emit(self, event: RuntimeEvidenceEvent) -> None:
        raise OSError("conformance evidence sink unavailable")


class SdkRuntimeConformanceDriver:
    """Reference driver for the optional ``prometa.runtime`` kernel."""

    name = "prometa-sdk-runtime-kernel"

    async def run_case(
        self, case: RuntimeConformanceCase
    ) -> RuntimeConformanceObservation:
        vector = copy.deepcopy(dict(case.vector))
        if case.case_id == "admission.valid":
            _admit(vector, InMemoryAdmissionReplayStore())
            return RuntimeConformanceObservation(accepted=True)

        if case.case_id == "admission.tampered-bundle":
            vector["bundle"]["envelopeSignature"] = "AAAA"
            return self._observe_admission(vector, InMemoryAdmissionReplayStore())

        if case.case_id == "admission.replay":
            replay = InMemoryAdmissionReplayStore()
            _admit(vector, replay)
            return self._observe_admission(vector, replay)

        admitted = _admit(vector, InMemoryAdmissionReplayStore())
        adapter = _ConformanceModelAdapter(vector["sampleOutput"])

        if case.case_id == "execution.evidence-fail-closed":
            try:
                RuntimeKernel(
                    admitted,
                    model_adapter=adapter,
                    evidence_emitter=_FailingEvidenceEmitter(),
                    runtime_id="conformance-runtime",
                    runtime_version="1",
                )
            except RuntimeExecutionError as exc:
                return RuntimeConformanceObservation(
                    accepted=False,
                    error_code=exc.code,
                    model_invocations=adapter.invocations,
                )
            return RuntimeConformanceObservation(
                accepted=True,
                model_invocations=adapter.invocations,
            )

        emitter = InMemoryEvidenceEmitter()
        kernel = RuntimeKernel(
            admitted,
            model_adapter=adapter,
            evidence_emitter=emitter,
            runtime_id="conformance-runtime",
            runtime_version="1",
        )
        payload = (
            {"unexpected": True}
            if case.case_id == "execution.invalid-input"
            else vector["sampleInput"]
        )
        try:
            result = await kernel.execute(
                payload,
                request_id="conformance-%s" % case.case_id.replace(".", "-"),
            )
        except RuntimeExecutionError as exc:
            return RuntimeConformanceObservation(
                accepted=False,
                error_code=exc.code,
                model_invocations=adapter.invocations,
                evidence_events=emitter.events,
            )
        return RuntimeConformanceObservation(
            accepted=True,
            output=result.output,
            model_invocations=adapter.invocations,
            evidence_events=emitter.events,
        )

    @staticmethod
    def _observe_admission(
        vector: Mapping[str, Any], replay: InMemoryAdmissionReplayStore
    ) -> RuntimeConformanceObservation:
        try:
            _admit(vector, replay)
        except BundleVerificationError as exc:
            return RuntimeConformanceObservation(
                accepted=False,
                error_code=exc.code,
            )
        return RuntimeConformanceObservation(accepted=True)


def load_runtime_conformance_fixture(
    path: Optional[Path] = None,
) -> Mapping[str, Any]:
    """Load a conformance vector from disk or the installed SDK package."""

    if path is None:
        text = (
            resources.files("prometa.runtime.fixtures")
            .joinpath("runtime-kernel-v1.json")
            .read_text(encoding="utf-8")
        )
    else:
        text = path.read_text(encoding="utf-8")
    value = json.loads(text)
    if not isinstance(value, Mapping) or value.get("fixtureVersion") not in {1, "1"}:
        raise ValueError("unsupported runtime conformance fixture")
    return value


def _cases(vector: Mapping[str, Any]) -> Tuple[RuntimeConformanceCase, ...]:
    return (
        RuntimeConformanceCase(
            "admission.valid",
            "valid signed and promoted runtime release is admitted",
            vector,
        ),
        RuntimeConformanceCase(
            "admission.tampered-bundle",
            "tampered bundle is rejected before execution",
            vector,
        ),
        RuntimeConformanceCase(
            "admission.replay",
            "replayed bundle or promotion identity is rejected",
            vector,
        ),
        RuntimeConformanceCase(
            "execution.valid",
            "valid input executes and emits joinable evidence",
            vector,
        ),
        RuntimeConformanceCase(
            "execution.invalid-input",
            "invalid input is rejected before model invocation",
            vector,
        ),
        RuntimeConformanceCase(
            "execution.evidence-fail-closed",
            "unavailable evidence sink prevents model invocation",
            vector,
        ),
    )


def _check_case(
    case: RuntimeConformanceCase,
    observation: RuntimeConformanceObservation,
) -> RuntimeConformanceCheck:
    expected = {
        "admission.valid": (True, None, None),
        "admission.tampered-bundle": (False, "invalid_signature", None),
        "admission.replay": (False, "replayed_runtime_release", None),
        "execution.valid": (True, None, 1),
        "execution.invalid-input": (False, "input_schema_invalid", 0),
        "execution.evidence-fail-closed": (False, "evidence_emit_failed", 0),
    }[case.case_id]
    expected_accepted, expected_code, expected_model_invocations = expected
    reasons = []
    if observation.accepted != expected_accepted:
        reasons.append("acceptance_mismatch")
    if observation.error_code != expected_code:
        reasons.append("error_code_mismatch")
    if (
        expected_model_invocations is not None
        and observation.model_invocations != expected_model_invocations
    ):
        reasons.append("model_invocation_mismatch")

    if case.case_id == "execution.valid":
        if observation.output != case.vector["sampleOutput"]:
            reasons.append("output_mismatch")
        completed = next(
            (
                event
                for event in reversed(observation.evidence_events)
                if event.name == "runtime.request" and event.outcome == "completed"
            ),
            None,
        )
        if completed is None:
            reasons.append("completion_evidence_missing")
        elif any(not completed.attributes.get(key) for key in _REQUIRED_IDENTITY_KEYS):
            reasons.append("identity_evidence_incomplete")

    if case.case_id == "execution.invalid-input":
        schema_denied = any(
            event.name == "runtime.schema.input" and event.outcome == "denied"
            for event in observation.evidence_events
        )
        request_failed = any(
            event.name == "runtime.request" and event.outcome == "failed"
            for event in observation.evidence_events
        )
        if not schema_denied or not request_failed:
            reasons.append("denial_evidence_incomplete")

    details = {
        "accepted": observation.accepted,
        "errorCode": observation.error_code,
        "modelInvocations": observation.model_invocations,
        "evidenceEvents": [event.name for event in observation.evidence_events],
    }
    return RuntimeConformanceCheck(
        case_id=case.case_id,
        description=case.description,
        passed=not reasons,
        reason="passed" if not reasons else ",".join(reasons),
        details=details,
    )


def _sdk_version() -> str:
    try:
        return importlib.metadata.version("prometa-sdk")
    except importlib.metadata.PackageNotFoundError:  # pragma: no cover
        return "source"


async def run_runtime_conformance(
    driver: Optional[RuntimeConformanceDriver] = None,
    *,
    fixture: Optional[Mapping[str, Any]] = None,
) -> RuntimeConformanceReport:
    """Run the standard isolated cases against one runtime driver."""

    subject = driver or SdkRuntimeConformanceDriver()
    vector = fixture or load_runtime_conformance_fixture()
    canonical = json.dumps(
        vector,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    checks = []
    for case in _cases(vector):
        try:
            observation = await subject.run_case(case)
            check = _check_case(case, observation)
        except Exception as exc:
            check = RuntimeConformanceCheck(
                case_id=case.case_id,
                description=case.description,
                passed=False,
                reason="driver_error:%s" % type(exc).__name__,
                details={},
            )
        checks.append(check)
    return RuntimeConformanceReport(
        report_version=CONFORMANCE_REPORT_VERSION,
        generated_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        sdk_version=_sdk_version(),
        driver_name=subject.name,
        fixture_version=str(vector["fixtureVersion"]),
        fixture_digest="sha256:%s" % hashlib.sha256(canonical).hexdigest(),
        passed=all(check.passed for check in checks),
        checks=tuple(checks),
    )


def _load_driver(spec: str) -> RuntimeConformanceDriver:
    module_name, separator, attribute_name = spec.partition(":")
    if not separator or not module_name or not attribute_name:
        raise ValueError("driver must use module:factory syntax")
    factory = getattr(importlib.import_module(module_name), attribute_name)
    driver = factory()
    if not hasattr(driver, "run_case") or not hasattr(driver, "name"):
        raise TypeError("driver factory returned an incompatible object")
    return driver


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="prometa-runtime-conformance",
        description="Run the Orchestra tenant-runtime conformance suite.",
    )
    parser.add_argument(
        "--driver",
        help="Optional Python module:factory implementing RuntimeConformanceDriver",
    )
    parser.add_argument("--fixture", type=Path, help="Alternate signed vector JSON")
    parser.add_argument(
        "--output", type=Path, help="Write the JSON report to this path"
    )
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON")
    args = parser.parse_args(argv)

    try:
        driver = _load_driver(args.driver) if args.driver else None
        fixture = load_runtime_conformance_fixture(args.fixture)
        report = asyncio.run(run_runtime_conformance(driver, fixture=fixture))
    except (OSError, ValueError, TypeError, ImportError) as exc:
        parser.error(str(exc))
        return 2  # pragma: no cover - argparse exits

    serialized = json.dumps(
        report.as_dict(),
        indent=None if args.compact else 2,
        sort_keys=True,
    )
    if args.output:
        args.output.write_text(serialized + "\n", encoding="utf-8")
    else:
        sys.stdout.write(serialized + "\n")
    return 0 if report.passed else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "CONFORMANCE_REPORT_VERSION",
    "RuntimeConformanceCase",
    "RuntimeConformanceObservation",
    "RuntimeConformanceDriver",
    "RuntimeConformanceCheck",
    "RuntimeConformanceReport",
    "SdkRuntimeConformanceDriver",
    "load_runtime_conformance_fixture",
    "run_runtime_conformance",
    "main",
]
