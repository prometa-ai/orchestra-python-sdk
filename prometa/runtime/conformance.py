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
import re
import shlex
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from importlib import resources
from pathlib import Path
from typing import Any, Mapping, Optional, Protocol, Sequence, Tuple

from .admission import (
    AdmissionReplayStore,
    BASE_RUNTIME_CAPABILITIES,
    CAPABILITY_SCHEMA_VALIDATE,
    InMemoryAdmissionReplayStore,
    RuntimeAdmissionPolicy,
    admit_runtime_release,
)
from .kernel import (
    InMemoryEvidenceEmitter,
    ModelAdapterError,
    ModelInvocationRequest,
    ModelInvocationResponse,
    RuntimeEvidenceEvent,
    RuntimeExecutionError,
    RuntimeExecutionPolicy,
    RuntimeKernel,
    RuntimeStateStore,
)
from .postgres import RuntimePersistenceError
from .trust import BundleTrustEntry, BundleTrustStore, BundleVerificationError


CONFORMANCE_REPORT_VERSION = 2
CONFORMANCE_COMMAND_PROTOCOL_VERSION = 1
CONFORMANCE_PROFILES = ("core", "resilience", "deployment")
_MAX_COMMAND_INPUT_BYTES = 2_097_152
_MAX_COMMAND_OUTPUT_BYTES = 2_097_152
_MAX_COMMAND_STDERR_BYTES = 16_384
_ERROR_CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$")
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

    def as_dict(self) -> Mapping[str, Any]:
        return {
            "caseId": self.case_id,
            "description": self.description,
            "vector": dict(self.vector),
        }


@dataclass(frozen=True)
class RuntimeConformanceObservation:
    accepted: bool
    error_code: Optional[str] = None
    output: Any = None
    model_invocations: int = 0
    control_plane_invocations: int = 0
    evidence_events: Tuple[RuntimeEvidenceEvent, ...] = ()


class RuntimeConformanceDriver(Protocol):
    """Adapter boundary for exercising a tenant runtime implementation."""

    name: str

    async def run_case(
        self, case: RuntimeConformanceCase
    ) -> RuntimeConformanceObservation:
        """Execute one isolated conformance case and return sanitized facts."""


class RuntimeConformanceProtocolError(RuntimeError):
    """Stable, payload-free subprocess conformance protocol failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code.replace("_", " "))


def _strict_json_loads(value: str) -> Any:
    def reject_duplicates(pairs):
        result = {}
        for key, child in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = child
        return result

    def reject_constant(constant):
        raise ValueError("non-finite JSON number")

    return json.loads(
        value,
        object_pairs_hook=reject_duplicates,
        parse_constant=reject_constant,
    )


def _protocol_string(value: Any, code: str, maximum: int = 256) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or value != value.strip()
        or len(value) > maximum
    ):
        raise RuntimeConformanceProtocolError(code)
    return value


def _protocol_count(value: Any, code: str) -> int:
    if type(value) is not int or value < 0 or value > 10_000:
        raise RuntimeConformanceProtocolError(code)
    return value


def _protocol_token(
    value: Any,
    code: str,
    *,
    error_code: bool = False,
    maximum: int = 256,
) -> str:
    candidate = _protocol_string(value, code, maximum)
    pattern = _ERROR_CODE_PATTERN if error_code else _TOKEN_PATTERN
    if not pattern.fullmatch(candidate):
        raise RuntimeConformanceProtocolError(code)
    return candidate


def _protocol_timestamp(value: Any) -> str:
    candidate = _protocol_string(value, "driver_evidence_invalid", 128)
    try:
        parsed = _instant(candidate)
    except ValueError:
        raise RuntimeConformanceProtocolError("driver_evidence_invalid") from None
    if parsed.tzinfo is None:
        raise RuntimeConformanceProtocolError("driver_evidence_invalid")
    return candidate


def _observation_as_dict(
    observation: RuntimeConformanceObservation,
) -> Mapping[str, Any]:
    return {
        "accepted": observation.accepted,
        "errorCode": observation.error_code,
        "output": observation.output,
        "modelInvocations": observation.model_invocations,
        "controlPlaneInvocations": observation.control_plane_invocations,
        "evidenceEvents": [
            {
                "name": event.name,
                "outcome": event.outcome,
                "occurredAt": event.occurred_at,
                "attributes": dict(event.attributes),
            }
            for event in observation.evidence_events
        ],
    }


def _parse_protocol_observation(value: Any) -> RuntimeConformanceObservation:
    if not isinstance(value, Mapping):
        raise RuntimeConformanceProtocolError("driver_observation_invalid")
    allowed = {
        "accepted",
        "errorCode",
        "output",
        "modelInvocations",
        "controlPlaneInvocations",
        "evidenceEvents",
    }
    if set(value) != allowed or type(value.get("accepted")) is not bool:
        raise RuntimeConformanceProtocolError("driver_observation_invalid")
    error_code = value.get("errorCode")
    if error_code is not None:
        error_code = _protocol_token(
            error_code,
            "driver_error_code_invalid",
            error_code=True,
        )
    events_value = value.get("evidenceEvents")
    if not isinstance(events_value, list) or len(events_value) > 256:
        raise RuntimeConformanceProtocolError("driver_evidence_invalid")
    events = []
    for event_value in events_value:
        if not isinstance(event_value, Mapping) or set(event_value) != {
            "name",
            "outcome",
            "occurredAt",
            "attributes",
        }:
            raise RuntimeConformanceProtocolError("driver_evidence_invalid")
        attributes = event_value.get("attributes")
        if not isinstance(attributes, Mapping) or len(attributes) > 256:
            raise RuntimeConformanceProtocolError("driver_evidence_invalid")
        events.append(
            RuntimeEvidenceEvent(
                name=_protocol_token(
                    event_value.get("name"), "driver_evidence_invalid"
                ),
                outcome=_protocol_token(
                    event_value.get("outcome"), "driver_evidence_invalid"
                ),
                occurred_at=_protocol_timestamp(event_value.get("occurredAt")),
                attributes=dict(attributes),
            )
        )
    return RuntimeConformanceObservation(
        accepted=value["accepted"],
        error_code=error_code,
        output=value.get("output"),
        model_invocations=_protocol_count(
            value.get("modelInvocations"), "driver_model_count_invalid"
        ),
        control_plane_invocations=_protocol_count(
            value.get("controlPlaneInvocations"),
            "driver_control_plane_count_invalid",
        ),
        evidence_events=tuple(events),
    )


async def _read_bounded(
    stream: Optional[asyncio.StreamReader], maximum: int
) -> Tuple[bytes, bool]:
    if stream is None:
        return b"", False
    chunks = []
    stored = 0
    exceeded = False
    while True:
        chunk = await stream.read(65_536)
        if not chunk:
            break
        remaining = max(0, maximum - stored)
        if remaining:
            chunks.append(chunk[:remaining])
            stored += min(len(chunk), remaining)
        if len(chunk) > remaining:
            exceeded = True
    return b"".join(chunks), exceeded


class SubprocessRuntimeConformanceDriver:
    """Run each case through a bounded, shell-free one-shot process protocol."""

    def __init__(
        self,
        command: Sequence[str],
        *,
        name: str,
        timeout_seconds: float = 60.0,
        max_output_bytes: int = _MAX_COMMAND_OUTPUT_BYTES,
    ) -> None:
        if (
            isinstance(command, (str, bytes))
            or not command
            or len(command) > 64
            or any(
                not isinstance(part, str) or not part or len(part) > 4096
                for part in command
            )
        ):
            raise ValueError("command must contain 1-64 non-empty argv values")
        if not 0.1 <= timeout_seconds <= 600:
            raise ValueError("timeout_seconds must be between 0.1 and 600")
        if not 1024 <= max_output_bytes <= 10_485_760:
            raise ValueError("max_output_bytes must be between 1 KiB and 10 MiB")
        self.command = tuple(command)
        self.name = _protocol_token(name, "driver_name_invalid", maximum=128)
        self.timeout_seconds = float(timeout_seconds)
        self.max_output_bytes = max_output_bytes

    async def run_case(
        self, case: RuntimeConformanceCase
    ) -> RuntimeConformanceObservation:
        payload = json.dumps(
            {
                "protocolVersion": CONFORMANCE_COMMAND_PROTOCOL_VERSION,
                "case": case.as_dict(),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        if len(payload) > _MAX_COMMAND_INPUT_BYTES:
            raise RuntimeConformanceProtocolError("driver_input_too_large")
        try:
            input_stream = tempfile.TemporaryFile()
            input_stream.write(payload)
            input_stream.seek(0)
        except OSError:
            raise RuntimeConformanceProtocolError("driver_io_failed") from None
        try:
            process = await asyncio.create_subprocess_exec(
                *self.command,
                stdin=input_stream,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except (OSError, ValueError):
            input_stream.close()
            raise RuntimeConformanceProtocolError("driver_start_failed") from None
        except asyncio.CancelledError:
            input_stream.close()
            raise

        async def terminate() -> None:
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                await process.wait()

        async def exchange():
            stdout_task = asyncio.create_task(
                _read_bounded(process.stdout, self.max_output_bytes)
            )
            stderr_task = asyncio.create_task(
                _read_bounded(process.stderr, _MAX_COMMAND_STDERR_BYTES)
            )
            try:
                return_code, stdout_result, stderr_result = await asyncio.gather(
                    process.wait(), stdout_task, stderr_task
                )
                return return_code, stdout_result, stderr_result
            except asyncio.CancelledError:
                await terminate()
                stdout_task.cancel()
                stderr_task.cancel()
                await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
                raise
            except OSError:
                await terminate()
                await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
                raise RuntimeConformanceProtocolError("driver_io_failed") from None

        try:
            (
                return_code,
                (stdout, output_exceeded),
                (_, stderr_exceeded),
            ) = await asyncio.wait_for(exchange(), timeout=self.timeout_seconds)
        except asyncio.TimeoutError:
            await terminate()
            input_stream.close()
            raise RuntimeConformanceProtocolError("driver_timeout") from None
        except asyncio.CancelledError:
            await terminate()
            input_stream.close()
            raise
        except OSError:
            await terminate()
            input_stream.close()
            raise RuntimeConformanceProtocolError("driver_io_failed") from None
        input_stream.close()
        if return_code != 0:
            raise RuntimeConformanceProtocolError("driver_exit_nonzero")
        if output_exceeded:
            raise RuntimeConformanceProtocolError("driver_output_too_large")
        if stderr_exceeded:
            raise RuntimeConformanceProtocolError("driver_stderr_too_large")
        try:
            response = _strict_json_loads(stdout.decode("utf-8"))
        except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
            raise RuntimeConformanceProtocolError("driver_response_invalid") from None
        if (
            not isinstance(response, Mapping)
            or set(response) != {"protocolVersion", "observation"}
            or response.get("protocolVersion") != CONFORMANCE_COMMAND_PROTOCOL_VERSION
        ):
            raise RuntimeConformanceProtocolError("driver_response_invalid")
        return _parse_protocol_observation(response.get("observation"))


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
    profile: str
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
            "profile": self.profile,
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


def _admit(vector: Mapping[str, Any], replay_store: AdmissionReplayStore) -> Any:
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


class _UnavailableReplayStore:
    def reserve_pair(self, bundle_jti: str, promotion_jti: str) -> bool:
        raise RuntimePersistenceError("replay_store_unavailable")


class _UnavailableStateStore:
    async def save(self, request_id: str, state: Mapping[str, Any]) -> None:
        raise RuntimePersistenceError("state_store_unavailable")


class _UnavailableModelAdapter:
    def __init__(self) -> None:
        self.invocations = 0

    async def invoke(self, request: ModelInvocationRequest) -> ModelInvocationResponse:
        self.invocations += 1
        raise ModelAdapterError("gateway_unavailable", retryable=True)


async def _no_sleep(seconds: float) -> None:
    return None


class SdkRuntimeConformanceDriver:
    """Reference driver for the optional ``prometa.runtime`` kernel."""

    name = "prometa-sdk-runtime-kernel"

    async def run_case(
        self, case: RuntimeConformanceCase
    ) -> RuntimeConformanceObservation:
        vector = copy.deepcopy(dict(case.vector))
        if case.case_id in {
            "admission.valid",
            "admission.control-plane-offline",
        }:
            _admit(vector, InMemoryAdmissionReplayStore())
            return RuntimeConformanceObservation(accepted=True)

        if case.case_id == "admission.tampered-bundle":
            vector["bundle"]["envelopeSignature"] = "AAAA"
            return self._observe_admission(vector, InMemoryAdmissionReplayStore())

        if case.case_id == "admission.replay":
            replay = InMemoryAdmissionReplayStore()
            _admit(vector, replay)
            return self._observe_admission(vector, replay)

        if case.case_id == "admission.offline-lease-expired":
            return self._observe_admission(vector, InMemoryAdmissionReplayStore())

        if case.case_id == "admission.replay-store-unavailable":
            return self._observe_admission(vector, _UnavailableReplayStore())

        admitted = _admit(vector, InMemoryAdmissionReplayStore())
        adapter = (
            _UnavailableModelAdapter()
            if case.case_id == "execution.model-plane-unavailable"
            else _ConformanceModelAdapter(vector["sampleOutput"])
        )

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
        state_store: Optional[RuntimeStateStore] = (
            _UnavailableStateStore()
            if case.case_id == "execution.state-store-unavailable"
            else None
        )
        execution_policy = (
            RuntimeExecutionPolicy(
                max_attempts_per_model=2,
                initial_backoff_seconds=0,
            )
            if case.case_id == "execution.model-plane-unavailable"
            else None
        )
        kernel = RuntimeKernel(
            admitted,
            model_adapter=adapter,
            evidence_emitter=emitter,
            runtime_id="conformance-runtime",
            runtime_version="1",
            state_store=state_store,
            execution_policy=execution_policy,
            sleep=_no_sleep,
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
        vector: Mapping[str, Any], replay: AdmissionReplayStore
    ) -> RuntimeConformanceObservation:
        try:
            _admit(vector, replay)
        except BundleVerificationError as exc:
            return RuntimeConformanceObservation(
                accepted=False,
                error_code=exc.code,
            )
        except RuntimePersistenceError as exc:
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


def _core_cases(vector: Mapping[str, Any]) -> Tuple[RuntimeConformanceCase, ...]:
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


def _resilience_cases(
    vector: Mapping[str, Any],
) -> Tuple[RuntimeConformanceCase, ...]:
    expired = copy.deepcopy(dict(vector))
    claims = [
        _strict_json_loads(expired[artifact]["signedPayload"])
        for artifact in ("bundle", "attestation")
    ]
    offline_expiry = min(_instant(value["offlineLeaseExpiresAt"]) for value in claims)
    artifact_expiry = min(_instant(value["expiresAt"]) for value in claims)
    expired_now = offline_expiry.replace(microsecond=0) + timedelta(seconds=1)
    if expired_now > artifact_expiry:
        raise ValueError("fixture has no independently testable offline lease")
    expired["verification"]["now"] = expired_now.isoformat().replace("+00:00", "Z")
    return (
        RuntimeConformanceCase(
            "admission.control-plane-offline",
            "valid local artifacts admit with no synchronous control-plane call",
            vector,
        ),
        RuntimeConformanceCase(
            "admission.offline-lease-expired",
            "expired offline authorization is rejected while artifact expiry remains valid",
            expired,
        ),
        RuntimeConformanceCase(
            "admission.replay-store-unavailable",
            "unavailable shared replay store fails admission closed",
            vector,
        ),
        RuntimeConformanceCase(
            "execution.state-store-unavailable",
            "unavailable configured state store prevents model invocation",
            vector,
        ),
        RuntimeConformanceCase(
            "execution.model-plane-unavailable",
            "model-plane outage exhausts bounded retries and emits failure evidence",
            vector,
        ),
    )


def _cases(
    vector: Mapping[str, Any], profile: str
) -> Tuple[RuntimeConformanceCase, ...]:
    if profile not in CONFORMANCE_PROFILES:
        raise ValueError("unsupported conformance profile")
    if profile == "core":
        return _core_cases(vector)
    if profile == "resilience":
        return _resilience_cases(vector)
    return _core_cases(vector) + _resilience_cases(vector)


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
        "admission.control-plane-offline": (True, None, 0),
        "admission.offline-lease-expired": (False, "offline_lease_expired", 0),
        "admission.replay-store-unavailable": (
            False,
            "replay_store_unavailable",
            0,
        ),
        "execution.state-store-unavailable": (False, "state_store_failed", 0),
        "execution.model-plane-unavailable": (False, "gateway_unavailable", 2),
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
    if observation.control_plane_invocations != 0:
        reasons.append("synchronous_control_plane_dependency")

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

    if case.case_id in {
        "execution.state-store-unavailable",
        "execution.model-plane-unavailable",
    }:
        request_failed = any(
            event.name == "runtime.request" and event.outcome == "failed"
            for event in observation.evidence_events
        )
        if not request_failed:
            reasons.append("failure_evidence_incomplete")

    if case.case_id == "execution.model-plane-unavailable":
        failed_attempts = sum(
            1
            for event in observation.evidence_events
            if event.name == "runtime.model.attempt" and event.outcome == "failed"
        )
        if failed_attempts != 2:
            reasons.append("retry_evidence_incomplete")

    details = {
        "accepted": observation.accepted,
        "errorCode": observation.error_code,
        "modelInvocations": observation.model_invocations,
        "controlPlaneInvocations": observation.control_plane_invocations,
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
    profile: str = "core",
) -> RuntimeConformanceReport:
    """Run the standard isolated cases against one runtime driver."""

    subject = driver or SdkRuntimeConformanceDriver()
    driver_name = _protocol_token(
        subject.name,
        "driver_name_invalid",
        maximum=128,
    )
    vector = fixture or load_runtime_conformance_fixture()
    canonical = json.dumps(
        vector,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    checks = []
    for case in _cases(vector, profile):
        try:
            observation = await subject.run_case(case)
            observation = _parse_protocol_observation(_observation_as_dict(observation))
            check = _check_case(case, observation)
        except RuntimeConformanceProtocolError as exc:
            check = RuntimeConformanceCheck(
                case_id=case.case_id,
                description=case.description,
                passed=False,
                reason="driver_error:%s" % exc.code,
                details={},
            )
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
        driver_name=driver_name,
        profile=profile,
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


def runtime_conformance_command_main(driver: RuntimeConformanceDriver) -> int:
    """Serve one subprocess-protocol case on stdin/stdout, then exit."""

    try:
        raw = sys.stdin.buffer.read(_MAX_COMMAND_INPUT_BYTES + 1)
        if len(raw) > _MAX_COMMAND_INPUT_BYTES:
            raise RuntimeConformanceProtocolError("driver_input_too_large")
        request = _strict_json_loads(raw.decode("utf-8"))
        if (
            not isinstance(request, Mapping)
            or set(request) != {"protocolVersion", "case"}
            or request.get("protocolVersion") != CONFORMANCE_COMMAND_PROTOCOL_VERSION
        ):
            raise RuntimeConformanceProtocolError("driver_request_invalid")
        case_value = request.get("case")
        if not isinstance(case_value, Mapping) or set(case_value) != {
            "caseId",
            "description",
            "vector",
        }:
            raise RuntimeConformanceProtocolError("driver_case_invalid")
        vector = case_value.get("vector")
        if not isinstance(vector, Mapping):
            raise RuntimeConformanceProtocolError("driver_case_invalid")
        case = RuntimeConformanceCase(
            case_id=_protocol_string(case_value.get("caseId"), "driver_case_invalid"),
            description=_protocol_string(
                case_value.get("description"), "driver_case_invalid", 1024
            ),
            vector=dict(vector),
        )
        observation = asyncio.run(driver.run_case(case))
        response = json.dumps(
            {
                "protocolVersion": CONFORMANCE_COMMAND_PROTOCOL_VERSION,
                "observation": _observation_as_dict(observation),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        if len(response) > _MAX_COMMAND_OUTPUT_BYTES:
            raise RuntimeConformanceProtocolError("driver_output_too_large")
    except (
        UnicodeDecodeError,
        ValueError,
        TypeError,
        RuntimeConformanceProtocolError,
    ):
        return 2
    except Exception:
        return 1
    sys.stdout.buffer.write(response)
    sys.stdout.buffer.flush()
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="prometa-runtime-conformance",
        description="Run the Orchestra tenant-runtime conformance suite.",
    )
    driver_group = parser.add_mutually_exclusive_group()
    driver_group.add_argument(
        "--driver",
        help="Optional Python module:factory implementing RuntimeConformanceDriver",
    )
    driver_group.add_argument(
        "--command",
        help="Shell-quoted argv for a one-shot subprocess driver; no shell is used",
    )
    parser.add_argument(
        "--driver-name",
        default="subprocess-runtime",
        help="Report identity used with --command",
    )
    parser.add_argument(
        "--command-timeout",
        type=float,
        default=60.0,
        help="Per-case subprocess timeout in seconds",
    )
    parser.add_argument(
        "--max-command-output-bytes",
        type=int,
        default=_MAX_COMMAND_OUTPUT_BYTES,
        help="Maximum stdout bytes accepted from each subprocess case",
    )
    parser.add_argument(
        "--profile",
        choices=CONFORMANCE_PROFILES,
        default="core",
        help="core, resilience-only, or combined deployment profile",
    )
    parser.add_argument("--fixture", type=Path, help="Alternate signed vector JSON")
    parser.add_argument(
        "--output", type=Path, help="Write the JSON report to this path"
    )
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON")
    args = parser.parse_args(argv)

    try:
        if args.driver:
            driver = _load_driver(args.driver)
        elif args.command:
            driver = SubprocessRuntimeConformanceDriver(
                shlex.split(args.command),
                name=args.driver_name,
                timeout_seconds=args.command_timeout,
                max_output_bytes=args.max_command_output_bytes,
            )
        else:
            driver = None
        fixture = load_runtime_conformance_fixture(args.fixture)
        report = asyncio.run(
            run_runtime_conformance(
                driver,
                fixture=fixture,
                profile=args.profile,
            )
        )
    except (
        OSError,
        ValueError,
        TypeError,
        ImportError,
        RuntimeConformanceProtocolError,
    ) as exc:
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
    "CONFORMANCE_COMMAND_PROTOCOL_VERSION",
    "CONFORMANCE_PROFILES",
    "RuntimeConformanceCase",
    "RuntimeConformanceObservation",
    "RuntimeConformanceDriver",
    "RuntimeConformanceProtocolError",
    "RuntimeConformanceCheck",
    "RuntimeConformanceReport",
    "SdkRuntimeConformanceDriver",
    "SubprocessRuntimeConformanceDriver",
    "load_runtime_conformance_fixture",
    "run_runtime_conformance",
    "runtime_conformance_command_main",
    "main",
]
