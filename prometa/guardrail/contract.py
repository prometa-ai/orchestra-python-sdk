"""Wire contract for ``orchestra-guardrail-evaluate-v1``.

Codec only: this module builds and reads the evaluate request/response
documents and carries no transport. It is stdlib-only so the contract
constants stay importable from a default ``prometa-sdk`` installation.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, Mapping, Optional, Protocol, Sequence, Tuple

from ..runtime.admission import RuntimeGuardrail, RuntimeTool
from ..runtime.kernel import GuardDecision, GuardRequest
from ..runtime.security_assurance import SecurityGuardAssessment, SecuritySignal


GUARDRAIL_CONTRACT = "orchestra-guardrail-evaluate-v1"

# The version this build emits, and the inclusive window it accepts. Acceptance
# is always a window comparison so two release trains can be a version apart.
GUARDRAIL_CONTRACT_VERSION = 1
GUARDRAIL_CONTRACT_MIN_SUPPORTED = 1
GUARDRAIL_CONTRACT_MAX_SUPPORTED = 1

GUARDRAIL_EVALUATE_PATH = "/v1/guardrail:evaluate"

GUARDRAIL_STAGES: FrozenSet[str] = frozenset(
    {"llm_input", "llm_output", "tool_call", "tool_result"}
)
# The wire enum. ``escalate`` is deliberately absent: routing a caller to
# human review needs a review plane at the far end of the call, and the HTTP
# binding has none. It survives only in the in-process binding below, where the
# kernel's ``_human_review`` is on the other side of the return.
GUARDRAIL_VERDICTS: FrozenSet[str] = frozenset({"allow", "deny", "transform"})
IN_PROCESS_VERDICTS: FrozenSet[str] = GUARDRAIL_VERDICTS | {"escalate"}
GUARDRAIL_FAIL_MODES: FrozenSet[str] = frozenset({"closed", "open"})

# ``GuardRequest.stage`` uses the kernel's vocabulary; the wire uses its own.
KERNEL_STAGE_TO_WIRE: Mapping[str, str] = {
    "input": "llm_input",
    "output": "llm_output",
    "tool": "tool_call",
    "tool_result": "tool_result",
}

TOOL_REQUIRED_STAGES: FrozenSet[str] = frozenset({"tool_call", "tool_result"})

# Wire stage to the ``security_decisions.surface`` value it is recorded under.
WIRE_STAGE_TO_SURFACE: Mapping[str, str] = {
    "llm_input": "input",
    "llm_output": "output",
    "tool_call": "tool_request",
    "tool_result": "tool_response",
}

# Span identity shared with the kernel's ``runtime.guard.<stage>`` events so a
# control-plane reader joins engine and kernel evidence without a translation.
GUARDRAIL_SPAN_NAME = "guardrail.evaluate"
GUARDRAIL_SPAN_ATTRIBUTES: Tuple[str, ...] = (
    "prometa.guardrail.stage",
    "prometa.guardrail.verdict",
    "prometa.guardrail.profile",
    "prometa.guardrail.evaluated",
    "prometa.guardrail.reason_code",
    "prometa.guardrail.detector_digest",
    "prometa.guardrail.latency_ms",
    "prometa.guardrail.deferred_count",
)

DEFAULT_STAGE_BUDGET_MS: Mapping[str, int] = {
    "llm_input": 25,
    "llm_output": 40,
    "tool_call": 25,
    "tool_result": 40,
}
DEFAULT_TRANSPORT_SLACK_MS = 10
DEFAULT_MAX_PAYLOAD_BYTES = 1048576
DEFAULT_FAIL_OPEN_MAX_CONSECUTIVE = 20
DEFAULT_FAIL_OPEN_WINDOW_SECONDS = 60.0

MIN_BUDGET_MS = 1
MAX_BUDGET_MS = 5000

# ``appliesTo`` values that a stage treats as applicable. ``tool-results`` is
# not in the bundle schema enum yet, so a tool result also honours it.
_STAGE_APPLIES_TO: Mapping[str, FrozenSet[str]] = {
    "llm_input": frozenset({None, "all", "input"}),
    "llm_output": frozenset({None, "all", "output"}),
    "tool_call": frozenset({None, "all", "tool-calls"}),
    "tool_result": frozenset({None, "all", "tool-results"}),
}

_SECURITY_ASSURANCE_FIELDS = (
    "enforcement_mode",
    "review_threshold",
    "enforce_threshold",
    "decision_action",
)

_RESPONSE_KEYS = frozenset(
    {
        "contractVersion",
        "requestId",
        "verdict",
        "reason",
        "reasonCode",
        "evaluatedGuardrails",
        "transformedPayload",
        "assessments",
        "deferred",
        "detectorPack",
        "latencyMs",
        "compat",
    }
)

_ASSESSMENT_KEYS = frozenset(
    {
        "guardrailName",
        "violated",
        "confidenceScore",
        "severity",
        "category",
        "subcategory",
        "detectorKind",
        "detectorDigest",
        "summary",
        "reasonCodes",
        "signals",
        "signalAgreement",
        "evidenceRefs",
        "contentFragmentDigests",
        "counterfactual",
        "actionRationale",
    }
)

_PAYLOAD_KEYS = frozenset({"kind", "text", "json"})
_SIGNAL_KEYS = frozenset({"kind", "score"})
_DETECTOR_PACK_KEYS = frozenset({"id", "version", "digest"})
_DEFERRED_KEYS = frozenset({"guardrailName", "reason", "receiptId"})
_COMPAT_KEYS = frozenset({"unknownFieldsDropped"})

REASON_CODE_TRANSFORM_MISSING = "guardrail_transform_missing"
REASON_CODE_VERDICT_UNRECOGNIZED = "guardrail_verdict_unrecognized"
REASON_CODE_UNAVAILABLE = "guardrail_unavailable"
REASON_CODE_TIMEOUT = "guardrail_timeout"
REASON_CODE_FAIL_OPEN = "guardrail_unavailable_fail_open"
REASON_CODE_FAIL_OPEN_EXHAUSTED = "guardrail_fail_open_budget_exhausted"
REASON_CODE_PAYLOAD_TOO_LARGE = "guardrail_payload_too_large"
REASON_CODE_VERSION_UNSUPPORTED = "guardrail_contract_version_unsupported"
REASON_CODE_RESPONSE_INVALID = "guardrail_response_invalid"
REASON_CODE_COVERAGE_EMPTY = "guardrail_coverage_empty"

# HTTP status to the reason code §2.8 assigns it. Every entry resolves through
# the profile fail mode; none of them is a verdict.
HTTP_STATUS_REASON_CODES: Mapping[int, str] = {
    400: "guardrail_request_invalid",
    401: "guardrail_unauthenticated",
    403: "guardrail_tenant_mismatch",
    404: "guardrail_profile_unknown",
    413: REASON_CODE_PAYLOAD_TOO_LARGE,
    422: "guardrail_request_unsupported",
    429: "guardrail_overloaded",
    500: "guardrail_detector_failed",
}

_NO_DETECTOR_KIND = "none.detector-unavailable"

# A real digest over the empty rule set, computed the same way a detector pack
# computes its own. An all-zero placeholder here would be indistinguishable
# from a corrupt digest, and E3 rejects placeholders outright.
_NO_DETECTOR_DIGEST = "sha256:" + hashlib.sha256(
    json.dumps(
        {"kind": _NO_DETECTOR_KIND, "version": 1, "rules": []},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()


class GuardrailContractError(ValueError):
    """A guardrail document could not be built or read at all."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class GuardrailSubject:
    """Identities the service authorizes and attributes a decision to."""

    tenant: str
    org_id: Optional[str] = None
    agent_id: Optional[str] = None
    release_id: Optional[str] = None
    deployment_id: Optional[str] = None
    environment: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.tenant, str) or not self.tenant.strip():
            raise GuardrailContractError("guardrail_subject_invalid")


@dataclass(frozen=True)
class GuardrailResponse:
    """A decoded evaluate response plus the evidence the kernel cannot carry."""

    decision: GuardDecision
    contract_version: int
    verdict: str
    reason_code: str
    detector_pack: Mapping[str, Any]
    deferred: Tuple[Mapping[str, Any], ...]
    latency_ms: Optional[float]
    unknown_fields_dropped: int
    # §5.2 defines ``compat.unknownFieldsDropped`` as what the SERVER dropped
    # from the REQUEST. It is kept apart from the client's own count above so a
    # request-side skew is observable instead of being conflated away.
    server_unknown_fields_dropped: int = 0


def canonical_json_bytes(value: Any) -> bytes:
    """Encode a payload the way the kernel already canonicalizes messages."""

    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise GuardrailContractError("guardrail_payload_not_json") from exc


def wire_stage(stage: str) -> str:
    """Map a ``GuardRequest.stage`` onto the wire enum."""

    mapped = KERNEL_STAGE_TO_WIRE.get(stage)
    if mapped is None:
        raise GuardrailContractError("guardrail_stage_unsupported")
    return mapped


def applicable_guardrail_names(
    guardrails: Sequence[RuntimeGuardrail], wire_stage_name: str
) -> FrozenSet[str]:
    """Guardrail names in scope for one stage, fired or not."""

    accepted = _STAGE_APPLIES_TO.get(wire_stage_name)
    if accepted is None:
        raise GuardrailContractError("guardrail_stage_unsupported")
    return frozenset(
        guardrail.name for guardrail in guardrails if guardrail.applies_to in accepted
    )


def encode_payload(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, str):
        return {"kind": "text", "text": payload}
    canonical_json_bytes(payload)
    return {"kind": "json", "json": payload}


def payload_size_bytes(payload: Any) -> int:
    if isinstance(payload, str):
        return len(payload.encode("utf-8"))
    return len(canonical_json_bytes(payload))


def encode_guardrail(guardrail: RuntimeGuardrail) -> Dict[str, Any]:
    present = [
        getattr(guardrail, field) is not None for field in _SECURITY_ASSURANCE_FIELDS
    ]
    if any(present) and not all(present):
        raise GuardrailContractError("guardrail_security_fields_partial")
    if all(present) and guardrail.review_threshold > guardrail.enforce_threshold:
        raise GuardrailContractError("guardrail_thresholds_inverted")
    return {
        "name": guardrail.name,
        "guardrailType": guardrail.guardrail_type,
        "onViolation": guardrail.on_violation,
        "appliesTo": guardrail.applies_to,
        "enforcementMode": guardrail.enforcement_mode,
        "reviewThreshold": guardrail.review_threshold,
        "enforceThreshold": guardrail.enforce_threshold,
        "decisionAction": guardrail.decision_action,
    }


def encode_tool(tool: RuntimeTool) -> Dict[str, Any]:
    return {
        "name": tool.name,
        "operation": tool.operation,
        "mcpServer": tool.mcp_server,
        "riskLevel": tool.risk_level,
        "sideEffects": tool.side_effects,
        "requiredGuardrails": list(tool.required_guardrails),
    }


def encode_subject(subject: GuardrailSubject) -> Dict[str, Any]:
    return {
        "tenant": subject.tenant,
        "orgId": subject.org_id,
        "agentId": subject.agent_id,
        "releaseId": subject.release_id,
        "deploymentId": subject.deployment_id,
        "environment": subject.environment,
    }


def encode_evaluate_request(
    request: GuardRequest,
    *,
    profile: str,
    subject: GuardrailSubject,
    budget_ms: int,
    traceparent: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the evaluate document for one ``GuardRequest``."""

    stage = wire_stage(request.stage)
    if len(request.guardrails) > 128:
        raise GuardrailContractError("guardrail_declaration_count_exceeded")
    if stage in TOOL_REQUIRED_STAGES and request.tool is None:
        raise GuardrailContractError("guardrail_tool_required")
    if stage not in TOOL_REQUIRED_STAGES and request.tool is not None:
        raise GuardrailContractError("guardrail_tool_unexpected")
    if not isinstance(budget_ms, int) or not MIN_BUDGET_MS <= budget_ms <= MAX_BUDGET_MS:
        raise GuardrailContractError("guardrail_budget_invalid")
    document: Dict[str, Any] = {
        "contractVersion": GUARDRAIL_CONTRACT_VERSION,
        "requestId": request.request_id,
        "stage": stage,
        "profile": profile,
        "budgetMs": budget_ms,
        "payload": encode_payload(request.payload),
        "guardrails": [
            encode_guardrail(guardrail) for guardrail in request.guardrails
        ],
        "subject": encode_subject(subject),
        "tool": encode_tool(request.tool) if request.tool is not None else None,
        "traceContext": {"traceparent": traceparent} if traceparent else None,
    }
    return document


def _take_known(
    value: Mapping[str, Any], allowed: FrozenSet[str]
) -> Tuple[Dict[str, Any], int]:
    kept = {key: item for key, item in value.items() if key in allowed}
    return kept, len(value) - len(kept)


def _decode_payload(value: Any) -> Tuple[Any, int]:
    if not isinstance(value, Mapping):
        raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
    kept, dropped = _take_known(value, _PAYLOAD_KEYS)
    kind = kept.get("kind")
    if kind == "text":
        text = kept.get("text")
        if not isinstance(text, str):
            raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
        return text, dropped
    if kind == "json":
        if "json" not in kept:
            raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
        return kept["json"], dropped
    raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)


def _decode_signals(values: Any) -> Tuple[Tuple[SecuritySignal, ...], int]:
    if values is None:
        return (), 0
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
    signals = []
    dropped = 0
    for entry in values:
        if not isinstance(entry, Mapping):
            raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
        kept, entry_dropped = _take_known(entry, _SIGNAL_KEYS)
        dropped += entry_dropped
        try:
            signals.append(
                SecuritySignal(kind=str(kept["kind"]), score=float(kept["score"]))
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID) from exc
    return tuple(signals), dropped


def _string_tuple(values: Any) -> Tuple[str, ...]:
    if values is None:
        return ()
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
    if not all(isinstance(value, str) for value in values):
        raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
    return tuple(values)


def _decode_assessment(
    value: Any,
) -> Tuple[SecurityGuardAssessment, int]:
    if not isinstance(value, Mapping):
        raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
    kept, dropped = _take_known(value, _ASSESSMENT_KEYS)
    signals, signal_dropped = _decode_signals(kept.get("signals"))
    dropped += signal_dropped
    defaults = SecurityGuardAssessment
    try:
        assessment = SecurityGuardAssessment(
            guardrail_name=str(kept["guardrailName"]),
            violated=bool(kept["violated"]),
            confidence_score=float(kept["confidenceScore"]),
            severity=str(kept["severity"]),
            category=str(kept["category"]),
            detector_kind=str(kept["detectorKind"]),
            detector_digest=str(kept["detectorDigest"]),
            summary=str(kept.get("summary", "")),
            reason_codes=_string_tuple(kept.get("reasonCodes")),
            signals=signals,
            signal_agreement=str(kept.get("signalAgreement", "single")),
            subcategory=kept.get("subcategory"),
            evidence_refs=_string_tuple(kept.get("evidenceRefs")),
            content_fragment_digests=_string_tuple(
                kept.get("contentFragmentDigests")
            ),
            counterfactual=str(
                kept.get(
                    "counterfactual",
                    defaults.__dataclass_fields__["counterfactual"].default,
                )
            ),
            action_rationale=str(
                kept.get(
                    "actionRationale",
                    defaults.__dataclass_fields__["action_rationale"].default,
                )
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID) from exc
    return assessment, dropped


def decode_evaluate_response(
    document: Any,
    *,
    request_id: str,
    verdicts: FrozenSet[str] = GUARDRAIL_VERDICTS,
) -> GuardrailResponse:
    """Read an evaluate response, dropping and counting unknown members.

    Unknown *values* of ``verdict`` resolve to ``deny``; only unknown fields are
    tolerated, because ignoring an unrecognized verdict is a fail-open.
    """

    if not isinstance(document, Mapping):
        raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
    kept, dropped = _take_known(document, _RESPONSE_KEYS)

    version = kept.get("contractVersion")
    if not isinstance(version, int) or isinstance(version, bool):
        raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
    if not GUARDRAIL_CONTRACT_MIN_SUPPORTED <= version <= GUARDRAIL_CONTRACT_MAX_SUPPORTED:
        raise GuardrailContractError(REASON_CODE_VERSION_UNSUPPORTED)
    if kept.get("requestId") != request_id:
        raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)

    reason = kept.get("reason", "")
    reason_code = kept.get("reasonCode", "")
    if not isinstance(reason, str) or not isinstance(reason_code, str):
        raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)

    evaluated = _string_tuple(kept.get("evaluatedGuardrails"))

    assessments = []
    raw_assessments = kept.get("assessments") or ()
    if isinstance(raw_assessments, (str, bytes)) or not isinstance(
        raw_assessments, Sequence
    ):
        raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
    for entry in raw_assessments:
        assessment, entry_dropped = _decode_assessment(entry)
        dropped += entry_dropped
        assessments.append(assessment)

    deferred = []
    raw_deferred = kept.get("deferred") or ()
    if isinstance(raw_deferred, (str, bytes)) or not isinstance(raw_deferred, Sequence):
        raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
    for entry in raw_deferred:
        if not isinstance(entry, Mapping):
            raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
        entry_kept, entry_dropped = _take_known(entry, _DEFERRED_KEYS)
        dropped += entry_dropped
        deferred.append(entry_kept)

    detector_pack: Dict[str, Any] = {}
    raw_pack = kept.get("detectorPack")
    if raw_pack is not None:
        if not isinstance(raw_pack, Mapping):
            raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
        detector_pack, pack_dropped = _take_known(raw_pack, _DETECTOR_PACK_KEYS)
        dropped += pack_dropped

    raw_compat = kept.get("compat")
    server_dropped = 0
    if raw_compat is not None:
        if not isinstance(raw_compat, Mapping):
            raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID)
        compat, compat_dropped = _take_known(raw_compat, _COMPAT_KEYS)
        dropped += compat_dropped
        reported = compat.get("unknownFieldsDropped")
        if isinstance(reported, int) and not isinstance(reported, bool):
            server_dropped = max(reported, 0)

    latency = kept.get("latencyMs")
    if latency is not None:
        try:
            latency = float(latency)
        except (TypeError, ValueError) as exc:
            raise GuardrailContractError(REASON_CODE_RESPONSE_INVALID) from exc

    verdict = kept.get("verdict")
    transformed = kept.get("transformedPayload")
    transformed_payload = None
    if verdict not in verdicts:
        decision = GuardDecision(
            allowed=False,
            action="deny",
            reason=REASON_CODE_VERDICT_UNRECOGNIZED,
            evaluated_guardrails=evaluated,
            security_assessments=tuple(assessments),
        )
        return GuardrailResponse(
            decision=decision,
            contract_version=version,
            verdict="deny",
            reason_code=REASON_CODE_VERDICT_UNRECOGNIZED,
            detector_pack=detector_pack,
            deferred=tuple(deferred),
            latency_ms=latency,
            unknown_fields_dropped=dropped,
            server_unknown_fields_dropped=server_dropped,
        )

    if verdict == "transform":
        if transformed is None:
            decision = GuardDecision(
                allowed=False,
                action="deny",
                reason=REASON_CODE_TRANSFORM_MISSING,
                evaluated_guardrails=evaluated,
                security_assessments=tuple(assessments),
            )
            return GuardrailResponse(
                decision=decision,
                contract_version=version,
                verdict="deny",
                reason_code=REASON_CODE_TRANSFORM_MISSING,
                detector_pack=detector_pack,
                deferred=tuple(deferred),
                latency_ms=latency,
                unknown_fields_dropped=dropped,
                server_unknown_fields_dropped=server_dropped,
            )
        transformed_payload, payload_dropped = _decode_payload(transformed)
        dropped += payload_dropped

    decision = GuardDecision(
        allowed=verdict in {"allow", "transform"},
        action=verdict,
        reason=reason or reason_code,
        evaluated_guardrails=evaluated,
        transformed_payload=transformed_payload,
        security_assessments=tuple(assessments),
    )
    return GuardrailResponse(
        decision=decision,
        contract_version=version,
        verdict=verdict,
        reason_code=reason_code,
        detector_pack=detector_pack,
        deferred=tuple(deferred),
        latency_ms=latency,
        unknown_fields_dropped=dropped,
        server_unknown_fields_dropped=server_dropped,
    )


def unavailable_assessment(
    guardrail_name: str, *, reason_code: str
) -> SecurityGuardAssessment:
    """Evidence for a guardrail that was declared but never actually scanned.

    ``detectorKind`` names the absence rather than borrowing a real detector's
    identity, so a fail-open is never mistaken for a clean scan downstream.
    """

    return SecurityGuardAssessment(
        guardrail_name=guardrail_name,
        violated=False,
        confidence_score=0.0,
        severity="high",
        category="policy_unavailable",
        detector_kind=_NO_DETECTOR_KIND,
        detector_digest=_NO_DETECTOR_DIGEST,
        summary="Guardrail service produced no verdict; no detector ran.",
        reason_codes=(reason_code,),
        signals=(SecuritySignal(kind="unavailable", score=0.0),),
        signal_agreement="single",
        counterfactual="A reachable guardrail service would have produced a verdict.",
        action_rationale="Applied the profile fail mode for an unusable verdict.",
    )


class GuardrailSpanRecorder(Protocol):
    """Sink for the span every evaluation emits; never raises into the caller."""

    def record(self, name: str, attributes: Mapping[str, Any]) -> None:
        """Persist one ``guardrail.evaluate`` span."""


def guardrail_span_attributes(
    response: GuardrailResponse, *, stage: str, profile: str
) -> Dict[str, Any]:
    """Build the §6 span attributes from one decoded response.

    ``detectorPack``, ``deferred`` and ``latencyMs`` are decoded but do not fit
    in a ``GuardDecision``, so without a producer here three of the eight
    attributes the contract names would have no value source anywhere in the
    SDK and a control-plane reader could not join engine and kernel evidence.
    """

    return {
        "prometa.guardrail.stage": stage,
        "prometa.guardrail.verdict": response.verdict,
        "prometa.guardrail.profile": profile,
        "prometa.guardrail.evaluated": len(response.decision.evaluated_guardrails),
        "prometa.guardrail.reason_code": response.reason_code,
        "prometa.guardrail.detector_digest": response.detector_pack.get("digest"),
        "prometa.guardrail.latency_ms": response.latency_ms,
        "prometa.guardrail.deferred_count": len(response.deferred),
    }


__all__ = [
    "GUARDRAIL_CONTRACT",
    "GUARDRAIL_CONTRACT_VERSION",
    "GUARDRAIL_CONTRACT_MIN_SUPPORTED",
    "GUARDRAIL_CONTRACT_MAX_SUPPORTED",
    "GUARDRAIL_EVALUATE_PATH",
    "GUARDRAIL_STAGES",
    "GUARDRAIL_VERDICTS",
    "IN_PROCESS_VERDICTS",
    "GUARDRAIL_FAIL_MODES",
    "GUARDRAIL_SPAN_ATTRIBUTES",
    "GUARDRAIL_SPAN_NAME",
    "KERNEL_STAGE_TO_WIRE",
    "WIRE_STAGE_TO_SURFACE",
    "TOOL_REQUIRED_STAGES",
    "DEFAULT_STAGE_BUDGET_MS",
    "DEFAULT_TRANSPORT_SLACK_MS",
    "DEFAULT_MAX_PAYLOAD_BYTES",
    "DEFAULT_FAIL_OPEN_MAX_CONSECUTIVE",
    "DEFAULT_FAIL_OPEN_WINDOW_SECONDS",
    "HTTP_STATUS_REASON_CODES",
    "GuardrailContractError",
    "GuardrailSubject",
    "GuardrailResponse",
    "GuardrailSpanRecorder",
    "applicable_guardrail_names",
    "canonical_json_bytes",
    "decode_evaluate_response",
    "encode_evaluate_request",
    "encode_guardrail",
    "encode_payload",
    "encode_subject",
    "encode_tool",
    "guardrail_span_attributes",
    "unavailable_assessment",
    "wire_stage",
]
