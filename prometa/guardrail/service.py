"""Reference implementation of ``orchestra-guardrail-evaluate-v1``.

The service is a pure function of the request document plus the loaded
profiles: same request, same profiles, same response. It holds no caller
content beyond the life of one call and puts none of it in the response except
``transformedPayload``.

Every detector runs in band and must be deterministic and allocation-bounded
*over one scan window*, which is why every scan is windowed and the budget is
re-checked between windows rather than only between detectors: a payload is
caller-controlled and may be a megabyte, so a budget checked only between
detectors is a budget the first detector can ignore. Deferrals carry one of two
reasons: ``budget_exhausted`` and ``detector_unavailable``. There is no
out-of-band band and no asynchronous result path, so ``receiptId`` is always
null.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from ..runtime.admission import RuntimeGuardrail, RuntimeTool
from ..runtime.kernel import GuardDecision, GuardRequest
from ..runtime.security_assurance import SecurityGuardAssessment, SecuritySignal
from .contract import (
    DEFAULT_STAGE_BUDGET_MS,
    GUARDRAIL_SPAN_NAME,
    GUARDRAIL_CONTRACT_MAX_SUPPORTED,
    GUARDRAIL_CONTRACT_MIN_SUPPORTED,
    GUARDRAIL_STAGES,
    IN_PROCESS_VERDICTS,
    MAX_BUDGET_MS,
    MIN_BUDGET_MS,
    REASON_CODE_COVERAGE_EMPTY,
    TOOL_REQUIRED_STAGES,
    GuardrailContractError,
    GuardrailSpanRecorder,
    GuardrailSubject,
    applicable_guardrail_names,
    canonical_json_bytes,
    decode_evaluate_response,
    encode_evaluate_request,
    guardrail_span_attributes,
    wire_stage,
)
from .detectors import (
    UNTRUSTED_PREFIX,
    UNTRUSTED_SUFFIX,
    Detector,
    DetectorContext,
    DetectorError,
    DetectorFinding,
    DetectorSpan,
    apply_spans,
    content_fragment_digest,
    merge_spans,
    rule_digest,
)
from .failmode import (
    FailOpenBudget,
    FailOpenObserver,
    GuardrailUnavailableError,
    apply_fail_mode,
    assert_workload_surface_fail_mode,
)
from .profiles import (
    DECISION_ACTIONS,
    DEFAULT_OVERSIZE_OVERLAP_BYTES,
    ENFORCEMENT_MODES,
    ON_VIOLATIONS,
    GuardrailProfile,
    GuardrailProfileError,
    assert_fail_open_permitted,
    guardrail_is_observe_only,
)


MAX_GUARDRAILS_PER_REQUEST = 128
MAX_REQUEST_ID_LENGTH = 256
MAX_REASON_LENGTH = 512

# The unit of work the budget can interrupt. It bounds both the overshoot past
# ``budgetMs`` and the peak allocation of one scan, so neither grows with the
# caller-supplied payload size.
SCAN_WINDOW_CHARS = 8192

_VERDICT_RANK = {"allow": 0, "transform": 1, "escalate": 2, "deny": 3}

_ON_VIOLATION_VERDICT = {
    "block": "deny",
    "redact": "transform",
    "escalate": "escalate",
    "log": "allow",
}
_DECISION_ACTION_VERDICT = {
    "allow": "allow",
    "deny": "deny",
    "mask": "transform",
    "rewrite": "transform",
}

_VERDICT_REASON_CODE = {
    "allow": "guardrail_allowed",
    "transform": "guardrail_content_transformed",
    "deny": "guardrail_blocked",
    "escalate": "guardrail_escalation_required",
}

_REQUEST_KEYS = frozenset(
    {
        "contractVersion",
        "requestId",
        "stage",
        "profile",
        "budgetMs",
        "payload",
        "guardrails",
        "subject",
        "tool",
        "traceContext",
    }
)
_REQUEST_GUARDRAIL_KEYS = frozenset(
    {
        "name",
        "guardrailType",
        "onViolation",
        "appliesTo",
        "enforcementMode",
        "reviewThreshold",
        "enforceThreshold",
        "decisionAction",
    }
)
_SUBJECT_KEYS = frozenset(
    {"tenant", "orgId", "agentId", "releaseId", "deploymentId", "environment"}
)
_TOOL_KEYS = frozenset(
    {
        "name",
        "operation",
        "mcpServer",
        "riskLevel",
        "sideEffects",
        "requiredGuardrails",
    }
)
_PAYLOAD_KEYS = frozenset({"kind", "text", "json"})
_TRACE_KEYS = frozenset({"traceparent"})

_ENVIRONMENTS = frozenset({"dev", "test", "staging", "prod"})


class GuardrailRequestError(Exception):
    """A request the service refuses, carrying its §2.8 status and code."""

    def __init__(
        self, status: int, reason_code: str, detail: Optional[Mapping[str, Any]] = None
    ) -> None:
        self.status = status
        self.reason_code = reason_code
        self.detail = dict(detail or {})
        super().__init__(reason_code)


@dataclass(frozen=True)
class GuardrailServiceResponse:
    """One HTTP-shaped result; the host adds only transport concerns."""

    status: int
    body: Dict[str, Any]


JSON_SEGMENT_SEPARATOR = "\n"


@dataclass(frozen=True)
class _ScanPayload:
    """The payload split into what detectors read and what the ceiling bounds."""

    kind: str
    text: str
    value: Any
    segments: Tuple[Tuple[int, int], ...]
    size_bytes: int
    unknown_fields_dropped: int


_REWRITE_UNAVAILABLE = object()


class _JsonRewriteUnavailable(Exception):
    """The rewritten strings cannot be put back without changing the shape."""


def _framed(text: str) -> str:
    return "%s\n%s\n%s" % (UNTRUSTED_PREFIX, text, UNTRUSTED_SUFFIX)


def _json_strings(value: Any) -> Iterator[str]:
    """Every string leaf, keys included, in one fixed traversal order.

    ``_json_rebuild`` walks the same order, so the two stay paired without
    carrying a path per leaf.
    """

    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for key, item in value.items():
            if isinstance(key, str):
                yield key
            yield from _json_strings(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _json_strings(item)


def _json_rebuild(value: Any, replacements: Iterator[str]) -> Any:
    if isinstance(value, str):
        return next(replacements)
    if isinstance(value, Mapping):
        rebuilt: Dict[str, Any] = {}
        for key, item in value.items():
            new_key = next(replacements) if isinstance(key, str) else key
            rebuilt[new_key] = _json_rebuild(item, replacements)
        if len(rebuilt) != len(value):
            raise _JsonRewriteUnavailable()
        return rebuilt
    if isinstance(value, (list, tuple)):
        return [_json_rebuild(item, replacements) for item in value]
    return value


def _json_projection(value: Any) -> Tuple[str, Tuple[Tuple[int, int], ...]]:
    """Project a JSON payload onto the text a detector must actually see.

    Detectors anchor on real newlines, quotes and control characters. The
    canonical encoding escapes every one of those inside a string value, so
    scanning it hides exactly the shapes the injection families are written
    for. The leaves are therefore scanned decoded.

    The separator is a newline because the line-anchored families must be able
    to fire on a leaf that *begins* with ``system:`` — a leaf boundary is a turn
    boundary as far as the model reading this result is concerned. A match may
    consequently span two leaves; ``_rewrite_json`` refuses to rewrite such a
    span rather than mangling the shape, and the refusal becomes a denial.
    """

    parts: List[str] = []
    segments: List[Tuple[int, int]] = []
    offset = 0
    for text in _json_strings(value):
        if parts:
            parts.append(JSON_SEGMENT_SEPARATOR)
            offset += len(JSON_SEGMENT_SEPARATOR)
        segments.append((offset, offset + len(text)))
        parts.append(text)
        offset += len(text)
    return "".join(parts), tuple(segments)


@dataclass(frozen=True)
class _DecodedRequest:
    contract_version: int
    request_id: str
    stage: str
    profile: GuardrailProfile
    budget_ms: int
    payload_kind: str
    payload_text: str
    payload_value: Any
    payload_segments: Tuple[Tuple[int, int], ...]
    guardrails: Tuple[RuntimeGuardrail, ...]
    subject: GuardrailSubject
    tool: Optional[Dict[str, Any]]
    unknown_fields_dropped: int


@dataclass(frozen=True)
class _Unit:
    name: str
    guardrail_type: str
    guardrail: Optional[RuntimeGuardrail]

    @property
    def observe_only(self) -> bool:
        if self.guardrail is None:
            return False
        return guardrail_is_observe_only(self.guardrail)


@dataclass(frozen=True)
class _Deferral:
    guardrail_name: str
    reason: str
    # Always ``None`` here: there is no asynchronous delivery path to correlate
    # a receipt against, and an identifier nothing can be redeemed for reads as
    # a promise that a result is coming.
    receipt_id: Optional[str]


@dataclass(frozen=True)
class _UnitOutcome:
    unit: _Unit
    verdict: str
    finding: DetectorFinding
    detector_kind: str
    detector_digest: str
    detectors_run: int = 0
    deferrals: Tuple[_Deferral, ...] = ()
    no_scan_reason: Optional[str] = None

    @property
    def enforcing_deferrals(self) -> Tuple[_Deferral, ...]:
        """Deferrals the profile's ``deferPolicy`` is allowed to act on.

        Only budget exhaustion qualifies: an observe-only guardrail cannot
        block, so its unscanned remainder must not become a denial either.
        """

        if self.unit.observe_only:
            return ()
        return tuple(
            deferral
            for deferral in self.deferrals
            if deferral.reason == "budget_exhausted"
        )

    @property
    def evidence_reason(self) -> str:
        """Why nothing was scanned, for a unit that ran no detector."""

        if self.no_scan_reason is not None:
            return self.no_scan_reason
        for deferral in self.deferrals:
            return deferral.reason
        return "detector_unavailable"


def _take_known(value: Mapping[str, Any], allowed) -> Tuple[Dict[str, Any], int]:
    kept = {key: item for key, item in value.items() if key in allowed}
    return kept, len(value) - len(kept)


def _require(document: Mapping[str, Any], key: str) -> Any:
    if key not in document or document[key] is None:
        raise GuardrailRequestError(400, "guardrail_request_invalid")
    return document[key]


class _BudgetClock:
    """One request's remaining in-band budget.

    Held per call rather than on the service: the host serves concurrently, and
    a shared exhaustion flag would let one slow request defer another's
    detectors.
    """

    def __init__(
        self, started: float, budget_ms: int, monotonic: Callable[[], float]
    ) -> None:
        self._started = started
        self._budget_ms = budget_ms
        self._monotonic = monotonic
        self._tripped = False

    def exhausted(self) -> bool:
        if not self._tripped:
            self._tripped = (
                self._monotonic() - self._started
            ) * 1000.0 >= self._budget_ms
        return self._tripped


def _windows(text: str, size: int, overlap: int):
    if len(text) <= size:
        yield (0, text)
        return
    step = max(1, size - overlap)
    start = 0
    while start < len(text):
        yield (start, text[start : start + size])
        if start + size >= len(text):
            return
        start += step


class GuardrailService:
    """The evaluate endpoint's behaviour, independent of any transport."""

    def __init__(
        self,
        profiles: Mapping[str, GuardrailProfile],
        *,
        default_profile: Optional[str] = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not profiles:
            raise GuardrailProfileError("guardrail_profiles_empty")
        self._profiles = dict(profiles)
        self._packs = {
            profile_id: profile.build_pack()
            for profile_id, profile in self._profiles.items()
        }
        selected = default_profile or sorted(self._profiles)[0]
        if selected not in self._profiles:
            raise GuardrailProfileError("guardrail_default_profile_unknown")
        self.default_profile = selected
        self._monotonic = monotonic

    @property
    def profiles(self) -> Mapping[str, GuardrailProfile]:
        return dict(self._profiles)

    def pack_for(self, profile_id: str):
        return self._packs[profile_id]

    def readiness(self) -> Dict[str, Any]:
        return {
            "status": "ok",
            "profiles": sorted(self._profiles),
            "detectorPack": self._packs[self.default_profile].descriptor(),
        }

    def evaluate(
        self, document: Any, *, escalation_supported: bool = False
    ) -> GuardrailServiceResponse:
        """Evaluate one request document into a response document.

        ``escalation_supported`` is false for the HTTP binding, whose caller has
        no review plane to route to, and true only for the in-process binding
        that hands the decision straight back to a kernel that does.
        """

        started = self._monotonic()
        try:
            request = self._decode(document)
        except GuardrailRequestError as error:
            return self._error_response(error)
        try:
            body = self._evaluate(
                request, started, escalation_supported=escalation_supported
            )
        except GuardrailRequestError as error:
            return self._error_response(error)
        return GuardrailServiceResponse(status=200, body=body)

    @staticmethod
    def _error_response(
        error: GuardrailRequestError,
    ) -> GuardrailServiceResponse:
        body: Dict[str, Any] = {
            "error": {
                "message": error.reason_code.replace("_", " "),
                "type": "guardrail_error",
                "code": error.reason_code,
                "param": None,
            }
        }
        body.update(error.detail)
        return GuardrailServiceResponse(status=error.status, body=body)

    def _decode(self, document: Any) -> _DecodedRequest:
        if not isinstance(document, Mapping):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        kept, dropped = _take_known(document, _REQUEST_KEYS)

        version = _require(kept, "contractVersion")
        if isinstance(version, bool) or not isinstance(version, int):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        if not (
            GUARDRAIL_CONTRACT_MIN_SUPPORTED
            <= version
            <= GUARDRAIL_CONTRACT_MAX_SUPPORTED
        ):
            raise GuardrailRequestError(
                422,
                "guardrail_contract_version_unsupported",
                {
                    "supported": {
                        "min": GUARDRAIL_CONTRACT_MIN_SUPPORTED,
                        "max": GUARDRAIL_CONTRACT_MAX_SUPPORTED,
                    }
                },
            )

        request_id = _require(kept, "requestId")
        if (
            not isinstance(request_id, str)
            or not request_id.strip()
            or request_id != request_id.strip()
            or len(request_id) > MAX_REQUEST_ID_LENGTH
        ):
            raise GuardrailRequestError(400, "guardrail_request_invalid")

        stage = _require(kept, "stage")
        if not isinstance(stage, str):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        if stage not in GUARDRAIL_STAGES:
            raise GuardrailRequestError(422, "guardrail_request_unsupported")

        profile_id = _require(kept, "profile")
        if not isinstance(profile_id, str):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        profile = self._profiles.get(profile_id)
        if profile is None:
            raise GuardrailRequestError(404, "guardrail_profile_unknown")

        budget_ms = _require(kept, "budgetMs")
        if isinstance(budget_ms, bool) or not isinstance(budget_ms, int):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        if not MIN_BUDGET_MS <= budget_ms <= MAX_BUDGET_MS:
            raise GuardrailRequestError(422, "guardrail_request_unsupported")

        payload = self._decode_payload(_require(kept, "payload"))
        dropped += payload.unknown_fields_dropped

        raw_guardrails = kept.get("guardrails")
        if raw_guardrails is None or isinstance(raw_guardrails, (str, bytes)):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        if not isinstance(raw_guardrails, (list, tuple)):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        if not raw_guardrails:
            # The caller supplies membership; the profile only defines what
            # each selected name is. An empty array is therefore a
            # misconfigured caller, and answering
            # it 200/allow would make that indistinguishable from an enforced
            # request.
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        if len(raw_guardrails) > MAX_GUARDRAILS_PER_REQUEST:
            raise GuardrailRequestError(422, "guardrail_request_unsupported")
        guardrails: List[RuntimeGuardrail] = []
        for entry in raw_guardrails:
            guardrail, entry_dropped = self._decode_guardrail(entry, profile)
            dropped += entry_dropped
            guardrails.append(guardrail)

        subject, subject_dropped = self._decode_subject(_require(kept, "subject"))
        dropped += subject_dropped

        raw_tool = kept.get("tool")
        if stage in TOOL_REQUIRED_STAGES:
            if raw_tool is None:
                raise GuardrailRequestError(400, "guardrail_request_invalid")
            tool, tool_dropped = self._decode_tool(raw_tool)
            dropped += tool_dropped
        else:
            if raw_tool is not None:
                raise GuardrailRequestError(422, "guardrail_request_unsupported")
            tool = None

        raw_trace = kept.get("traceContext")
        if raw_trace is not None:
            if not isinstance(raw_trace, Mapping):
                raise GuardrailRequestError(400, "guardrail_request_invalid")
            _, trace_dropped = _take_known(raw_trace, _TRACE_KEYS)
            dropped += trace_dropped

        if (
            payload.size_bytes > profile.max_payload_bytes
            and profile.oversize_policy != "chunk"
        ):
            raise GuardrailRequestError(413, "guardrail_payload_too_large")

        return _DecodedRequest(
            contract_version=version,
            request_id=request_id,
            stage=stage,
            profile=profile,
            budget_ms=budget_ms,
            payload_kind=payload.kind,
            payload_text=payload.text,
            payload_value=payload.value,
            payload_segments=payload.segments,
            guardrails=tuple(guardrails),
            subject=subject,
            tool=tool,
            unknown_fields_dropped=dropped,
        )

    @staticmethod
    def _decode_payload(value: Any) -> "_ScanPayload":
        if not isinstance(value, Mapping):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        kept, dropped = _take_known(value, _PAYLOAD_KEYS)
        kind = kept.get("kind")
        if kind == "text":
            text = kept.get("text")
            if not isinstance(text, str):
                raise GuardrailRequestError(400, "guardrail_request_invalid")
            return _ScanPayload(
                kind="text",
                text=text,
                value=text,
                segments=(),
                size_bytes=len(text.encode("utf-8")),
                unknown_fields_dropped=dropped,
            )
        if kind == "json":
            if "json" not in kept:
                raise GuardrailRequestError(400, "guardrail_request_invalid")
            try:
                encoded = canonical_json_bytes(kept["json"])
            except GuardrailContractError as exc:
                raise GuardrailRequestError(400, "guardrail_request_invalid") from exc
            text, segments = _json_projection(kept["json"])
            return _ScanPayload(
                kind="json",
                text=text,
                value=kept["json"],
                segments=segments,
                # The wire size is the encoding, not the projection: the
                # ceiling has to bound what the caller actually sent.
                size_bytes=len(encoded),
                unknown_fields_dropped=dropped,
            )
        raise GuardrailRequestError(422, "guardrail_request_unsupported")

    @staticmethod
    def _decode_guardrail(
        value: Any, profile: GuardrailProfile
    ) -> Tuple[RuntimeGuardrail, int]:
        """Resolve one caller-selected name against the profile's definition.

        Selection is the caller's, definition is the service's: the request
        says *which* guardrails apply, the profile says what each one *is*. A
        caller holding only a name list sends the type and action as null and
        gets the profile's; a caller holding a bundle may restate them, but
        only identically. Honouring a restatement that differs would let a
        request soften its own policy on the wire, which is the bypass this
        split exists to close.
        """

        if not isinstance(value, Mapping):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        kept, dropped = _take_known(value, _REQUEST_GUARDRAIL_KEYS)
        name = kept.get("name")
        if not isinstance(name, str) or not name:
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        definition = profile.guardrails.get(name)
        if definition is None:
            raise GuardrailRequestError(422, "guardrail_request_unsupported")

        guardrail_type = kept.get("guardrailType")
        if guardrail_type is None:
            guardrail_type = definition.guardrail_type
        elif not isinstance(guardrail_type, str) or not guardrail_type:
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        elif guardrail_type != definition.guardrail_type:
            raise GuardrailRequestError(422, "guardrail_request_unsupported")

        on_violation = kept.get("onViolation")
        if on_violation is None:
            on_violation = definition.on_violation
        elif on_violation not in ON_VIOLATIONS:
            raise GuardrailRequestError(422, "guardrail_request_unsupported")
        elif on_violation != definition.on_violation:
            raise GuardrailRequestError(422, "guardrail_request_unsupported")

        applies_to = kept.get("appliesTo")
        if applies_to is None:
            applies_to = definition.applies_to
        elif not isinstance(applies_to, str):
            raise GuardrailRequestError(422, "guardrail_request_unsupported")
        elif definition.applies_to is not None and applies_to != definition.applies_to:
            raise GuardrailRequestError(422, "guardrail_request_unsupported")

        enforcement_mode = kept.get("enforcementMode")
        if enforcement_mode is not None and enforcement_mode not in ENFORCEMENT_MODES:
            raise GuardrailRequestError(422, "guardrail_request_unsupported")
        decision_action = kept.get("decisionAction")
        if decision_action is not None and decision_action not in DECISION_ACTIONS:
            raise GuardrailRequestError(422, "guardrail_request_unsupported")
        review_threshold = GuardrailService._decode_threshold(kept.get("reviewThreshold"))
        enforce_threshold = GuardrailService._decode_threshold(
            kept.get("enforceThreshold")
        )
        present = [
            enforcement_mode is not None,
            review_threshold is not None,
            enforce_threshold is not None,
            decision_action is not None,
        ]
        if any(present) and not all(present):
            raise GuardrailRequestError(422, "guardrail_request_unsupported")
        if all(present) and review_threshold > enforce_threshold:
            raise GuardrailRequestError(422, "guardrail_request_unsupported")
        if not any(present):
            enforcement_mode = definition.enforcement_mode
            review_threshold = definition.review_threshold
            enforce_threshold = definition.enforce_threshold
            decision_action = definition.decision_action
        elif definition.enforcement_mode is not None and (
            enforcement_mode != definition.enforcement_mode
            or review_threshold != definition.review_threshold
            or enforce_threshold != definition.enforce_threshold
            or decision_action != definition.decision_action
        ):
            raise GuardrailRequestError(422, "guardrail_request_unsupported")

        return (
            RuntimeGuardrail(
                name=name,
                guardrail_type=guardrail_type,
                on_violation=on_violation,
                applies_to=applies_to,
                enforcement_mode=enforcement_mode,
                review_threshold=review_threshold,
                enforce_threshold=enforce_threshold,
                decision_action=decision_action,
            ),
            dropped,
        )

    @staticmethod
    def _decode_threshold(value: Any) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise GuardrailRequestError(422, "guardrail_request_unsupported")
        if not 0.0 <= float(value) <= 1.0:
            raise GuardrailRequestError(422, "guardrail_request_unsupported")
        return float(value)

    @staticmethod
    def _decode_subject(value: Any) -> Tuple[GuardrailSubject, int]:
        if not isinstance(value, Mapping):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        kept, dropped = _take_known(value, _SUBJECT_KEYS)
        tenant = kept.get("tenant")
        if not isinstance(tenant, str) or not tenant.strip():
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        environment = kept.get("environment")
        if environment is not None and environment not in _ENVIRONMENTS:
            raise GuardrailRequestError(422, "guardrail_request_unsupported")
        return (
            GuardrailSubject(
                tenant=tenant,
                org_id=kept.get("orgId"),
                agent_id=kept.get("agentId"),
                release_id=kept.get("releaseId"),
                deployment_id=kept.get("deploymentId"),
                environment=environment,
            ),
            dropped,
        )

    @staticmethod
    def _decode_tool(value: Any) -> Tuple[Dict[str, Any], int]:
        if not isinstance(value, Mapping):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        kept, dropped = _take_known(value, _TOOL_KEYS)
        name = kept.get("name")
        operation = kept.get("operation")
        if not isinstance(name, str) or not isinstance(operation, str):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        required = kept.get("requiredGuardrails") or ()
        if isinstance(required, (str, bytes)) or not isinstance(required, (list, tuple)):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        if not all(isinstance(item, str) and item for item in required):
            raise GuardrailRequestError(400, "guardrail_request_invalid")
        return (
            {
                "name": name,
                "operation": operation,
                "mcpServer": kept.get("mcpServer"),
                "riskLevel": kept.get("riskLevel"),
                "sideEffects": kept.get("sideEffects"),
                "requiredGuardrails": list(required),
            },
            dropped,
        )

    def _units(self, request: _DecodedRequest) -> Tuple[Tuple[_Unit, ...], Tuple[str, ...]]:
        applicable = applicable_guardrail_names(request.guardrails, request.stage)
        units = [
            _Unit(
                name=guardrail.name,
                guardrail_type=guardrail.guardrail_type,
                guardrail=guardrail,
            )
            for guardrail in request.guardrails
            if guardrail.name in applicable
        ]
        covered = {unit.name for unit in units} | {unit.guardrail_type for unit in units}
        required = tuple((request.tool or {}).get("requiredGuardrails", ()))
        for identifier in required:
            if identifier in covered:
                continue
            covered.add(identifier)
            units.append(
                _Unit(name=identifier, guardrail_type=identifier, guardrail=None)
            )
        return tuple(units), required

    def _scan(
        self,
        detector: Detector,
        request: _DecodedRequest,
        unit: _Unit,
        budget: "_BudgetClock",
    ) -> Tuple[DetectorFinding, bool]:
        """Scan the payload in bounded windows; report whether it was covered.

        The windows overlap by ``DEFAULT_OVERSIZE_OVERLAP_BYTES`` so a pattern
        straddling a boundary is still matched. A window that starts after the
        budget is gone is not run, and the uncovered remainder is reported to
        the caller rather than silently skipped: an unscanned tail is exactly
        the region an attacker would push an injection into.
        """

        context = DetectorContext(
            stage=request.stage,
            guardrail_name=unit.name,
            guardrail_type=unit.guardrail_type,
            tool=request.tool,
            settings=request.profile.detector_settings,
        )
        size = min(SCAN_WINDOW_CHARS, request.profile.max_payload_bytes)
        findings: List[DetectorFinding] = []
        covered = True
        for offset, window in _windows(
            request.payload_text, size, DEFAULT_OVERSIZE_OVERLAP_BYTES
        ):
            if offset and budget.exhausted():
                covered = False
                break
            finding = detector.scan(window, context)
            if finding.violated or finding.escalate:
                findings.append(_shift_finding(finding, offset))
        return _merge_findings(findings), covered

    def _evaluate(
        self,
        request: _DecodedRequest,
        started: float,
        *,
        escalation_supported: bool,
    ) -> Dict[str, Any]:
        profile = request.profile
        pack = self._packs[profile.profile_id]
        units, required = self._units(request)
        outcomes: List[_UnitOutcome] = []
        budget = _BudgetClock(started, request.budget_ms, self._monotonic)
        for unit in units:
            detectors = pack.select_all(unit.guardrail_type, request.stage)
            if not detectors:
                outcomes.append(self._uncovered_outcome(unit, pack, profile))
                continue
            outcomes.append(self._unit_outcome(unit, detectors, request, budget))
        return self._respond(
            request,
            pack,
            outcomes,
            required,
            started,
            escalation_supported=escalation_supported,
        )

    @staticmethod
    def _uncovered_outcome(unit: _Unit, pack, profile: GuardrailProfile) -> _UnitOutcome:
        if pack.serves(unit.guardrail_type):
            return _UnitOutcome(
                unit=unit,
                verdict="allow",
                finding=DetectorFinding(violated=False),
                detector_kind=_NO_STAGE_DETECTOR_KIND,
                detector_digest=_no_detector_digest(_NO_STAGE_DETECTOR_KIND),
                no_scan_reason="stage_not_applicable",
            )
        return _UnitOutcome(
            unit=unit,
            verdict=("deny" if profile.unknown_guardrail_policy == "deny" else "allow"),
            finding=DetectorFinding(violated=False),
            detector_kind=_NO_TYPE_DETECTOR_KIND,
            detector_digest=_no_detector_digest(_NO_TYPE_DETECTOR_KIND),
            deferrals=(_Deferral(unit.name, "detector_unavailable", None),),
            no_scan_reason="detector_unavailable",
        )

    def _unit_outcome(
        self,
        unit: _Unit,
        detectors: Sequence[Detector],
        request: _DecodedRequest,
        budget: "_BudgetClock",
    ) -> _UnitOutcome:
        deferrals: List[_Deferral] = []
        scanned: List[Tuple[DetectorFinding, Detector]] = []
        ran: List[Detector] = []
        for detector in detectors:
            if budget.exhausted():
                deferrals.append(
                    _Deferral(unit.name, "budget_exhausted", None)
                )
                continue
            try:
                finding, covered = self._scan(detector, request, unit, budget)
            except DetectorError as exc:
                raise GuardrailRequestError(500, "guardrail_detector_failed") from exc
            ran.append(detector)
            if not covered:
                deferrals.append(
                    _Deferral(unit.name, "budget_exhausted", None)
                )
            if finding.violated or finding.escalate:
                scanned.append((finding, detector))
        merged = _merge_findings([finding for finding, _ in scanned])
        if scanned:
            attributed = max(scanned, key=lambda entry: entry[0].confidence)[1]
        elif ran:
            attributed = ran[0]
        else:
            attributed = detectors[0]
        return _UnitOutcome(
            unit=unit,
            verdict=self._unit_verdict(unit, merged, request),
            finding=merged,
            detector_kind=attributed.kind,
            detector_digest=attributed.digest,
            detectors_run=len(ran),
            deferrals=tuple(deferrals),
        )

    @staticmethod
    def _unit_verdict(
        unit: _Unit, finding: DetectorFinding, request: _DecodedRequest
    ) -> str:
        if finding.escalate:
            return "escalate"
        if not finding.violated:
            return "allow"
        guardrail = unit.guardrail
        if guardrail is not None and guardrail.security_assurance_enabled:
            if (
                guardrail.enforcement_mode == "enforce"
                and guardrail.enforce_threshold is not None
                and finding.confidence >= guardrail.enforce_threshold
            ):
                return _DECISION_ACTION_VERDICT[guardrail.decision_action or "allow"]
            return "allow"
        on_violation = guardrail.on_violation if guardrail is not None else "block"
        verdict = _ON_VIOLATION_VERDICT[on_violation]
        if (
            request.stage == "tool_result"
            and verdict == "transform"
            and finding.category == "prompt_injection"
            and request.profile.tool_result_injection_verdict == "deny"
        ):
            return "deny"
        return verdict

    def _respond(
        self,
        request: _DecodedRequest,
        pack,
        outcomes: Sequence[_UnitOutcome],
        required: Sequence[str],
        started: float,
        *,
        escalation_supported: bool,
    ) -> Dict[str, Any]:
        profile = request.profile
        evaluated = sorted(
            {outcome.unit.name for outcome in outcomes} | set(required)
        )
        deferred = [
            {
                "guardrailName": deferral.guardrail_name,
                "reason": deferral.reason,
                "receiptId": deferral.receipt_id,
            }
            for outcome in outcomes
            for deferral in outcome.deferrals
        ]
        verdict = "allow"
        for outcome in outcomes:
            if _VERDICT_RANK[outcome.verdict] > _VERDICT_RANK[verdict]:
                verdict = outcome.verdict
        deferred_enforcing = [
            deferral for outcome in outcomes for deferral in outcome.enforcing_deferrals
        ]
        if deferred_enforcing and profile.defer_policy == "deny":
            verdict = "deny"
            reason_code = "guardrail_deferred_enforcing"
        elif deferred_enforcing:
            verdict = "deny"
            reason_code = "guardrail_defer_policy_unusable"
        elif verdict == "escalate" and not escalation_supported:
            verdict = "deny"
            reason_code = "guardrail_escalation_unsupported"
        else:
            reason_code = _VERDICT_REASON_CODE[verdict]
        unknown_type = [
            deferral
            for outcome in outcomes
            for deferral in outcome.deferrals
            if deferral.reason == "detector_unavailable"
        ]
        if unknown_type and verdict == "deny":
            reason_code = "guardrail_guardrail_type_unknown"

        transformed_payload = None
        if verdict == "transform":
            transformed_payload = self._transform(request, outcomes)
            if transformed_payload is None:
                verdict = "deny"
                reason_code = "guardrail_transform_unavailable"

        assessments = self._assessments(request, outcomes)
        violations = sum(1 for outcome in outcomes if outcome.finding.violated)
        latency_ms = (self._monotonic() - started) * 1000.0
        return {
            "contractVersion": min(
                request.contract_version, GUARDRAIL_CONTRACT_MAX_SUPPORTED
            ),
            "requestId": request.request_id,
            "verdict": verdict,
            "reason": (
                "evaluated %d guardrail(s); %d violated; %d deferred"
                % (len(evaluated), violations, len(deferred))
            )[:MAX_REASON_LENGTH],
            "reasonCode": reason_code,
            "evaluatedGuardrails": evaluated,
            "transformedPayload": transformed_payload,
            "assessments": assessments,
            "deferred": deferred,
            "detectorPack": pack.descriptor(),
            "latencyMs": latency_ms,
            "compat": {"unknownFieldsDropped": request.unknown_fields_dropped},
        }

    def _transform(
        self, request: _DecodedRequest, outcomes: Sequence[_UnitOutcome]
    ) -> Optional[Dict[str, Any]]:
        spans: List[DetectorSpan] = []
        wrap = False
        for outcome in outcomes:
            if outcome.verdict != "transform":
                continue
            spans.extend(outcome.finding.spans)
            wrap = wrap or outcome.finding.wrap_untrusted
        if not spans:
            return None
        if request.payload_kind == "text":
            text = apply_spans(request.payload_text, spans)
            return {"kind": "text", "text": _framed(text) if wrap else text}
        rebuilt = self._rewrite_json(request, spans)
        if rebuilt is _REWRITE_UNAVAILABLE:
            return None
        if wrap:
            # A JSON result carries no place to put prose, so the framing the
            # model must see costs the shape. Only a result that already had
            # to be rewritten is reshaped this way.
            return {
                "kind": "text",
                "text": _framed(canonical_json_bytes(rebuilt).decode("utf-8")),
            }
        return {"kind": "json", "json": rebuilt}

    @staticmethod
    def _rewrite_json(request: _DecodedRequest, spans: Sequence[DetectorSpan]) -> Any:
        """Put the neutralized leaves back, or refuse the rewrite entirely.

        A span that reaches across two leaves, or a rewritten key that collides
        with another key, cannot be applied without inventing a shape the
        caller did not send. ``_respond`` turns the ``None`` this produces into
        ``deny`` / ``guardrail_transform_unavailable``, so an unrewritable
        finding is never released as the original content.
        """

        merged = merge_spans(spans)
        replacements: List[str] = []
        for start, end in request.payload_segments:
            inner: List[DetectorSpan] = []
            for span in merged:
                if span.end <= start or span.start >= end:
                    continue
                if span.start < start or span.end > end:
                    return _REWRITE_UNAVAILABLE
                inner.append(
                    DetectorSpan(
                        start=span.start - start,
                        end=span.end - start,
                        label=span.label,
                        replacement=span.replacement,
                    )
                )
            replacements.append(apply_spans(request.payload_text[start:end], inner))
        stream = iter(replacements)
        try:
            rebuilt = _json_rebuild(request.payload_value, stream)
        except (_JsonRewriteUnavailable, StopIteration):
            return _REWRITE_UNAVAILABLE
        if next(stream, None) is not None:
            return _REWRITE_UNAVAILABLE
        return rebuilt

    def _assessments(
        self, request: _DecodedRequest, outcomes: Sequence[_UnitOutcome]
    ) -> List[Dict[str, Any]]:
        assessments: List[Dict[str, Any]] = []
        for outcome in outcomes:
            guardrail = outcome.unit.guardrail
            if guardrail is None or not guardrail.security_assurance_enabled:
                continue
            reasons = tuple(deferral.reason for deferral in outcome.deferrals)
            if not outcome.detectors_run:
                assessment = _assessment_without_scan(
                    guardrail.name,
                    detector_kind=outcome.detector_kind,
                    detector_digest=outcome.detector_digest,
                    reason_code=outcome.evidence_reason,
                )
            else:
                assessment = _assessment_from_finding(
                    guardrail.name,
                    outcome.finding,
                    detector_kind=outcome.detector_kind,
                    detector_digest=outcome.detector_digest,
                    text=request.payload_text,
                    deferred_reasons=reasons,
                )
            assessments.append(_encode_assessment(assessment))
        return assessments


_NO_STAGE_DETECTOR_KIND = "none.stage-not-applicable"
_NO_TYPE_DETECTOR_KIND = "none.detector-unavailable"


def _no_detector_digest(kind: str) -> str:
    """A real digest over an empty rule set, computed like any pack digest.

    An all-zero placeholder would be indistinguishable from a corrupt digest,
    and a reader cannot tell a fabricated assessment from a real one if the
    identity fields are constants.
    """

    return rule_digest(kind, 1, [])


# What each "nothing was scanned" outcome may truthfully claim. The prose is
# separated per reason because a single "the service produced no verdict, so
# the fail mode was applied" line is false for three of these four: a verdict
# was produced, no fail mode ran, and in two of them the detector exists.
_NO_SCAN_EVIDENCE: Mapping[str, Mapping[str, str]] = {
    "stage_not_applicable": {
        "severity": "low",
        "category": "policy_not_applicable",
        "signal": "not_applicable",
        "summary": "No detector for this guardrail type is valid at this stage.",
        "counterfactual": (
            "A stage this guardrail type serves would have produced a scan."
        ),
        "rationale": (
            "No detector serves this guardrail type at this stage, so no "
            "content was scanned and nothing was found or missed."
        ),
    },
    "detector_unavailable": {
        "severity": "high",
        "category": "policy_unavailable",
        "signal": "unavailable",
        "summary": "No detector in the active pack serves this guardrail type.",
        "counterfactual": (
            "A pack carrying a detector for this guardrail type would have "
            "produced a verdict."
        ),
        "rationale": "Applied the profile unknown-guardrail policy.",
    },
    "budget_exhausted": {
        "severity": "high",
        "category": "policy_deferred",
        "signal": "deferred",
        "summary": "The in-band budget ended before this guardrail was scanned.",
        "counterfactual": (
            "A budget covering the whole payload would have produced a verdict."
        ),
        "rationale": "Applied the profile defer policy for an unscanned guardrail.",
    },
}


def _assessment_without_scan(
    guardrail_name: str,
    *,
    detector_kind: str,
    detector_digest: str,
    reason_code: str,
) -> SecurityGuardAssessment:
    """Evidence for a unit no detector actually scanned, saying only that."""

    evidence = _NO_SCAN_EVIDENCE.get(
        reason_code, _NO_SCAN_EVIDENCE["detector_unavailable"]
    )
    return SecurityGuardAssessment(
        guardrail_name=guardrail_name,
        violated=False,
        confidence_score=0.0,
        severity=evidence["severity"],
        category=evidence["category"],
        detector_kind=detector_kind,
        detector_digest=detector_digest,
        summary=evidence["summary"],
        reason_codes=(reason_code,),
        signals=(SecuritySignal(kind=evidence["signal"], score=0.0),),
        signal_agreement="single",
        counterfactual=evidence["counterfactual"],
        action_rationale=evidence["rationale"],
    )


def _shift_span(span: DetectorSpan, offset: int) -> DetectorSpan:
    return DetectorSpan(
        start=span.start + offset,
        end=span.end + offset,
        label=span.label,
        replacement=span.replacement,
    )


def _shift_finding(finding: DetectorFinding, offset: int) -> DetectorFinding:
    if not offset:
        return finding
    return DetectorFinding(
        violated=finding.violated,
        confidence=finding.confidence,
        severity=finding.severity,
        category=finding.category,
        subcategory=finding.subcategory,
        reason_codes=finding.reason_codes,
        signals=finding.signals,
        spans=tuple(_shift_span(span, offset) for span in finding.spans),
        signal_agreement=finding.signal_agreement,
        summary=finding.summary,
        escalate=finding.escalate,
        wrap_untrusted=finding.wrap_untrusted,
    )


def _merge_findings(findings: Sequence[DetectorFinding]) -> DetectorFinding:
    if not findings:
        return DetectorFinding(violated=False)
    if len(findings) == 1:
        return findings[0]
    strongest = max(findings, key=lambda item: item.confidence)
    reason_codes: List[str] = []
    spans: List[DetectorSpan] = []
    for finding in findings:
        spans.extend(finding.spans)
        for code in finding.reason_codes:
            if code not in reason_codes:
                reason_codes.append(code)
    return DetectorFinding(
        violated=any(finding.violated for finding in findings),
        confidence=strongest.confidence,
        severity=strongest.severity,
        category=strongest.category,
        subcategory=strongest.subcategory,
        reason_codes=tuple(reason_codes),
        signals=strongest.signals,
        spans=tuple(spans),
        signal_agreement=strongest.signal_agreement,
        summary=strongest.summary,
        escalate=any(finding.escalate for finding in findings),
        wrap_untrusted=any(finding.wrap_untrusted for finding in findings),
    )


def _assessment_from_finding(
    guardrail_name: str,
    finding: DetectorFinding,
    *,
    detector_kind: str,
    detector_digest: str,
    text: str,
    deferred_reasons: Sequence[str],
) -> SecurityGuardAssessment:
    reason_codes = list(finding.reason_codes)
    for reason in deferred_reasons:
        if reason not in reason_codes:
            reason_codes.append(reason)
    # ``validate_security_decision`` rejects an empty reason-code or signal
    # list, so a guardrail that found nothing reports the scan itself.
    if not reason_codes:
        reason_codes.append("no_policy_signal")
    signals = finding.signals or (SecuritySignal(kind="scan", score=0.0),)
    return SecurityGuardAssessment(
        guardrail_name=guardrail_name,
        violated=finding.violated,
        confidence_score=finding.confidence,
        severity=finding.severity if finding.violated else "low",
        category=finding.category,
        detector_kind=detector_kind,
        detector_digest=detector_digest,
        summary=finding.summary[:MAX_REASON_LENGTH],
        reason_codes=tuple(reason_codes),
        signals=signals,
        signal_agreement=finding.signal_agreement,
        subcategory=finding.subcategory,
        evidence_refs=(),
        content_fragment_digests=tuple(
            content_fragment_digest(text, span) for span in finding.spans[:16]
        ),
        counterfactual=(
            "Content without the matched spans would not have fired this policy."
        ),
        action_rationale="Applied the signed tenant-runtime guardrail policy.",
    )


def _encode_assessment(assessment: SecurityGuardAssessment) -> Dict[str, Any]:
    return {
        "guardrailName": assessment.guardrail_name,
        "violated": assessment.violated,
        "confidenceScore": assessment.confidence_score,
        "severity": assessment.severity,
        "category": assessment.category,
        "subcategory": assessment.subcategory,
        "detectorKind": assessment.detector_kind,
        "detectorDigest": assessment.detector_digest,
        "summary": assessment.summary,
        "reasonCodes": list(assessment.reason_codes),
        "signals": [
            {"kind": signal.kind, "score": signal.score}
            for signal in assessment.signals
        ],
        "signalAgreement": assessment.signal_agreement,
        "evidenceRefs": list(assessment.evidence_refs),
        "contentFragmentDigests": list(assessment.content_fragment_digests),
        "counterfactual": assessment.counterfactual,
        "actionRationale": assessment.action_rationale,
    }


class LocalGuardEvaluator:
    """In-process ``GuardEvaluator`` binding, over the same wire codec as HTTP.

    Routing the in-process call through ``encode``/``decode`` is deliberate: it
    is what makes the two bindings one contract rather than two behaviours that
    happen to agree today.
    """

    def __init__(
        self,
        service: GuardrailService,
        *,
        profile: str,
        subject: GuardrailSubject,
        budget_ms: Optional[Mapping[str, int]] = None,
        observer: Optional[FailOpenObserver] = None,
        span_recorder: Optional[GuardrailSpanRecorder] = None,
        workload_surface: str = "",
        guardrails: Sequence[RuntimeGuardrail] = (),
    ) -> None:
        if profile not in service.profiles:
            raise GuardrailProfileError("guardrail_profile_unknown")
        assert_workload_surface_fail_mode(service.profiles[profile], workload_surface)
        # F4 is a construction-time refusal in this binding too, over the same
        # caller-supplied guardrail list the HTTP binding checks.
        assert_fail_open_permitted(service.profiles[profile].fail_mode, guardrails)
        self._service = service
        self._profile_id = profile
        self._profile = service.profiles[profile]
        self._subject = subject
        self._budget_ms = dict(budget_ms or DEFAULT_STAGE_BUDGET_MS)
        self._observer = observer
        self._span_recorder = span_recorder
        self._workload_surface = workload_surface
        self._fail_open_budget = FailOpenBudget(
            self._profile.fail_open_max_consecutive,
            self._profile.fail_open_window_seconds,
        )

    @property
    def fail_open_budget(self) -> FailOpenBudget:
        return self._fail_open_budget

    async def evaluate(self, request: GuardRequest) -> GuardDecision:
        stage = wire_stage(request.stage)
        document = encode_evaluate_request(
            request,
            profile=self._profile_id,
            subject=self._subject,
            budget_ms=self._budget_ms.get(stage, DEFAULT_STAGE_BUDGET_MS[stage]),
        )
        response = self._service.evaluate(document, escalation_supported=True)
        if response.status != 200:
            reason_code = str(response.body.get("error", {}).get("code", ""))
            return self._fail(request, stage, reason_code)
        try:
            decoded = decode_evaluate_response(
                response.body,
                request_id=request.request_id,
                verdicts=IN_PROCESS_VERDICTS,
            )
        except GuardrailContractError as error:
            return self._fail(request, stage, error.code)
        self._record_span(decoded, stage)
        if not decoded.decision.evaluated_guardrails:
            # Identical to the HTTP binding: empty coverage is a fail-mode
            # resolution, not a verdict, and it never records the success that
            # recovers the fail-open budget.
            return self._fail(request, stage, REASON_CODE_COVERAGE_EMPTY)
        self._fail_open_budget.record_success()
        return decoded.decision

    def _record_span(self, decoded, stage: str) -> None:
        if self._span_recorder is None:
            return
        try:
            self._span_recorder.record(
                GUARDRAIL_SPAN_NAME,
                guardrail_span_attributes(
                    decoded, stage=stage, profile=self._profile_id
                ),
            )
        except Exception:
            # Evidence is not a gate on enforcement (E5).
            pass

    def _fail(
        self, request: GuardRequest, stage: str, reason_code: str
    ) -> GuardDecision:
        tool: Optional[RuntimeTool] = request.tool
        outcome = apply_fail_mode(
            self._profile,
            request_id=request.request_id,
            wire_stage_name=stage,
            guardrails=request.guardrails,
            tool=tool,
            tenant=self._subject.tenant,
            reason_code=reason_code,
            budget=self._fail_open_budget,
            observer=self._observer,
            workload_surface=self._workload_surface,
        )
        if outcome.decision is None:
            raise GuardrailUnavailableError(outcome.reason_code)
        return outcome.decision


__all__ = [
    "MAX_GUARDRAILS_PER_REQUEST",
    "MAX_REASON_LENGTH",
    "MAX_REQUEST_ID_LENGTH",
    "SCAN_WINDOW_CHARS",
    "GuardrailRequestError",
    "GuardrailService",
    "GuardrailServiceResponse",
    "LocalGuardEvaluator",
]
