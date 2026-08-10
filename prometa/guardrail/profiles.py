"""Guardrail profiles: the operator-owned policy the service enforces.

Profile documents are configuration rather than wire traffic, so an unknown
member is rejected instead of dropped: a mistyped profile key would otherwise
be a silent policy change.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from ..runtime.admission import RuntimeGuardrail
from .contract import (
    DEFAULT_FAIL_OPEN_MAX_CONSECUTIVE,
    DEFAULT_FAIL_OPEN_WINDOW_SECONDS,
    DEFAULT_MAX_PAYLOAD_BYTES,
)
from .detectors import (
    BUILTIN_DETECTOR_KINDS,
    DetectorError,
    DetectorPack,
    build_detector_pack,
)


DEFAULT_FLUSH_HOLDBACK_CHARS = 256
DEFAULT_OVERSIZE_OVERLAP_BYTES = 512

FAIL_MODES = frozenset({"closed", "open"})
OVERSIZE_POLICIES = frozenset({"deny", "chunk"})
DEFER_POLICIES = frozenset({"deny", "allow"})
UNKNOWN_GUARDRAIL_POLICIES = frozenset({"deny", "allow"})
STREAMING_POLICIES = frozenset({"allow", "deny"})
TOOL_RESULT_INJECTION_VERDICTS = frozenset({"transform", "deny"})

ON_VIOLATIONS = frozenset({"block", "redact", "escalate", "log"})
ENFORCEMENT_MODES = frozenset({"observe", "review", "enforce"})
DECISION_ACTIONS = frozenset({"allow", "deny", "mask", "rewrite"})

_PROFILE_KEYS = frozenset(
    {
        "id",
        "guardrails",
        "failMode",
        "maxPayloadBytes",
        "oversizePolicy",
        "deferPolicy",
        "unknownGuardrailPolicy",
        "streaming",
        "flushHoldbackChars",
        "failOpenMaxConsecutive",
        "failOpenWindowSeconds",
        "toolResultInjectionVerdict",
        "detectorKinds",
        "detectorSettings",
    }
)

_DETECTOR_SETTINGS_KEYS = frozenset(
    {
        "deniedTerms",
        "egressAllowlist",
        "maxToolRiskLevel",
        "allowedSideEffects",
        "maxInputTokens",
    }
)

_GUARDRAIL_DEFINITION_KEYS = frozenset(
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

class GuardrailProfileError(ValueError):
    """A profile document could not be loaded, so nothing may run under it."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class GuardrailProfile:
    """One named policy: guardrail definitions, fail mode, budgets, detectors.

    Selection is the caller's and definition is the service's. A request names
    which guardrails apply to it; ``guardrails`` here says what each of those
    names *is* — type, violation action, thresholds, enforcement mode. A caller
    cannot widen or narrow the definition of a name it selects, so there is
    exactly one source of truth for policy while the caller keeps membership.
    """

    profile_id: str
    guardrails: Mapping[str, RuntimeGuardrail] = None  # type: ignore[assignment]
    fail_mode: str = "closed"
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES
    oversize_policy: str = "deny"
    defer_policy: str = "deny"
    unknown_guardrail_policy: str = "deny"
    streaming: str = "allow"
    flush_holdback_chars: int = DEFAULT_FLUSH_HOLDBACK_CHARS
    fail_open_max_consecutive: int = DEFAULT_FAIL_OPEN_MAX_CONSECUTIVE
    fail_open_window_seconds: float = DEFAULT_FAIL_OPEN_WINDOW_SECONDS
    tool_result_injection_verdict: str = "transform"
    detector_kinds: Tuple[str, ...] = BUILTIN_DETECTOR_KINDS
    detector_settings: Mapping[str, Any] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.detector_settings is None:
            object.__setattr__(self, "detector_settings", {})
        if self.guardrails is None:
            object.__setattr__(self, "guardrails", {})

    def build_pack(self) -> DetectorPack:
        try:
            return build_detector_pack(self.detector_kinds, self.detector_settings)
        except DetectorError as exc:
            raise GuardrailProfileError(exc.code) from exc


def _enum(name: str, value: Any, allowed: frozenset, default: str) -> str:
    if value is None:
        return default
    if not isinstance(value, str) or value not in allowed:
        raise GuardrailProfileError("guardrail_profile_%s_invalid" % name)
    return value


def _bounded_int(name: str, value: Any, minimum: int, maximum: int, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int):
        raise GuardrailProfileError("guardrail_profile_%s_invalid" % name)
    if not minimum <= value <= maximum:
        raise GuardrailProfileError("guardrail_profile_%s_invalid" % name)
    return value


def _bounded_float(
    name: str, value: Any, minimum: float, maximum: float, default: float
) -> float:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GuardrailProfileError("guardrail_profile_%s_invalid" % name)
    if not minimum <= float(value) <= maximum:
        raise GuardrailProfileError("guardrail_profile_%s_invalid" % name)
    return float(value)


def _string_tuple(name: str, value: Any) -> Tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise GuardrailProfileError("guardrail_profile_%s_invalid" % name)
    if not all(isinstance(item, str) and item for item in value):
        raise GuardrailProfileError("guardrail_profile_%s_invalid" % name)
    return tuple(value)


def _threshold(name: str, value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GuardrailProfileError("guardrail_profile_%s_invalid" % name)
    if not 0.0 <= float(value) <= 1.0:
        raise GuardrailProfileError("guardrail_profile_%s_invalid" % name)
    return float(value)


def _guardrail_definition(value: Any) -> RuntimeGuardrail:
    if not isinstance(value, Mapping):
        raise GuardrailProfileError("guardrail_profile_guardrails_invalid")
    if set(value) - _GUARDRAIL_DEFINITION_KEYS:
        raise GuardrailProfileError("guardrail_profile_guardrails_invalid")
    name = value.get("name")
    if not isinstance(name, str) or not name.strip() or name != name.strip():
        raise GuardrailProfileError("guardrail_profile_guardrails_invalid")
    guardrail_type = value.get("guardrailType")
    if not isinstance(guardrail_type, str) or not guardrail_type:
        raise GuardrailProfileError("guardrail_profile_guardrails_invalid")
    on_violation = value.get("onViolation")
    if on_violation not in ON_VIOLATIONS:
        raise GuardrailProfileError("guardrail_profile_guardrails_invalid")
    applies_to = value.get("appliesTo")
    if applies_to is not None and (not isinstance(applies_to, str) or not applies_to):
        raise GuardrailProfileError("guardrail_profile_guardrails_invalid")
    enforcement_mode = value.get("enforcementMode")
    if enforcement_mode is not None and enforcement_mode not in ENFORCEMENT_MODES:
        raise GuardrailProfileError("guardrail_profile_guardrails_invalid")
    decision_action = value.get("decisionAction")
    if decision_action is not None and decision_action not in DECISION_ACTIONS:
        raise GuardrailProfileError("guardrail_profile_guardrails_invalid")
    review_threshold = _threshold("guardrails", value.get("reviewThreshold"))
    enforce_threshold = _threshold("guardrails", value.get("enforceThreshold"))
    present = (
        enforcement_mode is not None,
        review_threshold is not None,
        enforce_threshold is not None,
        decision_action is not None,
    )
    # The security-assurance quartet is all-or-nothing, exactly as
    # ``admission._parse_guardrail`` enforces it on a bundle: a guessed
    # threshold beside three declared fields is a silent policy change.
    if any(present) and not all(present):
        raise GuardrailProfileError("guardrail_profile_guardrails_invalid")
    if all(present) and review_threshold > enforce_threshold:
        raise GuardrailProfileError("guardrail_profile_guardrails_invalid")
    return RuntimeGuardrail(
        name=name,
        guardrail_type=guardrail_type,
        on_violation=on_violation,
        applies_to=applies_to,
        enforcement_mode=enforcement_mode,
        review_threshold=review_threshold,
        enforce_threshold=enforce_threshold,
        decision_action=decision_action,
    )


def load_guardrail_definitions(value: Any) -> Dict[str, RuntimeGuardrail]:
    """Read a profile's guardrail definitions, keyed by the name callers select."""

    if value is None:
        return {}
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise GuardrailProfileError("guardrail_profile_guardrails_invalid")
    definitions: Dict[str, RuntimeGuardrail] = {}
    for item in value:
        definition = _guardrail_definition(item)
        if definition.name in definitions:
            raise GuardrailProfileError("guardrail_profile_guardrail_duplicate")
        definitions[definition.name] = definition
    return definitions


def guardrail_is_observe_only(guardrail: RuntimeGuardrail) -> bool:
    """True when the guardrail cannot block, which is what fail-open requires."""

    if guardrail.enforcement_mode is not None:
        return guardrail.enforcement_mode == "observe"
    return guardrail.on_violation == "log"


def assert_fail_open_permitted(
    fail_mode: str, guardrails: Sequence[RuntimeGuardrail]
) -> None:
    """Refuse ``failMode: open`` beside anything that can enforce.

    A fail-open profile that also enforces is a documented bypass. The pairing
    is checked wherever a guardrail list meets a profile: when the profile
    document is loaded, against the definitions it declares; at evaluator
    construction, where the caller declares what it will send; and on each
    request, where the list that will actually be evaluated arrives.
    """

    if fail_mode != "open":
        return
    if any(not guardrail_is_observe_only(item) for item in guardrails):
        raise GuardrailProfileError("guardrail_profile_fail_open_enforcing")


def load_guardrail_profile(value: Any) -> GuardrailProfile:
    """Read and validate one profile document."""

    if not isinstance(value, Mapping):
        raise GuardrailProfileError("guardrail_profile_invalid")
    if set(value) - _PROFILE_KEYS:
        raise GuardrailProfileError("guardrail_profile_invalid")
    profile_id = value.get("id")
    if not isinstance(profile_id, str) or not profile_id.strip():
        raise GuardrailProfileError("guardrail_profile_id_invalid")
    settings_value = value.get("detectorSettings") or {}
    if not isinstance(settings_value, Mapping):
        raise GuardrailProfileError("guardrail_profile_detector_settings_invalid")
    if set(settings_value) - _DETECTOR_SETTINGS_KEYS:
        raise GuardrailProfileError("guardrail_profile_detector_settings_invalid")
    settings: Dict[str, Any] = {
        "deniedTerms": _string_tuple("detector_settings", settings_value.get("deniedTerms")),
        "egressAllowlist": _string_tuple(
            "detector_settings", settings_value.get("egressAllowlist")
        ),
        "maxToolRiskLevel": settings_value.get("maxToolRiskLevel", "medium"),
        "allowedSideEffects": _string_tuple(
            "detector_settings", settings_value.get("allowedSideEffects")
        )
        or ("read-only",),
        "maxInputTokens": _bounded_int(
            "detector_settings", settings_value.get("maxInputTokens"), 1, 10_000_000, 32000
        ),
    }
    detector_kinds = _string_tuple("detector_kinds", value.get("detectorKinds"))
    fail_mode = _enum("fail_mode", value.get("failMode"), FAIL_MODES, "closed")
    profile = GuardrailProfile(
        profile_id=profile_id,
        guardrails=load_guardrail_definitions(value.get("guardrails")),
        fail_mode=fail_mode,
        max_payload_bytes=_bounded_int(
            "max_payload_bytes",
            value.get("maxPayloadBytes"),
            1024,
            64 * 1024 * 1024,
            DEFAULT_MAX_PAYLOAD_BYTES,
        ),
        oversize_policy=_enum(
            "oversize_policy", value.get("oversizePolicy"), OVERSIZE_POLICIES, "deny"
        ),
        defer_policy=_enum(
            "defer_policy", value.get("deferPolicy"), DEFER_POLICIES, "deny"
        ),
        unknown_guardrail_policy=_enum(
            "unknown_guardrail_policy",
            value.get("unknownGuardrailPolicy"),
            UNKNOWN_GUARDRAIL_POLICIES,
            "deny",
        ),
        streaming=_enum(
            "streaming", value.get("streaming"), STREAMING_POLICIES, "allow"
        ),
        flush_holdback_chars=_bounded_int(
            "flush_holdback_chars",
            value.get("flushHoldbackChars"),
            16,
            65536,
            DEFAULT_FLUSH_HOLDBACK_CHARS,
        ),
        fail_open_max_consecutive=_bounded_int(
            "fail_open_max_consecutive",
            value.get("failOpenMaxConsecutive"),
            1,
            10000,
            DEFAULT_FAIL_OPEN_MAX_CONSECUTIVE,
        ),
        fail_open_window_seconds=_bounded_float(
            "fail_open_window_seconds",
            value.get("failOpenWindowSeconds"),
            1.0,
            86400.0,
            DEFAULT_FAIL_OPEN_WINDOW_SECONDS,
        ),
        tool_result_injection_verdict=_enum(
            "tool_result_injection_verdict",
            value.get("toolResultInjectionVerdict"),
            TOOL_RESULT_INJECTION_VERDICTS,
            "transform",
        ),
        detector_kinds=detector_kinds or BUILTIN_DETECTOR_KINDS,
        detector_settings=settings,
    )
    profile.build_pack()
    assert_fail_open_permitted(profile.fail_mode, tuple(profile.guardrails.values()))
    return profile


def load_guardrail_profiles(values: Any) -> Dict[str, GuardrailProfile]:
    """Read a profile list, rejecting duplicates rather than last-write-wins."""

    if isinstance(values, (str, bytes)) or not isinstance(values, (list, tuple)):
        raise GuardrailProfileError("guardrail_profiles_invalid")
    if not values:
        raise GuardrailProfileError("guardrail_profiles_empty")
    profiles: Dict[str, GuardrailProfile] = {}
    for item in values:
        profile = load_guardrail_profile(item)
        if profile.profile_id in profiles:
            raise GuardrailProfileError("guardrail_profile_duplicate")
        profiles[profile.profile_id] = profile
    return profiles


__all__ = [
    "DEFAULT_FAIL_OPEN_MAX_CONSECUTIVE",
    "DEFAULT_FAIL_OPEN_WINDOW_SECONDS",
    "DEFAULT_FLUSH_HOLDBACK_CHARS",
    "DEFAULT_MAX_PAYLOAD_BYTES",
    "DEFAULT_OVERSIZE_OVERLAP_BYTES",
    "DECISION_ACTIONS",
    "DEFER_POLICIES",
    "ENFORCEMENT_MODES",
    "FAIL_MODES",
    "ON_VIOLATIONS",
    "OVERSIZE_POLICIES",
    "STREAMING_POLICIES",
    "TOOL_RESULT_INJECTION_VERDICTS",
    "UNKNOWN_GUARDRAIL_POLICIES",
    "GuardrailProfile",
    "GuardrailProfileError",
    "assert_fail_open_permitted",
    "guardrail_is_observe_only",
    "load_guardrail_definitions",
    "load_guardrail_profile",
    "load_guardrail_profiles",
]
