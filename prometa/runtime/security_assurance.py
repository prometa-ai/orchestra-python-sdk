"""Content-minimized security decisions and asynchronous platform delivery.

Tenant runtimes evaluate and enforce signed guardrail policy locally.  This
module only persists the resulting decision evidence and sends it to Prometa
from a background dispatcher; it is never a synchronous control-plane gate.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from typing import Any, Callable, Dict, Mapping, Optional, Protocol, Sequence, Tuple


SECURITY_DECISION_SCHEMA_VERSION = 1
SECURITY_DECISION_CAPABILITY = "security.decision.emit.v1"
MAX_SECURITY_DECISIONS_PER_BATCH = 500
MAX_SECURITY_DECISION_BODY_BYTES = 1024 * 1024

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_ENVIRONMENTS = frozenset({"dev", "test", "staging", "prod"})
_SURFACES = frozenset(
    {"input", "output", "tool_request", "tool_response", "retrieval", "memory"}
)
_ENFORCEMENT_MODES = frozenset({"observe", "review", "enforce"})
_ACTIONS = frozenset({"allow", "deny", "mask", "rewrite"})
_SEVERITIES = frozenset({"low", "medium", "high", "critical"})
_SIGNAL_AGREEMENTS = frozenset({"single", "mixed", "consensus"})
_MAX_RESPONSE_BYTES = 64 * 1024
_RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429})


class SecurityDecisionError(ValueError):
    """Invalid local decision construction or outbox input."""


class SecurityDecisionSubmissionError(RuntimeError):
    """The decision endpoint rejected or could not process a batch."""

    def __init__(self, status: Optional[int], message: str) -> None:
        self.status = status
        super().__init__(message)


@dataclass(frozen=True)
class SecuritySignal:
    kind: str
    score: float


@dataclass(frozen=True)
class SecurityGuardAssessment:
    """Detector evidence for one signed guardrail, without raw content."""

    guardrail_name: str
    violated: bool
    confidence_score: float
    severity: str
    category: str
    detector_kind: str
    detector_digest: str
    summary: str
    reason_codes: Tuple[str, ...]
    signals: Tuple[SecuritySignal, ...]
    signal_agreement: str = "single"
    subcategory: Optional[str] = None
    evidence_refs: Tuple[str, ...] = ()
    content_fragment_digests: Tuple[str, ...] = ()
    counterfactual: str = "No policy-relevant signal would allow this request."
    action_rationale: str = "Applied the signed tenant-runtime guardrail policy."


@dataclass(frozen=True)
class SecurityDecisionCorrelation:
    campaign_id: Optional[str] = None
    campaign_run_id: Optional[str] = None
    probe_id: Optional[str] = None


@dataclass(frozen=True)
class SecurityDecisionOutboxItem:
    decision_ids: Tuple[str, ...]
    decisions: Tuple[Mapping[str, Any], ...]
    attempts: int
    lease_token: str


class SecurityDecisionOutbox(Protocol):
    """Durable multi-replica queue used by a tenant runtime."""

    def enqueue(self, decision: Mapping[str, Any]) -> bool:
        """Persist one immutable decision; return true only for a new row."""

    def claim_batch(
        self, lease_seconds: float, maximum: int = MAX_SECURITY_DECISIONS_PER_BATCH
    ) -> Optional[SecurityDecisionOutboxItem]:
        """Lease one size-bounded decision batch or return none."""

    def mark_delivered(self, item: SecurityDecisionOutboxItem) -> None:
        """Complete every decision in a currently held lease."""

    def reschedule(
        self,
        item: SecurityDecisionOutboxItem,
        *,
        delay_seconds: float,
        error_code: str,
    ) -> None:
        """Release a batch lease after a bounded retry delay."""

    def mark_dead_letter(
        self, item: SecurityDecisionOutboxItem, *, error_code: str
    ) -> None:
        """Retain a permanently rejected batch without retrying it."""


class SecurityDecisionEmitter(Protocol):
    def emit(self, decision: Mapping[str, Any]) -> None:
        """Persist one decision locally; failures must raise."""


class InMemorySecurityDecisionEmitter:
    """Thread-safe decision collector for conformance tests and local adapters."""

    def __init__(self) -> None:
        self._decisions = []
        self._lock = threading.Lock()

    def emit(self, decision: Mapping[str, Any]) -> None:
        normalized = validate_security_decision(decision)
        with self._lock:
            self._decisions.append(normalized)

    @property
    def decisions(self) -> Tuple[Mapping[str, Any], ...]:
        with self._lock:
            return tuple(dict(decision) for decision in self._decisions)


class DurableSecurityDecisionEmitter:
    """Persist decisions synchronously, then wake the network dispatcher."""

    def __init__(
        self,
        outbox: SecurityDecisionOutbox,
        dispatcher: "SecurityDecisionDispatcher",
    ) -> None:
        self._outbox = outbox
        self._dispatcher = dispatcher

    def emit(self, decision: Mapping[str, Any]) -> None:
        self._outbox.enqueue(validate_security_decision(decision))
        self._dispatcher.wake()


def _identifier(name: str, value: Any, maximum: int = 200) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or _IDENTIFIER.fullmatch(value) is None
    ):
        raise SecurityDecisionError("%s must be a bounded identifier" % name)
    return value


def _optional_identifier(name: str, value: Any, maximum: int = 200) -> Optional[str]:
    if value is None:
        return None
    return _identifier(name, value, maximum)


def _bounded_text(name: str, value: Any, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
    ):
        raise SecurityDecisionError("%s must be bounded non-empty text" % name)
    return value


def _digest(name: str, value: Any) -> str:
    candidate = _bounded_text(name, value, 80)
    if _DIGEST.fullmatch(candidate) is None:
        raise SecurityDecisionError("%s must be a lowercase sha256 digest" % name)
    return candidate


def _score(name: str, value: Any) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not isfinite(value)
        or value < 0
        or value > 1
    ):
        raise SecurityDecisionError("%s must be between 0 and 1" % name)
    return float(value)


def _confidence_band(score: float) -> str:
    if score < 0.4:
        return "low"
    if score < 0.8:
        return "medium"
    return "high"


def _instant(value: Optional[datetime] = None) -> str:
    timestamp = value or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        raise SecurityDecisionError("event_at must be timezone-aware")
    return (
        timestamp.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _member(name: str, value: Any, allowed: Sequence[str]) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise SecurityDecisionError("%s is unsupported" % name)
    return value


def _text_tuple(
    name: str,
    value: Any,
    *,
    maximum_items: int,
    maximum_length: int,
    minimum_items: int = 0,
    identifiers: bool = False,
    digests: bool = False,
) -> Tuple[str, ...]:
    if (
        not isinstance(value, (list, tuple))
        or len(value) < minimum_items
        or len(value) > maximum_items
    ):
        raise SecurityDecisionError("%s has an invalid item count" % name)
    result = []
    for item in value:
        if digests:
            result.append(_digest(name, item))
        elif identifiers:
            result.append(_identifier(name, item, maximum_length))
        else:
            result.append(_bounded_text(name, item, maximum_length))
    return tuple(result)


def validate_security_decision(value: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize the exact ``prometa.security-decision.v1`` wire contract."""

    if not isinstance(value, Mapping):
        raise SecurityDecisionError("decision must be an object")
    allowed_keys = {
        "decisionId",
        "eventAt",
        "agentId",
        "solutionId",
        "traceId",
        "spanId",
        "sessionId",
        "environment",
        "releaseId",
        "deploymentId",
        "surface",
        "enforcementMode",
        "recommendedAction",
        "appliedAction",
        "reviewRequired",
        "severity",
        "category",
        "subcategory",
        "policy",
        "detector",
        "explanation",
        "confidence",
        "campaignId",
        "campaignRunId",
        "probeId",
    }
    if set(value) - allowed_keys:
        raise SecurityDecisionError("decision contains unsupported fields")
    policy = value.get("policy")
    detector = value.get("detector")
    explanation = value.get("explanation")
    confidence = value.get("confidence")
    if not all(
        isinstance(item, Mapping)
        for item in (policy, detector, explanation, confidence)
    ):
        raise SecurityDecisionError("decision nested contracts must be objects")
    if set(policy or {}) != {"id", "version", "digest"}:
        raise SecurityDecisionError("policy fields are invalid")
    if set(detector or {}) != {"kind", "digest"}:
        raise SecurityDecisionError("detector fields are invalid")
    explanation_keys = {
        "summary",
        "reasonCodes",
        "signals",
        "signalAgreement",
        "evidenceRefs",
        "contentFragmentDigests",
        "counterfactual",
        "actionRationale",
    }
    if set(explanation or {}) != explanation_keys:
        raise SecurityDecisionError("explanation fields are invalid")
    if set(confidence or {}) != {"score", "band"}:
        raise SecurityDecisionError("confidence fields are invalid")

    event_at = value.get("eventAt")
    if not isinstance(event_at, str) or not re.search(
        r"(?:Z|[+-]\d{2}:\d{2})$", event_at
    ):
        raise SecurityDecisionError("eventAt must include a timezone")
    try:
        normalized_event_at = (
            datetime.fromisoformat(event_at.replace("Z", "+00:00"))
            .astimezone(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
    except ValueError:
        raise SecurityDecisionError("eventAt must be ISO-8601") from None

    score = _score("confidence.score", (confidence or {}).get("score"))
    band = _member(
        "confidence.band",
        (confidence or {}).get("band"),
        ("low", "medium", "high"),
    )
    if band != _confidence_band(score):
        raise SecurityDecisionError("confidence.band does not match score")
    raw_signals = (explanation or {}).get("signals")
    if (
        not isinstance(raw_signals, (list, tuple))
        or not raw_signals
        or len(raw_signals) > 20
    ):
        raise SecurityDecisionError("signals has an invalid item count")
    signals = []
    for signal in raw_signals:
        if not isinstance(signal, Mapping) or set(signal) != {"kind", "score"}:
            raise SecurityDecisionError("signal fields are invalid")
        signals.append(
            {
                "kind": _identifier("signal.kind", signal.get("kind"), 100),
                "score": _score("signal.score", signal.get("score")),
            }
        )
    review_required = value.get("reviewRequired")
    if type(review_required) is not bool:
        raise SecurityDecisionError("reviewRequired must be a boolean")
    environment = _member(
        "environment", value.get("environment"), tuple(_ENVIRONMENTS)
    )

    normalized = {
        "decisionId": _identifier("decisionId", value.get("decisionId")),
        "eventAt": normalized_event_at,
        "agentId": _identifier("agentId", value.get("agentId")),
        "environment": environment,
        "surface": _member("surface", value.get("surface"), tuple(_SURFACES)),
        "enforcementMode": _member(
            "enforcementMode",
            value.get("enforcementMode"),
            tuple(_ENFORCEMENT_MODES),
        ),
        "recommendedAction": _member(
            "recommendedAction", value.get("recommendedAction"), tuple(_ACTIONS)
        ),
        "appliedAction": _member(
            "appliedAction", value.get("appliedAction"), tuple(_ACTIONS)
        ),
        "reviewRequired": review_required,
        "severity": _member(
            "severity", value.get("severity"), tuple(_SEVERITIES)
        ),
        "category": _identifier("category", value.get("category"), 100),
        "policy": {
            "id": _identifier("policy.id", (policy or {}).get("id")),
            "version": _identifier(
                "policy.version", (policy or {}).get("version"), 100
            ),
            "digest": _digest("policy.digest", (policy or {}).get("digest")),
        },
        "detector": {
            "kind": _identifier(
                "detector.kind", (detector or {}).get("kind"), 100
            ),
            "digest": _digest(
                "detector.digest", (detector or {}).get("digest")
            ),
        },
        "explanation": {
            "summary": _bounded_text(
                "explanation.summary", (explanation or {}).get("summary"), 1000
            ),
            "reasonCodes": list(
                _text_tuple(
                    "explanation.reasonCodes",
                    (explanation or {}).get("reasonCodes"),
                    minimum_items=1,
                    maximum_items=20,
                    maximum_length=100,
                    identifiers=True,
                )
            ),
            "signals": signals,
            "signalAgreement": _member(
                "explanation.signalAgreement",
                (explanation or {}).get("signalAgreement"),
                tuple(_SIGNAL_AGREEMENTS),
            ),
            "evidenceRefs": list(
                _text_tuple(
                    "explanation.evidenceRefs",
                    (explanation or {}).get("evidenceRefs"),
                    maximum_items=20,
                    maximum_length=500,
                )
            ),
            "contentFragmentDigests": list(
                _text_tuple(
                    "explanation.contentFragmentDigests",
                    (explanation or {}).get("contentFragmentDigests"),
                    maximum_items=20,
                    maximum_length=80,
                    digests=True,
                )
            ),
            "counterfactual": _bounded_text(
                "explanation.counterfactual",
                (explanation or {}).get("counterfactual"),
                1000,
            ),
            "actionRationale": _bounded_text(
                "explanation.actionRationale",
                (explanation or {}).get("actionRationale"),
                1000,
            ),
        },
        "confidence": {"score": score, "band": band},
    }
    optional_identifiers = {
        "solutionId": 200,
        "traceId": 200,
        "spanId": 200,
        "sessionId": 200,
        "releaseId": 200,
        "deploymentId": 200,
        "subcategory": 100,
        "campaignId": 200,
        "campaignRunId": 200,
        "probeId": 200,
    }
    for key, maximum in optional_identifiers.items():
        candidate = _optional_identifier(key, value.get(key), maximum)
        if candidate is not None:
            normalized[key] = candidate
    return normalized


def build_security_decision(
    *,
    request_id: str,
    agent_id: str,
    environment: str,
    release_id: str,
    deployment_id: str,
    surface: str,
    policy_id: str,
    policy_version: str,
    policy_digest: str,
    enforcement_mode: str,
    recommended_action: str,
    applied_action: str,
    review_required: bool,
    assessment: SecurityGuardAssessment,
    correlation: Optional[SecurityDecisionCorrelation] = None,
    event_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Build one strict decision without retaining evaluated content."""

    decision = {
        "decisionId": str(uuid.uuid4()),
        "eventAt": _instant(event_at),
        "agentId": agent_id,
        "environment": environment,
        "releaseId": release_id,
        "deploymentId": deployment_id,
        "surface": surface,
        "enforcementMode": enforcement_mode,
        "recommendedAction": recommended_action,
        "appliedAction": applied_action,
        "reviewRequired": review_required,
        "severity": assessment.severity,
        "category": assessment.category,
        "subcategory": assessment.subcategory,
        "policy": {
            "id": policy_id,
            "version": policy_version,
            "digest": policy_digest,
        },
        "detector": {
            "kind": assessment.detector_kind,
            "digest": assessment.detector_digest,
        },
        "explanation": {
            "summary": assessment.summary,
            "reasonCodes": list(assessment.reason_codes),
            "signals": [
                {"kind": signal.kind, "score": signal.score}
                for signal in assessment.signals
            ],
            "signalAgreement": assessment.signal_agreement,
            "evidenceRefs": list(assessment.evidence_refs),
            "contentFragmentDigests": list(
                assessment.content_fragment_digests
            ),
            "counterfactual": assessment.counterfactual,
            "actionRationale": assessment.action_rationale,
        },
        "confidence": {
            "score": assessment.confidence_score,
            "band": _confidence_band(assessment.confidence_score),
        },
    }
    if (
        isinstance(request_id, str)
        and len(request_id) <= 200
        and _IDENTIFIER.fullmatch(request_id) is not None
    ):
        decision["traceId"] = request_id
    if correlation is not None:
        decision.update(
            {
                "campaignId": correlation.campaign_id,
                "campaignRunId": correlation.campaign_run_id,
                "probeId": correlation.probe_id,
            }
        )
    return validate_security_decision(decision)


def _batch_id(decision_ids: Sequence[str]) -> str:
    material = "\x00".join(decision_ids).encode("utf-8")
    return "runtime-batch-" + hashlib.sha256(material).hexdigest()


def security_policy_identifier(name: str) -> str:
    """Derive a stable wire-safe identifier from a signed display name."""

    if not isinstance(name, str) or not name.strip() or name != name.strip():
        raise SecurityDecisionError("policy name must be bounded non-empty text")
    slug = re.sub(r"[^A-Za-z0-9._:/@+-]+", "-", name).strip("-")
    if not slug:
        slug = "guardrail"
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]
    return _identifier("policy.id", "%s-%s" % (slug[:180], digest))


def build_security_decision_batch(
    decisions: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    if not decisions or len(decisions) > MAX_SECURITY_DECISIONS_PER_BATCH:
        raise SecurityDecisionError("batch decision count is invalid")
    normalized = [validate_security_decision(decision) for decision in decisions]
    decision_ids = [decision["decisionId"] for decision in normalized]
    if len(set(decision_ids)) != len(decision_ids):
        raise SecurityDecisionError("batch decision IDs must be unique")
    batch = {
        "schemaVersion": SECURITY_DECISION_SCHEMA_VERSION,
        "batchId": _batch_id(decision_ids),
        "decisions": normalized,
    }
    return validate_security_decision_batch(batch)


def validate_security_decision_batch(value: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize the exact security-decision batch intake contract."""

    if not isinstance(value, Mapping) or set(value) != {
        "schemaVersion",
        "batchId",
        "decisions",
    }:
        raise SecurityDecisionError("batch fields are invalid")
    if value.get("schemaVersion") != SECURITY_DECISION_SCHEMA_VERSION:
        raise SecurityDecisionError("batch schema version is unsupported")
    decisions = value.get("decisions")
    if (
        not isinstance(decisions, (list, tuple))
        or not decisions
        or len(decisions) > MAX_SECURITY_DECISIONS_PER_BATCH
    ):
        raise SecurityDecisionError("batch decision count is invalid")
    normalized = [validate_security_decision(decision) for decision in decisions]
    decision_ids = [decision["decisionId"] for decision in normalized]
    if len(set(decision_ids)) != len(decision_ids):
        raise SecurityDecisionError("batch decision IDs must be unique")
    batch = {
        "schemaVersion": SECURITY_DECISION_SCHEMA_VERSION,
        "batchId": _identifier("batchId", value.get("batchId")),
        "decisions": normalized,
    }
    encoded = json.dumps(
        batch, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    if len(encoded) > MAX_SECURITY_DECISION_BODY_BYTES:
        raise SecurityDecisionError("batch exceeds 1 MiB")
    return batch


class SecurityDecisionClient:
    """Minimal stdlib client for ``POST /api/security/decision-batches``."""

    def __init__(self, base_url: str, api_key: str, timeout: float = 10.0) -> None:
        if not base_url or not api_key:
            raise SecurityDecisionError("base_url and api_key are required")
        self._url = base_url.rstrip("/") + "/api/security/decision-batches"
        self._api_key = api_key
        self._timeout = timeout

    def submit(self, decisions: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
        batch = build_security_decision_batch(decisions)
        body = json.dumps(
            batch, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
        request = urllib.request.Request(
            self._url,
            data=body,
            method="POST",
            headers={
                "content-type": "application/json",
                "x-api-key": self._api_key,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                geturl = getattr(response, "geturl", None)
                final_url = geturl() if callable(geturl) else self._url
                if final_url != self._url:
                    raise SecurityDecisionSubmissionError(
                        302, "Security decision endpoint redirected"
                    )
                response_body = response.read(_MAX_RESPONSE_BYTES + 1)
                if len(response_body) > _MAX_RESPONSE_BYTES:
                    raise SecurityDecisionSubmissionError(
                        None, "Security decision response was too large"
                    )
                decoded = json.loads(response_body.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read(1000).decode("utf-8", errors="replace")
            raise SecurityDecisionSubmissionError(
                exc.code,
                "Security decision batch rejected: HTTP %s: %s"
                % (exc.code, detail),
            ) from exc
        except SecurityDecisionSubmissionError:
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise SecurityDecisionSubmissionError(
                None,
                "Security decision transport failed: %s" % type(exc).__name__,
            ) from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SecurityDecisionSubmissionError(
                None, "Security decision endpoint returned invalid JSON"
            ) from exc
        if not isinstance(decoded, dict):
            raise SecurityDecisionSubmissionError(
                None, "Security decision endpoint returned a non-object response"
            )
        if (
            decoded.get("batchId") != batch["batchId"]
            or decoded.get("decisionCount") != len(batch["decisions"])
            or decoded.get("status") not in {"queued", "processed"}
        ):
            raise SecurityDecisionSubmissionError(
                None, "Security decision endpoint returned an invalid acknowledgement"
            )
        return decoded


def _positive_number(name: str, value: Any, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SecurityDecisionError("%s must be a positive number" % name)
    result = float(value)
    if result <= 0 or result > maximum:
        raise SecurityDecisionError("%s must be a positive number" % name)
    return result


class SecurityDecisionDispatcher:
    """Background batch sender that never gates runtime request serving."""

    def __init__(
        self,
        outbox: SecurityDecisionOutbox,
        client: SecurityDecisionClient,
        *,
        poll_interval_seconds: float = 2.0,
        lease_seconds: float = 30.0,
        initial_backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 300.0,
        shutdown_timeout_seconds: float = 10.0,
        on_status: Optional[Callable[[str, Mapping[str, str]], None]] = None,
    ) -> None:
        self._outbox = outbox
        self._client = client
        self._poll_interval_seconds = _positive_number(
            "poll_interval_seconds", poll_interval_seconds, 300
        )
        self._lease_seconds = _positive_number(
            "lease_seconds", lease_seconds, 3600
        )
        self._initial_backoff_seconds = _positive_number(
            "initial_backoff_seconds", initial_backoff_seconds, 3600
        )
        self._max_backoff_seconds = _positive_number(
            "max_backoff_seconds", max_backoff_seconds, 86_400
        )
        self._shutdown_timeout_seconds = _positive_number(
            "shutdown_timeout_seconds", shutdown_timeout_seconds, 300
        )
        if self._max_backoff_seconds < self._initial_backoff_seconds:
            raise SecurityDecisionError(
                "max_backoff_seconds must be at least initial_backoff_seconds"
            )
        self._on_status = on_status
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="prometa-runtime-security-decisions",
            daemon=True,
        )
        self._started = False
        self._start_lock = threading.Lock()

    def _status(
        self,
        outcome: str,
        item: Optional[SecurityDecisionOutboxItem],
        error_code: Optional[str] = None,
    ) -> None:
        if self._on_status is None:
            return
        details = {
            "decisionCount": str(len(item.decision_ids) if item else 0),
            "batchId": _batch_id(item.decision_ids) if item else "unavailable",
        }
        if error_code is not None:
            details["errorCode"] = error_code
        try:
            self._on_status(outcome, details)
        except Exception:
            return

    def _retry_delay(self, attempts: int) -> float:
        exponent = min(max(attempts - 1, 0), 16)
        return min(
            self._max_backoff_seconds,
            self._initial_backoff_seconds * (2**exponent),
        )

    @staticmethod
    def _retryable(status: Optional[int]) -> bool:
        return (
            status is None
            or status in _RETRYABLE_HTTP_STATUSES
            or (status is not None and status >= 500)
        )

    def dispatch_once(self) -> bool:
        item = self._outbox.claim_batch(self._lease_seconds)
        if item is None:
            return False
        try:
            self._client.submit(item.decisions)
        except SecurityDecisionSubmissionError as exc:
            error_code = "transport" if exc.status is None else "http_%d" % exc.status
            if self._retryable(exc.status):
                self._outbox.reschedule(
                    item,
                    delay_seconds=self._retry_delay(item.attempts),
                    error_code=error_code,
                )
                self._status("retry_scheduled", item, error_code)
            else:
                self._outbox.mark_dead_letter(item, error_code=error_code)
                self._status("dead_letter", item, error_code)
        except Exception:
            self._outbox.reschedule(
                item,
                delay_seconds=self._retry_delay(item.attempts),
                error_code="delivery_error",
            )
            self._status("retry_scheduled", item, "delivery_error")
        else:
            self._outbox.mark_delivered(item)
            self._status("delivered", item)
        return True

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                if self.dispatch_once():
                    continue
            except Exception:
                self._status("outbox_unavailable", None, "outbox_unavailable")
            self._wake.wait(self._poll_interval_seconds)
            self._wake.clear()

    def start(self) -> None:
        with self._start_lock:
            if self._started:
                return
            self._started = True
            self._thread.start()

    def wake(self) -> None:
        self._wake.set()

    def close(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._started:
            self._thread.join(timeout=self._shutdown_timeout_seconds)


__all__ = [
    "DurableSecurityDecisionEmitter",
    "InMemorySecurityDecisionEmitter",
    "MAX_SECURITY_DECISIONS_PER_BATCH",
    "MAX_SECURITY_DECISION_BODY_BYTES",
    "SECURITY_DECISION_CAPABILITY",
    "SECURITY_DECISION_SCHEMA_VERSION",
    "SecurityDecisionClient",
    "SecurityDecisionCorrelation",
    "SecurityDecisionDispatcher",
    "SecurityDecisionEmitter",
    "SecurityDecisionError",
    "SecurityDecisionOutbox",
    "SecurityDecisionOutboxItem",
    "SecurityDecisionSubmissionError",
    "SecurityGuardAssessment",
    "SecuritySignal",
    "build_security_decision",
    "build_security_decision_batch",
    "security_policy_identifier",
    "validate_security_decision",
    "validate_security_decision_batch",
]
