"""Payload-free workflow decision evidence and asynchronous delivery.

Workflow policy is evaluated and enforced inside the tenant runtime. This
module validates the minimized wire contract, persists it through an injected
outbox, and delivers batches asynchronously. It is never a synchronous
Orchestra dependency for tool execution.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping, Optional, Protocol, Sequence, Tuple

from .workflow_ontology import WorkflowDecisionEvidence


WORKFLOW_DECISION_SCHEMA = "prometa.workflow-decision.v1"
MAX_WORKFLOW_DECISIONS_PER_BATCH = 500
MAX_WORKFLOW_DECISION_BODY_BYTES = 1024 * 1024

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_OUTCOMES = frozenset({"allow", "deny", "indeterminate", "require_approval"})
_APPLIED_OUTCOMES = frozenset({"allow", "deny"})
_RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429})
_MAX_RESPONSE_BYTES = 64 * 1024


class WorkflowDecisionError(ValueError):
    """Invalid local workflow-decision construction or outbox input."""


class WorkflowDecisionSubmissionError(RuntimeError):
    """The workflow-decision endpoint rejected or could not process a batch."""

    def __init__(self, status: Optional[int], message: str) -> None:
        self.status = status
        super().__init__(message)


@dataclass(frozen=True)
class WorkflowDecisionOutboxItem:
    decision_ids: Tuple[str, ...]
    decisions: Tuple[Mapping[str, Any], ...]
    attempts: int
    lease_token: str


class WorkflowDecisionOutbox(Protocol):
    def enqueue(self, decision: Mapping[str, Any]) -> bool:
        """Persist one immutable minimized decision."""

    def claim_batch(
        self,
        lease_seconds: float,
        maximum: int = MAX_WORKFLOW_DECISIONS_PER_BATCH,
    ) -> Optional[WorkflowDecisionOutboxItem]:
        """Lease one bounded batch or return none."""

    def mark_delivered(self, item: WorkflowDecisionOutboxItem) -> None:
        """Complete a currently held batch lease."""

    def reschedule(
        self,
        item: WorkflowDecisionOutboxItem,
        *,
        delay_seconds: float,
        error_code: str,
    ) -> None:
        """Release a batch lease after a bounded retry delay."""

    def mark_dead_letter(
        self,
        item: WorkflowDecisionOutboxItem,
        *,
        error_code: str,
    ) -> None:
        """Retain a permanently rejected batch without retrying it."""


def _identifier(name: str, value: Any, maximum: int = 256) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or _IDENTIFIER.fullmatch(value) is None
    ):
        raise WorkflowDecisionError("%s must be a bounded identifier" % name)
    return value


def _optional_identifier(name: str, value: Any, maximum: int = 256) -> Optional[str]:
    return None if value is None else _identifier(name, value, maximum)


def _digest(name: str, value: Any) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise WorkflowDecisionError("%s must be a lowercase sha256 digest" % name)
    return value


def _integer(name: str, value: Any, minimum: int) -> int:
    if type(value) is not int or value < minimum:
        raise WorkflowDecisionError("%s is invalid" % name)
    return value


def _instant(name: str, value: Any) -> str:
    if not isinstance(value, str) or not re.search(r"(?:Z|[+-]\d{2}:\d{2})$", value):
        raise WorkflowDecisionError("%s must include a timezone" % name)
    try:
        return (
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            .astimezone(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )
    except ValueError:
        raise WorkflowDecisionError("%s must be ISO-8601" % name) from None


def _identifier_list(
    name: str,
    value: Any,
    *,
    maximum_items: int = 256,
    maximum_length: int = 512,
) -> list[str]:
    if not isinstance(value, (list, tuple)) or len(value) > maximum_items:
        raise WorkflowDecisionError("%s has an invalid item count" % name)
    normalized = [_identifier(name, item, maximum_length) for item in value]
    if len(set(normalized)) != len(normalized):
        raise WorkflowDecisionError("%s must contain unique values" % name)
    return normalized


_DECISION_KEYS = frozenset(
    {
        "decisionId",
        "occurredAt",
        "requestId",
        "workflowId",
        "workflowVersion",
        "workflowInstanceId",
        "ontologyDigest",
        "policyDigest",
        "sectorSnapshotDigest",
        "state",
        "stateVersion",
        "taskId",
        "transitionId",
        "recommendedOutcome",
        "appliedOutcome",
        "reasonCodes",
        "controlIds",
        "obligationIds",
        "factSetDigest",
        "missingFactIds",
        "staleFactIds",
        "approvalReferences",
        "evidenceReferences",
    }
)


def validate_workflow_decision(value: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize the exact payload-free workflow-decision wire contract."""

    if not isinstance(value, Mapping) or set(value) != _DECISION_KEYS:
        raise WorkflowDecisionError("workflow decision fields are invalid")
    recommended = value.get("recommendedOutcome")
    applied = value.get("appliedOutcome")
    if recommended not in _OUTCOMES:
        raise WorkflowDecisionError("recommendedOutcome is unsupported")
    if applied not in _APPLIED_OUTCOMES:
        raise WorkflowDecisionError("appliedOutcome is unsupported")
    if recommended == "allow" and applied != "allow":
        raise WorkflowDecisionError("allow recommendation must be applied")
    normalized = {
        "decisionId": _identifier("decisionId", value.get("decisionId")),
        "occurredAt": _instant("occurredAt", value.get("occurredAt")),
        "requestId": _identifier("requestId", value.get("requestId")),
        "workflowId": _identifier("workflowId", value.get("workflowId")),
        "workflowVersion": _integer("workflowVersion", value.get("workflowVersion"), 1),
        "workflowInstanceId": _identifier(
            "workflowInstanceId", value.get("workflowInstanceId")
        ),
        "ontologyDigest": _digest("ontologyDigest", value.get("ontologyDigest")),
        "policyDigest": _digest("policyDigest", value.get("policyDigest")),
        "sectorSnapshotDigest": _digest(
            "sectorSnapshotDigest", value.get("sectorSnapshotDigest")
        ),
        "state": _identifier("state", value.get("state"), 128),
        "stateVersion": _integer("stateVersion", value.get("stateVersion"), 0),
        "taskId": _identifier("taskId", value.get("taskId"), 128),
        "transitionId": _optional_identifier(
            "transitionId", value.get("transitionId"), 128
        ),
        "recommendedOutcome": recommended,
        "appliedOutcome": applied,
        "reasonCodes": _identifier_list(
            "reasonCodes", value.get("reasonCodes"), maximum_items=64
        ),
        "controlIds": _identifier_list(
            "controlIds", value.get("controlIds"), maximum_items=256
        ),
        "obligationIds": _identifier_list(
            "obligationIds", value.get("obligationIds"), maximum_items=256
        ),
        "factSetDigest": _digest("factSetDigest", value.get("factSetDigest")),
        "missingFactIds": _identifier_list(
            "missingFactIds", value.get("missingFactIds")
        ),
        "staleFactIds": _identifier_list("staleFactIds", value.get("staleFactIds")),
        "approvalReferences": _identifier_list(
            "approvalReferences", value.get("approvalReferences")
        ),
        "evidenceReferences": _identifier_list(
            "evidenceReferences", value.get("evidenceReferences")
        ),
    }
    identity = dict(normalized)
    decision_id = identity.pop("decisionId")
    expected_id = (
        "workflow-decision-"
        + hashlib.sha256(
            json.dumps(
                identity,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
    )
    if decision_id != expected_id:
        raise WorkflowDecisionError("decisionId does not match the minimized decision")
    return normalized


def workflow_decision_from_evidence(
    evidence: WorkflowDecisionEvidence,
) -> Dict[str, Any]:
    """Build a deterministic wire decision from minimized kernel evidence."""

    if not isinstance(evidence, WorkflowDecisionEvidence):
        raise WorkflowDecisionError("evidence must be WorkflowDecisionEvidence")
    body = {
        "occurredAt": _instant("occurredAt", evidence.occurred_at),
        "requestId": evidence.request_id,
        "workflowId": evidence.workflow_id,
        "workflowVersion": evidence.workflow_version,
        "workflowInstanceId": evidence.workflow_instance_id,
        "ontologyDigest": evidence.ontology_digest,
        "policyDigest": evidence.policy_digest,
        "sectorSnapshotDigest": evidence.sector_snapshot_digest,
        "state": evidence.state,
        "stateVersion": evidence.state_version,
        "taskId": evidence.task_id,
        "transitionId": evidence.transition_id,
        "recommendedOutcome": evidence.recommended_outcome,
        "appliedOutcome": evidence.applied_outcome,
        "reasonCodes": list(evidence.reason_codes),
        "controlIds": list(evidence.control_ids),
        "obligationIds": list(evidence.obligation_ids),
        "factSetDigest": evidence.fact_set_digest,
        "missingFactIds": list(evidence.missing_fact_ids),
        "staleFactIds": list(evidence.stale_fact_ids),
        "approvalReferences": list(evidence.approval_references),
        "evidenceReferences": list(evidence.evidence_references),
    }
    encoded = json.dumps(
        body,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    body["decisionId"] = "workflow-decision-" + hashlib.sha256(encoded).hexdigest()
    return validate_workflow_decision(body)


def _batch_id(decision_ids: Sequence[str]) -> str:
    material = "\x00".join(decision_ids).encode("utf-8")
    return "workflow-batch-" + hashlib.sha256(material).hexdigest()


def build_workflow_decision_batch(
    decisions: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    if not decisions or len(decisions) > MAX_WORKFLOW_DECISIONS_PER_BATCH:
        raise WorkflowDecisionError("batch decision count is invalid")
    normalized = [validate_workflow_decision(item) for item in decisions]
    decision_ids = [item["decisionId"] for item in normalized]
    if len(set(decision_ids)) != len(decision_ids):
        raise WorkflowDecisionError("batch decision IDs must be unique")
    return validate_workflow_decision_batch(
        {
            "schema": WORKFLOW_DECISION_SCHEMA,
            "batchId": _batch_id(decision_ids),
            "decisions": normalized,
        }
    )


def validate_workflow_decision_batch(value: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "schema",
        "batchId",
        "decisions",
    }:
        raise WorkflowDecisionError("batch fields are invalid")
    if value.get("schema") != WORKFLOW_DECISION_SCHEMA:
        raise WorkflowDecisionError("batch schema is unsupported")
    decisions = value.get("decisions")
    if (
        not isinstance(decisions, (list, tuple))
        or not decisions
        or len(decisions) > MAX_WORKFLOW_DECISIONS_PER_BATCH
    ):
        raise WorkflowDecisionError("batch decision count is invalid")
    normalized = [validate_workflow_decision(item) for item in decisions]
    decision_ids = [item["decisionId"] for item in normalized]
    if len(set(decision_ids)) != len(decision_ids):
        raise WorkflowDecisionError("batch decision IDs must be unique")
    batch = {
        "schema": WORKFLOW_DECISION_SCHEMA,
        "batchId": _identifier("batchId", value.get("batchId")),
        "decisions": normalized,
    }
    if batch["batchId"] != _batch_id(decision_ids):
        raise WorkflowDecisionError("batchId does not match its decisions")
    encoded = json.dumps(
        batch,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    if len(encoded) > MAX_WORKFLOW_DECISION_BODY_BYTES:
        raise WorkflowDecisionError("batch exceeds 1 MiB")
    return batch


class DurableWorkflowDecisionEmitter:
    """Persist decisions synchronously, then wake network delivery."""

    def __init__(
        self,
        outbox: WorkflowDecisionOutbox,
        dispatcher: "WorkflowDecisionDispatcher",
    ) -> None:
        self._outbox = outbox
        self._dispatcher = dispatcher

    def emit(self, decision: WorkflowDecisionEvidence) -> None:
        self._outbox.enqueue(workflow_decision_from_evidence(decision))
        self._dispatcher.wake()


class WorkflowDecisionClient:
    """Minimal client for the strict workflow-decision batch endpoint."""

    def __init__(self, base_url: str, api_key: str, timeout: float = 10.0) -> None:
        if not base_url or not api_key:
            raise WorkflowDecisionError("base_url and api_key are required")
        self._url = base_url.rstrip("/") + "/api/workflow-decision-batches"
        self._api_key = api_key
        self._timeout = timeout

    def submit(self, decisions: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
        batch = build_workflow_decision_batch(decisions)
        body = json.dumps(
            batch,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        request = urllib.request.Request(
            self._url,
            data=body,
            method="POST",
            headers={"content-type": "application/json", "x-api-key": self._api_key},
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                geturl = getattr(response, "geturl", None)
                final_url = geturl() if callable(geturl) else self._url
                if final_url != self._url:
                    raise WorkflowDecisionSubmissionError(
                        302, "Workflow decision endpoint redirected"
                    )
                response_body = response.read(_MAX_RESPONSE_BYTES + 1)
                if len(response_body) > _MAX_RESPONSE_BYTES:
                    raise WorkflowDecisionSubmissionError(
                        None, "Workflow decision response was too large"
                    )
                decoded = json.loads(response_body.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read(1000).decode("utf-8", errors="replace")
            raise WorkflowDecisionSubmissionError(
                exc.code,
                "Workflow decision batch rejected: HTTP %s: %s" % (exc.code, detail),
            ) from exc
        except WorkflowDecisionSubmissionError:
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise WorkflowDecisionSubmissionError(
                None,
                "Workflow decision transport failed: %s" % type(exc).__name__,
            ) from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WorkflowDecisionSubmissionError(
                None, "Workflow decision endpoint returned invalid JSON"
            ) from exc
        if not isinstance(decoded, dict):
            raise WorkflowDecisionSubmissionError(
                None, "Workflow decision endpoint returned invalid response"
            )
        if (
            decoded.get("batchId") != batch["batchId"]
            or decoded.get("decisionCount") != len(batch["decisions"])
            or decoded.get("status") not in {"queued", "processed"}
        ):
            raise WorkflowDecisionSubmissionError(
                None, "Workflow decision acknowledgement is invalid"
            )
        return decoded


def _positive_number(name: str, value: Any, maximum: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or value <= 0
        or value > maximum
    ):
        raise WorkflowDecisionError("%s must be a positive number" % name)
    return float(value)


class WorkflowDecisionDispatcher:
    """Background sender that never gates runtime request serving."""

    def __init__(
        self,
        outbox: WorkflowDecisionOutbox,
        client: WorkflowDecisionClient,
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
        self._lease_seconds = _positive_number("lease_seconds", lease_seconds, 3600)
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
            raise WorkflowDecisionError(
                "max_backoff_seconds must be at least initial_backoff_seconds"
            )
        self._on_status = on_status
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="prometa-runtime-workflow-decisions",
            daemon=True,
        )
        self._started = False
        self._start_lock = threading.Lock()

    def _status(
        self,
        outcome: str,
        item: Optional[WorkflowDecisionOutboxItem],
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
        except WorkflowDecisionSubmissionError as exc:
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
    "DurableWorkflowDecisionEmitter",
    "MAX_WORKFLOW_DECISIONS_PER_BATCH",
    "MAX_WORKFLOW_DECISION_BODY_BYTES",
    "WORKFLOW_DECISION_SCHEMA",
    "WorkflowDecisionClient",
    "WorkflowDecisionDispatcher",
    "WorkflowDecisionError",
    "WorkflowDecisionOutbox",
    "WorkflowDecisionOutboxItem",
    "WorkflowDecisionSubmissionError",
    "build_workflow_decision_batch",
    "validate_workflow_decision",
    "validate_workflow_decision_batch",
    "workflow_decision_from_evidence",
]
