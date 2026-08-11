"""Tenant-runtime lifecycle receipt construction and submission.

Receipts are authenticated with an explicitly scoped Orchestra API key. They
report admission/rollout state; they do not ask the control plane to perform a
deployment.
"""

from __future__ import annotations

import json
import re
import threading
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping, Optional, Protocol


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_ENVIRONMENTS = frozenset({"dev", "test", "staging", "prod"})
_MAX_RESPONSE_BYTES = 64 * 1024
_RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429})
_OUTCOME_BY_TRANSITION = {
    "admitted": frozenset({"accepted"}),
    "rollout_started": frozenset({"accepted"}),
    "active": frozenset({"succeeded"}),
    "paused": frozenset({"succeeded"}),
    "rollback_started": frozenset({"accepted"}),
    "rolled_back": frozenset({"succeeded"}),
    "failed": frozenset({"failed"}),
    "stopped": frozenset({"succeeded"}),
    "quarantined": frozenset({"succeeded"}),
    "resumed": frozenset({"succeeded"}),
    "controls_stale": frozenset({"failed", "succeeded"}),
}
_RUNTIME_CONTROL_TRANSITIONS = frozenset(
    {"quarantined", "resumed", "controls_stale"}
)
_ENFORCEMENT_VALUES = frozenset({"enforcing", "advisory"})
_SCOPES = frozenset({"org", "tenant", "deployment", "solution", "agent"})
#: The acknowledgement direction's key and its exact membership, pinned by the
#: runtime-control lease contract §7b and covered by the shared ``acks/``
#: vectors. Every member is required: defaulting an absent one would silently
#: rewrite what the replica said. Notably absent is ``mode`` — that is the
#: issuer's instruction, which already travels in the lease, and reporting it
#: back would say what this replica was told rather than what it did.
RUNTIME_CONTROL_ACK_KEY = "runtimeControlAck"
_ACK_FIELDS = (
    "leaseId",
    "revision",
    "enforcement",
    "enforceableScopes",
    "enforcedControlCount",
    "ignoredControlCount",
    "stale",
    "leaseExpiresAt",
    "leaseParseFailed",
)


class RuntimeReceiptError(ValueError):
    """Invalid local receipt construction."""


class RuntimeReceiptSubmissionError(RuntimeError):
    """A receipt endpoint rejected or could not process the request."""

    def __init__(self, status: Optional[int], message: str) -> None:
        self.status = status
        super().__init__(message)


@dataclass(frozen=True)
class RuntimeReceiptOutboxItem:
    """One leased receipt awaiting asynchronous delivery."""

    receipt_id: str
    receipt: Mapping[str, Any]
    attempts: int
    lease_token: str


class RuntimeReceiptOutbox(Protocol):
    """Durable multi-replica queue used by the reference runtime host."""

    def enqueue(self, receipt: Mapping[str, Any]) -> bool:
        """Persist a receipt once; return true only for a new row."""

    def claim_next(self, lease_seconds: float) -> Optional[RuntimeReceiptOutboxItem]:
        """Lease one currently deliverable item or return none."""

    def mark_delivered(self, item: RuntimeReceiptOutboxItem) -> None:
        """Complete a currently held lease."""

    def reschedule(
        self,
        item: RuntimeReceiptOutboxItem,
        *,
        delay_seconds: float,
        error_code: str,
    ) -> None:
        """Release a lease and make it available after a bounded delay."""

    def mark_dead_letter(
        self, item: RuntimeReceiptOutboxItem, *, error_code: str
    ) -> None:
        """Retain a permanently rejected receipt without retrying it."""


def _identifier(name: str, value: str, max_length: int = 200) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > max_length
        or _IDENTIFIER.fullmatch(value) is None
    ):
        raise RuntimeReceiptError("%s must be a bounded identifier" % name)
    return value


def _instant(value: Optional[datetime]) -> str:
    timestamp = value or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        raise RuntimeReceiptError("event_at must be timezone-aware")
    utc = timestamp.astimezone(timezone.utc)
    return utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _bounded_count(name: str, value: Any) -> int:
    if type(value) is not int or value < 0:
        raise RuntimeReceiptError("%s must be a non-negative integer" % name)
    return value


def _normalized_acknowledgement(value: Any) -> Dict[str, Any]:
    """Validate one ``runtimeControlAck`` and return it in the pinned shape.

    Unknown members are dropped rather than stored, and every pinned member is
    required. The one cross-field rule is the contract's opening premise: a
    surface may only claim enforcement it can prove, so ``enforcing`` without a
    lease to name is refused rather than counted.
    """

    if not isinstance(value, Mapping):
        raise RuntimeReceiptError("runtime_control_ack must be a mapping")
    missing = [field for field in _ACK_FIELDS if field not in value]
    if missing:
        raise RuntimeReceiptError(
            "runtime_control_ack is missing: %s" % ",".join(missing)
        )
    lease_id = value["leaseId"]
    revision = value["revision"]
    if lease_id is not None:
        _identifier("runtime_control_ack.leaseId", lease_id)
    if revision is not None and (type(revision) is not int or revision < 0):
        raise RuntimeReceiptError(
            "runtime_control_ack.revision must be a non-negative integer"
        )
    if (lease_id is None) != (revision is None):
        raise RuntimeReceiptError(
            "runtime_control_ack.leaseId and revision must be supplied together"
        )
    enforcement = value["enforcement"]
    if enforcement not in _ENFORCEMENT_VALUES:
        raise RuntimeReceiptError(
            "runtime_control_ack.enforcement must be enforcing or advisory"
        )
    if enforcement == "enforcing" and lease_id is None:
        raise RuntimeReceiptError(
            "runtime_control_ack cannot claim enforcement without a lease"
        )
    scopes = value["enforceableScopes"]
    if not isinstance(scopes, (list, tuple)):
        raise RuntimeReceiptError(
            "runtime_control_ack.enforceableScopes must be a sequence"
        )
    scopes = list(scopes)
    if not scopes or not set(scopes) <= _SCOPES or len(set(scopes)) != len(scopes):
        raise RuntimeReceiptError(
            "runtime_control_ack.enforceableScopes must be distinct control scopes"
        )
    expires_at = value["leaseExpiresAt"]
    if expires_at is not None and not isinstance(expires_at, str):
        raise RuntimeReceiptError(
            "runtime_control_ack.leaseExpiresAt must be an instant or null"
        )
    if (lease_id is None) and expires_at is not None:
        raise RuntimeReceiptError(
            "runtime_control_ack.leaseExpiresAt requires a lease"
        )
    for flag in ("stale", "leaseParseFailed"):
        if type(value[flag]) is not bool:
            raise RuntimeReceiptError("runtime_control_ack.%s must be a bool" % flag)
    return {
        "leaseId": lease_id,
        "revision": revision,
        "enforcement": enforcement,
        "enforceableScopes": scopes,
        "enforcedControlCount": _bounded_count(
            "runtime_control_ack.enforcedControlCount", value["enforcedControlCount"]
        ),
        "ignoredControlCount": _bounded_count(
            "runtime_control_ack.ignoredControlCount", value["ignoredControlCount"]
        ),
        "stale": value["stale"],
        "leaseExpiresAt": expires_at,
        "leaseParseFailed": value["leaseParseFailed"],
    }


def build_runtime_receipt(
    *,
    attestation_id: str,
    artifact_digest: str,
    release_id: str,
    deployment_id: str,
    target_environment: str,
    runtime_target: str,
    runtime_id: str,
    runtime_version: str,
    transition: str,
    outcome: str,
    policy_digest: Optional[str] = None,
    configuration_digest: Optional[str] = None,
    receipt_id: Optional[str] = None,
    event_at: Optional[datetime] = None,
    reason: Optional[str] = None,
    runtime_control_ack: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the normalized platform receipt payload.

    Keep the returned ``receiptId`` when retrying. The platform treats the
    same ID + semantic payload as idempotent and rejects reuse with new bytes.

    ``runtime_control_ack`` is the acknowledgement direction of the
    runtime-control lease contract, carried under the additive top-level
    ``runtimeControlAck`` key in exactly the shape §7b pins. It is what lets
    the control plane separate desired from enforced state: which lease this
    replica is applying, at what revision, whether it is still fresh, which
    scopes this replica can resolve at all, and how many controls it is
    actually enforcing rather than how many the lease names. It is required on
    the runtime-control transitions and rejected on every other transition, so
    a lifecycle receipt can never imply an acknowledgement it did not make.
    """

    if target_environment not in _ENVIRONMENTS:
        raise RuntimeReceiptError("unsupported target_environment")
    if _DIGEST.fullmatch(artifact_digest) is None:
        raise RuntimeReceiptError("artifact_digest must be sha256:<hex>")
    if (policy_digest is None) != (configuration_digest is None):
        raise RuntimeReceiptError(
            "policy_digest and configuration_digest must be supplied together"
        )
    if policy_digest is not None and (
        _DIGEST.fullmatch(policy_digest) is None
        or _DIGEST.fullmatch(configuration_digest or "") is None
    ):
        raise RuntimeReceiptError(
            "policy_digest and configuration_digest must be sha256:<hex>"
        )
    allowed_outcomes = _OUTCOME_BY_TRANSITION.get(transition)
    if allowed_outcomes is None:
        raise RuntimeReceiptError("unsupported transition")
    if outcome not in allowed_outcomes:
        raise RuntimeReceiptError("outcome is invalid for transition")
    if reason is not None and (not isinstance(reason, str) or len(reason) > 1000):
        raise RuntimeReceiptError("reason must be at most 1000 characters")
    control_transition = transition in _RUNTIME_CONTROL_TRANSITIONS
    if not control_transition and runtime_control_ack is not None:
        raise RuntimeReceiptError(
            "runtime_control_ack requires a runtime-control transition"
        )
    acknowledgement = None
    if control_transition:
        if runtime_control_ack is None:
            raise RuntimeReceiptError(
                "runtime_control_ack is required on runtime-control transitions"
            )
        acknowledgement = _normalized_acknowledgement(runtime_control_ack)
        if acknowledgement["leaseId"] is None and transition != "controls_stale":
            raise RuntimeReceiptError(
                "a lease is required to acknowledge %s" % transition
            )
        if transition == "controls_stale" and acknowledgement["stale"] != (
            outcome == "failed"
        ):
            raise RuntimeReceiptError(
                "runtime_control_ack.stale must agree with the controls_stale outcome"
            )
        if transition in {"quarantined", "resumed"} and acknowledgement["stale"]:
            raise RuntimeReceiptError(
                "a stale replica cannot acknowledge a control transition"
            )

    receipt = {
        "receiptId": _identifier("receipt_id", receipt_id or str(uuid.uuid4())),
        "attestationId": _identifier("attestation_id", attestation_id),
        "artifactDigest": artifact_digest,
        "releaseId": _identifier("release_id", release_id),
        "deploymentId": _identifier("deployment_id", deployment_id),
        "targetEnvironment": target_environment,
        "runtimeTarget": _identifier("runtime_target", runtime_target),
        "runtimeId": _identifier("runtime_id", runtime_id),
        "runtimeVersion": _identifier("runtime_version", runtime_version),
        "transition": transition,
        "outcome": outcome,
        "reason": reason,
        "eventAt": _instant(event_at),
    }
    if policy_digest is not None and configuration_digest is not None:
        receipt["policyDigest"] = policy_digest
        receipt["configurationDigest"] = configuration_digest
    if acknowledgement is not None:
        receipt[RUNTIME_CONTROL_ACK_KEY] = acknowledgement
    return receipt


class RuntimeReceiptClient:
    """Minimal stdlib client for ``POST /api/runtime-receipts``."""

    def __init__(self, base_url: str, api_key: str, timeout: float = 10.0) -> None:
        if not base_url or not api_key:
            raise RuntimeReceiptError("base_url and api_key are required")
        self._url = base_url.rstrip("/") + "/api/runtime-receipts"
        self._api_key = api_key
        self._timeout = timeout

    def submit(self, receipt: Mapping[str, Any]) -> Dict[str, Any]:
        body = json.dumps(
            dict(receipt), separators=(",", ":"), ensure_ascii=False
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
                    raise RuntimeReceiptSubmissionError(
                        302, "Runtime receipt endpoint redirected"
                    )
                response_body = response.read(_MAX_RESPONSE_BYTES + 1)
                if len(response_body) > _MAX_RESPONSE_BYTES:
                    raise RuntimeReceiptSubmissionError(
                        None, "Runtime receipt endpoint response was too large"
                    )
                decoded = json.loads(response_body.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read(1000).decode("utf-8", errors="replace")
            raise RuntimeReceiptSubmissionError(
                exc.code, "Runtime receipt rejected: HTTP %s: %s" % (exc.code, detail)
            ) from exc
        except RuntimeReceiptSubmissionError:
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RuntimeReceiptSubmissionError(
                None, "Runtime receipt transport failed: %s" % type(exc).__name__
            ) from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeReceiptSubmissionError(
                None, "Runtime receipt endpoint returned invalid JSON"
            ) from exc
        if not isinstance(decoded, dict):
            raise RuntimeReceiptSubmissionError(
                None, "Runtime receipt endpoint returned a non-object response"
            )
        if (
            decoded.get("receiptId") != receipt.get("receiptId")
            or decoded.get("status") != "recorded"
        ):
            raise RuntimeReceiptSubmissionError(
                None, "Runtime receipt endpoint returned an invalid acknowledgement"
            )
        return decoded


def _positive_number(name: str, value: Any, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuntimeReceiptError("%s must be a positive number" % name)
    result = float(value)
    if result <= 0 or result > maximum:
        raise RuntimeReceiptError("%s must be a positive number" % name)
    return result


class RuntimeReceiptDispatcher:
    """Background outbox dispatcher that never gates runtime request serving."""

    def __init__(
        self,
        outbox: RuntimeReceiptOutbox,
        client: RuntimeReceiptClient,
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
            raise RuntimeReceiptError(
                "max_backoff_seconds must be at least initial_backoff_seconds"
            )
        self._on_status = on_status
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="prometa-runtime-receipts",
            daemon=True,
        )
        self._started = False
        self._start_lock = threading.Lock()

    def _status(
        self,
        outcome: str,
        item: Optional[RuntimeReceiptOutboxItem],
        error_code: Optional[str] = None,
    ) -> None:
        if self._on_status is None:
            return
        details = {
            "receiptId": item.receipt_id if item is not None else "unavailable",
            "transition": (
                str(item.receipt.get("transition", "unknown"))
                if item is not None
                else "unknown"
            ),
        }
        if error_code is not None:
            details["errorCode"] = error_code
        try:
            self._on_status(outcome, details)
        except Exception:
            # Delivery status evidence must not kill the durable dispatcher.
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
        """Attempt one leased delivery; return false when no item was ready."""

        item = self._outbox.claim_next(self._lease_seconds)
        if item is None:
            return False
        try:
            self._client.submit(item.receipt)
        except RuntimeReceiptSubmissionError as exc:
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
    "RUNTIME_CONTROL_ACK_KEY",
    "RuntimeReceiptClient",
    "RuntimeReceiptDispatcher",
    "RuntimeReceiptError",
    "RuntimeReceiptOutbox",
    "RuntimeReceiptOutboxItem",
    "RuntimeReceiptSubmissionError",
    "build_runtime_receipt",
]
