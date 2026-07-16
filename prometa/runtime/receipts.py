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
}


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
) -> Dict[str, Any]:
    """Build the normalized platform receipt payload.

    Keep the returned ``receiptId`` when retrying. The platform treats the
    same ID + semantic payload as idempotent and rejects reuse with new bytes.
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
    "RuntimeReceiptClient",
    "RuntimeReceiptDispatcher",
    "RuntimeReceiptError",
    "RuntimeReceiptOutbox",
    "RuntimeReceiptOutboxItem",
    "RuntimeReceiptSubmissionError",
    "build_runtime_receipt",
]
