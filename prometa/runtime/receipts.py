"""Tenant-runtime lifecycle receipt construction and submission.

Receipts are authenticated with an explicitly scoped Orchestra API key. They
report admission/rollout state; they do not ask the control plane to perform a
deployment.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_ENVIRONMENTS = frozenset({"dev", "test", "staging", "prod"})
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
    allowed_outcomes = _OUTCOME_BY_TRANSITION.get(transition)
    if allowed_outcomes is None:
        raise RuntimeReceiptError("unsupported transition")
    if outcome not in allowed_outcomes:
        raise RuntimeReceiptError("outcome is invalid for transition")
    if reason is not None and (not isinstance(reason, str) or len(reason) > 1000):
        raise RuntimeReceiptError("reason must be at most 1000 characters")

    return {
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
                decoded = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:1000]
            raise RuntimeReceiptSubmissionError(
                exc.code, "Runtime receipt rejected: HTTP %s: %s" % (exc.code, detail)
            ) from exc
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
        return decoded


__all__ = [
    "RuntimeReceiptClient",
    "RuntimeReceiptError",
    "RuntimeReceiptSubmissionError",
    "build_runtime_receipt",
]
