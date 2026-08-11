"""Shipped ``HumanEscalation`` references for the tenant runtime.

The kernel fails closed when no escalation is wired: write and destructive
tools raise ``human_escalation_unavailable`` and nothing runs. That default is
correct and stays, but it also meant the approval path had never been exercised
end to end. These two references close that gap.

Nothing here sends payloads. An approval request carries identities, the tool
being asked about, and a digest of the material under review; the reviewer
opens the item in the control plane, where the payload already lives under its
own access control. The API key travels in a request header and never reaches
a request body, a log line, or an audit event.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Mapping, Optional

from .kernel import (
    HumanEscalation,
    HumanEscalationDecision,
    HumanEscalationRequest,
    RuntimeExecutionError,
)


HUMAN_APPROVAL_REQUEST_VERSION = 1
_MAX_RESPONSE_BYTES = 64 * 1024
_TERMINAL_STATUSES = frozenset({"approved", "denied"})
_STATUSES = _TERMINAL_STATUSES | {"pending"}


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


def _payload_digest(payload: Any) -> str:
    """Digest the material under review without ever transmitting it."""

    try:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError):
        # An unserializable payload still gets a stable, content-free marker so
        # the request id stays deterministic across retries.
        encoded = b"\x00non-canonical-payload"
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _approval_request_id(
    runtime_id: str, request: HumanEscalationRequest, payload_digest: str
) -> str:
    identity = "\x00".join(
        (
            runtime_id,
            request.request_id,
            request.stage,
            request.reason,
            request.tool.operation if request.tool is not None else "",
            payload_digest,
        )
    )
    return "approval-%s" % hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]


def _describe(
    runtime_id: str, request: HumanEscalationRequest, payload_digest: str
) -> dict:
    tool = request.tool
    return {
        "approvalRequestVersion": HUMAN_APPROVAL_REQUEST_VERSION,
        "approvalRequestId": _approval_request_id(runtime_id, request, payload_digest),
        "runtimeId": runtime_id,
        "requestId": request.request_id,
        "stage": request.stage,
        "reason": request.reason,
        "toolName": tool.name if tool is not None else None,
        "toolOperation": tool.operation if tool is not None else None,
        "sideEffects": tool.side_effects if tool is not None else None,
        "riskLevel": tool.risk_level if tool is not None else None,
        "payloadDigest": payload_digest,
    }


def _positive_number(name: str, value: Any, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("%s must be a positive number" % name)
    result = float(value)
    if result <= 0 or result > maximum:
        raise ValueError("%s must be a positive number" % name)
    return result


def _decision(
    document: Mapping[str, Any], approval_request_id: str
) -> Optional[HumanEscalationDecision]:
    if document.get("approvalRequestId") != approval_request_id:
        raise RuntimeExecutionError("human_review_response_invalid")
    status = document.get("status")
    if status not in _STATUSES:
        raise RuntimeExecutionError("human_review_response_invalid")
    if status == "pending":
        return None
    reviewer_reference = document.get("approvalId")
    if not isinstance(reviewer_reference, str) or not reviewer_reference.strip():
        raise RuntimeExecutionError("human_review_response_invalid")
    reason = document.get("reason")
    return HumanEscalationDecision(
        approved=status == "approved",
        reviewer_reference=reviewer_reference,
        reason=reason if isinstance(reason, str) else "",
    )


class ControlPlaneHumanEscalation:
    """Ask the control plane's approval models for a human decision.

    Only the approval surface is control-plane bound. Nothing else in the
    request path gains a dependency: a runtime with no write or destructive
    tools never calls this class at all.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        runtime_id: str,
        timeout_seconds: float = 5.0,
        poll_interval_seconds: float = 2.0,
        decision_timeout_seconds: float = 300.0,
        allow_insecure_http: bool = False,
        fallback: Optional[HumanEscalation] = None,
    ) -> None:
        parsed = urllib.parse.urlsplit(base_url or "")
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("base_url must be a bounded HTTP(S) URL")
        if parsed.scheme != "https" and not allow_insecure_http:
            raise ValueError("base_url must use HTTPS")
        if not isinstance(api_key, str) or len(api_key.encode("utf-8")) < 16:
            raise ValueError("api_key must contain at least 16 bytes")
        if not isinstance(runtime_id, str) or not runtime_id.strip():
            raise ValueError("runtime_id is required")
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._runtime_id = runtime_id
        self._timeout_seconds = _positive_number("timeout_seconds", timeout_seconds, 60)
        self._poll_interval_seconds = _positive_number(
            "poll_interval_seconds", poll_interval_seconds, 300
        )
        self._decision_timeout_seconds = _positive_number(
            "decision_timeout_seconds", decision_timeout_seconds, 86_400
        )
        self._fallback = fallback
        self._opener = urllib.request.build_opener(_RejectRedirects())

    def _call(self, method: str, url: str, body: Optional[bytes]) -> Mapping[str, Any]:
        headers = {"accept": "application/json", "x-api-key": self._api_key}
        if body is not None:
            headers["content-type"] = "application/json"
        request = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with self._opener.open(request, timeout=self._timeout_seconds) as response:
                raw = response.read(_MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            if exc.code in {401, 403}:
                raise RuntimeExecutionError("human_review_unauthorized") from None
            raise RuntimeExecutionError(
                "human_review_unavailable", retryable=True
            ) from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise RuntimeExecutionError(
                "human_review_unavailable", retryable=True
            ) from None
        if len(raw) > _MAX_RESPONSE_BYTES:
            raise RuntimeExecutionError("human_review_response_invalid")
        try:
            document = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            raise RuntimeExecutionError("human_review_response_invalid") from None
        if not isinstance(document, Mapping):
            raise RuntimeExecutionError("human_review_response_invalid")
        return document

    async def request_review(
        self, request: HumanEscalationRequest
    ) -> HumanEscalationDecision:
        payload_digest = _payload_digest(request.payload)
        document = _describe(self._runtime_id, request, payload_digest)
        approval_request_id = str(document["approvalRequestId"])
        body = json.dumps(document, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
        status_url = "%s/api/approval-requests/%s" % (
            self._base_url,
            urllib.parse.quote(approval_request_id, safe=""),
        )
        try:
            created = await asyncio.to_thread(
                self._call, "POST", self._base_url + "/api/approval-requests", body
            )
        except RuntimeExecutionError as exc:
            if self._fallback is None or exc.code != "human_review_unavailable":
                raise
            return await self._fallback.request_review(request)
        decision = _decision(created, approval_request_id)
        if decision is not None:
            return decision
        deadline = time.monotonic() + self._decision_timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # A reviewer who has not answered is not an approval.
                raise RuntimeExecutionError("human_review_timeout")
            await asyncio.sleep(min(self._poll_interval_seconds, remaining))
            polled = await asyncio.to_thread(self._call, "GET", status_url, None)
            decision = _decision(polled, approval_request_id)
            if decision is not None:
                return decision


class FileSystemHumanEscalation:
    """Air-gapped fallback: request files out, decision files in.

    The decision directory is a trust boundary. Anything that can write there
    can approve a destructive tool call, so it belongs on the same footing as
    the runtime's own credentials, not on a shared volume.

    Write decision files atomically (write elsewhere, then rename into place).
    A half-written file is read as an invalid decision and fails the step
    rather than being retried, because a partially readable approval is not an
    approval.
    """

    def __init__(
        self,
        directory: Path,
        *,
        runtime_id: str,
        poll_interval_seconds: float = 2.0,
        decision_timeout_seconds: float = 300.0,
    ) -> None:
        if not isinstance(runtime_id, str) or not runtime_id.strip():
            raise ValueError("runtime_id is required")
        self._root = Path(directory)
        self._requests = self._root / "requests"
        self._decisions = self._root / "decisions"
        self._runtime_id = runtime_id
        self._poll_interval_seconds = _positive_number(
            "poll_interval_seconds", poll_interval_seconds, 300
        )
        self._decision_timeout_seconds = _positive_number(
            "decision_timeout_seconds", decision_timeout_seconds, 86_400
        )

    def _write_request(self, document: Mapping[str, Any]) -> Path:
        self._requests.mkdir(parents=True, exist_ok=True)
        self._decisions.mkdir(parents=True, exist_ok=True)
        target = self._requests / ("%s.json" % document["approvalRequestId"])
        temporary = target.with_suffix(".json.%s.tmp" % uuid.uuid4().hex)
        temporary.write_text(
            json.dumps(dict(document), separators=(",", ":"), ensure_ascii=False),
            encoding="utf-8",
        )
        temporary.replace(target)
        return self._decisions / ("%s.json" % document["approvalRequestId"])

    def _read_decision(self, path: Path) -> Optional[Mapping[str, Any]]:
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return None
        except OSError:
            raise RuntimeExecutionError("human_review_unavailable", retryable=True)
        if len(raw) > _MAX_RESPONSE_BYTES:
            raise RuntimeExecutionError("human_review_response_invalid")
        try:
            document = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            raise RuntimeExecutionError("human_review_response_invalid") from None
        if not isinstance(document, Mapping):
            raise RuntimeExecutionError("human_review_response_invalid")
        return document

    async def request_review(
        self, request: HumanEscalationRequest
    ) -> HumanEscalationDecision:
        payload_digest = _payload_digest(request.payload)
        document = _describe(self._runtime_id, request, payload_digest)
        approval_request_id = str(document["approvalRequestId"])
        decision_path = await asyncio.to_thread(self._write_request, document)
        deadline = time.monotonic() + self._decision_timeout_seconds
        while True:
            found = await asyncio.to_thread(self._read_decision, decision_path)
            if found is not None:
                decision = _decision(
                    {
                        "approvalRequestId": found.get("approvalRequestId"),
                        "status": found.get("status"),
                        "approvalId": found.get("approvalId"),
                        "reason": found.get("reason"),
                    },
                    approval_request_id,
                )
                if decision is not None:
                    return decision
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeExecutionError("human_review_timeout")
            await asyncio.sleep(min(self._poll_interval_seconds, remaining))


__all__ = [
    "HUMAN_APPROVAL_REQUEST_VERSION",
    "ControlPlaneHumanEscalation",
    "FileSystemHumanEscalation",
]
