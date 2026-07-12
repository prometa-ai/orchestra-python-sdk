"""Outbound bootstrap pull for one tenant-authorized runtime release.

The client retrieves immutable release material selected by tenant CI/CD. It
does not watch desired state, activate a release, or participate in request-time
execution; the existing local admission verifier remains authoritative.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional


RUNTIME_RELEASE_HANDOFF_VERSION = 1
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_ENVIRONMENTS = frozenset({"dev", "test", "staging", "prod"})
_RETRYABLE_HTTP_STATUSES = frozenset({408, 425, 429})


class RuntimeControlPlaneError(RuntimeError):
    """Stable control-plane pull failure with explicit retry semantics."""

    def __init__(
        self,
        code: str,
        *,
        status: Optional[int] = None,
        retryable: bool = False,
    ) -> None:
        self.code = code
        self.status = status
        self.retryable = retryable
        super().__init__(code.replace("_", " "))


@dataclass(frozen=True)
class RuntimeReleaseHandoff:
    """One atomically retrieved bundle and promotion-attestation pair."""

    attestation_id: str
    artifact_id: str
    artifact_digest: str
    release_id: str
    deployment_id: str
    target_environment: str
    runtime_target: str
    bundle: Mapping[str, Any]
    promotion_attestation: Mapping[str, Any]
    checked_at: datetime
    fetched_at: datetime


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


def _strict_json_object(data: bytes) -> Mapping[str, Any]:
    def reject_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def reject_constant(value):
        raise ValueError("non-finite number")

    try:
        decoded = json.loads(
            data.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        raise RuntimeControlPlaneError("control_plane_response_invalid") from None
    if not isinstance(decoded, Mapping):
        raise RuntimeControlPlaneError("control_plane_response_invalid")
    return decoded


def _identifier(name: str, value: Any, maximum: int = 200) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or _IDENTIFIER.fullmatch(value) is None
    ):
        raise RuntimeControlPlaneError("control_plane_response_invalid")
    return value


def _instant(value: Any) -> datetime:
    if not isinstance(value, str):
        raise RuntimeControlPlaneError("control_plane_response_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise RuntimeControlPlaneError("control_plane_response_invalid") from None
    if parsed.tzinfo is None:
        raise RuntimeControlPlaneError("control_plane_response_invalid")
    return parsed.astimezone(timezone.utc)


def _service_url(base_url: str, allow_insecure_http: bool) -> str:
    if not isinstance(base_url, str) or not base_url or len(base_url) > 2048:
        raise ValueError("base_url must be a bounded HTTP(S) URL")
    parsed = urllib.parse.urlsplit(base_url)
    try:
        port = parsed.port
    except ValueError:
        raise ValueError("base_url must be a bounded HTTP(S) URL") from None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise ValueError("base_url must be a bounded HTTP(S) URL")
    if parsed.scheme != "https" and not allow_insecure_http:
        raise ValueError("base_url must use HTTPS")
    return base_url.rstrip("/")


def _http_error_code(error: urllib.error.HTTPError) -> str:
    try:
        body = error.read(4096)
        decoded = _strict_json_object(body)
        code = decoded.get("code")
        if isinstance(code, str) and _ERROR_CODE.fullmatch(code):
            return code
    except RuntimeControlPlaneError:
        pass
    return "control_plane_http_%d" % error.code


class RuntimeControlPlaneClient:
    """Minimal no-redirect client for the runtime release handoff API."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout_seconds: float = 5.0,
        max_response_bytes: int = 12 * 1024 * 1024,
        max_clock_skew_seconds: int = 300,
        allow_insecure_http: bool = False,
    ) -> None:
        self._base_url = _service_url(base_url, allow_insecure_http)
        if not isinstance(api_key, str) or len(api_key.encode("utf-8")) < 16:
            raise ValueError("api_key must contain at least 16 bytes")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or timeout_seconds <= 0
            or timeout_seconds > 60
        ):
            raise ValueError("timeout_seconds must be between 0 and 60")
        if (
            type(max_response_bytes) is not int
            or max_response_bytes < 1024
            or max_response_bytes > 16 * 1024 * 1024
        ):
            raise ValueError("max_response_bytes must be between 1 KiB and 16 MiB")
        if (
            type(max_clock_skew_seconds) is not int
            or max_clock_skew_seconds < 0
            or max_clock_skew_seconds > 3600
        ):
            raise ValueError("max_clock_skew_seconds must be between 0 and 3600")
        self._api_key = api_key
        self._timeout_seconds = float(timeout_seconds)
        self._max_response_bytes = max_response_bytes
        self._max_clock_skew_seconds = max_clock_skew_seconds
        self._opener = urllib.request.build_opener(_RejectRedirects())

    def fetch_release(
        self,
        attestation_id: str,
        *,
        expected_release_id: Optional[str] = None,
        expected_deployment_id: Optional[str] = None,
        expected_environment: Optional[str] = None,
        expected_runtime: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> RuntimeReleaseHandoff:
        """Fetch and transport-bind one handoff before cryptographic admission."""

        attestation = _identifier("attestation_id", attestation_id)
        url = "%s/api/runtime-releases/%s" % (
            self._base_url,
            urllib.parse.quote(attestation, safe=""),
        )
        request = urllib.request.Request(
            url,
            method="GET",
            headers={
                "accept": "application/json",
                "x-api-key": self._api_key,
            },
        )
        fetched_at = now or datetime.now(timezone.utc)
        if fetched_at.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        fetched_at = fetched_at.astimezone(timezone.utc)
        try:
            with self._opener.open(request, timeout=self._timeout_seconds) as response:
                body = response.read(self._max_response_bytes + 1)
        except urllib.error.HTTPError as exc:
            status = exc.code
            raise RuntimeControlPlaneError(
                _http_error_code(exc),
                status=status,
                retryable=(
                    status in _RETRYABLE_HTTP_STATUSES or status >= 500
                ),
            ) from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise RuntimeControlPlaneError(
                "control_plane_unavailable", retryable=True
            ) from None
        if len(body) > self._max_response_bytes:
            raise RuntimeControlPlaneError("control_plane_response_too_large")

        document = _strict_json_object(body)
        if document.get("handoffVersion") != RUNTIME_RELEASE_HANDOFF_VERSION:
            raise RuntimeControlPlaneError("control_plane_version_unsupported")
        response_attestation_id = _identifier(
            "attestation_id", document.get("attestationId")
        )
        artifact_id = _identifier("artifact_id", document.get("artifactId"))
        artifact_digest = document.get("artifactDigest")
        release_id = _identifier("release_id", document.get("releaseId"))
        deployment_id = _identifier(
            "deployment_id", document.get("deploymentId")
        )
        target_environment = _identifier(
            "target_environment", document.get("targetEnvironment")
        )
        runtime_target = _identifier("runtime_target", document.get("runtimeTarget"))
        if (
            response_attestation_id != attestation
            or not isinstance(artifact_digest, str)
            or _DIGEST.fullmatch(artifact_digest) is None
            or target_environment not in _ENVIRONMENTS
        ):
            raise RuntimeControlPlaneError("control_plane_binding_mismatch")
        expected_bindings = (
            (expected_release_id, release_id),
            (expected_deployment_id, deployment_id),
            (expected_environment, target_environment),
            (expected_runtime, runtime_target),
        )
        if any(expected is not None and expected != actual for expected, actual in expected_bindings):
            raise RuntimeControlPlaneError("control_plane_binding_mismatch")

        bundle = document.get("bundle")
        promotion = document.get("promotionAttestation")
        if not isinstance(bundle, Mapping) or not isinstance(promotion, Mapping):
            raise RuntimeControlPlaneError("control_plane_response_invalid")
        if (
            bundle.get("artifactDigest") != artifact_digest
            or promotion.get("attestationId") != attestation
        ):
            raise RuntimeControlPlaneError("control_plane_binding_mismatch")

        checked_at = _instant(document.get("checkedAt"))
        if (
            abs((checked_at - fetched_at).total_seconds())
            > self._max_clock_skew_seconds
        ):
            raise RuntimeControlPlaneError("control_plane_response_stale")
        return RuntimeReleaseHandoff(
            attestation_id=attestation,
            artifact_id=artifact_id,
            artifact_digest=artifact_digest,
            release_id=release_id,
            deployment_id=deployment_id,
            target_environment=target_environment,
            runtime_target=runtime_target,
            bundle=dict(bundle),
            promotion_attestation=dict(promotion),
            checked_at=checked_at,
            fetched_at=fetched_at,
        )


__all__ = [
    "RUNTIME_RELEASE_HANDOFF_VERSION",
    "RuntimeControlPlaneError",
    "RuntimeReleaseHandoff",
    "RuntimeControlPlaneClient",
]
