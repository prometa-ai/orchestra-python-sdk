"""Tenant-deployed reference HTTP service for the guardrail contract.

The service handles caller content by definition, so what it records is bounded
by construction: every log line is metadata — request id, tenant, profile,
stage, verdict, reason code, counts, digests, timing — and the evaluated
payload, the transformed payload, and any matched fragment are never written
anywhere. Content evidence leaves only as a sha256 digest inside an assessment.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import signal
import ssl
import sys
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, TextIO, Tuple

from .contract import GUARDRAIL_EVALUATE_PATH
from .profiles import GuardrailProfileError, load_guardrail_profiles
from .service import GuardrailService, GuardrailServiceResponse


GUARDRAIL_HOST_CONFIG_VERSION = 1
DEFAULT_MAX_REQUEST_BYTES = 2 * 1024 * 1024
DEFAULT_MAX_CONCURRENT_REQUESTS = 64
DEFAULT_RETRY_AFTER_SECONDS = 1
_MAX_CONFIG_BYTES = 4 * 1024 * 1024
_MAX_KEYS_BYTES = 1024 * 1024
_MIN_API_KEY_BYTES = 32

# Placeholder markers the shipped example carries. The example file has to look
# like a placeholder to a reader and still clear the length floor, so length
# alone cannot tell an unedited example from a real credential; an operator who
# copies the example and starts the service would otherwise be serving on a
# token published in this repository.
_PLACEHOLDER_API_KEY_MARKERS: Tuple[str, ...] = (
    "replace-with",
    "replace_with",
    "changeme",
    "change-me",
    "your-api-key",
    "placeholder",
    "example-key",
)

_CONFIG_KEYS = frozenset(
    {
        "configVersion",
        "defaultProfile",
        "apiKeysFile",
        "maxRequestBytes",
        "maxConcurrentRequests",
        "profiles",
    }
)
_API_KEY_KEYS = frozenset({"key", "tenant", "org_id"})


class GuardrailHostError(RuntimeError):
    """Fail-closed startup or configuration refusal, without any payload."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class GuardrailApiKey:
    """One bearer credential bound to the tenant it may evaluate for."""

    key_digest: bytes
    tenant: str
    org_id: Optional[str]


@dataclass(frozen=True)
class GuardrailHostConfig:
    """Everything the service needs to start, already validated."""

    default_profile: str
    api_keys_file: Path
    max_request_bytes: int
    max_concurrent_requests: int
    profiles: Mapping[str, Any]


def _strict_json_loads(data: bytes, code: str) -> Any:
    def reject_duplicates(pairs):
        result: Dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def reject_constant(value):
        raise ValueError("non-finite number")

    try:
        return json.loads(
            data.decode("utf-8"),
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, ValueError) as exc:
        raise GuardrailHostError(code) from exc


def load_guardrail_api_keys(path: Path) -> Tuple[GuardrailApiKey, ...]:
    """Read the keys file, refusing anything a caller could brute force."""

    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise GuardrailHostError("guardrail_api_keys_unreadable") from exc
    if len(raw) > _MAX_KEYS_BYTES:
        raise GuardrailHostError("guardrail_api_keys_too_large")
    document = _strict_json_loads(raw, "guardrail_api_keys_invalid")
    if not isinstance(document, list) or not document:
        raise GuardrailHostError("guardrail_api_keys_invalid")
    keys = []
    seen = set()
    for entry in document:
        if not isinstance(entry, Mapping) or set(entry) - _API_KEY_KEYS:
            raise GuardrailHostError("guardrail_api_keys_invalid")
        key = entry.get("key")
        tenant = entry.get("tenant")
        org_id = entry.get("org_id")
        if not isinstance(key, str) or len(key.encode("utf-8")) < _MIN_API_KEY_BYTES:
            raise GuardrailHostError("guardrail_api_key_too_short")
        folded = key.casefold()
        if any(marker in folded for marker in _PLACEHOLDER_API_KEY_MARKERS):
            raise GuardrailHostError("guardrail_api_key_placeholder")
        if not isinstance(tenant, str) or not tenant.strip():
            raise GuardrailHostError("guardrail_api_keys_invalid")
        if org_id is not None and not isinstance(org_id, str):
            raise GuardrailHostError("guardrail_api_keys_invalid")
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        if digest in seen:
            raise GuardrailHostError("guardrail_api_key_duplicate")
        seen.add(digest)
        keys.append(GuardrailApiKey(key_digest=digest, tenant=tenant, org_id=org_id))
    return tuple(keys)


def load_guardrail_host_config(path: Path) -> GuardrailHostConfig:
    """Read the service configuration, rejecting any member it does not know."""

    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise GuardrailHostError("guardrail_config_unreadable") from exc
    if len(raw) > _MAX_CONFIG_BYTES:
        raise GuardrailHostError("guardrail_config_too_large")
    document = _strict_json_loads(raw, "guardrail_config_invalid")
    if not isinstance(document, Mapping) or set(document) - _CONFIG_KEYS:
        raise GuardrailHostError("guardrail_config_invalid")
    if document.get("configVersion") != GUARDRAIL_HOST_CONFIG_VERSION:
        raise GuardrailHostError("guardrail_config_version_unsupported")
    try:
        profiles = load_guardrail_profiles(document.get("profiles"))
    except GuardrailProfileError as exc:
        raise GuardrailHostError(exc.code) from exc
    default_profile = document.get("defaultProfile")
    if default_profile is None:
        default_profile = sorted(profiles)[0]
    if default_profile not in profiles:
        raise GuardrailHostError("guardrail_default_profile_unknown")
    api_keys_file = document.get("apiKeysFile")
    if not isinstance(api_keys_file, str) or not api_keys_file:
        raise GuardrailHostError("guardrail_config_invalid")
    max_request_bytes = document.get("maxRequestBytes", DEFAULT_MAX_REQUEST_BYTES)
    max_concurrent = document.get(
        "maxConcurrentRequests", DEFAULT_MAX_CONCURRENT_REQUESTS
    )
    for value in (max_request_bytes, max_concurrent):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise GuardrailHostError("guardrail_config_invalid")
    return GuardrailHostConfig(
        default_profile=default_profile,
        api_keys_file=Path(api_keys_file),
        max_request_bytes=max_request_bytes,
        max_concurrent_requests=max_concurrent,
        profiles=profiles,
    )


class JsonLineGuardrailLogger:
    """Metadata-only structured log; no evaluated content ever reaches it."""

    def __init__(self, stream: Optional[TextIO] = None) -> None:
        self._stream = stream if stream is not None else sys.stdout
        self._lock = threading.Lock()

    def record(self, event: Mapping[str, Any]) -> None:
        line = json.dumps(
            dict(event), separators=(",", ":"), sort_keys=True, ensure_ascii=False
        )
        with self._lock:
            self._stream.write(line + "\n")
            self._stream.flush()


class GuardrailServiceHost:
    """Authenticated HTTP application around one :class:`GuardrailService`."""

    def __init__(
        self,
        service: GuardrailService,
        api_keys: Sequence[GuardrailApiKey],
        *,
        max_request_bytes: int = DEFAULT_MAX_REQUEST_BYTES,
        max_concurrent_requests: int = DEFAULT_MAX_CONCURRENT_REQUESTS,
        logger: Optional[JsonLineGuardrailLogger] = None,
    ) -> None:
        if not api_keys:
            raise GuardrailHostError("guardrail_api_keys_invalid")
        self.service = service
        self._api_keys = tuple(api_keys)
        self.max_request_bytes = max_request_bytes
        self._slots = threading.Semaphore(max_concurrent_requests)
        self._logger = logger if logger is not None else JsonLineGuardrailLogger()

    @staticmethod
    def error(status: int, code: str) -> GuardrailServiceResponse:
        return GuardrailServiceResponse(
            status=status,
            body={
                "error": {
                    "message": code.replace("_", " "),
                    "type": "guardrail_error",
                    "code": code,
                    "param": None,
                }
            },
        )

    def _resolve_key(self, headers: Mapping[str, str]) -> Optional[GuardrailApiKey]:
        value = headers.get("authorization", "")
        if not value.startswith("Bearer "):
            return None
        digest = hashlib.sha256(value[7:].encode("utf-8")).digest()
        for key in self._api_keys:
            if hmac.compare_digest(digest, key.key_digest):
                return key
        return None

    def handle(
        self,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
    ) -> GuardrailServiceResponse:
        if method == "GET" and path == "/healthz":
            return GuardrailServiceResponse(status=200, body={"status": "ok"})
        if method == "GET" and path == "/readyz":
            return GuardrailServiceResponse(status=200, body=self.service.readiness())
        if method != "POST" or path != GUARDRAIL_EVALUATE_PATH:
            return self.error(404, "guardrail_route_unknown")
        key = self._resolve_key(headers)
        if key is None:
            return self.error(401, "guardrail_unauthenticated")
        if not self._slots.acquire(blocking=False):
            return self.error(429, "guardrail_overloaded")
        started = time.monotonic()
        try:
            try:
                document = _strict_json_loads(body, "guardrail_request_invalid")
            except GuardrailHostError:
                return self._logged(
                    self.error(400, "guardrail_request_invalid"), key, None, started
                )
            if not isinstance(document, Mapping):
                return self._logged(
                    self.error(400, "guardrail_request_invalid"), key, None, started
                )
            subject = document.get("subject")
            tenant = subject.get("tenant") if isinstance(subject, Mapping) else None
            # A request that names no tenant is malformed, not a mismatch; the
            # service answers 400 for it. Answering 403 here would report a
            # missing REQUIRED field as an authorization failure.
            if isinstance(tenant, str) and tenant != key.tenant:
                return self._logged(
                    self.error(403, "guardrail_tenant_mismatch"), key, document, started
                )
            return self._logged(self.service.evaluate(document), key, document, started)
        finally:
            self._slots.release()

    def _logged(
        self,
        response: GuardrailServiceResponse,
        key: GuardrailApiKey,
        document: Optional[Mapping[str, Any]],
        started: float,
    ) -> GuardrailServiceResponse:
        body = response.body
        error = body.get("error") if isinstance(body, Mapping) else None
        self._logger.record(
            {
                "event": "guardrail.evaluate",
                "tenant": key.tenant,
                "requestId": _bounded_identifier(document, "requestId"),
                "profile": _bounded_identifier(document, "profile"),
                "stage": _bounded_identifier(document, "stage"),
                "status": response.status,
                "verdict": body.get("verdict") if response.status == 200 else None,
                "reasonCode": (
                    body.get("reasonCode")
                    if response.status == 200
                    else (error or {}).get("code")
                ),
                "evaluatedCount": len(body.get("evaluatedGuardrails") or ()),
                "deferredCount": len(body.get("deferred") or ()),
                "detectorPackDigest": (body.get("detectorPack") or {}).get("digest"),
                "latencyMs": body.get("latencyMs"),
                "serviceLatencyMs": (time.monotonic() - started) * 1000.0,
            }
        )
        return response


def _bounded_identifier(
    document: Optional[Mapping[str, Any]], key: str
) -> Optional[str]:
    if not isinstance(document, Mapping):
        return None
    value = document.get(key)
    if not isinstance(value, str):
        return None
    return value[:256]


class _GuardrailHttpServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: Tuple[str, int],
        application: GuardrailServiceHost,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        self.application = application
        super().__init__(address, _GuardrailRequestHandler)
        if ssl_context is not None:
            self.socket = ssl_context.wrap_socket(self.socket, server_side=True)


class _GuardrailRequestHandler(BaseHTTPRequestHandler):
    server: _GuardrailHttpServer
    protocol_version = "HTTP/1.1"
    server_version = "prometa-guardrail"
    sys_version = ""

    def log_message(self, format: str, *args: Any) -> None:
        return None

    def _body(self) -> Tuple[bytes, Optional[GuardrailServiceResponse]]:
        if self.headers.get("transfer-encoding") is not None:
            return b"", GuardrailServiceHost.error(400, "guardrail_request_invalid")
        values = self.headers.get_all("content-length", [])
        if len(values) != 1:
            return b"", GuardrailServiceHost.error(400, "guardrail_request_invalid")
        try:
            length = int(values[0])
        except ValueError:
            return b"", GuardrailServiceHost.error(400, "guardrail_request_invalid")
        if length < 0:
            return b"", GuardrailServiceHost.error(400, "guardrail_request_invalid")
        if length > self.server.application.max_request_bytes:
            return b"", GuardrailServiceHost.error(413, "guardrail_payload_too_large")
        data = self.rfile.read(length)
        if len(data) != length:
            return b"", GuardrailServiceHost.error(400, "guardrail_request_invalid")
        return data, None

    def _dispatch(self) -> None:
        path = self.path.split("?", 1)[0]
        headers = {key.lower(): value for key, value in self.headers.items()}
        body = b""
        response = None
        if self.command == "POST":
            body, response = self._body()
        if response is None:
            response = self.server.application.handle(self.command, path, headers, body)
        try:
            encoded = json.dumps(
                dict(response.body),
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError):
            response = GuardrailServiceHost.error(500, "guardrail_detector_failed")
            encoded = b'{"error":{"message":"guardrail detector failed",'
            encoded += b'"type":"guardrail_error","code":"guardrail_detector_failed",'
            encoded += b'"param":null}}'
        self.close_connection = True
        self.send_response(response.status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(encoded)))
        self.send_header("cache-control", "no-store")
        self.send_header("x-content-type-options", "nosniff")
        if response.status == 401:
            self.send_header("www-authenticate", "Bearer")
        if response.status == 429:
            self.send_header("retry-after", str(DEFAULT_RETRY_AFTER_SECONDS))
        self.send_header("connection", "close")
        self.end_headers()
        self.wfile.write(encoded)

    do_GET = _dispatch
    do_POST = _dispatch
    do_PUT = _dispatch
    do_PATCH = _dispatch
    do_DELETE = _dispatch


def build_guardrail_service_host(
    config: GuardrailHostConfig,
    *,
    logger: Optional[JsonLineGuardrailLogger] = None,
) -> GuardrailServiceHost:
    """Build the application, failing startup on any unusable policy."""

    try:
        service = GuardrailService(
            config.profiles, default_profile=config.default_profile
        )
    except GuardrailProfileError as exc:
        raise GuardrailHostError(exc.code) from exc
    return GuardrailServiceHost(
        service,
        load_guardrail_api_keys(config.api_keys_file),
        max_request_bytes=config.max_request_bytes,
        max_concurrent_requests=config.max_concurrent_requests,
        logger=logger,
    )


def serve_guardrail_service(
    application: GuardrailServiceHost,
    *,
    bind_host: str = "0.0.0.0",
    port: int = 8080,
    ssl_context: Optional[ssl.SSLContext] = None,
) -> None:
    """Serve until SIGINT/SIGTERM, then drain."""

    if not bind_host or not 1 <= port <= 65535:
        raise GuardrailHostError("listen_address_invalid")
    server = _GuardrailHttpServer((bind_host, port), application, ssl_context)
    stopping = threading.Event()

    def stop(signum=None, frame=None):
        if stopping.is_set():
            return
        stopping.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    previous = {}
    if threading.current_thread() is threading.main_thread():
        for selected in (signal.SIGINT, signal.SIGTERM):
            previous[selected] = signal.signal(selected, stop)
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        server.server_close()
        for selected, handler in previous.items():
            signal.signal(selected, handler)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="prometa-guardrail-service",
        description="Serve the reference orchestra-guardrail-evaluate-v1 service.",
    )
    parser.add_argument(
        "--config",
        default=os.environ.get(
            "PROMETA_GUARDRAIL_CONFIG", "/etc/prometa-guardrail/config.json"
        ),
    )
    parser.add_argument(
        "--host", default=os.environ.get("PROMETA_GUARDRAIL_HOST", "0.0.0.0")
    )
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8080")))
    args = parser.parse_args(argv)
    try:
        config = load_guardrail_host_config(Path(args.config))
        application = build_guardrail_service_host(config)
    except GuardrailHostError as error:
        sys.stderr.write(error.code + "\n")
        return 2
    serve_guardrail_service(application, bind_host=args.host, port=args.port)
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    sys.exit(main())


__all__ = [
    "DEFAULT_MAX_CONCURRENT_REQUESTS",
    "DEFAULT_MAX_REQUEST_BYTES",
    "DEFAULT_RETRY_AFTER_SECONDS",
    "GUARDRAIL_HOST_CONFIG_VERSION",
    "GuardrailApiKey",
    "GuardrailHostConfig",
    "GuardrailHostError",
    "GuardrailServiceHost",
    "JsonLineGuardrailLogger",
    "build_guardrail_service_host",
    "load_guardrail_api_keys",
    "load_guardrail_host_config",
    "main",
    "serve_guardrail_service",
]
