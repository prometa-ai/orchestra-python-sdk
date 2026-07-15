"""In-cluster model gateway and payload-free topology probes."""

from __future__ import annotations

import argparse
import json
import os
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Mapping, Optional, Sequence, Tuple


class ProbeError(RuntimeError):
    pass


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _print(value: Mapping[str, Any]) -> None:
    print(_json_bytes(value).decode("utf-8"), flush=True)


def _strict_json(data: bytes) -> Any:
    def reject_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def reject_constant(value):
        raise ValueError("non-finite number: %s" % value)

    return json.loads(
        data.decode("utf-8"),
        object_pairs_hook=reject_duplicates,
        parse_constant=reject_constant,
    )


def _api_token() -> str:
    token = os.environ.get("RUNTIME_API_TOKEN", "")
    if len(token.encode("utf-8")) < 32:
        raise ProbeError("runtime_api_token_missing")
    return token


def _read_response(response: Any) -> Mapping[str, Any]:
    data = response.read(1_048_577)
    if len(data) > 1_048_576:
        raise ProbeError("response_too_large")
    value = _strict_json(data)
    if not isinstance(value, Mapping):
        raise ProbeError("response_invalid")
    return value


def _http_json(request: urllib.request.Request, timeout: float) -> Tuple[int, Any]:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status), _read_response(response)
    except urllib.error.HTTPError as exc:
        return int(exc.code), _read_response(exc)


def _runtime_request(url: str, request_id: str, timeout: float) -> Tuple[int, Any]:
    request = urllib.request.Request(
        url.rstrip("/") + "/v1/runtime/execute",
        method="POST",
        data=_json_bytes(
            {
                "requestId": request_id,
                "input": {"question": "topology certification"},
            }
        ),
        headers={
            "authorization": "Bearer %s" % _api_token(),
            "content-type": "application/json",
            "accept": "application/json",
        },
    )
    return _http_json(request, timeout)


def request_probe(
    url: str,
    request_id: str,
    expected_status: int,
    expected_answer: Optional[str],
    expected_error: Optional[str],
    timeout: float,
) -> None:
    status, body = _runtime_request(url, request_id, timeout)
    if status != expected_status:
        raise ProbeError("unexpected_runtime_status")
    if expected_answer is not None:
        if body.get("output") != {"answer": expected_answer}:
            raise ProbeError("tenant_output_mismatch")
    if expected_error is not None:
        if body.get("error") != {"code": expected_error}:
            raise ProbeError("runtime_error_mismatch")
    _print({"passed": True, "status": status})


def blocked_request_probe(url: str, timeout: float) -> None:
    parsed = urllib.parse.urlparse(url)
    if not parsed.hostname:
        raise ProbeError("blocked_url_invalid")
    socket.getaddrinfo(parsed.hostname, parsed.port or 80, type=socket.SOCK_STREAM)
    try:
        _runtime_request(url, "blocked-topology-request", timeout)
    except (OSError, TimeoutError, urllib.error.URLError):
        _print({"passed": True, "resolved": True, "reachable": False})
        return
    raise ProbeError("blocked_runtime_reachable")


def load_probe(
    urls: Sequence[str],
    prefix: str,
    requests: int,
    concurrency: int,
    expected_answer: str,
    timeout: float,
) -> None:
    if not urls or requests < 1 or not 1 <= concurrency <= requests:
        raise ProbeError("load_parameters_invalid")

    def invoke(index: int) -> int:
        status, body = _runtime_request(
            urls[index % len(urls)],
            "%s-%03d" % (prefix, index),
            timeout,
        )
        if status != 200 or body.get("output") != {"answer": expected_answer}:
            raise ProbeError("load_request_failed")
        return status

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        statuses = list(executor.map(invoke, range(requests)))
    if statuses.count(200) != requests:
        raise ProbeError("load_request_failed")
    _print(
        {
            "passed": True,
            "requests": requests,
            "successes": requests,
            "concurrency": concurrency,
        }
    )


def duplicate_probe(
    urls: Sequence[str],
    request_id: str,
    attempts: int,
    expected_answer: str,
    timeout: float,
    mcp: bool = False,
) -> None:
    if len(urls) < 2 or attempts < 2:
        raise ProbeError("duplicate_parameters_invalid")
    barrier = threading.Barrier(attempts)

    def invoke(index: int) -> Tuple[int, Any]:
        barrier.wait(timeout=5)
        return _runtime_request(urls[index % len(urls)], request_id, timeout)

    with ThreadPoolExecutor(max_workers=attempts) as executor:
        outcomes = list(executor.map(invoke, range(attempts)))
    winners = [body for status, body in outcomes if status == 200]
    conflict_statuses = {409, 500} if mcp else {409}
    conflicts = [body for status, body in outcomes if status in conflict_statuses]
    if len(winners) != 1 or len(conflicts) != attempts - 1:
        raise ProbeError("duplicate_winner_count_invalid")
    if winners[0].get("output") != {"answer": expected_answer}:
        raise ProbeError("duplicate_winner_output_invalid")
    allowed_errors = {
        "request_in_progress",
        "task_in_progress",
        "task_already_completed",
    }
    if mcp:
        allowed_errors.update(
            {"mcp_tool_call_in_progress", "mcp_duplicate_tool_call"}
        )
    if any(body.get("error", {}).get("code") not in allowed_errors for body in conflicts):
        raise ProbeError("duplicate_conflict_invalid")
    _print(
        {
            "passed": True,
            "attempts": attempts,
            "winners": 1,
            "conflicts": len(conflicts),
        }
    )


def task_status_probe(
    url: str, request_id: str, expected_status: str, timeout: float
) -> None:
    request = urllib.request.Request(
        url.rstrip("/") + "/v1/runtime/tasks/" + urllib.parse.quote(request_id),
        headers={"authorization": "Bearer %s" % _api_token()},
    )
    status, body = _http_json(request, timeout)
    if status != 200 or body.get("status") != expected_status:
        raise ProbeError("task_status_invalid")
    _print(
        {
            "passed": True,
            "status": status,
            "taskStatus": expected_status,
            "attempt": body.get("attempt"),
        }
    )


def socket_probe(host: str, port: int, expected: str, timeout: float) -> None:
    socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    connected = False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            connected = True
    except (OSError, TimeoutError):
        connected = False
    if (expected == "allowed") != connected:
        raise ProbeError("socket_policy_mismatch")
    _print(
        {
            "passed": True,
            "resolved": True,
            "connected": connected,
            "expected": expected,
        }
    )


class _ModelState:
    def __init__(self, tenant: str) -> None:
        self.tenant = tenant
        self.count = 0
        self.lock = threading.Lock()


def serve_model_gateway(tenant: str, port: int, *, mcp: bool = False) -> None:
    state = _ModelState(tenant)

    class Handler(BaseHTTPRequestHandler):
        server_version = "TopologyModelGateway"
        sys_version = ""

        def log_message(self, format, *args):
            return None

        def _send(self, status: int, body: Mapping[str, Any]) -> None:
            payload = _json_bytes(body)
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.send_header("cache-control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path != "/count":
                self._send(404, {"error": {"code": "not_found"}})
                return
            with state.lock:
                count = state.count
            self._send(200, {"count": count})

        def do_POST(self):
            if self.path != "/v1/chat/completions":
                self._send(404, {"error": {"code": "not_found"}})
                return
            try:
                length = int(self.headers.get("content-length", "0"))
                if not 1 <= length <= 65536:
                    raise ValueError("invalid length")
                document = _strict_json(self.rfile.read(length))
                request_id = self.headers.get("x-orchestra-runtime-request-id", "")
                if (
                    not isinstance(document, Mapping)
                    or document.get("model") != "golden-model"
                    or not request_id
                ):
                    raise ValueError("invalid request")
                messages = document.get("messages")
                tools = document.get("tools", [])
                if not isinstance(messages, list):
                    raise ValueError("invalid messages")
                tool_completed = any(
                    isinstance(message, Mapping) and message.get("role") == "tool"
                    for message in messages
                )
                if mcp and (
                    not isinstance(tools, list)
                    or len(tools) != 1
                    or not isinstance(tools[0], Mapping)
                    or not isinstance(tools[0].get("function"), Mapping)
                    or tools[0].get("function", {}).get("name") != "lookup_tenant"
                ):
                    raise ValueError("invalid tools")
            except (ValueError, UnicodeError, json.JSONDecodeError):
                self._send(400, {"error": {"code": "request_invalid"}})
                return
            with state.lock:
                state.count += 1
            if "duplicate" in request_id:
                time.sleep(0.8)
            if mcp and not tool_completed:
                self._send(
                    200,
                    {
                        "model": "golden-model",
                        "choices": [
                            {
                                "message": {
                                    "content": None,
                                    "tool_calls": [
                                        {
                                            "id": "call-%s" % request_id,
                                            "type": "function",
                                            "function": {
                                                "name": "lookup_tenant",
                                                "arguments": json.dumps(
                                                    {"requestId": request_id},
                                                    sort_keys=True,
                                                    separators=(",", ":"),
                                                ),
                                            },
                                        }
                                    ],
                                },
                                "finish_reason": "tool_calls",
                            }
                        ],
                    },
                )
                return
            self._send(
                200,
                {
                    "model": "golden-model",
                    "choices": [
                        {
                            "message": {"content": {"answer": state.tenant}},
                            "finish_reason": "stop",
                        }
                    ],
                },
            )

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()


def model_count_probe(url: str, timeout: float) -> None:
    request = urllib.request.Request(url, headers={"accept": "application/json"})
    status, body = _http_json(request, timeout)
    count = body.get("count") if isinstance(body, Mapping) else None
    if status != 200 or type(count) is not int or count < 0:
        raise ProbeError("model_count_invalid")
    _print({"passed": True, "count": count})


def _urls(value: str) -> Tuple[str, ...]:
    urls = tuple(item.strip().rstrip("/") for item in value.split(",") if item.strip())
    if not urls or any(not item.startswith("http://") for item in urls):
        raise ProbeError("probe_urls_invalid")
    return urls


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="topology_probe.py")
    subparsers = parser.add_subparsers(dest="command", required=True)

    model = subparsers.add_parser("model-gateway")
    model.add_argument("--tenant", required=True)
    model.add_argument("--port", type=int, default=8000)
    model.add_argument("--mcp", action="store_true")

    subparsers.add_parser("sleep")

    request = subparsers.add_parser("request")
    request.add_argument("--url", required=True)
    request.add_argument("--request-id", required=True)
    request.add_argument("--expect-status", type=int, default=200)
    request.add_argument("--expect-answer")
    request.add_argument("--expect-error")
    request.add_argument("--timeout", type=float, default=12)

    blocked = subparsers.add_parser("blocked-request")
    blocked.add_argument("--url", required=True)
    blocked.add_argument("--timeout", type=float, default=2)

    load = subparsers.add_parser("load")
    load.add_argument("--urls", required=True)
    load.add_argument("--prefix", required=True)
    load.add_argument("--requests", type=int, required=True)
    load.add_argument("--concurrency", type=int, required=True)
    load.add_argument("--expect-answer", required=True)
    load.add_argument("--timeout", type=float, default=12)

    duplicate = subparsers.add_parser("duplicates")
    duplicate.add_argument("--urls", required=True)
    duplicate.add_argument("--request-id", required=True)
    duplicate.add_argument("--attempts", type=int, required=True)
    duplicate.add_argument("--expect-answer", required=True)
    duplicate.add_argument("--timeout", type=float, default=12)
    duplicate.add_argument("--mcp", action="store_true")

    task = subparsers.add_parser("task-status")
    task.add_argument("--url", required=True)
    task.add_argument("--request-id", required=True)
    task.add_argument("--expect-status", required=True)
    task.add_argument("--timeout", type=float, default=8)

    socket_parser = subparsers.add_parser("socket")
    socket_parser.add_argument("--host", required=True)
    socket_parser.add_argument("--port", type=int, required=True)
    socket_parser.add_argument("--expect", choices=("allowed", "denied"), required=True)
    socket_parser.add_argument("--timeout", type=float, default=2)

    count = subparsers.add_parser("model-count")
    count.add_argument("--url", required=True)
    count.add_argument("--timeout", type=float, default=3)
    mcp_count = subparsers.add_parser("mcp-count")
    mcp_count.add_argument("--url", required=True)
    mcp_count.add_argument("--timeout", type=float, default=3)

    args = parser.parse_args(argv)
    try:
        if args.command == "model-gateway":
            serve_model_gateway(args.tenant, args.port, mcp=args.mcp)
        elif args.command == "sleep":
            while True:
                time.sleep(3600)
        elif args.command == "request":
            request_probe(
                args.url,
                args.request_id,
                args.expect_status,
                args.expect_answer,
                args.expect_error,
                args.timeout,
            )
        elif args.command == "blocked-request":
            blocked_request_probe(args.url, args.timeout)
        elif args.command == "load":
            load_probe(
                _urls(args.urls),
                args.prefix,
                args.requests,
                args.concurrency,
                args.expect_answer,
                args.timeout,
            )
        elif args.command == "duplicates":
            duplicate_probe(
                _urls(args.urls),
                args.request_id,
                args.attempts,
                args.expect_answer,
                args.timeout,
                args.mcp,
            )
        elif args.command == "task-status":
            task_status_probe(
                args.url,
                args.request_id,
                args.expect_status,
                args.timeout,
            )
        elif args.command == "socket":
            socket_probe(args.host, args.port, args.expect, args.timeout)
        elif args.command in {"model-count", "mcp-count"}:
            model_count_probe(args.url, args.timeout)
    except (ProbeError, OSError, TimeoutError, urllib.error.URLError) as exc:
        code = str(exc) if isinstance(exc, ProbeError) else "probe_transport_failed"
        _print({"passed": False, "errorCode": code})
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
