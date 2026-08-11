"""End-to-end exercise of the shipped ``HumanEscalation`` references."""

from __future__ import annotations

import asyncio
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from prometa.runtime import (
    ControlPlaneHumanEscalation,
    FileSystemHumanEscalation,
    HumanEscalationRequest,
    RuntimeExecutionError,
    RuntimeTool,
)


API_KEY = "approval-api-key-value-32-bytes-x"
RUNTIME_ID = "runtime-approvals"
TOOL = RuntimeTool(
    name="Refund order",
    source="mcp",
    operation="orders.refund",
    input_schema={"type": "object"},
    mcp_server="Orders",
    side_effects="destructive",
    risk_level="high",
    auth_binding="api-key",
    scopes=("orders.write",),
    approval_required=True,
    required_guardrails=(),
)
SECRET_PAYLOAD = {"orderId": "order-secret-42", "iban": "DE00-secret"}


class _ApprovalService:
    """Minimal stand-in for the control plane's approval-request models."""

    def __init__(self, script) -> None:
        self.script = list(script)
        self.posts = []
        self.gets = []
        self.authorizations = []
        service = self

        class _Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args) -> None:
                return None

            def _reply(self, status, body):
                encoded = json.dumps(body).encode("utf-8")
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def do_POST(self):
                length = int(self.headers.get("content-length", "0"))
                document = json.loads(self.rfile.read(length).decode("utf-8"))
                service.posts.append(document)
                service.authorizations.append(self.headers.get("x-api-key"))
                status, body = service.script.pop(0)
                if isinstance(body, dict) and "approvalRequestId" in body:
                    body = dict(body)
                    body.setdefault(
                        "approvalRequestId", document["approvalRequestId"]
                    )
                self._reply(status, service._resolve(body, document))

            def do_GET(self):
                service.gets.append(self.path)
                status, body = service.script.pop(0)
                approval_request_id = self.path.rsplit("/", 1)[-1]
                self._reply(
                    status,
                    service._resolve(body, {"approvalRequestId": approval_request_id}),
                )

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @staticmethod
    def _resolve(body, document):
        resolved = dict(body)
        resolved.setdefault("approvalRequestId", document["approvalRequestId"])
        return resolved

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return "http://%s:%d" % (host, port)

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


@contextmanager
def _service(script):
    service = _ApprovalService(script)
    try:
        yield service
    finally:
        service.close()


def _closed_base_url() -> str:
    service = _ApprovalService([])
    base_url = service.base_url
    service.close()
    return base_url


def _request() -> HumanEscalationRequest:
    return HumanEscalationRequest(
        request_id="request-refund-1",
        reason="destructive tool requires approval",
        stage="tool_call",
        payload=SECRET_PAYLOAD,
        tool=TOOL,
    )


def _escalation(base_url, **overrides):
    values = {
        "runtime_id": RUNTIME_ID,
        "allow_insecure_http": True,
        "poll_interval_seconds": 0.01,
        "decision_timeout_seconds": 5,
        "timeout_seconds": 5,
    }
    values.update(overrides)
    return ControlPlaneHumanEscalation(base_url, API_KEY, **values)


def test_an_immediate_approval_returns_the_reviewer_reference() -> None:
    with _service(
        [(201, {"status": "approved", "approvalId": "approval-9", "reason": "ok"})]
    ) as service:
        decision = asyncio.run(_escalation(service.base_url).request_review(_request()))
        posted = service.posts[0]
        authorization = service.authorizations[0]

    assert decision.approved is True
    assert decision.reviewer_reference == "approval-9"
    assert authorization == API_KEY
    assert posted["toolOperation"] == "orders.refund"
    assert posted["sideEffects"] == "destructive"
    assert posted["payloadDigest"].startswith("sha256:")


def test_the_approval_request_never_carries_the_payload_or_the_api_key() -> None:
    with _service(
        [(201, {"status": "approved", "approvalId": "approval-9"})]
    ) as service:
        asyncio.run(_escalation(service.base_url).request_review(_request()))
        rendered = json.dumps(service.posts)

    assert "order-secret-42" not in rendered
    assert "DE00-secret" not in rendered
    assert API_KEY not in rendered


def test_a_pending_request_is_polled_until_the_reviewer_answers() -> None:
    with _service(
        [
            (201, {"status": "pending"}),
            (200, {"status": "pending"}),
            (200, {"status": "denied", "approvalId": "approval-10", "reason": "no"}),
        ]
    ) as service:
        decision = asyncio.run(_escalation(service.base_url).request_review(_request()))

    assert decision.approved is False
    assert decision.reviewer_reference == "approval-10"
    assert len(service.gets) == 2


def test_a_reviewer_who_never_answers_is_not_an_approval() -> None:
    script = [(201, {"status": "pending"})] + [(200, {"status": "pending"})] * 500
    with _service(script) as service:
        escalation = _escalation(service.base_url, decision_timeout_seconds=0.05)
        with pytest.raises(RuntimeExecutionError) as caught:
            asyncio.run(escalation.request_review(_request()))

    assert caught.value.code == "human_review_timeout"


def test_an_unreachable_control_plane_fails_closed_without_a_fallback() -> None:
    escalation = _escalation(_closed_base_url())

    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(escalation.request_review(_request()))

    assert caught.value.code == "human_review_unavailable"


def test_an_air_gapped_fallback_takes_over_when_the_control_plane_is_gone(
    tmp_path,
) -> None:
    reviewed = {}

    class _Reviewer:
        async def request_review(self, request):
            reviewed["stage"] = request.stage
            return await FileSystemHumanEscalation(
                tmp_path,
                runtime_id=RUNTIME_ID,
                poll_interval_seconds=0.01,
                decision_timeout_seconds=5,
            ).request_review(request)

    escalation = _escalation(_closed_base_url(), fallback=_Reviewer())

    async def scenario():
        task = asyncio.ensure_future(escalation.request_review(_request()))
        for _ in range(500):
            pending = list((tmp_path / "requests").glob("*.json")) if (
                tmp_path / "requests"
            ).exists() else []
            if pending:
                document = json.loads(pending[0].read_text(encoding="utf-8"))
                (tmp_path / "decisions" / pending[0].name).write_text(
                    json.dumps(
                        {
                            "approvalRequestId": document["approvalRequestId"],
                            "status": "approved",
                            "approvalId": "operator-console-1",
                        }
                    ),
                    encoding="utf-8",
                )
                break
            await asyncio.sleep(0.01)
        return await task

    decision = asyncio.run(scenario())

    assert reviewed["stage"] == "tool_call"
    assert decision.approved is True
    assert decision.reviewer_reference == "operator-console-1"
    written = json.loads(
        next((tmp_path / "requests").glob("*.json")).read_text(encoding="utf-8")
    )
    assert "order-secret-42" not in json.dumps(written)
    assert written["payloadDigest"].startswith("sha256:")


def test_the_local_reviewer_times_out_rather_than_assuming_approval(tmp_path) -> None:
    escalation = FileSystemHumanEscalation(
        tmp_path,
        runtime_id=RUNTIME_ID,
        poll_interval_seconds=0.01,
        decision_timeout_seconds=0.05,
    )

    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(escalation.request_review(_request()))

    assert caught.value.code == "human_review_timeout"


def test_a_decision_for_a_different_request_is_refused(tmp_path) -> None:
    escalation = FileSystemHumanEscalation(
        tmp_path,
        runtime_id=RUNTIME_ID,
        poll_interval_seconds=0.01,
        decision_timeout_seconds=5,
    )

    async def scenario():
        task = asyncio.ensure_future(escalation.request_review(_request()))
        for _ in range(500):
            pending = list((tmp_path / "requests").glob("*.json")) if (
                tmp_path / "requests"
            ).exists() else []
            if pending:
                (tmp_path / "decisions" / pending[0].name).write_text(
                    json.dumps(
                        {
                            "approvalRequestId": "approval-somebody-else",
                            "status": "approved",
                            "approvalId": "operator-console-1",
                        }
                    ),
                    encoding="utf-8",
                )
                break
            await asyncio.sleep(0.01)
        return await task

    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(scenario())

    assert caught.value.code == "human_review_response_invalid"


def test_plaintext_http_requires_an_explicit_opt_in() -> None:
    with pytest.raises(ValueError):
        ControlPlaneHumanEscalation(
            "http://approvals.internal",
            API_KEY,
            runtime_id=RUNTIME_ID,
        )
