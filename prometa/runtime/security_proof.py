"""Deterministic staging-only runtime for the security-assurance proof loop.

This module is intentionally not a production guard implementation.  It gives
operators a repeatable tenant-runtime fixture that proves signed guardrail
changes alter local enforcement while Orchestra observes only minimized
decision evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping, Sequence

from .host import (
    RuntimeHostConfig,
    RuntimeHostError,
    build_reference_runtime_host,
    load_runtime_host_config,
    serve_reference_runtime_host,
)
from .kernel import GuardDecision, GuardRequest
from .security_assurance import SecurityGuardAssessment, SecuritySignal


PROOF_ENABLE_ENV = "PROMETA_RUNTIME_SECURITY_PROOF"
PROOF_ENABLE_VALUE = "enabled"
PROOF_MODEL_PORT_ENV = "PROMETA_SECURITY_PROOF_MODEL_PORT"
REWRITTEN_INPUT = "[ORCHESTRA_SECURITY_PROOF_REWRITTEN]"
LEAKED_OUTPUT = "Leaked credential: sk-proof-abcdefghijklmnop1234"
SAFE_OUTPUT = "I cannot comply with that request."
_DETECTOR_DIGEST = "sha256:" + hashlib.sha256(
    b"orchestra-security-proof-guard-v1"
).hexdigest()


def _content_digest(payload: Any) -> str:
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class DeterministicSecurityProofGuard:
    """Return complete, content-free evidence for every signed guardrail."""

    async def evaluate(self, request: GuardRequest) -> GuardDecision:
        violated = request.stage == "input" or (
            request.stage == "output"
            and isinstance(request.payload, str)
            and "sk-proof-" in request.payload
        )
        confidence = 1.0 if violated else 0.0
        assessments = tuple(
            SecurityGuardAssessment(
                guardrail_name=guardrail.name,
                violated=violated,
                confidence_score=confidence,
                severity="high" if violated else "low",
                category="data_exfiltration",
                detector_kind="deterministic-conformance",
                detector_digest=_DETECTOR_DIGEST,
                summary=(
                    "Staging security probe detected."
                    if violated
                    else "No policy-relevant signal detected."
                ),
                reason_codes=(
                    ("staging_probe_detected",)
                    if violated
                    else ("policy_clear",)
                ),
                signals=(
                    SecuritySignal(
                        kind="deterministic-conformance",
                        score=confidence,
                    ),
                ),
                content_fragment_digests=(_content_digest(request.payload),),
                counterfactual=(
                    "A non-probe request would not trigger this detector."
                ),
                action_rationale=(
                    "Applied the signed staging guardrail configuration."
                ),
            )
            for guardrail in request.guardrails
        )
        return GuardDecision(
            allowed=True,
            action="rewrite" if violated else "allow",
            reason=(
                "staging_probe_detected" if violated else "policy_clear"
            ),
            evaluated_guardrails=tuple(
                guardrail.name for guardrail in request.guardrails
            ),
            transformed_payload=(
                REWRITTEN_INPUT if request.stage == "input" else SAFE_OUTPUT
            ),
            security_assessments=assessments,
        )


def deterministic_model_output(messages: Sequence[Mapping[str, Any]]) -> str:
    user_content = next(
        (
            message.get("content")
            for message in reversed(messages)
            if message.get("role") == "user"
        ),
        "",
    )
    return SAFE_OUTPUT if REWRITTEN_INPUT in str(user_content) else LEAKED_OUTPUT


class _DeterministicModelHandler(BaseHTTPRequestHandler):
    server_version = "orchestra-security-proof-model"

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        if self.path != "/v1/chat/completions":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("content-length", "0"))
            if length < 1 or length > 1024 * 1024:
                raise ValueError("invalid request length")
            body = json.loads(self.rfile.read(length))
            messages = body["messages"]
            if not isinstance(messages, list):
                raise ValueError("messages must be a list")
            response = {
                "model": str(body.get("model") or "security-proof-model"),
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {
                            "content": deterministic_model_output(messages)
                        },
                    }
                ],
            }
            encoded = json.dumps(response, separators=(",", ":")).encode(
                "utf-8"
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            self.send_error(400)
            return
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, _format: str, *args: object) -> None:
        return


def _validate_proof_config(config: RuntimeHostConfig, model_port: int) -> None:
    if config.environment != "staging":
        raise RuntimeHostError("security_proof_staging_only")
    expected_gateway = "http://127.0.0.1:%d" % model_port
    if config.model_gateway_base_url.rstrip("/") != expected_gateway:
        raise RuntimeHostError("security_proof_model_gateway_mismatch")
    if config.security_decision_base_url is None:
        raise RuntimeHostError("security_proof_decision_delivery_required")
    if config.receipt_base_url is None:
        raise RuntimeHostError("security_proof_receipt_delivery_required")


def main() -> int:
    if os.environ.get(PROOF_ENABLE_ENV) != PROOF_ENABLE_VALUE:
        raise RuntimeHostError("security_proof_not_enabled")
    config_path = Path(
        os.environ.get(
            "PROMETA_RUNTIME_CONFIG", "/etc/prometa-runtime/config.json"
        )
    )
    model_port = int(os.environ.get(PROOF_MODEL_PORT_ENV, "8091"))
    if not 1 <= model_port <= 65535:
        raise RuntimeHostError("security_proof_model_port_invalid")
    config = load_runtime_host_config(config_path)
    _validate_proof_config(config, model_port)
    model_server = ThreadingHTTPServer(
        ("127.0.0.1", model_port), _DeterministicModelHandler
    )
    model_thread = threading.Thread(
        target=model_server.serve_forever,
        name="orchestra-security-proof-model",
        daemon=True,
    )
    model_thread.start()
    try:
        host, _ = build_reference_runtime_host(
            config,
            environment=os.environ,
            guard_evaluator=DeterministicSecurityProofGuard(),
        )
        serve_reference_runtime_host(
            host,
            port=int(os.environ.get("PORT", "8080")),
        )
    finally:
        model_server.shutdown()
        model_server.server_close()
        model_thread.join(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
