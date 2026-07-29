"""Staging-only tenant adapters for the company-workflow runtime proof.

The fixture exercises the real signed bundle, policy evaluator, PostgreSQL CAS
ledger, MCP idempotency store and asynchronous decision outbox.  It never
connects to SAP: a deterministic local transport represents the side-effect
boundary and can return success, timeout/unknown, or invalid postconditions.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import threading
from dataclasses import replace
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping

from .host import (
    RuntimeHostConfig,
    RuntimeHostError,
    build_reference_runtime_host,
    load_runtime_host_config,
    serve_reference_runtime_host,
)
from .kernel import (
    HumanEscalationDecision,
    HumanEscalationRequest,
    RuntimeExecutionError,
)
from .mcp import (
    McpServerConfig,
    McpTransportCredentials,
    McpTransportError,
)
from .workflow_ontology import (
    VerifiedWorkflowContext,
    WorkflowContextRequest,
    WorkflowPostconditionRequest,
)


PROOF_ENABLE_ENV = "PROMETA_RUNTIME_WORKFLOW_PROOF"
PROOF_ENABLE_VALUE = "enabled"
PROOF_MODEL_PORT_ENV = "PROMETA_WORKFLOW_PROOF_MODEL_PORT"
PROOF_APPROVAL_ENV = "PROMETA_WORKFLOW_PROOF_APPROVAL_JSON"
PROOF_INITIAL_STATE = "ready_to_post"
PROOF_PURPOSE = "staging invoice posting proof"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _instant(value: Any, code: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise RuntimeHostError(code)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise RuntimeHostError(code) from None
    if parsed.tzinfo is None:
        raise RuntimeHostError(code)
    return parsed.astimezone(timezone.utc)


def load_proof_approval(
    value: str, *, now: datetime | None = None
) -> Mapping[str, Any]:
    """Parse one tenant-provisioned, bounded approval without inventing it."""

    try:
        document = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        raise RuntimeHostError("workflow_proof_approval_invalid") from None
    if not isinstance(document, dict) or set(document) != {
        "reference",
        "actorRef",
        "approvedAt",
        "expiresAt",
    }:
        raise RuntimeHostError("workflow_proof_approval_invalid")
    reference = document["reference"]
    actor_ref = document["actorRef"]
    if (
        not isinstance(reference, str)
        or not reference
        or len(reference) > 512
        or reference != reference.strip()
        or not isinstance(actor_ref, str)
        or not actor_ref
        or len(actor_ref) > 512
        or actor_ref != actor_ref.strip()
    ):
        raise RuntimeHostError("workflow_proof_approval_invalid")
    approved_at = _instant(document["approvedAt"], "workflow_proof_approval_invalid")
    expires_at = _instant(document["expiresAt"], "workflow_proof_approval_invalid")
    checked_at = now or _utc_now()
    if (
        approved_at > checked_at
        or expires_at <= checked_at
        or expires_at <= approved_at
        or (expires_at - approved_at).total_seconds() > 3600
    ):
        raise RuntimeHostError("workflow_proof_approval_expired")
    return {
        "reference": reference,
        "actorRef": actor_ref,
        "roleIds": ["tenant_approver"],
        "approvedAt": approved_at.isoformat().replace("+00:00", "Z"),
        "expiresAt": expires_at.isoformat().replace("+00:00", "Z"),
    }


class PostgresWorkflowProofContextResolver:
    """Resolve minimized authoritative context from the tenant database."""

    def __init__(
        self,
        dsn: str,
        *,
        tenant_id: str,
        runtime_id: str,
        approval: Mapping[str, Any],
    ) -> None:
        self._dsn = dsn
        self._tenant_id = tenant_id
        self._runtime_id = runtime_id
        self._approval = dict(approval)

    def _state(self, request: WorkflowContextRequest) -> tuple[str, int]:
        try:
            import psycopg

            with psycopg.connect(self._dsn) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT state, state_version, quarantined
                        FROM prometa_runtime_workflow_instance
                        WHERE tenant_id = %s AND runtime_id = %s
                          AND workflow_id = %s AND workflow_version = %s
                          AND instance_id = %s
                        """,
                        (
                            self._tenant_id,
                            self._runtime_id,
                            request.workflow.ontology_id,
                            request.workflow.version,
                            request.workflow.instance_id,
                        ),
                    )
                    row = cursor.fetchone()
        except Exception as exc:
            raise RuntimeExecutionError("workflow_context_store_unavailable") from exc
        if row is None:
            return PROOF_INITIAL_STATE, 0
        if bool(row[2]):
            raise RuntimeExecutionError("workflow_instance_indeterminate")
        state = row[0]
        version = row[1]
        if not isinstance(state, str) or type(version) is not int:
            raise RuntimeExecutionError("workflow_context_store_invalid")
        return state, version

    async def resolve(self, request: WorkflowContextRequest) -> VerifiedWorkflowContext:
        state, version = await asyncio.to_thread(self._state, request)
        observed_at = (
            _utc_now().isoformat(timespec="milliseconds").replace("+00:00", "Z")
        )
        return VerifiedWorkflowContext(
            current_state=state,
            state_version=version,
            actor_role_ids=("invoice_agent",),
            purpose=PROOF_PURPOSE,
            facts={
                "invoice_valid": {
                    "value": True,
                    "observedAt": observed_at,
                    "authoritative": True,
                    "source": "invoice-system",
                },
                "po_match": {
                    "value": True,
                    "observedAt": observed_at,
                    "authoritative": True,
                    "source": "procurement-system",
                },
                "variance_percent": {
                    "value": 1.5,
                    "observedAt": observed_at,
                    "authoritative": True,
                    "source": "procurement-system",
                },
            },
            approvals=(self._approval,),
        )


class WorkflowProofPostconditions:
    """Accept only the deterministic local posting receipt."""

    async def validate(
        self, request: WorkflowPostconditionRequest
    ) -> VerifiedWorkflowContext:
        result = request.tool_result
        if (
            not isinstance(result, Mapping)
            or result.get("status") != "posted"
            or not isinstance(result.get("receiptRef"), str)
            or not result["receiptRef"].startswith("sap-proof:")
        ):
            raise RuntimeExecutionError("workflow_proof_postcondition_invalid")
        observed_at = (
            _utc_now().isoformat(timespec="milliseconds").replace("+00:00", "Z")
        )
        return replace(
            request.prior_context,
            facts={
                **request.prior_context.facts,
                "posting_status": {
                    "value": "posted",
                    "observedAt": observed_at,
                    "authoritative": True,
                    "source": "sap",
                },
            },
            evidence_references=tuple(
                sorted(
                    {
                        *request.prior_context.evidence_references,
                        "posting_receipt",
                        result["receiptRef"],
                    }
                )
            ),
        )


class WorkflowProofHumanEscalation:
    """Expose the already-provisioned tenant approval to generic tool gates."""

    def __init__(self, approval: Mapping[str, Any]) -> None:
        self._reference = str(approval["reference"])

    async def request_review(
        self, _request: HumanEscalationRequest
    ) -> HumanEscalationDecision:
        return HumanEscalationDecision(
            approved=True,
            reviewer_reference=self._reference,
            reason="tenant_provisioned_staging_approval",
        )


class DeterministicSapTransport:
    """A local MCP boundary with explicit unknown-outcome test scenarios."""

    async def call_tool(
        self,
        server: McpServerConfig,
        operation: str,
        arguments: Mapping[str, Any],
        credentials: McpTransportCredentials,
        metadata: Mapping[str, Any],
    ) -> Any:
        if (
            server.name != "sap"
            or operation != "post_invoice"
            or credentials.headers
            or credentials.environment
            or not isinstance(metadata.get("prometa.io/idempotency-key"), str)
        ):
            raise McpTransportError(
                "workflow_proof_transport_binding_invalid",
                outcome_unknown=False,
            )
        invoice_id = arguments.get("invoiceId")
        if invoice_id == "timeout":
            raise McpTransportError(
                "workflow_proof_transport_timeout",
                outcome_unknown=True,
            )
        if invoice_id == "postcondition-fail":
            return {"status": "unknown", "receiptRef": "sap-proof:invalid"}
        if not isinstance(invoice_id, str) or not invoice_id:
            raise McpTransportError(
                "workflow_proof_invoice_invalid",
                outcome_unknown=False,
            )
        receipt = hashlib.sha256(invoice_id.encode("utf-8")).hexdigest()[:24]
        return {"status": "posted", "receiptRef": "sap-proof:%s" % receipt}


def _scenario(messages: Any) -> str:
    if not isinstance(messages, list):
        raise ValueError("messages must be a list")
    content = " ".join(
        str(message.get("content", ""))
        for message in messages
        if isinstance(message, Mapping)
    )
    if "simulate-timeout" in content:
        return "timeout"
    if "simulate-postcondition-fail" in content:
        return "postcondition-fail"
    return "invoice-uat-001"


class _DeterministicWorkflowModelHandler(BaseHTTPRequestHandler):
    server_version = "orchestra-workflow-proof-model"

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
            has_tool_result = any(
                isinstance(message, Mapping) and message.get("role") == "tool"
                for message in messages
            )
            if has_tool_result:
                message = {"content": '{"status":"completed"}'}
                finish_reason = "stop"
            else:
                invoice_id = _scenario(messages)
                message = {
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call-workflow-proof",
                            "type": "function",
                            "function": {
                                "name": "post_invoice",
                                "arguments": json.dumps(
                                    {
                                        "invoiceId": invoice_id,
                                        "idempotencyKey": "posting:%s" % invoice_id,
                                    },
                                    separators=(",", ":"),
                                ),
                            },
                        }
                    ],
                }
                finish_reason = "tool_calls"
            response = {
                "model": str(body.get("model") or "workflow-proof-model"),
                "choices": [
                    {
                        "finish_reason": finish_reason,
                        "message": message,
                    }
                ],
            }
            encoded = json.dumps(response, separators=(",", ":")).encode("utf-8")
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
        raise RuntimeHostError("workflow_proof_staging_only")
    expected_gateway = "http://127.0.0.1:%d" % model_port
    if config.model_gateway_base_url.rstrip("/") != expected_gateway:
        raise RuntimeHostError("workflow_proof_model_gateway_mismatch")
    if config.workflow_decision_base_url is None:
        raise RuntimeHostError("workflow_proof_decision_delivery_required")
    if config.receipt_base_url is None:
        raise RuntimeHostError("workflow_proof_receipt_delivery_required")
    if config.mcp_broker is None:
        raise RuntimeHostError("workflow_proof_mcp_broker_required")


def main() -> int:
    if os.environ.get(PROOF_ENABLE_ENV) != PROOF_ENABLE_VALUE:
        raise RuntimeHostError("workflow_proof_not_enabled")
    config_path = Path(
        os.environ.get("PROMETA_RUNTIME_CONFIG", "/etc/prometa-runtime/config.json")
    )
    model_port = int(os.environ.get(PROOF_MODEL_PORT_ENV, "8091"))
    if not 1 <= model_port <= 65535:
        raise RuntimeHostError("workflow_proof_model_port_invalid")
    config = load_runtime_host_config(config_path)
    _validate_proof_config(config, model_port)
    approval_value = os.environ.get(PROOF_APPROVAL_ENV, "")
    approval = load_proof_approval(approval_value)
    dsn = os.environ.get(config.database_dsn_env, "").strip()
    if not dsn:
        raise RuntimeHostError("runtime_database_url_missing")
    model_server = ThreadingHTTPServer(
        ("127.0.0.1", model_port), _DeterministicWorkflowModelHandler
    )
    model_thread = threading.Thread(
        target=model_server.serve_forever,
        name="orchestra-workflow-proof-model",
        daemon=True,
    )
    model_thread.start()
    try:
        host, _ = build_reference_runtime_host(
            config,
            environment=os.environ,
            human_escalation=WorkflowProofHumanEscalation(approval),
            workflow_context_resolver=PostgresWorkflowProofContextResolver(
                dsn,
                tenant_id=config.tenant_id,
                runtime_id=config.runtime_id,
                approval=approval,
            ),
            workflow_postcondition_validator=WorkflowProofPostconditions(),
            mcp_transport_client=DeterministicSapTransport(),
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
