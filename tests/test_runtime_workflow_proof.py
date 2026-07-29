"""Tests for the explicitly enabled, staging-only workflow policy proof."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from prometa.runtime.host import RuntimeHostError
from prometa.runtime.kernel import RuntimeExecutionError
from prometa.runtime.mcp import (
    McpServerConfig,
    McpTransportCredentials,
    McpTransportError,
)
from prometa.runtime.workflow_ontology import (
    VerifiedWorkflowContext,
    WorkflowContextRequest,
    WorkflowExecutionContext,
    WorkflowPostconditionRequest,
)
from prometa.runtime.workflow_proof import (
    DeterministicSapTransport,
    PostgresWorkflowProofContextResolver,
    WorkflowProofPostconditions,
    _scenario,
    _validate_proof_config,
    load_proof_approval,
)


NOW = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)


def _approval(
    *,
    approved_at: datetime | None = None,
    expires_at: datetime | None = None,
) -> str:
    return json.dumps(
        {
            "reference": "approval:workflow-uat-001",
            "actorRef": "actor:finance-approver",
            "approvedAt": (approved_at or NOW - timedelta(minutes=1))
            .isoformat()
            .replace("+00:00", "Z"),
            "expiresAt": (expires_at or NOW + timedelta(minutes=10))
            .isoformat()
            .replace("+00:00", "Z"),
        }
    )


def _context_request() -> WorkflowContextRequest:
    return WorkflowContextRequest(
        request_id="request-1",
        workflow=WorkflowExecutionContext(
            ontology_id="workflow-1",
            version=1,
            instance_id="invoice-1",
            actor_ref="actor:invoice-agent",
        ),
        task_id="post_to_sap",
        transition_id="post_to_posted",
        request_attributes={},
    )


def _verified_context() -> VerifiedWorkflowContext:
    return VerifiedWorkflowContext(
        current_state="ready_to_post",
        state_version=0,
        actor_role_ids=("invoice_agent",),
        purpose="staging invoice posting proof",
        facts={},
    )


def _sap_server() -> McpServerConfig:
    return McpServerConfig(
        name="sap",
        connection_id="mcp-sap-workflow-uat",
        transport="streamable-http",
        environment="staging",
        endpoint="http://127.0.0.1:8092/mcp",
        auth_mode="none",
        scopes=(),
        risk_level="medium",
        allow_insecure_http=True,
    )


def test_approval_is_strict_bounded_and_minimized() -> None:
    approval = load_proof_approval(_approval(), now=NOW)

    assert approval == {
        "reference": "approval:workflow-uat-001",
        "actorRef": "actor:finance-approver",
        "roleIds": ["tenant_approver"],
        "approvedAt": "2026-07-29T11:59:00Z",
        "expiresAt": "2026-07-29T12:10:00Z",
    }

    with pytest.raises(RuntimeHostError) as expired:
        load_proof_approval(
            _approval(expires_at=NOW),
            now=NOW,
        )
    assert expired.value.code == "workflow_proof_approval_expired"

    with pytest.raises(RuntimeHostError) as overlong:
        load_proof_approval(
            _approval(
                approved_at=NOW - timedelta(minutes=1),
                expires_at=NOW + timedelta(hours=2),
            ),
            now=NOW,
        )
    assert overlong.value.code == "workflow_proof_approval_expired"

    invalid = json.loads(_approval())
    invalid["businessFacts"] = {"invoice": "raw payload forbidden"}
    with pytest.raises(RuntimeHostError) as unknown_field:
        load_proof_approval(json.dumps(invalid), now=NOW)
    assert unknown_field.value.code == "workflow_proof_approval_invalid"


@pytest.mark.parametrize(
    ("messages", "expected"),
    [
        ([{"role": "user", "content": "post invoice"}], "invoice-uat-001"),
        ([{"role": "user", "content": "simulate-timeout"}], "timeout"),
        (
            [{"role": "user", "content": "simulate-postcondition-fail"}],
            "postcondition-fail",
        ),
    ],
)
def test_model_scenario_is_explicit(messages: object, expected: str) -> None:
    assert _scenario(messages) == expected


def test_transport_succeeds_without_calling_sap() -> None:
    output = asyncio.run(
        DeterministicSapTransport().call_tool(
            _sap_server(),
            "post_invoice",
            {
                "invoiceId": "invoice-uat-001",
                "idempotencyKey": "posting:invoice-uat-001",
            },
            McpTransportCredentials(),
            {"prometa.io/idempotency-key": "posting:invoice-uat-001"},
        )
    )

    assert output["status"] == "posted"
    assert output["receiptRef"].startswith("sap-proof:")


def test_transport_marks_timeout_as_unknown_and_rejects_bad_binding() -> None:
    with pytest.raises(McpTransportError) as timeout:
        asyncio.run(
            DeterministicSapTransport().call_tool(
                _sap_server(),
                "post_invoice",
                {"invoiceId": "timeout", "idempotencyKey": "posting:timeout"},
                McpTransportCredentials(),
                {"prometa.io/idempotency-key": "posting:timeout"},
            )
        )
    assert timeout.value.outcome_unknown is True

    with pytest.raises(McpTransportError) as bad_binding:
        asyncio.run(
            DeterministicSapTransport().call_tool(
                _sap_server(),
                "delete_invoice",
                {"invoiceId": "invoice-uat-001"},
                McpTransportCredentials(),
                {"prometa.io/idempotency-key": "posting:invoice-uat-001"},
            )
        )
    assert bad_binding.value.outcome_unknown is False


def test_postconditions_add_only_authoritative_fact_and_references() -> None:
    context = asyncio.run(
        WorkflowProofPostconditions().validate(
            WorkflowPostconditionRequest(
                context_request=_context_request(),
                prior_context=_verified_context(),
                proposed_state="posted",
                tool_audit_reference="audit:tool-1",
                tool_result={
                    "status": "posted",
                    "receiptRef": "sap-proof:abc123",
                },
            )
        )
    )

    assert context.facts["posting_status"]["value"] == "posted"
    assert context.evidence_references == (
        "posting_receipt",
        "sap-proof:abc123",
    )

    with pytest.raises(RuntimeExecutionError) as invalid:
        asyncio.run(
            WorkflowProofPostconditions().validate(
                WorkflowPostconditionRequest(
                    context_request=_context_request(),
                    prior_context=_verified_context(),
                    proposed_state="posted",
                    tool_audit_reference="audit:tool-2",
                    tool_result={"status": "unknown"},
                )
            )
        )
    assert invalid.value.code == "workflow_proof_postcondition_invalid"


def test_context_resolver_defaults_and_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    approval = load_proof_approval(_approval(), now=NOW)
    resolver = PostgresWorkflowProofContextResolver(
        "postgresql://tenant-runtime",
        tenant_id="tenant-1",
        runtime_id="runtime-1",
        approval=approval,
    )
    monkeypatch.setattr(resolver, "_state", lambda _request: ("ready_to_post", 0))

    context = asyncio.run(resolver.resolve(_context_request()))

    assert context.current_state == "ready_to_post"
    assert context.state_version == 0
    assert context.facts["variance_percent"]["authoritative"] is True
    assert context.approvals == (approval,)

    def unavailable(_request: WorkflowContextRequest) -> tuple[str, int]:
        raise RuntimeExecutionError("workflow_context_store_unavailable")

    monkeypatch.setattr(resolver, "_state", unavailable)
    with pytest.raises(RuntimeExecutionError) as unavailable_error:
        asyncio.run(resolver.resolve(_context_request()))
    assert unavailable_error.value.code == "workflow_context_store_unavailable"


def test_proof_config_requires_staging_local_model_and_durable_delivery() -> None:
    config = SimpleNamespace(
        environment="staging",
        model_gateway_base_url="http://127.0.0.1:8091",
        workflow_decision_base_url="https://orchestra.example",
        receipt_base_url="https://orchestra.example",
        mcp_broker=object(),
    )

    _validate_proof_config(config, 8091)  # type: ignore[arg-type]
    for field, value, code in (
        ("environment", "prod", "workflow_proof_staging_only"),
        (
            "model_gateway_base_url",
            "https://model.example",
            "workflow_proof_model_gateway_mismatch",
        ),
        (
            "workflow_decision_base_url",
            None,
            "workflow_proof_decision_delivery_required",
        ),
        ("receipt_base_url", None, "workflow_proof_receipt_delivery_required"),
        ("mcp_broker", None, "workflow_proof_mcp_broker_required"),
    ):
        with pytest.raises(RuntimeHostError) as caught:
            _validate_proof_config(
                SimpleNamespace(**{**vars(config), field: value}),
                8091,
            )  # type: ignore[arg-type]
        assert caught.value.code == code
