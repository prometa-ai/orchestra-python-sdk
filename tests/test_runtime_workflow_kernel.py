from __future__ import annotations

import asyncio
import copy
import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from prometa.runtime import (
    CAPABILITY_WORKFLOW_CONTEXT_RESOLVE,
    CAPABILITY_WORKFLOW_DECISION_EMIT,
    CAPABILITY_WORKFLOW_POLICY_EVALUATE,
    CAPABILITY_WORKFLOW_STATE_PERSIST,
    InMemoryWorkflowDecisionEmitter,
    InMemoryWorkflowStateStore,
    ModelInvocationResponse,
    ModelToolCall,
    ReferenceRuntimeHost,
    RuntimeExecutionError,
    RuntimeExecutionPolicy,
    ToolInvocationResult,
    VerifiedWorkflowContext,
    WorkflowExecutionContext,
    canonical_workflow_digest,
    parse_workflow_ontology_artifact,
)
from tests.test_runtime_kernel import (
    RecordingToolBroker,
    SequenceModelAdapter,
    _admitted,
    _kernel,
    _with_tool,
)
from tests.test_runtime_workflow_ontology import _artifact


WORKFLOW_CAPABILITIES = frozenset(
    {
        CAPABILITY_WORKFLOW_POLICY_EVALUATE,
        CAPABILITY_WORKFLOW_CONTEXT_RESOLVE,
        CAPABILITY_WORKFLOW_STATE_PERSIST,
        CAPABILITY_WORKFLOW_DECISION_EMIT,
    }
)


def _runtime_artifact():
    artifact = copy.deepcopy(_artifact())
    spec = artifact["compiledPolicy"]["spec"]
    task = spec["tasks"][0]
    task["kind"] = "tool"
    task["tool"] = {
        "server": "Orders",
        "name": "orders.lookup",
        "risk": "read",
        "sideEffect": False,
    }
    spec["controls"][0]["idempotency"]["keyPath"] = "request.orderId"
    ontology_digest = canonical_workflow_digest(spec)
    projection = {
        key: spec[key]
        for key in (
            "allowedConditionPaths",
            "states",
            "tasks",
            "transitions",
            "facts",
            "evidenceRequirements",
            "obligations",
            "controls",
        )
    }
    policy_digest = canonical_workflow_digest(projection)
    artifact["ontologyDigest"] = ontology_digest
    artifact["policyDigest"] = policy_digest
    artifact["compiledPolicy"]["ontologyDigest"] = ontology_digest
    artifact["compiledPolicy"]["policyDigest"] = policy_digest
    return parse_workflow_ontology_artifact(artifact)


def _workflow_admitted(*, side_effects="read-only"):
    _, admitted = _admitted()
    admitted = _with_tool(admitted)
    tool = replace(admitted.config.tools[0], side_effects=side_effects)
    contract = replace(
        admitted.config.contract,
        required_capabilities=frozenset(
            {*admitted.config.contract.required_capabilities, *WORKFLOW_CAPABILITIES}
        ),
    )
    config = replace(
        admitted.config,
        tools=(tool,),
        workflow_ontologies=(_runtime_artifact(),),
        contract=contract,
    )
    return replace(admitted, config=config)


def _context():
    return WorkflowExecutionContext(
        ontology_id="ontology-1",
        version=1,
        instance_id="invoice-1",
        actor_ref="actor:opaque",
    )


def _verified(*, observed_at=None):
    observed_at = observed_at or datetime.now(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )
    return VerifiedWorkflowContext(
        current_state="matched",
        state_version=0,
        actor_role_ids=("invoice_agent",),
        purpose="order lookup",
        facts={
            "variance_percent": {
                "value": 1.5,
                "observedAt": observed_at,
                "authoritative": True,
                "source": "procurement",
            }
        },
    )


class StaticResolver:
    def __init__(self, context=None):
        self.context = context or _verified()
        self.requests = []

    async def resolve(self, request):
        self.requests.append(request)
        return self.context


class StaticPostconditions:
    def __init__(self, context=None):
        self.context = context or _verified()
        self.requests = []

    async def validate(self, request):
        self.requests.append(request)
        return self.context


class FailingWorkflowEmitter:
    def emit(self, decision):
        raise OSError("decision outbox unavailable")


class FailAfterFirstWorkflowEmitter:
    def __init__(self):
        self.decisions = []

    def emit(self, decision):
        self.decisions.append(decision)
        if len(self.decisions) > 1:
            raise OSError("decision outbox unavailable")


class TimeoutBroker:
    async def invoke(self, request):
        await asyncio.sleep(1)
        return ToolInvocationResult(output={"status": "late"})


def _adapter():
    return SequenceModelAdapter(
        ModelInvocationResponse(
            content=None,
            tool_calls=(
                ModelToolCall(
                    call_id="call-1",
                    name="orders.lookup",
                    arguments={"orderId": "post-1"},
                ),
            ),
        ),
        ModelInvocationResponse(content='{"answer":"done"}'),
    )


def test_workflow_evaluates_before_tool_validates_after_and_commits_once():
    admitted = _workflow_admitted()
    resolver = StaticResolver()
    postconditions = StaticPostconditions()
    state = InMemoryWorkflowStateStore()
    state.seed(_context(), "matched", 0)
    decisions = InMemoryWorkflowDecisionEmitter()
    broker = RecordingToolBroker()
    kernel, _ = _kernel(
        admitted,
        _adapter(),
        tool_broker=broker,
        workflow_context_resolver=resolver,
        workflow_state_store=state,
        workflow_decision_emitter=decisions,
        workflow_postcondition_validator=postconditions,
    )

    result = asyncio.run(
        kernel.execute(
            {"question": "lookup"},
            request_id="request-workflow-1",
            workflow_context=_context(),
        )
    )

    assert result.tool_calls == 1
    assert len(broker.requests) == 1
    assert len(state.ledger) == 1
    assert state.ledger[0].next_state == "ready"
    assert [item.recommended_outcome for item in decisions.decisions] == [
        "allow",
        "allow",
    ]
    assert all(
        item.fact_set_digest.startswith("sha256:") for item in decisions.decisions
    )
    assert resolver.requests[0].workflow.actor_ref == "actor:opaque"
    assert not hasattr(resolver.requests[0].workflow, "role_ids")


def test_missing_context_stale_fact_and_emitter_failure_block_before_tool():
    admitted = _workflow_admitted()
    broker = RecordingToolBroker()
    components = {
        "tool_broker": broker,
        "workflow_context_resolver": StaticResolver(),
        "workflow_state_store": InMemoryWorkflowStateStore(),
        "workflow_decision_emitter": InMemoryWorkflowDecisionEmitter(),
        "workflow_postcondition_validator": StaticPostconditions(),
    }
    kernel, _ = _kernel(admitted, _adapter(), **components)
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute({"question": "lookup"}))
    assert caught.value.code == "workflow_context_required"
    assert broker.requests == []

    stale = replace(
        _verified(),
        facts={
            "variance_percent": {
                "value": 1.5,
                "observedAt": "2020-01-01T00:00:00Z",
                "authoritative": True,
                "source": "procurement",
            }
        },
    )
    kernel, _ = _kernel(
        admitted,
        _adapter(),
        **{**components, "workflow_context_resolver": StaticResolver(stale)},
    )
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute({"question": "lookup"}, workflow_context=_context()))
    assert caught.value.code == "workflow_policy_indeterminate"
    assert broker.requests == []

    kernel, _ = _kernel(
        admitted,
        _adapter(),
        **{
            **components,
            "workflow_decision_emitter": FailingWorkflowEmitter(),
        },
    )
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute({"question": "lookup"}, workflow_context=_context()))
    assert caught.value.code == "workflow_decision_emit_failed"
    assert broker.requests == []


def test_side_effect_timeout_is_quarantined_and_never_committed():
    admitted = _workflow_admitted(side_effects="write")
    state = InMemoryWorkflowStateStore()
    state.seed(_context(), "matched", 0)
    decisions = InMemoryWorkflowDecisionEmitter()
    kernel, _ = _kernel(
        admitted,
        _adapter(),
        tool_broker=TimeoutBroker(),
        execution_policy=RuntimeExecutionPolicy(tool_timeout_seconds=0.001),
        workflow_context_resolver=StaticResolver(),
        workflow_state_store=state,
        workflow_decision_emitter=decisions,
        workflow_postcondition_validator=StaticPostconditions(),
    )

    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(
            kernel.execute(
                {"question": "lookup"},
                request_id="request-timeout",
                workflow_context=_context(),
            )
        )
    assert caught.value.code == "tool_timeout"
    assert len(state.ledger) == 1
    assert state.ledger[0].reason_code == "tool_outcome_indeterminate"
    assert decisions.decisions[-1].recommended_outcome == "indeterminate"
    assert decisions.decisions[-1].applied_outcome == "deny"


def test_quarantine_is_durable_even_when_indeterminate_evidence_emission_fails():
    admitted = _workflow_admitted(side_effects="write")
    state = InMemoryWorkflowStateStore()
    state.seed(_context(), "matched", 0)
    decisions = FailAfterFirstWorkflowEmitter()
    kernel, _ = _kernel(
        admitted,
        _adapter(),
        tool_broker=TimeoutBroker(),
        execution_policy=RuntimeExecutionPolicy(tool_timeout_seconds=0.001),
        workflow_context_resolver=StaticResolver(),
        workflow_state_store=state,
        workflow_decision_emitter=decisions,
        workflow_postcondition_validator=StaticPostconditions(),
    )

    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(
            kernel.execute(
                {"question": "lookup"},
                request_id="request-timeout-emitter-failure",
                workflow_context=_context(),
            )
        )
    assert caught.value.code == "workflow_decision_emit_failed"
    assert len(state.ledger) == 1
    assert state.ledger[0].reason_code == "tool_outcome_indeterminate"


def test_observe_mode_logs_postcondition_drift_without_committing_or_blocking():
    admitted = _workflow_admitted()
    observed_artifact = replace(admitted.config.workflow_ontologies[0], mode="observe")
    admitted = replace(
        admitted,
        config=replace(
            admitted.config,
            workflow_ontologies=(observed_artifact,),
        ),
    )
    postcondition_context = replace(
        _verified(),
        facts={
            "variance_percent": {
                "value": 4,
                "observedAt": datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "authoritative": True,
                "source": "procurement",
            }
        },
    )
    state = InMemoryWorkflowStateStore()
    state.seed(_context(), "matched", 0)
    decisions = InMemoryWorkflowDecisionEmitter()
    kernel, _ = _kernel(
        admitted,
        _adapter(),
        tool_broker=RecordingToolBroker(),
        workflow_context_resolver=StaticResolver(),
        workflow_state_store=state,
        workflow_decision_emitter=decisions,
        workflow_postcondition_validator=StaticPostconditions(postcondition_context),
    )

    result = asyncio.run(
        kernel.execute(
            {"question": "lookup"},
            request_id="request-observe-postcondition",
            workflow_context=_context(),
        )
    )
    assert result.tool_calls == 1
    assert state.ledger == []
    assert decisions.decisions[-1].recommended_outcome == "deny"
    assert decisions.decisions[-1].applied_outcome == "allow"


def test_runtime_host_accepts_only_opaque_workflow_context() -> None:
    admitted = _workflow_admitted()
    kernel, _ = _kernel(
        admitted,
        _adapter(),
        tool_broker=RecordingToolBroker(),
        workflow_context_resolver=StaticResolver(),
        workflow_state_store=InMemoryWorkflowStateStore(),
        workflow_decision_emitter=InMemoryWorkflowDecisionEmitter(),
        workflow_postcondition_validator=StaticPostconditions(),
    )
    token = "workflow-runtime-token-0123456789abcdef"
    host = ReferenceRuntimeHost(kernel, api_token=token)
    request = {
        "requestId": "request-workflow-context",
        "input": {"question": "lookup"},
        "workflowContext": {
            "workflowId": "ontology-1",
            "version": 1,
            "instanceId": "invoice-1",
            "actorRef": "actor:opaque",
            "actorRoles": ["finance_admin"],
            "facts": {"variance": 0},
        },
    }
    try:
        response = host.handle(
            "POST",
            "/v1/runtime/execute",
            {
                "authorization": "Bearer %s" % token,
                "content-type": "application/json",
            },
            json.dumps(request).encode("utf-8"),
        )
    finally:
        host.close()
    assert response.status == 400
    assert response.body == {"error": {"code": "invalid_workflow_context"}}
