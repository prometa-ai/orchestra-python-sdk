"""Phase 2A execution-kernel behavior and evidence conformance tests."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("cryptography")
pytest.importorskip("jsonschema")

from prometa.runtime import (
    BASE_RUNTIME_CAPABILITIES,
    CAPABILITY_GUARD_EVALUATE,
    CAPABILITY_HUMAN_ESCALATION,
    CAPABILITY_SCHEMA_VALIDATE,
    CAPABILITY_TOOL_BROKER,
    BundleTrustEntry,
    BundleTrustStore,
    GuardDecision,
    HumanEscalationDecision,
    InMemoryAdmissionReplayStore,
    InMemoryEvidenceEmitter,
    InMemoryRuntimeStateStore,
    ModelAdapterError,
    ModelInvocationResponse,
    ModelToolCall,
    RuntimeAdmissionPolicy,
    RuntimeEvidenceEvent,
    RuntimeExecutionError,
    RuntimeExecutionPolicy,
    RuntimeGuardrail,
    RuntimeKernel,
    RuntimeTool,
    ToolInvocationResult,
    admit_runtime_release,
    available_runtime_capabilities,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "runtime-kernel-v1.json"


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _trust(value) -> BundleTrustStore:
    return BundleTrustStore(
        [
            BundleTrustEntry(
                issuer=value["issuer"],
                key_id=value["keyId"],
                public_key_spki_der_base64=value["publicKeySpkiDerBase64"],
            )
        ]
    )


def _admitted():
    vector = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    verification = vector["verification"]
    policy = RuntimeAdmissionPolicy(
        expected_org_id=verification["expectedOrgId"],
        expected_environment=verification["expectedEnvironment"],
        expected_release_id=verification["expectedReleaseId"],
        expected_deployment_id=verification["expectedDeploymentId"],
        expected_runtime=verification["expectedRuntime"],
        supported_capabilities=frozenset(
            {*BASE_RUNTIME_CAPABILITIES, CAPABILITY_SCHEMA_VALIDATE}
        ),
    )
    admitted = admit_runtime_release(
        vector["bundle"],
        vector["attestation"],
        bundle_trust_store=_trust(vector["bundleTrust"]),
        promotion_trust_store=_trust(vector["promotionTrust"]),
        replay_store=InMemoryAdmissionReplayStore(),
        policy=policy,
        now=_instant(verification["now"]),
    )
    return vector, admitted


class SequenceModelAdapter:
    def __init__(self, *items) -> None:
        self.items = list(items)
        self.requests = []

    async def invoke(self, request):
        self.requests.append(request)
        if not self.items:
            raise AssertionError("model adapter received an unexpected invocation")
        item = self.items.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class AllowGuard:
    def __init__(self, *, transformed=None, include=()) -> None:
        self.transformed = transformed
        self.include = tuple(include)
        self.requests = []

    async def evaluate(self, request):
        self.requests.append(request)
        names = self.include or tuple(guard.name for guard in request.guardrails)
        return GuardDecision(
            allowed=True,
            action="pass",
            evaluated_guardrails=names,
            transformed_payload=self.transformed,
        )


class DenyGuard:
    async def evaluate(self, request):
        return GuardDecision(
            allowed=False,
            action="block",
            reason="tenant policy",
            evaluated_guardrails=tuple(guard.name for guard in request.guardrails),
        )


class RecordingToolBroker:
    def __init__(self, output=None) -> None:
        self.output = output if output is not None else {"status": "found"}
        self.requests = []

    async def invoke(self, request):
        self.requests.append(request)
        return ToolInvocationResult(
            output=self.output,
            audit_reference="tenant-audit-1",
        )


class ApproveHuman:
    def __init__(self, approved=True) -> None:
        self.approved = approved
        self.requests = []

    async def request_review(self, request):
        self.requests.append(request)
        return HumanEscalationDecision(
            approved=self.approved,
            reviewer_reference="tenant-review-1",
        )


class FailingEmitter:
    def emit(self, event: RuntimeEvidenceEvent) -> None:
        raise OSError("tenant evidence sink unavailable")


class CompletionFailingStateStore:
    def __init__(self) -> None:
        self.calls = []

    async def save(self, request_id, state) -> None:
        self.calls.append((request_id, dict(state)))
        if state["status"] == "completed":
            raise OSError("tenant state store unavailable")


def _kernel(admitted, adapter, emitter=None, **overrides):
    values = {
        "model_adapter": adapter,
        "evidence_emitter": emitter or InMemoryEvidenceEmitter(),
        "runtime_id": "tenant-runtime-01",
        "runtime_version": "0.17.0",
    }
    values.update(overrides)
    return RuntimeKernel(admitted, **values), values["evidence_emitter"]


def _with_guard(admitted):
    guardrail = RuntimeGuardrail(
        name="PII",
        guardrail_type="pii-dlp",
        on_violation="block",
        applies_to="all",
    )
    contract = replace(
        admitted.config.contract,
        required_capabilities=frozenset(
            {*admitted.config.contract.required_capabilities, CAPABILITY_GUARD_EVALUATE}
        ),
    )
    config = replace(admitted.config, guardrails=(guardrail,), contract=contract)
    return replace(admitted, config=config)


def _with_tool(admitted, *, approval_required=False, required_guardrails=()):
    tool = RuntimeTool(
        name="Lookup order",
        source="mcp",
        operation="orders.lookup",
        input_schema={
            "type": "object",
            "properties": {"orderId": {"type": "string"}},
            "required": ["orderId"],
            "additionalProperties": False,
        },
        mcp_server="Orders",
        side_effects="read-only",
        risk_level="low",
        auth_binding="service-account",
        scopes=("orders.read",),
        approval_required=approval_required,
        required_guardrails=tuple(required_guardrails),
    )
    contract = replace(
        admitted.config.contract,
        required_capabilities=frozenset(
            {
                *admitted.config.contract.required_capabilities,
                CAPABILITY_TOOL_BROKER,
                *((CAPABILITY_GUARD_EVALUATE,) if required_guardrails else ()),
                *((CAPABILITY_HUMAN_ESCALATION,) if approval_required else ()),
            }
        ),
    )
    config = replace(admitted.config, tools=(tool,), contract=contract)
    return replace(admitted, config=config)


def test_successful_execution_validates_schemas_and_emits_joinable_identity() -> None:
    vector, admitted = _admitted()
    adapter = SequenceModelAdapter(
        ModelInvocationResponse(
            content=json.dumps(vector["sampleOutput"]),
            finish_reason="stop",
            provider_model="golden-model@sha256:test",
        )
    )
    state = InMemoryRuntimeStateStore()
    kernel, emitter = _kernel(admitted, adapter, state_store=state)

    result = asyncio.run(
        kernel.execute(vector["sampleInput"], request_id="request-golden-1")
    )

    assert result.output == vector["sampleOutput"]
    assert result.model_name == "golden-model@sha256:test"
    assert result.attempts == 1
    assert result.tool_calls == 0
    assert state.states["request-golden-1"]["status"] == "completed"
    assert adapter.requests[0].messages[0]["content"] == admitted.config.system_prompt
    names = [event.name for event in emitter.events]
    assert names == [
        "runtime.release.admitted",
        "runtime.request",
        "runtime.schema.input",
        "runtime.model.attempt",
        "runtime.model.attempt",
        "runtime.schema.output",
        "runtime.request",
    ]
    completed = emitter.events[-1]
    assert completed.outcome == "completed"
    for key in (
        "prometa.agent_id",
        "prometa.bundle.digest",
        "prometa.bundle.jti",
        "prometa.attestation.id",
        "prometa.policy.decision_id",
        "prometa.release.id",
        "prometa.deployment.id",
        "prometa.runtime.target",
        "prometa.runtime.id",
        "prometa.runtime.version",
    ):
        assert completed.attributes[key]


def test_invalid_input_and_output_fail_before_downstream_work() -> None:
    _, admitted = _admitted()
    unused = SequenceModelAdapter()
    kernel, emitter = _kernel(admitted, unused)
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute({"unexpected": True}, request_id="bad-input"))
    assert caught.value.code == "input_schema_invalid"
    assert unused.requests == []
    assert (
        emitter.events[-1].attributes["prometa.runtime.reason"]
        == "input_schema_invalid"
    )

    bad_output = SequenceModelAdapter(
        ModelInvocationResponse(content='{"wrong":"shape"}')
    )
    kernel, emitter = _kernel(admitted, bad_output)
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute({"question": "hello"}, request_id="bad-output"))
    assert caught.value.code == "output_schema_invalid"
    assert emitter.events[-1].outcome == "failed"

    duplicate_output = SequenceModelAdapter(
        ModelInvocationResponse(content='{"answer":"a","answer":"b"}')
    )
    kernel, _ = _kernel(admitted, duplicate_output)
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute({"question": "hello"}))
    assert caught.value.code == "output_json_invalid"


def test_retry_circuit_open_and_deterministic_fallback() -> None:
    _, admitted = _admitted()
    adapter = SequenceModelAdapter(
        ModelAdapterError("overloaded", retryable=True),
        ModelAdapterError("overloaded", retryable=True),
        ModelInvocationResponse(content='{"answer":"fallback"}'),
        ModelInvocationResponse(content='{"answer":"still fallback"}'),
    )
    sleeps = []

    async def no_sleep(value):
        sleeps.append(value)

    kernel, emitter = _kernel(
        admitted,
        adapter,
        execution_policy=RuntimeExecutionPolicy(
            max_attempts_per_model=2,
            fallback_model_names=("Fallback",),
            circuit_failure_threshold=2,
        ),
        sleep=no_sleep,
    )
    first = asyncio.run(kernel.execute({"question": "hello"}, request_id="fallback-1"))
    second = asyncio.run(kernel.execute({"question": "again"}, request_id="fallback-2"))

    assert first.used_fallback is True
    assert first.attempts == 3
    assert second.used_fallback is True
    assert sleeps == [0.1]
    assert [request.model.name for request in adapter.requests] == [
        "Primary",
        "Primary",
        "Fallback",
        "Fallback",
    ]
    assert any(
        event.name == "runtime.circuit_breaker" and event.outcome == "opened"
        for event in emitter.events
    )
    assert any(
        event.name == "runtime.circuit_breaker" and event.outcome == "denied"
        for event in emitter.events
    )


def test_non_retryable_model_failure_does_not_fallback() -> None:
    _, admitted = _admitted()
    adapter = SequenceModelAdapter(
        ModelAdapterError("request_rejected", retryable=False),
        ModelInvocationResponse(content='{"answer":"must not run"}'),
    )
    kernel, _ = _kernel(
        admitted,
        adapter,
        execution_policy=RuntimeExecutionPolicy(
            fallback_model_names=("Fallback",),
        ),
    )
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute({"question": "hello"}))
    assert caught.value.code == "request_rejected"
    assert len(adapter.requests) == 1


def test_embedding_model_cannot_be_selected_as_a_response_fallback() -> None:
    _, admitted = _admitted()
    embedding = replace(admitted.config.models[1], role="embedding")
    config = replace(
        admitted.config,
        models=(admitted.config.models[0], embedding),
    )
    with pytest.raises(ValueError, match="Embedding model"):
        _kernel(
            replace(admitted, config=config),
            SequenceModelAdapter(),
            execution_policy=RuntimeExecutionPolicy(
                fallback_model_names=(embedding.name,),
            ),
        )


def test_cancellation_is_persisted_and_re_raised() -> None:
    _, admitted = _admitted()

    class BlockingAdapter:
        async def invoke(self, request):
            await asyncio.Event().wait()

    state = InMemoryRuntimeStateStore()
    kernel, emitter = _kernel(admitted, BlockingAdapter(), state_store=state)

    async def scenario():
        task = asyncio.create_task(
            kernel.execute({"question": "wait"}, request_id="cancel-1")
        )
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert state.states["cancel-1"]["status"] == "cancelled"
    assert any(
        event.name == "runtime.request" and event.outcome == "cancelled"
        for event in emitter.events
    )


def test_declared_guardrails_require_complete_evidence_and_can_block() -> None:
    vector, admitted = _admitted()
    guarded = _with_guard(admitted)
    response = ModelInvocationResponse(content=json.dumps(vector["sampleOutput"]))

    incomplete = AllowGuard(include=("not-declared",))
    kernel, _ = _kernel(
        guarded,
        SequenceModelAdapter(response),
        guard_evaluator=incomplete,
    )
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute(vector["sampleInput"]))
    assert caught.value.code == "guardrail_evidence_incomplete"

    kernel, emitter = _kernel(
        guarded,
        SequenceModelAdapter(response),
        guard_evaluator=DenyGuard(),
    )
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute(vector["sampleInput"]))
    assert caught.value.code == "guard_denied"
    assert any(event.name == "runtime.guard.input" for event in emitter.events)


def test_guard_transform_is_revalidated_against_the_signed_schema() -> None:
    vector, admitted = _admitted()
    guarded = _with_guard(admitted)
    kernel, _ = _kernel(
        guarded,
        SequenceModelAdapter(
            ModelInvocationResponse(content=json.dumps(vector["sampleOutput"]))
        ),
        guard_evaluator=AllowGuard(transformed={"unexpected": True}),
    )

    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute(vector["sampleInput"]))
    assert caught.value.code == "input_schema_invalid"


def test_tool_calls_are_declared_schema_guarded_and_brokered() -> None:
    vector, admitted = _admitted()
    with_tool = _with_tool(admitted)
    adapter = SequenceModelAdapter(
        ModelInvocationResponse(
            content=None,
            tool_calls=(
                ModelToolCall(
                    call_id="call-1",
                    name="orders.lookup",
                    arguments={"orderId": "order-42"},
                ),
            ),
            finish_reason="tool_calls",
        ),
        ModelInvocationResponse(content=json.dumps(vector["sampleOutput"])),
    )
    broker = RecordingToolBroker()
    kernel, emitter = _kernel(with_tool, adapter, tool_broker=broker)
    result = asyncio.run(kernel.execute(vector["sampleInput"], request_id="tool-1"))

    assert result.tool_calls == 1
    assert result.attempts == 2
    assert broker.requests[0].arguments == {"orderId": "order-42"}
    assert adapter.requests[1].messages[-1]["role"] == "tool"
    assert any(
        event.name == "runtime.tool.call" and event.outcome == "completed"
        for event in emitter.events
    )


def test_server_required_tool_guards_cannot_be_silently_skipped() -> None:
    vector, admitted = _admitted()
    governed = _with_tool(admitted, required_guardrails=("tenant_risk_gate",))
    call = ModelInvocationResponse(
        content=None,
        tool_calls=(
            ModelToolCall(
                call_id="call-governed",
                name="orders.lookup",
                arguments={"orderId": "order-42"},
            ),
        ),
    )
    broker = RecordingToolBroker()

    with pytest.raises(RuntimeExecutionError) as caught:
        _kernel(governed, SequenceModelAdapter(call), tool_broker=broker)
    assert caught.value.code == "runtime_component_missing"

    guard = AllowGuard(include=("tenant_risk_gate",))
    kernel, _ = _kernel(
        governed,
        SequenceModelAdapter(
            call,
            ModelInvocationResponse(content=json.dumps(vector["sampleOutput"])),
        ),
        tool_broker=broker,
        guard_evaluator=guard,
    )
    assert asyncio.run(kernel.execute(vector["sampleInput"])).tool_calls == 1
    assert any(request.stage == "tool" for request in guard.requests)


def test_duplicate_tool_calls_and_retry_after_tool_are_denied() -> None:
    vector, admitted = _admitted()
    with_tool = _with_tool(admitted)
    duplicate = ModelInvocationResponse(
        content=None,
        tool_calls=(
            ModelToolCall("call-1", "orders.lookup", {"orderId": "one"}),
            ModelToolCall("call-1", "orders.lookup", {"orderId": "two"}),
        ),
    )
    broker = RecordingToolBroker()
    kernel, _ = _kernel(with_tool, SequenceModelAdapter(duplicate), tool_broker=broker)
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute(vector["sampleInput"]))
    assert caught.value.code == "duplicate_or_invalid_tool_call"
    assert broker.requests == []

    first_call = ModelInvocationResponse(
        content=None,
        tool_calls=(ModelToolCall("call-2", "orders.lookup", {"orderId": "order-42"}),),
    )
    adapter = SequenceModelAdapter(
        first_call,
        ModelAdapterError("gateway_unavailable", retryable=True),
        ModelInvocationResponse(content=json.dumps(vector["sampleOutput"])),
    )
    kernel, _ = _kernel(
        with_tool,
        adapter,
        tool_broker=broker,
        execution_policy=RuntimeExecutionPolicy(max_attempts_per_model=2),
    )
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute(vector["sampleInput"]))
    assert caught.value.code == "retry_after_tool_denied"
    assert len(broker.requests) == 1
    assert len(adapter.requests) == 2


def test_tool_approval_is_tenant_owned_and_fail_closed() -> None:
    vector, admitted = _admitted()
    with_tool = _with_tool(admitted, approval_required=True)
    call = ModelInvocationResponse(
        content=None,
        tool_calls=(
            ModelToolCall(
                call_id="call-1",
                name="orders.lookup",
                arguments={"orderId": "order-42"},
            ),
        ),
    )
    broker = RecordingToolBroker()
    with pytest.raises(RuntimeExecutionError) as caught:
        _kernel(
            with_tool,
            SequenceModelAdapter(call),
            tool_broker=broker,
        )
    assert caught.value.code == "runtime_component_missing"
    assert broker.requests == []

    # Supplying the tenant-owned review adapter satisfies capability admission.
    denying_reviewer = ApproveHuman(approved=False)
    kernel, _ = _kernel(
        with_tool,
        SequenceModelAdapter(call),
        tool_broker=broker,
        human_escalation=denying_reviewer,
    )
    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute(vector["sampleInput"]))
    assert caught.value.code == "human_review_denied"
    assert broker.requests == []

    reviewer = ApproveHuman()
    kernel, _ = _kernel(
        with_tool,
        SequenceModelAdapter(
            call,
            ModelInvocationResponse(content=json.dumps(vector["sampleOutput"])),
        ),
        tool_broker=broker,
        human_escalation=reviewer,
    )
    assert asyncio.run(kernel.execute(vector["sampleInput"])).tool_calls == 1
    assert reviewer.requests[0].stage == "tool"
    assert reviewer.requests[0].payload == {"orderId": "order-42"}


def test_human_approval_guard_requires_an_explicit_tenant_decision() -> None:
    vector, admitted = _admitted()
    guardrail = RuntimeGuardrail(
        name="Input approval",
        guardrail_type="human-approval",
        on_violation="escalate",
        applies_to="input",
    )
    contract = replace(
        admitted.config.contract,
        required_capabilities=frozenset(
            {
                *admitted.config.contract.required_capabilities,
                CAPABILITY_GUARD_EVALUATE,
                CAPABILITY_HUMAN_ESCALATION,
            }
        ),
    )
    guarded = replace(
        admitted,
        config=replace(admitted.config, guardrails=(guardrail,), contract=contract),
    )
    reviewer = ApproveHuman()
    kernel, _ = _kernel(
        guarded,
        SequenceModelAdapter(
            ModelInvocationResponse(content=json.dumps(vector["sampleOutput"]))
        ),
        guard_evaluator=AllowGuard(),
        human_escalation=reviewer,
    )
    asyncio.run(kernel.execute(vector["sampleInput"]))
    assert [request.stage for request in reviewer.requests] == ["input"]
    assert reviewer.requests[0].payload == vector["sampleInput"]


def test_state_failure_is_not_misclassified_or_retried_as_a_model_failure() -> None:
    vector, admitted = _admitted()
    adapter = SequenceModelAdapter(
        ModelInvocationResponse(content=json.dumps(vector["sampleOutput"])),
        ModelInvocationResponse(content=json.dumps(vector["sampleOutput"])),
    )
    state = CompletionFailingStateStore()
    kernel, emitter = _kernel(admitted, adapter, state_store=state)

    with pytest.raises(RuntimeExecutionError) as caught:
        asyncio.run(kernel.execute(vector["sampleInput"]))
    assert caught.value.code == "state_store_failed"
    assert len(adapter.requests) == 1
    assert (
        emitter.events[-1].attributes["prometa.runtime.reason"] == "state_store_failed"
    )


def test_missing_components_and_evidence_sink_fail_before_model_work() -> None:
    _, admitted = _admitted()
    with_tool = _with_tool(admitted)
    adapter = SequenceModelAdapter()
    with pytest.raises(RuntimeExecutionError) as caught:
        _kernel(with_tool, adapter)
    assert caught.value.code == "runtime_component_missing"

    with pytest.raises(RuntimeExecutionError) as caught:
        _kernel(admitted, adapter, emitter=FailingEmitter())
    assert caught.value.code == "evidence_emit_failed"
    assert adapter.requests == []


def test_available_capabilities_reflect_only_configured_active_components() -> None:
    base = available_runtime_capabilities()
    assert BASE_RUNTIME_CAPABILITIES.issubset(base)
    assert CAPABILITY_SCHEMA_VALIDATE in base
    assert CAPABILITY_TOOL_BROKER not in base
    assert CAPABILITY_GUARD_EVALUATE not in base
    assert CAPABILITY_HUMAN_ESCALATION not in base

    active = available_runtime_capabilities(
        guard_evaluator=AllowGuard(),
        tool_broker=RecordingToolBroker(),
        human_escalation=ApproveHuman(),
    )
    assert CAPABILITY_TOOL_BROKER in active
    assert CAPABILITY_GUARD_EVALUATE in active
    assert CAPABILITY_HUMAN_ESCALATION in active
