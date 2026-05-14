"""prometa-sdk — official Python SDK for the Prometa Agentic Lifecycle
Intelligence Platform.

Wraps OpenTelemetry GenAI semantic conventions with @prometa decorators
that automatically emit lifecycle metadata (solution_id, stage, agent name)
to the Prometa OTLP ingest endpoint.

Quick start:

    from prometa import Prometa

    prometa = Prometa(
        endpoint="https://prometa.example.com/api/v2/otlp/v1/traces",
        api_key="prm_live_...",
        solution_id="sol_abc123",
        stage="production",
    )

    @prometa.workflow(name="customer-support")
    async def handle_ticket(ticket_id: str):
        @prometa.agent(name="classifier")
        async def classify():
            return await llm.classify(ticket.description)

        return await classify()

v0.4 AML instrumentation contract (preview — Phase 2 skeleton):

    # Guardrail check (A2 ethical / A3 prompt injection)
    with prometa.guardrail("ethical", raw_input=user_query) as g:
        v = my_classifier.check(user_query)
        g.verdict("block" if v.harmful else "pass", confidence=v.score)

    # PII filter (A1)
    with prometa.pii_filter("input", raw_input=text) as pii:
        cleaned, matches = redactor.scrub(text)
        pii.result(matches_found=len(matches),
                   match_categories=[m.kind for m in matches])

    # Memory read (B3 / B4 / C6 / E3 / E4)
    with prometa.memory_read("profile", key=f"user:{uid}") as m:
        rec = profile_store.get(uid)
        m.hit(source_record_id=rec.id) if rec else m.miss()

    # Retry attempt (E6)
    prometa.record_retry_attempt(
        attempt_number=2, backoff_ms=1000,
        idempotency_key=idem, outcome="success",
    )

    # Dual-channel raw capture (off by default — opt in at startup):
    prometa.raw_channel.enable()

See https://github.com/prometa-ai/agent-hook-v2/tree/main/resources/aml/phase-0
for the 41-feature AML catalog and the full SDK contract this is built against.
"""

from . import _raw_channel as raw_channel
from .client import Prometa
from .decorators import workflow, agent, tool, task
from .budget import TokenBudget, BudgetExceededError
from .session import set_session_id, get_session_id
from .refs import (
    set_input_ref,
    set_output_ref,
    get_input_ref,
    get_output_ref,
    current_span_id,
)

# AML v0.4 instrumentation helpers — all 16 primitives from the contract
# bundled at `resources/aml/phase-0/instrumentation-spec.yaml` in
# agent-hook-v2. The AML scoring engine consumes these spans to score
# agents against the 41-feature catalog.
from .guardrails import guardrail, pii_filter
from .memory import memory_read, memory_write
from .resilience import record_retry_attempt, record_circuit_breaker_state
from .prompt import prompt_render
from .authz import auth_check, consent_check
from .retrieval import cache_lookup, retrieval_query
from .reasoning import plan_generate, confidence_score, schema_validate
from .reviewer import reviewer_invoke
from .events import event_trigger
from .routing import model_route
from .sentiment import sentiment_classify

__version__ = "0.4.0"
__all__ = [
    "Prometa",
    "workflow",
    "agent",
    "tool",
    "task",
    "TokenBudget",
    "BudgetExceededError",
    "set_session_id",
    "get_session_id",
    "set_input_ref",
    "set_output_ref",
    "get_input_ref",
    "get_output_ref",
    "current_span_id",
    # v0.4 AML helpers — dual-channel toggle
    "raw_channel",
    # v0.4 AML helpers — safety / governance (A1-A8)
    "pii_filter",
    "guardrail",
    "prompt_render",
    "auth_check",
    "consent_check",
    # v0.4 AML helpers — knowledge & memory (B1-B5)
    "cache_lookup",
    "memory_read",
    "memory_write",
    "retrieval_query",
    # v0.4 AML helpers — reasoning (C2-C5)
    "plan_generate",
    "confidence_score",
    "schema_validate",
    "sentiment_classify",
    # v0.4 AML helpers — orchestration & proactivity (E1-E6)
    "event_trigger",
    "reviewer_invoke",
    "record_retry_attempt",
    "record_circuit_breaker_state",
    # v0.4 AML helpers — observability (F1)
    "model_route",
]
