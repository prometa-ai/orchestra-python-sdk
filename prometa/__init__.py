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
"""

from .client import Prometa
from .decorators import workflow, agent, tool, task
from .budget import TokenBudget, BudgetExceededError

__version__ = "0.1.0"
__all__ = [
    "Prometa",
    "workflow",
    "agent",
    "tool",
    "task",
    "TokenBudget",
    "BudgetExceededError",
]
