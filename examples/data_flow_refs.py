"""Demonstrates explicit data-flow refs between sibling spans.

The classic case: an LLM emits a tool_call -> the agent runtime
dispatches a tool span. Without input_ref, the trace shows two sibling
spans under the same parent and temporal proximity is the only hint
that one fed the other. With input_ref, the tool span declares "I was
called with the output of <llm span_id>" and the platform's
Causal-context block surfaces that link as a clickable row.

Run a local Prometa instance on http://localhost:3000, then::

    PROMETA_API_KEY=prm_live_xxx python examples/data_flow_refs.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prometa import (  # noqa: E402
    Prometa,
    current_span_id,
    set_input_ref,
)

prometa = Prometa(
    endpoint=os.environ.get(
        "PROMETA_ENDPOINT",
        "http://localhost:3000/api/v2/otlp/v1/traces",
    ),
    api_key=os.environ.get("PROMETA_API_KEY"),
    solution_id="sol_demo",
    agent_name="data-flow-refs-example",
    stage="development",
)


# Capture-the-span pattern: the LLM call snapshots its own span id into
# a closed-over dict so the orchestrator can hand it to the downstream
# tool. A contextvar / queue / state object would work equivalently.
_pipeline_state: dict[str, str] = {}


@prometa.agent(name="openai.chat")
async def call_llm(prompt: str) -> dict:
    # Snapshot the active span so the next step can reference it.
    span_id = current_span_id()
    if span_id:
        _pipeline_state["last_llm_span_id"] = span_id
    await asyncio.sleep(0.03)
    # Pretend the model asked for a tool call.
    return {"tool_call": {"name": "kb-search", "args": {"q": prompt}}}


@prometa.tool(name="kb-search")
async def kb_search(q: str) -> list[str]:
    # Declare: this tool span consumed the output of the LLM span. The
    # platform reads this from prometa.input_ref, persists it as a
    # dedicated column, and the trace UI renders an "Input from ->
    # openai.chat" row in the Causal-context block.
    upstream = _pipeline_state.get("last_llm_span_id")
    if upstream:
        set_input_ref(upstream)
    await asyncio.sleep(0.05)
    return [f"doc-{q}-1", f"doc-{q}-2"]


@prometa.workflow(name="handle-ticket")
async def handle_ticket(ticket_id: str, body: str) -> dict:
    decision = await call_llm(body)
    docs = await kb_search(decision["tool_call"]["args"]["q"])
    return {"ticket_id": ticket_id, "docs": docs}


async def main() -> None:
    out = await handle_ticket("T-1234", "Need help with my invoice")
    print(out)
    prometa.flush()
    print("flushed spans to", prometa.endpoint)


if __name__ == "__main__":
    asyncio.run(main())
