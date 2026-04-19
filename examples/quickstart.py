"""End-to-end smoke test for the Python SDK.

Run a local Prometa instance on http://localhost:3000 then:

    PROMETA_API_KEY=prm_live_xxx python examples/quickstart.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prometa import Prometa  # noqa: E402

prometa = Prometa(
    endpoint=os.environ.get(
        "PROMETA_ENDPOINT",
        "http://localhost:3000/api/v2/otlp/v1/traces",
    ),
    api_key=os.environ.get("PROMETA_API_KEY"),
    solution_id="sol_demo",
    agent_name="quickstart-agent",
    stage="development",
)


@prometa.tool(name="kb-search")
async def kb_search(query: str) -> list[str]:
    await asyncio.sleep(0.05)
    return [f"doc-{query}-1", f"doc-{query}-2"]


@prometa.agent(name="classifier")
async def classify(text: str) -> str:
    await asyncio.sleep(0.02)
    return "billing" if "invoice" in text.lower() else "general"


@prometa.workflow(name="handle-ticket")
async def handle_ticket(ticket_id: str, body: str) -> dict:
    category = await classify(body)
    results = await kb_search(category)
    return {"ticket_id": ticket_id, "category": category, "kb": results}


async def main() -> None:
    out = await handle_ticket("T-1234", "Need help with my invoice")
    print(out)
    prometa.flush()
    print("flushed spans to", prometa.endpoint)


if __name__ == "__main__":
    asyncio.run(main())
