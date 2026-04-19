"""Smoke-test the OpenAI Agents SDK auto-instrumentation hook.

Wraps the OpenAI Agents SDK ``Agent.run`` / ``Runner.run`` entry points
so each agent invocation becomes a Prometa span tagged with
``gen_ai.framework=openai-agents``.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prometa import Prometa  # noqa: E402
from prometa.integrations import openai_agents as prometa_openai_agents  # noqa: E402

prometa = Prometa(
    endpoint=os.environ.get(
        "PROMETA_ENDPOINT",
        "http://localhost:3000/api/v2/otlp/v1/traces",
    ),
    api_key=os.environ.get("PROMETA_API_KEY"),
    solution_id="sol_demo",
    agent_name="openai-agents-demo",
)

installed = prometa_openai_agents.install()
print(f"openai-agents instrumentation installed: {installed}")


async def main() -> None:
    if not installed:
        print(
            "openai-agents isn't installed — skipping auto-instrumented run.\n"
            "Install with `pip install openai-agents` and re-run with an OPENAI_API_KEY."
        )
        with prometa._span("agent", "openai-agents-fallback-demo"):
            await asyncio.sleep(0)
        prometa.flush()
        return

    # Late import so the script runs without the optional dep.
    from agents import Agent, Runner  # type: ignore

    triage = Agent(
        name="Triage",
        instructions="Classify the user request as billing | support | other.",
    )
    result = await Runner.run(triage, "I was overcharged on invoice #4521")
    print("agent result:", result.final_output)
    prometa.flush()


if __name__ == "__main__":
    asyncio.run(main())
