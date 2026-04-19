"""Smoke-test the CrewAI auto-instrumentation hook.

Same shape as ``langchain_quickstart.py``: install CrewAI to see real
spans flow; otherwise the example shows how to wire the hook safely
even without the optional dependency.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prometa import Prometa  # noqa: E402
from prometa.integrations import crewai as prometa_crewai  # noqa: E402

prometa = Prometa(
    endpoint=os.environ.get(
        "PROMETA_ENDPOINT",
        "http://localhost:3000/api/v2/otlp/v1/traces",
    ),
    api_key=os.environ.get("PROMETA_API_KEY"),
    solution_id="sol_demo",
    agent_name="crewai-demo-crew",
)

installed = prometa_crewai.install()
print(f"crewai instrumentation installed: {installed}")


def main() -> None:
    if not installed:
        print(
            "CrewAI isn't installed — skipping the auto-instrumented run.\n"
            "Install with `pip install crewai` and re-run."
        )
        # Demonstrate the manual fallback so the example still exercises the SDK.
        with prometa._span("workflow", "crewai-fallback-demo"):
            pass
        prometa.flush()
        return

    from crewai import Agent, Crew, Task  # type: ignore

    # Minimal 1-agent / 1-task crew. Real configurations bind a Bedrock,
    # OpenAI, or local-LLM client to each agent; this stub keeps the
    # example dependency-light.
    researcher = Agent(
        role="Researcher",
        goal="Summarize the prompt",
        backstory="A demo agent.",
        verbose=False,
        allow_delegation=False,
    )
    task = Task(description="Echo: hello prometa", agent=researcher)
    crew = Crew(agents=[researcher], tasks=[task], verbose=False)
    result = crew.kickoff()
    print("crew result:", result)
    prometa.flush()


if __name__ == "__main__":
    main()
