"""Smoke-test the LangChain auto-instrumentation hook.

Runs without LangChain installed (the install() call returns False) so
the example is safe to commit. With ``pip install langchain-core``
installed, every Runnable.invoke call becomes a Prometa span
automatically.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prometa import Prometa  # noqa: E402
from prometa.integrations import langchain as prometa_langchain  # noqa: E402

prometa = Prometa(
    endpoint=os.environ.get(
        "PROMETA_ENDPOINT",
        "http://localhost:3000/api/v2/otlp/v1/traces",
    ),
    api_key=os.environ.get("PROMETA_API_KEY"),
    solution_id="sol_demo",
    agent_name="langchain-demo-agent",
)

installed = prometa_langchain.install()
print(f"langchain instrumentation installed: {installed}")


async def main() -> None:
    if not installed:
        print(
            "LangChain isn't installed — skipping the auto-instrumented run.\n"
            "Install with `pip install langchain-core` and re-run."
        )
        # Still ship a manual span so the example exercises the SDK end-to-end.
        with prometa._span("workflow", "langchain-fallback-demo"):
            await asyncio.sleep(0)
        prometa.flush()
        return

    from langchain_core.runnables import RunnableLambda  # type: ignore

    pipeline = (
        RunnableLambda(lambda s: s.upper())
        | RunnableLambda(lambda s: s + "!")
    )
    result = pipeline.invoke("hello prometa")
    print("pipeline output:", result)
    prometa.flush()


if __name__ == "__main__":
    asyncio.run(main())
