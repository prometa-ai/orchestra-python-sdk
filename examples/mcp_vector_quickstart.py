"""Combined MCP + vector-DB instrumentation example.

Two of the most common "edge" surfaces in an agentic stack:
  - MCP tool calls (``mcp.client.session.ClientSession.call_tool``)
  - Vector-DB retrievals (Pinecone, Chroma, Weaviate)

Both install hooks return False when the underlying package is missing,
so this example runs cleanly out of the box and serves as an installer
sanity check.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from prometa import Prometa  # noqa: E402
from prometa.integrations import mcp as prometa_mcp  # noqa: E402
from prometa.integrations import vector as prometa_vector  # noqa: E402

prometa = Prometa(
    endpoint=os.environ.get(
        "PROMETA_ENDPOINT",
        "http://localhost:3000/api/v2/otlp/v1/traces",
    ),
    api_key=os.environ.get("PROMETA_API_KEY"),
    solution_id="sol_demo",
    agent_name="mcp-vector-demo-agent",
)

mcp_installed = prometa_mcp.install()
vector_results = prometa_vector.install_all()

print(f"mcp instrumentation installed:    {mcp_installed}")
print("vector instrumentation:")
for name, ok in vector_results.items():
    flag = "✓" if ok else "✗ (package not installed)"
    print(f"  - {name}: {flag}")


def main() -> None:
    # The example always emits one manual span so flush() has something
    # to send and the OTLP endpoint can be smoke-tested even without any
    # of the optional deps.
    with prometa._span("workflow", "mcp-vector-installer-check") as span:
        span.attributes["mcp_installed"] = str(mcp_installed)
        span.attributes["vectors_installed"] = ",".join(
            n for n, ok in vector_results.items() if ok
        )
    prometa.flush()
    print("emitted 1 installer-check span")


if __name__ == "__main__":
    main()
