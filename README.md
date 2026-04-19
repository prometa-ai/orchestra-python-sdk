# prometa-sdk (Python)

Official Python SDK for the **Prometa Agentic Lifecycle Intelligence Platform**.

Wraps OpenTelemetry GenAI semantic conventions with `@prometa` decorators that
automatically emit lifecycle metadata (`solution_id`, `stage`, `agent`) to your
Prometa instance via OTLP/JSON.

## Install

```bash
pip install prometa-sdk
```

## Quick start

```python
import asyncio
from prometa import Prometa

prometa = Prometa(
    endpoint="https://prometa.example.com/api/v2/otlp/v1/traces",
    api_key="prm_live_...",
    solution_id="sol_abc123",
    agent_name="customer-support",
    stage="production",
)

@prometa.workflow(name="handle-ticket")
async def handle_ticket(ticket_id: str) -> str:
    @prometa.agent(name="classifier")
    async def classify() -> str:
        return "billing"

    @prometa.tool(name="kb-search")
    async def kb_search(q: str) -> list[str]:
        return ["doc1", "doc2"]

    category = await classify()
    results = await kb_search(category)
    return f"resolved {ticket_id} via {results}"

asyncio.run(handle_ticket("T-1234"))
prometa.flush()
```

## What gets captured

Each decorated function emits a span with:

- `prometa.kind` — `workflow | agent | tool | task`
- `prometa.solution_id`, `prometa.stage`
- `gen_ai.agent.name`, `gen_ai.agent.id`
- Parent/child relationships across async/sync calls
- Errors → span status `error` plus `error.message`

## Configuration

| Param | Env var | Default |
|---|---|---|
| `endpoint` | — | required |
| `api_key` | `PROMETA_API_KEY` | none |
| `solution_id` | — | none |
| `agent_name` | — | `"prometa-agent"` |
| `stage` | — | `"development"` |
| `flush_interval_seconds` | — | `2.0` |

## License

Apache-2.0
