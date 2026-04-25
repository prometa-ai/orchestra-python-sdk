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

## LLM client auto-instrumentation

For traces to show **token usage, cost, and prompt/completion text**,
opt in to the per-client patcher matching the LLM library you use.
Without this, spans render but the cost panel reads `$0.000` and the
trace UI has no prompt/completion to display.

```python
from prometa import Prometa
from prometa.integrations import openai as prometa_openai
from prometa.integrations import anthropic as prometa_anthropic
from prometa.integrations import google as prometa_google

Prometa(endpoint=..., agent_name="my-agent")

# Call install() once at startup. Each returns False (no-op) if the
# corresponding library isn't installed — so it's safe to call all three.
prometa_openai.install()
prometa_anthropic.install()
prometa_google.install()
```

Once installed, every `client.chat.completions.create(...)`,
`client.messages.create(...)`, and `client.models.generate_content(...)`
call (sync, async, **and streaming**) emits a child span carrying:

- `gen_ai.system` (`openai` / `anthropic` / `google`)
- `gen_ai.request.model`, `temperature`, `top_p`, `max_tokens`
- `gen_ai.usage.input_tokens` / `gen_ai.usage.output_tokens` → drives cost
- `gen_ai.prompt` (truncated JSON of input messages)
- `gen_ai.completion` (truncated assistant reply)
- `gen_ai.response.id`, `gen_ai.response.model`, `gen_ai.response.finish_reasons`

Streaming spans propagate context properly — any `@prometa.tool` /
`@prometa.agent` invoked from inside the stream consumer nests under
the LLM span, not under whatever was active when `.create()` returned.

### A note on the trace "Conversation" panel

The Prometa trace UI renders a **Conversation** panel that reads from a
dedicated `prometa.conversation_turns` ClickHouse table. As of platform
version compatible with this SDK release, **no ingest path writes to
that table** — so the panel will show *"No conversation turns recorded
for this trace."* even with auto-instrumentation enabled.

The prompt/completion text is still captured as `gen_ai.prompt` /
`gen_ai.completion` span attributes and is queryable from the Spans
table, the trace export API, and any downstream LLM-as-a-Judge / replay
tooling. The Conversation panel itself will start populating once the
platform team wires the OTLP ingest handler to fan-out span attributes
into `prometa.conversation_turns` (tracked separately).

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
