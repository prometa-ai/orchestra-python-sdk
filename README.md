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
- `gen_ai.conversation.id` — when the producer opts into session grouping (see below)
- Parent/child relationships across async/sync calls
- Errors → span status `error` plus `error.message`

## Grouping traces into conversational sessions

A chat-style agent typically produces many traces per user conversation
(one per message turn, one per background tool call, one per retry).
The platform's **Session Explorer** groups all traces sharing a session
id into one row, with aggregated cost, tokens, duration, and a
side-by-side conversation timeline that spans the whole session.

To opt in, stamp the session id on the current span — anywhere inside
a `@prometa.workflow / .agent / .tool / .task` block:

```python
from prometa import set_session_id

@prometa.workflow(name="handle-turn")
async def handle_turn(conversation_id: str, user_message: str):
    set_session_id(conversation_id)   # any opaque key your app uses
    # ... do the work; nested spans inherit automatically
```

Or, when the id is known at decorator time, use the `session_id=` kwarg:

```python
@prometa.workflow(name="handle-turn", session_id=conversation_id)
async def handle_turn(...): ...
```

Either form writes the OTel-standard `gen_ai.conversation.id` attribute
onto the span; the platform ingest reads it (and accepts `session.id`
or `prometa.session_id` as fallbacks for non-Prometa producers) and
propagates it onto every span + the trace row at write time. Nothing
else needs to be configured.

**Use opaque ids, not user-identifying values.** The session id is
indexed and visible to anyone with `traces:read` permission. Don't
stuff emails, names, or PII in there.

**Session retention** mirrors trace retention (currently 365 days).
Long-running sessions touching old + new traces will appear truncated
once the oldest member trace ages out — acceptable for chat workloads,
flag if you have multi-week audit needs.

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

### How the trace "Conversation" panel populates

The Prometa trace UI renders a **Conversation** panel that derives turns
directly from the `gen_ai.prompt` / `gen_ai.completion` span attributes
emitted by the integrations on this page. Each LLM span becomes a
`user` turn (the latest user message extracted from `gen_ai.prompt`)
followed by an `agent` turn (the completion). Token counts and
timestamps come straight off the span — nothing else needs to be wired
up on the platform side.

The **After preprocessing** vs **Raw** toggle is also rendered, but
both modes show the same text until the platform's PII redactor /
policy gate ships and starts populating the `prometa.conversation_turns`
table. When that lands, the panel will switch back to reading the
processed-vs-raw pair from that table; the SDK contract does not
change.

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
