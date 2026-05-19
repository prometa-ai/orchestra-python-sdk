# prometa-sdk (Python)

[![PyPI version](https://img.shields.io/pypi/v/prometa-sdk.svg)](https://pypi.org/project/prometa-sdk/)
[![Python](https://img.shields.io/pypi/pyversions/prometa-sdk.svg)](https://pypi.org/project/prometa-sdk/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Official Python SDK for the **Prometa Agentic Lifecycle Intelligence Platform**.

Wraps OpenTelemetry GenAI semantic conventions with `@prometa` decorators that
automatically emit lifecycle metadata to your Prometa instance via OTLP/JSON.
Three families of helpers ship in the SDK today:

- **Lifecycle decorators** — `@prometa.workflow / .agent / .tool / .task`
  wrap any sync/async function and emit a span carrying `solution_id`,
  `stage`, `agent.name`, kind, and parent/child relationships.
- **Correlation-chain setters** — `set_customer_id`, `set_user_id`,
  `set_conversation_id`, `set_request_model`, `set_tool_name`. Light up
  the platform's canonical correlation chain
  (`org:sol:agent:tool:cus:user::session:trace:span`) so registry / AML
  / lineage readers can join across the full identity prefix. Optional
  but unlocks the richer end-to-end view.
- **AML v0.4 instrumentation contract** — 16 helpers
  (`pii_filter`, `guardrail`, `memory_read`, `record_retry_attempt`,
  `confidence_score`, `schema_validate`, `model_route`,
  `sentiment_classify`, …) that emit the spans the platform's AML
  scoring engine consumes to score agents against the 41-feature catalog.

## Install

```bash
pip install prometa-sdk
```

Current source version: **0.7.1**. Release history is on
[PyPI](https://pypi.org/project/prometa-sdk/#history).

**Repository:** [`prometa-ai/orchestra-python-sdk`](https://github.com/prometa-ai/orchestra-python-sdk) — canonical source. Releases publish from GitHub Actions via OIDC Trusted Publishing on `v*` tag push (see [`.github/workflows/publish.yml`](.github/workflows/publish.yml) and the [`Release`](.github/workflows/release.yml) one-click workflow). Older docs may still mention `sdks/python/` in the platform monorepo; that path is obsolete for Python.

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
- `gen_ai.agent.name`
- `gen_ai.agent.id` — only when you explicitly pin one; otherwise the platform auto-registers the Agent from `solution_id` + `agent_name`
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

## Correlation-chain helpers (v0.5.0+)

The platform's correlation-id resolver consumes five optional OTLP
attributes to materialise its canonical chain end-to-end. Setting them
is purely additive — without them, the unset chain segments stay
empty but the platform still works; with them, every reader on the
platform side (registry, AML scoring, incident lineage, annotation
chain queries) joins by a single canonical address.

```python
from prometa import (
    Prometa,
    set_customer_id,    # → prometa.customer_id
    set_user_id,        # → gen_ai.user.id (+ prometa.user.id fallback)
    set_conversation_id,# → gen_ai.conversation.id (alias of set_session_id)
    set_request_model,  # → gen_ai.request.model
    set_tool_name,      # → prometa.tool_name (on tool-typed spans)
)

prometa = Prometa(
    endpoint="https://prometa.example.com/api/v2/otlp/v1/traces",
    api_key="prm_live_...",
    solution_id="sol_billing",
    agent_name="declarai-assistant",
    customer_id="cus_org_wide_default",   # org-wide default; overridable per-span
)

@prometa.workflow(name="handle-ticket")
def handle(ticket):
    # Per-span override of customer_id wins over the constructor
    # default for this span AND every nested span (parent-attribute
    # inheritance in the span builder).
    set_customer_id(ticket.customer_external_id)
    set_user_id(ticket.agent_email)
    set_conversation_id(ticket.thread_id)

    @prometa.tool(name="search-kb")
    def lookup():
        set_tool_name("knowledge-base-search")   # auto-registers Tool entity in PG
        ...
```

| Helper | OTLP key | Platform-side effect |
|---|---|---|
| `set_customer_id` | `prometa.customer_id` | Validated against `Organization.customerNamespace` regex at ingest; bridges Prometa telemetry to your CRM / data warehouse |
| `set_user_id` | `gen_ai.user.id` + `prometa.user.id` | End-user attribution; lights up the user segment of the chain |
| `set_conversation_id` | `gen_ai.conversation.id` | Auto-registers a Session row in Postgres; equivalent to `set_session_id` |
| `set_request_model` | `gen_ai.request.model` | Cost rollup keys on this; LLM-instrumentation libs usually set it automatically |
| `set_tool_name` | `prometa.tool_name` | Auto-registers a Tool row per `(orgId, solutionId, name)` triple on first sighting |

All five helpers follow the same contract as `set_session_id`:
synchronous, no-op outside an active span context (returns `False`),
empty value pops the attribute. See the platform-side design at
[`resources/correlation/correlation-id-design.md`](https://github.com/caglarsubas/agent-hook-v2/blob/main/resources/correlation/correlation-id-design.md)
for the full canonical-chain grammar.

### Stable Agent IDs

`agent_id` is optional. By default the SDK emits `solution_id` and
`agent_name`, then the platform mirrors the Tool registration model:
on first sighting it auto-registers the Agent row for the
`(orgId, solutionId, agentName)` tuple and attaches the canonical
Agent ID during ingest.

You can still pin an ID by passing `agent_id="..."` to `Prometa(...)`
or setting `PROMETA_AGENT_ID`. When pinned, the SDK includes
`gen_ai.agent.id` on resource and span attributes. When absent, the
SDK deliberately omits `gen_ai.agent.id`; it does not generate a
random per-process fallback.

### Agent names — always set them

`agent_name` is the customer-owned half of the
`(orgId, solutionId, agent_name)` tuple the platform's Agent registry
keys on. Two apps in the same solution that share the same
`agent_name` collapse into a single Agent row — every downstream
metric (latency, error rate, PAMI, cost) then fans across the wrong
population.

Resolution precedence:

1. Explicit `agent_name="..."` kwarg to `Prometa(...)`.
2. `PROMETA_AGENT_NAME` environment variable.
3. Literal fallback `"prometa-agent"`, **emitted with a `UserWarning`**
   at startup so the collision risk is visible in your logs the
   moment you run an unconfigured app.

```bash
# Production
export PROMETA_AGENT_NAME=declarai-assistant
```

```python
# Or per-instance
prometa = Prometa(endpoint=..., agent_name="declarai-assistant")
```

The fallback warning is intentional: silent registry collisions
were the most-reported "AML score shows 0" symptom before this
warning landed. If you genuinely want the literal name
`"prometa-agent"`, pass it explicitly (`agent_name="prometa-agent"`)
— the warning fires only on the unset path.

## AML v0.4 instrumentation contract

The SDK ships 16 helpers that emit the spans the platform's AML
scoring engine consumes to score agents against its 41-feature
catalog. The full catalog lives at
[`resources/aml/phase-0/catalog.yaml`](https://github.com/caglarsubas/agent-hook-v2/blob/main/resources/aml/phase-0/catalog.yaml)
on the platform; the SDK-side primitives are:

```python
from prometa import (
    # Safety / governance (A1-A8)
    pii_filter, guardrail, prompt_render, auth_check, consent_check,
    # Knowledge & memory (B1-B5)
    cache_lookup, memory_read, memory_write, retrieval_query,
    # Reasoning (C2-C5)
    plan_generate, confidence_score, schema_validate, sentiment_classify,
    # Orchestration & proactivity (E1-E6)
    event_trigger, reviewer_invoke,
    record_retry_attempt, record_circuit_breaker_state,
    # Observability (F1)
    model_route,
)

with guardrail("ethical", raw_input=user_query) as g:
    v = my_classifier.check(user_query)
    g.verdict("block" if v.harmful else "pass", confidence=v.score)

with pii_filter("input", raw_input=text) as pii:
    cleaned, matches = redactor.scrub(text)
    pii.result(matches_found=len(matches),
               match_categories=[m.kind for m in matches])

prometa.raw_channel.enable()   # dual-channel raw capture (opt-in)
```

Each helper is a context manager (or a synchronous record call for
fire-and-forget events) that emits a typed span with the attribute
shape the AML detectors expect. Calling them from inside an active
`@prometa.workflow / .agent / .tool` decorator nests the AML spans
under the parent — no extra wiring needed.

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
directly from the span attributes emitted by the integrations on this
page. For each LLM span:

- The user turn shows `gen_ai.prompt.user` — the latest `role: "user"`
  message, pre-extracted by the SDK from the `messages` / `contents`
  array at instrumentation time.
- The agent turn shows `gen_ai.completion` — the assistant reply.

Token counts and timestamps come straight off the span. Nothing else
needs to be wired up on the platform side.

`gen_ai.prompt` (the full messages-array JSON) is also captured for
debugging — that's what downstream judge / replay tooling reads when
it needs the complete prompt context, including system instructions
and history. The Conversation panel intentionally surfaces only
`gen_ai.prompt.user` to keep the chat view readable; the full payload
is one click away on the span detail.

The **After preprocessing** vs **Raw** toggle is also rendered, but
both modes show the same text until the platform's PII redactor /
policy gate ships and starts populating the `prometa.conversation_turns`
table. When that lands, the panel will switch back to reading the
processed-vs-raw pair from that table; the SDK contract does not
change.

## OpenLLMetry bridge (optional)

Prometa can also use Traceloop's OpenLLMetry instrumentors as the
first-choice auto-instrumentation layer. OpenLLMetry is Apache-2.0 and
its instrumentors are standard OpenTelemetry instrumentations, so the
SDK keeps them optional and bridges their finished OTel spans back into
Prometa's existing OTLP/JSON shipper.

```bash
pip install "prometa-sdk[openllmetry]"
```

```python
from prometa import Prometa
from prometa.integrations import openllmetry

Prometa(endpoint=..., api_key=..., solution_id=..., agent_name="my-agent")

result = openllmetry.install()
# {'openai': True, 'anthropic': True, 'langchain': True,
#  'chromadb': True, 'pinecone': True}
```

By default this attempts OpenLLMetry for OpenAI, Anthropic, LangChain /
LangGraph, Chroma, and Pinecone. If an OpenLLMetry package or target
library is missing, `fallback=True` uses Prometa's native wrappers for
the same target where one exists. The bridge also maps OpenLLMetry's
newer `gen_ai.input.messages` / `gen_ai.output.messages` attributes
onto Prometa's existing `gen_ai.prompt`, `gen_ai.prompt.user`, and
`gen_ai.completion` fields so current trace UI behavior stays stable.

For broader OpenLLMetry coverage, install:

```bash
pip install "prometa-sdk[openllmetry-all]"
```

Then pass the extra targets explicitly:

```python
openllmetry.install(
    targets=[
        "openai", "anthropic", "langchain", "chromadb", "pinecone",
        "bedrock", "cohere", "haystack", "llamaindex",
    ]
)
```

**Migration note:** do not install both `openllmetry.install()` and the
matching native `prometa.integrations.openai.install()` /
`anthropic.install()` wrappers for the same process unless you are
intentionally comparing span output; double-patching a client can emit
duplicate spans. Existing customers can stay on the native wrappers and
migrate target-by-target when ready.

## Reliability & retry semantics

The SDK ships traces over OTLP/JSON with **at-least-once** delivery: a
background thread flushes the in-memory span buffer every
`flush_interval_seconds` (default `2.0`), and on any send failure
(network blip, timeout, slow server response) the spans are
re-buffered and retried on the next flush.

**The platform deduplicates by id at the storage layer.**
`prometa.spans` and `prometa.traces` are backed by ClickHouse's
`ReplacingMergeTree` (or `SharedReplacingMergeTree` on ClickHouse
Cloud), keyed by `(trace_id, span_id)` and `(org_id, trace_id)`
respectively. Any number of duplicate sends of the same span collapse
to a single row during background merges; user-facing read paths
(trace explorer, session explorer, conversation panel, cost panels)
use `SELECT … FINAL` to enforce dedup at read time too. Cost and
token aggregates are not inflated by retries.

**Net: use the default `flush_interval_seconds=2.0`** even on
long-running requests (RAG pipelines, multi-round tool loops, chat
turns spanning tens of seconds). No consumer-side workaround needed.

If you previously raised the interval (e.g. to `120.0`) to dodge
platform-side double-counting in the cost / conversation panels, you
can revert to the default. The platform-side dedup landed in the
release alongside this SDK version (see CHANGELOG.md).

## Configuration

| Param | Env var | Default |
|---|---|---|
| `endpoint` | — | required |
| `api_key` | `PROMETA_API_KEY` | none |
| `solution_id` | — | none |
| `agent_name` | — | `"prometa-agent"` |
| `agent_id` | `PROMETA_AGENT_ID` | none — when omitted, the SDK leaves `gen_ai.agent.id` absent and the platform auto-registers/attaches the canonical Agent ID from `(orgId, solutionId, agentName)` |
| `stage` | — | `"development"` |
| `customer_id` | — | none — org-wide default for `prometa.customer_id` |
| `flush_interval_seconds` | — | `2.0` |
| `timeout_seconds` | — | `5.0` |

## Architectural fit

Prometa's stack is a multi-language SDK family plus a single platform.
This SDK is the Python edge of that family; sister bindings ship for
[Node.js](https://github.com/prometa-ai/orchestra-node-sdk) (`prometa-sdk`)
and [Java](https://github.com/prometa-ai/orchestra-java-sdk) (`io.prometa:prometa-sdk`).
All three emit the same OTLP attribute shape so the platform's
correlation-id resolver materialises the same canonical chain
regardless of which language the agent is written in.

The architectural picture, end-to-end:

```
┌──────────────┐                           ┌──────────────────────────┐
│  Your agent  │   OTLP/JSON (this SDK)    │  Prometa platform        │
│  (Python)    │ ─────────────────────────►│  /api/v2/otlp/v1/traces  │
└──────────────┘                           │  ┌──────────────────┐    │
                                           │  │ correlation      │    │
       ▼ Spans carry:                      │  │ resolver         │    │
                                           │  │ (PG-side)        │    │
   `prometa.solution_id`                   │  └────────┬─────────┘    │
   `gen_ai.agent.name`                     │           │              │
   `prometa.tool_name`        (optional)   │  canonical agent_id/etc  │
   `prometa.customer_id`      (optional)   │           ▼              │
   `gen_ai.conversation.id`   (optional)   │  ┌──────────────────┐    │
   `gen_ai.user.id`           (optional)   │  │ ClickHouse       │    │
   `gen_ai.request.model`     (optional)   │  │ (telemetry)      │    │
   AML v0.4 helper spans      (optional)   │  └──────────────────┘    │
                                           └──────────────────────────┘
```

Once the canonical ids land in ClickHouse, the platform's readers
(registry, AML scoring engine, incidents, annotations, workflow runs)
all join on the same chain. The end-to-end design lives in
[`resources/correlation/correlation-id-design.md`](https://github.com/caglarsubas/agent-hook-v2/blob/main/resources/correlation/correlation-id-design.md).

**Technology dependencies**

- Python ≥ 3.9
- `urllib` (standard library) for OTLP POST
- No required third-party deps; LLM auto-instrumentation hooks are
  opt-in and only run when the corresponding library is installed.
- Optional OpenLLMetry bridge extras require Python ≥ 3.10 because the
  current OpenLLMetry packages do.

## Contributing

Use a **`feat/...`** branch for each change set, open a PR to **`main`**, and integrate **only** via GitHub’s PR merge (do not push `main` directly). Details: [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache-2.0
