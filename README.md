# prometa-sdk (Python)

[![PyPI version](https://img.shields.io/pypi/v/prometa-sdk.svg)](https://pypi.org/project/prometa-sdk/)
[![Python](https://img.shields.io/pypi/pyversions/prometa-sdk.svg)](https://pypi.org/project/prometa-sdk/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Official Python SDK for the **Prometa Agentic Lifecycle Intelligence Platform**.

Wraps OpenTelemetry GenAI semantic conventions with `@prometa` decorators that
automatically emit lifecycle metadata to your Prometa instance via OTLP/JSON.
The SDK ships telemetry surfaces that make agent behavior queryable, evaluable,
and joinable on the platform.

The package has two clearly separated surfaces:

1. **Observability (default install)** — decorators, correlation helpers, AML
   instrumentation primitives, LLM-client patchers, and framework
   auto-instrumentation. Zero required third-party dependencies; everything
   optional degrades to a no-op when the target library is absent.
2. **Tenant runtime kit (optional extras)** — `prometa.runtime`, a
   fail-closed execution kernel for signed Agent Builder bundles, plus a
   governed MCP tool broker, PostgreSQL durability, a reference host, and a
   conformance runner. Nothing here is imported, or adds a dependency, unless
   you install the extra and import `prometa.runtime` explicitly.

### Observability

- **Lifecycle decorators** — `@prometa.workflow / .agent / .tool / .task`
  wrap any sync/async function and emit a span carrying `solution_id`,
  `stage`, `agent.name`, kind, and parent/child relationships.
- **Correlation-chain setters** — `set_customer_id`, `set_user_id`,
  `set_conversation_id`, `set_request_model`, `set_tool_name`. Light up
  the platform's canonical correlation chain
  (`org:sol:agent:tool:cus:user::session:trace:span`) so registry /
  AML (Agentic Maturity Leveling) / lineage readers can join across the
  full identity prefix. Optional
  but unlocks the richer end-to-end view.
- **Custom span attributes** — `set_attribute` / `set_attributes` stamp
  scalar attributes such as `your.integration.*` on the active span. Prometa
  preserves non-promoted attributes in span metadata for governance and
  evaluation workflows.
- **Assistant intent labels** — `set_assistant_intent` /
  `set_assistant_intent_from_text` stamp deterministic Prometa intent
  labels before LLM, tool, or action work.
- **User feedback feeding** — `set_user_feedback` /
  `record_user_feedback` collect thumbs-up / thumbs-down, 1-5 star
  ratings, and open-text comments as generic `prometa.feedback.*`
  telemetry for platform ingestion.
- **Data-flow refs** — `set_input_ref` / `set_output_ref` /
  `current_span_id` declare "whose output did this span consume?" between
  sibling spans, which parent/child edges alone cannot express.
- **Token budgets** — `TokenBudget` enforces a per-process sliding-window
  token cap in `hard` (raises) or `soft` (warns) mode.
- **LLM client auto-instrumentation** — opt-in patchers for OpenAI
  (chat completions, responses, **embeddings**), Anthropic, and Google
  GenAI capture prompts, completions, token usage, and cost signals.
- **Framework auto-instrumentation** — opt-in patchers for LangChain,
  LangGraph, CrewAI, Semantic Kernel, the OpenAI Agents SDK, MCP client
  sessions, and Pinecone / Chroma / Weaviate.
- **OpenLLMetry bridge** — optionally use Traceloop's OpenTelemetry
  instrumentors as the auto-instrumentation layer and bridge their finished
  spans into Prometa's OTLP/JSON shipper.
- **AQL (Agentic Quality Leveling) trace metadata** — lifecycle,
  correlation, refs, intent, feedback, prompt, completion, usage, and
  model attributes give Prometa's AQL / PrometaQL query and evaluation
  layer stable fields to filter, aggregate, replay, and judge traces.
- **AML (Agentic Maturity Leveling) v0.4 instrumentation contract** —
  16 helpers (`pii_filter`, `guardrail`, `memory_read`,
  `record_retry_attempt`, `confidence_score`, `schema_validate`,
  `model_route`, `sentiment_classify`, …) that emit the spans the
  platform's AML scoring engine consumes to score agents against the
  41-feature catalog.

### Tenant runtime kit (optional)

- **Signed-release admission** — bundle schema v1–v3 and runtime contract
  v1–v3, verified locally against a tenant-controlled trust store, with
  replay reservation, capability negotiation, and digest binding.
- **Execution kernel** — bounded model/tool timeouts, retries, backoff,
  circuit breaking, cancellation, and deterministic signed fallback, emitting
  payload-free evidence.
- **Company Workflow Ontology (contract v3)** — a deterministic closed-AST
  workflow policy evaluator with tenant-owned fact/approval resolution,
  pre-tool authorization, postcondition validation, and compare-and-set state
  transitions.
- **Governed MCP tool broker** — official stdio and Streamable HTTP
  transports behind exact egress allowlists, late-bound credentials, signed
  scope/risk intersection, and payload-free audit.
- **PostgreSQL durability** — schema v8 covering admission replay, request
  state, release activation and cache, task lifecycle, MCP idempotency and
  audit, the workflow ledger, and the receipt / security / workflow decision
  outboxes.
- **Reference host and conformance runner** — `prometa-runtime-host` plus
  `prometa-runtime-conformance` for adapter-level contract testing.

## Install

```bash
pip install prometa-sdk
```

Current source version: **0.20.1**. Release history is on
[PyPI](https://pypi.org/project/prometa-sdk/#history).

The default wheel has **no required third-party dependencies** and requires
Python ≥ 3.9. Every extra below is optional and additive:

| Extra | Installs | For |
|---|---|---|
| `openai` | `openai>=1.0` | OpenAI chat/responses/embeddings instrumentation |
| `anthropic` | `anthropic>=0.40` | Anthropic messages instrumentation |
| `google` | `google-genai>=0.3` | Google GenAI instrumentation |
| `langchain` | `langchain-core>=0.3` | LangChain / LangGraph instrumentation |
| `openllmetry` | OpenTelemetry SDK + 5 instrumentors (py ≥ 3.10) | OpenLLMetry bridge, default targets |
| `openllmetry-all` | the above + Bedrock, Cohere, Haystack, LlamaIndex | broader OpenLLMetry coverage |
| `runtime` | `cryptography`, `jsonschema` | tenant runtime kernel + admission |
| `runtime-mcp` | `runtime` + `mcp>=1.28.1` (py ≥ 3.10) | governed MCP tool broker |
| `runtime-postgres` | `runtime` + `psycopg[binary]` | multi-replica durability |
| `runtime-host` | `runtime` + `psycopg[binary]` | reference tenant runtime host |
| `dev` | `pytest`, `ruff` | contributing |

You do **not** need the `openai` / `anthropic` / `google` / `langchain` extras
if the library is already in your environment — the patchers only ever wrap what
is importable, and `install()` returns `False` otherwise.

### Optional tenant-runtime kit

The default install remains dependency-free and telemetry-first. Install the
runtime extra only in a tenant-owned component that admits signed Agent Builder
artifacts, invokes tenant models/tools, and reports release lifecycle evidence:

```bash
pip install "prometa-sdk[runtime]"
```

```python
import asyncio
import json
import os
from datetime import datetime, timezone

from prometa import Prometa
from prometa.runtime import (
    InMemoryAdmissionReplayStore,
    OpenAICompatibleModelAdapter,
    PrometaEvidenceEmitter,
    BundleTrustEntry,
    BundleTrustStore,
    RuntimeAdmissionPolicy,
    RuntimeKernel,
    RuntimeReceiptClient,
    admit_runtime_release,
    available_runtime_capabilities,
    build_runtime_receipt,
)

with open("agent-bundle.json", encoding="utf-8") as bundle_file:
    bundle = json.load(bundle_file)
with open("promotion-attestation.json", encoding="utf-8") as attestation_file:
    attestation = json.load(attestation_file)

bundle_trust_store = BundleTrustStore(
    [
        BundleTrustEntry(
            issuer="https://orchestra.example.com",
            key_id="orchestra-bundle-2026-07",
            public_key_spki_der_base64=os.environ["ORCHESTRA_BUNDLE_PUBLIC_KEY"],
            allowed_org_ids=frozenset({"org_example"}),
            allowed_audiences=frozenset({"prometa-runtime"}),
            allowed_environments=frozenset({"prod"}),
        )
    ]
)
promotion_trust_store = BundleTrustStore(
    [
        BundleTrustEntry(
            issuer="https://orchestra.example.com/promotion",
            key_id="orchestra-promotion-2026-07",
            public_key_spki_der_base64=os.environ[
                "ORCHESTRA_PROMOTION_PUBLIC_KEY"
            ],
            allowed_org_ids=frozenset({"org_example"}),
            allowed_audiences=frozenset({"prometa-runtime-admission"}),
            allowed_environments=frozenset({"prod"}),
        )
    ]
)

# Replace this with a tenant database implementation whose reserve_pair()
# performs one unique transaction when the host has multiple replicas.
replay_store = InMemoryAdmissionReplayStore()
admitted = admit_runtime_release(
    bundle,
    attestation,
    bundle_trust_store=bundle_trust_store,
    promotion_trust_store=promotion_trust_store,
    replay_store=replay_store,
    policy=RuntimeAdmissionPolicy(
        expected_org_id="org_example",
        expected_environment="prod",
        expected_release_id="release-2026-07-10.1",
        expected_deployment_id="deployment-42",
        expected_runtime="tenant-runtime",
        supported_capabilities=available_runtime_capabilities(),
        minimum_approvals=1,
        required_approval_roles={"Compliance Officer": 1, "Security": 1},
    ),
    now=datetime.now(timezone.utc),
)

telemetry = Prometa(
    endpoint="https://orchestra.example.com/api/v2/otlp/v1/traces",
    api_key=os.environ["PROMETA_API_KEY"],
    solution_id="customer-support",
    agent_name=admitted.config.manifest.name,
    agent_id=admitted.config.manifest.agent_id,
    stage="production",
)
kernel = RuntimeKernel(
    admitted,
    model_adapter=OpenAICompatibleModelAdapter(
        "http://inference-engine.tenant.svc:8080",
        api_key=os.environ.get("MODEL_GATEWAY_API_KEY"),
    ),
    evidence_emitter=PrometaEvidenceEmitter(telemetry),
    runtime_id="tenant-runtime-01",
    runtime_version="1.0.0",
)
result = asyncio.run(
    kernel.execute(
        {"question": "Where is my order?"},
        request_id="request-42",
    )
)
print(result.output)

receipt = build_runtime_receipt(
    attestation_id=admitted.promotion.attestation_id,
    artifact_digest=admitted.artifact_digest,
    release_id="release-2026-07-10.1",
    deployment_id="deployment-42",
    target_environment="prod",
    runtime_target="tenant-runtime",
    runtime_id="tenant-runtime-01",
    runtime_version="1.0.0",
    transition="admitted",
    outcome="accepted",
    policy_digest=admitted.config.contract.policy_digest,
    configuration_digest=admitted.config.contract.configuration_digest,
)
RuntimeReceiptClient(
    "https://orchestra.example.com",
    os.environ["ORCHESTRA_RUNTIME_RECEIPT_API_KEY"],
).submit(receipt)
```

`available_runtime_capabilities()` advertises only installed and configured
components. A bundle declaring guardrails or tools is refused unless the host
supplies a `GuardEvaluator` or tenant `ToolBroker`; the built-in broker denies
all calls. Tool arguments, request payloads, and structured outputs are checked
against the schemas inside the verified bundle before crossing their boundary.
Guard-transformed values are checked again, and server-declared tool guard
requirements cannot be skipped merely because the bundle has no local guard
block.

Bundle schema/runtime contract v2 adds exact capability-version ranges,
deterministic policy and execution-configuration digests, and typed logical
secret references. Admission recomputes both digests and cross-checks the range
form against the exact `name.vN` compatibility mirror. Secret references name a
tenant-resolved provider and purpose only; credential values never enter the
signed artifact. Contract v3 adds the signed Company Workflow Ontology and its
four workflow capabilities (see
[Company Workflow Ontology](#company-workflow-ontology-runtime-contract-v3)).
Bundle schema versions 1–3 and runtime contract versions 1–3 are all admissible
(`SUPPORTED_BUNDLE_SCHEMA_VERSIONS`, `SUPPORTED_RUNTIME_CONTRACT_VERSIONS`), so
v1 releases stay admissible during the production profile transition. Bundles
without a runtime contract remain integrity-verifiable but non-executable by
default.

The kernel bounds model/tool timeouts, retries, exponential backoff, circuit
breaking, topology steps, cancellation, and deterministic fallback. Under the
optional `orchestra-runtime-edge-overload-v1` contract, retryable model errors
may carry normalized server `Retry-After` metadata. The runtime waits for the
greater of its local backoff and the server delay, bounded to 30 seconds by the
reference policy; a longer server delay skips that model retry and advances to
an explicitly signed fallback or fails. It refuses
model retry or fallback after a tool call and rejects duplicate tool-call IDs.
The initial kernel accepts only `single-react` bundles; other signed topology
patterns fail admission until their execution contracts exist. It emits
identity-only decision evidence by default, not raw prompts or tool payloads.
Every event carries the verified bundle, attestation, policy decision, release,
deployment, runtime, environment, manifest, solution, and agent identities.
`prometa.artifact.type=agent-bundle` disambiguates the canonical
`prometa.artifact.digest` join, and `prometa.bundle.digest` remains its
compatibility alias. Runtime contract v2 also adds `prometa.policy.digest` and
`prometa.configuration.digest`; lifecycle receipts carry the same digest pair.
Contract v1 evidence omits the pair rather than inventing values.

The verifier ignores the public key embedded in the transport bundle and
resolves `(issuer, keyId)` from the tenant-controlled trust store. Combined
admission rejects unsigned, tampered, expired, revoked, replayed, wrong-org,
wrong-audience, wrong-environment, offline-lease-expired, non-deployable,
non-promoted, contract-downgraded, or unsupported-capability artifacts. The two
JTIs are reserved together only after every check passes.

Bundle integrity, promotion authorization, and runtime evidence remain
separate. The platform stays outside the synchronous request path; the model
gateway, tool broker, replay/state stores, human escalation, rollout, rollback,
and emergency stop are tenant-owned. The tenant-deployed reference host can
wire a strictly configured MCP broker for signed read-only tools. It requires
exact release/config binding, explicit egress, late-bound credentials, and
shared PostgreSQL idempotency and payload-free audit. The stock host CLI does
not supply a human-escalation adapter, so write or destructive bundles remain
fail-closed; tenants may inject that adapter only through the library builder.
The shipped increment does not include stored-payload or automatic task replay,
resumable HITL checkpoints, memory, compression, A2A, rollout automation,
write/destructive MCP topology evidence, or managed-CNI/database proof. Pinned
two-node K3s/kube-router reference profiles supply narrower model-only and
read-only MCP multi-tenant isolation, load, database-partition,
duplicate-claim, pod-replacement, and credential-rotation evidence. Both
profiles now activate signed bundle schema/runtime contract v2, including
exact capability ranges and independently verified policy/configuration
digests; the live-platform mode requires those same digests on every receipt.

The human-review protocol receives request or tool context only inside the
tenant process. The default evidence adapter never copies that payload into
telemetry.

Receipt submission requires an API key carrying the platform's explicit
`runtime:write` scope and is safe to retry with the same `receiptId` and
semantic payload. A receipt is an authenticated runtime assertion, not an
independent proof of cluster state. Policy and configuration digests are
optional for legacy receipts, but must be supplied together when present.

Importing `prometa.runtime` does not add dependencies to `import prometa` or
change telemetry behavior. Cryptographic and JSON Schema enforcement are
installed only through the `runtime` extra, and the official MCP transport SDK
is installed only through `runtime-mcp` on Python 3.10 or newer.

This package boundary is a compatibility contract:

- the default wheel declares no unconditional third-party dependency;
- `import prometa` does not import `prometa.runtime` or any runtime-only library;
- `import prometa.runtime` exposes contracts without importing optional
  libraries, while use of an unavailable capability fails explicitly; and
- PostgreSQL, MCP, cryptographic verification, and JSON Schema execution stay
  behind their named extras.

CI verifies these invariants from a clean wheel installed with `--no-deps`.
A separate `prometa-runtime` distribution requires an ADR when any one of these
objective triggers occurs: the runtime needs a different Python floor or an
unconditional dependency, telemetry and runtime dependency bounds become
unsatisfiable, two runtime security fixes in a rolling 12 months require a
release independent of the telemetry SDK, or runtime sources make the core
wheel exceed 5 MiB for two consecutive releases. Until then, the optional
extras and `prometa.runtime` import path remain stable.

#### Governed tenant-side MCP adapter

Install the transport adapter only in a tenant-owned executor:

```bash
pip install "prometa-sdk[runtime-mcp]"
```

The broker receives only tool calls already admitted by `RuntimeKernel`. It
intersects the signed tool's logical server, scopes, auth binding, risk, side
effects, and approval requirement with a tenant-local connection and a
CI/GitOps-projected permission row. The platform is not called synchronously.

```python
import os

from prometa.runtime import (
    EnvironmentMcpCredentialProvider,
    ExplicitMcpEgressPolicy,
    GovernedMcpToolBroker,
    InMemoryMcpAuditSink,
    McpBrokerPolicy,
    McpCredentialBinding,
    McpServerConfig,
    McpToolGrant,
    OfficialMcpTransportClient,
)

server = McpServerConfig(
    name="Integration Tools",
    connection_id="mcp-integration-prod",
    transport="streamable-http",
    endpoint="https://mcp.integration.example.com/mcp",
    environment="production",
    auth_mode="service-account",
    scopes=("mcp.integration.read", "mcp.integration.write"),
    risk_level="medium",
)

broker = GovernedMcpToolBroker(
    servers=(server,),
    grants=(
        McpToolGrant(
            tool_name="mcp.integration.lookup",
            agent_ids=("agent-integration",),
            permission="read",
            risk_level="low",
            server_connection_id=server.connection_id,
        ),
    ),
    policy=McpBrokerPolicy(max_risk_level="medium"),
    egress_policy=ExplicitMcpEgressPolicy(
        allowed_http_origins=frozenset(
            {"https://mcp.integration.example.com"}
        )
    ),
    credential_provider=EnvironmentMcpCredentialProvider(
        (
            McpCredentialBinding(
                server_name=server.name,
                auth_mode="service-account",
                http_headers={
                    "Authorization": "MCP_INTEGRATION_AUTHORIZATION"
                },
            ),
        ),
        environ=os.environ,
    ),
    transport_client=OfficialMcpTransportClient(),
    audit_sink=InMemoryMcpAuditSink(),
)
```

Pass `broker` to `RuntimeKernel(tool_broker=broker, ...)`. Tenant production
execution is allowed only when the verified bundle target and tenant-local
server environment both resolve to production. This does not change the
platform control plane's separate hard block on production tool execution.

The adapter supports official stdio and Streamable HTTP transports. HTTP
redirects and ambient proxy variables are disabled, public plain HTTP is
rejected, and egress requires an exact origin or command allowlist. Credentials
are resolved at call time from named environment variables or a tenant-supplied
provider and never appear in audit events.

Permission selection follows the platform matrix's server and agent
specificity. The permission label is preserved as evidence; signed side-effect
classification drives the stronger runtime rules. Write and destructive calls
require a tenant reviewer reference and an idempotency store by default. Audit
records contain identities and SHA-256 digests, not arguments or outputs, and
an uncertain transport or post-call audit outcome is marked indeterminate so
the runtime cannot retry it automatically.

`InMemoryMcpIdempotencyStore` and `InMemoryMcpAuditSink` are for tests and
single-process development only. `PostgresMcpIdempotencyStore` and
`PostgresMcpAuditSink` provide the reference multi-replica implementation. A
stale reservation becomes `indeterminate`, never automatically reacquired,
because the prior replica may already have reached the tool. DNS and
network-layer enforcement remain the tenant's NetworkPolicy, firewall, or
service-mesh responsibility; the SDK allowlist validates the declared
destination.

#### Multi-replica durability

Install the database extra only when replicas must share replay and request
state:

```bash
pip install "prometa-sdk[runtime-postgres]"
```

Run the fixed schema installer once with a migration credential, then use a
lower-privilege runtime credential for request traffic. The DSN must be a
libpq-compatible PostgreSQL DSN; ORM-only query parameters such as Prisma's
`?schema=public` are not accepted by psycopg. Migration and runtime DSNs must
resolve to the same database schema/search path. The serving role needs
`SELECT, INSERT` on `prometa_runtime_admission_replay`,
`SELECT, INSERT, UPDATE, DELETE` on `prometa_runtime_request_state`, and
`SELECT, INSERT, UPDATE` on `prometa_runtime_release_activation`, plus
`SELECT, INSERT` on `prometa_runtime_bundle_identity`. When lifecycle receipt
delivery is configured, it also needs `SELECT, INSERT, UPDATE` on
`prometa_runtime_receipt_outbox`; it does not need DDL privileges.
Pull-mode hosts also need `SELECT, INSERT, UPDATE` on
`prometa_runtime_release_cache`. Hosts with `taskRecovery` also need
`SELECT, INSERT, UPDATE` on `prometa_runtime_task` and `SELECT, INSERT` on
`prometa_runtime_task_event`. MCP-enabled hosts need
`SELECT, INSERT, UPDATE, DELETE` on `prometa_runtime_mcp_idempotency` and
`INSERT` on `prometa_runtime_mcp_audit`; an operator identity may additionally
receive `SELECT` on the audit table for verification. Hosts admitting runtime
contract v3 workflow releases also need `SELECT, INSERT, UPDATE` on
`prometa_runtime_workflow_instance`, `SELECT, INSERT` on the append-only
`prometa_runtime_workflow_ledger`, and — when `workflowDecisionDelivery` is
configured — `SELECT, INSERT, UPDATE` on
`prometa_runtime_workflow_decision_outbox`. Security-decision delivery likewise
needs `SELECT, INSERT, UPDATE` on `prometa_runtime_security_decision_outbox`.

```bash
export PROMETA_RUNTIME_DATABASE_URL='postgresql://...'
prometa-runtime-postgres-init
prometa-runtime-postgres-compatibility
prometa-runtime-postgres-verify
```

`prometa-runtime-postgres-compatibility` is the serving-image gate. It reads
only migration and table metadata and rejects an uninitialized, gapped, older,
newer, or structurally incompatible schema before the host activates a release.
The installer remains the separate mutating step.

`prometa-runtime-postgres-verify` is a payload-free pre-cutover check for a
newly restored database. It requires exact migrations through the current
schema version (**v8**), required task/event and MCP columns, valid lease and
terminal projections, complete ordered task history, and payload-free MCP audit
records. Its JSON
output contains only schema versions and table counts. Logical backup/restore
scripts and the optional encrypted-PVC Helm backup CronJob live under
[`deploy/reference-runtime/`](deploy/reference-runtime/README.md); restore is
refused unless the target database is fresh and the archive checksum matches.

```python
import os

from prometa.runtime import (
    PostgresAdmissionReplayStore,
    PostgresMcpAuditSink,
    PostgresMcpIdempotencyStore,
    PostgresRuntimeStateStore,
    PostgresRuntimeTaskStore,
    PostgresWorkflowStateStore,
    install_postgres_runtime_schema,
)

install_postgres_runtime_schema(os.environ["RUNTIME_MIGRATION_DATABASE_URL"])

replay_store = PostgresAdmissionReplayStore(
    os.environ["RUNTIME_DATABASE_URL"],
    tenant_id="org_example",
)
state_store = PostgresRuntimeStateStore(
    os.environ["RUNTIME_DATABASE_URL"],
    tenant_id="org_example",
    runtime_id="tenant-runtime-01",
)
task_store = PostgresRuntimeTaskStore(
    os.environ["RUNTIME_DATABASE_URL"],
    tenant_id="org_example",
    runtime_id="tenant-runtime-01",
)
mcp_idempotency_store = PostgresMcpIdempotencyStore(
    os.environ["RUNTIME_DATABASE_URL"],
    tenant_id="org_example",
    runtime_id="tenant-runtime-01",
)
mcp_audit_sink = PostgresMcpAuditSink(
    os.environ["RUNTIME_DATABASE_URL"],
    tenant_id="org_example",
    runtime_id="tenant-runtime-01",
)
# Runtime contract v3 workflow releases only.
workflow_state_store = PostgresWorkflowStateStore(
    os.environ["RUNTIME_DATABASE_URL"],
    tenant_id="org_example",
    runtime_id="tenant-runtime-01",
)
```

Pass `replay_store` to `admit_runtime_release()` and `state_store` to
`RuntimeKernel`. Replay reservation uses one database transaction with
tenant-wide unique bundle and promotion identities, so changing replicas or
runtime IDs cannot make the same authorization reusable. Request state is
tenant/runtime scoped and versioned, with `load()` and `delete()` available for
replica handoff and retention. State writes are atomic last-write-wins
snapshots; they are not an exactly-once request lock or a resumable HITL
workflow. `PostgresRuntimeTaskStore` is a separate lifecycle v1 contract: it
atomically leases one request attempt across replicas, binds retries to the
same input/release/deployment identity, appends ordered payload-free events,
and permits bounded reclaim after an expired safe lease. The MCP stores apply a
different safety rule: expired or uncertain tool-call reservations become
indeterminate and require operator reconciliation rather than automatic replay.

#### Company Workflow Ontology (runtime contract v3)

Runtime contract v3 carries a signed **Company Workflow Ontology** inside the
bundle: a compiled, closed-AST policy describing states, tasks, transitions,
roles, business objects, facts, evidence requirements, obligations, and
controls, bound to a sector snapshot. The runtime resolves **no** ontology or
authorization data from Orchestra during execution — it verifies the exact
ontology, compiled-policy, sector-snapshot, and bundle digest bindings at
admission and evaluates locally.

A v3 contract **must** carry an ontology (admission fails with
`runtime_v3_workflow_ontology_missing` otherwise), and an ontology-bearing
bundle infers all four workflow capabilities — so a host serving one has to
supply all three tenant-owned adapters. Conversely, declaring an ontology under
contract v1 or v2 fails with `workflow_ontology_requires_runtime_v3`.

| Capability | Supplied by |
|---|---|
| `workflow.policy.evaluate.v1` | built in (deterministic evaluator; always advertised) |
| `workflow.context.resolve.v1` | your `WorkflowContextResolver` |
| `workflow.state.persist.v1` | your `WorkflowStateStore` (or `PostgresWorkflowStateStore`) |
| `workflow.decision.emit.v1` | your `WorkflowDecisionEmitter` (or `DurableWorkflowDecisionEmitter`) |

```python
from prometa.runtime import (
    InMemoryWorkflowDecisionEmitter,
    InMemoryWorkflowStateStore,
    RuntimeKernel,
    VerifiedWorkflowContext,
    available_runtime_capabilities,
)


class TenantWorkflowContextResolver:
    async def resolve(self, request) -> VerifiedWorkflowContext:
        # Tenant-owned and authoritative. Roles, purpose, state version and
        # facts come from your systems of record — never from the caller.
        record = my_erp.load(request.workflow.instance_id)
        return VerifiedWorkflowContext(
            current_state=record.state,
            state_version=record.version,
            actor_role_ids=("ap_clerk",),
            purpose="invoice_posting",
            facts={"invoice.total": {"value": record.total, "asOf": record.as_of}},
            approvals=(),
        )


resolver = TenantWorkflowContextResolver()
state_store = InMemoryWorkflowStateStore()          # dev only
decision_emitter = InMemoryWorkflowDecisionEmitter()  # dev only

kernel = RuntimeKernel(
    admitted,
    model_adapter=...,
    evidence_emitter=...,
    workflow_context_resolver=resolver,
    workflow_state_store=state_store,
    workflow_decision_emitter=decision_emitter,
    workflow_postcondition_validator=my_postcondition_validator,
    runtime_id="tenant-runtime-01",
    runtime_version="1.0.0",
)
```

Declare the same adapters to `available_runtime_capabilities(...)` when building
your `RuntimeAdmissionPolicy`, or admission refuses a workflow bundle whose
declared capabilities the host cannot satisfy.

Signed `mode` selects enforcement strength. **Observe** mode records
recommendations without blocking; **enforce** mode fails closed, including on
indeterminate state. Authorization runs *before* the tool call; postconditions
are validated after it, and the state transition is a compare-and-set against
the resolved `state_version`. A side effect whose outcome is uncertain
(timeout, unknown result, invalid postcondition) is quarantined as
`INDETERMINATE` in the append-only workflow ledger and is **never** replayed
automatically — it requires operator reconciliation.

Callers may pass an optional `workflowContext` on `POST /v1/runtime/execute`,
limited to immutable workflow identity plus an opaque actor reference:

```json
{
  "requestId": "request-42",
  "input": {"invoiceId": "INV-1001"},
  "workflowContext": {
    "workflowId": "ap-invoice-posting",
    "version": 4,
    "instanceId": "instance-9f2c",
    "actorRef": "actor-ref-7731"
  }
}
```

Caller-supplied roles, permissions, or authorization facts are rejected by the
contract — those are resolved tenant-side by your `WorkflowContextResolver`.

Decisions are emitted as strict `prometa.workflow-decision.v1` batches through a
durable, payload-free outbox (`workflowDecisionDelivery` on the host, or
`DurableWorkflowDecisionEmitter` in the library). The schema rejects raw
prompts, tool arguments, results, messages, business facts, unknown fields, and
mutable decision identifiers. Batches are bounded by
`MAX_WORKFLOW_DECISIONS_PER_BATCH` and `MAX_WORKFLOW_DECISION_BODY_BYTES`.

A separate, explicitly enabled **staging-only** workflow proof host
(`PROMETA_RUNTIME_WORKFLOW_PROOF=enabled`) combines signed v3 admission with
tenant-owned approval/fact resolution, PostgreSQL CAS, a deterministic local
SAP-boundary simulation, postcondition validation, and payload-free
asynchronous evidence. It never connects to a real ERP and is not a production
profile.

#### Reference tenant runtime host

Install the host extra only in the tenant runtime image:

```bash
pip install "prometa-sdk[runtime-host]"
```

For an MCP-enabled host installation, include both optional surfaces:

```bash
pip install "prometa-sdk[runtime-host,runtime-mcp]"
```

`prometa-runtime-host` loads one strict mounted configuration, resolves either
an embedded release pair or a tenant-selected outbound handoff, verifies both
signed artifacts locally, and atomically creates or joins an immutable release
activation in tenant PostgreSQL. Exact replicas and restarts may join. A fresh
promotion can authorize the same signed bundle bytes for a new deployment;
changed activation identity, promotion-JTI reuse, or a bundle JTI bound to a
different artifact digest fails closed. The host then serves:

- `GET /healthz` for liveness;
- `GET /readyz` for payload-free readiness;
- `GET /v1/runtime/tasks/{requestId}` for authenticated payload-free lifecycle
  replay when `taskRecovery` is configured;
- `POST /v1/runtime/execute` for bounded bearer-authenticated JSON requests.

The request endpoint calls only tenant-owned model, state, and optional MCP
planes. It validates schemas before model invocation, rejects duplicate
in-flight IDs within a replica, returns stable payload-free errors, and shuts
down its persistent kernel event loop gracefully. An `mcpBroker` block must
match the signed server/tool contract and names only credential environment
variables; no secret value belongs in mounted configuration. Missing transport
dependencies, credentials, egress grants, or release bindings fail startup or
the call before transport. Optional `taskRecovery` extends duplicate rejection
across replicas and records ordered model-only lifecycle metadata. It cannot be
combined with tool-bearing releases and does not claim exactly-once model
invocation, distributed rate limiting, or overload fairness. Server TLS is
optional and fail-closed when selected; mTLS adds a tenant-owned client CA while
the bearer-token boundary remains active. The default installation still serves
plain HTTP for backward compatibility.
The declared OpenShift profile sets
`PROMETA_RUNTIME_EDGE_OVERLOAD_CONTRACT=orchestra-runtime-edge-overload-v1`
and accepts only `/v1/chat/completions`; development profiles remain
unrestricted. The tenant gateway still owns admission control, distributed
rate limits, fairness, queueing, load shedding, and autoscaling.

The mounted configuration is strict — unknown keys fail closed. Beyond the
required release/model/database fields, these optional blocks are recognized:

| Block | Adds |
|---|---|
| `controlPlanePull` | bootstrap-only outbound release handoff instead of an embedded pair |
| `receiptDelivery` | durable asynchronous `admitted` / `active` lifecycle receipts |
| `securityDecisionDelivery` | `prometa.security-decision.v1` evidence (required by guarded v2+ releases) |
| `workflowDecisionDelivery` | `prometa.workflow-decision.v1` evidence for contract v3 workflow releases |
| `taskRecovery` | lifecycle contract v1 cross-replica duplicate rejection and task history |
| `mcpBroker` | governed read-only MCP tools bound to the signed contract |

Optional `receiptDelivery` configuration adds durable asynchronous `admitted`
and `active` lifecycle evidence. The host commits receipts to its PostgreSQL
outbox before a background dispatcher contacts Orchestra, so platform outage
does not change readiness or request behavior. Replica leases prevent duplicate
workers, deterministic receipt IDs preserve idempotency across restarts, and
permanent rejections are dead-lettered with sanitized evidence.

Runtime-contract v2 and v3 releases whose guardrails enable security assurance
additionally require `security.decision.emit.v1`. Configure `securityDecisionDelivery` with a
machine key limited to `security-decisions:write`; otherwise admission fails
before the host serves traffic. The kernel honors signed `observe`, `review`,
and `enforce` modes plus review/enforcement thresholds locally, persists the
strict content-minimized `prometa.security-decision.v1` evidence in tenant
PostgreSQL, and sends batches of at most 500 decisions/1 MiB from a background
dispatcher. Prometa is never consulted to take or resume an action.

Security campaigns may pass `X-Prometa-Campaign-Id`,
`X-Prometa-Campaign-Run-Id`, and `X-Prometa-Probe-Id` to
`POST /v1/runtime/execute`. The host validates them as bounded identifiers and
copies only those correlation IDs into emitted decisions; prompts, completions,
messages, tool arguments, and credentials are rejected by the decision
contract.

Optional `controlPlanePull` configuration replaces the embedded `bundle` and
`promotionAttestation` fields with an attestation ID selected by tenant CI/CD.
The host calls the API only during bootstrap with a narrow `runtime:read` key,
refuses redirects and non-HTTPS endpoints by default, requires the platform's
`checkedAt` to fall inside the configured clock-skew window, then performs the
normal local signature, binding, expiry, capability, and activation checks. A verified
pair is cached in tenant PostgreSQL. Only transport, 408/425/429, or 5xx failures
may use that cache, and only within `maxCacheAgeSeconds` and the signed offline
lease. Revocation, authorization, binding, or signature failures never fall
back. Changing the attestation ID still requires tenant CI/CD to update the
mounted config and roll the workload; this is not a hot-reload controller.

Optional `taskRecovery` configuration enables lifecycle contract v1 only for a
model-only host:

```json
{
  "taskRecovery": {
    "leaseSeconds": 90,
    "maxAttempts": 3,
    "historyLimit": 50
  }
}
```

The lease must be longer than `requestTimeoutSeconds`. Before model invocation,
the host atomically claims `(tenant, runtime, requestId)` and binds it to the
canonical input digest plus artifact, release, deployment, recovery policy, and
attempt ceiling. An active claim returns `task_in_progress`; changed input or
release identity returns `task_identity_conflict`. Retryable failures become
immediately reclaimable, while a process-killed `running` attempt becomes
reclaimable only after its lease expires. PostgreSQL's transaction clock, not a
replica clock, governs production lease decisions. Every reclaim increments the
host attempt and every transition gets a monotonic sequence.

The task tables and status API contain digests, model metadata, stable error
codes, timestamps, and transitions only. They do not store request or response
bodies, credentials, prompts, or model output. Recovery is therefore
client-driven: the caller resubmits semantically identical finite JSON input
with the same request ID.
Model invocation is at-least-once, and a completed task reports conflict rather
than replaying its response body. MCP call idempotency is a separate durable
contract and does not make the enclosing task replayable. Automatic replay,
encrypted result retention, and resumable HITL checkpoints require later
contracts.

The non-root container, Compose example, tenant-owned Helm chart, logical
backup/restore assets, strict configuration shape, and operator commands live in
[`deploy/reference-runtime/`](deploy/reference-runtime/README.md).
The release-bound chart runs the target image's compatibility check after
migration and before future chart rollback. Its `runtimeConfig.rolloutId` pod annotation makes
tenant-selected immutable config revisions explicit. The CI drill uses a real
schema-v2 source baseline, upgrades to the current schema version and bundle B,
then starts the
baseline host again with bundle A's exact bytes under a fresh promotion and
deployment identity. This is source-level compatibility evidence, not a
published-version certification claim. A separate manual release-channel drill
executes an older signed chart/image, a newer signed chart/image, and a forward
deployment of the older pair with a freshly authorized prior bundle. Separate
pinned K3s/kube-router
profiles now prove the chart's model-only and read-only MCP paths in one
two-node, two-tenant reference topology. The current v2 profiles admit runtime
contract v2 releases and bind capability ranges plus policy/configuration
digests into emitted evidence; they do not generalize to OpenShift, managed
CNIs/databases, write/destructive MCP, or production.

#### Runtime conformance

The installed runner validates signed admission, tamper and replay denial,
schema-before-model ordering, joinable completion evidence, and fail-closed
evidence behavior:

```bash
prometa-runtime-conformance --output runtime-conformance-report.json
```

Profiles are explicit:

- `core` retains the existing six-case library contract;
- `resilience` tests local admission during control-plane outage, offline-lease
  expiry, replay/state-store outages, and bounded model-plane failure;
- `deployment` runs both profiles and is the expected image/deployment gate.

To exercise another process, container wrapper, or adapter to a deployed tenant
runtime, provide a command. The runner parses it to argv and executes it directly
without a shell. Each case gets a fresh process, bounded stdin/stdout/stderr,
and a hard timeout:

```bash
prometa-runtime-conformance \
  --profile deployment \
  --driver-name tenant-runtime-staging \
  --command "python examples/runtime_conformance_command_driver.py" \
  --output runtime-deployment-conformance.json
```

The child receives protocol v1 JSON on stdin and writes one observation to
stdout. `runtime_conformance_command_main()` provides the child-side framing;
replace its `SdkRuntimeConformanceDriver` with an adapter that calls the runtime
under test. The protocol requires an explicit synchronous control-plane call
count, and every shipped case expects zero.

The language-neutral wire shape is:

```json
{
  "protocolVersion": 1,
  "case": {
    "caseId": "execution.valid",
    "description": "...",
    "vector": {}
  }
}
```

```json
{
  "protocolVersion": 1,
  "observation": {
    "accepted": true,
    "errorCode": null,
    "output": {},
    "modelInvocations": 1,
    "controlPlaneInvocations": 0,
    "evidenceEvents": [
      {
        "name": "runtime.request",
        "outcome": "completed",
        "occurredAt": "2026-07-12T00:00:00Z",
        "attributes": {}
      }
    ]
  }
}
```

The runner uses `output` and evidence attributes only to evaluate expectations;
neither is copied into the final report.

The command exits nonzero when a check fails. Reports contain fixture identity,
check outcomes, error codes, model-call counts, and evidence event names; they
exclude fixture payloads, model outputs, trust keys, and credentials. A tenant
runtime can implement `RuntimeConformanceDriver` and select a factory with
`--driver package.module:create_driver`. `--command-timeout`,
`--max-command-output-bytes`, `--fixture`, and `--compact` bound and redirect a
subprocess run. `prometa-runtime-host-conformance-driver` is the shipped
child-side adapter that exercises the reference host's own request boundary
rather than the library kernel. This is an adapter-level test contract,
not certification by the Prometa control plane. The separate
`deploy/reference-runtime/ci/topology-certification.sh` profiles add retained
K3s kube-router evidence for two-tenant isolation, load, a database-egress
partition, pod replacement, and live contract-v2 admission. The explicit
read-only MCP profile additionally
proves signed tool binding, exact tools-plane policy, cross-replica call
admission, payload-free audit, and fail-closed credential rotation. The
model-only profile's opt-in live-platform mode also verifies asynchronous
lifecycle receipts, exact policy/configuration digest binding, and the
release-scoped Orchestra projection;
see [`deploy/reference-runtime/README.md`](deploy/reference-runtime/README.md#optional-live-orchestra-receipt-proof).
Production acceptance still requires the same
proof against the tenant's actual CNI, ingress, database, storage, and recovery
topology.

#### Runtime console scripts

The runtime extras install these commands; none are present in a default
observability install:

| Command | Extra | Purpose |
|---|---|---|
| `prometa-runtime-host` | `runtime-host` | serve the reference tenant runtime host |
| `prometa-runtime-postgres-init` | `runtime-postgres` | install the fixed schema (migration credential) |
| `prometa-runtime-postgres-compatibility` | `runtime-postgres` | serving-image gate; read-only metadata check |
| `prometa-runtime-postgres-verify` | `runtime-postgres` | payload-free pre-cutover integrity check |
| `prometa-runtime-conformance` | `runtime` | run the conformance suite (`core` / `resilience` / `deployment`) |
| `prometa-runtime-host-conformance-driver` | `runtime-host` | child-side adapter exercising the host request boundary |

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
- `prometa.agent_id` — only when you explicitly pin one; otherwise the platform auto-registers the Agent from `solution_id` + `agent_name`
- `gen_ai.agent.id` — legacy compatibility only; Prometa correlation keys on `prometa.agent_id` or the name fallback
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
    agent_name="support-assistant",
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

## Custom span attributes

Use `set_attribute` and `set_attributes` when an integration needs to
stamp scalar metadata that Prometa should preserve, but that is not part
of the core correlation chain.

```python
from prometa import set_attribute, set_attributes

@prometa.tool(name="prepare-action")
def prepare_action():
    set_attribute("your.integration.mcp.server.name", "example-server")
    set_attributes(
        {
            "your.integration.mcp.tool.name": "prepare_action",
            "your.integration.mcp.direct_action": False,
            "your.integration.mcp.args_count": 3,
        }
    )
```

Values must be `str`, `int`, `float`, or `bool`. Both helpers return
`False` when called outside an active span.

## Data-flow refs between sibling spans

`parent_span_id` captures the call stack — who *invoked* whom. For agent traces
the more interesting question is usually "whose **output** did this span
consume?", and that is almost always a sibling relationship: an LLM emits a
tool_call, the agent dispatches a sibling tool span. Without an explicit ref the
platform can only infer that link from temporal proximity.

```python
from prometa import current_span_id, set_input_ref, set_output_ref

state = {}

@prometa.agent(name="openai.chat")
async def call_llm(prompt: str) -> dict:
    # Snapshot the active span so a later sibling can reference it.
    state["last_llm_span_id"] = current_span_id()
    ...

@prometa.tool(name="kb-search")
async def kb_search(q: str) -> list[str]:
    # "I consumed the output of <llm span>"
    set_input_ref(state["last_llm_span_id"])
    ...
```

`set_input_ref` writes `prometa.input_ref`, `set_output_ref` writes
`prometa.output_ref`, and `get_input_ref` / `get_output_ref` read them back. The
platform persists both as dedicated columns and the trace UI's Causal-context
block renders them as clickable "Input from" / "Output to" rows. Calling any of
them outside an active span is a no-op.

See [`examples/data_flow_refs.py`](examples/data_flow_refs.py) for a runnable
end-to-end version.

## Token budgets

`TokenBudget` is a per-process sliding-window cap for throttling LLM spend
before the call goes out. It is thread-safe but **not** distributed — for
cross-instance budgets, call the platform API at runtime.

```python
from prometa import TokenBudget, BudgetExceededError

budget = TokenBudget(
    limit_tokens=1_000_000,
    window_seconds=86_400,
    mode="hard",        # "hard" raises; "soft" allows the call and flags the span
    label="default",
)

try:
    budget.check(approx_input_tokens + approx_output_tokens)
    resp = llm.chat(...)
except BudgetExceededError:
    resp = "(request throttled: token budget exceeded)"
```

`check()` stamps `budget.label`, `budget.limit`, `budget.used`,
`budget.remaining`, and `budget.exceeded` on the active span in both modes, so
soft-mode overruns stay visible in the trace. `used()` and `remaining()` expose
the current window without consuming from it.

## Assistant intent labels

Applications can stamp assistant intent before any LLM/tool/action work
so Prometa can index and filter traces by the user's intended operation.

```python
from prometa import set_assistant_intent, set_assistant_intent_from_text

@prometa.workflow(name="assistant-turn")
def handle_turn(user_text: str, from_quick_action: bool = False):
    if from_quick_action:
        set_assistant_intent(
            "D,E",
            source="quick_action",
            preclassified=True,
        )
    else:
        set_assistant_intent_from_text(user_text)

    # Nested LLM/tool/action spans inherit the labels unless they
    # explicitly override them.
    ...
```

Labels are stable single-letter codes:

| Code | Label name |
|---|---|
| `A` | `general_information_gathering` |
| `B` | `pipeline_flow_information_gathering` |
| `C` | `current_status_information_gathering` |
| `D` | `configuration_editing_execution` |
| `E` | `flow_process_execution` |

The SDK stamps platform-indexable Prometa trace attributes:

- `prometa.intent.labels`, `prometa.intent.label_names`,
  `prometa.intent.count`, `prometa.intent.source`,
  `prometa.intent.preclassified`, `prometa.intent.classifier_version`

Free-text turns use deterministic clause decomposition, so a request
such as "change the settings, then run the flow" emits `D,E` without
LLM token usage. Provider integrations also classify the latest
`role: "user"` text automatically when no active parent span already
has intent labels.

`classify_assistant_intent(text)` exposes the same deterministic classifier
without touching a span, if you want to inspect or route on the labels before
deciding what to instrument.

For deterministic UI actions, pass local-only kwargs through supported
LLM integrations; the SDK strips them before calling the provider:

```python
client.responses.create(
    model="gpt-4o-mini",
    input=[{"role": "user", "content": prompt}],
    prometa_intent_labels="D,E",
    prometa_intent_source="quick_action",
    prometa_intent_preclassified=True,
)
```

## User feedback feeding

Applications can feed user feedback into Prometa as generic
`prometa.feedback.*` telemetry. The SDK supports thumbs-up /
thumbs-down, 1-5 star ratings, open-text comments, and optional target
ids so the platform can attach delayed feedback to the original trace,
span, or session.

If feedback is collected before the traced workflow exits, stamp it on
the active span:

```python
from prometa import set_user_feedback

@prometa.workflow(name="assistant-turn")
def handle_turn(user_text: str):
    answer = run_assistant(user_text)

    if user_clicked_dislike:
        set_user_feedback(
            liked=False,
            comment="Missed the billing policy exception.",
            source="thumbs_down",
            feedback_id="fb_123",
            user_id="user_456",
        )

    return answer
```

If feedback arrives later from a UI callback or API endpoint, emit a
dedicated `feedback.record` span:

```python
from prometa import record_user_feedback

record_user_feedback(
    rating=5,
    comment="Exactly what I needed.",
    source="stars",
    target_trace_id=trace_id,
    target_span_id=span_id,
    target_session_id=session_id,
    submitted_at="2026-06-05T09:30:00Z",
)
```

The SDK emits these platform-facing attributes:

- `prometa.feedback.signal` — `like`, `dislike`, `rating`, `comment`,
  or a comma-separated combination.
- `prometa.feedback.liked` — boolean thumbs signal when supplied.
- `prometa.feedback.rating` — integer 1-5 star score when supplied.
- `prometa.feedback.score` — normalized score in `[-1.0, 1.0]`.
- `prometa.feedback.sentiment` — `positive`, `neutral`, or `negative`.
- `prometa.feedback.comment` — open-text comment, truncated to 4096
  characters.
- `prometa.feedback.source`, `prometa.feedback.id`,
  `prometa.feedback.user_id`, `prometa.feedback.submitted_at`.
- `prometa.feedback.target.trace_id`,
  `prometa.feedback.target.span_id`,
  `prometa.feedback.target.session_id`.

Avoid putting PII in comments or user ids unless your Prometa
deployment is configured for that data class.

### Stable Agent IDs

`agent_id` is optional. By default the SDK emits `solution_id` and
`agent_name`, then the platform mirrors the Tool registration model:
on first sighting it auto-registers the Agent row for the
`(orgId, solutionId, agentName)` tuple and attaches the canonical
Agent ID during ingest.

You can still pin an ID by passing `agent_id="..."` to `Prometa(...)`
or setting `PROMETA_AGENT_ID`. When pinned, the SDK includes
`prometa.agent_id` on resource and span attributes, alongside
`gen_ai.agent.name` and `service.name`. The SDK also emits
`gen_ai.agent.id` for legacy readers, but Prometa correlation should key
on `prometa.agent_id` and fall back to the name tuple when absent. When
absent, the SDK deliberately omits both ID attributes; it does not
generate a random per-process fallback.

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
export PROMETA_AGENT_NAME=support-assistant
```

```python
# Or per-instance
prometa = Prometa(endpoint=..., agent_name="support-assistant")
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

# Optional retrieval.namespace identifies the searched corpus / collection
# (not tenant, solution, agent, or environment). Omit or leave blank to
# keep the attribute unset.
with retrieval_query(
    "hybrid",
    query_text=query,
    top_k=4,
    namespace="knowledge-bank",
) as r:
    docs = store.search(query, top_k=4)
    r.results(
        result_ids=[d.id for d in docs],
        scores=[d.score for d in docs],
        permissions_enforced=True,
    )
```

Each helper is a context manager (or a synchronous record call for
fire-and-forget events) that emits a typed span with the attribute
shape the AML detectors expect. Calling them from inside an active
`@prometa.workflow / .agent / .tool` decorator nests the AML spans
under the parent — no extra wiring needed.

See [`examples/aml_instrumentation.py`](examples/aml_instrumentation.py) for a
full simulated agent turn wired through these helpers.

### Dual-channel raw capture

Eight of the 41 AML features exist precisely to *transform* raw input, so they
cannot be detected from the sanitized record alone. The raw channel is the
opt-in second stream that makes them auditable.

```python
import prometa

prometa.raw_channel.enable()    # process-wide; call once at startup
prometa.raw_channel.is_enabled()
prometa.raw_channel.disable()
```

**Off by default.** While disabled, `raw_input=` / `raw_*` arguments to the
guardrail, PII-filter, prompt, sentiment, and MCP helpers are dropped at the SDK
boundary, so a misconfiguration cannot leak raw PII upstream. While enabled,
those helpers stamp truncated `prometa.raw.*` attributes.

The toggle is a module-level flag, not a per-client setting — flipping it
mid-process affects every active client. Enable it only after confirming the org
is authorized to emit raw attributes.

## AQL / PrometaQL query readiness

AQL is the platform-side query and evaluation layer over the telemetry
this SDK emits. That is why it does not have a separate family of
`aql_*` instrumentation helpers: AML helpers create additional detector
evidence spans, while AQL reads the normalized trace/span attributes
already emitted by decorators, setters, refs, and LLM integrations.

To make traces useful for AQL queries and judge/replay workflows, stamp
the stable fields AQL filters and aggregates on:

| SDK surface | AQL-readable signal |
|---|---|
| `@prometa.workflow / .agent / .tool / .task` | `trace_id`, `span_id`, parent/child edges, `prometa.kind`, `prometa.solution_id`, `gen_ai.agent.name` |
| `set_customer_id`, `set_user_id`, `set_conversation_id`, `set_request_model`, `set_tool_name` | canonical customer, user, session, model, and tool dimensions |
| `set_assistant_intent` / `set_assistant_intent_from_text` | `prometa.intent.*` filters for user-turn intent and preclassified UI actions |
| `set_input_ref`, `set_output_ref`, `current_span_id` | lineage edges for replay, judge, and flow-level queries |
| `set_user_feedback` / `record_user_feedback` | `prometa.feedback.*` outcome labels for eval cohorts and regression tracking |
| LLM integrations | `gen_ai.prompt`, `gen_ai.prompt.user`, `gen_ai.completion`, token usage, response model, finish reasons |
| Framework integrations | `gen_ai.framework`, plus framework-native `db.*` / `mcp.*` / `crewai.*` / `sk.*` dimensions |
| `TokenBudget.check` | `budget.used`, `budget.remaining`, `budget.exceeded` for throttling and spend queries |
| AML helpers | typed evidence spans that AQL can join with ordinary lifecycle and LLM spans |

In short: instrument once with the SDK, then run AQL / PrometaQL on the
Prometa platform to query traces, compare sessions, inspect failure
patterns, build eval cohorts, and feed LLM-as-judge or replay tooling.

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
`client.responses.create(...)` (openai ≥ 1.40),
`client.embeddings.create(...)`, `client.messages.create(...)`, and
`client.models.generate_content(...)` call (sync, async, **and streaming**
where applicable) emits a child span carrying:

- `gen_ai.system` (`openai` / `anthropic` / `google`)
- `gen_ai.request.model`, `temperature`, `top_p`, `max_tokens`
- `gen_ai.usage.input_tokens` / `gen_ai.usage.output_tokens` → drives cost
- `gen_ai.prompt` (truncated JSON of input messages)
- `gen_ai.completion` (truncated assistant reply)
- `gen_ai.response.id`, `gen_ai.response.model`, `gen_ai.response.finish_reasons`

OpenAI embedding spans use `gen_ai.operation.name=embeddings` and stamp
model + usage (`gen_ai.usage.input_tokens` /
`gen_ai.usage.total_tokens`) plus request-size signals
(`gen_ai.request.embedding.input_count` /
`gen_ai.request.embedding.input_chars`). Embedding **vectors** are never
written into span attributes.

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

## Framework auto-instrumentation

Beyond the raw LLM clients, the SDK ships opt-in patchers for the agent
frameworks and edge surfaces that most stacks already go through. Each is a
`install()` call at startup that monkey-patches the framework's canonical entry
points, so your existing code emits Prometa spans without decorators.

Every `install()` returns `False` (a clean no-op) when the target library isn't
importable — it is safe to call all of them unconditionally.

```python
from prometa import Prometa
from prometa.integrations import (
    langchain as prometa_langchain,
    langgraph as prometa_langgraph,
    crewai as prometa_crewai,
    semantic_kernel as prometa_sk,
    openai_agents as prometa_openai_agents,
    mcp as prometa_mcp,
    vector as prometa_vector,
)

Prometa(endpoint=..., api_key=..., solution_id=..., agent_name="my-agent")

prometa_langchain.install()
prometa_langgraph.install()
prometa_crewai.install()
prometa_sk.install()
prometa_openai_agents.install()
prometa_mcp.install()
prometa_vector.install_all()   # {'pinecone': True, 'chroma': False, ...}
```

| Module | Patched entry points | Span kinds |
|---|---|---|
| `langchain` | `Runnable.invoke/ainvoke/batch/abatch`, `BaseChatModel.invoke/ainvoke`, `BaseTool.run/arun` | `agent`, `tool`, `task` |
| `langgraph` | `StateGraph` / compiled-graph invocation | `workflow` (nodes nest via the LangChain patch) |
| `crewai` | `Crew.kickoff`/`kickoff_async`, `Agent.execute_task`, `Task.execute_sync`/`execute_async` | `workflow`, `agent`, `task` |
| `semantic_kernel` | `Kernel.invoke*`, `KernelFunction.invoke*`, `agents.Agent.invoke`/`get_response` | `workflow`, `task`, `agent` |
| `openai_agents` | `Runner.run`/`run_sync`, `Agent.run`/`run_sync` | `agent` |
| `mcp` | `mcp.ClientSession.call_tool` | `tool` |
| `vector` | Pinecone `Index.query`, Chroma `Collection.query`, Weaviate query | `retrieval` |

All of these stamp `gen_ai.framework` (`langchain`, `langgraph`, `crewai`,
`semantic-kernel`, `openai-agents`, `mcp`, `pinecone` / `chroma` / `weaviate`),
so you can filter or group by framework in AQL without knowing which patcher
produced the span. They nest correctly under an enclosing
`@prometa.workflow / .agent / .tool` when one is active, and stand alone when
one isn't.

Framework subclasses are walked at install time (LangChain, LangGraph, and
CrewAI users commonly subclass), so `install()` must run **after** your classes
are imported and **before** the first invocation.

### MCP tool calls

`prometa.integrations.mcp` turns each MCP tool invocation into a `tool` span
carrying `mcp.server.name`, `mcp.tool.name`, `mcp.tool.args_count`, and the
`gen_ai.tool.name` / `prometa.tool_name` aliases the platform's Tool registry
keys on. Arguments and results are **not** captured unless the process-wide raw
channel is enabled, in which case they are truncated into `prometa.raw.*`.

Note this is the *client-side observability* patcher, distinct from
`prometa.runtime`'s `GovernedMcpToolBroker`, which is a fail-closed execution
control for signed bundles.

### Vector retrievals

Each `install_*` is independently opt-in and returns whether the client was
importable:

```python
from prometa.integrations import vector as prometa_vector

prometa_vector.install_pinecone()
prometa_vector.install_chroma()
prometa_vector.install_weaviate()
```

Retrieval spans carry OTel semantic `db.*` attributes plus a best-effort
`db.result_count`. For richer AML-scored retrieval evidence (query text, scores,
permission enforcement), use the `retrieval_query` helper described under
[AML v0.4](#aml-v04-instrumentation-contract) instead of, or alongside, this
patcher.

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
| `agent_id` | `PROMETA_AGENT_ID` | none — when set, the SDK emits canonical `prometa.agent_id`; when omitted, the platform auto-registers/attaches the canonical Agent ID from `(orgId, solutionId, agentName)` |
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
   `prometa.agent_id`        (optional)    │           │              │
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
- No required third-party deps; LLM and framework auto-instrumentation
  hooks are opt-in and only run when the corresponding library is installed.
- Optional OpenLLMetry bridge extras require Python ≥ 3.10 because the
  current OpenLLMetry packages do. The `runtime-mcp` extra likewise requires
  Python ≥ 3.10 for the official MCP transport SDK.
- The `runtime*` extras add `cryptography` / `jsonschema` (and `psycopg` for the
  PostgreSQL and host extras) only inside the tenant runtime component.

## Examples

Runnable examples live in [`examples/`](examples/). Each one degrades to a clean
no-op when its optional dependency is missing, so they are all safe to run
against a local Prometa instance:

| Example | Shows |
|---|---|
| [`quickstart.py`](examples/quickstart.py) | end-to-end decorator smoke test |
| [`aml_instrumentation.py`](examples/aml_instrumentation.py) | AML v0.4 helpers wired into a simulated agent turn |
| [`data_flow_refs.py`](examples/data_flow_refs.py) | sibling input/output refs between an LLM and a tool span |
| [`langchain_quickstart.py`](examples/langchain_quickstart.py) | LangChain auto-instrumentation |
| [`crewai_quickstart.py`](examples/crewai_quickstart.py) | CrewAI auto-instrumentation |
| [`openai_agents_quickstart.py`](examples/openai_agents_quickstart.py) | OpenAI Agents SDK auto-instrumentation |
| [`mcp_vector_quickstart.py`](examples/mcp_vector_quickstart.py) | MCP tool calls + Pinecone / Chroma / Weaviate retrievals |
| [`runtime_kernel_quickstart.py`](examples/runtime_kernel_quickstart.py) | admitting and executing one signed model-only bundle |
| [`runtime_conformance_command_driver.py`](examples/runtime_conformance_command_driver.py) | subprocess adapter for `prometa-runtime-conformance --command` |

## Contributing

Use a **`feat/...`** branch for each change set, open a PR to **`main`**, and integrate **only** via GitHub’s PR merge (do not push `main` directly). Details: [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache-2.0
