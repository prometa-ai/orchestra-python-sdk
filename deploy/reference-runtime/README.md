# Reference tenant runtime host

This image is an optional tenant-plane host for the Phase 2A kernel. It is not
part of the Prometa control plane and does not make a synchronous control-plane
call while serving requests.

The host supports signed, promoted, model-only `single-react` bundles through a
generic OpenAI-compatible model gateway. Optional payload-free task recovery
coordinates retries across replicas. Concrete MCP, resumable HITL, stored
payload replay, memory, A2A, rollout automation, and production certification
remain later work.

## Build and conformance

Build from the SDK repository root:

```bash
docker build -f deploy/reference-runtime/Dockerfile \
  -t prometa-runtime-host:0.18.0 .
```

Run the combined profile through a fresh container process per case:

```bash
prometa-runtime-conformance \
  --profile deployment \
  --driver-name reference-host-container \
  --command "docker run --rm -i --entrypoint prometa-runtime-host-conformance-driver prometa-runtime-host:0.18.0"
```

A green report proves the packaged admission, execution, failure, and
zero-synchronous-control-plane cases. It is deployment evidence, not topology
chaos or production certification.

## Configuration

Mount one strict JSON document at `/etc/prometa-runtime/config.json`.
`config.example.json` embeds the exact signed Builder bundle and promotion
attestation. `config.pull.example.json` instead names one attestation selected
by tenant CI/CD and retrieves the pair through the outbound bootstrap channel.
The two release sources are mutually exclusive.

The JSON file contains public trust material and immutable rollout identity,
not credentials. Supply these through the environment or a workload secret
provider:

- `PROMETA_RUNTIME_DATABASE_URL`: tenant PostgreSQL DSN;
- `PROMETA_RUNTIME_API_TOKEN`: at least 32 bytes, required by the request API;
- `MODEL_GATEWAY_API_KEY`: required only when `modelGateway.apiKeyEnv` names it.
- `ORCHESTRA_RUNTIME_CONTROL_PLANE_API_KEY`: required only when
  `controlPlanePull.apiKeyEnv` names it; use a narrow `runtime:read` key.
- `ORCHESTRA_RUNTIME_RECEIPT_API_KEY`: required only when the optional
  `receiptDelivery.apiKeyEnv` names it; use a narrow `runtime:write` key.

To enable asynchronous lifecycle receipts, add this optional block to the
mounted configuration. HTTPS is required unless `allowInsecureHttp` is set
explicitly for a local/test endpoint:

```json
{
  "receiptDelivery": {
    "baseUrl": "https://orchestra.example.com",
    "apiKeyEnv": "ORCHESTRA_RUNTIME_RECEIPT_API_KEY",
    "timeoutSeconds": 5,
    "pollIntervalSeconds": 2,
    "leaseSeconds": 30,
    "initialBackoffSeconds": 1,
    "maxBackoffSeconds": 300
  }
}
```

The host durably enqueues deterministic deployment-level `admitted` and
`active` receipts in tenant PostgreSQL. Replicas lease delivery with
`SKIP LOCKED`; transport, 429, and 5xx failures back off without affecting
readiness or request execution, while permanent 4xx responses are retained as
dead letters with payload-free evidence. Pod termination does not emit a
deployment-level `stopped` receipt.

Pull mode is bootstrap-only. The host refuses redirects, requires HTTPS unless
local/test configuration explicitly opts into HTTP, and rejects a stale
`checkedAt` outside `maxClockSkewSeconds`. It retrieves one atomic handoff from
`/api/runtime-releases/{attestationId}`, performs the normal
local cryptographic admission, and records verified bytes in
`prometa_runtime_release_cache`. A retryable transport/server outage may use
that tenant-side cache only within `maxCacheAgeSeconds` and the signed offline
lease. Terminal 4xx, revocation, binding, or signature failures fail closed.
The control plane is never called while a runtime request is being served.

To enable lifecycle contract v1, add this optional block. The lease must be
strictly longer than `requestTimeoutSeconds`:

```json
{
  "taskRecovery": {
    "leaseSeconds": 90,
    "maxAttempts": 3,
    "historyLimit": 50
  }
}
```

The host then uses `prometa_runtime_task` for atomic cross-replica claims and
`prometa_runtime_task_event` for ordered transitions. Both are payload-free:
only canonical input/output digests, immutable release identity, model
metadata, stable error codes, attempts, leases, and timestamps are retained.
PostgreSQL's transaction clock is authoritative for production lease expiry.
The serving role needs `SELECT, INSERT, UPDATE` on the task table and
`SELECT, INSERT` on the event table.

Install the database schema with a migration identity before starting the
lower-privilege host:

```bash
prometa-runtime-postgres-init
prometa-runtime-host --config /etc/prometa-runtime/config.json
```

`compose.yaml` demonstrates that ordering with PostgreSQL 16. Bind the request
port only to the tenant gateway or private network in production. This first
host does not terminate TLS, implement a distributed rate limit, or prove
overload fairness; the tenant gateway and deployment topology own those controls.

## Kubernetes / OpenShift

The tenant-owned chart lives at `deploy/reference-runtime/chart`. It creates
the host Deployment, internal Service, optional migration hook, ServiceAccount,
and optional HPA/PDB. It does **not** create runtime configuration, credentials,
PostgreSQL, a model gateway, an ingress, or any Prometa control-plane service.

Start from `values.production.example.yaml`. The chart refuses to render until
`credentials.existingSecret` and exactly one of
`runtimeConfig.existingSecret` or `runtimeConfig.existingConfigMap` are set:

```bash
helm lint deploy/reference-runtime/chart \
  -f deploy/reference-runtime/chart/values.production.example.yaml

helm upgrade --install tenant-runtime deploy/reference-runtime/chart \
  --namespace tenant-runtime \
  -f deploy/reference-runtime/chart/values.production.example.yaml
```

The credential Secret must expose the configured runtime database, request API
token, optional model/control-plane/receipt API keys, and migration database
keys. Use an external secret manager or sealed-secret workflow; do not commit
the rendered Secret. Embedded mode stores the exact signed pair in the runtime
config; pull mode stores only the selected attestation ID and non-secret trust
configuration. Updating either source does not automatically restart pods, so
tenant CI/CD must update the object and roll the Deployment as one promotion
operation.

Security defaults are deliberately fail-closed:

- no runtime ingress is allowed, and an enabled runtime or migration
  NetworkPolicy refuses to render without explicit destination egress rules;
- the migration hook has its own temporary DNS-plus-explicit-egress policy;
- the runtime uses a restricted, read-only, non-root container with no Linux
  capabilities and no mounted Kubernetes API token;
- the chart-created ServiceAccount accepts cloud workload-identity annotations;
- the pre-install migration hook uses `migration.serviceAccountName`, which
  must already exist when it is not the namespace `default` account.

The production example opens only tenant-gateway ingress plus PostgreSQL and
model-gateway egress. Add control-plane, telemetry, or receipt-endpoint egress
only when the corresponding path is configured. For external services use a
tightly scoped `ipBlock` or a CNI-supported FQDN policy; Kubernetes
NetworkPolicy does not natively express DNS names.

Enabling the HPA or multiple replicas does not by itself add distributed
request locking. Configure `taskRecovery` to enable the shipped lease and
lifecycle ledger. Even then, model invocation is at-least-once and recovery is
caller-driven; the host does not persist request/output bodies or resume an
HITL/tool checkpoint. Test install, upgrade, rollback, database outage, and
termination behavior in the tenant's actual CNI and ingress topology before
production certification.

## Request API

- `GET /healthz`: process liveness;
- `GET /readyz`: payload-free readiness;
- `GET /v1/runtime/tasks/{requestId}`: authenticated payload-free lifecycle
  projection when task recovery is configured;
- `POST /v1/runtime/execute`: bearer-authenticated execution.

```json
{
  "requestId": "tenant-request-123",
  "input": {"question": "Where is my order?"}
}
```

Requests are strict JSON, bounded by `maxRequestBytes`, schema-validated before
model invocation, and subject to a host timeout. Without `taskRecovery`,
duplicate request IDs are rejected only inside one replica. With it, one active
lease wins across replicas; retryable failures can be retried immediately and
orphaned work can be reclaimed after lease expiry. The caller must resubmit the
same request ID and exact input digest. Completed tasks return
`task_already_completed`; the lifecycle endpoint reports completion metadata
but never replays the response body.

This is cross-replica coordination and lifecycle replay, not exactly-once
inference. A process can fail after a model call and before the completion
commit, so a recovered attempt may call the model again. Side-effecting tools,
automatic background replay, encrypted payload/result retention, and resumable
HITL checkpoints are not part of lifecycle v1.

## Activation semantics

The first replica atomically creates an immutable PostgreSQL activation for the
tenant, runtime, deployment, release, bundle JTI, and promotion JTI. Exact
replicas and restarts join that activation. A fresh promotion may authorize the
same bundle bytes for redeploy or rollback. Changed activation identity,
promotion-JTI reuse, or a bundle JTI bound to another digest fails closed.
