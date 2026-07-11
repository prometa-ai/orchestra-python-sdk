# Reference tenant runtime host

This image is an optional tenant-plane host for the Phase 2A kernel. It is not
part of the Prometa control plane and does not make a synchronous control-plane
call while serving requests.

The first host slice supports signed, promoted, model-only `single-react`
bundles through a generic OpenAI-compatible model gateway. Concrete MCP, HITL,
memory, A2A, rollout automation, and production certification remain later
work.

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

Mount one strict JSON document at `/etc/prometa-runtime/config.json`; see
`config.example.json` for the versioned shape. Replace both empty artifact
objects with the exact signed Builder bundle and promotion attestation.

The JSON file contains public trust material and immutable rollout identity,
not credentials. Supply these through the environment or a workload secret
provider:

- `PROMETA_RUNTIME_DATABASE_URL`: tenant PostgreSQL DSN;
- `PROMETA_RUNTIME_API_TOKEN`: at least 32 bytes, required by the request API;
- `MODEL_GATEWAY_API_KEY`: required only when `modelGateway.apiKeyEnv` names it.

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

## Request API

- `GET /healthz`: process liveness;
- `GET /readyz`: payload-free readiness;
- `POST /v1/runtime/execute`: bearer-authenticated execution.

```json
{
  "requestId": "tenant-request-123",
  "input": {"question": "Where is my order?"}
}
```

Requests are strict JSON, bounded by `maxRequestBytes`, schema-validated before
model invocation, and subject to a host timeout. Duplicate request IDs are
rejected while one replica is processing them. This slice does not claim
cross-replica exactly-once execution or resumable task/HITL state.

## Activation semantics

The first replica atomically creates an immutable PostgreSQL activation for the
tenant, runtime, deployment, release, bundle JTI, and promotion JTI. Exact
replicas and restarts join that activation. A fresh promotion may authorize the
same bundle bytes for redeploy or rollback. Changed activation identity,
promotion-JTI reuse, or a bundle JTI bound to another digest fails closed.
