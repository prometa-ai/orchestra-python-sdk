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
token, optional model API key, and migration database keys. Use an external
secret manager or sealed-secret workflow; do not commit the rendered Secret.
The runtime config object contains the exact signed bundle and promotion
attestation. Updating either external object does not automatically restart
pods, so tenant CI/CD must update the object and roll the Deployment as one
promotion operation.

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
model-gateway egress. Add telemetry egress only when a configured evidence
emitter needs it. For external services use a tightly scoped `ipBlock` or a
CNI-supported FQDN policy; Kubernetes NetworkPolicy does not natively express
DNS names.

Enabling the HPA or multiple replicas does not add distributed request locking,
exactly-once execution, or resumable task state. PostgreSQL activation and
state sharing remain the implemented durability boundary. Test install,
upgrade, rollback, database outage, and termination behavior in the tenant's
actual CNI and ingress topology before production certification.

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
