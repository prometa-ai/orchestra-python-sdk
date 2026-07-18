# Reference tenant runtime host

This image is an optional tenant-plane host for the Phase 2A kernel. It is not
part of the Prometa control plane and does not make a synchronous control-plane
call while serving requests.

The host supports signed, promoted `single-react` bundles through a generic
OpenAI-compatible model gateway and an optional governed MCP broker. MCP
connections, grants, credentials, and egress remain tenant-owned; durable
PostgreSQL call admission and payload-free audit coordinate replicas. Optional
model-only task recovery coordinates request retries across replicas. Resumable
HITL, stored payload replay, memory, A2A, rollout automation, standby failover,
write/destructive MCP topology evidence, and production certification remain
later work.

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

## Published release artifacts

Each SDK release publishes the optional tenant runtime as three independent OCI
artifacts from the exact immutable SDK tag:

```text
ghcr.io/prometa-ai/orchestra-python-sdk/prometa-runtime-host:v0.18.0
ghcr.io/prometa-ai/orchestra-python-sdk/prometa-runtime-host-ubi9:v0.18.0
oci://ghcr.io/prometa-ai/orchestra-python-sdk/charts/prometa-runtime:0.3.1
```

The Debian and UBI9 images are Linux AMD64 release artifacts. The workflow
records their immutable digests, SPDX and CycloneDX SBOMs, keyless signatures,
CycloneDX attestations, and GitHub build provenance. The chart is packaged from
the same tag, binds both image digests in its CycloneDX SBOM, and is separately
signed and attested. Chart and application versions remain independent.
GHCR visibility and access remain organization policy; authenticate the tenant
registry mirror or pull client when these packages are not public.

Operators should resolve and mirror digest references from the workflow's
`release-*.json` evidence, then verify the keyless signature before admission.
For example, after setting `RUNTIME_IMAGE` to an `image@sha256:...` reference:

```bash
cosign verify \
  --certificate-identity-regexp \
  'https://github.com/prometa-ai/orchestra-python-sdk/.github/workflows/publish-runtime-artifacts.yml@refs/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "$RUNTIME_IMAGE"
```

The signatures, SBOMs, and provenance establish release identity and supply
chain evidence. They do not by themselves certify an OpenShift deployment or
replace tenant registry, policy, vulnerability, and fault-testing controls.

## Configuration

Mount one strict JSON document at `/etc/prometa-runtime/config.json`.
`config.example.json` embeds the exact signed Builder bundle and promotion
attestation. `config.pull.example.json` instead names one attestation selected
by tenant CI/CD and retrieves the pair through the outbound bootstrap channel.
`config.mcp.example.json` adds a strict read-only MCP broker to the embedded
shape. The two release sources are mutually exclusive.

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
- MCP credential variables such as `MCP_INTEGRATION_AUTHORIZATION`: required
  only when named by `mcpBroker.credentialBindings`; values stay in a workload
  secret and out of the mounted configuration.

The optional `mcpBroker` block must bind every host connection to the exact
signed `mcpServers` declaration and every configured grant to a signed tool.
It also requires explicit HTTP origins or stdio commands, late-bound credential
names, write/destructive approval and idempotency policy, and a reservation
timeout longer than any tool call. Missing official transport dependencies or a
weakened/mismatched binding fails closed. The stock CLI can execute read-only
tools; write or destructive bundles additionally require a tenant-supplied
`HumanEscalation` adapter through `build_reference_runtime_host()`.

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

An MCP-enabled host uses `prometa_runtime_mcp_idempotency` to reserve calls
across replicas and `prometa_runtime_mcp_audit` for append-only payload-free
decisions. Stale or uncertain reservations become `indeterminate` and cannot be
automatically reacquired. The serving role needs `SELECT, INSERT, UPDATE,
DELETE` on the idempotency table and `INSERT` on the audit table.

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

## Backup, restore, and recovery verification

`operations/backup-postgres.sh` creates an atomic custom-format `pg_dump`
archive and SHA-256 manifest. It uses standard libpq `PGHOST`, `PGPORT`,
`PGDATABASE`, `PGUSER`, and `PGPASSWORD`/`PGPASSFILE` inputs, so credentials are
not placed in command arguments or archive names. The Compose `operations`
profile runs the same script with PostgreSQL 16 client tools:

```bash
PROMETA_RUNTIME_BACKUP_FILE=/backups/runtime-20260712T020000Z.dump \
  docker compose -f deploy/reference-runtime/compose.yaml \
  --profile operations run --rm backup
```

A full runtime database backup is sensitive even though the task ledger is
payload-free: release-cache documents, request-state snapshots, and receipt
outbox records may contain tenant configuration or evidence. Store archives
only on tenant-approved encrypted storage with restricted backup credentials,
retention, replication, deletion, and audit controls. The optional Helm backup
CronJob requires an explicit sensitive-data acknowledgement, a separately
provisioned PVC and database Secret, and explicit database egress.

Restore is deliberately fresh-database-only:

1. Fence the old deployment and database so two runtimes cannot write the same
   restored identity.
2. Create an empty PostgreSQL database and set standard libpq variables for it.
3. Set `PROMETA_RUNTIME_RESTORE_FILE` and
   `PROMETA_RUNTIME_RESTORE_CONFIRM=restore-tenant-runtime`, then run
   `operations/restore-postgres.sh` with matching PostgreSQL client tools.
4. Point `PROMETA_RUNTIME_DATABASE_URL` at the restored database and run
   `prometa-runtime-postgres-verify`. The verifier checks schema v6, required
   payload-free task and MCP columns, migration continuity, lease/status
   projections, ordered task history, and payload-free MCP audit while returning
   only table counts.
5. Start an isolated runtime, exercise health and one controlled request, then
   let tenant CI/CD perform cutover or rollback.

An expired `running` model-only task remains caller-recoverable after restore;
the host still does not retain or replay request/output bodies. The automated
tests prove process-kill reclaim, database-path denial/reconnect, and logical
restore into a fresh database. They do not prove PostgreSQL replication,
managed-service promotion, point-in-time recovery, storage durability, or a
tenant-specific RPO/RTO.

## Kubernetes / OpenShift

The tenant-owned chart lives at `deploy/reference-runtime/chart`. It creates
the host Deployment, internal Service, migration and compatibility hooks,
optional backup CronJob, ServiceAccount, and optional HPA/PDB. It does **not** create runtime
configuration, credentials, backup storage, PostgreSQL, a model gateway, an
ingress, or any Prometa control-plane service.

Start from `values.production.example.yaml`, or
`values.mcp.example.yaml` for the explicit MCP Secret projection and egress
shape. The chart refuses to render until `credentials.existingSecret` and exactly one of
`runtimeConfig.existingSecret` or `runtimeConfig.existingConfigMap` are set:

```bash
helm lint deploy/reference-runtime/chart \
  -f deploy/reference-runtime/chart/values.production.example.yaml

helm upgrade --install tenant-runtime deploy/reference-runtime/chart \
  --namespace tenant-runtime \
  -f deploy/reference-runtime/chart/values.production.example.yaml
```

### Declared OpenShift runtime profile

`chart/values.openshift-production.yaml` is the fail-closed tenant-runtime
overlay for profile `orchestra-ocp-4.20-amd64-v1`. It is intentionally not
installable unchanged. A tenant overlay must provide the immutable UBI9 image
digest, one immutable signed-release config Secret per deployment, separate
runtime and migration credential Secrets, the release rollout ID, and exact
gateway/dependency NetworkPolicy rules.

Before Helm runs, the tenant operator must create the namespace-wide
default-deny policy and the dedicated `migration.serviceAccountName`. The chart
creates a hook-weighted allow policy for the migration and compatibility Jobs,
but it does not create that pre-install ServiceAccount or any Secret. The
profile keeps the runtime behind an internal ClusterIP; the tenant gateway owns
the request edge. Asynchronous receipt delivery may call Orchestra, but
Orchestra remains outside the synchronous production request path.

Build the UBI variant with the pinned build/runtime bases:

```bash
docker build -f deploy/reference-runtime/Dockerfile.ubi \
  -t registry.example.com/orchestra/prometa-runtime-host-ubi9:0.18.0 .
```

Then render with customer-owned values:

```bash
helm template orchestra-runtime deploy/reference-runtime/chart \
  --namespace orchestra-runtime \
  -f deploy/reference-runtime/chart/values.openshift-production.yaml \
  -f customer-orchestra-runtime.yaml
```

The chart verifies immutable digest selection and declared security inputs. It
cannot verify image signatures, SBOMs, provenance, registry mirroring, the
actual contents of referenced objects, or cluster policy enforcement. Those,
plus OpenShift fault, restore, overload, upgrade/rollback, and soak evidence,
remain separate certification gates. This profile is therefore a deployment
contract, not a production-certification claim.

For the MCP example, mount `config.mcp.example.json` through the referenced
runtime config Secret, replace all placeholders with one admitted release, and
provision the separately referenced MCP credential Secret. The chart never
creates, copies, or renders that credential.

The credential Secret must expose the configured runtime database, request API
token, optional model/control-plane/receipt API keys, and migration database
keys. Use an external secret manager or sealed-secret workflow; do not commit
the rendered Secret. Embedded mode stores the exact signed pair in the runtime
config; pull mode stores only the selected attestation ID and non-secret trust
configuration. Use one immutable, versioned config object per deployment and set
`runtimeConfig.rolloutId` to that deployment ID. Changing either the object
reference or rollout ID updates the pod template; mutating an object in place is
not a supported release operation.

Security defaults are deliberately fail-closed:

- no runtime ingress is allowed, and an enabled runtime or migration
  NetworkPolicy refuses to render without explicit destination egress rules;
- the migration and compatibility hooks share a dedicated
  DNS-plus-explicit-egress policy that remains present for the complete hook
  sequence;
- the target runtime image runs a read-only schema compatibility hook after
  migration and before future chart rollback, using the same database-maintenance
  identity and egress policy;
- an enabled backup CronJob has a separate identity and NetworkPolicy, and
  refuses to render without external storage, credentials, sensitive-data
  acknowledgement, retention, and explicit database egress;
- the runtime uses a restricted, read-only, non-root container with no Linux
  capabilities and no mounted Kubernetes API token;
- the chart-created ServiceAccount accepts cloud workload-identity annotations;
- the pre-install migration hook uses `migration.serviceAccountName`, which
  must already exist when it is not the namespace `default` account.

The production example opens only tenant-gateway ingress plus PostgreSQL and
model-gateway egress. The MCP example adds only the declared tenant-tools pod
and port. Add control-plane, telemetry, or receipt-endpoint egress only when the
corresponding path is configured. For external services use a tightly scoped
`ipBlock` or a CNI-supported FQDN policy; Kubernetes NetworkPolicy does not
natively express DNS names.

Enabling the HPA or multiple replicas does not by itself add distributed
request locking. Configure `taskRecovery` to enable the shipped lease and
lifecycle ledger. Even then, model invocation is at-least-once and recovery is
caller-driven; the host does not persist request/output bodies or resume an
HITL/tool checkpoint. Test install, upgrade, rollback, database outage, and
termination behavior in the tenant's actual CNI and ingress topology before
production certification.

## Upgrade and prior-bundle rollback

Database migration and compatibility are separate gates. The migration hook may
advance the fixed schema; `prometa-runtime-postgres-compatibility` then proves
that the target image accepts the installed version and required tables. The
compatibility hook remains available when chart-managed migration is disabled.
The host repeats that read-only check before resolving release material. A newer
unknown schema, a migration gap, or an older schema fails before activation.

A bundle rollback is a new forward deployment, not reuse of stale authorization:

1. Select the previously persisted bundle artifact and obtain a current gate
   decision, required approvals, and a **fresh** promotion attestation.
2. Assign a new release ID, deployment ID, attestation ID, and promotion JTI.
   The exact prior bundle digest and bundle JTI remain unchanged.
3. Create a new immutable config Secret or ConfigMap and set
   `runtimeConfig.rolloutId` to the new deployment ID.
4. Run `helm upgrade`, wait for the compatibility hook and Deployment readiness,
   then retain the resulting admission/active receipts as rollout evidence.

Do not use a blind `helm rollback` to revive an expired or revoked attestation.
Helm rollback is appropriate for chart/image state only when the target revision
contains a valid freshly authorized config and its pre-rollback compatibility
hook accepts the current database. Tenant CI/CD remains the deployment authority.
Helm hook resources are not release-managed: the maintenance NetworkPolicy is
replaced before each hook operation and must be removed by the tenant's uninstall
cleanup after the release is deleted.

The repository runs a repeatable source-baseline drill in CI:

```bash
export PROMETA_RUNTIME_TEST_POSTGRES_DSN='postgresql://...'
deploy/reference-runtime/ci/upgrade-rollback-drill.sh
```

`compatibility-baselines.json` pins chart `0.1.0` commit `51e2faa` at schema v2.
The drill starts release A on that source, migrates to v6 and starts release B on
current code, then starts the baseline host against v6 with release A's exact
bundle bytes and a fresh rollback promotion/deployment. It verifies three
immutable activation rows and zero synchronous control-plane calls. Because the
baseline was not a separately published artifact, this is not release-channel,
Kubernetes CNI, managed-database, or production certification.

## K3s kube-router topology certification profiles

The repository runs two pinned, repeatable tenant-cluster reference profiles.
They are intentionally narrower than production certification and share the
same K3d, K3s, PostgreSQL, runtime, and chart pins.

### Model-only profile

- K3d `v5.8.3` with K3s `v1.34.8+k3s1` and its embedded kube-router
  NetworkPolicy controller;
- one server plus one agent node;
- two isolated tenant topologies with two runtime replicas each;
- signed bundle schema/runtime contract v2 admission with exact capability
  ranges and independently recomputed policy/configuration digests;
- the real chart migration and target-image compatibility hooks;
- restricted pod security for runtime, gateway, and model fixtures;
- authorized gateway ingress, same-namespace pod-label denial, cross-tenant
  ingress denial, and own-dependency versus cross-tenant egress checks;
- 24 unique concurrent requests per tenant and 12 simultaneous duplicate
  attempts, with exactly one winner and one model invocation;
- a live database-egress partition that fails before model invocation, leaves
  the other tenant healthy, and recovers after policy restoration; and
- a runtime-pod replacement that joins the existing activation, serves load,
  preserves two-node spread, and retains prior payload-free task status.

Run it from the repository root with Docker, Helm, kubectl, and Python already
available:

```bash
deploy/reference-runtime/ci/install-k3d.sh .tmp/k3d
K3D="$PWD/.tmp/k3d" \
  PROMETA_RUNTIME_TOPOLOGY_REPORT=runtime-topology-certification.json \
  deploy/reference-runtime/ci/topology-certification.sh
```

[`topology-profiles.json`](topology-profiles.json) pins the K3d binary
checksums, runtime/chart versions, and upstream K3s/PostgreSQL image digests.
The harness refuses version drift, verifies the upstream PostgreSQL digest,
then normalizes it to a single-platform local image so Docker Desktop and Linux
runners import the same OCI content into every K3s node. It verifies both
imported images on every node before applying any workload.

### Read-only MCP profile

[`topology-profiles.mcp.json`](topology-profiles.mcp.json) adds a distinct
`mcp-read-only` workload without weakening the model-only profile. It proves:

- exact signed bundle binding for one low-risk, read-only tool through the
  official stateless Streamable HTTP transport;
- separate runtime-client and MCP-server Secret projections plus a rollout
  requirement after credential rotation;
- runtime-to-own-tools ingress and egress while same-tenant rogue and
  cross-tenant callers remain denied;
- PostgreSQL-backed one-winner call admission across two runtime replicas;
- payload-free MCP audit persistence across runtime pod replacement;
- fail-closed stale-credential handling, indeterminate call quarantine, and
  denial of automatic replay after the runtime adopts the rotated Secret; and
- tenant isolation throughout rotation, partition, and recovery drills.

Run the MCP profile by selecting it explicitly:

```bash
K3D="$PWD/.tmp/k3d" \
  PROMETA_RUNTIME_TOPOLOGY_PROFILE="$PWD/deploy/reference-runtime/topology-profiles.mcp.json" \
  PROMETA_RUNTIME_TOPOLOGY_REPORT=runtime-mcp-topology-certification.json \
  deploy/reference-runtime/ci/topology-certification.sh
```

This profile certifies only the stock host's read-only MCP contract. It does
not certify write/destructive tools, resumable approval, tool-result replay,
or exactly-once execution.

The resulting report contains profile/version identifiers, runtime contract
and bundle schema versions, digest-binding booleans, counts, and boolean checks
only. Ephemeral bundle signatures, API tokens, database credentials, request
bodies, and model outputs are never retained in the report. The
profile makes no claim about OpenShift, managed Kubernetes CNIs, managed
PostgreSQL failover/PITR, production ingress/TLS, autoscaling, overload
fairness, storage durability, air-gap installation, or a tenant-specific
RPO/RTO. Those environments still require their own certification evidence.

### Optional live Orchestra receipt proof

The same harness can additionally prove asynchronous lifecycle-receipt delivery
against a running Orchestra platform container. This mode is intentionally not
part of the SDK-only job: it needs a platform database fixture whose release and
attestation IDs match the dynamically signed tenant bundles.

Set `PROMETA_RUNTIME_TOPOLOGY_RECEIPT_PROOF=true` together with:

- `PROMETA_RUNTIME_TOPOLOGY_PLATFORM_CONTAINER`, the running platform container;
- `PROMETA_RUNTIME_TOPOLOGY_PLATFORM_VERIFY_URL`, its operator-reachable base URL;
- `PROMETA_RUNTIME_TOPOLOGY_PLATFORM_PROVISIONER`, an executable accepting
  `setup --fixture <path>` and `cleanup --fixture <path>`.

The harness connects only that container to the ephemeral K3d network, adds its
exact IPv4 `/32` and port `3000` to each runtime NetworkPolicy, provisions
separate `runtime:write` and `release:read` keys per tenant, and removes the
fixture on exit. It requires two delivered outbox rows per tenant, the complete
`admitted`/`active` platform projection, exact contract-v2 policy/configuration
digest binding on every receipt, a rejected binding mismatch, and both read-
and write-side tenant isolation. The retained report still records zero
synchronous control-plane calls and never includes keys or signed payloads.

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
commit, so a recovered attempt may call the model again. Tool-bearing releases
cannot enable `taskRecovery`; their per-call MCP reservations use the separate
fail-closed indeterminate contract. Automatic background replay, encrypted
payload/result retention, and resumable HITL checkpoints are not part of
lifecycle v1.

## Activation semantics

The first replica atomically creates an immutable PostgreSQL activation for the
tenant, runtime, deployment, release, bundle JTI, and promotion JTI. Exact
replicas and restarts join that activation. A fresh promotion may authorize the
same bundle bytes for redeploy or rollback. Changed activation identity,
promotion-JTI reuse, or a bundle JTI bound to another digest fails closed.
