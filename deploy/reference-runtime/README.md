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
  -t prometa-runtime-host:0.18.4 .
```

Run the combined profile through a fresh container process per case:

```bash
prometa-runtime-conformance \
  --profile deployment \
  --driver-name reference-host-container \
  --command "docker run --rm -i --entrypoint prometa-runtime-host-conformance-driver prometa-runtime-host:0.18.4"
```

A green report proves the packaged admission, execution, failure, and
zero-synchronous-control-plane cases. It is deployment evidence, not topology
chaos or production certification.

## Published release artifacts

Each SDK release publishes the optional tenant runtime as three independent OCI
artifacts from the exact immutable SDK tag:

```text
ghcr.io/prometa-ai/orchestra-python-sdk/prometa-runtime-host:v0.18.4
ghcr.io/prometa-ai/orchestra-python-sdk/prometa-runtime-host-ubi9:v0.18.4
oci://ghcr.io/prometa-ai/orchestra-python-sdk/charts/prometa-runtime:0.3.5
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
- `PROMETA_RUNTIME_EDGE_OVERLOAD_CONTRACT`: optional outside the declared
  production profile; the only supported value is
  `orchestra-runtime-edge-overload-v1`.
- `MODEL_GATEWAY_API_KEY`: required only when `modelGateway.apiKeyEnv` names it.
- `ORCHESTRA_RUNTIME_CONTROL_PLANE_API_KEY`: required only when
  `controlPlanePull.apiKeyEnv` names it; use a narrow `runtime:read` key.
- `ORCHESTRA_RUNTIME_RECEIPT_API_KEY`: required only when the optional
  `receiptDelivery.apiKeyEnv` names it; use a narrow `runtime:write` key.
- `ORCHESTRA_SECURITY_DECISION_API_KEY`: required when a guarded v2 release
  uses `securityDecisionDelivery.apiKeyEnv`; use a key limited to
  `security-decisions:write`.
- MCP credential variables such as `MCP_INTEGRATION_AUTHORIZATION`: required
  only when named by `mcpBroker.credentialBindings`; values stay in a workload
  secret and out of the mounted configuration.

The optional `mcpBroker` block must bind every host connection to the exact
signed `mcpServers` declaration and every configured grant to a signed tool.
It also requires explicit HTTP origins or stdio commands, late-bound credential
names, write/destructive approval and idempotency policy, and a reservation
timeout longer than any tool call. Missing official transport dependencies or a
weakened/mismatched binding fails closed. The stock CLI can execute read-only
tools; write or destructive bundles additionally require a reviewer, configured
through the optional `humanEscalation` block below or injected through
`build_reference_runtime_host()`.

Two optional sub-blocks harden the broker further:

```json
{
  "mcpBroker": {
    "toolLimits": {
      "default": {
        "maxCallsPerWindow": 60,
        "windowSeconds": 60,
        "maxConcurrentCalls": 4
      },
      "perTool": {
        "orders.lookup": { "maxCallsPerWindow": 10, "maxConcurrentCalls": 2 }
      }
    },
    "toolDescriptors": { "requireSignedDigest": false, "cacheSeconds": 300 }
  }
}
```

`toolLimits` bounds per-tool call rate and in-flight calls **within one
replica**; N replicas admit N times these numbers, and distributed rate
limiting stays with the tenant gateway. Omit the block and no ceiling applies:
the counters aggregate every request a replica serves at once, so a ceiling
under this runtime's own request concurrency refuses ordinary traffic — size
`maxConcurrentCalls` above the concurrency one replica accepts. Every name
under `perTool` must be a configured grant.

`toolDescriptors.cacheSeconds` is how long a server's advertised tool listing
is reused before drift is re-checked, and therefore the longest a swapped tool
description can go unnoticed. It also decides whether a credential the server
has since rotated is caught by the listing re-read or by the tool call itself,
which is the difference between a knowably-no-side-effect refusal and an
indeterminate one.

`requireSignedDigest: true` refuses any MCP tool whose signed declaration does
not pin `mcpToolDescriptorDigest`. When a tool does pin it, the value is inside
the signed configuration-digest projection — the same projection the control
plane signs — so the two sides compute the same digest.

To require a human reviewer for write and destructive tools, add:

```json
{
  "humanEscalation": {
    "baseUrl": "https://orchestra.example.com",
    "apiKeyEnv": "ORCHESTRA_APPROVAL_API_KEY",
    "pollIntervalSeconds": 2,
    "decisionTimeoutSeconds": 300,
    "localDirectory": "/var/lib/prometa/approvals"
  }
}
```

Either `baseUrl` (with `apiKeyEnv`) or `localDirectory` is required; supplying
both falls back to the local directory when the control plane is unreachable.
`allowInsecureHttp` permits an `http://` `baseUrl` for a local or test approval
endpoint, and is passed through to the client, so the whole block agrees on
one answer instead of accepting the option and then refusing to start.
Requests carry identities, the tool, and a payload digest — never the payload
itself. A reviewer who does not answer inside `decisionTimeoutSeconds` fails
the step. Anything able to write into `localDirectory/decisions` can approve a
destructive call, so mount it with the same care as a credential, and write
decision files atomically — a half-written file is read as an invalid decision
and fails the step.

To enable the operator kill switch, add `runtimeControl` alongside
`controlPlanePull` (it is carried on that same pull and requires it):

```json
{
  "runtimeControl": {
    "refreshIntervalSeconds": 60,
    "maxLeaseSeconds": 900,
    "staleAction": "continue"
  }
}
```

The lease's signing key is provisioned in `promotionTrust` as its own entry
declaring `"allowedArtifactTypes": ["orchestra.runtime-control-lease"]`. The
host refuses to start with `runtime_control_signing_key_missing` if no entry
declares that purpose, because a kill switch whose every lease is refused is
configured and inert. That entry must not set `allowedAudiences`: a lease
carries no audience claim, and a constraint that cannot be evaluated is
refused rather than skipped.

An enforcing lease whose `quarantined` control names a scope this pod holds an
identity for makes `POST /v1/runtime/execute` answer `503` with
`runtime_quarantined`. A control naming another subject, a scope this pod
cannot resolve, or a matched control that says `serving` is not enforcement and
is not counted as enforcement. An `advisory` lease never refuses anything,
however stale it gets.

A lease whose `revision` is below the highest this pod has accepted is refused
as out-of-order whatever `leaseId` it carries: revisions are ordered globally
per `(orgId, targetEnvironment)`, so a captured `serving` lease cannot lift a
live quarantine by arriving under a fresh lease stream.

`GET /readyz` answers `503` while not admitting, but carries counts and
booleans only — it is unauthenticated, so it never names the lease, its expiry,
the subject, or the operator's reason. It is an operator view, not a probe: all
three chart probes use `/healthz`, so a quarantined pod keeps its Service
endpoint and answers every request with the typed `503` and the full control
state. Gating readiness on `/readyz` instead would empty the Service under a
fleet-wide quarantine, and callers would get a connection error indistinguishable
from a crash — the one outcome the typed refusal exists to prevent. It would add
no enforcement either: the probe and the refusal both read the same gate in the
same process, so a replica dishonest enough to admit quarantined work would
report itself ready anyway. Holding a compromised replica out of service is a
job for NetworkPolicy or the endpoint, not for that replica's own probe.

Quarantine keeps applying after the lease expires, so it cannot be evaded by
cutting the runtime off from the control plane. A `serving` runtime whose lease
expires keeps serving and reports `stale` — a control-plane outage must not
become an inference outage. The signed `staleAction: "stop"` inverts that
second rule for deployments that prefer a hard stop; the cost is that the
runtime stops whenever refreshes stop, including on restart during an outage,
because the lease is not persisted across restarts. The `staleAction` in this
mounted block applies only until the first lease is adopted; after that the
signed one governs.

Every refresh acknowledges through the receipt outbox, not only state edges, so
a pod sitting steady in `quarantined` stays in the enforcement count.
Acknowledgements travel under the additive top-level `runtimeControlAck` key,
in the nine-member shape the lease contract pins; repeats within one revision
collapse to a single outbox row, while a pod that goes stale and recovers under
one revision reports both.

Each refresh re-reads the whole release handoff, because the lease rides that
response — 8.5x the lease's own bytes for a handoff built from the SDK's
bundle, promotion-attestation and lease fixtures, and
`controlPlaneMaxResponseBytes` allows up to 12 MiB for a real release. Raise
`refreshIntervalSeconds` to trade detection latency for bandwidth.

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

Guarded bundles declare `security.decision.emit.v1` and will not admit unless
the host has a durable decision emitter. Add the delivery block below and
provision its separately scoped key:

```json
{
  "securityDecisionDelivery": {
    "baseUrl": "https://orchestra.example.com",
    "apiKeyEnv": "ORCHESTRA_SECURITY_DECISION_API_KEY",
    "timeoutSeconds": 5,
    "pollIntervalSeconds": 2,
    "leaseSeconds": 30,
    "initialBackoffSeconds": 1,
    "maxBackoffSeconds": 300
  }
}
```

The request path evaluates and applies the signed mode, thresholds, and action
without a Prometa call. It commits one strict, content-minimized decision per
applicable guardrail to
`prometa_runtime_security_decision_outbox`; a background worker then posts
batches to `/api/security/decision-batches`. Network failures back off without
changing readiness, while a local persistence failure fails the guarded
request because the declared evidence capability could not be satisfied.
Campaign callers may supply `X-Prometa-Campaign-Id`,
`X-Prometa-Campaign-Run-Id`, and `X-Prometa-Probe-Id`; only these bounded IDs
are retained for cross-plane correlation.

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
port only to the tenant gateway or private network in production. Plain HTTP is
the backward-compatible default. Set `PROMETA_RUNTIME_SERVER_TLS_CERT_FILE` and
`PROMETA_RUNTIME_SERVER_TLS_KEY_FILE` together to terminate TLS in the host.
Optional mTLS additionally sets `PROMETA_RUNTIME_SERVER_TLS_CLIENT_CA_FILE` and
`PROMETA_RUNTIME_SERVER_TLS_REQUIRE_CLIENT_CERTIFICATE=true`; malformed or
incomplete material fails startup. The bearer-token boundary remains active
after the handshake. The host does not implement a distributed rate limit or
prove overload fairness; the tenant gateway and deployment topology own those controls.
When `PROMETA_RUNTIME_EDGE_OVERLOAD_CONTRACT` selects
`orchestra-runtime-edge-overload-v1`, the host accepts only the chat-completions
model workload, honors normalized `Retry-After` delays within its 30-second
budget, skips longer waits, and stamps the contract ID on runtime evidence.
This is per-request protection, not distributed admission control: tenant edge
infrastructure still owns fairness, queueing, load shedding, and autoscaling.

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
payload-free: release-cache documents, request-state snapshots, receipt outbox
records, and minimized security-decision evidence may contain tenant
configuration or operational metadata. Store archives
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
   `prometa-runtime-postgres-verify`. The verifier checks schema v7, required
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

### OpenShift SNO engineering trial

`chart/values.openshift-sno-trial.yaml` is the source-only,
non-certifying runtime overlay for
`orchestra-ocp-sno-trial-amd64-v1`. It is deliberately sized for the
single-node, budget-capped lab and is not the production profile. The overlay
keeps one replica, disables HPA, PDB, topology spread and backups, and fixes
runtime requests at `100m` CPU and `128Mi` memory with limits of `500m` CPU and
`512Mi` memory.

The chart refuses to render this overlay without an immutable UBI image
digest. It also freezes Secret-backed runtime, credential, server-certificate
and migration inputs; an external CA ConfigMap; native HTTPS serving; the
runtime edge overload contract; a distinct migration identity; and explicit
tenant gateway, PostgreSQL, model gateway, asynchronous receipt/telemetry,
read-only MCP and OpenShift DNS paths. The control-plane pull credential stays
optional so Orchestra remains outside the synchronous request path.

Run the source contract locally or in CI:

```bash
deploy/reference-runtime/ci/render-sno-trial-profile.sh \
  /tmp/orchestra-runtime-sno.yaml
```

This proves Helm rendering and negative admission cases only. It does not
prove that a released image exists, that OpenShift admits it, or that any live
recovery, load, restore, RPO/RTO or availability requirement passes.

### Staging company-workflow proof

`Dockerfile.workflow-proof` packages an opt-in fixture for proving a published
Company Workflow Ontology against the real runtime v3 admission, evaluator,
PostgreSQL state/CAS ledger, MCP idempotency store and asynchronous receipt and
workflow-decision outboxes. The process refuses to start unless:

- `PROMETA_RUNTIME_WORKFLOW_PROOF=enabled`;
- the runtime environment is exactly `staging`;
- the model gateway is the fixture-local loopback endpoint;
- receipt and workflow-decision delivery are configured; and
- a bounded tenant approval is supplied through
  `PROMETA_WORKFLOW_PROOF_APPROVAL_JSON`.

The fixture's SAP transport is deterministic and local; it never connects to
SAP. Inputs containing `simulate-timeout` exercise an outcome-unknown boundary,
while `simulate-postcondition-fail` exercises a result that cannot satisfy the
published transition postcondition. Both cases fail closed and are expected to
leave the workflow instance quarantined rather than replaying the side effect.

Build it only for a staging proof:

```bash
gcloud builds submit \
  --config deploy/reference-runtime/cloudbuild.workflow-proof.yaml \
  --substitutions _VCS_REF="$(git rev-parse HEAD)",_IMAGE_VERSION=0.20.0,_IMAGE=REGISTRY/IMAGE:TAG
```

The resulting evidence is a staging runtime proof. It does not enable a real
SAP write, replace tenant approval infrastructure, or satisfy the production
activation and OpenShift certification gates.

### Declared OpenShift runtime profile

`chart/values.openshift-production.yaml` is the fail-closed tenant-runtime
overlay for profile `orchestra-ocp-4.20-amd64-v1`. It is intentionally not
installable unchanged. A tenant overlay must provide the immutable UBI9 image
digest, one immutable signed-release config Secret per deployment, separate
runtime and migration credential Secrets, the release rollout ID, and exact
gateway/dependency NetworkPolicy rules.

The OpenShift profile also requires `serverTls.enabled=true`, an existing
certificate Secret, and a bounded `serverTls.rolloutId`. The chart never creates
certificate material. Ordinary TLS uses kubelet HTTPS probes. Enabling
`serverTls.requireClientCertificate` additionally requires a separate
least-privilege `serverTls.probeClient.existingSecret`; exec probes use that
identity because kubelet HTTP probes cannot present a client certificate. The
server and probe Secrets can be independently rotated by cert-manager or an
external Secret operator, with rollout IDs driving process restart.

The overlay also pins
`runtimeEdge.overloadContract=orchestra-runtime-edge-overload-v1`. Helm rejects
an unknown or missing production contract and prevents `extraEnv` from
overriding it. Host startup fails if that contract is paired with a model path
other than `/v1/chat/completions`. The tenant gateway remains the owner of
distributed overload controls.

Before Helm runs, the tenant operator must create the namespace-wide
default-deny policy and the dedicated `migration.serviceAccountName`. The chart
creates a hook-weighted allow policy for the migration and compatibility Jobs,
but it does not create that pre-install ServiceAccount or any Secret. The
profile keeps the runtime behind an internal ClusterIP; the tenant gateway owns
the request edge. Asynchronous receipt and security-decision delivery may call
Orchestra, but Orchestra remains outside the synchronous production request
path.

Build the UBI variant with the pinned build/runtime bases:

```bash
docker build -f deploy/reference-runtime/Dockerfile.ubi \
  -t registry.example.com/orchestra/prometa-runtime-host-ubi9:0.18.4 .
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
token, optional model/control-plane/receipt/security-decision API keys, and
migration database keys. Use an external secret manager or sealed-secret
workflow; do not commit
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
and port. Add control-plane, telemetry, receipt, or security-decision endpoint
egress only when the corresponding path is configured. For external services
use a tightly scoped
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
The drill starts release A on that source, migrates to v7 and starts release B on
current code, then starts the baseline host against v7 with release A's exact
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
imported images on every node before applying any workload. Each K3d import
attempt is terminated after 120 seconds by default so a broken Docker stream
cannot consume the full certification-job deadline; override the bound with
`PROMETA_K3D_IMAGE_IMPORT_TIMEOUT_SECONDS` when slower local storage requires
it.

### Published-artifact install proof

The manual `Verify published runtime install` workflow closes a separate proof
gap: it exercises the immutable UBI image and packaged chart downloaded from an
exact GitHub release instead of rebuilding either artifact from the checkout.
For the selected tag it:

- checks out the exact tagged source and requires an exact tag match;
- verifies GitHub release asset SHA-256 digests and their embedded source
  revision;
- verifies the consumed image and chart signatures plus CycloneDX attestations;
- installs the chart package using the runtime image's immutable digest; and
- runs the model-only K3d topology, recording the tag, source revision, runtime
  digest, chart OCI digest, and chart-package SHA-256 in payload-free evidence.

Dispatch it with an exact release tag:

```bash
gh workflow run runtime-published-install.yml \
  --repo prometa-ai/orchestra-python-sdk \
  --ref main \
  -f source_tag=v0.18.0
```

This is release-channel installation and K3d behavioral evidence. It remains
`reference-profile-not-production-certification`: it does not prove OpenShift,
customer registry mirroring, managed dependencies, fault recovery, RPO/RTO, or
soak.

### Published-artifact upgrade and rollback proof

The release coordinator runs the full CI matrix against the exact source tag
before dispatching package or runtime publication. After OCI verification, the
publisher creates a digest-checked GitHub Release containing immutable image
metadata, the packaged chart, and CycloneDX/SPDX SBOMs. Re-running publication
in verification-only mode can add missing assets, but it refuses to overwrite
an existing asset whose digest differs. The install and transition drills use
that release as their discovery boundary and independently re-verify the OCI
signatures and attestations.

The manual `Verify published runtime upgrade and rollback` workflow accepts an
older baseline tag and a newer target tag. It verifies both signed release sets,
establishes the exact baseline K3d topology, then executes three forward Helm
deployments:

1. the baseline chart/image with release A;
2. the target chart/image with release B, including target-image migration and
   compatibility hooks; and
3. the baseline chart/image with release A's exact bundle bytes, a fresh
   promotion attestation, and a new deployment identity.

Every stage must serve both tenants from two ready replicas and create exactly
one matching activation row per tenant. The evidence binds both source tags and
revisions, image digests, chart OCI digests, chart-package hashes, deployment
identities, artifact digests, and promotion JTIs without retaining signed
payloads or credentials.

```bash
gh workflow run runtime-published-upgrade-rollback.yml \
  --repo prometa-ai/orchestra-python-sdk \
  --ref main \
  -f baseline_tag=v0.18.0 \
  -f target_tag=v0.18.4
```

This closes the separately published release-channel transition gap only for
the pinned K3d reference profile. It is not OpenShift, managed-database,
customer-registry, disaster-recovery, RPO/RTO, or soak certification. The SDK
release workflow advances the independently versioned chart patch whenever the
runtime version advances, preventing an immutable chart version from being
republished for a new runtime release.

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

- `GET /healthz`: process liveness, and every chart probe;
- `GET /readyz`: payload-free runtime-control state for operators;
- `GET /v1/runtime/tasks/{requestId}`: authenticated payload-free lifecycle
  projection when task recovery is configured;
- `POST /v1/runtime/execute`: bearer-authenticated execution.

```json
{
  "requestId": "tenant-request-123",
  "input": {"question": "Where is my order?"}
}
```

In 0.20.2, request IDs are exact 1-256 character visible ASCII values and the
case-insensitive null sentinels `null`, `none`, `nil`, and `undefined` are
rejected before any task claim. Use opaque, non-PII values because request and
derived model identities cross model-plane headers and can appear in
payload-free evidence and the model billing ledger/stdout. Before rollout,
inventory generators and durable task rows and reconcile legacy in-flight
work. Historical sentinel rows stay readable through the task-status endpoint,
but cannot be executed or retried by 0.20.2; do not rewrite a claimed row, and
use a new conformant ID only for genuinely new work.

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
