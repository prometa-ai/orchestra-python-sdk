# Reference guardrail service

The tenant-plane reference implementation of `orchestra-guardrail-evaluate-v1`.
It is the shared evaluator behind the blueprint rule **one shared service, many
enforcement points**: the SDK kernel, the SDK MCP broker, and the inference
engine all reach the same contract, so a guardrail declared once is enforced the
same way wherever content crosses a boundary.

It is not part of the Prometa control plane and makes no synchronous
control-plane call while serving a request. It opens no outbound connection at
all — the detector pack is compiled from the image and the profile document.

## Selection is the caller's, definition is this service's

A request names *which* guardrails apply to it; the profile says what each of
those names *is*. So a caller that holds no bundle — the engine — sends
`{"name": "secret-egress", "guardrailType": null, "onViolation": null}` and this
service fills the nulls from `profiles[].guardrails`. A caller that restates a
field must restate it identically: a declaration contradicting the profile is
`422`, because a request that could assert a softer `onViolation` on the wire
would be rewriting the policy it is being measured against. A name the profile
does not declare is also `422`, which under the default fail mode is a loud
`503` rather than an unguarded deployment.

That makes `profiles[].guardrails` load-bearing: **a profile that declares no
guardrails can serve no request.** `config.example.json` declares a set for both
of its profiles for exactly this reason.

## What it evaluates

Four stages, four verdicts, one schema:

| Stage | Fired by | Guards |
| --- | --- | --- |
| `llm_input` | engine route entry, kernel `_guard("input", …)` | the prompt before the model sees it |
| `llm_output` | engine post-generation, kernel `_guard("output", …)` | the completion before the caller sees it |
| `tool_call` | kernel `_guard("tool", …)` | arguments the model wants to send to a tool |
| `tool_result` | MCP broker post-call, engine inbound `role="tool"` | data coming back into the context window |

`tool_result` is the stage this service exists for. Input-only guardrails are an
anti-pattern: leading agents fail roughly half of injection-via-tool-output
scenarios, and a broker that returns a tool result uninspected has no seam at
which to catch them.

The wire carries three verdicts: `allow`, `transform`, `deny`. `escalate` is
deliberately not among them — routing a caller to human review needs a review
plane at the far end of the call and the HTTP binding has none, so a guardrail
that would escalate comes back as `deny` with
`reasonCode: "guardrail_escalation_unsupported"`, and an `escalate` that does
arrive on the wire is read as `deny` with
`reasonCode: "guardrail_verdict_unrecognized"` rather than acted on. It
survives only in the in-process binding, where the kernel's human-review path
is on the other side of the return.

`transform` always carries a `transformedPayload` and every other verdict
always carries `null` — a `transform` with no payload is a protocol violation
the client converts to `deny`, never a soft error that releases the original
content.

## Two bindings, one contract

| Binding | Used by | Entry point |
| --- | --- | --- |
| In-process | SDK kernel, SDK MCP broker, CI | `prometa.guardrail.LocalGuardEvaluator` |
| HTTP | engine, any non-Python caller | `POST /v1/guardrail:evaluate` |

The in-process binding routes through the same encode/decode codec as HTTP on
purpose. That is what makes them one contract rather than two behaviours that
happen to agree today.

## Every detector runs in band

The latency budget is part of the contract, not an operator knob discovered
later. `budgetMs` is per stage — 25 ms for `llm_input` and `tool_call`, 40 ms
for `llm_output` and `tool_result` — and the service returns within it rather
than blocking on a detector that has overrun.

Every detector in the pack runs on the request path. There is no out-of-band
band: a detector that could never run would leave its guardrail permanently
deferred, which is a guardrail reporting coverage it does not have.

| Detector | Type | Cost shape |
| --- | --- | --- |
| `builtin.secret-dlp` | `secret-dlp` | regex + Shannon entropy over bounded runs |
| `builtin.pii-dlp` | `pii-dlp` | regex + Luhn / mod-97 / octet checksums |
| `builtin.content-policy` | `content-policy`, `input-filter`, `output-filter` | Aho–Corasick over normalized text |
| `builtin.mcp-risk-gate` | `mcp-risk-gate` | set membership + host allowlist |
| `builtin.injection-heuristics` | `input-filter`, `output-filter`, `content-policy` | five independent regex families + invisible-character scan |
| `builtin.cost-budget` | `cost-budget` | arithmetic |
| `builtin.human-approval` | `human-approval` | declarative; escalates in-process, `deny` over HTTP |

A guardrail whose `guardrailType` no detector serves is not silently allowed: it
appears in `evaluatedGuardrails` and in `deferred[]` with
`reason: "detector_unavailable"`, and the profile's `unknownGuardrailPolicy`
decides the verdict.

Budget exhaustion is not fail-open. Remaining in-band detectors move to
`deferred[]` with `reason: "budget_exhausted"`, and under the default
`deferPolicy: "deny"` a deferred *enforcing* guardrail yields `deny`. The
budget is enforced *inside* a detector as well as between detectors: every scan
runs in bounded overlapping windows and the budget is re-checked between them,
so one detector on a large payload cannot spend the whole budget and report
nothing deferred.

### Sizing `maxPayloadBytes` against `budgetMs`

These two settings constrain each other and the defaults do not pick a winner
for you. `maxPayloadBytes` defaults to 1 MiB and `budgetMs` to 25–40 ms; if the
detector pack on your hardware cannot scan `maxPayloadBytes` inside `budgetMs`,
payloads above whatever it *can* scan come back as `verdict: "deny"` with
`reasonCode: "guardrail_deferred_enforcing"` and a `deferred[]` entry naming the
guardrail that was cut short. That is fail-closed and correct, but if you see it
routinely it is a sizing problem, not an attack: measure the pack on the
hardware you deploy on, then either lower `maxPayloadBytes` — which turns the
same case into a `413` with `guardrail_payload_too_large`, an unambiguous
signal — or raise `budgetMs` for the affected stage. Do not raise
`maxPayloadBytes` past what the budget covers and expect coverage.

Guardrail types overlap by design — `input-filter` is served by both the content
policy and the injection heuristics — so one guardrail runs every detector that
matches its type and stage, and the merged finding decides the verdict. A
narrower detector is never shadowed by a broader one that happens to sort first.

## Fail modes

Default `failMode` is `closed`, matching every other decision point on the
governed surface. A guardrail that turns itself off under load is a guardrail
with a documented bypass, and unavailability is exactly the failure an attacker
induces.

`failMode: "open"` exists for genuine observe-only rollouts and is bounded so it
cannot become the accidental production state:

- **Load-time gate.** A `failMode: "open"` profile whose `guardrails` array
  defines anything that can enforce is refused when the config is read, before
  the process serves a request.
- **Construction-time gate.** An evaluator constructed with a `failMode: "open"`
  profile beside any guardrail that can enforce is refused there and then. The
  guardrail list that is actually evaluated arrives on each request, so the HTTP
  binding re-checks that one before issuing the call: the pairing cannot run
  healthy for months and surface the first time the service is unreachable.
- **Never a clean scan on empty coverage.** A `200` whose `evaluatedGuardrails`
  is empty resolves through the fail mode as `guardrail_coverage_empty`, never
  as an allow, and never recovers the fail-open budget.
- **Always observable.** Every fail-open emits `appliedAction: "allow"`,
  `reasonCode: "guardrail_unavailable_fail_open"`, `severity: "high"`,
  `reviewRequired: true`.
- **Bounded.** `failOpenMaxConsecutive` fail-opens inside `failOpenWindowSeconds`
  trip the client back to closed until an evaluation succeeds.
- **Never certified.** Under the certified model-workload surface a fail-open
  profile is refused at evaluator construction.

A timeout is never a verdict. There is no "timed out therefore allow" and no
"timed out therefore deny" anywhere in the code; an unusable verdict resolves
through the profile fail mode and only through it.

## What it logs

The service handles caller content by definition, so what it records is bounded
by construction. Every log line is metadata — request id, tenant, profile,
stage, status, verdict, reason code, evaluated and deferred counts, detector
pack digest, timings. The evaluated payload, the transformed payload, and every
matched fragment are never written anywhere. Content evidence leaves only as a
sha256 digest in `contentFragmentDigests`.

The same discipline holds on the wire: apart from `transformedPayload`, no
response field carries raw content. `reason`, `summary`, `counterfactual` and
`actionRationale` are policy prose. Conformance check D8 asserts this over the
whole serialized body against a planted secret.

## Configuration

```bash
docker build -f deploy/guardrail-service/Dockerfile \
  -t prometa-guardrail-service:0.20.2 .
```

`config.example.json` is the profile document; `api-keys.example.json` is the
bearer keys file, in the same shape the engine already uses
(`[{"key":…,"tenant":…,"org_id":…}]`). A request whose token resolves to a
tenant other than `subject.tenant` is rejected `403`. Keys shorter than 32 bytes
are refused at startup.

Both files are mounted read-only; the chart references but never creates them.
The container runs non-root (`10001` on Debian, `1001` in GID 0 on UBI9) with a
read-only root filesystem.

## Conformance

The contract's checklist ships as an executable runner, not a document:

```bash
prometa-guardrail-conformance            # JSON report, exit 1 on any failure
```

It runs against the built-in service through a driver protocol, so a
third-party implementation is conformant exactly when the same runner says so.
Checks that need the kernel or the MCP broker are listed in
`DELEGATED_CHECKS` with the suite that owns them; nothing in the checklist is
unowned.

The whole suite runs with no network and no third-party package — that is
enforced by checks D1 and D2, and it is what makes CI and the air-gapped
OpenShift profile the same code path.

## Deployment topology

The contract supports all three placements: sidecar (lowest latency, per-pod
detector-pack drift), shared service (single digest, one network hop inside the
budget), and in-process for the SDK (no hop). Sidecar is the recommended default
for the engine. `detectorPack.digest` is on every response, so a fleet running
mixed packs is visible rather than silent.
