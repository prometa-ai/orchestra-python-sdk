# Runtime-control lease conformance vectors

The acceptance gate for `runtime-control-lease-contract.md` §8. Both blocking
interop defects in this programme were found by running one side's verifier
against the other's fixture, so the fixtures are shared and the format is
pinned here.

Two directions, two lists in `manifest.json`:

* `vectors[]` — nine lease-direction vectors under `vectors/`: the six §8
  names, plus `lease-quarantine-expired` (§7a), `lease-serving-replay-across-streams`
  (§8a) and `lease-serving-not-enforcement` (§8b/§8c).
* `acknowledgementVectors[]` — ten acknowledgement-direction cases under
  `acks/`, pinning the §7b wire shape a runtime reports back.

`generate.py` reproduces every byte from fixed seeds and one fixed time anchor;
re-run it after any wire change. `engine_check.py` runs the lease vectors
through the inference engine's implementation; the Python SDK's
`tests/test_runtime_control_lease_vectors.py` runs the same bytes through its
own. A runner that only knows `vectors[]` keeps working: the acknowledgement
list is a separate key it can ignore until it consumes that direction.

## What a runner must do

Each vector is a small script. Build a gate for the runtime you are testing
using `context` and your entry in `runtimes`, load `trust`, then walk `steps`
in order against one gate instance — state carries between steps, which is what
makes the replay and expiry cases meaningful.

`{"action": "apply"}` hands `lease` to your verifier at time `at`.

* `expect.outcome: "verified"` — it must verify, and `digest`, `leaseId`,
  `revision`, `mode`, `staleAction` and `controlCount` must match. `digest` is
  `sha256:` over the exact UTF-8 bytes of `signedPayload`, never over a
  re-serialization of the parsed claims.
* `expect.outcome: "refused"` — it must be rejected with `expect.errorCode`.
  The previously adopted lease stays in force.

`{"action": "evaluate"}` asks the gate what it is enforcing at time `at`.
`expectByRuntime` holds one expectation per runtime id; assert only your own.
`admitting`, `state`, `stale`, `enforcement` and `enforcedControlCount` all
apply.

## The four things runners get wrong

**`enforcedControlCount` is not `controls.length`.** Per §8b it counts only
controls whose scope this runtime declares enforceable, whose `subjectId`
matches an identity this runtime holds, *and* whose `state` is `quarantined`.
`lease-agent-scope` catches the scope half: the same lease is `1`/`enforcing`
on the SDK host and `0`/`advisory` on the engine, because an engine has no
agent identity to match. `lease-serving-not-enforcement` catches the state
half: a matched `serving` control is not an enforcement, and an implementation
counting every matched control reports `1` where the contract says `0`.

**Zero enforcement is `advisory`, whichever way you got there** (§8c). Not only
when no control sits in an enforceable scope — also when the scopes resolve and
nothing matched. `lease-serving-not-enforcement` pins this too: both runtimes
resolve both scopes in that lease, one of them matches a control, and neither
is refusing anybody, so both report `advisory`.

**Mode is checked before staleness.** `lease-advisory-expired` names a
quarantine, asks for `staleAction: "stop"`, and is evaluated four hours after
it expired. Every runtime must still admit. A runtime that tests staleness
first hard-stops its fleet on a dry run.

**The replay guard is global per `(orgId, targetEnvironment)`, not per
`leaseId`** (§8a). `lease-serving-replay-across-streams` replays an authentic,
in-window, lower-revision `serving` lease under a *different* `leaseId` against
a live quarantine. An implementation keyed on `leaseId` has no recorded
revision for a stream it has never seen, accepts it, and serves — the same
evasion as `lease-lower-revision`, bought by changing one string.

## `lease-quarantine-expired`: the case that was read backwards

An earlier contract said a matched quarantine on an expired lease means
"staleAction decides", without saying which way `continue` goes, and two
implementations read it opposite ways. Under the reading where `continue` means
"resume serving", a quarantine is lifted by making the control plane
unreachable — the exact evasion §1 exists to prevent.

Contract §7a now settles it and this vector pins it: `continue` means keep
enforcing whatever the last lease said. `stop` refuses everything. Both stale
branches refuse, so a quarantine can never be waited out.

## The acknowledgement direction (`acks/`, §7b)

The lease direction had vectors and this one did not, which is how a runtime
came to emit a shape the control plane does not read. Each case is a single
object, not a step sequence:

* `observedAt` — the observation instant it arrived on. `stale` is only
  checkable against this.
* `acknowledgement` — the `runtimeControlAck` object, or `null` meaning the
  observation carried no such key at all.
* `expect.outcome`:
  * `"accepted"` — intake must accept it, and read back exactly
    `expect.normalized`: the nine pinned keys, with unknown members dropped
    rather than stored.
  * `"absent"` — the replica said nothing. Intake must record that, and must
    never count it as compliance or as "enforcing nothing successfully".
  * `"rejected"` — intake must refuse it. No error code is asserted: §7b pins
    the shape, not any implementation's refusal vocabulary.

For a producer these run the other way round: every `accepted` case is a shape
you must be able to emit, and no `rejected` case may ever leave your runtime.

## Trust entries

`trust[]` entries carry `allowedArtifactTypes` — the key's purpose. A verifier
must refuse a lease whose key does not name
`orchestra.runtime-control-lease` there. `lease-wrong-key-purpose` supplies a
genuine, correctly-signed lease under the routing-policy key: the signature is
valid, and it must still be refused.

## Error codes

`errorCode` values are the contract's, not any one implementation's:
`signing_key_purpose_denied`, `control_lease_out_of_order`,
`invalid_signature`.
