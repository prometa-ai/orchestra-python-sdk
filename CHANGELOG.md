# Changelog

All notable changes to the `prometa-sdk` Python package.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **AML v0.4 instrumentation contract — Phase 2 skeleton (4 of 18 helpers).**
  New helpers map the agent's runtime behavior to the
  Agentic Maturity Level (AML) feature catalog so the Prometa platform's
  scoring engine can audit the agent against 41 features across six
  capability domains. Strictly additive — no existing decorator or
  integration changes.
  - **`prometa.guardrail(type_, raw_input=…, raw_retrieved=…)`** context
    manager — emits a `guardrail.check` span. Feeds AML A2 (ethical
    guardrailing) and A3 (prompt-injection defense). The yielded handle
    exposes `.verdict(outcome, confidence=…, classifier=…, categories=…)`
    for the classifier result.
  - **`prometa.pii_filter(direction, raw_input=…, raw_output=…)`** context
    manager — emits a `pii.filter` span. Feeds AML A1 (PII filtering).
    Handle: `.result(matches_found=…, match_categories=…, redacted=…)`.
  - **`prometa.memory_read(scope, key)`** context manager — emits a
    `memory.read` span. Feeds AML B3 / B4 / C6 / E3 / E4. Handle:
    `.hit(source_record_id=…, user_visible=…)` or `.miss()`. Scope ∈
    `working | episodic | profile | procedural | goal`.
  - **`prometa.memory_write(scope, key, consent_id=…, ttl_seconds=…)`**
    one-shot — emits a `memory.write` span. `consent_id` is required for
    cross-session writes (B4 / E3) per AML A8.
  - **`prometa.record_retry_attempt(attempt_number, backoff_ms, idempotency_key=…, outcome)`**
    — emits a `retry.attempt` span. Feeds AML E6 (resilience). `outcome` ∈
    `success | fail | exhausted`. SDK does not implement retry logic; this
    just records what the customer's retry library did.
  - **`prometa.record_circuit_breaker_state(target, from_state, to_state, failure_count=…)`**
    — emits a `circuit_breaker.state` span on transitions. States ∈
    `closed | open | half_open`.
  - **`prometa.raw_channel`** module — process-wide toggle for dual-channel
    raw-attribute capture. `enable()` / `disable()` / `is_enabled()`. When
    enabled, helpers stamp `prometa.raw.input` / `prometa.raw.output` /
    `prometa.raw.retrieved_content` so the platform can route them to
    `prometa.spans_raw` (30-day TTL, access-gated). Off by default so an
    accidental misconfiguration cannot leak raw PII upstream.
  See `examples/aml_instrumentation.py` for end-to-end usage. Full
  contract: <https://github.com/prometa-ai/agent-hook-v2/tree/main/resources/aml/phase-0>.
  Remaining 14 of 18 helpers (prompt.render, auth.check, consent.check,
  cache.lookup, retrieval.query attributes, plan.generate, confidence.score,
  schema.validate, reviewer.invoke, event.trigger, model.route,
  sentiment.classify) land in follow-up commits.

- **Explicit data-flow refs between sibling spans.** New helpers
  `set_input_ref(span_id)` / `set_output_ref(span_id)` /
  `get_input_ref()` / `get_output_ref()` / `current_span_id()` let a
  span declare "I consumed the output of span X" — typically used by a
  tool span to reference the LLM whose `tool_call` it's executing.
  Emitted as `prometa.input_ref` / `prometa.output_ref`; the platform
  promotes them to dedicated columns and the trace UI surfaces them
  as clickable rows in the Causal-context block. Required for
  LLM-as-a-Judge traces to reason about flow rather than timing.
  See `examples/data_flow_refs.py` for the capture-the-span pattern.

## [0.3.4] — 2026-05-01

### Changed

- Release automation now publishes to production PyPI (workflow no longer targets TestPyPI).

## [0.3.3] — 2026-04-26

### Fixed

- **Conversation panel showing the system prompt instead of the user's
  question** for traces with long chat histories. Root cause was
  truncation: `gen_ai.prompt` (the JSON-serialized messages array) was
  capped at 8KB, exceeded by realistic chat sessions with multi-KB
  system prompts + tool-call rounds, then the platform's `JSON.parse`
  failed on the truncated payload and the panel fell back to rendering
  the raw text — which begins with the system message because OpenAI's
  `messages` array always has system first.

  Two complementary fixes:

  1. The OpenAI / Anthropic / Google integrations now pre-extract the
     latest user-role text into a separate `gen_ai.prompt.user` span
     attribute. Extraction happens *before* JSON serialization /
     truncation, on the in-memory list, so it can never be cut off.
     The platform's Conversation panel reads this attribute first
     (with `gen_ai.prompt` as fallback for older SDK versions).
     Reference: [agent-hook-v2#40](https://github.com/caglarsubas/agent-hook-v2/pull/40).
  2. `MAX_TEXT_ATTR_BYTES` raised from `8000` → `32000`. Most chat
     sessions now fit the full payload without truncation, so the
     `gen_ai.prompt` attribute is also more reliable for downstream
     judge / replay tooling that wants the complete messages array.

  Backward compatible: span attributes are additive, no config flag.
  Old SDK versions continue to work; old platform versions ignore the
  new attribute (it lands harmlessly in span metadata).

  No SDK API change. Consumers do not need to touch their integration
  code; `pip install -U prometa-sdk==0.3.3` is sufficient.

### Internal

- `_SCOPE_VERSION` (used as the OTLP instrumentation-scope version on
  every emitted span) is now derived from `importlib.metadata.version`
  at import time instead of being a hand-maintained mirror constant in
  `client.py`. Eliminates the drift bug that mis-reported the SDK
  version on every span across `0.3.0` / `0.3.1` / `0.3.2`. Reference:
  [agent-hook-v2#39](https://github.com/caglarsubas/agent-hook-v2/pull/39).
  (Released as part of `0.3.3` because it landed on `main` after the
  `0.3.2` tag was cut.)

## [0.3.2] — 2026-04-26

### Documentation only — no SDK code changes

Reflects a platform-side fix that shipped alongside this version. The
SDK API surface, behavior, and on-the-wire format are unchanged from
0.3.1.

#### What changed on the platform

`prometa.spans` and `prometa.traces` migrated from `MergeTree` to
`ReplacingMergeTree`, with `FINAL` added to user-facing read paths.
Duplicate sends of the same `span_id` (or `trace_id`) now collapse to
a single row instead of inflating cost / token / conversation
aggregates. This closes the *"every span appears doubled"* class of
bugs that surfaced when SDK consumers had long-running requests
(RAG pipelines, multi-round tool loops, chat turns spanning tens of
seconds), where the SDK's at-least-once retry semantics interacted
with the platform's previous at-most-once-friendly storage.

Reference: [agent-hook-v2#37](https://github.com/caglarsubas/agent-hook-v2/pull/37).

#### What this means for SDK consumers

- The default `flush_interval_seconds=2.0` is **safe to use** even on
  long requests. The SDK's retry-on-failure no longer risks platform
  double-counting.
- If you raised the interval as a workaround (e.g. `120.0`), you can
  revert to the default. The workaround traded duplicate-risk for
  data-loss-risk on worker crashes; with platform-side dedup in
  place, neither risk applies.
- See the **Reliability & retry semantics** section in `README.md` for
  the full contract.

## [0.3.1] — 2026-04-26

### Fixed

- Release-workflow safety: the `Release Python SDK` GitHub workflow
  used to silently advance the version every run, which could skip
  the version a feature PR had hand-bumped (PyPI history showed
  `0.1.0 → 0.1.2 → 0.2.1 → 0.2.3 → 0.3.1`, missing `0.1.1, 0.2.0,
  0.2.2, 0.3.0`). Added a `bump: as-is` mode and a tag-existence
  guard so the workflow refuses to clobber a version already
  released. Reference: [agent-hook-v2#36](https://github.com/caglarsubas/agent-hook-v2/pull/36).

This release was the first one published *with* the new mode but
*before* this CHANGELOG existed; recording it here for completeness.

## [0.3.0] — 2026-04-25

### Added

- **Sessions / conversational trace grouping.** Stamp a session id on
  the current span (or on a workflow at decorator time) so the
  Prometa trace UI can group related traces into one chat thread /
  user task and render aggregate cost / tokens / duration plus a
  unified conversation timeline.

  ```python
  from prometa import set_session_id
  set_session_id("chat-conv-abc123")  # any opaque key your app uses
  ```

  Or:
  ```python
  @prometa.workflow(name="handle-turn", session_id=conversation_id)
  ```

  Reference: [agent-hook-v2#35](https://github.com/caglarsubas/agent-hook-v2/pull/35).

(Note: 0.3.0 was hand-bumped in the feature PR but never tagged on
PyPI due to the release-workflow bug fixed in 0.3.1; the `set_session_id`
API surface first reached PyPI in 0.3.1.)

## [0.2.3] — 2026-04-25

### Documentation

- Refreshed the trace "Conversation" panel section after the platform
  switched to deriving turns directly from `gen_ai.prompt` /
  `gen_ai.completion` span attributes (PR #31). Earlier README copy
  said the panel would be empty even with auto-instrumentation —
  no longer true. Reference: [agent-hook-v2#32](https://github.com/caglarsubas/agent-hook-v2/pull/32).

## [0.2.1] — 2026-04-25

### Added

- **LLM-client auto-instrumentation** for the three major providers,
  opt-in via `install()`. Captures `gen_ai.usage.input_tokens`,
  `gen_ai.usage.output_tokens`, `gen_ai.prompt`, `gen_ai.completion`,
  request params, and response metadata on every `chat.completions.
  create` / `messages.create` / `models.generate_content` call (sync,
  async, and streaming).

  ```python
  from prometa.integrations import openai, anthropic, google
  openai.install(); anthropic.install(); google.install()
  ```

  Streaming proxies push the LLM span onto the contextvar stack
  during iteration so any `@prometa.tool` invoked inside the stream
  consumer nests under the LLM span. Reference: [agent-hook-v2#30](https://github.com/caglarsubas/agent-hook-v2/pull/30).

## [0.1.2] — 2026-04-24

(Initial public release captured here for reference; older releases
predate this changelog.)
