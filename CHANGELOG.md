# Changelog

All notable changes to the `prometa-sdk` Python package.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
