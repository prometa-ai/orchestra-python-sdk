"""Dual-channel raw-attribute capture toggle.

When this module is ``enable()``-d, AML instrumentation helpers that accept
raw user inputs (guardrails, PII filter, prompt assembly, sentiment) stamp
``prometa.raw.*`` attributes on their spans. When disabled (the default),
those arguments are silently dropped at the SDK boundary so an accidental
misconfiguration cannot leak raw PII upstream.

Rationale: eight of the 41 AML features (A1, A2, A3, A4, B6, B7, C5, F6)
cannot be detected from the sanitized log alone — they exist precisely to
TRANSFORM raw input. The AML critique §6.3 calls this out: without the
pre-sanitization stream you are auditing the cleaned record, not the
actual exposure. The dual-channel architecture is the only way these
features become auditable.

The platform side of the contract (``prometa.spans_raw`` table, 30-day TTL,
access-gated reads) is documented at:

  https://github.com/caglarsubas/agent-hook-v2/pull/67   # Phase 3 schema

By design this toggle is process-wide (a module-level flag, not a
per-client setting). Routing decisions for raw attributes happen at the
OTLP exporter — flipping it mid-process WILL affect every active client.
Most deployments will call :func:`enable` once at startup behind a strict
allowlist check, or leave it off entirely until the platform-side
``spans_raw`` channel is wired up.
"""

from __future__ import annotations


_enabled: bool = False


def enable() -> None:
    """Turn on dual-channel raw capture for the process.

    Call once at startup, AFTER verifying that the customer agent is
    authorized to emit raw attributes (e.g. the org has accepted the
    short-TTL raw-channel access agreement). There is no per-call gate.
    """
    global _enabled
    _enabled = True


def disable() -> None:
    """Turn off raw capture. Subsequent helper calls drop raw_* kwargs."""
    global _enabled
    _enabled = False


def is_enabled() -> bool:
    """Return whether dual-channel raw capture is currently enabled."""
    return _enabled
