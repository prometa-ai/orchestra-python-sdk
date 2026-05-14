"""AML instrumentation helper — ``prompt.render`` span.

Maps to the SDK v0.4 contract bundled in the platform repo at
``resources/aml/phase-0/instrumentation-spec.yaml`` (span name
``prompt.render``). The contract feeds the platform's AML scoring engine
for features A4 (user/system/tool prompt isolation), B7 (multilingual &
localization), C4 (explainability), C5 (sentiment awareness), C6 (dynamic
context assembly) — so an instrumented agent becomes auditable for those
features.

ADDITIVE only — existing v0.3.x decorators and integrations are unchanged.

Dual-channel: when :func:`prometa.raw_channel.is_enabled` is True the
``raw_rendered_prompt`` kwarg is stamped on the span as
``prometa.raw.rendered_prompt`` so the platform can route it to
``prometa.spans_raw`` (short-TTL, access-gated). When disabled (default),
the kwarg is silently dropped at the SDK boundary.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence

from . import _raw_channel
from .client import Prometa


def _client() -> Optional[Prometa]:
    return Prometa._current


class _PromptRenderHandle:
    """Yielded inside :func:`prompt_render`. Records the final assembly.

    No-op when no Prometa client is active.
    """

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def assembled(
        self,
        *,
        template_version: Optional[str] = None,
        system_token_count: int = 0,
        user_token_count: int = 0,
        tool_token_count: int = 0,
        role_boundaries: Optional[Sequence[dict]] = None,
        context_components: Optional[Sequence[str]] = None,
    ) -> None:
        """Record the final-assembly attributes per the AML contract.

        ``role_boundaries`` is a list of ``{"role", "start", "end"}`` dicts
        with byte ranges for system / user / tool / assistant segments —
        what A4 reads to verify isolation.
        ``context_components`` is the list of knowledge sources blended
        in (e.g. ``["system_prompt", "user_profile", "retrieved_docs",
        "tool_schemas", "policy_pack:banking_v3"]``) — what C6 reads.
        """
        if self._span is None:
            return
        a = self._span.attributes
        if template_version is not None:
            a["prompt.template_version"] = template_version
        a["prompt.system_token_count"] = int(system_token_count)
        a["prompt.user_token_count"] = int(user_token_count)
        a["prompt.tool_token_count"] = int(tool_token_count)
        if role_boundaries:
            # OTel attribute exporter wants stringified JSON for compound
            # structures; the platform parses it back. Compact form so
            # the attribute fits within the per-attribute size cap.
            a["prompt.role_boundaries"] = json.dumps(
                list(role_boundaries), separators=(",", ":")
            )
        if context_components:
            a["prompt.context_components"] = ",".join(context_components)


@contextmanager
def prompt_render(
    *,
    template_version: Optional[str] = None,
    raw_rendered_prompt: Optional[str] = None,
) -> Iterator[_PromptRenderHandle]:
    """Emit a ``prompt.render`` span around the final prompt assembly.

    Usage::

        with prometa.prompt_render(
            template_version="customer-support@v3",
            raw_rendered_prompt=rendered_json,   # raw_channel-gated
        ) as p:
            rendered_json = my_template_engine.assemble(...)
            p.assembled(
                system_token_count=42,
                user_token_count=131,
                role_boundaries=[
                    {"role": "system", "start": 0, "end": 168},
                    {"role": "user", "start": 169, "end": 720},
                ],
                context_components=["system_prompt", "retrieved_docs"],
            )
    """
    c = _client()
    if c is None:
        yield _PromptRenderHandle(None)
        return
    with c._span("prompt", "prompt.render") as span:
        if template_version is not None:
            span.attributes["prompt.template_version"] = template_version
        if _raw_channel.is_enabled() and raw_rendered_prompt is not None:
            span.attributes["prometa.raw.rendered_prompt"] = raw_rendered_prompt
        yield _PromptRenderHandle(span)
