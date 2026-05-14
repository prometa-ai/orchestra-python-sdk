"""AML instrumentation helpers — ``cache.lookup`` and ``retrieval.query`` spans.

Maps to the SDK v0.4 contract bundled in the platform repo at
``resources/aml/phase-0/instrumentation-spec.yaml``. Together they feed
the platform's AML scoring engine for features B1 (RAG), B2 (semantic
caching), and B5 (graph reasoning).

Dual-channel: ``retrieval.query`` accepts a ``raw_retrieved`` kwarg that
stamps the indirect-injection-detection input on the span as
``prometa.raw.retrieved_content`` when raw capture is enabled (A3
indirect-injection signal). ``cache.lookup`` has no raw kwargs —
cache keys and hit/miss outcomes are non-sensitive metadata.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence

from . import _raw_channel
from .client import Prometa


def _client() -> Optional[Prometa]:
    return Prometa._current


# =============================================================================
# Cache lookup (AML B2)
# =============================================================================

_CACHE_KINDS = {"response", "tool_call", "embedding"}


class _CacheLookupHandle:
    """Yielded inside :func:`cache_lookup`. Records hit/miss + TTL."""

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def hit(self, *, ttl_remaining_seconds: int = 0) -> None:
        """Record a cache hit."""
        if self._span is None:
            return
        a = self._span.attributes
        a["cache.hit"] = True
        a["cache.ttl_remaining_seconds"] = int(ttl_remaining_seconds)

    def miss(self) -> None:
        """Record a cache miss (or no-op when caching is bypassed)."""
        if self._span is None:
            return
        self._span.attributes["cache.hit"] = False

    def write_action_blocked(self) -> None:
        """Mark that the lookup was blocked because the request mutates state.

        The AML contract requires this for any cache lookup on a write-
        API request — caches must never serve writes. Calling this also
        records ``cache.hit = False`` so the metrics rollup is consistent.
        """
        if self._span is None:
            return
        a = self._span.attributes
        a["cache.write_action_blocked"] = True
        a["cache.hit"] = False


@contextmanager
def cache_lookup(
    kind: str,
    *,
    key: str,
) -> Iterator[_CacheLookupHandle]:
    """Emit a ``cache.lookup`` span around a cache fetch.

    ``kind`` ∈ ``{response, tool_call, embedding}``.

    Usage::

        with prometa.cache_lookup("response", key=cache_key) as ch:
            entry = my_cache.get(cache_key)
            if entry and not request.is_write():
                ch.hit(ttl_remaining_seconds=entry.ttl_remaining())
                return entry.value
            elif request.is_write():
                ch.write_action_blocked()
            else:
                ch.miss()
    """
    if kind not in _CACHE_KINDS:
        raise ValueError(
            f"cache_lookup: kind must be one of {sorted(_CACHE_KINDS)}, got {kind!r}"
        )
    c = _client()
    if c is None:
        yield _CacheLookupHandle(None)
        return
    with c._span("cache", "cache.lookup") as span:
        a = span.attributes
        a["cache.kind"] = kind
        a["cache.key"] = key
        yield _CacheLookupHandle(span)


# =============================================================================
# Retrieval query (AML B1, B5)
# =============================================================================

_RETRIEVAL_SYSTEMS = {"vector", "graph", "keyword", "hybrid"}


class _RetrievalQueryHandle:
    """Yielded inside :func:`retrieval_query`. Records results."""

    __slots__ = ("_span",)

    def __init__(self, span: Any) -> None:
        self._span = span

    def results(
        self,
        *,
        result_ids: Sequence[str],
        scores: Optional[Sequence[float]] = None,
        permissions_enforced: bool = True,
    ) -> None:
        """Record the retrieved-result attributes.

        ``result_ids`` is the list of document / node ids returned —
        required for B1 citation grounding. ``scores`` is recommended
        for confidence calibration but optional.
        ``permissions_enforced`` records whether the retrieval layer
        applied row-level / doc-level ACLs before returning results.
        """
        if self._span is None:
            return
        a = self._span.attributes
        a["retrieval.result_count"] = int(len(result_ids))
        if result_ids:
            a["retrieval.result_ids"] = ",".join(result_ids)
        if scores is not None and len(scores) > 0:
            a["retrieval.scores"] = ",".join(f"{s:.4f}" for s in scores)
        a["retrieval.permissions_enforced"] = bool(permissions_enforced)


@contextmanager
def retrieval_query(
    system: str,
    *,
    query_text: str,
    top_k: int,
    raw_retrieved: Optional[str] = None,
) -> Iterator[_RetrievalQueryHandle]:
    """Emit a ``retrieval.query`` span around a RAG / graph / keyword fetch.

    ``system`` ∈ ``{vector, graph, keyword, hybrid}``.

    Dual-channel: ``raw_retrieved`` is the concatenated raw text of the
    retrieved results — what A3 indirect-injection scans. Only stamped
    when :func:`prometa.raw_channel.is_enabled` is True.

    Usage::

        with prometa.retrieval_query(
            "vector", query_text=query, top_k=5,
            raw_retrieved=raw_text,        # raw_channel-gated
        ) as r:
            docs = vector_store.search(query, top_k=5)
            raw_text = "\\n---\\n".join(d.text for d in docs)
            r.results(
                result_ids=[d.id for d in docs],
                scores=[d.score for d in docs],
                permissions_enforced=True,
            )
    """
    if system not in _RETRIEVAL_SYSTEMS:
        raise ValueError(
            f"retrieval_query: system must be one of {sorted(_RETRIEVAL_SYSTEMS)}, got {system!r}"
        )
    c = _client()
    if c is None:
        yield _RetrievalQueryHandle(None)
        return
    with c._span("retrieval", "retrieval.query") as span:
        a = span.attributes
        a["retrieval.system"] = system
        a["retrieval.query_text"] = query_text
        a["retrieval.top_k"] = int(top_k)
        if _raw_channel.is_enabled() and raw_retrieved is not None:
            a["prometa.raw.retrieved_content"] = raw_retrieved
        yield _RetrievalQueryHandle(span)
