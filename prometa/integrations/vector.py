"""Vector database auto-instrumentation.

Patches the canonical query methods of popular vector DB clients so that
every retrieval becomes a Prometa ``retrieval`` span with OTel semantic
``db.*`` attributes.

Each ``install_*`` function is opt-in; they return True/False depending
on whether the target client is importable. Typical usage::

    from prometa.integrations import vector as prometa_vector

    prometa_vector.install_pinecone()
    prometa_vector.install_chroma()
    prometa_vector.install_weaviate()

Or just::

    prometa_vector.install_all()
"""

from __future__ import annotations

import functools
from typing import Any, Callable, Optional

from ..client import Prometa


def _client() -> Optional[Prometa]:
    return Prometa._current


def _span(label: str, attrs: dict, fn: Callable, *args: Any, **kwargs: Any) -> Any:
    c = _client()
    if c is None:
        return fn(*args, **kwargs)
    with c._span("retrieval", label) as span:
        span.attributes.update(attrs)
        try:
            result = fn(*args, **kwargs)
        except Exception as e:
            span.status = "error"
            span.attributes["error.message"] = str(e)
            raise
        # Best-effort result count
        try:
            if hasattr(result, "matches"):  # Pinecone
                span.attributes["db.result_count"] = len(result.matches)
            elif isinstance(result, dict) and "ids" in result:  # Chroma
                ids = result.get("ids") or []
                if isinstance(ids, list) and ids and isinstance(ids[0], list):
                    span.attributes["db.result_count"] = len(ids[0])
                else:
                    span.attributes["db.result_count"] = len(ids)
            elif isinstance(result, list):
                span.attributes["db.result_count"] = len(result)
        except Exception:
            pass
        return result


def _already_patched(obj: Any, name: str) -> bool:
    return getattr(getattr(obj, name, None), "__prometa_wrapped__", False)


def _mark(fn: Callable) -> Callable:
    fn.__prometa_wrapped__ = True  # type: ignore[attr-defined]
    return fn


# ---------------------------------------------------------------------------
# Pinecone
# ---------------------------------------------------------------------------

def install_pinecone() -> bool:
    try:
        from pinecone import Index  # type: ignore
    except Exception:
        try:
            from pinecone.data.index import Index  # type: ignore
        except Exception:  # pragma: no cover
            return False

    if _already_patched(Index, "query"):
        return True

    original = Index.query

    @functools.wraps(original)
    def query(self, *args, **kwargs):  # type: ignore[override]
        ns = kwargs.get("namespace", "") or ""
        top_k = kwargs.get("top_k", kwargs.get("topK", 0))
        attrs = {
            "gen_ai.framework": "pinecone",
            "db.system": "pinecone",
            "db.namespace": str(ns),
            "db.query.top_k": int(top_k or 0),
        }
        return _span(
            f"pinecone.query:{ns or 'default'}",
            attrs,
            original,
            self,
            *args,
            **kwargs,
        )

    Index.query = _mark(query)  # type: ignore[assignment]
    return True


# ---------------------------------------------------------------------------
# Chroma
# ---------------------------------------------------------------------------

def install_chroma() -> bool:
    try:
        from chromadb.api.models.Collection import Collection  # type: ignore
    except Exception:  # pragma: no cover
        try:
            from chromadb import Collection  # type: ignore
        except Exception:
            return False

    if _already_patched(Collection, "query"):
        return True

    original = Collection.query

    @functools.wraps(original)
    def query(self, *args, **kwargs):  # type: ignore[override]
        n_results = kwargs.get("n_results", 0)
        attrs = {
            "gen_ai.framework": "chroma",
            "db.system": "chroma",
            "db.namespace": getattr(self, "name", ""),
            "db.query.top_k": int(n_results or 0),
        }
        return _span(
            f"chroma.query:{getattr(self, 'name', '')}",
            attrs,
            original,
            self,
            *args,
            **kwargs,
        )

    Collection.query = _mark(query)  # type: ignore[assignment]
    return True


# ---------------------------------------------------------------------------
# Weaviate (v3 Client / v4 Collections)
# ---------------------------------------------------------------------------

def install_weaviate() -> bool:
    patched = False

    # Weaviate v4: Collections / Query API
    try:
        from weaviate.collections.queries.near_text import NearTextQuery  # type: ignore

        if not _already_patched(NearTextQuery, "near_text"):
            original = NearTextQuery.near_text

            @functools.wraps(original)
            def near_text(self, *args, **kwargs):  # type: ignore[override]
                attrs = {
                    "gen_ai.framework": "weaviate",
                    "db.system": "weaviate",
                    "db.operation": "near_text",
                }
                return _span("weaviate.near_text", attrs, original, self, *args, **kwargs)

            NearTextQuery.near_text = _mark(near_text)  # type: ignore[assignment]
            patched = True
    except Exception:
        pass

    # Weaviate v3: Client.query.get(...).do()
    try:
        from weaviate.gql.query import GetBuilder  # type: ignore

        if not _already_patched(GetBuilder, "do"):
            original = GetBuilder.do

            @functools.wraps(original)
            def do(self, *args, **kwargs):  # type: ignore[override]
                attrs = {
                    "gen_ai.framework": "weaviate",
                    "db.system": "weaviate",
                    "db.operation": "graphql.get",
                }
                return _span("weaviate.query", attrs, original, self, *args, **kwargs)

            GetBuilder.do = _mark(do)  # type: ignore[assignment]
            patched = True
    except Exception:
        pass

    return patched


# ---------------------------------------------------------------------------
# All-in-one
# ---------------------------------------------------------------------------

def install_all() -> dict:
    """Install every available vector DB instrumentation. Returns a dict
    {name: bool} reporting which were successfully patched."""
    return {
        "pinecone": install_pinecone(),
        "chroma": install_chroma(),
        "weaviate": install_weaviate(),
    }
