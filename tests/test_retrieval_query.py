"""Tests for ``prometa.retrieval_query`` namespace stamping."""

from __future__ import annotations

import unittest

from prometa import Prometa, retrieval_query


class RetrievalQueryNamespaceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.prometa = Prometa(
            endpoint="http://localhost:0/never-flushed",
            api_key=None,
            solution_id="sol-test",
            agent_name="test-agent",
            agent_id="test-agent-id",
            stage="test",
        )

    def _retrieval_attrs(self) -> dict:
        spans = [
            s for s in self.prometa._buffer
            if getattr(s, "name", None) == "retrieval.query"
            or (
                isinstance(getattr(s, "attributes", None), dict)
                and "retrieval.system" in s.attributes
            )
        ]
        self.assertTrue(spans, "expected a retrieval.query span")
        return spans[-1].attributes

    def test_namespace_emitted_when_populated(self) -> None:
        @self.prometa.workflow(name="rag")
        def handle():
            with retrieval_query(
                "hybrid",
                query_text="What does PSI mean?",
                top_k=4,
                namespace="knowledge-bank",
            ) as r:
                r.results(result_ids=["chunk-a"], scores=[0.9])

        handle()
        attrs = self._retrieval_attrs()
        self.assertEqual(attrs["retrieval.namespace"], "knowledge-bank")
        self.assertEqual(attrs["retrieval.system"], "hybrid")
        self.assertEqual(attrs["retrieval.top_k"], 4)

    def test_namespace_omitted_when_absent(self) -> None:
        @self.prometa.workflow(name="rag")
        def handle():
            with retrieval_query(
                "keyword",
                query_text="psi",
                top_k=2,
            ) as r:
                r.results(result_ids=["chunk-a"])

        handle()
        attrs = self._retrieval_attrs()
        self.assertNotIn("retrieval.namespace", attrs)

    def test_namespace_omitted_when_blank(self) -> None:
        @self.prometa.workflow(name="rag")
        def handle():
            with retrieval_query(
                "vector",
                query_text="psi",
                top_k=2,
                namespace="   ",
            ) as r:
                r.results(result_ids=[])

        handle()
        attrs = self._retrieval_attrs()
        self.assertNotIn("retrieval.namespace", attrs)

    def test_namespace_is_whitespace_trimmed(self) -> None:
        # Padding is stripped rather than stamped verbatim — otherwise
        # "support-kb" and " support-kb" become two corpora in the RAG
        # dashboard's per-namespace rollup.
        @self.prometa.workflow(name="rag")
        def handle():
            with retrieval_query(
                "graph",
                query_text="psi",
                top_k=2,
                namespace="  support-kb  ",
            ) as r:
                r.results(result_ids=["chunk-a"])

        handle()
        attrs = self._retrieval_attrs()
        self.assertEqual(attrs["retrieval.namespace"], "support-kb")

    def test_namespace_cannot_be_passed_positionally(self) -> None:
        # `namespace` is keyword-only, so no pre-existing positional call
        # site can silently bind it. This is the source-compatibility
        # guard for callers written before the kwarg existed.
        with self.assertRaises(TypeError):
            retrieval_query("vector", "psi", 5, "knowledge-bank")  # type: ignore[misc]

    def test_namespace_ignored_without_an_active_client(self) -> None:
        # No client installed → the helper yields an inert handle rather
        # than raising, with or without a namespace.
        previous = Prometa._current
        Prometa._current = None
        try:
            with retrieval_query(
                "vector",
                query_text="psi",
                top_k=3,
                namespace="knowledge-bank",
            ) as r:
                r.results(result_ids=["chunk-a"])
        finally:
            Prometa._current = previous

        self.assertEqual(len(self.prometa._buffer), 0)


if __name__ == "__main__":
    unittest.main()
