"""Tests for OpenAI embeddings.create auto-instrumentation."""

from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace

from prometa import Prometa
from prometa.integrations import openai as prometa_openai


def _openai_embeddings_importable() -> bool:
    try:
        from openai.resources.embeddings import AsyncEmbeddings, Embeddings  # noqa: F401

        return True
    except Exception:
        return False


class _FakeEmbeddings:
    def create(self, **kwargs):
        return SimpleNamespace(
            model=kwargs.get("model", "text-embedding-3-small"),
            data=[
                SimpleNamespace(embedding=[0.1, 0.2, 0.3], index=0),
                SimpleNamespace(embedding=[0.4, 0.5, 0.6], index=1),
            ],
            usage=SimpleNamespace(prompt_tokens=12, total_tokens=12),
        )


class _FakeAsyncEmbeddings:
    async def create(self, **kwargs):
        return SimpleNamespace(
            model=kwargs.get("model", "text-embedding-3-small"),
            data=[SimpleNamespace(embedding=[0.7, 0.8], index=0)],
            usage=SimpleNamespace(prompt_tokens=4, total_tokens=4),
        )


class EmbeddingInputSizeTest(unittest.TestCase):
    def test_string_input(self) -> None:
        self.assertEqual(
            prometa_openai._embedding_input_size("hello"),
            (1, 5),
        )

    def test_string_batch(self) -> None:
        self.assertEqual(
            prometa_openai._embedding_input_size(["ab", "cde"]),
            (2, 5),
        )

    def test_token_id_batch(self) -> None:
        self.assertEqual(
            prometa_openai._embedding_input_size([[1, 2], [3]]),
            (2, 3),
        )


class EmbeddingsRequestAttrsTest(unittest.TestCase):
    def test_stamps_model_operation_and_size_signals(self) -> None:
        attrs = prometa_openai._embeddings_request_attrs(
            {
                "model": "text-embedding-3-small",
                "input": ["alpha", "beta"],
                "dimensions": 512,
                "encoding_format": "float",
            }
        )
        self.assertEqual(attrs["gen_ai.system"], "openai")
        self.assertEqual(attrs["gen_ai.operation.name"], "embeddings")
        self.assertEqual(attrs["gen_ai.request.model"], "text-embedding-3-small")
        self.assertEqual(attrs["gen_ai.request.embedding.dimensions"], 512)
        self.assertEqual(attrs["gen_ai.request.embedding.encoding_format"], "float")
        self.assertEqual(attrs["gen_ai.request.embedding.input_count"], 2)
        self.assertEqual(attrs["gen_ai.request.embedding.input_chars"], 9)
        # Size signals only — never dump full inputs into attributes.
        self.assertNotIn("gen_ai.prompt", attrs)
        self.assertNotIn("input", attrs)


class EmbeddingsResponseAttrsTest(unittest.TestCase):
    def test_stamps_usage_without_vectors(self) -> None:
        span = SimpleNamespace(attributes={})
        response = SimpleNamespace(
            model="text-embedding-3-small",
            data=[
                SimpleNamespace(embedding=[0.1, 0.2], index=0),
                SimpleNamespace(embedding=[0.3, 0.4], index=1),
            ],
            usage=SimpleNamespace(prompt_tokens=8, total_tokens=8),
        )
        prometa_openai._apply_embeddings_response_attrs(span, response)
        self.assertEqual(span.attributes["gen_ai.usage.input_tokens"], 8)
        self.assertEqual(span.attributes["gen_ai.usage.total_tokens"], 8)
        self.assertEqual(span.attributes["gen_ai.response.model"], "text-embedding-3-small")
        self.assertEqual(span.attributes["gen_ai.response.embedding.count"], 2)
        # Vectors must not appear as attribute values.
        self.assertTrue(all(not isinstance(v, list) for v in span.attributes.values()))
        self.assertNotIn("0.1", str(span.attributes.values()))


class EmbeddingsWrapTest(unittest.TestCase):
    def setUp(self) -> None:
        self.prometa = Prometa(
            endpoint="http://localhost:0/never-flushed",
            api_key=None,
            solution_id="sol-test",
            agent_name="test-agent",
            agent_id="test-agent-id",
            stage="test",
        )

    def test_sync_wrap_stamps_attrs_and_nests_under_parent(self) -> None:
        prometa_openai._wrap_sync_create(_FakeEmbeddings, "embeddings")

        @self.prometa.workflow(name="retrieval.query")
        def handle():
            return _FakeEmbeddings().create(
                model="text-embedding-3-small",
                input=["doc one", "doc two"],
            )

        response = handle()
        self.assertEqual(len(response.data), 2)

        embedding_span = self.prometa._buffer[0]
        parent_span = self.prometa._buffer[1]
        self.assertEqual(embedding_span.name, "openai.embeddings:text-embedding-3-small")
        self.assertEqual(embedding_span.parent_span_id, parent_span.span_id)
        self.assertEqual(embedding_span.trace_id, parent_span.trace_id)
        attrs = embedding_span.attributes
        self.assertEqual(attrs["gen_ai.system"], "openai")
        self.assertEqual(attrs["gen_ai.operation.name"], "embeddings")
        self.assertEqual(attrs["gen_ai.request.model"], "text-embedding-3-small")
        self.assertEqual(attrs["gen_ai.request.embedding.input_count"], 2)
        self.assertEqual(attrs["gen_ai.usage.input_tokens"], 12)
        self.assertEqual(attrs["gen_ai.usage.total_tokens"], 12)
        self.assertEqual(attrs["gen_ai.response.embedding.count"], 2)
        self.assertNotIn("gen_ai.completion", attrs)
        # Vectors must never land in attributes.
        self.assertTrue(all("0.1" not in str(v) for v in attrs.values()))

    def test_async_wrap_stamps_attrs(self) -> None:
        prometa_openai._wrap_async_create(_FakeAsyncEmbeddings, "embeddings")

        @self.prometa.workflow(name="retrieval.query")
        async def handle():
            return await _FakeAsyncEmbeddings().create(
                model="text-embedding-3-small",
                input="single query",
            )

        asyncio.run(handle())
        embedding_span = self.prometa._buffer[0]
        self.assertEqual(
            embedding_span.attributes["gen_ai.operation.name"],
            "embeddings",
        )
        self.assertEqual(
            embedding_span.attributes["gen_ai.request.embedding.input_count"],
            1,
        )
        self.assertEqual(embedding_span.attributes["gen_ai.usage.input_tokens"], 4)


@unittest.skipUnless(
    _openai_embeddings_importable(),
    "openai embeddings resource not importable",
)
class EmbeddingsInstallTest(unittest.TestCase):
    def test_install_patches_sync_and_async_create(self) -> None:
        from openai.resources.embeddings import AsyncEmbeddings, Embeddings

        prometa_openai._INSTALLED = False
        self.assertTrue(prometa_openai.install())
        self.assertTrue(getattr(Embeddings.create, "__prometa_wrapped__", False))
        self.assertTrue(getattr(AsyncEmbeddings.create, "__prometa_wrapped__", False))


if __name__ == "__main__":
    unittest.main()
