"""Test that MemoryCache and DiskCache behave identically."""

import tempfile
from typing import Dict, List

import pytest

from daft_func import CacheConfig, DiskCache, MemoryCache, Pipeline, Runner, func
from examples.retrieval import (
    IdentityReranker,
    Query,
    RerankedHit,
    Reranker,
    RetrievalResult,
    Retriever,
    ToyRetriever,
)


@func(output="index_path", cache=True)
def index(retriever: Retriever, corpus: Dict[str, str], test: bool = True) -> str:
    """Index function that returns str (not bool)."""
    index_path = retriever.index(corpus)
    return index_path


@func(output="hits", map_axis="query", key_attr="query_uuid", cache=True)
def retrieve(
    retriever: Retriever, query: Query, top_k: int, index_path: str
) -> RetrievalResult:
    return retriever.retrieve(index_path, query, top_k=top_k)


@func(output="reranked_hits", map_axis="query", key_attr="query_uuid", cache=True)
def rerank(
    reranker: Reranker, query: Query, hits: RetrievalResult, top_k: int
) -> List[RerankedHit]:
    return reranker.rerank(query, hits, top_k=top_k)


@pytest.fixture
def corpus():
    """Test corpus."""
    return {
        "d1": "a quick brown fox jumps",
        "d2": "brown dog sleeps",
        "d3": "five boxing wizards jump quickly",
    }


@pytest.fixture
def pipeline():
    """Test pipeline."""
    return Pipeline(functions=[index, retrieve, rerank])


def test_memory_cache_with_new_instances(corpus, pipeline, capsys):
    """Test that MemoryCache works correctly with new instances."""
    runner = Runner(
        pipeline=pipeline,
        mode="local",
        batch_threshold=2,
        cache_config=CacheConfig(enabled=True, backend=MemoryCache()),
    )

    # Run 1: MISS
    runner.run(
        inputs={
            "retriever": ToyRetriever(),
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        }
    )
    captured = capsys.readouterr()
    assert "index_path: ✗ MISS" in captured.out
    assert "hits: ✗ MISS" in captured.out
    assert "reranked_hits: ✗ MISS" in captured.out

    # Run 2: HIT (new instance but same __cache_key__)
    runner.run(
        inputs={
            "retriever": ToyRetriever(),
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        }
    )
    captured = capsys.readouterr()
    assert "index_path: ✓ HIT" in captured.out
    assert "hits: ✓ HIT" in captured.out
    assert "reranked_hits: ✓ HIT" in captured.out

    # Run 3: HIT (another new instance)
    runner.run(
        inputs={
            "retriever": ToyRetriever(),
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        }
    )
    captured = capsys.readouterr()
    assert "index_path: ✓ HIT" in captured.out
    assert "hits: ✓ HIT" in captured.out
    assert "reranked_hits: ✓ HIT" in captured.out


def test_disk_cache_with_new_instances(corpus, pipeline, capsys):
    """Test that DiskCache works correctly with new instances."""
    with tempfile.TemporaryDirectory() as tmpdir:
        runner = Runner(
            pipeline=pipeline,
            mode="local",
            batch_threshold=2,
            cache_config=CacheConfig(enabled=True, backend=DiskCache(cache_dir=tmpdir)),
        )

        # Run 1: MISS
        runner.run(
            inputs={
                "retriever": ToyRetriever(),
                "corpus": corpus,
                "reranker": IdentityReranker(),
                "query": Query(query_uuid="q1", text="quick brown"),
                "top_k": 2,
            }
        )
        captured = capsys.readouterr()
        assert "index_path: ✗ MISS" in captured.out
        assert "hits: ✗ MISS" in captured.out
        assert "reranked_hits: ✗ MISS" in captured.out

        # Run 2: HIT (new instance but same __cache_key__)
        runner.run(
            inputs={
                "retriever": ToyRetriever(),
                "corpus": corpus,
                "reranker": IdentityReranker(),
                "query": Query(query_uuid="q1", text="quick brown"),
                "top_k": 2,
            }
        )
        captured = capsys.readouterr()
        assert "index_path: ✓ HIT" in captured.out
        assert "hits: ✓ HIT" in captured.out
        assert "reranked_hits: ✓ HIT" in captured.out

        # Run 3: HIT (another new instance)
        runner.run(
            inputs={
                "retriever": ToyRetriever(),
                "corpus": corpus,
                "reranker": IdentityReranker(),
                "query": Query(query_uuid="q1", text="quick brown"),
                "top_k": 2,
            }
        )
        captured = capsys.readouterr()
        assert "index_path: ✓ HIT" in captured.out
        assert "hits: ✓ HIT" in captured.out
        assert "reranked_hits: ✓ HIT" in captured.out


def test_memory_and_disk_cache_parity(corpus, pipeline):
    """Test that MemoryCache and DiskCache produce identical results."""

    # Test with MemoryCache
    runner_mem = Runner(
        pipeline=pipeline,
        mode="local",
        cache_config=CacheConfig(enabled=True, backend=MemoryCache()),
    )

    result_mem = runner_mem.run(
        inputs={
            "retriever": ToyRetriever(),
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        }
    )

    # Test with DiskCache
    with tempfile.TemporaryDirectory() as tmpdir:
        runner_disk = Runner(
            pipeline=pipeline,
            mode="local",
            cache_config=CacheConfig(enabled=True, backend=DiskCache(cache_dir=tmpdir)),
        )

        result_disk = runner_disk.run(
            inputs={
                "retriever": ToyRetriever(),
                "corpus": corpus,
                "reranker": IdentityReranker(),
                "query": Query(query_uuid="q1", text="quick brown"),
                "top_k": 2,
            }
        )

    # Results should be identical
    assert result_mem["reranked_hits"] == result_disk["reranked_hits"]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
