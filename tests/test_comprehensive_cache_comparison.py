"""Comprehensive test comparing MemoryCache and DiskCache behavior."""

import tempfile
from typing import Dict, List

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


def run_test_sequence(backend_name: str, cache_backend):
    """Run identical test sequence with given cache backend."""
    print("\n" + "=" * 70)
    print(f"{backend_name} TEST")
    print("=" * 70)

    pipeline = Pipeline(functions=[index, retrieve, rerank])
    runner = Runner(
        mode="local",
        batch_threshold=2,
        cache_config=CacheConfig(enabled=True, backend=cache_backend),
    )

    corpus = {
        "d1": "a quick brown fox jumps",
        "d2": "brown dog sleeps",
        "d3": "five boxing wizards jump quickly",
    }

    # Run 1: Fresh instance
    print("\nRun 1: New retriever instance")
    result1 = runner.run(
        inputs={
            "retriever": ToyRetriever(),
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        }
    )

    # Run 2: Another new instance (same config)
    print("\nRun 2: Another new retriever instance")
    result2 = runner.run(
        inputs={
            "retriever": ToyRetriever(),
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        }
    )

    # Run 3: Yet another new instance
    print("\nRun 3: Yet another new retriever instance")
    result3 = runner.run(
        inputs={
            "retriever": ToyRetriever(),
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        }
    )

    # Run 4: Reuse same instance
    print("\nRun 4: Reusing same retriever instance from run 3")
    r = ToyRetriever()
    runner.run(
        inputs={
            "retriever": r,
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        }
    )

    print("\nRun 5: Same instance again")
    runner.run(
        inputs={
            "retriever": r,
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        }
    )

    return result1, result2, result3


def main():
    """Run comprehensive comparison."""

    # Test with MemoryCache
    memory_results = run_test_sequence("MEMORY CACHE", MemoryCache())

    # Test with DiskCache (fresh directory)
    with tempfile.TemporaryDirectory() as tmpdir:
        disk_results = run_test_sequence("DISK CACHE", DiskCache(cache_dir=tmpdir))

    print("\n" + "=" * 70)
    print("COMPARISON SUMMARY")
    print("=" * 70)
    print("Both backends should show:")
    print("  Run 1: MISS MISS MISS")
    print("  Run 2: HIT HIT HIT")
    print("  Run 3: HIT HIT HIT")
    print("  Run 4: HIT HIT HIT")
    print("  Run 5: HIT HIT HIT")
    print("\nIf they differ, there's a discrepancy to investigate!")


if __name__ == "__main__":
    main()
