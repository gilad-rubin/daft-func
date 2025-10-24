"""Test to reproduce memory cache vs disk cache difference."""

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
    """Index returns a path string (not bool)."""
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


def test_memory_cache():
    """Test with MemoryCache - expect issue on third run."""
    pipeline = Pipeline(functions=[index, retrieve, rerank])

    corpus = {
        "d1": "a quick brown fox jumps",
        "d2": "brown dog sleeps",
        "d3": "five boxing wizards jump quickly",
    }

    runner = Runner(
        pipeline=pipeline,
        mode="local",
        batch_threshold=2,
        cache_config=CacheConfig(enabled=True, backend=MemoryCache()),
    )

    print("\n" + "=" * 70)
    print("MEMORY CACHE TEST")
    print("=" * 70)

    # Run 1: New retriever instance
    print("\n1. First run (new retriever instance):")
    single_inputs = {
        "retriever": ToyRetriever(),
        "corpus": corpus,
        "reranker": IdentityReranker(),
        "query": Query(query_uuid="q1", text="quick brown"),
        "top_k": 2,
    }
    result = runner.run(inputs=single_inputs)
    print("   Expected: MISS MISS MISS")

    # Run 2: Another new retriever instance (same config)
    print("\n2. Second run (another new retriever instance):")
    single_inputs = {
        "retriever": ToyRetriever(),
        "corpus": corpus,
        "reranker": IdentityReranker(),
        "query": Query(query_uuid="q1", text="quick brown"),
        "top_k": 2,
    }
    result = runner.run(inputs=single_inputs)
    print("   Expected: HIT HIT HIT")

    # Run 3: Yet another new retriever instance (same config)
    print("\n3. Third run (yet another new retriever instance):")
    single_inputs = {
        "retriever": ToyRetriever(),
        "corpus": corpus,
        "reranker": IdentityReranker(),
        "query": Query(query_uuid="q1", text="quick brown"),
        "top_k": 2,
    }
    result = runner.run(inputs=single_inputs)
    print("   Expected: HIT HIT HIT")
    print("   PROBLEM: Getting MISS instead!")


def test_disk_cache():
    """Test with DiskCache - works correctly."""
    pipeline = Pipeline(functions=[index, retrieve, rerank])

    corpus = {
        "d1": "a quick brown fox jumps",
        "d2": "brown dog sleeps",
        "d3": "five boxing wizards jump quickly",
    }

    runner = Runner(
        pipeline=pipeline,
        mode="local",
        batch_threshold=2,
        cache_config=CacheConfig(enabled=True, backend=DiskCache(cache_dir=".cache")),
    )

    print("\n" + "=" * 70)
    print("DISK CACHE TEST")
    print("=" * 70)

    # Run 1: New retriever instance
    print("\n1. First run (new retriever instance):")
    single_inputs = {
        "retriever": ToyRetriever(),
        "corpus": corpus,
        "reranker": IdentityReranker(),
        "query": Query(query_uuid="q1", text="quick brown"),
        "top_k": 2,
    }
    result = runner.run(inputs=single_inputs)
    print("   Expected: MISS MISS MISS")

    # Run 2: Another new retriever instance (same config)
    print("\n2. Second run (another new retriever instance):")
    single_inputs = {
        "retriever": ToyRetriever(),
        "corpus": corpus,
        "reranker": IdentityReranker(),
        "query": Query(query_uuid="q1", text="quick brown"),
        "top_k": 2,
    }
    result = runner.run(inputs=single_inputs)
    print("   Expected: HIT HIT HIT")

    # Run 3: Yet another new retriever instance (same config)
    print("\n3. Third run (yet another new retriever instance):")
    single_inputs = {
        "retriever": ToyRetriever(),
        "corpus": corpus,
        "reranker": IdentityReranker(),
        "query": Query(query_uuid="q1", text="quick brown"),
        "top_k": 2,
    }
    result = runner.run(inputs=single_inputs)
    print("   Expected: HIT HIT HIT")
    print("   SUCCESS: All hits!")


if __name__ == "__main__":
    test_memory_cache()
    test_disk_cache()
