"""Reproduce the exact user scenario."""

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


def test_memory_cache():
    """Exact user scenario with MemoryCache."""
    pipeline = Pipeline(functions=[index, retrieve, rerank])

    corpus = {
        "d1": "a quick brown fox jumps",
        "d2": "brown dog sleeps",
        "d3": "five boxing wizards jump quickly",
    }

    runner = Runner(
        mode="local",
        batch_threshold=2,
        cache_config=CacheConfig(enabled=True, backend=MemoryCache()),
    )

    print("=" * 70)
    print("MEMORY CACHE - User's exact scenario")
    print("=" * 70)

    # First run
    print("\nRun 1:")
    single_inputs = {
        "retriever": ToyRetriever(),
        "corpus": corpus,
        "reranker": IdentityReranker(),
        "query": Query(query_uuid="q1", text="quick brown"),
        "top_k": 2,
    }
    result = runner.run(pipeline, inputs=single_inputs)

    # Second run
    print("\nRun 2:")
    result = runner.run(pipeline, inputs=single_inputs)

    # Third run - NEW inputs dict with NEW instances
    print("\nRun 3 (new inputs dict, new instances):")
    corpus = {
        "d1": "a quick brown fox jumps",
        "d2": "brown dog sleeps",
        "d3": "five boxing wizards jump quickly",
    }
    single_inputs = {
        "retriever": ToyRetriever(),
        "corpus": corpus,
        "reranker": IdentityReranker(),
        "query": Query(query_uuid="q1", text="quick brown"),
        "top_k": 2,
    }
    result = runner.run(pipeline, inputs=single_inputs)


def test_disk_cache():
    """Exact user scenario with DiskCache."""
    pipeline = Pipeline(functions=[index, retrieve, rerank])

    corpus = {
        "d1": "a quick brown fox jumps",
        "d2": "brown dog sleeps",
        "d3": "five boxing wizards jump quickly",
    }

    runner = Runner(
        mode="local",
        batch_threshold=2,
        cache_config=CacheConfig(enabled=True, backend=DiskCache(cache_dir=".cache")),
    )

    print("\n" + "=" * 70)
    print("DISK CACHE - User's exact scenario")
    print("=" * 70)

    # First run
    print("\nRun 1:")
    single_inputs = {
        "retriever": ToyRetriever(),
        "corpus": corpus,
        "reranker": IdentityReranker(),
        "query": Query(query_uuid="q1", text="quick brown"),
        "top_k": 2,
    }
    result = runner.run(pipeline, inputs=single_inputs)

    # Second run
    print("\nRun 2:")
    result = runner.run(pipeline, inputs=single_inputs)

    # Third run - NEW inputs dict with NEW instances
    print("\nRun 3 (new inputs dict, new instances):")
    corpus = {
        "d1": "a quick brown fox jumps",
        "d2": "brown dog sleeps",
        "d3": "five boxing wizards jump quickly",
    }
    single_inputs = {
        "retriever": ToyRetriever(),
        "corpus": corpus,
        "reranker": IdentityReranker(),
        "query": Query(query_uuid="q1", text="quick brown"),
        "top_k": 2,
    }
    result = runner.run(pipeline, inputs=single_inputs)


if __name__ == "__main__":
    test_memory_cache()
    test_disk_cache()
