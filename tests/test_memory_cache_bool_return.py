"""Test memory cache with bool return type (original issue)."""

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


# Original side-effect version returning bool
@func(output="index", cache=True)
def index_bool(retriever: Retriever, corpus: Dict[str, str], test: bool = True) -> bool:
    """Index with side effect - returns bool."""
    retriever.index(corpus)
    return True


@func(output="hits", map_axis="query", key_attr="query_uuid", cache=True)
def retrieve_bool(
    retriever: Retriever, query: Query, top_k: int, index: bool
) -> RetrievalResult:
    # This uses the side-effect: retriever._doc_tokens
    # which was set by index_bool
    return retriever.retrieve(retriever.index({"dummy": "corpus"}), query, top_k=top_k)


@func(output="reranked_hits", map_axis="query", key_attr="query_uuid", cache=True)
def rerank(
    reranker: Reranker, query: Query, hits: RetrievalResult, top_k: int
) -> List[RerankedHit]:
    return reranker.rerank(query, hits, top_k=top_k)


def test_with_bool_return():
    """Test with bool return type."""
    pipeline = Pipeline(functions=[index_bool, retrieve_bool, rerank])

    corpus = {
        "d1": "a quick brown fox jumps",
        "d2": "brown dog sleeps",
        "d3": "five boxing wizards jump quickly",
    }

    print("\n" + "=" * 70)
    print("MEMORY CACHE - BOOL RETURN TYPE")
    print("=" * 70)

    runner = Runner(
        mode="local",
        batch_threshold=2,
        cache_config=CacheConfig(enabled=True, backend=MemoryCache()),
    )

    # Run 1
    print("\n1. First run:")
    result = runner.run(
        pipeline,
        inputs={
            "retriever": ToyRetriever(),
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        },
    )

    # Run 2
    print("\n2. Second run:")
    result = runner.run(
        pipeline,
        inputs={
            "retriever": ToyRetriever(),
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        },
    )

    # Run 3
    print("\n3. Third run:")
    result = runner.run(
        pipeline,
        inputs={
            "retriever": ToyRetriever(),
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        },
    )

    print("\n" + "=" * 70)
    print("DISK CACHE - BOOL RETURN TYPE")
    print("=" * 70)

    runner_disk = Runner(
        mode="local",
        batch_threshold=2,
        cache_config=CacheConfig(enabled=True, backend=DiskCache(cache_dir=".cache")),
    )

    # Run 1
    print("\n1. First run:")
    result = runner_disk.run(
        pipeline,
        inputs={
            "retriever": ToyRetriever(),
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        },
    )

    # Run 2
    print("\n2. Second run:")
    result = runner_disk.run(
        pipeline,
        inputs={
            "retriever": ToyRetriever(),
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        },
    )

    # Run 3
    print("\n3. Third run:")
    result = runner_disk.run(
        pipeline,
        inputs={
            "retriever": ToyRetriever(),
            "corpus": corpus,
            "reranker": IdentityReranker(),
            "query": Query(query_uuid="q1", text="quick brown"),
            "top_k": 2,
        },
    )


if __name__ == "__main__":
    test_with_bool_return()
