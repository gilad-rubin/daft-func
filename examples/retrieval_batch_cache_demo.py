"""Demo showing per-item caching with batch retrieval."""

from typing import Dict, List

from daft_func import CacheConfig, DiskCache, Pipeline, Runner, func
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
def index(retriever: Retriever, corpus: Dict[str, str]) -> str:
    """Index the corpus."""
    print("  [Indexing corpus]")
    index_path = retriever.index(corpus)
    return index_path


@func(output="hits", map_axis="query", key_attr="query_uuid", cache=True)
def retrieve(
    retriever: Retriever, query: Query, top_k: int, index_path: str
) -> RetrievalResult:
    """Retrieve documents."""
    print(f"  [Retrieving for '{query.text}' (id={query.query_uuid})]")
    return retriever.retrieve(index_path, query, top_k=top_k)


@func(output="reranked_hits", map_axis="query", key_attr="query_uuid", cache=True)
def rerank(
    reranker: Reranker, query: Query, hits: RetrievalResult, top_k: int
) -> List[RerankedHit]:
    """Rerank retrieved documents."""
    print(f"  [Reranking for '{query.text}' (id={query.query_uuid})]")
    return reranker.rerank(query, hits, top_k=top_k)


def main():
    """Run the batch retrieval caching demo."""
    pipeline = Pipeline(functions=[index, retrieve, rerank])

    corpus = {
        "d1": "a quick brown fox jumps",
        "d2": "brown dog sleeps",
        "d3": "five boxing wizards jump quickly",
    }

    queries = [
        Query(query_uuid="q1", text="quick brown"),
        Query(query_uuid="q2", text="wizards jump"),
        Query(query_uuid="q3", text="brown dog"),
    ]

    print("=" * 70)
    print("Batch Retrieval with Per-Item Caching Demo")
    print("=" * 70)

    # Create runner with caching
    runner = Runner(
        pipeline=pipeline,
        mode="local",  # Use local mode for demonstration
        cache_config=CacheConfig(enabled=True, backend=DiskCache(cache_dir=".cache")),
    )

    # Reuse same instances (more realistic - you don't recreate index each time)
    retriever = ToyRetriever()
    reranker = IdentityReranker()

    print("\n📝 Run 1: Process 3 queries (cold cache)")
    print("-" * 70)
    result1 = runner.run(
        inputs={
            "retriever": retriever,
            "corpus": corpus,
            "reranker": reranker,
            "query": queries,
            "top_k": 2,
        }
    )
    print(f"✓ Processed {len(result1['reranked_hits'])} queries")

    print("\n📝 Run 2: Same 3 queries (warm cache)")
    print("-" * 70)
    result2 = runner.run(
        inputs={
            "retriever": retriever,
            "corpus": corpus,
            "reranker": reranker,
            "query": queries,
            "top_k": 2,
        }
    )
    print(f"✓ Processed {len(result2['reranked_hits'])} queries")

    print("\n📝 Run 3: Mix of cached and new queries")
    print("-" * 70)
    mixed_queries = [
        Query(query_uuid="q1", text="quick brown"),  # Cached
        Query(query_uuid="q4", text="new query"),  # New
        Query(query_uuid="q2", text="wizards jump"),  # Cached
    ]
    result3 = runner.run(
        inputs={
            "retriever": retriever,
            "corpus": corpus,
            "reranker": reranker,
            "query": mixed_queries,
            "top_k": 2,
        }
    )
    print(f"✓ Processed {len(result3['reranked_hits'])} queries")

    print("\n" + "=" * 70)
    print("✨ Per-Item Caching Features:")
    print("=" * 70)
    print("• Each query is cached individually by its query_uuid")
    print("• Reprocessing q1 and q2 uses cache (instant!)")
    print("• New query q4 executes normally")
    print("• Non-mapped nodes (index) cache once for all items")
    print("=" * 70)


if __name__ == "__main__":
    main()
