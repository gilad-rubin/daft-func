"""Test caching with the retrieval pipeline."""

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
def index(retriever: Retriever, corpus: Dict[str, str], test: bool = True) -> str:
    """Index the corpus."""
    print("  [Indexing corpus...]")
    index_path = retriever.index(corpus)
    return index_path


@func(output="hits", map_axis="query", key_attr="query_uuid", cache=True)
def retrieve(
    retriever: Retriever, query: Query, top_k: int, index_path: str
) -> RetrievalResult:
    """Retrieve documents."""
    print(f"  [Retrieving for '{query.text}'...]")
    return retriever.retrieve(index_path, query, top_k=top_k)


@func(output="reranked_hits", map_axis="query", key_attr="query_uuid", cache=True)
def rerank(
    reranker: Reranker, query: Query, hits: RetrievalResult, top_k: int
) -> List[RerankedHit]:
    """Rerank retrieved documents."""
    print(f"  [Reranking for '{query.text}'...]")
    return reranker.rerank(query, hits, top_k=top_k)


def main():
    """Run the retrieval caching demo."""
    # Create pipeline with explicit functions
    pipeline = Pipeline(functions=[index, retrieve, rerank])

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

    # Create runner with caching enabled
    runner = Runner(
        mode="auto",
        batch_threshold=2,
        cache_config=CacheConfig(enabled=True, backend=DiskCache(cache_dir=".cache")),
    )

    print("=" * 70)
    print("Retrieval Pipeline Caching Demo")
    print("=" * 70)

    print("\n📝 Run 1: Cold cache (all functions execute)")
    print("-" * 70)
    result = runner.run(pipeline, inputs=single_inputs)
    print(f"✓ Got {len(result['reranked_hits'])} reranked hits")

    print("\n📝 Run 2: Warm cache (all cached)")
    print("-" * 70)
    result = runner.run(pipeline, inputs=single_inputs)
    print(f"✓ Got {len(result['reranked_hits'])} reranked hits")

    print("\n📝 Run 3: Change top_k (only rerank re-executes)")
    print("-" * 70)
    single_inputs["top_k"] = 3
    result = runner.run(pipeline, inputs=single_inputs)
    print(f"✓ Got {len(result['reranked_hits'])} reranked hits")

    print("\n" + "=" * 70)
    print("Notice how the cache logging shows which functions executed!")
    print("=" * 70)


if __name__ == "__main__":
    main()
