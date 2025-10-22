"""Benchmark the exact user example."""

import time
from typing import Dict, List

from daft_func import Pipeline, func
from examples.retrieval import (
    Query,
    RerankedHit,
    Reranker,
    RetrievalResult,
    Retriever,
)


@func(output="index")
def index(retriever: Retriever, corpus: Dict[str, str], test: bool = True) -> bool:
    retriever.index(corpus)
    return True


@func(output="hits", map_axis="query", key_attr="query_uuid")
def retrieve(
    retriever: Retriever, query: Query, top_k: int, index: bool
) -> RetrievalResult:
    return retriever.retrieve(query, top_k=top_k)


@func(output="reranked_hits", map_axis="query", key_attr="query_uuid")
def rerank(
    reranker: Reranker, query: Query, hits: RetrievalResult, top_k: int
) -> List[RerankedHit]:
    return reranker.rerank(query, hits, top_k=top_k)


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("USER EXAMPLE PERFORMANCE TEST")
    print("=" * 80)

    # Create pipeline with explicit functions
    pipeline = Pipeline(functions=[index, retrieve, rerank])

    # Test multiple runs
    times = []
    for i in range(10):
        start = time.perf_counter()
        result = pipeline.visualize()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        print(f"  Run {i + 1:2d}: {elapsed * 1000:6.2f} ms")

    print("\n" + "=" * 80)
    print(f"  Mean:  {sum(times) / len(times) * 1000:6.2f} ms")
    print(f"  Min:   {min(times) * 1000:6.2f} ms")
    print(f"  Max:   {max(times) * 1000:6.2f} ms")
    print(f"  First: {times[0] * 1000:6.2f} ms (includes import overhead)")
    print(f"  Avg (excluding first): {sum(times[1:]) / len(times[1:]) * 1000:6.2f} ms")
    print("=" * 80)
