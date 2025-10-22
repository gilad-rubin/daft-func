"""Test the user's example to verify cache hits on 3rd run."""

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

# Create runner with auto mode (chooses based on batch size)
runner = Runner(
    pipeline=pipeline,
    mode="local",
    batch_threshold=2,
    cache_config=CacheConfig(enabled=True, backend=DiskCache(cache_dir=".cache")),
)

print("=" * 60)
print("RUN 1: First execution (should be MISS)")
print("=" * 60)
result1 = runner.run(inputs=single_inputs)

print("\n" + "=" * 60)
print("RUN 2: Same inputs, same instances (should be HIT)")
print("=" * 60)
result2 = runner.run(inputs=single_inputs)

print("\n" + "=" * 60)
print("RUN 3: New instances, same config (should be HIT now!)")
print("=" * 60)
# Create NEW instances with same configuration
single_inputs_new = {
    "retriever": ToyRetriever(),  # NEW instance
    "corpus": corpus,
    "reranker": IdentityReranker(),  # NEW instance
    "query": Query(query_uuid="q1", text="quick brown"),
    "top_k": 2,
}
result3 = runner.run(inputs=single_inputs_new)

print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)
print(f"Run 1 result == Run 2 result: {result1 == result2}")
print(f"Run 1 result == Run 3 result: {result1 == result3}")
print("\nExpected output:")
print("- Run 1: MISS on all nodes")
print("- Run 2: HIT on all nodes")
print("- Run 3: HIT on all nodes (NEW instances, same config)")
print("\nIf Run 3 shows HITs, the caching fix works! ✓")
