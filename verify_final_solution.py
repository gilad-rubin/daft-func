"""Final verification of the caching solution with user's exact code."""

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


@func(output="index", cache=True)
def index(retriever: Retriever, corpus: Dict[str, str], test: bool = True) -> bool:
    retriever.index(corpus)
    return True


@func(output="hits", map_axis="query", key_attr="query_uuid", cache=True)
def retrieve(
    retriever: Retriever, query: Query, top_k: int, index: bool
) -> RetrievalResult:
    return retriever.retrieve(query, top_k=top_k)


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

print("Run 1: should show misses")
result = runner.run(inputs=single_inputs)

print("\nRun 2: should show hits (same instances)")
result = runner.run(inputs=single_inputs)

# NEW INSTANCES - this was the problem!
single_inputs = {
    "retriever": ToyRetriever(),
    "corpus": corpus,
    "reranker": IdentityReranker(),
    "query": Query(query_uuid="q1", text="quick brown"),
    "top_k": 2,
}

print("\nRun 3: should show hits (new instances, same config) - THE FIX!")
result = runner.run(inputs=single_inputs)

print("\n✓ SUCCESS! Run 3 shows cache HITs with new instances.")

