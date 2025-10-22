"""Verify the user's original example now works."""

from typing import Dict, List

from daft_func import Pipeline, Runner, func
from examples.retrieval import (
    IdentityReranker,
    Query,
    RerankedHit,
    Reranker,
    RetrievalResult,
    Retriever,
    ToyRetriever,
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


# Create pipeline with explicit functions
pipeline = Pipeline(functions=[index, retrieve, rerank])

corpus = {
    "d1": "a quick brown fox jumps",
    "d2": "brown dog sleeps",
    "d3": "five boxing wizards jump quickly",
}

multi_inputs = {
    "test": True,
    "corpus": corpus,
    "retriever": ToyRetriever(),
    "reranker": IdentityReranker(),
    "query": [
        Query(query_uuid="q1", text="quick brown"),
        Query(query_uuid="q2", text="wizards jump"),
        Query(query_uuid="q3", text="brown dog"),
    ],
    "top_k": 2,
}

# Create runner with auto mode (chooses based on batch size)
runner = Runner(pipeline=pipeline, mode="auto", batch_threshold=2)

print("Running with multi_inputs...")
result = runner.run(inputs=multi_inputs)

print("\n✓ Success! No AttributeError")
print(f"\nResult keys: {result.keys()}")
print(f"Index result: {result['index']}")
print(f"Number of queries: {len(result['reranked_hits'])}")
print(f"First reranked hits: {result['reranked_hits'][0]}")
