"""Test script to verify default parameters work correctly."""

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


@func(output="hits")
def retrieve(
    retriever: Retriever, query: Query, top_k: int, index: bool
) -> RetrievalResult:
    return retriever.retrieve(query, top_k=top_k)


@func(output="reranked_hits")
def rerank(
    reranker: Reranker, query: Query, hits: RetrievalResult, top_k: int
) -> List[RerankedHit]:
    return reranker.rerank(query, hits, top_k=top_k)


# Create pipeline with explicit functions
pipeline = Pipeline(functions=[index, retrieve, rerank])
print("Pipeline created successfully!")

# Visualize to verify structure
pipeline.visualize()
print("Pipeline visualization created!")

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
    # Note: NOT providing "test" parameter - should use default value of True
}

# Create runner with auto mode (chooses based on batch size)
runner = Runner(pipeline=pipeline, mode="auto", batch_threshold=2)
print("Runner created successfully!")

result = runner.run(inputs=single_inputs)
print(f"Result: {result}")
print("\nSuccess! The pipeline ran with default parameters.")
