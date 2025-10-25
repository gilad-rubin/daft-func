"""Test script to verify improved error messages."""

from typing import Dict, List

from daft_func import Pipeline, Runner, func
from examples.retrieval import (
    Query,
    RerankedHit,
    Reranker,
    RetrievalResult,
    Retriever,
    ToyRetriever,
)


@func(output="index_path")
def index(retriever: Retriever, corpus: Dict[str, str], test: bool = True) -> str:
    index_path = retriever.index(corpus)
    return index_path


@func(output="hits")
def retrieve(
    retriever: Retriever, query: Query, top_k: int, index_path: str
) -> RetrievalResult:
    return retriever.retrieve(index_path, query, top_k=top_k)


@func(output="reranked_hits")
def rerank(
    reranker: Reranker, query: Query, hits: RetrievalResult, top_k: int
) -> List[RerankedHit]:
    return reranker.rerank(query, hits, top_k=top_k)


# Create pipeline
pipeline = Pipeline(functions=[index, retrieve, rerank])

# Intentionally missing inputs to test error message
incomplete_inputs = {
    "retriever": ToyRetriever(),
    # Missing: corpus, query, reranker, top_k
}

runner = Runner(mode="auto")
try:
    result = runner.run(pipeline, inputs=incomplete_inputs)
except RuntimeError as e:
    print("Expected error caught!")
    print(f"\nError message:\n{e}")
