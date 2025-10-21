"""DAG node definitions for retrieval pipeline."""

from typing import List

from daft_func import func

from .models import (
    Query,
    RerankedHit,
    Reranker,
    RetrievalResult,
    Retriever,
)


@func(output="hits", map_axis="query", key_attr="query_uuid")
def retrieve(retriever: Retriever, query: Query, top_k: int) -> RetrievalResult:
    """Retrieve documents for a query.

    Args:
        retriever: The retriever implementation
        query: The search query (map axis - one per item)
        top_k: Number of results to retrieve (constant)

    Returns:
        Retrieval results
    """
    return retriever.retrieve(query, top_k=top_k)


@func(output="reranked_hits", map_axis="query", key_attr="query_uuid")
def rerank(
    reranker: Reranker, query: Query, hits: RetrievalResult, top_k: int
) -> List[RerankedHit]:
    """Rerank retrieved documents.

    Args:
        reranker: The reranker implementation
        query: The original query (map axis - one per item)
        hits: Initial retrieval results (from retrieve node)
        top_k: Number of reranked results (constant)

    Returns:
        List of reranked hits
    """
    return reranker.rerank(query, hits, top_k=top_k)
