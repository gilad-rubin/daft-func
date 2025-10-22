"""DAG node definitions for retrieval pipeline."""

from typing import Dict, List

from daft_func import func

from .implementations import normalize
from .models import (
    Query,
    RerankedHit,
    Reranker,
    RetrievalResult,
    Retriever,
)


@func(output="index_path", cache=True)
def index(retriever: Retriever, corpus: Dict[str, str]) -> str:
    """Index the corpus and return path to index file.

    Args:
        retriever: The retriever implementation
        corpus: Dictionary mapping doc_id to document text

    Returns:
        Path to the index file on disk
    """
    return retriever.index(corpus)


@func(output="hits", map_axis="query", key_attr="query_uuid", cache=False)
def retrieve(
    retriever: Retriever, query: Query, top_k: int, index_path: str
) -> RetrievalResult:
    """Retrieve documents for a query.

    Args:
        retriever: The retriever implementation
        query: The search query (map axis - one per item)
        top_k: Number of results to retrieve (constant)
        index_path: Path to the index file

    Returns:
        Retrieval results
    """
    return retriever.retrieve(index_path, query, top_k=top_k)


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


# --- Pure index artifact variant (no side effects) ---


@func(output="index_artifact", cache=True)
def build_index_artifact(corpus: Dict[str, str]) -> Dict[str, set[str]]:
    """Build a pure index artifact from the corpus.

    Returns a mapping of doc_id to normalized token set. This object is fully
    cacheable and contains no side effects on retriever objects.
    """
    return {doc_id: normalize(txt) for doc_id, txt in corpus.items()}


@func(output="hits", map_axis="query", key_attr="query_uuid", cache=True)
def retrieve_with_index(
    index_artifact: Dict[str, set[str]], query: Query, top_k: int
) -> RetrievalResult:
    """Retrieve documents using the pure index artifact.

    This is functionally equivalent to `ToyRetriever.retrieve`, but it does not
    depend on mutable object state.
    """
    q = normalize(query.text)
    scored = [(doc_id, float(len(q & toks))) for doc_id, toks in index_artifact.items()]
    scored.sort(key=lambda t: t[1], reverse=True)
    from .models import RetrievalHit

    top = [RetrievalHit(doc_id=d, score=s) for d, s in scored[:top_k]]
    return RetrievalResult(query_uuid=query.query_uuid, hits=top)
