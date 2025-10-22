"""Retrieval pipeline example for daft_func."""

from .implementations import IdentityReranker, ToyRetriever
from .models import (
    Query,
    RerankedHit,
    Reranker,
    RetrievalHit,
    RetrievalResult,
    Retriever,
)
from .nodes import (
    build_index_artifact,
    rerank,
    retrieve,
    retrieve_with_index,
)

__all__ = [
    "Query",
    "RetrievalHit",
    "RetrievalResult",
    "RerankedHit",
    "Retriever",
    "Reranker",
    "ToyRetriever",
    "IdentityReranker",
    "retrieve",
    "build_index_artifact",
    "retrieve_with_index",
    "rerank",
]
