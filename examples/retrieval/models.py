"""Pydantic models and protocols for retrieval pipeline."""

from typing import List, Protocol

from pydantic import BaseModel


class Query(BaseModel):
    """A search query."""

    query_uuid: str
    text: str


class RetrievalHit(BaseModel):
    """A single retrieval result."""

    doc_id: str
    score: float


class RetrievalResult(BaseModel):
    """Results from the retrieval stage."""

    query_uuid: str
    hits: List[RetrievalHit]


class RerankedHit(BaseModel):
    """A reranked result."""

    query_uuid: str
    doc_id: str
    score: float


class Retriever(Protocol):
    """Protocol for retrieval implementations."""

    def index(self, corpus: dict[str, str]) -> str:
        """Index a corpus and return the path to the index file.

        Args:
            corpus: Dictionary mapping doc_id to document text

        Returns:
            Path to the index file on disk
        """
        ...

    def retrieve(self, index_path: str, query: Query, top_k: int) -> RetrievalResult:
        """Retrieve documents for a query.

        Args:
            index_path: Path to the index file
            query: The search query
            top_k: Number of results to return

        Returns:
            Retrieval results
        """
        ...


class Reranker(Protocol):
    """Protocol for reranking implementations."""

    def rerank(
        self, query: Query, hits: RetrievalResult, top_k: int
    ) -> List[RerankedHit]:
        """Rerank retrieved documents.

        Args:
            query: The original query
            hits: Initial retrieval results
            top_k: Number of reranked results to return

        Returns:
            List of reranked hits
        """
        ...
