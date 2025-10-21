"""Toy implementations of retriever and reranker for demonstration."""

import re
from typing import Dict, List

from examples.retrieval.models import (
    Query,
    RerankedHit,
    RetrievalHit,
    RetrievalResult,
)


def normalize(text: str) -> set[str]:
    """Normalize text for simple token matching."""
    toks = re.findall(r"[a-zA-Z]+", text.lower())
    return {t[:-1] if t.endswith("s") else t for t in toks}


class ToyRetriever:
    """Simple token-overlap based retriever for demonstration."""

    def __init__(self, corpus: Dict[str, str]):
        """Initialize with a document corpus.

        Args:
            corpus: Dictionary mapping doc_id to document text
        """
        self._doc_tokens = {doc_id: normalize(txt) for doc_id, txt in corpus.items()}

    def retrieve(self, query: Query, top_k: int) -> RetrievalResult:
        """Retrieve documents using token overlap scoring."""
        q = normalize(query.text)
        scored = [
            RetrievalHit(doc_id=d, score=float(len(q & toks)))
            for d, toks in self._doc_tokens.items()
        ]
        scored.sort(key=lambda h: h.score, reverse=True)
        return RetrievalResult(query_uuid=query.query_uuid, hits=scored[:top_k])


class IdentityReranker:
    """Pass-through reranker that returns hits unchanged."""

    def rerank(
        self, query: Query, hits: RetrievalResult, top_k: int
    ) -> List[RerankedHit]:
        """Return hits as reranked hits without modification."""
        return [
            RerankedHit(query_uuid=query.query_uuid, doc_id=h.doc_id, score=h.score)
            for h in hits.hits[:top_k]
        ]
