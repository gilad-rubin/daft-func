"""Minimal test case for batch execution bug with List return types."""

from typing import Dict, List

from pydantic import BaseModel

from daft_func import Pipeline, Runner, func


class Query(BaseModel):
    """Query model."""

    id: str
    text: str


class Hit(BaseModel):
    """Hit model."""

    doc_id: str
    score: float


class RetrievalResult(BaseModel):
    """Retrieval result with list of hits."""

    query_id: str
    hits: List[Hit]


class RankedHit(BaseModel):
    """Final ranked hit."""

    query_id: str
    doc_id: str
    score: float


@func(output="index")
def index(corpus: Dict[str, str]) -> bool:
    """Index corpus."""
    return True


@func(output="hits", map_axis="query", key_attr="id")
def retrieve(query: Query, top_k: int, index: bool) -> RetrievalResult:
    """Retrieve documents."""
    return RetrievalResult(
        query_id=query.id,
        hits=[
            Hit(doc_id="d1", score=0.9),
            Hit(doc_id="d2", score=0.7),
        ][:top_k],
    )


@func(output="ranked_hits", map_axis="query", key_attr="id")
def rerank(query: Query, hits: RetrievalResult, top_k: int) -> List[RankedHit]:
    """Rerank hits - returns a list."""
    return [
        RankedHit(query_id=query.id, doc_id=h.doc_id, score=h.score + 0.1)
        for h in hits.hits[:top_k]
    ]


def test_batch_with_list_return():
    """Test batch execution with List return type."""
    # Create pipeline
    pipeline = Pipeline(functions=[index, retrieve, rerank])

    # Create inputs
    inputs = {
        "corpus": {"d1": "text1", "d2": "text2"},
        "query": [
            Query(id="q1", text="query1"),
            Query(id="q2", text="query2"),
            Query(id="q3", text="query3"),
        ],
        "top_k": 2,
    }

    # Create runner in batch mode
    runner = Runner(mode="daft")

    # This might fail with the AttributeError
    result = runner.run(pipeline, inputs=inputs)

    print("Result:", result)
    print("Type of ranked_hits:", type(result["ranked_hits"]))
    print("First element:", result["ranked_hits"][0])


if __name__ == "__main__":
    test_batch_with_list_return()
