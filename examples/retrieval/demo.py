"""Demo script showing retrieval pipeline with daft_func."""

from daft_func import Pipeline, Runner

# Import from the examples.retrieval package
# Note: Run this script from the project root with: uv run python examples/retrieval/demo.py
try:
    from examples.retrieval import (
        IdentityReranker,
        Query,
        ToyRetriever,
        index,
        rerank,
        retrieve,
    )
except ImportError:
    # If running from the examples/retrieval directory
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from examples.retrieval import (
        IdentityReranker,
        Query,
        ToyRetriever,
        index,
        rerank,
        retrieve,
    )


def main():
    """Run the retrieval pipeline demo."""

    # Setup: Create a toy corpus and implementations
    corpus = {
        "d1": "a quick brown fox jumps",
        "d2": "brown dog sleeps",
        "d3": "five boxing wizards jump quickly",
    }
    retriever = ToyRetriever()
    reranker = IdentityReranker()

    # Create pipeline with explicit functions including index
    pipeline = Pipeline(functions=[index, retrieve, rerank])

    # Create runner with auto mode (chooses based on batch size)
    runner = Runner(pipeline=pipeline, mode="auto", batch_threshold=2)

    print("=" * 70)
    print("daft_func Retrieval Pipeline Demo")
    print("=" * 70)

    # Example 1: Single query
    print("\n📝 Example 1: Single Query\n")
    single_inputs = {
        "corpus": corpus,
        "retriever": retriever,
        "reranker": reranker,
        "query": Query(query_uuid="q1", text="quick brown"),
        "top_k": 2,
    }

    result = runner.run(inputs=single_inputs)
    print(f"Query: {single_inputs['query'].text}")
    print(f"Results: {len(result['reranked_hits'])} hits")
    for hit in result["reranked_hits"]:
        print(f"  - {hit.doc_id}: {hit.score}")

    # Example 2: Multiple queries (batch processing)
    print("\n📝 Example 2: Multiple Queries (Batch Processing)\n")
    multi_inputs = {
        "corpus": corpus,
        "retriever": retriever,
        "reranker": reranker,
        "query": [
            Query(query_uuid="q1", text="quick brown"),
            Query(query_uuid="q2", text="wizards jump"),
            Query(query_uuid="q3", text="brown dog"),
        ],
        "top_k": 2,
    }

    result = runner.run(inputs=multi_inputs)
    print(f"Processed {len(result['reranked_hits'])} queries")
    for i, (query, hits) in enumerate(
        zip(multi_inputs["query"], result["reranked_hits"])
    ):
        print(f"\nQuery {i + 1}: '{query.text}'")
        print(f"  Results: {len(hits)} hits")
        for hit in hits:
            print(f"    - {hit.doc_id}: {hit.score}")

    # Example 3: Different execution modes
    print("\n📝 Example 3: Testing Different Execution Modes\n")

    for mode in ["local", "daft", "auto"]:
        runner = Runner(pipeline=pipeline, mode=mode, batch_threshold=2)
        result = runner.run(inputs=multi_inputs)
        print(
            f"✅ {mode.upper():5s} mode: {len(result['reranked_hits'])} queries processed"
        )

    print("\n" + "=" * 70)
    print("🎉 Demo complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
