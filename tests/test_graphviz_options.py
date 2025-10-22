"""Test different graphviz rendering options for performance."""

import time
from typing import Dict, List

from daft_func import Pipeline, func
from examples.retrieval import (
    Query,
    RerankedHit,
    Reranker,
    RetrievalResult,
    Retriever,
)


@func(output="index_path")
def index(retriever: Retriever, corpus: Dict[str, str], test: bool = True) -> str:
    index_path = retriever.index(corpus)
    return index_path


@func(output="hits", map_axis="query", key_attr="query_uuid")
def retrieve(
    retriever: Retriever, query: Query, top_k: int, index_path: str
) -> RetrievalResult:
    return retriever.retrieve(index_path, query, top_k=top_k)


@func(output="reranked_hits", map_axis="query", key_attr="query_uuid")
def rerank(
    reranker: Reranker, query: Query, hits: RetrievalResult, top_k: int
) -> List[RerankedHit]:
    return reranker.rerank(query, hits, top_k=top_k)


def benchmark_rendering_engines():
    """Test different graphviz rendering engines."""
    import graphviz

    from daft_func.visualization import (
        GraphvizStyle,
        build_graph,
        create_grouped_parameter_graph,
    )

    pipeline = Pipeline(functions=[index, retrieve, rerank])
    graph = build_graph(pipeline)
    plot_graph = create_grouped_parameter_graph(graph, min_arg_group_size=1)

    style = GraphvizStyle()

    # Create a simple digraph for testing
    digraph = graphviz.Digraph(
        comment="Test",
        graph_attr={
            "rankdir": "TB",
            "fontsize": str(style.font_size),
            "fontname": style.font_name,
        },
    )

    # Add a few nodes
    digraph.node("a", "Node A")
    digraph.node("b", "Node B")
    digraph.node("c", "Node C")
    digraph.edge("a", "b")
    digraph.edge("b", "c")

    # Test different engines
    engines = ["dot", "neato", "fdp", "sfdp", "circo", "twopi"]

    print("\n" + "=" * 80)
    print("GRAPHVIZ ENGINE COMPARISON (Simple graph)")
    print("=" * 80)

    results = {}
    for engine in engines:
        try:
            times = []
            for _ in range(5):
                test_digraph = digraph.copy()
                test_digraph.engine = engine

                start = time.perf_counter()
                svg = test_digraph._repr_image_svg_xml()
                elapsed = time.perf_counter() - start
                times.append(elapsed)

            avg_time = sum(times) / len(times)
            results[engine] = avg_time
            print(f"  {engine:10s}: {avg_time * 1000:6.2f} ms")
        except Exception as e:
            print(f"  {engine:10s}: FAILED ({e})")

    print("=" * 80)
    return results


def test_graphviz_optimization():
    """Test if certain graphviz options can speed up rendering."""
    print("\nTesting graphviz rendering engines...")
    results = benchmark_rendering_engines()

    if results:
        fastest = min(results, key=results.get)
        slowest = max(results, key=results.get)

        print(f"\nFastest engine: {fastest} ({results[fastest] * 1000:.2f} ms)")
        print(f"Slowest engine: {slowest} ({results[slowest] * 1000:.2f} ms)")
        print(f"Speed improvement: {results[slowest] / results[fastest]:.2f}x")


if __name__ == "__main__":
    test_graphviz_optimization()
