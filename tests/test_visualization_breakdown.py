"""Detailed breakdown of visualization performance by component."""

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


@func(output="index")
def index(retriever: Retriever, corpus: Dict[str, str], test: bool = True) -> bool:
    retriever.index(corpus)
    return True


@func(output="hits", map_axis="query", key_attr="query_uuid")
def retrieve(
    retriever: Retriever, query: Query, top_k: int, index: bool
) -> RetrievalResult:
    return retriever.retrieve(query, top_k=top_k)


@func(output="reranked_hits", map_axis="query", key_attr="query_uuid")
def rerank(
    reranker: Reranker, query: Query, hits: RetrievalResult, top_k: int
) -> List[RerankedHit]:
    return reranker.rerank(query, hits, top_k=top_k)


def profile_visualization_components():
    """Break down visualization time by component."""
    from daft_func.visualization import build_graph, create_grouped_parameter_graph

    pipeline = Pipeline(functions=[index, retrieve, rerank])

    timings = {}

    # Build graph
    start = time.perf_counter()
    graph = build_graph(pipeline)
    timings["build_graph"] = time.perf_counter() - start

    # Group parameters
    start = time.perf_counter()
    plot_graph = create_grouped_parameter_graph(graph, min_arg_group_size=1)
    timings["group_parameters"] = time.perf_counter() - start

    # Create graphviz object (without rendering)
    start = time.perf_counter()
    import graphviz

    from daft_func.visualization import GraphvizStyle

    style = GraphvizStyle()

    # Setup graph attributes
    graph_attr = {
        "rankdir": "TB",
        "fontsize": str(style.font_size),
        "fontname": style.font_name,
    }

    # Create Graphviz digraph
    digraph = graphviz.Digraph(
        comment="Pipeline Visualization",
        graph_attr=graph_attr,
        node_attr={
            "shape": "rectangle",
            "fontname": style.font_name,
            "fontsize": str(style.font_size),
        },
    )
    timings["create_digraph"] = time.perf_counter() - start

    # Add nodes and edges
    start = time.perf_counter()
    import inspect
    from inspect import Parameter
    from typing import get_type_hints

    from daft_func.visualization import _generate_node_label

    _empty = Parameter.empty

    # Collect type hints and defaults
    hints = {}
    defaults = {}
    for node, data in plot_graph.nodes(data=True):
        if data.get("node_type") == "function":
            from daft_func.pipeline import NodeDef

            node_def: NodeDef = node
            try:
                fn_hints = get_type_hints(node_def.fn)
                hints.update(fn_hints)
            except Exception:
                pass
            sig = inspect.signature(node_def.fn)
            for param_name, param in sig.parameters.items():
                if param.default is not _empty:
                    defaults[param_name] = param.default

    for node, data in plot_graph.nodes(data=True):
        node_type = data.get("node_type", "input")
        label = _generate_node_label(node, node_type, hints, defaults)

        if node_type == "input":
            digraph.node(
                str(id(node)),
                label=f"<{label}>",
                fillcolor=style.arg_node_color,
                shape="rectangle",
                style="filled,dashed",
            )
        elif node_type == "grouped_args":
            digraph.node(
                str(id(node)),
                label=f"<{label}>",
                fillcolor=style.grouped_args_node_color,
                shape="rectangle",
                style="filled,solid",
            )
        else:
            digraph.node(
                str(id(node)),
                label=f"<{label}>",
                fillcolor=style.func_node_color,
                shape="box",
                style="filled,rounded",
            )

    for source, target, data in plot_graph.edges(data=True):
        param_name = data.get("param_name", "")
        source_type = plot_graph.nodes[source].get("node_type", "input")
        if source_type == "input":
            edge_color = style.arg_edge_color or style.arg_node_color
        elif source_type == "grouped_args":
            edge_color = style.grouped_args_edge_color or style.grouped_args_node_color
        else:
            edge_color = style.output_edge_color or style.func_node_color

        digraph.edge(
            str(id(source)),
            str(id(target)),
            label=param_name if param_name != "grouped" else "",
            color=edge_color,
            fontname=style.font_name,
            fontsize=str(style.edge_font_size),
            fontcolor="transparent",
        )

    timings["add_nodes_edges"] = time.perf_counter() - start

    # Render to SVG (this is where graphviz C library is called)
    start = time.perf_counter()
    svg_content = digraph._repr_image_svg_xml()
    timings["graphviz_render"] = time.perf_counter() - start

    # HTML wrapping
    start = time.perf_counter()
    html_content = (
        f'<div id="svg-container" style="max-width: 100%;">{svg_content}</div>'
        "<style>#svg-container svg {max-width: 100%; height: auto;}</style>"
    )
    timings["html_wrap"] = time.perf_counter() - start

    return timings


def test_visualization_breakdown():
    """Test and show breakdown of visualization time."""
    print("\n" + "=" * 80)
    print("VISUALIZATION TIME BREAKDOWN")
    print("=" * 80)

    # Run multiple times to get stable measurements
    num_runs = 5
    all_timings = []

    for i in range(num_runs):
        timings = profile_visualization_components()
        all_timings.append(timings)

    # Calculate averages
    avg_timings = {}
    for key in all_timings[0].keys():
        avg_timings[key] = sum(t[key] for t in all_timings) / num_runs

    # Print results
    print(f"\nAveraged over {num_runs} runs:\n")
    total = sum(avg_timings.values())

    for component, duration in sorted(avg_timings.items(), key=lambda x: -x[1]):
        percentage = (duration / total) * 100 if total > 0 else 0
        print(f"  {component:25s}: {duration * 1000:6.2f} ms  ({percentage:5.1f}%)")

    print(f"\n  {'TOTAL':25s}: {total * 1000:6.2f} ms")
    print("\n" + "=" * 80)


if __name__ == "__main__":
    test_visualization_breakdown()
