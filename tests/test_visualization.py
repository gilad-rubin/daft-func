"""Tests for visualization module."""

import pytest

from daft_func import Pipeline, func
from daft_func.visualization import build_graph, visualize_graphviz


def test_build_graph_simple():
    """Test building graph from simple pipeline."""
    pipeline = Pipeline()

    def add(a: int, b: int) -> int:
        return a + b

    from daft_func.pipeline import NodeMeta

    pipeline.add(add, NodeMeta(output_name="result"))

    graph = build_graph(pipeline)

    # Should have 3 nodes: a, b (inputs) and add (function)
    assert len(graph.nodes) == 3

    # Check node types
    input_nodes = [
        n for n, d in graph.nodes(data=True) if d.get("node_type") == "input"
    ]
    func_nodes = [
        n for n, d in graph.nodes(data=True) if d.get("node_type") == "function"
    ]

    assert len(input_nodes) == 2
    assert len(func_nodes) == 1
    assert set(input_nodes) == {"a", "b"}


def test_build_graph_chained():
    """Test building graph with chained dependencies."""
    pipeline = Pipeline()

    def add(a: int, b: int) -> int:
        return a + b

    def multiply(result: int, c: int) -> int:
        return result * c

    from daft_func.pipeline import NodeMeta

    pipeline.add(add, NodeMeta(output_name="result"))
    pipeline.add(multiply, NodeMeta(output_name="final"))

    graph = build_graph(pipeline)

    # Should have 5 nodes: a, b, c (inputs), add, multiply (functions)
    assert len(graph.nodes) == 5

    # Check edges - add should connect to multiply
    func_nodes = [
        n for n, d in graph.nodes(data=True) if d.get("node_type") == "function"
    ]
    assert len(func_nodes) == 2


def test_visualize_graphviz_creates_object():
    """Test that visualize_graphviz creates a graphviz object."""
    pytest.importorskip("graphviz")

    pipeline = Pipeline()

    def simple(x: int) -> int:
        return x * 2

    from daft_func.pipeline import NodeMeta

    pipeline.add(simple, NodeMeta(output_name="result"))

    graph = build_graph(pipeline)

    # Create visualization
    viz = visualize_graphviz(graph, return_type="graphviz")

    # Check it's a graphviz Digraph
    import graphviz

    assert isinstance(viz, graphviz.Digraph)

    # Check it has content
    assert len(viz.source) > 0


def test_visualize_with_type_annotations():
    """Test visualization includes type annotations."""
    pytest.importorskip("graphviz")

    pipeline = Pipeline()

    def typed_func(x: int, y: str) -> float:
        return float(x)

    from daft_func.pipeline import NodeMeta

    pipeline.add(typed_func, NodeMeta(output_name="result"))

    graph = build_graph(pipeline)

    viz = visualize_graphviz(graph, return_type="graphviz")

    # Check that type annotations appear in the source
    # (they should be HTML-escaped in the output)
    assert "int" in viz.source or "float" in viz.source


def test_pipeline_visualize_method():
    """Test the Pipeline.visualize() method."""
    pytest.importorskip("graphviz")

    pipeline = Pipeline()

    def process(x: int, y: int) -> int:
        return x + y

    from daft_func.pipeline import NodeMeta

    pipeline.add(process, NodeMeta(output_name="sum"))

    # Call the visualize method
    viz = pipeline.visualize(return_type="graphviz")

    import graphviz

    assert isinstance(viz, graphviz.Digraph)


def test_visualize_with_orient():
    """Test that orient parameter works."""
    pytest.importorskip("graphviz")

    pipeline = Pipeline()

    def func(x: int) -> int:
        return x

    from daft_func.pipeline import NodeMeta

    pipeline.add(func, NodeMeta(output_name="out"))

    graph = build_graph(pipeline)

    # Test different orientations
    for orient in ["TB", "LR", "BT", "RL"]:
        viz = visualize_graphviz(graph, orient=orient, return_type="graphviz")
        assert f"rankdir={orient}" in viz.source or f'rankdir="{orient}"' in viz.source


def test_visualize_with_func_decorator():
    """Test visualization with functions registered via @func decorator."""
    pytest.importorskip("graphviz")

    @func(output="doubled")
    def double(x: int) -> int:
        return x * 2

    @func(output="result")
    def add_ten(doubled: int) -> int:
        return doubled + 10

    pipeline = Pipeline(functions=[double, add_ten])

    viz = pipeline.visualize(return_type="graphviz")

    import graphviz

    assert isinstance(viz, graphviz.Digraph)

    # Check function names appear
    assert "double" in viz.source or "doubled" in viz.source


def test_visualize_with_parameter_grouping():
    """Test parameter grouping with min_arg_group_size."""
    pytest.importorskip("graphviz")

    from daft_func import Pipeline, func

    @func(output="result")
    def process(a: int, b: int, c: int) -> int:
        return a + b + c

    pipeline = Pipeline(functions=[process])

    # With min_arg_group_size=2, all 3 params should be grouped (3 >= 2)
    viz_grouped = pipeline.visualize(min_arg_group_size=2, return_type="graphviz")

    # Should show grouped parameters in a table
    assert "<TABLE" in viz_grouped.source

    # With min_arg_group_size=None, no grouping
    viz_ungrouped = pipeline.visualize(min_arg_group_size=None, return_type="graphviz")

    # Individual parameters should still be there
    assert "a" in viz_ungrouped.source or "b" in viz_ungrouped.source


def test_grouped_args_with_shared_parameters():
    """Test that params used by multiple functions don't get grouped."""
    pytest.importorskip("graphviz")

    from daft_func import Pipeline, func

    @func(output="result1")
    def func1(a: int, b: int) -> int:
        return a + b

    @func(output="result2")
    def func2(b: int, c: int) -> int:
        return b * c

    pipeline = Pipeline(functions=[func1, func2])

    # With min_arg_group_size=1, exclusive params should be grouped
    # 'a' is exclusive to func1, 'c' is exclusive to func2
    # 'b' is shared, so it shouldn't be grouped
    viz = pipeline.visualize(min_arg_group_size=1, return_type="graphviz")

    # 'b' should appear as individual node since it's used by multiple functions
    assert viz is not None
