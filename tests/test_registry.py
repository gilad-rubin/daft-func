"""Tests for DAG pipeline."""

import pytest

from daft_func.pipeline import Pipeline, NodeMeta


def test_pipeline_add_node():
    """Test adding nodes to pipeline."""
    registry = Pipeline()

    def my_func(a: int, b: int) -> int:
        return a + b

    meta = NodeMeta(output_name="result")
    registry.add(my_func, meta)

    assert len(registry.nodes) == 1
    assert "result" in registry.by_output
    assert registry.by_output["result"].fn == my_func


def test_pipeline_topo_sort_simple():
    """Test topological sort with simple dependencies."""
    registry = Pipeline()

    def add(a: int, b: int) -> int:
        return a + b

    def multiply(result: int, c: int) -> int:
        return result * c

    registry.add(add, NodeMeta(output_name="result"))
    registry.add(multiply, NodeMeta(output_name="final"))

    # Should order: add before multiply (multiply depends on result)
    order = registry.topo({"a": 1, "b": 2, "c": 3})

    assert len(order) == 2
    assert order[0].meta.output_name == "result"
    assert order[1].meta.output_name == "final"


def test_pipeline_topo_sort_parallel():
    """Test topological sort with parallel nodes."""
    registry = Pipeline()

    def func_a(x: int) -> int:
        return x + 1

    def func_b(x: int) -> int:
        return x * 2

    def func_c(a_out: int, b_out: int) -> int:
        return a_out + b_out

    registry.add(func_a, NodeMeta(output_name="a_out"))
    registry.add(func_b, NodeMeta(output_name="b_out"))
    registry.add(func_c, NodeMeta(output_name="c_out"))

    order = registry.topo({"x": 5})

    assert len(order) == 3
    # a_out and b_out can be in any order, but both before c_out
    output_names = [n.meta.output_name for n in order]
    assert output_names[2] == "c_out"
    assert set(output_names[:2]) == {"a_out", "b_out"}


def test_pipeline_topo_sort_circular_deps():
    """Test that circular dependencies raise an error."""
    registry = Pipeline()

    # This is contrived, but simulates circular dependency
    def func_a(b_out: int) -> int:
        return b_out + 1

    def func_b(a_out: int) -> int:
        return a_out * 2

    registry.add(func_a, NodeMeta(output_name="a_out"))
    registry.add(func_b, NodeMeta(output_name="b_out"))

    with pytest.raises(RuntimeError, match="Cannot resolve dependencies"):
        registry.topo({"x": 5})


def test_pipeline_clear():
    """Test clearing pipeline."""
    registry = Pipeline()

    def my_func(a: int) -> int:
        return a + 1

    registry.add(my_func, NodeMeta(output_name="result"))
    assert len(registry.nodes) == 1

    registry.clear()
    assert len(registry.nodes) == 0
    assert len(registry.by_output) == 0
