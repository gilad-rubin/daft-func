"""Tests for DAG runner."""

import pytest
from pydantic import BaseModel

from daft_func import Pipeline, Runner, func


class Item(BaseModel):
    """Test item model."""

    item_id: str
    value: int


class Result(BaseModel):
    """Test result model."""

    item_id: str
    doubled: int


def test_runner_single_item():
    """Test runner with single item."""

    @func(output="result")
    def process(item: Item, multiplier: int) -> Result:
        return Result(item_id=item.item_id, doubled=item.value * multiplier)

    pipeline = Pipeline(functions=[process])
    runner = Runner(mode="auto")
    inputs = {
        "item": Item(item_id="i1", value=5),
        "multiplier": 2,
    }

    result = runner.run(pipeline, inputs=inputs)
    assert "result" in result
    # Single item is not wrapped in a list
    assert isinstance(result["result"], Result)
    assert result["result"].item_id == "i1"
    assert result["result"].doubled == 10


def test_runner_multiple_items_local():
    """Test runner with multiple items in local mode."""

    @func(output="result", map_axis="item", key_attr="item_id")
    def process(item: Item, multiplier: int) -> Result:
        return Result(item_id=item.item_id, doubled=item.value * multiplier)

    pipeline = Pipeline(functions=[process])
    runner = Runner(mode="local")
    inputs = {
        "item": [
            Item(item_id="i1", value=5),
            Item(item_id="i2", value=10),
        ],
        "multiplier": 2,
    }

    result = runner.run(pipeline, inputs=inputs)
    assert "result" in result
    assert len(result["result"]) == 2
    assert result["result"][0].item_id == "i1"
    assert result["result"][0].doubled == 10
    assert result["result"][1].item_id == "i2"
    assert result["result"][1].doubled == 20


def test_runner_multiple_items_daft():
    """Test runner with multiple items in daft mode."""
    pytest.importorskip("daft")

    @func(output="result", map_axis="item", key_attr="item_id")
    def process(item: Item, multiplier: int) -> Result:
        return Result(item_id=item.item_id, doubled=item.value * multiplier)

    pipeline = Pipeline(functions=[process])
    runner = Runner(mode="daft")
    inputs = {
        "item": [
            Item(item_id="i1", value=5),
            Item(item_id="i2", value=10),
        ],
        "multiplier": 2,
    }

    result = runner.run(pipeline, inputs=inputs)
    assert "result" in result
    assert len(result["result"]) == 2
    # Daft runner returns Pydantic models (reconstructed from dicts)
    assert isinstance(result["result"][0], Result)
    assert result["result"][0].item_id == "i1"
    assert result["result"][0].doubled == 10


def test_runner_auto_mode_threshold():
    """Test auto mode respects batch threshold."""

    @func(output="result", map_axis="item", key_attr="item_id")
    def process(item: Item, multiplier: int) -> Result:
        return Result(item_id=item.item_id, doubled=item.value * multiplier)

    pipeline = Pipeline(functions=[process])
    # With 2 items and threshold 3, should use local
    runner = Runner(mode="auto", batch_threshold=3)
    inputs = {
        "item": [
            Item(item_id="i1", value=5),
            Item(item_id="i2", value=10),
        ],
        "multiplier": 2,
    }

    result = runner.run(pipeline, inputs=inputs)
    assert len(result["result"]) == 2


def test_runner_chained_nodes():
    """Test runner with multiple dependent nodes."""

    @func(output="doubled", map_axis="item", key_attr="item_id")
    def double(item: Item) -> Result:
        return Result(item_id=item.item_id, doubled=item.value * 2)

    @func(output="final", map_axis="item", key_attr="item_id")
    def add_ten(item: Item, doubled: Result) -> Result:
        return Result(item_id=item.item_id, doubled=doubled.doubled + 10)

    pipeline = Pipeline(functions=[double, add_ten])
    runner = Runner(mode="local")
    inputs = {
        "item": [
            Item(item_id="i1", value=5),
        ],
    }

    result = runner.run(pipeline, inputs=inputs)
    assert "final" in result
    assert result["final"][0].doubled == 20  # (5 * 2) + 10


def test_runner_constants_filtered():
    """Test that only relevant constants are passed to each node."""

    @func(output="result1")
    def node1(const1: int) -> int:
        return const1 * 2

    @func(output="result2")
    def node2(const2: int) -> int:
        return const2 * 3

    pipeline = Pipeline(functions=[node1, node2])
    runner = Runner()
    inputs = {
        "const1": 5,
        "const2": 10,
    }

    # Should not raise error about unexpected keyword arguments
    result = runner.run(pipeline, inputs=inputs)
    # Single items are not wrapped
    assert result["result1"] == 10
    assert result["result2"] == 30


def test_runner_default_parameters():
    """Test that functions with default parameters work correctly."""

    @func(output="step1")
    def first_step(value: int, multiplier: int = 2) -> int:
        return value * multiplier

    @func(output="step2")
    def second_step(step1: int, add_value: int = 10) -> int:
        return step1 + add_value

    pipeline = Pipeline(functions=[first_step, second_step])
    runner = Runner()

    # Test 1: Don't provide any optional parameters (use defaults)
    inputs = {"value": 5}
    result = runner.run(pipeline, inputs=inputs)
    assert result["step1"] == 10  # 5 * 2 (default multiplier)
    assert result["step2"] == 20  # 10 + 10 (default add_value)

    # Test 2: Override one default parameter
    inputs = {"value": 5, "multiplier": 3}
    result = runner.run(pipeline, inputs=inputs)
    assert result["step1"] == 15  # 5 * 3
    assert result["step2"] == 25  # 15 + 10 (default add_value)

    # Test 3: Override all default parameters
    inputs = {"value": 5, "multiplier": 3, "add_value": 20}
    result = runner.run(pipeline, inputs=inputs)
    assert result["step1"] == 15  # 5 * 3
    assert result["step2"] == 35  # 15 + 20


def test_runner_non_mapped_with_mapped_daft():
    """Test batch mode with non-mapped functions followed by mapped functions with List returns.

    Regression test for bug where non-mapped functions were incorrectly processed as batch operations,
    causing batch UDFs with no series arguments to return lists instead of Expressions.
    """
    pytest.importorskip("daft")

    from typing import Dict, List

    class Query(BaseModel):
        """Query model."""

        id: str
        text: str

    class Hit(BaseModel):
        """Hit model."""

        doc_id: str
        score: float

    # Non-mapped function that runs once
    @func(output="index")
    def index(corpus: Dict[str, str]) -> bool:
        """Index corpus - runs once, not per query."""
        return True

    # Mapped function that returns a list of BaseModels
    @func(output="hits", map_axis="query", key_attr="id")
    def retrieve(query: Query, top_k: int, index: bool) -> List[Hit]:
        """Retrieve hits for a query."""
        return [
            Hit(doc_id="d1", score=0.9),
            Hit(doc_id="d2", score=0.7),
        ][:top_k]

    pipeline = Pipeline(functions=[index, retrieve])
    runner = Runner(mode="daft")

    inputs = {
        "corpus": {"d1": "text1", "d2": "text2"},
        "query": [
            Query(id="q1", text="query1"),
            Query(id="q2", text="query2"),
            Query(id="q3", text="query3"),
        ],
        "top_k": 2,
    }

    result = runner.run(pipeline, inputs=inputs)

    # Check non-mapped function result
    assert result["index"] is True

    # Check mapped function results
    assert "hits" in result
    assert len(result["hits"]) == 3  # One list per query
    assert all(isinstance(hits, list) for hits in result["hits"])
    assert all(len(hits) == 2 for hits in result["hits"])  # top_k=2
    assert all(isinstance(hit, Hit) for hits in result["hits"] for hit in hits)
