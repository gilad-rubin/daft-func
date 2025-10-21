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
    runner = Runner(pipeline=pipeline, mode="auto")
    inputs = {
        "item": Item(item_id="i1", value=5),
        "multiplier": 2,
    }

    result = runner.run(inputs=inputs)
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
    runner = Runner(pipeline=pipeline, mode="local")
    inputs = {
        "item": [
            Item(item_id="i1", value=5),
            Item(item_id="i2", value=10),
        ],
        "multiplier": 2,
    }

    result = runner.run(inputs=inputs)
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
    runner = Runner(pipeline=pipeline, mode="daft")
    inputs = {
        "item": [
            Item(item_id="i1", value=5),
            Item(item_id="i2", value=10),
        ],
        "multiplier": 2,
    }

    result = runner.run(inputs=inputs)
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
    runner = Runner(pipeline=pipeline, mode="auto", batch_threshold=3)
    inputs = {
        "item": [
            Item(item_id="i1", value=5),
            Item(item_id="i2", value=10),
        ],
        "multiplier": 2,
    }

    result = runner.run(inputs=inputs)
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
    runner = Runner(pipeline=pipeline, mode="local")
    inputs = {
        "item": [
            Item(item_id="i1", value=5),
        ],
    }

    result = runner.run(inputs=inputs)
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
    runner = Runner(pipeline=pipeline)
    inputs = {
        "const1": 5,
        "const2": 10,
    }

    # Should not raise error about unexpected keyword arguments
    result = runner.run(inputs=inputs)
    # Single items are not wrapped
    assert result["result1"] == 10
    assert result["result2"] == 30
