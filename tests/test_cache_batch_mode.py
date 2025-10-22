"""Test that caching works with batch/Daft mode."""

import tempfile

import pytest
from pydantic import BaseModel

from daft_func import CacheConfig, DiskCache, Pipeline, Runner, func


class Item(BaseModel):
    id: str
    value: int


class Result(BaseModel):
    id: str
    doubled: int


@pytest.fixture
def temp_cache_dir():
    """Create a temporary cache directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


def test_cache_with_batch_mode(temp_cache_dir, capsys):
    """Test that caching works with batch/local loop mode."""
    executions = []

    @func(output="results", map_axis="item", key_attr="id", cache=True)
    def process(item: Item, multiplier: int) -> Result:
        executions.append(item.id)
        return Result(id=item.id, doubled=item.value * multiplier)

    pipeline = Pipeline(functions=[process])
    cache_config = CacheConfig(
        enabled=True, backend=DiskCache(cache_dir=temp_cache_dir)
    )
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    items = [
        Item(id="i1", value=5),
        Item(id="i2", value=10),
    ]

    # First run - should execute all
    result1 = runner.run(inputs={"item": items, "multiplier": 2})
    captured = capsys.readouterr()
    print("Run 1 output:", captured.out)
    assert len(executions) == 2
    assert "[CACHE]" in captured.out
    assert "results: ✗ MISS" in captured.out

    # Second run with same inputs - should use cache
    result2 = runner.run(inputs={"item": items, "multiplier": 2})
    captured = capsys.readouterr()
    print("Run 2 output:", captured.out)
    # Now works! Per-item caching is enabled
    assert len(executions) == 2  # Should still be 2 (no new executions)
    assert "[CACHE]" in captured.out
    assert "results: ✓ HIT" in captured.out


def test_cache_with_daft_mode(temp_cache_dir, capsys):
    """Test that caching works with Daft mode."""
    try:
        import daft  # noqa
    except ImportError:
        pytest.skip("Daft not available")

    executions = []

    @func(output="results", map_axis="item", key_attr="id", cache=True)
    def process(item: Item, multiplier: int) -> Result:
        executions.append(item.id)
        return Result(id=item.id, doubled=item.value * multiplier)

    pipeline = Pipeline(functions=[process])
    cache_config = CacheConfig(
        enabled=True, backend=DiskCache(cache_dir=temp_cache_dir)
    )
    runner = Runner(pipeline=pipeline, mode="daft", cache_config=cache_config)

    items = [
        Item(id="i1", value=5),
        Item(id="i2", value=10),
    ]

    # First run
    result1 = runner.run(inputs={"item": items, "multiplier": 2})
    captured = capsys.readouterr()
    print("Daft Run 1 output:", captured.out)
    # Currently no cache output at all in Daft mode!
    # This is the bug we need to fix


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v", "-s"]))
