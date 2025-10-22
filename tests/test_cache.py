"""Tests for caching functionality."""

import tempfile
from pathlib import Path

import pytest
from pydantic import BaseModel

from daft_func import CacheConfig, Pipeline, Runner, func


# Define Pydantic models at module level for pickling
class InputModel(BaseModel):
    value: int
    name: str


class OutputModel(BaseModel):
    result: int
    message: str


@pytest.fixture
def temp_cache_dir():
    """Create a temporary cache directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


def test_cache_disabled_by_default(temp_cache_dir):
    """Verify caching is opt-in (disabled by default)."""

    @func(output="doubled")
    def double(x: int) -> int:
        return x * 2

    pipeline = Pipeline(functions=[double])

    # Without cache_config
    runner1 = Runner(pipeline=pipeline, mode="local")
    assert not runner1.cache_config.enabled
    assert runner1.meta_store is None
    assert runner1.blob_store is None

    # With cache_config but enabled=False
    runner2 = Runner(
        pipeline=pipeline,
        mode="local",
        cache_config=CacheConfig(enabled=False, cache_dir=temp_cache_dir),
    )
    assert not runner2.cache_config.enabled
    assert runner2.meta_store is None
    assert runner2.blob_store is None


def test_downstream_only_change(temp_cache_dir):
    """Test caching when only downstream inputs change.

    DAG: (a,b) -> foo -> bar(foo_out, c)
    Change only c -> foo should be cached, bar should be recomputed.
    """
    foo_executions = []
    bar_executions = []

    @func(output="foo_out", cache=True)
    def foo(a: int, b: int) -> int:
        foo_executions.append((a, b))
        return a + b

    @func(output="bar_out", cache=True)
    def bar(foo_out: int, c: int) -> int:
        bar_executions.append((foo_out, c))
        return foo_out * c

    pipeline = Pipeline(functions=[foo, bar])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run
    result1 = runner.run(inputs={"a": 1, "b": 2, "c": 3})
    assert result1["bar_out"] == 9  # (1+2) * 3
    assert len(foo_executions) == 1
    assert len(bar_executions) == 1

    # Second run: change only c (downstream)
    result2 = runner.run(inputs={"a": 1, "b": 2, "c": 5})
    assert result2["bar_out"] == 15  # (1+2) * 5
    # foo should not re-execute (cached)
    assert len(foo_executions) == 1
    # bar should re-execute (input changed)
    assert len(bar_executions) == 2


def test_full_cache_hit(temp_cache_dir):
    """Test full cache hit when nothing changes.

    No changes -> bar loaded from cache, foo not loaded.
    """
    foo_executions = []
    bar_executions = []

    @func(output="foo_out", cache=True)
    def foo(a: int, b: int) -> int:
        foo_executions.append((a, b))
        return a + b

    @func(output="bar_out", cache=True)
    def bar(foo_out: int, c: int) -> int:
        bar_executions.append((foo_out, c))
        return foo_out * c

    pipeline = Pipeline(functions=[foo, bar])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run
    result1 = runner.run(inputs={"a": 1, "b": 2, "c": 3})
    assert result1["bar_out"] == 9
    assert len(foo_executions) == 1
    assert len(bar_executions) == 1

    # Second run: no changes
    result2 = runner.run(inputs={"a": 1, "b": 2, "c": 3})
    assert result2["bar_out"] == 9
    # Neither should re-execute
    assert len(foo_executions) == 1
    assert len(bar_executions) == 1


def test_upstream_change(temp_cache_dir):
    """Test cache invalidation when upstream inputs change.

    DAG: (a,b) -> foo -> bar(foo_out, c)
    Change a -> foo recomputed, bar recomputed.
    """
    foo_executions = []
    bar_executions = []

    @func(output="foo_out", cache=True)
    def foo(a: int, b: int) -> int:
        foo_executions.append((a, b))
        return a + b

    @func(output="bar_out", cache=True)
    def bar(foo_out: int, c: int) -> int:
        bar_executions.append((foo_out, c))
        return foo_out * c

    pipeline = Pipeline(functions=[foo, bar])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run
    result1 = runner.run(inputs={"a": 1, "b": 2, "c": 3})
    assert result1["bar_out"] == 9
    assert len(foo_executions) == 1
    assert len(bar_executions) == 1

    # Second run: change a (upstream)
    result2 = runner.run(inputs={"a": 10, "b": 2, "c": 3})
    assert result2["bar_out"] == 36  # (10+2) * 3
    # Both should re-execute
    assert len(foo_executions) == 2
    assert len(bar_executions) == 2


def test_code_change_invalidates_cache(temp_cache_dir):
    """Test that changing function code invalidates cache.

    Note: This test demonstrates the concept but is limited because
    we can't actually modify function code at runtime. In practice,
    code changes between runs would trigger cache invalidation.
    """
    executions = []

    @func(output="result", cache=True)
    def compute(x: int) -> int:
        executions.append(x)
        return x * 2

    pipeline = Pipeline(functions=[compute])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run
    result1 = runner.run(inputs={"x": 5})
    assert result1["result"] == 10
    assert len(executions) == 1

    # Second run with same input - should use cache
    result2 = runner.run(inputs={"x": 5})
    assert result2["result"] == 10
    assert len(executions) == 1

    # Simulate code change by creating a new function with different code
    executions2 = []

    @func(output="result", cache=True)
    def compute2(x: int) -> int:
        """Different function with different code."""
        executions2.append(x)
        return x * 3  # Different computation

    pipeline2 = Pipeline(functions=[compute2])
    runner2 = Runner(pipeline=pipeline2, mode="local", cache_config=cache_config)

    # Should execute because code hash is different
    result3 = runner2.run(inputs={"x": 5})
    assert result3["result"] == 15  # Different result
    assert len(executions2) == 1  # New function executed


def test_custom_cache_key(temp_cache_dir):
    """Test env_hash overrides via cache_key parameter."""
    executions = []

    @func(output="result", cache=True, cache_key="v1")
    def compute(x: int) -> int:
        executions.append(("v1", x))
        return x * 2

    pipeline = Pipeline(functions=[compute])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run
    result1 = runner.run(inputs={"x": 5})
    assert result1["result"] == 10
    assert len(executions) == 1

    # Second run - should use cache
    result2 = runner.run(inputs={"x": 5})
    assert result2["result"] == 10
    assert len(executions) == 1

    # Now create a function with different cache_key
    executions2 = []

    @func(output="result", cache=True, cache_key="v2")
    def compute_v2(x: int) -> int:
        executions2.append(("v2", x))
        return x * 2

    pipeline2 = Pipeline(functions=[compute_v2])
    runner2 = Runner(pipeline=pipeline2, mode="local", cache_config=cache_config)

    # Should execute because cache_key is different
    result3 = runner2.run(inputs={"x": 5})
    assert result3["result"] == 10
    assert len(executions2) == 1


def test_mixed_cached_uncached_nodes(temp_cache_dir):
    """Test pipeline with some nodes cached and some not."""
    cached_executions = []
    uncached_executions = []

    @func(output="cached_result", cache=True)
    def cached_node(x: int) -> int:
        cached_executions.append(x)
        return x * 2

    @func(output="uncached_result", cache=False)
    def uncached_node(cached_result: int) -> int:
        uncached_executions.append(cached_result)
        return cached_result + 1

    pipeline = Pipeline(functions=[cached_node, uncached_node])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run
    result1 = runner.run(inputs={"x": 5})
    assert result1["uncached_result"] == 11
    assert len(cached_executions) == 1
    assert len(uncached_executions) == 1

    # Second run
    result2 = runner.run(inputs={"x": 5})
    assert result2["uncached_result"] == 11
    # Cached node should not re-execute
    assert len(cached_executions) == 1
    # Uncached node should always re-execute
    assert len(uncached_executions) == 2


def test_cache_backend_diskcache(temp_cache_dir):
    """Test diskcache backend."""
    executions = []

    @func(output="result", cache=True)
    def compute(x: int) -> int:
        executions.append(x)
        return x * 2

    pipeline = Pipeline(functions=[compute])
    cache_config = CacheConfig(
        enabled=True, backend="diskcache", cache_dir=temp_cache_dir
    )
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run
    result1 = runner.run(inputs={"x": 5})
    assert result1["result"] == 10
    assert len(executions) == 1

    # Second run - should use cache
    result2 = runner.run(inputs={"x": 5})
    assert result2["result"] == 10
    assert len(executions) == 1

    # Verify cache files exist
    cache_path = Path(temp_cache_dir)
    assert (cache_path / "metadata.json").exists()
    assert (cache_path / "blobs").exists()


def test_cache_backend_cachier(temp_cache_dir):
    """Test cachier backend."""
    executions = []

    @func(output="result", cache=True)
    def compute(x: int) -> int:
        executions.append(x)
        return x * 2

    pipeline = Pipeline(functions=[compute])
    cache_config = CacheConfig(
        enabled=True, backend="cachier", cache_dir=temp_cache_dir
    )
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run
    result1 = runner.run(inputs={"x": 5})
    assert result1["result"] == 10
    assert len(executions) == 1

    # Second run - should use cache
    result2 = runner.run(inputs={"x": 5})
    assert result2["result"] == 10
    assert len(executions) == 1

    # Verify cache files exist
    cache_path = Path(temp_cache_dir)
    assert (cache_path / "metadata.json").exists()
    assert (cache_path / "cachier_blobs").exists()


def test_dependency_depth(temp_cache_dir):
    """Test that dependency_depth parameter is used."""

    @func(output="result", cache=True)
    def compute(x: int) -> int:
        return x * 2

    pipeline = Pipeline(functions=[compute])

    # Test with different dependency depths
    for depth in [1, 2, 3]:
        cache_config = CacheConfig(
            enabled=True, cache_dir=temp_cache_dir, dependency_depth=depth
        )
        runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)
        assert runner.cache_config.dependency_depth == depth


def test_cache_with_complex_types(temp_cache_dir):
    """Test caching with complex types (Pydantic models)."""
    executions = []

    @func(output="output", cache=True)
    def process(inp: InputModel) -> OutputModel:
        executions.append(inp)
        return OutputModel(result=inp.value * 2, message=f"Processed {inp.name}")

    pipeline = Pipeline(functions=[process])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run
    input1 = InputModel(value=5, name="test")
    result1 = runner.run(inputs={"inp": input1})
    assert result1["output"].result == 10
    assert result1["output"].message == "Processed test"
    assert len(executions) == 1

    # Second run with same input
    input2 = InputModel(value=5, name="test")
    result2 = runner.run(inputs={"inp": input2})
    assert result2["output"].result == 10
    assert len(executions) == 1  # Should use cache

    # Third run with different input
    input3 = InputModel(value=10, name="test")
    result3 = runner.run(inputs={"inp": input3})
    assert result3["output"].result == 20
    assert len(executions) == 2  # Should execute


def test_cache_clears_signatures_between_runs(temp_cache_dir):
    """Test that signatures are cleared between runs."""

    @func(output="step1", cache=True)
    def compute1(x: int) -> int:
        return x * 2

    @func(output="step2", cache=True)
    def compute2(step1: int) -> int:
        return step1 + 1

    pipeline = Pipeline(functions=[compute1, compute2])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run
    result1 = runner.run(inputs={"x": 5})
    assert result1["step2"] == 11
    sigs_after_run1 = dict(runner._current_signatures)

    # Second run - signatures should be reset
    result2 = runner.run(inputs={"x": 5})
    assert result2["step2"] == 11

    # Verify signatures are consistent
    assert len(runner._current_signatures) == len(sigs_after_run1)
