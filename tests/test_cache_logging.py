"""Tests for cache logging functionality."""

import tempfile

import pytest

from daft_func import CacheConfig, Pipeline, Runner, func


@pytest.fixture
def temp_cache_dir():
    """Create a temporary cache directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


def test_cache_logging_enabled_by_default(temp_cache_dir, capsys):
    """Test that cache logging is enabled by default."""
    executions = []

    @func(output="result", cache=True)
    def compute(x: int) -> int:
        executions.append(x)
        return x * 2

    pipeline = Pipeline(functions=[compute])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir, verbose=True)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run - cache miss
    runner.run(inputs={"x": 5})
    captured = capsys.readouterr()
    assert "[CACHE]" in captured.out
    assert "result: ✗ MISS" in captured.out

    # Second run - cache hit
    runner.run(inputs={"x": 5})
    captured = capsys.readouterr()
    assert "[CACHE]" in captured.out
    assert "result: ✓ HIT" in captured.out


def test_cache_logging_disabled(temp_cache_dir, capsys):
    """Test that cache logging can be disabled."""

    @func(output="result", cache=True)
    def compute(x: int) -> int:
        return x * 2

    pipeline = Pipeline(functions=[compute])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir, verbose=False)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # Run - should not print cache info
    runner.run(inputs={"x": 5})
    captured = capsys.readouterr()
    assert "[CACHE]" not in captured.out


def test_cache_logging_shows_execution_time(temp_cache_dir, capsys):
    """Test that cache logging shows execution time for cache misses."""

    @func(output="result", cache=True)
    def slow_compute(x: int) -> int:
        import time

        time.sleep(0.01)
        return x * 2

    pipeline = Pipeline(functions=[slow_compute])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run - should show execution time
    runner.run(inputs={"x": 5})
    captured = capsys.readouterr()
    assert "[CACHE]" in captured.out
    assert "result: ✗ MISS" in captured.out
    # Should include timing in seconds
    assert "s)" in captured.out


def test_cache_logging_multiple_nodes(temp_cache_dir, capsys):
    """Test cache logging with multiple nodes."""

    @func(output="step1", cache=True)
    def compute1(x: int) -> int:
        return x * 2

    @func(output="step2", cache=True)
    def compute2(step1: int) -> int:
        return step1 + 1

    @func(output="step3", cache=False)  # Not cached
    def compute3(step2: int) -> int:
        return step2 * 3

    pipeline = Pipeline(functions=[compute1, compute2, compute3])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run - all miss or no-cache
    runner.run(inputs={"x": 5})
    captured = capsys.readouterr()
    assert "[CACHE]" in captured.out
    assert "step1: ✗ MISS" in captured.out
    assert "step2: ✗ MISS" in captured.out
    assert "step3: NO-CACHE" in captured.out

    # Second run - cached nodes hit, uncached always executes
    runner.run(inputs={"x": 5})
    captured = capsys.readouterr()
    assert "[CACHE]" in captured.out
    assert "step1: ✓ HIT" in captured.out
    assert "step2: ✓ HIT" in captured.out
    assert "step3: NO-CACHE" in captured.out


def test_cache_logging_smart_invalidation(temp_cache_dir, capsys):
    """Test that cache logging shows smart invalidation."""

    @func(output="foo", cache=True)
    def foo(a: int, b: int) -> int:
        return a + b

    @func(output="bar", cache=True)
    def bar(foo: int, c: int) -> int:
        return foo * c

    pipeline = Pipeline(functions=[foo, bar])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run
    runner.run(inputs={"a": 1, "b": 2, "c": 3})
    capsys.readouterr()  # Clear

    # Change only c (downstream) - foo should be cached
    runner.run(inputs={"a": 1, "b": 2, "c": 5})
    captured = capsys.readouterr()
    assert "[CACHE]" in captured.out
    assert "foo: ✓ HIT" in captured.out  # Upstream cached
    assert "bar: ✗ MISS" in captured.out  # Downstream re-executed


def test_cache_logging_format(temp_cache_dir, capsys):
    """Test that cache logging format is readable."""

    @func(output="a", cache=True)
    def step_a(x: int) -> int:
        return x

    @func(output="b", cache=True)
    def step_b(a: int) -> int:
        return a

    @func(output="c", cache=True)
    def step_c(b: int) -> int:
        return b

    pipeline = Pipeline(functions=[step_a, step_b, step_c])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    runner.run(inputs={"x": 1})
    captured = capsys.readouterr()

    # Check format: [CACHE] node1: status | node2: status | node3: status
    assert captured.out.startswith("\n[CACHE]")
    assert " | " in captured.out
    assert "a:" in captured.out
    assert "b:" in captured.out
    assert "c:" in captured.out
