"""Test caching with stateful objects that mutate during execution."""

import tempfile

import pytest

from daft_func import CacheConfig, DiskCache, Pipeline, Runner, func


class StatefulObject:
    """Object that changes state during pipeline execution."""

    def __init__(self):
        self.config = {"key": "value"}
        # Note: no _state attribute initially

    def process(self, data):
        """Process data and update internal state."""
        # This mutates the object during execution
        self._state = f"processed_{data}"
        return self._state


@pytest.fixture
def temp_cache_dir():
    """Create a temporary cache directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


def test_stateful_object_causes_cache_miss(temp_cache_dir):
    """Demonstrate that stateful objects cause cache misses."""
    executions = []

    @func(output="initialized", cache=True)
    def init_obj(obj: StatefulObject) -> bool:
        # This function mutates obj by calling process
        obj.process("init")
        return True

    @func(output="result", cache=True)
    def use_obj(obj: StatefulObject, initialized: bool, data: str) -> str:
        executions.append(data)
        return obj.process(data)

    pipeline = Pipeline(functions=[init_obj, use_obj])
    cache_config = CacheConfig(
        enabled=True, backend=DiskCache(cache_dir=temp_cache_dir), verbose=True
    )
    runner = Runner(mode="local", cache_config=cache_config)

    # First run
    obj = StatefulObject()
    print(f"\nBefore run 1: obj.__dict__ = {obj.__dict__}")
    result1 = runner.run(pipeline, inputs={"obj": obj, "data": "hello"})
    print(f"After run 1: obj.__dict__ = {obj.__dict__}")
    assert len(executions) == 1

    # Second run with SAME object instance
    # Fixed: Private attributes (_state) are excluded from hash, so cache hits!
    print(f"\nBefore run 2: obj.__dict__ = {obj.__dict__}")
    result2 = runner.run(pipeline, inputs={"obj": obj, "data": "hello"})
    print(f"After run 2: obj.__dict__ = {obj.__dict__}")

    # Cache should hit because only public attributes (config) are hashed
    print(f"\nExecutions: {len(executions)}")
    assert len(executions) == 1, (
        "Expected cache hit - private attributes should be excluded from hash"
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v", "-s"]))
