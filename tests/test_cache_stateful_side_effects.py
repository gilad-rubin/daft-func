"""Test for caching with stateful objects that have side effects."""

import tempfile

import pytest

from daft_func import CacheConfig, DiskCache, Pipeline, Runner, func


class StatefulProcessor:
    """Object that needs initialization with side effects."""

    def __init__(self):
        self._initialized = False
        self._data = None

    def initialize(self, data: str):
        """Initialize with side effects."""
        self._initialized = True
        self._data = data

    def process(self, input_text: str) -> str:
        """Process input - requires initialization."""
        if not self._initialized:
            raise AttributeError("Processor not initialized. Call initialize() first.")
        return f"{self._data}:{input_text}"


def test_stateful_side_effects_different_instances():
    """Test that different instances of stateful objects are correctly distinguished.

    This verifies that the fix for the cache collision issue works correctly:
    objects with no public attributes now include their object ID in the hash,
    preventing different instances from being treated as identical.
    """

    @func(output="initialized", cache=True)
    def initialize_processor(processor: StatefulProcessor, data: str) -> bool:
        """Initialize processor with side effects."""
        processor.initialize(data)
        return True

    @func(output="result", cache=True)
    def use_processor(
        processor: StatefulProcessor, initialized: bool, text: str
    ) -> str:
        """Use the processor - depends on initialization side effect."""
        return processor.process(text)

    pipeline = Pipeline(functions=[initialize_processor, use_processor])

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(
            enabled=True, backend=DiskCache(cache_dir=tmpdir), verbose=True
        )
        runner = Runner(mode="local", cache_config=cache_config)

        # First run: everything works (cold cache)
        processor1 = StatefulProcessor()
        result1 = runner.run(
            pipeline,
            inputs={"processor": processor1, "data": "PREFIX", "text": "hello"},
        )
        assert result1["result"] == "PREFIX:hello"
        assert processor1._initialized is True

        # Second run with DIFFERENT instance
        # With the fix: processor2 has a different ID, so cache miss occurs
        # Both functions execute normally, processor2 gets initialized
        processor2 = StatefulProcessor()
        result2 = runner.run(
            pipeline,
            inputs={"processor": processor2, "data": "PREFIX", "text": "world"},
        )

        # This works correctly because processor2 was initialized
        # (no cache hit occurred due to different object IDs)
        assert result2["result"] == "PREFIX:world"
        assert processor2._initialized is True


def test_stateful_side_effects_same_instance():
    """Test that reusing the same instance works (but is fragile)."""

    @func(output="initialized", cache=True)
    def initialize_processor(processor: StatefulProcessor, data: str) -> bool:
        processor.initialize(data)
        return True

    @func(output="result", cache=True)
    def use_processor(
        processor: StatefulProcessor, initialized: bool, text: str
    ) -> str:
        return processor.process(text)

    pipeline = Pipeline(functions=[initialize_processor, use_processor])

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(
            enabled=True, backend=DiskCache(cache_dir=tmpdir), verbose=True
        )
        runner = Runner(mode="local", cache_config=cache_config)

        # Using the SAME processor instance
        processor = StatefulProcessor()

        # First run: cold cache, initializes
        result1 = runner.run(
            pipeline, inputs={"processor": processor, "data": "PREFIX", "text": "hello"}
        )
        assert result1["result"] == "PREFIX:hello"

        # Second run: cache hit, but processor is ALREADY initialized from run 1
        # This "works" but only because we're reusing the same instance
        result2 = runner.run(
            pipeline, inputs={"processor": processor, "data": "PREFIX", "text": "world"}
        )
        assert result2["result"] == "PREFIX:world"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
