"""Test the __cache_key__() protocol for custom cache keys."""

import tempfile

import pytest

from daft_func import CacheConfig, Pipeline, Runner, func


class ModelWithCacheKey:
    """Model that implements __cache_key__() protocol."""

    def __init__(self, model_path: str, device: str = "cpu"):
        self.model_path = model_path
        self.device = device
        self._loaded_model = None  # Private attribute, changes during execution

    def __cache_key__(self):
        """Return stable cache key based on model_path only."""
        # Only model_path matters for caching, not the loaded model
        return f"model:{self.model_path}:{self.device}"

    def load(self):
        """Simulate loading a model."""
        if self._loaded_model is None:
            self._loaded_model = f"loaded_{self.model_path}"
        return self._loaded_model


@pytest.fixture
def temp_cache_dir():
    """Create a temporary cache directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


def test_cache_key_protocol(temp_cache_dir):
    """Test that __cache_key__() protocol is respected."""
    executions = []

    @func(output="result", cache=True)
    def use_model(model: ModelWithCacheKey, text: str) -> str:
        executions.append(text)
        loaded = model.load()
        return f"{loaded}:{text}"

    pipeline = Pipeline(functions=[use_model])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run
    model1 = ModelWithCacheKey(model_path="bert-base", device="cpu")
    result1 = runner.run(inputs={"model": model1, "text": "hello"})
    assert len(executions) == 1

    # Second run with DIFFERENT instance but same cache key
    model2 = ModelWithCacheKey(model_path="bert-base", device="cpu")
    result2 = runner.run(inputs={"model": model2, "text": "hello"})
    # Should use cache because __cache_key__() returns same value
    assert len(executions) == 1, "Expected cache hit with same __cache_key__()"

    # Third run with different model path
    model3 = ModelWithCacheKey(model_path="bert-large", device="cpu")
    result3 = runner.run(inputs={"model": model3, "text": "hello"})
    # Should miss cache because __cache_key__() is different
    assert len(executions) == 2, "Expected cache miss with different __cache_key__()"


def test_cache_key_with_state_changes(temp_cache_dir):
    """Test that state changes don't affect caching with __cache_key__()."""
    executions = []

    @func(output="loaded", cache=True)
    def load_model(model: ModelWithCacheKey) -> str:
        model.load()  # This mutates model._loaded_model
        return "loaded"

    @func(output="result", cache=True)
    def use_model(model: ModelWithCacheKey, loaded: str, text: str) -> str:
        executions.append(text)
        return f"{model._loaded_model}:{text}"

    pipeline = Pipeline(functions=[load_model, use_model])
    cache_config = CacheConfig(enabled=True, cache_dir=temp_cache_dir)
    runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    # First run - model gets loaded, _loaded_model is set
    model = ModelWithCacheKey(model_path="bert-base")
    result1 = runner.run(inputs={"model": model, "text": "hello"})
    assert len(executions) == 1
    assert model._loaded_model is not None

    # Second run - model now has _loaded_model set
    # But should still cache hit because __cache_key__() ignores it
    result2 = runner.run(inputs={"model": model, "text": "hello"})
    assert len(executions) == 1, "Expected cache hit despite state change"
