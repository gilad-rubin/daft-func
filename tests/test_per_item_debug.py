"""Debug per-item caching to see what's happening."""

import tempfile

from pydantic import BaseModel

from daft_func import CacheConfig, Pipeline, Runner, func


class Item(BaseModel):
    id: str
    value: int


class Result(BaseModel):
    id: str
    result: int


def test_per_item_caching_debug():
    """Debug per-item caching."""
    executions = []

    @func(output="results", map_axis="item", key_attr="id", cache=True)
    def process(item: Item) -> Result:
        print(f"\n>>> EXECUTING process() for item {item.id}")
        executions.append(item.id)
        return Result(id=item.id, result=item.value * 2)

    pipeline = Pipeline(functions=[process])

    with tempfile.TemporaryDirectory() as tmpdir:
        cache_config = CacheConfig(enabled=True, cache_dir=tmpdir)
        runner = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

        items = [
            Item(id="i1", value=10),
            Item(id="i2", value=20),
            Item(id="i3", value=30),
        ]

        print("\n" + "=" * 70)
        print("RUN 1: Three items (should all be MISS)")
        print("=" * 70)
        result1 = runner.run(inputs={"item": items})
        print(f"\nExecutions: {executions}")
        print("Expected: ['i1', 'i2', 'i3']")
        assert executions == ["i1", "i2", "i3"], (
            f"Expected all items to execute, got {executions}"
        )

        print("\n" + "=" * 70)
        print("RUN 2: Same three items (should all be HIT)")
        print("=" * 70)
        result2 = runner.run(inputs={"item": items})
        print(f"\nExecutions: {executions}")
        print("Expected: ['i1', 'i2', 'i3'] (no new executions)")
        assert executions == ["i1", "i2", "i3"], (
            f"Expected no new executions, got {executions}"
        )

        print("\n✅ Per-item caching works correctly!")


if __name__ == "__main__":
    test_per_item_caching_debug()
