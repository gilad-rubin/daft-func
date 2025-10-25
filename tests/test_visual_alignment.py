"""Visual test for progress bar alignment."""

import tempfile
import time

from daft_func import CacheConfig, DiskCache, Pipeline, Runner, func


# Create nodes with names matching the user's screenshot
@func(output="index_path", cache=True)
def index_path(x: int) -> str:
    """Index step."""
    time.sleep(0.3)
    return f"index_{x}"


@func(output="hits", cache=True)
def hits(index_path: str) -> int:
    """Hits step."""
    time.sleep(0.3)
    return len(index_path) * 2


@func(output="reranked_hits", cache=True)
def reranked_hits(hits: int) -> int:
    """Reranked hits step."""
    time.sleep(0.3)
    return hits * 3


print("=" * 80)
print("VISUAL ALIGNMENT TEST - Matching User's Scenario")
print("=" * 80)
print()
print("Nodes: index_path (10 chars), hits (4 chars), reranked_hits (13 chars)")
print()

pipeline = Pipeline(functions=[index_path, hits, reranked_hits])

with tempfile.TemporaryDirectory() as tmpdir:
    cache_backend = DiskCache(cache_dir=tmpdir)
    cache_config = CacheConfig(enabled=True, backend=cache_backend)
    runner = Runner(cache_config=cache_config)

    print("First run (all execute with ✓):")
    print()
    result1 = runner.run(pipeline, inputs={"x": 10})
    print()
    print(f"Result: {result1['reranked_hits']}")
    print()

    time.sleep(0.5)

    print("-" * 80)
    print("Second run (all cached with ⚡):")
    print()
    result2 = runner.run(pipeline, inputs={"x": 10})
    print()
    print(f"Result: {result2['reranked_hits']}")
    print()

print("=" * 80)
print("Check: All colons should be perfectly aligned!")
print("=" * 80)
