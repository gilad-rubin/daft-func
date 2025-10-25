"""Demo to test progress bar alignment with cached vs non-cached nodes."""

import tempfile
import time

from daft_func import CacheConfig, DiskCache, Pipeline, Runner, func


# Create a mix of cached and non-cached nodes
@func(output="step1", cache=True)
def step1(x: int) -> int:
    """Cached step."""
    time.sleep(0.5)
    return x * 2


@func(output="step2", cache=True)
def step2(step1: int) -> int:
    """Cached step."""
    time.sleep(0.5)
    return step1 + 10


@func(output="step3")
def step3(step2: int) -> int:
    """Non-cached step."""
    time.sleep(0.3)
    return step2 * 3


@func(output="final_result")
def final_result(step3: int) -> int:
    """Non-cached step."""
    time.sleep(0.3)
    return step3 + 5


print("=" * 70)
print("ALIGNMENT TEST: Cached (⚡) vs Non-Cached (✓) Icons")
print("=" * 70)
print()
print("First run: All nodes execute (all show ✓)")
print()

pipeline = Pipeline(functions=[step1, step2, step3, final_result])

with tempfile.TemporaryDirectory() as tmpdir:
    cache_backend = DiskCache(cache_dir=tmpdir)
    cache_config = CacheConfig(enabled=True, backend=cache_backend)
    runner = Runner(cache_config=cache_config)

    # First run - all execute
    print("--- First Run (all execute) ---")
    result1 = runner.run(pipeline, inputs={"x": 10})
    print(f"Result: {result1['final_result']}")
    print()

    time.sleep(1)

    # Second run - cached nodes show ⚡
    print("--- Second Run (step1 and step2 cached with ⚡) ---")
    result2 = runner.run(pipeline, inputs={"x": 10})
    print(f"Result: {result2['final_result']}")
    print()

print("=" * 70)
print("Check above: All progress bars aligned despite different icons!")
print("  ✓ (checkmark) and ⚡ (lightning) should be same visual width")
print("=" * 70)
