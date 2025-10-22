"""Demo script showing caching functionality with daft_func."""

import time

from daft_func import CacheConfig, DiskCache, Pipeline, Runner, func


# Simulate expensive operations
@func(output="processed", cache=True, cache_key="v1")
def expensive_processing(data: str) -> str:
    """Simulate expensive data processing."""
    print(f"  [Processing '{data}']... ", end="", flush=True)
    time.sleep(1)  # Simulate expensive operation
    result = data.upper()
    print("Done!")
    return result


@func(output="result", cache=True)
def expensive_analysis(processed: str, threshold: int) -> dict:
    """Simulate expensive analysis."""
    print(
        f"  [Analyzing '{processed}' with threshold {threshold}]... ",
        end="",
        flush=True,
    )
    time.sleep(0.5)  # Simulate expensive operation
    result = {
        "length": len(processed),
        "threshold": threshold,
        "valid": len(processed) > threshold,
    }
    print("Done!")
    return result


def main():
    """Run the caching demo."""
    print("=" * 70)
    print("daft_func Caching Demo")
    print("=" * 70)

    # Create pipeline
    pipeline = Pipeline(functions=[expensive_processing, expensive_analysis])

    # Demo 1: Without caching
    print("\n📝 Demo 1: Without Caching")
    print("-" * 70)
    runner_no_cache = Runner(pipeline=pipeline, mode="local")

    print("\nRun 1 (no cache):")
    start = time.time()
    result1 = runner_no_cache.run(inputs={"data": "hello world", "threshold": 5})
    time1 = time.time() - start
    print(f"Result: {result1['result']}")
    print(f"Time: {time1:.2f}s")

    print("\nRun 2 (no cache, same inputs):")
    start = time.time()
    result2 = runner_no_cache.run(inputs={"data": "hello world", "threshold": 5})
    time2 = time.time() - start
    print(f"Result: {result2['result']}")
    print(f"Time: {time2:.2f}s")
    print("→ Both operations re-execute every time")

    # Demo 2: With caching
    print("\n📝 Demo 2: With Caching Enabled")
    print("-" * 70)
    cache_config = CacheConfig(enabled=True, backend=DiskCache(cache_dir=".cache/demo"))
    runner_cache = Runner(pipeline=pipeline, mode="local", cache_config=cache_config)

    print("\nRun 1 (cold cache):")
    start = time.time()
    result3 = runner_cache.run(inputs={"data": "hello world", "threshold": 5})
    time3 = time.time() - start
    print(f"Result: {result3['result']}")
    print(f"Time: {time3:.2f}s")

    print("\nRun 2 (warm cache, same inputs):")
    start = time.time()
    result4 = runner_cache.run(inputs={"data": "hello world", "threshold": 5})
    time4 = time.time() - start
    print(f"Result: {result4['result']}")
    print(f"Time: {time4:.2f}s")
    print("→ Both operations use cache - instant!")

    # Demo 3: Downstream change only
    print("\n📝 Demo 3: Smart Cache Invalidation (Downstream Change)")
    print("-" * 70)
    print("\nRun 3 (change threshold only):")
    start = time.time()
    result5 = runner_cache.run(inputs={"data": "hello world", "threshold": 10})
    time5 = time.time() - start
    print(f"Result: {result5['result']}")
    print(f"Time: {time5:.2f}s")
    print("→ Processing uses cache, only analysis re-executes!")

    # Demo 4: Upstream change
    print("\n📝 Demo 4: Smart Cache Invalidation (Upstream Change)")
    print("-" * 70)
    print("\nRun 4 (change data):")
    start = time.time()
    result6 = runner_cache.run(inputs={"data": "goodbye", "threshold": 5})
    time6 = time.time() - start
    print(f"Result: {result6['result']}")
    print(f"Time: {time6:.2f}s")
    print("→ Data changed, both operations re-execute")

    # Summary
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"Without caching: {time1:.2f}s + {time2:.2f}s = {time1 + time2:.2f}s")
    print(f"With caching:    {time3:.2f}s + {time4:.2f}s = {time3 + time4:.2f}s")
    print(f"Speedup: {(time1 + time2) / (time3 + time4):.1f}x")
    print("\n✨ Caching provides:")
    print("  • Automatic invalidation on code/input/dependency changes")
    print("  • Smart detection of upstream vs downstream changes")
    print("  • Zero overhead when disabled")
    print("  • Support for complex types (Pydantic models)")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
