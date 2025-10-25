"""
Progress Bar Demo - Multi-Row Display

This script demonstrates the new multi-row progress bar that shows all nodes
simultaneously with individual progress tracking.

Run with: uv run examples/progress_bar_demo.py
"""

import time
from typing import List

from daft_func import Pipeline, Runner, func
from daft_func.progress import ProgressConfig

# ============================================================================
# Test Pipeline with Varying Execution Times
# ============================================================================


@func(output="data")
def load_data(source: str) -> List[int]:
    """Load data from source."""
    time.sleep(0.8)
    return [1, 2, 3, 4, 5]


@func(output="cleaned")
def clean_data(data: List[int]) -> List[int]:
    """Clean the data."""
    time.sleep(1.2)
    return [x for x in data if x > 0]


@func(output="processed")
def process_data(cleaned: List[int]) -> List[float]:
    """Process the data."""
    time.sleep(0.9)
    return [float(x) * 2.0 for x in cleaned]


@func(output="features")
def extract_features(processed: List[float]) -> List[float]:
    """Extract features."""
    time.sleep(1.0)
    return [x / max(processed) for x in processed]


@func(output="result")
def compute_result(features: List[float]) -> float:
    """Compute final result."""
    time.sleep(0.7)
    return sum(features) / len(features)


# ============================================================================
# Demo Functions
# ============================================================================


def demo_single_item():
    """Demo: Single item run - each node goes 0→100% quickly."""
    print("=" * 70)
    print("DEMO 1: Single Item Run")
    print("=" * 70)
    print()
    print("All nodes visible from the start.")
    print("Each node transitions 0→100% when executed.")
    print()

    pipeline = Pipeline(
        functions=[
            load_data,
            clean_data,
            process_data,
            extract_features,
            compute_result,
        ]
    )

    # Create runner with progress enabled (default)
    runner = Runner(mode="local")

    # Run pipeline
    inputs = {"source": "test_data"}
    result = runner.run(pipeline, inputs=inputs)

    print()
    print(f"✓ Final result: {result['result']:.3f}")
    print()


def demo_multi_item():
    """Demo: Multi-item run - nodes show progressive completion (3/10, 7/10, etc.)."""
    from pydantic import BaseModel

    class Query(BaseModel):
        text: str
        id: int

    @func(output="processed_queries", map_axis="queries")
    def process_queries(queries: Query) -> str:
        """Process each query."""
        time.sleep(0.3)
        return f"Processed: {queries.text}"

    @func(output="results", map_axis="queries")
    def analyze_queries(processed_queries: str, queries: Query) -> dict:
        """Analyze processed queries."""
        time.sleep(0.4)
        return {
            "query_id": queries.id,
            "result": processed_queries,
            "score": len(queries.text),
        }

    print("=" * 70)
    print("DEMO 2: Multi-Item Run")
    print("=" * 70)
    print()
    print("Processing 5 items through pipeline.")
    print("Each node shows: '3/5 items' (60%) as it progresses.")
    print()

    pipeline = Pipeline(functions=[process_queries, analyze_queries])
    runner = Runner(mode="local")

    # Create multiple items
    queries = [Query(text=f"Query {i}", id=i) for i in range(5)]

    inputs = {"queries": queries}
    result = runner.run(pipeline, inputs=inputs)

    print()
    print(f"✓ Processed {len(result['results'])} items")
    print()


def demo_with_cache():
    """Demo: Run with caching - cached nodes show ⚡ icon."""
    import tempfile

    from daft_func import CacheConfig, DiskCache

    @func(output="expensive_data", cache=True)
    def expensive_computation(x: int) -> int:
        """Expensive computation that benefits from caching."""
        time.sleep(1.5)
        return x * x

    @func(output="cheap_result")
    def cheap_computation(expensive_data: int) -> int:
        """Quick computation."""
        time.sleep(0.2)
        return expensive_data + 1

    print("=" * 70)
    print("DEMO 3: Caching with Progress")
    print("=" * 70)
    print()
    print("First run: Both nodes execute normally")
    print("Second run: Cached node shows ⚡ (lightning bolt)")
    print()

    pipeline = Pipeline(functions=[expensive_computation, cheap_computation])

    # Use temporary cache directory
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_backend = DiskCache(cache_dir=tmpdir)
        cache_config = CacheConfig(enabled=True, backend=cache_backend)
        runner = Runner(cache_config=cache_config)

        # First run
        print("--- First Run ---")
        inputs = {"x": 10}
        result1 = runner.run(pipeline, inputs=inputs)
        print(f"Result: {result1['cheap_result']}")
        print()

        # Second run (should hit cache)
        print("--- Second Run (with cache) ---")
        result2 = runner.run(pipeline, inputs=inputs)
        print(f"Result: {result2['cheap_result']}")
        print()


def demo_theme_selection():
    """Demo: Explicit theme selection."""
    print("=" * 70)
    print("DEMO 4: Explicit Theme Selection")
    print("=" * 70)
    print()

    # Create simple pipeline
    @func(output="step1")
    def step1(x: int) -> int:
        time.sleep(0.5)
        return x * 2

    @func(output="step2")
    def step2(step1: int) -> int:
        time.sleep(0.5)
        return step1 + 10

    pipeline = Pipeline(functions=[step1, step2])

    # Demo with explicit dark theme
    print("Using DARK theme (bright colors):")
    progress_config = ProgressConfig(theme="dark")
    runner = Runner(progress_config=progress_config)
    result = runner.run(pipeline, inputs={"x": 5})
    print(f"Result: {result['step2']}")
    print()

    # Demo with explicit light theme
    print("Using LIGHT theme (darker colors):")
    progress_config = ProgressConfig(theme="light")
    runner = Runner(progress_config=progress_config)
    result = runner.run(pipeline, inputs={"x": 5})
    print(f"Result: {result['step2']}")
    print()


# ============================================================================
# Main
# ============================================================================


def main():
    """Run all demos."""
    print("\n")
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 15 + "DAFT-FUNC PROGRESS BAR DEMOS" + " " * 25 + "║")
    print("╚" + "=" * 68 + "╝")
    print()

    demo_single_item()
    time.sleep(1)

    demo_multi_item()
    time.sleep(1)

    demo_with_cache()
    time.sleep(1)

    demo_theme_selection()

    print("=" * 70)
    print("All demos completed!")
    print("=" * 70)


if __name__ == "__main__":
    main()
