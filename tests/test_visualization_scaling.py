"""Test how visualization time scales with pipeline complexity."""

import time

from daft_func import Pipeline, func


def create_pipeline(num_functions: int) -> Pipeline:
    """Create a pipeline with a chain of N functions."""
    functions = []

    # First function
    @func(output="output_0")
    def func_0(input_0: int) -> int:
        return input_0 + 1

    functions.append(func_0)

    # Chain of functions
    for i in range(1, num_functions):
        # We need to create functions dynamically with unique names
        # Using exec to create proper function objects
        exec(
            f"""
@func(output="output_{i}")
def func_{i}(output_{i - 1}: int, param_{i}: int = 1) -> int:
    return output_{i - 1} + param_{i}
""",
            globals(),
        )
        functions.append(globals()[f"func_{i}"])

    return Pipeline(functions=functions)


def benchmark_pipeline_size():
    """Benchmark visualization time for different pipeline sizes."""
    sizes = [3, 5, 10, 15, 20]
    results = {}

    print("\n" + "=" * 80)
    print("VISUALIZATION SCALING WITH PIPELINE SIZE")
    print("=" * 80)

    for size in sizes:
        pipeline = create_pipeline(size)

        # Warm-up run
        pipeline.visualize()

        # Timed runs
        times = []
        for _ in range(3):
            start = time.perf_counter()
            pipeline.visualize()
            elapsed = time.perf_counter() - start
            times.append(elapsed)

        avg_time = sum(times) / len(times)
        results[size] = avg_time

        print(f"  {size:2d} functions: {avg_time * 1000:6.2f} ms")

    print("=" * 80)
    return results


def test_visualization_scaling():
    """Test that visualization scales reasonably with size."""
    results = benchmark_pipeline_size()

    # Check that the scaling is reasonable
    # Should be roughly linear or sub-linear
    smallest = results[min(results.keys())]
    largest = results[max(results.keys())]

    size_ratio = max(results.keys()) / min(results.keys())
    time_ratio = largest / smallest

    print(f"\nSize increased by {size_ratio:.1f}x")
    print(f"Time increased by {time_ratio:.1f}x")

    # Time should not increase more than quadratically with size
    assert time_ratio < size_ratio**2, "Visualization scaling worse than O(n^2)"


if __name__ == "__main__":
    test_visualization_scaling()
