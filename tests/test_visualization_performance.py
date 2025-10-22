"""Performance benchmarking and profiling for visualization."""

import cProfile
import pstats
import time
from io import StringIO
from typing import Dict, List

from daft_func import Pipeline, func
from examples.retrieval import (
    Query,
    RerankedHit,
    Reranker,
    RetrievalResult,
    Retriever,
)


@func(output="index")
def index(retriever: Retriever, corpus: Dict[str, str], test: bool = True) -> bool:
    retriever.index(corpus)
    return True


@func(output="hits", map_axis="query", key_attr="query_uuid")
def retrieve(
    retriever: Retriever, query: Query, top_k: int, index: bool
) -> RetrievalResult:
    return retriever.retrieve(query, top_k=top_k)


@func(output="reranked_hits", map_axis="query", key_attr="query_uuid")
def rerank(
    reranker: Reranker, query: Query, hits: RetrievalResult, top_k: int
) -> List[RerankedHit]:
    return reranker.rerank(query, hits, top_k=top_k)


def benchmark_visualization(num_runs: int = 10) -> dict:
    """Benchmark visualization performance over multiple runs."""
    pipeline = Pipeline(functions=[index, retrieve, rerank])

    times = []
    for _ in range(num_runs):
        start = time.perf_counter()
        pipeline.visualize()
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return {
        "mean": sum(times) / len(times),
        "min": min(times),
        "max": max(times),
        "times": times,
    }


def profile_visualization() -> str:
    """Profile visualization to see where time is being spent."""
    pipeline = Pipeline(functions=[index, retrieve, rerank])

    profiler = cProfile.Profile()
    profiler.enable()

    # Run visualization
    pipeline.visualize()

    profiler.disable()

    # Get stats
    s = StringIO()
    stats = pstats.Stats(profiler, stream=s)
    stats.strip_dirs()
    stats.sort_stats("cumulative")
    stats.print_stats(30)  # Show top 30 functions

    return s.getvalue()


def test_visualization_performance():
    """Test that visualization is reasonably fast."""
    print("\n" + "=" * 80)
    print("VISUALIZATION PERFORMANCE BENCHMARK")
    print("=" * 80)

    # Benchmark
    print("\nRunning 10 iterations...")
    results = benchmark_visualization(num_runs=10)

    print("\nResults:")
    print(f"  Mean time: {results['mean'] * 1000:.2f} ms")
    print(f"  Min time:  {results['min'] * 1000:.2f} ms")
    print(f"  Max time:  {results['max'] * 1000:.2f} ms")
    print(f"  All times: {[f'{t * 1000:.2f}ms' for t in results['times']]}")

    # Profile
    print("\n" + "=" * 80)
    print("PROFILING RESULTS (Top 30 functions by cumulative time)")
    print("=" * 80)
    profile_output = profile_visualization()
    print(profile_output)

    # Assert reasonable performance (< 1 second)
    assert results["mean"] < 1.0, f"Visualization too slow: {results['mean']:.3f}s"


if __name__ == "__main__":
    test_visualization_performance()
