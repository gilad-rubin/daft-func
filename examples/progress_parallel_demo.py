"""
Progress Bar Parallel Execution Demo

This script demonstrates how the multi-row progress display would work
with parallel/concurrent execution (future feature).

Currently executes sequentially, but the display is designed to show
multiple nodes "in progress" simultaneously when parallel execution is added.

Run with: uv run examples/progress_parallel_demo.py
"""

import time

from daft_func import Pipeline, Runner, func

# ============================================================================
# Pipeline with Independent Branches
# ============================================================================


@func(output="input_data")
def load_input(source: str) -> dict:
    """Load initial data."""
    time.sleep(0.5)
    return {"values": [1, 2, 3, 4, 5], "metadata": {"source": source}}


# Branch 1: Statistical Analysis
@func(output="stats")
def compute_statistics(input_data: dict) -> dict:
    """Compute statistics (independent branch 1)."""
    time.sleep(1.5)
    values = input_data["values"]
    return {
        "mean": sum(values) / len(values),
        "min": min(values),
        "max": max(values),
    }


# Branch 2: Data Transformation
@func(output="transformed")
def transform_data(input_data: dict) -> list:
    """Transform data (independent branch 2)."""
    time.sleep(1.2)
    return [x * 2 for x in input_data["values"]]


# Branch 3: Metadata Enrichment
@func(output="enriched_meta")
def enrich_metadata(input_data: dict) -> dict:
    """Enrich metadata (independent branch 3)."""
    time.sleep(1.0)
    meta = input_data["metadata"].copy()
    meta["processed_at"] = time.time()
    meta["item_count"] = len(input_data["values"])
    return meta


# Final aggregation (depends on all branches)
@func(output="final_result")
def aggregate_results(stats: dict, transformed: list, enriched_meta: dict) -> dict:
    """Aggregate all branch results."""
    time.sleep(0.8)
    return {
        "statistics": stats,
        "transformed_data": transformed,
        "metadata": enriched_meta,
        "summary": f"Processed {enriched_meta['item_count']} items",
    }


# ============================================================================
# Demo Functions
# ============================================================================


def demo_sequential_execution():
    """Demo: Current sequential execution with multi-row display."""
    print("=" * 70)
    print("DEMO: Sequential Execution with Multi-Row Display")
    print("=" * 70)
    print()
    print("Pipeline Structure:")
    print("  load_input")
    print("  ├─→ compute_statistics    (Branch 1)")
    print("  ├─→ transform_data         (Branch 2)")
    print("  └─→ enrich_metadata        (Branch 3)")
    print("      └─→ aggregate_results")
    print()
    print("Currently: Nodes execute sequentially")
    print("Display: All nodes visible, showing execution order")
    print()
    print("Future: Branches 1-3 could execute in parallel")
    print("Display: Would show multiple nodes 'in progress' simultaneously")
    print()

    pipeline = Pipeline(
        functions=[
            load_input,
            compute_statistics,
            transform_data,
            enrich_metadata,
            aggregate_results,
        ]
    )

    runner = Runner()

    inputs = {"source": "demo_data.csv"}
    result = runner.run(pipeline, inputs=inputs)

    print()
    print("✓ Result Summary:")
    print(f"  {result['final_result']['summary']}")
    print(f"  Statistics: {result['final_result']['statistics']}")
    print()


def demo_visualization():
    """Show how the pipeline would be visualized."""
    print("=" * 70)
    print("Pipeline Visualization Benefits")
    print("=" * 70)
    print()
    print("Multi-row progress display advantages:")
    print()
    print("1. Complete Pipeline View")
    print("   - See all nodes at once")
    print("   - Understand pipeline structure")
    print("   - Track overall progress")
    print()
    print("2. Parallel Execution Ready")
    print("   - Independent branches clearly visible")
    print("   - Multiple 'in progress' indicators possible")
    print("   - Natural fit for concurrent execution")
    print()
    print("3. Per-Node Progress")
    print("   - Single item: 0→100% per node")
    print("   - Multi-item: '3/10 items' (30%) per node")
    print("   - Cache status: ⚡ for cached nodes")
    print()
    print("4. Timing Information")
    print("   - Execution time per node")
    print("   - Identify bottlenecks")
    print("   - Optimize pipeline performance")
    print()

    # Quick demo
    print("Running quick example...")
    print()

    @func(output="a")
    def node_a(x: int) -> int:
        time.sleep(0.3)
        return x * 2

    @func(output="b")
    def node_b(x: int) -> int:
        time.sleep(0.3)
        return x + 10

    @func(output="c")
    def node_c(a: int, b: int) -> int:
        time.sleep(0.3)
        return a + b

    pipeline = Pipeline(functions=[node_a, node_b, node_c])
    runner = Runner()
    result = runner.run(pipeline, inputs={"x": 5})

    print()
    print(f"✓ Result: {result['c']}")
    print()


def demo_future_parallel():
    """Describe how parallel execution would work."""
    print("=" * 70)
    print("Future: Parallel Execution")
    print("=" * 70)
    print()
    print("When parallel execution is implemented:")
    print()
    print("Current display (sequential):")
    print("  ⏸  load_input      [----------] 0%")
    print("  ⏸  branch_1        [----------] 0%")
    print("  ⏸  branch_2        [----------] 0%")
    print("  ⏸  branch_3        [----------] 0%")
    print("  ⏸  aggregate       [----------] 0%")
    print()
    print("After load_input completes:")
    print("  ✓  load_input      [##########] 100%  0.5s")
    print("  ⏸  branch_1        [----------] 0%")
    print("  ⏸  branch_2        [----------] 0%")
    print("  ⏸  branch_3        [----------] 0%")
    print("  ⏸  aggregate       [----------] 0%")
    print()
    print("With parallel execution, branches execute simultaneously:")
    print("  ✓  load_input      [##########] 100%  0.5s")
    print("  ⚙  branch_1        [####------] 40%")
    print("  ⚙  branch_2        [######----] 60%")
    print("  ⚙  branch_3        [########--] 80%")
    print("  ⏸  aggregate       [----------] 0%")
    print()
    print("All branches complete, aggregate runs:")
    print("  ✓  load_input      [##########] 100%  0.5s")
    print("  ✓  branch_1        [##########] 100%  1.5s")
    print("  ✓  branch_2        [##########] 100%  1.2s")
    print("  ✓  branch_3        [##########] 100%  1.0s")
    print("  ⚙  aggregate       [######----] 60%")
    print()
    print("The multi-row display naturally supports this visualization!")
    print()


# ============================================================================
# Main
# ============================================================================


def main():
    """Run parallel execution demos."""
    print("\n")
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 14 + "PARALLEL EXECUTION VISUALIZATION" + " " * 22 + "║")
    print("╚" + "=" * 68 + "╝")
    print()

    demo_sequential_execution()
    time.sleep(1)

    demo_visualization()
    time.sleep(1)

    demo_future_parallel()

    print("=" * 70)
    print("Demo completed!")
    print()
    print("The multi-row progress display is designed to support")
    print("parallel execution when it's implemented in the future.")
    print("=" * 70)


if __name__ == "__main__":
    main()
