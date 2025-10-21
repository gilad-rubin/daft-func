"""Demo script showing pipeline visualization."""

from daft_func import Pipeline, func


# Define a simple pipeline
@func(output="doubled")
def double(x: int) -> int:
    """Double the input value."""
    return x * 2


@func(output="result")
def add_value(doubled: int, offset: int = 5) -> int:
    """Add an offset to the doubled value."""
    return doubled + offset


def main():
    """Run the visualization demo."""
    print("=" * 70)
    print("daft_func Visualization Demo")
    print("=" * 70)

    # Create pipeline with explicit functions
    pipeline = Pipeline(functions=[double, add_value])

    # Create visualization
    print("\n📊 Generating pipeline visualization...\n")

    viz = pipeline.visualize(
        orient="LR",  # Left to right layout
        show_legend=True,
        return_type="graphviz",
    )

    # Save to file
    output_file = "pipeline_visualization.png"
    viz.render("pipeline_visualization", format="png", cleanup=True)
    print(f"✅ Visualization saved to {output_file}")

    # Also save as SVG for vector graphics
    viz.render("pipeline_visualization", format="svg", cleanup=True)
    print("✅ Visualization saved to pipeline_visualization.svg")

    print("\nVisualization features:")
    print("  - Input parameters (green, dashed boxes) with type annotations")
    print("  - Function nodes (blue, rounded boxes) with output types")
    print("  - Dependency edges labeled with parameter names")
    print("  - Default values displayed on parameters (offset=5)")

    print("\n" + "=" * 70)
    print("🎉 Demo complete!")
    print("=" * 70)


if __name__ == "__main__":
    main()
