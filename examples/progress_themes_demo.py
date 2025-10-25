"""
Progress Bar Theme Demo

This script demonstrates theme detection and customization for the progress bar.

Run with: uv run examples/progress_themes_demo.py
"""

import time

from daft_func import Pipeline, Runner, func
from daft_func.progress import ProgressConfig
from daft_func.theme import detect_theme, is_jupyter

# ============================================================================
# Simple Pipeline for Theme Testing
# ============================================================================


@func(output="step1")
def step1(x: int) -> int:
    """Step 1: Simple multiplication."""
    time.sleep(0.6)
    return x * 2


@func(output="step2")
def step2(step1: int) -> int:
    """Step 2: Add offset."""
    time.sleep(0.6)
    return step1 + 10


@func(output="step3")
def step3(step2: int) -> int:
    """Step 3: Square the result."""
    time.sleep(0.6)
    return step2**2


@func(output="final")
def final_step(step3: int) -> float:
    """Final step: Normalize."""
    time.sleep(0.6)
    return step3 / 100.0


# ============================================================================
# Demo Functions
# ============================================================================


def show_theme_detection():
    """Show what theme was detected."""
    print("=" * 70)
    print("THEME DETECTION")
    print("=" * 70)
    print()

    theme_info = detect_theme()

    print(f"Environment: {'Jupyter' if is_jupyter() else 'Terminal'}")
    print(f"Detected theme: {theme_info['mode']}")
    print(f"Auto-detected: {'Yes' if theme_info['detected'] else 'No (using default)'}")
    print()

    if theme_info["mode"] == "dark":
        print("Color scheme: Bright colors on dark background")
        print("  - Executing: Cyan")
        print("  - Completed: Bright Green")
        print("  - Cached: Yellow")
        print("  - Pending: Dim White")
    else:
        print("Color scheme: Dark colors on light background")
        print("  - Executing: Blue")
        print("  - Completed: Green")
        print("  - Cached: Yellow")
        print("  - Pending: Dim White")
    print()


def demo_auto_theme():
    """Demo with automatic theme detection."""
    print("=" * 70)
    print("DEMO 1: Automatic Theme Detection")
    print("=" * 70)
    print()
    print("Progress bar will use auto-detected theme.")
    print()

    pipeline = Pipeline(functions=[step1, step2, step3, final_step])

    # Auto-detect theme (default)
    runner = Runner()

    inputs = {"x": 5}
    result = runner.run(pipeline, inputs=inputs)

    print()
    print(f"✓ Result: {result['final']:.2f}")
    print()


def demo_explicit_dark():
    """Demo with explicit dark theme."""
    print("=" * 70)
    print("DEMO 2: Explicit DARK Theme")
    print("=" * 70)
    print()
    print("Forcing dark theme with bright colors.")
    print()

    pipeline = Pipeline(functions=[step1, step2, step3, final_step])

    # Explicit dark theme
    progress_config = ProgressConfig(theme="dark")
    runner = Runner(progress_config=progress_config)

    inputs = {"x": 7}
    result = runner.run(pipeline, inputs=inputs)

    print()
    print(f"✓ Result: {result['final']:.2f}")
    print()


def demo_explicit_light():
    """Demo with explicit light theme."""
    print("=" * 70)
    print("DEMO 3: Explicit LIGHT Theme")
    print("=" * 70)
    print()
    print("Forcing light theme with darker colors.")
    print()

    pipeline = Pipeline(functions=[step1, step2, step3, final_step])

    # Explicit light theme
    progress_config = ProgressConfig(theme="light")
    runner = Runner(progress_config=progress_config)

    inputs = {"x": 3}
    result = runner.run(pipeline, inputs=inputs)

    print()
    print(f"✓ Result: {result['final']:.2f}")
    print()


def demo_disable_progress():
    """Demo with progress disabled."""
    print("=" * 70)
    print("DEMO 4: Progress Disabled")
    print("=" * 70)
    print()
    print("Running pipeline with progress bar disabled.")
    print()

    pipeline = Pipeline(functions=[step1, step2, step3, final_step])

    # Disable progress
    progress_config = ProgressConfig(enabled=False)
    runner = Runner(progress_config=progress_config)

    inputs = {"x": 8}
    result = runner.run(pipeline, inputs=inputs)

    print()
    print(f"✓ Result: {result['final']:.2f}")
    print("(No progress bar shown)")
    print()


def demo_customize_indicators():
    """Demo with customized indicators."""
    print("=" * 70)
    print("DEMO 5: Customize Progress Display")
    print("=" * 70)
    print()

    # Different configurations
    configs = [
        ("Default (all indicators)", ProgressConfig()),
        ("No cache indicators", ProgressConfig(show_cache_indicators=False)),
        ("No timing", ProgressConfig(show_timing=False)),
        (
            "Minimal (no extras)",
            ProgressConfig(show_cache_indicators=False, show_timing=False),
        ),
    ]

    for desc, config in configs:
        print(f"--- {desc} ---")
        pipeline = Pipeline(functions=[step1, step2])
        runner = Runner(progress_config=config)
        result = runner.run(pipeline, inputs={"x": 4})
        print()
        time.sleep(0.5)


# ============================================================================
# Main
# ============================================================================


def main():
    """Run all theme demos."""
    print("\n")
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 18 + "THEME CONFIGURATION DEMOS" + " " * 25 + "║")
    print("╚" + "=" * 68 + "╝")
    print()

    show_theme_detection()
    time.sleep(1)

    demo_auto_theme()
    time.sleep(1)

    demo_explicit_dark()
    time.sleep(1)

    demo_explicit_light()
    time.sleep(1)

    demo_disable_progress()
    time.sleep(1)

    demo_customize_indicators()

    print("=" * 70)
    print("All theme demos completed!")
    print("=" * 70)


if __name__ == "__main__":
    main()
