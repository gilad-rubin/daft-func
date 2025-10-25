"""Demo to test progress bar alignment with different node name lengths."""

import time

from daft_func import Pipeline, Runner, func


# Create nodes with varying name lengths to test alignment
@func(output="a")
def a(x: int) -> int:
    """Very short name."""
    time.sleep(0.3)
    return x * 2


@func(output="short_name")
def short_name(a: int) -> int:
    """Short name."""
    time.sleep(0.3)
    return a + 5


@func(output="medium_length_name")
def medium_length_name(short_name: int) -> int:
    """Medium length name."""
    time.sleep(0.3)
    return short_name * 3


@func(output="very_very_long_node_name")
def very_very_long_node_name(medium_length_name: int) -> int:
    """Very long name."""
    time.sleep(0.3)
    return medium_length_name - 10


@func(output="result")
def result(very_very_long_node_name: int) -> int:
    """Back to short name."""
    time.sleep(0.3)
    return very_very_long_node_name + 100


print("=" * 70)
print("ALIGNMENT TEST: Different Node Name Lengths")
print("=" * 70)
print()
print("Testing that all progress bars align correctly despite")
print("different node name lengths:")
print("  - 'a' (1 char)")
print("  - 'short_name' (10 chars)")
print("  - 'medium_length_name' (18 chars)")
print("  - 'very_very_long_node_name' (24 chars)")
print("  - 'result' (6 chars)")
print()
print("All bars should start and end at the same column!")
print()

pipeline = Pipeline(
    functions=[a, short_name, medium_length_name, very_very_long_node_name, result]
)

runner = Runner()
result = runner.run(pipeline, inputs={"x": 10})

print()
print(f"✓ Result: {result['result']}")
print()
print("=" * 70)
print("Check above: All progress bars should be perfectly aligned!")
print("=" * 70)
