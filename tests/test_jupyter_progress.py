"""
Test script for Jupyter progress bar with tqdm.

Copy the code below into your Jupyter notebook cells to test:
"""

# Cell 1: Test Theme Detection
# -----------------------------
from daft_func.theme import detect_theme, is_jupyter

print("Environment Check:")
print(f"  In Jupyter: {is_jupyter()}")
print()

theme_info = detect_theme()
print(f"Detected Theme: {theme_info['mode']}")
print(f"Auto-detected: {'Yes' if theme_info['detected'] else 'No (using default)'}")
print()

if theme_info["mode"] == "light":
    print("✓ Theme correctly detected as 'light' for VSCode Jupyter")
    print("  (Better contrast on white output background)")
else:
    print("Theme is 'dark'")
print()
print("tqdm.notebook will be used for progress bars in Jupyter!")
print()


# Cell 2: Test Single-Item Run
# -----------------------------
from typing import List

from daft_func import Pipeline, Runner, func


@func(output="data")
def load_data(x: int) -> List[int]:
    import time

    time.sleep(0.5)
    return [x, x + 1, x + 2]


@func(output="processed")
def process_data(data: List[int]) -> List[int]:
    import time

    time.sleep(0.5)
    return [x * 2 for x in data]


@func(output="result")
def compute_result(processed: List[int]) -> int:
    import time

    time.sleep(0.5)
    return sum(processed)


print("=" * 60)
print("Test 1: Single-Item Run")
print("=" * 60)
print("Expected: 3 progress bars, each going 0→100%")
print()

pipeline = Pipeline(functions=[load_data, process_data, compute_result])
runner = Runner()
result = runner.run(pipeline, inputs={"x": 5})

print()
print(f"✓ Result: {result['result']}")
print()


# Cell 3: Test Multi-Item Run
# ----------------------------
from pydantic import BaseModel


class Item(BaseModel):
    value: int
    id: str


@func(output="doubled", map_axis="items", key_attr="id")
def double_item(items: Item) -> int:
    import time

    time.sleep(0.3)
    return items.value * 2


@func(output="squared", map_axis="items", key_attr="id")
def square_item(doubled: int, items: Item) -> int:
    import time

    time.sleep(0.3)
    return doubled**2


print("=" * 60)
print("Test 2: Multi-Item Run (5 items)")
print("=" * 60)
print("Expected: 2 progress bars showing 1/5, 2/5, 3/5, 4/5, 5/5")
print()

pipeline = Pipeline(functions=[double_item, square_item])
runner = Runner(mode="local")

items = [Item(value=i, id=f"item_{i}") for i in range(1, 6)]
result = runner.run(pipeline, inputs={"items": items})

print()
print(f"✓ Processed {len(result['squared'])} items")
print(f"  Results: {result['squared']}")
print()


# Cell 4: Test with Caching
# --------------------------
import tempfile

from daft_func import CacheConfig, DiskCache


@func(output="expensive", cache=True)
def expensive_operation(x: int) -> int:
    import time

    time.sleep(1.0)
    return x**3


@func(output="cheap")
def cheap_operation(expensive: int) -> int:
    import time

    time.sleep(0.2)
    return expensive + 1


print("=" * 60)
print("Test 3: Caching")
print("=" * 60)
print()

pipeline = Pipeline(functions=[expensive_operation, cheap_operation])

with tempfile.TemporaryDirectory() as tmpdir:
    cache_backend = DiskCache(cache_dir=tmpdir)
    cache_config = CacheConfig(enabled=True, backend=cache_backend)
    runner = Runner(cache_config=cache_config)

    print("First run (should execute):")
    result1 = runner.run(pipeline, inputs={"x": 10})
    print(f"Result: {result1['cheap']}")
    print()

    print("Second run (should hit cache - look for ⚡ icon):")
    result2 = runner.run(pipeline, inputs={"x": 10})
    print(f"Result: {result2['cheap']}")
    print()

print("=" * 60)
print("All tests completed!")
print("=" * 60)
print()
print("Note: In Jupyter, you should see beautiful tqdm.notebook widgets!")
print("      In CLI, you'll see tqdm.rich progress bars with colors!")
