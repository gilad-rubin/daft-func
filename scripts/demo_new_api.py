"""Demo showing the new Runner and Pipeline API."""

from pydantic import BaseModel

from daft_func import Pipeline, Runner, func


class Item(BaseModel):
    """Test item model."""

    value: int


@func(output="doubled")
def double_value(item: Item) -> int:
    """Double the item's value."""
    return item.value * 2


@func(output="result")
def add_ten(doubled: int) -> int:
    """Add ten to the doubled value."""
    return doubled + 10


def main():
    """Demonstrate the new API."""
    print("\n" + "=" * 70)
    print("DEMO: New Runner and Pipeline API")
    print("=" * 70)

    pipeline = Pipeline(functions=[double_value, add_ten])

    print("\n1. Using Runner explicitly:")
    print("-" * 70)
    runner = Runner(mode="local")
    result = runner.run(pipeline, inputs={"item": Item(value=5)})
    print(f"   Result: {result['result']}")  # (5 * 2) + 10 = 20

    print("\n2. Using Pipeline.run() (convenience method):")
    print("-" * 70)
    result = pipeline.run(inputs={"item": Item(value=7)}, mode="local")
    print(f"   Result: {result['result']}")  # (7 * 2) + 10 = 24

    print("\n" + "=" * 70)
    print("Key Changes:")
    print("  - Runner.__init__() no longer takes 'pipeline' parameter")
    print("  - Runner.run() now takes 'pipeline' as first positional argument")
    print("  - Pipeline.run() is a new convenience method with default runner")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
