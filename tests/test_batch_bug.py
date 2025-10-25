"""Minimal test case for batch execution bug."""

from pydantic import BaseModel

from daft_func import Pipeline, Runner, func


class Item(BaseModel):
    """Simple item model."""

    id: str
    value: int


class Result(BaseModel):
    """Simple result model."""

    id: str
    doubled: int


@func(output="processed", map_axis="item", key_attr="id")
def process(item: Item, multiplier: int) -> Result:
    """Process an item."""
    return Result(id=item.id, doubled=item.value * multiplier)


@func(output="final_results", map_axis="item", key_attr="id")
def finalize(processed: Result) -> Result:
    """Finalize processing."""
    return Result(id=processed.id, doubled=processed.doubled + 1)


def test_batch_execution():
    """Test batch execution with multiple items."""
    # Create pipeline
    pipeline = Pipeline(functions=[process, finalize])

    # Create inputs
    inputs = {
        "item": [
            Item(id="i1", value=10),
            Item(id="i2", value=20),
            Item(id="i3", value=30),
        ],
        "multiplier": 2,
    }

    # Create runner in batch mode
    runner = Runner(mode="daft")

    # This should fail with the AttributeError
    result = runner.run(pipeline, inputs=inputs)

    print("Result:", result)
    assert len(result["final_results"]) == 3


if __name__ == "__main__":
    test_batch_execution()
