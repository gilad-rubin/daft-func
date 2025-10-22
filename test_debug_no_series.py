"""Debug batch UDF with no series arguments."""

import daft


@daft.func.batch(return_dtype=daft.DataType.bool())
def my_udf():
    """Batch UDF with no arguments."""
    print("my_udf called")
    result = [True, True, True]
    print(f"Returning: {result}, type: {type(result)}")
    return result


# Try calling it with no arguments
result = my_udf()
print(f"Result type: {type(result)}")
print(f"Result: {result}")
