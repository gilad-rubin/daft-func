"""Debug the select issue with batch UDFs."""

from typing import List

import daft
from pydantic import BaseModel

from daft_func.types import daft_datatype


class Item(BaseModel):
    id: str
    value: int


# Test with List[Item] return type
dtype = daft_datatype(List[Item])
print(f"Daft dtype for List[Item]: {dtype}")


@daft.func.batch(return_dtype=dtype)
def batch_udf_list(col):
    data = col.to_pylist()
    print(f"Input data: {data}")
    results = []
    for item in data:
        # Each result is a list of items
        results.append(
            [
                {"id": f"result_{item}_1", "value": 1},
                {"id": f"result_{item}_2", "value": 2},
            ]
        )
    print(f"Results: {results}")
    print(f"Type of results: {type(results)}")
    return results


# Create a dataframe and try to select with the UDF
df = daft.from_pylist([{"id": "a"}, {"id": "b"}])
print(f"Initial df:\n{df.collect()}")

expr = batch_udf_list(df["id"])
print(f"\nExpression type: {type(expr)}")
print(f"Expression: {expr}")

# Now try to select with it
try:
    df2 = df.select(df["id"].alias("id"), expr.alias("items"))
    print(f"\nAfter select df:\n{df2.collect()}")
except Exception as e:
    print(f"\nError during select: {e}")
    import traceback

    traceback.print_exc()
