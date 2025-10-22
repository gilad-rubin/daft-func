"""Debug the batch UDF return type issue."""

from typing import List, get_type_hints

import daft
from pydantic import BaseModel

from daft_func.types import daft_datatype


class Item(BaseModel):
    id: str
    value: int


def func_returns_item() -> Item:
    return Item(id="test", value=1)


def func_returns_list_of_items() -> List[Item]:
    return [Item(id="test1", value=1), Item(id="test2", value=2)]


# Test type conversion
hints1 = get_type_hints(func_returns_item)
return_type1 = hints1.get("return")
print(f"Return type 1: {return_type1}")
dtype1 = daft_datatype(return_type1)
print(f"Daft dtype 1: {dtype1}")
print()

hints2 = get_type_hints(func_returns_list_of_items)
return_type2 = hints2.get("return")
print(f"Return type 2: {return_type2}")
dtype2 = daft_datatype(return_type2)
print(f"Daft dtype 2: {dtype2}")
print()

# Test creating batch UDFs
print("Testing batch UDF with Item return type:")


@daft.func.batch(return_dtype=dtype1)
def batch_udf_item(col):
    data = col.to_pylist()
    results = []
    for item in data:
        results.append({"id": f"result_{item['id']}", "value": item["value"] * 2})
    return results


df = daft.from_pylist([{"id": "a", "value": 1}, {"id": "b", "value": 2}])
result1 = batch_udf_item(df["id"])
print(f"Type of result1: {type(result1)}")
print(f"Result1: {result1}")
print()

print("Testing batch UDF with List[Item] return type:")


@daft.func.batch(return_dtype=dtype2)
def batch_udf_list(col):
    data = col.to_pylist()
    results = []
    for item in data:
        # Each result is a list of items
        results.append(
            [
                {"id": f"result_{item['id']}_1", "value": item["value"] * 2},
                {"id": f"result_{item['id']}_2", "value": item["value"] * 3},
            ]
        )
    return results


df2 = daft.from_pylist([{"id": "a", "value": 1}, {"id": "b", "value": 2}])
result2 = batch_udf_list(df2["id"])
print(f"Type of result2: {type(result2)}")
print(f"Result2: {result2}")
