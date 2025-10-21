"""Tests for type conversion utilities."""

import pytest
from pydantic import BaseModel

from dagflow.types import DAFT_AVAILABLE, daft_datatype, pyarrow_datatype


@pytest.mark.skipif(not DAFT_AVAILABLE, reason="Daft not available")
def test_pyarrow_primitive_types():
    """Test conversion of primitive Python types to PyArrow."""
    import pyarrow as pa

    assert pyarrow_datatype(str) == pa.string()
    assert pyarrow_datatype(int) == pa.int64()
    assert pyarrow_datatype(float) == pa.float64()
    assert pyarrow_datatype(bool) == pa.bool_()
    assert pyarrow_datatype(bytes) == pa.binary()


@pytest.mark.skipif(not DAFT_AVAILABLE, reason="Daft not available")
def test_pyarrow_list_types():
    """Test conversion of list types to PyArrow."""
    from typing import List

    import pyarrow as pa

    list_int = pyarrow_datatype(List[int])
    assert isinstance(list_int, pa.ListType)
    assert list_int.value_type == pa.int64()

    list_str = pyarrow_datatype(List[str])
    assert isinstance(list_str, pa.ListType)
    assert list_str.value_type == pa.string()


@pytest.mark.skipif(not DAFT_AVAILABLE, reason="Daft not available")
def test_pyarrow_optional_types():
    """Test conversion of optional/union types to PyArrow."""
    from typing import Optional

    import pyarrow as pa

    # Optional[int] should convert to int64
    opt_int = pyarrow_datatype(Optional[int])
    assert opt_int == pa.int64()


@pytest.mark.skipif(not DAFT_AVAILABLE, reason="Daft not available")
def test_pyarrow_pydantic_model():
    """Test conversion of Pydantic models to PyArrow struct."""
    import pyarrow as pa

    class Person(BaseModel):
        name: str
        age: int

    person_type = pyarrow_datatype(Person)
    assert isinstance(person_type, pa.StructType)

    # Check fields
    fields = {f.name: f.type for f in person_type}
    assert fields["name"] == pa.string()
    assert fields["age"] == pa.int64()


@pytest.mark.skipif(not DAFT_AVAILABLE, reason="Daft not available")
def test_pyarrow_nested_pydantic():
    """Test conversion of nested Pydantic models."""
    import pyarrow as pa

    class Address(BaseModel):
        street: str
        city: str

    class Person(BaseModel):
        name: str
        address: Address

    person_type = pyarrow_datatype(Person)
    assert isinstance(person_type, pa.StructType)

    fields = {f.name: f.type for f in person_type}
    assert fields["name"] == pa.string()
    assert isinstance(fields["address"], pa.StructType)


@pytest.mark.skipif(not DAFT_AVAILABLE, reason="Daft not available")
def test_pyarrow_list_of_models():
    """Test conversion of List[BaseModel]."""
    from typing import List

    import pyarrow as pa

    class Item(BaseModel):
        id: str
        value: int

    list_item_type = pyarrow_datatype(List[Item])
    assert isinstance(list_item_type, pa.ListType)
    assert isinstance(list_item_type.value_type, pa.StructType)


@pytest.mark.skipif(not DAFT_AVAILABLE, reason="Daft not available")
def test_daft_datatype_conversion():
    """Test conversion to Daft DataType."""
    import daft

    # Primitive type
    daft_int = daft_datatype(int)
    assert isinstance(daft_int, daft.DataType)

    # Pydantic model
    class Person(BaseModel):
        name: str
        age: int

    daft_person = daft_datatype(Person)
    assert isinstance(daft_person, daft.DataType)


def test_type_conversion_without_daft():
    """Test that appropriate errors are raised without Daft."""
    if DAFT_AVAILABLE:
        pytest.skip("Daft is available, cannot test missing import")

    with pytest.raises(ImportError):
        pyarrow_datatype(int)

    with pytest.raises(ImportError):
        daft_datatype(int)


@pytest.mark.skipif(not DAFT_AVAILABLE, reason="Daft not available")
def test_unsupported_types():
    """Test that unsupported types raise appropriate errors."""
    from typing import Tuple

    # Tuples are not supported
    with pytest.raises(TypeError, match="Cannot support tuple types"):
        pyarrow_datatype(Tuple[int, str])

    # General objects are not supported
    class CustomClass:
        pass

    with pytest.raises(TypeError, match="Cannot handle general Python objects"):
        pyarrow_datatype(CustomClass)
