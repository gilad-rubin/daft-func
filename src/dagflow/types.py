"""Type conversion utilities for Pydantic models to Daft/PyArrow types."""

from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel

try:
    import daft
    import pyarrow

    DAFT_AVAILABLE = True
except ImportError:
    DAFT_AVAILABLE = False


def pyarrow_datatype(f_type: type[Any]) -> "pyarrow.DataType":
    """Convert Python/Pydantic types to PyArrow DataTypes.

    Based on the pattern from Daft's PDF processing example.
    Supports: Pydantic models, lists, dicts, unions/optionals, and primitives.
    """
    if not DAFT_AVAILABLE:
        raise ImportError("pyarrow is required for type conversion")

    if get_origin(f_type) is Union:
        targs = get_args(f_type)
        if len(targs) == 2:
            if targs[0] is type(None) and targs[1] is not type(None):
                refined_inner = targs[1]
            elif targs[0] is not type(None) and targs[1] is type(None):
                refined_inner = targs[0]
            else:
                raise TypeError(
                    f"Cannot convert a general union type {f_type} into a pyarrow.DataType!"
                )
            inner_type = pyarrow_datatype(refined_inner)
        else:
            raise TypeError(
                f"Cannot convert a general union type {f_type} into a pyarrow.DataType!"
            )

    elif get_origin(f_type) is list:
        targs = get_args(f_type)
        if len(targs) != 1:
            raise TypeError(
                f"Expected list type {f_type} with inner element type but got {len(targs)} inner-types: {targs}"
            )
        element_type = targs[0]
        inner_type = pyarrow.list_(pyarrow_datatype(element_type))

    elif get_origin(f_type) is dict:
        targs = get_args(f_type)
        if len(targs) != 2:
            raise TypeError(
                f"Expected dict type {f_type} with inner key-value types but got {len(targs)} inner-types: {targs}"
            )
        kt, vt = targs
        pyarrow_kt = pyarrow_datatype(kt)
        pyarrow_vt = pyarrow_datatype(vt)
        inner_type = pyarrow.map_(pyarrow_kt, pyarrow_vt)

    elif get_origin(f_type) is tuple:
        raise TypeError(f"Cannot support tuple types: {f_type}")

    elif isinstance(f_type, type) and issubclass(f_type, BaseModel):
        # Get PyArrow schema from Pydantic model
        schema_dict = {}
        for field_name, field_info in f_type.model_fields.items():
            schema_dict[field_name] = pyarrow_datatype(field_info.annotation)
        inner_type = pyarrow.struct([(k, v) for k, v in schema_dict.items()])

    elif isinstance(f_type, type) and issubclass(f_type, bool):
        # Check bool before int since bool is a subclass of int in Python
        inner_type = pyarrow.bool_()

    elif isinstance(f_type, type) and issubclass(f_type, str):
        inner_type = pyarrow.string()

    elif isinstance(f_type, type) and issubclass(f_type, int):
        inner_type = pyarrow.int64()

    elif isinstance(f_type, type) and issubclass(f_type, float):
        inner_type = pyarrow.float64()

    elif isinstance(f_type, type) and issubclass(f_type, bytes):
        inner_type = pyarrow.binary()

    else:
        raise TypeError(f"Cannot handle general Python objects in Arrow: {f_type}")

    return inner_type


def daft_datatype(f_type: type[Any]) -> "daft.DataType":
    """Convert Python/Pydantic types to Daft DataTypes via PyArrow."""
    if not DAFT_AVAILABLE:
        raise ImportError("daft is required for type conversion")
    return daft.DataType.from_arrow_type(pyarrow_datatype(f_type))
