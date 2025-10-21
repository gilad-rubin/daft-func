# Fixes Applied to flow.py

## Summary
Fixed critical issues in the Daft integration to make the DAG flow system work correctly with Pydantic models and batch processing.

## Issues Fixed

### 1. **Lambda Closure Bug** ⚠️ CRITICAL
**Problem**: Lambda functions in the batch UDF were capturing loop variables incorrectly, causing all deserializers to reference the same final iteration value.

**Solution**: 
- Created a factory function `_make_batch_udf()` that properly captures variables in closures
- Used default parameters (`t=inner_type`) in lambda functions to capture values correctly

### 2. **Pydantic Type Conversion** 🔧 CRITICAL
**Problem**: Daft couldn't cast complex Pydantic struct types to Python, throwing:
```
DaftError: not implemented: Daft casting from Struct[...] to Python not implemented
```

**Solution**: 
- Implemented `pyarrow_datatype()` and `daft_datatype()` helper functions (from daft-pdf-example.md)
- These convert Pydantic BaseModel types to PyArrow struct types
- Now batch UDFs use proper `return_dtype` based on function type hints instead of generic `python()` type
- Supports: Pydantic models, Lists, Unions/Optionals, primitives (str, int, float, bool)

### 3. **Parameter Filtering** 🐛
**Problem**: Batch UDFs were passing ALL constants to every node function, causing "unexpected keyword argument" errors.

**Solution**: 
- Filter constants to only those that are actual parameters of each node: 
  ```python
  node_constants = {name: constants[name] for name in arg_names if name in constants}
  ```

### 4. **Series-to-Parameter Mapping** 🐛
**Problem**: Index mismatch between series columns and function parameters due to constants being mixed with series args.

**Solution**: 
- Created `series_param_indices` list that maps series column index to (param_name, deserializer)
- Separated constant handling from series handling in batch UDF

### 5. **Execution Mode Handling** 🔧
**Problem**: `mode='local'` with list inputs tried to pass the entire list as a single query.

**Solution**: 
- Refactored `run()` method to handle three cases:
  1. **Batching mode** (Daft): Use `_run_batch()`
  2. **List inputs without batching**: Loop manually over items with `_run_single()`
  3. **True single item**: Direct `_run_single()` call

## Key Improvements

✅ **Type Safety**: Proper Pydantic → Daft type conversion  
✅ **Closure Safety**: All closures properly capture variables  
✅ **Parameter Safety**: Only relevant constants passed to each node  
✅ **Mode Flexibility**: All execution modes (local, daft, auto) work correctly  
✅ **Clean Code**: Removed debug logging, kept only warnings

## Testing Results

All execution modes verified working:
- ✅ Single item execution
- ✅ Multi-item with Daft batching
- ✅ Multi-item with local looping
- ✅ Auto mode with threshold logic
- ✅ Pydantic models properly serialized/deserialized

## Architecture Patterns Used

Following the daft-pdf-example.md pattern:
1. Convert Pydantic types to PyArrow schemas
2. Use explicit `return_dtype` in batch UDFs
3. Call `.model_dump()` on Pydantic objects before returning
4. Validate with `.model_validate()` when deserializing

