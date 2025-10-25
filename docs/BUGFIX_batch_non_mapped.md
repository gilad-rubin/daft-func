# Bug Fix: AttributeError in Batch Mode with Non-Mapped Functions

## Issue
When running a pipeline in batch mode (`mode="daft"`) that contained both non-mapped functions (no `map_axis`) and mapped functions, an `AttributeError` occurred:

```python
AttributeError: 'list' object has no attribute 'alias'
```

## Root Cause
The batch execution code in `runner.py` was attempting to process ALL nodes as batch operations, even those without a `map_axis`. When a batch UDF decorated with `@daft.func.batch` is called with no series arguments (empty `call_series`), it executes immediately and returns a plain Python list instead of a Daft Expression. This caused the subsequent `.alias()` call to fail.

## Example That Failed
```python
@func(output="index")
def index(corpus: Dict[str, str]) -> bool:
    """Non-mapped function - should run once."""
    return True

@func(output="hits", map_axis="query", key_attr="query_uuid")
def retrieve(query: Query, top_k: int, index: bool) -> RetrievalResult:
    """Mapped function - runs per query."""
    return retriever.retrieve(query, top_k=top_k)

# This would fail in batch mode
runner = Runner(
        mode="daft")
result = runner.run(pipeline, inputs=multi_inputs)  # AttributeError!
```

## Solution
Modified `_run_batch()` in `runner.py` to use a two-pass approach:

1. **First pass**: Execute non-mapped functions (those without `map_axis`) once using plain Python and add their results to the constants dictionary
2. **Second pass**: Process only mapped functions using Daft batch operations

This ensures:
- Non-mapped functions run exactly once (not once per batch item)
- Only mapped functions are processed as batch UDFs with series arguments
- Batch UDFs always receive at least one series argument, preventing immediate execution

## Changes
- `src/daft_func/runner.py`: Added two-pass processing in `_run_batch()`
- `tests/test_runner.py`: Added regression test `test_runner_non_mapped_with_mapped_daft()`

## Testing
The fix was verified with:
1. Minimal reproduction test case
2. All existing tests continue to pass
3. New regression test added to prevent future occurrences

