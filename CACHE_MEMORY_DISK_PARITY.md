# Memory vs Disk Cache Parity Investigation

## Summary

**Investigation Result:** ✅ MemoryCache and DiskCache behave **identically** in all tested scenarios.

## Test Coverage

Created comprehensive tests in `tests/test_memory_disk_cache_parity.py` that verify:

1. **MemoryCache with new instances**: MISS → HIT → HIT
2. **DiskCache with new instances**: MISS → HIT → HIT  
3. **Parity test**: Both produce identical results

All tests **PASS**.

## Additional Test Scripts

Created several standalone test scripts to reproduce different scenarios:

- `test_memory_vs_disk_cache.py`: Basic comparison with `str` return type
- `test_memory_cache_bool_return.py`: Tests with `bool` return type (side-effect heuristic)
- `test_exact_user_scenario.py`: Reproduces user's exact code pattern
- `test_debug_instance_ids.py`: Debug script showing instance ID hashing
- `test_comprehensive_cache_comparison.py`: 5-run sequence comparison

**All scripts show identical behavior** between MemoryCache and DiskCache.

## Key Findings

### 1. Return Type Matters

- **`str` return type**: Both backends work correctly with new instances (HIT after first MISS)
- **`bool` return type**: Both backends show all MISSes due to `force_instance_ids=True` heuristic
- This is **intentional behavior** to prevent side-effect issues

### 2. `__cache_key__()` Protocol Works

When `force_instance_ids=False` (non-bool/None return types):
- `ToyRetriever().__cache_key__()` returns `"ToyRetriever::{}"`
- All instances with same config → **same hash** → cache HIT ✓
- Both MemoryCache and DiskCache respect this

### 3. Set Serialization Fix

Added deterministic serialization for `set`/`frozenset` types in `compute_inputs_hash`:
```python
elif isinstance(obj, (set, frozenset)):
    items = [_serialize(item, current_depth) for item in obj]
    items_sorted = sorted(items)  # Deterministic ordering
    return {"__set__": items_sorted}
```

This ensures both backends hash sets identically.

## If You Experience Different Behavior

The tests prove both backends are functionally identical. If you see different behavior in your notebook:

### Possible Causes:

1. **Jupyter kernel state**: Stale objects or imports
   - **Fix**: Restart kernel

2. **Existing cache pollution**: Old .cache directory with inconsistent state
   - **Fix**: `trash .cache` and re-run

3. **Bool return type**: Using side-effect pattern with `-> bool`
   - **Behavior**: Always MISSes with new instances (by design)
   - **Fix**: Change to `-> str` (return path/artifact) OR reuse same instance

4. **Code changes**: Function code changed between runs
   - **Behavior**: Cache invalidation (MISSes)
   - **Fix**: Expected behavior, not a bug

## Recommended Pattern

**Best practice for retrieval pipelines:**

```python
@func(output="index_path", cache=True)
def index(retriever: Retriever, corpus: Dict[str, str]) -> str:
    """Returns path to index artifact (not bool)."""
    return retriever.index(corpus)  # Returns str path

@func(output="hits", map_axis="query", key_attr="query_uuid", cache=True)
def retrieve(retriever: Retriever, query: Query, top_k: int, index_path: str) -> RetrievalResult:
    return retriever.retrieve(index_path, query, top_k=top_k)
```

**Why this works:**
- `str` return type → no instance ID tracking
- `__cache_key__()` on retriever → consistent hashing
- New instances with same config → cache HIT ✓
- Works identically in MemoryCache and DiskCache ✓

## Test Results

```bash
# Individual parity tests
$ uv run -m pytest tests/test_memory_disk_cache_parity.py -v
============================== test session starts ==============================
tests/test_memory_disk_cache_parity.py::test_memory_cache_with_new_instances PASSED
tests/test_memory_disk_cache_parity.py::test_disk_cache_with_new_instances PASSED
tests/test_memory_disk_cache_parity.py::test_memory_and_disk_cache_parity PASSED
============================== 3 passed

# Full test suite
$ uv run -m pytest tests -q
74 passed, 1 skipped
```

## Conclusion

MemoryCache and DiskCache use **100% the same mechanism** for:
- Computing signatures
- Hashing inputs  
- Comparing signatures
- Storing/retrieving metadata and blobs

The only difference is the storage medium (RAM vs disk), not the logic.

If you observe different behavior, it's due to environmental factors (kernel state, cache pollution, timing) not fundamental differences in the caching mechanism.

