# Cache Fix: Stateful Objects with No Public Attributes

## Problem

When caching functions that operate on stateful objects with no public attributes, different instances were incorrectly treated as identical, causing cache collisions. This led to functions skipping execution on cache hits, even when operating on different object instances.

### Example Issue

```python
class ToyRetriever:
    def __init__(self):
        pass  # No public attributes
    
    def index(self, corpus):
        self._doc_tokens = corpus  # Sets private attribute

@func(output="index", cache=True)
def index_func(retriever: ToyRetriever, corpus: dict) -> bool:
    retriever.index(corpus)
    return True

# Run 1: Works fine
retriever1 = ToyRetriever()
result1 = runner.run(inputs={"retriever": retriever1, "corpus": data})

# Run 2: FAILS - cache hit skips index(), retriever2._doc_tokens not set
retriever2 = ToyRetriever()  # Different instance!
result2 = runner.run(inputs={"retriever": retriever2, "corpus": data})
# AttributeError: 'ToyRetriever' object has no attribute '_doc_tokens'
```

## Root Cause

In `compute_inputs_hash()`, Strategy 6 handles objects with `__dict__` by serializing only public attributes (not starting with `_`). This design intentionally excludes private/mutable state to prevent cache invalidation from internal state changes.

However, when an object has **only** private attributes (no public ones), the serialization becomes:
```python
{"__class__": "ToyRetriever", "__state__": {}}
```

Two different instances of such objects produce identical hashes:
```python
hash(retriever1) == hash(retriever2)  # True, even though id(retriever1) != id(retriever2)
```

This causes the cache to treat them as the same object, leading to incorrect cache hits.

## Solution

Include the object ID when there are no public attributes to hash:

```python
# Strategy 6: Objects with __dict__ (filter private/mutable attributes)
elif hasattr(obj, "__dict__"):
    state = {}
    for k, v in obj.__dict__.items():
        if not k.startswith("_") and not callable(v):
            try:
                state[k] = _serialize(v)
            except (TypeError, ValueError):
                state[k] = str(v)
    
    # NEW: If no public attributes, include object id to distinguish instances
    if not state:
        return {"__class__": obj.__class__.__name__, "__id__": id(obj)}
    
    return {"__class__": obj.__class__.__name__, "__state__": state}
```

## Impact

### Before Fix
- Different instances with no public attributes → **Same hash** → Incorrect cache hits
- Functions skip execution, side effects don't occur
- Runtime errors when subsequent functions depend on those side effects

### After Fix  
- Different instances with no public attributes → **Different hashes** → Cache misses
- Functions execute normally for each instance
- Each instance gets properly initialized

## Trade-offs

**Pros:**
- ✅ Fixes cache collisions for stateful objects
- ✅ Prevents mysterious AttributeErrors from skipped initialization
- ✅ More intuitive behavior: different instances → different cache entries

**Cons:**
- ⚠️ Less cache reuse for "equivalent" objects without public attributes
- ⚠️ Object ID changes across Python sessions (not an issue with DiskCache across sessions anyway)

## Best Practices

To maximize cache effectiveness with stateful objects, consider these options:

### Option 1: Add Public Attributes
```python
class ToyRetriever:
    def __init__(self, name: str = "default"):
        self.name = name  # Public attribute for cache key
        self._doc_tokens = None
```
Now caching works on `name`, and instances with the same name share cache.

### Option 2: Implement `__cache_key__()` Protocol
```python
class ToyRetriever:
    def __init__(self, config_id: str = "default"):
        self._config_id = config_id
        self._doc_tokens = None
    
    def __cache_key__(self):
        return f"ToyRetriever:{self._config_id}"
```
Full control over cache key generation.

### Option 3: Don't Cache Side-Effect Functions
```python
@func(output="index", cache=False)  # Don't cache initialization
def index_func(retriever: ToyRetriever, corpus: dict) -> bool:
    retriever.index(corpus)
    return True
```
Let side effects always execute.

### Option 4: Reuse Same Instance
```python
# Reuse the same retriever instance across runs
retriever = ToyRetriever()
result1 = runner.run(inputs={"retriever": retriever, ...})
result2 = runner.run(inputs={"retriever": retriever, ...})  # Same instance
```
Cache based on the same object ID.

## Testing

Added comprehensive test coverage in `tests/test_cache_stateful_side_effects.py`:

- `test_stateful_side_effects_different_instances()` - Verifies different instances get cache misses
- `test_stateful_side_effects_same_instance()` - Verifies same instance works correctly

## Migration

No changes needed! The fix is backward compatible:
- Objects with public attributes continue to hash based on those attributes
- Objects without public attributes now include object ID (preventing collisions)
- All existing tests pass

## Related Issues

This fix resolves issues like:
- "AttributeError: object has no attribute '_xxx'" in cached workflows
- Mysterious cache hits when using fresh object instances
- Functions not executing when they should

If you encounter caching issues with stateful objects, review the best practices above.

