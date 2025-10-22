# Caching with Batch Operations

## Problem Solved

Previously, caching only worked with single-item execution (`mode="local"` with single inputs). When processing batches (multiple items with `map_axis`), caching had two critical issues:

1. **Batch/Local Mode**: All items in a batch overwrote each other's cache entries
2. **Daft Mode**: No cache integration at all

## Solution: Per-Item Caching

We implemented **per-item caching** for `map_axis` nodes that includes the item's unique identifier in the cache key.

### How It Works

For nodes with `map_axis`, the cache key includes the item's identifier:

```python
# Non-map_axis node
cache_key = "index"  # Just the node name

# map_axis node
cache_key = "hits::q1"  # node_name::item_id
cache_key = "hits::q2"  # Different item, different cache entry
```

The item identifier is extracted from the `key_attr` parameter:

```python
@func(output="hits", map_axis="query", key_attr="query_uuid", cache=True)
def retrieve(retriever: Retriever, query: Query, top_k: int) -> Result:
    return retriever.retrieve(query, top_k=top_k)

# When processing Query(query_uuid="q1", ...):
# Cache key: "hits::q1"

# When processing Query(query_uuid="q2", ...):
# Cache key: "hits::q2"
```

## Example: Retrieval Pipeline

### Single Query (works before and after)

```python
runner = Runner(pipeline, cache_config=CacheConfig(enabled=True))

# Run 1: Cold cache
result1 = runner.run(inputs={"query": Query(query_uuid="q1", text="test"), ...})
# [CACHE] index: ✗ MISS | hits: ✗ MISS | reranked_hits: ✗ MISS

# Run 2: Warm cache  
result2 = runner.run(inputs={"query": Query(query_uuid="q1", text="test"), ...})
# [CACHE] index: ✓ HIT | hits: ✓ HIT | reranked_hits: ✓ HIT
```

### Multiple Queries (NOW WORKS!)

```python
queries = [
    Query(query_uuid="q1", text="quick brown"),
    Query(query_uuid="q2", text="wizards jump"),
    Query(query_uuid="q3", text="brown dog"),
]

# Run 1: Cold cache - all execute
result1 = runner.run(inputs={"query": queries, ...})
# [CACHE] index: ✗ MISS | hits: ✗ MISS | reranked_hits: ✗ MISS  (q1)
#         index: ✓ HIT  | hits: ✗ MISS | reranked_hits: ✗ MISS  (q2)
#         index: ✓ HIT  | hits: ✗ MISS | reranked_hits: ✗ MISS  (q3)

# Run 2: Warm cache - all cached!
result2 = runner.run(inputs={"query": queries, ...})
# [CACHE] index: ✓ HIT | hits: ✓ HIT | reranked_hits: ✓ HIT  (q1)
#         index: ✓ HIT | hits: ✓ HIT | reranked_hits: ✓ HIT  (q2)
#         index: ✓ HIT | hits: ✓ HIT | reranked_hits: ✓ HIT  (q3)

# Run 3: Mix of cached and new queries
mixed_queries = [
    Query(query_uuid="q1", text="quick brown"),  # Cached
    Query(query_uuid="q4", text="new query"),    # New!
    Query(query_uuid="q2", text="wizards jump"), # Cached
]
result3 = runner.run(inputs={"query": mixed_queries, ...})
# [CACHE] index: ✓ HIT | hits: ✓ HIT  | reranked_hits: ✓ HIT   (q1 - cached!)
#         index: ✓ HIT | hits: ✗ MISS | reranked_hits: ✗ MISS  (q4 - new!)
#         index: ✓ HIT | hits: ✓ HIT  | reranked_hits: ✓ HIT   (q2 - cached!)
```

## Cache Key Extraction

The system uses the following strategy to extract item identifiers:

1. **If `key_attr` is specified**: Use that attribute (e.g., `query_uuid`)
2. **For Pydantic models**: Try common ID fields (`id`, `uuid`, `key`, `name`)
3. **Fallback**: Use hash of the item's string representation

```python
def get_item_cache_key(item: Any, key_attr: Optional[str] = None) -> str:
    if key_attr and hasattr(item, key_attr):
        return str(getattr(item, key_attr))
    elif isinstance(item, BaseModel):
        for attr in ['id', 'uuid', 'key', 'name']:
            if hasattr(item, attr):
                return str(getattr(item, attr))
    return str(hash(str(item)))
```

## Configuration

Per-item caching is **enabled by default** but can be controlled:

```python
# Enable per-item caching (default)
CacheConfig(enabled=True, per_item_caching=True)

# Disable per-item caching (not recommended for map_axis nodes)
CacheConfig(enabled=True, per_item_caching=False)
```

## Execution Modes

Per-item caching works across all execution modes:

### Local Mode (Python loop)
```python
Runner(pipeline, mode="local", cache_config=cache_config)
```
- ✅ Per-item caching fully implemented
- ✅ Cache logging works
- ✅ Smart invalidation per item

### Daft Mode (DataFrame execution)
```python
Runner(pipeline, mode="daft", cache_config=cache_config)
```
- ⚠️ Cache integration pending (orthogonal design needed)
- Currently: No cache operations in Daft mode
- Future: Cache entire batch results or integrate per-item caching

### Auto Mode
```python
Runner(pipeline, mode="auto", batch_threshold=10)
```
- Uses `local` mode for small batches (caching works!)
- Uses `daft` mode for large batches (caching pending)

## Implementation Details

### Cache Storage

Per-item cache entries are stored separately:

```
.cache/
├── metadata.json
│   ├── "index": {...}           # Single entry for non-map_axis node
│   ├── "hits::q1": {...}        # Separate entry per query
│   ├── "hits::q2": {...}
│   └── "reranked_hits::q1": {...}
└── blobs/
    ├── index                    # Single blob
    ├── hits::q1                 # Separate blob per query
    ├── hits::q2
    └── reranked_hits::q1
```

### Dependency Tracking

Parent signatures are tracked per-node, not per-item:

```python
# When processing q2's retrieve node:
parent_sigs = {
    "index": "abc123..."  # Shared signature from indexing
}
# This ensures that if index changes, all queries invalidate
```

## Best Practices

### 1. Always Specify `key_attr` for Map-Axis Nodes

```python
@func(output="results", map_axis="query", key_attr="query_uuid", cache=True)
def process(query: Query) -> Result:
    ...
```

### 2. Use Consistent Item Identifiers

```python
# Good: Stable UUIDs
Query(query_uuid="q1", text="hello")
Query(query_uuid="q1", text="hello")  # Same ID -> cache hit

# Bad: Auto-generated IDs
Query(query_uuid=str(uuid.uuid4()), text="hello")  # Always misses
```

### 3. Reuse Object Instances

```python
# Good: Reuse retriever instance
retriever = ToyRetriever()
runner.run(inputs={"retriever": retriever, ...})  # Run 1
runner.run(inputs={"retriever": retriever, ...})  # Run 2 - cache hits!

# Problematic: New instances each time
runner.run(inputs={"retriever": ToyRetriever(), ...})  # Run 1
runner.run(inputs={"retriever": ToyRetriever(), ...})  # Run 2
# May or may not cache hit depending on object state
```

### 4. Understand Side Effects

Caching returns stored results without executing the function:

```python
@func(output="indexed", cache=True)
def index(retriever: Retriever, corpus: dict) -> bool:
    retriever.index(corpus)  # Side effect: mutates retriever
    return True

# First run: executes, mutates retriever
runner.run(inputs={"retriever": retriever, "corpus": corpus})

# Second run: returns True from cache, DOESN'T mutate new retriever!
new_retriever = ToyRetriever()  
runner.run(inputs={"retriever": new_retriever, "corpus": corpus})
# new_retriever is NOT indexed! Use same instance instead.
```

## Troubleshooting

### Issue: Cache Always Misses for Different Items

**Symptom**: Each item in a batch always shows MISS

**Cause**: `key_attr` not set or item doesn't have that attribute

**Solution**: 
```python
# Ensure key_attr is specified and exists on items
@func(output="results", map_axis="item", key_attr="id", cache=True)
```

### Issue: All Items Share Same Cache Entry

**Symptom**: Only first item executes, rest use its cached result

**Cause**: `per_item_caching=False` in CacheConfig

**Solution**:
```python
CacheConfig(enabled=True, per_item_caching=True)  # Default
```

### Issue: Unexpected Cache Misses After Code Changes

**Symptom**: Cache doesn't work after editing unrelated code

**Cause**: Dependency tracking detected import changes

**Solution**: This is correct behavior! Cache invalidates when dependencies change.

## Future Enhancements

### Daft Mode Integration

Currently pending: Integrate caching with Daft batch execution. Two approaches being considered:

1. **Batch-Level Caching**: Cache entire batch results
   - Pro: Simple, works with Daft's DataFrame model
   - Con: Can't reuse cached items across different batch compositions

2. **Per-Item Caching in Daft**: Extract and cache individual items
   - Pro: Maximum cache reuse, consistent with local mode
   - Con: More complex, requires DataFrame manipulation

### Lazy Output Loading

For nodes whose outputs aren't needed downstream:

```python
# Future: Don't load cached output if not used
cache_hit = True
loaded = False  # Lazy - not actually loaded yet
# [CACHE] foo: ○ HIT (skipped)
```

### Cache Statistics

```python
runner.cache_stats.summary()
# Cache Statistics:
# - Total nodes: 10
# - Cache hits: 7 (70%)
# - Cache misses: 3 (30%)
# - Time saved: 15.3s
```

## Examples

See:
- `examples/test_retrieval_cache.py` - Single-item caching
- `examples/retrieval_batch_cache_demo.py` - Batch caching with per-item support
- `tests/test_cache_batch_mode.py` - Per-item caching tests

## Related Documentation

- [Caching Usage Guide](caching-usage.md) - Complete caching guide
- [Caching Design](caching.md) - Technical design details

