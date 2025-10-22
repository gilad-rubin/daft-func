# Conversation Summary: Advanced Caching for `pipefunc`

This conversation details the design of an advanced, dependency-aware incremental caching system tailored for your `pipefunc` pipeline. The primary goal was to overcome the limitations of `pipefunc`'s default caching (which is based only on root inputs) and implement a more granular, DAG-aware strategy.

### The Core Problem

Your pipeline (e.g., for `pylate` indexing) involves expensive, sequential steps like `load_corpus` -> `encode_corpus` -> `build_index`. You noted two major issues:

1. **`pipefunc`'s Default Cache:** The built-in caching mechanism bases its keys *only on the root inputs* (`root_args`) of the *entire pipeline*. It doesn't intelligently track changes in intermediate dependencies.
2. **Expensive Outputs:** Hashing the *outputs* of nodes (e.g., a multi-gigabyte embedding matrix or a complete index) to check for changes is computationally prohibitive and undesirable.

### Our Solution: Dependency-Aware Signatures (No Output Hashing)

We designed a more intelligent caching system based on a key assumption: **"Same code + same inputs + same parent signatures = same output."**

Instead of hashing a node's *output*, we compute a lightweight **computation signature** for each node. This signature acts as a "fingerprint" of the *work* that was done, not the *result* of that work.

The signature is a hash of several components:
`sig(N) = H(code_hash + env_hash + inputs_hash + deps_hash)`

- **`code_hash`:** A hash of the function's source code.
- **`env_hash`:** A hash of external factors that affect the result (e.g., a model name, library versions, or a manually-bumped "salt" string).
- **`inputs_hash`:** A hash of the node's *direct* input values (e.g., simple parameters).
- **`deps_hash`:** A hash of the **signatures** of all its direct parent nodes.

### Execution Logic & Key Scenarios

We established that this system would be managed by a `MetaStore` (to save the signatures) and a `BlobStore` (to save the actual outputs, like your existing `DiskCache`).

When the pipeline runs, it checks each node in topological order:

1. It computes the node's *new signature* based on its current code, inputs, and the *actual signatures* of its parents from the last run (retrieved from the `MetaStore`).
2. **Cache Hit:** If this `new_signature` matches the `stored_signature` in the `MetaStore`, the node is skipped. Its output is loaded from the `BlobStore` *only if* another node actually needs it (lazy materialization).
3. **Cache Miss:** If the signatures don't match, the node is re-executed, its output is saved to the `BlobStore`, and its `new_signature` is updated in the `MetaStore`.

This design explicitly handles your key scenarios:

- **Scenario 1 (Downstream Change):**
    - **DAG:** `(a,b) -> foo -> bar(foo_out, c)`
    - **Change:** Only `c` changes.
    - **Result:** `sig(foo)` is a **hit**. `sig(bar)` is a **miss** (because its `inputs_hash` changed). `bar` is recomputed, but it uses the *cached output* of `foo`. `foo` is *not* re-run.
- **Scenario 2 (Full Hit & Lazy Loading):**
    - **Change:** `a, b, c` are all unchanged.
    - **Result:** `sig(bar)` is a **hit**. The cached `bar_output` is returned *without* recomputing `bar` and, crucially, *without even loading `foo_out` from the cache* (unless another node also needs it).
- **Scenario 3 (Upstream Change):**
    - **Change:** `a` changes.
    - **Result:** `sig(foo)` is a **miss**. `foo` is recomputed. This generates a new `sig(foo)`. Because `bar`'s `deps_hash` (which depends on `sig(foo)`) is now different, `sig(bar)` is also a **miss**, and `bar` is recomputed.

### Final Steps

To validate this design, we concluded by creating a **test suite** with 5 specific `pytest` examples (e.g., `test_downstream_only_change`, `test_full_cache_hit`, `test_upstream_change`) and provided **guidance for an LLM** on how to surgically integrate this logic into the existing `pipefunc` codebase.

---

## Object Serialization for Cache Keys

### Problem

When using custom objects (like retrievers, models, or other stateful components) as function inputs, the caching system needs to determine whether two object instances should be treated as equivalent for caching purposes.

Previously, the system included `id(obj)` in the hash, which meant:
- Run 1: New `ToyRetriever()` instance → MISS ✓
- Run 2: Same instance in memory → HIT ✓
- Run 3: New `ToyRetriever()` instance → MISS ✗ (should be HIT!)

### Solution: Smart Object Serialization

The caching system now uses a multi-strategy approach to serialize objects for hashing:

#### Strategy 1: Custom Cache Keys via `__cache_key__()`

Classes can define their own cache key method:

```python
class ToyRetriever:
    def __init__(self, config: dict = {}):
        self.config = config
    
    def __cache_key__(self):
        """Return deterministic cache key based on configuration."""
        import json
        return f"{self.__class__.__name__}::{json.dumps(self.config, sort_keys=True)}"
```

**When to use:** Best for objects where you want explicit control over what makes them equivalent for caching. Recommended for complex stateful objects.

#### Strategy 2: Automatic Deep Serialization

If `__cache_key__()` is not defined, the system automatically serializes public attributes (non-`_` prefixed) up to a configurable depth:

```python
class SimpleRetriever:
    def __init__(self, model_name: str, top_k: int = 10):
        self.model_name = model_name
        self.top_k = top_k
        self._internal_state = {}  # Excluded from hash (private)

# Two instances with same config will share cache:
r1 = SimpleRetriever("bert-base", top_k=10)
r2 = SimpleRetriever("bert-base", top_k=10)
# r1 and r2 will produce same cache hash!
```

**When to use:** Good for simple objects where public attributes fully determine behavior. No need to write custom serialization code.

### Configuring Serialization Depth

Control how deep the serialization goes with `serialization_depth`:

```python
cache_config = CacheConfig(
    enabled=True,
    backend=DiskCache(cache_dir=".cache"),
    serialization_depth=2,  # Default: 2 levels deep
)
```

- **Depth 0:** Objects immediately use `id()`, different instances always differ
- **Depth 1:** Serialize object's direct attributes, nested objects use `id()`
- **Depth 2:** Serialize object and its nested objects (default)
- **Depth 3+:** Continue deeper for highly nested structures

### Complete Example

```python
from typing import Dict, List
from daft_func import Pipeline, Runner, func, CacheConfig, DiskCache

# Option 1: Define custom cache key
class ConfigurableRetriever:
    def __init__(self, model: str, settings: dict):
        self.model = model
        self.settings = settings
    
    def __cache_key__(self):
        import json
        return f"Retriever::{self.model}::{json.dumps(self.settings, sort_keys=True)}"

# Option 2: Rely on automatic serialization
class SimpleReranker:
    def __init__(self, threshold: float = 0.5):
        self.threshold = threshold
        # No __cache_key__ needed - automatic serialization works!

@func(output="results", cache=True)
def retrieve_and_rerank(
    retriever: ConfigurableRetriever, 
    reranker: SimpleReranker, 
    query: str
) -> List[str]:
    # ... implementation
    pass

# Create pipeline
pipeline = Pipeline(functions=[retrieve_and_rerank])
runner = Runner(
    pipeline=pipeline,
    cache_config=CacheConfig(
        enabled=True,
        backend=DiskCache(cache_dir=".cache"),
        serialization_depth=2,  # Adjust as needed
    )
)

# First run: cache miss
result1 = runner.run(inputs={
    "retriever": ConfigurableRetriever("bert", {"top_k": 10}),
    "reranker": SimpleReranker(threshold=0.5),
    "query": "test query"
})

# Second run with NEW instances but SAME config: cache hit!
result2 = runner.run(inputs={
    "retriever": ConfigurableRetriever("bert", {"top_k": 10}),  # New instance
    "reranker": SimpleReranker(threshold=0.5),  # New instance
    "query": "test query"
})
# ✓ Cache HIT - same configuration = same cache key
```

### Best Practices

1. **For stateful objects with complex initialization:** Implement `__cache_key__()`
2. **For simple configuration objects:** Rely on automatic serialization
3. **For stateless singletons:** Return just the class name:
   ```python
   def __cache_key__(self):
       return self.__class__.__name__
   ```
4. **Exclude private state:** Private attributes (`_attr`) are automatically excluded from serialization
5. **Adjust depth as needed:** If you have deeply nested configurations, increase `serialization_depth`

### Important Edge Cases

**Objects with only private attributes:** If an object has NO public attributes (all attributes start with `_`), the system falls back to using object ID. This is a safety feature for objects that rely on side effects:

```python
class StatefulProcessor:
    def __init__(self):
        self._initialized = False  # Private attribute
        self._data = None           # Private attribute
    
    def initialize(self, data):
        self._initialized = True
        self._data = data

# Different instances will hash differently (uses object ID)
# This prevents cache collisions for objects with side effects
processor1 = StatefulProcessor()
processor2 = StatefulProcessor()
# These will NOT share cache (correctly, since they need separate initialization)
```

If you want such objects to share cache, add a `__cache_key__()` method or expose configuration as public attributes.

### Testing Object Serialization

Run the comprehensive test suite to verify serialization behavior:

```bash
uv run pytest tests/test_cache_object_serialization.py -v
```

This tests all scenarios:
- Custom `__cache_key__()` methods
- Automatic serialization
- Depth limits
- Nested objects
- Mixed object types