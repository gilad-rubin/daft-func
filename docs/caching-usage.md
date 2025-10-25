# Caching Usage Guide

The caching system in daft-func provides dependency-aware incremental caching that intelligently invalidates cache based on code changes, input changes, and dependency changes.

## Quick Start

Enable caching with minimal configuration:

```python
from daft_func import func, Pipeline, Runner, CacheConfig

# Define nodes with caching enabled
@func(output="processed", cache=True)
def process_data(data: str) -> str:
    # Expensive computation
    return data.upper()

# Create pipeline and runner with caching
pipeline = Pipeline(functions=[process_data])
cache_config = CacheConfig(enabled=True, cache_dir=".cache")
runner = Runner(
        cache_config=cache_config)

# First run: executes and caches
result1 = runner.run(pipeline, inputs={"data": "hello"})

# Second run with same input: uses cache
result2 = runner.run(pipeline, inputs={"data": "hello"})  # Instant!
```

## Core Concepts

### Computation Signatures

The caching system works by computing a signature for each node:

```
signature = hash(code_hash + env_hash + inputs_hash + deps_hash)
```

- **code_hash**: Hash of the function's source code
- **env_hash**: Manual version/environment identifier
- **inputs_hash**: Hash of the direct input parameters
- **deps_hash**: Hash of parent node signatures + dependency modules

### Cache Hit Logic

When executing a node:
1. Compute new signature using current code + inputs + parent signatures
2. Compare with stored signature from previous run
3. **Cache hit**: Load output from cache, skip execution
4. **Cache miss**: Execute node, save output and signature

### Smart Invalidation

The system automatically invalidates cache when:
- Function code changes
- Input parameters change
- Upstream dependencies change
- Project dependencies change (up to configured depth)
- Manual cache key changes

## Configuration

### CacheConfig Options

```python
from daft_func import CacheConfig

cache_config = CacheConfig(
    enabled=True,           # Enable/disable caching
    backend="diskcache",    # Backend: "diskcache" or "cachier"
    cache_dir=".cache",     # Cache storage directory
    env_hash=None,          # Optional global environment hash
    dependency_depth=2,     # Levels of imports to track
)
```

### Node-Level Configuration

Enable caching per node with fine-grained control:

```python
@func(
    output="result",
    cache=True,              # Enable caching for this node
    cache_key="v2",          # Optional version override
    cache_backend="diskcache" # Optional backend override
)
def compute(x: int) -> int:
    return x * 2
```

## Storage Backends

### DiskCache (Default)

Fast, pure-Python disk-backed cache using SQLite:

```python
cache_config = CacheConfig(
    enabled=True,
    backend="diskcache",
    cache_dir=".cache"
)
```

**Pros:**
- Fast read/write operations
- No external dependencies
- Automatic eviction policies
- Thread-safe

**Best for:**
- Local development
- Single-machine deployments
- Large objects that pickle well

### Cachier

Pickle-based caching with support for cross-machine caching:

```python
cache_config = CacheConfig(
    enabled=True,
    backend="cachier",
    cache_dir=".cache"
)
```

**Pros:**
- Simple pickle-based storage
- Extensible to MongoDB for cross-machine caching
- Familiar Python serialization

**Best for:**
- Simple use cases
- When you need custom serialization
- Prototyping

## Cache Invalidation Strategies

### Automatic Invalidation

The system automatically detects:

1. **Code Changes**
```python
@func(output="result", cache=True)
def compute(x: int) -> int:
    return x * 2  # Change to x * 3 -> cache invalidated
```

2. **Input Changes**
```python
result1 = runner.run(pipeline, inputs={"x": 5})   # Executes
result2 = runner.run(pipeline, inputs={"x": 10})  # Executes (different input)
result3 = runner.run(pipeline, inputs={"x": 5})   # Uses cache from result1
```

3. **Dependency Changes**
```python
# DAG: (a,b) -> foo -> bar(foo_out, c)
result = runner.run(pipeline, inputs={"a": 1, "b": 2, "c": 3})

# Change upstream: a or b -> foo and bar re-execute
# Change downstream: c -> only bar re-executes (foo uses cache)
```

### Manual Invalidation

Use cache keys for manual version control:

```python
# Version 1
@func(output="result", cache=True, cache_key="model_v1")
def process(data: str) -> str:
    return model_v1.predict(data)

# Version 2 (forces cache miss)
@func(output="result", cache=True, cache_key="model_v2")
def process(data: str) -> str:
    return model_v2.predict(data)
```

## Advanced Examples

### Multi-Stage Pipeline with Selective Caching

```python
from daft_func import func, Pipeline, Runner, CacheConfig

@func(output="corpus", cache=True, cache_key="corpus_v1")
def load_corpus(corpus_path: str) -> list:
    """Expensive: load large corpus from disk."""
    return load_from_disk(corpus_path)

@func(output="embeddings", cache=True, cache_key="model_v2")
def encode_corpus(corpus: list, model_name: str) -> np.ndarray:
    """Very expensive: encode corpus with LLM."""
    model = load_model(model_name)
    return model.encode(corpus)

@func(output="index", cache=True)
def build_index(embeddings: np.ndarray, index_type: str):
    """Expensive: build search index."""
    return create_index(embeddings, index_type)

# Create pipeline
pipeline = Pipeline(functions=[load_corpus, encode_corpus, build_index])
cache_config = CacheConfig(enabled=True, cache_dir=".cache/retrieval")
runner = Runner(
        cache_config=cache_config)

# First run: everything executes
result1 = runner.run(pipeline, inputs={
    "corpus_path": "data/corpus.json",
    "model_name": "sentence-transformers/all-MiniLM-L6-v2",
    "index_type": "faiss"
})

# Change only index type: only build_index re-executes
result2 = runner.run(pipeline, inputs={
    "corpus_path": "data/corpus.json",
    "model_name": "sentence-transformers/all-MiniLM-L6-v2",
    "index_type": "annoy"  # Different index type
})
# load_corpus and encode_corpus use cache!
```

### Cache with Complex Types

```python
from pydantic import BaseModel

class Document(BaseModel):
    id: str
    text: str
    metadata: dict

class ProcessedDoc(BaseModel):
    id: str
    embeddings: list[float]
    tokens: list[str]

@func(output="processed", cache=True)
def process_document(doc: Document, config: dict) -> ProcessedDoc:
    """Process a single document."""
    tokens = tokenize(doc.text)
    embeddings = compute_embeddings(tokens)
    return ProcessedDoc(
        id=doc.id,
        embeddings=embeddings,
        tokens=tokens
    )
```

### Mixing Cached and Non-Cached Nodes

```python
@func(output="data", cache=True)
def load_data(path: str) -> pd.DataFrame:
    """Cache this: expensive I/O."""
    return pd.read_parquet(path)

@func(output="filtered", cache=False)
def filter_data(data: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """Don't cache: fast operation, threshold changes often."""
    return data[data['score'] > threshold]

@func(output="result", cache=True)
def compute_result(filtered: pd.DataFrame) -> dict:
    """Cache this: expensive aggregation."""
    return expensive_aggregation(filtered)
```

## Best Practices

### 1. Cache Expensive Operations

Enable caching for nodes that are:
- I/O intensive (loading large files)
- Computationally expensive (ML inference, encoding)
- Time-consuming (API calls, database queries)

### 2. Use Cache Keys for Model Versions

```python
@func(output="predictions", cache=True, cache_key="model_v3")
def predict(data: list, model_path: str):
    return model.predict(data)
```

Update the cache key when:
- Model architecture changes
- Training data changes
- Hyperparameters change

### 3. Set Appropriate Dependency Depth

```python
cache_config = CacheConfig(
    enabled=True,
    dependency_depth=2  # Track 2 levels of imports
)
```

- Increase for projects with deep import hierarchies
- Decrease for faster signature computation
- Default (2) works well for most cases

### 4. Organize Cache by Project

```python
# Separate cache for each project/experiment
cache_config = CacheConfig(
    enabled=True,
    cache_dir=f".cache/{project_name}"
)
```

### 5. Clean Cache Manually

```bash
# Remove cache when needed
rm -rf .cache/
# or
trash .cache/
```

Future versions will support automatic cleanup policies.

## Cache Storage Structure

The cache directory contains:

```
.cache/
├── metadata.json          # Node signatures
└── blobs/                 # Cached outputs
    ├── <node1_output>
    ├── <node2_output>
    └── ...
```

For cachier backend:
```
.cache/
├── metadata.json
└── cachier_blobs/
    ├── <node1>.pkl
    ├── <node2>.pkl
    └── ...
```

## Troubleshooting

### Cache Not Working

**Problem**: Node executes every time despite cache enabled.

**Solutions**:
1. Verify cache is enabled: `cache_config.enabled = True`
2. Check node has `cache=True` in decorator
3. Ensure inputs are hashable (use Pydantic models for complex types)
4. Check for code changes (even whitespace counts)

### Cache Size Growing Too Large

**Problem**: `.cache/` directory becomes very large.

**Solutions**:
1. Delete cache manually: `trash .cache/`
2. Use separate cache directories per experiment
3. Cache only the most expensive operations
4. Consider using cachier with MongoDB for shared caching

### Dependency Changes Not Detected

**Problem**: Cache not invalidated when imports change.

**Solutions**:
1. Increase `dependency_depth` in CacheConfig
2. Use manual cache keys for critical dependencies
3. Clear cache after major refactoring

### Pickle Errors with Custom Types

**Problem**: Can't pickle certain objects.

**Solutions**:
1. Use Pydantic models instead of custom classes
2. Define classes at module level (not inside functions)
3. Implement `__getstate__` and `__setstate__` for custom pickling

## Performance Tips

1. **Profile First**: Cache only operations that are actually slow
2. **Lazy Loading**: Cache system only loads outputs when needed
3. **Batch Operations**: Caching works with all execution modes (local/daft/auto)
4. **Monitor Cache**: Check `.cache/` size periodically
5. **Clear Strategically**: Clear cache after code refactoring

## Future Enhancements

Planned features for future versions:
- Automatic cache size limits with LRU eviction
- Cache statistics and hit rate reporting
- Cache versioning with automatic migration
- Distributed caching with Redis backend
- Cache warming strategies
- Fine-grained cache control (TTL per node)

## Related Documentation

- [Architecture Overview](overview.md)
- [Caching Design](caching.md)
- [Visualization Guide](visualization.md)


