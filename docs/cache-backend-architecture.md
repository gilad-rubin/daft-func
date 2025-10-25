# Cache Backend Architecture

## Overview

The cache system has been refactored to follow SOLID principles with self-contained backend classes that encapsulate both metadata and blob storage along with their specific configuration.

## Key Benefits

1. **Single Responsibility**: Each backend owns its complete storage strategy
2. **Open/Closed**: Easy to add new backends without modifying existing code
3. **Type Safety**: `backend` is now a typed object, not a string
4. **Simpler API**: Clear, explicit initialization for each backend
5. **Flexible Defaults**: Each backend can have sensible defaults for its parameters

## Architecture

### CacheBackend Protocol

All cache backends implement the `CacheBackend` protocol:

```python
class CacheBackend(Protocol):
    def get_meta(self, node_name: str) -> Optional[NodeSignature]: ...
    def set_meta(self, signature: NodeSignature) -> None: ...
    def get_blob(self, key: str) -> Optional[Any]: ...
    def set_blob(self, key: str, value: Any) -> None: ...
    def clear(self) -> None: ...
```

### Available Backends

#### MemoryCache

Ephemeral in-memory storage for the current session. Perfect for development and testing.

```python
from daft_func import MemoryCache

cache = MemoryCache()
```

**Use Cases:**
- Development and testing
- Temporary caching within a single session
- When persistence is not needed

**Advantages:**
- No disk I/O overhead
- No configuration needed
- Automatic cleanup on session end

#### DiskCache

Persistent disk-based storage using diskcache + JSON metadata.

```python
from daft_func import DiskCache

cache = DiskCache(cache_dir=".cache")  # Default: ".cache"
```

**Use Cases:**
- Production workflows
- Long-running processes
- When caching across sessions is needed

**Advantages:**
- Persistent across sessions
- Survives process restarts
- Configurable storage location

## Usage Examples

### Basic Usage with MemoryCache (Default)

```python
from daft_func import CacheConfig, Runner, Pipeline

# MemoryCache is the default backend
config = CacheConfig(enabled=True)
runner = Runner(pipeline, cache_config=config)
```

### Using DiskCache

```python
from daft_func import CacheConfig, DiskCache, Runner, Pipeline

# Explicit DiskCache with custom directory
config = CacheConfig(
    enabled=True,
    backend=DiskCache(cache_dir=".my_cache")
)
runner = Runner(pipeline, cache_config=config)
```

### Using MemoryCache Explicitly

```python
from daft_func import CacheConfig, MemoryCache, Runner, Pipeline

# Explicit MemoryCache (useful for clarity)
config = CacheConfig(
    enabled=True,
    backend=MemoryCache()
)
runner = Runner(pipeline, cache_config=config)
```

### Complete Example

```python
from daft_func import CacheConfig, DiskCache, Pipeline, Runner, func

@func(output="result", cache=True)
def expensive_operation(x: int) -> int:
    import time
    time.sleep(1)  # Simulate expensive operation
    return x * 2

pipeline = Pipeline(functions=[expensive_operation])

# Create runner with disk-based caching
cache_config = CacheConfig(
    enabled=True,
    backend=DiskCache(cache_dir=".cache"),
    dependency_depth=2,
    verbose=True
)

runner = Runner(
        mode="local",
    cache_config=cache_config
)

# First run: cache miss, executes function
result1 = runner.run(pipeline, inputs={"x": 5})
# Output: [CACHE] result: ✗ MISS (1.00s)

# Second run: cache hit, instant
result2 = runner.run(pipeline, inputs={"x": 5})
# Output: [CACHE] result: ✓ HIT
```

## Migration Guide

### Old API (Deprecated)

```python
# Old: String-based backend selection
cache_config = CacheConfig(
    enabled=True,
    backend="diskcache",  # ❌ String-based
    cache_dir=".cache"
)
```

### New API

```python
# New: Object-based backend selection
from daft_func import DiskCache

cache_config = CacheConfig(
    enabled=True,
    backend=DiskCache(cache_dir=".cache")  # ✅ Type-safe
)
```

## Creating Custom Backends

To create a custom backend, implement the `CacheBackend` protocol:

```python
from typing import Any, Dict, Optional
from daft_func.cache import CacheBackend, NodeSignature

class RedisCache:
    """Custom Redis-based cache backend."""
    
    def __init__(self, host: str = "localhost", port: int = 6379):
        import redis
        self.redis = redis.Redis(host=host, port=port)
    
    def get_meta(self, node_name: str) -> Optional[NodeSignature]:
        data = self.redis.get(f"meta:{node_name}")
        if data:
            import json
            return NodeSignature(**json.loads(data))
        return None
    
    def set_meta(self, signature: NodeSignature) -> None:
        import json
        data = {
            "node_name": signature.node_name,
            "code_hash": signature.code_hash,
            "env_hash": signature.env_hash,
            "inputs_hash": signature.inputs_hash,
            "deps_hash": signature.deps_hash,
            "timestamp": signature.timestamp,
        }
        self.redis.set(f"meta:{signature.node_name}", json.dumps(data))
    
    def get_blob(self, key: str) -> Optional[Any]:
        import pickle
        data = self.redis.get(f"blob:{key}")
        if data:
            return pickle.loads(data)
        return None
    
    def set_blob(self, key: str, value: Any) -> None:
        import pickle
        self.redis.set(f"blob:{key}", pickle.dumps(value))
    
    def clear(self) -> None:
        # Clear all cache keys
        for key in self.redis.scan_iter("meta:*"):
            self.redis.delete(key)
        for key in self.redis.scan_iter("blob:*"):
            self.redis.delete(key)

# Usage
from daft_func import CacheConfig

config = CacheConfig(
    enabled=True,
    backend=RedisCache(host="localhost", port=6379)
)
```

## Implementation Details

### Internal Structure

The new architecture uses:

1. **Public Protocol**: `CacheBackend` - defines the interface
2. **Public Implementations**: `MemoryCache`, `DiskCache` - user-facing backends
3. **Internal Stores**: `_JSONMetaStore`, `_DiskCacheBlobStore` - implementation details

The internal store classes (prefixed with `_`) are used by `DiskCache` but are not exposed in the public API.

### Backward Compatibility

The old `create_stores()` factory function is still available but deprecated. It's maintained for backward compatibility but will be removed in a future version.

## Configuration Options

All backends share these `CacheConfig` options:

```python
CacheConfig(
    enabled: bool = False,              # Enable/disable caching
    backend: CacheBackend = MemoryCache(),  # Cache backend instance
    env_hash: Optional[str] = None,     # Manual environment hash override
    dependency_depth: int = 2,          # Levels of imports to track
    verbose: bool = True,               # Print cache status after runs
    per_item_caching: bool = True       # Per-item caching for map_axis nodes
)
```

## Best Practices

1. **Development**: Use `MemoryCache()` for fast iteration
2. **Production**: Use `DiskCache(cache_dir=...)` for persistence
3. **Testing**: Use `MemoryCache()` to avoid test pollution
4. **CI/CD**: Use `DiskCache(cache_dir=".cache")` with cache directory caching

## Performance Considerations

### MemoryCache
- **Pros**: Fastest possible cache access, no I/O
- **Cons**: Lost on process exit, limited by RAM

### DiskCache
- **Pros**: Persistent, unlimited size (disk-bound)
- **Cons**: Slower than memory (disk I/O overhead)

Choose based on your workflow needs!

