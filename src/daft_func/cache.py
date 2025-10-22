"""Caching infrastructure for DAG pipeline execution.

Implements dependency-aware incremental caching with:
- Signature-based cache invalidation
- Diskcache storage backend
- Code change detection with dependency tracking
"""

import ast
import hashlib
import inspect
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Protocol

from pydantic import BaseModel

if TYPE_CHECKING:
    from typing import CacheBackend


@dataclass
class CacheEvent:
    """Record of a cache operation for a single node."""

    node_name: str
    cache_enabled: bool
    cache_hit: bool
    loaded: bool  # whether output was actually loaded from cache
    execution_time: float = 0.0  # seconds spent executing (if executed)


class CacheStats:
    """Statistics collector for cache operations during a run."""

    def __init__(self):
        self.events: list[CacheEvent] = []

    def record(
        self,
        node_name: str,
        cache_enabled: bool,
        cache_hit: bool,
        loaded: bool,
        execution_time: float = 0.0,
    ):
        """Record a cache event."""
        self.events.append(
            CacheEvent(
                node_name=node_name,
                cache_enabled=cache_enabled,
                cache_hit=cache_hit,
                loaded=loaded,
                execution_time=execution_time,
            )
        )

    def print_summary(self):
        """Print a concise summary of cache operations."""
        if not self.events:
            return

        print("\n[CACHE]", end=" ")
        parts = []

        for event in self.events:
            if not event.cache_enabled:
                # Node doesn't use cache
                status = f"{event.node_name}: NO-CACHE"
                if event.execution_time > 0:
                    status += f" ({event.execution_time:.2f}s)"
            elif event.cache_hit:
                if event.loaded:
                    # Cache hit and output was loaded
                    status = f"{event.node_name}: ✓ HIT"
                else:
                    # Cache hit but output not needed (lazy)
                    status = f"{event.node_name}: ○ HIT (skipped)"
            else:
                # Cache miss, executed
                status = f"{event.node_name}: ✗ MISS"
                if event.execution_time > 0:
                    status += f" ({event.execution_time:.2f}s)"

            parts.append(status)

        print(" | ".join(parts))


@dataclass
class NodeSignature:
    """Signature representing a node's computation state."""

    node_name: str
    code_hash: str
    env_hash: str
    inputs_hash: str
    deps_hash: str
    timestamp: float


def compute_code_hash(fn: Any) -> str:
    """Compute hash of function's source code.

    Args:
        fn: Function to hash

    Returns:
        Hash string of the function's source code
    """
    try:
        source = inspect.getsource(fn)
        return hashlib.sha256(source.encode()).hexdigest()[:16]
    except (OSError, TypeError):
        # For built-in functions or lambdas, use qualified name
        return hashlib.sha256(fn.__qualname__.encode()).hexdigest()[:16]


def compute_deps_hash(
    fn: Any, depth: int = 2, project_root: Optional[Path] = None
) -> str:
    """Compute hash of function's dependencies up to specified depth.

    Analyzes imports in the function's module and recursively tracks
    internal project dependencies.

    Args:
        fn: Function to analyze
        depth: How many levels of dependencies to track
        project_root: Root of project (auto-detected if None)

    Returns:
        Hash string of dependencies
    """
    if depth <= 0:
        return ""

    if project_root is None:
        # Try to find project root
        try:
            fn_file = inspect.getfile(fn)
            current = Path(fn_file).parent
            # Look for common project markers
            for _ in range(10):  # max 10 levels up
                if (current / "pyproject.toml").exists() or (current / ".git").exists():
                    project_root = current
                    break
                if current.parent == current:
                    break
                current = current.parent
        except (TypeError, OSError):
            pass

    if project_root is None:
        project_root = Path.cwd()

    visited = set()
    dep_hashes = []

    def _get_module_deps(module_path: Path, current_depth: int) -> None:
        """Recursively extract dependencies from a module."""
        if current_depth > depth or str(module_path) in visited:
            return

        visited.add(str(module_path))

        try:
            with open(module_path, "r") as f:
                tree = ast.parse(f.read())

            # Extract imports
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    if isinstance(node, ast.ImportFrom) and node.module:
                        module_name = node.module
                    elif isinstance(node, ast.Import):
                        module_name = node.names[0].name if node.names else ""
                    else:
                        continue

                    # Check if it's an internal import
                    module_parts = module_name.split(".")
                    potential_path = project_root / module_parts[0]

                    # Look for the module file
                    if potential_path.exists() and potential_path.is_dir():
                        # Try to find the actual module file
                        if len(module_parts) > 1:
                            module_file = (
                                project_root / "/".join(module_parts)
                            ).with_suffix(".py")
                            if not module_file.exists():
                                module_file = (
                                    project_root
                                    / "/".join(module_parts)
                                    / "__init__.py"
                                )
                        else:
                            module_file = potential_path / "__init__.py"

                        if module_file.exists():
                            # Hash this dependency's content
                            with open(module_file, "rb") as mf:
                                content_hash = hashlib.sha256(mf.read()).hexdigest()[
                                    :16
                                ]
                                dep_hashes.append(content_hash)

                            # Recursively process
                            _get_module_deps(module_file, current_depth + 1)

        except (OSError, SyntaxError):
            pass

    # Start from the function's module
    try:
        fn_file = Path(inspect.getfile(fn))
        if fn_file.exists():
            _get_module_deps(fn_file, 0)
    except (TypeError, OSError):
        pass

    # Combine all dependency hashes
    combined = "".join(sorted(dep_hashes))
    if combined:
        return hashlib.sha256(combined.encode()).hexdigest()[:16]
    return ""


def compute_inputs_hash(
    kwargs: Dict[str, Any],
    serialization_depth: int = 2,
    *,
    force_instance_ids: bool = False,
) -> str:
    """Compute hash of input parameters with smart object handling.

    Args:
        kwargs: Dictionary of input parameters
        serialization_depth: Maximum depth for recursive object serialization

    Returns:
        Hash string of inputs
    """

    def _serialize(obj: Any, current_depth: int = 0) -> Any:
        """Serialize objects for hashing with depth-limited recursion."""

        # Strategy 1: Object defines its own cache key
        if hasattr(obj, "__cache_key__") and callable(obj.__cache_key__):
            # For side-effect style nodes (e.g., indexers returning bool/None),
            # we must distinguish different instances even if their __cache_key__
            # is identical, otherwise side effects may be skipped incorrectly.
            if force_instance_ids:
                return {
                    "__cache_key__": str(obj.__cache_key__()),
                    "__id__": id(obj),
                }
            return {"__cache_key__": str(obj.__cache_key__())}

        # Strategy 2: Pydantic models
        if isinstance(obj, BaseModel):
            return obj.model_dump()

        # Strategy 3: Lists and tuples (recursive with depth tracking)
        elif isinstance(obj, (list, tuple)):
            return [_serialize(item, current_depth) for item in obj]

        # Strategy 3b: Sets and frozensets (order-independent deterministic serialization)
        elif isinstance(obj, (set, frozenset)):
            try:
                items = [_serialize(item, current_depth) for item in obj]
                # Try direct sort first; if unorderable, sort by JSON string
                try:
                    items_sorted = sorted(items)
                except TypeError:
                    import json as _json

                    items_sorted = sorted(
                        [_json.dumps(it, sort_keys=True, default=str) for it in items]
                    )
                return {"__set__": items_sorted}
            except Exception:
                # Fallback to string representation if something goes wrong
                return {"__set__": sorted([str(x) for x in obj])}

        # Strategy 4: Dictionaries (recursive with depth tracking)
        elif isinstance(obj, dict):
            return {k: _serialize(v, current_depth) for k, v in obj.items()}

        # Strategy 5: Simple hashable types
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj

        # Strategy 6: Deep object serialization with depth limit
        elif hasattr(obj, "__dict__") and current_depth < serialization_depth:
            # Only serialize public attributes (exclude _ prefixed ones)
            state = {}
            for k, v in obj.__dict__.items():
                if not k.startswith("_") and not callable(v):
                    try:
                        # Recursively serialize, incrementing depth
                        state[k] = _serialize(v, current_depth + 1)
                    except (TypeError, ValueError, RecursionError):
                        # If can't serialize, use string representation
                        state[k] = str(v)

            # If object has NO public attributes, it might rely on side effects
            # Use object ID to distinguish different instances
            if not state:
                return {"__id__": id(obj), "__class__": obj.__class__.__name__}

            # Only include class name and state (no object ID!)
            # This allows instances with identical configuration to share cache
            return {
                "__class__": obj.__class__.__name__,
                "__state__": state,
            }

        # Fallback: Use object ID only when depth limit reached or can't serialize
        else:
            return {"__id__": id(obj), "__class__": obj.__class__.__name__}

    # Sort keys for consistent hashing
    serialized = {k: _serialize(v, 0) for k, v in sorted(kwargs.items())}
    json_str = json.dumps(serialized, sort_keys=True, default=str)
    return hashlib.sha256(json_str.encode()).hexdigest()[:16]


def compute_signature(
    node_name: str,
    fn: Any,
    kwargs: Dict[str, Any],
    parent_sigs: Dict[str, str],
    env_hash: Optional[str] = None,
    dependency_depth: int = 2,
    serialization_depth: int = 2,
) -> NodeSignature:
    """Compute complete signature for a node.

    Args:
        node_name: Name of the node
        fn: Function to compute
        kwargs: Input parameters
        parent_sigs: Signatures of parent nodes
        env_hash: Optional environment hash override
        dependency_depth: How many levels of dependencies to track
        serialization_depth: How deep to serialize object attributes

    Returns:
        Complete NodeSignature
    """
    import time

    code_hash = compute_code_hash(fn)
    deps_code_hash = compute_deps_hash(fn, depth=dependency_depth)

    # Heuristic: treat functions that return bool/None as potential side-effect nodes.
    # For such nodes, include object instance IDs (even when __cache_key__ exists)
    # to avoid skipping necessary initialization on new instances.
    try:
        from typing import get_type_hints

        hints = get_type_hints(fn)
        ret = hints.get("return", None)
        force_ids = ret is bool or ret is type(None)
    except Exception:
        force_ids = False

    inputs_hash = compute_inputs_hash(
        kwargs, serialization_depth=serialization_depth, force_instance_ids=force_ids
    )

    # Combine parent signatures
    parent_hash = hashlib.sha256(
        "".join(sorted(parent_sigs.values())).encode()
    ).hexdigest()[:16]

    # Use provided env_hash or empty string
    final_env_hash = env_hash or ""

    return NodeSignature(
        node_name=node_name,
        code_hash=code_hash,
        env_hash=final_env_hash,
        inputs_hash=inputs_hash,
        deps_hash=parent_hash + deps_code_hash,
        timestamp=time.time(),
    )


class CacheBackend(Protocol):
    """Protocol for cache backends that manage both metadata and blob storage."""

    def get_meta(self, node_name: str) -> Optional[NodeSignature]:
        """Retrieve signature for a node."""
        ...

    def set_meta(self, signature: NodeSignature) -> None:
        """Store signature for a node."""
        ...

    def get_blob(self, key: str) -> Optional[Any]:
        """Retrieve cached output."""
        ...

    def set_blob(self, key: str, value: Any) -> None:
        """Store output."""
        ...

    def clear(self) -> None:
        """Clear all stored data (metadata and blobs)."""
        ...


# Legacy protocols - kept for internal use
class _MetaStore(Protocol):
    """Protocol for storing/retrieving node signatures."""

    def get(self, node_name: str) -> Optional[NodeSignature]:
        """Retrieve signature for a node."""
        ...

    def set(self, signature: NodeSignature) -> None:
        """Store signature for a node."""
        ...

    def clear(self) -> None:
        """Clear all stored signatures."""
        ...


class _BlobStore(Protocol):
    """Protocol for storing/retrieving node outputs."""

    def get(self, key: str) -> Optional[Any]:
        """Retrieve cached output."""
        ...

    def set(self, key: str, value: Any) -> None:
        """Store output."""
        ...

    def clear(self) -> None:
        """Clear all stored outputs."""
        ...


class _JSONMetaStore:
    """JSON file-based metadata store (internal)."""

    def __init__(self, cache_dir: str):
        """Initialize JSON metadata store.

        Args:
            cache_dir: Directory for cache storage
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.meta_file = self.cache_dir / "metadata.json"

        # Load existing metadata
        self._meta: Dict[str, Dict[str, Any]] = {}
        if self.meta_file.exists():
            try:
                with open(self.meta_file, "r") as f:
                    self._meta = json.load(f)
            except (json.JSONDecodeError, OSError):
                self._meta = {}

    def get(self, node_name: str) -> Optional[NodeSignature]:
        """Retrieve signature for a node."""
        if node_name in self._meta:
            data = self._meta[node_name]
            return NodeSignature(**data)
        return None

    def set(self, signature: NodeSignature) -> None:
        """Store signature for a node."""
        self._meta[signature.node_name] = {
            "node_name": signature.node_name,
            "code_hash": signature.code_hash,
            "env_hash": signature.env_hash,
            "inputs_hash": signature.inputs_hash,
            "deps_hash": signature.deps_hash,
            "timestamp": signature.timestamp,
        }
        # Persist to disk
        with open(self.meta_file, "w") as f:
            json.dump(self._meta, f, indent=2)

    def clear(self) -> None:
        """Clear all stored signatures."""
        self._meta = {}
        if self.meta_file.exists():
            self.meta_file.unlink()


class _DiskCacheBlobStore:
    """Diskcache-based blob store (internal)."""

    def __init__(self, cache_dir: str):
        """Initialize diskcache blob store.

        Args:
            cache_dir: Directory for cache storage
        """
        import diskcache

        blob_dir = Path(cache_dir) / "blobs"
        self.cache = diskcache.Cache(str(blob_dir))

    def get(self, key: str) -> Optional[Any]:
        """Retrieve cached output."""
        return self.cache.get(key)

    def set(self, key: str, value: Any) -> None:
        """Store output."""
        self.cache.set(key, value)

    def clear(self) -> None:
        """Clear all stored outputs."""
        self.cache.clear()


class MemoryCache:
    """In-memory cache backend - ephemeral storage for current session."""

    def __init__(self):
        """Initialize in-memory cache with no configuration needed."""
        self._meta: Dict[str, NodeSignature] = {}
        self._blobs: Dict[str, Any] = {}

    def get_meta(self, node_name: str) -> Optional[NodeSignature]:
        """Retrieve signature for a node."""
        return self._meta.get(node_name)

    def set_meta(self, signature: NodeSignature) -> None:
        """Store signature for a node."""
        self._meta[signature.node_name] = signature

    def get_blob(self, key: str) -> Optional[Any]:
        """Retrieve cached output."""
        return self._blobs.get(key)

    def set_blob(self, key: str, value: Any) -> None:
        """Store output."""
        self._blobs[key] = value

    def clear(self) -> None:
        """Clear all stored data."""
        self._meta.clear()
        self._blobs.clear()


class DiskCache:
    """Disk-based cache backend with persistent storage."""

    def __init__(self, cache_dir: str = ".cache"):
        """Initialize disk cache with specified directory.

        Args:
            cache_dir: Directory for cache storage (default: .cache)
        """
        self.cache_dir = cache_dir
        self._meta_store = _JSONMetaStore(cache_dir)
        self._blob_store = _DiskCacheBlobStore(cache_dir)

    def get_meta(self, node_name: str) -> Optional[NodeSignature]:
        """Retrieve signature for a node."""
        return self._meta_store.get(node_name)

    def set_meta(self, signature: NodeSignature) -> None:
        """Store signature for a node."""
        self._meta_store.set(signature)

    def get_blob(self, key: str) -> Optional[Any]:
        """Retrieve cached output."""
        return self._blob_store.get(key)

    def set_blob(self, key: str, value: Any) -> None:
        """Store output."""
        self._blob_store.set(key, value)

    def clear(self) -> None:
        """Clear all stored data."""
        self._meta_store.clear()
        self._blob_store.clear()


@dataclass
class CacheConfig:
    """Configuration for caching behavior with backend support."""

    enabled: bool = False
    backend: CacheBackend = field(default_factory=MemoryCache)
    env_hash: Optional[str] = None  # manual override
    dependency_depth: int = 2  # levels of imports to track
    verbose: bool = True  # print cache status after each run
    per_item_caching: bool = True  # Enable per-item caching for map_axis nodes
    serialization_depth: int = 2  # depth for object attribute serialization


# Legacy factory function - deprecated, use backend classes directly
def create_stores(
    config: CacheConfig,
) -> tuple[_MetaStore, _BlobStore]:
    """Factory function to create metadata and blob stores (deprecated).

    Args:
        config: Cache configuration

    Returns:
        Tuple of (_MetaStore, _BlobStore)

    Deprecated:
        Use CacheConfig with backend parameter instead.
    """
    # For backward compatibility, extract backend from config
    if hasattr(config, "backend") and isinstance(config.backend, CacheBackend):
        # New-style config with CacheBackend
        backend = config.backend

        # Wrap in legacy interface
        class _BackendWrapper:
            def __init__(self, b):
                self.backend = b

            def get(self, key):
                return (
                    self.backend.get_meta(key)
                    if hasattr(self, "_is_meta")
                    else self.backend.get_blob(key)
                )

            def set(self, key_or_sig, value=None):
                if value is None:  # It's a signature
                    self.backend.set_meta(key_or_sig)
                else:
                    self.backend.set_blob(key_or_sig, value)

            def clear(self):
                self.backend.clear()

        meta = _BackendWrapper(backend)
        meta._is_meta = True
        blob = _BackendWrapper(backend)
        return meta, blob
    else:
        # Old-style config with string backend
        cache_dir = getattr(config, "cache_dir", ".cache")
        meta_store = _JSONMetaStore(cache_dir)
        blob_store = _DiskCacheBlobStore(cache_dir)
        return meta_store, blob_store


def signatures_match(sig1: NodeSignature, sig2: NodeSignature) -> bool:
    """Check if two signatures are equivalent (ignoring timestamp).

    Args:
        sig1: First signature
        sig2: Second signature

    Returns:
        True if signatures match
    """
    return (
        sig1.code_hash == sig2.code_hash
        and sig1.env_hash == sig2.env_hash
        and sig1.inputs_hash == sig2.inputs_hash
        and sig1.deps_hash == sig2.deps_hash
    )


def get_item_cache_key(item: Any, key_attr: Optional[str] = None) -> str:
    """Extract cache key from an item for map_axis nodes.

    Args:
        item: The item being processed
        key_attr: Attribute name to use as unique identifier

    Returns:
        String identifier for the item
    """
    if key_attr and hasattr(item, key_attr):
        return str(getattr(item, key_attr))
    elif isinstance(item, BaseModel):
        # For Pydantic models, try common ID fields
        for attr in ["id", "uuid", "key", "name"]:
            if hasattr(item, attr):
                return str(getattr(item, attr))
    # Fallback: use hash of the item
    return str(hash(str(item)))


def make_cache_key(node_name: str, item_key: Optional[str] = None) -> str:
    """Create a cache key for a node, optionally including item identifier.

    Args:
        node_name: Name of the node output
        item_key: Optional item identifier for map_axis nodes

    Returns:
        Cache key string
    """
    if item_key:
        return f"{node_name}::{item_key}"
    return node_name
