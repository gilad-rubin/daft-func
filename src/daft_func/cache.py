"""Caching infrastructure for DAG pipeline execution.

Implements dependency-aware incremental caching with:
- Signature-based cache invalidation
- Multiple storage backends (diskcache, cachier)
- Code change detection with dependency tracking
"""

import ast
import hashlib
import inspect
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Protocol

from pydantic import BaseModel


@dataclass
class CacheConfig:
    """Configuration for caching behavior."""

    enabled: bool = False
    backend: str = "diskcache"  # "diskcache" or "cachier"
    cache_dir: str = ".cache"
    env_hash: Optional[str] = None  # manual override
    dependency_depth: int = 2  # levels of imports to track


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


def compute_inputs_hash(kwargs: Dict[str, Any]) -> str:
    """Compute hash of input parameters.

    Args:
        kwargs: Dictionary of input parameters

    Returns:
        Hash string of inputs
    """

    def _serialize(obj: Any) -> Any:
        """Serialize objects for hashing."""
        if isinstance(obj, BaseModel):
            return obj.model_dump()
        elif isinstance(obj, (list, tuple)):
            return [_serialize(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: _serialize(v) for k, v in obj.items()}
        elif hasattr(obj, "__dict__"):
            # For other objects, try to get their dict
            return str(obj.__dict__)
        else:
            return str(obj)

    # Sort keys for consistent hashing
    serialized = {k: _serialize(v) for k, v in sorted(kwargs.items())}
    json_str = json.dumps(serialized, sort_keys=True, default=str)
    return hashlib.sha256(json_str.encode()).hexdigest()[:16]


def compute_signature(
    node_name: str,
    fn: Any,
    kwargs: Dict[str, Any],
    parent_sigs: Dict[str, str],
    env_hash: Optional[str] = None,
    dependency_depth: int = 2,
) -> NodeSignature:
    """Compute complete signature for a node.

    Args:
        node_name: Name of the node
        fn: Function to compute
        kwargs: Input parameters
        parent_sigs: Signatures of parent nodes
        env_hash: Optional environment hash override
        dependency_depth: How many levels of dependencies to track

    Returns:
        Complete NodeSignature
    """
    import time

    code_hash = compute_code_hash(fn)
    deps_code_hash = compute_deps_hash(fn, depth=dependency_depth)
    inputs_hash = compute_inputs_hash(kwargs)

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


class MetaStore(Protocol):
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


class BlobStore(Protocol):
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


class JSONMetaStore:
    """JSON file-based metadata store."""

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


class DiskCacheBlobStore:
    """Diskcache-based blob store."""

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


class CachierBlobStore:
    """Cachier-based blob store (using pickle backend)."""

    def __init__(self, cache_dir: str):
        """Initialize cachier blob store.

        Args:
            cache_dir: Directory for cache storage
        """
        self.cache_dir = Path(cache_dir) / "cachier_blobs"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._cache: Dict[str, Any] = {}

    def get(self, key: str) -> Optional[Any]:
        """Retrieve cached output."""
        import pickle

        cache_file = self.cache_dir / f"{key}.pkl"
        if cache_file.exists():
            try:
                with open(cache_file, "rb") as f:
                    return pickle.load(f)
            except (pickle.PickleError, OSError):
                return None
        return None

    def set(self, key: str, value: Any) -> None:
        """Store output."""
        import pickle

        cache_file = self.cache_dir / f"{key}.pkl"
        with open(cache_file, "wb") as f:
            pickle.dump(value, f)

    def clear(self) -> None:
        """Clear all stored outputs."""
        import shutil

        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)


def create_stores(
    config: CacheConfig,
) -> tuple[MetaStore, BlobStore]:
    """Factory function to create metadata and blob stores.

    Args:
        config: Cache configuration

    Returns:
        Tuple of (MetaStore, BlobStore)
    """
    meta_store = JSONMetaStore(config.cache_dir)

    if config.backend == "diskcache":
        blob_store = DiskCacheBlobStore(config.cache_dir)
    elif config.backend == "cachier":
        blob_store = CachierBlobStore(config.cache_dir)
    else:
        raise ValueError(f"Unknown cache backend: {config.backend}")

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

