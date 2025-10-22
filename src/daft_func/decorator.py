"""Decorator for defining DAG nodes."""

from functools import wraps
from typing import Callable, Optional

from daft_func.pipeline import NodeMeta


def func(
    *,
    output: str,
    map_axis: Optional[str] = None,
    key_attr: Optional[str] = None,
    cache: bool = False,
    cache_key: Optional[str] = None,
    cache_backend: Optional[str] = None,
):
    """Decorator to attach metadata to a function for DAG pipeline use.

    Args:
        output: Name of the produced value (binds it into the DAG namespace)
        map_axis: Name of the parameter that carries the per-item object (for multi-input runs)
        key_attr: Attribute on the map_axis object that uniquely identifies items (alignment)
        cache: Enable caching for this node
        cache_key: Optional environment hash override for cache invalidation
        cache_backend: Optional backend override (diskcache or cachier)

    Example:
        @func(output="result", map_axis="query", key_attr="query_uuid", cache=True)
        def process(query: Query, config: Config) -> Result:
            return compute(query, config)
    """
    meta = NodeMeta(
        output_name=output,
        map_axis=map_axis,
        key_attr=key_attr,
        cache=cache,
        cache_key=cache_key,
        cache_backend=cache_backend,
    )

    def deco(fn: Callable):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        wrapper._func_meta = meta
        return wrapper

    return deco
