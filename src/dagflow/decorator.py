"""Decorator for defining DAG nodes."""

from functools import wraps
from typing import Callable, Optional

from dagflow.registry import DagRegistry, NodeMeta

# Global registry instance
_GLOBAL_REGISTRY = DagRegistry()


def get_registry() -> DagRegistry:
    """Get the global DAG registry."""
    return _GLOBAL_REGISTRY


def dagflow(
    *, output: str, map_axis: Optional[str] = None, key_attr: Optional[str] = None
):
    """Decorator to register a function as a DAG node.

    Args:
        output: Name of the produced value (binds it into the DAG namespace)
        map_axis: Name of the parameter that carries the per-item object (for multi-input runs)
        key_attr: Attribute on the map_axis object that uniquely identifies items (alignment)

    Example:
        @dagflow(output="result", map_axis="query", key_attr="query_uuid")
        def process(query: Query, config: Config) -> Result:
            return compute(query, config)
    """
    meta = NodeMeta(output_name=output, map_axis=map_axis, key_attr=key_attr)

    def deco(fn: Callable):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        wrapper._dagflow_meta = meta
        _GLOBAL_REGISTRY.add(wrapper, meta)
        return wrapper

    return deco
