"""DAG registry for managing nodes and dependencies."""

import inspect
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class NodeMeta:
    """Metadata for a DAG node."""

    output_name: str
    # For auto-batching: which parameter is the per-item "map axis"?
    map_axis: Optional[str] = None  # e.g., "query"
    # If nodes align by a key (same item across nodes), what attribute on the map_axis carries it?
    key_attr: Optional[str] = None  # e.g., "query_uuid"


@dataclass(frozen=True)
class NodeDef:
    """Definition of a DAG node including function, metadata, and parameters."""

    fn: Callable
    meta: NodeMeta
    params: Tuple[str, ...]  # ordered parameter names (from signature)


class DagRegistry:
    """Registry for DAG nodes with automatic topological sorting."""

    def __init__(self):
        self.nodes: List[NodeDef] = []
        self.by_output: Dict[str, NodeDef] = {}

    def add(self, fn: Callable, meta: NodeMeta):
        """Add a node to the registry."""
        sig = inspect.signature(fn)
        params = tuple(sig.parameters.keys())
        node = NodeDef(fn=fn, meta=meta, params=params)
        self.nodes.append(node)
        self.by_output[meta.output_name] = node

    def topo(self, initial_inputs: Dict[str, Any]) -> List[NodeDef]:
        """Perform topological sort based on parameter availability.

        Args:
            initial_inputs: Dictionary of initially available values

        Returns:
            List of nodes in execution order

        Raises:
            RuntimeError: If dependencies cannot be resolved (circular deps)
        """
        available = set(initial_inputs.keys())
        ordered: List[NodeDef] = []
        remaining = set(self.nodes)

        while remaining:
            progress = False
            for node in list(remaining):
                needed = {p for p in node.params if p not in ("self",)}
                if needed.issubset(available):
                    ordered.append(node)
                    available.add(node.meta.output_name)
                    remaining.remove(node)
                    progress = True
            if not progress:
                raise RuntimeError(
                    f"Cannot resolve dependencies; remaining: {[n.fn.__name__ for n in remaining]}"
                )
        return ordered

    def clear(self):
        """Clear all registered nodes."""
        self.nodes.clear()
        self.by_output.clear()
