"""DAG pipeline for managing nodes and dependencies."""

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
    # Caching configuration
    cache: bool = False  # Enable caching for this node
    cache_key: Optional[str] = None  # Optional env_hash override
    cache_backend: Optional[str] = None  # Optional backend override (diskcache)


@dataclass(frozen=True)
class NodeDef:
    """Definition of a DAG node including function, metadata, and parameters."""

    fn: Callable
    meta: NodeMeta
    params: Tuple[str, ...]  # ordered parameter names (from signature)
    params_with_defaults: Tuple[str, ...]  # parameters that have default values


class Pipeline:
    """Pipeline for DAG nodes with automatic topological sorting."""

    def __init__(self, functions: Optional[List[Callable]] = None):
        """Initialize pipeline with optional list of decorated functions.

        Args:
            functions: List of functions decorated with @func. If provided,
                      they will be automatically registered to the pipeline.

        Raises:
            ValueError: If a function is missing @func decorator metadata.
        """
        self.nodes: List[NodeDef] = []
        self.by_output: Dict[str, NodeDef] = {}

        if functions:
            for fn in functions:
                meta = getattr(fn, "_func_meta", None)
                if meta is None:
                    raise ValueError(
                        f"Function {fn.__name__} is missing @func decorator metadata"
                    )
                self.add(fn, meta)

    def add(self, fn: Callable, meta: NodeMeta):
        """Add a node to the pipeline."""
        sig = inspect.signature(fn)
        params = tuple(sig.parameters.keys())
        # Track which parameters have default values
        params_with_defaults = tuple(
            name
            for name, param in sig.parameters.items()
            if param.default is not inspect.Parameter.empty
        )
        node = NodeDef(
            fn=fn, meta=meta, params=params, params_with_defaults=params_with_defaults
        )
        self.nodes.append(node)
        self.by_output[meta.output_name] = node

    def topo(self, initial_inputs: Dict[str, Any]) -> List[NodeDef]:
        """Perform topological sort based on parameter availability.

        Args:
            initial_inputs: Dictionary of initially available values

        Returns:
            List of nodes in execution order

        Raises:
            RuntimeError: If dependencies cannot be resolved (circular deps or missing inputs)
        """
        available = set(initial_inputs.keys())
        ordered: List[NodeDef] = []
        remaining = set(self.nodes)

        while remaining:
            progress = False
            for node in list(remaining):
                # Required parameters are those without defaults
                required = {
                    p
                    for p in node.params
                    if p not in ("self",) and p not in node.params_with_defaults
                }
                if required.issubset(available):
                    ordered.append(node)
                    available.add(node.meta.output_name)
                    remaining.remove(node)
                    progress = True
            if not progress:
                # Build detailed error message showing what's missing for each node
                error_details = []
                for node in remaining:
                    required = {
                        p
                        for p in node.params
                        if p not in ("self",) and p not in node.params_with_defaults
                    }
                    missing = required - available
                    error_details.append(
                        f"  - Function '{node.fn.__name__}' (output='{node.meta.output_name}') "
                        f"is missing required inputs: {sorted(missing)}"
                    )

                raise RuntimeError(
                    "Cannot resolve pipeline dependencies. The following functions have missing inputs:\n"
                    + "\n".join(error_details)
                )
        return ordered

    def clear(self):
        """Clear all registered nodes."""
        self.nodes.clear()
        self.by_output.clear()

    def run(
        self,
        *,
        inputs: Dict[str, Any],
        mode: str = "auto",
        batch_threshold: int = 2,
        cache_config: Optional[Any] = None,
        progress_config: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Run the pipeline with a default runner.

        Args:
            inputs: Dictionary of input values including the map_axis (if applicable)
            mode: Execution mode ("local", "daft", or "auto")
            batch_threshold: Minimum number of items to trigger Daft batching in auto mode
            cache_config: Optional caching configuration
            progress_config: Optional progress bar configuration

        Returns:
            Dictionary containing all outputs including final results
        """
        from daft_func.runner import Runner

        runner = Runner(
            mode=mode,
            batch_threshold=batch_threshold,
            cache_config=cache_config,
            progress_config=progress_config,
        )
        return runner.run(self, inputs=inputs)

    def visualize(self, **kwargs):
        """Visualize the pipeline as a directed graph.

        Args:
            **kwargs: Additional arguments passed to visualize_graphviz
                (orient, style, figsize, filename, show_legend, return_type)

        Returns:
            graphviz.Digraph or IPython.display.HTML object
        """
        from daft_func.visualization import build_graph, visualize_graphviz

        graph = build_graph(self)
        return visualize_graphviz(graph, **kwargs)
