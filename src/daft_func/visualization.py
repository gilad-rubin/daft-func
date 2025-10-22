"""Visualization utilities for DAG pipelines using Graphviz."""

import html
import inspect
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Literal, Optional, get_type_hints

import networkx as nx

from daft_func.pipeline import NodeDef, Pipeline

_empty = inspect.Parameter.empty
MAX_LABEL_LENGTH = 30


@dataclass(frozen=True)
class GroupedArgs:
    """A tuple of exclusive input parameters for a function."""

    args: tuple[str, ...]

    def __str__(self) -> str:
        return ", ".join(self.args)


@dataclass
class GraphvizStyle:
    """Style configuration for Graphviz visualization."""

    # Node colors
    arg_node_color: str = "#90EE90"  # lightgreen
    func_node_color: str = "#87CEEB"  # skyblue
    grouped_args_node_color: str = "#90EE90"  # lightgreen (same as arg)
    # Edge colors
    arg_edge_color: Optional[str] = None  # defaults to arg_node_color
    output_edge_color: Optional[str] = None  # defaults to func_node_color
    grouped_args_edge_color: Optional[str] = None  # defaults to grouped_args_node_color
    # Font settings
    font_name: str = "Helvetica"
    font_size: int = 12
    edge_font_size: int = 10
    legend_font_size: int = 16
    font_color: str = "black"
    # Background
    legend_background_color: str = "lightgrey"
    background_color: Optional[str] = None
    # Other
    legend_border_color: str = "black"


def _trim(s: Any, max_len: int = MAX_LABEL_LENGTH) -> str:
    """Trim a string to specified max length, adding ellipses if needed."""
    s = str(s)
    if len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s


def _type_as_string(type_hint: Any) -> str:
    """Convert type hint to readable string."""
    if type_hint is _empty or type_hint is None:
        return ""

    # Handle string representations
    if isinstance(type_hint, str):
        return type_hint

    # Get the type name
    if hasattr(type_hint, "__name__"):
        return type_hint.__name__

    # Handle generic types (List, Dict, etc.)
    type_str = str(type_hint)
    # Clean up common type representations
    type_str = type_str.replace("typing.", "")
    type_str = type_str.replace("<class '", "").replace("'>", "")

    return type_str


def build_graph(pipeline: Pipeline) -> nx.DiGraph:
    """Build a NetworkX directed graph from a pipeline.

    Args:
        pipeline: The pipeline to visualize

    Returns:
        NetworkX directed graph with nodes and edges representing the DAG
    """
    graph = nx.DiGraph()

    # Use all registered nodes
    ordered_nodes = pipeline.nodes

    # Track which outputs are available (from node outputs)
    available_outputs = {node.meta.output_name for node in pipeline.nodes}

    # Add nodes and edges
    for node in ordered_nodes:
        # Add function node with metadata
        graph.add_node(node, node_type="function")

        # Add edges from parameters to function
        for param in node.params:
            # Check if this param is an output from another node
            source_node = None
            for other_node in pipeline.nodes:
                if other_node.meta.output_name == param:
                    source_node = other_node
                    break

            if source_node:
                # Edge from function to function
                graph.add_edge(source_node, node, param_name=param)
            else:
                # Edge from input parameter to function
                if param not in graph.nodes:
                    graph.add_node(param, node_type="input")
                graph.add_edge(param, node, param_name=param)

    return graph


def _find_exclusive_parameters(
    graph: nx.DiGraph,
    min_arg_group_size: int = 2,
) -> Dict[NodeDef, list[str]]:
    """Find parameters that are used exclusively by a single function.

    Args:
        graph: The pipeline graph
        min_arg_group_size: Minimum number of parameters to group

    Returns:
        Dictionary mapping functions to their exclusive parameter lists
    """
    grouped_params: Dict[NodeDef, list[str]] = defaultdict(list)

    for node in graph.nodes:
        # Check if this is a string node (input parameter)
        if isinstance(node, str):
            successors = list(graph.successors(node))
            # Check if it has exactly one successor which is a NodeDef (function)
            if len(successors) == 1 and isinstance(successors[0], NodeDef):
                target_func = successors[0]
                grouped_params[target_func].append(node)

    # Sort the parameters within each group for consistent labeling
    for func in grouped_params:
        grouped_params[func].sort()

    # Only return groups that meet the minimum size
    return {
        func: params
        for func, params in grouped_params.items()
        if len(params) >= min_arg_group_size
    }


def create_grouped_parameter_graph(
    graph: nx.DiGraph,
    min_arg_group_size: Optional[int],
) -> nx.DiGraph:
    """Create a new graph with grouped exclusive input parameters for each function.

    Args:
        graph: The original graph with individual parameter nodes
        min_arg_group_size: Minimum number of parameters to group. If None, no grouping.

    Returns:
        New graph with grouped parameter nodes where applicable
    """
    new_graph = graph.copy()

    if min_arg_group_size is None:
        return new_graph

    # Ensure minimum of 2 for grouping to make sense
    min_arg_group_size = max(min_arg_group_size, 2)

    groups_to_create = _find_exclusive_parameters(graph, min_arg_group_size)

    if not groups_to_create:
        return new_graph  # Return copy if no grouping needed

    # Track which parameters are in any group
    params_in_any_group: set[str] = set()
    for params in groups_to_create.values():
        params_in_any_group.update(params)

    # Create grouped nodes
    for target_func, params_list in groups_to_create.items():
        grouped_node = GroupedArgs(args=tuple(sorted(params_list)))

        # Add grouped node
        new_graph.add_node(grouped_node, node_type="grouped_args")
        new_graph.add_edge(grouped_node, target_func, param_name="grouped")

        # Remove individual parameter nodes and their edges
        for param_name in params_list:
            if new_graph.has_edge(param_name, target_func):
                new_graph.remove_edge(param_name, target_func)
            if new_graph.has_node(param_name):
                new_graph.remove_node(param_name)

    return new_graph


def _generate_node_label(
    node: Any,
    node_type: str,
    hints: Dict[str, Any],
    defaults: Dict[str, Any],
) -> str:
    """Generate HTML-like label for a graph node.

    Args:
        node: The node (string for input, NodeDef for function, or GroupedArgs)
        node_type: Type of node ("input", "function", or "grouped_args")
        hints: Type hints for parameters and return values
        defaults: Default values for parameters

    Returns:
        HTML-formatted label string
    """
    if node_type == "grouped_args":
        # Grouped parameters node
        grouped: GroupedArgs = node
        label = '<TABLE BORDER="0">'
        for param_name in grouped.args:
            type_hint = hints.get(param_name)
            default_value = defaults.get(param_name, _empty)

            parts = [f"<b>{html.escape(param_name)}</b>"]

            if type_hint:
                type_str = _trim(_type_as_string(type_hint))
                parts.append(f" : <i>{html.escape(type_str)}</i>")

            if default_value is not _empty:
                default_str = _trim(str(default_value))
                parts.append(f" = {html.escape(default_str)}")

            label += f"<TR><TD>{' '.join(parts)}</TD></TR>"

        label += "</TABLE>"
        return label

    elif node_type == "input":
        # Input parameter node
        param_name = str(node)
        type_hint = hints.get(param_name)
        default_value = defaults.get(param_name, _empty)

        parts = [f"<b>{html.escape(param_name)}</b>"]

        if type_hint:
            type_str = _trim(_type_as_string(type_hint))
            parts.append(f" : <i>{html.escape(type_str)}</i>")

        if default_value is not _empty:
            default_str = _trim(str(default_value))
            parts.append(f" = {html.escape(default_str)}")

        return " ".join(parts)

    else:
        # Function node
        node_def: NodeDef = node
        fn_name = node_def.fn.__name__

        # Get type hints for the function
        try:
            fn_hints = get_type_hints(node_def.fn)
        except Exception:
            fn_hints = {}

        # Build HTML table for function node
        label = (
            f'<TABLE BORDER="0"><TR><TD><B>{html.escape(fn_name)}</B></TD></TR><HR/>'
        )

        # Add output with type annotation
        output_name = node_def.meta.output_name
        return_type = fn_hints.get("return")

        parts = [f"<b>{html.escape(output_name)}</b>"]
        if return_type:
            type_str = _trim(_type_as_string(return_type))
            parts.append(f" : <i>{html.escape(type_str)}</i>")

        label += f"<TR><TD>{' '.join(parts)}</TD></TR>"
        label += "</TABLE>"

        return label


def visualize_graphviz(
    graph: nx.DiGraph,
    *,
    style: Optional[GraphvizStyle] = None,
    figsize: Optional[tuple[int, int]] = None,
    filename: Optional[str] = None,
    show_legend: bool = False,
    orient: Literal["TB", "LR", "BT", "RL"] = "TB",
    min_arg_group_size: Optional[int] = 1,
    return_type: Optional[Literal["graphviz", "html"]] = None,
):
    """Visualize a pipeline graph using Graphviz.

    Args:
        graph: NetworkX directed graph to visualize
        style: Style configuration for the visualization
        figsize: Width and height of the figure in inches
        filename: Path to save the figure (e.g., "output.png", "output.svg")
        show_legend: Whether to show the legend
        orient: Graph orientation - 'TB', 'LR', 'BT', 'RL'
        min_arg_group_size: Minimum number of parameters to group together (default: 1, None to disable)
        return_type: Format to return - 'graphviz' or 'html' (auto-detected if None)

    Returns:
        graphviz.Digraph or IPython.display.HTML object
    """
    try:
        import graphviz
    except ImportError:
        raise ImportError(
            "graphviz package is required for visualization. "
            "Install with: uv add --optional viz graphviz"
        )

    if style is None:
        style = GraphvizStyle()

    # Group parameters if requested
    plot_graph = create_grouped_parameter_graph(graph, min_arg_group_size)

    # Setup graph attributes
    graph_attr: Dict[str, Any] = {
        "rankdir": orient,
        "fontsize": str(style.font_size),
        "fontname": style.font_name,
    }

    if figsize:
        graph_attr["size"] = f"{figsize[0]},{figsize[1]}"
        graph_attr["ratio"] = "fill"

    if style.background_color:
        graph_attr["bgcolor"] = style.background_color

    # Create Graphviz digraph
    digraph = graphviz.Digraph(
        comment="Pipeline Visualization",
        graph_attr=graph_attr,
        node_attr={
            "shape": "rectangle",
            "fontname": style.font_name,
            "fontsize": str(style.font_size),
        },
    )

    # Collect type hints and defaults from all function nodes
    hints: Dict[str, Any] = {}
    defaults: Dict[str, Any] = {}

    for node, data in plot_graph.nodes(data=True):
        if data.get("node_type") == "function":
            node_def: NodeDef = node
            try:
                fn_hints = get_type_hints(node_def.fn)
                hints.update(fn_hints)
            except Exception:
                pass

            # Get parameter defaults
            sig = inspect.signature(node_def.fn)
            for param_name, param in sig.parameters.items():
                if param.default is not _empty:
                    defaults[param_name] = param.default

    # Add nodes to the graph
    input_nodes = []
    function_nodes = []
    grouped_args_nodes = []

    for node, data in plot_graph.nodes(data=True):
        node_type = data.get("node_type", "input")
        label = _generate_node_label(node, node_type, hints, defaults)

        if node_type == "input":
            input_nodes.append(node)
            digraph.node(
                str(id(node)),  # Use id for unique node identifier
                label=f"<{label}>",
                fillcolor=style.arg_node_color,
                shape="rectangle",
                style="filled,dashed",
            )
        elif node_type == "grouped_args":
            grouped_args_nodes.append(node)
            digraph.node(
                str(id(node)),
                label=f"<{label}>",
                fillcolor=style.grouped_args_node_color,
                shape="rectangle",
                style="filled,solid",
            )
        else:
            function_nodes.append(node)
            digraph.node(
                str(id(node)),
                label=f"<{label}>",
                fillcolor=style.func_node_color,
                shape="box",
                style="filled,rounded",
            )

    # Add edges
    for source, target, data in plot_graph.edges(data=True):
        param_name = data.get("param_name", "")

        # Determine edge color
        source_type = plot_graph.nodes[source].get("node_type", "input")
        if source_type == "input":
            edge_color = style.arg_edge_color or style.arg_node_color
        elif source_type == "grouped_args":
            edge_color = style.grouped_args_edge_color or style.grouped_args_node_color
        else:
            edge_color = style.output_edge_color or style.func_node_color

        digraph.edge(
            str(id(source)),
            str(id(target)),
            label=param_name if param_name != "grouped" else "",
            color=edge_color,
            fontname=style.font_name,
            fontsize=str(style.edge_font_size),
            fontcolor="transparent",  # Hide edge labels
        )

    # Add legend if requested
    if show_legend:
        legend_subgraph = graphviz.Digraph(
            name="cluster_legend",
            graph_attr={
                "label": "Legend",
                "rankdir": orient,
                "fontsize": str(style.legend_font_size),
                "fontcolor": style.font_color,
                "fontname": style.font_name,
                "color": style.legend_border_color,
                "style": "filled",
                "fillcolor": style.legend_background_color,
            },
        )

        legend_items = []

        if input_nodes:
            legend_subgraph.node(
                "legend_input",
                label="Input",
                fillcolor=style.arg_node_color,
                shape="rectangle",
                style="filled,dashed",
            )
            legend_items.append("legend_input")

        if grouped_args_nodes:
            legend_subgraph.node(
                "legend_grouped",
                label="Grouped Inputs",
                fillcolor=style.grouped_args_node_color,
                shape="rectangle",
                style="filled,solid",
            )
            legend_items.append("legend_grouped")

        if function_nodes:
            legend_subgraph.node(
                "legend_function",
                label="Function",
                fillcolor=style.func_node_color,
                shape="box",
                style="filled,rounded",
            )
            legend_items.append("legend_function")

        # Connect legend items invisibly for layout
        for i in range(len(legend_items) - 1):
            legend_subgraph.edge(legend_items[i], legend_items[i + 1], style="invis")

        digraph.subgraph(legend_subgraph)

    # Save to file if requested
    if filename is not None:
        name, extension = filename.rsplit(".", 1)
        digraph.render(name, format=extension, cleanup=True)

    # Determine return type
    if return_type is None:
        # Auto-detect: HTML if in notebook, otherwise graphviz object
        try:
            from IPython import get_ipython

            if get_ipython() is not None:
                return_type = "html"
            else:
                return_type = "graphviz"
        except ImportError:
            return_type = "graphviz"

    if return_type == "html":
        from IPython.display import HTML

        svg_content = digraph._repr_image_svg_xml()
        html_content = (
            f'<div id="svg-container" style="max-width: 100%;">{svg_content}</div>'
            "<style>#svg-container svg {max-width: 100%; height: auto;}</style>"
        )

        return HTML(html_content)

    return digraph
