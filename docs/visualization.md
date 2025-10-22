# Visualization Feature

## Overview

The daft-func library now includes Graphviz-based visualization for DAG pipelines, inspired by the pipefunc library. This feature allows you to visualize your computational pipelines with type annotations, dependency relationships, and default values.

## Installation

Install the optional visualization dependencies:

```bash
uv add networkx graphviz
```

Or they're already included if you installed from the updated project.

## Usage

### Basic Visualization

```python
from daft_func import daft_func
from daft_func.decorator import get_pipeline

# Define your pipeline
@daft_func(output="doubled")
def double(x: int) -> int:
    return x * 2

@daft_func(output="result")
def add_ten(doubled: int, offset: int = 5) -> int:
    return doubled + offset

# Get the pipeline and visualize
pipeline = get_pipeline()
viz = pipeline.visualize(
    orient="LR",  # Left to right layout
    show_legend=True
)

# In Jupyter notebooks, this displays inline
# Otherwise, save to file:
viz.render("my_pipeline", format="png", cleanup=True)
```

### Visualization Options

The `visualize()` method accepts several parameters:

- **orient**: Graph orientation - 'TB' (top-bottom), 'LR' (left-right), 'BT' (bottom-top), 'RL' (right-left)
- **show_legend**: Whether to display the legend (default: False)
- **min_arg_group_size**: Minimum number of parameters to group together (default: 1, None to disable)
- **figsize**: Tuple of (width, height) for the figure size
- **filename**: Path to save the visualization (e.g., "output.png", "output.svg")
- **style**: Custom GraphvizStyle object for advanced styling
- **return_type**: 'graphviz' or 'html' (auto-detected in notebooks)

### Advanced Styling

```python
from daft_func.visualization import GraphvizStyle

# Create custom style
custom_style = GraphvizStyle(
    arg_node_color="#FFE6E6",
    func_node_color="#E6F3FF",
    font_size=14,
    font_name="Arial"
)

viz = pipeline.visualize(
    style=custom_style,
    orient="TB",
    show_legend=True
)
```

### Parameter Grouping

When a function has multiple input parameters used exclusively by that function, they can be grouped together in the visualization for clarity:

```python
@func(output="result")
def complex_process(a: int, b: str, c: float, d: bool) -> dict:
    return {"sum": a + c, "text": b, "flag": d}

pipeline = Pipeline(functions=[complex_process])

# With grouping (default: min_arg_group_size=1)
# Parameters a, b, c, d will be grouped in a table since they're all exclusive to this function
viz = pipeline.visualize(min_arg_group_size=2)

# Without grouping
viz = pipeline.visualize(min_arg_group_size=None)
```

The grouping only applies to parameters that are used by a single function. If a parameter is shared by multiple functions, it remains as a separate node.

## Visual Elements

The visualization includes:

1. **Input Parameters** (Green, Dashed Boxes)
   - Shows parameter names with type annotations
   - Displays default values if present
   - Example: `x : int` or `offset : int = 5`

2. **Grouped Input Parameters** (Green, Solid Boxes with Table)
   - Multiple parameters grouped together when used exclusively by one function
   - Each parameter shown as a table row with type and default value
   - Only appears when `min_arg_group_size` threshold is met

3. **Function Nodes** (Blue, Rounded Boxes)
   - Function name as header
   - Output name with return type
   - Example: `double` → `doubled : int`

4. **Dependency Edges**
   - Show connections between nodes
   - Color-coded by source type (green for inputs, blue for function outputs)
   - Edge labels are hidden for cleaner visualization

5. **Legend** (Optional)
   - Shows node type meanings
   - Useful for complex pipelines

## Examples

See `examples/visualize_demo.py` for a complete working example:

```bash
uv run python examples/visualize_demo.py
```

This creates a simple two-node pipeline and saves the visualization to PNG and SVG formats.

## Implementation Details

### Architecture

The visualization feature consists of three main components:

1. **build_graph()**: Converts a Pipeline into a NetworkX directed graph
   - Extracts nodes and dependencies from the pipeline structure
   - Creates both input parameter nodes and function nodes
   - No execution required - purely structural visualization

2. **GraphvizStyle**: Dataclass for styling configuration
   - Node colors for different types
   - Font settings
   - Legend appearance
   - Fully customizable

3. **visualize_graphviz()**: Renders the graph using Graphviz
   - Generates HTML-like labels with type annotations
   - Supports multiple output formats (PNG, SVG, PDF, etc.)
   - Auto-detects Jupyter notebooks for inline display

### Type Annotation Support

The visualization automatically extracts and displays:
- Parameter type hints from function signatures
- Return type annotations
- Default parameter values

Type hints are displayed in a readable format (e.g., `List[int]` instead of `typing.List[int]`).

## Refactoring: Registry → Pipeline

As part of this feature, we renamed the core concept from "registry" to "pipeline" throughout the codebase:

- `DagRegistry` → `Pipeline`
- `get_registry()` → `get_pipeline()`
- All references updated in tests and examples

This better reflects the library's purpose and aligns with the visualization terminology.

## Tests

The visualization feature includes comprehensive tests in `tests/test_visualization.py`:

- Graph building from simple and chained pipelines
- Node and edge structure validation
- Graphviz object creation
- Type annotation rendering
- Orientation parameter support
- Integration with @daft_func decorator

Run tests with:

```bash
uv run pytest tests/test_visualization.py -v
```

