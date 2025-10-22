"""
Simple Progress Bar Demo

This script demonstrates a progress bar that works both in Jupyter and CLI.
Run with: uv run examples/progress_bar_demo.py
"""

import time
from typing import Any, Dict, List

# Import Rich for both environments
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)

from daft_func import Pipeline, Runner, func

# Check if we're in Jupyter
try:
    get_ipython()
    IN_JUPYTER = True
except NameError:
    IN_JUPYTER = False


# ============================================================================
# Progress Bar - Rich-based (works in both Jupyter and CLI!)
# ============================================================================

class RichProgressBar:
    """Rich-based progress bar that works in both Jupyter and CLI."""
    
    def __init__(self):
        self.total_nodes = 0
        self.completed_nodes = 0
        self.current_node = ''
        self.task_id = None
        
        # Create Rich progress bar with beautiful styling
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(complete_style="green", finished_style="bright_green"),
            TaskProgressColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            TextColumn("[dim]{task.fields[current_node]}"),
        )
    
    def set_total(self, total: int):
        self.total_nodes = total
        self.progress.start()
        self.task_id = self.progress.add_task(
            "Pipeline",
            total=total,
            current_node=""
        )
    
    def reset(self):
        if self.task_id is not None:
            self.progress.reset(self.task_id)
        self.completed_nodes = 0
        self.current_node = ''
    
    def start_node(self, node_name: str):
        self.current_node = node_name
        if self.task_id is not None:
            self.progress.update(
                self.task_id,
                current_node=f"Running: [yellow]{node_name}[/]"
            )
    
    def complete_node(self):
        self.completed_nodes += 1
        self.current_node = ''
        if self.task_id is not None:
            self.progress.update(
                self.task_id,
                advance=1,
                current_node="[green]✓ Completed[/]" if self.completed_nodes == self.total_nodes else ""
            )
            
            # Stop progress when done
            if self.completed_nodes == self.total_nodes:
                self.progress.stop()


def create_progress_bar():
    """Create Rich progress bar for both environments."""
    return RichProgressBar()


# ============================================================================
# Progress-Aware Runner
# ============================================================================

class ProgressRunner(Runner):
    """Runner that updates a progress bar widget."""
    
    def __init__(self, *args, progress_bar=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.progress_bar = progress_bar
    
    def _run_single(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Execute DAG for a single item with progress tracking."""
        pipeline = self.pipeline
        outputs = dict(inputs)
        order = pipeline.topo(inputs)

        for node in order:
            # Update progress bar - node starting
            if self.progress_bar:
                self.progress_bar.start_node(node.meta.output_name)
            
            # Get kwargs for this node
            kwargs = {p: outputs[p] for p in node.params if p in outputs}
            
            # Execute node (simplified - no caching for demo)
            res = node.fn(**kwargs)
            outputs[node.meta.output_name] = res
            
            # Update progress bar - node completed
            if self.progress_bar:
                self.progress_bar.complete_node()

        return outputs


# ============================================================================
# Test Pipeline
# ============================================================================

@func(output="data")
def load_data(source: str) -> List[int]:
    """Load data from source."""
    if IN_JUPYTER:
        print("Loading data...")
    time.sleep(1)
    return [1, 2, 3, 4, 5]


@func(output="cleaned")
def clean_data(data: List[int]) -> List[int]:
    """Clean the data."""
    if IN_JUPYTER:
        print("Cleaning data...")
    time.sleep(1.5)
    return [x for x in data if x > 0]



@func(output="processed")
def process_data(cleaned: List[int]) -> List[float]:
    """Process the data."""
    if IN_JUPYTER:
        print("Processing data...")
    time.sleep(1.2)
    return [float(x) * 2.0 for x in cleaned]


@func(output="features")
def extract_features(processed: List[float]) -> List[float]:
    """Extract features."""
    if IN_JUPYTER:
        print("Extracting features...")
    time.sleep(1)
    return [x / max(processed) for x in processed]


@func(output="result")
def compute_result(features: List[float]) -> float:
    """Compute final result."""
    if IN_JUPYTER:
        print("Computing result...")
    time.sleep(0.8)
    return sum(features) / len(features)



# ============================================================================
# Main Demo
# ============================================================================

def main():
    """Run the demo."""
    if IN_JUPYTER:
        print("Creating pipeline...")
    
    pipeline = Pipeline(
        functions=[load_data, clean_data, process_data, extract_features, compute_result]
    )
    
    if IN_JUPYTER:
        print(f"Pipeline has {len(pipeline.nodes)} nodes\n")
    
    # Create progress bar
    progress_bar = create_progress_bar()
    progress_bar.set_total(len(pipeline.nodes))
    
    # Display widget in Jupyter
    if IN_JUPYTER:
        from IPython.display import display
        print("Running pipeline...\n")
        display(progress_bar)
    
    # Create runner with progress tracking
    runner = ProgressRunner(pipeline=pipeline, mode="sequential", progress_bar=progress_bar)
    
    # Run pipeline
    inputs = {"source": "test_data"}
    result = runner.run(inputs=inputs)
    
    print(f"\nFinal result: {result['result']:.3f}")
    if IN_JUPYTER:
        print("Done!")


if __name__ == "__main__":
    main()
