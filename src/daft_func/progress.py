"""Progress bar implementation with theme-aware multi-row display using tqdm."""

import sys
import warnings
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from daft_func.theme import detect_theme, is_jupyter

# Suppress tqdm experimental warnings for rich
warnings.filterwarnings("ignore", message="rich is experimental/alpha")


@dataclass
class ThemeColors:
    """Color scheme for progress display based on theme."""

    pending: str
    executing: str
    cached: str
    completed: str
    bar_complete: str
    bar_finished: str
    text: str
    text_dim: str

    @classmethod
    def for_theme(cls, theme: str) -> "ThemeColors":
        """Create color scheme for given theme.

        Args:
            theme: "dark" or "light"

        Returns:
            ThemeColors instance
        """
        if theme == "light":
            # Darker colors for light background
            return cls(
                pending="dim white",
                executing="bold blue",
                cached="bold yellow",
                completed="bold green",
                bar_complete="blue",
                bar_finished="green",
                text="black",
                text_dim="dim black",
            )
        else:
            # Bright colors for dark background
            return cls(
                pending="dim white",
                executing="bold cyan",
                cached="bold yellow",
                completed="bold bright_green",
                bar_complete="cyan",
                bar_finished="bright_green",
                text="white",
                text_dim="dim white",
            )


@dataclass
class ProgressConfig:
    """Configuration for progress display."""

    enabled: bool = True
    theme: Optional[str] = None  # "dark", "light", or None for auto-detect
    show_cache_indicators: bool = True
    show_timing: bool = True


@dataclass
class NodeProgressBar:
    """Multi-row progress bar showing all pipeline nodes simultaneously."""

    config: ProgressConfig
    theme_colors: ThemeColors
    items_per_node: Dict[str, int] = field(default_factory=dict)
    node_progress: Dict[str, "tqdm"] = field(default_factory=dict)  # type: ignore
    node_status: Dict[str, str] = field(default_factory=dict)
    node_cached: Dict[str, bool] = field(default_factory=dict)
    node_times: Dict[str, float] = field(default_factory=dict)
    _started: bool = False
    _in_jupyter: bool = False

    def __post_init__(self):
        """Initialize tqdm variant based on environment."""
        self._in_jupyter = is_jupyter()

    def initialize_nodes(self, node_names: List[str], items_count: int = 1):
        """Initialize progress display with all nodes.

        Args:
            node_names: List of node names in execution order
            items_count: Number of items to process (1 for single-item runs)
        """
        if not self.config.enabled or self._started:
            return

        # Import appropriate tqdm variant
        if self._in_jupyter:
            from tqdm.notebook import tqdm
        else:
            try:
                # Suppress experimental warning for tqdm.rich
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    from tqdm.rich import tqdm
            except ImportError:
                from tqdm import tqdm

        # Store items count for each node
        for node_name in node_names:
            self.items_per_node[node_name] = items_count

        # Create progress bar for each node
        for node_name in node_names:
            # Initial status
            self.node_status[node_name] = "pending"
            self.node_cached[node_name] = False

            # Create progress bar
            desc = self._format_description(node_name, "pending")

            # Different formatting for Jupyter vs CLI
            if self._in_jupyter:
                # Jupyter: simpler format, let tqdm.notebook handle the display
                pbar = tqdm(
                    total=items_count,
                    desc=desc,
                    leave=True,
                )
            else:
                # CLI: use tqdm.rich with custom format
                pbar = tqdm(
                    total=items_count,
                    desc=desc,
                    leave=True,
                    bar_format="{desc}: {percentage:3.0f}%|{bar}| {n}/{total} {postfix}",
                    colour="blue",
                    file=sys.stdout,
                    dynamic_ncols=True,
                )

            self.node_progress[node_name] = pbar

        self._started = True

    def _format_description(
        self, node_name: str, status: str, cached: bool = False
    ) -> str:
        """Format node description with status icon.

        Args:
            node_name: Node name
            status: Node status (pending, executing, completed)
            cached: Whether result was cached

        Returns:
            Formatted description string
        """
        # Status icons - use consistent 2-character width
        if status == "completed":
            icon = "⚡" if cached else "✓"
        elif status == "executing":
            icon = "⚙"
        else:  # pending
            icon = "⏸"

        # Calculate max node name length for proper padding
        if self.items_per_node:
            max_len = max(len(name) for name in self.items_per_node.keys())
        else:
            max_len = 20

        # Add extra space to ensure proper alignment
        # Format: "icon name      " with consistent total width
        # Using 2 chars for icon, 1 space, then padded name
        total_width = max_len + 2  # icon + space + name padding
        formatted = f"{icon} {node_name}".ljust(total_width)

        return formatted

    def start_node(self, name: str):
        """Mark a node as currently executing.

        Args:
            name: Node name
        """
        if not self.config.enabled or not self._started:
            return

        if name in self.node_progress:
            self.node_status[name] = "executing"
            desc = self._format_description(name, "executing")
            self.node_progress[name].set_description(desc)
            self.node_progress[name].refresh()

    def update_node_progress(self, name: str, completed: int, total: int):
        """Update progress for a node (for multi-item runs).

        Args:
            name: Node name
            completed: Number of items completed
            total: Total number of items
        """
        if not self.config.enabled or not self._started:
            return

        if name in self.node_progress:
            # Update to current completion
            current = self.node_progress[name].n
            if completed > current:
                self.node_progress[name].update(completed - current)
            self.node_progress[name].refresh()

    def complete_node(
        self,
        name: str,
        execution_time: float = 0.0,
        cached: bool = False,
    ):
        """Mark a node as completed.

        Args:
            name: Node name
            execution_time: Execution time in seconds
            cached: Whether result was loaded from cache
        """
        if not self.config.enabled or not self._started:
            return

        if name in self.node_progress:
            # Update to 100%
            total = self.items_per_node.get(name, 1)
            current = self.node_progress[name].n
            if current < total:
                self.node_progress[name].update(total - current)

            # Update status
            self.node_status[name] = "completed"
            self.node_cached[name] = cached
            self.node_times[name] = execution_time

            # Update description with completion icon
            desc = self._format_description(name, "completed", cached)
            self.node_progress[name].set_description(desc)

            # Add timing/cache info as postfix
            postfix = {}
            if cached and self.config.show_cache_indicators:
                postfix["status"] = "⚡ cached"
            elif self.config.show_timing and execution_time > 0:
                postfix["time"] = f"{execution_time:.2f}s"

            if postfix:
                self.node_progress[name].set_postfix(postfix)

            self.node_progress[name].refresh()

    def stop(self):
        """Stop the progress display."""
        if not self._started:
            return

        # Close all progress bars
        for pbar in self.node_progress.values():
            pbar.close()

        self._started = False


def create_progress_bar(
    config: Optional[ProgressConfig] = None,
) -> Optional[NodeProgressBar]:
    """Create a progress bar with theme detection.

    Args:
        config: Progress configuration (None uses defaults)

    Returns:
        NodeProgressBar instance or None if progress disabled
    """
    if config is None:
        config = ProgressConfig()

    if not config.enabled:
        return None

    # Detect theme
    theme_info = detect_theme(config.theme)
    theme_colors = ThemeColors.for_theme(theme_info["mode"])

    return NodeProgressBar(
        config=config,
        theme_colors=theme_colors,
    )
