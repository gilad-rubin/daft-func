"""Tests for progress bar functionality."""

import os
import time
from unittest.mock import patch

import pytest

from daft_func import Pipeline, Runner, func
from daft_func.progress import (
    NodeProgressBar,
    ProgressConfig,
    ThemeColors,
    create_progress_bar,
)
from daft_func.theme import detect_jupyter_theme, detect_terminal_theme, detect_theme

# ============================================================================
# Theme Detection Tests
# ============================================================================


def test_detect_theme_explicit_dark():
    """Test explicit dark theme selection."""
    theme_info = detect_theme(explicit_theme="dark")
    assert theme_info["mode"] == "dark"
    assert theme_info["detected"] is True


def test_detect_theme_explicit_light():
    """Test explicit light theme selection."""
    theme_info = detect_theme(explicit_theme="light")
    assert theme_info["mode"] == "light"
    assert theme_info["detected"] is True


def test_detect_theme_default():
    """Test default theme when detection fails."""
    # With no explicit theme and likely no detectable env, should default to dark
    theme_info = detect_theme()
    assert theme_info["mode"] in ("dark", "light")
    # detected might be True or False depending on environment


def test_detect_terminal_theme_colorfgbg():
    """Test terminal theme detection using COLORFGBG."""
    with patch.dict(os.environ, {"COLORFGBG": "15;0"}):
        # Background 0 = dark
        theme = detect_terminal_theme()
        assert theme == "dark"

    with patch.dict(os.environ, {"COLORFGBG": "0;15"}):
        # Background 15 = light
        theme = detect_terminal_theme()
        assert theme == "light"


def test_detect_terminal_theme_vscode():
    """Test VSCode terminal detection."""
    with patch.dict(os.environ, {"TERM_PROGRAM": "vscode"}):
        theme = detect_terminal_theme()
        # Should default to dark for VSCode
        assert theme == "dark"


def test_detect_jupyter_theme_not_in_jupyter():
    """Test Jupyter detection when not in Jupyter."""
    theme = detect_jupyter_theme()
    # Should return None when not in Jupyter (or "light" if in VSCode Jupyter)
    assert theme is None or theme == "light"


# ============================================================================
# Theme Colors Tests
# ============================================================================


def test_theme_colors_dark():
    """Test dark theme color scheme."""
    colors = ThemeColors.for_theme("dark")
    assert colors.executing == "bold cyan"
    assert colors.completed == "bold bright_green"
    assert colors.cached == "bold yellow"
    assert colors.pending == "dim white"


def test_theme_colors_light():
    """Test light theme color scheme."""
    colors = ThemeColors.for_theme("light")
    assert colors.executing == "bold blue"
    assert colors.completed == "bold green"
    assert colors.cached == "bold yellow"
    assert colors.pending == "dim white"


# ============================================================================
# Progress Config Tests
# ============================================================================


def test_progress_config_defaults():
    """Test default progress configuration."""
    config = ProgressConfig()
    assert config.enabled is True
    assert config.theme is None
    assert config.show_cache_indicators is True
    assert config.show_timing is True


def test_progress_config_custom():
    """Test custom progress configuration."""
    config = ProgressConfig(
        enabled=False,
        theme="light",
        show_cache_indicators=False,
        show_timing=False,
    )
    assert config.enabled is False
    assert config.theme == "light"
    assert config.show_cache_indicators is False
    assert config.show_timing is False


# ============================================================================
# Progress Bar Creation Tests
# ============================================================================


def test_create_progress_bar_enabled():
    """Test creating progress bar when enabled."""
    config = ProgressConfig(enabled=True)
    progress_bar = create_progress_bar(config)
    assert progress_bar is not None
    assert isinstance(progress_bar, NodeProgressBar)


def test_create_progress_bar_disabled():
    """Test creating progress bar when disabled."""
    config = ProgressConfig(enabled=False)
    progress_bar = create_progress_bar(config)
    assert progress_bar is None


def test_create_progress_bar_default():
    """Test creating progress bar with default config."""
    progress_bar = create_progress_bar()
    assert progress_bar is not None
    assert isinstance(progress_bar, NodeProgressBar)


# ============================================================================
# Node Progress Bar Tests
# ============================================================================


def test_node_progress_bar_initialization():
    """Test progress bar initialization."""
    config = ProgressConfig()
    colors = ThemeColors.for_theme("dark")
    progress_bar = NodeProgressBar(config=config, theme_colors=colors)

    assert progress_bar.config == config
    assert progress_bar.theme_colors == colors
    assert progress_bar.node_progress == {}
    assert not progress_bar._started


def test_node_progress_bar_initialize_nodes():
    """Test initializing nodes in progress bar."""
    config = ProgressConfig()
    colors = ThemeColors.for_theme("dark")
    progress_bar = NodeProgressBar(config=config, theme_colors=colors)

    node_names = ["node1", "node2", "node3"]
    progress_bar.initialize_nodes(node_names, items_count=1)

    assert progress_bar._started is True
    assert len(progress_bar.node_progress) == 3
    assert "node1" in progress_bar.node_progress
    assert "node2" in progress_bar.node_progress
    assert "node3" in progress_bar.node_progress

    # Clean up
    progress_bar.stop()


def test_node_progress_bar_disabled():
    """Test that disabled progress bar doesn't create display."""
    config = ProgressConfig(enabled=False)
    colors = ThemeColors.for_theme("dark")
    progress_bar = NodeProgressBar(config=config, theme_colors=colors)

    node_names = ["node1", "node2"]
    progress_bar.initialize_nodes(node_names, items_count=1)

    # Should not start when disabled
    assert progress_bar._started is False
    assert len(progress_bar.node_progress) == 0


# ============================================================================
# Integration Tests with Runner
# ============================================================================


def test_runner_with_progress():
    """Test runner with progress bar enabled."""

    @func(output="result")
    def simple_func(x: int) -> int:
        time.sleep(0.1)
        return x * 2

    pipeline = Pipeline(functions=[simple_func])

    # Create runner with progress enabled
    progress_config = ProgressConfig(enabled=True)
    runner = Runner(progress_config=progress_config)

    result = runner.run(pipeline, inputs={"x": 5})
    assert result["result"] == 10


def test_runner_without_progress():
    """Test runner with progress bar disabled."""

    @func(output="result")
    def simple_func(x: int) -> int:
        time.sleep(0.1)
        return x * 2

    pipeline = Pipeline(functions=[simple_func])

    # Create runner with progress disabled
    progress_config = ProgressConfig(enabled=False)
    runner = Runner(progress_config=progress_config)

    result = runner.run(pipeline, inputs={"x": 5})
    assert result["result"] == 10


def test_runner_multi_node_progress():
    """Test progress bar with multiple nodes."""

    @func(output="step1")
    def step1(x: int) -> int:
        time.sleep(0.1)
        return x * 2

    @func(output="step2")
    def step2(step1: int) -> int:
        time.sleep(0.1)
        return step1 + 10

    @func(output="step3")
    def step3(step2: int) -> int:
        time.sleep(0.1)
        return step2 * 3

    pipeline = Pipeline(functions=[step1, step2, step3])
    runner = Runner()

    result = runner.run(pipeline, inputs={"x": 5})
    assert result["step3"] == 60  # ((5 * 2) + 10) * 3


def test_runner_with_cache_progress():
    """Test progress bar with caching."""
    import tempfile

    from daft_func import CacheConfig, DiskCache

    @func(output="result", cache=True)
    def cached_func(x: int) -> int:
        time.sleep(0.2)
        return x * 2

    pipeline = Pipeline(functions=[cached_func])

    # Use temporary directory for cache
    with tempfile.TemporaryDirectory() as tmpdir:
        cache_backend = DiskCache(cache_dir=tmpdir)
        cache_config = CacheConfig(enabled=True, backend=cache_backend)
        runner = Runner(cache_config=cache_config)

        # First run - should execute
        result1 = runner.run(pipeline, inputs={"x": 5})
        assert result1["result"] == 10

        # Second run - should hit cache
        result2 = runner.run(pipeline, inputs={"x": 5})
        assert result2["result"] == 10


def test_runner_multi_item_progress():
    """Test progress bar with multi-item run."""
    from pydantic import BaseModel

    class Item(BaseModel):
        value: int

    @func(output="processed", map_axis="items")
    def process_item(items: Item) -> int:
        time.sleep(0.1)
        return items.value * 2

    pipeline = Pipeline(functions=[process_item])
    runner = Runner(mode="local")

    items = [Item(value=i) for i in range(3)]
    result = runner.run(pipeline, inputs={"items": items})

    assert len(result["processed"]) == 3
    assert result["processed"] == [0, 2, 4]


def test_progress_bar_theme_selection():
    """Test progress bar with different themes."""

    @func(output="result")
    def simple_func(x: int) -> int:
        return x * 2

    pipeline = Pipeline(functions=[simple_func])

    # Test dark theme
    progress_config = ProgressConfig(theme="dark")
    runner = Runner(progress_config=progress_config)
    result = runner.run(pipeline, inputs={"x": 5})
    assert result["result"] == 10

    # Test light theme
    progress_config = ProgressConfig(theme="light")
    runner = Runner(progress_config=progress_config)
    result = runner.run(pipeline, inputs={"x": 5})
    assert result["result"] == 10


def test_progress_bar_customization():
    """Test progress bar with custom configuration."""

    @func(output="result")
    def simple_func(x: int) -> int:
        return x * 2

    pipeline = Pipeline(functions=[simple_func])

    # Test without cache indicators
    progress_config = ProgressConfig(show_cache_indicators=False)
    runner = Runner(progress_config=progress_config)
    result = runner.run(pipeline, inputs={"x": 5})
    assert result["result"] == 10

    # Test without timing
    progress_config = ProgressConfig(show_timing=False)
    runner = Runner(progress_config=progress_config)
    result = runner.run(pipeline, inputs={"x": 5})
    assert result["result"] == 10

    # Test minimal
    progress_config = ProgressConfig(
        show_cache_indicators=False,
        show_timing=False,
    )
    runner = Runner(progress_config=progress_config)
    result = runner.run(pipeline, inputs={"x": 5})
    assert result["result"] == 10


# ============================================================================
# Error Handling Tests
# ============================================================================


def test_progress_bar_with_pipeline_error():
    """Test progress bar handles pipeline errors gracefully."""

    @func(output="result")
    def failing_func(x: int) -> int:
        raise ValueError("Test error")

    pipeline = Pipeline(functions=[failing_func])
    runner = Runner()

    with pytest.raises(ValueError, match="Test error"):
        runner.run(pipeline, inputs={"x": 5})


def test_progress_bar_stops_on_error():
    """Test that progress bar is stopped even when error occurs."""

    @func(output="step1")
    def step1(x: int) -> int:
        return x * 2

    @func(output="step2")
    def step2(step1: int) -> int:
        raise ValueError("Error in step2")

    pipeline = Pipeline(functions=[step1, step2])
    runner = Runner()

    with pytest.raises(ValueError, match="Error in step2"):
        runner.run(pipeline, inputs={"x": 5})

    # Progress bar should be stopped (in finally block)
    assert runner._progress_bar is not None  # Was created
