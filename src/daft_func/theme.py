"""Theme detection for Jupyter and terminal environments."""

import os
from typing import Literal, Optional, TypedDict


class ThemeInfo(TypedDict):
    """Theme information."""

    mode: Literal["dark", "light"]
    detected: bool


def detect_jupyter_theme() -> Optional[Literal["dark", "light"]]:
    """Detect Jupyter notebook theme (dark/light).

    Returns:
        "dark" or "light" if detected, None if not in Jupyter or cannot detect
    """
    try:
        # Check if we're in a Jupyter environment
        ipython = get_ipython()  # type: ignore

        # Check if running in VSCode (IPython has vscode attribute)
        if hasattr(ipython, "config"):
            # VSCode Jupyter notebooks have specific config
            config_str = str(ipython.config)
            if "vscode" in config_str.lower() or os.getenv("VSCODE_PID"):
                # In VSCode, check the TERM_PROGRAM env var
                # VSCode sets this to "vscode"
                # For now, we'll default to dark as it's most common
                # The user can override with explicit theme setting
                # Note: VSCode's Jupyter output area always has white background
                # regardless of theme, so we default to light for better contrast
                return "light"

        # Google Colab detection
        if "COLAB_GPU" in os.environ or "COLAB_TPU_ADDR" in os.environ:
            return "light"  # Colab default is light

        # Try to detect JupyterLab theme
        # JupyterLab sets various env vars we can check
        jupyter_config_dir = os.getenv("JUPYTER_CONFIG_DIR", "")
        if jupyter_config_dir:
            # In JupyterLab, we default to light (most common)
            # Users can override with explicit theme
            return None  # Let it fall through to default

        # For other Jupyter environments, we can't reliably detect
        # so return None to use default
        return None

    except (NameError, ImportError):
        # Not in Jupyter
        return None


def detect_terminal_theme() -> Optional[Literal["dark", "light"]]:
    """Detect terminal theme (dark/light).

    Returns:
        "dark" or "light" if detected, None if cannot detect
    """
    # Check COLORFGBG environment variable (set by some terminals)
    # Format: "foreground;background" where 0-7 are dark colors, 8-15 are light
    colorfgbg = os.getenv("COLORFGBG", "")
    if colorfgbg:
        try:
            parts = colorfgbg.split(";")
            if len(parts) >= 2:
                bg_color = int(parts[-1])
                # Background colors 0-7 are dark, 8-15 are light (in 16-color mode)
                # Background color 15 or 7 is usually white/light
                if bg_color >= 15 or bg_color == 7:
                    return "light"
                elif bg_color <= 8:
                    return "dark"
        except (ValueError, IndexError):
            pass

    # Check for VSCode integrated terminal
    if os.getenv("TERM_PROGRAM") == "vscode":
        # VSCode doesn't expose terminal theme directly
        # Check if VSCODE_THEME_NAME is set (custom)
        theme_name = os.getenv("VSCODE_THEME_NAME", "").lower()
        if "light" in theme_name:
            return "light"
        elif "dark" in theme_name:
            return "dark"
        # Default to dark for VSCode (most common)
        return "dark"

    # Check iTerm2
    if os.getenv("TERM_PROGRAM") == "iTerm.app":
        # iTerm2 doesn't expose theme info via env vars
        # Default to dark (most common)
        return "dark"

    # For other terminals, we can't reliably detect
    return None


def detect_theme(explicit_theme: Optional[str] = None) -> ThemeInfo:
    """Detect the current environment theme.

    Args:
        explicit_theme: Explicit theme override ("dark" or "light")

    Returns:
        ThemeInfo dict with mode and whether it was detected or defaulted
    """
    # If explicit theme provided, use it
    if explicit_theme in ("dark", "light"):
        return ThemeInfo(mode=explicit_theme, detected=True)

    # Try Jupyter detection first
    jupyter_theme = detect_jupyter_theme()
    if jupyter_theme:
        return ThemeInfo(mode=jupyter_theme, detected=True)

    # Try terminal detection
    terminal_theme = detect_terminal_theme()
    if terminal_theme:
        return ThemeInfo(mode=terminal_theme, detected=True)

    # Default to dark (most common for developers)
    return ThemeInfo(mode="dark", detected=False)


def is_jupyter() -> bool:
    """Check if running in Jupyter environment."""
    try:
        get_ipython()  # type: ignore
        return True
    except NameError:
        return False
