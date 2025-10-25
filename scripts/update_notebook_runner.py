"""Script to update Runner usage patterns in Jupyter notebooks."""

import json
import re
from pathlib import Path


def update_cell_source(source_lines: list) -> tuple[list, bool]:
    """Update Runner usage in a cell's source lines.

    Returns (updated_lines, modified).
    """
    # Join lines to work with full text
    content = "".join(source_lines)
    original = content

    # Update runner.run calls
    content = re.sub(r"runner\.run\(inputs=", "runner.run(pipeline, inputs=", content)
    content = re.sub(r"runner\.run\(\{", "runner.run(pipeline, inputs={", content)

    # Remove pipeline= from Runner() constructor
    content = re.sub(
        r"Runner\(\s*pipeline=pipeline,\s*",
        "Runner(\n        ",
        content,
        flags=re.DOTALL,
    )
    content = re.sub(r"Runner\(pipeline=pipeline,\s*", "Runner(", content)
    content = re.sub(
        r"Runner\(pipeline=pipeline\)(?=\s*$|\s*\n)",
        "Runner()",
        content,
        flags=re.MULTILINE,
    )
    content = re.sub(r"Runner\(pipeline=pipeline\)(?=\s)", "Runner()", content)

    if content == original:
        return source_lines, False

    # Split back into lines, preserving newlines
    new_lines = []
    for line in content.split("\n"):
        if line or content.endswith("\n"):  # Keep empty lines if original had them
            new_lines.append(line + "\n" if not line.endswith("\n") else line)

    # Remove trailing newline if original didn't have it
    if new_lines and not original.endswith("\n"):
        new_lines[-1] = new_lines[-1].rstrip("\n")

    return new_lines, True


def update_notebook(filepath: Path) -> bool:
    """Update Runner usage in a Jupyter notebook.

    Returns True if file was modified.
    """
    print(f"Processing: {filepath}")

    with open(filepath, "r") as f:
        notebook = json.load(f)

    modified = False
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") == "code":
            source = cell.get("source", [])
            if isinstance(source, list) and any("Runner(" in line for line in source):
                new_source, cell_modified = update_cell_source(source)
                if cell_modified:
                    cell["source"] = new_source
                    modified = True

    if modified:
        with open(filepath, "w") as f:
            json.dump(notebook, f, indent=1, ensure_ascii=False)
        print(f"  ✓ Updated: {filepath}")
        return True
    else:
        print(f"  - No changes: {filepath}")
        return False


def main():
    """Update all Jupyter notebooks."""
    root = Path("/Users/giladrubin/python_workspace/daft-func")

    # Find all notebook files
    files_to_update = list(root.glob("notebooks/**/*.ipynb"))

    modified_count = 0
    for filepath in sorted(files_to_update):
        if update_notebook(filepath):
            modified_count += 1

    print(f"\n✓ Updated {modified_count} notebook files")


if __name__ == "__main__":
    main()
