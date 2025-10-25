"""Script to update Runner usage patterns in markdown files."""

import re
from pathlib import Path


def update_markdown(filepath: Path) -> bool:
    """Update Runner usage in markdown code blocks.

    Returns True if file was modified.
    """
    content = filepath.read_text()
    original = content

    if "Runner(" not in content:
        return False

    print(f"Processing: {filepath}")

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

    # Special case: "Runner(pipeline," without the "=pipeline" part (might be variable)
    # Don't touch these as they might be intentional variable names

    if content != original:
        filepath.write_text(content)
        print(f"  ✓ Updated: {filepath}")
        return True
    else:
        print(f"  - No changes: {filepath}")
        return False


def main():
    """Update all markdown files."""
    root = Path("/Users/giladrubin/python_workspace/daft-func")

    # Find all markdown files
    files_to_update = []
    files_to_update.extend(root.glob("*.md"))
    files_to_update.extend(root.glob("docs/**/*.md"))

    modified_count = 0
    for filepath in sorted(files_to_update):
        if update_markdown(filepath):
            modified_count += 1

    print(f"\n✓ Updated {modified_count} markdown files")


if __name__ == "__main__":
    main()
