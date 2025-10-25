"""Script to update Runner usage patterns in all files."""

import re
from pathlib import Path


def update_file(filepath: Path) -> bool:
    """Update a single file's Runner usage patterns.

    Returns True if file was modified.
    """
    content = filepath.read_text()
    original = content

    if "Runner(" not in content:
        return False

    print(f"Processing: {filepath}")

    # Step 1: Handle various runner.run() patterns
    # Pattern 1a: runner.run(inputs= -> runner.run(pipeline, inputs=
    content = re.sub(r"runner\.run\(inputs=", "runner.run(pipeline, inputs=", content)

    # Pattern 1b: runner.run( with no inputs keyword -> runner.run(pipeline, inputs=
    # Look for: runner.run({...})
    content = re.sub(r"runner\.run\(\{", "runner.run(pipeline, inputs={", content)

    # Pattern 1c: runner1.run, runner2.run etc
    content = re.sub(r"runner1\.run\(inputs=", "runner1.run(pipeline, inputs=", content)
    content = re.sub(r"runner2\.run\(inputs=", "runner2.run(pipeline, inputs=", content)
    content = re.sub(
        r"runner_\w+\.run\(inputs=",
        lambda m: m.group(0).replace("inputs=", "pipeline, inputs="),
        content,
    )

    # Step 2: Remove pipeline= from Runner(...) constructor calls
    # Handle multiline patterns with DOTALL flag

    # Case 1: Runner(\n        pipeline=pipeline,\n        ...) -> Runner(\n        ...)
    # Match pipeline=pipeline, with optional whitespace and newlines
    content = re.sub(
        r"Runner\(\s*pipeline=pipeline,\s*",
        "Runner(\n        ",
        content,
        flags=re.DOTALL,
    )

    # Case 2: Runner(pipeline=pipeline, ...) -> Runner(...) (single line)
    content = re.sub(r"Runner\(pipeline=pipeline,\s*", "Runner(", content)

    # Case 3: Runner(pipeline=pipeline) at end -> Runner()
    content = re.sub(
        r"Runner\(pipeline=pipeline\)(?=\s*$|\s*\n)",
        "Runner()",
        content,
        flags=re.MULTILINE,
    )

    # Case 4: Runner(pipeline=pipeline) followed by other code -> Runner()
    content = re.sub(r"Runner\(pipeline=pipeline\)(?=\s)", "Runner()", content)

    if content != original:
        filepath.write_text(content)
        print(f"  ✓ Updated: {filepath}")
        return True
    else:
        print(f"  - No changes: {filepath}")
        return False


def main():
    """Update all test and example files."""
    root = Path("/Users/giladrubin/python_workspace/daft-func")

    # Find all Python files in tests/ and examples/
    files_to_update = []
    files_to_update.extend(root.glob("tests/*.py"))
    files_to_update.extend(root.glob("examples/**/*.py"))

    modified_count = 0
    for filepath in sorted(files_to_update):
        if update_file(filepath):
            modified_count += 1

    print(f"\n✓ Updated {modified_count} files")


if __name__ == "__main__":
    main()
