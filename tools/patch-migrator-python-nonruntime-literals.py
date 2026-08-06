#!/usr/bin/env python3
"""Patch the standalone migrator to ignore non-runtime Python path mentions.

The transfer closure must retain literal paths used by executable code, but it
must not treat prose in module/class/function docstrings or comments as runtime
dependencies.  The concrete false edge was:

    tests/_helpers/no_network.py docstring
        -> tests/conftest.py
        -> main.py

No Region Talk product module depended on ``main.py`` through that chain.
"""
from __future__ import annotations

import sys
from pathlib import Path


OLD = '''def literal_dependencies(source_root: Path, path: str, tracked: set[str]) -> set[str]:
    text = read_text(source_root / path)
    if text is None:
        return set()
    found: set[str] = set()
'''

NEW = '''def _python_runtime_literal_text(text: str, path: str) -> str:
    """Blank Python comments/docstrings while preserving executable strings."""

    try:
        tree = ast.parse(text, filename=path)
    except SyntaxError:
        return text

    lines = text.splitlines(keepends=True)
    docstring_lines: set[int] = set()
    owners = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
    for owner in ast.walk(tree):
        if not isinstance(owner, owners) or not getattr(owner, "body", None):
            continue
        first = owner.body[0]
        if not (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            continue
        start = max(1, int(getattr(first, "lineno", 1)))
        end = max(start, int(getattr(first, "end_lineno", start)))
        docstring_lines.update(range(start, end + 1))

    # Comments can contain documentation paths too.  Blanking the comment tail
    # keeps line endings and executable string literals intact.
    for line_number, line in enumerate(lines, start=1):
        if line_number in docstring_lines:
            lines[line_number - 1] = "\\n" if line.endswith("\\n") else ""
            continue
        quote = None
        escaped = False
        comment_at = None
        for index, char in enumerate(line):
            if escaped:
                escaped = False
                continue
            if char == "\\\\":
                escaped = True
                continue
            if quote:
                if char == quote:
                    quote = None
                continue
            if char in {"'", '"'}:
                quote = char
                continue
            if char == "#":
                comment_at = index
                break
        if comment_at is not None:
            newline = "\\n" if line.endswith("\\n") else ""
            lines[line_number - 1] = line[:comment_at] + newline
    return "".join(lines)


def literal_dependencies(source_root: Path, path: str, tracked: set[str]) -> set[str]:
    text = read_text(source_root / path)
    if text is None:
        return set()
    if PurePosixPath(path).suffix == ".py":
        text = _python_runtime_literal_text(text, path)
    found: set[str] = set()
'''


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: patch-migrator-python-nonruntime-literals.py MIGRATOR")
    path = Path(sys.argv[1])
    text = path.read_text(encoding="utf-8")
    if NEW in text:
        print(f"already_patched_python_nonruntime_literals={path}")
        return 0
    if OLD not in text:
        raise SystemExit("literal_dependencies anchor not found")
    path.write_text(text.replace(OLD, NEW, 1), encoding="utf-8")
    print(f"patched_python_nonruntime_literals={path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
