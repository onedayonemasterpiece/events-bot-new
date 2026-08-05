#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

OLD = '''        for path in sorted(selected):
            if not is_text_path(path):
                continue
            for dep in literal_dependencies(source, path, tracked):
'''

NEW = '''        for path in sorted(selected):
            if not is_text_path(path):
                continue
            # Documentation, research records and agent instructions may link
            # to historical or neighbouring implementation files. Such links
            # are evidence/cross-references, not executable dependency edges.
            # Runtime code, tests, workflows and configs still drive literal
            # closure, so invoked scripts and fixtures remain fail-closed.
            if path.startswith(("docs/", ".codex/", ".agent/", ".agents/", ".claude/")):
                continue
            if path in {"README.md", "CHANGELOG.md"}:
                continue
            for dep in literal_dependencies(source, path, tracked):
'''


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    text = args.path.read_text(encoding="utf-8")
    count = text.count(OLD)
    if count != 1:
        raise SystemExit(f"expected exactly one literal-closure block, found {count}")
    args.path.write_text(text.replace(OLD, NEW), encoding="utf-8")
    print(f"patched_documentation_literal_closure={args.path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
