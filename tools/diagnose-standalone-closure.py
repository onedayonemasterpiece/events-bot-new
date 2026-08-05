#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
from collections import deque
from pathlib import Path


def load_migrator(path: Path):
    spec = importlib.util.spec_from_file_location("rt_migrator", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--migrator", type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    m = load_migrator(args.migrator.resolve())

    tracked_list = m.git_files(source)
    tracked = set(tracked_list)
    roots = sorted(
        path for path in tracked_list
        if m.RT_RE.search(path) and m.is_safe_copy_path(path)
    )
    selected = set(roots)
    reason: dict[str, str] = {path: "explicit-region-talk-path" for path in roots}
    parent: dict[str, tuple[str, str]] = {}

    support = "tests/_helpers/no_network.py"
    if support in tracked and support not in selected:
        selected.add(support)
        reason[support] = "pytest-support"

    module_index = m.build_module_index(tracked_list)
    queue: deque[str] = deque(path for path in sorted(selected) if path.endswith(".py"))

    def add(dep: str, owner: str, edge: str) -> bool:
        if dep not in tracked or dep in selected or not m.is_safe_copy_path(dep):
            return False
        selected.add(dep)
        reason[dep] = edge
        parent[dep] = (owner, edge)
        if dep.endswith(".py"):
            queue.append(dep)
        return True

    while queue:
        owner = queue.popleft()
        deps, _ = m.python_dependencies(source, owner, module_index)
        for dep in sorted(deps):
            add(dep, owner, "python-import")

    changed = True
    while changed:
        changed = False
        for owner in sorted(selected):
            if not m.is_text_path(owner):
                continue
            for dep in sorted(m.literal_dependencies(source, owner, tracked)):
                changed = add(dep, owner, "literal-path-reference") or changed
        while queue:
            owner = queue.popleft()
            deps, _ = m.python_dependencies(source, owner, module_index)
            for dep in sorted(deps):
                changed = add(dep, owner, "python-import") or changed

    target = "main.py"
    chain: list[dict[str, str]] = []
    current = target
    seen: set[str] = set()
    while current in parent and current not in seen:
        seen.add(current)
        owner, edge = parent[current]
        chain.append({"from": owner, "edge": edge, "to": current})
        current = owner
    chain.reverse()

    direct_importers: list[str] = []
    for path in sorted(selected):
        if not path.endswith(".py"):
            continue
        deps, _ = m.python_dependencies(source, path, module_index)
        if target in deps:
            direct_importers.append(path)

    report = {
        "roots_count": len(roots),
        "selected_count": len(selected),
        "main_selected": target in selected,
        "main_reason": reason.get(target),
        "chain": chain,
        "direct_importers": direct_importers,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if target not in selected or not chain:
        raise SystemExit("main.py was not selected or parent chain is missing")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
