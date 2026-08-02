#!/usr/bin/env python3
"""Watch documentation changes and push each saved update to origin/main."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".git" / "autopush-docs-state.json"
EXTENSIONS = {".md", ".mdx", ".rst", ".txt", ".csv", ".json", ".yml", ".yaml"}
EXCLUDED = {"docs/reference/visitors-30072026.md"}


def git(*args: str, input: bytes | None = None) -> bytes:
    return subprocess.check_output(["git", *args], cwd=ROOT, input=input, stderr=subprocess.STDOUT)


def documents() -> dict[str, str]:
    result: dict[str, str] = {}
    for path in (ROOT / "docs").rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(ROOT).as_posix()
        if path.suffix.lower() not in EXTENSIONS or relative in EXCLUDED:
            continue
        result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return result


def load_state() -> dict[str, str] | None:
    try:
        return json.loads(STATE.read_text())
    except FileNotFoundError:
        return None


def save_state(state: dict[str, str]) -> None:
    temporary = STATE.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    temporary.replace(STATE)


def push(paths: list[str]) -> None:
    git("fetch", "origin", "main")
    base = git("rev-parse", "origin/main").decode().strip()
    index = STATE.with_name("autopush-docs-index")
    env = os.environ | {"GIT_INDEX_FILE": str(index)}
    subprocess.run(["git", "read-tree", base], cwd=ROOT, env=env, check=True)
    for relative in paths:
        content = (ROOT / relative).read_bytes()
        blob = git("hash-object", "-w", "--stdin", input=content).decode().strip()
        subprocess.run(
            ["git", "update-index", "--add", "--cacheinfo", f"100644,{blob},{relative}"],
            cwd=ROOT, env=env, check=True,
        )
    tree = subprocess.check_output(["git", "write-tree"], cwd=ROOT, env=env).decode().strip()
    message = "docs: auto-sync updated documentation\n"
    commit = git("commit-tree", tree, "-p", base, input=message.encode()).decode().strip()
    subprocess.run(["git", "push", "origin", f"{commit}:refs/heads/main"], cwd=ROOT, check=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=float, default=10)
    args = parser.parse_args()
    state = load_state()
    if state is None:
        save_state(documents())
        print("Baseline saved; existing local documents will not be auto-pushed.", flush=True)
    while True:
        current = documents()
        previous = load_state() or {}
        changed = sorted(path for path, digest in current.items() if previous.get(path) != digest)
        if changed:
            try:
                push(changed)
            except subprocess.CalledProcessError as error:
                print(error.output.decode(errors="replace") if error.output else str(error), file=sys.stderr, flush=True)
            else:
                save_state(current)
                print("Pushed: " + ", ".join(changed), flush=True)
        if not args.watch:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
