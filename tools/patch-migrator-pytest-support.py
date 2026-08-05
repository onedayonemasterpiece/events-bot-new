#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

OLD = '''    # Pytest support is implicit rather than imported: preserve conftest hooks and
    # fixture/resource trees used by the transferred product suite.
    support_prefixes = (
        "tests/fixtures/", "tests/data/", "tests/resources/",
        "tests/golden/", "tests/snapshots/", "tests/_helpers/",
    )
    for path in tracked_list:
        # The source-wide conftest imports the monolithic Telegram bot entrypoint
        # and installs fixtures for unrelated products. A dedicated standalone
        # Region Talk conftest is generated below instead of copying that shell.
        if path.startswith(support_prefixes) and is_safe_copy_path(path):
            if path not in selected:
                selected.add(path)
                reasons[path] = "pytest-support"
'''

NEW = '''    # The source-wide conftest and generic fixture trees belong to events-bot-new
    # and may import its monolithic Telegram entrypoint. Region Talk tests use a
    # generated narrow conftest; other resources enter through literal/import
    # closure. Only the no-network guard is an implicit pytest dependency.
    for support_path in ("tests/_helpers/no_network.py",):
        if support_path in tracked and support_path not in selected:
            selected.add(support_path)
            reasons[support_path] = "pytest-support"
'''


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    text = args.path.read_text(encoding="utf-8")
    count = text.count(OLD)
    if count != 1:
        raise SystemExit(f"expected exactly one pytest-support block, found {count}")
    args.path.write_text(text.replace(OLD, NEW), encoding="utf-8")
    print(f"patched={args.path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
