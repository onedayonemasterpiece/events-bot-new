#!/usr/bin/env python3
"""Boundedly remove confidently irrelevant legacy rows from VK auto-import.

Dry-run is the default. ``--apply`` changes only the admission receipt and the
linked ``vk_inbox`` status; it never parses or creates events. Provider/schema
uncertainty fails open and leaves the row selectable for the later scheduled
auto-import.
"""

from __future__ import annotations

import argparse
import asyncio
from contextlib import redirect_stdout
import importlib
import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ``vk_intake`` deliberately resolves shared provider/timezone state from the
# already-running ``main`` module instead of importing it implicitly.  This
# standalone operator entrypoint therefore owns the explicit bootstrap. Some
# optional SDKs print setup guidance while ``main`` imports; keep stdout a
# single machine-readable JSON receipt by routing that chatter to stderr.
with redirect_stdout(sys.stderr):
    _runtime_main = importlib.import_module("main")  # noqa: F841
from db import Database
from vk_intake import requalify_vk_inbox_admission


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify legacy pending VK rows before auto-import."
    )
    parser.add_argument("--db", default=os.getenv("DB_PATH") or "/data/db.sqlite")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument(
        "--oldest-first",
        action="store_true",
        help="Process oldest publication dates first (default protects fresh intake first)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist decisions; without this flag the command is read-only",
    )
    parser.add_argument(
        "--retry-transient-fail-open",
        action="store_true",
        help=(
            "Reclassify only pending rows admitted after a provider/schema failure; "
            "use in small bounded chunks after provider capacity has recovered"
        ),
    )
    return parser.parse_args()


async def run(args: argparse.Namespace) -> dict[str, object]:
    db = Database(str(args.db))
    try:
        return await requalify_vk_inbox_admission(
            db,
            limit=max(1, min(int(args.limit), 500)),
            newest_first=not bool(args.oldest_first),
            dry_run=not bool(args.apply),
            retry_transient_fail_open=bool(args.retry_transient_fail_open),
        )
    finally:
        await db.close()


def main() -> int:
    result = asyncio.run(run(parse_args()))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if not result.get("invalid_source_packets") else 2


if __name__ == "__main__":
    raise SystemExit(main())
