#!/usr/bin/env python3
"""Inventory or apply bounded retention to GUIDE_MEDIA_STORE_ROOT."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database  # noqa: E402
from guide_excursions.media_retention import prune_guide_media_store  # noqa: E402


async def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=os.getenv("DB_PATH") or "/data/db.sqlite")
    parser.add_argument(
        "--root",
        default=os.getenv("GUIDE_MEDIA_STORE_ROOT") or "/data/guide_media",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="delete planned files and heal matching stale DB paths (default is dry-run)",
    )
    parser.add_argument("--reason", default="operator_cli")
    args = parser.parse_args()

    db = Database(str(args.db))
    try:
        result = await prune_guide_media_store(
            db,
            root=args.root,
            reason=args.reason,
            dry_run=not args.apply,
        )
    finally:
        await db.close()
    print(json.dumps(result.as_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not result.errors else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
