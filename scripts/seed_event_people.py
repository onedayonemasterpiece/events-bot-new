#!/usr/bin/env python3
"""Idempotently seed the checked-in people catalog into a SQLite database."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in os.sys.path:
    os.sys.path.insert(0, str(PROJECT_ROOT))

from db import Database  # noqa: E402
from event_people import ensure_kgd80_registry  # noqa: E402


async def run(path: Path) -> dict[str, int | str]:
    db = Database(str(path))
    await db.init()
    try:
        return await ensure_kgd80_registry(db)
    finally:
        await db.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.db)), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
