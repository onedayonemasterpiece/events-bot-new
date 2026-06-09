#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from afishaengagement import DEFAULT_DEBUG_MARKER, cleanup_debug_posts


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


async def _run() -> None:
    parser = argparse.ArgumentParser(description="Delete afishaengagement debug VK postponed posts.")
    parser.add_argument("--group-id", default=os.getenv("VK_EVENTS_GROUP_ID") or os.getenv("VK_AFISHA_GROUP_ID"))
    parser.add_argument("--marker", default=os.getenv("AFISHAENGAGEMENT_DEBUG_MARKER") or DEFAULT_DEBUG_MARKER)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dotenv", default=".env")
    args = parser.parse_args()

    _load_dotenv(Path(args.dotenv))
    import main

    group_id = args.group_id or os.getenv("VK_EVENTS_GROUP_ID") or os.getenv("VK_AFISHA_GROUP_ID")
    if not group_id:
        raise SystemExit("VK_EVENTS_GROUP_ID or --group-id is required")
    result = await cleanup_debug_posts(
        group_id=str(group_id),
        marker=str(args.marker),
        vk_api_fn=main._vk_api,
        db=None,
        bot=None,
        dry_run=bool(args.dry_run),
    )
    print(
        "afishaengagement cleanup "
        f"matched={result.get('matched', 0)} "
        f"deleted={result.get('deleted', 0)} "
        f"errors={result.get('errors', 0)} "
        f"dry_run={bool(args.dry_run)}"
    )


if __name__ == "__main__":
    asyncio.run(_run())
