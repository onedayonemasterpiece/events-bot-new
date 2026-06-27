#!/usr/bin/env python3
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

from db import Database
from reaction_counter_sync import (
    aggregate_source_reaction_counters,
    sync_source_reaction_counters,
)


def _load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _parse_event_ids(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out


async def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Aggregate raw Telegram/VK source likes from Fly SQLite and upsert source counters to personalization Supabase."
    )
    parser.add_argument(
        "--sqlite-db",
        default=os.getenv("EVENTS_BOT_SQLITE_DB") or os.getenv("DB_PATH") or "db.sqlite",
        help="Path to the canonical events-bot SQLite DB (on Fly: /data/db.sqlite).",
    )
    parser.add_argument("--env-file", default=".env", help="Optional env file for local runs.")
    parser.add_argument("--event-ids", help="Comma-separated event ids. Omit to aggregate all events that have source metrics.")
    parser.add_argument("--dry-run", action="store_true", help="Only print aggregate summary and sample rows; do not upsert.")
    parser.add_argument("--sample", type=int, default=10, help="Number of sample rows to include in dry-run output.")
    args = parser.parse_args()

    _load_env(Path(args.env_file))
    db = Database(str(args.sqlite_db))
    event_ids = _parse_event_ids(args.event_ids)

    try:
        if args.dry_run:
            counters = await aggregate_source_reaction_counters(db, event_ids=event_ids)
            sample = [
                {
                    "event_id": c.event_id,
                    "source_likes_count": c.source_likes_count,
                    "source_views_count": c.source_views_count,
                    "source_engagement_sources_count": c.source_engagement_sources_count,
                }
                for c in counters[: max(0, int(args.sample))]
            ]
            payload = {
                "sqlite_db": str(args.sqlite_db),
                "events_aggregated": len(counters),
                "source_likes_count": sum(c.source_likes_count for c in counters),
                "source_views_count": sum(c.source_views_count for c in counters),
                "source_engagement_sources_count": sum(c.source_engagement_sources_count for c in counters),
                "sample": sample,
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0

        result = await sync_source_reaction_counters(db, event_ids=event_ids, raise_on_error=True)
        print(json.dumps({"sqlite_db": str(args.sqlite_db), **result}, ensure_ascii=False, indent=2))
        return 0
    finally:
        await db.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
