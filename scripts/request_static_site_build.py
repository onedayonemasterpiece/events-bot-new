#!/usr/bin/env python3
"""Create a durable, secret-preview static-site build request."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database
from main import enqueue_static_site_build_request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enqueue an on-demand static-site build (secret preview only)."
    )
    parser.add_argument("--db", default=os.getenv("DB_PATH") or "/data/db.sqlite")
    parser.add_argument("--reason", required=True, help="Short operator/audit reason")
    parser.add_argument("--event-id", action="append", type=int, default=[])
    parser.add_argument("--correlation-id", default="")
    parser.add_argument("--delay-seconds", type=int, default=0)
    return parser.parse_args()


async def run(args: argparse.Namespace) -> dict[str, object]:
    db = Database(args.db)
    correlation_id = args.correlation_id.strip() or f"operator:{uuid.uuid4().hex}"
    try:
        action = await enqueue_static_site_build_request(
            db,
            reason=args.reason,
            event_ids=args.event_id,
            correlation_id=correlation_id,
            delay_seconds=max(0, args.delay_seconds),
            trigger="operator_request",
        )
    finally:
        await db.close()
    return {
        "ok": True,
        "action": action,
        "release_channel": "secret_preview",
        "correlation_id": correlation_id,
        "event_ids": args.event_id,
    }


def main() -> int:
    result = asyncio.run(run(parse_args()))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
