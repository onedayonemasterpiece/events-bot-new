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
from static_site_release import resolve_current_secret_candidate


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enqueue an on-demand static-site build (secret preview only)."
    )
    parser.add_argument("--db", default=os.getenv("DB_PATH") or "/data/db.sqlite")
    parser.add_argument("--reason", default="", help="Short operator/audit reason")
    parser.add_argument("--event-id", action="append", type=int, default=[])
    parser.add_argument("--correlation-id", default="")
    parser.add_argument("--delay-seconds", type=int, default=0)
    parser.add_argument(
        "--semantic-cache-mode",
        choices=["warm", "cold"],
        default="warm",
        help="Reuse prior semantic caches (warm) or explicitly recompute without them (cold)",
    )
    parser.add_argument(
        "--show-current-review",
        action="store_true",
        help="Print the canonical checked immutable preproduction review target without enqueueing",
    )
    args = parser.parse_args()
    if not args.show_current_review and not args.reason.strip():
        parser.error("--reason is required unless --show-current-review is used")
    return args


async def run(args: argparse.Namespace) -> dict[str, object]:
    if args.show_current_review:
        current = await asyncio.to_thread(resolve_current_secret_candidate, args.db)
        if current is None:
            return {
                "ok": False,
                "status": "current_review_unavailable",
                "release_channel": "secret_preview",
            }
        return {
            "ok": True,
            "status": "current_review_ready",
            "release_channel": current.release_channel,
            "public_url": current.public_url,
            "build_id": current.build_id,
            "run_id": current.run_id,
            "repo_sha": current.repo_sha,
            "snapshot_id": current.snapshot_id,
            "result_sha256": current.result_sha256,
            "manifest_sha256": current.manifest_sha256,
            "token_sha256": current.token_sha256,
            "input_fingerprint": current.input_fingerprint,
            "verified_at": current.verified_at,
        }
    from main import enqueue_static_site_build_request

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
            semantic_cache_mode=args.semantic_cache_mode,
        )
    finally:
        await db.close()
    return {
        "ok": True,
        "action": action,
        "release_channel": "secret_preview",
        "correlation_id": correlation_id,
        "event_ids": args.event_id,
        "semantic_cache_mode": args.semantic_cache_mode,
    }


def main() -> int:
    result = asyncio.run(run(parse_args()))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
