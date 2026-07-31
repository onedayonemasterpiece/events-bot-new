#!/usr/bin/env python3
"""Approve or reject a local collect-only festival research candidate."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_id", type=int)
    parser.add_argument("decision", choices=("approve", "reject"))
    parser.add_argument("--db", default="artifacts/codex/festival-web-research.sqlite")
    parser.add_argument("--operator", required=True)
    parser.add_argument("--reason")
    return parser.parse_args()


async def _main() -> int:
    args = _args()
    from db import Database
    from festival_web_research.repository import FestivalResearchRepository

    db = Database(args.db)
    await db.init()
    try:
        repository = FestivalResearchRepository(db)
        run = await repository.get_run(args.run_id)
        if run is None:
            raise LookupError(f"research run {args.run_id} not found")
        if args.decision == "approve":
            if run.state != "review" or not run.candidate_json:
                raise ValueError("only a validated review candidate can be approved")
            decision = "approved"
        else:
            if not (args.reason or "").strip():
                raise ValueError("--reason is required for rejection")
            decision = "rejected"
        run = await repository.review(
            args.run_id,
            decision=decision,
            operator=args.operator,
            reason=(args.reason or "").strip() or None,
        )
        print(json.dumps({
            "run_id": run.id,
            "state": run.state,
            "review_status": run.review_status,
            "public_apply": False,
        }, ensure_ascii=False))
        return 0
    finally:
        await db.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
