#!/usr/bin/env python3
"""Resume a persisted Antigravity A/B interaction without creating a new POST."""
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


def _load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        if name.strip() and name.strip() not in os.environ:
            os.environ[name.strip()] = value.strip().strip('"').strip("'")


async def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lane_id", type=int)
    parser.add_argument("--db", default="artifacts/codex/festival-web-research.sqlite")
    parser.add_argument("--env-file", default=str(ROOT / ".env"))
    args = parser.parse_args()
    _load_env(Path(args.env_file))
    from db import Database
    from festival_web_research.runtime import build_festival_web_research_service

    db = Database(args.db)
    await db.init()
    try:
        service = build_festival_web_research_service(db)
        result = await service.coordinator.resume_lane(args.lane_id)
        print(json.dumps(result.model_dump(mode="json", exclude={"candidate"}), ensure_ascii=False))
        return 0 if result.semantic_status == "passed" else 2
    finally:
        await db.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
