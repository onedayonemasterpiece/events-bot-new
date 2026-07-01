#!/usr/bin/env python3
"""Build/render/publish the guide visual VK digest.

Examples:
  python scripts/guide_visual_digest.py render --db /data/db.sqlite --out artifacts/codex/guide-visual
  python scripts/guide_visual_digest.py publish --db /data/db.sqlite --group-id 238875824 --max-cards 2 --review-delay-days 3
"""

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

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None

from db import Database
from guide_excursions.visual_digest import (
    build_visual_digest_issue,
    build_visual_digest_vk_text,
    default_review_publish_date,
    load_visual_digest_issue,
    publish_visual_digest_to_vk,
    render_visual_digest_cards,
)


def _load_env() -> None:
    if load_dotenv is not None:
        load_dotenv(ROOT / ".env", override=False)


def _db_path(args: argparse.Namespace) -> str:
    return str(args.db or os.getenv("DB_PATH") or (ROOT / "db.sqlite"))


async def _render(args: argparse.Namespace) -> int:
    db = Database(_db_path(args))
    try:
        if args.issue_id:
            issue = await load_visual_digest_issue(db, int(args.issue_id))
            if not issue:
                print(json.dumps({"ok": False, "reason": "no_issue", "issue_id": args.issue_id}, ensure_ascii=False))
                return 2
            issue_id = int(issue["id"])
            rows = list(issue.get("items") or [])[: args.max_cards * 5]
        else:
            built = await build_visual_digest_issue(db, max_cards=args.max_cards)
            issue_id = int(built["issue_id"])
            rows = list(built.get("items") or [])[: args.max_cards * 5]
        out_dir = Path(args.out)
        out_dir.mkdir(parents=True, exist_ok=True)
        cards = render_visual_digest_cards(rows, issue_id=issue_id) if rows else []
        paths = []
        for idx, payload in enumerate(cards, start=1):
            path = out_dir / f"guide_visual_digest_{issue_id}_{idx}.jpg"
            path.write_bytes(payload)
            paths.append(str(path))
        text = await build_visual_digest_vk_text(rows, issue_id=issue_id, vk_api_fn=None) if rows else ""
        (out_dir / f"guide_visual_digest_{issue_id}_vk_text.txt").write_text(text, encoding="utf-8")
        (out_dir / f"guide_visual_digest_{issue_id}_manifest.json").write_text(
            json.dumps(
                {
                    "issue_id": issue_id,
                    "cards": paths,
                    "items": [
                        {
                            "id": int(row.get("id") or 0),
                            "title": row.get("canonical_title"),
                            "date": row.get("date"),
                            "time": row.get("time"),
                            "guide_names": row.get("guide_names"),
                            "source_post_url": row.get("source_post_url"),
                        }
                        for row in rows
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print(json.dumps({"ok": True, "issue_id": issue_id, "cards": paths, "items": len(rows)}, ensure_ascii=False))
        return 0
    finally:
        await db.close()


async def _publish(args: argparse.Namespace) -> int:
    db = Database(_db_path(args))
    try:
        publish_date = args.publish_date
        if publish_date is None and args.review_delay_days is not None:
            publish_date = default_review_publish_date(delay_days=int(args.review_delay_days))
        result = await publish_visual_digest_to_vk(
            db,
            None,
            issue_id=args.issue_id,
            max_cards=args.max_cards,
            group_id=args.group_id,
            target=args.target,
            publish_date=publish_date,
            publish_stories=bool(args.stories),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result.get("published") else 1
    finally:
        await db.close()


def main() -> int:
    _load_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite DB path; defaults to DB_PATH or ./db.sqlite")
    sub = parser.add_subparsers(dest="cmd", required=True)

    render = sub.add_parser("render")
    render.add_argument("--issue-id", type=int)
    render.add_argument("--max-cards", type=int, default=1)
    render.add_argument("--out", default=str(ROOT / "artifacts" / "codex" / "guide-visual-digest"))
    render.set_defaults(func=_render)

    publish = sub.add_parser("publish")
    publish.add_argument("--issue-id", type=int)
    publish.add_argument("--max-cards", type=int, default=1)
    publish.add_argument("--group-id")
    publish.add_argument("--target")
    publish.add_argument("--publish-date", type=int, help="Unix timestamp; use 0 for immediate")
    publish.add_argument("--review-delay-days", type=int, default=3, help="Schedule to postponed queue this many days ahead")
    publish.add_argument("--stories", action="store_true", help="Also publish first card to VK stories when wall post is immediate")
    publish.set_defaults(func=_publish)

    args = parser.parse_args()
    return asyncio.run(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
