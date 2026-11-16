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
import tempfile
from datetime import date
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
    VISUAL_DIGEST_CARD_LIMIT,
    build_visual_digest_issue,
    build_visual_digest_vk_text,
    default_review_publish_date,
    load_visual_digest_issue,
    publish_due_visual_digest_vk_stories,
    publish_visual_digest_daily,
    publish_visual_digest_to_telegram,
    publish_visual_digest_to_vk,
    render_visual_digest_cards,
    select_visual_digest_rows,
)


def _telegram_bot_from_env():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        return None
    from aiogram import Bot

    return Bot(token)


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
        review_dir = None
        if args.review_dir:
            if args.test_mention is None:
                raise ValueError("--review-dir requires --test-mention")
            parent = Path(args.review_dir)
            parent.mkdir(parents=True, exist_ok=True)
            review_dir = Path(tempfile.mkdtemp(prefix="guide-vk-", dir=parent))
            print(f"VK review files: {review_dir}", file=sys.stderr)
        review_rows = None
        selection = {}
        issue_id = args.issue_id
        if (args.select_only or args.previous_review) and args.as_of is None:
            raise ValueError("--select-only and --previous-review require --as-of")
        if args.as_of is not None:
            if args.group_id != "243343951" or args.test_mention not in (858363572, 1134828279):
                raise ValueError("--as-of requires the review community and an approved test recipient")
            previous_ids = set()
            last_number = 0
            for path in args.previous_review:
                receipt = json.loads(path.read_text(encoding="utf-8"))
                if not (receipt.get("published") is True and str(receipt.get("group_id")) == args.group_id
                        and receipt.get("url") and date.fromisoformat(receipt["as_of"]) < args.as_of
                        and receipt.get("covered_occurrence_ids")):
                    raise ValueError(f"Expected a successful earlier dated review in {path}")
                previous_ids.update(int(i) for i in receipt["covered_occurrence_ids"])
                last_number = max(last_number, int(receipt["issue_id"]))
            max_items = max(1, min(args.max_cards, 10)) * VISUAL_DIGEST_CARD_LIMIT
            if issue_id is not None:
                if previous_ids:
                    raise ValueError("Use --issue-id for the existing edition or --previous-review for projected selection")
                issue = await load_visual_digest_issue(db, issue_id, selection_only=args.select_only)
                if not issue:
                    raise ValueError(f"Visual issue {issue_id} does not exist")
                review_rows = list(issue["items"])[:max_items]
            else:
                review_rows = await select_visual_digest_rows(db, max_items=max_items, as_of=args.as_of,
                                                              previous_ids=sorted(previous_ids))
                if not last_number:
                    async with db.raw_conn() as conn:
                        cursor = await conn.execute("SELECT COALESCE(MAX(id), 0) FROM guide_digest_issue")
                        last_number = int((await cursor.fetchone())[0])
                issue_id = last_number + 1
            selection = {
                "as_of": args.as_of.isoformat(), "issue_id": issue_id, "projected_number": args.issue_id is None,
                "covered_occurrence_ids": sorted({i for r in review_rows for i in r.get("covered_visual_ids", [r["id"]])}),
                "items": [{"id": r["id"], "title": r.get("canonical_title"), "date": r.get("date"),
                           "time": r.get("time"), "source": r.get("source_post_url")} for r in review_rows],
            }
            output = json.dumps(selection, ensure_ascii=False, indent=2)
            print(output, file=sys.stderr, flush=True)
            if review_dir is not None:
                (review_dir / "selection.json").write_text(output, encoding="utf-8")
            if args.select_only or not review_rows:
                print(output)
                return 0 if review_rows else 2
            if args.issue_id is None:
                from source_avatars import prepare_guide_avatars
                await prepare_guide_avatars(review_rows, db)
        result = await publish_visual_digest_to_vk(
            db,
            None,
            issue_id=issue_id,
            max_cards=args.max_cards,
            group_id=args.group_id,
            target=args.target,
            publish_date=publish_date,
            publish_stories=bool(args.stories),
            test_mention=args.test_mention,
            review_dir=review_dir,
            review_rows=review_rows,
        )
        result.update(selection)
        if review_dir is not None:
            result["review_dir"] = str(review_dir)
            (review_dir / "receipt.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result.get("published") else 1
    finally:
        await db.close()


async def _publish_daily(args: argparse.Namespace) -> int:
    db = Database(_db_path(args))
    bot = _telegram_bot_from_env()
    try:
        result = await publish_visual_digest_daily(
            db,
            bot,
            max_cards=1,
            target_chats=args.target_chats,
            vk_group_id=args.group_id,
            vk_target=args.target,
            vk_delay_seconds=int(args.vk_delay_seconds),
            publish_stories=True if args.stories else None,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result.get("published") else 1
    finally:
        if bot is not None:
            await bot.session.close()
        await db.close()


async def _publish_tg(args: argparse.Namespace) -> int:
    db = Database(_db_path(args))
    bot = _telegram_bot_from_env()
    try:
        result = await publish_visual_digest_to_telegram(
            db,
            bot,
            issue_id=args.issue_id,
            max_cards=1,
            target_chats=args.target_chats,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result.get("published") else 1
    finally:
        if bot is not None:
            await bot.session.close()
        await db.close()


async def _publish_stories_due(args: argparse.Namespace) -> int:
    db = Database(_db_path(args))
    bot = _telegram_bot_from_env()
    try:
        result = await publish_due_visual_digest_vk_stories(db, bot, now_ts=args.now_ts, limit=args.limit)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    finally:
        if bot is not None:
            await bot.session.close()
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
    publish.add_argument("--test-mention", type=int, help="Substitute recipient for eligible guides in the owner's review group")
    publish.add_argument("--as-of", type=date.fromisoformat, help="Preview the daily selection for this local date in the review community")
    publish.add_argument("--previous-review", type=Path, action="append", default=[], help="Earlier published dated review receipt; repeat for each earlier day")
    publish.add_argument("--select-only", action="store_true", help="Print the dated selection without generating or posting a digest")
    publish.add_argument("--review-dir", help="Save the input, model response, posted text, cards and receipt for this test run")
    publish.add_argument("--target")
    publish.add_argument("--publish-date", type=int, help="Unix timestamp; use 0 for immediate")
    publish.add_argument("--review-delay-days", type=int, default=3, help="Schedule to postponed queue this many days ahead")
    publish.add_argument("--stories", action="store_true", help="Also publish first card to VK stories when wall post is immediate")
    publish.set_defaults(func=_publish)

    daily = sub.add_parser("publish-daily")
    daily.add_argument("--target-chats", help="Comma-separated Telegram targets; defaults to GUIDE_VISUAL/GUIDE_DIGEST targets")
    daily.add_argument("--group-id")
    daily.add_argument("--target")
    daily.add_argument("--vk-delay-seconds", type=int, default=600)
    daily.add_argument("--stories", action="store_true", help="Queue VK story after the postponed VK wall post becomes live")
    daily.set_defaults(func=_publish_daily)

    publish_tg = sub.add_parser("publish-telegram")
    publish_tg.add_argument("--issue-id", type=int)
    publish_tg.add_argument("--target-chats")
    publish_tg.set_defaults(func=_publish_tg)

    stories_due = sub.add_parser("publish-stories-due")
    stories_due.add_argument("--now-ts", type=int)
    stories_due.add_argument("--limit", type=int, default=10)
    stories_due.set_defaults(func=_publish_stories_due)

    args = parser.parse_args()
    return asyncio.run(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
