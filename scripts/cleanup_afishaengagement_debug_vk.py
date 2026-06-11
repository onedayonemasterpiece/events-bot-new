#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

from afishaengagement import DEFAULT_DEBUG_MARKER, cleanup_debug_posts


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _vk_post_id_from_url(url: str | None) -> int | None:
    match = re.search(r"wall-?\d+_(\d+)", str(url or ""))
    return int(match.group(1)) if match else None


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


def _db_path(raw: str | None) -> str:
    return raw or os.getenv("DB_PATH") or "/data/db.sqlite"


def _load_stale_debug_rows(
    *,
    db_path: str,
    stale_before: datetime | None,
    include_all: bool,
) -> list[dict]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        """
        SELECT id, created_at, published_at, details_json, public_targets_json
        FROM promo_exposure
        WHERE surface = ? AND publish_status = ?
        ORDER BY id
        """,
        ("afishaengagement", "VK_SCHEDULED_DEBUG"),
    ).fetchall()
    now = datetime.now(timezone.utc)
    selected: list[dict] = []
    for row in rows:
        published_at = _parse_dt(row["published_at"])
        created_at = _parse_dt(row["created_at"])
        if not published_at or published_at <= now:
            continue
        if stale_before and created_at and created_at >= stale_before:
            continue
        if not include_all and stale_before is None:
            continue
        try:
            details = json.loads(row["details_json"] or "{}")
        except Exception:
            details = {}
        url = str(details.get("target_url") or "")
        if not url:
            try:
                targets = json.loads(row["public_targets_json"] or "[]")
            except Exception:
                targets = []
            if targets:
                url = str((targets[0] or {}).get("url") or "")
        post_id = _vk_post_id_from_url(url)
        if not post_id:
            continue
        selected.append(
            {
                "exposure_id": int(row["id"]),
                "post_id": post_id,
                "url": url,
                "created_at": row["created_at"],
                "published_at": row["published_at"],
                "cta_text": details.get("cta_text"),
                "event_title": details.get("event_title"),
                "details": details,
            }
        )
    return selected


async def _cleanup_from_db(
    *,
    db_path: str,
    group_id: str,
    stale_before: datetime | None,
    include_all: bool,
    dry_run: bool,
) -> dict[str, int]:
    import main

    selected = _load_stale_debug_rows(db_path=db_path, stale_before=stale_before, include_all=include_all)
    owner_id = -int(str(group_id).lstrip("-"))
    if dry_run:
        for item in selected:
            print(
                "candidate "
                f"exposure_id={item['exposure_id']} "
                f"post_id={item['post_id']} "
                f"published_at={item['published_at']} "
                f"event={item.get('event_title')!r} "
                f"cta={item.get('cta_text')!r}"
            )
        return {"matched": len(selected), "deleted": 0, "errors": 0}

    con = sqlite3.connect(db_path)
    deleted = 0
    errors = 0
    for item in selected:
        details = dict(item.get("details") or {})
        try:
            await main._vk_api(
                "wall.delete",
                {"owner_id": owner_id, "post_id": item["post_id"]},
                None,
                None,
            )
            details["cleanup_deleted_at"] = datetime.now(timezone.utc).isoformat()
            details["cleanup_reason"] = "stale_afishaengagement_debug_from_db_cleanup"
            con.execute(
                "UPDATE promo_exposure SET publish_status=?, details_json=? WHERE id=?",
                ("VK_DELETED_DEBUG", json.dumps(details, ensure_ascii=False), item["exposure_id"]),
            )
            deleted += 1
        except Exception as exc:
            errors += 1
            print(f"delete_error exposure_id={item['exposure_id']} post_id={item['post_id']} error={exc}")
    con.commit()
    return {"matched": len(selected), "deleted": deleted, "errors": errors}


async def _run() -> None:
    parser = argparse.ArgumentParser(description="Delete afishaengagement debug VK postponed posts.")
    parser.add_argument("--group-id", default=os.getenv("VK_EVENTS_GROUP_ID") or os.getenv("VK_AFISHA_GROUP_ID"))
    parser.add_argument("--marker", default=os.getenv("AFISHAENGAGEMENT_DEBUG_MARKER") or DEFAULT_DEBUG_MARKER)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--dotenv", default=".env")
    parser.add_argument("--from-db", action="store_true", help="Delete scheduled debug posts by promo_exposure URLs.")
    parser.add_argument("--db-path", default=None, help="SQLite DB path for --from-db. Defaults to DB_PATH or /data/db.sqlite.")
    parser.add_argument(
        "--stale-before",
        default=None,
        help="Only --from-db rows created before this ISO datetime are selected.",
    )
    parser.add_argument(
        "--all-db-scheduled",
        action="store_true",
        help="With --from-db and no --stale-before, include all future VK_SCHEDULED_DEBUG rows.",
    )
    args = parser.parse_args()

    _load_dotenv(Path(args.dotenv))
    import main

    group_id = args.group_id or os.getenv("VK_EVENTS_GROUP_ID") or os.getenv("VK_AFISHA_GROUP_ID")
    if not group_id:
        raise SystemExit("VK_EVENTS_GROUP_ID or --group-id is required")
    if args.from_db:
        stale_before = _parse_dt(args.stale_before)
        if args.stale_before and stale_before is None:
            raise SystemExit("--stale-before must be an ISO datetime")
        result = await _cleanup_from_db(
            db_path=_db_path(args.db_path),
            group_id=str(group_id),
            stale_before=stale_before,
            include_all=bool(args.all_db_scheduled),
            dry_run=bool(args.dry_run),
        )
    else:
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
