#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _default_db_path() -> str:
    return os.getenv("DB_PATH") or ("/tmp/db.sqlite" if os.path.exists("/tmp/db.sqlite") else "db.sqlite")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_plan(path: str) -> dict[str, Any]:
    raw = Path(path).read_text(encoding="utf-8")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("plan root must be an object")
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise ValueError("plan.entries must be a list")
    return payload


def _ensure_event_exists(cur: sqlite3.Cursor, event_id: int) -> None:
    row = cur.execute("select id from event where id = ?", (int(event_id),)).fetchone()
    if row is None:
        raise RuntimeError(f"event_not_found:{int(event_id)}")


def _apply_event(cur: sqlite3.Cursor, entry: dict[str, Any]) -> bool:
    event_id = int(entry["event_id"])
    photo_urls = [str(item).strip() for item in list(entry.get("photo_urls") or []) if str(item or "").strip()]
    photo_count = int(entry.get("photo_count") or len(photo_urls))
    _ensure_event_exists(cur, event_id)
    cur.execute(
        "update event set photo_urls = ?, photo_count = ? where id = ?",
        (json.dumps(photo_urls, ensure_ascii=False), photo_count, event_id),
    )
    return cur.rowcount > 0


def _apply_poster_updates(cur: sqlite3.Cursor, entry: dict[str, Any]) -> tuple[int, int]:
    event_id = int(entry["event_id"])
    updated = 0
    inserted = 0

    for row in list(entry.get("poster_updates") or []):
        row_id = int(row["id"])
        cur.execute(
            """
            update eventposter
               set supabase_url = ?,
                   supabase_path = ?,
                   phash = coalesce(?, phash),
                   updated_at = ?
             where id = ? and event_id = ?
            """,
            (
                str(row.get("supabase_url") or "").strip() or None,
                str(row.get("supabase_path") or "").strip() or None,
                str(row.get("phash") or "").strip() or None,
                str(row.get("updated_at") or _now_iso()),
                row_id,
                event_id,
            ),
        )
        updated += int(cur.rowcount or 0)

    for row in list(entry.get("poster_inserts") or []):
        poster_hash = str(row.get("poster_hash") or "").strip()
        if not poster_hash:
            continue
        existing = cur.execute(
            "select id from eventposter where event_id = ? and poster_hash = ? limit 1",
            (event_id, poster_hash),
        ).fetchone()
        if existing is not None:
            cur.execute(
                """
                update eventposter
                   set catbox_url = coalesce(catbox_url, ?),
                       supabase_url = ?,
                       supabase_path = ?,
                       phash = coalesce(?, phash),
                       updated_at = ?
                 where id = ?
                """,
                (
                    str(row.get("catbox_url") or "").strip() or None,
                    str(row.get("supabase_url") or "").strip() or None,
                    str(row.get("supabase_path") or "").strip() or None,
                    str(row.get("phash") or "").strip() or None,
                    str(row.get("updated_at") or _now_iso()),
                    int(existing[0]),
                ),
            )
            updated += int(cur.rowcount or 0)
            continue

        cur.execute(
            """
            insert into eventposter (
                event_id, catbox_url, supabase_url, supabase_path, poster_hash, phash, updated_at
            ) values (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                str(row.get("catbox_url") or "").strip() or None,
                str(row.get("supabase_url") or "").strip() or None,
                str(row.get("supabase_path") or "").strip() or None,
                poster_hash,
                str(row.get("phash") or "").strip() or None,
                str(row.get("updated_at") or _now_iso()),
            ),
        )
        inserted += 1

    return updated, inserted


def _enqueue_telegraph(cur: sqlite3.Cursor, event_id: int) -> bool:
    now = _now_iso()
    row = cur.execute(
        "select id, status from joboutbox where event_id = ? and task = ? order by id desc limit 1",
        (int(event_id), "telegraph_build"),
    ).fetchone()
    if row is None:
        cur.execute(
            """
            insert into joboutbox (
                event_id, task, payload, status, attempts, last_error, last_result, updated_at, next_run_at, coalesce_key, depends_on
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(event_id), "telegraph_build", None, "pending", 0, None, None, now, now, None, None),
        )
        return True

    job_id = int(row[0])
    status = str(row[1] or "").strip().lower()
    if status == "pending":
        cur.execute(
            """
            update joboutbox
               set updated_at = ?, next_run_at = ?, attempts = 0, last_error = null
             where id = ?
            """,
            (now, now, job_id),
        )
        return True
    if status == "running":
        return False

    cur.execute(
        """
        insert into joboutbox (
            event_id, task, payload, status, attempts, last_error, last_result, updated_at, next_run_at, coalesce_key, depends_on
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (int(event_id), "telegraph_build", None, "pending", 0, None, None, now, now, None, None),
    )
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply a locally generated catbox->Yandex backfill plan to a target SQLite DB.")
    parser.add_argument("--db", default=_default_db_path(), help="Path to target SQLite DB")
    parser.add_argument("--plan", required=True, help="JSON plan created by backfill_catbox_posters_to_yandex.py --plan-out")
    parser.add_argument("--dry-run", action="store_true", help="Validate plan and print summary without writing changes")
    args = parser.parse_args()

    payload = _load_plan(str(args.plan))
    entries = list(payload.get("entries") or [])
    print(f"plan_entries={len(entries)} dry_run={int(bool(args.dry_run))} db={args.db}")

    con = sqlite3.connect(str(args.db))
    try:
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        event_updates = 0
        poster_updates = 0
        poster_inserts = 0
        telegraph_jobs = 0
        for entry in entries:
            event_id = int(entry["event_id"])
            _ensure_event_exists(cur, event_id)
            if args.dry_run:
                print(f"event_id={event_id} dry_run=1")
                continue
            event_updates += int(_apply_event(cur, entry))
            upd, ins = _apply_poster_updates(cur, entry)
            poster_updates += upd
            poster_inserts += ins
            if bool(entry.get("enqueue_telegraph")):
                telegraph_jobs += int(_enqueue_telegraph(cur, event_id))
        if args.dry_run:
            con.rollback()
        else:
            con.commit()
        print(
            "summary "
            f"event_updates={event_updates} "
            f"poster_updates={poster_updates} "
            f"poster_inserts={poster_inserts} "
            f"telegraph_jobs={telegraph_jobs}"
        )
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
