from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from pathlib import Path


def _init_db(path: Path) -> None:
    con = sqlite3.connect(path)
    try:
        cur = con.cursor()
        cur.execute(
            """
            create table event (
                id integer primary key,
                photo_urls text,
                photo_count integer
            )
            """
        )
        cur.execute(
            """
            create table eventposter (
                id integer primary key,
                event_id integer,
                catbox_url text,
                supabase_url text,
                supabase_path text,
                poster_hash text,
                phash text,
                updated_at text
            )
            """
        )
        cur.execute("create unique index ux_eventposter_event_hash on eventposter(event_id, poster_hash)")
        cur.execute(
            """
            create table joboutbox (
                id integer primary key,
                event_id integer,
                task text,
                payload text,
                status text,
                attempts integer,
                last_error text,
                last_result text,
                updated_at text,
                next_run_at text,
                coalesce_key text,
                depends_on text
            )
            """
        )
        cur.execute(
            "insert into event(id, photo_urls, photo_count) values (?, ?, ?)",
            (101, json.dumps(["https://files.catbox.moe/old.jpg"]), 1),
        )
        cur.execute(
            """
            insert into eventposter(id, event_id, catbox_url, supabase_url, supabase_path, poster_hash, phash, updated_at)
            values (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (501, 101, "https://files.catbox.moe/old.jpg", None, None, "hash-old", None, "2026-04-13T00:00:00+00:00"),
        )
        con.commit()
    finally:
        con.close()


def test_apply_backfill_plan_updates_event_posters_and_jobs(tmp_path: Path):
    db_path = tmp_path / "db.sqlite"
    plan_path = tmp_path / "plan.json"
    _init_db(db_path)

    plan = {
        "version": 1,
        "entries": [
            {
                "event_id": 101,
                "title": "Demo",
                "source_kind": "tg",
                "source_url": "https://t.me/demo/1",
                "status": "applied_full",
                "note": "full_match",
                "photo_urls": [
                    "https://storage.yandexcloud.net/kenigevents/p/demo/new.webp",
                    "https://example.com/keep.jpg",
                ],
                "photo_count": 2,
                "poster_updates": [
                    {
                        "id": 501,
                        "supabase_url": "https://storage.yandexcloud.net/kenigevents/p/demo/new.webp",
                        "supabase_path": "p/demo/new.webp",
                        "phash": "abcd",
                        "updated_at": "2026-04-13T12:00:00+00:00",
                    }
                ],
                "poster_inserts": [
                    {
                        "catbox_url": None,
                        "supabase_url": "https://storage.yandexcloud.net/kenigevents/p/demo/extra.webp",
                        "supabase_path": "p/demo/extra.webp",
                        "poster_hash": "hash-extra",
                        "phash": "ef01",
                        "updated_at": "2026-04-13T12:00:00+00:00",
                    }
                ],
                "enqueue_telegraph": True,
            }
        ],
    }
    plan_path.write_text(json.dumps(plan, ensure_ascii=False), encoding="utf-8")

    script = Path(__file__).resolve().parents[1] / "scripts" / "apply_backfill_plan.py"
    for _ in range(2):
        subprocess.run(
            [sys.executable, str(script), "--db", str(db_path), "--plan", str(plan_path)],
            check=True,
            capture_output=True,
            text=True,
        )

    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        photo_urls_raw, photo_count = cur.execute(
            "select photo_urls, photo_count from event where id = 101"
        ).fetchone()
        assert json.loads(photo_urls_raw) == plan["entries"][0]["photo_urls"]
        assert photo_count == 2

        updated = cur.execute(
            "select supabase_url, supabase_path, phash from eventposter where id = 501"
        ).fetchone()
        assert updated == (
            "https://storage.yandexcloud.net/kenigevents/p/demo/new.webp",
            "p/demo/new.webp",
            "abcd",
        )

        inserts = cur.execute(
            "select count(*) from eventposter where event_id = 101 and poster_hash = 'hash-extra'"
        ).fetchone()[0]
        assert inserts == 1

        pending_jobs = cur.execute(
            "select count(*) from joboutbox where event_id = 101 and task = 'telegraph_build' and status = 'pending'"
        ).fetchone()[0]
        assert pending_jobs == 1
    finally:
        con.close()
