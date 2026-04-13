#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _default_db_path() -> str:
    return os.getenv("DB_PATH") or ("/tmp/db.sqlite" if os.path.exists("/tmp/db.sqlite") else "db.sqlite")


def _open_db(path: str) -> sqlite3.Connection:
    try:
        return sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except Exception:
        return sqlite3.connect(path)


def _parse_dt(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    raw = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _load_json_list(value: Any) -> list[str]:
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except Exception:
            return [raw]
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item or "").strip()]
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item or "").strip()]
    return []


def _classify_url(url: str | None) -> str:
    raw = str(url or "").strip().lower()
    if not raw:
        return "none"
    if "storage.yandexcloud.net/" in raw:
        return "yandex"
    if "files.catbox.moe/" in raw or "catbox.moe/" in raw:
        return "catbox"
    if "supabase.co/storage/" in raw or "/storage/v1/object/" in raw:
        return "supabase"
    return "other"


def _classify_url_list(urls: list[str]) -> str:
    if not urls:
        return "empty"
    kinds = {_classify_url(url) for url in urls if str(url or "").strip()}
    if not kinds:
        return "empty"
    if len(kinds) == 1:
        return next(iter(kinds))
    if kinds <= {"catbox", "yandex"}:
        return "mixed_catbox_yandex"
    if kinds <= {"catbox", "supabase"}:
        return "mixed_catbox_supabase"
    return "mixed_other"


def _source_kind(source_post_url: str | None, source_vk_post_url: str | None) -> str:
    raw = str(source_vk_post_url or source_post_url or "").strip().lower()
    if "vk.com/wall" in raw:
        return "vk"
    if "t.me/" in raw:
        return "tg"
    return "other"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit event image hosts (Yandex/Catbox/Supabase) for recent events."
    )
    parser.add_argument("--db", default=_default_db_path(), help="Path to SQLite DB")
    parser.add_argument("--days", type=int, default=7, help="Inspect only events added in the last N days (0 = all)")
    parser.add_argument("--source", choices=["all", "tg", "vk"], default="all", help="Restrict by source kind")
    parser.add_argument("--samples", type=int, default=5, help="How many sample event_ids to show per bucket")
    args = parser.parse_args()

    con = _open_db(str(args.db))
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cur.execute(
            """
            select id, title, added_at, photo_urls, source_post_url, source_vk_post_url
            from event
            order by added_at desc, id desc
            """
        )
        rows = cur.fetchall()
    finally:
        con.close()

    cutoff = None
    if int(args.days or 0) > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(args.days))

    by_source: dict[str, Counter[str]] = defaultdict(Counter)
    samples: dict[tuple[str, str], list[str]] = defaultdict(list)

    total = 0
    for row in rows:
        added_at = _parse_dt(row["added_at"])
        if cutoff and (not added_at or added_at < cutoff):
            continue
        source_kind = _source_kind(row["source_post_url"], row["source_vk_post_url"])
        if args.source != "all" and source_kind != args.source:
            continue
        photo_urls = _load_json_list(row["photo_urls"])
        bucket = _classify_url_list(photo_urls)
        by_source[source_kind][bucket] += 1
        total += 1
        key = (source_kind, bucket)
        if len(samples[key]) < max(0, int(args.samples or 0)):
            title = str(row["title"] or "").strip().replace("\n", " ")[:80]
            samples[key].append(f"{int(row['id'])}:{title}")

    print(f"events={total} source_filter={args.source} days={int(args.days or 0)}")
    for source_kind in sorted(by_source):
        counter = by_source[source_kind]
        details = " ".join(f"{bucket}={counter[bucket]}" for bucket in sorted(counter))
        print(f"{source_kind} {details}")
        for bucket in sorted(counter):
            sample_values = samples.get((source_kind, bucket)) or []
            if sample_values:
                print(f"  samples[{bucket}] {' | '.join(sample_values)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
