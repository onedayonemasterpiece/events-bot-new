"""Honest today's-selection for AfishaThumb.

Picks the 6 events that would land on the column today, mirroring the
operational CherryFlash `popular_review` selection but without the
weekly anti-repeat join (we don't have history for the new
`afishathumb` profile_key yet). Hard rules from requirements:

- future-start-only (`date >= today`)
- has at least one non-`files.catbox.moe` poster URL
- non-empty `search_digest` (so we can show essence stickers)
- 6 events, ordered by popularity proxy (here: by id descending —
  newer rows have generally been added closer to surface volume)

Writes `artifacts/afishathumb/selection_today.json` with the picks.
"""

from __future__ import annotations

import argparse
import datetime
import json
import sqlite3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = REPO_ROOT / "db_prod_snapshot.sqlite"
OUT = REPO_ROOT / "artifacts" / "afishathumb" / "selection_today.json"


def _renderable(urls_json: str | None) -> bool:
    try:
        urls = json.loads(urls_json or "[]")
    except Exception:
        return False
    return any(u and "catbox.moe" not in u for u in urls)


def select(n: int = 6) -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = datetime.date.today().isoformat()
    rows = c.execute(
        "SELECT id,title,date,time,location_name,location_address,city,"
        "is_free,ticket_price_min,ticket_price_max,search_digest,photo_urls "
        "FROM event WHERE date >= ? AND photo_urls IS NOT NULL AND photo_urls != '[]' "
        "AND search_digest IS NOT NULL AND search_digest != '' "
        "ORDER BY id DESC",
        (today,),
    ).fetchall()
    cols = ["id","title","date","time","location_name","location_address",
            "city","is_free","ticket_price_min","ticket_price_max",
            "search_digest","photo_urls"]
    picks: list[dict] = []
    for row in rows:
        d = dict(zip(cols, row))
        if not _renderable(d["photo_urls"]):
            continue
        # Skip near-duplicate titles (same title + same date → likely a
        # double-import). Cheap dedupe by (title.lower(), date).
        key = (d["title"].lower().strip(), d["date"])
        if any((p["title"].lower().strip(), p["date"]) == key for p in picks):
            continue
        picks.append(d)
        if len(picks) >= n:
            break
    return picks


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", type=int, default=6)
    args = ap.parse_args()
    picks = select(args.n)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(picks, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[selection] wrote {len(picks)} events to {OUT}")
    for p in picks:
        free_or_price = "БЕСПЛАТНО" if p["is_free"] else (f"{p['ticket_price_min']}₽" if p["ticket_price_min"] else "уточн.")
        print(f"  {p['id']:5d}  {p['date']} {p['time']:8s}  {p['title'][:60]:60s}  {free_or_price}")


if __name__ == "__main__":
    main()
