#!/usr/bin/env python3
"""Audit public-active KenigEvents media from a local read-only SQLite snapshot."""
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


def canonical_url(value: str | None) -> str:
    if not value:
        return ""
    p = urlsplit(value.strip())
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path, "", ""))


def parse_urls(raw: object) -> list[str]:
    if not raw:
        return []
    try:
        value = json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        url = canonical_url(str(item)) if item else ""
        if url and url not in result:
            result.append(url)
    return result


def meaningful_ocr(value: str | None) -> bool:
    text = " ".join((value or "").split())
    lowered = text.lower()
    if any(marker in lowered for marker in ("no text", "нет текста", "без текста")):
        return False
    return len(text) >= 60 and sum(ch.isalpha() for ch in text) >= 20


def orientation(width: int | None, height: int | None) -> str:
    if not width or not height:
        return "unknown"
    ratio = width / height
    if ratio < 0.9:
        return "portrait"
    if ratio > 1.1:
        return "landscape"
    return "square"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, type=Path)
    ap.add_argument("--as-of", default=date.today().isoformat())
    ap.add_argument("--output", type=Path)
    args = ap.parse_args()

    db_uri = f"file:{args.db.resolve()}?mode=ro"
    con = sqlite3.connect(db_uri, uri=True)
    con.row_factory = sqlite3.Row
    if con.execute("PRAGMA quick_check").fetchone()[0] != "ok":
        raise SystemExit("SQLite quick_check failed")

    event_cols = {r[1] for r in con.execute("PRAGMA table_info(event)")}
    required = {"id", "date", "end_date", "lifecycle_status", "silent", "photo_urls"}
    missing = sorted(required - event_cols)
    if missing:
        raise SystemExit(f"event table missing columns: {', '.join(missing)}")

    rows = con.execute(
        """
        SELECT id, title, date, end_date, lifecycle_status, silent, photo_count, photo_urls
        FROM event
        WHERE COALESCE(silent, 0)=0
          AND lifecycle_status='active'
          AND date(date) IS NOT NULL
          AND (date(date) >= date(?) OR date(COALESCE(end_date, date)) >= date(?))
        ORDER BY id
        """,
        (args.as_of, args.as_of),
    ).fetchall()

    poster_rows = con.execute(
        """
        SELECT event_id, supabase_url, catbox_url, width, height, ocr_text,
               image_text_mode, media_role, focal_x, focal_y, safe_crop,
               thumbnail_256_url, thumbnail_512_url, display_order, review_status
        FROM eventposter
        WHERE review_status='approved'
        """
    ).fetchall()
    by_event_url: dict[tuple[int, str], sqlite3.Row] = {}
    for row in poster_rows:
        for field in ("supabase_url", "catbox_url"):
            url = canonical_url(row[field])
            if url:
                by_event_url[(row["event_id"], url)] = row

    gallery_size_buckets = Counter()
    orientation_event_buckets = Counter()
    asset_orientations = Counter()
    ocr_sets = Counter()
    matched_assets = 0
    unknown_assets = 0
    quality_large_events = 0
    strict_photo_events = 0
    records = []

    for event in rows:
        urls = parse_urls(event["photo_urls"])
        assets = []
        orientations = set()
        ocr_values = []
        has_large = False
        has_strict_photo = False
        for url in urls:
            poster = by_event_url.get((event["id"], url))
            if poster is None:
                unknown_assets += 1
                assets.append({"url": url, "matched_approved": False})
                continue
            matched_assets += 1
            orient = orientation(poster["width"], poster["height"])
            orientations.add(orient)
            asset_orientations[orient] += 1
            is_ocr = meaningful_ocr(poster["ocr_text"])
            ocr_values.append(is_ocr)
            width, height = poster["width"] or 0, poster["height"] or 0
            large = min(width, height) >= 720 and width * height >= 900_000
            strict_photo = (
                poster["image_text_mode"] == "visual_only"
                and poster["media_role"] == "event_photo"
                and poster["safe_crop"] == 1
                and poster["focal_x"] is not None
                and poster["focal_y"] is not None
                and min(width, height) >= 640
            )
            has_large |= large
            has_strict_photo |= strict_photo
            assets.append(
                {
                    "url": url,
                    "matched_approved": True,
                    "width": width or None,
                    "height": height or None,
                    "orientation": orient,
                    "meaningful_ocr": is_ocr,
                    "image_text_mode": poster["image_text_mode"],
                    "media_role": poster["media_role"],
                    "safe_crop": poster["safe_crop"],
                    "focal": [poster["focal_x"], poster["focal_y"]],
                    "large_card_quality": large,
                    "strict_sales_photo": strict_photo,
                }
            )
        count = len(urls)
        gallery_size_buckets["no_image" if count == 0 else "one_image" if count == 1 else "many_images"] += 1
        known_orients = orientations - {"unknown"}
        orient_bucket = (
            "no_image" if not urls else "incomplete" if not known_orients else
            next(iter(known_orients)) + "_only" if len(known_orients) == 1 else "mixed_orientation"
        )
        orientation_event_buckets[orient_bucket] += 1
        ocr_bucket = "no_image" if not urls else "mixed_ocr" if any(ocr_values) and not all(ocr_values) else "all_ocr" if ocr_values and all(ocr_values) else "no_ocr"
        ocr_sets[ocr_bucket] += 1
        quality_large_events += int(has_large)
        strict_photo_events += int(has_strict_photo)
        records.append({"id": event["id"], "title": event["title"], "urls": urls, "assets": assets, "orientation_bucket": orient_bucket, "ocr_bucket": ocr_bucket})

    report = {
        "as_of": args.as_of,
        "quick_check": "ok",
        "public_active_events": len(rows),
        "gallery_size_buckets": dict(gallery_size_buckets),
        "orientation_event_buckets": dict(orientation_event_buckets),
        "ocr_event_buckets": dict(ocr_sets),
        "approved_asset_orientations": dict(asset_orientations),
        "canonical_url_assets": sum(len(parse_urls(row["photo_urls"])) for row in rows),
        "matched_approved_assets": matched_assets,
        "unmatched_canonical_assets": unknown_assets,
        "events_with_large_card_quality_asset": quality_large_events,
        "events_with_strict_sales_photo": strict_photo_events,
        "events": records,
    }
    payload = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "events"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
