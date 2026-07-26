#!/usr/bin/env python3
"""Seed the accepted 2026 festival calendar into core SQLite.

The script is intentionally dry-run by default.  It writes only the dedicated
``festival_calendar_item`` edition table and never rewrites retained rows in
the legacy ``festival`` table.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import unicodedata
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SEED = ROOT / "site/src/data/festivalTimelineSeed.json"
EXPECTED_STATUS_COUNTS = {
    "announced": 9,
    "program-pending": 8,
    "date-pending": 4,
}
EXPECTED_MONTH_COUNTS = {
    "july": 1,
    "august": 7,
    "september": 5,
    "october": 5,
    "november": 2,
    "december": 1,
}
DB_FIELDS = (
    "calendar_year",
    "slug",
    "title",
    "description",
    "start_date",
    "end_date",
    "date_precision",
    "date_label",
    "sort_date",
    "month_key",
    "display_order",
    "place_label",
    "category",
    "status",
    "status_label",
    "source_url",
    "source_label",
    "internal_event_id",
    "festival_id",
    "cover_key",
    "image_width",
    "image_height",
    "media_mode",
    "object_position",
    "catalog_version",
    "is_public",
)


def normalized_identity(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return " ".join("".join(char if char.isalnum() else " " for char in text).split())


def load_seed(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "festival_timeline_seed_v1":
        raise ValueError("unsupported festival timeline seed schema")
    items = payload.get("items")
    if not isinstance(items, list) or len(items) != 21:
        raise ValueError("festival timeline seed must contain exactly 21 items")
    slugs = [str(item.get("slug") or "") for item in items]
    orders = [int(item.get("displayOrder") or 0) for item in items]
    if len(set(slugs)) != 21 or len(set(orders)) != 21 or sorted(orders) != list(range(1, 22)):
        raise ValueError("festival timeline slugs/orders must be unique and contiguous")
    status_counts: dict[str, int] = {}
    month_counts: dict[str, int] = {}
    for item in items:
        status_counts[item["status"]] = status_counts.get(item["status"], 0) + 1
        month_counts[item["monthKey"]] = month_counts.get(item["monthKey"], 0) + 1
        review = item.get("mediaReview") or {}
        if (
            review.get("semanticClass") != item.get("mediaMode")
            or len(str(review.get("assetSha256") or "")) != 64
        ):
            raise ValueError(f"invalid media provenance for {item.get('slug')}")
        if item.get("datePrecision") != "exact" and not item.get("dateLabel"):
            raise ValueError(f"imprecise date requires a truthful label: {item.get('slug')}")
    if status_counts != EXPECTED_STATUS_COUNTS:
        raise ValueError(f"unexpected festival status counts: {status_counts}")
    if month_counts != EXPECTED_MONTH_COUNTS:
        raise ValueError(f"unexpected festival month counts: {month_counts}")
    return payload


def db_row(item: dict[str, Any], catalog_version: str) -> dict[str, Any]:
    return {
        "calendar_year": int(item["calendarYear"]),
        "slug": item["slug"],
        "title": item["title"],
        "description": item["description"],
        "start_date": item.get("startDate"),
        "end_date": item.get("endDate"),
        "date_precision": item["datePrecision"],
        "date_label": item["dateLabel"],
        "sort_date": item["sortDate"],
        "month_key": item["monthKey"],
        "display_order": int(item["displayOrder"]),
        "place_label": item["placeLabel"],
        "category": item["category"],
        "status": item["status"],
        "status_label": item["statusLabel"],
        "source_url": item["sourceUrl"],
        "source_label": item["sourceLabel"],
        "internal_event_id": item.get("internalEventId"),
        "festival_id": item.get("festivalId"),
        "cover_key": item["coverKey"],
        "image_width": int(item["imageWidth"]),
        "image_height": int(item["imageHeight"]),
        "media_mode": item["mediaMode"],
        "object_position": item.get("objectPosition"),
        "catalog_version": catalog_version,
        "is_public": 1 if item.get("isPublic", True) else 0,
    }


def validate_links(
    connection: sqlite3.Connection,
    item: dict[str, Any],
    *,
    strict: bool,
) -> tuple[int | None, int | None]:
    festival_id = item.get("festivalId")
    if festival_id is not None:
        row = connection.execute(
            "SELECT id,name,full_name,aliases FROM festival WHERE id=?",
            (int(festival_id),),
        ).fetchone()
        if row is None:
            if strict:
                raise ValueError(f"missing legacy festival id {festival_id} for {item['slug']}")
            festival_id = None
        else:
            candidates = {
                normalized_identity(item["title"]),
                *(normalized_identity(alias) for alias in item.get("aliases") or []),
            }
            stored = {normalized_identity(row["name"]), normalized_identity(row["full_name"])}
            try:
                stored.update(
                    normalized_identity(alias)
                    for alias in json.loads(row["aliases"] or "[]")
                )
            except (TypeError, ValueError):
                pass
            if candidates.isdisjoint(stored):
                raise ValueError(
                    f"legacy festival id {festival_id} identity conflict for {item['slug']}"
                )
    event_id = item.get("internalEventId")
    if event_id is not None:
        exists = connection.execute(
            "SELECT 1 FROM event WHERE id=?",
            (int(event_id),),
        ).fetchone()
        if not exists:
            if strict:
                raise ValueError(f"missing internal event id {event_id} for {item['slug']}")
            event_id = None
    return (
        int(festival_id) if festival_id is not None else None,
        int(event_id) if event_id is not None else None,
    )


def backfill(
    database_path: Path,
    seed_path: Path = DEFAULT_SEED,
    *,
    apply: bool = False,
    strict_links: bool = True,
) -> dict[str, Any]:
    seed = load_seed(seed_path)
    catalog_version = str(seed["catalog_version"])
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    try:
        if connection.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ValueError("SQLite quick_check failed before festival backfill")
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "festival_calendar_item" not in tables:
            raise ValueError(
                "festival_calendar_item is missing; run Database.init/schema migration first"
            )

        expected_orders: dict[int, str] = {}
        for row in connection.execute(
            "SELECT slug,display_order FROM festival_calendar_item WHERE calendar_year=?",
            (int(seed["calendar_year"]),),
        ):
            expected_orders[int(row["display_order"])] = str(row["slug"])

        inserts = updates = unchanged = 0
        prepared: list[dict[str, Any]] = []
        for item in seed["items"]:
            occupied = expected_orders.get(int(item["displayOrder"]))
            if occupied and occupied != item["slug"]:
                raise ValueError(
                    f"display-order conflict {item['displayOrder']}: {occupied} != {item['slug']}"
                )
            festival_id, event_id = validate_links(
                connection,
                item,
                strict=strict_links,
            )
            record = db_row(item, catalog_version)
            record["festival_id"] = festival_id
            record["internal_event_id"] = event_id
            current = connection.execute(
                "SELECT * FROM festival_calendar_item WHERE calendar_year=? AND slug=?",
                (record["calendar_year"], record["slug"]),
            ).fetchone()
            if current is None:
                inserts += 1
            elif all(current[field] == record[field] for field in DB_FIELDS):
                unchanged += 1
            else:
                updates += 1
            prepared.append(record)

        if apply:
            placeholders = ",".join("?" for _ in DB_FIELDS)
            update_sql = ",".join(
                f"{field}=excluded.{field}"
                for field in DB_FIELDS
                if field not in {"calendar_year", "slug"}
            )
            sql = (
                f"INSERT INTO festival_calendar_item ({','.join(DB_FIELDS)}) "
                f"VALUES ({placeholders}) "
                "ON CONFLICT(calendar_year,slug) DO UPDATE SET "
                f"{update_sql},updated_at=CURRENT_TIMESTAMP"
            )
            connection.execute("BEGIN IMMEDIATE")
            for record in prepared:
                connection.execute(sql, tuple(record[field] for field in DB_FIELDS))
            connection.commit()

        counts = connection.execute(
            """
            SELECT COUNT(*) AS total,
                   COUNT(DISTINCT slug) AS slugs,
                   COUNT(DISTINCT display_order) AS orders
            FROM festival_calendar_item
            WHERE calendar_year=? AND is_public=1
            """,
            (int(seed["calendar_year"]),),
        ).fetchone()
        return {
            "status": "applied" if apply else "dry-run",
            "catalog_version": catalog_version,
            "inserts": inserts,
            "updates": updates,
            "unchanged": unchanged,
            "public_count_after": int(counts["total"]) if apply else None,
            "public_distinct_slugs_after": int(counts["slugs"]) if apply else None,
            "public_distinct_orders_after": int(counts["orders"]) if apply else None,
        }
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--seed", type=Path, default=DEFAULT_SEED)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--allow-missing-links",
        action="store_true",
        help="Clear unavailable optional festival/event FKs instead of failing.",
    )
    args = parser.parse_args()
    try:
        report = backfill(
            args.db,
            args.seed,
            apply=args.apply,
            strict_links=not args.allow_missing_links,
        )
    except Exception as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
