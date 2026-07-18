#!/usr/bin/env python3
"""Read-only inventory of projected static events without canonical media."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import unicodedata
import urllib.parse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "static_no_image_inventory_v1"
REASONS = ("no_ledger", "no_approved", "approved_non_cdn", "projection_mismatch")


def clean(value: Any) -> str:
    return " ".join(str(value or "").split())


def normalize_event_type(value: Any) -> str:
    normalized = unicodedata.normalize("NFKC", clean(value)).casefold()
    return normalized or "unknown"


def is_canonical_cdn_url(value: Any, asset_base_url: str) -> bool:
    raw = clean(value)
    if not raw:
        return False
    try:
        url = urllib.parse.urlsplit(raw)
        cdn = urllib.parse.urlsplit(asset_base_url)
    except ValueError:
        return False
    if url.scheme not in {"http", "https"} or not url.netloc or not cdn.netloc:
        return False
    if url.netloc.casefold() == cdn.netloc.casefold():
        return True
    return url.netloc.casefold() == "storage.yandexcloud.net" and url.path.startswith("/kenigevents.ru/")


def open_read_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("pragma query_only=on")
    return connection


def has_table(connection: sqlite3.Connection, table: str) -> bool:
    return connection.execute(
        "select 1 from sqlite_master where type='table' and name=?", (table,)
    ).fetchone() is not None


def poster_rows(connection: sqlite3.Connection, event_id: int) -> list[sqlite3.Row]:
    if not has_table(connection, "eventposter"):
        return []
    columns = {str(row[1]) for row in connection.execute("pragma table_info('eventposter')")}
    status_sql = "review_status" if "review_status" in columns else "'approved' as review_status"
    url_sql = "supabase_url" if "supabase_url" in columns else "NULL as supabase_url"
    return connection.execute(
        f"select {status_sql}, {url_sql} from eventposter where event_id=? order by id", (event_id,)
    ).fetchall()


def classify_no_image_reason(
    connection: sqlite3.Connection,
    event: dict[str, Any],
    *,
    asset_base_url: str,
) -> tuple[str, dict[str, int]]:
    rows = poster_rows(connection, int(event["id"]))
    evidence = {"ledger_rows": len(rows), "approved_rows": 0, "approved_cdn_rows": 0}
    if not rows:
        return "no_ledger", evidence
    approved = [row for row in rows if clean(row["review_status"]).casefold() == "approved"]
    evidence["approved_rows"] = len(approved)
    if not approved:
        return "no_approved", evidence
    approved_cdn = [row for row in approved if is_canonical_cdn_url(row["supabase_url"], asset_base_url)]
    evidence["approved_cdn_rows"] = len(approved_cdn)
    if not approved_cdn:
        return "approved_non_cdn", evidence
    return "projection_mismatch", evidence


def parse_timestamp(value: Any) -> datetime | None:
    raw = clean(value)
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_utc(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if value else None


def build_inventory(
    connection: sqlite3.Connection,
    preview_payload: dict[str, Any],
    *,
    db_path: Path,
    as_of: datetime,
    asset_base_url: str,
) -> dict[str, Any]:
    inventory_date = as_of.date().isoformat()
    all_no_image_events = [
        event for event in preview_payload.get("events", [])
        if not clean(event.get("image_url")) and not event.get("image_assets")
    ]
    no_image_events = [
        event for event in all_no_image_events
        if clean(event.get("lifecycle_status")).casefold() not in {"cancelled", "deleted", "duplicate"}
        and (clean(event.get("end_date") or event.get("start_date")) or inventory_date) >= inventory_date
    ]
    past_or_inactive_count = len(all_no_image_events) - len(no_image_events)
    rows: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    type_counts: Counter[str] = Counter()
    reason_type_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for event in sorted(no_image_events, key=lambda item: int(item["id"])):
        reason, evidence = classify_no_image_reason(connection, event, asset_base_url=asset_base_url)
        event_type = normalize_event_type(event.get("event_type"))
        reason_counts[reason] += 1
        type_counts[event_type] += 1
        reason_type_counts[reason][event_type] += 1
        rows.append({
            "event_id": int(event["id"]),
            "title": clean(event.get("title")),
            "event_type": event_type,
            "reason": reason,
            **evidence,
        })

    build_generated_at = parse_timestamp(preview_payload.get("build", {}).get("generated_at"))
    max_event_updated_at: datetime | None = None
    if has_table(connection, "event") and "updated_at" in {
        str(row[1]) for row in connection.execute("pragma table_info('event')")
    }:
        max_event_updated_at = parse_timestamp(connection.execute("select max(updated_at) from event").fetchone()[0])
    db_mtime = datetime.fromtimestamp(db_path.stat().st_mtime, tz=timezone.utc)
    reference = build_generated_at or max_event_updated_at or db_mtime
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": iso_utc(as_of),
        "snapshot": {
            "db_path": str(db_path),
            "db_mtime": iso_utc(db_mtime),
            "max_event_updated_at": iso_utc(max_event_updated_at),
            "preview_generated_at": iso_utc(build_generated_at),
            "freshness_reference": iso_utc(reference),
            "age_hours": round(max(0.0, (as_of - reference).total_seconds() / 3600), 3),
        },
        "summary": {
            "no_image_total": len(rows),
            "past_or_inactive_projection_count": past_or_inactive_count,
            "by_reason": {reason: reason_counts.get(reason, 0) for reason in REASONS},
            "by_event_type": dict(sorted(type_counts.items())),
            "by_reason_and_event_type": {
                reason: dict(sorted(reason_type_counts[reason].items())) for reason in REASONS
            },
        },
        "events": rows,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True, help="SQLite snapshot opened with mode=ro")
    parser.add_argument("--preview-events", type=Path, required=True, help="Exported preview-events.json")
    parser.add_argument("--output", type=Path, help="Optional JSON report path; stdout is always emitted")
    parser.add_argument("--as-of", help="Deterministic ISO timestamp for freshness calculations")
    parser.add_argument("--asset-base-url", default=os.getenv("PUBLIC_ASSET_BASE_URL", "https://static.kenigevents.ru"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    as_of = parse_timestamp(args.as_of) if args.as_of else datetime.now(timezone.utc)
    if as_of is None:
        raise SystemExit("--as-of must be an ISO timestamp")
    preview_payload = json.loads(args.preview_events.read_text(encoding="utf-8"))
    with open_read_only(args.db) as connection:
        report = build_inventory(
            connection,
            preview_payload,
            db_path=args.db,
            as_of=as_of,
            asset_base_url=args.asset_base_url.rstrip("/"),
        )
    rendered = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
