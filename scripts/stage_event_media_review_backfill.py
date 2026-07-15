#!/usr/bin/env python3
"""Stage unaudited active galleries for automatic event-media review.

Dry-run is the default.  This is the automatic bridge for rows that changed
after the dated visual audit or were created before the Smart Update media gate
was deployed.  It never deletes rows or storage objects and never creates a
manual-review queue: one current image stays approved, every additional logical
image is quarantined and the normal ``event_media_review`` outbox worker decides
one pair per call under its feature-scoped budgets.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from static_site_public_projection import public_occurrence_gate_reason


BACKUP_PREFIX = "codex_backup_event_media_review_stage_20260713"
AUDIT_REASON_PREFIX = "event_image_audit_20260713:"


def _json_list(value: Any) -> list[str]:
    try:
        parsed = value if isinstance(value, list) else json.loads(value or "[]")
    except Exception:
        return []
    return [str(item).strip() for item in parsed if str(item or "").strip()] if isinstance(parsed, list) else []


def _managed(url: str | None) -> bool:
    low = str(url or "").casefold()
    return "/p/dh16/" in low or "/storage/v1/object/" in low or "static.kenigevents.ru/" in low


def _display(row: sqlite3.Row) -> str | None:
    values = [str(row[key] or "").strip() for key in ("supabase_url", "catbox_url")]
    return ([_ for _ in values if _ and _managed(_)] or [_ for _ in values if _] or [None])[0]


def _enqueue(
    con: sqlite3.Connection,
    event_id: int,
    task: str,
    next_run_at: datetime,
) -> None:
    if con.execute(
        "SELECT 1 FROM joboutbox WHERE event_id=? AND task=? AND status IN ('pending','running') LIMIT 1",
        (event_id, task),
    ).fetchone():
        return
    con.execute(
        """
        INSERT INTO joboutbox(event_id, task, status, attempts, updated_at, next_run_at, coalesce_key)
        VALUES (?, ?, 'pending', 0, CURRENT_TIMESTAMP, ?, ?)
        """,
        (
            event_id,
            task,
            next_run_at.astimezone(timezone.utc)
            .replace(tzinfo=None)
            .strftime("%Y-%m-%d %H:%M:%S.%f"),
            f"{task}:{event_id}",
        ),
    )


def _eligible_ids(con: sqlite3.Connection, current_date: str) -> list[int]:
    columns = {str(row[1]) for row in con.execute("PRAGMA table_info(event)")}
    clauses = [
        "date GLOB '20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]'",
        "(date >= :today OR (end_date GLOB '20[0-9][0-9]-[0-9][0-9]-[0-9][0-9]' AND end_date >= :today))",
    ]
    if "silent" in columns:
        clauses.append("COALESCE(silent, 0)=0")
    if "lifecycle_status" in columns:
        clauses.append("COALESCE(NULLIF(TRIM(lifecycle_status),''),'active')='active'")
    if "status" in columns:
        clauses.append("LOWER(COALESCE(NULLIF(TRIM(status),''),'active')) NOT IN ('rejected','quarantine','deleted','inactive')")
    if "moderation_status" in columns:
        clauses.append("LOWER(COALESCE(NULLIF(TRIM(moderation_status),''),'accepted')) NOT IN ('rejected','quarantine')")
    if "identity_status" in columns:
        clauses.append("LOWER(TRIM(identity_status))='canonical'")
    if "merged_into_event_id" in columns:
        clauses.append("merged_into_event_id IS NULL")
    query = "SELECT * FROM event WHERE " + " AND ".join(clauses) + " ORDER BY id"
    return [
        int(row["id"])
        for row in con.execute(query, {"today": current_date}).fetchall()
        if public_occurrence_gate_reason(row, current_date) is None
    ]


def stage(con: sqlite3.Connection, *, current_date: str, apply: bool) -> dict[str, Any]:
    event_ids = _eligible_ids(con, current_date)
    now = datetime.now(timezone.utc)
    planned: list[dict[str, Any]] = []
    for event_id in event_ids:
        event = con.execute("SELECT * FROM event WHERE id=?", (event_id,)).fetchone()
        rows = con.execute(
            "SELECT * FROM eventposter WHERE event_id=? ORDER BY display_order ASC, id ASC",
            (event_id,),
        ).fetchall()
        active = [
            row for row in rows
            if str(row["review_status"] or "") in {"approved", "pending_review"}
        ]
        represented = {
            str(row[field] or "").strip()
            for row in rows
            for field in ("supabase_url", "catbox_url")
            if str(row[field] or "").strip()
        }
        missing_urls = [url for url in _json_list(event["photo_urls"]) if url not in represented]
        already_audited = bool(active) and all(
            str(row["review_reason"] or "").startswith(AUDIT_REASON_PREFIX)
            for row in active
        )
        has_pending = any(str(row["review_status"] or "") == "pending_review" for row in active)
        # Rows created before this feature have no review_reason.  A non-empty
        # reason proves that the dated audit or automatic reviewer already
        # adjudicated the row and must never be undone by a later backfill run.
        has_unreviewed_legacy_multi = len(active) > 1 and any(
            str(row["review_status"] or "") == "approved"
            and not str(row["review_reason"] or "").strip()
            for row in active
        )
        needs_stage = bool(missing_urls or has_pending or has_unreviewed_legacy_multi)
        if needs_stage:
            planned.append(
                {
                    "event_id": event_id,
                    "missing_urls": missing_urls,
                    "active_ids": [int(row["id"]) for row in active],
                    "already_audited": already_audited,
                    "has_unreviewed_legacy_multi": has_unreviewed_legacy_multi,
                    "before_projection": _json_list(event["photo_urls"]),
                    "before_photo_count": int(event["photo_count"] or 0),
                    "has_telegraph_publication": bool(str(event["telegraph_url"] or "").strip() or str(event["telegraph_path"] or "").strip()),
                    "has_managed_vk_publication": bool(str(event["source_vk_post_url"] or "").strip() and str(event["vk_source_hash"] or "").strip()),
                    "has_tg_publication": bool(str(event["tg_event_post_url"] or "").strip() or event["tg_event_post_id"]),
                }
            )

    result = {
        "eligible_events": len(event_ids),
        "staged_events": len(planned),
        "materialized_rows": sum(len(item["missing_urls"]) for item in planned),
        "event_ids": [item["event_id"] for item in planned],
        "public_projection_changed_events": 0,
        "public_rebuild_jobs": {"telegraph_build": 0, "vk_sync": 0, "tg_event_publish": 0, "static_site_build": 0},
        "mode": "apply" if apply else "dry-run",
    }
    if not apply or not planned:
        return result

    ids_sql = ",".join(str(item["event_id"]) for item in planned)
    con.execute(f"CREATE TABLE IF NOT EXISTS {BACKUP_PREFIX}_event AS SELECT * FROM event WHERE id IN ({ids_sql})")
    con.execute(f"CREATE TABLE IF NOT EXISTS {BACKUP_PREFIX}_eventposter AS SELECT * FROM eventposter WHERE event_id IN ({ids_sql})")
    con.execute(f"CREATE TABLE IF NOT EXISTS {BACKUP_PREFIX}_joboutbox AS SELECT * FROM joboutbox WHERE event_id IN ({ids_sql})")

    for ordinal, item in enumerate(planned):
        event_id = int(item["event_id"])
        for url in item["missing_urls"]:
            digest = hashlib.sha256(f"url:{url}".encode()).hexdigest()
            if con.execute(
                "SELECT 1 FROM eventposter WHERE event_id=? AND poster_hash=? LIMIT 1",
                (event_id, digest),
            ).fetchone():
                continue
            con.execute(
                """
                INSERT INTO eventposter(
                    event_id, catbox_url, supabase_url, poster_hash,
                    review_status, review_reason, display_order, updated_at
                ) VALUES (?, ?, ?, ?, 'pending_review', 'legacy_projection_awaiting_automated_review',
                          COALESCE((SELECT MAX(display_order)+1 FROM eventposter WHERE event_id=?), 0),
                          CURRENT_TIMESTAMP)
                """,
                (event_id, None if _managed(url) else url, url if _managed(url) else None, digest, event_id),
            )

        rows = con.execute(
            """
            SELECT * FROM eventposter
            WHERE event_id=? AND review_status IN ('approved','pending_review')
            ORDER BY display_order ASC, id ASC
            """,
            (event_id,),
        ).fetchall()
        if not rows:
            continue
        adjudicated_approved_ids = {
            int(row["id"])
            for row in rows
            if str(row["review_status"] or "") == "approved"
            and str(row["review_reason"] or "").strip()
        }
        blank_approved_ids = [
            int(row["id"])
            for row in rows
            if str(row["review_status"] or "") == "approved"
            and not str(row["review_reason"] or "").strip()
        ]
        seed_id = None if adjudicated_approved_ids else (blank_approved_ids[0] if blank_approved_ids else None)
        for row in rows:
            row_id = int(row["id"])
            current_status = str(row["review_status"] or "")
            current_reason = str(row["review_reason"] or "").strip()
            if current_status == "pending_review":
                status = current_status
                reason = current_reason or "backfill_awaiting_automated_pair_review"
            elif current_reason:
                status = current_status
                reason = current_reason
            elif row_id == seed_id:
                status = "approved"
                reason = "automatic_review_seed"
            else:
                status = "pending_review"
                reason = "backfill_awaiting_automated_pair_review"
            con.execute(
                "UPDATE eventposter SET review_status=?, duplicate_of_id=NULL, review_reason=?, reviewed_at=? WHERE id=?",
                (status, reason, now.isoformat() if status == "approved" else None, row_id),
            )

        approved = con.execute(
            "SELECT * FROM eventposter WHERE event_id=? AND review_status='approved' ORDER BY display_order ASC, id ASC",
            (event_id,),
        ).fetchall()
        projection: list[str] = []
        for row in approved:
            url = _display(row)
            if url and url not in projection:
                projection.append(url)
        con.execute(
            "UPDATE event SET photo_urls=?, photo_count=?, preview_3d_url=NULL WHERE id=?",
            (json.dumps(projection, ensure_ascii=False), len(projection), event_id),
        )
        if con.execute(
            "SELECT 1 FROM eventposter WHERE event_id=? AND review_status='pending_review' LIMIT 1",
            (event_id,),
        ).fetchone():
            _enqueue(con, event_id, "event_media_review", now + timedelta(seconds=ordinal * 2))
        projection_changed = item["before_projection"] != projection or item["before_photo_count"] != len(projection)
        if projection_changed:
            result["public_projection_changed_events"] += 1
        if projection_changed and item["has_telegraph_publication"]:
            _enqueue(con, event_id, "telegraph_build", now + timedelta(seconds=ordinal * 4))
            result["public_rebuild_jobs"]["telegraph_build"] += 1
        if projection_changed and item["has_managed_vk_publication"]:
            _enqueue(con, event_id, "vk_sync", now + timedelta(seconds=ordinal * 15))
            result["public_rebuild_jobs"]["vk_sync"] += 1
        if projection_changed and item["has_tg_publication"]:
            _enqueue(con, event_id, "tg_event_publish", now + timedelta(seconds=ordinal * 90))
            result["public_rebuild_jobs"]["tg_event_publish"] += 1
    if result["public_projection_changed_events"]:
        _enqueue(con, int(planned[0]["event_id"]), "static_site_build", now + timedelta(minutes=20))
        result["public_rebuild_jobs"]["static_site_build"] = 1
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--current-date", default=datetime.now(timezone.utc).date().isoformat())
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--result-output")
    args = parser.parse_args()
    con = sqlite3.connect(Path(args.db))
    con.row_factory = sqlite3.Row
    try:
        con.execute("BEGIN IMMEDIATE" if args.apply else "BEGIN")
        result = stage(con, current_date=args.current_date, apply=bool(args.apply))
        con.commit() if args.apply else con.rollback()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    result["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    output = json.dumps(result, ensure_ascii=False, indent=2)
    print(output)
    if args.result_output:
        Path(args.result_output).write_text(output + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
