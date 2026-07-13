#!/usr/bin/env python3
"""Apply the reviewed event-image audit with strict stale checks.

Dry-run is the default.  The script never deletes EventPoster or storage
objects: redundant rows become ``duplicate`` and broken rows ``unavailable``.
Before ``--apply`` it snapshots every touched table into dated backup tables.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


AUDIT_TAG = "event_image_audit_20260713"
BACKUP_PREFIX = "codex_backup_event_media_cleanup_20260713"


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item or "").strip()]
    try:
        parsed = json.loads(value or "[]")
    except Exception:
        return []
    return [str(item).strip() for item in parsed if str(item or "").strip()] if isinstance(parsed, list) else []


def _managed(url: str) -> bool:
    low = str(url or "").casefold()
    return (
        "/p/dh16/" in low
        or "/storage/v1/object/" in low
        or "storage.yandexcloud.net/kenigevents/" in low
        or "static.kenigevents.ru/" in low
    )


def _legacy_gallery(event: sqlite3.Row, posters: list[sqlite3.Row]) -> list[str]:
    out: list[str] = []
    for raw in _json_list(event["photo_urls"]):
        if raw not in out:
            out.append(raw)
    for row in posters:
        for field in ("supabase_url", "catbox_url"):
            raw = str(row[field] or "").strip()
            if raw and raw not in out:
                out.append(raw)
    return out


def _display_url(row: sqlite3.Row | dict[str, Any]) -> str | None:
    values = [str(row[key] or "").strip() for key in ("supabase_url", "catbox_url")]
    managed = [value for value in values if value and _managed(value)]
    return (managed or [value for value in values if value] or [None])[0]


def _load_inventory(path: Path) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            value = json.loads(line)
            out[int(value["event"]["id"])] = value
    return out


def _load_reviews(path: Path) -> dict[int, dict[str, Any]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return {int(row["event_id"]): row for row in csv.DictReader(handle)}


def _load_downloads(path: Path) -> dict[str, dict[str, Any]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    return {str(item["url"]): item for item in value.get("items", [])}


def _choose_group_survivor(group: list[int], gallery: list[dict[str, Any]], downloads: dict[str, dict[str, Any]]) -> int:
    def score(position: int) -> tuple[int, int, int, int]:
        item = gallery[position]
        url = str(item.get("url") or "")
        manifest = downloads.get(url) or {}
        available = int(manifest.get("http_status") == 200 and not manifest.get("download_error") and not manifest.get("decode_error"))
        area = int(manifest.get("width") or 0) * int(manifest.get("height") or 0)
        return available, int(_managed(url)), area, -position

    return max(group, key=score)


def build_plan(audit_dir: Path) -> dict[str, Any]:
    inventory = _load_inventory(audit_dir / "inventory.jsonl")
    reviews = _load_reviews(audit_dir / "visual-review.csv")
    downloads = _load_downloads(audit_dir / "downloaded-media-manifest.json")
    events: list[dict[str, Any]] = []
    for event_id, ledger in sorted(inventory.items()):
        gallery = list(ledger.get("static_gallery") or [])
        review = reviews.get(event_id)
        groups = json.loads(review.get("confirmed_duplicate_groups") or "[]") if review else []
        loser_positions: set[int] = set()
        survivor_by_loser: dict[int, int] = {}
        for raw_group in groups:
            group = sorted({int(value) for value in raw_group if 0 <= int(value) < len(gallery)})
            if len(group) < 2:
                continue
            survivor = _choose_group_survivor(group, gallery, downloads)
            for position in group:
                if position != survivor:
                    loser_positions.add(position)
                    survivor_by_loser[position] = survivor
        unavailable_positions: set[int] = set()
        for position, item in enumerate(gallery):
            manifest = downloads.get(str(item.get("url") or "")) or {}
            usable = (
                manifest.get("http_status") == 200
                and not manifest.get("download_error")
                and not manifest.get("decode_error")
                and bool(manifest.get("sha256"))
            )
            if not usable:
                unavailable_positions.add(position)
        events.append(
            {
                "event_id": event_id,
                "title": ledger["event"].get("title"),
                "date": ledger["event"].get("date"),
                "expected_gallery": [str(item.get("url") or "") for item in gallery],
                "gallery": gallery,
                "loser_positions": sorted(loser_positions),
                "survivor_by_loser": {str(key): value for key, value in survivor_by_loser.items()},
                "unavailable_positions": sorted(unavailable_positions),
                "classification": review.get("classification") if review else "single_or_empty",
                "visual_review_status": review.get("visual_review_status") if review else "not_required_lt2",
            }
        )
    return {
        "schema_version": 1,
        "audit_tag": AUDIT_TAG,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "events": events,
    }


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"PRAGMA table_info({table})")}


def _backup(con: sqlite3.Connection, event_ids: list[int]) -> None:
    if not event_ids:
        return
    ids = ",".join(str(int(value)) for value in sorted(set(event_ids)))
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {BACKUP_PREFIX}_event AS SELECT * FROM event WHERE id IN ({ids})"
    )
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {BACKUP_PREFIX}_eventposter AS SELECT * FROM eventposter WHERE event_id IN ({ids})"
    )
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {BACKUP_PREFIX}_joboutbox AS SELECT * FROM joboutbox WHERE event_id IN ({ids})"
    )


def _insert_job(
    con: sqlite3.Connection,
    *,
    event_id: int,
    task: str,
    next_run_at: str,
    coalesce_key: str,
) -> None:
    exists = con.execute(
        """
        SELECT 1 FROM joboutbox
        WHERE event_id=? AND task=? AND status IN ('pending','running')
        LIMIT 1
        """,
        (event_id, task),
    ).fetchone()
    if exists:
        return
    con.execute(
        """
        INSERT INTO joboutbox(
            event_id, task, status, attempts, updated_at, next_run_at, coalesce_key
        ) VALUES (?, ?, 'pending', 0, CURRENT_TIMESTAMP, ?, ?)
        """,
        (event_id, task, next_run_at, coalesce_key),
    )


def apply_plan(con: sqlite3.Connection, plan: dict[str, Any], *, apply: bool) -> dict[str, Any]:
    required = {
        "review_status",
        "duplicate_of_id",
        "review_reason",
        "display_order",
        "raw_sha256",
        "pixel_sha256",
        "perceptual_hash",
    }
    missing = required - _table_columns(con, "eventposter")
    if missing:
        raise RuntimeError(f"event-media schema is not deployed; missing={sorted(missing)}")

    downloads: dict[str, dict[str, Any]] = {}
    audit_dir_value = plan.get("_audit_dir")
    if audit_dir_value:
        downloads = _load_downloads(Path(audit_dir_value) / "downloaded-media-manifest.json")

    stats = {
        "planned_events": len(plan.get("events") or []),
        "unchanged_events": 0,
        "stale_skipped_events": 0,
        "missing_events": 0,
        "materialized_rows": 0,
        "duplicate_rows": 0,
        "unavailable_rows": 0,
        "pending_review_rows": 0,
        "projection_refs_before": 0,
        "projection_refs_after": 0,
        "public_projection_changed_events": 0,
        "public_rebuild_jobs": {"telegraph_build": 0, "vk_sync": 0, "tg_event_publish": 0, "static_site_build": 0},
        "changed_event_ids": [],
        "stale": [],
    }
    changes: list[dict[str, Any]] = []
    for item in plan.get("events") or []:
        event_id = int(item["event_id"])
        event = con.execute("SELECT * FROM event WHERE id=?", (event_id,)).fetchone()
        if event is None:
            stats["missing_events"] += 1
            continue
        posters = con.execute(
            "SELECT * FROM eventposter WHERE event_id=? ORDER BY id ASC",
            (event_id,),
        ).fetchall()
        current_gallery = _legacy_gallery(event, posters)
        expected_gallery = list(item.get("expected_gallery") or [])
        # The audited exporter intentionally caps a gallery at 12. A longer
        # legacy DB union is not stale when the visible prefix is unchanged;
        # its unaudited tail is quarantined below for automatic review.
        if (
            str(event["title"] or "") != str(item.get("title") or "")
            or str(event["date"] or "") != str(item.get("date") or "")
            or current_gallery[: len(expected_gallery)] != expected_gallery
        ):
            stats["stale_skipped_events"] += 1
            stats["stale"].append(
                {
                    "event_id": event_id,
                    "expected_count": len(expected_gallery),
                    "current_count": len(current_gallery),
                }
            )
            continue

        gallery = list(item.get("gallery") or [])
        loser_positions = {int(value) for value in item.get("loser_positions") or []}
        unavailable_positions = {int(value) for value in item.get("unavailable_positions") or []}
        survivor_by_loser = {int(key): int(value) for key, value in (item.get("survivor_by_loser") or {}).items()}
        rows_by_id = {int(row["id"]): row for row in posters}
        rows_by_url: dict[str, list[sqlite3.Row]] = {}
        for row in posters:
            for field in ("supabase_url", "catbox_url"):
                url = str(row[field] or "").strip()
                if url:
                    rows_by_url.setdefault(url, []).append(row)

        position_row_id: dict[int, int] = {}
        new_rows: list[dict[str, Any]] = []
        for position, gallery_item in enumerate(gallery):
            url = str(gallery_item.get("url") or "").strip()
            matches = rows_by_url.get(url) or []
            if matches:
                position_row_id[position] = int(matches[0]["id"])
                continue
            if position in loser_positions or position in unavailable_positions:
                continue
            manifest = downloads.get(url) or {}
            raw_sha = str(manifest.get("sha256") or "").strip() or None
            poster_hash = raw_sha or hashlib.sha256(f"url:{url}".encode()).hexdigest()
            existing = con.execute(
                "SELECT id FROM eventposter WHERE event_id=? AND poster_hash=?",
                (event_id, poster_hash),
            ).fetchone()
            if existing:
                position_row_id[position] = int(existing[0])
                continue
            new_rows.append(
                {
                    "position": position,
                    "url": url,
                    "poster_hash": poster_hash,
                    "raw_sha256": raw_sha,
                    "phash": str(manifest.get("dhash16") or "").strip() or None,
                    "width": manifest.get("width"),
                    "height": manifest.get("height"),
                    "mime_type": manifest.get("mime"),
                }
            )

        changes.append(
            {
                "event_id": event_id,
                "gallery": gallery,
                "loser_positions": loser_positions,
                "unavailable_positions": unavailable_positions,
                "survivor_by_loser": survivor_by_loser,
                "position_row_id": position_row_id,
                "new_rows": new_rows,
                "unreviewed_extra_urls": current_gallery[len(expected_gallery) :],
                "posters": posters,
                "before_projection": _json_list(event["photo_urls"]),
                "before_photo_count": int(event["photo_count"] or 0),
                "has_telegraph_publication": bool(str(event["telegraph_url"] or "").strip() or str(event["telegraph_path"] or "").strip()),
                # source_vk_post_url may point to an external source.  A
                # persisted renderer hash is the existing managed-post proof.
                "has_managed_vk_publication": bool(str(event["source_vk_post_url"] or "").strip() and str(event["vk_source_hash"] or "").strip()),
                "has_tg_publication": bool(str(event["tg_event_post_url"] or "").strip() or event["tg_event_post_id"]),
            }
        )

    if not apply:
        stats["changed_event_ids"] = [item["event_id"] for item in changes]
        stats["projection_refs_before"] = sum(len(item["gallery"]) for item in changes)
        stats["projection_refs_after"] = sum(
            len(item["gallery"]) - len(item["loser_positions"] | item["unavailable_positions"])
            for item in changes
        )
        stats["public_projection_changed_events"] = sum(
            1
            for item in changes
            if item["before_projection"]
            != [
                str(gallery_item.get("url") or "")
                for position, gallery_item in enumerate(item["gallery"])
                if position not in item["loser_positions"] | item["unavailable_positions"]
                and str(gallery_item.get("url") or "")
            ]
            or item["before_photo_count"]
            != len(item["gallery"]) - len(item["loser_positions"] | item["unavailable_positions"])
        )
        return stats

    _backup(con, [item["event_id"] for item in changes])
    now = datetime.now(timezone.utc)
    for change in changes:
        event_id = int(change["event_id"])
        stats["projection_refs_before"] += len(change["gallery"])
        for row in change["new_rows"]:
            supabase_url = row["url"] if _managed(row["url"]) else None
            catbox_url = None if supabase_url else row["url"]
            cursor = con.execute(
                """
                INSERT INTO eventposter(
                    event_id, catbox_url, supabase_url, poster_hash, phash,
                    raw_sha256, width, height, mime_type, review_status,
                    review_reason, reviewed_at, display_order, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'approved', ?, CURRENT_TIMESTAMP, ?, CURRENT_TIMESTAMP)
                """,
                (
                    event_id,
                    catbox_url,
                    supabase_url,
                    row["poster_hash"],
                    row["phash"],
                    row["raw_sha256"],
                    row["width"],
                    row["height"],
                    row["mime_type"],
                    f"{AUDIT_TAG}:materialized",
                    int(row["position"]),
                ),
            )
            change["position_row_id"][int(row["position"])] = int(cursor.lastrowid)
            stats["materialized_rows"] += 1

        # Preserve every DB reference beyond the 12-item visual audit cap, but
        # never approve it by inference. The normal outbox reviewer owns it.
        for url in change["unreviewed_extra_urls"]:
            existing = con.execute(
                "SELECT id FROM eventposter WHERE event_id=? AND (catbox_url=? OR supabase_url=?) ORDER BY id LIMIT 1",
                (event_id, url, url),
            ).fetchone()
            if existing:
                continue
            digest = hashlib.sha256(f"url:{url}".encode()).hexdigest()
            con.execute(
                """
                INSERT INTO eventposter(
                    event_id, catbox_url, supabase_url, poster_hash,
                    review_status, review_reason, display_order, updated_at
                ) VALUES (?, ?, ?, ?, 'pending_review', ?,
                          COALESCE((SELECT MAX(display_order)+1 FROM eventposter WHERE event_id=?), 0),
                          CURRENT_TIMESTAMP)
                """,
                (
                    event_id,
                    None if _managed(url) else url,
                    url if _managed(url) else None,
                    digest,
                    f"{AUDIT_TAG}:unreviewed_export_tail",
                    event_id,
                ),
            )
            stats["materialized_rows"] += 1

        desired_approved_ids = {
            int(change["position_row_id"][position])
            for position in range(len(change["gallery"]))
            if position not in change["loser_positions"]
            and position not in change["unavailable_positions"]
            and change["position_row_id"].get(position)
        }
        approved_ids: list[int] = []
        terminal_ids: set[int] = set()
        for position, gallery_item in enumerate(change["gallery"]):
            row_id = change["position_row_id"].get(position)
            if position in change["unavailable_positions"]:
                if row_id and int(row_id) not in desired_approved_ids:
                    con.execute(
                        "UPDATE eventposter SET review_status='unavailable', duplicate_of_id=NULL, review_reason=?, reviewed_at=CURRENT_TIMESTAMP WHERE id=?",
                        (f"{AUDIT_TAG}:download_unavailable", row_id),
                    )
                    stats["unavailable_rows"] += 1
                    terminal_ids.add(int(row_id))
                continue
            if position in change["loser_positions"]:
                survivor_position = change["survivor_by_loser"][position]
                survivor_id = change["position_row_id"].get(survivor_position)
                if row_id and survivor_id and row_id != survivor_id:
                    con.execute(
                        "UPDATE eventposter SET review_status='duplicate', duplicate_of_id=?, review_reason=?, reviewed_at=CURRENT_TIMESTAMP WHERE id=?",
                        (survivor_id, f"{AUDIT_TAG}:visual_duplicate", row_id),
                    )
                    stats["duplicate_rows"] += 1
                    terminal_ids.add(int(row_id))
                continue
            if row_id and row_id not in approved_ids:
                approved_ids.append(row_id)
                con.execute(
                    "UPDATE eventposter SET review_status='approved', duplicate_of_id=NULL, review_reason=?, reviewed_at=CURRENT_TIMESTAMP, display_order=? WHERE id=?",
                    (f"{AUDIT_TAG}:approved", position, row_id),
                )

        # Rows outside the visual ledger (including the >12 tail) stay durable
        # but cannot leak through the canonical projection while awaiting VLM.
        for row in con.execute(
            "SELECT id, review_status, catbox_url, supabase_url FROM eventposter WHERE event_id=? ORDER BY id",
            (event_id,),
        ).fetchall():
            row_id = int(row["id"])
            if row_id in approved_ids or row_id in terminal_ids:
                continue
            if not str(row["catbox_url"] or "").strip() and not str(row["supabase_url"] or "").strip():
                con.execute(
                    "UPDATE eventposter SET review_status='unavailable', duplicate_of_id=NULL, review_reason=?, reviewed_at=CURRENT_TIMESTAMP WHERE id=?",
                    (f"{AUDIT_TAG}:missing_url", row_id),
                )
                terminal_ids.add(row_id)
                stats["unavailable_rows"] += 1
                continue
            if str(row["review_status"] or "") != "pending_review":
                con.execute(
                    "UPDATE eventposter SET review_status='pending_review', duplicate_of_id=NULL, review_reason=?, reviewed_at=NULL WHERE id=?",
                    (f"{AUDIT_TAG}:unreviewed_db_only", row_id),
                )
                stats["pending_review_rows"] += 1

        approved_rows = []
        if approved_ids:
            placeholders = ",".join("?" for _ in approved_ids)
            approved_rows = con.execute(
                f"SELECT * FROM eventposter WHERE id IN ({placeholders}) ORDER BY display_order ASC, id ASC",
                approved_ids,
            ).fetchall()
        projection: list[str] = []
        for row in approved_rows:
            url = _display_url(row)
            if url and url not in projection:
                projection.append(url)
        stats["projection_refs_after"] += len(projection)
        con.execute(
            "UPDATE event SET photo_urls=?, photo_count=?, preview_3d_url=NULL WHERE id=?",
            (json.dumps(projection, ensure_ascii=False), len(projection), event_id),
        )
        stats["changed_event_ids"].append(event_id)
        projection_changed = (
            change["before_projection"] != projection
            or change["before_photo_count"] != len(projection)
        )
        if projection_changed:
            stats["public_projection_changed_events"] += 1

        if con.execute(
            "SELECT 1 FROM eventposter WHERE event_id=? AND review_status='pending_review' LIMIT 1",
            (event_id,),
        ).fetchone():
            _insert_job(
                con,
                event_id=event_id,
                task="event_media_review",
                next_run_at=(now + timedelta(seconds=len(stats["changed_event_ids"]) * 2)).isoformat(),
                coalesce_key=f"event_media_review:{event_id}",
            )
        # Cleanup repairs existing public projections; it must never turn a DB
        # cleanup into first-time publication.  Enqueue only when the approved
        # projection actually changed and the surface already exists.
        if projection_changed and change["has_telegraph_publication"]:
            ordinal = stats["public_rebuild_jobs"]["telegraph_build"]
            _insert_job(
                con,
                event_id=event_id,
                task="telegraph_build",
                next_run_at=(now + timedelta(seconds=ordinal * 4)).isoformat(),
                coalesce_key=f"telegraph_build:{event_id}",
            )
            stats["public_rebuild_jobs"]["telegraph_build"] += 1
        if projection_changed and change["has_managed_vk_publication"]:
            ordinal = stats["public_rebuild_jobs"]["vk_sync"]
            _insert_job(
                con,
                event_id=event_id,
                task="vk_sync",
                next_run_at=(now + timedelta(seconds=ordinal * 15)).isoformat(),
                coalesce_key=f"vk_sync:{event_id}",
            )
            stats["public_rebuild_jobs"]["vk_sync"] += 1
        if projection_changed and change["has_tg_publication"]:
            ordinal = stats["public_rebuild_jobs"]["tg_event_publish"]
            _insert_job(
                con,
                event_id=event_id,
                task="tg_event_publish",
                next_run_at=(now + timedelta(seconds=ordinal * 90)).isoformat(),
                coalesce_key=f"tg_event_publish:{event_id}",
            )
            stats["public_rebuild_jobs"]["tg_event_publish"] += 1

    if stats["public_projection_changed_events"]:
        owner = int(stats["changed_event_ids"][0])
        _insert_job(
            con,
            event_id=owner,
            task="static_site_build",
            next_run_at=(now + timedelta(minutes=20)).isoformat(),
            coalesce_key="static_site_build:prod",
        )
        stats["public_rebuild_jobs"]["static_site_build"] = 1
    stats["unchanged_events"] = stats["planned_events"] - stats["stale_skipped_events"] - stats["missing_events"] - len(changes)
    return stats


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--audit-dir", required=True)
    parser.add_argument("--plan-output")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--result-output")
    args = parser.parse_args()

    audit_dir = Path(args.audit_dir).resolve()
    plan = build_plan(audit_dir)
    plan["_audit_dir"] = str(audit_dir)
    if args.plan_output:
        safe_plan = {key: value for key, value in plan.items() if not key.startswith("_")}
        Path(args.plan_output).write_text(json.dumps(safe_plan, ensure_ascii=False, indent=2), encoding="utf-8")

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA foreign_keys=ON")
        con.execute("BEGIN IMMEDIATE" if args.apply else "BEGIN")
        result = apply_plan(con, plan, apply=bool(args.apply))
        if args.apply:
            con.commit()
        else:
            con.rollback()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()

    result["mode"] = "apply" if args.apply else "dry-run"
    result["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    output = json.dumps(result, ensure_ascii=False, indent=2)
    print(output)
    if args.result_output:
        Path(args.result_output).write_text(output + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
