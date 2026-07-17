#!/usr/bin/env python3
"""Plan, run and import a paced EventPoster image-geometry backfill.

The run phase is intentionally external to Fly: it reads a production snapshot,
downloads public image URLs from the local/Kaggle runtime and writes resumable
JSONL.  The import phase performs only small, fingerprint-guarded SQLite writes.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import random
import sys
import time
from typing import Any

from sqlalchemy import func, or_, select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database
from event_media import (
    IMAGE_GEOMETRY_PROMPT_VERSION,
    analyze_event_poster_geometry,
    image_geometry_model,
    resolve_poster_display_url,
)
from models import Event, EventImageGeometry, EventPoster


@dataclass(frozen=True, slots=True)
class Candidate:
    poster_id: int
    event_id: int
    poster_hash: str
    url: str


def _load_env(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path).expanduser()
    if not env_path.exists():
        raise FileNotFoundError(env_path)
    from dotenv import load_dotenv

    load_dotenv(env_path, override=False)


def _append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{line_no}: expected object")
        out.append(value)
    return out


def _validated_normalized_geometry(row: dict[str, Any]) -> dict[str, Any] | None:
    def box(value: Any) -> list[float] | None:
        if not isinstance(value, (list, tuple)) or len(value) != 4:
            return None
        try:
            ymin, xmin, ymax, xmax = [float(item) for item in value]
        except Exception:
            return None
        if not all(0.0 <= item <= 1.0 for item in (ymin, xmin, ymax, xmax)):
            return None
        if ymax <= ymin or xmax <= xmin:
            return None
        return [round(ymin, 6), round(xmin, 6), round(ymax, 6), round(xmax, 6)]

    raw_faces = row.get("face_boxes_yxyx")
    if not isinstance(raw_faces, list) or len(raw_faces) > 50:
        return None
    faces: list[list[float]] = []
    for raw_face in raw_faces:
        normalized = box(raw_face)
        if normalized is None:
            return None
        faces.append(normalized)
    valuable = box(row.get("valuable_region_yxyx"))
    if valuable is None:
        return None
    try:
        confidence = float(row.get("valuable_region_confidence"))
    except Exception:
        return None
    if not 0.0 <= confidence <= 1.0:
        return None
    return {
        "face_boxes_yxyx": faces,
        "valuable_region_yxyx": valuable,
        "valuable_region_confidence": round(confidence, 6),
        "reason_code": str(row.get("reason_code") or "viewer_value_region")[:120],
    }


async def _candidates(db: Database, *, from_date: str, limit: int | None) -> list[Candidate]:
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(EventPoster, Event)
                .join(Event, Event.id == EventPoster.event_id)
                .where(
                    func.coalesce(func.nullif(func.trim(Event.lifecycle_status), ""), "active")
                    == "active",
                    func.coalesce(Event.silent, False).is_(False),
                    or_(Event.date >= from_date, Event.end_date >= from_date),
                    EventPoster.review_status.in_(("approved", "pending_review")),
                    EventPoster.duplicate_of_id.is_(None),
                )
                .order_by(Event.date.asc(), Event.id.asc(), EventPoster.display_order.asc())
            )
        ).all()
    out: list[Candidate] = []
    for poster, event in rows:
        url = resolve_poster_display_url(poster)
        if not poster.id or not event.id or not url:
            continue
        out.append(
            Candidate(
                poster_id=int(poster.id),
                event_id=int(event.id),
                poster_hash=str(poster.poster_hash),
                url=url,
            )
        )
        if limit is not None and limit > 0 and len(out) >= int(limit):
            break
    return out


async def _result_record(db: Database, candidate: Candidate, outcome) -> dict[str, Any]:
    record: dict[str, Any] = {
        **asdict(candidate),
        "status": outcome.status,
        "provider_called": bool(outcome.provider_called),
        "cache_hit": bool(outcome.cache_hit),
        "pixel_sha256": outcome.pixel_sha256,
        "geometry_id_local": outcome.geometry_id,
        "model": image_geometry_model(),
        "prompt_version": IMAGE_GEOMETRY_PROMPT_VERSION,
        "error": outcome.error,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    if outcome.geometry_id:
        async with db.get_session() as session:
            geometry = await session.get(EventImageGeometry, int(outcome.geometry_id))
            if geometry is not None:
                record.update(
                    {
                        "source_width": geometry.source_width,
                        "source_height": geometry.source_height,
                        "face_boxes_yxyx": geometry.face_boxes_yxyx_json,
                        "valuable_region_yxyx": geometry.valuable_region_yxyx_json,
                        "valuable_region_confidence": geometry.valuable_region_confidence,
                        "reason_code": geometry.reason_code,
                        "prompt_tokens": geometry.prompt_tokens,
                        "completion_tokens": geometry.completion_tokens,
                        "total_tokens": geometry.total_tokens,
                    }
                )
    return record


async def _run(args: argparse.Namespace) -> int:
    db = Database(str(args.db))
    await db.init()
    os.environ["EVENT_IMAGE_GEOMETRY_DAILY_CALLS"] = str(max(1, args.daily_cap))
    previous = _read_jsonl(Path(args.output))
    completed = {
        (int(row.get("poster_id") or 0), str(row.get("poster_hash") or ""), row.get("model"), row.get("prompt_version"))
        for row in previous
        if row.get("status") == "classified"
        or (
            row.get("status") == "error"
            and str(row.get("error") or "").startswith("image_download_failed:")
        )
    }
    # Select the next N unfinished rows rather than truncating before the resume
    # filter. Otherwise a restarted --limit 20 run would keep seeing the same
    # already-completed first page forever.
    all_candidates = await _candidates(db, from_date=args.from_date, limit=None)
    candidates = [
        candidate
        for candidate in all_candidates
        if (
            candidate.poster_id,
            candidate.poster_hash,
            image_geometry_model(),
            IMAGE_GEOMETRY_PROMPT_VERSION,
        )
        not in completed
    ][: max(1, int(args.limit))]
    print(
        json.dumps(
            {
                "mode": "run",
                "candidates": len(candidates),
                "already_completed": len(completed),
                "model": image_geometry_model(),
                "prompt_version": IMAGE_GEOMETRY_PROMPT_VERSION,
                "min_delay_seconds": args.min_delay,
                "chunk_size": args.chunk_size,
            },
            ensure_ascii=False,
        )
    )
    processed = provider_calls = ok = failed = 0
    for candidate in candidates:
        key = (
            candidate.poster_id,
            candidate.poster_hash,
            image_geometry_model(),
            IMAGE_GEOMETRY_PROMPT_VERSION,
        )
        if key in completed:
            continue
        outcome = await analyze_event_poster_geometry(
            candidate.event_id, candidate.poster_id, db
        )
        record = await _result_record(db, candidate, outcome)
        _append_jsonl(Path(args.output), record)
        processed += 1
        provider_calls += int(outcome.provider_called)
        ok += int(outcome.status == "classified")
        failed += int(outcome.status not in {"classified", "pending"})
        print(
            f"image={processed}/{len(candidates)} poster_id={candidate.poster_id} "
            f"status={outcome.status} provider={int(outcome.provider_called)} cache={int(outcome.cache_hit)}",
            flush=True,
        )
        if outcome.provider_called:
            delay = max(5.0, float(args.min_delay)) + random.uniform(0, max(0.0, args.jitter))
            time.sleep(delay)
            if args.chunk_size > 0 and provider_calls % args.chunk_size == 0:
                time.sleep(max(0.0, float(args.chunk_pause)))
        if outcome.error and ("429" in outcome.error or "RateLimit" in outcome.error):
            print("rate-limit signal: checkpoint saved; stopping safely", file=sys.stderr)
            break
    await db.engine.dispose()
    print(
        json.dumps(
            {
                "processed": processed,
                "provider_calls": provider_calls,
                "classified": ok,
                "failed": failed,
                "output": str(args.output),
            },
            ensure_ascii=False,
        )
    )
    return 0 if failed == 0 else 2


async def _plan(args: argparse.Namespace) -> int:
    db = Database(str(args.db))
    await db.init()
    candidates = await _candidates(db, from_date=args.from_date, limit=args.limit)
    print(
        json.dumps(
            {
                "mode": "plan",
                "db": str(args.db),
                "from_date": args.from_date,
                "count": len(candidates),
                "sample": [asdict(item) for item in candidates[:10]],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    await db.engine.dispose()
    return 0


async def _import(args: argparse.Namespace) -> int:
    rows = [row for row in _read_jsonl(Path(args.input)) if row.get("status") == "classified"]
    db = Database(str(args.db))
    await db.init()
    valid: list[tuple[dict[str, Any], EventPoster]] = []
    stale = 0
    dry_run = not args.apply
    async with db.get_session() as session:
        for row in rows:
            poster = await session.get(EventPoster, int(row.get("poster_id") or 0))
            normalized = _validated_normalized_geometry(row)
            if (
                poster is None
                or int(poster.event_id) != int(row.get("event_id") or 0)
                or str(poster.poster_hash) != str(row.get("poster_hash") or "")
                or not str(row.get("pixel_sha256") or "")
                or str(row.get("model") or "") != image_geometry_model()
                or str(row.get("prompt_version") or "") != IMAGE_GEOMETRY_PROMPT_VERSION
                or normalized is None
            ):
                stale += 1
                continue
            valid.append(({**row, **normalized}, poster))
        print(
            json.dumps(
                {
                    "mode": "import",
                    "input_successes": len(rows),
                    "valid": len(valid),
                    "stale": stale,
                    "apply": bool(args.apply),
                },
                ensure_ascii=False,
            )
        )
        if dry_run:
            await session.rollback()
        else:
            backup = [
                {
                    "poster_id": int(poster.id or 0),
                    "event_id": int(poster.event_id),
                    "poster_hash": poster.poster_hash,
                    "pixel_sha256": poster.pixel_sha256,
                    "image_geometry_id": poster.image_geometry_id,
                }
                for _row, poster in valid
            ]
            backup_path = Path(args.backup_out)
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            backup_path.write_text(json.dumps(backup, ensure_ascii=False, indent=2), encoding="utf-8")

            now = datetime.now(timezone.utc)
            for row, poster in valid:
                pixel = str(row["pixel_sha256"])
                geometry = (
                    await session.execute(
                        select(EventImageGeometry).where(
                            EventImageGeometry.pixel_sha256 == pixel,
                            EventImageGeometry.model == str(row["model"]),
                            EventImageGeometry.prompt_version == str(row["prompt_version"]),
                        )
                    )
                ).scalar_one_or_none()
                if geometry is None:
                    geometry = EventImageGeometry(
                        pixel_sha256=pixel,
                        model=str(row["model"]),
                        prompt_version=str(row["prompt_version"]),
                    )
                if geometry.status != "classified" or geometry.face_boxes_yxyx_json is None:
                    geometry.status = "classified"
                    geometry.source_width = int(row.get("source_width") or 0) or None
                    geometry.source_height = int(row.get("source_height") or 0) or None
                    geometry.face_boxes_yxyx_json = list(row.get("face_boxes_yxyx") or [])
                    geometry.valuable_region_yxyx_json = list(row.get("valuable_region_yxyx") or [])
                    geometry.valuable_region_confidence = float(row.get("valuable_region_confidence") or 0)
                    geometry.reason_code = str(row.get("reason_code") or "backfill_import")[:120]
                    geometry.prompt_tokens = int(row.get("prompt_tokens") or 0)
                    geometry.completion_tokens = int(row.get("completion_tokens") or 0)
                    geometry.total_tokens = int(row.get("total_tokens") or 0)
                    geometry.analyzed_at = now
                    geometry.updated_at = now
                    session.add(geometry)
                    await session.flush()
                poster.pixel_sha256 = pixel
                poster.width = poster.width or (int(row.get("source_width") or 0) or None)
                poster.height = poster.height or (int(row.get("source_height") or 0) or None)
                poster.image_geometry_id = int(geometry.id or 0)
                poster.updated_at = now
                session.add(poster)
            await session.commit()
    await db.engine.dispose()
    if dry_run:
        return 0
    print(f"applied={len(valid)} backup={args.backup_out}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("plan", "run", "import"), default="plan")
    parser.add_argument("--db", required=True)
    parser.add_argument("--from-date", default=date.today().isoformat())
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--env-file")
    parser.add_argument("--output", default="artifacts/codex/image-geometry-backfill/results.jsonl")
    parser.add_argument("--input")
    parser.add_argument("--daily-cap", type=int, default=400)
    parser.add_argument("--min-delay", type=float, default=6.0)
    parser.add_argument("--jitter", type=float, default=1.0)
    parser.add_argument("--chunk-size", type=int, default=100)
    parser.add_argument("--chunk-pause", type=float, default=75.0)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--backup-out",
        default="artifacts/codex/image-geometry-backfill/import-backup.json",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    _load_env(args.env_file)
    if args.mode == "run" and not args.output:
        raise SystemExit("--output is required")
    if args.mode == "import" and not args.input:
        raise SystemExit("--input is required")
    if args.mode == "plan":
        return asyncio.run(_plan(args))
    if args.mode == "run":
        return asyncio.run(_run(args))
    return asyncio.run(_import(args))


if __name__ == "__main__":
    raise SystemExit(main())
