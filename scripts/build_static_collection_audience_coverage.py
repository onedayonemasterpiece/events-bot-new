#!/usr/bin/env python3
"""Build a provider-free current/future audience facts-v3 coverage receipt."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import shlex
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database
from models import Event, EventSource
from scripts.backfill_static_collection_facts import (
    build_candidate,
    evaluation_receipt_covers,
    event_is_current,
    route_backfill_reasons,
    select_sources,
)
from smart_event_update import collection_adjudication_input_hash

SCHEMA_VERSION = "static-collection-audience-coverage-v1"


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _hash_ids(values: Sequence[int]) -> str:
    return hashlib.sha256(_canonical_bytes(list(values))).hexdigest()


def _repo_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "unknown"


def _read_ids(path: Path | None) -> set[int]:
    if path is None:
        return set()
    decoded = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(decoded, Mapping):
        decoded = decoded.get("event_ids")
    if not isinstance(decoded, list):
        raise ValueError("deferred event file must be a JSON list or {event_ids:[...]}")
    values = {int(value) for value in decoded}
    if any(value <= 0 for value in values):
        raise ValueError("deferred event IDs must be positive")
    return values


async def build_coverage(
    db: Database,
    *,
    current_date: date,
    explicit_deferred_event_ids: set[int] | None = None,
    generator_command: str,
) -> dict[str, Any]:
    explicit_deferred_event_ids = set(explicit_deferred_event_ids or ())
    async with db.get_session() as session:
        events = list((await session.execute(select(Event).order_by(Event.id))).scalars())
        source_rows = list(
            (await session.execute(select(EventSource).order_by(EventSource.event_id, EventSource.id))).scalars()
        )
    sources_by_event: dict[int, list[EventSource]] = {}
    for source in source_rows:
        sources_by_event.setdefault(int(source.event_id), []).append(source)

    candidates: list[int] = []
    evaluated: list[int] = []
    deferred: list[int] = []
    unprocessed: list[int] = []
    selected_bindings: list[dict[str, int]] = []
    deferred_reasons: dict[str, str] = {}
    for event in events:
        if not event.id or not event_is_current(event, current_date=current_date):
            continue
        reasons = route_backfill_reasons(event, enabled_reasons={"audience"})
        if "audience" not in reasons:
            continue
        event_id = int(event.id)
        candidates.append(event_id)
        selected = select_sources(sources_by_event.get(event_id, ()), maximum=1)
        if not selected:
            deferred.append(event_id)
            deferred_reasons[str(event_id)] = "no_usable_source"
            continue
        source = selected[0]
        source_id = int(source.id or 0)
        selected_bindings.append({"event_id": event_id, "source_id": source_id})
        candidate = build_candidate(event, source, reasons)
        input_hash = collection_adjudication_input_hash(candidate)
        if evaluation_receipt_covers(
            event,
            reasons=reasons,
            input_hash=input_hash,
            source_id=source_id,
        ):
            evaluated.append(event_id)
        elif event_id in explicit_deferred_event_ids:
            deferred.append(event_id)
            deferred_reasons[str(event_id)] = "explicit_provider_or_validation_deferred"
        else:
            unprocessed.append(event_id)

    candidate_set = set(candidates)
    invalid_deferred = sorted(explicit_deferred_event_ids - candidate_set)
    if invalid_deferred:
        raise ValueError(f"deferred IDs are outside the current audience candidate universe: {invalid_deferred}")
    candidates.sort()
    evaluated.sort()
    deferred.sort()
    unprocessed.sort()
    selected_bindings.sort(key=lambda row: (row["event_id"], row["source_id"]))
    status = "complete" if not unprocessed else "partial"
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "current_date": current_date.isoformat(),
        "repo_sha": _repo_sha(),
        "generator_command": generator_command,
        "provider_calls": 0,
        "publication_status": "blocked",
        "candidate_event_count": len(candidates),
        "evaluated_event_count": len(evaluated),
        "deferred_event_count": len(deferred),
        "unprocessed_event_count": len(unprocessed),
        "candidate_event_ids": candidates,
        "evaluated_event_ids": evaluated,
        "deferred_event_ids": deferred,
        "unprocessed_event_ids": unprocessed,
        "candidate_event_ids_sha256": _hash_ids(candidates),
        "evaluated_event_ids_sha256": _hash_ids(evaluated),
        "deferred_event_ids_sha256": _hash_ids(deferred),
        "unprocessed_event_ids_sha256": _hash_ids(unprocessed),
        "selected_source_bindings": selected_bindings,
        "selected_source_bindings_sha256": hashlib.sha256(
            _canonical_bytes(selected_bindings)
        ).hexdigest(),
        "deferred_reasons": deferred_reasons,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--current-date", type=date.fromisoformat, required=True)
    parser.add_argument("--deferred-event-id-file", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.db.is_file():
        raise SystemExit(f"SQLite DB does not exist: {args.db}")
    command = shlex.join([sys.executable, str(Path(__file__).relative_to(ROOT)), *(argv or sys.argv[1:])])
    deferred = _read_ids(args.deferred_event_id_file)

    async def run() -> dict[str, Any]:
        db = Database(str(args.db.resolve()))
        try:
            return await build_coverage(
                db,
                current_date=args.current_date,
                explicit_deferred_event_ids=deferred,
                generator_command=command,
            )
        finally:
            await db.close()

    result = asyncio.run(run())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
