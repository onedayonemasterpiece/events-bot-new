#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import re
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db import Database
from smart_event_update import EventCandidate, smart_event_update


ARTIFACTS_ROOT = PROJECT_ROOT / "artifacts" / "codex"
DEFAULT_EVENT_IDS = "4594,4598,4538"
DEFAULT_MODEL = "gemma-4-31b-it"


def _load_stage_module() -> Any:
    path = Path(__file__).with_name("benchmark_smart_update_g4_stages.py")
    spec = importlib.util.spec_from_file_location("benchmark_smart_update_g4_stages", path)
    if not spec or not spec.loader:
        raise RuntimeError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


stage = _load_stage_module()


FIELD_KEYS = [
    "title",
    "event_type",
    "date",
    "time",
    "end_date",
    "end_date_is_inferred",
    "location_name",
    "location_address",
    "city",
    "ticket_price_min",
    "ticket_price_max",
    "ticket_link",
    "ticket_status",
    "is_free",
    "pushkin_card",
    "lifecycle_status",
]


def _db_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _event_ids(value: str) -> list[int]:
    return [int(item.strip()) for item in str(value or "").split(",") if item.strip()]


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any]:
    if row is None:
        return {}
    return {key: row[key] for key in row.keys()}


def _event_fields(row: dict[str, Any]) -> dict[str, Any]:
    return {key: row.get(key) for key in FIELD_KEYS}


def _fetch_event(con: sqlite3.Connection, event_id: int) -> dict[str, Any]:
    row = con.execute("SELECT * FROM event WHERE id = ?", (event_id,)).fetchone()
    if row is None:
        raise RuntimeError(f"Event {event_id} not found")
    return _row_dict(row)


def _fetch_sources(con: sqlite3.Connection, event_id: int) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT *
        FROM event_source
        WHERE event_id = ?
        ORDER BY id
        """,
        (event_id,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = _row_dict(row)
        if _clean_text(item.get("source_text")):
            out.append(item)
    return out


def _fetch_facts(con: sqlite3.Connection, event_id: int) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT esf.id, esf.source_id, es.source_type, es.source_url, esf.status, esf.fact
        FROM event_source_fact esf
        LEFT JOIN event_source es ON es.id = esf.source_id
        WHERE esf.event_id = ?
        ORDER BY esf.id
        """,
        (event_id,),
    ).fetchall()
    return [_row_dict(row) for row in rows]


def _active_fact_texts(facts: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in facts:
        status = str(item.get("status") or "").strip().casefold()
        if status in {"conflict", "drop", "skipped"}:
            continue
        fact = _clean_text(item.get("fact"))
        if not fact:
            continue
        key = fact.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(fact)
    return out


def _fixture_from_event(event: dict[str, Any], sources: list[dict[str, Any]], facts: list[dict[str, Any]], *, db_path: Path) -> Any:
    source_packets = [
        stage.bench.SourcePacket(
            source_id=f"{row.get('source_type') or 'source'}-{row.get('id')}",
            source_type=str(row.get("source_type") or "source"),
            url=str(row.get("source_url") or "").strip(),
            text=_clean_text(row.get("source_text")),
        )
        for row in sources
        if _clean_text(row.get("source_text"))
    ]
    fixture = stage.StageBenchmarkFixture(
        fixture_id=f"PRODDB-{event.get('id')}",
        title=_clean_text(event.get("title")),
        event_type=_clean_text(event.get("event_type")),
        date=_clean_text(event.get("date")) or None,
        time=_clean_text(event.get("time")) or None,
        end_date=_clean_text(event.get("end_date")) or None,
        end_date_is_inferred=bool(event.get("end_date_is_inferred") or False),
        location_name=_clean_text(event.get("location_name")) or None,
        location_address=_clean_text(event.get("location_address")) or None,
        city=_clean_text(event.get("city")) or None,
        ticket_price_min=event.get("ticket_price_min"),
        ticket_price_max=event.get("ticket_price_max"),
        ticket_link=_clean_text(event.get("ticket_link")) or None,
        ticket_status=_clean_text(event.get("ticket_status")) or None,
        is_free=stage._as_bool(event.get("is_free")),
        pushkin_card=stage._as_bool(event.get("pushkin_card")),
        lifecycle_status=_clean_text(event.get("lifecycle_status")) or None,
        telegraph_url=_clean_text(event.get("telegraph_url")) or None,
        source_post_url=_clean_text(event.get("source_post_url")) or None,
        sources=source_packets,
        baseline_description_md=str(event.get("description") or "").strip(),
        baseline_short_description=str(event.get("short_description") or "").strip(),
        baseline_search_digest=str(event.get("search_digest") or "").strip(),
        baseline_raw_facts=_active_fact_texts(facts),
        baseline_source_artifact=str(db_path),
    )
    fixture.baseline_facts_text_clean = stage._clean_db_facts(fixture.baseline_raw_facts, fixture)
    return fixture


def _snapshot_from_db(con: sqlite3.Connection, event_id: int, *, db_path: Path) -> dict[str, Any]:
    event = _fetch_event(con, event_id)
    sources = _fetch_sources(con, event_id)
    facts = _fetch_facts(con, event_id)
    fixture = _fixture_from_event(event, sources, facts, db_path=db_path)
    active_facts = _active_fact_texts(facts)
    facts_text_clean = stage._clean_db_facts(active_facts, fixture)
    description = str(event.get("description") or "").strip()
    data = {
        "event_id": event_id,
        "fields": _event_fields(event),
        "sources": sources,
        "source_count": len(sources),
        "raw_facts": active_facts,
        "facts_by_status": facts,
        "facts_text_clean": facts_text_clean,
        "description_md": description,
        "short_description": str(event.get("short_description") or "").strip(),
        "search_digest": str(event.get("search_digest") or "").strip(),
        "metrics": stage._variant_metrics(description),
        "quality_profile": stage.writer_final_family._describe_text_quality(description),
        "summary_infoblock": stage._summary_infoblock_lines(fixture),
    }
    data["telegraph_preview_text"] = stage._telegraph_preview_text(fixture, data)
    return data


def _candidate_from_source(event: dict[str, Any], source: dict[str, Any]) -> EventCandidate:
    return EventCandidate(
        source_type=str(source.get("source_type") or "source"),
        source_url=str(source.get("source_url") or "").strip() or None,
        source_text=str(source.get("source_text") or ""),
        title=str(event.get("title") or "").strip() or None,
        date=str(event.get("date") or "").strip() or None,
        time=str(event.get("time") or "").strip() or None,
        time_is_default=bool(event.get("time_is_default") or False),
        end_date=str(event.get("end_date") or "").strip() or None,
        end_date_is_inferred=bool(event.get("end_date_is_inferred") or False),
        festival=str(event.get("festival") or "").strip() or None,
        location_name=str(event.get("location_name") or "").strip() or None,
        location_address=str(event.get("location_address") or "").strip() or None,
        city=str(event.get("city") or "").strip() or None,
        ticket_link=str(event.get("ticket_link") or "").strip() or None,
        ticket_price_min=event.get("ticket_price_min"),
        ticket_price_max=event.get("ticket_price_max"),
        ticket_status=str(event.get("ticket_status") or "").strip() or None,
        event_type=str(event.get("event_type") or "").strip() or None,
        emoji=str(event.get("emoji") or "").strip() or None,
        is_free=stage._as_bool(event.get("is_free")),
        pushkin_card=stage._as_bool(event.get("pushkin_card")),
        search_digest=str(event.get("search_digest") or "").strip() or None,
        raw_excerpt=str(event.get("short_description") or "").strip() or None,
        source_chat_username=str(source.get("source_chat_username") or "").strip() or None,
        source_chat_id=source.get("source_chat_id"),
        source_message_id=source.get("source_message_id"),
        trust_level=str(source.get("trust_level") or "").strip() or None,
        metrics={"benchmark_baseline_event_id": event.get("id")},
    )


def _prepare_sandbox(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        path = Path(str(dst) + suffix)
        if path.exists():
            path.unlink()
    shutil.copy2(src, dst)
    con = sqlite3.connect(dst)
    try:
        con.execute("PRAGMA foreign_keys=OFF")
        for table in ("event_source_fact", "event_source", "event_media_asset", "eventposter", "event"):
            con.execute(f"DELETE FROM {table}")
        con.commit()
    finally:
        con.close()


async def _run_event(db: Database, baseline_event: dict[str, Any], sources: list[dict[str, Any]]) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    candidate_event_id: int | None = None
    started = time.perf_counter()
    for source in sources:
        candidate = _candidate_from_source(baseline_event, source)
        result = await smart_event_update(
            db,
            candidate,
            check_source_url=False,
            schedule_tasks=False,
        )
        results.append(
            {
                "source_url": candidate.source_url,
                "source_type": candidate.source_type,
                "status": result.status,
                "event_id": result.event_id,
                "created": result.created,
                "merged": result.merged,
                "added_sources": result.added_sources,
                "added_facts": list(result.added_facts or []),
                "skipped_conflicts": list(result.skipped_conflicts or []),
                "reason": result.reason,
            }
        )
        if result.event_id:
            candidate_event_id = int(result.event_id)
    return {
        "candidate_event_id": candidate_event_id,
        "smart_update_results": results,
        "wall_clock_sec": round(time.perf_counter() - started, 6),
    }


def _field_diff(baseline: dict[str, Any], candidate: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    b_fields = baseline.get("fields") or {}
    c_fields = candidate.get("fields") or {}
    for key in FIELD_KEYS:
        before = b_fields.get(key)
        after = c_fields.get(key)
        if before != after:
            out.append({"field": key, "baseline": before, "candidate": after})
    return out


async def _run(args: argparse.Namespace) -> tuple[Path, Path]:
    stage._load_env_file()
    stage._set_smart_update_model(args.model)
    os.environ["EVENT_TOPICS_MODEL"] = args.model
    src_db = _db_path(args.prod_db)
    event_ids = _event_ids(args.event_ids)
    generated_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    sandbox_db = ARTIFACTS_ROOT / f"smart_update_g4_full_surface_sandbox_{generated_at}.sqlite"
    json_path = ARTIFACTS_ROOT / f"smart_update_g4_full_surface_benchmark_{generated_at}.json"
    md_path = json_path.with_suffix(".md")

    baseline_con = sqlite3.connect(src_db)
    baseline_con.row_factory = sqlite3.Row
    baselines: dict[int, dict[str, Any]] = {}
    source_sets: dict[int, list[dict[str, Any]]] = {}
    events: dict[int, dict[str, Any]] = {}
    try:
        for event_id in event_ids:
            events[event_id] = _fetch_event(baseline_con, event_id)
            source_sets[event_id] = _fetch_sources(baseline_con, event_id)
            if not source_sets[event_id]:
                raise RuntimeError(f"Event {event_id} has no source rows with source_text")
            baselines[event_id] = _snapshot_from_db(baseline_con, event_id, db_path=src_db)
    finally:
        baseline_con.close()

    _prepare_sandbox(src_db, sandbox_db)
    db = Database(str(sandbox_db))
    results: list[dict[str, Any]] = []
    try:
        for event_id in event_ids:
            run_info = await _run_event(db, events[event_id], source_sets[event_id])
            candidate_event_id = run_info.get("candidate_event_id")
            if not candidate_event_id:
                candidate_snapshot: dict[str, Any] = {"error": "candidate_event_not_created"}
                coverage = {"summary": {"verdict": "review_error"}, "errors": ["candidate_event_not_created"]}
            else:
                con = sqlite3.connect(sandbox_db)
                con.row_factory = sqlite3.Row
                try:
                    candidate_snapshot = _snapshot_from_db(con, int(candidate_event_id), db_path=sandbox_db)
                finally:
                    con.close()
                baseline_fixture = _fixture_from_event(
                    events[event_id],
                    source_sets[event_id],
                    baselines[event_id].get("facts_by_status") or [],
                    db_path=src_db,
                )
                candidate_snapshot["model"] = args.model
                candidate_snapshot["path"] = "smart_update_g4_sandbox_create_merge_persisted"
                coverage = await stage._run_fact_coverage(
                    baseline_fixture,
                    baselines[event_id],
                    candidate_snapshot,
                    model=args.model,
                )
            result = {
                "fixture": {
                    "fixture_id": f"PRODDB-{event_id}",
                    "title": events[event_id].get("title"),
                    "baseline_event_id": event_id,
                    "candidate_event_id": candidate_event_id,
                },
                "baseline": baselines[event_id],
                "candidate": candidate_snapshot,
                "smart_update_run": run_info,
                "field_diff": _field_diff(baselines[event_id], candidate_snapshot),
                "fact_coverage": coverage,
            }
            results.append(result)

            data = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "prod_db": str(src_db),
                "sandbox_db": str(sandbox_db),
                "model": args.model,
                "event_ids": event_ids,
                "results": results,
                "checkpoint": {
                    "completed": len(results),
                    "total": len(event_ids),
                    "complete": len(results) == len(event_ids),
                },
                "limitations": [
                    "event_source rows do not store the original upstream EventCandidate; candidate anchor fields are seeded from the baseline event row",
                    "this benchmark validates Smart Update G4 create/merge/persisted surfaces on copied source rows, not Telegram/parser upstream extraction parity",
                ],
            }
            json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            md_path.write_text(_render_report(data, json_path), encoding="utf-8")
    finally:
        try:
            await db.close()
        except Exception:
            pass

    return json_path, md_path


def _render_fact_list(lines: list[str], title: str, facts: list[str]) -> None:
    lines.extend(["", f"#### {title}", ""])
    if not facts:
        lines.append("_empty_")
        return
    for fact in facts:
        lines.append(f"- {fact}")


def _render_report(data: dict[str, Any], json_path: Path) -> str:
    lines = [
        "# Smart Update G4 Full-Surface Sandbox Benchmark",
        "",
        f"- generated_at: `{data.get('generated_at')}`",
        f"- artifact_json: `{json_path}`",
        f"- prod_db: `{data.get('prod_db')}`",
        f"- sandbox_db: `{data.get('sandbox_db')}`",
        f"- model: `{data.get('model')}`",
        f"- checkpoint: `{data.get('checkpoint')}`",
        "",
        "## Scope",
        "",
    ]
    for item in data.get("limitations") or []:
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## Summary",
            "",
            "| Fixture | Candidate ID | Fields | Facts | Text chars | Verdict | Smart Update statuses |",
            "| --- | ---: | --- | --- | --- | --- | --- |",
        ]
    )
    for result in data.get("results") or []:
        fixture = result.get("fixture") or {}
        baseline = result.get("baseline") or {}
        candidate = result.get("candidate") or {}
        coverage_summary = ((result.get("fact_coverage") or {}).get("summary") or {})
        statuses = ", ".join(
            str(item.get("status") or "?")
            for item in ((result.get("smart_update_run") or {}).get("smart_update_results") or [])
        )
        lines.append(
            "| "
            + f"`{fixture.get('fixture_id')}` {stage._clip(fixture.get('title'), 56)} | "
            + f"`{fixture.get('candidate_event_id')}` | "
            + f"`{len(result.get('field_diff') or [])} diff` | "
            + f"`{stage._coverage_cell(coverage_summary)}` | "
            + f"`{(baseline.get('metrics') or {}).get('chars')} -> {(candidate.get('metrics') or {}).get('chars')}` | "
            + f"`{coverage_summary.get('verdict')}` | "
            + f"`{statuses}` |"
        )
    for result in data.get("results") or []:
        fixture = result.get("fixture") or {}
        baseline = result.get("baseline") or {}
        candidate = result.get("candidate") or {}
        coverage_summary = ((result.get("fact_coverage") or {}).get("summary") or {})
        lines.extend(["", "", f"## {fixture.get('fixture_id')}: {fixture.get('title')}", ""])
        lines.append(f"- baseline_event_id: `{fixture.get('baseline_event_id')}`")
        lines.append(f"- candidate_event_id: `{fixture.get('candidate_event_id')}`")
        lines.append(f"- fact verdict: `{coverage_summary.get('verdict')}`")
        lines.append(f"- grounded covered: `{stage._coverage_cell(coverage_summary)}`")
        lines.append(f"- smart_update wall: `{(result.get('smart_update_run') or {}).get('wall_clock_sec')}`")
        lines.extend(["", "### Field Diff", ""])
        diffs = result.get("field_diff") or []
        if not diffs:
            lines.append("_no field diffs_")
        else:
            lines.extend(["| Field | Baseline | Candidate |", "| --- | --- | --- |"])
            for diff in diffs:
                lines.append(
                    f"| `{diff.get('field')}` | `{stage._clip(diff.get('baseline'), 120)}` | `{stage._clip(diff.get('candidate'), 120)}` |"
                )
        lines.extend(["", "### Infoblock / Logistics", ""])
        lines.extend(["#### Baseline infoblock", ""])
        for item in baseline.get("summary_infoblock") or []:
            lines.append(f"- {item}")
        lines.extend(["", "#### Candidate infoblock", ""])
        for item in candidate.get("summary_infoblock") or []:
            lines.append(f"- {item}")
        _render_fact_list(lines, "Baseline raw facts", list(baseline.get("raw_facts") or []))
        _render_fact_list(lines, "Candidate raw facts", list(candidate.get("raw_facts") or []))
        _render_fact_list(lines, "Baseline facts_text_clean", list(baseline.get("facts_text_clean") or []))
        _render_fact_list(lines, "Candidate facts_text_clean", list(candidate.get("facts_text_clean") or []))
        logistics = list(coverage_summary.get("g4_logistics_fact_texts") or [])
        _render_fact_list(lines, "Candidate logistics / infoblock facts", logistics)
        lost = list(coverage_summary.get("lost_baseline_facts") or [])
        lines.extend(["", "### Lost Baseline Facts", ""])
        if not lost:
            lines.append("_none or reviewer unavailable_")
        else:
            for item in lost:
                lines.append(
                    f"- `{item.get('loss_severity')}` {item.get('baseline_fact')} — {item.get('reason')}"
                )
        lines.extend(["", "### Baseline Telegraph Body", ""])
        lines.extend(stage._fenced(baseline.get("description_md"), "md"))
        lines.extend(["", "### Candidate Telegraph Body", ""])
        lines.extend(stage._fenced(candidate.get("description_md"), "md"))
        lines.extend(["", "### Derived Fields", ""])
        lines.append(f"- baseline short_description: `{baseline.get('short_description') or ''}`")
        lines.append(f"- candidate short_description: `{candidate.get('short_description') or ''}`")
        lines.append(f"- baseline search_digest: `{baseline.get('search_digest') or ''}`")
        lines.append(f"- candidate search_digest: `{candidate.get('search_digest') or ''}`")
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prod-db", required=True)
    parser.add_argument("--event-ids", default=DEFAULT_EVENT_IDS)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    args = parser.parse_args()
    json_path, md_path = asyncio.run(_run(args))
    print(json_path)
    print(md_path)


if __name__ == "__main__":
    main()
