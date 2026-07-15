#!/usr/bin/env python3
"""Plan or apply a bounded, resumable event age-rating backfill.

Default mode is read-only.  Regex is used only to retrieve candidate evidence;
it never assigns a semantic rating.  Writes require both ``--apply`` and a
reviewed decision-plan whose input hashes still match current data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from event_age_rating import (  # noqa: E402
    DEFAULT_DECISION_VERSION,
    DEFAULT_RUBRIC_VERSION,
    age_input_hash,
    decision_from_semantic_payload,
    normalize_age_restriction,
)


AGE_CANDIDATE_RE = re.compile(r"(?<!\d)(0|6|12|16|18)\s*\+(?!\d)")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"pragma table_info({table})").fetchall()}


def evidence_spans(text: str | None, *, limit: int = 8) -> list[dict[str, Any]]:
    raw = str(text or "")
    out: list[dict[str, Any]] = []
    for match in AGE_CANDIDATE_RE.finditer(raw):
        lo = max(0, match.start() - 90)
        hi = min(len(raw), match.end() + 90)
        span = re.sub(r"\s+", " ", raw[lo:hi]).strip()
        out.append({"value": f"{match.group(1)}+", "span": span[:240], "offset": match.start()})
        if len(out) >= limit:
            break
    return out


def load_event_evidence(con: sqlite3.Connection, event_id: int, event: sqlite3.Row) -> dict[str, Any]:
    sources = []
    if "event_source" in {row[0] for row in con.execute("select name from sqlite_master where type='table'")}:
        sources = [dict(row) for row in con.execute(
            "select id, source_type, source_url, source_text from event_source where event_id=? order by id",
            (event_id,),
        ).fetchall()]
    posters = []
    if "eventposter" in {row[0] for row in con.execute("select name from sqlite_master where type='table'")}:
        posters = [dict(row) for row in con.execute(
            "select id, poster_hash, ocr_text, ocr_title from eventposter where event_id=? order by id",
            (event_id,),
        ).fetchall()]
    source_texts = [str(row.get("source_text") or "") for row in sources]
    poster_texts = [
        str(value or "")
        for row in posters
        for value in (row.get("ocr_text"), row.get("ocr_title"))
        if str(value or "").strip()
    ]
    description = str(event["description"] or "") if "description" in event.keys() else ""
    primary_source = sources[0] if sources else {}
    input_hash = age_input_hash(
        source_type=str(primary_source.get("source_type") or "legacy"),
        source_url=str(primary_source.get("source_url") or "") or None,
        source_text="\n\n".join(source_texts) or str(event["source_text"] or ""),
        raw_excerpt=description,
        poster_ocr=poster_texts,
    )
    return {
        "sources": sources,
        "posters": posters,
        "source_texts": source_texts,
        "poster_texts": poster_texts,
        "description": description,
        "input_hash": input_hash,
    }


def classify_for_audit(event: sqlite3.Row, evidence: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    canonical = normalize_age_restriction(event["age_restriction"] if "age_restriction" in event.keys() else None)
    candidates: list[dict[str, Any]] = []
    for source in evidence["sources"]:
        for span in evidence_spans(source.get("source_text")):
            candidates.append({**span, "kind": "source_text", "source_url": source.get("source_url")})
    for poster in evidence["posters"]:
        for field in ("ocr_text", "ocr_title"):
            for span in evidence_spans(poster.get(field)):
                candidates.append({**span, "kind": "poster_ocr", "poster_hash": poster.get("poster_hash")})
    description_spans = evidence_spans(evidence["description"])
    candidates.extend({**span, "kind": "description"} for span in description_spans)
    values = {item["value"] for item in candidates}
    if canonical:
        return ("source_declared_consistent" if not values or values == {canonical} else "source_declared_conflict"), candidates
    if len(values) > 1:
        return "source_declared_conflict", candidates
    if any(item["kind"] == "source_text" for item in candidates):
        return "source_declared_consistent", candidates
    if any(item["kind"] == "poster_ocr" for item in candidates):
        return "poster_only", candidates
    if candidates:
        return "description_only", candidates
    content_chars = sum(len(str(x or "")) for x in evidence["source_texts"] + [evidence["description"]])
    return ("source_missing_assessable" if content_chars >= 320 else "source_missing_insufficient"), candidates


def catalog_hash(rows: Iterable[dict[str, Any]]) -> str:
    payload = [(int(row["event_id"]), str(row["input_hash"])) for row in rows]
    return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def anonymize_candidate(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "value": item.get("value"),
        "kind": item.get("kind"),
        "span_hash": hashlib.sha256(str(item.get("span") or "").encode()).hexdigest()[:20],
        "document_hash": hashlib.sha256(
            str(item.get("source_url") or item.get("poster_hash") or "").encode()
        ).hexdigest()[:20],
    }


def build_plan(
    con: sqlite3.Connection,
    *,
    after_id: int,
    batch_size: int,
    include_evidence: bool = False,
) -> dict[str, Any]:
    event_columns = table_columns(con, "event")
    rows = con.execute(
        "select * from event where id>? order by id limit ?", (after_id, batch_size)
    ).fetchall()
    plan_rows: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for event in rows:
        event_id = int(event["id"])
        evidence = load_event_evidence(con, event_id, event)
        group, candidates = classify_for_audit(event, evidence)
        counts[group] += 1
        plan_rows.append(
            {
                "event_id": event_id,
                "title_hash": hashlib.sha256(str(event["title"] or "").encode()).hexdigest()[:16],
                "input_hash": evidence["input_hash"],
                "group": group,
                "candidate_evidence": (
                    candidates[:12]
                    if include_evidence
                    else [anonymize_candidate(candidate) for candidate in candidates[:12]]
                ),
                "current": {
                    "age_restriction": event["age_restriction"] if "age_restriction" in event_columns else None,
                    "status": event["age_restriction_status"] if "age_restriction_status" in event_columns else "schema_missing",
                },
                "proposed_decision": None,
            }
        )
    return {
        "schema_version": "event-age-backfill-plan-v1",
        "created_at": utc_now_iso(),
        "rubric_version": DEFAULT_RUBRIC_VERSION,
        "decision_version": DEFAULT_DECISION_VERSION,
        "after_id": after_id,
        "next_after_id": int(rows[-1]["id"]) if rows else after_id,
        "batch_size": batch_size,
        "evidence_mode": "raw_operator_review" if include_evidence else "anonymized",
        "counts": dict(counts),
        "rows": plan_rows,
        "catalog_hash": catalog_hash(plan_rows),
    }


def apply_reviewed_plan(con: sqlite3.Connection, plan: dict[str, Any]) -> dict[str, Any]:
    if plan.get("schema_version") != "event-age-backfill-plan-v1":
        raise ValueError("unsupported plan schema")
    if plan.get("catalog_hash") != catalog_hash(plan.get("rows") or []):
        raise ValueError("plan catalog hash mismatch")
    counts: Counter[str] = Counter()
    now = utc_now_iso()
    for item in plan.get("rows") or []:
        payload = item.get("proposed_decision")
        if not isinstance(payload, dict):
            counts["skipped_no_reviewed_decision"] += 1
            continue
        event_id = int(item["event_id"])
        event = con.execute("select * from event where id=?", (event_id,)).fetchone()
        if event is None:
            counts["skipped_missing_event"] += 1
            continue
        evidence = load_event_evidence(con, event_id, event)
        if evidence["input_hash"] != item.get("input_hash"):
            counts["skipped_stale_input_hash"] += 1
            continue
        corpora = evidence["source_texts"] + evidence["poster_texts"] + [evidence["description"]]
        decision = decision_from_semantic_payload(
            payload,
            source_url=None,
            source_corpora=corpora,
            input_hash=evidence["input_hash"],
            decision_version=str(plan.get("decision_version") or DEFAULT_DECISION_VERSION),
            rubric_version=str(plan.get("rubric_version") or DEFAULT_RUBRIC_VERSION),
        )
        if decision is None:
            counts["skipped_invalid_decision"] += 1
            continue
        if decision.status == "declared":
            if (
                normalize_age_restriction(event["age_restriction"]) == decision.value
                and event["age_restriction_status"] == "declared"
                and event["age_restriction_provenance"] == decision.provenance
                and event["age_restriction_input_hash"] == decision.input_hash
            ):
                counts["unchanged_declared"] += 1
                continue
            con.execute(
                """update event set age_restriction=?, age_restriction_status='declared',
                   age_restriction_provenance=?, age_restriction_confidence=?,
                   age_restriction_evidence=?, age_restriction_decision_version=?,
                   age_restriction_input_hash=?, age_restriction_updated_at=? where id=?""",
                (decision.value, decision.provenance, decision.confidence, json.dumps(decision.evidence, ensure_ascii=False), decision.decision_version, decision.input_hash, now, event_id),
            )
        elif decision.status == "assessed":
            if (
                normalize_age_restriction(event["age_assessment"]) == decision.value
                and event["age_assessment_provenance"] == decision.provenance
                and event["age_assessment_input_hash"] == decision.input_hash
            ):
                counts["unchanged_assessed"] += 1
                continue
            con.execute(
                """update event set age_assessment=?, age_assessment_provenance=?,
                   age_assessment_confidence=?, age_assessment_evidence=?,
                   age_assessment_decision_version=?, age_assessment_input_hash=?,
                   age_assessment_engine=?, age_restriction_status=case when age_restriction is null then 'assessed' else age_restriction_status end,
                   age_restriction_updated_at=? where id=?""",
                (decision.value, decision.provenance, decision.confidence, json.dumps(decision.evidence, ensure_ascii=False), decision.decision_version, decision.input_hash, decision.assessment_engine, now, event_id),
            )
        elif decision.status == "conflict" and event["age_restriction_provenance"] != "manual_override":
            if (
                event["age_restriction"] is None
                and event["age_restriction_status"] == "conflict"
                and event["age_restriction_input_hash"] == decision.input_hash
            ):
                counts["unchanged_conflict"] += 1
                continue
            con.execute(
                """update event set age_restriction=null, age_restriction_status='conflict',
                   age_restriction_provenance=null, age_restriction_confidence=null,
                   age_restriction_evidence=?, age_restriction_decision_version=?,
                   age_restriction_input_hash=?, age_restriction_updated_at=? where id=?""",
                (
                    json.dumps(decision.evidence, ensure_ascii=False),
                    decision.decision_version,
                    decision.input_hash,
                    now,
                    event_id,
                ),
            )
        else:
            if (
                event["age_restriction"] is None
                and event["age_restriction_status"] == decision.status
                and event["age_restriction_input_hash"] == decision.input_hash
            ):
                counts[f"unchanged_{decision.status}"] += 1
                continue
            con.execute(
                "update event set age_restriction_status=?, age_restriction_decision_version=?, age_restriction_input_hash=?, age_restriction_updated_at=? where id=? and age_restriction is null",
                (decision.status, decision.decision_version, decision.input_hash, now, event_id),
            )
        counts[f"applied_{decision.status}"] += 1
    con.commit()
    return {"status": "applied", "counts": dict(counts), "applied_at": now}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--after-id", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--decision-plan", type=Path)
    parser.add_argument("--max-llm-calls", type=int, default=0, help="Recorded cap; this script never calls an LLM.")
    parser.add_argument(
        "--include-evidence",
        action="store_true",
        help="Include raw public/source spans and URLs for an operator-reviewed plan; default is anonymized.",
    )
    args = parser.parse_args()
    if args.batch_size < 1 or args.batch_size > 5000:
        parser.error("--batch-size must be between 1 and 5000")
    con = sqlite3.connect(f"file:{args.db}?mode={'rw' if args.apply else 'ro'}", uri=True)
    con.row_factory = sqlite3.Row
    try:
        if args.apply:
            if not args.decision_plan:
                parser.error("--apply requires --decision-plan")
            plan = json.loads(args.decision_plan.read_text(encoding="utf-8"))
            report = apply_reviewed_plan(con, plan)
            atomic_write_json(args.output, report)
        else:
            after_id = args.after_id
            if args.checkpoint and args.checkpoint.exists():
                checkpoint = json.loads(args.checkpoint.read_text(encoding="utf-8"))
                after_id = max(after_id, int(checkpoint.get("next_after_id") or 0))
            plan = build_plan(
                con,
                after_id=after_id,
                batch_size=args.batch_size,
                include_evidence=bool(args.include_evidence),
            )
            plan["max_llm_calls"] = max(0, int(args.max_llm_calls))
            plan["llm_calls_used"] = 0
            atomic_write_json(args.output, plan)
            if args.checkpoint:
                atomic_write_json(args.checkpoint, {k: plan[k] for k in ("schema_version", "catalog_hash", "next_after_id", "rubric_version", "decision_version")})
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
