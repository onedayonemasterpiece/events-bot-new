#!/usr/bin/env python3
"""Build the source-bound facts-v3 product-monitor snapshot.

This is a projection adapter at the existing static exporter boundary.  It
does not classify text, call a provider, or grant publication eligibility.
Only direct ``confirmed`` facts-v3 decisions are projected; malformed claimed
facts stay visible with blocking review/source states so the product monitor
can fail closed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "static-collection-product-snapshot-v1"
FACTS_POLICY_VERSION = "static-collection-facts-v3"
FACTS_SCHEMA_VERSION = "static-collection-adjudication-v2"
LABEL_FACT_KEYS = {
    "child_directed": ("child_directed_decision",),
    "family_suitable": ("family_suitable_decision",),
    "joint_family_activity": ("joint_family_activity_decision",),
    "kids": ("child_directed_decision", "family_suitable_decision"),
}
LABEL_MODES = {
    "child_directed": "shadow",
    "family_suitable": "shadow",
    "joint_family_activity": "shadow",
    "kids": "experimental",
}
EVIDENCE_TRUST_SCOPES = {"all", "trusted"}
TRUSTED_LEVELS = {"official", "high"}


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def stable_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(decoded) if isinstance(decoded, Mapping) else {}
    return {}


def _list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return []
        return list(decoded) if isinstance(decoded, list) else []
    return []


def _date(value: Any) -> str | None:
    raw = str(value or "").strip()[:10]
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        return None


def _text(value: Any) -> str | None:
    clean = str(value or "").strip()
    return clean or None


def _event_id(event: Mapping[str, Any]) -> int:
    return int(event.get("id", event.get("event_id")) or 0)


def normalize_mutual_occurrence_families(
    events: Sequence[Mapping[str, Any]],
) -> dict[int, tuple[int, ...]]:
    """Mirror the frontend mutual-link union contract; infer nothing else."""

    by_id = {_event_id(event): event for event in events if _event_id(event) > 0}
    parent = {event_id: event_id for event_id in by_id}

    def find(value: int) -> int:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    links = {
        event_id: {
            int(value)
            for value in _list(event.get("other_date_ids", event.get("linked_event_ids")))
            if str(value).isdigit() and int(value) != event_id
        }
        for event_id, event in by_id.items()
    }
    for event_id, linked_ids in links.items():
        for linked_id in linked_ids:
            if linked_id in by_id and event_id in links.get(linked_id, set()):
                union(event_id, linked_id)
    members: dict[int, list[int]] = defaultdict(list)
    for event_id in sorted(by_id):
        members[find(event_id)].append(event_id)
    return {
        event_id: tuple(members[find(event_id)])
        for event_id in by_id
    }


def _family_id(event_id: int, members: Sequence[int]) -> str:
    normalized = sorted({int(value) for value in members})
    return f"linked:{normalized[0]}" if len(normalized) > 1 else f"event:{event_id}"


def _source_allowed(source: Mapping[str, Any], evidence_trust_scope: str) -> bool:
    if evidence_trust_scope == "all":
        return True
    return str(source.get("trust_level") or "").strip().casefold() in TRUSTED_LEVELS


def _fact_projection(
    *,
    event_id: int,
    fact_key: str,
    raw_fact: Any,
    sources: Mapping[int, Mapping[str, Any]],
    evidence_trust_scope: str,
) -> dict[str, Any] | None:
    if not isinstance(raw_fact, Mapping) or raw_fact.get("value") != "confirmed":
        return None
    fact = dict(raw_fact)
    failures: list[str] = []
    if fact.get("policy_version") != FACTS_POLICY_VERSION:
        failures.append("facts_v3_policy_missing")
    source_id = 0
    try:
        source_id = int(fact.get("source_id") or 0)
    except (TypeError, ValueError):
        failures.append("source_id_invalid")
    source = sources.get(source_id)
    if source is None:
        failures.append("source_missing_or_wrong_event")
        source = {}
    elif int(source.get("event_id") or 0) != event_id:
        failures.append("source_event_mismatch")
    if source and not _source_allowed(source, evidence_trust_scope):
        failures.append("source_outside_scope")
    quote = str(fact.get("evidence_quote") or "").strip()
    source_text = str(source.get("source_text") or "")
    if not quote:
        failures.append("evidence_quote_missing")
    elif quote not in source_text:
        failures.append("evidence_quote_not_exact")
    input_hash = str(fact.get("input_hash") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", input_hash):
        failures.append("input_hash_invalid")
    source_status = "grounded" if not failures else "blocked"
    review_status = "accepted" if not failures else "needs_source_review"
    return {
        "fact_key": fact_key,
        "value": "confirmed",
        "source_status": source_status,
        "review_status": review_status,
        "failure_codes": sorted(set(failures)),
        "fact_provenance": {
            "policy_version": fact.get("policy_version"),
            "schema_version": FACTS_SCHEMA_VERSION,
            "input_hash": input_hash or None,
            "decided_at": fact.get("decided_at"),
            "reason_code": fact.get("reason_code"),
            "confidence": fact.get("confidence"),
            "evidence_quote": quote or None,
            "evidence_quote_sha256": hashlib.sha256(quote.encode("utf-8")).hexdigest()
            if quote
            else None,
        },
        "source_provenance": {
            "source_id": source_id or None,
            "event_id": source.get("event_id"),
            "source_type": source.get("source_type"),
            "source_url": source.get("source_url"),
            "trust_level": source.get("trust_level"),
            "source_text_sha256": hashlib.sha256(source_text.encode("utf-8")).hexdigest()
            if source_text
            else None,
            "evidence_quote_exact": bool(quote and quote in source_text),
        },
    }


def _event_projection(
    event: Mapping[str, Any],
    *,
    family_members: Sequence[int],
    facts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    event_id = _event_id(event)
    blocked = any(fact.get("source_status") != "grounded" for fact in facts)
    organizers = event.get("organizer_names")
    if not isinstance(organizers, list):
        organizers = _list(organizers)
    organizer = next((str(value).strip() for value in organizers if str(value).strip()), None)
    return {
        "event_id": event_id,
        "title": _text(event.get("title")) or f"Событие {event_id}",
        "family_id": _family_id(event_id, family_members),
        "start_date": _date(event.get("start_date", event.get("date"))),
        "end_date": _date(event.get("end_date"))
        or _date(event.get("start_date", event.get("date"))),
        "venue": _text(event.get("venue_name", event.get("location_name"))),
        "organizer": organizer,
        "event_type": _text(event.get("event_type")),
        "source_status": "blocked" if blocked else "grounded",
        "review_status": "needs_source_review" if blocked else "accepted",
        "fact_provenance": [dict(fact["fact_provenance"]) for fact in facts],
        "source_provenance": [dict(fact["source_provenance"]) for fact in facts],
        "member_provenance": {
            "family_member_event_ids": list(sorted(int(value) for value in family_members)),
            "representative_event_id": event_id,
            "representative_rule": "earliest_start_date_then_event_id_among_direct_fact_members",
            "fact_transfer_across_siblings": False,
            "direct_fact_event_id": event_id,
        },
    }


def normalized_visible_output(snapshot: Mapping[str, Any]) -> dict[str, list[tuple[str, int]]]:
    """Match the PR #234 runner's collection -> ordered family/event view."""

    normalized: dict[str, list[tuple[str, int]]] = {}
    collections = snapshot.get("collections")
    if not isinstance(collections, Mapping):
        return normalized
    for label, raw_collection in sorted(collections.items()):
        rows = raw_collection.get("items") if isinstance(raw_collection, Mapping) else []
        values: list[tuple[str, int]] = []
        for raw in rows if isinstance(rows, list) else []:
            if not isinstance(raw, Mapping):
                continue
            try:
                event_id = int(raw.get("event_id", raw.get("id")) or 0)
            except (TypeError, ValueError, OverflowError):
                continue
            if event_id <= 0:
                continue
            family_id = str(raw.get("family_id") or "").strip() or f"event:{event_id}"
            values.append((family_id, event_id))
        normalized[str(label)] = values
    return normalized


def build_product_snapshot(
    events: Sequence[Mapping[str, Any]],
    *,
    collection_decisions_by_id: Mapping[int, Any],
    source_records_by_event: Mapping[int, Sequence[Mapping[str, Any]]],
    current_date: str,
    generated_at: str,
    source_scope: str = "standalone-db",
    evidence_trust_scope: str = "all",
) -> dict[str, Any]:
    """Build one deterministic product projection from already-extracted facts."""

    source_scope = str(source_scope or "").strip()
    if not source_scope:
        raise ValueError("source_scope must identify the snapshot/stage provenance")
    if evidence_trust_scope not in EVIDENCE_TRUST_SCOPES:
        raise ValueError(f"unsupported evidence trust scope: {evidence_trust_scope}")
    current = date.fromisoformat(current_date).isoformat()
    eligible = []
    for raw in events:
        event = dict(raw)
        event_id = _event_id(event)
        start = _date(event.get("start_date", event.get("date")))
        end = _date(event.get("end_date")) or start
        lifecycle = str(event.get("lifecycle_status") or "active").casefold()
        if event_id > 0 and start and end and end >= current and lifecycle == "active":
            eligible.append(event)
    eligible.sort(key=lambda row: (_date(row.get("start_date", row.get("date"))) or "", _event_id(row)))
    families = normalize_mutual_occurrence_families(eligible)
    by_id = {_event_id(event): event for event in eligible}
    sources_by_event: dict[int, dict[int, dict[str, Any]]] = {}
    input_sources: list[dict[str, Any]] = []
    for event_id in sorted(by_id):
        indexed: dict[int, dict[str, Any]] = {}
        for raw_source in source_records_by_event.get(event_id, ()):
            source = dict(raw_source)
            try:
                source_id = int(source.get("id") or source.get("source_id") or 0)
            except (TypeError, ValueError):
                continue
            if source_id <= 0:
                continue
            source["id"] = source_id
            source["event_id"] = int(source.get("event_id") or event_id)
            indexed[source_id] = source
            source_text = str(source.get("source_text") or "")
            input_sources.append(
                {
                    "source_id": source_id,
                    "event_id": source["event_id"],
                    "source_type": source.get("source_type"),
                    "source_url": source.get("source_url"),
                    "trust_level": source.get("trust_level"),
                    "source_text_sha256": hashlib.sha256(source_text.encode("utf-8")).hexdigest(),
                }
            )
        sources_by_event[event_id] = indexed

    rows_by_label: dict[str, list[dict[str, Any]]] = {label: [] for label in LABEL_FACT_KEYS}
    input_facts: dict[str, Any] = {}
    for event_id in sorted(by_id):
        event = by_id[event_id]
        decisions = _mapping(collection_decisions_by_id.get(event_id))
        input_facts[str(event_id)] = {
            key: decisions.get(key) for key in sorted({key for keys in LABEL_FACT_KEYS.values() for key in keys})
        }
        for label, fact_keys in LABEL_FACT_KEYS.items():
            facts = [
                projected
                for fact_key in fact_keys
                if (
                    projected := _fact_projection(
                        event_id=event_id,
                        fact_key=fact_key,
                        raw_fact=decisions.get(fact_key),
                        sources=sources_by_event[event_id],
                        evidence_trust_scope=evidence_trust_scope,
                    )
                )
                is not None
            ]
            if not facts:
                continue
            rows_by_label[label].append(
                _event_projection(event, family_members=families[event_id], facts=facts)
            )

    collections: dict[str, Any] = {}
    for label in LABEL_FACT_KEYS:
        candidates = sorted(
            rows_by_label[label],
            key=lambda row: (str(row.get("start_date") or ""), int(row["event_id"])),
        )
        representatives: list[dict[str, Any]] = []
        seen_families: set[str] = set()
        for row in candidates:
            family_id = str(row["family_id"])
            if family_id in seen_families:
                continue
            seen_families.add(family_id)
            representatives.append(row)
        collections[label] = {
            "mode": LABEL_MODES[label],
            "state": "blocked"
            if any(row["review_status"] != "accepted" for row in representatives)
            else "ready",
            "publication_status": "blocked",
            "source_grounding_required": True,
            "using_last_good": False,
            "items": representatives,
        }

    input_projection = {
        "schema_version": SCHEMA_VERSION,
        "facts_policy_version": FACTS_POLICY_VERSION,
        "current_date": current,
        "source_scope": source_scope,
        "evidence_trust_scope": evidence_trust_scope,
        "events": [
            {
                "event_id": event_id,
                "title": by_id[event_id].get("title"),
                "start_date": _date(by_id[event_id].get("start_date", by_id[event_id].get("date"))),
                "end_date": _date(by_id[event_id].get("end_date")),
                "venue": by_id[event_id].get("venue_name", by_id[event_id].get("location_name")),
                "organizer_names": _list(by_id[event_id].get("organizer_names")),
                "event_type": by_id[event_id].get("event_type"),
                "family_members": list(families[event_id]),
            }
            for event_id in sorted(by_id)
        ],
        "facts": input_facts,
        "sources": sorted(input_sources, key=lambda row: (row["event_id"], row["source_id"])),
    }
    snapshot: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "facts_policy_version": FACTS_POLICY_VERSION,
        "generated_at": generated_at,
        "current_date": current,
        "source_scope": source_scope,
        "evidence_trust_scope": evidence_trust_scope,
        "input_fingerprint": stable_hash(input_projection),
        "provider_calls": 0,
        "publication": {
            "status": "blocked",
            "allowed_modes": ["shadow", "experimental"],
            "reason": "semantic_publication_requires_pr_b_owner_gold_and_calibration",
        },
        "collections": collections,
    }
    snapshot["normalized_output_sha256"] = stable_hash(normalized_visible_output(snapshot))
    snapshot["snapshot_sha256"] = stable_hash(
        {key: value for key, value in snapshot.items() if key != "snapshot_sha256"}
    )
    return snapshot


def validate_product_snapshot(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if snapshot.get("schema_version") != SCHEMA_VERSION:
        errors.append("schema_version_mismatch")
    if snapshot.get("facts_policy_version") != FACTS_POLICY_VERSION:
        errors.append("facts_policy_version_mismatch")
    if not str(snapshot.get("source_scope") or "").strip():
        errors.append("source_scope_missing")
    if snapshot.get("evidence_trust_scope") not in EVIDENCE_TRUST_SCOPES:
        errors.append("evidence_trust_scope_invalid")
    if snapshot.get("provider_calls") != 0:
        errors.append("provider_calls_must_be_zero")
    publication = snapshot.get("publication")
    if not isinstance(publication, Mapping) or publication.get("status") != "blocked":
        errors.append("publication_must_be_blocked")
    collections = snapshot.get("collections")
    if not isinstance(collections, Mapping) or set(collections) != set(LABEL_FACT_KEYS):
        errors.append("collection_labels_mismatch")
        collections = {}
    current = _date(snapshot.get("current_date"))
    for label, raw_collection in collections.items():
        if not isinstance(raw_collection, Mapping):
            errors.append(f"{label}:collection_invalid")
            continue
        if raw_collection.get("mode") not in {"shadow", "experimental"}:
            errors.append(f"{label}:public_mode_forbidden")
        if raw_collection.get("publication_status") != "blocked":
            errors.append(f"{label}:publication_not_blocked")
        if raw_collection.get("source_grounding_required") is not True:
            errors.append(f"{label}:source_grounding_not_required")
        seen: set[str] = set()
        for row in raw_collection.get("items") or []:
            if not isinstance(row, Mapping):
                errors.append(f"{label}:item_invalid")
                continue
            family_id = str(row.get("family_id") or "")
            if not family_id or family_id in seen:
                errors.append(f"{label}:family_duplicate_or_missing")
            seen.add(family_id)
            if current and (_date(row.get("end_date")) or "") < current:
                errors.append(f"{label}:expired_event")
            member = row.get("member_provenance")
            if not isinstance(member, Mapping) or member.get("fact_transfer_across_siblings") is not False:
                errors.append(f"{label}:member_provenance_invalid")
    declared_normalized = str(snapshot.get("normalized_output_sha256") or "")
    if declared_normalized != stable_hash(normalized_visible_output(snapshot)):
        errors.append("normalized_output_sha256_mismatch")
    declared_snapshot = str(snapshot.get("snapshot_sha256") or "")
    if declared_snapshot != stable_hash(
        {key: value for key, value in snapshot.items() if key != "snapshot_sha256"}
    ):
        errors.append("snapshot_sha256_mismatch")
    if not re.fullmatch(r"[0-9a-f]{64}", str(snapshot.get("input_fingerprint") or "")):
        errors.append("input_fingerprint_invalid")
    return {"valid": not errors, "errors": errors}


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def load_snapshot_inputs(
    con: sqlite3.Connection,
    *,
    current_date: str,
) -> tuple[list[dict[str, Any]], dict[int, Any], dict[int, list[dict[str, Any]]]]:
    con.row_factory = sqlite3.Row
    rows = con.execute("select * from event order by id asc").fetchall()
    events: list[dict[str, Any]] = []
    decisions: dict[int, Any] = {}
    for raw in rows:
        row = _row_dict(raw)
        event_id = int(row.get("id") or 0)
        start = _date(row.get("date"))
        end = _date(row.get("end_date")) or start
        if (
            event_id <= 0
            or not start
            or not end
            or end < current_date
            or str(row.get("identity_status") or "canonical") != "canonical"
            or row.get("merged_into_event_id") not in {None, "", 0, "0"}
            or bool(row.get("silent") or False)
            or str(row.get("lifecycle_status") or "active") != "active"
        ):
            continue
        events.append(
            {
                "id": event_id,
                "title": row.get("title"),
                "start_date": start,
                "end_date": _date(row.get("end_date")),
                "start_time": row.get("time"),
                "venue_name": row.get("location_name"),
                "organizer_names": _list(row.get("organizer_names")),
                "event_type": row.get("event_type"),
                "lifecycle_status": row.get("lifecycle_status") or "active",
                "other_date_ids": _list(row.get("linked_event_ids")),
            }
        )
        decisions[event_id] = row.get("collection_decisions")
    event_ids = {int(event["id"]) for event in events}
    return events, decisions, load_source_records(con, event_ids=event_ids)


def load_source_records(
    con: sqlite3.Connection,
    *,
    event_ids: Sequence[int] | set[int],
) -> dict[int, list[dict[str, Any]]]:
    """Load only evidence fields needed by the product projection."""

    normalized_ids = sorted({int(value) for value in event_ids if int(value) > 0})
    sources: dict[int, list[dict[str, Any]]] = defaultdict(list)
    if not normalized_ids:
        return {}
    con.row_factory = sqlite3.Row
    placeholders = ",".join("?" for _ in normalized_ids)
    source_rows = con.execute(
        f"select id,event_id,source_type,source_url,source_text,trust_level "
        f"from event_source where event_id in ({placeholders}) order by event_id,id",
        tuple(normalized_ids),
    ).fetchall()
    for row in source_rows:
        value = _row_dict(row)
        sources[int(value["event_id"])].append(value)
    return dict(sources)


def write_product_snapshot(path: Path, snapshot: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--current-date", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--source-scope",
        default="standalone-db",
        help="Arbitrary provenance label for the exact DB snapshot/stage.",
    )
    parser.add_argument(
        "--evidence-trust-scope",
        choices=sorted(EVIDENCE_TRUST_SCOPES),
        default="all",
        help="Optional evidence trust filter; separate from stage provenance.",
    )
    args = parser.parse_args()
    uri = f"file:{Path(args.db).resolve().as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as con:
        events, decisions, sources = load_snapshot_inputs(con, current_date=args.current_date)
    snapshot = build_product_snapshot(
        events,
        collection_decisions_by_id=decisions,
        source_records_by_event=sources,
        current_date=args.current_date,
        generated_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        source_scope=args.source_scope,
        evidence_trust_scope=args.evidence_trust_scope,
    )
    validation = validate_product_snapshot(snapshot)
    if not validation["valid"]:
        raise SystemExit("product snapshot validation failed: " + "; ".join(validation["errors"]))
    write_product_snapshot(args.output, snapshot)
    print(json.dumps({
        "output": str(args.output),
        "input_fingerprint": snapshot["input_fingerprint"],
        "normalized_output_sha256": snapshot["normalized_output_sha256"],
        "snapshot_sha256": snapshot["snapshot_sha256"],
        "provider_calls": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
