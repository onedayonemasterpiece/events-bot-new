#!/usr/bin/env python3
"""Bounded real-data replay/backfill for Smart Update collection facts.

Safety is deliberately asymmetric: ``plan`` and ``evaluate`` work from a
temporary online-backup copy and never open the requested SQLite file through
the application's writable engine.  ``apply`` requires an explicit event-ID
allowlist and verifies the complete logical Event/EventSource diff before it
commits each source result.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import inspect
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import tempfile
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database
from models import Event, EventSource
import smart_event_update as collection_core
from smart_event_update import (
    STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
    STATIC_COLLECTION_FACTS_POLICY_VERSION,
    EventCandidate,
    adjudicate_collection_candidate,
    apply_collection_decisions,
    collection_adjudication_input_hash,
)

REASONS = frozenset({"admission", "audience", "people"})
TRUST_RANK = {"official": 4, "high": 3, "medium": 2, "low": 1}
REPORT_SCHEMA_VERSION = "static-collection-facts-v3-real-data-report-v1"
REPORT_SCHEMA_PATH = (
    ROOT / "docs/review-data/static_collection_facts_v3_real_data_report.schema.json"
)
MAX_EVENT_IDS = 100
MAX_SOURCES_PER_EVENT = 4
MAX_SOURCE_IDS = MAX_EVENT_IDS * MAX_SOURCES_PER_EVENT
AUDIENCE_FACT_KEYS = (
    "child_directed_decision",
    "family_suitable_decision",
    "joint_family_activity_decision",
)


@dataclass(frozen=True)
class PlannedEvent:
    event_id: int
    reasons: tuple[str, ...]
    source_ids: tuple[int, ...]
    unselected_source_ids: tuple[int, ...] = ()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _quick_check(path: Path) -> str:
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        rows = conn.execute("PRAGMA quick_check").fetchall()
    return "\n".join(str(row[0]) for row in rows)


@contextlib.contextmanager
def _read_only_working_copy(source: Path) -> Iterator[Path]:
    """Yield a consistent disposable copy without writing source DB/WAL."""

    with tempfile.TemporaryDirectory(prefix="facts-v3-readonly-") as directory:
        target = Path(directory) / "snapshot.sqlite"
        uri = f"file:{source.resolve().as_posix()}?mode=ro"
        with sqlite3.connect(uri, uri=True) as source_conn, sqlite3.connect(target) as target_conn:
            source_conn.backup(target_conn)
        yield target


def _repo_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return "unknown"


def _topics(event: Event) -> set[str]:
    raw = event.topics or []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            raw = []
    return {str(value or "").strip().upper() for value in raw if str(value or "").strip()}


def event_is_current(event: Event, *, current_date: date) -> bool:
    if str(event.identity_status or "canonical") != "canonical":
        return False
    if event.merged_into_event_id is not None:
        return False
    if str(event.lifecycle_status or "active") != "active" or bool(event.silent):
        return False
    effective_end = str(event.end_date or event.date or "")
    try:
        return date.fromisoformat(effective_end) >= current_date
    except ValueError:
        return False


def _audience_router_text(event: Event) -> str:
    values = [event.title, event.description, event.search_digest, event.source_text]
    return " ".join(str(value or "") for value in values).casefold().replace("ё", "е")


def route_backfill_reasons(
    event: Event,
    *,
    enabled_reasons: Iterable[str],
    forced: bool = False,
) -> tuple[str, ...]:
    """Return high-recall reasons; routing signals are never semantic proof."""

    enabled = REASONS & {str(value).strip().lower() for value in enabled_reasons}
    topics = _topics(event)
    decisions = event.collection_decisions if isinstance(event.collection_decisions, dict) else {}
    result: set[str] = set()
    if "admission" in enabled and (
        forced or bool(event.is_free) or "admission_decision" in decisions
    ):
        result.add("admission")
    audience_keys = {*AUDIENCE_FACT_KEYS, "audience_decision"}
    audience_text = _audience_router_text(event)
    broad_audience_signal = any(
        phrase in audience_text
        for phrase in (
            "для детей",
            "детский спектакль",
            "детская программа",
            "для всей семьи",
            "всей семьей",
            "детям и взрослым",
            "взрослым и детям",
            "родители и дети",
            "родителей с детьми",
            "семейная команда",
            "вместе с ребенком",
            "вместе с детьми",
        )
    )
    if "audience" in enabled and (
        forced
        or bool(topics & {"FAMILY", "KIDS_SCHOOL"})
        or bool(audience_keys & decisions.keys())
        or broad_audience_signal
    ):
        result.add("audience")
    if "people" in enabled and (
        forced or "PERSONALITIES" in topics or "people_appearances" in decisions
    ):
        result.add("people")
    return tuple(sorted(result))


def select_sources(sources: Iterable[EventSource], *, maximum: int) -> tuple[EventSource, ...]:
    usable = [
        source
        for source in sources
        if source.id and str(source.source_url or "").strip() and str(source.source_text or "").strip()
    ]

    def score(source: EventSource) -> tuple[int, datetime, int, int]:
        imported = source.imported_at or datetime.min.replace(tzinfo=timezone.utc)
        if imported.tzinfo is None:
            imported = imported.replace(tzinfo=timezone.utc)
        return (
            TRUST_RANK.get(str(source.trust_level or "").strip().lower(), 0),
            imported,
            len(str(source.source_text or "")),
            int(source.id or 0),
        )

    return tuple(sorted(usable, key=score, reverse=True)[:maximum])


def build_candidate(event: Event, source: EventSource, reasons: Iterable[str]) -> EventCandidate:
    return EventCandidate(
        source_type=str(source.source_type or "backfill"),
        source_url=str(source.source_url or ""),
        source_text=str(source.source_text or ""),
        title=event.title,
        date=event.date,
        time=event.time,
        end_date=event.end_date,
        festival=event.festival,
        location_name=event.location_name,
        location_address=event.location_address,
        city=event.city,
        ticket_link=event.ticket_link,
        ticket_price_min=event.ticket_price_min,
        ticket_price_max=event.ticket_price_max,
        ticket_status=getattr(event, "ticket_status", None),
        age_restriction=event.age_restriction,
        event_type=event.event_type,
        is_free=event.is_free,
        search_digest=event.search_digest,
        source_chat_username=source.source_chat_username,
        source_chat_id=source.source_chat_id,
        source_message_id=source.source_message_id,
        trust_level=source.trust_level,
        topics=sorted(_topics(event)),
        collection_adjudication_reasons=[*reasons, "backfill"],
    )


def evaluation_receipt_covers(
    event: Event,
    *,
    reasons: Iterable[str],
    input_hash: str,
    source_id: int,
) -> bool:
    """Use the v3 per-source receipt; retain a strict legacy fallback for tests."""

    decisions = event.collection_decisions if isinstance(event.collection_decisions, dict) else {}
    helper = getattr(collection_core, "collection_decision_hash_covers", None)
    if callable(helper):
        return bool(
            helper(
                decisions,
                reasons=reasons,
                input_hash=input_hash,
                source_id=source_id,
            )
        )
    helper = getattr(collection_core, "collection_evaluation_covers", None)
    if callable(helper):
        return bool(helper(decisions, input_hash=input_hash, source_id=source_id))

    # This path does not make v2 audience truth satisfy v3: all three v3 keys
    # must carry this exact source/hash. It exists only for stack compatibility.
    for reason in reasons:
        if reason == "admission":
            items = (decisions.get("admission_decision"),)
        elif reason == "audience":
            items = tuple(decisions.get(key) for key in AUDIENCE_FACT_KEYS)
        elif reason == "people":
            items = tuple(decisions.get("people_appearances") or ())
            if not items:
                return False
        else:
            continue
        if not items or any(
            not isinstance(item, Mapping)
            or str(item.get("input_hash") or "") != input_hash
            or int(item.get("source_id") or 0) != source_id
            for item in items
        ):
            return False
    return True


async def build_plan(
    db: Database,
    *,
    current_date: date,
    enabled_reasons: Iterable[str],
    event_ids: set[int],
    limit: int,
    max_sources_per_event: int,
    source_ids: set[int] | None = None,
) -> list[PlannedEvent]:
    source_ids = source_ids or set()
    async with db.get_session() as session:
        statement = select(Event).order_by(Event.id)
        if event_ids:
            statement = statement.where(Event.id.in_(event_ids))
        events = list((await session.execute(statement)).scalars())
        plan: list[PlannedEvent] = []
        for event in events:
            if not event.id or not event_is_current(event, current_date=current_date):
                continue
            reasons = route_backfill_reasons(
                event,
                enabled_reasons=enabled_reasons,
                forced=int(event.id) in event_ids,
            )
            if not reasons:
                continue
            sources = list(
                (
                    await session.execute(
                        select(EventSource).where(EventSource.event_id == int(event.id))
                    )
                ).scalars()
            )
            if source_ids:
                sources = [source for source in sources if int(source.id or 0) in source_ids]
            ranked = select_sources(sources, maximum=max(len(sources), 1))
            selected = ranked[:max_sources_per_event]
            if not selected:
                continue
            plan.append(
                PlannedEvent(
                    event_id=int(event.id),
                    reasons=reasons,
                    source_ids=tuple(int(source.id or 0) for source in selected),
                    unselected_source_ids=tuple(
                        int(source.id or 0) for source in ranked[max_sources_per_event:]
                    ),
                )
            )
            if limit and len(plan) >= limit:
                break
        return plan


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return _stamp(value if value.tzinfo else value.replace(tzinfo=timezone.utc))
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return {"sha256": hashlib.sha256(value).hexdigest(), "byte_count": len(value)}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _row_state(row: Any) -> dict[str, Any]:
    return {
        column.name: _jsonable(getattr(row, column.name))
        for column in row.__table__.columns
    }


async def _logical_db_snapshot(db: Database) -> dict[str, Any]:
    async with db.get_session() as session:
        events = list((await session.execute(select(Event).order_by(Event.id))).scalars())
        sources = list((await session.execute(select(EventSource).order_by(EventSource.id))).scalars())
    event_hashes = {
        str(event.id): hashlib.sha256(_canonical_json(_row_state(event))).hexdigest()
        for event in events
    }
    source_hashes = {
        str(source.id): hashlib.sha256(_canonical_json(_row_state(source))).hexdigest()
        for source in sources
    }
    return {
        "event_hashes": event_hashes,
        "event_source_hashes": source_hashes,
        "sha256": hashlib.sha256(
            _canonical_json({"events": event_hashes, "event_sources": source_hashes})
        ).hexdigest(),
    }


def _logical_changed_ids(before: Mapping[str, str], after: Mapping[str, str]) -> list[int]:
    return sorted(
        int(key)
        for key in set(before) | set(after)
        if before.get(key) != after.get(key)
    )


def _changed_keys(before: Mapping[str, Any], after: Mapping[str, Any]) -> list[str]:
    return sorted(key for key in set(before) | set(after) if before.get(key) != after.get(key))


def _legacy_projection(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = payload if isinstance(payload, Mapping) else {}
    child = payload.get("child_directed_decision")
    family = payload.get("family_suitable_decision")
    joint = payload.get("joint_family_activity_decision")
    child = child if isinstance(child, Mapping) else {}
    family = family if isinstance(family, Mapping) else {}
    joint = joint if isinstance(joint, Mapping) else {}
    if family.get("value") == "confirmed":
        value = "family"
    elif child.get("value") == "confirmed":
        value = "kids"
    elif (
        child.get("value") == family.get("value") == "denied"
        and "adults_only" in str(child.get("reason_code") or "")
        and "adults_only" in str(family.get("reason_code") or "")
    ):
        value = "none"
    else:
        value = "unknown"
    return {
        "value": value,
        "derived_from_facts_v3": True,
        "input_values": {
            "child_directed": child.get("value", "unknown"),
            "family_suitable": family.get("value", "unknown"),
            "joint_family_activity": joint.get("value", "unknown"),
        },
    }


def _validated_outcomes(payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, Mapping):
        return None
    keys = ("admission_decision", *AUDIENCE_FACT_KEYS, "people_appearances")
    return {key: _jsonable(payload.get(key)) for key in keys}


def _trace_summary(records: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [row for row in records if row.get("label") == "collection_candidate_adjudication"]
    physical_values = [row.get("physical_sends") for row in rows]
    physical_sends = (
        sum(int(value) for value in physical_values if isinstance(value, int))
        if physical_values and all(isinstance(value, int) for value in physical_values)
        else None
    )
    requested = next(
        (str(row.get("requested_model") or row.get("model")) for row in rows if row.get("requested_model") or row.get("model")),
        None,
    )
    actual_values: list[str] = []
    for row in rows:
        nested = row.get("actual_models")
        if isinstance(nested, list):
            actual_values.extend(str(value) for value in nested if str(value or "").strip())
        elif row.get("actual_model") or row.get("provider_path"):
            actual_values.append(str(row.get("actual_model") or row.get("provider_path")))
    actual = " -> ".join(dict.fromkeys(actual_values)) or None
    statuses = [str(row.get("status") or "") for row in rows]
    token_rows = [row.get("token_usage") for row in rows]
    nested_input_tokens = sum(
        int(usage.get("input_tokens") or usage.get("prompt_tokens") or 0)
        for usage in token_rows
        if isinstance(usage, Mapping)
    )
    nested_output_tokens = sum(
        int(usage.get("output_tokens") or usage.get("completion_tokens") or 0)
        for usage in token_rows
        if isinstance(usage, Mapping)
    )
    return {
        "logical_calls": 1 if rows else 0,
        "physical_sends": physical_sends,
        "requested_model": requested,
        "actual_model_path": actual,
        "fallback_used": any("4o" in status.casefold() for status in statuses)
        or any(bool(row.get("fallback_used")) for row in rows),
        "attempts": sum(int(row.get("attempts") or 0) for row in rows),
        "rate_limit_waits": sum(int(row.get("rate_limit_waits") or 0) for row in rows),
        "input_tokens": sum(int(row.get("input_tokens") or 0) for row in rows)
        + nested_input_tokens,
        "output_tokens": sum(int(row.get("output_tokens") or 0) for row in rows)
        + nested_output_tokens,
        "latency_sec": round(sum(float(row.get("duration_sec") or 0) for row in rows), 6),
        "statuses": statuses,
    }


@contextlib.contextmanager
def _primary_only(enabled: bool) -> Iterator[None]:
    overrides = {
        "SMART_UPDATE_MODEL": "gemma-4-31b-it",
        "SMART_UPDATE_4O_FALLBACK": "0",
        "GOOGLE_AI_FALLBACK_MODELS": "",
        "SMART_UPDATE_GEMMA_RETRIES": "1",
    }
    previous = {name: os.environ.get(name) for name in overrides}
    if enabled:
        os.environ.update(overrides)
    try:
        yield
    finally:
        if enabled:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


def _apply_with_reason_allowlist(
    event: Event,
    payload: Mapping[str, Any],
    *,
    source: EventSource,
    source_corpus: str,
    input_hash: str,
    reasons: Iterable[str],
) -> bool:
    kwargs: dict[str, Any] = {
        "source": source,
        "source_corpus": source_corpus,
        "input_hash": input_hash,
    }
    parameters = inspect.signature(apply_collection_decisions).parameters
    if "reasons" in parameters:
        kwargs["reasons"] = set(reasons)
    elif "allowed_reasons" in parameters or any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()
    ):
        kwargs["allowed_reasons"] = set(reasons)
    return bool(apply_collection_decisions(event, payload, **kwargs))


async def execute_plan(
    db: Database,
    plan: Iterable[PlannedEvent],
    *,
    mode: str,
    primary_only: bool = False,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "attempted_sources": 0,
        "provider_calls": 0,
        "physical_sends": 0,
        "physical_sends_complete": True,
        "writes": 0,
        "cached_sources": 0,
        "deferred_sources": 0,
        "evaluated_sources": 0,
        "applied_sources": 0,
        "unchanged_sources": 0,
        "events": [],
    }
    for item in plan:
        event_report: dict[str, Any] = {
            "event_id": item.event_id,
            "reasons": list(item.reasons),
            "selected_source_ids": list(item.source_ids),
            "unselected_source_ids": list(item.unselected_source_ids),
            "sources": [],
        }
        # Apply lower-ranked evidence first; highest-ranked evidence wins last.
        for source_id in reversed(item.source_ids):
            async with db.get_session() as session:
                event = await session.get(Event, item.event_id)
                source = await session.get(EventSource, source_id)
            source_report: dict[str, Any] = {
                "source_id": source_id,
                "provider_called": False,
                "write_status": "none",
                "changed_keys": [],
            }
            if event is None or source is None or int(source.event_id) != item.event_id:
                source_report.update(status="missing", deferred_reason="source_event_binding")
                report["deferred_sources"] += 1
                event_report["sources"].append(source_report)
                continue
            candidate = build_candidate(event, source, item.reasons)
            input_hash = collection_adjudication_input_hash(candidate)
            source_report["input_hash"] = input_hash
            if evaluation_receipt_covers(
                event,
                reasons=item.reasons,
                input_hash=input_hash,
                source_id=source_id,
            ):
                source_report.update(status="cached", write_status="cached_noop")
                report["cached_sources"] += 1
                report["unchanged_sources"] += 1
                event_report["sources"].append(source_report)
                continue

            report["attempted_sources"] += 1
            reset_trace = getattr(collection_core, "reset_smart_update_llm_trace", None)
            get_trace = getattr(collection_core, "get_smart_update_llm_trace", None)
            if callable(reset_trace):
                reset_trace()
            with _primary_only(primary_only):
                payload = await adjudicate_collection_candidate(candidate)
            trace = _trace_summary(get_trace() if callable(get_trace) else [])
            # A logical adjudicator invocation is provider-bound unless it
            # returned a candidate-preloaded result; backfill never preloads it.
            source_report["provider_called"] = True
            source_report["trace"] = trace
            report["provider_calls"] += 1
            if trace["physical_sends"] is None:
                report["physical_sends_complete"] = False
            else:
                report["physical_sends"] += int(trace["physical_sends"])
            if payload is None:
                source_report.update(status="deferred", deferred_reason="provider_or_validation_failure")
                report["deferred_sources"] += 1
                event_report["sources"].append(source_report)
                continue
            source_report["validated_outcomes"] = _validated_outcomes(payload)
            source_report["legacy_projection"] = _legacy_projection(payload)
            if mode == "evaluate":
                source_report.update(status="evaluated", write_status="not_requested")
                report["evaluated_sources"] += 1
                event_report["sources"].append(source_report)
                continue

            async with db.get_session() as session:
                persisted_event = await session.get(Event, item.event_id)
                persisted_source = await session.get(EventSource, source_id)
                if (
                    persisted_event is None
                    or persisted_source is None
                    or int(persisted_source.event_id) != item.event_id
                ):
                    source_report.update(status="deferred", deferred_reason="source_event_binding")
                    report["deferred_sources"] += 1
                else:
                    before = _row_state(persisted_event)
                    changed = _apply_with_reason_allowlist(
                        persisted_event,
                        payload,
                        source=persisted_source,
                        source_corpus=str(persisted_source.source_text or ""),
                        input_hash=input_hash,
                        reasons=item.reasons,
                    )
                    after = _row_state(persisted_event)
                    changed_keys = _changed_keys(before, after)
                    allowed = {"collection_decisions"}
                    if "admission" in item.reasons:
                        allowed.add("is_free")
                    forbidden = sorted(set(changed_keys) - allowed)
                    if forbidden:
                        await session.rollback()
                        source_report.update(
                            status="deferred",
                            write_status="rejected_forbidden_diff",
                            changed_keys=changed_keys,
                            deferred_reason="forbidden_event_fields:" + ",".join(forbidden),
                        )
                        report["deferred_sources"] += 1
                    elif changed:
                        session.add(persisted_event)
                        await session.commit()
                        source_report.update(
                            status="applied", write_status="committed", changed_keys=changed_keys
                        )
                        report["applied_sources"] += 1
                        report["writes"] += 1
                    else:
                        await session.rollback()
                        source_report.update(status="unchanged", write_status="noop")
                        report["unchanged_sources"] += 1
            event_report["sources"].append(source_report)
        report["events"].append(event_report)
    return report


async def apply_plan(db: Database, plan: Iterable[PlannedEvent]) -> dict[str, Any]:
    """Compatibility entry point for callers of the v1 script."""

    return await execute_plan(db, plan, mode="apply")


def _parse_id_file(path: Path, *, object_key: str) -> list[int]:
    raw = path.read_text(encoding="utf-8")
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        decoded = [line.strip() for line in raw.splitlines() if line.strip()]
    if isinstance(decoded, Mapping):
        decoded = decoded.get(object_key)
    if not isinstance(decoded, list):
        raise ValueError("event ID file must be a JSON list, {event_ids:[...]}, or one ID per line")
    return [int(value) for value in decoded]


def _requested_event_ids(args: argparse.Namespace) -> list[int]:
    values = [int(value) for value in (args.event_id or [])]
    if getattr(args, "event_id_file", None):
        values.extend(_parse_id_file(args.event_id_file, object_key="event_ids"))
    if any(value <= 0 for value in values):
        raise ValueError("event IDs must be positive integers")
    if len(values) != len(set(values)):
        raise ValueError("duplicate event IDs are not allowed")
    if len(values) > MAX_EVENT_IDS:
        raise ValueError(f"at most {MAX_EVENT_IDS} explicit event IDs are allowed")
    return values


def _requested_source_ids(args: argparse.Namespace) -> list[int]:
    values = [int(value) for value in (getattr(args, "source_id", None) or [])]
    if getattr(args, "source_id_file", None):
        values.extend(_parse_id_file(args.source_id_file, object_key="source_ids"))
    if any(value <= 0 for value in values):
        raise ValueError("source IDs must be positive integers")
    if len(values) != len(set(values)):
        raise ValueError("duplicate source IDs are not allowed")
    if len(values) > MAX_SOURCE_IDS:
        raise ValueError(f"at most {MAX_SOURCE_IDS} explicit source IDs are allowed")
    return values


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=os.getenv("DB_PATH") or "/data/db.sqlite")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan", dest="mode", action="store_const", const="plan")
    mode.add_argument("--evaluate", dest="mode", action="store_const", const="evaluate")
    mode.add_argument("--apply", dest="mode", action="store_const", const="apply")
    parser.set_defaults(mode="plan")
    parser.add_argument("--primary-only", action="store_true")
    parser.add_argument("--current-date", default=_utc_now().date().isoformat())
    parser.add_argument("--reason", action="append", choices=sorted(REASONS), default=[])
    parser.add_argument("--event-id", action="append", type=int, default=[])
    parser.add_argument("--event-id-file", type=Path)
    parser.add_argument("--source-id", action="append", type=int, default=[])
    parser.add_argument("--source-id-file", type=Path)
    parser.add_argument("--limit", type=int, default=MAX_EVENT_IDS)
    parser.add_argument("--max-sources-per-event", type=int, default=1)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        validate_args(args)
    except ValueError as exc:
        parser.error(str(exc))
    return args


def validate_args(args: argparse.Namespace) -> tuple[list[int], list[int]]:
    event_ids = _requested_event_ids(args)
    source_ids = _requested_source_ids(args)
    if args.mode in {"evaluate", "apply"} and not event_ids:
        raise ValueError(f"--{args.mode} requires an explicit --event-id/--event-id-file allowlist")
    if int(args.limit) < 1 or int(args.limit) > MAX_EVENT_IDS:
        raise ValueError(f"--limit must be between 1 and {MAX_EVENT_IDS}")
    if args.mode != "plan" and len(event_ids) > int(args.limit):
        raise ValueError("explicit event IDs exceed --limit; refusing silent truncation")
    if not 1 <= int(args.max_sources_per_event) <= MAX_SOURCES_PER_EVENT:
        raise ValueError(
            f"--max-sources-per-event must be between 1 and {MAX_SOURCES_PER_EVENT}"
        )
    if bool(args.primary_only) and args.mode == "plan":
        raise ValueError("--primary-only is only meaningful with --evaluate or --apply")
    if source_ids and not event_ids:
        raise ValueError("explicit source IDs require an explicit event-ID allowlist")
    return event_ids, source_ids


async def _validate_source_bindings(
    db: Database,
    *,
    event_ids: set[int],
    source_ids: set[int],
    max_sources_per_event: int,
) -> list[dict[str, int]]:
    if not source_ids:
        return []
    async with db.get_session() as session:
        rows = list(
            (
                await session.execute(
                    select(EventSource).where(EventSource.id.in_(source_ids)).order_by(EventSource.id)
                )
            ).scalars()
        )
    found = {int(row.id or 0) for row in rows}
    missing = sorted(source_ids - found)
    if missing:
        raise ValueError(f"explicit source IDs not found: {missing}")
    wrong_event = sorted(
        (int(row.id or 0), int(row.event_id))
        for row in rows
        if int(row.event_id) not in event_ids
    )
    if wrong_event:
        raise ValueError(f"source/event allowlist binding mismatch: {wrong_event}")
    unusable = sorted(
        int(row.id or 0)
        for row in rows
        if not str(row.source_url or "").strip() or not str(row.source_text or "").strip()
    )
    if unusable:
        raise ValueError(f"explicit sources lack source_url/source_text: {unusable}")
    counts: dict[int, int] = {}
    for row in rows:
        counts[int(row.event_id)] = counts.get(int(row.event_id), 0) + 1
    over = {event_id: count for event_id, count in counts.items() if count > max_sources_per_event}
    if over:
        raise ValueError(
            "explicit sources exceed --max-sources-per-event; refusing truncation: " + str(over)
        )
    return [
        {"event_id": int(row.event_id), "source_id": int(row.id or 0)}
        for row in rows
    ]


def _command(args: argparse.Namespace) -> str:
    explicit = getattr(args, "generator_command", None)
    if explicit:
        return str(explicit)
    return shlex.join([sys.executable, str(Path(__file__).relative_to(ROOT)), *sys.argv[1:]])


async def run(args: argparse.Namespace) -> dict[str, Any]:
    # Older internal callers may still provide ``apply`` instead of ``mode``.
    if not hasattr(args, "mode"):
        args.mode = "apply" if bool(getattr(args, "apply", False)) else "plan"
    for name, default in (
        ("event_id", []),
        ("event_id_file", None),
        ("source_id", []),
        ("source_id_file", None),
        ("primary_only", False),
        ("reason", []),
        ("limit", MAX_EVENT_IDS),
        ("max_sources_per_event", 1),
    ):
        if not hasattr(args, name):
            setattr(args, name, default)
    requested_ids, explicit_source_ids = validate_args(args)
    source_path = Path(args.db).expanduser().resolve()
    if not source_path.is_file():
        raise ValueError(f"SQLite DB does not exist: {source_path}")
    started = _utc_now()
    sha_before = _sha256_file(source_path)
    quick_before = _quick_check(source_path)
    if quick_before != "ok":
        raise ValueError(f"SQLite quick_check failed: {quick_before}")
    enabled_reasons = args.reason or sorted(REASONS)

    readonly = args.mode in {"plan", "evaluate"}
    copy_context = _read_only_working_copy(source_path) if readonly else contextlib.nullcontext(source_path)
    with copy_context as working_path:
        db = Database(str(working_path))
        try:
            source_bindings = await _validate_source_bindings(
                db,
                event_ids=set(requested_ids),
                source_ids=set(explicit_source_ids),
                max_sources_per_event=int(args.max_sources_per_event),
            )
            # Query all eligible plan rows first. Any later bound is explicit in
            # report metadata instead of being silently hidden in build_plan.
            complete_plan = await build_plan(
                db,
                current_date=date.fromisoformat(args.current_date),
                enabled_reasons=enabled_reasons,
                event_ids=set(requested_ids),
                limit=0,
                max_sources_per_event=int(args.max_sources_per_event),
                source_ids=set(explicit_source_ids),
            )
            complete_resolved_ids = {item.event_id for item in complete_plan}
            unresolved_requested_ids = sorted(set(requested_ids) - complete_resolved_ids)
            if args.mode in {"evaluate", "apply"} and unresolved_requested_ids:
                raise ValueError(
                    "requested events are missing, ineligible, or have no usable selected source: "
                    + str(unresolved_requested_ids)
                )
            selected_explicit_sources = {
                source_id for item in complete_plan for source_id in item.source_ids
            }
            unresolved_explicit_sources = sorted(
                set(explicit_source_ids) - selected_explicit_sources
            )
            if args.mode in {"evaluate", "apply"} and unresolved_explicit_sources:
                raise ValueError(
                    "requested sources were not selected; refusing silent truncation: "
                    + str(unresolved_explicit_sources)
                )
            plan = complete_plan[: int(args.limit)]
            resolved = [item.event_id for item in plan]
            unresolved = sorted(set(requested_ids) - set(resolved))
            logical_before = await _logical_db_snapshot(db)
            execution = None
            if args.mode in {"evaluate", "apply"}:
                execution = await execute_plan(
                    db,
                    plan,
                    mode=args.mode,
                    primary_only=bool(args.primary_only),
                )
            logical_after = await _logical_db_snapshot(db)
        finally:
            await db.close()

    sha_after = _sha256_file(source_path)
    quick_after = _quick_check(source_path)
    changed_event_ids = _logical_changed_ids(
        logical_before["event_hashes"], logical_after["event_hashes"]
    )
    changed_source_ids = _logical_changed_ids(
        logical_before["event_source_hashes"], logical_after["event_source_hashes"]
    )
    selected_ids = {item.event_id for item in plan}
    allowlist_ok = not changed_source_ids and set(changed_event_ids) <= selected_ids
    if readonly:
        allowlist_ok = allowlist_ok and logical_before["sha256"] == logical_after["sha256"]
    finished = _utc_now()
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "repo_sha": _repo_sha(),
        "generator_command": _command(args),
        "mode": args.mode,
        "primary_only": bool(args.primary_only),
        "facts_policy_version": STATIC_COLLECTION_FACTS_POLICY_VERSION,
        "adjudication_schema_version": STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION,
        "started_at": _stamp(started),
        "finished_at": _stamp(finished),
        "db_snapshot": {
            "path": str(source_path),
            "sha256_before": sha_before,
            "sha256_after": sha_after,
            "quick_check_before": quick_before,
            "quick_check_after": quick_after,
        },
        "selection": {
            "current_date": args.current_date,
            "reasons": sorted(enabled_reasons),
            "requested_event_ids": requested_ids,
            "resolved_event_ids": resolved,
            "unresolved_event_ids": unresolved,
            "eligible_event_count": len(complete_plan),
            "selected_event_count": len(plan),
            "selection_truncated": len(plan) != len(complete_plan),
            "omitted_event_ids": [item.event_id for item in complete_plan[len(plan):]],
            "max_sources_per_event": int(args.max_sources_per_event),
            "requested_source_ids": sorted(
                explicit_source_ids
                or [source_id for item in plan for source_id in item.source_ids]
            ),
            "requested_source_bindings": source_bindings,
        },
        "plan": [_jsonable(asdict(item)) for item in plan],
        "execution": execution,
        "logical_diff": {
            "sha256_before": logical_before["sha256"],
            "sha256_after": logical_after["sha256"],
            "changed_event_ids": changed_event_ids,
            "changed_event_source_ids": changed_source_ids,
            "selected_event_allowlist_ok": allowlist_ok,
        },
    }
    return report


def validate_report_schema(report: Mapping[str, Any]) -> None:
    """Validate a report against the committed versioned JSON schema."""

    import jsonschema

    schema = json.loads(REPORT_SCHEMA_PATH.read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.validate(instance=dict(report), schema=schema)


def main() -> int:
    args = parse_args()
    args.generator_command = shlex.join([sys.executable, sys.argv[0], *sys.argv[1:]])
    result = asyncio.run(run(args))
    validate_report_schema(result)
    encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
