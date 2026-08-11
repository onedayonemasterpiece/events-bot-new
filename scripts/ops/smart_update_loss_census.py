#!/usr/bin/env python3
"""Deterministic, read-only Smart Update loss census.

The census deliberately consumes durable metadata/evidence rather than trying
to rediscover semantics with regular expressions.  A carrier revision is the
unit of inventory; event occurrences and lifecycle actions are reported as
separate measures.  Missing evidence is classified as ``T`` and is never
extrapolated from ``vk_misses_sample``.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from typing import Any, Iterable, Mapping, Sequence


REPORT_SCHEMA = "kenigevents.smart_update_loss_census.v1"
LOSS_CLASSES: tuple[tuple[str, str], ...] = (
    ("A", "DISCOVERY_NO_KEYWORDS"),
    ("B", "DISCOVERY_NO_DATE"),
    ("C", "DISCOVERY_PAST_HINT"),
    ("D", "DISCOVERY_TOO_FAR_HINT"),
    ("E", "DISCOVERY_CURSOR_OR_PAGE_GAP"),
    ("F", "INBOX_HINT_AUTO_REJECT"),
    ("G", "PRE_LLM_HISTORY_PREFILTER"),
    ("H", "PRE_LLM_ADMIN_PREFILTER"),
    ("I", "PRE_LLM_CANCELLATION_SHORT_CIRCUIT"),
    ("J", "PRE_LLM_PAYLOAD_OR_OCR_FAILURE"),
    ("K", "LLM_PROVIDER_OR_SCHEMA_FAILURE"),
    ("L", "LLM_OUTPUT_TRUNCATION"),
    ("M", "EVIDENCE_OMITTED_FROM_PROMPT"),
    ("N", "POST_LLM_REJECT_REASON_FULL"),
    ("O", "POST_LLM_REJECT_REASON_PARTIAL"),
    ("P", "SMART_UPDATE_IDENTITY_LOSS"),
    ("Q", "TECHNICAL_TERMINAL_FAILED"),
    ("R", "VALID_CONFIRMED_NO_EVENT"),
    ("S", "EXACT_REPLAY_OR_ALREADY_IMPORTED"),
    ("T", "UNKNOWN_EVIDENCE_UNAVAILABLE"),
)
CLASS_NAME = dict(LOSS_CLASSES)
CLASS_CODE = {name: code for code, name in LOSS_CLASSES}
VALID_CLASSES = frozenset(CLASS_NAME) | frozenset(CLASS_CODE)
COUNT_FIELDS = (
    "carrier_count",
    "llm_started_count",
    "llm_completed_count",
    "full_evidence_count",
    "incomplete_evidence_count",
    "extracted_event_occurrences",
    "lifecycle_actions",
    "would_create",
    "would_merge",
    "would_noop",
    "would_apply_lifecycle",
    "would_confirm_no_event",
    "would_retry",
    "unavailable_payload_count",
)
SUCCESS_TERMINALS = frozenset(
    {
        "CREATED", "MERGED", "NOOP_EXACT_REPLAY", "LIFECYCLE_APPLIED",
        "EVENTS_RESOLVED", "LIFECYCLE_RESOLVED", "MIXED_RESOLVED", "EXACT_REPLAY",
    }
)


class CensusError(RuntimeError):
    pass


def parse_utc(value: str, *, field: str) -> datetime:
    raw = str(value or "").strip()
    try:
        parsed = (
            datetime.combine(date.fromisoformat(raw), datetime.min.time())
            if len(raw) == 10
            else datetime.fromisoformat(raw.replace("Z", "+00:00"))
        )
    except ValueError as exc:
        raise CensusError(f"invalid_{field}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()


def normalize_class(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    if text in CLASS_NAME:
        return text
    return CLASS_CODE.get(text)


def _truth(evidence: Mapping[str, Any], *names: str) -> bool:
    return any(evidence.get(name) is True or evidence.get(name) == 1 for name in names)


def carrier_revision_key(evidence: Mapping[str, Any]) -> str:
    """Return an opaque immutable key; raw carrier identifiers never leak."""

    source = str(evidence.get("source_type") or evidence.get("source") or "unknown").strip().lower()
    carrier = evidence.get("carrier_id")
    if carrier in (None, ""):
        carrier = evidence.get("source_packet_id")
    if carrier in (None, ""):
        carrier = evidence.get("source_url")
    revision = evidence.get("source_revision_hash")
    if revision in (None, ""):
        revision = evidence.get("payload_hash")
    # No invented identity: unavailable carrier/revision material remains
    # distinct only by an explicitly supplied evidence row key.
    if carrier in (None, ""):
        carrier = evidence.get("evidence_row_id") or "unavailable"
    if revision in (None, ""):
        revision = "revision-unavailable"
    return hashlib.sha256(f"{source}\0{carrier}\0{revision}".encode("utf-8")).hexdigest()


def classify_carrier(evidence: Mapping[str, Any]) -> str:
    """Assign exactly one A--T origin using earliest irreversible loss.

    A durable successful terminal is definitive and overrides stale failure
    breadcrumbs from earlier attempts of the same revision.
    """

    terminals = evidence.get("terminal_outcomes") or evidence.get("terminal_outcome") or []
    if isinstance(terminals, str):
        terminals = [terminals]
    terminal_set = {str(item).strip().upper() for item in terminals if item is not None}
    if terminal_set & SUCCESS_TERMINALS or _truth(evidence, "already_imported", "exact_replay"):
        return "S"

    explicit = normalize_class(evidence.get("loss_class"))
    signals: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("A", ("discovery_no_keywords", "no_keywords")),
        ("B", ("discovery_no_date", "no_date")),
        ("C", ("discovery_past_hint", "past_event", "past_hint")),
        ("D", ("discovery_too_far_hint", "too_far", "too_far_hint")),
        ("E", ("cursor_gap", "page_gap", "discovery_gap")),
        ("F", ("inbox_hint_auto_reject", "null_event_ts_hint_reject")),
        ("G", ("history_prefilter", "pre_llm_history_prefilter")),
        ("H", ("admin_prefilter", "pre_llm_admin_prefilter")),
        ("I", ("cancellation_short_circuit", "cancellation_no_match")),
        ("J", ("payload_failure", "ocr_failure", "attachment_failure")),
        ("K", ("llm_provider_failure", "llm_schema_failure", "malformed_llm_response")),
        ("L", ("llm_output_truncated", "output_truncation")),
        ("M", ("evidence_omitted", "incomplete_prompt_evidence")),
        ("N", ("post_llm_reject_full", "low_confidence_full")),
        ("O", ("post_llm_reject_partial", "low_confidence_partial", "partial_child_loss")),
        ("P", ("smart_update_identity_loss", "identity_loss")),
        ("Q", ("technical_terminal_failed", "persist_failure", "technical_failed")),
        ("R", ("confirmed_no_event", "valid_no_event")),
    )
    active = {code for code, names in signals if _truth(evidence, *names)}
    if explicit:
        active.add(explicit)
    for code, _name in LOSS_CLASSES:
        if code in active:
            return code
    return "T"


def _int(evidence: Mapping[str, Any], name: str) -> int:
    try:
        return max(0, int(evidence.get(name) or 0))
    except (TypeError, ValueError):
        return 0


def _merge_evidence(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        key = carrier_revision_key(row)
        if key not in merged:
            row["carrier_revision_key"] = key
            merged[key] = row
            continue
        current = merged[key]
        # Duplicate observations may add evidence but must not double count a
        # carrier or its children/actions.
        for name, value in row.items():
            if isinstance(value, bool):
                current[name] = bool(current.get(name)) or value
            elif name in {
                "extracted_event_occurrences", "lifecycle_actions", "would_create",
                "would_merge", "would_noop", "would_apply_lifecycle",
                "would_confirm_no_event", "would_retry",
            }:
                current[name] = max(_int(current, name), _int(row, name))
            elif current.get(name) in (None, "", []):
                current[name] = value
        old_terminals = current.get("terminal_outcomes") or []
        new_terminals = row.get("terminal_outcomes") or []
        if isinstance(old_terminals, str):
            old_terminals = [old_terminals]
        if isinstance(new_terminals, str):
            new_terminals = [new_terminals]
        current["terminal_outcomes"] = sorted({*old_terminals, *new_terminals})
    return [merged[key] for key in sorted(merged)]


def build_census(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    carriers = _merge_evidence(rows)
    buckets: dict[str, dict[str, Any]] = {}
    for code, name in LOSS_CLASSES:
        buckets[code] = {"code": code, "name": name, **{field: 0 for field in COUNT_FIELDS}, "source_count": 0}
        buckets[code]["_sources"] = set()
    for row in carriers:
        code = classify_carrier(row)
        bucket = buckets[code]
        bucket["carrier_count"] += 1
        bucket["_sources"].add(str(row.get("source_type") or row.get("source") or "unknown"))
        bucket["llm_started_count"] += int(_truth(row, "llm_started"))
        bucket["llm_completed_count"] += int(_truth(row, "llm_completed"))
        full = _truth(row, "full_evidence")
        incomplete = _truth(row, "incomplete_evidence", "evidence_omitted", "partial_child_loss")
        bucket["full_evidence_count"] += int(full)
        bucket["incomplete_evidence_count"] += int(incomplete)
        for name in COUNT_FIELDS[5:-1]:
            bucket[name] += _int(row, name)
        unavailable = _truth(row, "payload_unavailable") or not _truth(row, "raw_payload_available")
        bucket["unavailable_payload_count"] += int(unavailable)
    classes = []
    for code, _name in LOSS_CLASSES:
        bucket = buckets[code]
        bucket["source_count"] = len(bucket.pop("_sources"))
        classes.append(bucket)
    totals = {field: sum(int(item[field]) for item in classes) for field in COUNT_FIELDS}
    totals["source_count"] = len(
        {str(row.get("source_type") or row.get("source") or "unknown") for row in carriers}
    )
    inventory = [
        {
            "carrier_revision_key": row["carrier_revision_key"],
            "source_type": str(row.get("source_type") or row.get("source") or "unknown"),
            "loss_class": classify_carrier(row),
            "extracted_event_occurrences": _int(row, "extracted_event_occurrences"),
            "lifecycle_actions": _int(row, "lifecycle_actions"),
            "payload_available": _truth(row, "raw_payload_available"),
        }
        for row in carriers
    ]
    return {
        "schema": REPORT_SCHEMA,
        "unit": "carrier_revision",
        "classes": classes,
        "totals": totals,
        "inventory_hash": stable_hash(inventory),
        "inventory": inventory,
        "extrapolation": {"vk_misses_sample_multiplier": None, "permitted": False},
    }


def stratified_sample(
    rows: Iterable[Mapping[str, Any]], *, per_stratum: int, start: str = "2026-02-01", until: str = "2026-08-01"
) -> dict[str, Any]:
    """Select deterministic source/month/class strata without extrapolation."""

    if per_stratum <= 0:
        raise CensusError("invalid_per_stratum")
    start_dt, until_dt = parse_utc(start, field="sample_start"), parse_utc(until, field="sample_until")
    if until_dt <= start_dt:
        raise CensusError("invalid_sample_window")
    strata: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in _merge_evidence(rows):
        observed = row.get("observed_at") or row.get("published_at")
        try:
            when = parse_utc(str(observed), field="observed_at")
        except CensusError:
            continue
        if not start_dt <= when < until_dt:
            continue
        source = str(row.get("source_type") or row.get("source") or "unknown")
        key = (source, when.strftime("%Y-%m"), classify_carrier(row))
        strata[key].append(row)
    report = []
    for key in sorted(strata):
        population = sorted(strata[key], key=lambda item: item["carrier_revision_key"])
        chosen = population[:per_stratum]
        denominator = len(population)
        report.append(
            {
                "source": key[0], "month": key[1], "loss_class": key[2],
                "population_denominator": denominator,
                "sample_count": len(chosen),
                "coverage_ratio": len(chosen) / denominator,
                "carrier_revision_keys": [item["carrier_revision_key"] for item in chosen],
            }
        )
    return {
        "window": {"since": utc(start_dt), "until_exclusive": utc(until_dt)},
        "per_stratum": per_stratum,
        "strata": report,
        "population_denominator": sum(item["population_denominator"] for item in report),
        "sample_count": sum(item["sample_count"] for item in report),
        "extrapolation_permitted": False,
        "vk_misses_sample_multiplier": None,
    }


def _load_json_rows(path: str | Path | None) -> list[dict[str, Any]]:
    if not path:
        return []
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(raw, Mapping):
        for key in ("rows", "data", "evidence", "vk_misses_sample"):
            if isinstance(raw.get(key), list):
                raw = raw[key]
                break
    if not isinstance(raw, list) or not all(isinstance(row, Mapping) for row in raw):
        raise CensusError("invalid_evidence_json")
    return [dict(row) for row in raw]


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    exists = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return {str(row[1]) for row in con.execute(f'PRAGMA table_info("{table}")')} if exists else set()


def _json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (ValueError, json.JSONDecodeError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _raw_packet_rows(
    con: sqlite3.Connection, since: datetime, until: datetime
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Feature-detect the planned raw-first packet/attempt contract."""

    rows: list[dict[str, Any]] = []
    features: dict[str, Any] = {}
    columns = _table_columns(con, "vk_source_packet")
    identity = {"id", "source_revision_hash"}
    time_col = "published_at" if "published_at" in columns else "fetched_at" if "fetched_at" in columns else None
    if not identity.issubset(columns) or not time_col:
        features["vk_source_packet"] = {"available": False, "rows": 0}
    else:
        selected = con.execute(
            f'SELECT * FROM vk_source_packet WHERE datetime("{time_col}")>=datetime(?) '
            f'AND datetime("{time_col}")<datetime(?) ORDER BY id,source_revision_hash',
            (utc(since), utc(until)),
        ).fetchall()
        attempts_by_packet: dict[Any, list[Mapping[str, Any]]] = defaultdict(list)
        attempt_columns = _table_columns(con, "vk_source_packet_attempt")
        packet_fk = "source_packet_id" if "source_packet_id" in attempt_columns else "packet_id" if "packet_id" in attempt_columns else None
        if packet_fk:
            for attempt in con.execute(f'SELECT * FROM vk_source_packet_attempt ORDER BY "{packet_fk}",id').fetchall():
                attempts_by_packet[attempt[packet_fk]].append(dict(attempt))
        for item in selected:
            data = dict(item)
            source_identity = (
                data.get("owner_id") or data.get("group_id") or data.get("source_url") or "unknown"
            )
            native_id = data.get("native_post_id") or data.get("post_id") or data["id"]
            evidence: dict[str, Any] = {
                "source_type": data.get("source_type") or "vk",
                "carrier_id": f"{source_identity}:{native_id}",
                "source_revision_hash": data.get("source_revision_hash") or data.get("payload_hash"),
                "raw_payload_available": bool(str(data.get("raw_text") or "").strip()),
                "observed_at": data.get(time_col),
                "payload_unavailable": not bool(str(data.get("raw_text") or "").strip()),
            }
            for name in ("discovery_hints_json", "evidence_manifest_json", "parse_result_json"):
                evidence.update(_json_mapping(data.get(name)))
            reason = str(data.get("last_typed_reason") or "").strip().upper()
            explicit = normalize_class(reason)
            if explicit:
                evidence["loss_class"] = explicit
            outcome = str(data.get("carrier_outcome") or "").strip()
            if outcome:
                evidence["terminal_outcomes"] = [outcome]
            if outcome.upper() == "CONFIRMED_NO_EVENT":
                evidence["confirmed_no_event"] = True
            if outcome.upper() == "CONFIRMED_PRODUCT_EXCLUSION":
                # This is definitive policy evidence but not proof of a valid
                # no-event; keep it unavailable to the loss taxonomy.
                evidence["payload_unavailable"] = not evidence["raw_payload_available"]
            typed_reason_flags = {
                "NO_KEYWORDS": "discovery_no_keywords", "NO_DATE": "discovery_no_date",
                "PAST_HINT": "discovery_past_hint", "TOO_FAR_HINT": "discovery_too_far_hint",
                "HISTORY_PREFILTER": "history_prefilter", "ADMIN_PREFILTER": "admin_prefilter",
                "CANCELLATION": "cancellation_short_circuit", "OCR": "ocr_failure",
                "PROVIDER": "llm_provider_failure", "SCHEMA": "llm_schema_failure",
                "TRUNCAT": "llm_output_truncated", "EVIDENCE_OMITTED": "evidence_omitted",
                "LOW_CONFIDENCE_PARTIAL": "post_llm_reject_partial",
                "LOW_CONFIDENCE": "post_llm_reject_full", "IDENTITY": "smart_update_identity_loss",
            }
            for marker, flag in typed_reason_flags.items():
                if marker == "LOW_CONFIDENCE" and "LOW_CONFIDENCE_PARTIAL" in reason:
                    continue
                if marker in reason:
                    evidence[flag] = True
            llm_status = str(data.get("llm_status") or "").lower()
            evidence["llm_started"] = llm_status not in {"", "not_started", "pending"}
            evidence["llm_completed"] = llm_status in {"completed", "success", "succeeded"}
            evidence["ocr_failure"] = str(data.get("ocr_status") or "").lower() in {"error", "failed"}
            for attempt in attempts_by_packet.get(data["id"], []):
                for name in ("evidence_json", "evidence_manifest_json", "result_json", "parse_result_json"):
                    evidence.update(_json_mapping(attempt.get(name)))
                terminal = attempt.get("carrier_outcome") or attempt.get("terminal_outcome")
                if terminal:
                    evidence.setdefault("terminal_outcomes", []).append(str(terminal))
            parse_result = _json_mapping(data.get("parse_result_json"))
            children = parse_result.get("event_children") or parse_result.get("events")
            actions = parse_result.get("lifecycle_actions")
            if isinstance(children, list):
                evidence["extracted_event_occurrences"] = len(children)
            if isinstance(actions, list):
                evidence["lifecycle_actions"] = len(actions)
            rows.append(evidence)
        features["vk_source_packet"] = {
            "available": True, "rows": len(selected), "attempt_ledger": bool(packet_fk),
            "revision_evidence": "durable",
        }
    continuation_columns = _table_columns(con, "vk_crawl_continuation")
    features["vk_crawl_continuation"] = {
        "available": bool(continuation_columns), "columns": sorted(continuation_columns)
    }
    return rows, features


def _legacy_rows(con: sqlite3.Connection, since: datetime, until: datetime) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    features: dict[str, Any] = {}
    raw_rows, raw_features = _raw_packet_rows(con, since, until)
    rows.extend(raw_rows)
    features.update(raw_features)
    columns = _table_columns(con, "vk_inbox")
    if {"id", "group_id", "post_id", "status"}.issubset(columns):
        time_col = "created_at" if "created_at" in columns else "date" if "date" in columns else None
        if time_col:
            time_expr = f'datetime("{time_col}")' if time_col != "date" else 'datetime("date",\'unixepoch\')'
            selected = con.execute(
                f'SELECT * FROM vk_inbox WHERE {time_expr}>=datetime(?) AND {time_expr}<datetime(?) ORDER BY id',
                (utc(since), utc(until)),
            ).fetchall()
            for item in selected:
                data = dict(item)
                # A linked raw packet is already represented by its immutable
                # revision above; do not count the mutable inbox carrier twice.
                if raw_rows and data.get("source_packet_id") is not None:
                    continue
                reason = " ".join(str(data.get(k) or "") for k in ("status", "last_error", "last_result_json")).lower()
                evidence: dict[str, Any] = {
                    "source_type": "vk", "carrier_id": f'{data["group_id"]}:{data["post_id"]}',
                    "source_revision_hash": data.get("payload_hash") or "revision-unavailable",
                    "raw_payload_available": bool(str(data.get("text") or "").strip()),
                    "observed_at": data.get(time_col),
                    "already_imported": data.get("imported_event_id") is not None,
                }
                marker_map = {
                    "no_keywords": "discovery_no_keywords", "no_date": "discovery_no_date",
                    "past_event": "discovery_past_hint", "too_far": "discovery_too_far_hint",
                    "low_confidence": "post_llm_reject_full", "identity": "smart_update_identity_loss",
                    "ocr": "ocr_failure", "provider": "llm_provider_failure", "schema": "llm_schema_failure",
                }
                for marker, flag in marker_map.items():
                    if marker in reason:
                        evidence[flag] = True
                if str(data.get("status") or "").lower() in {"failed", "deferred"} and classify_carrier(evidence) == "T":
                    evidence["technical_terminal_failed"] = True
                rows.append(evidence)
            features["vk_inbox"] = {"available": True, "rows": len(selected), "revision_evidence": "partial"}
    else:
        features["vk_inbox"] = {"available": False, "rows": 0}

    telegram_columns = _table_columns(con, "telegram_scanned_message")
    telegram_time = "processed_at" if "processed_at" in telegram_columns else None
    if telegram_time and {"source_id", "message_id", "status"}.issubset(telegram_columns):
        selected = con.execute(
            "SELECT * FROM telegram_scanned_message WHERE datetime(processed_at)>=datetime(?) "
            "AND datetime(processed_at)<datetime(?) ORDER BY source_id,message_id",
            (utc(since), utc(until)),
        ).fetchall()
        for item in selected:
            data = dict(item)
            extracted, imported = _int(data, "events_extracted"), _int(data, "events_imported")
            reason = " ".join(str(data.get(k) or "") for k in ("status", "error", "last_error", "last_result_json")).lower()
            evidence = {
                "source_type": "telegram", "carrier_id": f'{data["source_id"]}:{data["message_id"]}',
                "source_revision_hash": data.get("source_revision_hash") or "revision-unavailable",
                "observed_at": data.get("processed_at"),
                "raw_payload_available": _truth(data, "raw_payload_available"),
                "extracted_event_occurrences": extracted,
                "already_imported": bool(imported and imported >= extracted),
                "partial_child_loss": bool(extracted > imported and imported > 0),
                "post_llm_reject_full": bool(extracted > 0 and imported == 0 and "low_confidence" in reason),
                "smart_update_identity_loss": "identity" in reason,
                "llm_provider_failure": "provider" in reason,
                "llm_schema_failure": "schema" in reason or "malformed" in reason,
                "ocr_failure": "ocr" in reason,
            }
            if str(data.get("status") or "").lower() in {"error", "failed", "retry_scheduled"} and classify_carrier(evidence) == "T":
                evidence["technical_terminal_failed"] = True
            rows.append(evidence)
        features["telegram_scanned_message"] = {
            "available": True, "rows": len(selected), "raw_payload_evidence": "feature_detected"
        }
    else:
        features["telegram_scanned_message"] = {"available": False, "rows": 0}

    candidate_columns = _table_columns(con, "smart_update_candidate_state")
    candidate_time = "created_at" if "created_at" in candidate_columns else "updated_at" if "updated_at" in candidate_columns else None
    if candidate_time and {"id", "current_outcome"}.issubset(candidate_columns):
        selected = con.execute(
            f'SELECT current_outcome FROM smart_update_candidate_state WHERE datetime("{candidate_time}")>=datetime(?) '
            f'AND datetime("{candidate_time}")<datetime(?) ORDER BY id',
            (utc(since), utc(until)),
        ).fetchall()
        outcomes: dict[str, int] = defaultdict(int)
        for item in selected:
            outcomes[str(item[0] or "UNKNOWN").upper()] += 1
        features["smart_update_candidate_state"] = {
            "available": True, "child_rows": len(selected), "by_outcome": dict(sorted(outcomes.items())),
            "carrier_inventory": "requires_source_packet_link;child_rows_not_counted_as_carriers",
        }
    else:
        features["smart_update_candidate_state"] = {"available": False, "rows": 0}

    for table, source_type in (("ticket_site_queue", "ticket"), ("festival_queue", "festival")):
        queue_columns = _table_columns(con, table)
        time_col = "updated_at" if "updated_at" in queue_columns else "created_at" if "created_at" in queue_columns else None
        if time_col and {"id", "status"}.issubset(queue_columns):
            selected = con.execute(
                f'SELECT * FROM "{table}" WHERE datetime("{time_col}")>=datetime(?) '
                f'AND datetime("{time_col}")<datetime(?) ORDER BY id',
                (utc(since), utc(until)),
            ).fetchall()
            for item in selected:
                data = dict(item)
                status = str(data.get("status") or "").lower()
                evidence = {
                    "source_type": source_type, "carrier_id": data["id"],
                    "source_revision_hash": data.get("payload_hash") or "revision-unavailable",
                    "observed_at": data.get(time_col),
                    "raw_payload_available": any(
                        bool(str(data.get(name) or "").strip())
                        for name in ("payload_json", "result_json", "last_result_json")
                    ),
                    "technical_terminal_failed": status in {"error", "failed"},
                    "already_imported": status in {"done", "imported", "complete", "completed"},
                }
                rows.append(evidence)
            features[table] = {"available": True, "rows": len(selected)}
        else:
            features[table] = {"available": False, "rows": 0}

    ops_columns = _table_columns(con, "ops_run")
    if {"kind", "started_at", "details_json"}.issubset(ops_columns):
        observations = 0
        runs = con.execute(
            "SELECT details_json FROM ops_run WHERE kind='parse' AND datetime(started_at)>=datetime(?) "
            "AND datetime(started_at)<datetime(?) ORDER BY started_at",
            (utc(since), utc(until)),
        ).fetchall()
        for run in runs:
            details = _json_mapping(run[0])
            for values in (details.get("sources") or {}).values() if isinstance(details.get("sources"), Mapping) else ():
                if isinstance(values, Mapping):
                    observations += _int(values, "failed") + _int(values, "retry_scheduled")
        features["source_parser_ops_run"] = {
            "available": True, "runs": len(runs), "failed_observations": observations,
            "carrier_inventory": "unavailable_without_raw_carrier_ids",
        }
    else:
        features["source_parser_ops_run"] = {"available": False, "runs": 0}
    # New ledger schemas are consumed only when their typed contract is present.
    for table in ("ingestion_funnel_ledger", "source_packet_ledger", "source_packet_attempt"):
        cols = _table_columns(con, table)
        required = {"source_type", "carrier_id", "source_revision_hash", "evidence_json"}
        time_col = next(
            (name for name in ("observed_at", "created_at", "started_at", "fetched_at") if name in cols),
            None,
        )
        if required.issubset(cols) and time_col:
            query = (
                f'SELECT source_type,carrier_id,source_revision_hash,evidence_json FROM "{table}" '
                f'WHERE datetime("{time_col}")>=datetime(?) AND datetime("{time_col}")<datetime(?) '
                f'ORDER BY source_type,carrier_id,source_revision_hash'
            )
            selected = con.execute(query, (utc(since), utc(until))).fetchall()
            accepted = 0
            for item in selected:
                try:
                    evidence = json.loads(item[3]) if isinstance(item[3], str) else dict(item[3] or {})
                except (TypeError, ValueError, json.JSONDecodeError):
                    evidence = {"payload_unavailable": True}
                evidence.update(source_type=item[0], carrier_id=item[1], source_revision_hash=item[2])
                rows.append(evidence)
                accepted += 1
            features[table] = {"available": True, "rows": accepted}
            break
        features[table] = {
            "available": False, "rows": 0,
            "reason": "window_boundary_unavailable" if required.issubset(cols) else "schema_unavailable",
        }
    return rows, features


def run(
    db_path: str | Path, *, since: str, until: str, evidence_paths: Sequence[str | Path] = (),
    supabase_evidence_paths: Sequence[str | Path] = (), sample_per_stratum: int | None = None,
) -> dict[str, Any]:
    since_dt, until_dt = parse_utc(since, field="since"), parse_utc(until, field="until")
    if until_dt <= since_dt:
        raise CensusError("invalid_window")
    path = Path(db_path).resolve()
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    con = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=ON")
    try:
        quick = [str(row[0]) for row in con.execute("PRAGMA quick_check")]
        if quick != ["ok"]:
            raise CensusError("quick_check_failed")
        rows, features = _legacy_rows(con, since_dt, until_dt)
        # Explicit offline exports feature-detect Supabase/miss evidence without
        # requiring credentials or network access in CI.
        for evidence_path in evidence_paths:
            rows.extend(_load_json_rows(evidence_path))
        supabase_count = 0
        for evidence_path in supabase_evidence_paths:
            loaded = _load_json_rows(evidence_path)
            rows.extend(loaded)
            supabase_count += len(loaded)
        features["supabase_offline_evidence"] = {"available": bool(supabase_evidence_paths), "rows": supabase_count, "network_used": False}
        report = build_census(rows)
        report.update(
            mode="read-only", since=utc(since_dt), until_exclusive=utc(until_dt),
            quick_check=quick, before_db_hash=before, after_db_hash=hashlib.sha256(path.read_bytes()).hexdigest(),
            changed_rows=0, query_only=True, features=features,
        )
        if sample_per_stratum is not None:
            report["historical_sample"] = stratified_sample(rows, per_stratum=sample_per_stratum)
        return report
    finally:
        con.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--since", required=True)
    parser.add_argument("--until", required=True, help="exclusive UTC boundary")
    parser.add_argument("--evidence-json", action="append", default=[])
    parser.add_argument("--supabase-evidence-json", action="append", default=[])
    parser.add_argument("--sample-per-stratum", type=int)
    parser.add_argument("--output", default="-")
    args = parser.parse_args(argv)
    try:
        result = run(
            args.db, since=args.since, until=args.until, evidence_paths=args.evidence_json,
            supabase_evidence_paths=args.supabase_evidence_json, sample_per_stratum=args.sample_per_stratum,
        )
    except (CensusError, sqlite3.DatabaseError, OSError) as exc:
        sys.stderr.write(stable_json({"schema": REPORT_SCHEMA, "status": "blocked", "reason": str(exc)}) + "\n")
        return 2
    rendered = json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if args.output == "-":
        sys.stdout.write(rendered)
    else:
        Path(args.output).write_text(rendered, encoding="utf-8")
        sys.stdout.write(stable_json({"schema": REPORT_SCHEMA, "status": "ready", "output": args.output}) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
