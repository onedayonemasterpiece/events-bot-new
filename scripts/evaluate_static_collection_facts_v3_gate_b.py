#!/usr/bin/env python3
"""Pure offline Gate-B evaluator for Smart Update collection facts v3.

The evaluator never calls a model and opens SQLite in immutable read-only mode.
It binds a primary-only replay report to the corrected provisional seed, source
review index, exact repository revision and exact database bytes before it
computes occurrence-family-weighted recall.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
REPORT_SCHEMA = ROOT / "docs/review-data/static_collection_facts_v3_real_data_report.schema.json"
OUTPUT_SCHEMA = ROOT / "docs/review-data/static_collection_facts_v3_gate_b_report.schema.json"
OUTPUT_SCHEMA_VERSION = "static-collection-facts-v3-gate-b-report-v1"
TARGET_LABELS = {
    "child_directed": "child_directed_decision",
    "family_suitable": "family_suitable_decision",
    "joint_family_activity": "joint_family_activity_decision",
}
RUNTIME_OUTCOMES = {
    "confirmed",
    "unknown",
    "denied",
    "provider_deferred",
    "validator_reject",
}
REVIEW_CLASSIFICATIONS = {
    "match",
    "borderline_watch",
    "seed_conflict",
    "source_insufficient",
    "model_miss",
    "gross_false_positive",
}
SOURCE_REF_HASH_FIELDS = (
    "event_id",
    "source_id",
    "source_type",
    "source_url",
    "trust_level",
    "source_chat_username",
    "source_message_id",
    "source_text_sha256",
    "source_text_char_count",
)
SHA256_RE = re.compile(r"[0-9a-f]{64}")
GIT_SHA_RE = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?")
UTC_TIMESTAMP_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z")
SNAPSHOT_CONTRACT = {
    "schema_version": "static-collections-evidence-snapshot-v1",
    "serialization_contract": "canonical-json-v1",
    "query_contract": "event-review-source-v1",
    "encoding": "utf-8",
    "event_order": "id_ascending",
    "event_source_order": "id_ascending_per_event",
    "sqlite_json_columns": "preserved_as_stored_text",
    "artifact_path": (
        "artifacts/codex/static-collections-pr-a/"
        "static-collections-evidence-snapshot-v1.json"
    ),
}
SNAPSHOT_JSON_OPTIONS = {
    "allow_nan": False,
    "ensure_ascii": False,
    "separators": [",", ":"],
    "sort_keys": True,
    "trailing_newline": False,
}


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: top-level JSON value must be an object")
    return value


def _repo_sha(root: Path = ROOT) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        text=True,
        stderr=subprocess.DEVNULL,
    ).strip()


def _stamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _source_ref_hash(ref: Mapping[str, Any]) -> str:
    return _sha256_bytes(
        _canonical_bytes({field: ref.get(field) for field in SOURCE_REF_HASH_FIELDS})
    )


def _validate_json_schema(instance: Mapping[str, Any], schema_path: Path) -> None:
    import jsonschema

    schema = _load_object(schema_path)
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.validate(instance=dict(instance), schema=schema)


class Findings:
    def __init__(self) -> None:
        self.errors: list[dict[str, Any]] = []
        self.warnings: list[dict[str, Any]] = []

    def error(self, code: str, message: str, **context: Any) -> None:
        self.errors.append({"code": code, "message": message, **context})

    def warning(self, code: str, message: str, **context: Any) -> None:
        self.warnings.append({"code": code, "message": message, **context})


def _verify_seed_contract(seed: Mapping[str, Any], findings: Findings) -> None:
    if seed.get("schema_version") != "static-collections-review-seed-v1":
        findings.error("seed_schema_invalid", "corrected review seed v1 is required")
    source = seed.get("source") if isinstance(seed.get("source"), Mapping) else {}
    if "generator_repo_sha" in source:
        findings.error("generator_provenance_invalid", "legacy generator_repo_sha is ambiguous")
    for field in ("extraction_repo_sha", "seed_builder_repo_sha", "integration_repo_sha"):
        value = source.get(field)
        if not isinstance(value, str) or GIT_SHA_RE.fullmatch(value) is None:
            findings.error("generator_provenance_invalid", f"source.{field} is invalid")
    command = source.get("generator_command")
    if (
        not isinstance(command, str)
        or not command.startswith("python3 scripts/build_static_collections_review_seed.py ")
        or " --snapshot " not in command
        or " --output " not in command
    ):
        findings.error("generator_provenance_invalid", "generator_command is not reproducible")
    for field in ("extracted_at", "reviewed_at"):
        value = source.get(field)
        if not isinstance(value, str) or UTC_TIMESTAMP_RE.fullmatch(value) is None:
            findings.error("generator_provenance_invalid", f"source.{field} is invalid")
    contract = seed.get("snapshot_contract")
    invalid_snapshot = not isinstance(contract, Mapping)
    if isinstance(contract, Mapping):
        invalid_snapshot = (
            any(contract.get(field) != expected for field, expected in SNAPSHOT_CONTRACT.items())
            or contract.get("json_options") != SNAPSHOT_JSON_OPTIONS
            or any(
                not isinstance(contract.get(field), int)
                or isinstance(contract.get(field), bool)
                or contract.get(field) <= 0
                for field in ("event_count", "event_source_count")
            )
            or not isinstance(contract.get("db_file_mtime"), str)
            or UTC_TIMESTAMP_RE.fullmatch(str(contract.get("db_file_mtime"))) is None
        )
    if invalid_snapshot or not isinstance(seed.get("evidence_snapshot_sha256"), str) or SHA256_RE.fullmatch(str(seed.get("evidence_snapshot_sha256"))) is None:
        findings.error(
            "snapshot_contract_invalid",
            "canonical snapshot schema/serialization contract is missing or invalid",
        )


def _read_db_sources(
    db_path: Path,
    source_ids: Iterable[int],
    *,
    findings: Findings,
) -> tuple[dict[int, dict[str, Any]], set[int], str]:
    uri = f"file:{db_path.resolve().as_posix()}?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as connection:
        connection.row_factory = sqlite3.Row
        quick_rows = connection.execute("PRAGMA quick_check").fetchall()
        quick_check = "\n".join(str(row[0]) for row in quick_rows)
        if quick_check != "ok":
            findings.error("db_quick_check_failed", f"PRAGMA quick_check={quick_check}")
        event_ids = {
            int(row[0]) for row in connection.execute("SELECT id FROM event").fetchall()
        }
        ids = sorted({int(value) for value in source_ids if int(value) > 0})
        sources: dict[int, dict[str, Any]] = {}
        for offset in range(0, len(ids), 500):
            chunk = ids[offset : offset + 500]
            placeholders = ",".join("?" for _ in chunk)
            rows = connection.execute(
                f"SELECT * FROM event_source WHERE id IN ({placeholders})", chunk
            ).fetchall()
            sources.update({int(row["id"]): dict(row) for row in rows})
    return sources, event_ids, quick_check


def _verify_source_ref(
    ref: Mapping[str, Any],
    *,
    sources: Mapping[int, Mapping[str, Any]],
    event_ids: set[int],
    findings: Findings,
    context: str,
) -> str | None:
    event_id = ref.get("event_id")
    source_id = ref.get("source_id")
    if not isinstance(event_id, int) or event_id not in event_ids:
        findings.error("event_missing", "referenced event is absent from SQLite", context=context, event_id=event_id)
        return None
    if not isinstance(source_id, int) or source_id not in sources:
        findings.error("source_missing", "referenced EventSource is absent from SQLite", context=context, source_id=source_id)
        return None
    source = sources[source_id]
    if int(source.get("event_id") or 0) != event_id:
        findings.error(
            "source_event_mismatch",
            "EventSource belongs to another event",
            context=context,
            event_id=event_id,
            source_id=source_id,
        )
    for ref_field, db_field in (
        ("source_type", "source_type"),
        ("source_url", "source_url"),
        ("trust_level", "trust_level"),
        ("source_chat_username", "source_chat_username"),
        ("source_message_id", "source_message_id"),
    ):
        if ref.get(ref_field) != source.get(db_field):
            findings.error(
                "source_metadata_mismatch",
                f"{ref_field} differs from SQLite",
                context=context,
                event_id=event_id,
                source_id=source_id,
            )
    source_text = source.get("source_text")
    if not isinstance(source_text, str):
        findings.error("source_text_missing", "EventSource.source_text is empty", context=context, source_id=source_id)
        return None
    actual_text_hash = _sha256_bytes(source_text.encode("utf-8"))
    if ref.get("source_text_sha256") != actual_text_hash:
        findings.error("source_text_hash_mismatch", "source text SHA-256 differs from SQLite", context=context, source_id=source_id)
    if ref.get("source_text_char_count") != len(source_text):
        findings.error("source_text_length_mismatch", "source text length differs from SQLite", context=context, source_id=source_id)
    if ref.get("source_ref_sha256") != _source_ref_hash(ref):
        findings.error("source_ref_hash_mismatch", "canonical source_ref SHA-256 is invalid", context=context, source_id=source_id)
    record_hash = ref.get("source_record_sha256")
    if not isinstance(record_hash, str) or re.fullmatch(r"[0-9a-f]{64}", record_hash) is None:
        findings.error("source_record_hash_invalid", "source_record_sha256 is not SHA-256", context=context, source_id=source_id)
    return source_text


def _verify_quote(
    evidence: Mapping[str, Any],
    *,
    prefix: str,
    source_text: str,
    findings: Findings,
    context: str,
) -> None:
    quote = evidence.get(f"{prefix}quote")
    start = evidence.get(f"{prefix}quote_start_char")
    end = evidence.get(f"{prefix}quote_end_char")
    if not isinstance(quote, str) or not isinstance(start, int) or not isinstance(end, int):
        findings.error("quote_contract_missing", "quote/offset contract is incomplete", context=context)
        return
    if start < 0 or end < start or end > len(source_text):
        findings.error("quote_offsets_invalid", "quote offsets are outside source text", context=context)
        return
    if source_text[start:end] != quote or quote not in source_text:
        findings.error("quote_source_mismatch", "quote is not the declared exact SQLite substring", context=context)
    if evidence.get(f"{prefix}quote_sha256") != _sha256_bytes(quote.encode("utf-8")):
        findings.error("quote_hash_mismatch", "quote SHA-256 is invalid", context=context)
    kind = evidence.get(f"{prefix}quote_kind")
    truncated = evidence.get(f"{prefix}quote_truncated")
    char_count = evidence.get(f"{prefix}quote_char_count")
    omitted_prefix = evidence.get(f"{prefix}quote_omitted_prefix_chars")
    omitted_suffix = evidence.get(f"{prefix}quote_omitted_suffix_chars")
    expected_kind = "full" if start == 0 and end == len(source_text) else "excerpt"
    expected_truncated = expected_kind == "excerpt"
    if (
        kind != expected_kind
        or truncated is not expected_truncated
        or char_count != len(quote)
        or omitted_prefix != start
        or omitted_suffix != len(source_text) - end
    ):
        findings.error(
            "quote_serialization_mismatch",
            "quote kind/truncation/count metadata differs from exact offsets",
            context=context,
        )


def _load_index_receipts(
    index: Mapping[str, Any],
    index_path: Path,
    *,
    findings: Findings,
) -> list[tuple[Mapping[str, Any], Path, dict[str, Any]]]:
    loaded: list[tuple[Mapping[str, Any], Path, dict[str, Any]]] = []
    root = index_path.parent.resolve()
    for entry in index.get("receipts") or []:
        if not isinstance(entry, Mapping):
            findings.error("receipt_index_invalid", "receipt index entry is not an object")
            continue
        path = (index_path.parent / str(entry.get("path") or "")).resolve()
        if root not in path.parents or not path.is_file():
            findings.error(
                "receipt_path_invalid",
                "receipt path is missing or escapes index directory",
                path=str(path),
            )
            continue
        try:
            receipt = _load_object(path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            findings.error(
                "receipt_json_invalid",
                f"receipt cannot be loaded: {exc}",
                path=str(path),
            )
            continue
        loaded.append((entry, path, receipt))
    return loaded


def _verify_index(
    index: Mapping[str, Any],
    index_path: Path,
    *,
    sources: Mapping[int, Mapping[str, Any]],
    event_ids: set[int],
    receipts: Sequence[tuple[Mapping[str, Any], Path, Mapping[str, Any]]],
    findings: Findings,
) -> None:
    if index.get("schema_version") != "static-collection-source-review-index-v1":
        findings.error("index_schema_invalid", "source-review index v1 is required")
    if not isinstance(index.get("source_snapshot_sha256"), str) or SHA256_RE.fullmatch(str(index.get("source_snapshot_sha256"))) is None:
        findings.error("index_snapshot_hash_invalid", "index source snapshot SHA-256 is invalid")
    unhashed = dict(index)
    declared_index_hash = unhashed.pop("index_sha256", None)
    actual_index_hash = _sha256_bytes(_canonical_bytes(unhashed))
    if declared_index_hash != actual_index_hash:
        findings.error("index_hash_mismatch", "source-review index canonical hash is invalid")
    indexed_event_ids: set[int] = set()
    for entry, _path, receipt in receipts:
        if receipt.get("schema_version") != "static-collection-source-review-v1":
            findings.error(
                "receipt_schema_invalid",
                "source-review receipt v1 is required",
                receipt_id=entry.get("receipt_id"),
            )
        unhashed_receipt = dict(receipt)
        declared_receipt_hash = unhashed_receipt.pop("receipt_sha256", None)
        actual_receipt_hash = _sha256_bytes(_canonical_bytes(unhashed_receipt))
        if declared_receipt_hash != actual_receipt_hash or entry.get("receipt_sha256") != actual_receipt_hash:
            findings.error("receipt_hash_mismatch", "receipt canonical hash is invalid", receipt_id=entry.get("receipt_id"))
        if entry.get("receipt_id") != receipt.get("receipt_id") or entry.get("status") != receipt.get("status"):
            findings.error("receipt_index_mismatch", "receipt id/status differs from index", receipt_id=entry.get("receipt_id"))
        if entry.get("event_ids") != receipt.get("event_ids"):
            findings.error("receipt_event_ids_mismatch", "receipt event_ids differ from index", receipt_id=entry.get("receipt_id"))
        indexed_event_ids.update(
            int(event_id)
            for event_id in entry.get("event_ids") or []
            if isinstance(event_id, int)
        )
        for position, evidence in enumerate(receipt.get("source_evidence") or []):
            if not isinstance(evidence, Mapping) or not isinstance(evidence.get("source_ref"), Mapping):
                findings.error("receipt_evidence_invalid", "receipt evidence/source_ref is invalid", receipt_id=entry.get("receipt_id"))
                continue
            ref = evidence["source_ref"]
            context = f"receipt:{entry.get('receipt_id')}:{position}"
            source_text = _verify_source_ref(
                ref,
                sources=sources,
                event_ids=event_ids,
                findings=findings,
                context=context,
            )
            if source_text is not None:
                _verify_quote(evidence, prefix="raw_source_", source_text=source_text, findings=findings, context=context)
    required_event_ids = {
        int(event_id)
        for event_id in index.get("required_event_ids") or []
        if isinstance(event_id, int)
    }
    if required_event_ids != indexed_event_ids:
        findings.error(
            "index_required_event_ids_mismatch",
            "index.required_event_ids differs from the union of indexed receipt event_ids",
            missing=sorted(indexed_event_ids - required_event_ids),
            extra=sorted(required_event_ids - indexed_event_ids),
        )


def _source_execution_rows(report: Mapping[str, Any]) -> dict[tuple[int, int], Mapping[str, Any]]:
    result: dict[tuple[int, int], Mapping[str, Any]] = {}
    execution = report.get("execution")
    if not isinstance(execution, Mapping):
        return result
    for event in execution.get("events") or []:
        if not isinstance(event, Mapping) or not isinstance(event.get("event_id"), int):
            continue
        event_id = int(event["event_id"])
        for source in event.get("sources") or []:
            if isinstance(source, Mapping) and isinstance(source.get("source_id"), int):
                result[(event_id, int(source["source_id"]))] = source
    return result


def _runtime_outcome(source: Mapping[str, Any] | None, decision_key: str) -> str:
    if not isinstance(source, Mapping):
        return "provider_deferred"
    outcomes = source.get("validated_outcomes")
    if isinstance(outcomes, Mapping) and isinstance(outcomes.get(decision_key), Mapping):
        value = str(outcomes[decision_key].get("value") or "unknown")
        return value if value in {"confirmed", "unknown", "denied"} else "validator_reject"
    trace = source.get("trace") if isinstance(source.get("trace"), Mapping) else {}
    statuses = [str(value).casefold() for value in (trace.get("statuses") or [])]
    if source.get("status") == "deferred" and any(
        status.startswith("ok") for status in statuses
    ):
        return "validator_reject"
    return "provider_deferred"


def _aggregate_outcome(outcomes: Sequence[str]) -> str:
    for value in ("confirmed", "denied", "unknown", "validator_reject", "provider_deferred"):
        if value in outcomes:
            return value
    return "provider_deferred"


def _iter_target_rows(seed: Mapping[str, Any]) -> Iterable[tuple[str, str, Mapping[str, Any]]]:
    labels = seed.get("labels") if isinstance(seed.get("labels"), Mapping) else {}
    for label in TARGET_LABELS:
        payload = labels.get(label) if isinstance(labels.get(label), Mapping) else {}
        for side in ("positives", "hard_negatives"):
            for row in payload.get(side) or []:
                if isinstance(row, Mapping):
                    yield label, side, row


def _cohort(seed: Mapping[str, Any]) -> tuple[set[int], set[tuple[int, int]], set[int]]:
    event_ids: set[int] = set()
    bindings: set[tuple[int, int]] = set()
    source_ids: set[int] = set()
    for _label, _side, row in _iter_target_rows(seed):
        event_id = row.get("event_id")
        if isinstance(event_id, int):
            event_ids.add(event_id)
        for member in row.get("occurrence_member_ids") or []:
            if isinstance(member, int):
                event_ids.add(member)
        for ref in row.get("source_refs") or []:
            if isinstance(ref, Mapping) and isinstance(ref.get("event_id"), int) and isinstance(ref.get("source_id"), int):
                binding = (int(ref["event_id"]), int(ref["source_id"]))
                bindings.add(binding)
                source_ids.add(binding[1])
    return event_ids, bindings, source_ids


def _validate_report_safety(
    report: Mapping[str, Any],
    *,
    expected_repo_sha: str,
    current_repo_sha: str,
    db_sha256: str,
    cohort_event_ids: set[int],
    cohort_bindings: set[tuple[int, int]],
    runtime_rows: Mapping[tuple[int, int], Mapping[str, Any]],
    sources: Mapping[int, Mapping[str, Any]],
    findings: Findings,
) -> None:
    if report.get("repo_sha") != expected_repo_sha or current_repo_sha != expected_repo_sha:
        findings.error(
            "repo_sha_mismatch",
            "report, expected SHA and current checkout must be identical",
            report_repo_sha=report.get("repo_sha"),
            expected_repo_sha=expected_repo_sha,
            current_repo_sha=current_repo_sha,
        )
    if report.get("mode") != "evaluate" or report.get("primary_only") is not True:
        findings.error("report_mode_invalid", "Gate B requires evaluate + primary_only")
    snapshot = report.get("db_snapshot") if isinstance(report.get("db_snapshot"), Mapping) else {}
    if snapshot.get("sha256_before") != db_sha256 or snapshot.get("sha256_after") != db_sha256:
        findings.error("db_hash_mismatch", "report DB hashes do not match the read-only SQLite bytes")
    execution = report.get("execution") if isinstance(report.get("execution"), Mapping) else {}
    logical = report.get("logical_diff") if isinstance(report.get("logical_diff"), Mapping) else {}
    if int(execution.get("writes") or 0) != 0:
        findings.error("writes_nonzero", "Gate B evaluate report contains writes")
    if execution.get("physical_sends_complete") is not True:
        findings.error("physical_sends_incomplete", "Gate B requires exact physical send counts")
    if (
        logical.get("sha256_before") != logical.get("sha256_after")
        or logical.get("changed_event_ids")
        or logical.get("changed_event_source_ids")
        or logical.get("selected_event_allowlist_ok") is not True
    ):
        findings.error("logical_db_changed", "Gate B report records logical database changes")
    selection = report.get("selection") if isinstance(report.get("selection"), Mapping) else {}
    reported_bindings = {
        (int(item.get("event_id") or 0), int(item.get("source_id") or 0))
        for item in selection.get("requested_source_bindings") or []
        if isinstance(item, Mapping)
    }
    if reported_bindings != cohort_bindings:
        findings.error(
            "stale_seed_cohort",
            "report source cohort does not exactly match corrected seed",
            missing=sorted(cohort_bindings - reported_bindings),
            extra=sorted(reported_bindings - cohort_bindings),
        )
    requested_source_ids = {int(value) for value in selection.get("requested_source_ids") or []}
    binding_source_ids = {source_id for _event_id, source_id in cohort_bindings}
    if requested_source_ids != binding_source_ids:
        findings.error(
            "stale_source_cohort",
            "report requested_source_ids do not match corrected seed bindings",
            missing=sorted(binding_source_ids - requested_source_ids),
            extra=sorted(requested_source_ids - binding_source_ids),
        )
    requested_events = {int(value) for value in selection.get("requested_event_ids") or []}
    # Occurrence members may be included for family coverage, but every seed
    # evidence event must be requested and no unrelated event may enter.
    binding_event_ids = {event_id for event_id, _source_id in cohort_bindings}
    if requested_events != binding_event_ids:
        findings.error(
            "stale_event_cohort",
            "report event cohort does not exactly match corrected seed evidence events",
            missing=sorted(binding_event_ids - requested_events),
            extra=sorted(requested_events - binding_event_ids),
        )
    if set(runtime_rows) != cohort_bindings:
        findings.error(
            "execution_cohort_mismatch",
            "execution rows do not exactly match requested seed source bindings",
            missing=sorted(cohort_bindings - set(runtime_rows)),
            extra=sorted(set(runtime_rows) - cohort_bindings),
        )
    execution_binding_count = sum(
        1
        for event in execution.get("events") or []
        if isinstance(event, Mapping)
        for source in event.get("sources") or []
        if isinstance(source, Mapping)
    )
    if execution_binding_count != len(runtime_rows):
        findings.error(
            "duplicate_execution_binding",
            "execution contains duplicate or invalid event/source rows",
        )
    for (event_id, source_id), source in runtime_rows.items():
        trace = source.get("trace") if isinstance(source.get("trace"), Mapping) else {}
        if source.get("provider_called") is not True or trace.get("logical_calls") != 1:
            findings.error("logical_call_invalid", "each replay source must make exactly one logical provider call", event_id=event_id, source_id=source_id)
        physical = trace.get("physical_sends")
        if not isinstance(physical, int) or physical > 1:
            findings.error("physical_send_overrun", "physical sends must be known and <=1", event_id=event_id, source_id=source_id)
        if trace.get("fallback_used") is not False or "4o" in str(trace.get("actual_model_path") or "").casefold():
            findings.error("fallback_used", "primary-only report used fallback", event_id=event_id, source_id=source_id)
        if source.get("write_status") not in {"not_requested", "none"} or source.get("changed_keys"):
            findings.error("source_write_detected", "evaluate source row records a write/diff", event_id=event_id, source_id=source_id)
        db_source = sources.get(source_id)
        if db_source is None or int(db_source.get("event_id") or 0) != event_id:
            findings.error("runtime_source_event_mismatch", "runtime source/event binding differs from SQLite", event_id=event_id, source_id=source_id)
            continue
        source_text = str(db_source.get("source_text") or "")
        outcomes = source.get("validated_outcomes")
        if isinstance(outcomes, Mapping):
            for label, decision_key in TARGET_LABELS.items():
                decision = outcomes.get(decision_key)
                if not isinstance(decision, Mapping):
                    findings.error("runtime_decision_missing", "runtime outcome misses a required fact", label=label, event_id=event_id, source_id=source_id)
                    continue
                value = decision.get("value")
                quote = decision.get("evidence_quote")
                if value not in {"confirmed", "denied", "unknown"}:
                    findings.error(
                        "runtime_decision_invalid",
                        "runtime outcome is outside the facts-v3 enum",
                        label=label,
                        event_id=event_id,
                        source_id=source_id,
                    )
                if value in {"confirmed", "denied"} and (
                    not isinstance(quote, str) or not quote or quote not in source_text
                ):
                    findings.error("runtime_quote_mismatch", "non-unknown runtime quote is not an exact DB substring", label=label, event_id=event_id, source_id=source_id)
            joint = outcomes.get("joint_family_activity_decision")
            if isinstance(joint, Mapping) and joint.get("value") == "confirmed":
                child = outcomes.get("child_directed_decision")
                family = outcomes.get("family_suitable_decision")
                if not (
                    isinstance(child, Mapping)
                    and child.get("value") == "confirmed"
                    and isinstance(family, Mapping)
                    and family.get("value") == "confirmed"
                ):
                    findings.error("impossible_joint", "joint confirmed without child+family confirmed", event_id=event_id, source_id=source_id)
    logical_calls = sum(
        int(source.get("trace", {}).get("logical_calls") or 0)
        for source in runtime_rows.values()
        if isinstance(source.get("trace"), Mapping)
    )
    physical_sends = sum(
        int(source.get("trace", {}).get("physical_sends") or 0)
        for source in runtime_rows.values()
        if isinstance(source.get("trace"), Mapping)
    )
    if execution.get("provider_calls") != logical_calls:
        findings.error("provider_call_total_mismatch", "execution.provider_calls differs from source traces")
    if execution.get("physical_sends") != physical_sends:
        findings.error("physical_send_total_mismatch", "execution.physical_sends differs from source traces")
    if execution.get("attempted_sources") != len(runtime_rows):
        findings.error(
            "attempted_source_total_mismatch",
            "execution.attempted_sources differs from the exact replay cohort",
        )


def _evaluate_labels(
    seed: Mapping[str, Any],
    *,
    runtime_rows: Mapping[tuple[int, int], Mapping[str, Any]],
    minimum_recall: float,
    findings: Findings,
) -> dict[str, Any]:
    labels_out: dict[str, Any] = {}
    grouped: dict[tuple[str, str], dict[str, list[Mapping[str, Any]]]] = defaultdict(
        lambda: {"positives": [], "hard_negatives": []}
    )
    for label, side, row in _iter_target_rows(seed):
        family_id = str(row.get("family_id") or f"event:{row.get('event_id')}")
        grouped[(label, family_id)][side].append(row)

    for label, decision_key in TARGET_LABELS.items():
        family_results: list[dict[str, Any]] = []
        for (group_label, family_id), sides in sorted(grouped.items()):
            if group_label != label:
                continue
            positive_rows = sides["positives"]
            negative_rows = sides["hard_negatives"]
            rows = [*positive_rows, *negative_rows]
            side = "seed_conflict" if positive_rows and negative_rows else (
                "positive" if positive_rows else "hard_negative"
            )
            refs = [
                ref
                for row in rows
                for ref in (row.get("source_refs") or [])
                if isinstance(ref, Mapping)
            ]
            source_bindings = sorted(
                {
                    (int(ref.get("event_id") or 0), int(ref.get("source_id") or 0))
                    for ref in refs
                }
            )
            outcomes = [
                _runtime_outcome(runtime_rows.get(binding), decision_key)
                for binding in source_bindings
            ]
            runtime_outcome = _aggregate_outcome(outcomes)
            source_statuses = {str(row.get("source_status") or "") for row in rows}
            confidences = {str(row.get("confidence") or "") for row in rows}
            review_decisions = {str(row.get("review_decision") or "") for row in rows}
            eligible = bool(
                side == "positive"
                and any(
                    row.get("confidence") == "high"
                    and row.get("review_decision") == "keep"
                    and row.get("source_status") == "sufficient"
                    for row in positive_rows
                )
            )
            if side == "seed_conflict" or (positive_rows and "keep" not in review_decisions):
                classification = "seed_conflict"
            elif "sufficient" not in source_statuses:
                classification = "source_insufficient"
            elif positive_rows and not eligible:
                classification = "borderline_watch"
            elif negative_rows and runtime_outcome == "confirmed":
                classification = "gross_false_positive"
            elif positive_rows and runtime_outcome != "confirmed":
                classification = "model_miss"
            else:
                classification = "match"
            if classification == "gross_false_positive":
                findings.error(
                    "hard_negative_confirmed",
                    "model confirmed a corrected-seed hard negative",
                    label=label,
                    family_id=family_id,
                )
            elif classification == "seed_conflict":
                findings.error(
                    "seed_family_conflict",
                    "family appears on incompatible corrected-seed sides",
                    label=label,
                    family_id=family_id,
                )
            elif classification in {"borderline_watch", "source_insufficient"}:
                findings.warning(
                    classification,
                    "family is excluded from the hard recall denominator",
                    label=label,
                    family_id=family_id,
                )
            family_results.append(
                {
                    "family_id": family_id,
                    "side": side,
                    "event_ids": sorted(
                        {
                            int(value)
                            for row in rows
                            for value in ([row.get("event_id")] + list(row.get("occurrence_member_ids") or []))
                            if isinstance(value, int)
                        }
                    ),
                    "source_ids": sorted({source_id for _event_id, source_id in source_bindings}),
                    "confidence": sorted(confidences),
                    "review_decision": sorted(review_decisions),
                    "source_status": sorted(source_statuses),
                    "eligible_hard_positive": eligible,
                    "runtime_outcome": runtime_outcome,
                    "review_classification": classification,
                }
            )
        eligible_rows = [row for row in family_results if row["eligible_hard_positive"]]
        hits = sum(row["runtime_outcome"] == "confirmed" for row in eligible_rows)
        recall = hits / len(eligible_rows) if eligible_rows else None
        threshold_pass = recall is not None and recall >= minimum_recall
        if not eligible_rows:
            findings.error("eligible_positive_supply_empty", "label has no eligible hard positives", label=label)
        elif not threshold_pass:
            findings.error(
                "recall_below_minimum",
                f"eligible family recall {recall:.3f} < {minimum_recall:.3f}",
                label=label,
            )
        labels_out[label] = {
            "decision_key": decision_key,
            "eligible_positive_families": len(eligible_rows),
            "confirmed_eligible_families": hits,
            "recall": recall,
            "minimum_recall": minimum_recall,
            "threshold_pass": threshold_pass,
            "hard_negative_families": sum(row["side"] == "hard_negative" for row in family_results),
            "confirmed_hard_negative_families": sum(
                row["side"] == "hard_negative" and row["runtime_outcome"] == "confirmed"
                for row in family_results
            ),
            "runtime_outcomes": dict(sorted(Counter(row["runtime_outcome"] for row in family_results).items())),
            "review_classifications": dict(sorted(Counter(row["review_classification"] for row in family_results).items())),
            "families": family_results,
        }
    return labels_out


def evaluate_gate_b(
    *,
    report_path: Path,
    seed_path: Path,
    source_review_index_path: Path,
    db_path: Path,
    minimum_recall: float = 0.80,
    expected_repo_sha: str,
    repo_root: Path = ROOT,
) -> dict[str, Any]:
    if not 0 <= minimum_recall <= 1:
        raise ValueError("minimum_recall must be between 0 and 1")
    report = _load_object(report_path)
    seed = _load_object(seed_path)
    index = _load_object(source_review_index_path)
    _validate_json_schema(report, REPORT_SCHEMA)
    findings = Findings()
    current_repo_sha = _repo_sha(repo_root)
    report_hash = _sha256_file(report_path)
    seed_hash = _sha256_file(seed_path)
    index_file_hash = _sha256_file(source_review_index_path)
    db_hash = _sha256_file(db_path)

    if seed.get("publication_eligible") is not False:
        findings.error("seed_publication_boundary", "corrected provisional seed must remain publication_eligible=false")
    _verify_seed_contract(seed, findings)
    if seed.get("evidence_snapshot_sha256") != index.get("source_snapshot_sha256"):
        findings.error("seed_index_snapshot_mismatch", "seed and index snapshot hashes differ")
    seed_index_hash = (
        seed.get("source", {}).get("source_review_index_sha256")
        if isinstance(seed.get("source"), Mapping)
        else None
    )
    if seed_index_hash != index.get("index_sha256"):
        findings.error("seed_index_hash_mismatch", "seed does not bind the supplied source-review index")

    cohort_event_ids, cohort_bindings, seed_source_ids = _cohort(seed)
    receipts = _load_index_receipts(index, source_review_index_path, findings=findings)
    index_source_ids = {
        int(evidence["source_ref"]["source_id"])
        for _entry, _path, receipt in receipts
        for evidence in receipt.get("source_evidence") or []
        if isinstance(evidence, Mapping)
        and isinstance(evidence.get("source_ref"), Mapping)
        and isinstance(evidence["source_ref"].get("source_id"), int)
    }
    sources, db_event_ids, quick_check = _read_db_sources(
        db_path,
        seed_source_ids | index_source_ids,
        findings=findings,
    )
    for label, side, row in _iter_target_rows(seed):
        for position, ref in enumerate(row.get("source_refs") or []):
            if not isinstance(ref, Mapping):
                findings.error("seed_source_ref_invalid", "seed source_ref is invalid", label=label, side=side)
                continue
            context = f"seed:{label}:{side}:{row.get('family_id')}:{position}"
            source_text = _verify_source_ref(
                ref,
                sources=sources,
                event_ids=db_event_ids,
                findings=findings,
                context=context,
            )
            if source_text is not None:
                _verify_quote(row, prefix="source_", source_text=source_text, findings=findings, context=context)
    _verify_index(
        index,
        source_review_index_path,
        sources=sources,
        event_ids=db_event_ids,
        receipts=receipts,
        findings=findings,
    )

    runtime_rows = _source_execution_rows(report)
    _validate_report_safety(
        report,
        expected_repo_sha=expected_repo_sha,
        current_repo_sha=current_repo_sha,
        db_sha256=db_hash,
        cohort_event_ids=cohort_event_ids,
        cohort_bindings=cohort_bindings,
        runtime_rows=runtime_rows,
        sources=sources,
        findings=findings,
    )
    labels = _evaluate_labels(
        seed,
        runtime_rows=runtime_rows,
        minimum_recall=minimum_recall,
        findings=findings,
    )
    result = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "generated_at": _stamp(),
        "status": "blocked" if findings.errors else "pass",
        "copy_gates_allowed": not findings.errors,
        "publication_status": "blocked",
        "minimum_recall": minimum_recall,
        "hashes": {
            "report_file_sha256": report_hash,
            "seed_file_sha256": seed_hash,
            "source_review_index_file_sha256": index_file_hash,
            "db_file_sha256": db_hash,
            "report_repo_sha": str(report.get("repo_sha") or ""),
            "expected_repo_sha": expected_repo_sha,
            "current_repo_sha": current_repo_sha,
            "seed_source_review_index_sha256": str(seed_index_hash or ""),
            "index_sha256": str(index.get("index_sha256") or ""),
            "seed_source_snapshot_sha256": str(seed.get("evidence_snapshot_sha256") or ""),
            "index_source_snapshot_sha256": str(index.get("source_snapshot_sha256") or ""),
        },
        "db_quick_check": quick_check,
        "cohort": {
            "event_ids": sorted({event_id for event_id, _source_id in cohort_bindings}),
            "source_bindings": [
                {"event_id": event_id, "source_id": source_id}
                for event_id, source_id in sorted(cohort_bindings)
            ],
        },
        "labels": labels,
        "errors": findings.errors,
        "warnings": findings.warnings,
    }
    _validate_json_schema(result, OUTPUT_SCHEMA)
    return result


def render_markdown(result: Mapping[str, Any]) -> str:
    lines = [
        "# Static-collection facts v3 — Gate B",
        "",
        f"- Status: **{str(result.get('status') or '').upper()}**",
        f"- Copy gates allowed: **{str(bool(result.get('copy_gates_allowed'))).lower()}**",
        "- Semantic publication: **BLOCKED**",
        f"- Minimum hard-positive family recall: `{float(result.get('minimum_recall') or 0):.2f}`",
        f"- Errors: `{len(result.get('errors') or [])}`",
        f"- Warnings: `{len(result.get('warnings') or [])}`",
        "",
        "## Labels",
        "",
        "| Label | Eligible | Confirmed | Recall | Hard-negative confirmed | Gate |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for label, payload in (result.get("labels") or {}).items():
        recall = payload.get("recall")
        recall_text = "n/a" if recall is None else f"{float(recall):.3f}"
        lines.append(
            f"| `{label}` | {payload.get('eligible_positive_families')} | "
            f"{payload.get('confirmed_eligible_families')} | {recall_text} | "
            f"{payload.get('confirmed_hard_negative_families')} | "
            f"{'PASS' if payload.get('threshold_pass') else 'BLOCKED'} |"
        )
    if result.get("errors"):
        lines.extend(["", "## Errors", ""])
        for issue in result["errors"]:
            lines.append(f"- `{issue.get('code')}` — {issue.get('message')}")
    lines.append("")
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--seed", type=Path, required=True)
    parser.add_argument("--source-review-index", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--minimum-recall", type=float, default=0.80)
    parser.add_argument("--expected-repo-sha", required=True)
    parser.add_argument("--json-output", type=Path, required=True)
    parser.add_argument("--markdown-output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    result = evaluate_gate_b(
        report_path=args.report,
        seed_path=args.seed,
        source_review_index_path=args.source_review_index,
        db_path=args.db,
        minimum_recall=args.minimum_recall,
        expected_repo_sha=args.expected_repo_sha,
    )
    args.json_output.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
    args.json_output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    args.markdown_output.write_text(render_markdown(result), encoding="utf-8")
    print(render_markdown(result), end="")
    return 0 if result["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
