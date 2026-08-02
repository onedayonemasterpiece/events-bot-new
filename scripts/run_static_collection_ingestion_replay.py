#!/usr/bin/env python3
"""Run bounded facts-v3 replays through the ordinary ingestion adapters.

The harness is intentionally an acceptance tool, not another ingestion path.
It invokes the production Telegram, VK and official-parser entry points against
an explicitly acknowledged mutable SQLite *copy*.  Publication/task side
effects are disabled, while Smart Update and its collection adjudication remain
real.  Every case is then replayed a second time to prove warm idempotency.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import dataclasses
import hashlib
import importlib
import json
import os
import shlex
import sqlite3
import sys
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database


MANIFEST_SCHEMA_VERSION = "static-collection-ingestion-replay-manifest-v1"
REPORT_SCHEMA_VERSION = "static-collection-ingestion-replay-report-v1"
FACTS_POLICY_VERSION = "static-collection-facts-v3"
FACT_KEYS = (
    "child_directed_decision",
    "family_suitable_decision",
    "joint_family_activity_decision",
)
MAX_CASES = 24
TRACE_SAFE_KEYS = {
    "kind",
    "label",
    "model",
    "requested_model",
    "actual_model",
    "actual_models",
    "provider_path",
    "fallback_used",
    "attempts",
    "physical_sends",
    "provider_errors",
    "rate_limit_waits",
    "input_tokens",
    "output_tokens",
    "token_usage",
    "duration_sec",
    "status",
}
_CONTENT_KEY_MARKERS = (
    "description",
    "excerpt",
    "payload",
    "prompt",
    "raw",
    "response",
    "source_text",
    "title",
)
_SECRET_KEY_MARKERS = ("api_key", "authorization", "cookie", "password", "secret", "token")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _jsonable(value: Any) -> Any:
    if dataclasses.is_dataclass(value):
        return _jsonable(dataclasses.asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bytes):
        return {"sha256": hashlib.sha256(value).hexdigest(), "byte_count": len(value)}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _redacted_value(value: Any) -> dict[str, Any]:
    encoded = _canonical_bytes(_jsonable(value))
    return {
        "redacted": True,
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "byte_count": len(encoded),
    }


def _safe_adapter_result(value: Any, *, key: str = "") -> Any:
    """Keep adapter accounting while excluding source copy, URLs and secrets."""

    normalized_key = key.strip().lower()
    if any(marker in normalized_key for marker in _SECRET_KEY_MARKERS):
        return {"redacted": True}
    if any(marker in normalized_key for marker in _CONTENT_KEY_MARKERS):
        return _redacted_value(value)
    if "url" in normalized_key or "link" in normalized_key:
        return _redacted_value(value)
    if dataclasses.is_dataclass(value):
        return _safe_adapter_result(dataclasses.asdict(value), key=key)
    if isinstance(value, Mapping):
        return {
            str(child_key): _safe_adapter_result(child_value, key=str(child_key))
            for child_key, child_value in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_safe_adapter_result(item, key=key) for item in value]
    if isinstance(value, str) and (
        value.startswith("http://") or value.startswith("https://")
    ):
        return _redacted_value(value)
    return _jsonable(value)


def _load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: top-level JSON must be an object")
    return value


def _resolve_fixture(manifest_path: Path, raw: Any) -> Path:
    value = str(raw or "").strip()
    if not value:
        raise ValueError("fixture_path is required")
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = manifest_path.parent / candidate
    candidate = candidate.resolve()
    if not candidate.is_file():
        raise ValueError(f"fixture does not exist: {candidate}")
    return candidate


def validate_manifest(manifest: Mapping[str, Any], *, manifest_path: Path) -> list[dict[str, Any]]:
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise ValueError(f"manifest schema_version must be {MANIFEST_SCHEMA_VERSION}")
    if not isinstance(manifest.get("run_id"), str) or not str(manifest["run_id"]).strip():
        raise ValueError("manifest run_id must be a non-empty string")
    expected_logical_sha = manifest.get("db_logical_sha256_before")
    if expected_logical_sha is not None and (
        not isinstance(expected_logical_sha, str)
        or len(expected_logical_sha) != 64
        or any(char not in "0123456789abcdef" for char in expected_logical_sha)
    ):
        raise ValueError("db_logical_sha256_before must be lowercase SHA-256 when supplied")
    cases = manifest.get("cases")
    if not isinstance(cases, list) or not cases or len(cases) > MAX_CASES:
        raise ValueError(f"manifest cases must contain 1..{MAX_CASES} rows")
    seen: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(cases):
        if not isinstance(raw, Mapping):
            raise ValueError(f"case #{index + 1} must be an object")
        case_id = str(raw.get("case_id") or "").strip()
        adapter = str(raw.get("adapter") or "").strip().lower()
        expected = raw.get("expected")
        options = raw.get("adapter_options") or {}
        if not case_id or case_id in seen:
            raise ValueError(f"case #{index + 1} has missing/duplicate case_id")
        if adapter not in {"telegram", "vk", "parser"}:
            raise ValueError(f"case {case_id}: unsupported adapter {adapter!r}")
        if not isinstance(expected, Mapping) or not isinstance(options, Mapping):
            raise ValueError(f"case {case_id}: expected/adapter_options must be objects")
        try:
            expected_event_id = int(expected.get("event_id"))
        except (TypeError, ValueError, OverflowError):
            expected_event_id = 0
        source_url = str(expected.get("source_url") or "").strip()
        source_type = str(expected.get("source_type") or "").strip()
        if expected_event_id <= 0 or not source_url or not source_type:
            raise ValueError(
                f"case {case_id}: positive expected.event_id/source_url/source_type are required"
            )
        expected_source_id = expected.get("source_id")
        if expected_source_id is not None:
            try:
                expected_source_id = int(expected_source_id)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"case {case_id}: expected.source_id must be positive") from exc
            if expected_source_id <= 0:
                raise ValueError(f"case {case_id}: expected.source_id must be positive")
        for field, default in (
            ("first_collection_calls", None),
            ("warm_collection_calls", 0),
        ):
            value = expected.get(field, default)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > 1:
                raise ValueError(f"case {case_id}: expected.{field} must be 0 or 1")
        for field in ("first_collection_write", "warm_collection_write"):
            default = False if field == "warm_collection_write" else None
            value = expected.get(field, default)
            if not isinstance(value, bool):
                raise ValueError(f"case {case_id}: expected.{field} must be boolean")
        if adapter == "telegram":
            username = str(options.get("source_username") or "").strip().lstrip("@").lower()
            try:
                message_id = int(options.get("message_id"))
            except (TypeError, ValueError, OverflowError):
                message_id = 0
            if not username or message_id <= 0:
                raise ValueError(
                    f"case {case_id}: Telegram source_username/message_id are required"
                )
        normalized.append(
            {
                **dict(raw),
                "case_id": case_id,
                "adapter": adapter,
                "fixture_path": str(_resolve_fixture(manifest_path, raw.get("fixture_path"))),
                "expected": {
                    **dict(expected),
                    "event_id": expected_event_id,
                    "source_id": expected_source_id,
                    "source_url": source_url,
                    "source_type": source_type,
                    "first_collection_calls": int(expected["first_collection_calls"]),
                    "warm_collection_calls": int(expected.get("warm_collection_calls", 0)),
                    "first_collection_write": bool(expected["first_collection_write"]),
                    "warm_collection_write": bool(expected.get("warm_collection_write", False)),
                },
                "adapter_options": dict(options),
            }
        )
        seen.add(case_id)
    return normalized


def _sqlite_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"sha256": hashlib.sha256(value).hexdigest(), "byte_count": len(value)}
    return value


def capture_db_state(path: Path) -> dict[str, Any]:
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        quick = "\n".join(str(row[0]) for row in conn.execute("PRAGMA quick_check"))
        events = {
            str(row["id"]): {key: _sqlite_value(row[key]) for key in row.keys()}
            for row in conn.execute("SELECT * FROM event ORDER BY id")
        }
        sources = {
            str(row["id"]): {key: _sqlite_value(row[key]) for key in row.keys()}
            for row in conn.execute("SELECT * FROM event_source ORDER BY id")
        }
    payload = {"events": events, "event_sources": sources}
    return {
        **payload,
        "quick_check": quick,
        "event_count": len(events),
        "event_source_count": len(sources),
        "logical_sha256": _sha256(payload),
    }


def _changed_ids(before: Mapping[str, Any], after: Mapping[str, Any]) -> list[int]:
    return sorted(
        int(key)
        for key in set(before) | set(after)
        if before.get(key) != after.get(key)
    )


def _changed_keys(before: Mapping[str, Any] | None, after: Mapping[str, Any] | None) -> list[str]:
    before = before or {}
    after = after or {}
    return sorted(key for key in set(before) | set(after) if before.get(key) != after.get(key))


def _parse_decisions(raw: Any) -> dict[str, Any]:
    if isinstance(raw, Mapping):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _binding_rows(
    state: Mapping[str, Any], *, source_url: str, source_type: str
) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in (state.get("event_sources") or {}).values()
        if str(row.get("source_url") or "") == source_url
        and str(row.get("source_type") or "") == source_type
    ]


def _decision_evidence(
    event_row: Mapping[str, Any] | None,
    source_row: Mapping[str, Any] | None,
) -> dict[str, Any]:
    decisions = _parse_decisions((event_row or {}).get("collection_decisions"))
    source_id = int((source_row or {}).get("id") or 0)
    source_text = str((source_row or {}).get("source_text") or "")
    source_url = str((source_row or {}).get("source_url") or "")
    source_type = str((source_row or {}).get("source_type") or "")
    facts: dict[str, Any] = {}
    quote_errors: list[str] = []
    for key in FACT_KEYS:
        raw = decisions.get(key)
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        quote = str(row.get("evidence_quote") or "")
        source_matches = int(row.get("source_id") or 0) == source_id
        quote_matches = not quote or quote in source_text
        facts[key] = {
            "value": row.get("value"),
            "reason_code": row.get("reason_code"),
            "policy_version": row.get("policy_version"),
            "source_id": row.get("source_id"),
            "input_hash": row.get("input_hash"),
            "source_matches": source_matches,
            "quote_matches": quote_matches,
            "evidence_quote_sha256": hashlib.sha256(quote.encode("utf-8")).hexdigest()
            if quote
            else None,
        }
        if row.get("policy_version") == FACTS_POLICY_VERSION and (
            not source_matches or not quote_matches
        ):
            quote_errors.append(key)
    receipts = [
        receipt
        for receipt in (decisions.get("evaluation_receipts") or [])
        if isinstance(receipt, Mapping)
        and int(receipt.get("source_id") or 0) == source_id
        and receipt.get("policy_version") == FACTS_POLICY_VERSION
    ]
    receipt_summaries: list[dict[str, Any]] = []
    receipt_errors: list[str] = []
    for receipt_index, receipt in enumerate(receipts):
        prefix = f"receipt[{receipt_index}]"
        input_hash = str(receipt.get("input_hash") or "")
        if str(receipt.get("source_url") or "") != source_url:
            receipt_errors.append(f"{prefix}.source_url")
        if str(receipt.get("source_type") or "") != source_type:
            receipt_errors.append(f"{prefix}.source_type")
        if len(input_hash) != 64 or any(char not in "0123456789abcdef" for char in input_hash):
            receipt_errors.append(f"{prefix}.input_hash")
        payload = receipt.get("payload")
        payload_facts: dict[str, Any] = {}
        if not isinstance(payload, Mapping):
            receipt_errors.append(f"{prefix}.payload")
            payload = {}
        for key in FACT_KEYS:
            raw = payload.get(key)
            if not isinstance(raw, Mapping):
                receipt_errors.append(f"{prefix}.payload.{key}")
                continue
            quote = str(raw.get("evidence_quote") or "")
            quote_matches = not quote or quote in source_text
            if not quote_matches:
                receipt_errors.append(f"{prefix}.payload.{key}.evidence_quote")
            payload_facts[key] = {
                "value": raw.get("value"),
                "reason_code": raw.get("reason_code"),
                "quote_matches": quote_matches,
                "evidence_quote_sha256": hashlib.sha256(quote.encode("utf-8")).hexdigest()
                if quote
                else None,
            }
        receipt_summaries.append(
            {
                "input_hash": input_hash,
                "payload_sha256": _sha256(payload),
                "facts": payload_facts,
            }
        )
    return {
        "collection_decisions_sha256": _sha256(decisions),
        "matching_receipt_count": len(receipts),
        "receipt_input_hashes": sorted(
            {str(receipt.get("input_hash")) for receipt in receipts if receipt.get("input_hash")}
        ),
        "receipts": receipt_summaries,
        "receipt_grounding_errors": receipt_errors,
        "facts": facts,
        "source_grounding_errors": quote_errors,
    }


def _safe_trace(trace: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {key: _jsonable(value) for key, value in row.items() if key in TRACE_SAFE_KEYS}
        for row in trace
        if isinstance(row, Mapping)
    ]


def _trace_summary(trace: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    safe = _safe_trace(trace)
    collection = [row for row in safe if row.get("label") == "collection_candidate_adjudication"]

    def physical(rows: Sequence[Mapping[str, Any]]) -> int | None:
        values = [row.get("physical_sends") for row in rows]
        if values and all(isinstance(value, int) for value in values):
            return sum(int(value) for value in values)
        return 0 if not rows else None

    return {
        "all_logical_calls": len(safe),
        "all_physical_sends": physical(safe),
        "collection_logical_calls": len(collection),
        "collection_physical_sends": physical(collection),
        "labels": [str(row.get("label") or "") for row in safe],
        "trace": safe,
    }


async def _async_noop(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
    return {}


async def _async_none(*_args: Any, **_kwargs: Any) -> None:
    return None


async def _async_empty(*_args: Any, **_kwargs: Any) -> list[Any]:
    return []


@contextlib.contextmanager
def publication_side_effect_guard():
    """Disable publication/network-media effects without replacing ingestion."""

    main_module = importlib.import_module("main")
    tg = importlib.import_module("source_parsing.telegram.handlers")
    parser_handlers = importlib.import_module("source_parsing.handlers")
    smart_update = importlib.import_module("smart_event_update")
    real_smart_update = smart_update.smart_event_update

    async def smart_update_without_tasks(db: Database, candidate: Any, **kwargs: Any) -> Any:
        kwargs["schedule_tasks"] = False
        return await real_smart_update(db, candidate, **kwargs)

    old_env = {
        "TG_MONITORING_GLOBAL_VK_RECONCILE": os.environ.get(
            "TG_MONITORING_GLOBAL_VK_RECONCILE"
        ),
        "TG_MONITORING_KEEP_FORCE_MESSAGE_IDS": os.environ.get(
            "TG_MONITORING_KEEP_FORCE_MESSAGE_IDS"
        ),
    }
    os.environ["TG_MONITORING_GLOBAL_VK_RECONCILE"] = "0"
    os.environ["TG_MONITORING_KEEP_FORCE_MESSAGE_IDS"] = "1"
    try:
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch.object(smart_update, "smart_event_update", smart_update_without_tasks))
            stack.enter_context(patch.object(tg, "smart_event_update", smart_update_without_tasks))
            for name in ("schedule_event_update_tasks", "rebuild_fest_nav_if_changed"):
                if hasattr(main_module, name):
                    stack.enter_context(patch.object(main_module, name, _async_noop))
            if hasattr(tg, "_schedule_primary_import_event_tasks"):
                stack.enter_context(
                    patch.object(tg, "_schedule_primary_import_event_tasks", _async_none)
                )
            # The fixture is the evidence. Do not enrich it from live web/media
            # while an acceptance replay is supposed to be reproducible.
            for name, replacement in (
                ("_fallback_fetch_posters_from_public_tg_page", _async_empty),
                ("_fallback_fetch_full_text_from_public_tg_page", _async_none),
                ("_collect_linked_source_posters", _async_empty),
                ("_collect_linked_source_texts", _async_empty),
            ):
                if hasattr(tg, name):
                    stack.enter_context(patch.object(tg, name, replacement))
            if hasattr(parser_handlers, "download_images"):
                stack.enter_context(patch.object(parser_handlers, "download_images", _async_empty))
            if hasattr(parser_handlers, "_fetch_og_image_for_dramteatr"):
                stack.enter_context(
                    patch.object(parser_handlers, "_fetch_og_image_for_dramteatr", _async_none)
                )
            if hasattr(parser_handlers, "_ensure_telegraph_url"):
                stack.enter_context(patch.object(parser_handlers, "_ensure_telegraph_url", _async_none))
            if hasattr(parser_handlers, "EVENT_ADD_DELAY_SECONDS"):
                stack.enter_context(patch.object(parser_handlers, "EVENT_ADD_DELAY_SECONDS", 0))
            yield
    finally:
        for name, value in old_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


async def _force_telegram_message(db: Database, *, username: str, message_id: int) -> None:
    username = username.strip().lstrip("@").lower()
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT id FROM telegram_source WHERE lower(username)=? LIMIT 1",
                (username,),
            )
        ).fetchone()
        if row is None:
            raise ValueError(f"Telegram source @{username} is absent from DB copy")
        await conn.execute(
            "INSERT OR IGNORE INTO telegram_source_force_message(source_id,message_id) VALUES(?,?)",
            (int(row[0]), int(message_id)),
        )
        await conn.commit()


def _dataclass_kwargs(cls: type, payload: Mapping[str, Any], *, label: str) -> dict[str, Any]:
    allowed = {field.name for field in dataclasses.fields(cls) if field.init}
    unknown = sorted(set(payload) - allowed)
    if unknown:
        raise ValueError(f"{label}: unsupported fields: {unknown}")
    return {key: value for key, value in payload.items() if key in allowed}


async def invoke_adapter(case: Mapping[str, Any], db: Database) -> Any:
    adapter = str(case["adapter"])
    fixture_path = Path(str(case["fixture_path"]))
    options = dict(case.get("adapter_options") or {})
    if adapter == "telegram":
        tg = importlib.import_module("source_parsing.telegram.handlers")
        await _force_telegram_message(
            db,
            username=str(options["source_username"]),
            message_id=int(options["message_id"]),
        )
        return await tg.process_telegram_results(fixture_path, db, bot=None)
    payload = _load_object(fixture_path)
    if adapter == "vk":
        vk = importlib.import_module("vk_intake")
        draft_payload = payload.get("draft")
        if not isinstance(draft_payload, Mapping):
            raise ValueError(f"{fixture_path}: VK fixture requires draft object")
        if "poster_media" in draft_payload:
            raise ValueError(
                f"{fixture_path}: draft.poster_media is not a JSON fixture contract; "
                "put reproducible URL strings in top-level photos"
            )
        draft = vk.EventDraft(
            **_dataclass_kwargs(vk.EventDraft, draft_payload, label=f"{fixture_path}: draft")
        )
        photos = payload.get("photos") or []
        if not isinstance(photos, list) or any(not isinstance(item, str) for item in photos):
            raise ValueError(f"{fixture_path}: photos must be a string array")
        return await vk.persist_event_and_pages(
            draft,
            list(photos),
            db,
            source_post_url=str(payload.get("source_post_url") or case["expected"]["source_url"]),
            wait_for_telegraph_url=False,
        )
    parser_handlers = importlib.import_module("source_parsing.handlers")
    parser_module = importlib.import_module("source_parsing.parser")
    event_payload = payload.get("event")
    if not isinstance(event_payload, Mapping):
        raise ValueError(f"{fixture_path}: parser fixture requires event object")
    event = parser_module.TheatreEvent(
        **_dataclass_kwargs(
            parser_module.TheatreEvent,
            event_payload,
            label=f"{fixture_path}: event",
        )
    )
    source = str(payload.get("source") or event.source_type or "").strip()
    if not source:
        raise ValueError(f"{fixture_path}: parser source is required")
    stats, _progress = await parser_handlers.process_source_events(
        db,
        None,
        [event],
        source=source,
        start_index=0,
        total_count=1,
    )
    return stats


def _pass_receipt(
    *,
    case: Mapping[str, Any],
    pass_name: str,
    result: Any,
    trace: Sequence[Mapping[str, Any]],
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> dict[str, Any]:
    expected = case["expected"]
    event_id = int(expected["event_id"])
    bindings = _binding_rows(
        after,
        source_url=str(expected["source_url"]),
        source_type=str(expected["source_type"]),
    )
    exact_bindings = [row for row in bindings if int(row.get("event_id") or 0) == event_id]
    source_row = exact_bindings[0] if len(exact_bindings) == 1 else None
    event_before = (before.get("events") or {}).get(str(event_id))
    event_after = (after.get("events") or {}).get(str(event_id))
    collection_before = _parse_decisions((event_before or {}).get("collection_decisions"))
    collection_after = _parse_decisions((event_after or {}).get("collection_decisions"))
    event_changed_ids = _changed_ids(before.get("events") or {}, after.get("events") or {})
    source_changed_ids = _changed_ids(
        before.get("event_sources") or {}, after.get("event_sources") or {}
    )
    event_changed_keys = _changed_keys(event_before, event_after)
    collection_write = collection_before != collection_after
    summary = _trace_summary(trace)
    errors: list[str] = []
    if after.get("quick_check") != "ok":
        errors.append("sqlite_quick_check_failed")
    if event_after is None:
        errors.append("expected_event_missing")
    if len(exact_bindings) != 1 or len(bindings) != 1:
        errors.append("source_binding_not_unique")
    expected_source_id = expected.get("source_id")
    if expected_source_id is not None and (
        source_row is None or int(source_row.get("id") or 0) != int(expected_source_id)
    ):
        errors.append("source_id_mismatch")
    expected_calls = int(
        expected["first_collection_calls" if pass_name == "first" else "warm_collection_calls"]
    )
    if int(summary["collection_logical_calls"]) != expected_calls:
        errors.append("collection_call_count_mismatch")
    expected_write = bool(
        expected["first_collection_write" if pass_name == "first" else "warm_collection_write"]
    )
    if collection_write != expected_write:
        errors.append("collection_write_mismatch")
    if pass_name == "warm":
        if after.get("event_count") != before.get("event_count"):
            errors.append("warm_event_count_changed")
        if after.get("event_source_count") != before.get("event_source_count"):
            errors.append("warm_event_source_count_changed")
        if source_changed_ids:
            errors.append("warm_event_source_changed")
        if event_changed_ids:
            errors.append("warm_event_changed")
        if collection_write:
            errors.append("warm_collection_decisions_changed")
    evidence = _decision_evidence(event_after, source_row)
    if evidence["source_grounding_errors"]:
        errors.append("facts_v3_source_grounding_failed")
    if evidence["receipt_grounding_errors"]:
        errors.append("facts_v3_receipt_grounding_failed")
    return {
        "pass": pass_name,
        "status": "PASS" if not errors else "FAIL",
        "errors": errors,
        "adapter_result": _safe_adapter_result(result),
        "resolved": {
            "event_id": event_id if event_after is not None else None,
            "source_id": int(source_row["id"]) if source_row is not None else None,
            "source_url_sha256": hashlib.sha256(
                str(expected["source_url"]).encode("utf-8")
            ).hexdigest(),
            "source_type": expected["source_type"],
        },
        "provider": summary,
        "writes": {
            "collection_decisions": int(collection_write),
            "changed_event_ids": event_changed_ids,
            "changed_event_source_ids": source_changed_ids,
            "event_changed_keys": event_changed_keys,
            "logical_sha256_before": before.get("logical_sha256"),
            "logical_sha256_after": after.get("logical_sha256"),
        },
        "evidence": evidence,
    }


async def run_manifest(
    *,
    db_path: Path,
    manifest_path: Path,
    cases: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    import smart_event_update as collection_core

    started_at = _utc_now()
    initial_file_sha256 = _sha256_file(db_path)
    initial_state = capture_db_state(db_path)
    manifest = _load_object(manifest_path)
    expected_logical_sha = manifest.get("db_logical_sha256_before")
    if expected_logical_sha and expected_logical_sha != initial_state["logical_sha256"]:
        raise ValueError(
            "DB copy logical hash does not match manifest db_logical_sha256_before"
        )
    db = Database(str(db_path))
    case_reports: list[dict[str, Any]] = []
    try:
        with publication_side_effect_guard():
            for case in cases:
                passes: list[dict[str, Any]] = []
                for pass_name in ("first", "warm"):
                    before = capture_db_state(db_path)
                    collection_core.reset_smart_update_llm_trace()
                    result = await invoke_adapter(case, db)
                    trace = collection_core.get_smart_update_llm_trace()
                    after = capture_db_state(db_path)
                    passes.append(
                        _pass_receipt(
                            case=case,
                            pass_name=pass_name,
                            result=result,
                            trace=trace,
                            before=before,
                            after=after,
                        )
                    )
                case_reports.append(
                    {
                        "case_id": case["case_id"],
                        "adapter": case["adapter"],
                        "fixture_sha256": hashlib.sha256(
                            Path(str(case["fixture_path"])).read_bytes()
                        ).hexdigest(),
                        "status": (
                            "PASS" if all(item["status"] == "PASS" for item in passes) else "FAIL"
                        ),
                        "passes": passes,
                    }
                )
    finally:
        await db.close()
    final_state = capture_db_state(db_path)
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "run_id": str(manifest.get("run_id") or manifest_path.stem),
        "repo_sha": _repo_sha(),
        "generator_command": shlex.join([sys.executable, *sys.argv]),
        "started_at": started_at,
        "finished_at": _utc_now(),
        "db_copy": {
            "path": str(db_path),
            "file_sha256_before": initial_file_sha256,
            "file_sha256_after": _sha256_file(db_path),
            "logical_sha256_before": initial_state["logical_sha256"],
            "quick_check": final_state["quick_check"],
            "logical_sha256_after": final_state["logical_sha256"],
        },
        "manifest": {
            "path": str(manifest_path),
            "sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            "case_count": len(cases),
        },
        "publication_side_effects": "disabled",
        "direct_apply_collection_decisions": False,
        "status": "PASS" if all(item["status"] == "PASS" for item in case_reports) else "FAIL",
        "cases": case_reports,
    }


def _repo_sha() -> str:
    import subprocess

    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "unknown"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-copy", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--allow-mutable-copy",
        action="store_true",
        help="Required acknowledgement that --db-copy may be mutated.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    db_path = args.db_copy.expanduser().resolve()
    manifest_path = args.manifest.expanduser().resolve()
    if not args.allow_mutable_copy:
        raise SystemExit("refusing mutation without --allow-mutable-copy")
    if db_path == Path("/data/db.sqlite") or str(db_path).startswith("/data/"):
        raise SystemExit("refusing to run ingestion acceptance against /data; use a DB copy")
    if not db_path.is_file() or not manifest_path.is_file():
        raise SystemExit("DB copy and manifest must exist")
    manifest = _load_object(manifest_path)
    cases = validate_manifest(manifest, manifest_path=manifest_path)
    result = asyncio.run(
        run_manifest(db_path=db_path, manifest_path=manifest_path, cases=cases)
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
