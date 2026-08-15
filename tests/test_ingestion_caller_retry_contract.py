from __future__ import annotations

import ast
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from db import Database
from festival_queue import festival_queue_retry_delay, process_festival_queue
from models import FestivalQueueItem
from source_parsing.handlers import _schedule_source_parser_recovery_request
from source_parsing.telegram.handlers import (
    process_telegram_results,
    _source_evidence_incomplete_reason,
    _source_parse_decision,
    _source_zero_event_is_confirmed,
)
from source_parse_contract import EvidenceManifest, SourceDisposition, SourceParseRetryReason


ROOT = Path(__file__).resolve().parents[1]


def test_zero_extraction_requires_complete_typed_no_event() -> None:
    legacy = {"events": []}
    assert _source_parse_decision(legacy) is None
    assert not _source_zero_event_is_confirmed(legacy, None)

    confirmed = {
        "events": [],
        "source_parse_decision": {
            "disposition": "CONFIRMED_NO_EVENT",
            "no_event_reason": "NO_ATTENDABLE_EVENT",
            "events": [],
            "lifecycle_actions": [],
            "evidence_manifest": EvidenceManifest.complete_source(
                "typed source", ["ocr 1", "ocr 2"], attachment_count=2
            ).to_payload(),
            "evidence_complete": True,
            "parse_version": "test-v1",
        },
    }
    decision = _source_parse_decision(confirmed)
    assert _source_zero_event_is_confirmed(confirmed, decision)

    retry = {
        **confirmed,
        "source_parse_decision": {
            **confirmed["source_parse_decision"],
            "disposition": "RETRY_REQUIRED",
        },
    }
    assert not _source_zero_event_is_confirmed(
        retry, _source_parse_decision(retry)
    )


def test_unknown_decision_and_event_mismatch_are_typed_schema_retries() -> None:
    manifest = EvidenceManifest.complete_source("source", [], attachment_count=0).to_payload()
    unknown = {
        "events": [],
        "source_parse_decision": {
            "disposition": "UNKNOWN",
            "events": [],
            "lifecycle_actions": [],
            "evidence_manifest": manifest,
            "evidence_complete": True,
            "parse_version": "test-v1",
        },
    }
    mismatch = {
        "events": [{"title": "Outer"}],
        "source_parse_decision": {
            "disposition": "CONFIRMED_NO_EVENT",
            "no_event_reason": "NO_ATTENDABLE_EVENT",
            "events": [],
            "lifecycle_actions": [],
            "evidence_manifest": manifest,
            "evidence_complete": True,
            "parse_version": "test-v1",
        },
    }
    for message in (unknown, mismatch):
        decision = _source_parse_decision(message)
        assert decision.disposition is SourceDisposition.RETRY_REQUIRED
        assert decision.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH
        assert not _source_zero_event_is_confirmed(message, decision)


def test_manifest_hash_or_attachment_cardinality_mismatch_is_retryable() -> None:
    manifest = EvidenceManifest.complete_source("different", [], attachment_count=0).to_payload()
    message = {
        "text": "actual source",
        "_source_attachment_count": 1,
        "events": [],
        "source_parse_decision": {
            "disposition": "CONFIRMED_NO_EVENT",
            "no_event_reason": "NO_ATTENDABLE_EVENT",
            "events": [],
            "lifecycle_actions": [],
            "evidence_manifest": manifest,
            "evidence_complete": True,
            "parse_version": "test-v1",
        },
    }
    decision = _source_parse_decision(message)
    assert decision.disposition is SourceDisposition.RETRY_REQUIRED
    assert decision.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH
    assert not _source_zero_event_is_confirmed(message, decision)


def test_missing_ocr_or_truncation_keeps_carrier_unresolved() -> None:
    incomplete = {
        "source_parse_decision": SimpleNamespace(
            disposition="EVENTS_FOUND",
            evidence_complete=False,
            evidence_manifest={"ocr_blocks_available": 3, "ocr_blocks_included": 1},
        )
    }
    decision = _source_parse_decision(incomplete)
    assert _source_evidence_incomplete_reason(incomplete, decision)

    truncated = {
        "source_parse_decision": {
            "disposition": "CONFIRMED_NO_EVENT",
            "no_event_reason": "NO_ATTENDABLE_EVENT",
            "evidence_complete": True,
            "evidence_manifest": {"truncation_flag": True},
        }
    }
    decision = _source_parse_decision(truncated)
    assert _source_evidence_incomplete_reason(truncated, decision)
    assert not _source_zero_event_is_confirmed(truncated, decision)


@pytest.mark.asyncio
async def test_untyped_empty_telegram_carrier_is_visible_linear_terminal(tmp_path) -> None:
    db = Database(str(tmp_path / "telegram-retry.sqlite"))
    await db.init()
    try:
        async with db.raw_conn() as conn:
            source_row = await (
                await conn.execute(
                    "SELECT id FROM telegram_source WHERE username='ecodvor39'"
                )
            ).fetchone()
            source_id = int(source_row[0])
            await conn.execute(
                "UPDATE telegram_source SET last_scanned_message_id=NULL WHERE id=?",
                (source_id,),
            )
            await conn.commit()

        path = tmp_path / "telegram-results.json"
        path.write_text(
            json.dumps(
                {
                    "run_id": "caller-retry-test",
                    "stats": {"sources_total": 1, "messages_scanned": 1},
                    "messages": [
                        {
                            "source_username": "ecodvor39",
                            "message_id": 98765,
                            "message_date": "2026-08-11T08:00:00+00:00",
                            "source_link": "https://t.me/ecodvor39/98765",
                            "text": "Carrier with an ambiguous empty legacy result",
                            "events": [],
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        report = await process_telegram_results(path, db)

        async with db.raw_conn() as conn:
            cursor_row = await (
                await conn.execute(
                    "SELECT last_scanned_message_id FROM telegram_source WHERE id=?",
                    (source_id,),
                )
            ).fetchone()
            scan_row = await (
                await conn.execute(
                    "SELECT status FROM telegram_scanned_message "
                    "WHERE source_id=? AND message_id=98765",
                    (source_id,),
                )
            ).fetchone()
            force_row = await (
                await conn.execute(
                    "SELECT 1 FROM telegram_source_force_message "
                    "WHERE source_id=? AND message_id=98765",
                    (source_id,),
                )
            ).fetchone()
        assert cursor_row[0] == 98765
        assert scan_row[0] == "terminal_error"
        assert force_row is None
        assert report.messages_new_raw == 1
        assert report.messages_forced_replay == 0
        assert report.messages_metrics_only == 0
        assert report.messages_terminal_errors == 1
        assert report.skipped_posts[0].status == "terminal_error"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_complete_typed_no_event_advances_telegram_cursor(tmp_path) -> None:
    db = Database(str(tmp_path / "telegram-no-event.sqlite"))
    await db.init()
    try:
        async with db.raw_conn() as conn:
            source_row = await (
                await conn.execute(
                    "SELECT id FROM telegram_source WHERE username='ecodvor39'"
                )
            ).fetchone()
            source_id = int(source_row[0])

        path = tmp_path / "telegram-results.json"
        path.write_text(
            json.dumps(
                {
                    "run_id": "caller-no-event-test",
                    "stats": {"sources_total": 1, "messages_scanned": 1},
                    "messages": [
                        {
                            "source_username": "ecodvor39",
                            "message_id": 98766,
                            "message_date": "2026-08-11T08:05:00+00:00",
                            "source_link": "https://t.me/ecodvor39/98766",
                            "text": "A typed legitimate non-event",
                            "events": [],
                            "source_parse_decision": {
                                "disposition": "CONFIRMED_NO_EVENT",
                                "no_event_reason": "VAGUE_TEASER",
                                "events": [],
                                "lifecycle_actions": [],
                                "evidence_manifest": EvidenceManifest.complete_source(
                                    "A typed legitimate non-event", [], attachment_count=0
                                ).to_payload(),
                                "evidence_complete": True,
                                "parse_version": "test-v1",
                            },
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        await process_telegram_results(path, db)

        async with db.raw_conn() as conn:
            cursor_row = await (
                await conn.execute(
                    "SELECT last_scanned_message_id FROM telegram_source WHERE id=?",
                    (source_id,),
                )
            ).fetchone()
            scan_row = await (
                await conn.execute(
                    "SELECT status FROM telegram_scanned_message "
                    "WHERE source_id=? AND message_id=98766",
                    (source_id,),
                )
            ).fetchone()
        assert cursor_row[0] == 98766
        assert scan_row[0] == "confirmed_no_event"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_official_parser_item_failure_upserts_due_recovery(tmp_path) -> None:
    db = Database(str(tmp_path / "parser-retry.sqlite"))
    await db.init()
    try:
        assert await _schedule_source_parser_recovery_request(
            db, source_type="dramteatr", reason="ocr_timeout"
        )
        assert await _schedule_source_parser_recovery_request(
            db, source_type="dramteatr", reason="persist_timeout"
        )
        async with db.raw_conn() as conn:
            cursor = await conn.execute(
                "SELECT source_type,status,attempts,last_error "
                "FROM source_parser_recovery_request"
            )
            rows = await cursor.fetchall()
            await cursor.close()
        assert rows == [("dramteatr", "pending", 0, "persist_timeout")]
    finally:
        await db.close()


def test_festival_error_backoff_is_bounded_and_errors_are_due_selectable() -> None:
    assert festival_queue_retry_delay(1) == timedelta(minutes=5)
    assert festival_queue_retry_delay(99) == timedelta(minutes=360)

    source = (ROOT / "festival_queue.py").read_text(encoding="utf-8")
    assert 'FestivalQueueItem.status.in_(["pending", "error"])' in source
    assert "next_run_at=retry_at" in source


@pytest.mark.asyncio
async def test_due_festival_error_row_is_selected_and_rescheduled(tmp_path) -> None:
    db = Database(str(tmp_path / "festival-retry.sqlite"))
    await db.init()
    try:
        async with db.get_session() as session:
            session.add(
                FestivalQueueItem(
                    status="error",
                    source_kind="unsupported_test_kind",
                    source_url="https://example.test/festival-retry",
                    attempts=2,
                    last_error="previous failure",
                    next_run_at=datetime.now(timezone.utc) - timedelta(seconds=1),
                )
            )
            await session.commit()

        report = await process_festival_queue(db, trigger="test", limit=1)
        assert report.processed == 1
        assert report.failed == 1
        async with db.get_session() as session:
            item = (
                await session.execute(
                    select(FestivalQueueItem)
                )
            ).scalar_one()
        assert item.status == "error"
        assert int(item.attempts or 0) == 3
        next_run_at = item.next_run_at
        if next_run_at.tzinfo is None:
            next_run_at = next_run_at.replace(tzinfo=timezone.utc)
        assert next_run_at > datetime.now(timezone.utc)
    finally:
        await db.close()


def test_telegram_semantic_detectors_are_hints_not_terminal_continues() -> None:
    tree = ast.parse(
        (ROOT / "source_parsing/telegram/handlers.py").read_text(encoding="utf-8")
    )
    detector_names = {
        "_is_ads_message",
        "_is_esoterica_message",
        "_looks_like_recurring_excursion",
        "_should_skip_past_event_candidate",
    }
    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        called = {
            call.func.id
            for call in ast.walk(node.test)
            if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
        }
        if not (called & detector_names):
            continue
        if any(isinstance(child, (ast.Continue, ast.Return)) for child in node.body):
            violations.append(f"line {node.lineno}: {sorted(called & detector_names)}")
    assert violations == []


def test_forwarded_empty_result_requires_typed_no_event_or_retry_message() -> None:
    source = (ROOT / "main_part2.py").read_text(encoding="utf-8")
    assert 'disposition == "CONFIRMED_NO_EVENT"' in source
    assert "no_event_reason in {item.value for item in SourceNoEventReason}" in source
    assert "это не означает, что событий нет" in source
    assert "TelegramSourceForceMessage" in source
