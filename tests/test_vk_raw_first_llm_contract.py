from __future__ import annotations

import ast
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

import main
from db import Database
from source_parse_contract import (
    EvidenceManifest,
    SourceDisposition,
    SourceParseDecision,
    SourceParseRetryReason,
)
import vk_auto_queue
import vk_intake
import vk_review


class _Bot:
    async def send_message(self, *_args, **_kwargs):
        return None

    async def get_me(self):
        class _Me:
            username = "eventsbotTestBot"

        return _Me()


async def _db(tmp_path: Path) -> Database:
    db = Database(str(tmp_path / "vk.sqlite"))
    await db.init()
    return db


@pytest.mark.asyncio
async def test_discovery_persists_every_hint_shape_before_semantics(tmp_path, monkeypatch):
    db = await _db(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'club1','source')"
        )
        await conn.commit()

    now = int(time.time())
    posts = [
        {"date": now, "post_id": 1, "text": "обычная запись", "photos": []},
        {"date": now + 1, "post_id": 2, "text": "Концерт без даты", "photos": []},
        {"date": now + 2, "post_id": 3, "text": "Концерт 1 января 2001", "photos": []},
        {"date": now + 3, "post_id": 4, "text": "   ", "photos": ["poster"]},
    ]

    async def wall(*_args, offset=0, **_kwargs):
        return posts if offset == 0 else []

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr(main, "vk_wall_since", wall)
    monkeypatch.setattr(vk_intake.asyncio, "sleep", no_sleep)
    result = await vk_intake.crawl_once(db)
    assert result["added"] == 4
    async with db.raw_conn() as conn:
        packets = await (await conn.execute(
            "SELECT post_id,discovery_keyword_hints_json,status FROM vk_source_packet ORDER BY post_id"
        )).fetchall()
        inbox = await (await conn.execute(
            "SELECT COUNT(*) FROM vk_inbox WHERE status='pending'"
        )).fetchone()
    assert inbox[0] == 4
    assert "hint:no_keywords" in json.loads(packets[0][1])
    assert "hint:no_date" in json.loads(packets[1][1])
    assert "hint:past_event" in json.loads(packets[2][1])
    assert vk_intake.OCR_PENDING_SENTINEL in json.loads(packets[3][1])
    assert all(row[2] == "pending" for row in packets)


@pytest.mark.asyncio
async def test_changed_revision_appends_and_exact_revision_reuses(tmp_path):
    db = await _db(tmp_path)
    base = {"date": 100, "post_id": 7, "text": "one", "photos": []}
    first, first_new = await vk_intake._persist_vk_source_packet(
        db, group_id=1, owner_type="group", post=base,
        source_url="https://vk.com/wall-1_7", keyword_hints=[], date_hints=[], event_ts_hint=None,
    )
    replay, replay_new = await vk_intake._persist_vk_source_packet(
        db, group_id=1, owner_type="group", post=dict(base),
        source_url="https://vk.com/wall-1_7", keyword_hints=[], date_hints=[], event_ts_hint=None,
    )
    changed, changed_new = await vk_intake._persist_vk_source_packet(
        db, group_id=1, owner_type="group", post={**base, "text": "two"},
        source_url="https://vk.com/wall-1_7", keyword_hints=[], date_hints=[], event_ts_hint=None,
    )
    assert (first_new, replay_new, changed_new) == (True, False, True)
    assert first == replay and changed != first
    async with db.raw_conn() as conn:
        rows = await (await conn.execute(
            "SELECT revision,raw_text FROM vk_source_packet ORDER BY revision"
        )).fetchall()
        inbox = await (await conn.execute(
            "SELECT source_packet_id,status FROM vk_inbox WHERE post_id=7"
        )).fetchone()
    assert rows == [(1, "one"), (2, "two")]
    assert inbox == (changed, "pending")


@pytest.mark.asyncio
async def test_cursor_does_not_advance_when_packet_persist_fails(tmp_path, monkeypatch):
    db = await _db(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute("INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'g','G')")
        await conn.execute("INSERT INTO vk_crawl_cursor(group_id,last_seen_ts,last_post_id) VALUES(1,10,1)")
        await conn.commit()

    async def wall(*_args, offset=0, **_kwargs):
        return [{"date": 20, "post_id": 2, "text": "x", "photos": []}] if offset == 0 else []

    async def fail(*_args, **_kwargs):
        raise OSError("disk full")

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr(main, "vk_wall_since", wall)
    monkeypatch.setattr(vk_intake, "_persist_vk_source_packet", fail)
    monkeypatch.setattr(vk_intake.asyncio, "sleep", no_sleep)
    await vk_intake.crawl_once(db)
    async with db.raw_conn() as conn:
        cursor = await (await conn.execute(
            "SELECT last_seen_ts,last_post_id FROM vk_crawl_cursor WHERE group_id=1"
        )).fetchone()
    assert cursor == (10, 1)


@pytest.mark.asyncio
async def test_safety_cap_creates_durable_continuation(tmp_path, monkeypatch):
    db = await _db(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute("INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'g','G')")
        await conn.execute(
            "INSERT INTO vk_crawl_cursor(group_id,last_seen_ts,last_post_id,updated_at) VALUES(1,10,1,CURRENT_TIMESTAMP)"
        )
        await conn.commit()
    page_size = vk_intake.VK_CRAWL_PAGE_SIZE

    async def wall(*_args, offset=0, **_kwargs):
        start = offset + 2
        return [
            {"date": 1000 + i, "post_id": start + i, "text": "x", "photos": []}
            for i in range(page_size)
        ]

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr(main, "vk_wall_since", wall)
    monkeypatch.setattr(vk_intake.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(vk_intake, "VK_CRAWL_MAX_PAGES_INC", 1)
    await vk_intake.crawl_once(db)
    async with db.raw_conn() as conn:
        row = await (await conn.execute(
            "SELECT reason,status,offset FROM vk_crawl_continuation WHERE owner_id=1"
        )).fetchone()
    assert row and row[0] in {"hard_cap", "page_safety_cap"} and row[1] == "pending"
    assert row[2] > 0


def test_ocr_budget_includes_every_block_regardless_of_text_shape():
    blocks = [f"card {idx} " + "x" * 1000 for idx in range(8)]
    assert vk_intake._budget_vk_parse_poster_texts("x" * 20_000, blocks) == blocks
    assert vk_intake._budget_vk_parse_poster_texts("нет фразы про карточки", blocks) == blocks


@pytest.mark.asyncio
async def test_incomplete_evidence_forbids_negative_but_keeps_positive(monkeypatch):
    manifest = EvidenceManifest(
        raw_text_chars=1, raw_text_hash="h", attachment_count=2,
        ocr_blocks_available=1, ocr_blocks_included=1,
        included_chars=5, unavailable_attachment_count=1, ocr_complete=False,
    )

    async def no_event(*_args, **_kwargs):
        return SourceParseDecision(
            [], disposition=SourceDisposition.RETRY_REQUIRED,
            evidence_manifest=manifest,
            evidence_complete=False,
            retry_reason=SourceParseRetryReason.EVIDENCE_INCOMPLETE,
        )

    monkeypatch.setattr(main, "parse_event_via_llm", no_event)
    drafts, _ = await vk_intake.build_event_drafts_from_vk("x", evidence_manifest=manifest)
    assert drafts.disposition is SourceDisposition.RETRY_REQUIRED

    async def positive(*_args, **_kwargs):
        return SourceParseDecision(
            [{"title": "Event", "date": "2026-10-01"}],
            disposition=SourceDisposition.EVENTS_FOUND,
            evidence_manifest=manifest,
            evidence_complete=False,
        )

    monkeypatch.setattr(main, "parse_event_via_llm", positive)
    drafts, _ = await vk_intake.build_event_drafts_from_vk("x", evidence_manifest=manifest)
    assert len(drafts) == 1 and drafts.enrichment_required is True


@pytest.mark.asyncio
async def test_parse_receipt_is_replayable_without_second_provider_call(tmp_path):
    db = await _db(tmp_path)
    packet_id, _ = await vk_intake._persist_vk_source_packet(
        db, group_id=1, owner_type="group",
        post={"date": 1, "post_id": 1, "text": "x", "photos": []},
        source_url="https://vk.com/wall-1_1", keyword_hints=[], date_hints=[], event_ts_hint=None,
    )
    receipt = {
        "decision": SourceParseDecision(
            [{"title": "Event"}], disposition=SourceDisposition.EVENTS_FOUND,
            evidence_manifest=EvidenceManifest.complete_source("x"),
        ).to_payload(),
        "drafts": [{"title": "Event", "date": "2026-10-01"}],
    }
    await vk_review.record_source_parse_attempt(
        db, source_packet_id=packet_id, prompt_version="p", model="m",
        evidence_manifest=EvidenceManifest.complete_source("x").to_payload(),
        parse_result=receipt, disposition="EVENTS_FOUND", retry_reason=None,
        event_child_count=1, lifecycle_action_count=0,
    )
    loaded = await vk_review.load_successful_parse_receipt(
        db, source_packet_id=packet_id, prompt_version="p", model="m"
    )
    assert loaded == receipt
    assert vk_intake.DraftParseResult.from_receipt_payload(loaded)[0].title == "Event"
    await vk_review.record_exact_parse_replay(
        db, source_packet_id=packet_id, prompt_version="p", model="m"
    )
    async with db.raw_conn() as conn:
        attempts = await (await conn.execute(
            "SELECT attempt_kind,llm_started,llm_completed FROM vk_source_packet_attempt "
            "WHERE source_packet_id=? ORDER BY attempt_no",
            (packet_id,),
        )).fetchall()
    assert attempts == [("primary", 1, 1), ("exact_replay", 0, 0)]


@pytest.mark.asyncio
async def test_rate_limit_and_restart_never_terminal(tmp_path):
    db = await _db(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_inbox(id,group_id,post_id,date,text,has_date,status) VALUES(1,1,1,1,'x',0,'pending')"
        )
        await conn.commit()
    states = []
    for _ in range(8):
        state, _attempts = await vk_review.mark_rate_limited(
            db, 1, batch_id="b", retry_after_sec=1, max_attempts=2
        )
        states.append(state)
    async with db.raw_conn() as conn:
        row = await (await conn.execute(
            "SELECT status,last_typed_reason,next_attempt_at FROM vk_inbox WHERE id=1"
        )).fetchone()
    assert states == ["deferred"] * 8
    assert row[0] == "deferred" and row[1] == "RATE_LIMITED" and row[2]


@pytest.mark.asyncio
async def test_typed_provider_receipt_releases_lease_and_persists_quota_metadata(
    tmp_path, monkeypatch
):
    db = await _db(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'club1','source')"
        )
        await conn.commit()
    now = int(time.time())
    packet_id, _ = await vk_intake._persist_vk_source_packet(
        db,
        group_id=1,
        owner_type="group",
        post={"date": now, "post_id": 9, "text": "source", "photos": []},
        source_url="https://vk.com/wall-1_9",
        keyword_hints=[],
        date_hints=[],
        event_ts_hint=None,
    )
    post = await vk_review.pick_next(db, 7, "batch", resume_locked=False)
    assert post is not None and post.source_packet_id == packet_id

    async def fake_fetch(*_args, **_kwargs):
        return (
            "source",
            [],
            datetime.fromtimestamp(now, timezone.utc),
            {},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    decision = SourceParseDecision.retry(
        SourceParseRetryReason.TECHNICAL_ERROR,
        evidence_manifest=EvidenceManifest.complete_source("source"),
        provider_attempts=[
            {
                "attempt_kind": "primary",
                "model": "gemma-4-31b-it",
                "quota_scope": "google:shared-project",
                "quota_reason": "RPD_EXHAUSTED",
                "request_id": "request-1",
                "finish_reason": "RATE_LIMITED",
                "input_tokens": 700,
                "reserved_tokens": 1400,
                "provider_retry_after_ms": 3_600_000,
                "error_type": "rate_limit",
            }
        ],
    )

    async def fake_build(*_args, **_kwargs):
        return vk_intake.DraftParseResult([], decision=decision), None

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_auto_queue.vk_intake, "build_event_drafts", fake_build)
    report = vk_auto_queue.VkAutoImportReport(batch_id="batch")
    await vk_auto_queue._process_vk_inbox_row(
        db,
        _Bot(),
        chat_id=1,
        operator_id=7,
        batch_id="batch",
        post=post,
        source_url="https://vk.com/wall-1_9",
        report=report,
        festival_names=None,
        festival_alias_pairs=None,
        progress_message_id=None,
        progress_current_no=1,
        progress_total_txt="1",
    )

    async with db.raw_conn() as conn:
        inbox = await (await conn.execute(
            "SELECT status,quota_scope,provider_retry_after,last_typed_reason "
            "FROM vk_inbox WHERE source_packet_id=?",
            (packet_id,),
        )).fetchone()
        attempt = await (await conn.execute(
            "SELECT attempt_kind,llm_started,llm_completed,structured_response_valid,"
            "quota_scope,request_id,finish_reason,input_tokens,reserved_tokens,"
            "provider_retry_after,typed_error_reason "
            "FROM vk_source_packet_attempt WHERE source_packet_id=?",
            (packet_id,),
        )).fetchone()
    assert report.inbox_deferred == 1
    assert inbox == (
        "deferred",
        "google:shared-project",
        3600,
        SourceParseRetryReason.TECHNICAL_ERROR.value,
    )
    assert attempt == (
        "primary", 1, 0, 0, "google:shared-project", "request-1",
        "RATE_LIMITED", 700, 1400, 3600,
        SourceParseRetryReason.TECHNICAL_ERROR.value,
    )


@pytest.mark.asyncio
async def test_unknown_and_bad_hints_are_due_with_age_fairness(tmp_path):
    db = await _db(tmp_path)
    async with db.raw_conn() as conn:
        await conn.executemany(
            "INSERT INTO vk_inbox(group_id,post_id,date,text,has_date,event_ts_hint,status) VALUES(1,?,?,?,?,?,'pending')",
            [
                (1, 1, "old unknown", 0, None),
                (2, 2, "new urgent", 1, 3),
                (3, 3, "far", 1, 9999999999),
            ],
        )
        await conn.commit()
    picked = []
    for _ in range(3):
        post = await vk_review.pick_next(db, 10, "b", resume_locked=False)
        assert post is not None
        picked.append(post.post_id)
        await vk_review.mark_rejected(db, post.id)
    assert picked == [1, 2, 3]


@pytest.mark.asyncio
async def test_recorded_1_5x_p99_burst_is_lossless_and_backlog_drains(tmp_path):
    """T48/T51: audited p99 is six event-parse requests/minute; replay nine."""

    db = await _db(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'club1','source')"
        )
        await conn.commit()
    now = int(time.time())
    for post_id in range(1, 10):
        await vk_intake._persist_vk_source_packet(
            db,
            group_id=1,
            owner_type="group",
            post={
                "date": now + post_id,
                "post_id": post_id,
                "text": f"carrier {post_id}",
                "photos": [],
            },
            source_url=f"https://vk.com/wall-1_{post_id}",
            keyword_hints=[],
            date_hints=[],
            event_ts_hint=None,
        )

    # Recorded quota-exhaustion response: every lease is released into durable
    # retry; no attempt ceiling can terminalize one member of the burst.
    for _ in range(9):
        post = await vk_review.pick_next(db, 1, "limited", resume_locked=False)
        assert post is not None
        state, _ = await vk_review.mark_rate_limited(
            db, post.id, batch_id="limited", retry_after_sec=1, max_attempts=3
        )
        assert state == "deferred"
    async with db.raw_conn() as conn:
        assert (await (await conn.execute(
            "SELECT COUNT(*) FROM vk_inbox WHERE status='deferred'"
        )).fetchone())[0] == 9
        await conn.execute(
            "UPDATE vk_inbox SET next_attempt_at=datetime('now','-1 second')"
        )
        await conn.execute(
            "UPDATE vk_source_packet SET next_attempt_at=datetime('now','-1 second')"
        )
        await conn.commit()
    assert await vk_review.release_due_deferred(db) == 9

    # Quota recovers: the same durable carriers are selected and resolved;
    # backlog monotonically drains to zero without re-fetch identity loss.
    remaining = []
    for _ in range(9):
        post = await vk_review.pick_next(db, 1, "recovered", resume_locked=False)
        assert post is not None
        await vk_review.mark_carrier_outcome(
            db, inbox_id=post.id, outcome="EVENTS_RESOLVED"
        )
        async with db.raw_conn() as conn:
            remaining.append((await (await conn.execute(
                "SELECT COUNT(*) FROM vk_inbox WHERE status='pending'"
            )).fetchone())[0])
    assert remaining == sorted(remaining, reverse=True)
    async with db.raw_conn() as conn:
        statuses = await (await conn.execute(
            "SELECT status,COUNT(*) FROM vk_source_packet GROUP BY status"
        )).fetchall()
    assert statuses == [("imported", 9)]


def test_static_vk_ingestion_bans_semantic_shortcuts():
    intake = Path(vk_intake.__file__).read_text()
    queue = Path(vk_auto_queue.__file__).read_text()
    review = Path(vk_review.__file__).read_text()
    assert "prefilter_obvious_non_events" not in intake + queue
    assert "_vk_parse_preclassify" not in intake
    assert "_looks_like_cancellation_notice" not in queue
    assert "reject_reason" not in intake + queue
    assert "await vk_review.mark_failed" not in queue
    assert "def mark_failed" not in review
    assert "event_ts_hint IS NULL OR event_ts_hint >=" not in queue + review
    assert "lifecycle_unresolved" in queue
    assert 'typed_reason="LIFECYCLE_NO_MATCH"' in queue
    assert 'carrier_outcome = "MIXED_RESOLVED"' in queue
    ast.parse(intake)
    ast.parse(queue)
    ast.parse(review)
