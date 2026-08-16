from __future__ import annotations

import ast
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pytest_asyncio

import main
from db import Database
from models import PosterOcrCache
from source_parse_contract import (
    EvidenceManifest,
    LifecycleAction,
    LifecycleActionType,
    SourceDisposition,
    SourceNoEventReason,
    SourceParseDecision,
    SourceParseRetryReason,
)
import vk_auto_queue
import vk_intake
import vk_review
from poster_media import PosterMedia
from vk_source_envelope import build_vk_source_envelope


@pytest_asyncio.fixture(autouse=True)
async def _dispose_test_databases(monkeypatch):
    """Close every Database created by this module before interpreter shutdown."""

    instances: list[Database] = []
    original_init = Database.__init__

    def tracked_init(instance, *args, **kwargs):
        original_init(instance, *args, **kwargs)
        instances.append(instance)

    monkeypatch.setattr(Database, "__init__", tracked_init)
    yield
    for instance in instances:
        await instance.close()


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
async def test_envelope_revision_matrix_reopens_on_recursive_semantic_edits_only(tmp_path):
    db = await _db(tmp_path)
    raw = {
        "id": 70,
        "date": 100,
        "edited": 101,
        "text": "outer",
        "views": {"count": 1},
        "attachments": [
            {"type": "link", "link": {"url": "https://a", "title": "A"}}
        ],
        "copy_history": [
            {"id": 2, "text": "copy one", "attachments": []},
            {
                "id": 3,
                "text": "copy two",
                "attachments": [
                    {"type": "video", "video": {"owner_id": -1, "id": 4, "title": "V"}},
                    {"type": "doc", "doc": {"owner_id": -1, "id": 5, "title": "D"}},
                ],
            },
        ],
    }

    async def persist(item):
        return await vk_intake._persist_vk_source_packet(
            db,
            group_id=1,
            owner_type="group",
            post=build_vk_source_envelope(item, owner_id=1),
            source_url="https://vk.com/wall-1_70",
            keyword_hints=[],
            date_hints=[],
            event_ts_hint=None,
        )

    first, first_new = await persist(raw)
    counter = json.loads(json.dumps(raw))
    counter["views"]["count"] = 999
    same, same_new = await persist(counter)
    reordered = {key: raw[key] for key in reversed(list(raw))}
    reordered_id, reordered_new = await persist(reordered)
    assert first_new is True
    assert (same, same_new) == (first, False)
    assert (reordered_id, reordered_new) == (first, False)

    mutations = []
    for mutate in (
        lambda item: item.__setitem__("text", "outer changed"),
        lambda item: item["copy_history"][1].__setitem__("text", "copy two changed"),
        lambda item: item["attachments"][0]["link"].__setitem__("title", "B"),
        lambda item: item["copy_history"][1]["attachments"][0]["video"].__setitem__("id", 40),
        lambda item: item["copy_history"][1]["attachments"][1]["doc"].__setitem__("id", 50),
        lambda item: item.__setitem__("edited", 102),
    ):
        changed = json.loads(json.dumps(raw))
        mutate(changed)
        mutations.append(await persist(changed))
    assert all(is_new for _packet_id, is_new in mutations)
    assert len({packet_id for packet_id, _ in mutations}) == len(mutations)
    async with db.raw_conn() as conn:
        rows = await (await conn.execute(
            "SELECT envelope_version,capture_complete,evidence_replayability "
            "FROM vk_source_packet ORDER BY revision"
        )).fetchall()
        inbox = await (await conn.execute(
            "SELECT source_packet_id,status,text FROM vk_inbox WHERE post_id=70"
        )).fetchone()
    assert rows == [(1, 1, "replayable_lossless")] * 7
    assert inbox[0] == mutations[-1][0] and inbox[1] == "pending"
    assert "outer" in inbox[2]
    assert "copy two" in inbox[2]
    assert "https://a" in inbox[2]


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
        post=build_vk_source_envelope(
            {"id": 1, "date": 1, "text": "x", "attachments": []}, owner_id=1
        ),
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
    await vk_review.record_exact_parse_replay(
        db, source_packet_id=packet_id, prompt_version="p", model="m"
    )
    async with db.raw_conn() as conn:
        attempts = await (await conn.execute(
            "SELECT attempt_kind,llm_started,llm_completed,parse_key FROM vk_source_packet_attempt "
            "WHERE source_packet_id=? ORDER BY attempt_no",
            (packet_id,),
        )).fetchall()
    assert len(attempts) == 3
    assert attempts[0][0:3] == ("primary", 1, 1)
    assert attempts[0][3]
    assert attempts[1:] == [
        ("exact_replay", 0, 0, None),
        ("exact_replay", 0, 0, None),
    ]


@pytest.mark.asyncio
async def test_successful_parse_receipt_survives_later_terminal_carrier_failure(tmp_path):
    db = await _db(tmp_path)
    packet_id, _ = await vk_intake._persist_vk_source_packet(
        db,
        group_id=1,
        owner_type="group",
        post=build_vk_source_envelope(
            {"id": 2, "date": 1, "text": "event", "attachments": []},
            owner_id=1,
        ),
        source_url="https://vk.com/wall-1_2",
        keyword_hints=[],
        date_hints=[],
        event_ts_hint=None,
    )
    receipt = {
        "decision": SourceParseDecision(
            [{"title": "Event"}],
            disposition=SourceDisposition.EVENTS_FOUND,
            evidence_manifest=EvidenceManifest.complete_source("event"),
        ).to_payload(),
        "drafts": [{"title": "Event", "date": "2026-10-01"}],
    }
    await vk_review.record_source_parse_attempt(
        db,
        source_packet_id=packet_id,
        prompt_version="p",
        model="models/gemma-4-31b-it",
        evidence_manifest=EvidenceManifest.complete_source("event").to_payload(),
        parse_result=receipt,
        disposition="EVENTS_FOUND",
        retry_reason=None,
        event_child_count=1,
        lifecycle_action_count=0,
    )
    await vk_review.record_source_parse_attempt(
        db,
        source_packet_id=packet_id,
        prompt_version="p",
        model="models/gemma-4-31b-it",
        evidence_manifest=EvidenceManifest.complete_source("event").to_payload(),
        parse_result=None,
        disposition="RETRY_REQUIRED",
        retry_reason=SourceParseRetryReason.TECHNICAL_ERROR.value,
        event_child_count=0,
        lifecycle_action_count=0,
    )
    async with db.raw_conn() as conn:
        inbox_id = int(
            (await (await conn.execute(
                "SELECT id FROM vk_inbox WHERE source_packet_id=?",
                (packet_id,),
            )).fetchone())[0]
        )
    await vk_review.mark_carrier_outcome(
        db,
        inbox_id=inbox_id,
        outcome="FAILED_TECHNICAL",
        typed_reason="LIFECYCLE_NO_MATCH",
    )

    loaded = await vk_review.load_successful_parse_receipt(
        db,
        source_packet_id=packet_id,
        prompt_version="p",
        # Requested-model configuration may differ from the provider model
        # stored on the immutable receipt after large-carrier routing.
        model="gemini-3.1-flash-lite",
    )
    assert loaded == receipt
    await vk_review.record_exact_parse_replay(
        db,
        source_packet_id=packet_id,
        prompt_version="p",
        model="models/gemma-4-31b-it",
    )


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
async def test_typed_provider_receipt_terminalizes_and_persists_attempt_metadata(
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
    assert report.inbox_deferred == 0
    assert report.inbox_failed_technical == 1
    assert inbox == (
        "failed_technical",
        None,
        None,
        SourceParseRetryReason.TECHNICAL_ERROR.value,
    )
    assert attempt == (
        "primary", 1, 0, 0, "google:shared-project", "request-1",
        "RATE_LIMITED", 700, 1400, 3600,
        SourceParseRetryReason.TECHNICAL_ERROR.value,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure_kind", "error_code", "error"),
    [
        pytest.param("not_found", 100, "Post was deleted", id="deleted"),
        pytest.param("access_denied", 15, "Access denied", id="access-denied"),
        pytest.param("network_error", None, "connection reset", id="network-error"),
        pytest.param("vk_api_error", 6, "Too many requests", id="vk-api-error"),
    ],
)
async def test_any_fetch_failure_replays_complete_envelope_into_typed_parser(
    tmp_path, monkeypatch, failure_kind, error_code, error
):
    db = await _db(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'club1','source')"
        )
        await conn.commit()
    now = int(time.time())
    envelope = build_vk_source_envelope(
        {
            "id": 91,
            "date": now,
            "text": "outer event 12 August",
            "attachments": [],
            "copy_history": [
                {
                    "id": 92,
                    "text": "generic repost",
                    "attachments": [
                        {
                            "type": "link",
                            "link": {
                                "title": "Doors 19:00",
                                "url": "https://tickets.test/91",
                                "photo": {"sizes": [{"width": 1, "height": 1, "url": "https://img/91"}]},
                            },
                        }
                    ],
                }
            ],
        },
        owner_id=1,
    )
    packet_id, _ = await vk_intake._persist_vk_source_packet(
        db,
        group_id=1,
        owner_type="group",
        post=envelope,
        source_url="https://vk.com/wall-1_91",
        keyword_hints=[],
        date_hints=[],
        event_ts_hint=None,
    )
    post = await vk_review.pick_next(db, 7, "batch", resume_locked=False)
    assert post is not None and post.source_packet_id == packet_id

    async def unavailable(*_args, **_kwargs):
        return "", [], None, None, vk_auto_queue.VkFetchStatus(
            False, failure_kind, error_code=error_code, error=error
        )

    captured = {}

    async def typed_parse(text, *, photos, attachment_count_hint, **_kwargs):
        captured.update(
            text=text,
            photos=list(photos),
            attachment_count=attachment_count_hint,
        )
        decision = SourceParseDecision.retry(
            SourceParseRetryReason.TECHNICAL_ERROR,
            evidence_manifest=EvidenceManifest.complete_source(
                text, attachment_count=attachment_count_hint
            ),
        )
        return vk_intake.DraftParseResult([], decision=decision), None

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", unavailable)
    monkeypatch.setattr(vk_auto_queue.vk_intake, "build_event_drafts", typed_parse)
    report = vk_auto_queue.VkAutoImportReport(batch_id="batch")
    await vk_auto_queue._process_vk_inbox_row(
        db,
        _Bot(),
        chat_id=1,
        operator_id=7,
        batch_id="batch",
        post=post,
        source_url="https://vk.com/wall-1_91",
        report=report,
        festival_names=None,
        festival_alias_pairs=None,
        progress_message_id=None,
        progress_current_no=1,
        progress_total_txt="1",
    )
    assert "outer event" in captured["text"]
    assert "generic repost" in captured["text"]
    assert "Doors 19:00" in captured["text"]
    assert captured["photos"] == ["https://img/91"]
    assert captured["attachment_count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure_kind", "error_code"),
    [
        pytest.param("not_found", 100, id="deleted"),
        pytest.param("access_denied", 15, id="access-denied"),
        pytest.param("network_error", None, id="network-error"),
        pytest.param("vk_api_error", 6, id="vk-api-error"),
    ],
)
async def test_any_fetch_failure_keeps_legacy_packet_incomplete_without_parser(
    tmp_path, monkeypatch, failure_kind, error_code
):
    db = await _db(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'club1','source')"
        )
        await conn.commit()
    packet_id, _ = await vk_intake._persist_vk_source_packet(
        db,
        group_id=1,
        owner_type="group",
        post={"date": int(time.time()), "post_id": 92, "text": "legacy", "photos": []},
        source_url="https://vk.com/wall-1_92",
        keyword_hints=[],
        date_hints=[],
        event_ts_hint=None,
    )
    post = await vk_review.pick_next(db, 7, "batch", resume_locked=False)
    assert post is not None and post.source_packet_id == packet_id

    async def unavailable(*_args, **_kwargs):
        return "", [], None, None, vk_auto_queue.VkFetchStatus(
            False, failure_kind, error_code=error_code
        )

    async def must_not_parse(*_args, **_kwargs):
        raise AssertionError("legacy incomplete packet must not produce a semantic verdict")

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", unavailable)
    monkeypatch.setattr(vk_auto_queue.vk_intake, "build_event_drafts", must_not_parse)
    report = vk_auto_queue.VkAutoImportReport(batch_id="batch")
    await vk_auto_queue._process_vk_inbox_row(
        db,
        _Bot(),
        chat_id=1,
        operator_id=7,
        batch_id="batch",
        post=post,
        source_url="https://vk.com/wall-1_92",
        report=report,
        festival_names=None,
        festival_alias_pairs=None,
        progress_message_id=None,
        progress_current_no=1,
        progress_total_txt="1",
    )
    async with db.raw_conn() as conn:
        row = await (await conn.execute(
            "SELECT status,last_typed_reason FROM vk_inbox WHERE id=?", (post.id,)
        )).fetchone()
    assert report.inbox_deferred == 0
    assert report.inbox_failed_technical == 1
    assert row == ("failed_technical", "EVIDENCE_INCOMPLETE")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["network_error", "vk_api_error"])
async def test_fetch_failure_without_packet_uses_typed_terminal_not_stale_text(
    tmp_path, monkeypatch, failure_kind
):
    db = await _db(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'club1','source')"
        )
        await conn.execute(
            """
            INSERT INTO vk_inbox(
                id,group_id,post_id,date,text,has_date,status,owner_type
            ) VALUES(1,1,99,1,'cached text must not be parsed',0,'pending','group')
            """
        )
        await conn.commit()
    post = await vk_review.pick_next(db, 7, "batch", resume_locked=False)
    assert post is not None and post.source_packet_id is None

    async def unavailable(*_args, **_kwargs):
        return "", [], None, None, vk_auto_queue.VkFetchStatus(
            False, failure_kind, error_code=6, error="provider unavailable"
        )

    async def must_not_parse(*_args, **_kwargs):
        raise AssertionError("missing durable packet must not parse stale inbox text")

    # Even the legacy opt-in cannot replace a missing immutable revision.
    monkeypatch.setenv("VK_AUTO_IMPORT_ALLOW_STALE_INBOX_TEXT_ON_FETCH_FAIL", "1")
    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", unavailable)
    monkeypatch.setattr(vk_auto_queue.vk_intake, "build_event_drafts", must_not_parse)
    report = vk_auto_queue.VkAutoImportReport(batch_id="batch")
    await vk_auto_queue._process_vk_inbox_row(
        db,
        _Bot(),
        chat_id=1,
        operator_id=7,
        batch_id="batch",
        post=post,
        source_url="https://vk.com/wall-1_99",
        report=report,
        festival_names=None,
        festival_alias_pairs=None,
        progress_message_id=None,
        progress_current_no=1,
        progress_total_txt="1",
    )
    async with db.raw_conn() as conn:
        row = await (await conn.execute(
            "SELECT status,last_typed_reason FROM vk_inbox WHERE id=1"
        )).fetchone()
    assert report.inbox_failed == 1
    assert report.inbox_failed_technical == 1
    assert row == ("failed_technical", "SOURCE_FETCH_ERROR")


@pytest.mark.asyncio
async def test_fresh_fetch_persists_recursive_revision_before_typed_parser(tmp_path, monkeypatch):
    db = await _db(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'club1','source')"
        )
        await conn.commit()
    now = int(time.time())
    initial = build_vk_source_envelope(
        {"id": 93, "date": now, "text": "outer", "attachments": [], "copy_history": []},
        owner_id=1,
    )
    old_packet_id, _ = await vk_intake._persist_vk_source_packet(
        db,
        group_id=1,
        owner_type="group",
        post=initial,
        source_url="https://vk.com/wall-1_93",
        keyword_hints=[],
        date_hints=[],
        event_ts_hint=None,
    )
    post = await vk_review.pick_next(db, 7, "batch", resume_locked=False)
    assert post is not None
    edited = build_vk_source_envelope(
        {
            "id": 93,
            "date": now,
            "edited": now + 1,
            "text": "outer",
            "attachments": [],
            "copy_history": [
                {
                    "id": 94,
                    "text": "new nested event details",
                    "attachments": [
                        {"type": "doc", "doc": {"owner_id": -1, "id": 10, "title": "Program"}}
                    ],
                }
            ],
        },
        owner_id=1,
    )

    async def fetched(*_args, **_kwargs):
        return (
            edited["text"],
            edited["photos"],
            datetime.fromtimestamp(now, timezone.utc),
            None,
            vk_auto_queue.VkFetchStatus(True, "ok", source_envelope=edited),
        )

    observed = {}

    async def typed_parse(text, **_kwargs):
        async with db.raw_conn() as conn:
            row = await (await conn.execute(
                "SELECT source_packet_id FROM vk_inbox WHERE id=?", (post.id,)
            )).fetchone()
            observed["packet_id_during_parse"] = row[0]
        observed["text"] = text
        decision = SourceParseDecision.retry(
            SourceParseRetryReason.TECHNICAL_ERROR,
            evidence_manifest=EvidenceManifest.complete_source(text),
        )
        return vk_intake.DraftParseResult([], decision=decision), None

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fetched)
    monkeypatch.setattr(vk_auto_queue.vk_intake, "build_event_drafts", typed_parse)
    report = vk_auto_queue.VkAutoImportReport(batch_id="batch")
    await vk_auto_queue._process_vk_inbox_row(
        db,
        _Bot(),
        chat_id=1,
        operator_id=7,
        batch_id="batch",
        post=post,
        source_url="https://vk.com/wall-1_93",
        report=report,
        festival_names=None,
        festival_alias_pairs=None,
        progress_message_id=None,
        progress_current_no=1,
        progress_total_txt="1",
    )
    async with db.raw_conn() as conn:
        rows = await (await conn.execute(
            "SELECT id,revision,raw_text FROM vk_source_packet WHERE post_id=93 ORDER BY revision"
        )).fetchall()
    assert len(rows) == 2
    assert rows[0][0] == old_packet_id
    assert observed["packet_id_during_parse"] == rows[1][0]
    assert "new nested event details" in observed["text"]
    assert "Program" in observed["text"]


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
        await vk_review.mark_rejected(
            db, post.id, no_event_reason=SourceNoEventReason.OUT_OF_SCOPE
        )
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
    assert 'outcome="LIFECYCLE_NO_MATCH_NOOP"' in queue
    assert 'carrier_outcome = "MIXED_RESOLVED"' in queue
    ast.parse(intake)
    ast.parse(queue)
    ast.parse(review)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "case_id",
        "source_text",
        "incomplete",
        "events",
        "actions",
        "expected",
        "overlay_kwargs",
    ),
    [
        ("A", "Информационная справка без события", False, [], [], SourceDisposition.CONFIRMED_NO_EVENT, {}),
        ("B", "Расписание находится на недоступной карточке", True, [], [], SourceDisposition.RETRY_REQUIRED, {}),
        ("C", "Лекция 1 декабря, часть карточек недоступна", True, [{"title": "Лекция", "date": "2026-12-01"}], [], SourceDisposition.EVENTS_FOUND, {}),
        ("D", "Розыгрыш билетов: подпишись и сделай репост", False, [], [], SourceDisposition.CONFIRMED_NO_EVENT, {}),
        ("E", "Розыгрыш и концерт 1 декабря", False, [{"title": "Концерт", "date": "2026-12-01"}], [], SourceDisposition.EVENTS_FOUND, {}),
        ("F", "Скоро выставка, точную дату объявим позже", False, [], [], SourceDisposition.CONFIRMED_NO_EVENT, {}),
        (
            "G",
            "Лекция 1 декабря отменена",
            False,
            [],
            [LifecycleAction(LifecycleActionType.CANCEL, target_title="Лекция", evidence="отменена")],
            SourceDisposition.LIFECYCLE_ONLY,
            {},
        ),
        (
            "H",
            "Информационная справка без события",
            False,
            [],
            [],
            SourceDisposition.CONFIRMED_NO_EVENT,
            {"location_hint": "Научная библиотека"},
        ),
        (
            "I",
            "Информационная справка без события",
            False,
            [],
            [],
            SourceDisposition.CONFIRMED_NO_EVENT,
            {"default_ticket_link": "https://tickets.example/event"},
        ),
        ("J", "Только ссылка на другой канал с подробностями", False, [], [], SourceDisposition.CONFIRMED_NO_EVENT, {}),
        ("K", "Сдаётся зал в аренду для ваших мероприятий", False, [], [], SourceDisposition.CONFIRMED_NO_EVENT, {}),
        ("L", "Фотоотчёт о прошедшем концерте", False, [], [], SourceDisposition.CONFIRMED_NO_EVENT, {}),
        ("M", "Вакансия администратора без мероприятия", False, [], [], SourceDisposition.CONFIRMED_NO_EVENT, {}),
    ],
)
async def test_n9_vk_live_prompt_a_m_typed_provider_contract(
    monkeypatch,
    case_id,
    source_text,
    incomplete,
    events,
    actions,
    expected,
    overlay_kwargs,
):
    manifest = EvidenceManifest.complete_source(
        source_text,
        [] if incomplete else None,
        attachment_count=1 if incomplete else 0,
    )
    calls: list[str] = []
    emitted_payloads: list[dict] = []

    async def typed_provider(prompt, **_kwargs):
        calls.append(prompt)
        disposition = (
            SourceDisposition.MIXED
            if events and actions
            else SourceDisposition.EVENTS_FOUND
            if events
            else SourceDisposition.LIFECYCLE_ONLY
            if actions
            else SourceDisposition.CONFIRMED_NO_EVENT
        )
        if case_id == "D":
            payload = {
                "disposition": disposition.value,
                "events": list(events),
                "lifecycle_actions": [],
                "evidence_complete": manifest.evidence_complete,
                "parse_version": "source-parse-v1",
                "no_event_reason": "GIVEAWAY_ONLY",
            }
            emitted_payloads.append(payload)
            return payload
        reason = {
            "F": SourceNoEventReason.VAGUE_TEASER,
            "J": SourceNoEventReason.REFERRAL_ONLY,
            "K": SourceNoEventReason.SERVICE_OR_RENTAL,
            "L": SourceNoEventReason.RECAP_ONLY,
            "M": SourceNoEventReason.OUT_OF_SCOPE,
        }.get(
            case_id,
            SourceNoEventReason.NO_ATTENDABLE_EVENT
            if disposition is SourceDisposition.CONFIRMED_NO_EVENT
            else None,
        )
        return SourceParseDecision(
            events,
            disposition=disposition,
            lifecycle_actions=actions,
            evidence_manifest=manifest,
            evidence_complete=manifest.evidence_complete,
            no_event_reason=reason,
        )

    monkeypatch.setattr(main, "parse_event_via_llm", typed_provider)
    drafts, _festival = await vk_intake.build_event_drafts_from_vk(
        source_text,
        evidence_manifest=manifest,
        **overlay_kwargs,
    )

    assert len(calls) == 1, f"prompt case {case_id} must not enter a schema retry loop"
    assert drafts.disposition is expected
    assert drafts.retry_reason is not SourceParseRetryReason.SCHEMA_MISMATCH
    prompt = calls[0]
    for token in (
        "EVENTS_FOUND",
        "CONFIRMED_NO_EVENT",
        "LIFECYCLE_ONLY",
        "MIXED",
        "RETRY_REQUIRED",
        "EVIDENCE_INCOMPLETE",
    ):
        assert token in prompt
    assert "розыгрыш вместе с реальным событием" in prompt
    assert "Расплывчатый тизер" in prompt
    if case_id == "H":
        assert "Не создавай событие только из-за этого хинта" in prompt
    if case_id == "I":
        assert "если и только если это событие" in prompt
    if case_id == "D":
        assert emitted_payloads[0]["no_event_reason"] == "GIVEAWAY_ONLY"
        assert drafts.decision.no_event_reason is SourceNoEventReason.GIVEAWAY_ONLY
        assert (
            drafts.to_receipt_payload()["decision"]["no_event_reason"]
            == "GIVEAWAY_ONLY"
        )
    if case_id in {"A", "H", "I"}:
        assert drafts.decision.no_event_reason is SourceNoEventReason.NO_ATTENDABLE_EVENT
    if case_id == "F":
        assert drafts.decision.no_event_reason is SourceNoEventReason.VAGUE_TEASER
    if case_id in {"J", "K", "L", "M"}:
        expected_reason = {
            "J": SourceNoEventReason.REFERRAL_ONLY,
            "K": SourceNoEventReason.SERVICE_OR_RENTAL,
            "L": SourceNoEventReason.RECAP_ONLY,
            "M": SourceNoEventReason.OUT_OF_SCOPE,
        }[case_id]
        assert drafts.decision.no_event_reason is expected_reason
    if incomplete and events:
        assert drafts.enrichment_required is True
        assert len(drafts) == len(events)


@pytest.mark.asyncio
async def test_v1_vk_production_wiring_auto_verifies_strong_no_event_contradiction(monkeypatch):
    source = "Приглашаем на концерт 15.09 в 18:00, билеты доступны по ссылке"
    manifest = EvidenceManifest.complete_source(source)
    calls = []

    async def fake_gemma(text, source_channel=None, **kwargs):
        calls.append(kwargs)
        if kwargs.get("verification_request"):
            request = kwargs["verification_request"]
            assert request["source_text"] == source
            assert "NO_EVENT_WITH_STRONG_SIGNALS" in {
                fact["reason"] for fact in request["contradiction_facts"]
            }
            return SourceParseDecision(
                [{"title": "Концерт группы Север", "date": "2026-09-15", "time": "18:00"}],
                evidence_manifest=manifest,
            )
        return SourceParseDecision(
            [],
            disposition=SourceDisposition.CONFIRMED_NO_EVENT,
            no_event_reason=SourceNoEventReason.NO_ATTENDABLE_EVENT,
            evidence_manifest=manifest,
        )

    monkeypatch.setattr(main, "_parse_event_via_gemma", fake_gemma)
    monkeypatch.setenv("EVENT_PARSE_LARGE_POST_THRESHOLD_CHARS", "0")
    drafts, _ = await vk_intake.build_event_drafts_from_vk(
        source,
        evidence_manifest=manifest,
    )
    assert len(calls) == 2
    assert drafts.disposition is SourceDisposition.EVENTS_FOUND
    assert [draft.title for draft in drafts] == ["Концерт группы Север"]


@pytest.mark.asyncio
@pytest.mark.parametrize("raw_payload", [[], None, {"unexpected": "shape"}])
async def test_vk_untyped_empty_none_and_malformed_payloads_use_central_retry_adapter(
    monkeypatch, raw_payload, caplog
):
    calls = 0

    async def provider(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return raw_payload

    monkeypatch.setattr(main, "parse_event_via_llm", provider)
    with caplog.at_level("WARNING"):
        drafts, _ = await vk_intake.build_event_drafts_from_vk("source")
    assert calls == 1
    assert drafts.disposition is SourceDisposition.RETRY_REQUIRED
    assert drafts.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH
    assert "untyped/invalid source parse payload rejected" in caplog.text


@pytest.mark.asyncio
async def test_vk_legacy_positive_list_uses_central_validated_adapter(monkeypatch):
    async def provider(*_args, **_kwargs):
        return [{"title": "Лекция", "date": "2026-12-01"}]

    monkeypatch.setattr(main, "parse_event_via_llm", provider)
    drafts, _ = await vk_intake.build_event_drafts_from_vk("Лекция 1 декабря")
    assert len(drafts) == 1
    assert drafts.disposition is SourceDisposition.EVENTS_FOUND
    assert drafts.parse_version == "legacy-array-adapter-v1"


def test_draft_result_without_decision_is_schema_retry_not_no_event():
    result = vk_intake.DraftParseResult([])
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH
    assert result.evidence_manifest is None


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (lambda payload: payload.pop("decision"), "missing typed decision"),
        (lambda payload: payload["decision"].pop("evidence_manifest"), "missing evidence_manifest"),
        (lambda payload: payload["decision"].pop("disposition"), "missing disposition"),
        (lambda payload: payload["decision"].update(disposition="UNKNOWN"), "unknown disposition"),
        (
            lambda payload: payload["decision"].update(
                disposition="RETRY_REQUIRED", retry_reason="UNKNOWN"
            ),
            "unknown retry_reason",
        ),
        (
            lambda payload: payload["decision"].update(
                disposition="RETRY_REQUIRED", retry_reason=None
            ),
            "missing retry_reason",
        ),
        (
            lambda payload: payload["decision"].update(no_event_reason="UNKNOWN"),
            "unknown no_event_reason",
        ),
        (
            lambda payload: payload["decision"].update(
                no_event_reason="GIVEAWAY_ONLY"
            ),
            "only valid for CONFIRMED_NO_EVENT",
        ),
    ],
)
def test_receipt_a_f_invalid_typed_fields_force_invalidation_and_reparse(mutation, error):
    result = vk_intake.DraftParseResult(
        [vk_intake.EventDraft(title="Event")],
        decision=SourceParseDecision(
            [{"title": "Event"}],
            disposition=SourceDisposition.EVENTS_FOUND,
            evidence_manifest=EvidenceManifest.complete_source("Event"),
        ),
    )
    payload = result.to_receipt_payload()
    mutation(payload)
    with pytest.raises(ValueError, match=error):
        vk_intake.DraftParseResult.from_receipt_payload(payload)


@pytest.mark.parametrize(
    ("decision_events", "drafts", "error"),
    [
        ([{"title": "Other"}], [{"title": "Event"}], "title mismatch"),
        ([{"title": "Event"}, {"title": "Second"}], [{"title": "Event"}], "child count mismatch"),
    ],
)
def test_receipt_decision_and_draft_envelopes_must_correspond(
    decision_events, drafts, error
):
    payload = {
        "decision": SourceParseDecision(
            decision_events,
            disposition=SourceDisposition.EVENTS_FOUND,
            evidence_manifest=EvidenceManifest.complete_source("source"),
        ).to_payload(),
        "drafts": drafts,
    }
    with pytest.raises(ValueError, match=error):
        vk_intake.DraftParseResult.from_receipt_payload(payload)


@pytest.mark.asyncio
async def test_direct_poster_media_manifest_counts_missing_ocr_as_incomplete(monkeypatch):
    captured = {}

    async def provider(*_args, **kwargs):
        captured.update(kwargs["evidence_manifest"])
        return {
            "disposition": "RETRY_REQUIRED",
            "events": [],
            "lifecycle_actions": [],
            "evidence_complete": False,
            "parse_version": "source-parse-v1",
            "retry_reason": "EVIDENCE_INCOMPLETE",
        }

    monkeypatch.setattr(main, "parse_event_via_llm", provider)
    drafts, _ = await vk_intake.build_event_drafts_from_vk(
        "caption",
        poster_media=[PosterMedia(data=b"image", name="poster.jpg")],
    )
    assert captured["attachment_count"] == 1
    assert captured["ocr_blocks_available"] == 0
    assert captured["unavailable_attachment_count"] == 1
    assert captured["evidence_complete"] is False
    assert drafts.retry_reason is SourceParseRetryReason.EVIDENCE_INCOMPLETE


@pytest.mark.asyncio
async def test_vk_build_without_photos_has_explicit_ocr_state(tmp_path, monkeypatch):
    db = await _db(tmp_path)

    async def recognize(_db, photo_bytes, **_kwargs):
        assert photo_bytes == []
        return [], 0, None

    async def provider(*_args, **_kwargs):
        return {
            "disposition": "CONFIRMED_NO_EVENT",
            "events": [],
            "lifecycle_actions": [],
            "evidence_complete": True,
            "parse_version": "source-parse-v1",
            "no_event_reason": "NO_ATTENDABLE_EVENT",
        }

    monkeypatch.setattr(vk_intake.poster_ocr, "recognize_posters", recognize)
    monkeypatch.setattr(main, "parse_event_via_llm", provider)
    drafts, _ = await vk_intake.build_event_drafts("plain text", photos=[], db=db)
    assert drafts.disposition is SourceDisposition.CONFIRMED_NO_EVENT
    await db.close()


@pytest.mark.asyncio
async def test_successful_blank_ocr_counts_as_complete_evidence(tmp_path, monkeypatch):
    db = await _db(tmp_path)
    captured = {}

    async def download(_urls):
        return [(b"blank-image", "blank.jpg")]

    async def media(_items, **_kwargs):
        return [PosterMedia(data=b"blank-image", name="blank.jpg")], None

    async def recognize(_db, _items, **_kwargs):
        return [
            PosterOcrCache(
                hash="blank",
                detail="auto",
                model="mock",
                text="",
                title="",
            )
        ], 0, 100

    async def provider(*_args, **kwargs):
        captured.update(kwargs["evidence_manifest"])
        return {
            "disposition": "CONFIRMED_NO_EVENT",
            "events": [],
            "lifecycle_actions": [],
            "evidence_complete": True,
            "parse_version": "source-parse-v1",
            "no_event_reason": "NO_ATTENDABLE_EVENT",
        }

    monkeypatch.setattr(vk_intake, "_download_photo_media", download)
    monkeypatch.setattr(vk_intake, "process_media", media)
    monkeypatch.setattr(vk_intake.poster_ocr, "recognize_posters", recognize)
    monkeypatch.setattr(main, "parse_event_via_llm", provider)
    drafts, _ = await vk_intake.build_event_drafts(
        "archival venue photo",
        photos=["https://example.test/blank.jpg"],
        db=db,
    )

    assert drafts.disposition is SourceDisposition.CONFIRMED_NO_EVENT
    assert captured["attachment_count"] == 1
    assert captured["ocr_blocks_available"] == 1
    assert captured["ocr_blocks_included"] == 1
    assert captured["unavailable_attachment_count"] == 0
    assert captured["ocr_complete"] is True
    assert captured["evidence_complete"] is True


@pytest.mark.parametrize("legacy", [[], None, "malformed"])
def test_vk_queue_legacy_empty_none_and_malformed_results_are_retryable(legacy):
    result = vk_auto_queue._adapt_vk_draft_result(legacy, source_text="source")
    assert result.disposition is SourceDisposition.RETRY_REQUIRED
    assert result.retry_reason is SourceParseRetryReason.SCHEMA_MISMATCH


def test_vk_queue_legacy_positive_receipt_strips_poster_objects_and_serializes():
    draft = vk_intake.EventDraft(title="Event", date="2026-12-01")
    draft.poster_media = [PosterMedia(data=b"image", name="poster.jpg")]
    result = vk_auto_queue._adapt_vk_draft_result([draft], source_text="source")
    assert result.disposition is SourceDisposition.EVENTS_FOUND
    encoded = json.dumps(result.to_receipt_payload(), ensure_ascii=False)
    assert "poster_media" not in encoded
    assert "Event" in encoded


@pytest.mark.asyncio
async def test_load_successful_receipt_invalidates_old_untyped_terminal(tmp_path, caplog):
    db = await _db(tmp_path)
    packet_id, _ = await vk_intake._persist_vk_source_packet(
        db,
        group_id=1,
        owner_type="group",
        post={"date": 1, "post_id": 77, "text": "source", "photos": []},
        source_url="https://vk.com/wall-1_77",
        keyword_hints=[],
        date_hints=[],
        event_ts_hint=None,
    )
    # This is shaped like the historical terminal receipt which omitted the
    # source decision. It may be reparsed, but can never be an exact replay.
    await vk_review.record_source_parse_attempt(
        db,
        source_packet_id=packet_id,
        prompt_version="p",
        model="m",
        evidence_manifest=EvidenceManifest.complete_source("source").to_payload(),
        parse_result={"drafts": []},
        disposition="CONFIRMED_NO_EVENT",
        retry_reason=None,
        event_child_count=0,
        lifecycle_action_count=0,
    )
    with caplog.at_level("WARNING"):
        loaded = await vk_review.load_successful_parse_receipt(
            db, source_packet_id=packet_id, prompt_version="p", model="m"
        )
    assert loaded is None
    assert "source_parse_schema_alert" in caplog.text
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT structured_response_valid,no_event_reason FROM vk_source_packet_attempt WHERE source_packet_id=?",
                (packet_id,),
            )
        ).fetchone()
    assert tuple(row) == (0, None)


@pytest.mark.asyncio
async def test_n7_vk_terminal_rejection_requires_closed_reason_and_persists_typed_value(tmp_path):
    db = await _db(tmp_path)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_inbox(group_id,post_id,date,text,has_date,status) VALUES(1,1,1,'x',0,'pending')"
        )
        await conn.commit()
        inbox_id = int((await (await conn.execute("SELECT id FROM vk_inbox")).fetchone())[0])

    with pytest.raises(ValueError, match="closed no_event_reason"):
        await vk_review.mark_rejected(db, inbox_id, no_event_reason="UNKNOWN")

    await vk_review.mark_rejected(
        db,
        inbox_id,
        no_event_reason=SourceNoEventReason.NO_ATTENDABLE_EVENT,
    )
    async with db.raw_conn() as conn:
        value = (await (await conn.execute(
            "SELECT last_typed_reason FROM vk_inbox WHERE id=?", (inbox_id,)
        )).fetchone())[0]
    assert value == "CONFIRMED_NO_EVENT:NO_ATTENDABLE_EVENT"


@pytest.mark.asyncio
async def test_n7_valid_no_event_reason_is_durable_on_parse_attempt(tmp_path):
    db = await _db(tmp_path)
    packet_id, _ = await vk_intake._persist_vk_source_packet(
        db,
        group_id=1,
        owner_type="group",
        post={"date": 1, "post_id": 88, "text": "recap", "photos": []},
        source_url="https://vk.com/wall-1_88",
        keyword_hints=[],
        date_hints=[],
        event_ts_hint=None,
    )
    manifest = EvidenceManifest.complete_source("recap")
    decision = SourceParseDecision(
        [],
        disposition=SourceDisposition.CONFIRMED_NO_EVENT,
        no_event_reason=SourceNoEventReason.RECAP_ONLY,
        evidence_manifest=manifest,
    )
    receipt = vk_intake.DraftParseResult([], decision=decision).to_receipt_payload()
    await vk_review.record_source_parse_attempt(
        db,
        source_packet_id=packet_id,
        prompt_version="p",
        model="m",
        evidence_manifest=manifest.to_payload(),
        parse_result=receipt,
        disposition="CONFIRMED_NO_EVENT",
        retry_reason=None,
        no_event_reason="RECAP_ONLY",
        event_child_count=0,
        lifecycle_action_count=0,
    )
    async with db.raw_conn() as conn:
        row = await (await conn.execute(
            "SELECT structured_response_valid,no_event_reason FROM vk_source_packet_attempt WHERE source_packet_id=?",
            (packet_id,),
        )).fetchone()
    assert tuple(row) == (1, "RECAP_ONLY")


@pytest.mark.asyncio
async def test_v10_verifier_timeout_is_recorded_as_durable_retry(tmp_path):
    db = await _db(tmp_path)
    packet_id, _ = await vk_intake._persist_vk_source_packet(
        db,
        group_id=1,
        owner_type="group",
        post={"date": 1, "post_id": 89, "text": "source", "photos": []},
        source_url="https://vk.com/wall-1_89",
        keyword_hints=[],
        date_hints=[],
        event_ts_hint=None,
    )
    await vk_review.record_source_parse_attempt(
        db,
        source_packet_id=packet_id,
        prompt_version="p",
        model="m",
        evidence_manifest=EvidenceManifest.complete_source("source").to_payload(),
        parse_result=None,
        disposition="RETRY_REQUIRED",
        retry_reason="VERIFICATION_TECHNICAL_ERROR",
        event_child_count=1,
        lifecycle_action_count=0,
        attempt_kind="verification",
        verification_triggered=True,
        verification_reason="EVENT_DATE_CONFLICT",
        verification_disposition="RETRY_REQUIRED",
    )
    async with db.raw_conn() as conn:
        attempt = await (await conn.execute(
            "SELECT attempt_kind,structured_response_valid,typed_error_reason,verification_reason "
            "FROM vk_source_packet_attempt WHERE source_packet_id=?",
            (packet_id,),
        )).fetchone()
        packet = await (await conn.execute(
            "SELECT llm_status,last_typed_reason FROM vk_source_packet WHERE id=?",
            (packet_id,),
        )).fetchone()
    assert tuple(attempt) == (
        "verification", 0, "VERIFICATION_TECHNICAL_ERROR", "EVENT_DATE_CONFLICT"
    )
    assert tuple(packet) == ("retry_scheduled", "VERIFICATION_TECHNICAL_ERROR")
    await db.close()
