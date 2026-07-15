from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from db import Database
from event_age_bge_service import (
    apply_event_age_bge_report,
    build_event_age_bge_input,
    collect_event_age_bge_inputs,
    current_assessment_policy_version,
)
from models import Event, EventPoster, EventSource


def make_event(**overrides) -> Event:
    values = {
        "id": 10,
        "title": "Спектакль",
        "description": "Драма о взрослении",
        "date": "2026-08-20",
        "time": "19:00",
        "location_name": "Театр",
        "source_text": "Источник",
        "added_at": datetime(2026, 7, 15, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return Event(**values)


def test_corpus_prioritizes_approved_ocr_title_and_body_and_excludes_rejected():
    event = make_event()
    sources = [
        EventSource(
            id=1,
            event_id=10,
            source_type="telegram",
            source_url="https://t.me/example/1",
            source_text="Подробное описание",
        )
    ]
    posters = [
        EventPoster(
            id=1,
            event_id=10,
            poster_hash="approved",
            review_status="approved",
            media_role="event_identity_poster",
            ocr_title="ТОЛЬКО ДЛЯ ВЗРОСЛЫХ",
            ocr_text="Возрастное ограничение 18+",
        ),
        EventPoster(
            id=2,
            event_id=10,
            poster_hash="rejected",
            review_status="rejected",
            media_role="event_identity_poster",
            ocr_text="Детям 0+",
        ),
    ]
    row = build_event_age_bge_input(event, sources=sources, posters=posters)
    assert row.ocr_coverage == "complete"
    assert row.poster_ocr_count == 1
    assert "ТОЛЬКО ДЛЯ ВЗРОСЛЫХ" in row.text
    assert "Возрастное ограничение 18+" in row.text
    assert "Детям 0+" not in row.text
    assert row.text.index("[EVENT_POSTER_OCR]") < row.text.index("[DESCRIPTION]")


def test_ocr_missing_is_pending_then_terminal_not_silent():
    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    poster = EventPoster(
        event_id=10,
        poster_hash="missing",
        review_status="approved",
        media_role="event_identity_poster",
    )
    pending = build_event_age_bge_input(
        make_event(added_at=now - timedelta(minutes=30)),
        sources=[],
        posters=[poster],
        now=now,
    )
    terminal = build_event_age_bge_input(
        make_event(added_at=now - timedelta(hours=3)),
        sources=[],
        posters=[poster],
        now=now,
    )
    assert pending.ocr_coverage == "pending"
    assert terminal.ocr_coverage == "terminal_unavailable"
    assert pending.input_hash != terminal.input_hash


@pytest.mark.asyncio
async def test_selector_is_missing_only_and_terminal_report_is_idempotent(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "age-bge.sqlite"))
    await db.init()
    now = datetime(2026, 7, 15, 12, tzinfo=timezone.utc)
    async with db.get_session() as session:
        missing = make_event(id=None, added_at=now - timedelta(hours=3))
        declared = make_event(
            id=None,
            title="Объявленный",
            age_restriction="12+",
            age_restriction_status="declared",
        )
        session.add(missing)
        session.add(declared)
        await session.commit()
        missing_id = int(missing.id or 0)
    rows, stats = await collect_event_age_bge_inputs(db, now=now)
    assert [row.event_id for row in rows] == [missing_id]
    assert stats["selected"] == 1
    row = rows[0]
    report = {
        "schema_version": "event-age-bge-shadow-v1",
        "assessment_policy_version": current_assessment_policy_version(),
        "run_id": "test-run",
        "model_revision": "revision",
        "encoder_contract": "bge_m3_cpu_dense_retrieval_v1",
        "classifier_sha256": None,
        "evaluation_approval_status": "missing",
        "results": [
            {
                "event_id": missing_id,
                "input_hash": row.input_hash,
                "status": "insufficient_evidence",
            }
        ],
    }
    imported = await apply_event_age_bge_report(db, report, now=now)
    assert imported["terminal_unrateable"] == 1
    second, second_stats = await collect_event_age_bge_inputs(db, now=now)
    assert second == []
    assert second_stats["current"] == 1
    async with db.get_session() as session:
        stored = await session.get(Event, missing_id)
        assert stored is not None
        assert stored.age_assessment is None
        assert stored.age_assessment_status == "insufficient_evidence"
        assert stored.age_assessment_run_id == "test-run"
    monkeypatch.setenv("EVENT_AGE_BGE_CLASSIFIER_SHA256", "new-classifier")
    refreshed, _ = await collect_event_age_bge_inputs(db, now=now)
    assert [item.event_id for item in refreshed] == [missing_id]
    await db.close()


@pytest.mark.asyncio
async def test_import_rejects_stale_or_ungated_assessment(tmp_path):
    db = Database(str(tmp_path / "age-bge-import.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = make_event(id=None)
        session.add(event)
        await session.commit()
        event_id = int(event.id or 0)
    rows, _ = await collect_event_age_bge_inputs(db)
    report = {
        "schema_version": "event-age-bge-shadow-v1",
        "assessment_policy_version": current_assessment_policy_version(),
        "run_id": "test-run",
        "model_revision": "revision",
        "encoder_contract": "bge_m3_cpu_dense_retrieval_v1",
        "classifier_sha256": "abc",
        "evaluation_approval_status": "shadow",
        "results": [
            {
                "event_id": event_id,
                "input_hash": rows[0].input_hash,
                "status": "assessed",
                "age_assessment": "16+",
                "model_revision": "revision",
                "encoder_contract": "bge_m3_cpu_dense_retrieval_v1",
                "classifier_sha256": "abc",
            }
        ],
    }
    counts = await apply_event_age_bge_report(db, report)
    assert counts["invalid"] == 1
    async with db.get_session() as session:
        stored = await session.get(Event, event_id)
        assert stored and stored.age_assessment is None
    await db.close()


@pytest.mark.asyncio
async def test_new_hash_abstention_clears_old_numeric_assessment(tmp_path):
    db = Database(str(tmp_path / "age-bge-stale-assessment.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = make_event(
            id=None,
            age_assessment="6+",
            age_assessment_status="assessed",
            age_assessment_provenance="bge_assessed",
            age_assessment_input_hash="old-hash",
            age_restriction_status="assessed",
        )
        session.add(event)
        await session.commit()
        event_id = int(event.id or 0)
    rows, _ = await collect_event_age_bge_inputs(db)
    assert len(rows) == 1
    report = {
        "schema_version": "event-age-bge-shadow-v1",
        "assessment_policy_version": current_assessment_policy_version(),
        "run_id": "new-run",
        "model_revision": "revision",
        "encoder_contract": "bge_m3_cpu_dense_retrieval_v1",
        "classifier_sha256": None,
        "evaluation_approval_status": "missing",
        "results": [
            {
                "event_id": event_id,
                "input_hash": rows[0].input_hash,
                "status": "insufficient_evidence",
            }
        ],
    }
    await apply_event_age_bge_report(db, report)
    async with db.get_session() as session:
        stored = await session.get(Event, event_id)
        assert stored is not None
        assert stored.age_assessment is None
        assert stored.age_assessment_input_hash == rows[0].input_hash
        assert stored.age_assessment_status == "insufficient_evidence"
        assert stored.age_restriction_status == "insufficient_evidence"
    await db.close()


@pytest.mark.asyncio
async def test_smart_update_fanout_coalesces_age_batch_for_25_minutes(tmp_path, monkeypatch):
    import main

    db = Database(str(tmp_path / "age-schedule.sqlite"))
    await db.init()
    async with db.get_session() as session:
        event = make_event(id=None)
        session.add(event)
        await session.commit()
        await session.refresh(event)
    calls: list[tuple] = []

    async def fake_enqueue(_db, event_id, task, **kwargs):
        calls.append((event_id, task, kwargs))
        return "queued"

    before = datetime.now(timezone.utc)
    monkeypatch.setenv("ENABLE_EVENT_AGE_BGE_ASSESSMENT", "1")
    monkeypatch.setenv("EVENT_AGE_BGE_DEBOUNCE_SECONDS", "1500")
    monkeypatch.delenv("ENABLE_EVENT_VECTOR_SYNC", raising=False)
    monkeypatch.setattr(main, "enqueue_job", fake_enqueue)
    monkeypatch.setattr(main, "DISABLE_PAGE_JOBS", True)
    await main.schedule_event_update_tasks(db, event, skip_vk_sync=True)
    age_calls = [call for call in calls if call[1] == main.JobTask.event_age_bge_assessment]
    assert len(age_calls) == 1
    kwargs = age_calls[0][2]
    assert kwargs["coalesce_key"] == "event_age_bge_assessment:prod"
    assert kwargs["payload"]["reason"] == "smart_update"
    delay = (kwargs["next_run_at"] - before).total_seconds()
    assert 1499 <= delay <= 1502
    await db.close()


@pytest.mark.asyncio
async def test_ocr_pending_batch_always_schedules_recheck(tmp_path, monkeypatch):
    import event_age_bge_service
    import main

    db = Database(str(tmp_path / "age-ocr-recheck.sqlite"))
    await db.init()

    async def fake_run(_db):
        return {
            "status": "empty",
            "selection": {"selected": 0, "ocr_pending": 2},
            "import": {},
        }

    calls: list[dict] = []

    async def fake_enqueue(*_args, **kwargs):
        calls.append(kwargs)
        return "queued"

    monkeypatch.setattr(event_age_bge_service, "run_event_age_bge_batch", fake_run)
    monkeypatch.setattr(main, "enqueue_job", fake_enqueue)
    monkeypatch.setenv("EVENT_AGE_BGE_OCR_RECHECK_SECONDS", "1800")
    before = datetime.now(timezone.utc)
    assert not await main.job_event_age_bge_assessment(1, db, None)
    assert len(calls) == 1
    assert calls[0]["payload"]["reason"] == "ocr_recheck"
    delay = (calls[0]["next_run_at"] - before).total_seconds()
    assert 1799 <= delay <= 1802
    await db.close()


@pytest.mark.asyncio
async def test_startup_seed_catches_preexisting_not_scheduled_event(tmp_path, monkeypatch):
    import main

    db = Database(str(tmp_path / "age-startup.sqlite"))
    await db.init()
    async with db.get_session() as session:
        terminal = make_event(id=None, title="terminal", age_assessment_status="insufficient_evidence")
        missing = make_event(id=None, title="missing")
        session.add_all([terminal, missing])
        await session.commit()
        missing_id = int(missing.id or 0)
    calls: list[tuple] = []

    async def fake_enqueue(*args, **kwargs):
        calls.append((args, kwargs))
        return "new"

    monkeypatch.setenv("ENABLE_EVENT_AGE_BGE_ASSESSMENT", "1")
    monkeypatch.setattr(main, "enqueue_job", fake_enqueue)
    assert await main.seed_event_age_bge_backlog(db) == "new"
    assert len(calls) == 1
    assert calls[0][0][1] == missing_id
    assert calls[0][1]["payload"]["reason"] == "startup_backlog"
    await db.close()
