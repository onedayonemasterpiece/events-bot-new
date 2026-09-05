import os
import sys
import json
import logging
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pytest
import pytest_asyncio

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import main
from main import Database
from ops_run import start_ops_run as real_start_ops_run

import vk_intake
import vk_auto_queue
import poster_ocr
from poster_media import PosterMedia
from source_parsing.handlers import AddedEventInfo
from smart_event_update import SmartUpdateResult, SmartUpdateTerminalOutcome
from smart_update_state import ProductExclusionReason
from source_parse_contract import (
    EvidenceManifest,
    LifecycleAction,
    LifecycleActionType,
    SourceDisposition,
    SourceNoEventReason,
    SourceParseDecision,
    SourceParseRetryReason,
)


@pytest_asyncio.fixture(autouse=True)
async def _dispose_test_databases(monkeypatch):
    """Close SQLAlchemy/aiosqlite workers created by every test in this module."""

    instances: list[Database] = []
    original_init = Database.__init__

    def tracked_init(instance, *args, **kwargs):
        original_init(instance, *args, **kwargs)
        instances.append(instance)

    monkeypatch.setattr(Database, "__init__", tracked_init)
    yield
    for instance in instances:
        await instance.close()


class DummyBot:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str]] = []

    async def send_message(self, chat_id, text, **_kwargs):
        self.messages.append((int(chat_id), str(text)))

    async def get_me(self):
        class Me:
            username = "eventsbotTestBot"
        return Me()


def test_schedule_cards_use_expanded_bounded_photo_cap(monkeypatch):
    monkeypatch.setenv("VK_AUTO_IMPORT_MAX_PHOTOS", "4")
    monkeypatch.setenv("VK_AUTO_IMPORT_SCHEDULE_MAX_PHOTOS", "10")
    text = (
        "С 1 по 9 августа пройдут соревнования. "
        "Расписание и места проведения – в карточках."
    )
    assert vk_auto_queue._vk_auto_import_photo_limit_for_text(text) == 100


def test_ordinary_vk_gallery_keeps_all_evidence(monkeypatch):
    monkeypatch.setenv("VK_AUTO_IMPORT_MAX_PHOTOS", "4")
    monkeypatch.setenv("VK_AUTO_IMPORT_SCHEDULE_MAX_PHOTOS", "10")
    assert vk_auto_queue._vk_auto_import_photo_limit_for_text("Фото с открытия выставки") == 100


@pytest.mark.asyncio
async def test_vk_fetch_transient_failure_retries_only_inline(monkeypatch):
    calls = 0

    async def fake_fetch(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return "", [], None, None, vk_auto_queue.VkFetchStatus(False, "network_error")
        return "event", [], None, None, vk_auto_queue.VkFetchStatus(True, "ok")

    async def no_wait(_seconds):
        return None

    monkeypatch.setenv("VK_AUTO_IMPORT_FETCH_INLINE_ATTEMPTS", "2")
    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_auto_queue.asyncio, "sleep", no_wait)

    result = await vk_auto_queue._fetch_vk_post_with_inline_retry(1, 2)

    assert calls == 2
    assert result[0] == "event"
    assert result[4].ok is True


@pytest.mark.asyncio
async def test_failed_technical_clears_packet_and_inbox_due_state(tmp_path):
    db = Database(str(tmp_path / "terminal-clears-due.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'club1','Source')"
        )
        await conn.commit()
    packet_id, _ = await vk_intake._persist_vk_source_packet(
        db,
        group_id=1,
        owner_type="group",
        post={"date": 1, "post_id": 2, "text": "event", "photos": []},
        source_url="https://vk.com/wall-1_2",
        keyword_hints=(),
        date_hints=(),
        event_ts_hint=None,
    )
    async with db.raw_conn() as conn:
        inbox_id = (
            await (
                await conn.execute(
                    "SELECT id FROM vk_inbox WHERE source_packet_id=?", (packet_id,)
                )
            ).fetchone()
        )[0]
    await vk_auto_queue.vk_review.schedule_retry(
        db, inbox_id, typed_reason="RATE_LIMITED", batch_id="old-auto"
    )
    await vk_auto_queue.vk_review.mark_carrier_outcome(
        db,
        inbox_id=inbox_id,
        outcome="FAILED_TECHNICAL",
        typed_reason="RATE_LIMITED",
    )

    async with db.raw_conn() as conn:
        inbox = await (
            await conn.execute(
                "SELECT status,next_attempt_at FROM vk_inbox WHERE id=?", (inbox_id,)
            )
        ).fetchone()
        packet = await (
            await conn.execute(
                "SELECT status,llm_status,next_attempt_at,terminal_carrier_outcome "
                "FROM vk_source_packet WHERE id=?",
                (packet_id,),
            )
        ).fetchone()
    assert inbox == ("failed_technical", None)
    assert packet[0:2] == ("failed_technical", "failed_technical")
    # The immutable packet schema keeps a NOT NULL timestamp, but its terminal
    # status makes that value inert; the selectable inbox projection is clear.
    assert packet[2] is not None
    assert packet[3] == "FAILED_TECHNICAL"


@pytest.mark.asyncio
async def test_vk_auto_import_scheduler_uses_db_superadmin_when_env_missing(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            'INSERT INTO "user"(user_id, username, is_superadmin, blocked) VALUES(?, ?, 1, 0)',
            (185169715, "max"),
        )
        await conn.commit()

    monkeypatch.setenv("ENABLE_VK_AUTO_IMPORT", "1")
    monkeypatch.delenv("ADMIN_CHAT_ID", raising=False)

    captured: dict[str, object] = {}

    async def fake_run(_db, _bot, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(vk_auto_queue, "run_vk_auto_import", fake_run)

    bot = DummyBot()
    await vk_auto_queue.vk_auto_import_scheduler(db, bot, run_id="sched-missing-admin")

    assert captured["chat_id"] == 185169715
    assert captured["trigger"] == "scheduled"
    assert captured["operator_id"] == 0
    assert captured["run_id"] == "sched-missing-admin"
    assert int(captured["ops_run_id"]) > 0
    assert bot.messages == []


@pytest.mark.asyncio
async def test_vk_auto_import_scheduler_records_missing_superadmin_chat_skip(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    monkeypatch.setenv("ENABLE_VK_AUTO_IMPORT", "1")
    monkeypatch.delenv("ADMIN_CHAT_ID", raising=False)

    bot = DummyBot()
    await vk_auto_queue.vk_auto_import_scheduler(db, bot, run_id="sched-missing-admin")

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT trigger, status, details_json FROM ops_run WHERE kind='vk_auto_import' ORDER BY id ASC"
        )
        row = await cur.fetchone()

    assert row is not None
    trigger, status, details_raw = row
    details = json.loads(details_raw)
    assert trigger == "scheduled"
    assert status == "skipped"
    assert details["skip_reason"] == "missing_superadmin_chat"
    assert details["run_id"] == "sched-missing-admin"
    assert bot.messages == []


@pytest.mark.asyncio
async def test_vk_auto_import_scheduler_records_error_when_runner_crashes(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            'INSERT INTO "user"(user_id, username, is_superadmin, blocked) VALUES(?, ?, 1, 0)',
            (185169715, "max"),
        )
        await conn.commit()

    monkeypatch.setenv("ENABLE_VK_AUTO_IMPORT", "1")

    captured: dict[str, object] = {}

    async def fake_run(_db, _bot, **kwargs):
        captured.update(kwargs)
        raise RuntimeError("queue exploded")

    monkeypatch.setattr(vk_auto_queue, "run_vk_auto_import", fake_run)

    bot = DummyBot()
    await vk_auto_queue.vk_auto_import_scheduler(db, bot, run_id="sched-runner-crash")

    assert int(captured["ops_run_id"]) > 0
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, details_json FROM ops_run WHERE kind='vk_auto_import' ORDER BY id ASC"
        )
        row = await cur.fetchone()

    assert row is not None
    status, details_raw = row
    details = json.loads(details_raw)
    assert status == "error"
    assert details["run_id"] == "sched-runner-crash"
    assert "queue exploded" in details["fatal_error"]


@pytest.mark.asyncio
async def test_run_vk_auto_import_uses_existing_ops_run_id(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    ops_run_id = await real_start_ops_run(
        db,
        kind="vk_auto_import",
        trigger="scheduled",
        operator_id=0,
        details={"run_id": "sched-existing"},
    )

    async def fake_start_ops_run(*_args, **_kwargs):
        raise AssertionError("start_ops_run should not be called when ops_run_id is provided")

    async def fake_pick_next(*_args, **_kwargs):
        return None

    monkeypatch.setattr(vk_auto_queue, "start_ops_run", fake_start_ops_run)
    monkeypatch.setattr(vk_auto_queue.vk_review, "pick_next", fake_pick_next)

    bot = DummyBot()
    await vk_auto_queue.run_vk_auto_import(
        db,
        bot,
        chat_id=1,
        limit=1,
        operator_id=123,
        trigger="scheduled",
        run_id="sched-existing",
        ops_run_id=ops_run_id,
    )

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT id, status, details_json FROM ops_run WHERE kind='vk_auto_import' ORDER BY id ASC"
        )
        rows = await cur.fetchall()

    assert len(rows) == 1
    row_id, status, details_raw = rows[0]
    details = json.loads(details_raw)
    assert int(row_id) == int(ops_run_id)
    assert status == "success"
    assert details["run_id"] == "sched-existing"


@pytest.mark.asyncio
async def test_manual_vk_auto_import_does_not_wait_for_heavy_gate_by_default(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async def fake_pick_next(*_args, **_kwargs):
        return None

    def forbidden_heavy_operation(**_kwargs):
        raise AssertionError("manual vk_auto_import should not enter heavy gate by default")

    class BusyMeta:
        kind = "tg_monitoring"
        trigger = "scheduled"
        run_id = "tg-run"
        operator_id = 0

    monkeypatch.delenv("VK_AUTO_IMPORT_HEAVY_MODE", raising=False)
    monkeypatch.setattr(vk_auto_queue.vk_review, "pick_next", fake_pick_next)
    monkeypatch.setattr(vk_auto_queue, "current_heavy_meta", lambda: BusyMeta())
    monkeypatch.setattr(vk_auto_queue, "heavy_operation", forbidden_heavy_operation)

    bot = DummyBot()
    report = await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=1, operator_id=123)

    assert bot.messages
    assert all("ждёт завершения другой тяжёлой операции" not in text for _, text in bot.messages)


@pytest.mark.asyncio
async def test_vk_auto_import_wait_mode_reports_heavy_gate_wait(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async def fake_pick_next(*_args, **_kwargs):
        return None

    @asynccontextmanager
    async def fake_heavy_operation(**_kwargs):
        yield True

    class BusyMeta:
        kind = "tg_monitoring"
        trigger = "scheduled"
        run_id = "tg-run"
        operator_id = 0

    monkeypatch.setenv("VK_AUTO_IMPORT_HEAVY_MODE", "wait")
    monkeypatch.setattr(vk_auto_queue.vk_review, "pick_next", fake_pick_next)
    monkeypatch.setattr(vk_auto_queue, "current_heavy_meta", lambda: BusyMeta())
    monkeypatch.setattr(vk_auto_queue, "describe_heavy_meta", lambda _meta: "tg_monitoring (scheduled)")
    monkeypatch.setattr(vk_auto_queue, "heavy_operation", fake_heavy_operation)

    bot = DummyBot()
    await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=1, operator_id=123)

    assert any("ждёт завершения другой тяжёлой операции" in text for _, text in bot.messages)


@pytest.mark.asyncio
async def test_vk_auto_import_terminalizes_row_timeout_without_retry(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "Test Community", "Научная библиотека", None, None),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, event_ts_hint, status) VALUES(?,?,?,?,?,?,?,?,?)",
            (1, 1, 100, 0, "stub", vk_intake.OCR_PENDING_SENTINEL, 0, None, "pending"),
        )
        await conn.commit()

    monkeypatch.setenv("VK_AUTO_IMPORT_ROW_TIMEOUT_SEC", "0.01")

    async def fake_process(*_args, **_kwargs):
        await asyncio.sleep(0.05)

    monkeypatch.setattr(vk_auto_queue, "_process_vk_inbox_row", fake_process)

    bot = DummyBot()
    report = await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=1, operator_id=123)

    assert report.inbox_failed == 1
    assert report.inbox_failed_technical == 1
    assert report.inbox_deferred == 0
    assert any(
        "failed_technical https://vk.com/wall-1_100: ROW_TIMEOUT" in err
        for err in report.errors
    )
    assert any("таймаут обработки поста" in text for _, text in bot.messages)

    async with db.raw_conn() as conn:
        ops_cur = await conn.execute(
            "SELECT status FROM ops_run WHERE kind='vk_auto_import' ORDER BY id DESC LIMIT 1"
        )
        ops_row = await ops_cur.fetchone()
        assert ops_row is not None
        assert ops_row[0] == "failed"
        cur = await conn.execute("SELECT status, last_typed_reason, next_attempt_at FROM vk_inbox WHERE id=1")
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == "failed_technical"
        assert row[1] == "ROW_TIMEOUT"
        assert row[2] is None


@pytest.mark.asyncio
async def test_vk_auto_import_requests_strict_chronological_pick_next(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    calls: list[dict[str, object]] = []

    async def fake_pick_next(_db, _operator_id, _batch_id, **kwargs):
        calls.append(dict(kwargs))
        return None

    monkeypatch.setattr(vk_auto_queue.vk_review, "pick_next", fake_pick_next)

    bot = DummyBot()
    await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=1, operator_id=123)

    assert calls, "expected at least one pick_next call"
    assert calls[0].get("prefer_oldest") is True
    assert calls[0].get("strict_chronological") is True


@pytest.mark.asyncio
async def test_vk_auto_import_cancellation_requires_typed_llm_action(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    # Create an existing upcoming event that should be cancelled/hidden.
    async with db.get_session() as session:
        from models import Event

        session.add(
            Event(
            title="Manhattan Short Online",
            description="Описание",
            source_text="src",
            date="2026-02-15",
            time="16:00",
            location_name="арт-пространство «Сигнал»",
            location_address="ул. К. Леонова, 22",
            city="Калининград",
            )
        )
        await session.commit()
        # Reload the inserted event_id via query to avoid relying on ORM identity mechanics.
        from sqlalchemy import select

        res = await session.execute(select(Event.id).where(Event.title == "Manhattan Short Online"))
        event_id = int(res.scalar_one())

    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (211997788, "signal", "Пространство Сигнал", "арт-пространство «Сигнал»", None, None),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, event_ts_hint, status) VALUES(?,?,?,?,?,?,?,?,?)",
            (1, 211997788, 2754, 0, "stub", vk_intake.OCR_PENDING_SENTINEL, 0, None, "pending"),
        )
        await conn.commit()

    cancel_text = (
        "Друзья, объявление для любителей кинофестиваля Manhattan Short Online. "
        "К сожалению, организаторы сдвинули сроки фестиваля, поэтому показ 15 февраля не состоится."
    )

    async def fake_fetch(*_args, **_kwargs):
        return (
            cancel_text,
            [],
            datetime(2026, 2, 14, 12, 0, tzinfo=timezone.utc),
            {"views": 10, "likes": 1},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    called = 0

    async def fake_build(*_args, **_kwargs):
        nonlocal called
        called += 1
        decision = SourceParseDecision(
            [],
            disposition=SourceDisposition.LIFECYCLE_ONLY,
            lifecycle_actions=(LifecycleAction(
                action=LifecycleActionType.CANCEL,
                target_title="Manhattan Short Online",
                target_date="2026-02-15",
                evidence="показ 15 февраля не состоится",
            ),),
            evidence_manifest=EvidenceManifest.complete_source(cancel_text),
        )
        return vk_intake.DraftParseResult([], decision=decision), None

    seen_actions = []

    async def fake_apply(*_args, lifecycle_action=None, **_kwargs):
        seen_actions.append(lifecycle_action)
        return event_id, None

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build)
    monkeypatch.setattr(vk_auto_queue, "_cancel_matching_event_from_notice", fake_apply)

    bot = DummyBot()
    await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=1, operator_id=123)

    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT status, imported_event_id FROM vk_inbox WHERE id=1")
        status, imported_event_id = await cur.fetchone()
        assert status == "imported"
        assert int(imported_event_id) == int(event_id)
    assert called == 1
    assert len(seen_actions) == 1
    assert seen_actions[0].action is LifecycleActionType.CANCEL


@pytest.mark.asyncio
async def test_vk_auto_import_keeps_new_event_when_mixed_lifecycle_target_is_absent(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "mixed-lifecycle-noop.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name,location) "
            "VALUES(1,'club1','Source','Venue')"
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id,group_id,post_id,date,text,matched_kw,has_date,status) "
            "VALUES(1,1,100,0,'stub',?,0,'pending')",
            (vk_intake.OCR_PENDING_SENTINEL,),
        )
        await conn.commit()

    async def fake_fetch(*_args, **_kwargs):
        return (
            "Старая лекция отменена. Новая лекция 20 августа в 18:30.",
            [],
            datetime.now(timezone.utc),
            {},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    async def fake_build(*_args, **_kwargs):
        draft = vk_intake.EventDraft(
            title="Новая лекция", date="2026-08-20", time="18:30", venue="Venue"
        )
        decision = SourceParseDecision(
            [{"title": draft.title}],
            disposition=SourceDisposition.MIXED,
            lifecycle_actions=(
                LifecycleAction(
                    action=LifecycleActionType.CANCEL,
                    target_title="Старая лекция",
                    evidence="Старая лекция отменена",
                ),
            ),
            evidence_manifest=EvidenceManifest.complete_source("source"),
        )
        return vk_intake.DraftParseResult([draft], decision=decision), None

    async def fake_cancel(*_args, **_kwargs):
        return None, "no matching event"

    async def fake_persist(*_args, **_kwargs):
        return vk_intake.PersistResult(
            event_id=7001,
            telegraph_url="",
            ics_supabase_url="",
            ics_tg_url="",
            event_date="2026-08-20",
            event_end_date=None,
            event_time="18:30",
            event_type=None,
            is_free=False,
            smart_result=SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.CREATED,
                event_id=7001,
            ),
        )

    resolutions = []

    async def capture_resolution(*_args, **kwargs):
        resolutions.append(kwargs)

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build)
    monkeypatch.setattr(vk_auto_queue, "_cancel_matching_event_from_notice", fake_cancel)
    monkeypatch.setattr(vk_intake, "persist_event_and_pages", fake_persist)
    monkeypatch.setattr(vk_auto_queue.vk_review, "record_carrier_resolution", capture_resolution)

    report = await vk_auto_queue.run_vk_auto_import(
        db, DummyBot(), chat_id=1, limit=1, operator_id=123
    )

    assert report.inbox_imported == 1
    assert report.inbox_failed_technical == 0
    assert report.created_event_ids == [7001]
    async with db.raw_conn() as conn:
        inbox = await (await conn.execute(
            "SELECT status,imported_event_id,last_typed_reason FROM vk_inbox WHERE id=1"
        )).fetchone()
    assert inbox[0:2] == ("imported", 7001)
    assert inbox[2] == "MIXED_RESOLVED"
    assert resolutions[-1]["terminal_carrier_outcome"] == "MIXED_RESOLVED"
    assert "LIFECYCLE_NO_MATCH_NOOP" in resolutions[-1]["child_outcomes"]


@pytest.mark.asyncio
async def test_vk_auto_import_terminalizes_unmatched_lifecycle_only_as_product_noop(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "lifecycle-only-noop.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name) VALUES(1,'club1','Source')"
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id,group_id,post_id,date,text,matched_kw,has_date,status) "
            "VALUES(1,1,100,0,'stub',?,0,'pending')",
            (vk_intake.OCR_PENDING_SENTINEL,),
        )
        await conn.commit()

    async def fake_fetch(*_args, **_kwargs):
        return (
            "Несуществующая в каталоге лекция отменена.",
            [],
            datetime.now(timezone.utc),
            {},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    async def fake_build(*_args, **_kwargs):
        decision = SourceParseDecision(
            [],
            disposition=SourceDisposition.LIFECYCLE_ONLY,
            lifecycle_actions=(
                LifecycleAction(
                    action=LifecycleActionType.CANCEL,
                    target_title="Несуществующая лекция",
                ),
            ),
            evidence_manifest=EvidenceManifest.complete_source("source"),
        )
        return vk_intake.DraftParseResult([], decision=decision), None

    async def fake_cancel(*_args, **_kwargs):
        return None, "no matching event"

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build)
    monkeypatch.setattr(vk_auto_queue, "_cancel_matching_event_from_notice", fake_cancel)

    report = await vk_auto_queue.run_vk_auto_import(
        db, DummyBot(), chat_id=1, limit=1, operator_id=123
    )

    assert report.inbox_rejected == 1
    assert report.inbox_failed_technical == 0
    async with db.raw_conn() as conn:
        row = await (await conn.execute(
            "SELECT status,last_typed_reason,next_attempt_at FROM vk_inbox WHERE id=1"
        )).fetchone()
    assert row[0] == "confirmed_product_exclusion"
    assert row[1].startswith("LIFECYCLE_NO_MATCH_NOOP:")
    assert row[2] is None


def test_vk_auto_import_time_reschedule_notice_stays_on_normal_import_path():
    text = (
        "Друзья, 8 мая время начала Винного вечера перенесено на 19.30!\n"
        "Места еще есть! Приходите, ждем Вас✨"
    )

    assert vk_auto_queue._parse_ru_date_from_text(text, year_hint=2026) == "2026-05-08"
    assert vk_auto_queue._looks_like_time_reschedule_notice(text) is True
    assert not hasattr(vk_auto_queue, "_looks_like_cancellation_notice")


def test_vk_auto_import_previous_meeting_reschedule_stays_on_normal_import_path():
    text = (
        "«Прерафаэлиты: братство, приручившее вечность»\n"
        "Время: 22 мая, пятница, 19:00-20:30\n"
        "Место: Железнодорожные ворота\n"
        "Эта лекция - перенос несостоявшейся встречи в апреле."
    )

    assert vk_auto_queue._parse_ru_date_from_text(text, year_hint=2026) == "2026-05-22"
    assert vk_auto_queue._looks_like_retrospective_reschedule_context(text) is True
    assert not hasattr(vk_auto_queue, "_looks_like_cancellation_notice")


@pytest.mark.asyncio
async def test_vk_auto_cancel_match_requires_date_or_title_anchor(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        from models import Event

        session.add(
            Event(
                title="Старое событие",
                description="Описание",
                source_text="src",
                date="2026-01-01",
                time="19:30",
                location_name="Кирха Рудау",
                location_address="",
                city="Калининград",
            )
        )
        await session.commit()

    event_id, err = await vk_auto_queue._cancel_matching_event_from_notice(
        db,
        notice_text="Друзья, время начала перенесено на 19.30!",
        source_url="https://vk.com/wall-222857709_1116",
        source_name="Кирха Рудау. Парк-Музей.",
        location_hint="Кирха Рудау",
        published_at=datetime(2026, 5, 7, 14, 2, tzinfo=timezone.utc),
    )

    assert event_id is None
    assert err == "insufficient_anchors:no_date_no_title"


@pytest.mark.asyncio
async def test_vk_lifecycle_reschedule_replay_finds_target_on_new_date_not_same_location_neighbor(
    tmp_path,
):
    db = Database(str(tmp_path / "reschedule-replay.sqlite"))
    await db.init()

    async with db.get_session() as session:
        from models import Event

        target = Event(
            title="Косплей-фестиваль «Мимикрия»",
            description="Описание",
            source_text="src",
            date="2026-09-05",
            time="12:00",
            location_name="Калининградский зоопарк",
            city="Калининград",
        )
        neighbor = Event(
            title="Парикмахеры верблюдов, муравьедов повара",
            description="Описание",
            source_text="src",
            date="2026-08-23",
            time="10:00",
            location_name="Калининградский зоопарк",
            city="Калининград",
        )
        session.add_all([target, neighbor])
        await session.commit()
        await session.refresh(target)
        await session.refresh(neighbor)
        target_id = int(target.id)
        neighbor_id = int(neighbor.id)

    event_id, err = await vk_auto_queue._cancel_matching_event_from_notice(
        db,
        notice_text=(
            "Косплей-фестиваль «Мимикрия», который должен был состояться "
            "23 августа, переносим на 5 сентября."
        ),
        source_url="https://vk.com/wall-48383763_41891",
        source_name="Калининградский зоопарк",
        location_hint="Калининградский зоопарк",
        published_at=datetime(2026, 8, 20, 10, 0, tzinfo=timezone.utc),
        lifecycle_action=LifecycleAction(
            action=LifecycleActionType.RESCHEDULE_DATE,
            target_title="Косплей-фестиваль «Мимикрия»",
            target_date="2026-08-23",
            new_date="2026-09-05",
            target_location="Калининградский зоопарк",
        ),
    )

    assert err is None
    assert event_id == target_id
    async with db.get_session() as session:
        target = await session.get(Event, target_id)
        neighbor = await session.get(Event, neighbor_id)
    assert (target.date, target.lifecycle_status) == ("2026-09-05", "active")
    assert (neighbor.date, neighbor.lifecycle_status) == ("2026-08-23", "active")


@pytest.mark.asyncio
async def test_vk_lifecycle_cancel_replay_finds_cancelled_target_not_same_location_neighbor(
    tmp_path,
):
    db = Database(str(tmp_path / "cancel-replay.sqlite"))
    await db.init()

    async with db.get_session() as session:
        from models import Event

        target = Event(
            title="Музыкальный вечер в зоопарке",
            description="Описание",
            source_text="src",
            date="2026-08-22",
            time="19:00",
            location_name="Калининградский зоопарк",
            city="Калининград",
            lifecycle_status="cancelled",
        )
        neighbor = Event(
            title="Ветеринарный экспресс",
            description="Описание",
            source_text="src",
            date="2026-08-22",
            time="10:00",
            location_name="Калининградский зоопарк",
            city="Калининград",
        )
        session.add_all([target, neighbor])
        await session.commit()
        await session.refresh(target)
        await session.refresh(neighbor)
        target_id = int(target.id)
        neighbor_id = int(neighbor.id)

    event_id, err = await vk_auto_queue._cancel_matching_event_from_notice(
        db,
        notice_text="Музыкальный вечер, ожидаемый 22 августа, отменяем.",
        source_url="https://vk.com/wall-48383763_41891",
        source_name="Калининградский зоопарк",
        location_hint="Калининградский зоопарк",
        published_at=datetime(2026, 8, 20, 10, 0, tzinfo=timezone.utc),
        lifecycle_action=LifecycleAction(
            action=LifecycleActionType.CANCEL,
            target_title="Музыкальный вечер",
            target_date="2026-08-22",
            target_location="Калининградский зоопарк",
        ),
    )

    assert err is None
    assert event_id == target_id
    async with db.get_session() as session:
        target = await session.get(Event, target_id)
        neighbor = await session.get(Event, neighbor_id)
    assert target.lifecycle_status == "cancelled"
    assert neighbor.lifecycle_status == "active"


@pytest.mark.asyncio
async def test_vk_auto_import_marks_inbox_imported_and_links_multiple_events(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    # Queue row - use OCR_PENDING sentinel so vk_review doesn't try to recompute ts_hint/reject.
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "Test Community", "Научная библиотека", None, "https://tickets.local"),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, event_ts_hint, status) VALUES(?,?,?,?,?,?,?,?,?)",
            (1, 1, 100, 0, "stub", vk_intake.OCR_PENDING_SENTINEL, 0, None, "pending"),
        )
        await conn.commit()

    async def fake_fetch(*_args, **_kwargs):
        return (
            "text",
            ["https://example.com/a.jpg"],
            datetime.now(timezone.utc),
            {"views": 10, "likes": 1},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    async def fake_build_event_drafts(*_args, **_kwargs):
        d1 = vk_intake.EventDraft(title="E1", date="2026-12-31", time="18:30", venue="Научная библиотека")
        d2 = vk_intake.EventDraft(title="E2", date="2026-12-31", time="18:30", venue="Научная библиотека")
        decision = SourceParseDecision(
            [{"title": "E1"}, {"title": "E2"}],
            disposition=SourceDisposition.EVENTS_FOUND,
            evidence_manifest=EvidenceManifest.complete_source(
                "text", ["poster OCR"], attachment_count=1
            ),
        )
        return vk_intake.DraftParseResult([d1, d2], decision=decision), None

    # Persist stub: we only need deterministic ids to verify mapping table; the events
    # themselves are not required for this unit test.
    counter = {"n": 0, "persist_kwargs": []}

    async def fake_persist(*_args, **_kwargs):
        counter["n"] += 1
        counter["persist_kwargs"].append(dict(_kwargs))
        return vk_intake.PersistResult(
            event_id=1000 + counter["n"],
            telegraph_url="",
            ics_supabase_url="",
            ics_tg_url="",
            event_date="2026-12-31",
            event_end_date=None,
            event_time="18:30",
            event_type=None,
            is_free=False,
            smart_result=SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.CREATED,
                event_id=1000 + counter["n"],
            ),
        )

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build_event_drafts)
    monkeypatch.setattr(vk_intake, "persist_event_and_pages", fake_persist)

    bot = DummyBot()
    await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=10, operator_id=123)

    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT status, imported_event_id FROM vk_inbox WHERE id=1")
        status, imported_event_id = await cur.fetchone()
        assert status == "imported"
        assert imported_event_id == 1001

        cur = await conn.execute(
            "SELECT event_id FROM vk_inbox_import_event WHERE inbox_id=1 ORDER BY event_id"
        )
        rows = await cur.fetchall()
    assert [r[0] for r in rows] == [1001, 1002]
    assert len(counter["persist_kwargs"]) == 2
    for persist_kwargs in counter["persist_kwargs"]:
        assert persist_kwargs["source_disposition"] == "EVENTS_FOUND"
        assert persist_kwargs["source_parse_version"] == "source-parse-v1"
        assert persist_kwargs["source_evidence_complete"] is True


@pytest.mark.asyncio
async def test_vk_auto_import_terminalizes_smart_technical_failure_without_retry(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "failed-technical.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name,location) VALUES(1,'club1','Source','Venue')"
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id,group_id,post_id,date,text,matched_kw,has_date,status) "
            "VALUES(1,1,100,0,'stub',?,0,'pending')",
            (vk_intake.OCR_PENDING_SENTINEL,),
        )
        await conn.commit()

    async def fake_fetch(*_args, **_kwargs):
        return (
            "18 августа событие в Venue",
            [],
            datetime.now(timezone.utc),
            {},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    async def fake_build(*_args, **_kwargs):
        draft = vk_intake.EventDraft(
            title="Event", date="2026-08-18", time="15:00", venue="Venue"
        )
        decision = SourceParseDecision(
            [{"title": "Event"}],
            disposition=SourceDisposition.EVENTS_FOUND,
            evidence_manifest=EvidenceManifest.complete_source("18 августа событие в Venue"),
        )
        return vk_intake.DraftParseResult([draft], decision=decision), None

    async def fake_persist(*_args, **_kwargs):
        return vk_intake.PersistResult(
            event_id=None,
            telegraph_url=None,
            ics_supabase_url=None,
            ics_tg_url=None,
            event_date=None,
            event_end_date=None,
            event_time=None,
            event_type=None,
            is_free=False,
            smart_result=SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.FAILED_TECHNICAL,
                reason="provider_unavailable",
            ),
        )

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build)
    monkeypatch.setattr(vk_intake, "persist_event_and_pages", fake_persist)
    bot = DummyBot()
    report = await vk_auto_queue.run_vk_auto_import(
        db, bot, chat_id=1, limit=1, operator_id=123
    )

    assert report.inbox_failed == 1
    assert report.inbox_failed_technical == 1
    assert report.inbox_deferred == 0
    assert any("без автоматического повтора" in text for _, text in bot.messages)
    async with db.raw_conn() as conn:
        row = await (await conn.execute(
            "SELECT status,last_typed_reason,next_attempt_at FROM vk_inbox WHERE id=1"
        )).fetchone()
    assert row == ("failed_technical", "provider_unavailable", None)


@pytest.mark.asyncio
async def test_vk_auto_import_fifteen_row_batch_is_fully_terminal_without_retry(
    tmp_path, monkeypatch
):
    """Regression for the operator screenshot: 15 selected rows must balance.

    The historical batch had fourteen automatic deferrals across incomplete
    evidence, schema/provider failures and Smart/persist failures.  The linear
    contract keeps the one proved no-event rejection and makes every other row
    a visible technical terminal with no due timestamp.
    """

    from google_ai.exceptions import RateLimitError

    db = Database(str(tmp_path / "batch-balance.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id,screen_name,name,location) "
            "VALUES(1,'club1','Source','Venue')"
        )
        for inbox_id in range(1, 16):
            await conn.execute(
                "INSERT INTO vk_inbox(id,group_id,post_id,date,text,matched_kw,has_date,status) "
                "VALUES(?,1,?,0,?,?,0,'pending')",
                (inbox_id, 100 + inbox_id, f"case-{inbox_id}", vk_intake.OCR_PENDING_SENTINEL),
            )
        await conn.commit()

    async def fake_fetch(_group_id, post_id, **_kwargs):
        inbox_id = int(post_id) - 100
        return (
            f"case-{inbox_id}",
            [],
            datetime.now(timezone.utc),
            {},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    async def fake_build(text, **_kwargs):
        inbox_id = int(text.split("-", 1)[1])
        manifest = EvidenceManifest.complete_source(text)
        if inbox_id <= 8:
            return vk_intake.DraftParseResult(
                [],
                decision=SourceParseDecision.retry(
                    SourceParseRetryReason.EVIDENCE_INCOMPLETE,
                    evidence_manifest=manifest,
                ),
            ), None
        if inbox_id == 9:
            return vk_intake.DraftParseResult(
                [],
                decision=SourceParseDecision.retry(
                    SourceParseRetryReason.SCHEMA_MISMATCH,
                    evidence_manifest=manifest,
                ),
            ), None
        if inbox_id in {10, 11}:
            raise RateLimitError(blocked_reason="tpm", retry_after_ms=1)
        if inbox_id == 15:
            decision = SourceParseDecision(
                [],
                disposition=SourceDisposition.CONFIRMED_NO_EVENT,
                no_event_reason=SourceNoEventReason.NO_ATTENDABLE_EVENT,
                evidence_manifest=manifest,
            )
            return vk_intake.DraftParseResult([], decision=decision), None
        draft = vk_intake.EventDraft(
            title=f"case-{inbox_id}",
            date="2026-08-18",
            time="15:00",
            venue="Venue",
        )
        decision = SourceParseDecision(
            [{"title": draft.title}],
            disposition=SourceDisposition.EVENTS_FOUND,
            evidence_manifest=manifest,
        )
        return vk_intake.DraftParseResult([draft], decision=decision), None

    async def fake_persist(draft, *_args, **_kwargs):
        inbox_id = int(draft.title.split("-", 1)[1])
        if inbox_id == 12:
            raise RuntimeError("database write unavailable")
        if inbox_id == 13:
            smart_result = SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.RETRY_SCHEDULED,
                reason="legacy_retry_result",
            )
        else:
            smart_result = SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.FAILED_TECHNICAL,
                reason="smart_provider_unavailable",
            )
        return vk_intake.PersistResult(
            event_id=None,
            telegraph_url=None,
            ics_supabase_url=None,
            ics_tg_url=None,
            event_date=None,
            event_end_date=None,
            event_time=None,
            event_type=None,
            is_free=False,
            smart_result=smart_result,
        )

    monkeypatch.setenv("VK_AUTO_IMPORT_INLINE_JOBS", "0")
    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build)
    monkeypatch.setattr(vk_intake, "persist_event_and_pages", fake_persist)

    report = await vk_auto_queue.run_vk_auto_import(
        db, DummyBot(), chat_id=1, limit=15, operator_id=123
    )

    assert report.inbox_processed == 15
    assert report.inbox_imported == 0
    assert report.inbox_rejected == 1
    assert report.inbox_failed == 14
    assert report.inbox_failed_technical == 14
    assert report.inbox_deferred == 0
    assert report.inbox_processed == (
        report.inbox_imported + report.inbox_rejected + report.inbox_failed
    )
    async with db.raw_conn() as conn:
        rows = await (
            await conn.execute(
                "SELECT status,next_attempt_at,COUNT(*) FROM vk_inbox "
                "GROUP BY status,next_attempt_at ORDER BY status"
            )
        ).fetchall()
    assert rows == [
        ("confirmed_no_event", None, 1),
        ("failed_technical", None, 14),
    ]


@pytest.mark.asyncio
async def test_vk_auto_import_keeps_valid_roundup_siblings_after_semantic_rejection(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "Test Community", "Научная библиотека", None, None),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, event_ts_hint, status) VALUES(?,?,?,?,?,?,?,?,?)",
            (1, 1, 100, 0, "stub", vk_intake.OCR_PENDING_SENTINEL, 0, None, "pending"),
        )
        await conn.commit()

    async def fake_fetch(*_args, **_kwargs):
        return (
            "text",
            [],
            datetime.now(timezone.utc),
            {"views": 10, "likes": 1},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    async def fake_build_event_drafts(*_args, **_kwargs):
        return [
            vk_intake.EventDraft(title="E1", date="2026-12-31", time="18:30"),
            vk_intake.EventDraft(title="E2", date="2027-01-01", time="19:00"),
        ], None

    calls = 0

    async def fake_persist(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            return vk_intake.PersistResult(
                event_id=None,
                telegraph_url="",
                ics_supabase_url="",
                ics_tg_url="",
                event_date="2027-01-01",
                event_end_date=None,
                event_time="19:00",
                event_type=None,
                is_free=False,
                smart_result=SmartUpdateResult(
                    outcome=SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY,
                    reason="past_event",
                    product_exclusion_reason=ProductExclusionReason.PAST_EVENT,
                ),
            )
        return vk_intake.PersistResult(
            event_id=1001,
            telegraph_url="",
            ics_supabase_url="",
            ics_tg_url="",
            event_date="2026-12-31",
            event_end_date=None,
            event_time="18:30",
            event_type=None,
            is_free=False,
            smart_result=SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.CREATED,
                event_id=1001,
            ),
        )

    monkeypatch.setenv("VK_AUTO_IMPORT_INLINE_JOBS", "0")
    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build_event_drafts)
    monkeypatch.setattr(vk_intake, "persist_event_and_pages", fake_persist)

    report = await vk_auto_queue.run_vk_auto_import(
        db, DummyBot(), chat_id=1, limit=1, operator_id=123
    )

    assert report.inbox_imported == 1
    assert report.inbox_rejected == 0
    assert report.inbox_deferred == 0
    assert report.created_event_ids == [1001]
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, imported_event_id, attempts FROM vk_inbox WHERE id=1"
        )
        assert await cur.fetchone() == ("imported", 1001, 0)
        cur = await conn.execute(
            "SELECT event_id FROM vk_inbox_import_event WHERE inbox_id=1 ORDER BY event_id"
        )
        assert await cur.fetchall() == [(1001,)]


@pytest.mark.asyncio
async def test_vk_auto_import_continues_when_first_roundup_draft_is_rejected(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "Test Community", None, None, None),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, event_ts_hint, status) VALUES(?,?,?,?,?,?,?,?,?)",
            (1, 1, 100, 0, "schedule cards", vk_intake.OCR_PENDING_SENTINEL, 0, None, "pending"),
        )
        await conn.commit()

    async def fake_fetch(*_args, **_kwargs):
        return "text", [], datetime.now(timezone.utc), {}, vk_auto_queue.VkFetchStatus(True, "ok")

    async def fake_build(*_args, **_kwargs):
        return [
            vk_intake.EventDraft(title="Past", date="2026-08-01"),
            vk_intake.EventDraft(title="Future", date="2026-08-07"),
        ], None

    calls = 0

    async def fake_persist(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return vk_intake.PersistResult(
                event_id=None,
                telegraph_url="",
                ics_supabase_url="",
                ics_tg_url="",
                event_date="2026-08-01",
                event_end_date=None,
                event_time="",
                event_type=None,
                is_free=False,
                smart_result=SmartUpdateResult(
                    outcome=SmartUpdateTerminalOutcome.REJECTED_PRODUCT_POLICY,
                    reason="past_event",
                    product_exclusion_reason=ProductExclusionReason.PAST_EVENT,
                ),
            )
        return vk_intake.PersistResult(
            event_id=2002,
            telegraph_url="",
            ics_supabase_url="",
            ics_tg_url="",
            event_date="2026-08-07",
            event_end_date=None,
            event_time="",
            event_type=None,
            is_free=False,
            smart_result=SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.CREATED,
                event_id=2002,
            ),
        )

    monkeypatch.setenv("VK_AUTO_IMPORT_INLINE_JOBS", "0")
    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build)
    monkeypatch.setattr(vk_intake, "persist_event_and_pages", fake_persist)

    report = await vk_auto_queue.run_vk_auto_import(
        db, DummyBot(), chat_id=1, limit=1, operator_id=123
    )

    assert calls == 2
    assert report.inbox_imported == 1
    assert report.created_event_ids == [2002]
    async with db.raw_conn() as conn:
        assert await (await conn.execute(
            "SELECT status, imported_event_id FROM vk_inbox WHERE id=1"
        )).fetchone() == ("imported", 2002)


@pytest.mark.asyncio
async def test_vk_auto_import_keeps_llm_child_despite_warning(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (26560795, "club", "Калининградская областная филармония", None, None, None),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, event_ts_hint, status) VALUES(?,?,?,?,?,?,?,?,?)",
            (1, 26560795, 11921, 0, "stub", vk_intake.OCR_PENDING_SENTINEL, 0, None, "pending"),
        )
        await conn.commit()

    async def fake_fetch(*_args, **_kwargs):
        return (
            "text",
            [],
            datetime.now(timezone.utc),
            {"views": 10, "likes": 1},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    async def fake_build_event_drafts(*_args, **_kwargs):
        d1 = vk_intake.EventDraft(
            title="Волшебный мир Хаяо Миядзаки",
            date="2026-03-19",
            time=None,
            venue="Филармония",
            verification_warnings=["Низкая уверенность: требуется условная проверка."],
        )
        return [d1], None

    async def fake_persist(*_args, **_kwargs):
        return vk_intake.PersistResult(
            event_id=77,
            telegraph_url="",
            ics_supabase_url="",
            ics_tg_url="",
            event_date="2026-03-19",
            event_end_date=None,
            event_time="",
            event_type=None,
            is_free=False,
            smart_result=SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.CREATED,
                event_id=77,
            ),
        )

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build_event_drafts)
    monkeypatch.setattr(vk_intake, "persist_event_and_pages", fake_persist)
    monkeypatch.setenv("VK_AUTO_IMPORT_INLINE_JOBS", "0")

    bot = DummyBot()
    report = await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=1, operator_id=123)

    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT status FROM vk_inbox WHERE id=1")
        (status,) = await cur.fetchone()
    assert status == "imported", report.errors


@pytest.mark.asyncio
async def test_vk_auto_import_uses_llm_decision_without_prefilter_argument(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (20898960, "kukly", "Калининградский областной театр кукол", None, None, None),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, event_ts_hint, status) VALUES(?,?,?,?,?,?,?,?,?)",
            (1, 20898960, 4794, 0, "stub", vk_intake.OCR_PENDING_SENTINEL, 0, None, "pending"),
        )
        await conn.commit()

    captured: dict[str, object] = {}

    async def fake_fetch(*_args, **_kwargs):
        return (
            "Исторический очерк о послевоенном театре кукол.",
            [],
            datetime.now(timezone.utc),
            {"views": 10, "likes": 1},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    async def fake_build_event_drafts(*_args, **kwargs):
        captured.update(kwargs)
        decision = SourceParseDecision(
            [],
            disposition=SourceDisposition.CONFIRMED_NO_EVENT,
            no_event_reason=SourceNoEventReason.RECAP_ONLY,
            evidence_manifest=EvidenceManifest.complete_source(
                "Исторический очерк о послевоенном театре кукол."
            ),
        )
        return vk_intake.DraftParseResult([], decision=decision), None

    async def should_not_be_called(*_args, **_kwargs):
        raise AssertionError("persist_event_and_pages must not be called for reject-only drafts")

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build_event_drafts)
    monkeypatch.setattr(vk_intake, "persist_event_and_pages", should_not_be_called)

    bot = DummyBot()
    await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=1, operator_id=123)

    assert "prefilter_obvious_non_events" not in captured
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT status FROM vk_inbox WHERE id=1")
        (status,) = await cur.fetchone()
    assert status == "confirmed_no_event"


@pytest.mark.asyncio
async def test_vk_auto_import_skips_festival_helper_for_regular_sources(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link, festival_source) VALUES(?,?,?,?,?,?,?)",
            (1, "club1", "Regular Community", None, None, None, 0),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, event_ts_hint, status) VALUES(?,?,?,?,?,?,?,?,?)",
            (1, 1, 100, 0, "stub", vk_intake.OCR_PENDING_SENTINEL, 0, None, "pending"),
        )
        await conn.commit()

    async def fake_fetch(*_args, **_kwargs):
        return (
            "text",
            [],
            datetime.now(timezone.utc),
            {"views": 10, "likes": 1},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    seen: dict[str, object] = {}

    async def fake_build_event_drafts(*_args, **kwargs):
        seen["festival_names"] = kwargs.get("festival_names")
        seen["festival_alias_pairs"] = kwargs.get("festival_alias_pairs")
        seen["festival_hint"] = kwargs.get("festival_hint")
        decision = SourceParseDecision(
            [],
            disposition=SourceDisposition.CONFIRMED_NO_EVENT,
            no_event_reason=SourceNoEventReason.NO_ATTENDABLE_EVENT,
            evidence_manifest=EvidenceManifest.complete_source("text"),
        )
        return vk_intake.DraftParseResult([], decision=decision), None

    async def fake_load_festival_hints(_db):
        return ["Fest"], [("fest", 0)]

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_auto_queue, "_load_festival_hints", fake_load_festival_hints)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build_event_drafts)

    bot = DummyBot()
    await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=1, operator_id=123)

    assert seen["festival_names"] is None
    assert seen["festival_alias_pairs"] is None
    assert seen["festival_hint"] is False


@pytest.mark.asyncio
async def test_vk_auto_import_keeps_festival_helper_for_festival_sources(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link, festival_source) VALUES(?,?,?,?,?,?,?)",
            (1, "club1", "Festival Community", None, None, None, 1),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, event_ts_hint, status) VALUES(?,?,?,?,?,?,?,?,?)",
            (1, 1, 100, 0, "stub", vk_intake.OCR_PENDING_SENTINEL, 0, None, "pending"),
        )
        await conn.commit()

    async def fake_fetch(*_args, **_kwargs):
        return (
            "text",
            [],
            datetime.now(timezone.utc),
            {"views": 10, "likes": 1},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    seen: dict[str, object] = {}

    async def fake_build_event_drafts(*_args, **kwargs):
        seen["festival_names"] = kwargs.get("festival_names")
        seen["festival_alias_pairs"] = kwargs.get("festival_alias_pairs")
        seen["festival_hint"] = kwargs.get("festival_hint")
        decision = SourceParseDecision(
            [],
            disposition=SourceDisposition.CONFIRMED_NO_EVENT,
            no_event_reason=SourceNoEventReason.NO_ATTENDABLE_EVENT,
            evidence_manifest=EvidenceManifest.complete_source("text"),
        )
        return vk_intake.DraftParseResult([], decision=decision), None

    async def fake_load_festival_hints(_db):
        return ["Fest"], [("fest", 0)]

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_auto_queue, "_load_festival_hints", fake_load_festival_hints)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build_event_drafts)

    bot = DummyBot()
    await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=1, operator_id=123)

    assert seen["festival_names"] == ["Fest"]
    assert seen["festival_alias_pairs"] == [("fest", 0)]
    assert seen["festival_hint"] is True


@pytest.mark.asyncio
async def test_vk_auto_import_include_skipped_requeues_and_imports(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "Test Community", "Научная библиотека", None, None),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, event_ts_hint, status) VALUES(?,?,?,?,?,?,?,?,?)",
            (1, 1, 100, 0, "stub", vk_intake.OCR_PENDING_SENTINEL, 0, None, "skipped"),
        )
        await conn.commit()

    async def fake_fetch(*_args, **_kwargs):
        return (
            "text",
            [],
            datetime.now(timezone.utc),
            {"views": 10, "likes": 1},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    async def fake_build_event_drafts(*_args, **_kwargs):
        d1 = vk_intake.EventDraft(title="E1", date="2026-12-31", time="18:30", venue="Научная библиотека")
        return [d1], None

    async def fake_persist(*_args, **_kwargs):
        return vk_intake.PersistResult(
            event_id=1001,
            telegraph_url="",
            ics_supabase_url="",
            ics_tg_url="",
            event_date="2026-12-31",
            event_end_date=None,
            event_time="18:30",
            event_type=None,
            is_free=False,
            smart_result=SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.CREATED,
                event_id=1001,
            ),
        )

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build_event_drafts)
    monkeypatch.setattr(vk_intake, "persist_event_and_pages", fake_persist)

    bot = DummyBot()
    report = await vk_auto_queue.run_vk_auto_import(
        db,
        bot,
        chat_id=1,
        limit=1,
        operator_id=123,
        include_skipped=True,
    )


@pytest.mark.asyncio
async def test_vk_auto_import_prefetch_does_not_reprocess_current_locked_row(tmp_path, monkeypatch):
    """Regression: prefetch must not pick the currently locked row again.

    Previously, run_vk_auto_import() picked the "next" post while the current post
    was still locked. vk_review.pick_next() prefers resuming locked rows for the
    operator, so it returned the same row and the importer processed it twice.
    """

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    now = int(datetime.now(timezone.utc).timestamp())
    future1 = now + 100_000
    future2 = now + 200_000

    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "Test Community", "Научная библиотека", None, None),
        )
        await conn.executemany(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, event_ts_hint, status) VALUES(?,?,?,?,?,?,?,?,?)",
            [
                (1, 1, 100, now, "Концерт 31.12.2099", "k", 1, future1, "pending"),
                (2, 1, 200, now + 1, "Концерт 01.01.2100", "k", 1, future2, "pending"),
            ],
        )
        await conn.commit()

    async def fake_fetch(group_id, post_id, *_args, **_kwargs):
        return (
            f"text {group_id}_{post_id}",
            [],
            datetime.now(timezone.utc),
            {"views": 10, "likes": 1},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    async def fake_build_event_drafts(*_args, **_kwargs):
        return [vk_intake.EventDraft(title="E", date="2026-12-31", time="18:30", venue="Научная библиотека")], None

    calls: list[str] = []

    async def fake_persist(*_args, **kwargs):
        calls.append(str(kwargs.get("source_post_url") or ""))
        n = len(calls)
        return vk_intake.PersistResult(
            event_id=2000 + n,
            telegraph_url="",
            ics_supabase_url="",
            ics_tg_url="",
            event_date="2026-12-31",
            event_end_date=None,
            event_time="18:30",
            event_type=None,
            is_free=False,
            smart_result=SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.CREATED,
                event_id=2000 + n,
            ),
        )

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build_event_drafts)
    monkeypatch.setattr(vk_intake, "persist_event_and_pages", fake_persist)

    bot = DummyBot()
    await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=2, operator_id=123)

    assert calls == [
        "https://vk.com/wall-1_100",
        "https://vk.com/wall-1_200",
    ]

    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT status FROM vk_inbox WHERE id=1")
        (status,) = await cur.fetchone()
        assert status == "imported"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("inline_jobs_env", "expected_wait_for_telegraph"),
    [
        (None, False),
        ("0", True),
    ],
)
async def test_vk_auto_import_skips_redundant_telegraph_wait_when_inline_jobs_enabled(
    tmp_path,
    monkeypatch,
    inline_jobs_env,
    expected_wait_for_telegraph,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "Test Community", "Научная библиотека", None, None),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, event_ts_hint, status) VALUES(?,?,?,?,?,?,?,?,?)",
            (1, 1, 100, 0, "stub", vk_intake.OCR_PENDING_SENTINEL, 0, None, "pending"),
        )
        await conn.commit()

    if inline_jobs_env is None:
        monkeypatch.delenv("VK_AUTO_IMPORT_INLINE_JOBS", raising=False)
    else:
        monkeypatch.setenv("VK_AUTO_IMPORT_INLINE_JOBS", inline_jobs_env)

    async def fake_fetch(*_args, **_kwargs):
        return (
            "text",
            [],
            datetime.now(timezone.utc),
            {"views": 10, "likes": 1},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    async def fake_build_event_drafts(*_args, **_kwargs):
        return [vk_intake.EventDraft(title="E", date="2026-12-31", time="18:30", venue="Научная библиотека")], None

    captured_waits: list[bool] = []

    async def fake_persist(*_args, **kwargs):
        captured_waits.append(bool(kwargs.get("wait_for_telegraph_url", True)))
        return vk_intake.PersistResult(
            event_id=1001,
            telegraph_url="",
            ics_supabase_url="",
            ics_tg_url="",
            event_date="2026-12-31",
            event_end_date=None,
            event_time="18:30",
            event_type=None,
            is_free=False,
            smart_result=SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.CREATED,
                event_id=1001,
            ),
        )

    captured_allowed: list[set[main.JobTask]] = []

    async def fake_run_jobs(*_args, **_kwargs):
        captured_allowed.append(set(_kwargs.get("allowed_tasks") or ()))
        return None

    async def fake_report(*_args, **_kwargs):
        return True

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build_event_drafts)
    monkeypatch.setattr(vk_intake, "persist_event_and_pages", fake_persist)
    monkeypatch.setattr(main, "run_event_update_jobs", fake_run_jobs)
    monkeypatch.setattr(vk_auto_queue, "_send_unified_event_report", fake_report)

    bot = DummyBot()
    await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=1, operator_id=123)

    assert captured_waits == [expected_wait_for_telegraph]
    if inline_jobs_env is None:
        assert captured_allowed == [
            {main.JobTask.telegraph_build, main.JobTask.tg_event_publish}
        ]
    else:
        assert captured_allowed == []


@pytest.mark.asyncio
async def test_vk_auto_import_logs_stage_timings_for_slow_rows_without_pipeline_timings(
    tmp_path,
    monkeypatch,
    caplog,
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO vk_source(group_id, screen_name, name, location, default_time, default_ticket_link) VALUES(?,?,?,?,?,?)",
            (1, "club1", "Test Community", "Научная библиотека", None, None),
        )
        await conn.execute(
            "INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, event_ts_hint, status) VALUES(?,?,?,?,?,?,?,?,?)",
            (1, 1, 100, 0, "stub", vk_intake.OCR_PENDING_SENTINEL, 0, None, "pending"),
        )
        await conn.commit()

    monkeypatch.setenv("VK_AUTO_IMPORT_INLINE_JOBS", "0")
    monkeypatch.setenv("VK_AUTO_IMPORT_SLOW_ROW_LOG_SEC", "0")
    monkeypatch.delenv("PIPELINE_TIMINGS", raising=False)

    async def fake_fetch(*_args, **_kwargs):
        return (
            "text",
            [],
            datetime.now(timezone.utc),
            {"views": 10, "likes": 1},
            vk_auto_queue.VkFetchStatus(True, "ok"),
        )

    async def fake_build_event_drafts(*_args, **_kwargs):
        return [vk_intake.EventDraft(title="E", date="2026-12-31", time="18:30", venue="Научная библиотека")], None

    async def fake_persist(*_args, **_kwargs):
        return vk_intake.PersistResult(
            event_id=1001,
            telegraph_url="",
            ics_supabase_url="",
            ics_tg_url="",
            event_date="2026-12-31",
            event_end_date=None,
            event_time="18:30",
            event_type=None,
            is_free=False,
            smart_result=SmartUpdateResult(
                outcome=SmartUpdateTerminalOutcome.CREATED,
                event_id=1001,
            ),
        )

    async def fake_report(*_args, **_kwargs):
        return True

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    monkeypatch.setattr(vk_intake, "build_event_drafts", fake_build_event_drafts)
    monkeypatch.setattr(vk_intake, "persist_event_and_pages", fake_persist)
    monkeypatch.setattr(vk_auto_queue, "_send_unified_event_report", fake_report)

    caplog.set_level(logging.INFO)
    bot = DummyBot()
    await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=1, operator_id=123)

    assert any(
        "timing vk_auto_import_row" in rec.message and "persist_total" in rec.message
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_vk_auto_report_is_unified_and_contains_fact_stats(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async def fake_added_info(_db, _event_id, _source, **_kwargs):
        return AddedEventInfo(
            event_id=2417,
            title="Фигаро",
            source="vk",
            telegraph_url="https://telegra.ph/Figaro-02-11",
            ics_url="https://example.test/figaro.ics",
            log_cmd="/log 2417",
            date="2026-02-12",
            time="19:00",
            source_url="https://vk.com/wall-30777579_14572",
            fact_stats={"added": 5, "duplicate": 3, "conflict": 1, "note": 2},
        )

    monkeypatch.setattr("source_parsing.handlers.build_added_event_info", fake_added_info)

    bot = DummyBot()
    await vk_auto_queue._send_unified_event_report(
        db,
        bot,
        1,
        created=[2417],
        updated=[],
        source_url="https://vk.com/wall-30777579_14572",
    )

    assert bot.messages, "VK auto report was not sent"
    _chat_id, text = bot.messages[-1]
    assert "Smart Update (детали событий)" in text
    assert "✅ Созданные события: 1" in text
    # Telegraph link is now embedded into the event title; the duplicated "Telegraph:" line
    # should not be present when telegraph_url exists.
    assert 'href="https://telegra.ph/Figaro-02-11"' in text
    assert "Telegraph:" not in text
    assert "Факты: ✅5 ↩️3 ⚠️1 ℹ️2" in text
    assert "Иллюстрации:" in text
    assert "start=log_2417" in text


@pytest.mark.asyncio
async def test_fetch_vk_post_text_and_photos_accepts_unwrapped_response(monkeypatch):
    async def fake_vk_api(_method, **_params):
        return {
            "items": [
                {
                    "text": "Тестовый пост",
                    "date": 1760000000,
                    "attachments": [
                        {
                            "type": "photo",
                            "photo": {
                                "sizes": [
                                    {
                                        "url": "https://img.test/p1.jpg",
                                        "width": 1200,
                                        "height": 900,
                                    }
                                ]
                            },
                        }
                    ],
                }
            ]
        }

    monkeypatch.setattr(main, "vk_api", fake_vk_api)

    text, photos, published_at, metrics, status = await vk_auto_queue.fetch_vk_post_text_and_photos(
        30777579, 14572
    )

    assert text == "Тестовый пост"
    assert photos == ["https://img.test/p1.jpg"]
    assert published_at is not None
    assert metrics is None
    assert status.ok is True
    assert status.kind == "ok"


@pytest.mark.asyncio
async def test_fetch_vk_post_text_and_photos_includes_repost_text(monkeypatch):
    async def fake_vk_api(_method, **_params):
        return {
            "items": [
                {
                    "text": "Комментарий к репосту",
                    "date": 1760000000,
                    "copy_history": [
                        {
                            "text": "Основной текст события в репосте",
                            "attachments": [],
                        }
                    ],
                    "attachments": [],
                }
            ]
        }

    monkeypatch.setattr(main, "vk_api", fake_vk_api)

    text, photos, published_at, metrics, status = await vk_auto_queue.fetch_vk_post_text_and_photos(1, 1)

    assert "Комментарий к репосту" in text
    assert "Основной текст события в репосте" in text
    assert photos == []
    assert published_at is not None
    assert metrics is None
    assert status.ok is True
    assert status.kind == "ok"


@pytest.mark.asyncio
async def test_fetch_vk_post_resolves_expiring_video_to_inline_evidence_file(monkeypatch):
    calls: list[tuple[str, dict]] = []

    async def fake_vk_api(method, **params):
        calls.append((method, params))
        if method == "wall.getById":
            return {
                "items": [
                    {
                        "id": 14766,
                        "text": "Завершился отборочный этап",
                        "date": 1760000000,
                        "attachments": [
                            {
                                "type": "video",
                                "video": {
                                    "owner_id": -179910542,
                                    "id": 456239978,
                                    "title": "Clip",
                                    "image": [
                                        {"width": 720, "height": 1280, "url": "https://old/expired"}
                                    ],
                                },
                            }
                        ],
                    }
                ]
            }
        assert method == "video.get"
        assert params["videos"] == "-179910542_456239978"
        assert params["_force_user_actor"] is True
        return {
            "items": [
                {
                    "owner_id": -179910542,
                    "id": 456239978,
                    "image": [
                        {"width": 720, "height": 1280, "url": "https://fresh/preview"}
                    ],
                    "files": {"mp4_144": "https://fresh/video.mp4"},
                }
            ]
        }

    monkeypatch.setattr(main, "vk_api", fake_vk_api)

    text, photos, _published_at, _metrics, status = (
        await vk_auto_queue.fetch_vk_post_text_and_photos(179910542, 14766)
    )

    assert [method for method, _params in calls] == ["wall.getById", "video.get"]
    assert "Завершился" in text and "Clip" in text
    assert photos == []
    assert status.ok is True
    assert status.video_urls == ("https://fresh/video.mp4",)


@pytest.mark.asyncio
async def test_fetch_vk_post_retains_link_doc_video_semantics_but_counts_only_visual_candidates(
    monkeypatch,
):
    async def fake_vk_api(_method, **_params):
        return {
            "items": [
                {
                    "id": 22,
                    "text": "",
                    "date": 1760000000,
                    "attachments": [
                        {
                            "type": "link",
                            "link": {
                                "title": "Tickets",
                                "description": "12 August 19:00",
                                "url": "https://tickets.test/22",
                                "photo": {"sizes": [{"width": 1, "height": 1, "url": "https://img/link"}]},
                            },
                        },
                        {
                            "type": "doc",
                            "doc": {
                                "owner_id": -1,
                                "id": 2,
                                "title": "Program",
                                "preview": {"photo": {"sizes": [{"width": 1, "height": 1, "url": "https://img/doc"}]}},
                            },
                        },
                        {
                            "type": "video",
                            "video": {
                                "owner_id": -1,
                                "id": 3,
                                "title": "Announcement",
                                "image": [{"width": 1, "height": 1, "url": "https://img/video"}],
                            },
                        },
                        {"type": "poll", "poll": {"id": 4, "question": "Will you attend?"}},
                    ],
                }
            ]
        }

    monkeypatch.setattr(main, "vk_api", fake_vk_api)
    text, photos, _published_at, _metrics, status = (
        await vk_auto_queue.fetch_vk_post_text_and_photos(1, 22)
    )
    assert "Tickets" in text and "Program" in text and "Announcement" in text
    assert "Will you attend?" in text
    assert photos == ["https://img/link", "https://img/doc", "https://img/video"]
    assert status.attachment_count == 3
    assert status.unavailable_attachment_count == 0
    assert len((status.source_envelope or {})["attachment_inventory"]) == 4


@pytest.mark.asyncio
async def test_prefetch_threads_user_owner_type_to_fresh_fetch(tmp_path, monkeypatch):
    from types import SimpleNamespace

    db = Database(str(tmp_path / "user.sqlite"))
    await db.init()
    seen = {}

    async def fake_fetch(group_id, post_id, **kwargs):
        seen.update(group_id=group_id, post_id=post_id, kwargs=kwargs)
        return "personal event", [], None, None, vk_auto_queue.VkFetchStatus(True, "ok")

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch)
    result = await vk_auto_queue._prefetch_vk_inbox_row(
        db,
        bot=None,
        post=SimpleNamespace(
            group_id=42,
            post_id=7,
            owner_type="user",
            text="old",
            date=100,
        ),
        source_url="https://vk.com/wall42_7",
        festival_names=None,
        festival_alias_pairs=None,
    )
    assert seen["kwargs"]["owner_type"] == "user"
    assert result.text == "personal event"


@pytest.mark.asyncio
async def test_vk_auto_queue_terminalizes_deleted_post_as_missing_evidence(tmp_path, monkeypatch):
    from types import SimpleNamespace

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, status, locked_by, locked_at, review_batch)
            VALUES(?, ?, ?, ?, ?, NULL, 1, 'locked', 777, CURRENT_TIMESTAMP, 'batch-x')
            """,
            (1, 29891284, 12930, 0, "cached text"),
        )
        await conn.commit()

    async def fake_fetch_vk_post_text_and_photos(_group_id, _post_id, *, db, bot, limit):  # noqa: ARG001
        return (
            "",
            [],
            None,
            None,
            vk_auto_queue.VkFetchStatus(False, "not_found", error_code=100, error="Post was deleted"),
        )

    async def should_not_be_called(*_args, **_kwargs):
        raise AssertionError("build_event_drafts must not be called when VK post is not found")

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch_vk_post_text_and_photos)
    monkeypatch.setattr(vk_auto_queue.vk_intake, "build_event_drafts", should_not_be_called)

    report = vk_auto_queue.VkAutoImportReport(batch_id="batch-x")
    post = SimpleNamespace(
        id=1,
        group_id=29891284,
        post_id=12930,
        date=0,
        text="cached text",
        event_ts_hint=None,
    )

    class DummyBot:
        pass

    await vk_auto_queue._process_vk_inbox_row(  # type: ignore[attr-defined]
        db,
        DummyBot(),
        chat_id=1,
        operator_id=1,
        batch_id="batch-x",
        post=post,
        source_url="https://vk.com/wall-29891284_12930",
        report=report,
        festival_names=None,
        festival_alias_pairs=None,
        progress_message_id=None,
        progress_current_no=1,
        progress_total_txt="1",
    )

    assert report.inbox_rejected == 0
    assert report.inbox_deferred == 0
    assert report.inbox_failed == 1
    assert report.inbox_failed_technical == 1

    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT status, last_typed_reason, next_attempt_at FROM vk_inbox WHERE id=?", (1,))
        row = await cur.fetchone()
    assert row[0] == "failed_technical"
    assert row[1] == "EVIDENCE_UNAVAILABLE"
    assert row[2] is None


@pytest.mark.asyncio
async def test_vk_auto_queue_rate_limit_terminalizes_after_inline_attempt(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from google_ai.exceptions import RateLimitError

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO vk_inbox(id, group_id, post_id, date, text, matched_kw, has_date, status, locked_by, locked_at, review_batch)
            VALUES(?, ?, ?, ?, ?, NULL, 1, 'locked', 777, CURRENT_TIMESTAMP, 'batch-x')
            """,
            (1, 123, 456, 0, "cached text"),
        )
        await conn.commit()

    async def fake_fetch_vk_post_text_and_photos(_group_id, _post_id, *, db, bot, limit):  # noqa: ARG001
        return "text", [], None, {"views": 10, "likes": 1}, vk_auto_queue.VkFetchStatus(True, "ok")

    async def fake_build_event_drafts(*_args, **_kwargs):
        assert _kwargs["rate_limit_max_wait_sec"] == 5.0
        raise RateLimitError(blocked_reason="tpm", retry_after_ms=3000)

    # Advance a deterministic clock instead of busy-spinning for five real
    # seconds.  A no-op sleep plus the real monotonic clock generated tens of
    # thousands of retries/log records and could occupy a CI runner for hours.
    clock = 100.0

    def fake_monotonic():
        return clock

    async def advance_clock(sec):
        nonlocal clock
        clock += float(sec)

    monkeypatch.setattr(vk_auto_queue, "fetch_vk_post_text_and_photos", fake_fetch_vk_post_text_and_photos)
    monkeypatch.setattr(vk_auto_queue.vk_intake, "build_event_drafts", fake_build_event_drafts)
    monkeypatch.setattr(vk_auto_queue.asyncio, "sleep", advance_clock)
    monkeypatch.setattr(vk_auto_queue.time, "monotonic", fake_monotonic)
    monkeypatch.setenv("VK_AUTO_IMPORT_RATE_LIMIT_MAX_WAIT_SEC", "5")

    report = vk_auto_queue.VkAutoImportReport(batch_id="batch-x")
    post = SimpleNamespace(
        id=1,
        group_id=123,
        post_id=456,
        date=0,
        text="cached text",
        event_ts_hint=None,
    )

    class DummyBot:
        pass

    await vk_auto_queue._process_vk_inbox_row(  # type: ignore[attr-defined]
        db,
        DummyBot(),
        chat_id=1,
        operator_id=1,
        batch_id="batch-x",
        post=post,
        source_url="https://vk.com/wall-123_456",
        report=report,
        festival_names=None,
        festival_alias_pairs=None,
        progress_message_id=None,
        progress_current_no=1,
        progress_total_txt="1",
    )

    assert report.inbox_deferred == 0
    assert report.inbox_failed == 1
    assert report.inbox_failed_technical == 1

    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT status, locked_by, review_batch, next_attempt_at FROM vk_inbox WHERE id=?",
            (1,),
        )
        status, locked_by, review_batch, next_attempt_at = await cur.fetchone()
    assert status == "failed_technical"
    assert locked_by is None
    assert review_batch == "batch-x"
    assert next_attempt_at is None


def test_build_smart_update_posters_falls_back_to_vk_photo_url_when_catbox_missing():
    draft = vk_intake.EventDraft(
        title="Тест",
        date="2026-02-20",
        time="19:00",
        venue="Локация",
    )
    draft.poster_media = [PosterMedia(data=b"img", name="poster.jpg")]
    photos = ["https://sun9-1.userapi.com/poster.jpg"]

    class _Poster:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    posters = vk_intake._build_smart_update_posters(
        draft,
        photos=photos,
        poster_cls=_Poster,
    )

    assert len(posters) == 1
    assert posters[0].catbox_url == photos[0]


def test_build_smart_update_posters_uses_source_photos_when_ocr_items_absent():
    draft = vk_intake.EventDraft(
        title="Тест",
        date="2026-02-20",
        time="19:00",
        venue="Локация",
    )
    draft.poster_media = []
    photos = ["https://sun9-1.userapi.com/p1.jpg", "https://sun9-1.userapi.com/p2.jpg"]

    class _Poster:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    posters = vk_intake._build_smart_update_posters(
        draft,
        photos=photos,
        poster_cls=_Poster,
    )

    assert [p.catbox_url for p in posters] == photos


def test_build_smart_update_posters_routes_supabase_urls_into_supabase_field():
    draft = vk_intake.EventDraft(
        title="Тест",
        date="2026-02-20",
        time="19:00",
        venue="Локация",
    )
    draft.poster_media = [
        PosterMedia(
            data=b"img",
            name="poster.jpg",
            catbox_url="https://project.supabase.co/storage/v1/object/public/events-media/p/dh16/ab/abc.webp",
        )
    ]

    class _Poster:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    posters = vk_intake._build_smart_update_posters(
        draft,
        photos=None,
        poster_cls=_Poster,
    )

    assert len(posters) == 1
    assert posters[0].catbox_url is None
    assert posters[0].supabase_url == draft.poster_media[0].catbox_url


def test_build_smart_update_posters_routes_yandex_urls_into_supabase_field():
    draft = vk_intake.EventDraft(
        title="Тест",
        date="2026-02-20",
        time="19:00",
        venue="Локация",
    )
    draft.poster_media = [
        PosterMedia(
            data=b"img",
            name="poster.jpg",
            catbox_url="https://storage.yandexcloud.net/kenigevents/p/dh16/ab/abc.webp",
        )
    ]

    class _Poster:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    posters = vk_intake._build_smart_update_posters(
        draft,
        photos=None,
        poster_cls=_Poster,
    )

    assert len(posters) == 1
    assert posters[0].catbox_url is None
    assert posters[0].supabase_url == draft.poster_media[0].catbox_url


@pytest.mark.asyncio
async def test_vk_build_event_drafts_does_not_fail_on_ocr_errors(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async def fake_download(_urls):
        return [(b"img", "p.jpg")]

    async def fake_process_media(_bytes, **_kwargs):
        return [PosterMedia(data=b"img", name="p.jpg")], None

    async def fake_recognize(_db, _photo_bytes, **_kwargs):
        raise RuntimeError("OCR request failed")

    class _Parsed(list):
        festival = None

    async def fake_parse(*_args, **_kwargs):
        return _Parsed(
            [
                {
                    "title": "Событие",
                    "date": "2026-02-20",
                    "time": "19:00",
                    "location_name": "Научная библиотека",
                    "short_description": "Тест",
                }
            ]
        )

    monkeypatch.setattr(vk_intake, "_download_photo_media", fake_download)
    monkeypatch.setattr(vk_intake, "process_media", fake_process_media)
    monkeypatch.setattr(poster_ocr, "recognize_posters", fake_recognize)
    monkeypatch.setattr(main, "parse_event_via_llm", fake_parse)

    drafts, _fest = await vk_intake.build_event_drafts(
        "Текст",
        photos=["https://example.com/a.jpg"],
        source_name="VK",
        location_hint=None,
        default_time=None,
        default_ticket_link=None,
        operator_extra=None,
        publish_ts=None,
        event_ts_hint=None,
        festival_names=[],
        festival_alias_pairs=[],
        festival_hint=False,
        db=db,
    )
    assert drafts


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [25, 0])
async def test_storage_admission_stops_batch_before_claim(tmp_path, monkeypatch, limit):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    def blocked(_db):
        raise RuntimeError("vk_crawl_storage_admission_blocked:free_mb=400:min_free_mb=512")

    async def no_claim(*args, **kwargs):
        pytest.fail("storage-blocked batch must not claim a carrier")

    monkeypatch.setattr(vk_intake, "_require_vk_crawl_storage_headroom", blocked)
    monkeypatch.setattr(vk_auto_queue.vk_review, "pick_next", no_claim)
    bot = DummyBot()
    report = await vk_auto_queue.run_vk_auto_import(db, bot, chat_id=1, limit=limit)
    assert report.inbox_processed == 0
    assert report.inbox_failed_technical == 0
    assert any("storage_admission" in error for error in report.errors)
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT status, details_json FROM ops_run ORDER BY id DESC LIMIT 1")
        status, details = await cur.fetchone()
    assert status == "error"
    assert json.loads(details)["skip_reason"] == "storage_admission"
    assert any("512" in text for _, text in bot.messages)
