import json
from pathlib import Path

import pytest

from db import Database
from models import TelegramSource
from smart_event_update import SmartUpdateResult
from source_parsing.telegram import handlers as tg_handlers


@pytest.fixture(autouse=True)
def _disable_public_tg_fallback(monkeypatch):
    async def fake_fetch_posters(*args, **kwargs):
        return []

    monkeypatch.setattr(tg_handlers, "_fallback_fetch_posters_from_public_tg_page", fake_fetch_posters)


def _results_path(tmp_path: Path) -> Path:
    payload = {
        "schema_version": 2,
        "run_id": "test-reprocess-incomplete-scan",
        "generated_at": "2026-04-27T00:00:00+00:00",
        "stats": {
            "sources_total": 1,
            "messages_scanned": 1,
            "messages_with_events": 1,
            "events_extracted": 1,
        },
        "messages": [
            {
                "source_username": "kraftmarket39",
                "source_title": "Полюбить 39 | Маркет",
                "message_id": 193,
                "message_date": "2026-04-26T18:59:35+00:00",
                "source_link": "https://t.me/kraftmarket39/193",
                "text": (
                    "Продолжается регистрация на лекцию\n\n"
                    "15 июля 16:00\n"
                    "Лекторий ОКЕАНиЯ\n"
                    "Музей Мирового океана, наб. Петра Великого 1, #Калининград"
                ),
                "events": [
                    {
                        "title": "О чём мечтали в советском Калининграде, куда стремились и куда попали",
                        "date": "2026-07-15",
                        "time": "16:00",
                        "location_name": "Лекторий ОКЕАНиЯ, Музей Мирового океана",
                        "location_address": "наб. Петра Великого 1",
                        "city": "Калининград",
                        "event_type": "лекция",
                    }
                ],
            }
        ],
    }
    path = tmp_path / "telegram_results.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


async def _seed_source(db: Database) -> int:
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT id FROM telegram_source WHERE username=? ORDER BY id LIMIT 1",
            ("kraftmarket39",),
        )
        row = await cur.fetchone()
        if row:
            await conn.execute(
                "UPDATE telegram_source SET title=?, enabled=1 WHERE id=?",
                ("Полюбить 39 | Маркет", int(row[0])),
            )
            await conn.commit()
            return int(row[0])
        cur = await conn.execute(
            """
            INSERT INTO telegram_source(username, title, enabled)
            VALUES(?, ?, 1)
            """,
            ("kraftmarket39", "Полюбить 39 | Маркет"),
        )
        await conn.commit()
        return int(cur.lastrowid)


@pytest.mark.asyncio
async def test_reprocesses_legacy_skipped_scan_without_reason(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    source_id = await _seed_source(db)
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO telegram_scanned_message(
                source_id, message_id, status, events_extracted, events_imported, error
            )
            VALUES(?, 193, 'skipped', 1, 0, NULL)
            """,
            (source_id,),
        )
        await conn.commit()

    calls = []

    async def fake_smart_update(db_arg, candidate, **kwargs):
        calls.append(candidate)
        return SmartUpdateResult(status="created")

    monkeypatch.setattr(tg_handlers, "smart_event_update", fake_smart_update)

    report = await tg_handlers.process_telegram_results(_results_path(tmp_path), db)

    assert len(calls) == 1
    assert report.events_created == 1
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, events_extracted, events_imported, error
            FROM telegram_scanned_message
            WHERE source_id=? AND message_id=193
            """,
            (source_id,),
        )
        row = await cur.fetchone()
    assert row == ("done", 1, 1, None)


@pytest.mark.asyncio
async def test_primary_telegram_import_does_not_suppress_vk_sync(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _seed_source(db)

    calls = []

    async def fake_smart_update(db_arg, candidate, **kwargs):
        calls.append(kwargs)
        return SmartUpdateResult(status="created", event_id=5656)

    monkeypatch.setattr(tg_handlers, "smart_event_update", fake_smart_update)

    report = await tg_handlers.process_telegram_results(_results_path(tmp_path), db)

    assert report.events_created == 1
    assert len(calls) == 1
    assert "schedule_kwargs" not in calls[0]


@pytest.mark.asyncio
async def test_primary_telegram_import_rearms_tasks_for_nochange_event(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _seed_source(db)

    scheduled = []

    async def fake_smart_update(db_arg, candidate, **kwargs):
        return SmartUpdateResult(status="skipped_nochange", event_id=5656)

    async def fake_schedule_tasks(db_arg, event_id):
        scheduled.append(event_id)

    monkeypatch.setattr(tg_handlers, "smart_event_update", fake_smart_update)
    monkeypatch.setattr(tg_handlers, "_schedule_primary_import_event_tasks", fake_schedule_tasks)

    report = await tg_handlers.process_telegram_results(_results_path(tmp_path), db)

    assert report.events_nochange == 1
    assert scheduled == [5656]


@pytest.mark.asyncio
async def test_primary_telegram_import_does_not_run_global_vk_reconcile_by_default(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    await _seed_source(db)
    monkeypatch.delenv("TG_MONITORING_GLOBAL_VK_RECONCILE", raising=False)

    async def fake_smart_update(db_arg, candidate, **kwargs):
        return SmartUpdateResult(status="skipped_nochange", event_id=5656)

    async def fake_schedule_tasks(db_arg, event_id):
        return None

    async def fail_global_reconcile(db_arg):
        raise AssertionError("global VK reconcile must be opt-in")

    monkeypatch.setattr(tg_handlers, "smart_event_update", fake_smart_update)
    monkeypatch.setattr(tg_handlers, "_schedule_primary_import_event_tasks", fake_schedule_tasks)
    monkeypatch.setattr(tg_handlers, "_reconcile_primary_import_vk_sync_jobs", fail_global_reconcile)

    report = await tg_handlers.process_telegram_results(_results_path(tmp_path), db)

    assert report.events_nochange == 1


@pytest.mark.asyncio
async def test_forced_existing_single_source_still_runs_smart_update_and_rearms_tasks(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    source_id = await _seed_source(db)
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO telegram_source_force_message(source_id, message_id) VALUES(?, 193)",
            (source_id,),
        )
        await conn.commit()

    scheduled = []

    async def fake_schedule_tasks(db_arg, event_id):
        scheduled.append(event_id)

    calls = []

    async def fake_smart_update(db_arg, candidate, **kwargs):
        calls.append(candidate)
        return SmartUpdateResult(status="skipped_nochange", event_id=5656)

    monkeypatch.setattr(tg_handlers, "_schedule_primary_import_event_tasks", fake_schedule_tasks)
    monkeypatch.setattr(tg_handlers, "smart_event_update", fake_smart_update)

    report = await tg_handlers.process_telegram_results(_results_path(tmp_path), db)

    assert report.events_nochange == 1
    assert len(calls) == 1
    assert calls[0].source_url == "https://t.me/kraftmarket39/193"
    assert scheduled == [5656]


@pytest.mark.asyncio
async def test_keeps_documented_skipped_scan_metrics_only(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    source_id = await _seed_source(db)
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO telegram_scanned_message(
                source_id, message_id, status, events_extracted, events_imported, error
            )
            VALUES(?, 193, 'skipped', 1, 0, '{"skip_breakdown":{"invalid:missing_location":1}}')
            """,
            (source_id,),
        )
        await conn.commit()

    calls = []

    async def fake_smart_update(db_arg, candidate, **kwargs):
        calls.append(candidate)
        return SmartUpdateResult(status="created")

    monkeypatch.setattr(tg_handlers, "smart_event_update", fake_smart_update)

    report = await tg_handlers.process_telegram_results(_results_path(tmp_path), db)

    assert calls == []
    assert report.messages_metrics_only == 1
    assert report.metrics_only_posts[0].reason == "already_scanned"


@pytest.mark.asyncio
async def test_stores_skip_breakdown_for_new_incomplete_scan(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    source_id = await _seed_source(db)

    async def fake_smart_update(db_arg, candidate, **kwargs):
        return SmartUpdateResult(status="invalid", reason="missing_location")

    monkeypatch.setattr(tg_handlers, "smart_event_update", fake_smart_update)

    report = await tg_handlers.process_telegram_results(_results_path(tmp_path), db)

    assert report.events_invalid == 1
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT status, events_extracted, events_imported, error
            FROM telegram_scanned_message
            WHERE source_id=? AND message_id=193
            """,
            (source_id,),
        )
        row = await cur.fetchone()
    assert row[0:3] == ("skipped", 1, 0)
    assert json.loads(row[3]) == {"skip_breakdown": {"invalid:missing_location": 1}}


def test_zero_ticket_price_does_not_override_false_free_flag_for_tg_candidate():
    source = TelegramSource(username="kraftmarket39", title="Полюбить 39 | Маркет", enabled=True)
    message = {
        "source_username": "kraftmarket39",
        "message_id": 196,
        "source_link": "https://t.me/kraftmarket39/196",
        "text": (
            "19.05 История парусного спорта в Калининградской области\n\n"
            "по регистрации\n\n"
            "19 мая 16:00\n"
            "Лекторий ОКЕАНиЯ\n"
            "Музей Мирового океана, наб. Петра Великого 1, #Калининград"
        ),
        "posters": [],
    }
    event = {
        "title": "История парусного спорта в Калининградской области",
        "date": "2026-05-19",
        "time": "16:00",
        "location_name": "Лекторий ОКЕАНиЯ, Музей Мирового океана",
        "location_address": "наб. Петра Великого 1",
        "city": "Калининград",
        "event_type": "лекция",
        "ticket_price_min": 0.0,
        "is_free": False,
    }

    candidate = tg_handlers._build_candidate(source, message, event)

    assert candidate.is_free is False
