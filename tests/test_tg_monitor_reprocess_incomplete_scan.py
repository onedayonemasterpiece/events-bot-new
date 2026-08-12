import json
from datetime import date
from pathlib import Path

import pytest
import pytest_asyncio

from db import Database
from models import TelegramSource
from smart_event_update import SmartUpdateResult
from source_parsing.telegram import handlers as tg_handlers


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


@pytest.fixture(autouse=True)
def _disable_public_tg_fallback(monkeypatch):
    async def fake_fetch_posters(*args, **kwargs):
        return []

    monkeypatch.setattr(tg_handlers, "_fallback_fetch_posters_from_public_tg_page", fake_fetch_posters)
    # Freeze the incident replay at its original operational window so the
    # fixture does not silently turn into a past-event test over time.
    should_skip = tg_handlers._should_skip_past_event_candidate
    can_import = tg_handlers._event_payload_can_still_be_imported
    fixed_today = date(2026, 6, 13)
    monkeypatch.setattr(
        tg_handlers,
        "_should_skip_past_event_candidate",
        lambda candidate, *, today=None: should_skip(candidate, today=fixed_today),
    )
    monkeypatch.setattr(
        tg_handlers,
        "_event_payload_can_still_be_imported",
        lambda event_data, *, today=None: can_import(event_data, today=fixed_today),
    )


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


def _kraftmarket_incident_results_path(tmp_path: Path) -> Path:
    payload = {
        "schema_version": 2,
        "run_id": "INC-2026-06-13-kraftmarket-promo-zero-events-replay",
        "generated_at": "2026-06-13T07:00:00+00:00",
        "stats": {
            "sources_total": 1,
            "messages_scanned": 2,
            "messages_with_events": 2,
            "events_extracted": 2,
        },
        "messages": [
            {
                "source_username": "kraftmarket39",
                "source_title": "Полюбить 39 | Маркет",
                "message_id": 285,
                "message_date": "2026-06-12T10:58:15+00:00",
                "source_link": "https://t.me/kraftmarket39/285",
                "text": (
                    "Друзья, вы этого просили, мы это сделали!\n\n"
                    "Лекции Дмитрия Манкевича о становлении здравоохранения в первые годы "
                    "становления области перенесена в Историко-художественный музей на ту же дату "
                    "и то же время.\n\n"
                    "19 августа 18:30\n"
                    "Историко-художественный музей, Клиническая 21, #Калининград\n"
                    "Лекция проходит в рамках фестиваля «80 историй о главном».\n"
                    "ПЕРЕНОС, билеты действительны\n"
                    "бесплатно, по регистрации"
                ),
                "links": [
                    {
                        "url": "https://kgd80.ru/sobytiya/kaliningradskoe-zdravoohranenie-v-period-stanovleniya-oblasti-osobennosti-vyzovy-pobedy-i-problemy/?register=1",
                        "text": "бесплатно, по регистрации",
                        "source": "entity",
                    }
                ],
                "events": [
                    {
                        "title": "Калининградское здравоохранение в период становления области",
                        "date": "2026-08-19",
                        "time": "18:30",
                        "location_name": "Историко-художественный музей",
                        "location_address": "Клиническая 21",
                        "city": "Калининград",
                        "ticket_link": "https://kgd80.ru/sobytiya/kaliningradskoe-zdravoohranenie-v-period-stanovleniya-oblasti-osobennosti-vyzovy-pobedy-i-problemy/?register=1",
                        "event_type": "лекция",
                        "is_free": True,
                        "festival": "80 историй о главном",
                    }
                ],
            },
            {
                "source_username": "kraftmarket39",
                "source_title": "Полюбить 39 | Маркет",
                "message_id": 287,
                "message_date": "2026-06-12T16:14:11+00:00",
                "source_link": "https://t.me/kraftmarket39/287",
                "text": (
                    "15.08 • 12:45 «Бородин. Гениальный дилетант» — почему великий учёный смог "
                    "стать великим композитором.\n\n"
                    "Событие проходит в рамках образовательной программы фестиваля Кантата.\n\n"
                    "15 августа 12:45\n"
                    "Филиал Третьяковской галереи, Парадная наб. 3, #Калининград\n"
                    "Бесплатно, по регистрации"
                ),
                "links": [
                    {
                        "url": "https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/46524/2026-06-15/12:45:00",
                        "text": "«Бородин. Гениальный дилетант»",
                        "source": "entity",
                    }
                ],
                "events": [
                    {
                        "title": "Бородин. Гениальный дилетант",
                        "date": "2026-08-15",
                        "time": "12:45",
                        "location_name": "Филиал Третьяковской галереи",
                        "location_address": "Парадная наб. 3",
                        "city": "Калининград",
                        "ticket_link": "https://kaliningrad.tretyakovgallery.ru/tickets/#/buy/event/46524/2026-06-15/12:45:00",
                        "event_type": "лекция",
                        "is_free": True,
                        "festival": "Кантата",
                    }
                ],
            },
        ],
    }
    path = tmp_path / "kraftmarket_incident_telegram_results.json"
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
        return SmartUpdateResult(status="created", event_id=5656)

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
async def test_reprocesses_kraftmarket_producer_zero_events_after_fixed_producer_payload(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    source_id = await _seed_source(db)
    async with db.raw_conn() as conn:
        await conn.executemany(
            """
            INSERT INTO telegram_scanned_message(
                source_id, message_id, status, events_extracted, events_imported, error
            )
            VALUES(?, ?, 'skipped', 0, 0, 'producer_zero_events:clear_event_signals')
            """,
            [(source_id, 285), (source_id, 287)],
        )
        await conn.commit()

    calls = []

    async def fake_smart_update(db_arg, candidate, **kwargs):
        calls.append(candidate)
        return SmartUpdateResult(status="created", event_id=9000 + len(calls))

    monkeypatch.setattr(tg_handlers, "smart_event_update", fake_smart_update)

    report = await tg_handlers.process_telegram_results(_kraftmarket_incident_results_path(tmp_path), db)

    assert report.events_created == 2
    assert [c.source_url for c in calls] == [
        "https://t.me/kraftmarket39/285",
        "https://t.me/kraftmarket39/287",
    ]
    assert calls[0].title == "Калининградское здравоохранение в период становления области"
    assert calls[0].date == "2026-08-19"
    assert calls[0].time == "18:30"
    assert calls[0].festival == "80 историй о главном"
    assert calls[1].title == "Бородин. Гениальный дилетант"
    assert calls[1].date == "2026-08-15"
    assert calls[1].time == "12:45"
    assert calls[1].festival == "Кантата"
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT message_id, status, events_extracted, events_imported, error
            FROM telegram_scanned_message
            WHERE source_id=? AND message_id IN (285, 287)
            ORDER BY message_id
            """,
            (source_id,),
        )
        rows = await cur.fetchall()
    assert rows == [(285, "done", 1, 1, None), (287, "done", 1, 1, None)]


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

    assert report.events_rejected == 1
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
    assert json.loads(row[3]) == {
        "skip_breakdown": {"rejected_product_policy:missing_location": 1}
    }


@pytest.mark.asyncio
async def test_no_manifest_zero_event_tail_remains_retryable(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            "SELECT id FROM telegram_source WHERE username=?",
            ("ecodvor39",),
        )
        row = await cur.fetchone()
        assert row is not None
        source_id = int(row[0])
        await conn.execute(
            "UPDATE telegram_source SET last_scanned_message_id=NULL, last_scan_at=NULL WHERE id=?",
            (source_id,),
        )
        await conn.commit()

    payload = {
        "schema_version": 2,
        "run_id": "INC-2026-07-14-ecodvor-zero-tail",
        "generated_at": "2026-07-14T15:00:00+00:00",
        "stats": {"sources_total": 1, "messages_scanned": 2, "events_extracted": 0},
        "messages": [
            {
                "source_username": "ecodvor39",
                "message_id": 934,
                "message_date": "2026-07-13T18:32:01+00:00",
                "source_link": "https://t.me/ecodvor39/934",
                "text": "История фриганской жизни. Подробности программы будут позже.",
                "events": [],
                "source_parse_decision": {
                    "disposition": "CONFIRMED_NO_EVENT",
                    "no_event_reason": "VAGUE_TEASER",
                    "events": [],
                    "lifecycle_actions": [],
                    "evidence_complete": True,
                    "parse_version": "source-parse-v1",
                },
            },
            {
                "source_username": "ecodvor39",
                "message_id": 935,
                "message_date": "2026-07-14T09:43:05+00:00",
                "source_link": "https://t.me/ecodvor39/935",
                "text": "Что можно принести в зону рукодельного свопа.",
                "events": [],
                "source_parse_decision": {
                    "disposition": "CONFIRMED_NO_EVENT",
                    "no_event_reason": "NO_ATTENDABLE_EVENT",
                    "events": [],
                    "lifecycle_actions": [],
                    "evidence_complete": True,
                    "parse_version": "source-parse-v1",
                },
            },
        ],
    }
    path = tmp_path / "zero_tail_results.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    async def fail_smart_update(*_args, **_kwargs):
        raise AssertionError("true zero-event tail must not enter Smart Update")

    monkeypatch.setattr(tg_handlers, "smart_event_update", fail_smart_update)
    report = await tg_handlers.process_telegram_results(path, db)

    assert report.events_created == 0
    assert report.events_merged == 0
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT last_scanned_message_id, last_scan_at FROM telegram_source WHERE id=?",
                (source_id,),
            )
        ).fetchone()
        scanned_count = (
            await (
                await conn.execute(
                    "SELECT count(*) FROM telegram_scanned_message WHERE source_id=?",
                    (source_id,),
                )
            ).fetchone()
        )[0]
    assert row[0] is None
    assert row[1] is None
    # A decision without its producer-owned evidence manifest is not a typed
    # terminal even when the old receipt claimed evidence_complete=true.
    assert scanned_count == 2
    await db.close()


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
