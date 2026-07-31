import json
from pathlib import Path

import pytest

from db import Database
import smart_event_update as smart_update_module
from smart_event_update import EventCandidate
from source_parsing.telegram import handlers as tg_handlers


REPLAY = (
    Path(__file__).parent
    / "replays"
    / "INC-2026-07-31-false-kgd80-festival-link"
    / "telegram_results.json"
)


def _messages() -> list[dict]:
    return json.loads(REPLAY.read_text(encoding="utf-8"))["messages"]


def _source() -> object:
    from models import TelegramSource

    return TelegramSource(
        username="kldevents",
        title="Калининград: мероприятия",
        enabled=True,
    )


def test_generic_regional_anniversary_does_not_ground_kgd80_campaign():
    negative = _messages()[0]

    candidate = tg_handlers._build_candidate(
        _source(),
        negative,
        negative["events"][0],
    )

    assert candidate.festival is None


def test_literal_kgd80_name_hashtag_or_domain_keeps_campaign():
    positive = _messages()[1]

    candidate = tg_handlers._build_candidate(
        _source(),
        positive,
        positive["events"][0],
    )

    assert candidate.festival == "80 историй о главном"


@pytest.mark.asyncio
async def test_replay_crosses_server_import_and_real_smart_update_boundary(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "replay.sqlite"))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO telegram_source(username, title, enabled)
            VALUES('kldevents', 'Калининград: мероприятия', 1)
            """
        )
        await conn.commit()

    monkeypatch.setattr(smart_update_module, "SMART_UPDATE_LLM_DISABLED", True)

    async def no_public_fallback(*_args, **_kwargs):
        return []

    monkeypatch.setattr(
        tg_handlers,
        "_fallback_fetch_posters_from_public_tg_page",
        no_public_fallback,
    )
    real_smart_update = smart_update_module.smart_event_update

    async def real_smart_update_without_outbox(db_arg, candidate, **kwargs):
        kwargs["schedule_tasks"] = False
        return await real_smart_update(db_arg, candidate, **kwargs)

    monkeypatch.setattr(
        tg_handlers,
        "smart_event_update",
        real_smart_update_without_outbox,
    )

    report = await tg_handlers.process_telegram_results(REPLAY, db)

    assert report.events_created == 2
    assert report.events_errored == 0
    async with db.raw_conn() as conn:
        rows = await (
            await conn.execute(
                "SELECT title, festival FROM event ORDER BY id"
            )
        ).fetchall()

    assert rows == [
        ("Встреча «Портрет на фоне города»", None),
        ("Лекция «История здравоохранения»", "80 историй о главном"),
    ]


@pytest.mark.asyncio
async def test_smart_update_central_guard_rejects_ungrounded_kgd80_from_vk(
    tmp_path, monkeypatch
):
    db = Database(str(tmp_path / "smart-update.sqlite"))
    await db.init()
    monkeypatch.setattr(smart_update_module, "SMART_UPDATE_LLM_DISABLED", True)
    candidate = EventCandidate(
        source_type="vk",
        source_url="https://vk.com/wall-114604162_40601",
        source_text=(
            "Встреча приурочена к 80-летию образования Калининградской области. "
            "5 августа в 15:00, Зеленоградская городская библиотека."
        ),
        title="Встреча «Портрет на фоне города»",
        date="2026-08-05",
        time="15:00",
        location_name="Зеленоградская городская библиотека",
        city="Зеленоградск",
        festival="80 историй о главном",
        festival_context="event_with_festival",
    )

    result = await smart_update_module.smart_event_update(
        db,
        candidate,
        schedule_tasks=False,
    )

    assert result.status == "created"
    assert candidate.festival is None
    assert candidate.festival_context is None
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT festival FROM event WHERE id=?",
                (result.event_id,),
            )
        ).fetchone()
    assert row == (None,)
