from __future__ import annotations

import pytest
from sqlalchemy import text

import smart_event_update as su
from db import Database
from models import Event


def _event(*, city: str, venue: str = "Дом культуры") -> Event:
    return Event(
        title="Вечерняя программа",
        description="Программа с участием артистов.",
        date="2026-08-15",
        time="19:00",
        location_name=venue,
        city=city,
        event_type="концерт",
        source_text="Начало в 19:00. Подробности программы опубликованы организатором.",
    )


def _candidate(*, city: str, venue: str = "Дом культуры", source_text: str) -> su.EventCandidate:
    return su.EventCandidate(
        source_type="telegram",
        source_url="https://t.me/example/101",
        source_text=source_text,
        title="Вечерняя программа",
        date="2026-08-15",
        time="19:00",
        location_name=venue,
        city=city,
        event_type="концерт",
    )


@pytest.mark.asyncio
async def test_extracted_duration_skips_forecast_provider(monkeypatch) -> None:
    event = _event(city="Светлогорск")
    candidate = _candidate(
        city="Светлогорск",
        source_text="Начало в 19:00. Продолжительность: 1 час 30 минут.",
    )

    async def unexpected_provider(*_args, **_kwargs):
        raise AssertionError("duration provider must not be called")

    monkeypatch.setattr(su, "_ask_gemma_json", unexpected_provider)
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)

    assert await su._ensure_transport_duration_forecast(event, candidate) is False
    assert event.duration_forecast_minutes is None


@pytest.mark.asyncio
async def test_non_transport_event_skips_forecast_provider(monkeypatch) -> None:
    event = _event(city="Калининград", venue="Драматический театр")
    candidate = _candidate(
        city="Калининград",
        venue="Драматический театр",
        source_text="Начало в 19:00. Новая сценическая программа.",
    )

    async def unexpected_provider(*_args, **_kwargs):
        raise AssertionError("duration provider must not be called")

    monkeypatch.setattr(su, "_ask_gemma_json", unexpected_provider)
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)

    assert await su._ensure_transport_duration_forecast(event, candidate) is False
    assert event.duration_forecast_minutes is None


@pytest.mark.asyncio
async def test_eligible_missing_duration_persists_forecast(tmp_path, monkeypatch) -> None:
    calls: list[dict] = []

    async def forecast_provider(_prompt, _schema, **kwargs):
        calls.append(kwargs)
        return {
            "duration_minutes": 110,
            "confidence": 0.82,
            "reason_short": "Обычная вечерняя программа с несколькими участниками.",
        }

    monkeypatch.setattr(su, "_ask_gemma_json", forecast_provider)
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)
    event = _event(city="Светлогорск")
    candidate = _candidate(
        city="Светлогорск",
        source_text="Начало в 19:00. В программе выступления нескольких артистов.",
    )

    assert await su._ensure_transport_duration_forecast(event, candidate) is True
    assert event.duration_forecast_minutes == 110
    assert calls == [{"max_tokens": 120, "label": "duration_forecast"}]

    db = Database(str(tmp_path / "duration-forecast.sqlite"))
    await db.init()
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        event_id = int(event.id or 0)
    async with db.get_session() as session:
        stored = await session.get(Event, event_id)
        assert stored is not None
        assert stored.duration_forecast_minutes == 110
    async with db.engine.connect() as conn:
        columns = {
            row[1]: row
            for row in (
                await conn.execute(text("pragma table_info(event)"))
            ).fetchall()
        }
        assert columns["duration_forecast_minutes"][3] == 0
        assert columns["duration_forecast_minutes"][4] is None
    await db.close()


@pytest.mark.asyncio
async def test_smart_update_create_commits_forecast_field(tmp_path, monkeypatch) -> None:
    async def no_topics(*_args, **_kwargs):
        return None

    async def forecast_hook(event, _candidate):
        event.duration_forecast_minutes = 105
        return True

    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", True)
    monkeypatch.setattr(su, "_classify_topics", no_topics)
    monkeypatch.setattr(su, "_ensure_transport_duration_forecast", forecast_hook)
    db = Database(str(tmp_path / "smart-update-duration.sqlite"))
    await db.init()
    candidate = su.EventCandidate(
        source_type="parser:venue",
        source_url="https://venue.example/events/winter-concert",
        source_text=(
            "Официальная карточка зимнего концерта. Начало 15 декабря в 19:00. "
            "В программе выступления ансамбля и солистов."
        ),
        title="Зимний концерт",
        date="2026-12-15",
        time="19:00",
        location_name="Дом культуры",
        city="Светлогорск",
        event_type="концерт",
        trust_level="high",
    )

    result = await su.smart_event_update(
        db,
        candidate,
        check_source_url=False,
        schedule_tasks=False,
    )

    assert result.status == "created"
    async with db.get_session() as session:
        stored = await session.get(Event, result.event_id)
        assert stored is not None
        assert stored.duration_forecast_minutes == 105
    await db.close()


@pytest.mark.asyncio
async def test_new_explicit_duration_clears_stale_forecast_without_provider(
    monkeypatch,
) -> None:
    event = _event(city="Светлогорск")
    event.duration_forecast_minutes = 120
    candidate = _candidate(
        city="Светлогорск",
        source_text="Продолжительность концерта составляет 80 минут.",
    )

    async def unexpected_provider(*_args, **_kwargs):
        raise AssertionError("duration provider must not be called")

    monkeypatch.setattr(su, "_ask_gemma_json", unexpected_provider)
    monkeypatch.setattr(su, "SMART_UPDATE_LLM_DISABLED", False)

    assert await su._ensure_transport_duration_forecast(event, candidate) is True
    assert event.duration_forecast_minutes is None
