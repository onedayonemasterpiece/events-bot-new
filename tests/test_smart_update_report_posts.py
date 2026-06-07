import pytest

import main
from source_parsing.handlers import AddedEventInfo, SourceParsingResult, format_parsing_report


@pytest.mark.asyncio
async def test_format_parsing_report_shows_vk_tg_posts_and_vk_coauthor(tmp_path, monkeypatch):
    db = main.Database(str(tmp_path / "db.sqlite"))
    await db.init()
    monkeypatch.setenv("VK_EVENTS_GROUP_ID", "231920894")
    monkeypatch.setenv("VK_AFISHA_GROUP_ID", "231920894")

    event = main.Event(
        title="Лекция в научной библиотеке",
        description="Калининградская областная научная библиотека",
        source_text="КОНБ приглашает на лекцию",
        date="2026-06-20",
        time="18:00",
        location_name="Калининградская областная научная библиотека",
        city="Калининград",
        telegraph_url="https://telegra.ph/event",
        ics_url="https://example.com/event.ics",
        source_vk_post_url="https://vk.com/wall-231920894_2395",
        tg_event_post_url="https://t.me/kldevents/42",
    )
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id = int(event.id)

    result = SourceParsingResult(
        total_events=1,
        added_events=[
            AddedEventInfo(
                event_id=event_id,
                title=event.title,
                telegraph_url=event.telegraph_url,
                ics_url=event.ics_url,
                log_cmd="/log 1",
                date=event.date,
                time=event.time,
                source="vk",
                source_url="https://vk.com/konb39?w=wall-30777579_15383",
            )
        ],
    )

    report = await format_parsing_report(result, bot_username="events_bot", db=db)

    assert "Посты: VK [пост](https://vk.com/wall-231920894_2395)" in report
    assert "TG [пост](https://t.me/kldevents/42)" in report
    assert "соавторство: [@konb39](https://vk.com/konb39) предложено" in report
