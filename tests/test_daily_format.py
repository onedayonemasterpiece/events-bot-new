from datetime import date, datetime, timezone, timedelta

import pytest

from db import Database

import main
from source_parsing.post_metrics import upsert_telegram_post_metric, upsert_vk_post_metric


def make_event(**kwargs: object) -> main.Event:
    base = {
        "title": "Event",
        "description": "Описание",
        "source_text": "source",
        "date": date(2024, 1, 1).isoformat(),
        "time": "18:00",
        "location_name": "Place",
    }
    base.update(kwargs)
    return main.Event(**base)


def test_format_event_daily_does_not_use_partner_vk_link_without_telegraph() -> None:
    event = make_event(
        source_post_url="https://vk.com/wall-1_1",
        creator_id=123,
    )

    rendered = main.format_event_daily(event, partner_creator_ids={123})

    assert '<a href="https://vk.com/wall-1_1">' not in rendered
    assert "<b>🚩 Event</b>" in rendered


def test_format_event_daily_prefers_telegraph_for_vk_queue() -> None:
    event = make_event(
        source_vk_post_url="https://vk.com/wall-1_2",
        telegraph_url="https://telegra.ph/test",
    )

    rendered = main.format_event_daily(event)

    assert '<a href="https://telegra.ph/test">' in rendered


def test_format_event_daily_prefers_telegraph_for_vk_source_url() -> None:
    event = make_event(
        source_post_url="https://vk.com/wall-1_3",
        telegraph_url="https://telegra.ph/source",
    )

    rendered = main.format_event_daily(event)

    assert '<a href="https://telegra.ph/source">' in rendered


def test_format_event_daily_promo_highlight_uses_subtle_marker() -> None:
    event = make_event()

    rendered = main.format_event_daily(event, promo_highlight=True)
    inline = main.format_event_daily_inline(event, promo_highlight=True)

    assert "<b>✨ " in rendered
    assert "Event</b>" in rendered
    assert inline.startswith("01.01 ✨ ")
    assert "Event" in inline
    assert "promo" not in rendered.casefold()
    assert "promo" not in inline.casefold()



def test_format_event_daily_marks_tretyakov_with_picture_pair() -> None:
    event = make_event(
        title="Александр Дейнека",
        emoji="🖼️",
        location_name="Филиал Третьяковской галереи",
    )

    rendered = main.format_event_daily(event, highlight=True)

    assert rendered.startswith("<b>👉 🚩 🖼️ Александр Дейнека</b>")
    assert "<i>1 января 18:00 🖼🖼 Филиал Третьяковской галереи</i>" in rendered


def test_format_event_daily_does_not_mark_tretyakov_by_title_only() -> None:
    event = make_event(
        title="Александр Дейнека",
        emoji="🖼️",
        description="Лекция о художниках Третьяковской галереи.",
        location_name="Другая площадка",
        added_at=datetime.now(timezone.utc),
    )

    rendered = main.format_event_daily(event, highlight=True)
    inline = main.format_event_daily_inline(event)

    assert rendered.startswith("<b>👉 🚩 🖼️ Александр Дейнека</b>")
    assert "🖼🖼 Другая площадка" not in rendered
    assert inline.startswith("01.01 🚩 🖼️ Александр Дейнека")


def test_format_event_daily_inline_replaces_recent_flag_for_tretyakov() -> None:
    event = make_event(
        title="Лекция «Чулки и носки»",
        emoji="🖼️",
        location_name="Филиал Третьяковской галереи",
        added_at=datetime.now(timezone.utc),
    )

    rendered = main.format_event_daily_inline(event)

    assert rendered.startswith("01.01 🖼🖼 Лекция")
    assert "🚩" not in rendered


def test_format_event_daily_marks_rock_concert_with_horns_icon() -> None:
    event = make_event(
        title="Концерт группы «Крематорий»",
        emoji="🎸",
        description="Концерт рок-группы.",
        search_digest="Легендарная рок-группа исполняет юбилейную программу.",
    )

    rendered = main.format_event_daily(event)
    inline = main.format_event_daily_inline(event)

    assert rendered.startswith("<b>🚩 🤘 Концерт группы «Крематорий»</b>")
    assert inline.startswith("01.01 🚩 🤘 Концерт группы «Крематорий»")


def test_format_event_daily_handles_timezone_aware_added_at() -> None:
    event = make_event(
        added_at=datetime(2024, 1, 2, 12, tzinfo=timezone.utc),
    )

    rendered = main.format_event_daily(event)

    assert isinstance(rendered, str)


def test_format_event_daily_keeps_city_hashtag_for_adjectival_venue_name() -> None:
    event = make_event(
        location_name="Клуб Светлогорского военного санатория",
        location_address="Октябрьская 28",
        city="Светлогорск",
    )

    rendered = main.format_event_daily(event)

    assert "Клуб Светлогорского военного санатория, Октябрьская 28, #Светлогорск" in rendered


def test_format_event_daily_drops_city_only_when_city_token_is_present() -> None:
    event = make_event(
        location_name="Телеграф, Островского 3, Светлогорск",
        location_address="Островского 3",
        city="Светлогорск",
    )

    rendered = main.format_event_daily(event)

    assert "Телеграф, Островского 3, Светлогорск, Островского 3, #Светлогорск" not in rendered
    assert "Телеграф, Островского 3, Светлогорск" in rendered


def test_split_daily_text_atomic_keeps_event_card_together() -> None:
    first = "\n".join(
        [
            "<b>👉 First</b>",
            "Описание первого события",
            "<i>26 апреля 12:00 Hall</i>",
        ]
    )
    second = "\n".join(
        [
            "<b>👉 Second</b>",
            "Описание второго события",
            "<i>26 апреля 13:00 Hall</i>",
        ]
    )
    text = "HEAD\n\n" + first + "\n\n" + second

    parts = main.split_daily_text_atomic(text, limit=len("HEAD\n\n" + first) + 1)

    assert len(parts) == 2
    assert "First" in parts[0]
    assert "26 апреля 12:00" in parts[0]
    assert "Second" in parts[1]
    assert "26 апреля 13:00" in parts[1]


@pytest.mark.asyncio
async def test_build_daily_posts_lists_recent_festivals(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    now = datetime(2025, 7, 15, 12, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(
            main.Festival(
                name="Fest",
                telegraph_path="Fest",
                created_at=now,
            )
        )
        session.add(
            main.Event(
                title="New Event",
                description="Desc",
                source_text="source",
                date=(now.date() + timedelta(days=1)).isoformat(),
                time="18:00",
                location_name="Place",
                added_at=now,
            )
        )
        await session.commit()

    posts = await main.build_daily_posts(db, timezone.utc, now)
    text = posts[0][0]

    assert "ФЕСТИВАЛИ" in text
    assert '<a href="https://telegra.ph/Fest">✨ Fest</a>' in text


@pytest.mark.asyncio
async def test_build_daily_posts_includes_fair_when_few_events(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    now = datetime(2026, 1, 3, 12, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(
            main.Event(
                title="Fair",
                description="Desc",
                source_text="source",
                date="2025-12-25",
                end_date="2026-01-10",
                time="10:00..17:30",
                location_name="Market",
                event_type="ярмарка",
            )
        )
        await session.commit()

    posts = await main.build_daily_posts(db, timezone.utc, now)
    combined = "\n".join(p[0] for p in posts)
    assert "Fair" in combined
    assert main.format_day_pretty(now.date()) in combined


@pytest.mark.asyncio
async def test_build_daily_posts_labels_popular_added_event_from_all_sources(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    now = datetime(2026, 6, 30, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        tg_source = main.TelegramSource(username="tgsrc", title="TG Source")
        session.add(tg_source)
        for idx in range(10):
            event = main.Event(
                title=f"Event {idx}",
                description="Desc",
                source_text="source",
                date=(now.date() + timedelta(days=idx + 1)).isoformat(),
                time="18:00",
                location_name="Place",
                city="Калининград",
                added_at=now - timedelta(hours=1),
                source_post_url="https://t.me/tgsrc/100" if idx == 0 else None,
                source_vk_post_url="https://vk.com/wall-1_200" if idx == 0 else None,
            )
            session.add(event)
        await session.flush()
        source_id = int(tg_source.id)
        await session.commit()

    collected_ts = int(now.timestamp())
    await upsert_telegram_post_metric(
        db,
        source_id=source_id,
        message_id=100,
        age_day=0,
        source_url="https://t.me/tgsrc/100",
        message_ts=collected_ts - 3600,
        views=100,
        likes=8,
        comments=3,
        collected_ts=collected_ts,
    )
    await upsert_vk_post_metric(
        db,
        group_id=1,
        post_id=200,
        age_day=0,
        source_url="https://vk.com/wall-1_200",
        post_ts=collected_ts - 3600,
        views=50,
        likes=4,
        comments=1,
        reposts=2,
        collected_ts=collected_ts,
    )

    posts = await main.build_daily_posts(db, timezone.utc, now)
    combined = "\n".join(p[0] for p in posts)

    assert "+10 ДОБАВИЛИ В АНОНС" in combined
    assert '<tg-emoji emoji-id="5339188899241570417">❤️</tg-emoji> 12' in combined
    assert '<tg-emoji emoji-id="5336998942661975661">🔂</tg-emoji> 2' in combined
    assert combined.count('<tg-emoji emoji-id="5339188899241570417">❤️</tg-emoji>') == 1
    await db.close()
