from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from db import Database
from kgd80_social import (
    calculate_social_bonus,
    collect_kgd80_social_summary,
    format_kgd80_social_report,
    parse_vk_wall_ids,
    send_kgd80_social_report,
)


class FakeBot:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str]] = []

    async def send_message(self, chat_id: int, text: str) -> None:
        self.messages.append((chat_id, text))


def test_parse_vk_wall_ids_normalizes_group_owner() -> None:
    assert parse_vk_wall_ids("https://vk.com/wall-231828790_42") == (231828790, 42)
    assert parse_vk_wall_ids("https://vk.com/wall123_7") == (123, 7)
    assert parse_vk_wall_ids("https://vk.com/photo-1_2") is None


def test_calculate_social_bonus_weights_reposts_strongest_and_caps_views() -> None:
    points = calculate_social_bonus(views=10_000, likes=3, comments=2, reposts=1, posts=1)
    assert points["repost"] == 8
    assert points["comment"] == 10
    assert points["like"] == 3
    assert points["view"] == 20
    assert points["total"] == 41


@pytest.mark.asyncio
async def test_collect_kgd80_summary_aggregates_verified_vk_metrics(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime(2026, 6, 24, 20, 30, tzinfo=ZoneInfo("Europe/Kaliningrad"))
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            INSERT INTO promo_campaign(title, status, starts_at)
            VALUES('80 историй о главном / summer visibility', 'active', '2026-06-01 00:00:00')
            """
        )
        campaign_id = int(cur.lastrowid)
        await conn.execute(
            """
            INSERT INTO promo_target(campaign_id, target_type, festival_name)
            VALUES(?, 'festival', '80 историй о главном')
            """,
            (campaign_id,),
        )
        await conn.execute(
            """
            INSERT INTO promo_exposure(campaign_id, event_id, surface, placement_kind, publish_status, published_at, details_json)
            VALUES(?, 1, 'vk_publication', 'vk_publication', 'VK_SCHEDULED', '2026-06-24 15:00:00', ?)
            """,
            (campaign_id, '{"target_url":"https://vk.com/wall-231828790_42"}'),
        )
        await conn.execute(
            """
            INSERT INTO vk_post_metric(group_id, post_id, age_day, source_url, post_ts, collected_ts, views, likes, comments, reposts)
            VALUES(231828790, 42, 0, 'https://vk.com/wall-231828790_42', 1, 2, 100, 7, 3, 2)
            """
        )
        await conn.commit()

    summary = await collect_kgd80_social_summary(db, now=now)

    assert summary.first_report is True
    assert summary.posts == 1
    assert summary.views == 100
    assert summary.likes == 7
    assert summary.comments == 3
    assert summary.reposts == 2
    assert summary.total_points == 40
    text = format_kgd80_social_report(summary)
    assert "Сообщение создано автоматически" in text
    assert "репосты: 2 → +16" in text


@pytest.mark.asyncio
async def test_send_kgd80_social_report_marks_first_send(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    bot = FakeBot()
    now = datetime(2026, 6, 24, 20, 30, tzinfo=ZoneInfo("Europe/Kaliningrad"))

    first = await send_kgd80_social_report(db, bot, chat_id=123, now=now)
    second = await send_kgd80_social_report(db, bot, chat_id=123, now=now)

    assert first and first.first_report is True
    assert second and second.first_report is False
    assert len(bot.messages) == 2
    assert bot.messages[0][0] == 123
    assert "Первичный отчёт" in bot.messages[0][1]
    assert "Ежедневный отчёт" in bot.messages[1][1]
