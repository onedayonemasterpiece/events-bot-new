import os
import sys

import pytest

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from db import Database
from reaction_counter_sync import (
    SourceReactionCounter,
    aggregate_source_reaction_counters,
    build_source_counter_payload,
)


async def _insert_event(conn, event_id: int, title: str = "Event") -> None:
    await conn.execute(
        """
        INSERT INTO event(
            id, title, description, date, time, location_name, source_text
        ) VALUES(?,?,?,?,?,?,?)
        """,
        (event_id, title, "Описание", "2099-01-01", "18:00", "Калининград", "source"),
    )


@pytest.mark.asyncio
async def test_source_reaction_counters_sum_raw_max_without_coefficients(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        async with db.raw_conn() as conn:
            await _insert_event(conn, 101, "One")
            await _insert_event(conn, 102, "Two")
            await conn.execute(
                "INSERT INTO telegram_source(username, title) VALUES(?,?)",
                ("reaction_counter_theatre", "Theatre"),
            )
            cur = await conn.execute("SELECT id FROM telegram_source WHERE username=?", ("reaction_counter_theatre",))
            tg_source_id = int((await cur.fetchone())[0])
            await conn.execute(
                "INSERT INTO event_source(event_id, source_type, source_url) VALUES(?,?,?)",
                (101, "telegram", "https://t.me/theatre/10"),
            )
            await conn.execute(
                "INSERT INTO event_source(event_id, source_type, source_url) VALUES(?,?,?)",
                (101, "vk", "https://vk.com/wall-123_55"),
            )
            await conn.execute(
                "INSERT INTO event_source(event_id, source_type, source_url) VALUES(?,?,?)",
                (102, "telegram", "https://t.me/empty/1"),
            )
            # Same TG post in two age buckets: the aggregate must use max(5, 9), not sum.
            await conn.execute(
                """
                INSERT INTO telegram_post_metric(source_id, message_id, age_day, source_url, collected_ts, views, likes)
                VALUES(?,?,?,?,?,?,?)
                """,
                (tg_source_id, 10, 0, "https://t.me/theatre/10", 1000, 100, 5),
            )
            await conn.execute(
                """
                INSERT INTO telegram_post_metric(source_id, message_id, age_day, source_url, collected_ts, views, likes)
                VALUES(?,?,?,?,?,?,?)
                """,
                (tg_source_id, 10, 1, "https://t.me/theatre/10", 1100, 150, 9),
            )
            await conn.execute(
                """
                INSERT INTO vk_post_metric(group_id, post_id, age_day, source_url, collected_ts, views, likes)
                VALUES(?,?,?,?,?,?,?)
                """,
                (123, 55, 0, "https://vk.com/wall-123_55", 1000, 200, 8),
            )
            await conn.commit()

        counters = await aggregate_source_reaction_counters(db, event_ids=[101, 102])
        by_event = {c.event_id: c for c in counters}

        assert by_event[101].source_likes_count == 17
        assert by_event[101].source_views_count == 350
        assert by_event[101].source_engagement_sources_count == 2
        assert by_event[102].source_likes_count == 0
        assert by_event[102].source_views_count == 0
        assert by_event[102].source_engagement_sources_count == 0
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_source_reaction_counters_deduplicate_vk_exact_and_inbox_mapping(tmp_path):
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    try:
        async with db.raw_conn() as conn:
            await _insert_event(conn, 201, "VK")
            await conn.execute(
                "INSERT INTO event_source(event_id, source_type, source_url) VALUES(?,?,?)",
                (201, "vk", "https://vk.com/wall-321_77"),
            )
            await conn.execute(
                """
                INSERT INTO vk_inbox(group_id, post_id, date, text, has_date, status)
                VALUES(?,?,?,?,?,?)
                """,
                (321, 77, 1000, "event", 1, "imported"),
            )
            cur = await conn.execute("SELECT id FROM vk_inbox WHERE group_id=? AND post_id=?", (321, 77))
            inbox_id = (await cur.fetchone())[0]
            await conn.execute(
                "INSERT INTO vk_inbox_import_event(inbox_id, event_id) VALUES(?,?)",
                (inbox_id, 201),
            )
            await conn.execute(
                """
                INSERT INTO vk_post_metric(group_id, post_id, age_day, source_url, collected_ts, views, likes)
                VALUES(?,?,?,?,?,?,?)
                """,
                (321, 77, 0, "https://vk.com/wall-321_77", 1000, 100, 4),
            )
            await conn.commit()

        counters = await aggregate_source_reaction_counters(db, event_ids=[201])
        assert len(counters) == 1
        assert counters[0].source_likes_count == 4
        assert counters[0].source_views_count == 100
        assert counters[0].source_engagement_sources_count == 1
    finally:
        await db.close()


def test_source_counter_payload_does_not_touch_service_fields():
    payload = build_source_counter_payload(
        counters=[SourceReactionCounter(event_id=1, source_likes_count=2, source_views_count=3, source_engagement_sources_count=1)],
        refreshed_at="2026-06-27T00:00:00Z",
    )
    assert payload == [
        {
            "event_id": 1,
            "source_likes_count": 2,
            "source_views_count": 3,
            "source_engagement_sources_count": 1,
            "source_refreshed_at": "2026-06-27T00:00:00Z",
            "updated_at": "2026-06-27T00:00:00Z",
        }
    ]
    assert "service_likes_count" not in payload[0]
    assert "likes_count" not in payload[0]
