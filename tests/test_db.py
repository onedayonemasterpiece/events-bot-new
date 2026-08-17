import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import json

import pytest
from sqlalchemy import text

from main import Database
from models import Event


@pytest.mark.asyncio
async def test_raw_conn_uses_isolated_file_connections_for_concurrent_transactions(
    tmp_path,
):
    """Concurrent raw transactions must not share SQLite transaction ownership."""

    db_path = tmp_path / "raw-isolation.sqlite"
    db = Database(str(db_path))
    await db.init()

    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    second_entered = asyncio.Event()
    connection_ids: list[int] = []

    async def first_writer() -> None:
        async with db.raw_conn() as conn:
            connection_ids.append(id(conn))
            await conn.execute("BEGIN IMMEDIATE")
            await conn.execute(
                "INSERT INTO setting(key, value) VALUES('raw-owner-1', 'ok')"
            )
            first_entered.set()
            await release_first.wait()
            await conn.commit()

    async def second_writer() -> None:
        await first_entered.wait()
        async with db.raw_conn() as conn:
            connection_ids.append(id(conn))
            second_entered.set()
            await conn.execute("BEGIN IMMEDIATE")
            await conn.execute(
                "INSERT INTO setting(key, value) VALUES('raw-owner-2', 'ok')"
            )
            await conn.commit()

    first_task = asyncio.create_task(first_writer())
    second_task = asyncio.create_task(second_writer())
    await asyncio.wait_for(first_entered.wait(), timeout=1)
    await asyncio.wait_for(second_entered.wait(), timeout=1)
    await asyncio.sleep(0.05)
    assert not second_task.done()

    release_first.set()
    await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=2)

    assert len(connection_ids) == 2
    assert connection_ids[0] != connection_ids[1]
    async with db.raw_conn() as conn:
        row = await (
            await conn.execute(
                "SELECT COUNT(*) FROM setting WHERE key LIKE 'raw-owner-%'"
            )
        ).fetchone()
    assert row == (2,)
    await db.close()


@pytest.mark.asyncio
async def test_journal_mode_wal(tmp_path):
    db_path = tmp_path / "test.sqlite"
    db = Database(str(db_path))
    await db.init()
    async with db.engine.connect() as conn:
        result = await conn.execute(text("PRAGMA journal_mode"))
        mode = result.scalar()
    await db.engine.dispose()
    assert mode.lower() == "wal"


@pytest.mark.asyncio
async def test_festival_has_source_text(tmp_path):
    db_path = tmp_path / "test.sqlite"
    db = Database(str(db_path))
    await db.init()
    async with db.engine.connect() as conn:
        result = await conn.execute(text("PRAGMA table_info(festival)"))
        cols = [r[1] for r in result.fetchall()]
    await db.engine.dispose()
    assert "source_text" in cols


@pytest.mark.asyncio
async def test_page_section_cache_exists(tmp_path):
    db_path = tmp_path / "test.sqlite"
    db = Database(str(db_path))
    await db.init()
    async with db.engine.connect() as conn:
        result = await conn.execute(text("PRAGMA table_info(page_section_cache)"))
        cols = [r[1] for r in result.fetchall()]
    await db.engine.dispose()
    assert {"page_key", "section_key", "hash", "updated_at"} <= set(cols)


@pytest.mark.asyncio
async def test_event_topics_columns(tmp_path):
    db_path = tmp_path / "test.sqlite"
    db = Database(str(db_path))
    await db.init()

    async with db.engine.connect() as conn:
        result = await conn.execute(text("PRAGMA table_info(event)"))
        cols = result.fetchall()

    await db.engine.dispose()

    col_map = {row[1]: row for row in cols}
    assert "topics" in col_map
    assert "topics_manual" in col_map
    assert col_map["topics"][2].upper() == "TEXT"
    assert col_map["topics"][4] == "'[]'"
    assert col_map["topics_manual"][2].upper() == "BOOLEAN"
    assert col_map["topics_manual"][4] in ("0", 0)


@pytest.mark.asyncio
async def test_event_topics_roundtrip(tmp_path):
    db_path = tmp_path / "test.sqlite"
    db = Database(str(db_path))
    await db.init()

    async with db.get_session() as session:
        ev = Event(
            title="T",
            description="D",
            festival=None,
            date="2025-01-02",
            time="10:00",
            location_name="Loc",
            source_text="Src",
            topics=["ART"],
            topics_manual=True,
        )
        session.add(ev)
        await session.commit()
        event_id = ev.id

    async with db.raw_conn() as conn:
        cursor = await conn.execute(
            "SELECT topics, topics_manual FROM event WHERE id=?", (event_id,)
        )
        row = await cursor.fetchone()
    assert json.loads(row[0]) == ["ART"]
    assert row[1] in (1, True)

    async with db.get_session() as session:
        stored = await session.get(Event, event_id)
        assert stored is not None
        assert stored.topics == ["ART"]
        assert stored.topics_manual is True


@pytest.mark.asyncio
async def test_event_source_backfill_excludes_and_purges_managed_vk_projection(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("VK_AFISHA_GROUP_ID", "231920894")
    db_path = tmp_path / "test.sqlite"
    db = Database(str(db_path))
    await db.init()

    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO event(
                title, description, date, time, location_name, source_text,
                source_post_url, source_vk_post_url
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Managed projection",
                "Projection text",
                "2026-08-08",
                "14:00",
                "Test venue",
                "Projection text",
                "https://vk.com/wall-231920894_7008",
                "https://vk.com/wall-231920894_7008",
            ),
        )
        managed_event_id = (await (await conn.execute("SELECT last_insert_rowid()")).fetchone())[0]
        await conn.execute(
            """
            INSERT INTO event_source(event_id, source_type, source_url)
            VALUES(?, 'vk', ?)
            """,
            (managed_event_id, "https://vk.com/wall-231920894_7008"),
        )
        managed_source_id = (await (await conn.execute("SELECT last_insert_rowid()")).fetchone())[0]
        await conn.execute(
            """
            INSERT INTO event_source_fact(event_id, source_id, fact, status)
            VALUES(?, ?, 'Published AI projection', 'added')
            """,
            (managed_event_id, managed_source_id),
        )
        await conn.execute(
            """
            INSERT INTO event(
                title, description, date, time, location_name, source_text,
                source_vk_post_url
            )
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "External source",
                "Organizer text",
                "2026-08-09",
                "15:00",
                "Test venue",
                "Organizer text",
                "https://vk.com/wall-24180215_123",
            ),
        )
        external_event_id = (await (await conn.execute("SELECT last_insert_rowid()")).fetchone())[0]
        await conn.commit()

    await db.engine.dispose()
    restarted = Database(str(db_path))
    await restarted.init()
    async with restarted.raw_conn() as conn:
        managed_sources = await (
            await conn.execute(
                "SELECT id FROM event_source WHERE event_id=?", (managed_event_id,)
            )
        ).fetchall()
        managed_facts = await (
            await conn.execute(
                "SELECT id FROM event_source_fact WHERE event_id=?", (managed_event_id,)
            )
        ).fetchall()
        external_sources = await (
            await conn.execute(
                "SELECT source_url FROM event_source WHERE event_id=?", (external_event_id,)
            )
        ).fetchall()

    await restarted.engine.dispose()
    assert managed_sources == []
    assert managed_facts == []
    assert [row[0] for row in external_sources] == ["https://vk.com/wall-24180215_123"]


@pytest.mark.asyncio
async def test_db_init_upgrades_interest_club_evaluation_history_constraint(tmp_path):
    db_path = tmp_path / "club-history.sqlite"
    db = Database(str(db_path))
    await db.init()
    async with db.raw_conn() as conn:
        await conn.execute(
            "INSERT INTO interest_club(slug,canonical_name,topic,public_status) "
            "VALUES('club','Club','topic','approved')"
        )
        await conn.execute(
            "INSERT INTO event(title,description,date,time,location_name,source_text) "
            "VALUES('Event','Description','2026-08-10','18:00','Hall','Source')"
        )
        await conn.execute("DROP TABLE interest_club_evaluation")
        await conn.execute(
            """
            CREATE TABLE interest_club_evaluation(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                club_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                verdict TEXT NOT NULL,
                decision_lane TEXT NOT NULL,
                evidence_quote TEXT,
                evidence_json JSON NOT NULL DEFAULT '{}',
                model TEXT,
                policy_version TEXT NOT NULL DEFAULT 'interest-club-relation-v1',
                input_hash TEXT NOT NULL,
                error_code TEXT,
                attempts INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(club_id,event_id)
            )
            """
        )
        await conn.execute(
            "INSERT INTO interest_club_evaluation("
            "club_id,event_id,status,verdict,decision_lane,input_hash"
            ") VALUES(1,1,'accepted','yes','source','hash-one')"
        )
        await conn.commit()
    await db.close()

    restarted = Database(str(db_path))
    await restarted.init()
    async with restarted.raw_conn() as conn:
        rows = await (
            await conn.execute(
                "SELECT input_hash FROM interest_club_evaluation ORDER BY id"
            )
        ).fetchall()
        await conn.execute(
            "INSERT INTO interest_club_evaluation("
            "club_id,event_id,status,verdict,decision_lane,input_hash"
            ") VALUES(1,1,'review','unclear','source','hash-two')"
        )
        await conn.commit()
        count = int(
            (
                await (
                    await conn.execute(
                        "SELECT COUNT(*) FROM interest_club_evaluation"
                    )
                ).fetchone()
            )[0]
        )
    await restarted.close()

    assert rows == [("hash-one",)]
    assert count == 2
