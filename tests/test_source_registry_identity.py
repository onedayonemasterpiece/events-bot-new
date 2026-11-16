import json
import sqlite3

import pytest

from db import Database


@pytest.mark.asyncio
async def test_event_source_rejects_signed_duplicate_but_keeps_other_wall(tmp_path):
    db = Database(str(tmp_path / 'events.sqlite'))
    await db.init()
    try:
        async with db.raw_conn() as conn:
            await conn.execute("INSERT INTO vk_source(group_id, owner_type) VALUES(900001, 'group')")
            with pytest.raises(sqlite3.IntegrityError):
                await conn.execute("INSERT INTO vk_source(group_id, owner_type) VALUES(-900001, 'group')")
            await conn.execute("INSERT INTO vk_source(group_id, owner_type) VALUES(900002, 'group')")
            assert (await (await conn.execute('SELECT COUNT(*) FROM vk_source')).fetchone())[0] == 2
    finally:
        await db.engine.dispose()


@pytest.mark.asyncio
async def test_guide_source_rejects_same_owner_alias_but_keeps_distinct_owner_type(tmp_path):
    db = Database(str(tmp_path / 'guide.sqlite'))
    await db.init()
    try:
        async with db.raw_conn() as conn:
            async def add(username, personal, owner=900003):
                await conn.execute(
                    "INSERT INTO guide_source(platform, username, source_kind, flags_json) VALUES('vk', ?, 'guide_project', ?)",
                    (username, json.dumps({'vk_id': owner, 'vk_personal_page': personal})),
                )
            await add('original_handle', False)
            with pytest.raises(sqlite3.IntegrityError):
                await add('club900003', False)
            await add('id900003', True)
            await add('different_group', False, owner=900004)
            assert (await (await conn.execute("SELECT COUNT(*) FROM guide_source WHERE username IN ('original_handle', 'id900003', 'different_group')")).fetchone())[0] == 3
    finally:
        await db.engine.dispose()
