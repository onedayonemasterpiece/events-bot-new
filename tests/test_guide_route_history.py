import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import json
from unittest.mock import AsyncMock

import aiosqlite
import pytest

from guide_excursions import route_history as rh
from guide_excursions.route_readers import RouteReadLimited

NOW = datetime(2026, 11, 17, 9, tzinfo=timezone.utc)
TITLE = "По следам мастеров"
STABLE = "На маршруте увидим фабричные ворота и узнаем, как городские мастера передавали ремесло ученикам."
PRIMARY = "22 ноября — «По следам мастеров». Гид Ольга Рубцова."
OWN = f"Приглашаю на экскурсию «{TITLE}». Гид Ольга Рубцова. {STABLE}\nЦена 1200 рублей. Запись по телефону."
SOURCE = dict(platform="telegram", username="masterguide", source_kind="guide_personal", profile_name="Ольга Рубцова")


@pytest.fixture(autouse=True)
def offline_catalog(monkeypatch):
    monkeypatch.setattr(rh, "refresh_tic_catalog", AsyncMock(return_value={"confirmed": 0}))


class DB:
    def __init__(self, path):
        self.path = path

    @asynccontextmanager
    async def raw_conn(self):
        async with aiosqlite.connect(self.path) as conn:
            conn.row_factory = aiosqlite.Row
            yield conn


async def make_db(path):
    db = DB(path)
    async with db.raw_conn() as conn:
        await conn.executescript("""
        CREATE TABLE guide_profile(id,display_name,marketing_name);
        INSERT INTO guide_profile VALUES (8,'Ольга Рубцова','Ольга Рубцова');
        CREATE TABLE guide_source(id PRIMARY KEY,platform,username,source_kind,enabled,primary_profile_id,flags_json);
        INSERT INTO guide_source VALUES (4,'telegram','masterguide','guide_personal',1,8,'{}');
        CREATE TABLE guide_occurrence(id PRIMARY KEY,canonical_title,title_normalized,date,primary_source_id,
             guide_names_json,organizer_names_json,status,digest_eligible);
        CREATE TABLE guide_monitor_post(id PRIMARY KEY,source_id,text,post_date,source_url,post_kind);
        CREATE TABLE guide_occurrence_source(occurrence_id,post_id);
        """)
        await conn.execute("INSERT INTO guide_occurrence VALUES (3,?,?,?,4,?,'[]','available',1)",
                           (TITLE,rh.normalize_title_key(TITLE),"2026-11-22",json.dumps(["Ольга Рубцова"])))
        await conn.execute("INSERT INTO guide_monitor_post VALUES (3,4,?,?,?,'announce_single')",
                           (PRIMARY,"2026-11-15","https://t.me/masterguide/3"))
        await conn.execute("INSERT INTO guide_occurrence_source VALUES (3,3)")
        await conn.commit()
    return db


async def saved(db):
    async with db.raw_conn() as con:
        rows = [dict(r) for r in await con.execute_fetchall("SELECT * FROM guide_occurrence WHERE id=3")]
        return await rh.load_cached_background(con,rows)


@pytest.mark.asyncio
async def test_annual_distinct_dates_and_single_edition_barriers(tmp_path):
    db = await make_db(tmp_path / "e.sqlite")
    async with db.raw_conn() as con:
        for i,day,guide,text in [
            (1,"2025-01-12","Ольга Рубцова",OWN), (2,"2025-01-12","Ольга Рубцова",OWN),
            (4,"2025-09-04","Ирина Смирнова","Другой гид рассказывает о ночном рынке."),
            (5,"2026-08-12","Ольга Рубцова","Только один раз. Расскажем о гостевой выставке.")]:
            await con.execute("INSERT INTO guide_occurrence VALUES (?,?,?,?,4,?,'[]','available',1)",
                              (i,TITLE,rh.normalize_title_key(TITLE),day,json.dumps([guide])))
            await con.execute("INSERT INTO guide_monitor_post VALUES (?,4,?,?,?,'announce_single')",
                              (i,text,day,f"https://t.me/masterguide/{i}"))
            await con.execute("INSERT INTO guide_occurrence_source VALUES (?,?)",(i,i))
        row = (await con.execute_fetchall("SELECT * FROM guide_occurrence WHERE id=3"))[0]
        hist = (await rh.load_route_history(con,[row],{3:[{"text":PRIMARY,"post_date":"2026-11-15"}]}))[3]
        assert hist["kind"] == "repeated_dates" and hist["dated_departures"] == 2
        assert len(hist["earlier"]) == 1 and hist["earlier"][0]["text"] == STABLE
        assert hist["earlier"][0]["url"] in {"https://t.me/masterguide/1","https://t.me/masterguide/2"}
        one = {"text":"Только один раз. " + PRIMARY,"post_date":"2026-11-15"}
        assert (await rh.load_route_history(con,[row],{3:[one]}))[3]["kind"] == "one_off"
        assert not (await rh.load_route_history(con,[row],{3:[one]}))[3]["earlier"]


@pytest.mark.asyncio
async def test_local_post_unlinked_before_network(tmp_path):
    db = await make_db(tmp_path / "e.sqlite")
    async with db.raw_conn() as con:
        await con.execute("INSERT INTO guide_monitor_post VALUES (11,4,?,?,?,'announce_single')",
                          (OWN,"2025-01-12","https://t.me/masterguide/11"))
        await con.commit()
    async def fail_search(*args):
        pytest.fail("A locally confirmed description covers this route")
    report = await rh.refresh_route_history(db,search=fail_search,now=NOW)
    assert report["searched"] == 0 and report["local"] == 1
    info = (await saved(db))[3]
    assert info["text"] == STABLE and info["url"] == "https://t.me/masterguide/11"
    async with db.raw_conn() as con:
        await con.execute("UPDATE guide_occurrence SET guide_names_json='[\"Другой Человек\"]' WHERE id=3")
        await con.commit()
    assert await saved(db) == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome",["hit","empty","flood"])
async def test_durable_query_and_provider_cooldown(tmp_path, outcome):
    db = await make_db(tmp_path / "e.sqlite")
    calls = []
    async def search(source,title):
        calls.append((source["username"],title))
        if outcome == "flood":
            raise RouteReadLimited(3 * 86400)
        if outcome == "empty":
            return []
        return [{"url":"https://t.me/masterguide/27","post_date":"2025-03-05","text":OWN}]
    await asyncio.gather(*(rh.refresh_route_history(db,search=search,now=NOW) for _ in range(3)))
    assert calls == [("masterguide",TITLE)]
    await rh.refresh_route_history(db,search=search,now=NOW+timedelta(hours=12))
    assert len(calls) == 1
    async with db.raw_conn() as con:
        lease = (await con.execute_fetchall("SELECT next_at FROM guide_route_lookup WHERE lookup_key='provider:telegram'"))[0][0]
        assert lease == NOW.timestamp() + (3 * 86400 if outcome == "flood" else rh._PROVIDER_GAP)
    if outcome == "hit":
        assert (await saved(db))[3]["text"] == STABLE


def test_another_program_or_guest_is_not_evidence():
    row = dict(canonical_title=TITLE,guide_names_json='["Ольга Рубцова"]')
    post = dict(url="https://t.me/masterguide/19",post_date="2026-11-05")
    for bad in [
        f"Приглашаю на экскурсию «{TITLE}». Гид Ирина Смирнова. {STABLE}",
        f"Расписание.\n22 ноября — «{TITLE}».\n23 ноября — «Другая экскурсия». Гид Ольга Рубцова. {STABLE}",
    ]:
        assert not rh._first_party(row,SOURCE,dict(post,text=bad),provider_verified=True)
    assert not rh._first_party(row,SOURCE,dict(post,url="https://t.me/other/19",text=OWN),provider_verified=True)


@pytest.mark.asyncio
async def test_warm_or_cold_digest_uses_indexed_cache_without_earlier_queries(tmp_path):
    db = await make_db(tmp_path / "e.sqlite")
    async with db.raw_conn() as con:
        rows = [dict(r) for r in await con.execute_fetchall("SELECT * FROM guide_occurrence")]
        cold = await rh.load_materialized_history(con,rows,{3:[]})
        assert cold[3]["kind"] == "one_observed_date"
    await rh.refresh_route_history(db,external=False,now=NOW)
    async with db.raw_conn() as con:
        queries=[]
        await con.set_trace_callback(queries.append)
        warm = await rh.load_materialized_history(con,rows, {})
        await rh.load_cached_background(con,rows,now=NOW)
        assert warm[3]["dated_departures"] == 1
        assert all("FROM guide_occurrence" not in q and "JOIN guide_monitor_post" not in q for q in queries)
        plan = await con.execute_fetchall("EXPLAIN QUERY PLAN SELECT history_json FROM guide_route_material WHERE occurrence_id IN (?)",(3,))
        assert "INTEGER PRIMARY KEY" in plan[0][3]
        await con.execute("UPDATE guide_route_material SET history_json=? WHERE occurrence_id=3",
                          (json.dumps(dict(warm[3],kind="repeated_dates",dated_departures=5)),))
        edited = dict(rows[0],date="2026-12-11")
        assert (await rh.load_materialized_history(con,[edited],{}))[3]["dated_departures"] == 1


@pytest.mark.asyncio
async def test_stale_check_temporarily_fails_but_valid_evidence_still_available(tmp_path):
    db = await make_db(tmp_path / "e.sqlite")
    async def hit(source,title):
        return [{"url":"https://t.me/masterguide/76","post_date":"2025-03-05","text":OWN}]
    await rh.refresh_route_history(db,search=hit,now=NOW)
    later=NOW+timedelta(days=100)
    async with db.raw_conn() as con:
        rows=[dict(r) for r in await con.execute_fetchall("SELECT * FROM guide_occurrence WHERE id=3")]
        key=rh._lookup_key(rows[0])
        assert not await rh.load_cached_background(con,rows,now=later,fresh_only=True)
        assert (await rh.load_cached_background(con,rows,now=later))[3]["url"].endswith("/76")
    assert await rh._claim(db,key,"telegram",later.timestamp())
    await rh._finish(db,key,"telegram",later.timestamp(),86400)
    async with db.raw_conn() as con:
        assert (await rh.load_cached_background(con,rows,now=later))[3]["text"] == STABLE
        assert not await rh.load_cached_background(con,rows,now=NOW+timedelta(days=366))
        await con.execute("INSERT INTO guide_route_lookup VALUES ('route:expired','vk',0,0,'{}')")
        await con.commit()
    await rh.refresh_route_history(db,external=False,now=later)
    async with db.raw_conn() as con:
        assert not await con.execute_fetchall("SELECT 1 FROM guide_route_lookup WHERE lookup_key='route:expired'")


def test_destination_only_needs_a_named_route_and_one_off_needs_its_date():
    city=dict(id=3,canonical_title="Светлогорск",title_normalized="светлогорск")
    assert rh._current_state(city,{3:[dict(text="Приглашаю в Светлогорск.",post_date="2026-11-01")]})["ambiguous_title"]
    row=dict(canonical_title=TITLE,date="2026-11-22",guide_names_json='["Ольга Рубцова"]',route_history={"kind":"one_off"})
    old=dict(url="https://t.me/masterguide/5",post_date="2026-08-01",text=OWN+"\nВыезд 15 августа.")
    assert not rh._first_party(row,SOURCE,old,provider_verified=True)
