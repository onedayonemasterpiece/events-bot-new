import ast
import asyncio
import importlib.util
import logging
from contextlib import asynccontextmanager
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace

import aiosqlite
import pytest
from operational_retention import cleanup_terminal_heartbeats


@pytest.mark.asyncio
async def test_daily_tick_failure_recovers_and_cancellation_propagates(caplog):
    tree = ast.parse((Path(__file__).parents[1] / 'main_part2.py').read_text())
    function = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'daily_scheduler')
    attempts = 0
    async def get_offset(db):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError('database or disk is full')
        return '+02:00'
    class DB:
        @asynccontextmanager
        async def raw_conn(self):
            async def rows(*args): return []
            yield SimpleNamespace(execute_fetchall=rows)
    sleeps = 0
    async def sleep(seconds):
        nonlocal sleeps
        sleeps += 1
        if sleeps == 2: raise asyncio.CancelledError
    namespace = dict(Database=object, Bot=object, timezone=timezone, get_tz_offset=get_offset,
                     offset_to_timezone=lambda _: timezone.utc, seconds_to_next_minute=lambda _: 60,
                     asyncio=asyncio)
    exec(compile(ast.Module(body=[function], type_ignores=[]), '<daily_scheduler>', 'exec'), namespace)
    original = asyncio.sleep
    asyncio.sleep = sleep
    try:
        with caplog.at_level(logging.ERROR), pytest.raises(asyncio.CancelledError):
            await namespace['daily_scheduler'](DB(), None)
    finally:
        asyncio.sleep = original
    assert attempts == 2
    assert 'retrying next minute' in caplog.text


@pytest.mark.asyncio
async def test_retention_preserves_failed_unknown_active_and_terminal_receipts(tmp_path):
    path = tmp_path / 'db.sqlite'
    async with aiosqlite.connect(path) as c:
        await c.executescript('''
        CREATE TABLE kaggle_run_ledger(run_id TEXT, status TEXT, terminal_at TEXT);
        CREATE TABLE kaggle_run_event(id INTEGER PRIMARY KEY, run_id TEXT, event_name TEXT, created_at TEXT);
        INSERT INTO kaggle_run_ledger VALUES('ok','done','2020-01-01'),('fail','failed','2020-01-01'),('unknown','unknown',NULL),('active','running',NULL),('recent','done',datetime('now'));
        INSERT INTO kaggle_run_event VALUES(1,'ok','alive','2020-01-01'),(2,'ok','report_written','2020-01-01'),(3,'fail','alive','2020-01-01'),(4,'unknown','alive','2020-01-01'),(5,'active','alive','2020-01-01'),(6,'recent','alive','2020-01-01');
        ''')
        await c.commit()
    class DB:
        @asynccontextmanager
        async def raw_conn(self):
            async with aiosqlite.connect(path) as c: yield c
    assert await cleanup_terminal_heartbeats(DB()) == 1
    assert await cleanup_terminal_heartbeats(DB()) == 0
    async with aiosqlite.connect(path) as c:
        assert [r[0] for r in await c.execute_fetchall('SELECT id FROM kaggle_run_event ORDER BY id')] == [2,3,4,5,6]


@pytest.mark.asyncio
async def test_slow_weather_collection_does_not_delay_publication_or_leak_tasks():
    path = Path(__file__).parents[1] / 'deploy/runcoveer/weather_scheduler.py'
    spec = importlib.util.spec_from_file_location('weather_scheduler_test', path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    blocked = asyncio.Event(); published = asyncio.Event(); stopped = asyncio.Event()
    class Bot:
        running = True
        async def collect_weather(self):
            blocked.set()
            try: await asyncio.Event().wait()
            finally: stopped.set()
        async def collect_sea(self): raise AssertionError('unreachable')
        async def process_due(self): pass
        async def process_weather_channels(self):
            await blocked.wait()
            published.set(); self.running = False
        async def process_rubric_schedule(self): pass
    await asyncio.wait_for(module.schedule_loop(Bot(), interval=0.001), timeout=1)
    assert published.is_set() and stopped.is_set()


def test_video_pattern_excludes_production_inherited_frames():
    import re
    from video_announce.poller import VIDEO_OUTPUT_FILE_PATTERN
    assert not re.search(VIDEO_OUTPUT_FILE_PATTERN, 'artifacts/codex/mobilefeed_intro_scene1_final/intro_frames_raw/frame_0103.png')
    for name in ('cherryflash_full_final.mp4', 'crumple_video_final.mp4', 'story_publish_report.json', 'kernel.log'):
        assert re.search(VIDEO_OUTPUT_FILE_PATTERN, name)
