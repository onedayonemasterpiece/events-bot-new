from datetime import datetime, timezone
from dataclasses import replace
import importlib.util
from pathlib import Path
import sqlite3
import sys

import pytest
from db import Database
from models import Event
from static_site_release import event_public_revision

spec = importlib.util.spec_from_file_location('tested_hero_resolver', Path(__file__).parents[1] / 'hero_talk/resolver.py')
r = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = r
spec.loader.exec_module(r)
NOW = datetime(2026, 9, 6, 10, tzinfo=timezone.utc)

@pytest.fixture
async def canonical(tmp_path, monkeypatch):
    monkeypatch.setenv('DB_INIT_SKIP_VK_SOURCES_SEED', '1')
    db = Database(str(tmp_path / 'event.sqlite'))
    await db.init()
    event = Event(title='Event', description='Description', date='2026-09-06', time='19:00', location_name='Hall', source_text='Source')
    async with db.get_session() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
        revision = event_public_revision(event)
    await db.close()
    return tmp_path / 'event.sqlite', event.id, revision


def update(canonical, **values):
    path, event_id, _ = canonical
    with sqlite3.connect(path) as con:
        con.execute('UPDATE event SET ' + ','.join(f'{key}=?' for key in values) + ' WHERE id=?', (*values.values(), event_id))


def resolve(canonical, **kwargs):
    return r.resolve_event_packet(canonical[0], canonical[1], now=NOW, **kwargs)

@pytest.mark.asyncio
async def test_revision_and_unresolved_evidence(canonical):
    packet = resolve(canonical)
    dep = packet['dependencies'][f'event:{canonical[1]}']
    assert dep['revision'] == canonical[2]
    assert dep['eligible_until'] == '2026-09-06T17:00:00+00:00'
    assert packet['links'] == packet['media'] == {}

@pytest.mark.asyncio
@pytest.mark.parametrize('fields,deadline', [
    ({'time': ''}, '2026-09-06T22:00:00+00:00'),
    ({'date': '2026-09-08', 'time': ''}, '2026-09-08T22:00:00+00:00'),
    ({'end_date': '2026-09-08'}, '2026-09-08T22:00:00+00:00'),
    ({'date': '2026-09-06..2026-09-08'}, '2026-09-08T22:00:00+00:00'),
    ({'end_date': '2026-09-08', 'end_date_is_inferred': 1}, '2026-09-06T17:00:00+00:00'),
])
async def test_deadlines(canonical, fields, deadline):
    update(canonical, **fields)
    assert next(iter(resolve(canonical)['dependencies'].values()))['eligible_until'] == deadline

@pytest.mark.asyncio
@pytest.mark.parametrize('fields', [
    {'lifecycle_status': 'cancelled'}, {'lifecycle_status': 'postponed'}, {'silent': 1},
    {'identity_status': 'merged'}, {'merged_into_event_id': 999},
    {'date': 'uncertain'}, {'end_date': 'uncertain'}, {'date': '2026-09-06..bad'},
    {'time': '09:00'}, {'date': '2026-09-05', 'time': ''},
])
async def test_ineligible(canonical, fields):
    update(canonical, **fields)
    with pytest.raises(r.EventResolutionError):
        resolve(canonical)

@pytest.mark.asyncio
async def test_route_exact_revision_identity_and_expiry(canonical):
    evidence = r.RouteReadinessEvidence(canonical[1], canonical[2], 'event-1', '/sobytiya/event-1/', NOW, datetime(2026,9,6,11,tzinfo=timezone.utc))
    packet = resolve(canonical, route_evidence=evidence)
    assert packet['links']
    assert next(iter(packet['dependencies'].values()))['eligible_until'] == '2026-09-06T11:00:00+00:00'
    for bad in (replace(evidence, event_revision='old'), replace(evidence, event_id=999),
                replace(evidence, expires_at=NOW), replace(evidence, href='/other/'),
                replace(evidence, slug='../private')):
        assert resolve(canonical, route_evidence=bad)['links'] == {}
    update(canonical, title='Changed')
    assert resolve(canonical, route_evidence=evidence)['links'] == {}

@pytest.mark.asyncio
async def test_read_only_and_timezone_independent(canonical, monkeypatch):
    import main
    monkeypatch.setattr(main, 'LOCAL_TZ', timezone.utc)
    before = canonical[0].read_bytes()
    packet = resolve(canonical)
    assert next(iter(packet['dependencies'].values()))['eligible_until'].startswith('2026-09-06T17:00:00')
    assert main.LOCAL_TZ is timezone.utc
    assert canonical[0].read_bytes() == before
    with pytest.raises(r.EventResolutionError, match='event_expired'):
        r.resolve_event_packet(canonical[0], canonical[1], now=datetime(2026,9,6,17,tzinfo=timezone.utc))

@pytest.mark.asyncio
async def test_missing_event_and_naive_clock(canonical):
    with pytest.raises(r.EventResolutionError, match='event_not_found'):
        r.resolve_event_packet(canonical[0], 999, now=NOW)
    with pytest.raises(r.EventResolutionError, match='timezone_required'):
        r.resolve_event_packet(canonical[0], canonical[1], now=NOW.replace(tzinfo=None))
