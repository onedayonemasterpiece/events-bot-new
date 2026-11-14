from __future__ import annotations

import json
import pytest
import main
import vk_intake
from types import SimpleNamespace
from db import Database
from models import Event, EventSource, EventSourceFact
from linked_events import recompute_linked_event_ids


@pytest.mark.parametrize('copy_relation', ['same_programme_other_slot', 'duplicate_same_slot'])
@pytest.mark.asyncio
async def test_source_grounded_address_keeps_course_dates_across_venue_labels(tmp_path, monkeypatch, copy_relation):
    db = Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    calls = []

    async def approve(prompt, schema, **kwargs):
        packet = json.loads(prompt[prompt.index('{"target_ids"'):])
        calls.append(packet)
        assert schema['type'] == 'object'
        from smart_event_update import _gemma_native_response_schema
        assert _gemma_native_response_schema(schema)['properties']['decisions']['type'] == 'ARRAY'
        return {'decisions': [dict(target_id=tid, peer_id=p['id'],
            relation=('same_programme_other_slot' if p['id'] < 10707 else
                copy_relation if p['date'] == f'2099-11-{tid - 10284:02d}' else 'unknown'),
            target_quote='Курс импровизации', peer_quote='Курс импровизации')
            for tid in packet['target_ids'] for p in packet['events']
            if (tid < p['id'] < 10707) or p['id'] == 10707 + tid - 10285]}

    async def drafts(*args, **kwargs):
        return list(range(10285, 10292)), None

    async def persist(eid, photos, db):
        # Normal persist's two observers (Smart Update + parser replay) share one context.
        await recompute_linked_event_ids(db, eid, selected_evidence='' if eid == 10286 else 'Курс импровизации')
        await recompute_linked_event_ids(db, eid)
        return SimpleNamespace(event_id=eid)

    monkeypatch.setattr(vk_intake, 'build_event_drafts', drafts)
    monkeypatch.setattr(vk_intake, 'persist_event_and_pages', persist)

    monkeypatch.setattr('smart_event_update._ask_gemma_json', approve)
    try:
        async with db.get_session() as session:
            for n in range(8):
                for eid in (10285 + n, 10707 + n):
                    session.add(Event(id=eid, title='Импровизация' if n % 2 else 'Курс сценической игры',
                        description='', source_text='', short_description=None if eid == 10286 else 'Курс импровизации', date=f'2099-11-{n + 1:02d}',
                        time='19:00', location_name='Светлый зал' if n % 2 else 'Винный факультет'))
            session.add_all([Event(title='Посторонняя программа', description='', source_text='',
                date='2099-08-01', time='19:00', location_name='Другая площадка')
                for _ in range(801)])
            await session.commit()
            session.add(EventSource(event_id=10285, source_type='telegram', source_url='https://t.me/a/1'))
            source = EventSource(event_id=10286, source_type='parser', source_url='https://example.org/course')
            session.add(source)
            await session.commit()
            await session.refresh(source)
            session.add(EventSourceFact(event_id=10286, source_id=source.id, fact='Курс импровизации'))
            await session.commit()
        await vk_intake.process_event('carrier', db=db)
        async with db.get_session() as session:
            for eid in range(10285, 10292):
                links = (await session.get(Event, eid)).linked_event_ids
                assert all(peer in links for peer in range(10285, 10293) if peer != eid)
                assert not set(range(10707, 10715)).intersection(links)
        assert len(calls) == 1  # Seven children, duplicate observers: one additional semantic request.
        assert all('facts' in p and 'type' in p for p in calls[0]['events'])
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_distinct_children_cleanup_and_unknown_preserves_known_edges(tmp_path, monkeypatch):
    db = Database(str(tmp_path / 'db.sqlite'))
    await db.init()

    async def classify(prompt, schema, **kwargs):
        return {'decisions': [dict(target_id=10990, peer_id=i, relation=relation, target_quote='5-е место', peer_quote=quote)
            for i, relation, quote in [(10991, 'independent_event', '3-е место'),
                (10992, 'independent_event', 'Финал'), (10993, 'unknown', 'Курс'),
                (999999, 'same_programme_other_slot', '5-е место'),
                (10994, 'same_programme_other_slot', 'выдуманная цитата'),
                *[(i, 'independent_event', q) for i, q in enumerate(
                    ['Фотовыставка', 'Детский спектакль', 'Живопись маслом', 'Живопись акварелью'], 10995)]]] +
            [dict(target_id=10997, peer_id=10998, relation='independent_event',
                target_quote='Живопись маслом', peer_quote='Живопись акварелью')]}

    monkeypatch.setattr('smart_event_update._ask_gemma_json', classify)
    try:
        async with db.get_session() as session:
            titles = ['Матч за 5-е место', 'Матч за 3-е место', 'Финал', 'Курс', 'Стендап',
                'Фотовыставка', 'Детский спектакль', 'Живопись маслом', 'Живопись акварелью']
            for eid, title in enumerate(titles, 10990):
                session.add(Event(id=eid, title=title, description='', source_text='composite carrier must not be evidence', short_description=title,
                    date='2099-11-01', time=f'{eid - 10980}:00', location_name='Общий зал',
                    linked_event_ids=[10991, 10993] if eid == 10990 else [10990] if eid in (10991, 10993) else []))
            await session.commit()
        await recompute_linked_event_ids(db, 10990)
        await recompute_linked_event_ids(db, 10997)
        async with db.get_session() as session:
            base = await session.get(Event, 10990)
            assert base.linked_event_ids == [10993]
            game = await session.get(Event, 10991)
            assert game.linked_event_ids == []
            for eid in range(10994, 10999):
                assert (await session.get(Event, eid)).linked_event_ids == []
        # Sports never collapse after adjudication; the independent rows remain available.
        group = await main._same_day_linked_publish_group(db, game)
        assert [e.id for e in group] == [10991]
    finally:
        await db.close()
