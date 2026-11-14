from __future__ import annotations

import json
import pytest
import main
import vk_intake
from types import SimpleNamespace
from datetime import date, timedelta
from unittest.mock import AsyncMock
from db import Database
from models import Event, EventSource, EventSourceFact
from linked_events import recompute_linked_event_ids


@pytest.mark.parametrize('size,transport', [(7, 'vk'), (7, 'tg'), (32, 'vk'), (33, 'vk')])
@pytest.mark.asyncio
async def test_source_grounded_address_keeps_course_dates_across_venue_labels(tmp_path, monkeypatch, size, transport):
    db = Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    calls = []
    start = date.today() + timedelta(days=20)
    ids = list(range(10285, 10285 + size))

    async def approve(prompt, schema, **kwargs):
        packet = json.loads(prompt[prompt.index('{"target_ids"'):])
        calls.append(packet)
        assert schema['type'] == 'object'
        from smart_event_update import _gemma_native_response_schema
        assert _gemma_native_response_schema(schema)['properties']['groups']['type'] == 'ARRAY'
        assert {p['address'] for p in packet['events']} <= {'', 'Гаражная 2'}
        assert any(p['address'] == 'Гаражная 2' for p in packet['events'])
        reply = {'groups': [{'members': [dict(event_id=p['id'], quote='Курс импровизации')
            for p in packet['events'] if p['id'] < 10707]}], 'decisions': []}
        assert len(json.dumps(reply, ensure_ascii=False)) < 4000  # Full group, not 496 quoted pairs.
        return reply

    async def drafts(*args, **kwargs):
        return ids, None

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
            for n in range(size + (size == 7)):
                for eid in (10285 + n, 10707 + n):
                    session.add(Event(id=eid, title='Импровизация' if n % 2 else 'Курс сценической игры',
                        description='', source_text='', short_description=None if eid == 10286 else 'Курс импровизации', date=(start + timedelta(days=n)).isoformat(),
                        time='19:00', telegraph_url='https://telegra.ph/course-fixture', location_name='Светлый зал' if n % 2 else 'Винный факультет', location_address='Гаражная 2' if n % 2 == 0 else None))
            session.add_all([Event(title='Посторонняя программа', description='', source_text='',
                date=(start - timedelta(days=60)).isoformat(), time='19:00', location_name='Другая площадка')
                for _ in range(801)])
            await session.commit()
            session.add(EventSource(event_id=10285, source_type='telegram', source_url='https://t.me/a/1'))
            source = EventSource(event_id=10286, source_type='parser', source_url='https://example.org/course')
            session.add(source)
            await session.commit()
            await session.refresh(source)
            session.add(EventSourceFact(event_id=10286, source_id=source.id, fact='Курс импровизации'))
            await session.commit()
        pagebuild = AsyncMock(side_effect=AssertionError('programme observer must only enqueue'))
        monkeypatch.setattr(main, 'update_telegraph_event_page', pagebuild)
        if transport == 'vk':
            await vk_intake.process_event('carrier', db=db)
        else:
            from source_parsing.telegram import handlers as tg
            from test_tg_monitor_reprocess_incomplete_scan import _results_path, _seed_source
            from smart_event_update import SmartUpdateResult
            await _seed_source(db)
            path = _results_path(tmp_path)
            payload = json.loads(path.read_text())
            template = payload['messages'][0]['events'][0]
            payload['messages'][0]['text'] = 'Курс импровизации. Гаражная 2. Калининград.'
            payload['messages'][0]['events'] = [dict(template, title='Курс импровизации',
                date=(start + timedelta(days=n)).isoformat(), time='19:00') for n in range(size)]
            path.write_text(json.dumps(payload))
            remaining = iter(ids)
            async def import_child(db_arg, candidate, **kwargs):
                eid = next(remaining)
                await persist(eid, [], db_arg)
                return SmartUpdateResult(status='created', event_id=eid)
            monkeypatch.setattr(tg, 'smart_event_update', import_child)
            monkeypatch.setattr(tg, '_fallback_fetch_posters_from_public_tg_page', AsyncMock(return_value=[]))
            monkeypatch.setattr(tg, '_schedule_primary_import_event_tasks', AsyncMock())
            report = await tg.process_telegram_results(path, db)
            assert report.events_created == size
        pagebuild.assert_not_awaited()
        async with db.get_session() as session:
            for eid in ids:
                links = (await session.get(Event, eid)).linked_event_ids
                assert all(peer in links for peer in range(10285, 10285 + size + (size == 7)) if peer != eid)
                assert not set(range(10707, 10707 + size)).intersection(links)
        assert len(calls) == (size + 31) // 32
        assert set(ids) == {i for call in calls for i in call['target_ids']}
        assert all('proof' in p and 'slot' in p for p in calls[0]['events'])
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_distinct_children_cleanup_and_unknown_preserves_known_edges(tmp_path, monkeypatch):
    db = Database(str(tmp_path / 'db.sqlite'))
    await db.init()

    async def classify(prompt, schema, **kwargs):
        packet = json.loads(prompt[prompt.index('{"target_ids"'):])
        if 10990 in packet['target_ids']:
            assert {10991, 10993} <= {p['id'] for p in packet['events']}
        return {'groups': [], 'decisions': [dict(target_id=10990, peer_id=i, relation=relation, target_quote='5-е место', peer_quote=quote)
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
            session.add_all([Event(title='Матч за 5-е место', description='', source_text='',
                short_description='Иная игра', date='2099-11-01', time='20:00', location_name='Общий зал')
                for _ in range(30)])
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


@pytest.mark.parametrize('invalid', ['same_slot', 'conflict', 'foreign_id', 'address_only'])
@pytest.mark.asyncio
async def test_group_grounding_rejects_unsafe_identity_without_erasing_known_links(tmp_path, monkeypatch, invalid):
    db = Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    try:
        async with db.get_session() as session:
            for eid, day in [(1, '2099-11-01'), (2, '2099-11-02'), (3, '2099-11-01')]:
                session.add(Event(id=eid, title='Курс импровизации', description='', source_text='shared sports carrier',
                    short_description='Курс импровизации', location_name='Зал', location_address='Гаражная 2',
                    date=day, time='19:00', linked_event_ids=[3 - eid] if eid < 3 else []))
            await session.commit()
        async def classify(prompt, schema, **kwargs):
            assert 'shared sports carrier' not in prompt
            ids = [1, 2, 3] if invalid == 'same_slot' else [1, 2, 999] if invalid == 'foreign_id' else [1, 2]
            quote = 'Гаражная 2' if invalid == 'address_only' else 'Курс импровизации'
            groups = [{'members': [dict(event_id=i, quote=quote) for i in ids]}]
            if invalid == 'conflict':
                groups.append({'members': [dict(event_id=i, quote=quote) for i in [2, 3]]})
            return {'groups': groups, 'decisions': []}
        monkeypatch.setattr('smart_event_update._ask_gemma_json', classify)
        await recompute_linked_event_ids(db, 1)
        async with db.get_session() as session:
            assert (await session.get(Event, 1)).linked_event_ids == [2]
            assert (await session.get(Event, 2)).linked_event_ids == [1]
            assert (await session.get(Event, 3)).linked_event_ids == []
    finally:
        await db.close()


@pytest.mark.parametrize('batch', ['course', 'sports', 'combined'])
@pytest.mark.asyncio
async def test_reported_probe_shape_compact_packets_and_title_grounding(tmp_path, monkeypatch, batch):
    """Reconstruction from owner report; retained exact probe JSON is absent in this sandbox."""
    from linked_events import _programme_decisions
    db = Database(str(tmp_path / 'probe.sqlite'))
    await db.init()
    course = list(range(10483, 10490))
    sports = [10990, 10991, 10992]
    targets = course if batch == 'course' else sports if batch == 'sports' else course + sports
    captured = []
    bad = True

    async def reply(prompt, schema, **kwargs):
        packet = json.loads(prompt[prompt.index('{"target_ids"'):])
        captured.append(packet)
        assert len(prompt) < 11000  # Reported 34-event prompt was 24039 characters.
        assert kwargs['max_tokens'] == 6000
        from smart_event_update import _gemma_native_response_schema
        native = _gemma_native_response_schema(schema)
        allowed = native['properties']['decisions']['items']['properties']['relation']['enum']
        assert set(allowed) == {'independent_event', 'duplicate_same_slot'} and 'old_links' not in allowed
        assert 'COMPOSITE CARRIER' not in prompt
        assert all(set(p) == {'id', 'title', 'slot', 'address', 'proof'} for p in packet['events'])
        available = {p['id']: p for p in packet['events']}
        aliases = [i for i in available if 10285 <= i <= 10291 or 10707 <= i <= 10713]
        if batch != 'sports':
            assert set(list(available)[:len(targets)]) == set(targets)  # Accepted targets precede external peers.
            assert len(available) <= len(targets) + 12 and not aliases
        if batch != 'course':
            assert [10990, 10991] in packet['review_pairs']
        members = [dict(event_id=i, quote='Курс импровизации') for i in course if i in available]
        if bad:
            # Reconstruct observed mixed-slot group and invalid field-name relation.
            if batch != 'sports':
                members.append(dict(event_id=aliases[0] if aliases else 10285, quote='Курс импровизации'))
            return {'groups': [{'members': members}] if batch != 'sports' else [], 'decisions': [
                dict(target_id=10990, peer_id=10991, relation='old_links',
                    target_quote='Матч за 5-е место', peer_quote='Матч за 3-е место')]}
        return {'groups': [{'members': members}] if len(members) > 1 else [], 'decisions': [
            dict(target_id=10990, peer_id=10991, relation='independent_event',
                target_quote='Матч за 5-е место', peer_quote='Матч за 3-е место')] if batch != 'course' else []}

    monkeypatch.setattr('smart_event_update._ask_gemma_json', reply)
    try:
        async with db.get_session() as session:
            for n in range(7):
                for eid in (10483 + n, 10285 + n, 10707 + n):
                    session.add(Event(id=eid, title='Курс импровизации', description='',
                        source_text='COMPOSITE CARRIER', short_description='Курс импровизации',
                        date=f'2099-12-{n + 1:02}', time='19:00', location_name='Зал', location_address='Гаражная 2'))
            for n, title in enumerate(['Матч за 5-е место', 'Матч за 3-е место', 'Финал']):
                session.add(Event(id=10990 + n, title=title, description='', source_text='COMPOSITE CARRIER',
                    date='2099-12-01', time=f'{12 + n * 2}:00', location_name='Зал',
                    linked_event_ids=[10991] if n == 0 else [10990] if n == 1 else []))
            session.add_all([Event(title='Независимый концерт', description='', source_text='',
                date='2099-12-08', time='19:00', location_name='Зал') for _ in range(10)])
            await session.commit()
        children = dict.fromkeys(targets, '')
        rejected = await _programme_decisions(db, children)
        assert not any(rejected.values())
        bad = False
        verdicts = await _programme_decisions(db, children)
        assert len(captured) == 2  # One bounded request for each separate reconstructed response.
        if batch != 'sports':
            for i in course:
                assert {j for j, v in verdicts[i].items() if v == 'same_programme_other_slot'} == set(course) - {i}
        if batch != 'course':
            assert verdicts[10990][10991] == 'independent_event'
            assert not any(v == 'same_programme_other_slot' for i in sports for v in verdicts.get(i, {}).values())
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_programme_group_resolves_existing_review_pairs_without_negative_decisions(tmp_path, monkeypatch):
    from itertools import combinations
    from linked_events import _programme_decisions
    db = Database(str(tmp_path / 'family.sqlite'))
    await db.init()
    ids = list(range(1, 8))
    pairs = list(combinations(ids, 2))
    group = {'members': [dict(event_id=i, quote='Курс импровизации') for i in ids]}
    # Owner-reported AUX failure shape, then ordinary-helper success shape; not exact captured JSON.
    answers = iter([
        {'groups': [group], 'decisions': [dict(target_id=i, peer_id=j, relation='independent_event',
            target_quote='Курс импровизации', peer_quote='Курс импровизации') for i, j in pairs]},
        {'groups': [group], 'decisions': []},
    ])

    async def replay(prompt, schema, **kwargs):
        packet = json.loads(prompt[prompt.index('{"target_ids"'):])
        assert packet['review_pairs'] == [list(pair) for pair in pairs]
        return next(answers)

    monkeypatch.setattr('smart_event_update._ask_gemma_json', replay)
    try:
        async with db.get_session() as session:
            for i in ids:
                session.add(Event(id=i, title='Курс импровизации', description='', source_text='',
                    date=f'2099-12-{i:02}', time='19:00', location_name='Зал',
                    linked_event_ids=[j for j in ids if j > i]))
            await session.commit()
        children = dict.fromkeys(ids, '')
        assert await _programme_decisions(db, children) == {}  # Group + 21 negatives contradict.
        verdicts = await _programme_decisions(db, children)
        assert sum(len(peers) for peers in verdicts.values()) == 42
        for i in ids:
            assert verdicts[i] == {j: 'same_programme_other_slot' for j in ids if j != i}
            await recompute_linked_event_ids(db, i, programme_decisions=verdicts[i])
        async with db.get_session() as session:
            for i in ids:
                assert set((await session.get(Event, i)).linked_event_ids) == set(ids) - {i}
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_child_scope_and_summary_precede_latest_media_receipt(tmp_path, monkeypatch):
    from linked_events import _programme_decisions
    db = Database(str(tmp_path / 'proof.sqlite'))
    await db.init()
    summary = 'Дмитрий Сущенко ведёт курс: сценическая импровизация.'
    selected = 'Выбранная программа: сценическая импровизация с Дмитрием Сущенко.'
    receipt = 'Добавлена афиша: https://static.kenigevents.ru/p/image/v2/fixture.webp'
    calls = []

    async def inspect(prompt, schema, **kwargs):
        packet = json.loads(prompt[prompt.index('{"target_ids"'):])
        calls.append(packet)
        proofs = {p['id']: p['proof'] for p in packet['events']}
        assert proofs == {1: summary, 2: selected, 3: receipt}
        return {'groups': [], 'decisions': []}

    monkeypatch.setattr('smart_event_update._ask_gemma_json', inspect)
    try:
        async with db.get_session() as session:
            for i in (1, 2, 3):
                session.add(Event(id=i, title='Практика импровизации', description='', source_text='',
                    short_description=summary if i < 3 else None,
                    date=f'2099-12-{i:02}', time='19:00', location_name='Зал'))
            await session.commit()
            for i in (1, 2, 3):
                source = EventSource(event_id=i, source_type='parser', source_url=f'https://example.test/{i}')
                session.add(source)
                await session.flush()
                session.add(EventSourceFact(event_id=i, source_id=source.id, status='added', fact='Прежний факт программы'))
                await session.flush()
                session.add(EventSourceFact(event_id=i, source_id=source.id, status='added', fact=receipt))
            await session.commit()
        await _programme_decisions(db, {1: '', 2: selected, 3: ''})
        assert len(calls) == 1
    finally:
        await db.close()
