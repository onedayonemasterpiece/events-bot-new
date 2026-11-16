"""Mechanical normal Qtickets facade replay; models and observers isolated."""
import json
import socket
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
import main
import smart_event_update as su
import linked_events
from db import Database
from models import Event, EventSource, EventSourceFact
from source_parsing.handlers import _smart_event_update_with_lock_retry

@pytest.mark.asyncio
async def test_qtickets_wrapper_unavailable_merge_keeps_before(tmp_path, monkeypatch):
    data = json.loads((Path(__file__).parent / 'replays' /
        'INC-2026-11-16-unavailable-merge-raw-description' / 'qtickets10424.json').read_text())
    monkeypatch.setattr(su, 'SMART_UPDATE_LLM_DISABLED', False)
    monkeypatch.setattr(su, 'SMART_UPDATE_FACT_FIRST', True)
    monkeypatch.setattr(su, 'SMART_UPDATE_MERGE_IDENTITY_GATE_MODE', su.IdentityGateMode.OFF)
    monkeypatch.setattr(su, '_llm_review_candidate_eventness', AsyncMock(return_value=('event', 1.0, 'fixture')))
    monkeypatch.setenv('REGION_FILTER_ENABLED', '0')
    merge = AsyncMock(return_value=None)
    monkeypatch.setattr(su, '_llm_merge_event', merge)
    model_stubs = {}
    for name in ['_ask_gemma_json', '_llm_build_short_description', '_llm_build_search_digest',
                 '_rewrite_description_full_from_sources', '_llm_fact_first_description_md_bounded']:
        mock = AsyncMock(return_value=None)
        monkeypatch.setattr(su, name, mock)
        model_stubs[name] = mock
    topics = AsyncMock(return_value=None)
    monkeypatch.setattr(su, '_classify_topics', topics)
    monkeypatch.setattr(su, '_apply_holiday_festival_mapping', AsyncMock(return_value=False))
    monkeypatch.setattr(linked_events, 'recompute_linked_event_ids', AsyncMock(return_value=SimpleNamespace(changed_event_ids=[])))
    schedule = AsyncMock()
    monkeypatch.setattr(main, 'schedule_event_update_tasks', schedule)
    network_attempts = []
    def blocked_connect(*args, **kwargs):
        network_attempts.append('socket.connect')
        raise AssertionError('External network forbidden in mechanical replay')
    monkeypatch.setattr(socket.socket, 'connect', blocked_connect)
    url = 'https://kaliningrad.qtickets.events/258183-23-oktyabrya-rammstein-tribute'
    title = '12 ДЕКАБРЯ - RAMMSTEIN TRIBUTE - КАЛИНИНГРАД'
    candidate = su.EventCandidate(source_type='parser:qtickets', source_url=url,
        source_text=data['source_text'], raw_excerpt=data['raw_excerpt'], title=title,
        date='2026-12-12', time='20:00', location_name='Бастион Холл',
        location_address='ул. Судостроительная, 6/1, Калининград, Россия', city='Калининград',
        ticket_link=url, ticket_price_min=1000, ticket_status='available', event_type='концерт',
        trust_level='high', candidate_key='qtickets-10424-normal-replay', occurrence_key='slot:0',
        defer_external_projections=True)
    db = Database(str(tmp_path / 'qtickets.sqlite'))
    await db.init()
    try:
        async with db.get_session() as session:
            session.add(Event(id=10424, title=title, description=data['base'], short_description=data['raw_excerpt'],
                search_digest=data['raw_excerpt'], date=candidate.date, time=candidate.time,
                location_name=candidate.location_name, location_address=candidate.location_address,
                city=candidate.city, source_text=data['base'], source_texts=[data['base']],
                ticket_link=url, ticket_price_min=1000, event_type='концерт'))
            await session.flush()
            session.add(EventSource(event_id=10424, source_type='parser:qtickets', source_url=url,
                canonical_source_url=url, source_text=data['base'], source_role='identity_bearing',
                candidate_key=candidate.candidate_key, occurrence_key=candidate.occurrence_key))
            await session.commit()
        result = await _smart_event_update_with_lock_retry(db, candidate, su.smart_event_update, attempts=1)
        async with db.get_session() as session:
            saved = await session.get(Event, 10424)
            sources = (await session.execute(select(EventSource).where(EventSource.event_id == 10424))).scalars().all()
            facts = (await session.execute(select(EventSourceFact).where(EventSourceFact.event_id == 10424))).scalars().all()
            observed = {'outcome': str(result.outcome), 'status': result.status, 'event_id': result.event_id,
                'description': saved.description, 'before': data['base'], 'base_provenance': data['base_provenance'],
                'source_receipts': [{'url': s.source_url, 'source_text': s.source_text} for s in sources],
                'fact_receipts': [{'fact': f.fact, 'status': f.status} for f in facts],
                'canonical': {k: getattr(saved, k) for k in ['title', 'date', 'time', 'location_name', 'location_address']},
                'merge_calls': merge.await_count, 'topic_calls': topics.await_count,
                'model_stub_calls': {k: m.await_count for k, m in model_stubs.items()},
                'schedule_calls': schedule.await_count, 'network_attempts': network_attempts,
                'replay_schedule_tasks': candidate.replay_schedule_tasks}
        with sqlite3.connect(db.path) as conn:
            conn.row_factory = sqlite3.Row
            observed['candidate_receipts'] = [dict(r) for r in conn.execute('select * from smart_update_candidate_state')]
        assert result.status == 'merged' and result.event_id == 10424
        assert merge.await_count == 1
        assert candidate.replay_schedule_tasks is False
        assert any(s['source_text'] == data['source_text'] for s in observed['source_receipts'])
        assert observed['fact_receipts']
        assert any(r['current_outcome'] == 'MERGED' and r['accepted_event_id'] == 10424
                   for r in observed['candidate_receipts'])
        assert network_attempts == []
        assert saved.description == data['base'], 'Unavailable merge must not append raw source instructions'
        assert 'OCR афиши используй' not in saved.description
        assert topics.await_count == 0
        assert schedule.await_count == 0
    finally:
        await db.close()
