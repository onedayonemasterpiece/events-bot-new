import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import event_media
import main
import smart_event_update as su
from db import Database
from models import Event
from source_parsing.telegram.handlers import _build_candidate

FIXTURE = Path(__file__).parent / 'replays/INC-2026-09-19-tg-monitoring-runtime-starvation/agropark2249.json'


def candidate(index=1, *, corrupt=False, legacy=False):
    message = json.loads(FIXTURE.read_text())
    if corrupt:
        message['text'] = message['semantic_source_text'] = 'different source'
    if legacy:
        message.pop('source_parse_decision')
    source = SimpleNamespace(default_location=None, default_ticket_link=None, trust_level='high')
    return _build_candidate(source, message, message['events'][index])


@pytest.mark.parametrize('index', range(6))
def test_verified_producer_children_keep_source_verdict_and_scope_routing(index):
    child = candidate(index)
    assert child.source_disposition == 'EVENTS_FOUND'
    assert child.source_evidence_complete is True
    assert child.metrics['source_event_count'] == 6
    assert su._candidate_needs_llm_occurrence_scope_review(child)


def test_invalid_receipt_and_legacy_input_do_not_gain_positive_verdict():
    assert candidate(corrupt=True).source_disposition == 'RETRY_REQUIRED'
    assert candidate(legacy=True).source_disposition is None


def test_single_child_without_multiple_date_evidence_does_not_need_scope_call():
    child = su.EventCandidate(source_type='telegram', source_url=None, source_text='19 сентября экскурсия в 14:00', metrics={'source_event_count': 1})
    assert not su._candidate_needs_llm_occurrence_scope_review(child)


def test_shared_month_date_list_grounds_both_days_without_inventing_range():
    assert su._extract_day_month_pairs('19 и 20 сентября') == {(19, 9), (20, 9)}
    assert su._extract_day_month_pairs('19, 21 и 23 сентября') == {(19, 9), (21, 9), (23, 9)}
    assert su._extract_day_month_pairs('19 участников, встреча 20 сентября') == {(20, 9)}


@pytest.mark.asyncio
async def test_scope_selects_only_excursion_and_shared_dates(monkeypatch):
    child = candidate()
    selected = ['19 и 20 сентября встречаемся в АгроПарке', '14:00 - экскурсия по АгроПарку', 'Знакомимся с нашими жителями поближе, узнаём интересные факты и, конечно, общаемся с животными.', 'АгроПарк «Некрасово поле»']
    async def ask(prompt, *_args, **_kwargs):
        assert 'в том числе в тот же день на той же площадке' in prompt
        return {'decision':'scoped','confidence':0.99,'selected_excerpts':selected}
    monkeypatch.setattr(su, 'SMART_UPDATE_LLM_DISABLED', False)
    monkeypatch.setattr(su, '_ask_gemma_json', ask)
    assert await su._llm_scope_candidate_occurrence(child) == (True, 'llm_scoped')
    assert 'картины из нитей' not in child.occurrence_scope_text
    assert 'картины из нитей' in child.source_text


@pytest.mark.asyncio
async def test_media_worker_early_success_refreshes_publication_dependencies(tmp_path, monkeypatch):
    db = Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    try:
        async with db.get_session() as session:
            event = Event(title='Экскурсия', description='Описание', source_text='Источник', date='2026-09-20', time='14:00', location_name='Парк')
            session.add(event)
            await session.commit()
            await session.refresh(event)
            event_id = event.id
        monkeypatch.setattr(main, '_rehydrate_missing_event_source_posters_for_telegraph', AsyncMock(return_value=0))
        monkeypatch.setattr(event_media, 'event_media_require_cdn', lambda: False)
        monkeypatch.setattr(event_media, 'review_next_event_media_pair', AsyncMock(return_value=True))
        schedule = AsyncMock(return_value={})
        monkeypatch.setattr(main, 'schedule_event_update_tasks', schedule)
        assert await main.job_event_media_review(event_id, db, None)
        schedule.assert_awaited_once()
    finally:
        await db.close()
