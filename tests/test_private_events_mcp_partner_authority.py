from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import select

from db import Database
from event_operation_receipts import EventOperationReceiptError
from models import Event, Festival
from smart_event_update import EventCandidate


@pytest.mark.asyncio
async def test_manual_partner_organizer_authority_forces_grounded_organizer_extraction(monkeypatch):
    import smart_event_update as module

    candidate = EventCandidate(
        source_type='manual',
        source_url='mcp-partner:organizer-test',
        source_text='Организатор события — Драмтеатр.',
        title='Спектакль',
        date='2026-11-20',
        time='19:00',
        location_name='Гостевая сцена',
        city='Калининград',
        event_operation_context={
            'operation_ref':'evt_op_' + 'a' * 24,
            'action_digest':'b' * 64,
            'actor_subject':'partner:' + 'c' * 32 + ':1',
            'actor_client_id':'partner-client',
            'actor_audience':'partner-resource',
            'partner_policy_revision':1,
            'partner_authority_kinds':['organizer'],
        },
    )
    calls = []

    async def fake_ask(_prompt, _schema, **_kwargs):
        calls.append(True)
        return {
            'public_core_facts':[],
            'program_or_examples':[],
            'context_methodology_facts':[],
            'people_org_facts':[],
            'organizer_names':[{
                'name':'Драмтеатр',
                'evidence_quote':'Организатор события — Драмтеатр',
            }],
            'logistics_facts':[],
            'uncertain_or_drop':[],
        }

    monkeypatch.setattr(module, 'SMART_UPDATE_LLM_DISABLED', False)
    monkeypatch.setattr(module, 'SMART_UPDATE_G4_SPLIT_CREATE', False)
    monkeypatch.setattr(module, '_ask_gemma_json', fake_ask)

    await module._llm_extract_candidate_facts(candidate)
    assert calls == [True]
    assert candidate.organizer_names == ['Драмтеатр']


@pytest.mark.asyncio
async def test_ordinary_manual_source_still_skips_rich_organizer_extraction(monkeypatch):
    import smart_event_update as module

    candidate = EventCandidate(
        source_type='manual',
        source_url='manual:test',
        source_text='Организатор события — Драмтеатр.',
        title='Спектакль',
        date='2026-11-20',
        time='19:00',
        location_name='Сцена',
    )

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("ordinary manual source must keep existing no-LLM behavior")

    monkeypatch.setattr(module, 'SMART_UPDATE_LLM_DISABLED', False)
    monkeypatch.setattr(module, 'SMART_UPDATE_G4_SPLIT_CREATE', False)
    monkeypatch.setattr(module, '_ask_gemma_json', forbidden)
    assert await module._llm_extract_candidate_facts(candidate) == []
    assert candidate.organizer_names == []


@pytest.mark.asyncio
async def test_partner_festival_candidate_cannot_write_festival_before_authority_gate(
    tmp_path, monkeypatch
):
    import main
    import smart_event_update as smart_module

    monkeypatch.setenv('DB_INIT_SKIP_VK_SOURCES_SEED', '1')
    db = Database(str(tmp_path / 'authority-festival.sqlite'))
    await db.init()
    future = (date.today() + timedelta(days=30)).isoformat()

    async def parse(_text, *_args, **_kwargs):
        return [{
            'title':'Концерт фестиваля',
            'short_description':'Один концерт фестиваля.',
            'date':future,
            'time':'19:00',
            'location_name':'Чужая площадка',
            'location_address':'ул. Тестовая, 1',
            'city':'Калининград',
            'event_type':'концерт',
            'festival':'Кантата',
            'festival_context':'event_with_festival',
        }]

    async def topics(_event):
        return ['CONCERTS']

    calls = []

    async def forbidden_ensure_festival(*_args, **_kwargs):
        calls.append(True)
        raise AssertionError('Festival registry write happened before authority gate')

    monkeypatch.setattr(main, 'parse_event_via_llm', parse)
    monkeypatch.setattr(main, 'classify_event_topics', topics)
    monkeypatch.setattr(main, 'ensure_festival', forbidden_ensure_festival)
    monkeypatch.setattr(smart_module, 'SMART_UPDATE_LLM_DISABLED', True)

    context = {
        'operation_ref':'evt_op_' + 'd' * 24,
        'action_digest':'e' * 64,
        'actor_subject':'partner:' + 'f' * 32 + ':1',
        'actor_client_id':'partner-client',
        'actor_audience':'partner-resource',
        'partner_policy_revision':1,
        'partner_authority_kinds':['festival_operator'],
    }
    try:
        with pytest.raises(EventOperationReceiptError):
            await main.add_events_from_text(
                db,
                f'Концерт фестиваля Кантата {future} в 19:00.',
                None,
                raise_exc=True,
                display_source=False,
                source_type_override='manual',
                source_url_override='mcp-partner:festival-side-effect',
                defer_external_projections=True,
                require_single_event=True,
                allow_festival_queue=False,
                allow_lifecycle_actions=False,
                event_operation_context=context,
            )
        assert calls == []
        async with db.get_session() as session:
            festivals = (await session.execute(select(Festival))).scalars().all()
            events = (await session.execute(select(Event))).scalars().all()
            assert festivals == []
            assert events == []
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_partner_series_authority_extracts_only_grounded_explicit_membership(monkeypatch):
    import smart_event_update as module

    candidate = EventCandidate(
        source_type='manual',
        source_url='mcp-partner:series-test',
        source_text='Лекция проходит в рамках проекта «Городские лекции».',
        title='Лекция о городе',
        date='2026-11-20',
        time='19:00',
        location_name='Лекторий',
        city='Калининград',
        event_operation_context={
            'operation_ref':'evt_op_' + '1' * 24,
            'action_digest':'2' * 64,
            'actor_subject':'partner:' + '3' * 32 + ':1',
            'actor_client_id':'partner-client',
            'actor_audience':'partner-resource',
            'partner_policy_revision':1,
            'partner_authority_kinds':['series_operator'],
        },
    )
    prompts = []

    async def fake_ask(prompt, schema, **kwargs):
        prompts.append((prompt, schema, kwargs.get('label')))
        return {
            'series_memberships':[
                {
                    'name':'Городские лекции',
                    'evidence_quote':'в рамках проекта «Городские лекции»',
                }
            ]
        }

    monkeypatch.setattr(module, 'SMART_UPDATE_LLM_DISABLED', False)
    monkeypatch.setattr(module, '_ask_gemma_json', fake_ask)
    names = await module.adjudicate_partner_series_authority(candidate)
    assert names == ['Городские лекции']
    assert candidate.authority_series_names == ['Городские лекции']
    assert prompts[0][2] == 'partner_series_authority'
    assert 'series_operator' not in prompts[0][0]


@pytest.mark.asyncio
async def test_partner_series_authority_rejects_ungrounded_or_shortenend_name(monkeypatch):
    import smart_event_update as module

    candidate = EventCandidate(
        source_type='manual',
        source_url='mcp-partner:series-negative',
        source_text='Лекция проходит в рамках проекта «Городские лекции».',
        title='Лекция',
        date='2026-11-20',
        time='19:00',
        location_name='Лекторий',
    )

    async def fake_ask(*_args, **_kwargs):
        return {
            'series_memberships':[
                {'name':'Другой проект','evidence_quote':'в рамках проекта «Городские лекции»'},
                {'name':'Городские лекции','evidence_quote':'несуществующая цитата'},
            ]
        }

    monkeypatch.setattr(module, 'SMART_UPDATE_LLM_DISABLED', False)
    monkeypatch.setattr(module, '_ask_gemma_json', fake_ask)
    assert await module.adjudicate_partner_series_authority(candidate) == []
    assert candidate.authority_series_names == []
