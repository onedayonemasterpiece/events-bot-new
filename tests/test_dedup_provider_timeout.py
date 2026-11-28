"""Exercise actual stage/gateway/provider timing with a cancellable SDK boundary."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import smart_event_update as su
from google_ai.client import GoogleAIClient, ReserveResult


def gateway(monkeypatch, *, primary='pending', fallback='ok', payload=None):
    client = GoogleAIClient()
    client.max_retries = 1
    client.dry_run = False
    client.provider_timeout_seconds = 120.0
    client.allow_provider_model_fallback = True
    client._genai = None
    calls, events = [], []
    client.fixture_prompts = []
    started = asyncio.Event()

    async def generate_content(*, model, **kwargs):
        calls.append(model)
        client.fixture_prompts.append(kwargs.get("contents"))
        started.set()
        behavior = primary if 'gemma' in model else fallback
        if behavior == 'pending':
            await asyncio.Event().wait()
        if behavior == 'error':
            raise RuntimeError('503 unavailable fixture')
        text = json.dumps(payload if payload is not None else {'decision': 'distinct'})
        return SimpleNamespace(text=text, candidates=[SimpleNamespace(
            content=SimpleNamespace(parts=[SimpleNamespace(text=text)]), finish_reason='STOP')],
            usage_metadata={})

    client._genai_new = SimpleNamespace(Client=lambda **kwargs: SimpleNamespace(
        aio=SimpleNamespace(models=SimpleNamespace(generate_content=generate_content))))
    monkeypatch.setattr(client, '_reserve', AsyncMock(return_value=ReserveResult(
        ok=True, api_key_id='fixture', env_var_name='FIXTURE_KEY')))
    monkeypatch.setattr(client, '_get_api_key', lambda *args: 'fixture')
    monkeypatch.setattr(client, '_mark_sent', AsyncMock())
    monkeypatch.setattr(client, '_finalize', AsyncMock())
    monkeypatch.setattr(client, '_notify_incident', AsyncMock())
    monkeypatch.setattr(client, '_log_event', lambda name, ctx, **kwargs: events.append(
        (name, ctx.logical_request_uid, ctx.request_uid)))
    monkeypatch.setattr(su, '_get_gemma_client', lambda: client)
    monkeypatch.setattr(su, '_smart_update_model_chain', lambda label: [
        'gemma-4-31b-it', 'gemini-3.1-flash-lite'])
    monkeypatch.setattr(su, '_smart_update_native_schema_enabled', lambda label: False)
    monkeypatch.setattr(su, '_smart_update_4o_fallback_enabled', lambda label: False)
    monkeypatch.setattr(su, 'SMART_UPDATE_GEMMA_JSON_WALL_CLOCK_SEC', 0.4)
    monkeypatch.setenv('SMART_UPDATE_GEMMA_RETRIES', '1')
    return client, calls, events, started


@pytest.mark.asyncio
@pytest.mark.parametrize('primary', ['pending', 'error'])
async def test_dedup_uses_existing_model_fallback_before_stage_cap(monkeypatch, primary):
    client, calls, events, _ = gateway(monkeypatch, primary=primary)
    result = await su._ask_gemma_json('fixture', {}, max_tokens=64, label='dedup_adjudicator')
    assert result == {'decision': 'distinct'}
    assert len(calls) == 2 and 'gemma' in calls[0] and 'gemini' in calls[1]
    outcomes = [x for x in events if x[0] in {'google_ai.call_error', 'google_ai.call_ok'}]
    assert [x[0] for x in outcomes] == ['google_ai.call_error', 'google_ai.call_ok']
    assert outcomes[0][1] == outcomes[1][1]  # Same logical request, distinct model receipts.
    assert outcomes[0][2] != outcomes[1][2]
    assert client._finalize.await_count == 2
    assert client.provider_timeout_seconds == 120.0


@pytest.mark.asyncio
async def test_permanent_failure_does_not_add_a_third_gateway_attempt(monkeypatch):
    _, calls, _, _ = gateway(monkeypatch, primary='error', fallback='error')
    assert await su._ask_gemma_json('fixture', {}, max_tokens=64, label='dedup_adjudicator') is None
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_unrelated_stage_keeps_existing_outer_timeout(monkeypatch):
    client, calls, events, _ = gateway(monkeypatch)
    assert await su._ask_gemma_json('fixture', {}, max_tokens=64, label='merge') is None
    assert len(calls) == 1
    assert any(x[0] == 'google_ai.call_cancelled' for x in events)
    assert client.provider_timeout_seconds == 120.0


@pytest.mark.asyncio
async def test_parent_cancellation_does_not_trigger_model_fallback(monkeypatch):
    _, calls, _, started = gateway(monkeypatch)
    task = asyncio.create_task(su._ask_gemma_json('fixture', {}, max_tokens=64,
        label='dedup_adjudicator'))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_request_timeout_does_not_mutate_concurrent_shared_client(monkeypatch):
    client, _, _, _ = gateway(monkeypatch, primary='ok')
    client.provider_timeout_seconds = 0.1
    original = client._genai_new.Client

    def sdk(**kwargs):
        instance = original(**kwargs)
        generate = instance.aio.models.generate_content
        async def slow(**params):
            await asyncio.sleep(0.04)
            return await generate(**params)
        instance.aio.models.generate_content = slow
        return instance

    client._genai_new.Client = sdk
    async def call(timeout=None):
        return await client._call_provider(api_key='fixture', model='gemma-4-31b-it',
            prompt='fixture', generation_config=None, safety_settings=None,
            max_output_tokens=64, provider_timeout_seconds=timeout)
    results = await asyncio.gather(call(0.01), call(), return_exceptions=True)
    assert isinstance(results[0], TimeoutError)
    assert isinstance(results[1], tuple)
    assert client.provider_timeout_seconds == 0.1


@pytest.mark.asyncio
async def test_explicit_timeout_keeps_a_stricter_configured_cap(monkeypatch):
    client, calls, _, _ = gateway(monkeypatch)
    client.provider_timeout_seconds = 0.01
    with pytest.raises(TimeoutError):
        await client._call_provider(api_key='fixture', model='gemma-4-31b-it',
            prompt='fixture', generation_config=None, safety_settings=None,
            max_output_tokens=64, provider_timeout_seconds=0.2)
    assert len(calls) == 1 and client.provider_timeout_seconds == 0.01


@pytest.mark.asyncio
@pytest.mark.parametrize('verdict', ['distinct', 'unknown', 'permanent'])
async def test_gateway_timeout_preserves_sibling_and_normal_creation(tmp_path, monkeypatch, verdict):
    from db import Database
    from models import Event, EventSource, JobOutbox
    from sqlalchemy import select
    from smart_update_identity import IdentityGateMode
    import main
    import linked_events

    source = '28 и 29 ноября 2099 в 11:30 Каппинг кофе в Музее, Калининград.'
    payload = dict(action='create', match_event_id=None, confidence=0.99,
        reason_code='session_split_keep', reason='Два дня в источнике.',
        relation='distinct_occurrence', source_grounded_evidence=['28 и 29 ноября'],
        blocking_conflicts=['Существующий сеанс 28 ноября, кандидат 29 ноября.'])
    if verdict == 'unknown':
        payload.update(relation='unknown', reason_code='no_candidate_match',
            source_grounded_evidence=[], blocking_conflicts=[])
    client, calls, _, _ = gateway(monkeypatch,
        primary='error' if verdict == 'permanent' else 'pending',
        fallback='error' if verdict == 'permanent' else 'ok', payload=payload)
    monkeypatch.setenv('DB_INIT_SKIP_VK_SOURCES_SEED', '1')
    monkeypatch.setenv('EVENT_UPDATE_SYNC', '0')
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '231920894')
    monkeypatch.setattr(su, 'SMART_UPDATE_LLM_DISABLED', False)
    monkeypatch.setattr(su, 'SMART_UPDATE_DEDUP_ADJUDICATOR', True)
    monkeypatch.setattr(su, 'SMART_UPDATE_IDENTITY_GATE_MODE', IdentityGateMode.OFF)
    monkeypatch.setattr(su, 'SMART_UPDATE_MERGE_IDENTITY_GATE_MODE', IdentityGateMode.OFF)
    monkeypatch.setattr(su, 'SMART_UPDATE_G4_SPLIT_CREATE', True)
    monkeypatch.setattr(su, '_dedup_adjudicator_block_candidates', lambda candidate, events, posters: list(events[:1]))
    monkeypatch.setattr(su, '_classify_topics', AsyncMock())
    monkeypatch.setattr(su, '_llm_review_candidate_eventness', AsyncMock(return_value=('event', 0.99, 'fixture')))
    monkeypatch.setattr(su, '_llm_match_or_create_bundle', AsyncMock(return_value=None))
    monkeypatch.setattr(su, '_llm_g4_split_create_bundle', AsyncMock(return_value=dict(
        _split_create=True, title='Каппинг кофе', description='Каппинг кофе в Музее.',
        facts=['Каппинг кофе в Музее.'], search_digest='Каппинг кофе в Музее.',
        short_description='Каппинг кофе в Музее.')))
    monkeypatch.setattr(su, '_llm_review_create_bundle_grounding', AsyncMock(return_value=(True, 'grounded', [])))
    # Programme-link semantics are independent; scheduling and identity/gateway remain real.
    monkeypatch.setattr(linked_events, 'recompute_linked_event_ids', AsyncMock(return_value=None))
    db = Database(str(tmp_path / 'events.sqlite'))
    try:
        await db.init()
        async with db.get_session() as session:
            old = Event(title='Каппинг кофе', description='Принятое описание 28 ноября.',
                date='2099-11-28', time='11:30', location_name='Музей', city='Калининград',
                event_type='мастер-класс', source_text=source)
            session.add(old)
            await session.flush()
            session.add(EventSource(event_id=old.id, source_type='telegram',
                source_url='https://t.me/timeout_fixture/9296', source_message_id=9296,
                source_chat_username='timeout_fixture', source_text=source,
                source_role='identity_bearing', canonical_source_url='https://t.me/timeout_fixture/9296',
                candidate_key='accepted-sibling', occurrence_key='accepted-28-nov'))
            await session.commit()
        candidate = su.EventCandidate(source_type='telegram',
            source_url='https://t.me/timeout_fixture/9296', source_message_id=9296,
            source_chat_username='timeout_fixture', source_text=source, raw_excerpt='Каппинг кофе в Музее.',
            title='Каппинг кофе', date='2099-11-29', time='11:30', location_name='Музей',
            city='Калининград', event_type='мастер-класс', producer_ordinal=1)
        result = await su.smart_event_update(db, candidate, check_source_url=False, schedule_tasks=True)
        assert len(calls) == 2, (result, calls)
        assert all('2099-11-28' in str(prompt) and '2099-11-29' in str(prompt)
            for prompt in client.fixture_prompts)
        async with db.get_session() as session:
            events = list((await session.execute(select(Event))).scalars())
            saved = await session.get(Event, old.id)
            assert (saved.date, saved.time, saved.description) == (
                '2099-11-28', '11:30', 'Принятое описание 28 ноября.')
            sources = list((await session.execute(select(EventSource))).scalars())
            original = next(s for s in sources if s.event_id == old.id)
            assert original.candidate_key == 'accepted-sibling'
            jobs = list((await session.execute(select(JobOutbox))).scalars())
            if verdict == 'distinct':
                assert result.outcome is su.SmartUpdateTerminalOutcome.CREATED
                assert len(events) == 2 and result.event_id != old.id
                created = await session.get(Event, result.event_id)
                assert (created.date, created.time) == ('2099-11-29', '11:30')
                assert any(s.event_id == created.id and s.source_url == candidate.source_url for s in sources)
                assert {'vk_sync', 'ics_publish', 'telegraph_build'} <= {
                    j.task.value for j in jobs if j.event_id == created.id}
            else:
                assert result.outcome is su.SmartUpdateTerminalOutcome.RETRY_SCHEDULED
                assert len(events) == 1 and not jobs
    finally:
        await db.close()
