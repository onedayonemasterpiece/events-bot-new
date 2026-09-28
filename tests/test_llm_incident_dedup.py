from unittest.mock import AsyncMock
import pytest
import main

@pytest.mark.asyncio
async def test_duplicate_alerts_across_clients_keep_new_failures_and_summary(monkeypatch):
    monkeypatch.setattr(main, '_llm_incident_recent', {})
    monkeypatch.setattr(main, 'get_db', lambda: object())
    monkeypatch.setattr(main, 'get_bot', lambda: object())
    notify = AsyncMock()
    monkeypatch.setattr(main, 'notify_superadmin', notify)
    now = [1000.0]
    monkeypatch.setattr(main._time, 'monotonic', lambda: now[0])
    payload = {'consumer':'event_parse','model':'gemini-2.5-flash','error_code':'503'}
    await main.notify_llm_incident('provider_error', {**payload, 'request_uid':'one'})
    await main.notify_llm_incident('provider_error', {**payload, 'request_uid':'two'})
    assert notify.await_count == 1
    await main.notify_llm_incident('provider_error', {**payload, 'error_code':'401'})
    assert notify.await_count == 2
    now[0] += 601
    await main.notify_llm_incident('provider_error', payload)
    assert notify.await_count == 3
    assert 'Повторов за предыдущие 10 минут: 1' in notify.call_args.args[2]
