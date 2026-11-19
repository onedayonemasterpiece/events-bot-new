from unittest.mock import AsyncMock
import logging
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


@pytest.mark.asyncio
async def test_intermediate_warning_is_logged_while_terminal_failure_alerts(monkeypatch, caplog):
    monkeypatch.setattr(main, '_llm_incident_recent', {})
    monkeypatch.setattr(main, 'get_db', lambda: object())
    monkeypatch.setattr(main, 'get_bot', lambda: object())
    notify = AsyncMock()
    monkeypatch.setattr(main, 'notify_superadmin', notify)
    payload = {'consumer':'smart_update','model':'gemma-4-31b-it','error_code':'503'}
    await main.notify_llm_incident('smart_update_gemma_fallback_4o',
                                   {**payload,'severity':'warning','next_model':'gpt-4o'})
    notify.assert_not_awaited()
    assert 'smart_update_gemma_fallback_4o' in caplog.text
    await main.notify_llm_incident('provider_exhausted', {**payload,'severity':'critical'})
    notify.assert_awaited_once()


@pytest.mark.asyncio
async def test_receipts_follow_acknowledged_sends_without_recipient_ids(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    bot = AsyncMock()
    monkeypatch.setattr(main, "_llm_incident_recent", {})
    monkeypatch.setattr(main, "get_db", lambda: object())
    monkeypatch.setattr(main, "get_bot", lambda: bot)
    monkeypatch.setattr(main, "get_superadmin_id", AsyncMock(return_value=81234))
    payload = {
        "consumer": "smart_update",
        "error_code": "503",
        "logical_request_uid": "logical-id",
        "request_uid": "attempt-id",
        "operator_chat_id": 81235,
        "message": "private message",
    }
    await main.notify_llm_incident("provider_error", {**payload, "severity": "warning"})
    bot.send_message.assert_not_awaited()
    await main.notify_llm_incident("provider_error", payload)
    await main.notify_llm_incident("provider_error", payload)

    assert bot.send_message.await_count == 2
    messages = [r.getMessage() for r in caplog.records if "llm_incident.delivered" in r.getMessage()]
    assert len(messages) == 2
    assert "recipient=operator " in messages[0]
    assert "recipient=superadmin " in messages[1]
    for receipt in messages:
        assert "kind=provider_error consumer=smart_update code=503" in receipt
        assert "logical_request_uid=logical-id" in receipt
        assert all(private not in receipt for private in ("81234", "81235", "private message"))


@pytest.mark.asyncio
@pytest.mark.parametrize("operator_reply,admin_reply,roles", [
    (None, RuntimeError("unavailable"), []),
    (RuntimeError("unavailable"), object(), ["superadmin"]),
    (object(), None, ["operator"]),
])
async def test_unacknowledged_recipient_has_no_receipt(monkeypatch, caplog, operator_reply, admin_reply, roles):
    caplog.set_level(logging.INFO)
    bot = AsyncMock()
    bot.send_message.side_effect = [operator_reply, admin_reply]
    monkeypatch.setattr(main, "_llm_incident_recent", {})
    monkeypatch.setattr(main, "get_db", lambda: object())
    monkeypatch.setattr(main, "get_bot", lambda: bot)
    monkeypatch.setattr(main, "get_superadmin_id", AsyncMock(return_value=81111))
    await main.notify_llm_incident("provider_error", {"operator_chat_id": 82222, "request_uid": "single-id"})
    receipts = [r.getMessage() for r in caplog.records if "llm_incident.delivered" in r.getMessage()]
    assert len(receipts) == len(roles)
    for receipt, role in zip(receipts, roles):
        assert f"recipient={role} " in receipt
        assert "logical_request_uid=single-id" in receipt
