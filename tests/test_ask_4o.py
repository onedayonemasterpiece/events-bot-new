import pytest

import main


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class DummySession:
    def __init__(self, payload):
        self._payload = payload
        self.post_calls = []

    async def post(self, url, json, headers):
        self.post_calls.append((url, json, headers))
        return DummyResponse(self._payload)


@pytest.mark.asyncio
async def test_ask_4o_keeps_long_response(monkeypatch):
    monkeypatch.setenv("FOUR_O_TOKEN", "test-token")
    long_content = "L" * (main.FOUR_O_RESPONSE_LIMIT + 200)
    payload = {
        "id": "chatcmpl-test",
        "choices": [
            {
                "message": {
                    "content": long_content,
                }
            }
        ],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": len(long_content) // 2,
            "total_tokens": len(long_content),
        },
    }
    session = DummySession(payload)
    monkeypatch.setattr(main, "get_http_session", lambda: session)

    calls: list[tuple] = []

    async def fake_log(bot, model, usage, *, endpoint, request_id, meta=None):
        calls.append((bot, model, usage, endpoint, request_id, {} if meta is None else meta))

    monkeypatch.setattr(main, "log_token_usage", fake_log)

    result = await main.ask_4o("prompt", max_tokens=len(long_content))

    assert result == long_content
    assert session.post_calls, "session.post should be invoked"
    assert session.post_calls[0][1]["max_tokens"] == len(long_content)
    assert calls == [
        (
            main.BOT_CODE,
            "gpt-4o",
            payload["usage"],
            "chat.completions",
            payload["id"],
            {},
        )
    ]


@pytest.mark.asyncio
async def test_ask_4o_falls_back_to_mini_near_daily_budget(monkeypatch):
    monkeypatch.setenv("FOUR_O_TOKEN", "test-token")
    payload = {
        "id": "chatcmpl-budget",
        "choices": [{"message": {"content": "ok"}}],
        "usage": {
            "prompt_tokens": 12,
            "completion_tokens": 2,
            "total_tokens": 14,
        },
    }
    session = DummySession(payload)
    monkeypatch.setattr(main, "get_http_session", lambda: session)
    monkeypatch.setattr(main, "FOUR_O_GPT4O_DAILY_TOKEN_LIMIT", 950_000)
    monkeypatch.setattr(main, "FOUR_O_GPT4O_FALLBACK_MODEL", "gpt-4o-mini")

    async def fake_daily_usage(today=None):
        return 949_900

    monkeypatch.setattr(main, "_get_gpt4o_daily_usage_total", fake_daily_usage)
    calls: list[tuple] = []

    async def fake_log(bot, model, usage, *, endpoint, request_id, meta=None):
        calls.append((bot, model, usage, endpoint, request_id, {} if meta is None else meta))

    monkeypatch.setattr(main, "log_token_usage", fake_log)

    result = await main.ask_4o("x" * 500, max_tokens=200)

    assert result == "ok"
    assert session.post_calls[0][1]["model"] == "gpt-4o-mini"
    assert calls == [
        (
            main.BOT_CODE,
            "gpt-4o-mini",
            payload["usage"],
            "chat.completions",
            payload["id"],
            {},
        )
    ]
