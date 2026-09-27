import asyncio
from types import SimpleNamespace

import pytest
from aiohttp import web

from static_site_live_search import (
    KenigEventsLiveSearchAdapter,
    LiveSearchConfig,
    RESOURCE_ID,
    _session_actor,
    _token_hash,
    register,
)


def enabled_config():
    return LiveSearchConfig.from_env(
        {
            "ENABLE_STATIC_SITE_LIVE_SEARCH": "1",
            "PERSONALIZATION_SUPABASE_URL": "https://example.supabase.co",
            "PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY": "public-key",
            "STATIC_SITE_LIVE_SEARCH_ALLOWED_ORIGINS": "https://kenigevents.ru,https://static.kenigevents.ru",
        }
    )


def test_disabled_register_has_no_routes_or_dependency_requirement():
    app = web.Application()
    assert register(app, env={}) is False
    assert not list(app.router.routes())


@pytest.mark.asyncio
async def test_tool_emits_canonical_payload_and_returns_bounded_model_summary():
    calls = []
    emitted = []

    async def search_call(config, token, query, offset):
        calls.append((token, query, offset))
        return {
            "items": [
                {"id": 11, "title": "Концерт", "date": "2026-09-27", "city": "Калининград"},
                {"id": 12, "title": "Лекция", "date": "2026-09-28", "city": "Светлогорск"},
            ],
            "fallback_items": [],
            "has_more": True,
            "request_id": "req",
        }

    adapter = KenigEventsLiveSearchAdapter(
        config=enabled_config(),
        emit=lambda session, event: emitted.append(event),
        write=lambda *_args: None,
        measure=lambda *_args: None,
        timing=lambda *_args: None,
        search_call=search_call,
    )
    initialized = adapter.initialize(
        resource_id=RESOURCE_ID,
        actor={"subject": "user", "tenant_id": "kenigevents"},
        model="gemini-3.8-live",
        access_token="token-a",
    )
    session = SimpleNamespace(state=initialized["state"])
    result = await adapter.execute_tool(
        session,
        {"name": "search_events", "id": "one", "args": {"query": "джаз завтра"}},
    )

    assert calls == [("token-a", "джаз завтра", 0)]
    assert emitted[0]["type"] == "search_results"
    assert emitted[0]["data"]["items"][0]["id"] == 11
    assert result["cards_already_shown_to_user"] is True
    assert result["has_more"] is True
    assert result["results"][0] == {
        "id": 11,
        "title": "Концерт",
        "date": "2026-09-27",
        "city": "Калининград",
    }


@pytest.mark.asyncio
async def test_continue_search_reuses_query_and_moves_one_page():
    calls = []

    async def search_call(_config, _token, query, offset):
        calls.append((query, offset))
        return {"items": [], "fallback_items": [], "has_more": False}

    adapter = KenigEventsLiveSearchAdapter(
        config=enabled_config(),
        emit=lambda *_args: None,
        write=lambda *_args: None,
        measure=lambda *_args: None,
        timing=lambda *_args: None,
        search_call=search_call,
    )
    initialized = adapter.initialize(
        resource_id=RESOURCE_ID,
        actor={"subject": "user", "tenant_id": "kenigevents"},
        model="gemini-3.8-live",
        access_token="token-a",
    )
    session = SimpleNamespace(state=initialized["state"])
    await adapter.execute_tool(session, {"name": "search_events", "args": {"query": "театр"}})
    await adapter.execute_tool(session, {"name": "continue_search", "args": {}})
    assert calls == [("театр", 0), ("театр", 8)]


def test_session_binding_requires_same_authorized_token():
    session = SimpleNamespace(
        actor={"subject": "u1", "tenant_id": "kenigevents"},
        state={"token_hash": _token_hash("token-one")},
    )
    host = SimpleNamespace(sessions={"live_x": session})
    assert _session_actor(host, "live_x", "token-one") == session.actor
    assert _session_actor(host, "live_x", "token-two") is False
    assert _session_actor(host, "missing", "token-one") is None


def test_enabled_register_uses_supplied_host_without_importing_private_controller():
    class Host:
        async def stop_all(self):
            return None

    app = web.Application()
    assert register(
        app,
        env={
            "ENABLE_STATIC_SITE_LIVE_SEARCH": "1",
            "PERSONALIZATION_SUPABASE_URL": "https://example.supabase.co",
            "PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY": "public-key",
        },
        host_override=Host(),
    ) is True
    methods = {(route.method, route.resource.canonical) for route in app.router.routes()}
    assert ("POST", "/api/live-search") in methods
    assert ("GET", "/api/live-search/{session_id}/events") in methods
    assert ("POST", "/api/live-search/{session_id}/stop") in methods
