"""Authenticated Live-search HTTP adapter for the KenigEvents static site.

The static site stays static. This module attaches a small authenticated aiohttp
session API to the existing EventsBot runtime. Gemini transport/audio/lifecycle
belong to live-interaction; Live capacity belongs to ai-resource-control; event
retrieval remains the existing Supabase event-search contract.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Mapping

from aiohttp import ClientSession, ClientTimeout, web

ROUTE_BASE = "/api/live-search"
RESOURCE_ID = "kenigevents-event-search"
DEFAULT_MODEL = "gemini-3.8-live"
MAX_BODY_BYTES = 32 * 1024
DEFAULT_ALLOWED_ORIGINS = (
    "https://kenigevents.ru",
    "https://static.kenigevents.ru",
)
LIVE_SEARCH_HOST = web.AppKey("static_site_live_search_host", object)
LIVE_SEARCH_CONFIG = web.AppKey("static_site_live_search_config", object)


class LiveSearchConfigError(RuntimeError):
    pass


class LiveSearchToolError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class LiveSearchConfig:
    enabled: bool
    supabase_url: str
    publishable_key: str
    allowed_origins: frozenset[str]
    max_sessions: int = 12
    client_liveness_timeout_ms: int = 45_000
    followup_window_ms: int = 15_000
    model: str = DEFAULT_MODEL

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> "LiveSearchConfig":
        enabled = str(env.get("ENABLE_STATIC_SITE_LIVE_SEARCH") or "").strip() == "1"
        if not enabled:
            return cls(False, "", "", frozenset())
        supabase_url = str(env.get("PERSONALIZATION_SUPABASE_URL") or "").strip().rstrip("/")
        publishable_key = str(
            env.get("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY")
            or env.get("STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY")
            or ""
        ).strip()
        raw_origins = str(env.get("STATIC_SITE_LIVE_SEARCH_ALLOWED_ORIGINS") or "").strip()
        origins = frozenset(
            part.strip().rstrip("/")
            for part in (raw_origins.split(",") if raw_origins else DEFAULT_ALLOWED_ORIGINS)
            if part.strip()
        )
        if not supabase_url.startswith("https://"):
            raise LiveSearchConfigError("live_search_supabase_url_missing")
        if not publishable_key:
            raise LiveSearchConfigError("live_search_publishable_key_missing")
        if not origins or any(not value.startswith("https://") for value in origins):
            raise LiveSearchConfigError("live_search_allowed_origins_invalid")
        max_sessions = _bounded_int(env.get("STATIC_SITE_LIVE_SEARCH_MAX_SESSIONS"), 12, 1, 64)
        liveness = _bounded_int(env.get("STATIC_SITE_LIVE_SEARCH_CLIENT_LIVENESS_MS"), 45_000, 15_000, 180_000)
        followup = _bounded_int(env.get("STATIC_SITE_LIVE_SEARCH_FOLLOWUP_MS"), 15_000, 5_000, 60_000)
        model = str(env.get("STATIC_SITE_LIVE_SEARCH_MODEL") or DEFAULT_MODEL).strip()
        if model not in {"gemini-3.8-live", "gemini-3.8-live-extended-thinking"}:
            raise LiveSearchConfigError("live_search_model_invalid")
        return cls(True, supabase_url, publishable_key, origins, max_sessions, liveness, followup, model)


def _bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _authorization(request: web.Request) -> str | None:
    value = str(request.headers.get("Authorization") or "").strip()
    scheme, separator, token = value.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()


def _origin(request: web.Request, config: LiveSearchConfig) -> str | None:
    value = str(request.headers.get("Origin") or "").strip().rstrip("/")
    if not value:
        return None
    return value if value in config.allowed_origins else ""


def _cors_headers(origin: str | None) -> dict[str, str]:
    headers = {
        "Cache-Control": "no-store",
        "Pragma": "no-cache",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "no-referrer",
        "Vary": "Origin",
    }
    if origin:
        headers.update(
            {
                "Access-Control-Allow-Origin": origin,
                "Access-Control-Allow-Headers": "authorization,content-type",
                "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
                "Access-Control-Max-Age": "600",
            }
        )
    return headers


def _json_response(request: web.Request, config: LiveSearchConfig, status: int, payload: Mapping[str, Any]) -> web.Response:
    origin = _origin(request, config)
    if origin == "":
        return web.json_response({"error": "origin_forbidden"}, status=403, headers=_cors_headers(None))
    return web.json_response(dict(payload), status=status, headers=_cors_headers(origin))


async def _read_json(request: web.Request, config: LiveSearchConfig) -> Mapping[str, Any]:
    if request.content_type != "application/json" or request.headers.get("Content-Encoding"):
        raise web.HTTPUnsupportedMediaType()
    declared = request.content_length
    if declared is not None and declared > MAX_BODY_BYTES:
        raise web.HTTPRequestEntityTooLarge(max_size=MAX_BODY_BYTES, actual_size=declared)
    raw = bytearray()
    async for chunk in request.content.iter_chunked(4096):
        raw.extend(chunk)
        if len(raw) > MAX_BODY_BYTES:
            raise web.HTTPRequestEntityTooLarge(max_size=MAX_BODY_BYTES, actual_size=len(raw))
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise web.HTTPBadRequest() from exc
    if not isinstance(payload, Mapping):
        raise web.HTTPBadRequest()
    return payload


async def verify_supabase_user(config: LiveSearchConfig, token: str) -> Mapping[str, Any] | None:
    timeout = ClientTimeout(total=8)
    headers = {"apikey": config.publishable_key, "Authorization": "Bearer " + token, "Accept": "application/json"}
    try:
        async with ClientSession(timeout=timeout) as session:
            async with session.get(config.supabase_url + "/auth/v1/user", headers=headers) as response:
                if response.status != 200:
                    return None
                payload = await response.json(content_type=None)
    except Exception:
        return None
    if not isinstance(payload, Mapping) or not str(payload.get("id") or "").strip():
        return None
    return payload


async def call_event_search(config: LiveSearchConfig, token: str, query: str, offset: int) -> Mapping[str, Any]:
    body = {
        "query": query,
        "limit": 8,
        "offset": offset,
        "candidate_window": 10,
        "include_fallback": True,
        "use_llm_verifier": False,
        "allow_llm_fallback": False,
        "client_request_id": str(uuid.uuid4()),
    }
    headers = {
        "apikey": config.publishable_key,
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    timeout = ClientTimeout(total=35)
    try:
        async with ClientSession(timeout=timeout) as client:
            async with client.post(config.supabase_url + "/functions/v1/event-search", headers=headers, json=body) as response:
                payload = await response.json(content_type=None)
                if response.status != 200 or not isinstance(payload, Mapping) or payload.get("error"):
                    code = str(payload.get("error") if isinstance(payload, Mapping) else "search_failed")
                    raise LiveSearchToolError("SEARCH_TOOL_FAILED", code[:180])
    except LiveSearchToolError:
        raise
    except Exception as exc:
        raise LiveSearchToolError("SEARCH_TOOL_UNAVAILABLE", type(exc).__name__) from exc
    return payload


def _tool_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    items = list(payload.get("items") or [])
    fallback = list(payload.get("fallback_items") or [])
    visible = items or fallback
    summary = []
    for item in visible[:8]:
        if not isinstance(item, Mapping):
            continue
        event = item.get("event") if isinstance(item.get("event"), Mapping) else item
        summary.append(
            {
                "id": event.get("id") or item.get("event_id"),
                "title": str(event.get("title") or item.get("title") or "")[:160],
                "date": event.get("date") or event.get("event_date") or item.get("date"),
                "city": str(event.get("city") or item.get("city") or "")[:80],
            }
        )
    return {
        "count": len(visible),
        "has_more": bool(payload.get("has_more")),
        "results": summary,
        "cards_already_shown_to_user": True,
    }


class KenigEventsLiveSearchAdapter:
    def __init__(
        self,
        *,
        config: LiveSearchConfig,
        emit: Callable[..., Any],
        write: Callable[..., Any],
        measure: Callable[..., Any],
        timing: Callable[..., Any],
        search_call: Callable[[LiveSearchConfig, str, str, int], Awaitable[Mapping[str, Any]]] = call_event_search,
    ):
        self.config = config
        self.emit = emit
        self.write = write
        self.measure = measure
        self.timing = timing
        self.search_call = search_call

    def initialize(self, *, resource_id: str, actor: Any, model: str, access_token: str = "", **_args: Any) -> dict[str, Any]:
        if resource_id != RESOURCE_ID or not isinstance(access_token, str) or not access_token:
            raise LiveSearchToolError("LIVE_AUTH_REQUIRED", "authorized_search_session_required")
        functions = [
            {
                "name": "search_events",
                "description": "Find current KenigEvents cards matching the user's request. Call before naming or recommending exact events.",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {"query": {"type": "STRING", "description": "Natural-language event request in Russian."}},
                    "required": ["query"],
                },
            },
            {
                "name": "continue_search",
                "description": "Show the next page for the last event query when the user asks for more options.",
                "parameters": {"type": "OBJECT", "properties": {}},
            },
        ]
        instruction = (
            "Ты голосовой помощник поиска KenigEvents. Отвечай по-русски кратко и разговорно. "
            "Для любого утверждения о конкретных событиях сначала вызывай search_events; для просьбы показать ещё — continue_search. "
            "Карточки результата интерфейс показывает сам, поэтому не перечисляй длинный каталог голосом. "
            "Не выдумывай даты, цены, места или события и не имитируй веб-поиск. "
            "После карточек помоги уточнить запрос или выбрать из найденного."
        )
        return {
            "state": {
                "access_token": access_token,
                "token_hash": _token_hash(access_token),
                "query": "",
                "offset": 0,
            },
            "context": {
                "product": "KenigEvents",
                "surface": "authorized_event_search",
                "current_date": datetime.now(timezone.utc).date().isoformat(),
            },
            "configuration": {
                "system_instruction": instruction,
                "functions": functions,
                "search_enabled": False,
                "voice": "Aoede",
            },
            "response": {
                "mode": "live",
                "followup_window_ms": self.config.followup_window_ms,
                "compatibility_adapter": "event-search-contract-v2",
            },
        }

    async def execute_tool(self, session: Any, call: Mapping[str, Any]) -> Mapping[str, Any]:
        name = str(call.get("name") or "")
        args = call.get("args") if isinstance(call.get("args"), Mapping) else {}
        if name == "search_events":
            query = re.sub(r"\s+", " ", str(args.get("query") or "")).strip()
            if len(query) < 3 or len(query) > 180:
                raise LiveSearchToolError("INVALID_SEARCH_QUERY", "query_length_invalid")
            session.state["query"] = query
            session.state["offset"] = 0
        elif name == "continue_search":
            query = str(session.state.get("query") or "")
            if not query:
                raise LiveSearchToolError("SEARCH_CONTEXT_MISSING", "search_events_required_first")
            session.state["offset"] = int(session.state.get("offset") or 0) + 8
        else:
            raise LiveSearchToolError("LIVE_TOOL_UNKNOWN", "unknown_live_search_tool")
        offset = int(session.state.get("offset") or 0)
        payload = await self.search_call(self.config, session.state["access_token"], query, offset)
        self.emit(
            session,
            {
                "type": "search_results",
                "query": query,
                "offset": offset,
                "data": payload,
            },
        )
        return _tool_summary(payload)


async def _managed_runner(*, session: Any, reader: Any, on_event: Callable[[dict], None]) -> None:
    from google_ai.live_resources import run_live_search

    await run_live_search(
        authorized_session_id=session.id,
        environment=os.environ,
        reader=reader,
        on_event=on_event,
    )


def build_host(
    config: LiveSearchConfig,
    *,
    managed_runner: Callable[..., Awaitable[None]] = _managed_runner,
    search_call: Callable[[LiveSearchConfig, str, str, int], Awaitable[Mapping[str, Any]]] = call_event_search,
):
    try:
        from live_interaction.session_host import LiveSessionHost
    except ImportError as exc:
        raise LiveSearchConfigError("live_interaction_package_missing") from exc
    return LiveSessionHost(
        adapter_factory=lambda **kwargs: KenigEventsLiveSearchAdapter(
            config=config, search_call=search_call, **kwargs
        ),
        managed_runner=managed_runner,
        models=(config.model,),
        ready_timeout_ms=30_000,
        max_sessions=config.max_sessions,
        client_liveness_timeout_ms=config.client_liveness_timeout_ms,
    )


def _http_status(exc: Exception) -> int:
    code = str(getattr(exc, "code", ""))
    if code in {"FORBIDDEN"}:
        return 403
    if code in {"LIVE_SESSION_NOT_FOUND"}:
        return 404
    if code in {"LIVE_BUSY"}:
        return 429
    if code.startswith("INVALID") or code in {"LIVE_AUTH_REQUIRED"}:
        return 400
    if code.startswith("RESOURCE_") or code.startswith("LIVE_PROVIDER") or code in {"LIVE_UNAVAILABLE"}:
        return 503
    return 500


def _session_actor(host: Any, session_id: str, token: str) -> Any:
    session = host.sessions.get(session_id)
    if session is None:
        return None
    expected = str(session.state.get("token_hash") or "")
    if not expected or not hmac.compare_digest(expected, _token_hash(token)):
        return False
    return session.actor


async def _preflight(request: web.Request) -> web.Response:
    config = request.app[LIVE_SEARCH_CONFIG]
    origin = _origin(request, config)
    if origin == "":
        return _json_response(request, config, 403, {"error": "origin_forbidden"})
    return web.Response(status=204, headers=_cors_headers(origin))


async def _start(request: web.Request) -> web.Response:
    config = request.app[LIVE_SEARCH_CONFIG]
    token = _authorization(request)
    if not token:
        return _json_response(request, config, 401, {"error": "unauthorized"})
    if _origin(request, config) == "":
        return _json_response(request, config, 403, {"error": "origin_forbidden"})
    payload = await _read_json(request, config)
    user = await verify_supabase_user(config, token)
    if not user:
        return _json_response(request, config, 401, {"error": "unauthorized"})
    actor = {"subject": str(user["id"]), "tenant_id": "kenigevents"}
    host = request.app[LIVE_SEARCH_HOST]
    try:
        result = await host.start(
            resource_id=RESOURCE_ID,
            actor=actor,
            model=str(payload.get("model") or config.model),
            access_token=token,
        )
    except Exception as exc:
        return _json_response(
            request,
            config,
            _http_status(exc),
            {"error": str(getattr(exc, "code", "LIVE_START_FAILED"))},
        )
    return _json_response(request, config, 200, result)


async def _session_request(request: web.Request, operation: str) -> web.Response:
    config = request.app[LIVE_SEARCH_CONFIG]
    token = _authorization(request)
    if not token:
        return _json_response(request, config, 401, {"error": "unauthorized"})
    if _origin(request, config) == "":
        return _json_response(request, config, 403, {"error": "origin_forbidden"})
    host = request.app[LIVE_SEARCH_HOST]
    session_id = str(request.match_info.get("session_id") or "")
    actor = _session_actor(host, session_id, token)
    if actor is False:
        return _json_response(request, config, 403, {"error": "forbidden"})
    if actor is None:
        return _json_response(request, config, 404, {"error": "LIVE_SESSION_NOT_FOUND"})
    try:
        if operation == "input":
            payload = await _read_json(request, config)
            result = await host.input(
                session_id=session_id,
                resource_id=RESOURCE_ID,
                actor=actor,
                message=dict(payload),
            )
        elif operation == "events":
            try:
                after = max(0, int(request.query.get("after", "0")))
            except ValueError:
                return _json_response(request, config, 400, {"error": "invalid_after"})
            result = host.events(
                session_id=session_id,
                resource_id=RESOURCE_ID,
                actor=actor,
                after=after,
            )
        elif operation == "stop":
            if request.can_read_body:
                await _read_json(request, config)
            result = await host.stop(
                session_id=session_id,
                resource_id=RESOURCE_ID,
                actor=actor,
            )
        else:
            return _json_response(request, config, 404, {"error": "not_found"})
    except Exception as exc:
        return _json_response(
            request,
            config,
            _http_status(exc),
            {"error": str(getattr(exc, "code", "LIVE_REQUEST_FAILED"))},
        )
    return _json_response(request, config, 200, result)


async def _input(request: web.Request) -> web.Response:
    return await _session_request(request, "input")


async def _events(request: web.Request) -> web.Response:
    return await _session_request(request, "events")


async def _stop(request: web.Request) -> web.Response:
    return await _session_request(request, "stop")


def register(
    app: web.Application,
    env: Mapping[str, str] = os.environ,
    *,
    managed_runner: Callable[..., Awaitable[None]] = _managed_runner,
    search_call: Callable[[LiveSearchConfig, str, str, int], Awaitable[Mapping[str, Any]]] = call_event_search,
    host_override: Any = None,
) -> bool:
    config = LiveSearchConfig.from_env(env)
    if not config.enabled:
        return False
    host = host_override or build_host(config, managed_runner=managed_runner, search_call=search_call)
    app[LIVE_SEARCH_CONFIG] = config
    app[LIVE_SEARCH_HOST] = host
    app.router.add_route("OPTIONS", ROUTE_BASE, _preflight)
    app.router.add_post(ROUTE_BASE, _start)
    app.router.add_route("OPTIONS", ROUTE_BASE + "/{session_id}/input", _preflight)
    app.router.add_post(ROUTE_BASE + "/{session_id}/input", _input)
    app.router.add_route("OPTIONS", ROUTE_BASE + "/{session_id}/events", _preflight)
    app.router.add_get(ROUTE_BASE + "/{session_id}/events", _events)
    app.router.add_route("OPTIONS", ROUTE_BASE + "/{session_id}/stop", _preflight)
    app.router.add_post(ROUTE_BASE + "/{session_id}/stop", _stop)

    async def cleanup(_app: web.Application) -> None:
        await host.stop_all()

    app.on_cleanup.append(cleanup)
    return True
