"""Fail-closed aiohttp transport for the static-site session broker.

The endpoint is intentionally server-to-server only. It has no CORS surface
and never logs request or response material; the broker's own audit sink emits
only keyed hashes.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from aiohttp import web

_BROKER_PATH = Path(__file__).with_name("static-site-auth-session-broker") / "index.py"
_SPEC = importlib.util.spec_from_file_location("static_site_auth_session_broker_runtime", _BROKER_PATH)
if not _SPEC or not _SPEC.loader:  # pragma: no cover - packaging invariant
    raise RuntimeError("static_site_auth_session_broker_unavailable")
broker = importlib.util.module_from_spec(_SPEC)
sys.modules.setdefault(_SPEC.name, broker)
_SPEC.loader.exec_module(broker)

ROUTE = "/internal/e2e/static-site-auth-session"
BROKER_CONCURRENCY = web.AppKey(
    "static_site_auth_session_broker_concurrency", asyncio.Semaphore
)
_SECURITY_HEADERS = {
    "Cache-Control": "no-store",
    "Pragma": "no-cache",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
}


def enabled(env: Mapping[str, str] = os.environ) -> bool:
    return str(env.get("ENABLE_STATIC_SITE_AUTH_SESSION_BROKER") or "").strip() == "1"


def validate_enabled_policy(env: Mapping[str, str] = os.environ) -> None:
    if enabled(env):
        broker.policy_from_env(env)


def _json_response(status: int, payload: Mapping[str, Any]) -> web.Response:
    return web.Response(
        status=status,
        body=json.dumps(dict(payload), separators=(",", ":")).encode(),
        content_type="application/json",
        headers=_SECURITY_HEADERS,
    )


async def handle(request: web.Request) -> web.Response:
    # Reject alternate encodings and query smuggling before reading any body.
    if request.query_string:
        return _json_response(400, {"error": "request_invalid"})
    if request.content_type != "application/json" or request.headers.get("Content-Encoding"):
        return _json_response(415, {"error": "content_type_invalid"})
    authorization = request.headers.get("Authorization", "").strip()
    scheme, separator, token = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer" or not token.strip():
        return _json_response(401, {"error": "unauthorized"})

    declared = request.content_length
    if declared is not None and (declared <= 0 or declared > broker.MAX_BODY_BYTES):
        status = 413 if declared > broker.MAX_BODY_BYTES else 400
        return _json_response(status, {"error": "request_invalid"})
    raw = bytearray()
    async for chunk in request.content.iter_chunked(4096):
        raw.extend(chunk)
        if len(raw) > broker.MAX_BODY_BYTES:
            return _json_response(413, {"error": "request_invalid"})
    if not raw:
        return _json_response(400, {"error": "request_invalid"})
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError):
        return _json_response(400, {"error": "request_invalid"})
    if not isinstance(payload, Mapping):
        return _json_response(400, {"error": "request_invalid"})

    concurrency = request.app[BROKER_CONCURRENCY]
    if concurrency.locked():
        return _json_response(429, {
            "error": "broker_busy",
            "claim": "overload",
            "product_health": "UNKNOWN",
            "execution_status": "BLOCKED",
            "failure_class": "UNKNOWN",
        })
    async with concurrency:
        try:
            result = await asyncio.to_thread(broker.process, payload, token=token.strip())
            return _json_response(200, result)
        except broker.BrokerError as exc:
            return _json_response(exc.status, exc.public_payload())


def register(app: web.Application, env: Mapping[str, str] = os.environ) -> bool:
    if not enabled(env):
        return False
    broker.policy_from_env(env)
    # One independent issuance for each Search health platform may overlap.
    # Any fourth request is rejected without queueing so overload stays an
    # infrastructure UNKNOWN rather than contaminating product health.
    app[BROKER_CONCURRENCY] = asyncio.Semaphore(3)
    app.router.add_post(ROUTE, handle)
    return True
