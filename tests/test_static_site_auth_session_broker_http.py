from __future__ import annotations

import asyncio
import base64
import json
import threading
from pathlib import Path

import pytest
import tomllib
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from serverless import static_site_auth_session_broker_http as http_broker

ROOT = Path(__file__).resolve().parents[1]


def _jwt(role: str = "service_role") -> str:
    enc = lambda value: base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")
    return f"{enc({'alg': 'HS256'})}.{enc({'role': role})}.sig"


def _env(**overrides: str) -> dict[str, str]:
    values = {
        "ENABLE_STATIC_SITE_AUTH_SESSION_BROKER": "1",
        "AUTH_SESSION_BROKER_OIDC_AUDIENCE": "kenigevents-static-search-broker",
        "AUTH_SESSION_BROKER_ALLOWED_REPOSITORIES": "onedayonemasterpiece/events-bot-new",
        "AUTH_SESSION_BROKER_ALLOWED_REFS": "refs/heads/main",
        "AUTH_SESSION_BROKER_ALLOWED_WORKFLOW_REFS": "onedayonemasterpiece/events-bot-new/.github/workflows/static-site-search-canary.yml@refs/heads/main",
        "AUTH_SESSION_BROKER_ALLOWED_ENVIRONMENTS": "search-e2e",
        "AUTH_SESSION_BROKER_ALLOWED_EVENTS": "schedule,workflow_dispatch,repository_dispatch",
        "AUTH_SESSION_BROKER_ALLOWED_RUNS": "github-claim-bound",
        "AUTH_SESSION_BROKER_PERSONAS_JSON": (
            '{"search-cached-browser":"browser@example.invalid",'
            '"search-cold-browser":"cold-browser@example.invalid",'
            '"search-cached-android":"android@example.invalid",'
            '"search-cached-ios":"ios@example.invalid"}'
        ),
        "AUTH_SESSION_BROKER_ALLOWED_REDIRECTS": "https://kenigevents.ru/poisk/\nhttps://kenigevents.ru/_review/{secret-candidate}/poisk/",
        "AUTH_SESSION_BROKER_PER_RUN_PERSONA_LIMIT": "1",
        "AUTH_SESSION_BROKER_AUDIT_HMAC_KEY": "unit-test-audit-key-with-enough-entropy",
        "PERSONALIZATION_SUPABASE_URL": "https://project.supabase.co",
        "AUTH_SESSION_BROKER_SUPABASE_SERVICE_ROLE_KEY": _jwt(),
    }
    values.update(overrides)
    return values


async def _client(monkeypatch, *, result=None, error=None, capacity: int = 3) -> TestClient:
    def process(payload, *, token):
        assert payload == {"ok": True}
        assert token == "oidc"
        if error:
            raise error
        return result or {"email_otp": "123456", "action_link": "https://project.supabase.co/auth/v1/verify?token=secret"}

    monkeypatch.setattr(http_broker.broker, "process", process)
    app = web.Application()
    assert http_broker.register(app, _env()) is True
    app[http_broker.BROKER_CONCURRENCY] = asyncio.Semaphore(capacity)
    client = TestClient(TestServer(app))
    await client.start_server()
    return client


def test_route_is_absent_unless_explicitly_enabled():
    app = web.Application()
    assert http_broker.register(app, {}) is False
    assert not list(app.router.routes())


def test_fly_plaintext_listener_redirects_to_https():
    config = tomllib.loads((ROOT / "fly.toml").read_text(encoding="utf-8"))
    http_port = next(
        port
        for service in config["services"]
        for port in service["ports"]
        if port["port"] == "80"
    )
    assert http_port["handlers"] == ["http"]
    assert http_port["force_https"] is True


@pytest.mark.asyncio
async def test_server_to_server_route_returns_no_store_security_headers(monkeypatch):
    client = await _client(monkeypatch)
    try:
        response = await client.post(
            http_broker.ROUTE,
            json={"ok": True},
            headers={"Authorization": "Bearer oidc"},
        )
        assert response.status == 200
        assert response.headers["Cache-Control"] == "no-store"
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        assert response.headers["Referrer-Policy"] == "no-referrer"
        payload = await response.json()
        assert payload["email_otp"] == "123456"
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(("kwargs", "status"), [
    ({"data": "{}", "headers": {"Content-Type": "text/plain"}}, 415),
    ({"data": "{", "headers": {"Content-Type": "application/json", "Authorization": "Bearer oidc"}}, 400),
    ({"data": "{}", "headers": {"Content-Type": "application/json"}}, 401),
    ({"data": "{}", "headers": {"Content-Type": "application/json", "Authorization": "Bearer oidc", "Content-Encoding": "gzip"}}, 415),
])
async def test_route_rejects_unsafe_request_shapes(monkeypatch, kwargs, status):
    client = await _client(monkeypatch)
    try:
        response = await client.post(http_broker.ROUTE, **kwargs)
        assert response.status == status
        assert response.headers["Cache-Control"] == "no-store"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_route_rejects_oversized_body_and_non_post_methods(monkeypatch):
    client = await _client(monkeypatch)
    headers = {"Content-Type": "application/json", "Authorization": "Bearer oidc"}
    try:
        response = await client.post(http_broker.ROUTE, data=b"x" * 16385, headers=headers)
        assert response.status == 413
        assert (await client.get(http_broker.ROUTE)).status == 405
        assert (await client.options(http_broker.ROUTE)).status == 405
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_route_rejects_when_process_local_broker_capacity_is_full(monkeypatch):
    client = await _client(monkeypatch, capacity=0)
    try:
        response = await client.post(
            http_broker.ROUTE,
            json={"ok": True},
            headers={"Authorization": "Bearer oidc"},
        )
        assert response.status == 429
        assert await response.json() == {
            "error": "broker_busy",
            "claim": "overload",
            "product_health": "UNKNOWN",
            "execution_status": "BLOCKED",
            "failure_class": "UNKNOWN",
        }
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_http_admits_three_platforms_and_rejects_fourth_without_queueing(monkeypatch):
    started = 0
    lock = threading.Lock()
    all_started = threading.Event()
    release = threading.Event()

    def process(payload, *, token):
        nonlocal started
        assert payload["platform"] in {"browser", "android", "ios"}
        assert token == "oidc"
        with lock:
            started += 1
            if started == 3:
                all_started.set()
        assert release.wait(timeout=3)
        return {"platform": payload["platform"], "email_otp": "123456"}

    monkeypatch.setattr(http_broker.broker, "process", process)
    app = web.Application()
    assert http_broker.register(app, _env()) is True
    client = TestClient(TestServer(app))
    await client.start_server()
    headers = {"Authorization": "Bearer oidc"}
    try:
        active = [
            asyncio.create_task(client.post(
                http_broker.ROUTE,
                json={"purpose": "production_health", "platform": platform, "redirect_to": "https://kenigevents.ru/poisk/"},
                headers=headers,
            ))
            for platform in ("browser", "android", "ios")
        ]
        assert await asyncio.to_thread(all_started.wait, 3)
        fourth = await client.post(
            http_broker.ROUTE,
            json={"purpose": "production_health", "platform": "browser", "redirect_to": "https://kenigevents.ru/poisk/"},
            headers=headers,
        )
        assert fourth.status == 429
        assert (await fourth.json())["failure_class"] == "UNKNOWN"
        release.set()
        responses = await asyncio.gather(*active)
        assert [response.status for response in responses] == [200, 200, 200]
    finally:
        release.set()
        await client.close()


@pytest.mark.asyncio
async def test_broker_work_runs_off_event_loop_and_errors_do_not_echo_secrets(monkeypatch):
    event_loop_thread = threading.get_ident()
    worker_thread = None

    def process(_payload, *, token):
        nonlocal worker_thread
        worker_thread = threading.get_ident()
        raise http_broker.broker.BrokerError("supabase_request_rejected", status=503)

    monkeypatch.setattr(http_broker.broker, "process", process)
    app = web.Application()
    http_broker.register(app, _env())
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.post(
            http_broker.ROUTE,
            json={"ok": True},
            headers={"Authorization": "Bearer oidc-secret"},
        )
        assert response.status == 503
        raw = await response.text()
        assert raw == '{"error":"supabase_request_rejected"}'
        assert "oidc-secret" not in raw
        assert "example.invalid" not in raw
        assert worker_thread is not None and worker_thread != event_loop_thread
    finally:
        await client.close()
