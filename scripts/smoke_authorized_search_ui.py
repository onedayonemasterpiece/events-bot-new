#!/usr/bin/env python3
"""Browser smoke for the authorized event-search UI.

This is intentionally a mocked-browser gate, not a replacement for the final
Yandex OAuth E2E. It proves that a static preview built with public Supabase
envs can:

1. restore an authenticated Supabase session in the browser;
2. render the one-line search form;
3. call the `event-search` Edge Function endpoint;
4. render returned rows through the shared feed-card renderer with like/share/
   not-interested/calendar actions and served-list investigation metadata.

The script does not print secrets and does not call real Supabase services.
"""
from __future__ import annotations

import argparse
import base64
import contextlib
import json
import mimetypes
import os
import socket
import sys
import threading
import time
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlencode


def latest_preview_dist(site_dist: Path) -> Path:
    candidates = sorted(
        [path for path in site_dist.glob("preview-*") if path.is_dir()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise RuntimeError(f"No preview-* build found under {site_dist}")
    return candidates[0]


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.contextmanager
def static_server(root: Path):
    port = free_port()
    handler = partial(SimpleHTTPRequestHandler, directory=str(root))
    server = ThreadingHTTPServer(("127.0.0.1", port), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def fake_jwt(user_id: str) -> str:
    def enc(payload: dict[str, Any]) -> str:
        raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    now = int(time.time())
    header = {"alg": "none", "typ": "JWT"}
    body = {
        "aud": "authenticated",
        "exp": now + 3600,
        "iat": now,
        "sub": user_id,
        "email": "ui-smoke@example.invalid",
        "role": "authenticated",
    }
    return f"{enc(header)}.{enc(body)}.c2ln"


def fake_user() -> dict[str, Any]:
    return {
        "id": "11111111-2222-4333-8444-555555555555",
        "aud": "authenticated",
        "role": "authenticated",
        "email": "ui-smoke@example.invalid",
        "app_metadata": {
            "provider": "custom:yandex",
            "providers": ["custom:yandex"],
        },
        "user_metadata": {"purpose": "authorized_search_ui_smoke"},
        "created_at": "2026-06-29T00:00:00.000Z",
    }


def implicit_auth_hash() -> str:
    user_id = fake_user()["id"]
    return urlencode(
        {
            "access_token": fake_jwt(str(user_id)),
            "expires_in": "86400",
            "refresh_token": "ui-smoke-refresh-token",
            "token_type": "bearer",
            "type": "signup",
        }
    )


def fake_search_response() -> dict[str, Any]:
    item = {
        "event_id": 6310,
        "id": 6310,
        "title": "Архитектурно-урбанистическая студия",
        "category": "лекция",
        "tags": ["урбанистика", "город", "регистрация"],
        "base_similarity": 0.9255,
        "semantic_score": 0.9255,
        "display": {
            "id": 6310,
            "event_id": 6310,
            "title": "Архитектурно-урбанистическая студия",
            "href": "/sobytiya/arhitekturno-urbanisticheskaya-studiya-zanyatie-3-formiruem-kontseptsii-i-kaliningrad-6310/",
            "absolute_url": "https://kenigevents.ru/sobytiya/arhitekturno-urbanisticheskaya-studiya-zanyatie-3-formiruem-kontseptsii-i-kaliningrad-6310/",
            "event_type": "лекция",
            "display_date": "2 июля",
            "display_time": "18:30",
            "display_date_time": "2 июля · 18:30",
            "city": "Калининград",
            "venue_name": "Музей",
            "place": "Калининград · Музей",
            "status_label": "Бесплатно · регистрация",
            "likes_count": 7,
            "shares_count": 2,
            "calendar_href": "/sobytiya/arhitekturno-urbanisticheskaya-studiya-zanyatie-3-formiruem-kontseptsii-i-kaliningrad-6310/event.ics",
            "calendar_eligible": True,
        },
    }
    fallback = {
        "event_id": 5878,
        "id": 5878,
        "title": "Песни СССР",
        "category": "концерт",
        "semantic_score": 0,
        "display": {
            "id": 5878,
            "event_id": 5878,
            "title": "Песни СССР",
            "href": "/sobytiya/pesni-sssr-svetlogorsk-5878/",
            "absolute_url": "https://kenigevents.ru/sobytiya/pesni-sssr-svetlogorsk-5878/",
            "event_type": "концерт",
            "display_date": "5 июля",
            "display_time": "19:00",
            "display_date_time": "5 июля · 19:00",
            "city": "Светлогорск",
            "place": "Светлогорск",
            "status_label": "По билетам",
            "likes_count": 12,
            "shares_count": 1,
        },
    }
    return {
        "schema_version": "event-search-results-v1",
        "surface": "authorized_event_search",
        "algorithm_id": "pgvector_gemini_embedding_2_llm_verify_v1",
        "request_id": "00000000-0000-4000-8000-000000000001",
        "served_list_id": "00000000-0000-4000-8000-000000000002",
        "served_list_hash": "ui-smoke-served-list-hash",
        "query_hash": "ui-smoke-query-hash",
        "query_facets": {
            "weekday_iso": 4,
            "weekday_ru": "четверг",
            "time_of_day": "evening",
            "admission": "registration_required",
        },
        "quota": {
            "day_remaining": 4,
            "month_remaining": 29,
            "llm_day_remaining": 1,
            "llm_month_remaining": 9,
        },
        "items": [item],
        "fallback_items": [fallback],
        "has_more": False,
        "llm_verifier": {"requested": True, "used": True, "status": "ok"},
        "timings_ms": {"total_ms": 42},
    }


async def run_smoke(args: argparse.Namespace) -> int:
    try:
        from playwright.async_api import async_playwright, expect
    except ModuleNotFoundError as exc:  # pragma: no cover - operator guidance
        raise RuntimeError(
            "Python Playwright is not installed. Install in an artifact venv, "
            "for example: python -m pip install playwright && python -m playwright install chromium"
        ) from exc

    dist = Path(args.dist).resolve() if args.dist else latest_preview_dist(Path("site/dist"))
    supabase_url = args.supabase_url.rstrip("/")
    calls: list[dict[str, Any]] = []

    server_root = dist.parent if dist.name.startswith("preview-") else dist
    preview_path = f"/{dist.name}/poisk/" if dist.name.startswith("preview-") else "/poisk/"
    with static_server(server_root) as base_url:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(viewport={"width": 390, "height": 844})

            async def route_static_cdn_assets(route):
                request = route.request
                url = request.url
                marker = f"/{dist.name}/"
                if marker not in url:
                    await route.fulfill(status=404, body="unexpected static CDN asset path")
                    return
                rel = url.split(marker, 1)[1].split("?", 1)[0]
                path = dist / rel
                if not path.is_file():
                    await route.fulfill(status=404, body=f"missing local asset: {rel}")
                    return
                content_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
                await route.fulfill(status=200, content_type=content_type, body=path.read_bytes())

            if dist.name.startswith("preview-"):
                await page.route(f"https://static.kenigevents.ru/{dist.name}/**", route_static_cdn_assets)

            async def route_supabase(route):
                request = route.request
                url = request.url
                if url.endswith("/auth/v1/user"):
                    await route.fulfill(
                        status=200,
                        content_type="application/json; charset=utf-8",
                        body=json.dumps(fake_user(), ensure_ascii=False),
                    )
                    return
                if url.endswith("/rest/v1/rpc/get_event_search_quota_v1"):
                    await route.fulfill(
                        status=200,
                        content_type="application/json; charset=utf-8",
                        body=json.dumps([
                            {"day_remaining": 5, "month_remaining": 30}
                        ]),
                    )
                    return
                if url.endswith("/functions/v1/event-search"):
                    try:
                        calls.append(json.loads(request.post_data or "{}"))
                    except json.JSONDecodeError:
                        calls.append({"bad_json": request.post_data})
                    await route.fulfill(
                        status=200,
                        content_type="application/json; charset=utf-8",
                        body=json.dumps(fake_search_response(), ensure_ascii=False),
                    )
                    return
                await route.fulfill(status=404, body="unexpected supabase call")

            await page.route(f"{supabase_url}/**", route_supabase)
            await page.goto(f"{base_url}{preview_path}", wait_until="networkidle")

            root = page.locator("[data-authorized-search]").first
            await expect(root).to_be_visible()
            await expect(page.locator("[data-search-login]").first).to_be_visible()
            await expect(page.locator("[data-search-logout]").first).to_be_hidden()
            await expect(page.locator("[data-search-form]").first).to_be_hidden()
            await expect(page.locator("[data-search-results]").first).to_be_hidden()
            await expect(page.locator("[data-search-more]").first).to_be_hidden()
            if await page.locator("[data-event-card]").count() != 0:
                raise AssertionError("dedicated search page must not show prefilled event-result cards before a query")

            await page.goto(f"{base_url}{preview_path}?auth-smoke=1#{implicit_auth_hash()}", wait_until="networkidle")
            root = page.locator("[data-authorized-search]").first
            await expect(root).to_be_visible()
            await page.wait_for_function(
                "() => document.querySelector('[data-authorized-search]')?.classList.contains('is-authorized')",
                timeout=5000,
            )
            await expect(page.locator("[data-search-login]").first).to_be_hidden()
            await expect(page.locator("[data-search-logout]").first).to_be_visible()
            await expect(page.locator("[data-search-form]").first).to_be_visible()

            await page.locator("[data-search-input]").first.fill("урбанистика в четверг вечером по регистрации")
            await page.locator("[data-search-form]").first.evaluate("(form) => form.requestSubmit()")

            results = page.locator("[data-search-results]").first
            await expect(results).to_be_visible()
            await expect(results).to_have_attribute("data-surface", "authorized_event_search")
            await expect(results).to_have_attribute("data-request-id", "00000000-0000-4000-8000-000000000001")
            await expect(results).to_have_attribute("data-served-list-id", "00000000-0000-4000-8000-000000000002")
            await expect(results).to_have_attribute("data-served-list-hash", "ui-smoke-served-list-hash")
            await expect(results).to_have_attribute("data-effective-algorithm-id", "pgvector_gemini_embedding_2_llm_verify_v1")

            first_card = page.locator("[data-search-results] [data-event-card]").first
            await expect(first_card).to_be_visible()
            await expect(first_card).to_have_attribute("data-event-id", "6310")
            await expect(first_card).to_have_attribute("data-feed-card-variant", "split-actions")
            await expect(first_card).to_have_attribute("data-rank", "0")
            await expect(first_card.locator("[data-feedback-action='like']")).to_be_visible()
            await expect(first_card.locator("[data-native-share]")).to_be_visible()
            await expect(first_card.locator("[data-feedback-action='not_interested']")).to_be_visible()
            await expect(first_card.locator(".feedback-button--calendar")).to_be_visible()
            await expect(page.get_by_text("Возможно, вам будет интересно")).to_be_visible()

            if not calls:
                raise AssertionError("event-search function was not called")
            body = calls[0]
            if body.get("query") != "урбанистика в четверг вечером по регистрации":
                raise AssertionError(f"unexpected query payload: {body}")
            if body.get("use_llm_verifier") is not True:
                raise AssertionError(f"LLM verifier flag was not requested: {body}")

            await browser.close()
    print(
        "authorized_search_ui_smoke=ok "
        f"dist={dist.name} cards=2 first_event=6310 request_calls={len(calls)}"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist", help="Path to site/dist/<preview-id>. Defaults to latest preview-* build.")
    parser.add_argument("--supabase-url", default="https://example.supabase.co")
    args = parser.parse_args()
    import asyncio

    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())
