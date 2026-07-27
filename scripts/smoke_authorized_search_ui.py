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
import asyncio
import base64
import contextlib
import hashlib
import json
import mimetypes
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


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
        "user_metadata": {"purpose": "authorized_search_ui_smoke", "preferred_username": "ui-smoke-user"},
        "created_at": "2026-06-29T00:00:00.000Z",
    }


def storage_key_for_supabase_url(supabase_url: str) -> str:
    host = urlparse(supabase_url).netloc
    project_ref = host.split(".", 1)[0]
    return f"sb-{project_ref}-auth-token"


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
            "image_url": "https://static.kenigevents.ru/p/dh16/21/2111009450924c4948058765c7664636c636ccb3489bce9b46331e634e630c77.webp",
            "image_alt": "Фотография события",
            "image_text_mode": "visual_only",
            "image_media_role": "unknown_document",
            "image_width": 800,
            "image_height": 534,
            "focal_y": 0.5,
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
        "next_offset": 12,
        "retrieved_count": 12,
        "llm_verifier": {"requested": True, "used": True, "status": "ok"},
        "timings_ms": {"total_ms": 42},
    }


def http_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        url,
        data=payload,
        method=method,
        headers=headers or {},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode())


def ensure_real_edge_session(args: argparse.Namespace) -> dict[str, Any]:
    """Create/reuse a dedicated smoke user and return a real Supabase session.

    This is intentionally opt-in (`--real-edge`) because it calls the live
    personalization Supabase Auth and the live `event-search` Edge Function.
    Secrets are read from env/args but never printed.
    """

    supabase_url = args.supabase_url.rstrip("/")
    publishable_key = args.supabase_publishable_key or os.getenv(
        "PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY",
        "",
    )
    secret_key = args.supabase_secret_key or os.getenv(
        "PERSONALIZATION_SUPABASE_SECRET_KEY",
        "",
    )
    if not publishable_key or not secret_key:
        raise RuntimeError(
            "--real-edge requires PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY "
            "and PERSONALIZATION_SUPABASE_SECRET_KEY (or explicit args).",
        )

    email = args.real_edge_email or f"authorized-search-smoke-{int(time.time())}@example.invalid"
    password = args.real_edge_password or (
        "AuthSearchSmoke!" + hashlib.sha256(secret_key.encode()).hexdigest()[:24]
    )
    admin_headers = {
        "apikey": secret_key,
        "Authorization": f"Bearer {secret_key}",
        "Content-Type": "application/json",
    }
    try:
        http_json(
            f"{supabase_url}/auth/v1/admin/users",
            method="POST",
            headers=admin_headers,
            body={
                "email": email,
                "password": password,
                "email_confirm": True,
                "user_metadata": {"preferred_username": "auth-search-smoke"},
            },
        )
    except urllib.error.HTTPError as error:
        detail = error.read().decode(errors="replace")
        if error.code not in {400, 422, 409} or not re_search_duplicate_user(detail):
            raise RuntimeError(f"smoke user creation failed with HTTP {error.code}") from error

    return http_json(
        f"{supabase_url}/auth/v1/token?grant_type=password",
        method="POST",
        headers={"apikey": publishable_key, "Content-Type": "application/json"},
        body={"email": email, "password": password},
    )


def re_search_duplicate_user(detail: str) -> bool:
    lowered = detail.lower()
    return any(token in lowered for token in ["already", "registered", "exists", "duplicate"])


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

            await page.add_init_script(
                """
                (() => {
                  window.__searchProgressAudit = [];
                  const nativeSetAttribute = Element.prototype.setAttribute;
                  const nativeRemoveAttribute = Element.prototype.removeAttribute;
                  Element.prototype.setAttribute = function(name, value) {
                    if (name === 'aria-valuenow' && this.matches?.('[data-search-progress]')) {
                      window.__searchProgressAudit.push(Number(value));
                    }
                    return nativeSetAttribute.call(this, name, value);
                  };
                  Element.prototype.removeAttribute = function(name) {
                    if (name === 'aria-valuenow' && this.matches?.('[data-search-progress]')) {
                      window.__searchProgressAudit.push(null);
                    }
                    return nativeRemoveAttribute.call(this, name);
                  };
                })();
                """
            )

            async def route_supabase(route):
                request = route.request
                url = request.url
                if "/auth/v1/token?grant_type=pkce" in url:
                    await route.fulfill(
                        status=200,
                        content_type="application/json; charset=utf-8",
                        body=json.dumps({
                            "access_token": fake_jwt(fake_user()["id"]),
                            "refresh_token": "ui-smoke-refresh-token",
                            "expires_in": 86400,
                            "expires_at": int(time.time()) + 86400,
                            "token_type": "bearer",
                            "user": fake_user(),
                        }, ensure_ascii=False),
                    )
                    return
                if url.endswith("/auth/v1/user"):
                    await route.fulfill(
                        status=200,
                        content_type="application/json; charset=utf-8",
                        body=json.dumps(fake_user(), ensure_ascii=False),
                    )
                    return
                if url.endswith("/rest/v1/rpc/get_event_search_quota_v1") or url.endswith("/rest/v1/rpc/get_event_search_quota_v2"):
                    await route.fulfill(
                        status=200,
                        content_type="application/json; charset=utf-8",
                        body=json.dumps([
                            {"day_remaining": 5, "month_remaining": 30}
                        ]),
                    )
                    return
                if url.endswith("/functions/v1/event-search"):
                    # Keep the loading state observable even after the pre-auth
                    # callback smoke warmed the client/session caches.
                    await asyncio.sleep(0.6)
                    try:
                        calls.append(json.loads(request.post_data or "{}"))
                    except json.JSONDecodeError:
                        calls.append({"bad_json": request.post_data})
                    accept = request.headers.get("accept", "")
                    if "application/x-ndjson" not in accept:
                        await route.fulfill(
                            status=200,
                            content_type="application/json; charset=utf-8",
                            body=json.dumps(fake_search_response(), ensure_ascii=False),
                        )
                        return
                    call_number = len(calls)
                    if call_number == 2:
                        # Keep the second request alive beyond the first
                        # request's 650ms completion-reset window.
                        await asyncio.sleep(1.0)
                    if call_number == 3:
                        progress = [
                            {"type": "progress", "request_id": "00000000-0000-4000-8000-000000000003", "stage": "accepted", "progress": 2, "label": "Запрос принят"},
                            {"type": "progress", "request_id": "00000000-0000-4000-8000-000000000003", "stage": "embedding", "progress": 28, "label": "Понимаю смысл запроса"},
                            {"type": "error", "request_id": "00000000-0000-4000-8000-000000000003", "status": 503, "error": "provider_unavailable", "message": "Поиск временно недоступен"},
                        ]
                    else:
                        progress = [
                            {"type": "progress", "request_id": "00000000-0000-4000-8000-000000000001", "stage": "accepted", "progress": 2, "label": "Запрос принят"},
                            {"type": "progress", "request_id": "00000000-0000-4000-8000-000000000001", "stage": "vector_search", "progress": 55, "label": "Ищу события"},
                            # Deliberately late/lower frames exercise both the
                            # stage-rank and numeric monotonic guards.
                            {"type": "progress", "request_id": "00000000-0000-4000-8000-000000000001", "stage": "validate", "progress": 10, "label": "Проверяю запрос"},
                            {"type": "progress", "request_id": "00000000-0000-4000-8000-000000000001", "stage": "llm_verify", "progress": 72, "label": "Проверяю релевантность"},
                            {"type": "progress", "request_id": "00000000-0000-4000-8000-000000000001", "stage": "vector_results", "progress": 62, "label": "Варианты найдены"},
                            {"type": "progress", "request_id": "00000000-0000-4000-8000-000000000001", "stage": "finalize", "progress": 96, "label": "Собираю результат"},
                            {"type": "result", "request_id": "00000000-0000-4000-8000-000000000001", "progress": 100, "label": "Готово", "data": fake_search_response()},
                        ]
                    await route.fulfill(
                        status=200,
                        content_type="application/x-ndjson; charset=utf-8",
                        body="\n".join(json.dumps(item, ensure_ascii=False) for item in progress) + "\n",
                    )
                    return
                await route.fulfill(status=404, body="unexpected supabase call")

            await page.route(f"{supabase_url}/**", route_supabase)
            await page.goto(f"{base_url}{preview_path}", wait_until="networkidle")

            root = page.locator("[data-authorized-search]").first
            await expect(root).to_be_visible()
            await expect(page.locator("[data-search-login]").first).to_be_visible()
            await expect(page.locator("[data-search-logout]").first).to_be_hidden()
            await expect(page.locator("[data-search-form]").first).to_be_visible()
            await expect(page.locator("[data-search-input]").first).to_be_editable()
            await expect(page.locator("[data-search-input]").first).to_have_attribute("enterkeyhint", "search")
            await expect(page.locator("[data-search-results]").first).to_be_hidden()
            await expect(page.locator("[data-search-more]").first).to_be_hidden()
            if await page.locator("[data-event-card]").count() != 0:
                raise AssertionError("dedicated search page must not show prefilled event-result cards before a query")

            await page.goto(f"{base_url}{preview_path}?auth-smoke=1&code=missing-verifier-code", wait_until="networkidle")
            await expect(page.locator("[data-search-login]").first).to_be_visible()
            await expect(page.locator("[data-search-form]").first).to_be_visible()
            await expect(page.locator("[data-search-input]").first).to_be_editable()
            await expect(page.locator("[data-search-status]").first).to_contain_text("сессия входа устарела")
            if "code=" in page.url:
                raise AssertionError("failed PKCE callback must remove stale code from the visible URL")

            # A signed-out visitor can type first. Submit stores the validated
            # draft, starts Yandex PKCE, and the successful callback below must
            # restore and execute that exact query without another click.
            pending_query = "урбанистика в четверг вечером по регистрации"
            await page.locator("[data-search-input]").first.fill(pending_query)
            await page.locator("[data-search-form]").first.evaluate("(form) => form.requestSubmit()")
            await page.wait_for_url(f"{supabase_url}/auth/v1/authorize**", timeout=5000)
            await page.goto(f"{base_url}{preview_path}?auth-smoke=1&code=ui-smoke-auth-code", wait_until="networkidle")
            root = page.locator("[data-authorized-search]").first
            await expect(root).to_be_visible()
            await page.wait_for_function(
                "() => document.querySelector('[data-authorized-search]')?.classList.contains('is-authorized')",
                timeout=5000,
            )
            await expect(page.locator("[data-search-login]").first).to_be_hidden()
            await expect(page.locator("[data-search-user]").first).to_be_visible()
            await expect(page.locator("[data-search-user-name]").first).to_contain_text("ui-smoke-user")
            await expect(page.locator("[data-search-logout]").first).to_be_hidden()
            await page.locator("[data-search-account-toggle]").first.click()
            await expect(page.locator("[data-search-logout]").first).to_be_visible()
            await page.keyboard.press("Escape")
            await expect(page.locator("[data-search-logout]").first).to_be_hidden()
            await expect(page.locator("[data-search-form]").first).to_be_visible()
            await expect(page.locator("[data-search-results]").first).to_be_visible(timeout=5000)
            await expect(page.locator("[data-search-results] [data-event-card]").first).to_have_attribute("data-event-id", "6310")
            if not calls or calls[0].get("query") != pending_query:
                raise AssertionError(f"pending pre-auth query did not run after callback: {calls}")
            pending_draft = await page.evaluate("() => localStorage.getItem('ke_authorized_search_draft_v1')")
            if pending_draft is not None:
                raise AssertionError("pending pre-auth query must be consumed after callback")
            calls.clear()

            await page.goto(f"{base_url}{preview_path}?new-link-smoke=1", wait_until="networkidle")
            await page.wait_for_function(
                "() => document.querySelector('[data-authorized-search]')?.classList.contains('is-authorized')",
                timeout=5000,
            )
            await expect(page.locator("[data-search-login]").first).to_be_hidden()
            await expect(page.locator("[data-search-user]").first).to_be_visible()
            await expect(page.locator("[data-search-user-name]").first).to_contain_text("ui-smoke-user")
            await expect(page.locator("[data-search-form]").first).to_be_visible()

            await page.locator("[data-search-input]").first.fill("<script>alert(1)</script>")
            await page.locator("[data-search-form]").first.evaluate("(form) => form.requestSubmit()")
            await expect(page.locator("[data-search-status]").first).to_contain_text("техническую команду")
            if calls:
                raise AssertionError(f"unsafe query must not call event-search: {calls}")

            await page.locator("[data-search-input]").first.fill("урбанистика в четверг вечером по регистрации")
            await page.locator("[data-search-input]").first.press("Enter")
            if "\n" in await page.locator("[data-search-input]").first.input_value():
                raise AssertionError("Enter must submit Search instead of inserting a textarea newline")
            await expect(page.locator("[data-search-submit]").first).to_have_attribute("aria-busy", "true")
            await expect(page.locator("[data-search-skeletons]").first).to_be_visible()
            await expect(page.locator("[data-search-skeletons] .authorized-search__skeleton-media").first).to_be_visible()
            if args.screenshot_dir:
                screenshot_dir = Path(args.screenshot_dir)
                screenshot_dir.mkdir(parents=True, exist_ok=True)
                await page.screenshot(path=str(screenshot_dir / "search-loading-390x844.png"), full_page=False)
            if await page.locator("[data-search-results] [data-event-card]").count() != 0:
                raise AssertionError("provisional vector phase must keep the large-card skeleton, not render unstable cards")
            button_progress = await page.locator("[data-search-submit]").first.evaluate(
                "button => getComputedStyle(button).getPropertyValue('--search-progress').trim()"
            )
            if button_progress in {"", "0%"}:
                raise AssertionError(f"search progress must paint inside the submit button: {button_progress!r}")
            button_progress_paint = await page.locator("[data-search-submit]").first.evaluate(
                """button => {
                  const base = getComputedStyle(button);
                  const fill = getComputedStyle(button, '::before');
                  return {
                    base: base.backgroundColor,
                    fill: fill.backgroundColor,
                    fillWidth: parseFloat(fill.width),
                    buttonWidth: button.getBoundingClientRect().width,
                  };
                }"""
            )
            if button_progress_paint["fill"] != "rgb(152, 64, 31)":
                raise AssertionError(f"mobile Search progress fill must use the accepted visible accent: {button_progress_paint}")
            if button_progress_paint["fillWidth"] < 4 or button_progress_paint["fillWidth"] >= button_progress_paint["buttonWidth"]:
                raise AssertionError(f"Search button must expose an in-progress partial fill: {button_progress_paint}")

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
            await expect(first_card).to_have_attribute("data-card-media-presentation", "flow")
            await expect(first_card).to_have_attribute("data-card-media-treatment", "visual-cover")
            first_image = first_card.locator("[data-card-image]")
            await expect(first_image).to_have_attribute("data-card-authoritative-fit", "cover")
            if await first_image.evaluate("image => getComputedStyle(image).objectFit") != "cover":
                raise AssertionError("runtime Search photo must use the donor cover treatment")
            if await first_card.evaluate("card => Boolean(card.style.gridRow || card.style.gridColumn)"):
                raise AssertionError("runtime Search flow card must not inherit related-grid placement")
            await expect(first_card.locator("[data-feedback-action='like']")).to_be_visible()
            await expect(first_card.locator("[data-native-share]")).to_be_visible()
            await expect(first_card.locator("[data-feedback-action='not_interested']")).to_be_visible()
            await expect(first_card.locator(".feedback-button--calendar")).to_be_visible()
            await expect(page.get_by_text("Результаты поиска", exact=True)).to_be_visible()
            await expect(page.get_by_text("Нашли то, что искали?", exact=True)).to_be_visible()
            await expect(page.get_by_role("button", name="Да, нашёл")).to_be_visible()
            await expect(page.get_by_role("button", name="Нет, не нашёл")).to_be_visible()
            await expect(page.get_by_text("Ещё можно посмотреть", exact=True)).to_be_visible()
            await expect(page.locator("[data-search-results] [data-event-card][data-event-id='5878']")).to_be_visible()
            await expect(page.locator("[data-search-more]").first).to_be_hidden()
            await expect(page.locator("[data-search-skeletons]").first).to_be_hidden()
            await expect(page.locator("[data-search-submit]").first).to_have_attribute("aria-busy", "false")
            await expect(page.locator("[data-search-submit-label]").first).to_contain_text("Искать")
            if args.screenshot_dir:
                await page.screenshot(path=str(Path(args.screenshot_dir) / "search-results-390x844.png"), full_page=True)

            cards = page.locator("[data-search-results] [data-event-card]")
            card_count = await cards.count()
            last_card = cards.nth(card_count - 1)
            await last_card.scroll_into_view_if_needed()
            await expect(last_card).to_be_visible()
            scrolled_event_id = await last_card.get_attribute("data-event-id")
            scroll_y = await page.evaluate("() => window.scrollY")
            if scroll_y <= 0:
                raise AssertionError("result feed did not scroll to rendered cards")

            if not calls:
                raise AssertionError("event-search function was not called")
            body = calls[0]
            if body.get("query") != "урбанистика в четверг вечером по регистрации":
                raise AssertionError(f"unexpected query payload: {body}")
            if body.get("use_llm_verifier") is not True:
                raise AssertionError(f"user-facing search must request bounded LLM verifier: {body}")

            # Start another request before the first run's owned 650ms reset
            # can fire. It must remain busy until its delayed response arrives.
            await page.locator("[data-search-input]").first.fill("джаз на выходных")
            await page.locator("[data-search-form]").first.evaluate("(form) => form.requestSubmit()")
            await expect(page.locator("[data-search-submit]").first).to_have_attribute("aria-busy", "true")
            await page.wait_for_timeout(750)
            await expect(page.locator("[data-search-submit]").first).to_have_attribute("aria-busy", "true")
            await expect(page.locator("[data-search-submit-label]").first).to_contain_text("Ищу")
            await expect(page.locator("[data-search-results]").first).to_be_visible(timeout=5000)
            await expect(page.locator("[data-search-submit]").first).to_have_attribute("aria-busy", "false")

            # A terminal stream error must return the control to an explicit
            # retryable state; the next submit must recover normally.
            await page.locator("[data-search-input]").first.fill("событие с временной ошибкой")
            await page.locator("[data-search-form]").first.evaluate("(form) => form.requestSubmit()")
            await expect(page.locator("[data-search-status][role='alert']").first).to_be_visible(timeout=5000)
            await expect(page.locator("[data-search-submit]").first).to_have_attribute("aria-busy", "false")
            await expect(page.locator("[data-search-submit-label]").first).to_contain_text("Искать")

            await page.locator("[data-search-input]").first.fill("повторный поиск после ошибки")
            await page.locator("[data-search-form]").first.evaluate("(form) => form.requestSubmit()")
            await expect(page.locator("[data-search-results]").first).to_be_visible(timeout=5000)
            await expect(page.locator("[data-search-submit]").first).to_have_attribute("aria-busy", "false")

            progress_audit = await page.evaluate("() => window.__searchProgressAudit || []")
            runs: list[list[int]] = []
            current_run: list[int] = []
            for value in progress_audit:
                if value is None:
                    if current_run:
                        runs.append(current_run)
                        current_run = []
                else:
                    current_run.append(int(value))
            if current_run:
                runs.append(current_run)
            if len(runs) < 4:
                raise AssertionError(f"expected progress evidence for success/race/error/retry runs: {runs}")
            for run in runs:
                if any(right < left for left, right in zip(run, run[1:])):
                    raise AssertionError(f"search progress moved backward: {runs}")

            await browser.close()
    print(
        "authorized_search_ui_smoke=ok "
        f"dist={dist.name} cards={card_count} first_event=6310 "
        f"scrolled_event={scrolled_event_id} scroll_y={scroll_y} request_calls={len(calls)} "
        f"progress_runs={runs}"
    )
    return 0


async def run_real_edge_smoke(args: argparse.Namespace) -> int:
    """Browser smoke with fake Yandex callback but real Supabase Auth/Edge search."""

    try:
        from playwright.async_api import async_playwright, expect
    except ModuleNotFoundError as exc:  # pragma: no cover - operator guidance
        raise RuntimeError(
            "Python Playwright is not installed. Install in an artifact venv, "
            "for example: python -m pip install playwright && python -m playwright install chromium"
        ) from exc

    dist = Path(args.dist).resolve() if args.dist else latest_preview_dist(Path("site/dist"))
    supabase_url = args.supabase_url.rstrip("/")
    real_session = ensure_real_edge_session(args)
    real_user = real_session.get("user") or {}
    storage_key = storage_key_for_supabase_url(supabase_url)

    server_root = dist.parent if dist.name.startswith("preview-") else dist
    preview_path = f"/{dist.name}/poisk/" if dist.name.startswith("preview-") else "/poisk/"
    with static_server(server_root) as base_url:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(viewport={"width": 390, "height": 844})
            console_errors: list[str] = []
            page.on(
                "console",
                lambda message: console_errors.append(message.text[:500])
                if message.type == "error"
                else None,
            )

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

            async def route_auth_only(route):
                url = route.request.url
                if "/auth/v1/token?grant_type=pkce" in url:
                    await route.fulfill(
                        status=200,
                        content_type="application/json; charset=utf-8",
                        body=json.dumps(real_session, ensure_ascii=False),
                    )
                    return
                if url.endswith("/auth/v1/user"):
                    await route.fulfill(
                        status=200,
                        content_type="application/json; charset=utf-8",
                        body=json.dumps(real_user, ensure_ascii=False),
                    )
                    return
                await route.continue_()

            await page.route(f"{supabase_url}/**", route_auth_only)

            # Seed the PKCE verifier on the same local origin first. This mimics a
            # static-page OAuth roundtrip without opening Yandex and without
            # bypassing Supabase Auth/Edge Function validation.
            await page.goto(f"{base_url}{preview_path}", wait_until="networkidle")
            await page.evaluate(
                """([key]) => {
                  localStorage.setItem(`${key}-code-verifier`, JSON.stringify('real-edge-smoke-verifier'));
                }""",
                [storage_key],
            )
            await page.goto(
                f"{base_url}{preview_path}?auth-smoke=real-edge&code=real-edge-smoke-code",
                wait_until="networkidle",
            )
            await page.wait_for_function(
                "() => document.querySelector('[data-authorized-search]')?.classList.contains('is-authorized')",
                timeout=10000,
            )
            await expect(page.locator("[data-search-form]").first).to_be_visible()
            await expect(page.locator("[data-search-login]").first).to_be_hidden()
            await expect(page.locator("[data-search-user]").first).to_be_visible()
            await expect(page.locator("[data-search-logout]").first).to_be_hidden()
            await page.locator("[data-search-account-toggle]").first.click()
            await expect(page.locator("[data-search-logout]").first).to_be_visible()
            await page.keyboard.press("Escape")
            await expect(page.locator("[data-search-logout]").first).to_be_hidden()

            await page.locator("[data-search-input]").first.fill(args.real_edge_query)
            await page.locator("[data-search-form]").first.evaluate("(form) => form.requestSubmit()")
            await expect(page.locator("[data-search-submit]").first).to_have_attribute("aria-busy", "true")

            results = page.locator("[data-search-results]").first
            await expect(results).to_be_visible(timeout=args.real_edge_timeout_ms)
            first_card = page.locator("[data-search-results] [data-event-card]").first
            await expect(first_card).to_be_visible(timeout=5000)
            await expect(first_card.locator("[data-feedback-action='like']").first).to_be_visible()
            await expect(first_card.locator("[data-native-share]").first).to_be_visible()
            await expect(first_card.locator("[data-feedback-action='not_interested']").first).to_be_visible()
            await page.wait_for_function(
                "() => document.querySelector('[data-search-submit]')?.getAttribute('aria-busy') === 'false'",
                timeout=5000,
            )

            cards = page.locator("[data-search-results] [data-event-card]")
            card_count = await cards.count()
            first_event_id = await first_card.get_attribute("data-event-id")
            status_text = await page.locator("[data-search-status]").first.text_content()
            if card_count < 1:
                raise AssertionError("real Edge search returned no rendered event cards")
            last_card = cards.nth(card_count - 1)
            await last_card.scroll_into_view_if_needed()
            await expect(last_card).to_be_visible()
            scrolled_event_id = await last_card.get_attribute("data-event-id")
            scroll_y = await page.evaluate("() => window.scrollY")
            if scroll_y <= 0:
                raise AssertionError("real Edge result feed did not scroll to rendered cards")
            if console_errors:
                raise AssertionError(f"browser console errors during real Edge smoke: {console_errors[:3]}")

            await browser.close()

    print(
        "authorized_search_real_edge_smoke=ok "
        f"dist={dist.name} cards={card_count} first_event={first_event_id} "
        f"scrolled_event={scrolled_event_id} scroll_y={scroll_y} "
        f"status={json.dumps(status_text, ensure_ascii=False)}"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist", help="Path to site/dist/<preview-id>. Defaults to latest preview-* build.")
    parser.add_argument("--supabase-url", default="https://example.supabase.co")
    parser.add_argument("--screenshot-dir", default="", help="Optional directory for mocked loading/result screenshots.")
    parser.add_argument(
        "--real-edge",
        action="store_true",
        help=(
            "Use a fake Yandex callback but a real Supabase Auth session and "
            "real event-search Edge Function. Calls live Supabase and consumes search quota."
        ),
    )
    parser.add_argument("--supabase-publishable-key", default=os.getenv("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY", ""))
    parser.add_argument("--supabase-secret-key", default=os.getenv("PERSONALIZATION_SUPABASE_SECRET_KEY", ""))
    parser.add_argument(
        "--real-edge-email",
        default="",
        help="Optional fixed smoke user email. Defaults to a unique example.invalid email to avoid quota collisions.",
    )
    parser.add_argument("--real-edge-password", default="")
    parser.add_argument("--real-edge-query", default="джаз на выходных")
    parser.add_argument("--real-edge-timeout-ms", type=int, default=70000)
    args = parser.parse_args()
    import asyncio

    if args.real_edge:
        return asyncio.run(run_real_edge_smoke(args))
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())
