#!/usr/bin/env python3
"""Materialize JavaScript-rendered Region Talk article image references.

The worker is deliberately small: it claims at most three ``image_queue_item``
rows, opens their canonical public article pages in Chromium, records
publisher-rendered image/DOM evidence and puts the same rows back into the
existing ImageDiagnostic queue.  It never reads Telegram and never publishes.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import ipaddress
import json
import os
import re
import socket
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin, urlsplit


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.region_talk_goal_notify import (  # noqa: E402
    ensure_ydb_module,
    load_env,
    read_kind_rows,
    ydb_credentials,
    ydb_endpoint_database,
    ydb_table_path,
)


CONTRACT_VERSION = "region_talk_bounded_article_browser_materialization_v1"
EVIDENCE_VERSION = "region_talk_rendered_article_image_evidence_v1"
MAX_PAGES_HARD = 3
MAX_ASSETS_HARD = 20
MAX_ATTEMPTS = 3
RETRY_DELAYS = (timedelta(hours=6), timedelta(hours=24))
LEASE_MINUTES = 12
TERMINAL_BROWSER_STATUSES = {
    "terminal_no_associated_images",
    "terminal_fetch_failed",
}
_UNRELATED_IMAGE_TOKENS = {
    "logo", "logotype", "avatar", "author-photo", "author_photo", "profile",
    "advert", "advertising", "advertisement", "banner", "promo", "sponsor",
    "pixel", "tracker", "related", "recommend", "subscription", "newsletter",
    "favicon", "icon", "логотип", "аватар", "реклама", "баннер", "похожие",
    "рекомендуем",
}


def direct_image_url(ref: str) -> str:
    """Match ImageDiagnostic's URL contract without importing its Kaggle runtime."""

    value = str(ref or "").strip()
    if not (value.startswith("http") or value.startswith("//")):
        return ""
    parsed = urlsplit("https:" + value if value.startswith("//") else value)
    return "" if parsed.fragment.lower() == "media" else value


def article_image_association_decision(
    candidate: dict[str, Any], *, article_title: str = "", article_summary: str = ""
) -> dict[str, Any]:
    """Browser-side mirror of ImageDiagnostic's deterministic association pre-gate."""

    evidence = " ".join(
        str(candidate.get(key) or "")
        for key in ("url", "alt", "caption", "class", "id", "role")
    ).lower()
    lexical = set(re.findall(r"[a-zа-яё]+", evidence))
    for token in _UNRELATED_IMAGE_TOKENS:
        if token in lexical or (("-" in token or "_" in token) and token in evidence):
            return {"decision": "reject", "reason": f"excluded_token:{token}", "matched_terms": []}
    width = int(float(candidate.get("width") or 0))
    height = int(float(candidate.get("height") or 0))
    if width and height and (width < 240 or height < 140):
        return {"decision": "reject", "reason": "declared_dimensions_too_small", "matched_terms": []}
    role = str(candidate.get("role") or "")
    declared_roles = {
        "jsonld_article_image", "article_main", "article_figure", "article_picture",
        "article_lightbox", "og_image", "twitter_image", "image_src",
    }
    if role not in declared_roles:
        return {"decision": "review", "reason": "no_article_dom_or_metadata_role", "matched_terms": []}
    ignored = {"https", "http", "www", "image", "photo", "фото", "изображение"}
    article_terms = {
        token for token in re.findall(r"[a-zа-яё0-9]{4,}", f"{article_title} {article_summary}".lower())
        if token not in ignored
    }
    image_terms = {
        token for token in re.findall(
            r"[a-zа-яё0-9]{4,}",
            " ".join(str(candidate.get(key) or "") for key in ("url", "alt", "caption")).lower(),
        )
        if token not in ignored
    }
    matched = sorted(article_terms & image_terms)[:12]
    return {
        "decision": "accept",
        "reason": "publisher_declared_role_with_textual_match" if matched else "publisher_declared_article_role",
        "matched_terms": matched,
    }


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(value: datetime | None = None) -> str:
    return (value or utc_now()).isoformat()


def parse_dt(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        result = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return result if result.tzinfo else result.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def public_http_url(
    raw: str,
    *,
    resolver: Callable[..., list[tuple[Any, ...]]] = socket.getaddrinfo,
) -> str:
    """Return a canonical public HTTP(S) URL or raise an SSRF-safe error."""

    value = str(raw or "").strip()
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("only public http/https URLs are allowed")
    if not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("URL must have a hostname and no embedded credentials")
    hostname = parsed.hostname.rstrip(".").lower()
    if hostname == "localhost" or hostname.endswith((".localhost", ".local", ".internal")):
        raise ValueError("local hostname is forbidden")
    try:
        answers = resolver(hostname, parsed.port or (443 if parsed.scheme == "https" else 80), type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ValueError(f"hostname resolution failed: {type(exc).__name__}") from exc
    addresses = {str(answer[4][0]).split("%", 1)[0] for answer in answers if len(answer) >= 5 and answer[4]}
    if not addresses:
        raise ValueError("hostname has no resolved addresses")
    for address in addresses:
        try:
            ip = ipaddress.ip_address(address)
        except ValueError as exc:
            raise ValueError("hostname resolved to an invalid address") from exc
        if not ip.is_global:
            raise ValueError(f"non-public destination is forbidden: {address}")
    return value


def request_is_allowed(url: str) -> bool:
    try:
        public_http_url(url)
        return True
    except ValueError:
        return False


def _candidate_url(raw: Any, base_url: str) -> str:
    absolute = urljoin(base_url, str(raw or "").strip())
    try:
        public_http_url(absolute)
    except ValueError:
        return ""
    return direct_image_url(absolute)


def normalize_rendered_candidates(
    raw_candidates: list[dict[str, Any]],
    *,
    page_url: str,
    article_title: str,
    article_summary: str,
    max_assets: int = MAX_ASSETS_HARD,
) -> list[dict[str, Any]]:
    """Normalize browser DOM evidence and apply the existing association pre-gate."""

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    priority = {
        "jsonld_article_image": 0,
        "article_figure": 1,
        "article_picture": 1,
        "article_main": 2,
        "article_lightbox": 2,
        "og_image": 3,
        "twitter_image": 4,
        "image_src": 5,
    }
    def dimension(value: Any) -> int:
        try:
            return max(0, int(float(value or 0)))
        except (TypeError, ValueError):
            return 0

    for raw in raw_candidates:
        if not isinstance(raw, dict):
            continue
        url = _candidate_url(raw.get("url"), page_url)
        if not url or url in seen:
            continue
        candidate = {
            "url": url,
            "source_url": url,
            "role": str(raw.get("role") or "")[:80],
            "alt": str(raw.get("alt") or "")[:300],
            "caption": str(raw.get("caption") or "")[:300],
            "width": dimension(raw.get("width")),
            "height": dimension(raw.get("height")),
            "selector": str(raw.get("selector") or "")[:300],
            "dom_path": str(raw.get("dom_path") or "")[:500],
            "referrer": page_url,
            "rendered_page_url": page_url,
            "evidence_version": EVIDENCE_VERSION,
        }
        decision = article_image_association_decision(
            candidate,
            article_title=article_title,
            article_summary=article_summary,
        )
        candidate.update({
            "association_decision": decision["decision"],
            "association_reason": decision["reason"],
            "association_matched_terms": decision["matched_terms"],
        })
        if decision["decision"] != "accept":
            continue
        seen.add(url)
        out.append(candidate)
    out.sort(key=lambda item: priority.get(str(item.get("role") or ""), 99))
    return out[:max(1, min(MAX_ASSETS_HARD, int(max_assets)))]


RENDERED_IMAGE_EVAL = r"""
() => {
  const result = [];
  const add = (url, role, el = null, extra = {}) => {
    if (!url) return;
    const fig = el && el.closest ? el.closest('figure') : null;
    const caption = fig ? (fig.querySelector('figcaption')?.innerText || '') : '';
    const path = el ? (() => {
      const parts = [];
      let node = el;
      while (node && node.nodeType === 1 && parts.length < 8) {
        let part = node.tagName.toLowerCase();
        if (node.id) { part += '#' + node.id; parts.unshift(part); break; }
        const cls = [...node.classList].slice(0, 2).join('.');
        if (cls) part += '.' + cls;
        parts.unshift(part); node = node.parentElement;
      }
      return parts.join(' > ');
    })() : '';
    result.push({url, role, alt: el?.getAttribute?.('alt') || '', caption,
      width: el?.naturalWidth || el?.width || extra.width || 0,
      height: el?.naturalHeight || el?.height || extra.height || 0,
      selector: extra.selector || '', dom_path: path});
  };
  document.querySelectorAll('meta[property="og:image"],meta[property="og:image:url"]').forEach(
    el => add(el.content, 'og_image', el, {selector: 'meta[property="og:image"]'}));
  document.querySelectorAll('meta[name="twitter:image"],meta[name="twitter:image:src"]').forEach(
    el => add(el.content, 'twitter_image', el, {selector: 'meta[name="twitter:image"]'}));
  document.querySelectorAll('link[rel~="image_src"]').forEach(
    el => add(el.href, 'image_src', el, {selector: 'link[rel~="image_src"]'}));
  const imageRole = el => el.closest('figure') ? 'article_figure'
    : el.closest('picture') ? 'article_picture' : 'article_main';
  document.querySelectorAll('article img, article picture source, main img, main picture source').forEach(el => {
    const srcset = el.srcset ? el.srcset.split(',').pop().trim().split(/\s+/)[0] : '';
    add(el.currentSrc || el.src || srcset || el.getAttribute('data-src') || el.getAttribute('data-original'),
      imageRole(el), el, {selector: 'article/main image'});
  });
  document.querySelectorAll('article [data-fancybox][href],article [data-lightbox][href],main [data-fancybox][href],main [data-lightbox][href]').forEach(
    el => add(el.href, 'article_lightbox', el, {selector: '[data-fancybox/data-lightbox]'}));
  document.querySelectorAll('script[type="application/ld+json"]').forEach(el => {
    let root; try { root = JSON.parse(el.textContent || ''); } catch (_) { return; }
    const queue = Array.isArray(root) ? [...root] : [root];
    while (queue.length) {
      const item = queue.shift(); if (!item || typeof item !== 'object') continue;
      if (Array.isArray(item['@graph'])) queue.push(...item['@graph']);
      const types = (Array.isArray(item['@type']) ? item['@type'] : [item['@type']]).map(x => String(x || '').toLowerCase());
      if (!types.some(x => ['article','newsarticle','reportagearticle','scholarlyarticle','blogposting'].includes(x))) continue;
      const images = Array.isArray(item.image) ? item.image : [item.image];
      images.forEach(img => typeof img === 'string'
        ? add(img, 'jsonld_article_image', el, {selector: 'script[type="application/ld+json"]'})
        : img && add(img.url || img.contentUrl, 'jsonld_article_image', el,
            {selector: 'script[type="application/ld+json"]', width: img.width, height: img.height}));
    }
  });
  return result;
}
"""


async def browser_materialize(
    page: Any,
    row: dict[str, Any],
    *,
    timeout_seconds: int,
    max_assets: int,
) -> dict[str, Any]:
    canonical_url = public_http_url(str(row.get("post_url") or ""))
    request_count = 0

    async def guard_route(route: Any, request: Any) -> None:
        nonlocal request_count
        request_count += 1
        if request_count > 160 or not request_is_allowed(str(request.url or "")):
            await route.abort()
        else:
            await route.continue_()

    await page.route("**/*", guard_route)
    response = await page.goto(canonical_url, wait_until="domcontentloaded", timeout=timeout_seconds * 1000)
    await page.wait_for_timeout(min(3000, max(500, timeout_seconds * 100)))
    final_url = public_http_url(str(page.url or canonical_url))
    if response is not None and int(response.status) >= 400:
        raise RuntimeError(f"article HTTP status {int(response.status)}")
    raw_candidates = await page.evaluate(RENDERED_IMAGE_EVAL)
    candidates = normalize_rendered_candidates(
        list(raw_candidates or []),
        page_url=final_url,
        article_title=str(row.get("title") or row.get("publication_title") or ""),
        article_summary=str(row.get("summary") or row.get("publication_summary") or row.get("publication_draft_text") or ""),
        max_assets=max_assets,
    )
    return {
        "canonical_page_url": canonical_url,
        "rendered_page_url": final_url,
        "request_count": request_count,
        "candidates": candidates,
    }


def row_due(row: dict[str, Any], now: datetime) -> bool:
    if str(row.get("browser_materialization_status") or "") in TERMINAL_BROWSER_STATUSES:
        return False
    if str(row.get("image_queue_status") or "") != "needs_browser_materialization":
        return False
    if int(row.get("browser_materialization_attempt_count") or 0) >= MAX_ATTEMPTS:
        return False
    next_attempt = parse_dt(row.get("browser_materialization_next_attempt_after"))
    if next_attempt and next_attempt > now:
        return False
    lease_until = parse_dt(row.get("browser_materialization_lease_expires_at"))
    if str(row.get("browser_materialization_lease_run_id") or "") and lease_until and lease_until > now:
        return False
    return bool(str(row.get("post_url") or "").strip())


def apply_success(row: dict[str, Any], result: dict[str, Any], *, run_id: str, now: datetime) -> dict[str, Any]:
    updated = dict(row)
    candidates = list(result.get("candidates") or [])
    urls = [str(item.get("url") or "") for item in candidates if item.get("url")]
    common = {
        "browser_materialization_contract_version": CONTRACT_VERSION,
        "browser_materialization_evidence_version": EVIDENCE_VERSION,
        "browser_materialization_attempt_count": int(row.get("browser_materialization_attempt_count") or 0) + 1,
        "browser_materialization_last_attempt_at": utc_iso(now),
        "browser_materialization_last_run_id": run_id,
        "browser_materialization_lease_run_id": "",
        "browser_materialization_lease_at": "",
        "browser_materialization_lease_expires_at": "",
        "browser_materialization_next_attempt_after": "",
        "browser_materialization_rendered_page_url": str(result.get("rendered_page_url") or ""),
        "browser_materialization_request_count": int(result.get("request_count") or 0),
        "browser_materialization_evidence_json": json.dumps(candidates, ensure_ascii=False, separators=(",", ":")),
        "browser_materialized_image_urls": urls,
        "web_image_candidates_json": json.dumps(candidates, ensure_ascii=False, separators=(",", ":")),
        "web_image_used_evidence_json": json.dumps(candidates, ensure_ascii=False, separators=(",", ":")),
        "web_gallery_discovered_count": len(urls),
        "web_gallery_used_count": len(urls),
        "web_gallery_discovery_status": "browser_article_images_found" if urls else "browser_zero_associated_images",
        "updated_at": utc_iso(now),
    }
    updated.update(common)
    if urls:
        updated.update({
            "browser_materialization_status": "materialized",
            "image_queue_status": "needs_actual_image_fetch",
            "media_fetch_status": "browser_refs_ready",
            "media_fetch_error": "",
            "presentation_recommendation": "source_media_pending_visual_selection",
            "presentation_recommendation_reason": "browser_found_article_associated_source_images",
            "next_action": "region_talk_image_diagnostic_download_and_vlm_rank",
        })
    else:
        updated.update({
            "browser_materialization_status": "terminal_no_associated_images",
            "image_queue_status": "not_reviewable_no_media",
            "media_fetch_status": "not_reviewable_no_media",
            "media_fetch_error": "rendered article has zero associated image candidates",
            "image_quality_reason": "browser_zero_associated_images",
            "image_quality_terminality": "terminal",
            "presentation_recommendation": "system_link_preview",
            "presentation_recommendation_reason": "rendered_article_has_no_associated_source_image",
            "next_action": "publish_with_native_link_preview_if_editorially_approved",
        })
    return updated


def apply_failure(row: dict[str, Any], error: Exception, *, run_id: str, now: datetime) -> dict[str, Any]:
    updated = dict(row)
    attempts = int(row.get("browser_materialization_attempt_count") or 0) + 1
    updated.update({
        "browser_materialization_contract_version": CONTRACT_VERSION,
        "browser_materialization_attempt_count": attempts,
        "browser_materialization_last_attempt_at": utc_iso(now),
        "browser_materialization_last_run_id": run_id,
        "browser_materialization_last_error": f"{type(error).__name__}: {str(error)[:300]}",
        "browser_materialization_lease_run_id": "",
        "browser_materialization_lease_at": "",
        "browser_materialization_lease_expires_at": "",
        "updated_at": utc_iso(now),
    })
    if attempts >= MAX_ATTEMPTS:
        updated.update({
            "browser_materialization_status": "terminal_fetch_failed",
            "browser_materialization_next_attempt_after": "",
            "image_queue_status": "broken_media",
            "media_fetch_status": "broken_media",
            "media_fetch_error": "browser materialization exhausted finite attempts",
            "image_quality_reason": "browser_materialization_retry_exhausted",
            "image_quality_terminality": "terminal",
            "presentation_recommendation": "system_link_preview",
            "presentation_recommendation_reason": "browser_materialization_failed_use_native_preview",
            "next_action": "publish_with_native_link_preview_if_editorially_approved",
        })
    else:
        delay = RETRY_DELAYS[min(attempts - 1, len(RETRY_DELAYS) - 1)]
        updated.update({
            "browser_materialization_status": "retry_wait",
            "browser_materialization_next_attempt_after": utc_iso(now + delay),
            "image_queue_status": "needs_browser_materialization",
            "media_fetch_status": "needs_browser_materialization",
            "media_fetch_error": f"browser retry {attempts}/{MAX_ATTEMPTS} scheduled",
            "next_action": "retry_bounded_browser_materialization_when_due",
        })
    return updated


def _payload(row: Any) -> dict[str, Any]:
    raw = row.payload_json
    return json.loads(raw) if isinstance(raw, str) else dict(raw or {})


def claim_row(pool: Any, ydb: Any, table: str, pk: str, *, run_id: str, now: datetime) -> dict[str, Any] | None:
    """Claim one row in a serializable transaction (the payload is the CAS state)."""

    select = f"DECLARE $pk AS Utf8; SELECT payload_json FROM `{table}` WHERE pk=$pk;"
    upsert = f"""
DECLARE $pk AS Utf8; DECLARE $payload AS Json; DECLARE $updated AS Utf8;
UPDATE `{table}` SET payload_json=$payload, updated_at=$updated WHERE pk=$pk;
"""

    def op(session: Any) -> dict[str, Any] | None:
        tx = session.transaction(ydb.SerializableReadWrite())
        result = tx.execute(session.prepare(select), {"$pk": pk}, commit_tx=False)
        rows = result[0].rows if result else []
        if not rows:
            tx.rollback()
            return None
        current = _payload(rows[0])
        if not row_due(current, now):
            tx.rollback()
            return None
        current.update({
            "browser_materialization_status": "in_progress",
            "browser_materialization_lease_run_id": run_id,
            "browser_materialization_lease_at": utc_iso(now),
            "browser_materialization_lease_expires_at": utc_iso(now + timedelta(minutes=LEASE_MINUTES)),
            "updated_at": utc_iso(now),
        })
        tx.execute(session.prepare(upsert), {
            "$pk": pk,
            "$payload": json.dumps(current, ensure_ascii=False),
            "$updated": utc_iso(now),
        }, commit_tx=True)
        current["_ydb_pk"] = pk
        return current

    return pool.retry_operation_sync(op)


def finish_row(pool: Any, ydb: Any, table: str, pk: str, result: dict[str, Any], *, run_id: str) -> bool:
    """Merge only browser-owned fields if this run still owns the lease."""

    select = f"DECLARE $pk AS Utf8; SELECT payload_json FROM `{table}` WHERE pk=$pk;"
    update = f"""
DECLARE $pk AS Utf8; DECLARE $payload AS Json; DECLARE $updated AS Utf8;
UPDATE `{table}` SET payload_json=$payload, updated_at=$updated WHERE pk=$pk;
"""
    browser_fields = {
        key for key in result
        if key.startswith("browser_materialization_")
        or key.startswith("browser_materialized_")
        or key in {
            "image_queue_status", "media_fetch_status", "media_fetch_error",
            "image_quality_reason", "image_quality_terminality", "next_action",
            "presentation_recommendation", "presentation_recommendation_reason",
            "web_image_candidates_json", "web_image_used_evidence_json",
            "web_gallery_discovered_count", "web_gallery_used_count",
            "web_gallery_discovery_status", "updated_at",
        }
    }

    def op(session: Any) -> bool:
        tx = session.transaction(ydb.SerializableReadWrite())
        response = tx.execute(session.prepare(select), {"$pk": pk}, commit_tx=False)
        rows = response[0].rows if response else []
        if not rows:
            tx.rollback()
            return False
        current = _payload(rows[0])
        if str(current.get("browser_materialization_lease_run_id") or "") != run_id:
            tx.rollback()
            return False
        for key in browser_fields:
            current[key] = result.get(key)
        now = str(result.get("updated_at") or utc_iso())
        tx.execute(session.prepare(update), {
            "$pk": pk,
            "$payload": json.dumps(current, ensure_ascii=False),
            "$updated": now,
        }, commit_tx=True)
        return True

    return bool(pool.retry_operation_sync(op))


def connect_ydb() -> tuple[Any, Any, Any, str]:
    ydb = ensure_ydb_module()
    endpoint, database = ydb_endpoint_database(allow_yc_fallback=False)
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb_credentials(ydb, allow_yc_fallback=False))
    driver.wait(timeout=10, fail_fast=True)
    return ydb, driver, ydb.SessionPool(driver), ydb_table_path(database)


async def run(args: argparse.Namespace) -> dict[str, Any]:
    page_limit = max(1, min(MAX_PAGES_HARD, int(args.limit)))
    run_id = args.run_id or "region-talk-browser-" + utc_now().strftime("%Y%m%dT%H%M%SZ")
    ydb, driver, pool, table = connect_ydb()
    try:
        rows = read_kind_rows(pool, ydb, table, "image_queue_item", max(1, int(args.scan_limit)))
        due = [row for row in rows if row_due(row, utc_now())]
        due.sort(key=lambda row: (
            int(row.get("browser_materialization_attempt_count") or 0),
            str(row.get("browser_materialization_next_attempt_after") or ""),
            str(row.get("added_at") or ""),
            str(row.get("_ydb_pk") or ""),
        ))
        summary: dict[str, Any] = {
            "run_id": run_id,
            "contract_version": CONTRACT_VERSION,
            "dry_run": not args.execute,
            "scanned": len(rows),
            "due": len(due),
            "page_limit": page_limit,
            "claimed": 0,
            "materialized": 0,
            "zero_associated_terminal": 0,
            "retry_wait": 0,
            "fetch_failed_terminal": 0,
            "lease_lost": 0,
        }
        if not args.execute:
            summary["selected_pks"] = [str(row.get("_ydb_pk") or "") for row in due[:page_limit]]
            return summary

        from playwright.async_api import async_playwright  # type: ignore

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True, args=["--disable-dev-shm-usage", "--no-sandbox"])
            try:
                for selected in due[:page_limit]:
                    now = utc_now()
                    pk = str(selected.get("_ydb_pk") or "")
                    claimed = claim_row(pool, ydb, table, pk, run_id=run_id, now=now)
                    if not claimed:
                        continue
                    summary["claimed"] += 1
                    context = await browser.new_context(
                        user_agent="Mozilla/5.0 (compatible; RegionTalkArticleMaterializer/1.0)",
                        java_script_enabled=True,
                        ignore_https_errors=False,
                        service_workers="block",
                    )
                    page = await context.new_page()
                    try:
                        browser_result = await browser_materialize(
                            page,
                            claimed,
                            timeout_seconds=max(5, min(60, int(args.timeout_seconds))),
                            max_assets=max(1, min(MAX_ASSETS_HARD, int(args.max_assets))),
                        )
                        finished = apply_success(claimed, browser_result, run_id=run_id, now=utc_now())
                    except Exception as exc:
                        finished = apply_failure(claimed, exc, run_id=run_id, now=utc_now())
                    finally:
                        await context.close()
                    if not finish_row(pool, ydb, table, pk, finished, run_id=run_id):
                        summary["lease_lost"] += 1
                        continue
                    status = str(finished.get("browser_materialization_status") or "")
                    if status == "materialized": summary["materialized"] += 1
                    elif status == "terminal_no_associated_images": summary["zero_associated_terminal"] += 1
                    elif status == "retry_wait": summary["retry_wait"] += 1
                    elif status == "terminal_fetch_failed": summary["fetch_failed_terminal"] += 1
            finally:
                await browser.close()
        return summary
    finally:
        driver.stop(timeout=5)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Claim rows, run Chromium and write results; default is dry-run selection")
    parser.add_argument("--limit", type=int, default=int(os.getenv("REGION_TALK_BROWSER_MAX_PAGES_PER_RUN") or "3"))
    parser.add_argument("--scan-limit", type=int, default=int(os.getenv("REGION_TALK_BROWSER_SCAN_LIMIT") or "5000"))
    parser.add_argument("--timeout-seconds", type=int, default=int(os.getenv("REGION_TALK_BROWSER_PAGE_TIMEOUT_SECONDS") or "35"))
    parser.add_argument("--max-assets", type=int, default=int(os.getenv("REGION_TALK_BROWSER_MAX_ASSETS_PER_PAGE") or "20"))
    parser.add_argument("--run-id", default=os.getenv("REGION_TALK_RUN_ID") or "")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    args = parser.parse_args()
    load_env(args.env_file)
    print(json.dumps(asyncio.run(run(args)), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
