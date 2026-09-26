#!/usr/bin/env python3
"""Bounded public-post collector for editorial corpus research.

Telegram is read from public t.me preview pages so shared Telethon sessions are
never opened. VK is read through the official API. The collector uses one
stable user agent, bounded jitter and backoff; it never uses proxies, CAPTCHA
bypass, fake interactions, read receipts or typing actions.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import random
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import requests
from bs4 import BeautifulSoup

VK_VERSION = "5.199"
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 EditorialCorpusResearch/1.0"
)
WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+(?:[-'][A-Za-zА-Яа-яЁё0-9]+)*")


class PoliteSession:
    def __init__(self) -> None:
        self.http = requests.Session()
        self.http.headers.update({"User-Agent": UA, "Accept-Language": "ru,en;q=0.7"})
        self.random = random.SystemRandom()

    def pause(self, low: float, high: float) -> None:
        time.sleep(self.random.uniform(low, high))

    def get(self, url: str, *, params: Mapping[str, Any] | None = None) -> requests.Response:
        last: Exception | None = None
        for attempt in range(5):
            try:
                response = self.http.get(url, params=params, timeout=35)
                if response.status_code in {429, 500, 502, 503, 504}:
                    base = min(30.0, 2.0 ** attempt)
                    self.pause(base, base + 1.5)
                    continue
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last = exc
                if attempt < 4:
                    base = min(30.0, 2.0 ** attempt)
                    self.pause(base, base + 2.0)
        status = getattr(getattr(last, "response", None), "status_code", None)
        raise RuntimeError(f"request_failed:{type(last).__name__}:status={status}")


def clean_text(value: str) -> str:
    value = (value or "").replace("\u00a0", " ")
    value = re.sub(r"[ \t]+", " ", value)
    return re.sub(r"\n{3,}", "\n\n", value).strip()


def compact_number(value: str | None) -> int | None:
    raw = (value or "").strip().replace(" ", "").replace(",", ".")
    match = re.match(r"^([0-9]+(?:\.[0-9]+)?)([KКMМ]?)$", raw, re.I)
    if not match:
        digits = re.sub(r"\D", "", raw)
        return int(digits) if digits else None
    number = float(match.group(1))
    suffix = match.group(2).lower()
    if suffix in {"k", "к"}:
        number *= 1_000
    elif suffix in {"m", "м"}:
        number *= 1_000_000
    return int(number)


def hash_text(text: str) -> str:
    normalized = re.sub(r"\s+", " ", clean_text(text).casefold().replace("ё", "е"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def basic_features(text: str) -> dict[str, Any]:
    words = WORD_RE.findall(text)
    paragraphs = [part for part in text.split("\n") if part.strip()]
    return {
        "char_count": len(text),
        "word_count": len(words),
        "paragraph_count": len(paragraphs),
        "question_count": text.count("?"),
        "exclamation_count": text.count("!"),
        "url_count": len(re.findall(r"https?://\S+", text)),
        "hashtag_count": len(re.findall(r"(?<!\w)#[\wа-яё]+", text, re.I)),
        "normalized_sha256": hash_text(text),
    }


def parse_tg_page(html: str, handle: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict[str, Any]] = []
    for wrap in soup.select(".tgme_widget_message_wrap"):
        message = wrap.select_one(".tgme_widget_message")
        data_post = str(message.get("data-post") or "") if message else ""
        if "/" not in data_post:
            continue
        channel, raw_id = data_post.rsplit("/", 1)
        if channel.casefold() != handle.casefold() or not raw_id.isdigit():
            continue
        text_node = wrap.select_one(".tgme_widget_message_text")
        text = clean_text(text_node.get_text("\n", strip=True) if text_node else "")
        time_node = wrap.select_one("time[datetime]")
        views_node = wrap.select_one(".tgme_widget_message_views")
        rows.append({
            "platform": "telegram",
            "post_id": int(raw_id),
            "source_public_id": handle,
            "url": f"https://t.me/{handle}/{raw_id}",
            "published_at": str(time_node.get("datetime") or "") if time_node else "",
            "text": text,
            "views": compact_number(views_node.get_text(" ", strip=True) if views_node else None),
            "likes": None,
            "comments": None,
            "shares": None,
            "is_repost": bool(wrap.select_one(".tgme_widget_message_forwarded_from")),
            "is_ad": False,
            "has_media": bool(wrap.select_one(
                ".tgme_widget_message_photo_wrap, .tgme_widget_message_video_player, "
                ".tgme_widget_message_document_wrap, .tgme_widget_message_poll"
            )),
        })
    return rows


def collect_tg(http: PoliteSession, source: Mapping[str, Any], target: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    handle = str(source["handle"]).lstrip("@")
    found: dict[int, dict[str, Any]] = {}
    before: int | None = None
    error = ""
    pages = 0
    try:
        for _ in range(20):
            pages += 1
            response = http.get(f"https://t.me/s/{handle}", params={"before": before} if before else None)
            page = parse_tg_page(response.text, handle)
            if not page:
                break
            for row in page:
                found[int(row["post_id"])] = row
            minimum = min(int(row["post_id"]) for row in page)
            eligible = [row for row in found.values() if len(str(row.get("text") or "")) >= 20]
            if len(eligible) >= target or minimum <= 1 or minimum == before:
                break
            before = minimum
            http.pause(1.25, 2.35)
    except Exception as exc:
        error = f"{type(exc).__name__}:{exc}"
    ordered = sorted(found.values(), key=lambda row: int(row["post_id"]), reverse=True)
    eligible = [row for row in ordered if len(str(row.get("text") or "")) >= 20]
    selected = eligible[:target]
    return selected, {
        "platform": "telegram", "source_id": source["id"], "title": source["title"],
        "public_id": handle, "raw_scanned": len(ordered), "text_bearing": len(eligible),
        "selected": len(selected), "pages_or_calls": pages, "collection_path": "public_t_me_preview",
        "error": error,
    }


def first_env(names: Sequence[str]) -> tuple[str, str]:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return name, value
    return "", ""


def vk_call(http: PoliteSession, token: str, method: str, params: Mapping[str, Any]) -> Any:
    payload = dict(params)
    payload.update({"access_token": token, "v": VK_VERSION})
    for attempt in range(7):
        data = http.get(f"https://api.vk.com/method/{method}", params=payload).json()
        error = data.get("error") if isinstance(data, Mapping) else None
        if not error:
            return data.get("response") if isinstance(data, Mapping) else None
        code = int(error.get("error_code") or 0)
        if code in {6, 9, 10, 29} and attempt < 6:
            base = min(20.0, 1.5 * (2 ** attempt))
            http.pause(base, base + 2.0)
            continue
        raise RuntimeError(f"vk_error_{code}:{str(error.get('error_msg') or 'api_error')}")
    raise RuntimeError("vk_retry_exhausted")


def metric(item: Mapping[str, Any], key: str) -> int | None:
    value = item.get(key)
    if isinstance(value, Mapping):
        value = value.get("count")
    return int(value) if isinstance(value, (int, float)) and value >= 0 else None


def collect_vk(http: PoliteSession, token: str, source: Mapping[str, Any], target: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    screen = str(source["screen_name"])
    found: dict[int, dict[str, Any]] = {}
    error = ""
    calls = 0
    group_id = 0
    try:
        resolved = vk_call(http, token, "utils.resolveScreenName", {"screen_name": screen})
        calls += 1
        if isinstance(resolved, Mapping) and str(resolved.get("type") or "") in {"group", "page", "event"}:
            group_id = int(resolved.get("object_id") or 0)
        if group_id <= 0:
            group_id = int(source.get("group_id_hint") or 0)
        if group_id <= 0:
            raise RuntimeError(f"unresolved_screen_name:{screen}")
        for offset in range(0, 300, 100):
            response = vk_call(http, token, "wall.get", {
                "owner_id": -abs(group_id), "filter": "owner", "count": 100, "offset": offset,
            })
            calls += 1
            items = response.get("items") if isinstance(response, Mapping) else []
            if not isinstance(items, list) or not items:
                break
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                post_id = int(item.get("id") or 0)
                if post_id <= 0:
                    continue
                text = clean_text(str(item.get("text") or ""))
                ts = int(item.get("date") or 0)
                found[post_id] = {
                    "platform": "vk", "post_id": post_id, "source_public_id": screen,
                    "resolved_group_id": group_id, "url": f"https://vk.com/wall-{group_id}_{post_id}",
                    "published_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts > 0 else "",
                    "text": text, "views": metric(item, "views"), "likes": metric(item, "likes"),
                    "comments": metric(item, "comments"), "shares": metric(item, "reposts"),
                    "is_repost": bool(item.get("copy_history")), "is_ad": bool(item.get("marked_as_ads")),
                    "has_media": bool(item.get("attachments")),
                }
            if sum(1 for row in found.values() if len(str(row.get("text") or "")) >= 20) >= target or len(items) < 100:
                break
            http.pause(0.55, 1.05)
    except Exception as exc:
        error = f"{type(exc).__name__}:{exc}"
    ordered = sorted(found.values(), key=lambda row: int(row["post_id"]), reverse=True)
    eligible = [row for row in ordered if len(str(row.get("text") or "")) >= 20]
    selected = eligible[:target]
    return selected, {
        "platform": "vk", "source_id": source["id"], "title": source["title"], "public_id": screen,
        "resolved_group_id": group_id or None, "raw_scanned": len(ordered), "text_bearing": len(eligible),
        "selected": len(selected), "pages_or_calls": calls, "collection_path": "vk_official_api",
        "error": error,
    }


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--target", type=int, default=100)
    args = parser.parse_args()
    config = json.loads(args.sources.read_text(encoding="utf-8"))
    target = max(1, min(150, args.target))
    raw_rows: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    http = PoliteSession()
    vk_secret, vk_token = first_env(("VK_USER_TOKEN", "VK_ACCESS_TOKEN4", "VK_SERVICE_TOKEN", "VK_TOKEN"))
    tg_secret, _ = first_env(("TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR", "TELEGRAM_AUTH_BUNDLE_S22", "TG_SESSION"))

    for index, source in enumerate(config.get("telegram") or []):
        posts, summary = collect_tg(http, source, target)
        for row in posts:
            row.update({"source_id": source["id"], "source_title": source["title"], "features": basic_features(row["text"])})
        raw_rows.extend(posts)
        summaries.append(summary)
        if index + 1 < len(config.get("telegram") or []):
            http.pause(2.0, 4.0)

    for index, source in enumerate(config.get("vk") or []):
        if vk_token:
            posts, summary = collect_vk(http, vk_token, source, target)
        else:
            posts, summary = [], {"platform": "vk", "source_id": source["id"], "title": source["title"],
                "public_id": source["screen_name"], "raw_scanned": 0, "text_bearing": 0, "selected": 0,
                "pages_or_calls": 0, "collection_path": "vk_official_api", "error": "missing_vk_token"}
        for row in posts:
            row.update({"source_id": source["id"], "source_title": source["title"], "features": basic_features(row["text"])})
        raw_rows.extend(posts)
        summaries.append(summary)
        if index + 1 < len(config.get("vk") or []):
            http.pause(1.5, 3.0)

    out = args.out
    raw = out / "raw"
    persist = out / "persist"
    raw.mkdir(parents=True, exist_ok=True)
    persist.mkdir(parents=True, exist_ok=True)
    write_jsonl(raw / "full_corpus.jsonl", raw_rows)
    write_jsonl(persist / "post_manifest.jsonl", [{k: v for k, v in row.items() if k != "text"} for row in raw_rows])
    fields = sorted({key for row in summaries for key in row})
    with (persist / "source_summary.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader(); writer.writerows(summaries)
    run = {
        "schema_version": 1, "collected_at": datetime.now(timezone.utc).isoformat(),
        "target_posts_per_source": target, "selected_posts": len(raw_rows),
        "source_count": len(summaries), "successful_sources": sum(int(row["selected"]) > 0 for row in summaries),
        "complete_sources": sum(int(row["selected"]) >= target for row in summaries),
        "platform_counts": dict(Counter(row["platform"] for row in raw_rows)),
        "secret_presence": {"vk_read_token": bool(vk_token), "vk_secret_name": vk_secret or None,
            "telegram_session_present_but_not_opened": bool(tg_secret), "telegram_secret_name": tg_secret or None},
        "policy": {"telegram": "public t.me preview; shared Telethon session not opened",
            "vk": f"official VK API {VK_VERSION}", "copyright": "full text only in one-day workflow artifact",
            "anti_abuse": "stable UA, bounded jitter/backoff, no bypass or simulated interaction"},
        "errors": [{"platform": row["platform"], "source_id": row["source_id"], "error": row["error"]}
            for row in summaries if row.get("error")],
    }
    (persist / "run_summary.json").write_text(json.dumps(run, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (persist / "sources.snapshot.json").write_text(json.dumps(config, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"selected_posts": len(raw_rows), "successful_sources": run["successful_sources"],
        "complete_sources": run["complete_sources"], "errors": len(run["errors"]),
        "vk_token_present": bool(vk_token), "telegram_session_present_but_not_opened": bool(tg_secret)}, ensure_ascii=False))
    return 0 if raw_rows else 2


if __name__ == "__main__":
    raise SystemExit(main())
