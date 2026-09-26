#!/usr/bin/env python3
"""Read a bounded VK corpus from inside the production runtime.

The script is transported over stdin and executes in memory. It reads the first
available VK token from the runtime environment, calls only official read API
methods, applies bounded pauses/backoff, and emits a JSON envelope that never
contains credential values or request URLs with credentials.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

VK_VERSION = "5.199"
WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+(?:[-'][A-Za-zА-Яа-яЁё0-9]+)*")
TOKEN_NAMES = (
    "VK_USER_TOKEN",
    "VK_ACCESS_TOKEN4",
    "VK_ACCESS_TOKEN",
    "VK_API_TOKEN",
    "VK_SERVICE_TOKEN",
    "VK_TOKEN",
)


def first_env(names: Sequence[str]) -> tuple[str, str]:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return name, value
    return "", ""


def clean_text(value: str) -> str:
    value = (value or "").replace("\u00a0", " ")
    value = re.sub(r"[ \t]+", " ", value)
    return re.sub(r"\n{3,}", "\n\n", value).strip()


def normalized_sha256(text: str) -> str:
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
        "normalized_sha256": normalized_sha256(text),
    }


def pause(low: float, high: float) -> None:
    time.sleep(random.SystemRandom().uniform(low, high))


def vk_call(token: str, method: str, params: Mapping[str, Any]) -> Any:
    payload = dict(params)
    payload.update({"access_token": token, "v": VK_VERSION})
    encoded = urllib.parse.urlencode(payload).encode("ascii")
    for attempt in range(7):
        request = urllib.request.Request(
            f"https://api.vk.com/method/{method}",
            data=encoded,
            headers={"User-Agent": "EditorialCorpusResearch/1.0"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=35) as response:
                data = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, ValueError):
            if attempt < 6:
                base = min(20.0, 1.5 * (2 ** attempt))
                pause(base, base + 2.0)
                continue
            raise RuntimeError("vk_transport_failed")
        error = data.get("error") if isinstance(data, Mapping) else None
        if not error:
            return data.get("response") if isinstance(data, Mapping) else None
        code = int(error.get("error_code") or 0)
        if code in {6, 9, 10, 29} and attempt < 6:
            base = min(20.0, 1.5 * (2 ** attempt))
            pause(base, base + 2.0)
            continue
        raise RuntimeError(f"vk_error_{code}")
    raise RuntimeError("vk_retry_exhausted")


def metric(item: Mapping[str, Any], key: str) -> int | None:
    value = item.get(key)
    if isinstance(value, Mapping):
        value = value.get("count")
    return int(value) if isinstance(value, (int, float)) and value >= 0 else None


def collect_source(token: str, source: Mapping[str, Any], target: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    screen = str(source["screen_name"])
    found: dict[int, dict[str, Any]] = {}
    error = ""
    calls = 0
    group_id = 0
    try:
        resolved = vk_call(token, "utils.resolveScreenName", {"screen_name": screen})
        calls += 1
        if isinstance(resolved, Mapping) and str(resolved.get("type") or "") in {"group", "page", "event"}:
            group_id = int(resolved.get("object_id") or 0)
        if group_id <= 0:
            group_id = int(source.get("group_id_hint") or 0)
        if group_id <= 0:
            raise RuntimeError("unresolved_screen_name")

        for offset in range(0, 500, 100):
            response = vk_call(token, "wall.get", {
                "owner_id": -abs(group_id),
                "filter": "owner",
                "count": 100,
                "offset": offset,
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
                    "platform": "vk",
                    "post_id": post_id,
                    "source_public_id": screen,
                    "source_id": source["id"],
                    "source_title": source["title"],
                    "resolved_group_id": group_id,
                    "url": f"https://vk.com/wall-{group_id}_{post_id}",
                    "published_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts > 0 else "",
                    "text": text,
                    "views": metric(item, "views"),
                    "likes": metric(item, "likes"),
                    "comments": metric(item, "comments"),
                    "shares": metric(item, "reposts"),
                    "is_repost": bool(item.get("copy_history")),
                    "is_ad": bool(item.get("marked_as_ads")),
                    "has_media": bool(item.get("attachments")),
                    "features": basic_features(text),
                }
            eligible = [row for row in found.values() if len(str(row.get("text") or "")) >= 20]
            if len(eligible) >= target or len(items) < 100:
                break
            pause(0.55, 1.05)
    except Exception as exc:
        error = f"{type(exc).__name__}:{exc}"

    ordered = sorted(found.values(), key=lambda row: int(row["post_id"]), reverse=True)
    eligible = [row for row in ordered if len(str(row.get("text") or "")) >= 20]
    selected = eligible[:target]
    summary = {
        "platform": "vk",
        "source_id": source["id"],
        "title": source["title"],
        "public_id": screen,
        "resolved_group_id": group_id or None,
        "raw_scanned": len(ordered),
        "text_bearing": len(eligible),
        "selected": len(selected),
        "pages_or_calls": calls,
        "collection_path": "vk_official_api_inside_production_readonly_observer",
        "error": error,
    }
    return selected, summary


def main() -> int:
    payload = json.loads(sys.stdin.read())
    sources = payload.get("vk") if isinstance(payload, Mapping) else None
    target = int(payload.get("target") or 100) if isinstance(payload, Mapping) else 100
    target = max(1, min(150, target))
    if not isinstance(sources, list):
        raise SystemExit("invalid_sources")

    token_name, token = first_env(TOKEN_NAMES)
    if not token:
        print(json.dumps({
            "schema_version": 1,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "token_present": False,
            "token_name": None,
            "rows": [],
            "summaries": [{
                "platform": "vk",
                "source_id": str(source.get("id") or ""),
                "title": str(source.get("title") or ""),
                "public_id": str(source.get("screen_name") or ""),
                "selected": 0,
                "error": "missing_vk_token_inside_runtime",
            } for source in sources if isinstance(source, Mapping)],
        }, ensure_ascii=False, separators=(",", ":")))
        return 2

    rows: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    for index, source in enumerate(sources):
        if not isinstance(source, Mapping):
            continue
        posts, summary = collect_source(token, source, target)
        rows.extend(posts)
        summaries.append(summary)
        if index + 1 < len(sources):
            pause(1.5, 3.0)

    envelope = {
        "schema_version": 1,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "target_posts_per_source": target,
        "token_present": True,
        "token_name": token_name,
        "selected_posts": len(rows),
        "complete_sources": sum(int(row.get("selected") or 0) >= target for row in summaries),
        "successful_sources": sum(int(row.get("selected") or 0) > 0 for row in summaries),
        "platform_counts": dict(Counter(row["platform"] for row in rows)),
        "rows": rows,
        "summaries": summaries,
    }
    print(json.dumps(envelope, ensure_ascii=False, separators=(",", ":")))
    return 0 if rows else 2


if __name__ == "__main__":
    raise SystemExit(main())
