#!/usr/bin/env python3
"""Probe public VK HTML availability without collecting or logging post text."""
from __future__ import annotations

import json
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

UA = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36 EditorialCorpusResearch/1.0"
)


def probe(url: str) -> dict[str, object]:
    response = requests.get(
        url,
        timeout=30,
        headers={"User-Agent": UA, "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.5"},
        allow_redirects=True,
    )
    text = response.text
    soup = BeautifulSoup(text, "lxml")
    post_ids = sorted(set(re.findall(r"wall-?\d+_\d+", text)))
    return {
        "requested_host": requests.utils.urlparse(url).netloc,
        "final_host": requests.utils.urlparse(response.url).netloc,
        "status": response.status_code,
        "bytes": len(response.content),
        "title": (soup.title.get_text(" ", strip=True) if soup.title else "")[:120],
        "post_id_count": len(post_ids),
        "has_login_form": bool(soup.select_one("form[action*='login'], input[name='email'], input[name='pass']")),
        "has_wall_marker": bool(re.search(r"wall-?\d+_\d+", text)),
        "has_access_denied_marker": bool(re.search(r"(?i)access denied|доступ ограничен|войдите", text)),
    }


def main() -> int:
    config = json.loads(Path(__file__).with_name("sources.json").read_text(encoding="utf-8"))
    results = []
    for source in config["vk"]:
        screen = source["screen_name"]
        hint = source.get("group_id_hint")
        urls = [f"https://m.vk.com/{screen}", f"https://vk.com/{screen}"]
        if hint:
            urls.append(f"https://m.vk.com/wall-{int(hint)}?own=1")
        source_result = {"source_id": source["id"], "probes": []}
        for url in urls:
            try:
                source_result["probes"].append(probe(url))
            except Exception as exc:
                source_result["probes"].append({"requested_host": requests.utils.urlparse(url).netloc, "error": type(exc).__name__})
            time.sleep(1.0)
        results.append(source_result)
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
