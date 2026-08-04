from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlsplit, urlunsplit

import requests


@dataclass(slots=True, frozen=True)
class SearchCandidate:
    provider: str
    url: str
    title: str
    snippet: str


class SearchProvider(Protocol):
    def search(self, query: str, *, limit: int = 8) -> list[SearchCandidate]: ...


def festival_source_query(
    *,
    name_hint: str,
    city: str | None,
    date_hint: str | None,
    organizer: str | None,
) -> str:
    parts = [f'"{name_hint.strip()}"']
    if city:
        parts.append(city.strip())
    if date_hint:
        parts.append(date_hint.strip())
    if organizer:
        parts.append(f'"{organizer.strip()}"')
    parts.extend(["фестиваль", "официальный сайт"])
    return " ".join(part for part in parts if part)


def _canonical_http_url(value: str) -> str | None:
    split = urlsplit(str(value or "").strip())
    if split.scheme not in {"http", "https"} or not split.hostname:
        return None
    return urlunsplit((split.scheme, split.netloc, split.path or "/", split.query, ""))


def _dedupe(rows: list[SearchCandidate], limit: int) -> list[SearchCandidate]:
    result: list[SearchCandidate] = []
    seen: set[str] = set()
    for row in rows:
        canonical = _canonical_http_url(row.url)
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        result.append(
            SearchCandidate(
                provider=row.provider,
                url=canonical,
                title=" ".join(row.title.split())[:300],
                snippet=" ".join(row.snippet.split())[:1000],
            )
        )
        if len(result) >= max(1, limit):
            break
    return result


class TavilySearchProvider:
    """Low-volume discovery using Tavily's free monthly credits.

    Results are candidates only. A separate HTTP/Playwright verifier must prove
    officiality and current-edition compatibility before any festival apply.
    """

    def __init__(self, api_key: str, *, timeout_seconds: float = 20.0) -> None:
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    def search(self, query: str, *, limit: int = 8) -> list[SearchCandidate]:
        response = requests.post(
            "https://api.tavily.com/search",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "query": query,
                "search_depth": "basic",
                "max_results": max(1, min(limit, 10)),
                "include_answer": False,
                "include_raw_content": False,
                "include_images": False,
                "include_usage": True,
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        rows = [
            SearchCandidate(
                provider="tavily",
                url=str(item.get("url") or ""),
                title=str(item.get("title") or ""),
                snippet=str(item.get("content") or ""),
            )
            for item in payload.get("results", [])
            if isinstance(item, dict)
        ]
        return _dedupe(rows, limit)


class SearxNGSearchProvider:
    """Candidate search through an operator-owned SearXNG instance."""

    def __init__(self, endpoint: str, *, timeout_seconds: float = 20.0) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def search(self, query: str, *, limit: int = 8) -> list[SearchCandidate]:
        response = requests.get(
            f"{self.endpoint}/search",
            params={"q": query, "format": "json", "language": "ru-RU"},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        rows = [
            SearchCandidate(
                provider="searxng",
                url=str(item.get("url") or ""),
                title=str(item.get("title") or ""),
                snippet=str(item.get("content") or ""),
            )
            for item in payload.get("results", [])
            if isinstance(item, dict)
        ]
        return _dedupe(rows, limit)


def _gemini_grounding_rows(response: Any) -> list[SearchCandidate]:
    rows: list[SearchCandidate] = []
    for candidate in getattr(response, "candidates", []) or []:
        metadata = getattr(candidate, "grounding_metadata", None)
        for chunk in getattr(metadata, "grounding_chunks", []) or []:
            web = getattr(chunk, "web", None)
            url = str(getattr(web, "uri", "") or "")
            if not url:
                continue
            rows.append(
                SearchCandidate(
                    provider="gemini_google_search",
                    url=url,
                    title=str(getattr(web, "title", "") or ""),
                    snippet="",
                )
            )
    return rows


class GeminiGroundedSearchProvider:
    """Closest replacement for Antigravity: Gemini + Google Search grounding.

    Only public festival hints should be sent. Grounded tool URLs are returned
    as candidates; generated prose is never accepted as a source URL.
    """

    def __init__(self, api_key: str, *, model: str = "gemini-2.5-flash-lite") -> None:
        self.api_key = api_key
        self.model = model

    def search(self, query: str, *, limit: int = 8) -> list[SearchCandidate]:
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=self.api_key)
        response = client.models.generate_content(
            model=self.model,
            contents=(
                "Найди возможные официальные источники текущей редакции события. "
                "Не считай агрегаторы официальными. Запрос: " + query
            ),
            config=types.GenerateContentConfig(
                tools=[types.Tool(google_search=types.GoogleSearch())],
                temperature=0,
            ),
        )
        return _dedupe(_gemini_grounding_rows(response), limit)
