from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable, Iterator
from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup, Tag

from .types import AvailabilityStatus, VolunteerOpportunity, canonical_json_hash, utc_now

_EVENT_PATH_RE = re.compile(r"^/event/(\d+)(?:/)?$")
_WS_RE = re.compile(r"\s+")
_EMAIL_RE = re.compile(r"(?i)(?<![\w.+-])[\w.+-]+@[\w.-]+\.[a-zа-яё]{2,}")
_PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?7|8)[\s().-]*(?:\d[\s().-]*){10}(?!\d)", re.IGNORECASE
)
_CLOSED_MARKERS = (
    "набор закрыт",
    "приём заявок завершен",
    "прием заявок завершен",
    "заявки больше не принимаются",
    "регистрация закрыта",
)
_OPEN_MARKERS = (
    "подать заявку",
    "откликнуться",
    "стать волонтёром",
    "стать волонтером",
)
_DESCRIPTION_LABELS = {"описание", "о мероприятии", "о добром деле"}
_HTTP_URL_RE = re.compile(r"https?://[^\s<>\"']+")
_PLATFORM_OR_RECOMMENDATION_HOSTS = {
    "dobro.ru",
    "www.dobro.ru",
    "dobro.press",
    "www.dobro.press",
    "edu.dobro.ru",
    "bot.dobro.ru",
    "storage.yandexcloud.net",
    "apps.apple.com",
    "play.google.com",
}
_RU_MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}
_MONTH_PATTERN = "|".join(_RU_MONTHS)
_DATE_PATTERNS = (
    # 21 января 2026 — 25 декабря 2026
    re.compile(
        rf"(?P<d1>\d{{1,2}})\s+(?P<m1>{_MONTH_PATTERN})\s+(?P<y1>20\d{{2}})\s*[–—-]\s*"
        rf"(?P<d2>\d{{1,2}})\s+(?P<m2>{_MONTH_PATTERN})\s+(?P<y2>20\d{{2}})",
        re.IGNORECASE,
    ),
    # 21 января – 25 декабря 2026
    re.compile(
        rf"(?P<d1>\d{{1,2}})\s+(?P<m1>{_MONTH_PATTERN})\s*[–—-]\s*"
        rf"(?P<d2>\d{{1,2}})\s+(?P<m2>{_MONTH_PATTERN})\s+(?P<y2>20\d{{2}})",
        re.IGNORECASE,
    ),
    # 3 – 10 августа 2026
    re.compile(
        rf"(?P<d1>\d{{1,2}})\s*[–—-]\s*(?P<d2>\d{{1,2}})\s+"
        rf"(?P<m2>{_MONTH_PATTERN})\s+(?P<y2>20\d{{2}})",
        re.IGNORECASE,
    ),
    # 18 июня 2026
    re.compile(
        rf"(?P<d1>\d{{1,2}})\s+(?P<m1>{_MONTH_PATTERN})\s+(?P<y1>20\d{{2}})",
        re.IGNORECASE,
    ),
)
_APPLICATION_DEADLINE_RE = re.compile(
    rf"(?:заявк\w*|регистрац\w*|отклик\w*)[^.\n]{{0,60}}?до\s+"
    rf"(?P<day>\d{{1,2}})\s+(?P<month>{_MONTH_PATTERN})\s+(?P<year>20\d{{2}})",
    re.IGNORECASE,
)


class DobroParseError(ValueError):
    """Raised when a source page cannot be parsed without a false success."""


@dataclass(slots=True, frozen=True)
class ParsedDateRange:
    start: date
    end: date
    raw: str


def _clean(value: Any) -> str:
    return _WS_RE.sub(" ", str(value or "")).strip()


def redact_public_excerpt(value: str, *, limit: int = 1_200) -> str:
    """Remove direct contact details before a bounded excerpt is persisted."""

    text = _clean(value)
    text = _EMAIL_RE.sub("[email удалён]", text)
    text = _PHONE_RE.sub("[телефон удалён]", text)
    return text[: max(1, limit)]


def canonicalize_event_url(value: str, *, base_url: str = "https://dobro.ru") -> str:
    absolute = urljoin(base_url, value)
    split = urlsplit(absolute)
    host = (split.hostname or "").casefold()
    if host not in {"dobro.ru", "www.dobro.ru"}:
        raise DobroParseError(f"unsupported Dobro host: {host or '<missing>'}")
    match = _EVENT_PATH_RE.fullmatch(split.path.rstrip("/") or "/")
    if not match:
        raise DobroParseError(f"not a canonical Dobro event URL: {value!r}")
    return urlunsplit(("https", "dobro.ru", f"/event/{match.group(1)}", "", ""))


def extract_event_urls(search_html: str, *, base_url: str = "https://dobro.ru") -> list[str]:
    soup = BeautifulSoup(search_html, "html.parser")
    found: dict[str, None] = {}
    for anchor in soup.find_all("a", href=True):
        try:
            canonical = canonicalize_event_url(str(anchor.get("href")), base_url=base_url)
        except DobroParseError:
            continue
        found.setdefault(canonical, None)
    return list(found)


def _json_ld_objects(soup: BeautifulSoup) -> Iterable[dict[str, Any]]:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            continue
        queue: list[Any] = payload if isinstance(payload, list) else [payload]
        while queue:
            current = queue.pop(0)
            if isinstance(current, dict):
                graph = current.get("@graph")
                if isinstance(graph, list):
                    queue.extend(graph)
                yield current
            elif isinstance(current, list):
                queue.extend(current)


def _event_json_ld(soup: BeautifulSoup) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for item in _json_ld_objects(soup):
        raw_type = item.get("@type")
        types = raw_type if isinstance(raw_type, list) else [raw_type]
        normalized = {_clean(value).casefold() for value in types if value}
        if normalized.intersection({"event", "socialevent", "sportsevent", "festival"}):
            candidates.append(item)
    return max(
        candidates,
        key=lambda item: len(json.dumps(item, ensure_ascii=False)),
        default={},
    )


def _parse_iso_date(value: Any) -> date | None:
    raw = _clean(value)
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).date()
    except ValueError:
        pass
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


def parse_russian_date_range(value: str) -> ParsedDateRange | None:
    text = _clean(value).casefold().replace("ё", "е")
    for pattern in _DATE_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        groups = match.groupdict()
        d1 = int(groups["d1"])
        d2 = int(groups.get("d2") or d1)
        m1_name = groups.get("m1") or groups.get("m2")
        m2_name = groups.get("m2") or m1_name
        y2 = int(groups.get("y2") or groups.get("y1") or 0)
        y1 = int(groups.get("y1") or y2)
        try:
            start = date(y1, _RU_MONTHS[str(m1_name)], d1)
            end = date(y2, _RU_MONTHS[str(m2_name)], d2)
        except (KeyError, TypeError, ValueError):
            continue
        if end < start:
            continue
        return ParsedDateRange(start=start, end=end, raw=match.group(0))
    return None


def _application_deadline_from_text(value: str) -> date | None:
    match = _APPLICATION_DEADLINE_RE.search(value.replace("ё", "е"))
    if not match:
        return None
    try:
        return date(
            int(match.group("year")),
            _RU_MONTHS[match.group("month").casefold()],
            int(match.group("day")),
        )
    except (KeyError, ValueError):
        return None


def _name_from(value: Any) -> str | None:
    if isinstance(value, dict):
        return _clean(value.get("name")) or None
    return _clean(value) or None


def _meta_content(soup: BeautifulSoup, *names: str) -> str | None:
    for name in names:
        tag = soup.find("meta", attrs={"name": name}) or soup.find(
            "meta", attrs={"property": name}
        )
        if tag and tag.get("content"):
            return _clean(tag.get("content")) or None
    return None


def _dedupe(values: Iterable[str], *, limit: int) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = _clean(raw)
        key = value.casefold()
        if not value or key in seen:
            continue
        seen.add(key)
        result.append(value)
        if len(result) >= limit:
            break
    return tuple(result)


