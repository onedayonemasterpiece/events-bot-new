from __future__ import annotations

import re
from datetime import date
from typing import Iterable, Protocol


VK_ANNOUNCE_BASE_HASHTAGS: tuple[str, ...] = (
    "#анонс",
    "#анонс39",
    "#кудапойтиКалининград",
    "#афишаКалининград",
)

_MONTHS_GENITIVE: tuple[str, ...] = (
    "",
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
)


class HashtagEvent(Protocol):
    date: str
    city: str | None
    festival: str | None


def _parse_iso_date(value: str | None) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    raw = raw.split("..", 1)[0].strip()
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def normalize_vk_hashtag(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    raw = raw.lstrip("#").strip()
    raw = re.sub(r"[^\w]+", "_", raw, flags=re.UNICODE)
    raw = re.sub(r"_+", "_", raw).strip("_")
    if not raw:
        return None
    return f"#{raw}"


def normalize_vk_festival_hashtag(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    raw = raw.lstrip("#").strip()
    raw = re.sub(r"[^\w]+", "", raw, flags=re.UNICODE)
    if not raw:
        return None
    return f"#{raw}"


def vk_date_hashtags(value: str | date | None) -> list[str]:
    if isinstance(value, date):
        parsed = value
    else:
        parsed = _parse_iso_date(str(value or ""))
    if parsed is None:
        return []
    month = _MONTHS_GENITIVE[parsed.month]
    return [f"#{parsed.day}{month}", f"#{parsed.day}_{month}"]


def dedupe_vk_hashtags(tags: Iterable[str | None]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in tags:
        tag = normalize_vk_hashtag(raw)
        if not tag:
            continue
        key = tag.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(tag)
    return out


def build_vk_announce_hashtags(
    *,
    cities: Iterable[str | None] = (),
    dates: Iterable[str | date | None] = (),
) -> list[str]:
    tags: list[str | None] = list(VK_ANNOUNCE_BASE_HASHTAGS)
    tags.extend(cities)
    for value in dates:
        tags.extend(vk_date_hashtags(value))
    return dedupe_vk_hashtags(tags)


def build_vk_event_hashtags(event: HashtagEvent) -> list[str]:
    tags = build_vk_announce_hashtags(
        cities=[getattr(event, "city", None)],
        dates=[getattr(event, "date", None)],
    )
    festival_tag = normalize_vk_festival_hashtag(getattr(event, "festival", None))
    if festival_tag:
        tags.append(festival_tag)
    return dedupe_vk_hashtags(tags)


def format_vk_hashtag_line(tags: Iterable[str | None]) -> str:
    return " ".join(dedupe_vk_hashtags(tags))


def build_vk_video_announce_caption(
    *,
    cities: Iterable[str | None] = (),
    dates: Iterable[str | date | None] = (),
    title: str = "Видеоанонс",
) -> str:
    title_text = (title or "Видеоанонс").strip() or "Видеоанонс"
    hashtags = format_vk_hashtag_line(
        build_vk_announce_hashtags(cities=cities, dates=dates)
    )
    if not hashtags:
        return title_text
    return f"{title_text}\n\n{hashtags}"


def is_vk_announce_hashtag_line(value: str | None) -> bool:
    text = str(value or "").strip()
    if not text or "#" not in text:
        return False
    tags = text.split()
    if len(tags) < 2 or not all(tag.startswith("#") for tag in tags):
        return False
    normalized = {tag.casefold() for tag in dedupe_vk_hashtags(tags)}
    return any(tag.casefold() in normalized for tag in VK_ANNOUNCE_BASE_HASHTAGS)
