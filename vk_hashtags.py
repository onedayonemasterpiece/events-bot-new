from __future__ import annotations

import re
from datetime import date
from typing import Iterable, Protocol


VK_ANNOUNCE_BASE_HASHTAGS: tuple[str, ...] = (
    "#анонс",
    "#анонс39",
    "#кудапойтиКалининград",
    "#афишакалининград",
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
    title: str | None
    description: str | None
    source_text: str | None


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


def city_afisha_hashtag(city: str | None) -> str | None:
    raw = str(city or "").strip()
    if not raw:
        return None
    compact = re.sub(r"[^\w]+", "", raw, flags=re.UNICODE).casefold()
    if not compact:
        return None
    return f"#афиша{compact}"


def vk_date_hashtags(value: str | date | None) -> list[str]:
    if isinstance(value, date):
        parsed = value
    else:
        parsed = _parse_iso_date(str(value or ""))
    if parsed is None:
        return []
    month = _MONTHS_GENITIVE[parsed.month]
    return [f"#{parsed.day}{month}", f"#{parsed.day}_{month}"]


def vk_date_hashtags_underscore_first(value: str | date | None) -> list[str]:
    tags = vk_date_hashtags(value)
    if len(tags) != 2:
        return tags
    return [tags[1], tags[0]]


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


_EVENT_TYPE_HASHTAG_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("#лекция", (r"\bлекци[яиюе]\b", r"\bлектори[йя]\b")),
    ("#спектакль", (r"\bспектакл[ьяеюи]\b", r"\bпостановк[аиуеой]\b")),
    ("#показ", (r"\bпоказ[а-я]*\b",)),
    ("#концерт", (r"\bконцерт[а-я]*\b",)),
    ("#выставка", (r"\bвыставк[аиуеой]\b", r"\bэкспозици[яиюе]\b")),
    ("#мастеркласс", (r"\bмастер[\s-]?класс[а-я]*\b", r"\bворкшоп[а-я]*\b")),
    ("#экскурсия", (r"\bэкскурси[яиюе]\b", r"\bпрогулк[аиуеой]\b")),
    ("#фестиваль", (r"\bфестивал[ьяеюи]\b",)),
    ("#ярмарка", (r"\bярмарк[аиуеой]\b", r"\bмаркет[а-я]*\b")),
    ("#кино", (r"\bкино\b", r"\bкинопоказ[а-я]*\b", r"\bфильм[а-я]*\b")),
    ("#встреча", (r"\bвстреч[аиуеой]\b",)),
    ("#презентация", (r"\bпрезентаци[яиюе]\b",)),
    ("#стендап", (r"\bстенда[пп][а-я]*\b", r"\bstand[\s-]?up\b")),
    ("#опера", (r"\bопера\b", r"\bопер[а-я]*\b")),
    ("#балет", (r"\bбалет[а-я]*\b",)),
    ("#мюзикл", (r"\bмюзикл[а-я]*\b",)),
    ("#квест", (r"\bквест[а-я]*\b",)),
    ("#вечеринка", (r"\bвечеринк[аиуеой]\b",)),
    ("#турнир", (r"\bтурнир[а-я]*\b", r"\bсоревновани[яейю]\b")),
    ("#чтение", (r"\bчтени[еяйю]\b", r"\bчитк[аиуеой]\b")),
)


def event_type_hashtags(*texts: str | None, limit: int = 3) -> list[str]:
    haystack = " ".join(str(t or "") for t in texts if str(t or "").strip()).casefold()
    if not haystack:
        return []
    out: list[str] = []
    for tag, patterns in _EVENT_TYPE_HASHTAG_PATTERNS:
        if any(re.search(pattern, haystack, flags=re.IGNORECASE | re.UNICODE) for pattern in patterns):
            out.append(tag)
            if len(out) >= limit:
                break
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


def build_vk_event_hashtags(
    event: HashtagEvent,
    *,
    festival_name: str | None = None,
    text: str | None = None,
) -> list[str]:
    tags = build_vk_announce_hashtags(
        cities=[getattr(event, "city", None), city_afisha_hashtag(getattr(event, "city", None))],
        dates=[getattr(event, "date", None)],
    )
    tags.extend(
        event_type_hashtags(
            getattr(event, "title", None),
            getattr(event, "description", None),
            getattr(event, "source_text", None),
            text,
        )
    )
    festival_tag = normalize_vk_festival_hashtag(
        festival_name if festival_name is not None else getattr(event, "festival", None)
    )
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


def _format_russian_date(value: date) -> str:
    return f"{value.day} {_MONTHS_GENITIVE[value.month]}"


def _format_russian_date_range(start: date, end: date) -> str:
    if start == end:
        return _format_russian_date(start)
    if start.month == end.month:
        return f"{start.day}-{end.day} {_MONTHS_GENITIVE[end.month]}"
    return f"{_format_russian_date(start)} - {_format_russian_date(end)}"


def build_vk_crumple_official_caption(
    *,
    cities: Iterable[str | None] = (),
    dates: Iterable[str | date | None] = (),
    tomorrow: date | None = None,
) -> str:
    parsed_dates: list[date] = []
    seen_dates: set[str] = set()
    for raw in dates:
        parsed = raw if isinstance(raw, date) else _parse_iso_date(str(raw or ""))
        if parsed is None:
            continue
        key = parsed.isoformat()
        if key in seen_dates:
            continue
        seen_dates.add(key)
        parsed_dates.append(parsed)
    parsed_dates.sort()

    if parsed_dates and tomorrow is not None and parsed_dates == [tomorrow]:
        title = "События на завтра"
    elif parsed_dates:
        title = f"События на {_format_russian_date_range(parsed_dates[0], parsed_dates[-1])}"
    else:
        title = "События"

    tags: list[str | None] = list(cities)
    for value in parsed_dates:
        tags.extend(vk_date_hashtags_underscore_first(value))
    hashtags = format_vk_hashtag_line(tags)
    if not hashtags:
        return title
    return f"{title} {hashtags}"


def is_vk_announce_hashtag_line(value: str | None) -> bool:
    text = str(value or "").strip()
    if not text or "#" not in text:
        return False
    tags = text.split()
    if len(tags) < 2 or not all(tag.startswith("#") for tag in tags):
        return False
    normalized = {tag.casefold() for tag in dedupe_vk_hashtags(tags)}
    return any(tag.casefold() in normalized for tag in VK_ANNOUNCE_BASE_HASHTAGS)
