from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from urllib.parse import parse_qs, quote, unquote, urljoin, urlsplit

from bs4 import BeautifulSoup, Tag

from .dobro_common import (
    DobroParseError,
    ParsedDateRange,
    canonicalize_event_url,
    extract_event_urls as _extract_direct_event_urls,
    parse_russian_date_range,
    redact_public_excerpt,
)
from .dobro_page import is_in_target_region, parse_event_page


_VACANCY_TARGET_RE = re.compile(r"^/event/(?P<event_id>\d+)/vacancy/(?P<vacancy_id>\d+)/?$")


@dataclass(slots=True, frozen=True)
class VacancyTarget:
    event_id: str
    vacancy_id: str
    event_url: str
    application_url: str
    card_text: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def _clean(value: str) -> str:
    return " ".join(str(value or "").split()).strip()


def _canonical_application_url(event_id: str, vacancy_id: str) -> str:
    target = f"/event/{event_id}/vacancy/{vacancy_id}"
    return f"https://dobro.ru/login/?__target_path={quote(target, safe='')}"


def _vacancy_identity_from_href(value: str, *, base_url: str) -> tuple[str, str] | None:
    absolute = urljoin(base_url, value)
    split = urlsplit(absolute)
    if (split.hostname or "").casefold() not in {"dobro.ru", "www.dobro.ru"}:
        return None
    query = parse_qs(split.query)
    raw_target = next(iter(query.get("__target_path", [])), "")
    if not raw_target:
        return None
    target = unquote(str(raw_target))
    target_split = urlsplit(urljoin("https://dobro.ru", target))
    if (target_split.hostname or "").casefold() not in {"dobro.ru", "www.dobro.ru"}:
        return None
    match = _VACANCY_TARGET_RE.fullmatch(target_split.path)
    if not match:
        return None
    return match.group("event_id"), match.group("vacancy_id")


def _vacancy_card_text(anchor: Tag) -> str:
    candidates: list[str] = []
    for parent in anchor.parents:
        if not isinstance(parent, Tag):
            continue
        if parent.name not in {"article", "li", "div", "button"}:
            continue
        text = _clean(parent.get_text(" ", strip=True))
        folded = text.casefold().replace("ё", "е")
        if "отправить заявку" not in folded and "подать заявку" not in folded:
            continue
        if 20 <= len(text) <= 1_200:
            candidates.append(text)
        if len(candidates) >= 6:
            break
    return min(candidates, key=len, default="")


def extract_vacancy_targets(
    search_html: str, *, base_url: str = "https://dobro.ru"
) -> list[VacancyTarget]:
    """Extract unique active vacancy identities and exact source CTA URLs."""

    soup = BeautifulSoup(search_html, "html.parser")
    found: dict[str, VacancyTarget] = {}
    for anchor in soup.find_all("a", href=True):
        identity = _vacancy_identity_from_href(str(anchor.get("href")), base_url=base_url)
        if identity is None:
            continue
        event_id, vacancy_id = identity
        found.setdefault(
            vacancy_id,
            VacancyTarget(
                event_id=event_id,
                vacancy_id=vacancy_id,
                event_url=f"https://dobro.ru/event/{event_id}",
                application_url=_canonical_application_url(event_id, vacancy_id),
                card_text=_vacancy_card_text(anchor),
            ),
        )
    return list(found.values())


def extract_event_urls(search_html: str, *, base_url: str = "https://dobro.ru") -> list[str]:
    """Extract unique parent event URLs from event cards or vacancy CTAs."""

    found = {url: None for url in _extract_direct_event_urls(search_html, base_url=base_url)}
    for target in extract_vacancy_targets(search_html, base_url=base_url):
        found.setdefault(target.event_url, None)
    return list(found)


__all__ = [
    "DobroParseError",
    "ParsedDateRange",
    "VacancyTarget",
    "canonicalize_event_url",
    "extract_event_urls",
    "extract_vacancy_targets",
    "is_in_target_region",
    "parse_event_page",
    "parse_russian_date_range",
    "redact_public_excerpt",
]
