from __future__ import annotations

import re
from urllib.parse import parse_qs, unquote, urljoin, urlsplit

from bs4 import BeautifulSoup

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


def _event_url_from_application_href(value: str, *, base_url: str) -> str | None:
    """Recover the parent Event URL from Dobro.ru's vacancy application link.

    The current `Вакансии` tab does not expose a normal event anchor. Its public
    CTA is a login URL whose `__target_path` points to
    `/event/<event_id>/vacancy/<vacancy_id>`. Discovery needs the parent event
    page for source-grounded title/organizer/date extraction, while the exact
    vacancy target remains available in the source HTML for the later
    application-link projection.
    """

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
    return f"https://dobro.ru/event/{match.group('event_id')}"


def extract_event_urls(search_html: str, *, base_url: str = "https://dobro.ru") -> list[str]:
    """Extract unique parent event URLs from event cards or vacancy CTAs."""

    found = {url: None for url in _extract_direct_event_urls(search_html, base_url=base_url)}
    soup = BeautifulSoup(search_html, "html.parser")
    for anchor in soup.find_all("a", href=True):
        event_url = _event_url_from_application_href(str(anchor.get("href")), base_url=base_url)
        if event_url:
            found.setdefault(event_url, None)
    return list(found)


__all__ = [
    "DobroParseError",
    "ParsedDateRange",
    "canonicalize_event_url",
    "extract_event_urls",
    "is_in_target_region",
    "parse_event_page",
    "parse_russian_date_range",
    "redact_public_excerpt",
]
