from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any, Iterator
from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup, Tag

from .dobro_common import (
    DobroParseError,
    _APPLICATION_DEADLINE_RE,
    _CLOSED_MARKERS,
    _DESCRIPTION_LABELS,
    _HTTP_URL_RE,
    _OPEN_MARKERS,
    _PLATFORM_OR_RECOMMENDATION_HOSTS,
    _application_deadline_from_text,
    _clean,
    _dedupe,
    _event_json_ld,
    _meta_content,
    _name_from,
    _parse_iso_date,
    canonicalize_event_url,
    parse_russian_date_range,
    redact_public_excerpt,
)
from .types import AvailabilityStatus, VolunteerOpportunity, canonical_json_hash, utc_now


def _location_from_json_ld(event: dict[str, Any]) -> tuple[str | None, str | None, str | None, str | None]:
    location = event.get("location")
    if not isinstance(location, dict):
        return None, None, None, None
    venue = _clean(location.get("name")) or None
    address = location.get("address")
    region: str | None = None
    city: str | None = None
    location_text: str | None = None
    if isinstance(address, dict):
        region = _clean(address.get("addressRegion")) or None
        city = _clean(address.get("addressLocality")) or None
        location_text = _clean(
            ", ".join(
                str(address.get(key) or "")
                for key in ("addressRegion", "addressLocality", "streetAddress")
                if address.get(key)
            )
        ) or None
    elif isinstance(address, str):
        location_text = _clean(address) or None
    return region, city, venue, location_text


def _visible_location_text(soup: BeautifulSoup, page_text: str) -> str | None:
    selectors = (
        "[data-location]",
        "[itemprop='address']",
        "[class*='location']",
        "[class*='address']",
    )
    candidates: list[str] = []
    for selector in selectors:
        for node in soup.select(selector):
            value = _clean(node.get_text(" ", strip=True))
            if 3 <= len(value) <= 300:
                candidates.append(value)
    for candidate in candidates:
        folded = candidate.casefold()
        if any(marker in folded for marker in ("обл", "область", "г ", "город", "р-н")):
            return candidate

    patterns = (
        r"Калининградская\s+(?:обл(?:асть)?\.?)[^|]{0,180}",
        r"(?:^|\s)г\.?\s+Калининград(?:[,;][^|]{0,140})?",
    )
    for pattern in patterns:
        match = re.search(pattern, page_text, flags=re.IGNORECASE)
        if match:
            return _clean(match.group(0))[:300]
    return None


def _derive_region_city(location_text: str | None) -> tuple[str | None, str | None]:
    text = _clean(location_text)
    if not text:
        return None, None
    folded = text.casefold().replace("ё", "е")
    region = "Калининградская область" if "калининградск" in folded and (
        "обл" in folded or "область" in folded
    ) else None
    city_match = re.search(
        r"(?:^|[,;]\s*|\b)г(?:ород)?\.?\s+([А-ЯЁ][А-Яа-яЁё\- ]{2,60})",
        text,
    )
    city = _clean(city_match.group(1)).rstrip(" ,;") if city_match else None
    if city:
        city = re.split(r"\s+(?:ул|пр-кт|проспект|пер|д)\b", city, maxsplit=1, flags=re.IGNORECASE)[0]
    if not city:
        for known in (
            "Калининград",
            "Светлогорск",
            "Зеленоградск",
            "Черняховск",
            "Советск",
            "Балтийск",
            "Янтарный",
            "Гурьевск",
            "Гусев",
            "Полесск",
        ):
            if known.casefold() in folded:
                city = known
                break
    return region, city


def _organizer_from_dom(soup: BeautifulSoup) -> str | None:
    for selector in ("[data-organizer]", "[itemprop='organizer']"):
        node = soup.select_one(selector)
        if node:
            value = _clean(node.get_text(" ", strip=True))
            if value:
                return value[:300]
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href") or "")
        if "/organizations/" not in href and "/organization/" not in href:
            continue
        value = _clean(anchor.get_text(" ", strip=True))
        if value and value.casefold() not in {"организатор", "организация"}:
            return value[:300]
    return None


def _iter_section_nodes(soup: BeautifulSoup, labels: set[str]) -> Iterator[Tag]:
    heading: Tag | None = None
    start_level = 7
    for candidate in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        value = _clean(candidate.get_text(" ", strip=True)).casefold().replace("ё", "е")
        if value in {label.replace("ё", "е") for label in labels}:
            heading = candidate
            start_level = int(candidate.name[1])
            break
    if heading is None:
        return
    for node in heading.find_all_next():
        if node is heading or not isinstance(node, Tag):
            continue
        if node.name and re.fullmatch(r"h[1-6]", node.name):
            if int(node.name[1]) <= start_level:
                break
        yield node


def _description_text(soup: BeautifulSoup) -> str | None:
    parts: list[str] = []
    seen: set[str] = set()
    for node in _iter_section_nodes(soup, _DESCRIPTION_LABELS):
        if node.name in {"script", "style", "svg", "img"}:
            continue
        value = _clean(node.get_text(" ", strip=True))
        if not value or value in seen:
            continue
        seen.add(value)
        parts.append(value)
        if sum(len(item) for item in parts) >= 1_800:
            break
    return _clean(" ".join(parts)) or None


def _extract_roles(soup: BeautifulSoup, event: dict[str, Any]) -> tuple[str, ...]:
    roles: list[str] = []
    for node in soup.select("[data-volunteer-role], [data-role='volunteer-function']"):
        roles.append(node.get_text(" ", strip=True))
    for selector in (".volunteer-role", ".function-item", ".vacancy-role"):
        for node in soup.select(selector):
            roles.append(node.get_text(" ", strip=True))
    sub_event = event.get("subEvent")
    if isinstance(sub_event, list):
        for item in sub_event:
            if isinstance(item, dict) and item.get("name"):
                roles.append(str(item["name"]))
    return _dedupe(roles, limit=12)


def _normalize_external_url(raw: str, canonical_url: str) -> str | None:
    absolute = urljoin(canonical_url, raw)
    split = urlsplit(absolute)
    if split.scheme not in {"http", "https"} or not split.hostname:
        return None
    host = split.hostname.casefold()
    if host in _PLATFORM_OR_RECOMMENDATION_HOSTS:
        return None
    return urlunsplit((split.scheme, split.netloc, split.path or "/", split.query, ""))


def _extract_external_links(
    soup: BeautifulSoup, event: dict[str, Any], canonical_url: str
) -> tuple[str, ...]:
    links: list[str] = []
    description_text_parts: list[str] = []
    for node in _iter_section_nodes(soup, _DESCRIPTION_LABELS):
        if node.name == "a" and node.get("href"):
            if normalized := _normalize_external_url(str(node.get("href")), canonical_url):
                links.append(normalized)
        if node.name not in {"script", "style", "svg"}:
            description_text_parts.append(node.get_text(" ", strip=True))
    for raw_url in _HTTP_URL_RE.findall(" ".join(description_text_parts)):
        if normalized := _normalize_external_url(raw_url.rstrip(".,;:!?)»"), canonical_url):
            links.append(normalized)
    for node in soup.select("[data-festival-source][href], [data-official-source][href]"):
        if normalized := _normalize_external_url(str(node.get("href")), canonical_url):
            links.append(normalized)
    same_as = event.get("sameAs")
    values = same_as if isinstance(same_as, list) else [same_as]
    for value in values:
        if normalized := _normalize_external_url(str(value or ""), canonical_url):
            links.append(normalized)
    return _dedupe(links, limit=20)


def _has_enabled_application_cta(soup: BeautifulSoup) -> bool:
    for node in soup.find_all(["a", "button"]):
        text = _clean(node.get_text(" ", strip=True)).casefold()
        if not any(marker in text for marker in _OPEN_MARKERS):
            continue
        classes = {str(item).casefold() for item in node.get("class", [])}
        disabled = (
            node.has_attr("disabled")
            or str(node.get("aria-disabled") or "").casefold() == "true"
            or "disabled" in classes
        )
        if not disabled:
            return True
    return False


def _availability(
    *,
    text: str,
    soup: BeautifulSoup,
    application_close_at: date | None,
    event_end_at: date | None,
    today: date,
) -> tuple[AvailabilityStatus, str]:
    lowered = text.casefold().replace("ё", "е")
    if any(marker.replace("ё", "е") in lowered for marker in _CLOSED_MARKERS):
        return AvailabilityStatus.CLOSED, "explicit_closed_marker"
    if application_close_at and application_close_at < today:
        return AvailabilityStatus.EXPIRED, "application_deadline_passed"
    if event_end_at and event_end_at < today:
        return AvailabilityStatus.EXPIRED, "event_period_passed"
    if _has_enabled_application_cta(soup):
        return AvailabilityStatus.OPEN, "enabled_application_cta"
    return AvailabilityStatus.UNKNOWN, "no_decisive_source_state"


def is_in_target_region(item: VolunteerOpportunity, region_name: str) -> bool:
    target = _clean(region_name).casefold().replace("ё", "е")
    target_root = target.replace("область", "").replace("обл.", "").replace("обл", "").strip()
    aliases = {target, target_root}
    if "калининград" in target:
        aliases.add("калининград")
    haystack = " ".join(
        value for value in (item.region, item.city, item.venue, item.location_text) if value
    ).casefold().replace("ё", "е")
    return any(alias and alias in haystack for alias in aliases)


def parse_event_page(
    html: str,
    *,
    source_url: str,
    checked_at: datetime | None = None,
    today: date | None = None,
) -> VolunteerOpportunity:
    canonical_url = canonicalize_event_url(source_url)
    external_id = canonical_url.rsplit("/", 1)[-1]
    checked = checked_at or utc_now()
    effective_today = today or checked.astimezone(timezone.utc).date()
    soup = BeautifulSoup(html, "html.parser")
    text = _clean(soup.get_text(" ", strip=True))
    if not text:
        raise DobroParseError(f"empty source page: {canonical_url}")

    event = _event_json_ld(soup)
    title = _clean(event.get("name"))
    if not title:
        heading = soup.find("h1") or soup.find("h2")
        title = _clean(heading.get_text(" ", strip=True) if heading else "")
    if not title:
        title = _meta_content(soup, "og:title", "twitter:title") or ""
    if not title:
        raise DobroParseError(f"missing event title: {canonical_url}")

    organizer_name = _name_from(event.get("organizer")) or _organizer_from_dom(soup)
    region, city, venue, location_text = _location_from_json_ld(event)
    if not location_text:
        location_text = _visible_location_text(soup, text)
    derived_region, derived_city = _derive_region_city(location_text)
    region = region or _meta_content(soup, "dobro:region") or derived_region
    city = city or _meta_content(soup, "dobro:city") or derived_city
    venue = venue or _meta_content(soup, "dobro:venue")

    application_open_at = _parse_iso_date(
        event.get("applicationStartDate")
        or event.get("validFrom")
        or _meta_content(soup, "application:start")
    )
    application_close_at = _parse_iso_date(
        event.get("applicationEndDate")
        or event.get("validThrough")
        or _meta_content(soup, "application:end")
    ) or _application_deadline_from_text(text)
    event_start_at = _parse_iso_date(event.get("startDate"))
    event_end_at = _parse_iso_date(event.get("endDate")) or event_start_at
    if not event_start_at:
        visible_range = parse_russian_date_range(text)
        if visible_range:
            event_start_at = visible_range.start
            event_end_at = visible_range.end

    roles = _extract_roles(soup, event)
    external_links = _extract_external_links(soup, event, canonical_url)
    description = (
        _clean(event.get("description"))
        or _description_text(soup)
        or _meta_content(soup, "description", "og:description")
        or text[:1_200]
    )
    source_excerpt = redact_public_excerpt(description)

    availability_status, availability_reason = _availability(
        text=text,
        soup=soup,
        application_close_at=application_close_at,
        event_end_at=event_end_at,
        today=effective_today,
    )
    semantic_payload = {
        "source_external_id": external_id,
        "title": title,
        "organizer_name": organizer_name,
        "region": region,
        "city": city,
        "venue": venue,
        "location_text": location_text,
        "application_open_at": application_open_at.isoformat() if application_open_at else None,
        "application_close_at": application_close_at.isoformat() if application_close_at else None,
        "event_start_at": event_start_at.isoformat() if event_start_at else None,
        "event_end_at": event_end_at.isoformat() if event_end_at else None,
        "roles": roles,
        "external_links": external_links,
        "source_excerpt": source_excerpt,
    }
    availability_payload = {
        "source_external_id": external_id,
        "availability_status": availability_status.value,
        "availability_reason": availability_reason,
        "application_close_at": semantic_payload["application_close_at"],
        "event_end_at": semantic_payload["event_end_at"],
    }
    return VolunteerOpportunity(
        source_type="dobro_ru",
        source_external_id=external_id,
        canonical_url=canonical_url,
        title=title,
        organizer_name=organizer_name,
        region=region,
        city=city,
        venue=venue,
        location_text=location_text,
        application_open_at=application_open_at,
        application_close_at=application_close_at,
        event_start_at=event_start_at,
        event_end_at=event_end_at,
        roles=roles,
        external_links=external_links,
        source_excerpt=source_excerpt,
        availability_status=availability_status,
        availability_reason=availability_reason,
        checked_at=checked,
        semantic_hash=canonical_json_hash(semantic_payload),
        availability_hash=canonical_json_hash(availability_payload),
    )
