from __future__ import annotations

import os
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Any


_LOCATION_NOISE_PREFIXES_RE = re.compile(
    r"^(?:"
    r"кинотеатр|"
    r"арт[- ]?пространство|"
    r"пространство|"
    r"арт[- ]?площадка|"
    r"культурн(?:ый|ое) центр|"
    r"центр|"
    r"площадка|"
    r"клуб"
    r")\s+",
    re.IGNORECASE,
)

_ADDRESS_ABBR_REPLACEMENTS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"(?i)\b(?:проспект|пр(?:\s*|-)?(?:кт|т)?|пр\.)\b"), "пр"),
    (re.compile(r"(?i)\b(?:улица|ул\.?)\b"), "ул"),
    (re.compile(r"(?i)\b(?:площадь|пл\.?)\b"), "пл"),
    (re.compile(r"(?i)\b(?:набережная|наб\.?)\b"), "наб"),
    (re.compile(r"(?i)\b(?:бульвар|бул\.?)\b"), "бульвар"),
    (re.compile(r"(?i)\b(?:переулок|пер\.?)\b"), "пер"),
)

_ADDRESS_NOISE_RE = re.compile(
    r"(?iu)\b(?:ул(?:ица)?|пр(?:оспект|осп)?|пр-?т|пр|пер(?:еулок)?|пер|б-р|бульвар|пл(?:ощадь)?|пл|наб(?:ережная)?|наб|д(?:ом)?)\b"
)


def normalize_venue_key(value: str | None) -> str:
    if not value:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = (
        text.replace("\u00ab", " ")
        .replace("\u00bb", " ")
        .replace("\u201c", " ")
        .replace("\u201d", " ")
        .replace("\u201e", " ")
        .replace("\u2019", " ")
        .replace('"', " ")
        .replace("'", " ")
        .replace("`", " ")
    )
    text = _LOCATION_NOISE_PREFIXES_RE.sub("", text).strip()
    text = text.casefold().replace("ё", "е")
    text = re.sub(r"[^\w\s]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_address_key(value: str | None, *, city: str | None = None) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    text = raw.casefold().replace("ё", "е")
    text = re.sub(r"[«»\"'`]", " ", text)
    text = re.sub(r"[^\w\s]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    for patt, repl in _ADDRESS_ABBR_REPLACEMENTS:
        text = patt.sub(repl, text)
    text = re.sub(r"\s+", " ", text).strip()
    text = _ADDRESS_NOISE_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    city_key = normalize_venue_key(city)
    if city_key:
        text = re.sub(rf"(?i)\b{re.escape(city_key)}\b", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"(?i)\b(?:г|город)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


@dataclass(frozen=True)
class KnownVenue:
    canonical_line: str
    name: str
    address: str
    city: str
    name_key: str
    line_key: str


@lru_cache(maxsize=1)
def read_known_venues_lines() -> tuple[str, ...]:
    loc_path = os.path.join("docs", "reference", "locations.md")
    if not os.path.exists(loc_path):
        return ()
    try:
        with open(loc_path, "r", encoding="utf-8") as f:
            locations = [
                line.strip()
                for line in f
                if line.strip() and not line.lstrip().startswith("#")
            ]
    except Exception:
        return ()
    return tuple(locations)


@lru_cache(maxsize=1)
def read_known_venues() -> tuple[KnownVenue, ...]:
    venues: list[KnownVenue] = []
    for line in read_known_venues_lines():
        parts = [p.strip() for p in line.split(",") if p.strip()]
        if not parts:
            continue
        name = parts[0]
        city = parts[-1] if len(parts) >= 2 else ""
        address = ", ".join(parts[1:-1]).strip() if len(parts) >= 3 else ""
        city_clean = city.lstrip("#").strip()
        venues.append(
            KnownVenue(
                canonical_line=line,
                name=name,
                address=address,
                city=city_clean,
                name_key=normalize_venue_key(name),
                line_key=normalize_venue_key(line),
            )
        )
    return tuple(venues)


@lru_cache(maxsize=1)
def read_known_venue_alias_lines() -> tuple[str, ...]:
    alias_path = os.path.join("docs", "reference", "location-aliases.md")
    if not os.path.exists(alias_path):
        return ()
    try:
        with open(alias_path, "r", encoding="utf-8") as f:
            aliases = [
                line.strip()
                for line in f
                if line.strip() and not line.lstrip().startswith("#")
            ]
    except Exception:
        return ()
    return tuple(aliases)


@lru_cache(maxsize=1)
def read_known_venue_aliases() -> dict[str, str]:
    aliases: dict[str, str] = {}
    for line in read_known_venue_alias_lines():
        if "=>" not in line:
            continue
        alias_raw, canonical_raw = line.split("=>", 1)
        alias_key = normalize_venue_key(alias_raw)
        canonical_key = normalize_venue_key(canonical_raw)
        if alias_key and canonical_key:
            aliases[alias_key] = canonical_key
    return aliases


@lru_cache(maxsize=1)
def read_known_place_aliases() -> dict[str, str]:
    aliases: dict[str, str] = {}
    path = os.path.join("docs", "reference", "kaliningrad_oblast_places.md")
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f]
    except Exception:
        return aliases
    for line in lines:
        if not line or line.startswith("#") or "=>" not in line:
            continue
        alias_raw, canonical_raw = line.split("=>", 1)
        alias_key = normalize_venue_key(alias_raw)
        canonical = canonical_raw.strip()
        if alias_key and canonical:
            aliases[alias_key] = canonical
    return aliases


@lru_cache(maxsize=1)
def read_known_place_names() -> dict[str, str]:
    names: dict[str, str] = {}
    path = os.path.join("docs", "reference", "kaliningrad_oblast_places.md")
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f]
    except Exception:
        return names
    for line in lines:
        if not line or line.startswith("#"):
            continue
        value = line.split("=>", 1)[1].strip() if "=>" in line else line
        key = normalize_venue_key(value)
        if key and value:
            names[key] = value
    return names


def canonicalize_known_place_name(value: str | None) -> str | None:
    key = normalize_venue_key(value)
    if not key:
        return None
    return read_known_place_aliases().get(key) or read_known_place_names().get(key)


def _filter_venues_by_city(
    venues: tuple[KnownVenue, ...],
    city: str | None,
) -> tuple[KnownVenue, ...]:
    city_key = normalize_venue_key(city)
    if not city_key:
        return venues
    by_city = [v for v in venues if normalize_venue_key(v.city) == city_key]
    if by_city:
        return tuple(by_city)
    return venues


def _match_known_venue_alias(
    key: str,
    venues: tuple[KnownVenue, ...],
) -> KnownVenue | None:
    canonical_key = read_known_venue_aliases().get(key)
    if not canonical_key:
        return None
    for venue in venues:
        if canonical_key in {venue.line_key, venue.name_key}:
            return venue
    return None


def match_known_venue_via_alias(value: str | None) -> KnownVenue | None:
    """Return a canonical venue iff ``value`` matches a curated alias key.

    Aliases are explicit dictionary entries that point to one canonical
    ``KnownVenue``; they should be trusted absolutely (override LLM-supplied
    address / city, ignore city filters) precisely because a human curator
    added them. ``match_known_venue`` returns the same object whether the
    match came from an alias or a fuzzy name; this helper exposes the alias
    path on its own so callers can decide to apply a forceful overwrite.
    """
    key = normalize_venue_key(value)
    if not key:
        return None
    venues = read_known_venues()
    if not venues:
        return None
    return _match_known_venue_alias(key, venues)


def match_known_venue_by_address(
    address: str | None, *, city: str | None = None
) -> KnownVenue | None:
    addr_key = normalize_address_key(address, city=city)
    if not addr_key:
        return None
    venues = read_known_venues()
    if not venues:
        return None

    venues = _filter_venues_by_city(venues, city)

    exact: list[KnownVenue] = []
    for venue in venues:
        if not venue.address:
            continue
        venue_key = normalize_address_key(venue.address, city=venue.city or city)
        if venue_key and venue_key == addr_key:
            exact.append(venue)
    if len(exact) == 1:
        return exact[0]

    fuzzy: list[KnownVenue] = []
    for venue in venues:
        if not venue.address:
            continue
        venue_key = normalize_address_key(venue.address, city=venue.city or city)
        if not venue_key:
            continue
        # Keep useful suffix tolerance (room/floor/building details) without
        # treating a house-number prefix as the same address.  Raw substring
        # matching made ``Советский 1`` match ``Советский 12`` and attached
        # ICAE to an unrelated casting at studio 809 (INC-2026-07-27).
        if (
            re.search(rf"(^|\s){re.escape(addr_key)}(\s|$)", venue_key)
            or re.search(rf"(^|\s){re.escape(venue_key)}(\s|$)", addr_key)
        ):
            fuzzy.append(venue)
    if len(fuzzy) == 1:
        return fuzzy[0]
    return None


def match_known_venue(value: str | None, *, city: str | None = None) -> KnownVenue | None:
    key = normalize_venue_key(value)
    if not key:
        return None
    venues = read_known_venues()
    if not venues:
        return None

    # Resolve aliases against the FULL venue set first. Aliases are explicit
    # dictionary entries that point to a single canonical venue; they must not
    # be silenced by a city filter that disagrees with the canonical row's city.
    # This was the root of the Янтарь-холл regression: 14 events arrived with
    # ``city='Калининград'`` (because their box-office sits in Калининград), and
    # the city filter dropped the canonical Светлогорск row before the alias
    # `янтарь-холл => Янтарь холл, Ленина 11, Светлогорск` could resolve.
    alias_match_full = _match_known_venue_alias(key, venues)
    if alias_match_full is not None:
        return alias_match_full

    venues = _filter_venues_by_city(venues, city)

    alias_match = _match_known_venue_alias(key, venues)
    if alias_match is not None:
        return alias_match

    for venue in venues:
        if key == venue.line_key or key == venue.name_key:
            return venue

    matches = [
        venue
        for venue in venues
        if venue.name_key and (key == venue.name_key or key in venue.name_key or venue.name_key in key)
    ]
    if len(matches) == 1:
        return matches[0]

    try:
        from difflib import SequenceMatcher
    except Exception:
        return None

    scored: list[tuple[float, KnownVenue]] = []
    for venue in venues:
        if not venue.name_key:
            continue
        scored.append((SequenceMatcher(None, key, venue.name_key).ratio(), venue))
    scored.sort(key=lambda item: item[0], reverse=True)
    if not scored:
        return None
    best_score, best_venue = scored[0]
    second = scored[1][0] if len(scored) > 1 else 0.0
    if best_score >= 0.92 and (best_score - second) >= 0.05:
        return best_venue

    stop = {
        "г",
        "город",
        "им",
        "имени",
        "ул",
        "улица",
        "проспект",
        "пр",
        "пл",
        "дом",
        "д",
        "к",
        "корп",
        "офис",
        "зал",
        "сцена",
        "студия",
        "кабинет",
        "этаж",
        "корпус",
        "помещение",
        "театр",
        "музей",
        "бар",
        "клуб",
        "центр",
        "пространство",
        "парк",
        "остров",
        "озеро",
        "школа",
        "библиотека",
        "галерея",
        "арена",
        "дворец",
        "резиденция",
        "городской",
        "городское",
        "музыкальная",
        "музыкальный",
        "музыкальное",
        "областная",
        "областной",
        "областное",
        "городская",
        "городские",
        "детская",
        "детский",
        "детское",
        "культура",
        "культуры",
        "искусство",
        "искусства",
        "молодежный",
        "молодежныйи",
        "молодежныи",
        "молодежный",
        # City names are useful filters, not venue identity tokens. Using them
        # as one-token fuzzy overlap can map an unknown source default such as
        # "Barn, ..., Калининград" to an unrelated venue whose official name
        # happens to include the city.
        "калининград",
        "светлогорск",
        "зеленоградск",
        "советск",
        "гусев",
        "черняховск",
    }

    def _tokens(s: str) -> set[str]:
        parts = re.findall(r"[a-zа-яё0-9]{4,}", s, flags=re.IGNORECASE)
        out = {p.casefold().replace("ё", "е") for p in parts if p}
        return {t for t in out if t not in stop and len(t) >= 4}

    key_tokens = _tokens(key)
    if not key_tokens:
        return None

    from collections import Counter

    freq: Counter[str] = Counter()
    venue_tokens: list[tuple[KnownVenue, set[str]]] = []
    for venue in venues:
        venue_token_set = _tokens(venue.name_key)
        venue_tokens.append((venue, venue_token_set))
        for token in venue_token_set:
            freq[token] += 1

    best: tuple[int, KnownVenue, set[str]] | None = None
    second_score = 0
    for venue, venue_token_set in venue_tokens:
        score = len(key_tokens & venue_token_set)
        if best is None or score > best[0]:
            if best is not None:
                second_score = max(second_score, best[0])
            best = (score, venue, venue_token_set)
        elif score > second_score:
            second_score = score

    if not best or best[0] <= 0:
        return None

    overlap = key_tokens & best[2]
    if best[0] >= 2 and (best[0] - second_score) >= 1:
        return best[1]
    if best[0] == 1 and (best[0] - second_score) >= 1:
        only = next(iter(overlap)) if overlap else ""
        if only and freq.get(only, 0) == 1:
            return best[1]
    return None


def _contains_normalized_phrase(haystack: str, needle: str) -> bool:
    if not haystack or not needle:
        return False
    return re.search(rf"(^|\s){re.escape(needle)}(\s|$)", haystack) is not None


def find_known_venue_in_text(text: str | None, *, city: str | None = None) -> KnownVenue | None:
    """Find a single known venue explicitly mentioned in free text.

    This is intentionally conservative: it returns a venue only when one
    canonical name, alias, or address is the strongest unique match.
    """
    text_key = normalize_venue_key(text)
    address_text_key = normalize_address_key(text, city=city)
    if not text_key and not address_text_key:
        return None

    venues = _filter_venues_by_city(read_known_venues(), city)
    if not venues:
        return None

    alias_by_key = read_known_venue_aliases()
    matches: list[tuple[int, KnownVenue]] = []

    for alias_key, canonical_key in alias_by_key.items():
        if not alias_key or len(alias_key) < 4:
            continue
        if not _contains_normalized_phrase(text_key, alias_key):
            continue
        for venue in venues:
            if canonical_key in {venue.line_key, venue.name_key}:
                matches.append((len(alias_key), venue))
                break

    for venue in venues:
        if venue.name_key and len(venue.name_key) >= 4:
            if _contains_normalized_phrase(text_key, venue.name_key):
                matches.append((len(venue.name_key), venue))
        if venue.address:
            address_key = normalize_address_key(venue.address, city=venue.city or city)
            if address_key and len(address_key) >= 4 and _contains_normalized_phrase(address_text_key, address_key):
                matches.append((len(address_key), venue))

    if not matches:
        return None

    best_score = max(score for score, _venue in matches)
    best = {venue for score, venue in matches if score == best_score}
    if len(best) == 1:
        return next(iter(best))
    return None


def normalise_event_location_from_reference(
    event_obj: dict[str, Any],
) -> KnownVenue | None:
    if not isinstance(event_obj, dict):
        return None
    raw_city = event_obj.get("city")
    raw_location_name = event_obj.get("location_name")
    raw_location_address = event_obj.get("location_address")

    canonical_city = canonicalize_known_place_name(raw_city)
    if canonical_city:
        event_obj["city"] = canonical_city
        raw_city = canonical_city

    canonicalized_location_place = False
    canonical_location_place = canonicalize_known_place_name(raw_location_name)
    if canonical_location_place and not str(raw_location_address or "").strip():
        city_key = normalize_venue_key(raw_city)
        name_key_for_place = normalize_venue_key(raw_location_name)
        canonical_key = normalize_venue_key(canonical_location_place)
        if not city_key or city_key in {name_key_for_place, canonical_key}:
            event_obj["location_name"] = canonical_location_place
            raw_location_name = canonical_location_place
            canonicalized_location_place = True
            if not raw_city:
                event_obj["city"] = canonical_location_place
                raw_city = canonical_location_place

    name_key = normalize_venue_key(raw_location_name)
    addr_key = normalize_address_key(raw_location_address, city=raw_city)
    if canonicalized_location_place:
        return None
    if (
        addr_key in {"мира 9", "мира 9 11"}
        and "библиотек" in name_key
        and "дом китобоя" not in name_key
    ):
        for venue in read_known_venues():
            if venue.name == "Научная библиотека":
                event_obj["location_name"] = venue.name
                event_obj["location_address"] = venue.address
                event_obj["city"] = venue.city
                return venue

    alias_venue = match_known_venue_via_alias(raw_location_name)
    if (
        alias_venue is not None
        and alias_venue.name
        and alias_venue.address
        and alias_venue.city
    ):
        event_obj["location_name"] = alias_venue.name
        event_obj["location_address"] = alias_venue.address
        event_obj["city"] = alias_venue.city
        return alias_venue

    venue_by_name = match_known_venue(raw_location_name, city=raw_city)
    venue_by_addr = match_known_venue_by_address(raw_location_address, city=raw_city)

    venue = venue_by_name
    addr_raw = str(raw_location_address or "").strip()
    addr_conflicts_with_name_match = False
    if venue_by_name is not None and addr_raw:
        raw_addr_key = normalize_address_key(addr_raw, city=raw_city)
        venue_addr_key = normalize_address_key(
            venue_by_name.address,
            city=venue_by_name.city or raw_city,
        )
        if raw_addr_key and venue_addr_key and raw_addr_key != venue_addr_key:
            addr_conflicts_with_name_match = True

    if venue_by_addr is not None and venue_by_addr != venue_by_name:
        venue = venue_by_addr
    elif addr_conflicts_with_name_match:
        return None

    if venue is None:
        return None

    event_obj["location_name"] = venue.name
    if venue.address:
        if (not addr_raw) or (
            normalize_address_key(addr_raw, city=raw_city)
            == normalize_address_key(venue.address, city=venue.city or raw_city)
        ):
            event_obj["location_address"] = venue.address
    if venue.city:
        # Once a source is matched to a single canonical venue from the reference list,
        # that venue's city is the authoritative value for the event.
        event_obj["city"] = venue.city
    return venue
