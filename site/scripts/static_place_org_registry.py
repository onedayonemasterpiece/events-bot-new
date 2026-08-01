"""Exact, evidence-only resolver for the static place/organization registry.

The registry deliberately does not inspect event titles, topics, descriptions or
festival names.  Organization and venue roles are resolved independently so an
official offsite event remains an organization's event without becoming an
event at that organization's home venue.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlsplit


SCHEMA_VERSION = "static_place_organization_registry_v1"
DEFAULT_REGISTRY_PATH = (
    Path(__file__).resolve().parents[1] / "src" / "data" / "placeOrganizationRegistry.json"
)
DEFAULT_MEDALLION_REGISTRY_PATH = (
    Path(__file__).resolve().parents[1] / "src" / "data" / "organizerMedallions.json"
)

_ID_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_KINDS = {"place", "organization", "mixed"}
_STATUSES = {"public", "candidate", "dormant", "excluded"}
_SOURCE_LIST_KEYS = (
    "sources",
    "event_sources",
    "source_records",
)
_SOURCE_URL_KEYS = ("source_url", "url")
_PARSER_KEYS = ("source_type", "parser_type", "parser_id", "parser")
_TELEGRAM_KEYS = ("telegram_username", "tg_username")
_VK_SCREEN_KEYS = ("vk_screen_name", "vk_username")
_VK_GROUP_KEYS = ("vk_group_id", "vk_owner_id")
_DOMAIN_KEYS = ("source_domain", "domain", "hostname")
_REASON_ORDER = {
    "official_source": 0,
    "organizer": 1,
    "venue": 2,
}


class RegistryValidationError(ValueError):
    """Raised when checked-in registry data violates the v1 contract."""


def _normalized_text(value: Any) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value)).replace("\u00a0", " ")
    return " ".join(text.split()).casefold()


def _normalized_identifier(value: Any) -> str:
    return _normalized_text(value).lstrip("@").strip("/")


def _normalized_parser_type(value: Any) -> str:
    normalized = _normalized_identifier(value)
    if normalized.startswith("parser:"):
        normalized = normalized.removeprefix("parser:")
    return normalized


def _normalized_vk_group_id(value: Any) -> str:
    normalized = _normalized_text(value)
    if normalized.startswith("-"):
        normalized = normalized[1:]
    return normalized if normalized.isdigit() else ""


def _normalized_domain(value: Any) -> str:
    raw = _normalized_text(value).strip(".")
    if not raw:
        return ""
    try:
        return raw.encode("idna").decode("ascii").lower()
    except UnicodeError:
        return ""


def _url_hostname(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    candidate = raw if "://" in raw else f"//{raw}"
    try:
        hostname = urlsplit(candidate).hostname or ""
    except ValueError:
        return ""
    return _normalized_domain(hostname)


def _official_domain_matches(candidate: str, official: str) -> bool:
    return bool(
        candidate
        and official
        and (candidate == official or candidate.endswith(f".{official}"))
    )


def _as_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [str(item) for item in value if item is not None]
    return [str(value)]


def _require_string_list(value: Any, path: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise RegistryValidationError(f"{path} must be a list of strings")
    return value


def _load_medallion_slugs(path: Path) -> set[str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RegistryValidationError(f"cannot read medallion registry {path}: {exc}") from exc
    items = payload.get("items") if isinstance(payload, Mapping) else None
    if not isinstance(items, list):
        raise RegistryValidationError(f"medallion registry {path} has no items list")
    return {
        item["slug"]
        for item in items
        if isinstance(item, Mapping) and isinstance(item.get("slug"), str)
    }


def validate_registry(
    registry: Mapping[str, Any],
    *,
    medallion_registry_path: Path | str | None = DEFAULT_MEDALLION_REGISTRY_PATH,
    enforce_v1_contract: bool = True,
) -> None:
    """Validate schema, stable identities, v1 counts and medallion references."""

    if registry.get("schemaVersion") != SCHEMA_VERSION:
        raise RegistryValidationError(f"schemaVersion must be {SCHEMA_VERSION!r}")
    if registry.get("registryVersion") != 1:
        raise RegistryValidationError("registryVersion must be 1")
    entities = registry.get("entities")
    if not isinstance(entities, list):
        raise RegistryValidationError("entities must be a list")

    seen_ids: set[str] = set()
    seen_slugs: set[str] = set()
    medallion_slugs = (
        _load_medallion_slugs(Path(medallion_registry_path))
        if medallion_registry_path is not None
        else None
    )

    for index, entity in enumerate(entities):
        path = f"entities[{index}]"
        if not isinstance(entity, Mapping):
            raise RegistryValidationError(f"{path} must be an object")
        entity_id = entity.get("id")
        slug = entity.get("slug")
        if not isinstance(entity_id, str) or not _ID_RE.fullmatch(entity_id):
            raise RegistryValidationError(f"{path}.id must be a stable kebab-case ID")
        if not isinstance(slug, str) or not _ID_RE.fullmatch(slug):
            raise RegistryValidationError(f"{path}.slug must be kebab-case")
        if entity_id in seen_ids:
            raise RegistryValidationError(f"duplicate entity id: {entity_id}")
        if slug in seen_slugs:
            raise RegistryValidationError(f"duplicate entity slug: {slug}")
        seen_ids.add(entity_id)
        seen_slugs.add(slug)

        kind = entity.get("kind")
        if kind not in _KINDS:
            raise RegistryValidationError(f"{path}.kind must be one of {sorted(_KINDS)}")
        if entity.get("status") not in _STATUSES:
            raise RegistryValidationError(f"{path}.status must be one of {sorted(_STATUSES)}")
        if not isinstance(entity.get("canonicalName"), str) or not entity["canonicalName"].strip():
            raise RegistryValidationError(f"{path}.canonicalName must be a non-empty string")
        _require_string_list(entity.get("organizerAliases"), f"{path}.organizerAliases")

        bindings = entity.get("sourceBindings")
        if not isinstance(bindings, Mapping):
            raise RegistryValidationError(f"{path}.sourceBindings must be an object")
        for key in (
            "parserTypes",
            "telegramUsernames",
            "vkScreenNames",
            "vkGroupIds",
            "domains",
        ):
            values = _require_string_list(bindings.get(key), f"{path}.sourceBindings.{key}")
            normalized = {
                _normalized_domain(item) if key == "domains" else _normalized_identifier(item)
                for item in values
            }
            if "" in normalized or len(normalized) != len(values):
                raise RegistryValidationError(f"{path}.sourceBindings.{key} has invalid/duplicate values")

        venue = entity.get("canonicalVenue")
        if kind == "organization" and venue is not None:
            raise RegistryValidationError(f"{path}: organization-only entities cannot own a venue")
        if kind in {"place", "mixed"}:
            if not isinstance(venue, Mapping):
                raise RegistryValidationError(f"{path}.canonicalVenue must be an object")
            for key in ("venueId", "name", "address", "city"):
                if not isinstance(venue.get(key), str) or not venue[key].strip():
                    raise RegistryValidationError(f"{path}.canonicalVenue.{key} is required")
            for key in ("approvedNameAliases", "approvedLocationAliases"):
                _require_string_list(venue.get(key), f"{path}.canonicalVenue.{key}")
            tuples = venue.get("approvedTuples")
            if not isinstance(tuples, list):
                raise RegistryValidationError(f"{path}.canonicalVenue.approvedTuples must be a list")
            for tuple_index, approved_tuple in enumerate(tuples):
                if not isinstance(approved_tuple, Mapping) or any(
                    not isinstance(approved_tuple.get(key), str) or not approved_tuple[key].strip()
                    for key in ("name", "address", "city")
                ):
                    raise RegistryValidationError(
                        f"{path}.canonicalVenue.approvedTuples[{tuple_index}] must contain name/address/city"
                    )

        flags = entity.get("flags")
        if not isinstance(flags, Mapping):
            raise RegistryValidationError(f"{path}.flags must be an object")
        for flag in ("official_theatre", "venue_page_candidate", "medieval_site"):
            if not isinstance(flags.get(flag), bool):
                raise RegistryValidationError(f"{path}.flags.{flag} must be boolean")
        if flags["official_theatre"] and kind == "place":
            raise RegistryValidationError(f"{path}: a place-only entity cannot be an official theatre")
        if flags["venue_page_candidate"] and kind == "organization":
            raise RegistryValidationError(f"{path}: an organization-only entity cannot be a venue page")

        medallion_slug = entity.get("medallionSlug")
        if medallion_slug is not None:
            if not isinstance(medallion_slug, str) or not medallion_slug:
                raise RegistryValidationError(f"{path}.medallionSlug must be null or a non-empty string")
            if medallion_slugs is not None and medallion_slug not in medallion_slugs:
                raise RegistryValidationError(
                    f"{path}.medallionSlug references unknown medallion {medallion_slug!r}"
                )
        elif not isinstance(entity.get("medallionReview"), str) or not entity["medallionReview"].strip():
            raise RegistryValidationError(f"{path}: a null medallion needs a review reason")

    exclusions = registry.get("reviewedExclusions")
    if not isinstance(exclusions, list) or not exclusions:
        raise RegistryValidationError("reviewedExclusions must be a non-empty list")
    exclusion_ids: set[str] = set()
    for index, exclusion in enumerate(exclusions):
        if not isinstance(exclusion, Mapping):
            raise RegistryValidationError(f"reviewedExclusions[{index}] must be an object")
        exclusion_id = exclusion.get("id")
        if not isinstance(exclusion_id, str) or not _ID_RE.fullmatch(exclusion_id):
            raise RegistryValidationError(f"reviewedExclusions[{index}].id must be kebab-case")
        if exclusion_id in exclusion_ids:
            raise RegistryValidationError(f"duplicate reviewed exclusion id: {exclusion_id}")
        exclusion_ids.add(exclusion_id)
        for key in ("label", "decision", "reason"):
            if not isinstance(exclusion.get(key), str) or not exclusion[key].strip():
                raise RegistryValidationError(f"reviewedExclusions[{index}].{key} is required")

    if enforce_v1_contract:
        kind_counts = {kind: sum(entity["kind"] == kind for entity in entities) for kind in _KINDS}
        official_theatres = sum(entity["flags"]["official_theatre"] for entity in entities)
        venue_candidates = sum(entity["flags"]["venue_page_candidate"] for entity in entities)
        if len(entities) != 11 or kind_counts != {"mixed": 7, "organization": 1, "place": 3}:
            raise RegistryValidationError(
                f"v1 requires 11 entities (7 mixed, 1 organization, 3 place), got {kind_counts}"
            )
        if official_theatres != 8:
            raise RegistryValidationError(f"v1 requires exactly 8 official theatres, got {official_theatres}")
        if venue_candidates != 6:
            raise RegistryValidationError(f"v1 requires exactly 6 venue page candidates, got {venue_candidates}")


def load_registry(
    path: Path | str = DEFAULT_REGISTRY_PATH,
    *,
    medallion_registry_path: Path | str | None = DEFAULT_MEDALLION_REGISTRY_PATH,
) -> dict[str, Any]:
    """Load and validate a registry document."""

    registry_path = Path(path)
    try:
        registry = json.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RegistryValidationError(f"cannot read registry {registry_path}: {exc}") from exc
    if not isinstance(registry, dict):
        raise RegistryValidationError("registry root must be an object")
    validate_registry(registry, medallion_registry_path=medallion_registry_path)
    return registry


def registry_hash(registry: Mapping[str, Any]) -> str:
    """Return a deterministic SHA-256 for the semantic JSON document."""

    canonical = json.dumps(
        registry,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _source_records(event: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    records: list[Mapping[str, Any]] = [event]
    for key in _SOURCE_LIST_KEYS:
        value = event.get(key)
        if isinstance(value, Mapping):
            records.append(value)
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            records.extend(item for item in value if isinstance(item, Mapping))
    return records


def _source_urls(event: Mapping[str, Any], records: Iterable[Mapping[str, Any]]) -> list[str]:
    urls: list[str] = []
    for value in _as_string_list(event.get("source_urls")):
        if value:
            urls.append(value)
    for record in records:
        for key in _SOURCE_URL_KEYS:
            value = record.get(key)
            if isinstance(value, str) and value:
                urls.append(value)
        for value in _as_string_list(record.get("source_urls")):
            if value:
                urls.append(value)
    return list(dict.fromkeys(urls))


def _telegram_username_from_url(url: str) -> str:
    if _url_hostname(url) not in {"t.me", "telegram.me"}:
        return ""
    path = [segment for segment in urlsplit(url).path.split("/") if segment]
    if path and path[0].casefold() == "s":
        path = path[1:]
    if not path or path[0].casefold() in {"c", "joinchat", "+"}:
        return ""
    return _normalized_identifier(path[0])


def _vk_identity_from_url(url: str) -> tuple[str, str]:
    if _url_hostname(url) not in {"vk.com", "www.vk.com", "m.vk.com"}:
        return "", ""
    path = [segment for segment in urlsplit(url).path.split("/") if segment]
    if not path:
        return "", ""
    first = path[0].casefold()
    wall_match = re.fullmatch(r"wall-([0-9]+)_[0-9]+", first)
    if wall_match:
        return "", wall_match.group(1)
    if re.fullmatch(r"(?:club|public)[0-9]+", first):
        digits = re.sub(r"^(?:club|public)", "", first)
        return "", digits
    if first in {"wall", "feed", "search", "video", "photo"}:
        return "", ""
    return _normalized_identifier(first), ""


def _source_evidence(event: Mapping[str, Any]) -> dict[str, set[str]]:
    records = _source_records(event)
    evidence: dict[str, set[str]] = {
        "parser": set(),
        "telegram": set(),
        "vk_screen": set(),
        "vk_group": set(),
        "domain": set(),
    }
    for record in records:
        for key in _PARSER_KEYS:
            for value in _as_string_list(record.get(key)):
                normalized = _normalized_parser_type(value)
                if normalized:
                    evidence["parser"].add(normalized)
        for key in _TELEGRAM_KEYS:
            for value in _as_string_list(record.get(key)):
                normalized = _normalized_identifier(value)
                if normalized:
                    evidence["telegram"].add(normalized)
        for key in _VK_SCREEN_KEYS:
            for value in _as_string_list(record.get(key)):
                normalized = _normalized_identifier(value)
                if normalized:
                    evidence["vk_screen"].add(normalized)
        for key in _VK_GROUP_KEYS:
            for value in _as_string_list(record.get(key)):
                normalized = _normalized_vk_group_id(value)
                if normalized:
                    evidence["vk_group"].add(normalized)
        for key in _DOMAIN_KEYS:
            for value in _as_string_list(record.get(key)):
                normalized = _normalized_domain(value)
                if normalized:
                    evidence["domain"].add(normalized)

        platform = _normalized_identifier(
            record.get("platform", record.get("source_platform"))
        )
        if not platform:
            source_type = _normalized_parser_type(record.get("source_type"))
            if source_type in {"telegram", "tg", "vk"}:
                platform = source_type
        if platform in {"telegram", "tg"}:
            normalized = _normalized_identifier(record.get("username"))
            if normalized:
                evidence["telegram"].add(normalized)
        elif platform == "vk":
            normalized = _normalized_identifier(record.get("username"))
            if normalized:
                evidence["vk_screen"].add(normalized)
            normalized_group = _normalized_vk_group_id(record.get("owner_id"))
            if normalized_group:
                evidence["vk_group"].add(normalized_group)

    for url in _source_urls(event, records):
        hostname = _url_hostname(url)
        if hostname:
            evidence["domain"].add(hostname)
        telegram = _telegram_username_from_url(url)
        if telegram:
            evidence["telegram"].add(telegram)
        vk_screen, vk_group = _vk_identity_from_url(url)
        if vk_screen:
            evidence["vk_screen"].add(vk_screen)
        if vk_group:
            evidence["vk_group"].add(vk_group)
    return evidence


def _official_source_reasons(
    entity: Mapping[str, Any], evidence: Mapping[str, set[str]]
) -> list[dict[str, str]]:
    bindings = entity["sourceBindings"]
    reasons: list[dict[str, str]] = []
    exact_binding_sets = (
        ("parser", "parserTypes", _normalized_parser_type),
        ("telegram", "telegramUsernames", _normalized_identifier),
        ("vk_screen", "vkScreenNames", _normalized_identifier),
        ("vk_group", "vkGroupIds", _normalized_vk_group_id),
    )
    for evidence_key, binding_key, normalizer in exact_binding_sets:
        accepted = {normalizer(value) for value in bindings[binding_key]}
        for match in sorted(evidence[evidence_key] & accepted):
            label = "vk" if evidence_key.startswith("vk_") else evidence_key
            reasons.append({"code": "official_source", "binding": f"{label}:{match}"})

    official_domains = {_normalized_domain(value) for value in bindings["domains"]}
    for candidate in sorted(evidence["domain"]):
        for official in sorted(official_domains):
            if _official_domain_matches(candidate, official):
                reasons.append(
                    {"code": "official_source", "binding": f"domain:{candidate}"}
                )
                break
    return reasons


def _organizer_names(event: Mapping[str, Any]) -> set[str]:
    result: set[str] = set()
    for key in ("organizer_name", "organizer_names", "organizer", "organizers"):
        value = event.get(key)
        values = value if isinstance(value, list) else [value]
        for item in values:
            if isinstance(item, Mapping):
                item = item.get("name")
            normalized = _normalized_text(item)
            if normalized:
                result.add(normalized)
    return result


def _structured_venue_candidates(event: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    candidates: list[Mapping[str, Any]] = [event]
    for key in ("venue", "location"):
        value = event.get(key)
        if isinstance(value, Mapping):
            candidates.append(value)
    return candidates


def _venue_reasons(entity: Mapping[str, Any], event: Mapping[str, Any]) -> list[dict[str, str]]:
    venue = entity.get("canonicalVenue")
    if not isinstance(venue, Mapping):
        return []
    reasons: list[dict[str, str]] = []

    accepted_ids = {_normalized_identifier(entity["id"]), _normalized_identifier(venue["venueId"])}
    for candidate in _structured_venue_candidates(event):
        for key in ("venue_id", "location_id"):
            venue_id = _normalized_identifier(candidate.get(key))
            if venue_id and venue_id in accepted_ids:
                reasons.append({"code": "venue", "binding": f"canonical_venue_id:{venue_id}"})

    canonical_tuple = tuple(
        _normalized_text(venue[key]) for key in ("name", "address", "city")
    )
    approved_tuples = {
        tuple(_normalized_text(item[key]) for key in ("name", "address", "city"))
        for item in venue["approvedTuples"]
    }
    canonical_names = {_normalized_text(venue["name"])}
    alias_names = {_normalized_text(value) for value in venue["approvedNameAliases"]}

    for candidate in _structured_venue_candidates(event):
        name = candidate.get("venue_name", candidate.get("location_name", candidate.get("name")))
        address = candidate.get(
            "venue_address", candidate.get("location_address", candidate.get("address"))
        )
        city = candidate.get("venue_city", candidate.get("city"))
        normalized = tuple(_normalized_text(value) for value in (name, address, city))
        if all(normalized):
            if normalized == canonical_tuple:
                reasons.append(
                    {
                        "code": "venue",
                        "binding": "canonical_tuple:" + "|".join(normalized),
                    }
                )
            elif normalized in approved_tuples:
                reasons.append(
                    {
                        "code": "venue",
                        "binding": "approved_tuple:" + "|".join(normalized),
                    }
                )
        elif normalized[0] and not normalized[1] and not normalized[2]:
            if normalized[0] in canonical_names:
                reasons.append(
                    {"code": "venue", "binding": f"canonical_name:{normalized[0]}"}
                )
            elif normalized[0] in alias_names:
                reasons.append(
                    {"code": "venue", "binding": f"approved_name:{normalized[0]}"}
                )

    raw_locations: list[str] = []
    for key in ("location", "location_text", "venue_text"):
        value = event.get(key)
        if isinstance(value, str):
            raw_locations.append(value)
    canonical_location = _normalized_text(
        f"{venue['name']}, {venue['address']}, {venue['city']}"
    )
    approved_locations = {
        _normalized_text(value) for value in venue["approvedLocationAliases"]
    }
    for value in raw_locations:
        normalized = _normalized_text(value)
        if normalized == canonical_location:
            reasons.append(
                {"code": "venue", "binding": f"canonical_location:{normalized}"}
            )
        elif normalized in approved_locations:
            reasons.append(
                {"code": "venue", "binding": f"approved_location:{normalized}"}
            )
    return _deduplicate_reasons(reasons)


def _deduplicate_reasons(reasons: Iterable[Mapping[str, str]]) -> list[dict[str, str]]:
    unique = {(reason["code"], reason["binding"]) for reason in reasons}
    return [
        {"code": code, "binding": binding}
        for code, binding in sorted(
            unique,
            key=lambda item: (_REASON_ORDER.get(item[0], 99), item[1]),
        )
    ]


def _membership(entity: Mapping[str, Any], reasons: Iterable[Mapping[str, str]]) -> dict[str, Any]:
    return {
        "entity_id": entity["id"],
        "slug": entity["slug"],
        "reasons": _deduplicate_reasons(reasons),
    }


def resolve_event_memberships(
    event: Mapping[str, Any],
    registry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Resolve exact organization, venue and official-theatre memberships.

    Only source fields, organizer fields and structured/raw venue fields are
    read.  A source match can only create an organization membership; a venue
    membership always requires explicit venue evidence.
    """

    if registry is None:
        registry = load_registry()
    evidence = _source_evidence(event)
    organizer_names = _organizer_names(event)

    organization_memberships: list[dict[str, Any]] = []
    venue_memberships: list[dict[str, Any]] = []
    theatre_by_id: dict[str, dict[str, Any]] = {}

    for entity in registry["entities"]:
        organization_reasons: list[dict[str, str]] = []
        if entity["kind"] in {"organization", "mixed"}:
            organization_reasons.extend(_official_source_reasons(entity, evidence))
            accepted_organizers = {
                _normalized_text(entity["canonicalName"]),
                *(_normalized_text(value) for value in entity["organizerAliases"]),
            }
            for match in sorted(organizer_names & accepted_organizers):
                organization_reasons.append(
                    {"code": "organizer", "binding": f"organizer:{match}"}
                )
        organization_reasons = _deduplicate_reasons(organization_reasons)
        if organization_reasons:
            organization_memberships.append(_membership(entity, organization_reasons))

        venue_reasons = _venue_reasons(entity, event)
        if venue_reasons:
            venue_memberships.append(_membership(entity, venue_reasons))

        if entity["flags"]["official_theatre"]:
            theatre_reasons = _deduplicate_reasons([*organization_reasons, *venue_reasons])
            if theatre_reasons:
                theatre_by_id[entity["id"]] = _membership(entity, theatre_reasons)

    return {
        "registry_version": registry["registryVersion"],
        "registry_hash": registry_hash(registry),
        "organization_memberships": organization_memberships,
        "venue_memberships": venue_memberships,
        "theatre_memberships": list(theatre_by_id.values()),
    }


__all__ = [
    "DEFAULT_MEDALLION_REGISTRY_PATH",
    "DEFAULT_REGISTRY_PATH",
    "RegistryValidationError",
    "load_registry",
    "registry_hash",
    "resolve_event_memberships",
    "validate_registry",
]
