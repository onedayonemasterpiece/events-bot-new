from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit

from sqlalchemy import select

from models import ArtistRegistryEntity, Event, EventArtistAppearance


CATALOG_PATH = Path(__file__).with_name("data") / "kgd80_people.json"
DECISION_VERSION = "event-people-semantic-v1"
SEED_SOURCE = "kgd80"
ALLOWED_ROLES = frozenset(
    {
        "speaker",
        "performer",
        "moderator",
        "host",
        "guide",
        "artist",
        "panelist",
        "author",
        "participant",
        "keynote",
        "other",
    }
)
ALLOWED_BILLING = frozenset({"headliner", "featured", "participant", "unknown"})
ALLOWED_PRESENCE = frozenset(
    {"in_person", "remote", "recorded", "subject_only", "unclear"}
)

EVENT_PEOPLE_DECISION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "participants": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "role": {
                        "type": "string",
                        "enum": sorted(ALLOWED_ROLES),
                    },
                    "billing": {
                        "type": "string",
                        "enum": sorted(ALLOWED_BILLING),
                    },
                    "presence": {
                        "type": "string",
                        "enum": sorted(ALLOWED_PRESENCE),
                    },
                    "evidence_quote": {"type": "string"},
                    "confidence": {"type": "number"},
                },
                "required": [
                    "name",
                    "role",
                    "billing",
                    "presence",
                    "evidence_quote",
                    "confidence",
                ],
                "additionalProperties": False,
            },
        },
        "roster_complete": {"type": "boolean"},
    },
    "required": ["participants", "roster_complete"],
    "additionalProperties": False,
}


@dataclass(frozen=True, slots=True)
class GroundedPersonDecision:
    name: str
    role: str
    billing: str
    presence: str
    evidence_quote: str
    confidence: float

    @property
    def public_role(self) -> str:
        if self.billing == "headliner":
            return "headliner"
        if self.role == "keynote":
            return "keynote"
        return self.role


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _text_key(value: Any) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or "")).casefold()
    normalized = normalized.replace("ё", "е")
    return re.sub(r"[^0-9a-zа-я]+", " ", normalized).strip()


def _quote_is_grounded(quote: str, source_corpus: str) -> bool:
    quote_key = re.sub(r"\s+", " ", unicodedata.normalize("NFKC", quote).casefold()).strip()
    corpus_key = re.sub(
        r"\s+", " ", unicodedata.normalize("NFKC", source_corpus).casefold()
    ).strip()
    return bool(quote_key and quote_key in corpus_key)


def _name_is_grounded(name: str, quote: str) -> bool:
    name_words = _text_key(name).split()
    quote_words = _text_key(quote).split()
    if len(name_words) < 2:
        return False
    cursor = 0
    for word in quote_words:
        if cursor < len(name_words) and word == name_words[cursor]:
            cursor += 1
    return cursor == len(name_words)


def grounded_people_decisions(
    payload: Any,
    *,
    source_corpus: str,
) -> tuple[list[GroundedPersonDecision], bool]:
    """Validate the LLM roster without repairing or inferring its semantics."""

    if not isinstance(payload, dict):
        return [], False
    items = payload.get("participants")
    roster_complete = payload.get("roster_complete")
    if not isinstance(items, list) or not isinstance(roster_complete, bool):
        return [], False

    out: list[GroundedPersonDecision] = []
    seen: set[str] = set()
    for item in items[:20]:
        if not isinstance(item, dict):
            continue
        if set(item) != {
            "name",
            "role",
            "billing",
            "presence",
            "evidence_quote",
            "confidence",
        }:
            continue
        name = re.sub(r"\s+", " ", str(item.get("name") or "")).strip(" ,.;:—–-")
        quote = re.sub(r"\s+", " ", str(item.get("evidence_quote") or "")).strip()
        role = str(item.get("role") or "").strip()
        billing = str(item.get("billing") or "").strip()
        presence = str(item.get("presence") or "").strip()
        try:
            confidence = float(item.get("confidence"))
        except (TypeError, ValueError):
            continue
        name_key = _text_key(name)
        if (
            not name_key
            or name_key in seen
            or role not in ALLOWED_ROLES
            or billing not in ALLOWED_BILLING
            or presence not in ALLOWED_PRESENCE
            or not 0.0 <= confidence <= 1.0
            or not _quote_is_grounded(quote, source_corpus)
            or not _name_is_grounded(name, quote)
        ):
            continue
        seen.add(name_key)
        out.append(
            GroundedPersonDecision(
                name=name,
                role=role,
                billing=billing,
                presence=presence,
                evidence_quote=quote[:500],
                confidence=confidence,
            )
        )
    return out, roster_complete


def load_kgd80_catalog(path: Path = CATALOG_PATH) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "kenigevents.kgd80_people.v1":
        raise ValueError("unsupported KGD80 people catalog schema")
    people = payload.get("people")
    if not isinstance(people, list) or not people:
        raise ValueError("KGD80 people catalog is empty")
    return payload


def _https(value: Any) -> str | None:
    raw = str(value or "").strip()
    parsed = urlsplit(raw)
    return raw if parsed.scheme == "https" and parsed.netloc else None


async def ensure_kgd80_registry(db: Any) -> dict[str, int | str]:
    """Idempotently seed all current KGD80 people; operator holds are preserved."""

    payload = load_kgd80_catalog()
    now = utc_now()
    created = 0
    updated = 0
    async with db.get_session() as session:
        for raw in payload["people"]:
            artist_id = str(raw.get("artist_id") or "").strip()
            display_name = str(raw.get("display_name") or "").strip()
            photo_source_url = _https(raw.get("photo_source_url"))
            profile_url = _https(raw.get("profile_url"))
            if not artist_id or not display_name:
                continue
            row = await session.get(ArtistRegistryEntity, artist_id)
            evidence = [
                {
                    "kind": "kgd80_public_catalog",
                    "source_url": profile_url or "https://kgd80.ru/",
                    "source_revision": payload["source_revision"],
                }
            ]
            photo_evidence = (
                [
                    {
                        "source_url": photo_source_url,
                        "credit_text": str(raw.get("credit_text") or "kgd80.ru"),
                        "credit_url": profile_url or "https://kgd80.ru/",
                        "rights_basis": "same-project-curated-public-media",
                        "review_status": "approved",
                    }
                ]
                if photo_source_url
                else []
            )
            photo_rights_status = (
                "kgd80_project_verified" if photo_source_url else "none"
            )
            if row is None:
                row = ArtistRegistryEntity(
                    artist_id=artist_id,
                    entity_type="person",
                    display_name=display_name,
                    canonical_name=display_name,
                    aliases_json=list(raw.get("aliases") or []),
                    primary_domain=str(raw.get("primary_domain") or "") or None,
                    locality_status=str(raw.get("locality_status") or "local_verified"),
                    base_country_code=str(raw.get("base_country_code") or "") or None,
                    base_region_code=str(raw.get("base_region_code") or "") or None,
                    base_city=str(raw.get("base_city") or "") or None,
                    locality_basis="kgd80_public_program",
                    evidence_json=evidence,
                    verification_status="verified",
                    confidence=1.0,
                    photo_url=str(raw.get("photo_url") or "") or None,
                    photo_rights_status=photo_rights_status,
                    photo_rights_evidence_json=photo_evidence,
                    seed_version=str(payload["source_revision"]),
                    decision_version="kgd80-catalog-v1",
                    verified_at=now,
                    created_at=now,
                    updated_at=now,
                )
                session.add(row)
                created += 1
                continue

            # A manual safety hold wins over catalog refresh. Other catalog-owned
            # fields follow the versioned source so new aliases/photos propagate.
            if row.verification_status in {"review", "manual_hold", "rejected"}:
                continue
            before = (
                row.display_name,
                tuple(row.aliases_json or []),
                row.primary_domain,
                row.photo_url,
                row.seed_version,
            )
            row.display_name = display_name
            row.canonical_name = display_name
            row.aliases_json = list(raw.get("aliases") or [])
            row.primary_domain = str(raw.get("primary_domain") or "") or None
            row.locality_status = str(raw.get("locality_status") or "local_verified")
            row.base_country_code = str(raw.get("base_country_code") or "") or None
            row.base_region_code = str(raw.get("base_region_code") or "") or None
            row.base_city = str(raw.get("base_city") or "") or None
            row.locality_basis = "kgd80_public_program"
            row.evidence_json = evidence
            row.verification_status = "verified"
            row.confidence = 1.0
            row.photo_url = str(raw.get("photo_url") or "") or None
            row.photo_rights_status = photo_rights_status
            row.photo_rights_evidence_json = photo_evidence
            row.seed_version = str(payload["source_revision"])
            row.decision_version = "kgd80-catalog-v1"
            row.updated_at = now
            after = (
                row.display_name,
                tuple(row.aliases_json or []),
                row.primary_domain,
                row.photo_url,
                row.seed_version,
            )
            if before != after:
                updated += 1
            session.add(row)
        await session.commit()
    return {
        "created": created,
        "updated": updated,
        "catalog_people": len(payload["people"]),
        "source_revision": str(payload["source_revision"]),
    }


def _alias_index(rows: Iterable[ArtistRegistryEntity]) -> dict[str, list[ArtistRegistryEntity]]:
    index: dict[str, list[ArtistRegistryEntity]] = {}
    for row in rows:
        row_keys: set[str] = set()
        values = [row.display_name, row.canonical_name, *(row.aliases_json or [])]
        for value in values:
            key = _text_key(value)
            if key and key not in row_keys:
                row_keys.add(key)
                index.setdefault(key, []).append(row)
    return index


def registry_candidates_in_text(
    rows: Iterable[ArtistRegistryEntity],
    text: str,
) -> list[ArtistRegistryEntity]:
    """High-precision CPU recall for periodic matching; it does not confirm a role."""

    corpus = f" {_text_key(text)} "
    found: dict[str, ArtistRegistryEntity] = {}
    for row in rows:
        for alias in [row.display_name, row.canonical_name, *(row.aliases_json or [])]:
            key = _text_key(alias)
            if len(key.split()) >= 2 and f" {key} " in corpus:
                found[row.artist_id] = row
                break
    return sorted(found.values(), key=lambda item: item.display_name)


def _input_hash(event: Event, decisions: Iterable[GroundedPersonDecision]) -> str:
    payload = {
        "event_id": event.id,
        "title": event.title,
        "date": event.date,
        "source_text": event.source_text,
        "source_texts": event.source_texts,
        "decisions": [
            {
                "name": item.name,
                "role": item.role,
                "billing": item.billing,
                "presence": item.presence,
                "quote": item.evidence_quote,
            }
            for item in decisions
        ],
        "decision_version": DECISION_VERSION,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def sync_event_people(
    db: Any,
    event_id: int,
    decisions: Iterable[GroundedPersonDecision],
    *,
    roster_complete: bool,
    source_url: str | None,
) -> dict[str, Any]:
    """Resolve a semantic roster to the registry and persist public-safe relations."""

    await ensure_kgd80_registry(db)
    decisions = list(decisions)
    now = utc_now()
    confirmed = 0
    review = 0
    cancelled = 0
    changed = 0
    unmatched: list[str] = []
    source_url = _https(source_url)

    async with db.get_session() as session:
        event = await session.get(Event, int(event_id))
        if event is None:
            return {
                "confirmed": 0,
                "review": 0,
                "cancelled": 0,
                "unmatched": [],
                "reason": "event_missing",
            }
        registry = (
            await session.execute(
                select(ArtistRegistryEntity).where(
                    ArtistRegistryEntity.verification_status == "verified"
                )
            )
        ).scalars().all()
        aliases = _alias_index(registry)
        existing = (
            await session.execute(
                select(EventArtistAppearance).where(
                    EventArtistAppearance.event_id == int(event_id)
                )
            )
        ).scalars().all()
        existing_by_artist = {row.artist_id: row for row in existing}
        touched: set[str] = set()
        input_hash = _input_hash(event, decisions)
        project_key = f"event:{int(event_id)}"

        for decision in decisions:
            matches = aliases.get(_text_key(decision.name), [])
            if len(matches) != 1:
                unmatched.append(decision.name)
                continue
            artist = matches[0]
            touched.add(artist.artist_id)
            publishable_presence = decision.presence in {"in_person", "remote"}
            publishable = (
                publishable_presence
                and decision.confidence >= 0.82
                and source_url is not None
            )
            evidence = [
                {
                    "pipeline": DECISION_VERSION,
                    "source_url": source_url,
                    "evidence_quote": decision.evidence_quote,
                    "declared_name": decision.name,
                    "role": decision.role,
                    "billing": decision.billing,
                    "presence": decision.presence,
                    "confidence": decision.confidence,
                    "match_method": "registry_alias_exact",
                }
            ]
            row = existing_by_artist.get(artist.artist_id)
            before = None
            if row is None:
                row = EventArtistAppearance(
                    event_id=int(event_id),
                    artist_id=artist.artist_id,
                    role=decision.public_role,
                    project_title=event.title,
                    project_key=project_key,
                    visit_cluster_key=f"{artist.artist_id}:{event.date}:{event.city or ''}",
                    appearance_input_hash=input_hash,
                    source_revision=input_hash,
                    created_at=now,
                    updated_at=now,
                )
            else:
                before = (
                    row.role,
                    row.project_title,
                    row.status,
                    row.identity_confidence,
                    row.physical_visit_status,
                    row.participant_evidence_json,
                    row.appearance_input_hash,
                    row.eligibility_status,
                    row.exclusion_reason,
                    row.media_identity_status,
                    row.media_rights_status,
                    row.cancelled_at,
                )
            row.role = decision.public_role
            row.project_title = event.title
            row.project_key = project_key
            row.visit_cluster_key = f"{artist.artist_id}:{event.date}:{event.city or ''}"
            row.status = "confirmed" if publishable else "review"
            row.identity_confidence = decision.confidence
            row.physical_visit_status = (
                "confirmed"
                if decision.presence == "in_person"
                else "remote_confirmed"
                if decision.presence == "remote"
                else "not_participating"
            )
            row.physical_visit_confidence = decision.confidence
            row.participant_evidence_json = evidence
            row.visit_evidence_json = evidence
            row.appearance_input_hash = input_hash
            row.source_revision = input_hash
            row.eligibility_status = "eligible" if publishable else "review"
            row.exclusion_reason = None if publishable else (
                "source_url_missing"
                if source_url is None
                else "semantic_presence_or_confidence_gate"
            )
            row.media_identity_status = (
                "verified" if artist.photo_url and publishable else "unverified"
            )
            row.media_rights_status = artist.photo_rights_status
            row.cancelled_at = None
            row.updated_at = now
            session.add(row)
            after = (
                row.role,
                row.project_title,
                row.status,
                row.identity_confidence,
                row.physical_visit_status,
                row.participant_evidence_json,
                row.appearance_input_hash,
                row.eligibility_status,
                row.exclusion_reason,
                row.media_identity_status,
                row.media_rights_status,
                row.cancelled_at,
            )
            if before is None or before != after:
                changed += 1
            if publishable:
                confirmed += 1
            else:
                review += 1

        if roster_complete:
            for row in existing:
                if row.artist_id in touched:
                    continue
                evidence = row.participant_evidence_json or []
                if not any(
                    isinstance(item, dict)
                    and item.get("pipeline") == DECISION_VERSION
                    for item in evidence
                ):
                    continue
                row.status = "cancelled"
                row.eligibility_status = "ineligible"
                row.exclusion_reason = "absent_from_complete_semantic_roster"
                row.cancelled_at = now
                row.updated_at = now
                session.add(row)
                cancelled += 1
                changed += 1
        await session.commit()

    return {
        "confirmed": confirmed,
        "review": review,
        "cancelled": cancelled,
        "changed": changed,
        "unmatched": unmatched,
        "decision_version": DECISION_VERSION,
    }
