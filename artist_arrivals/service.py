from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import select

from db import Database
from models import (
    ArtistDigestIssue,
    ArtistPublicationLedger,
    ArtistRegistryEntity,
    Event,
    EventArtistAppearance,
    PromoActivity,
    PromoCampaign,
    PromoTarget,
)

ARTIST_ARRIVAL_SCHEMA_VERSION = "kenigevents.artist_arrivals.v1"
CURATED_DATA_PATH = Path(__file__).with_name("data") / "curated_artist_evidence.json"
ELIGIBLE_LOCALITIES = frozenset(
    {"non_local_ru_verified", "non_local_international_verified"}
)
PHOTO_ALLOWED_STATUSES = frozenset({"event_artist_verified", "press_kit_verified", "cc_verified"})
PUBLICATION_SUCCESS_STATUSES = frozenset({"published", "scheduled", "tg_published", "vk_scheduled"})
PROFILE_SAFETY_HOLD_STATUSES = frozenset({"review", "manual_hold", "rejected", "needs_review"})


def _utc(value: str | datetime | None) -> datetime | None:
    if value is None:
        return value
    if isinstance(value, datetime):
        parsed = value
    else:
        clean = str(value).strip().replace("Z", "+00:00")
        if not clean:
            return None
        parsed = datetime.fromisoformat(clean)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _event_start_date(value: str | None) -> date | None:
    clean = str(value or "").strip().split("..", 1)[0]
    try:
        return date.fromisoformat(clean[:10])
    except ValueError:
        return None


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _clean_project_key(value: str) -> str:
    clean = value.casefold().replace("ё", "е")
    clean = re.sub(r"[^0-9a-zа-я]+", "-", clean).strip("-")
    return clean[:160] or "project"


def load_curated_artist_data(path: Path | str = CURATED_DATA_PATH) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("schema_version") != "kenigevents.artist_arrivals.curated.v1":
        raise ValueError("unsupported curated artist-arrivals schema")
    if not isinstance(payload.get("profiles"), list) or not isinstance(payload.get("appearances"), list):
        raise ValueError("curated artist-arrivals payload is incomplete")
    return payload


async def ensure_curated_artist_data(
    db: Database,
    *,
    path: Path | str = CURATED_DATA_PATH,
) -> dict[str, int]:
    """Idempotently apply the compact, evidence-reviewed registry overlay.

    The supplied XLSX/canonical JSON remains the broad identity seed. This
    operation only writes verified/reviewed sparse rows and explicit event
    appearances. It never infers locality from absence in the seed.
    """

    payload = load_curated_artist_data(path)
    profile_upserts = appearance_upserts = missing_events = 0
    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        for item in payload["profiles"]:
            artist_id = str(item["artist_id"])
            row = await session.get(ArtistRegistryEntity, artist_id)
            values = {
                "entity_type": item.get("entity_type") or "person",
                "display_name": str(item["display_name"]),
                "canonical_name": str(item.get("canonical_name") or item["display_name"]),
                "aliases_json": list(item.get("aliases") or []),
                "primary_domain": item.get("primary_domain"),
                "locality_status": str(item.get("locality_status") or "unknown"),
                "base_country_code": item.get("base_country_code"),
                "base_region_code": item.get("base_region_code"),
                "base_city": item.get("base_city"),
                "locality_basis": item.get("locality_basis"),
                "evidence_json": list(item.get("evidence") or []),
                "verification_status": str(item.get("verification_status") or "review"),
                "confidence": item.get("confidence"),
                "photo_url": item.get("photo_url"),
                "photo_rights_status": str(item.get("photo_rights_status") or "none"),
                "photo_rights_evidence_json": list(item.get("photo_rights_evidence") or []),
                "seed_version": payload.get("source_registry_sha256"),
                "decision_version": str(item.get("decision_version") or "artist-locality-v1"),
                "verified_at": _utc(item.get("verified_at")),
                "valid_until": _utc(item.get("valid_until")),
                "updated_at": now,
            }
            if row is None:
                row = ArtistRegistryEntity(artist_id=artist_id, **values)
                session.add(row)
            else:
                existing_locality = str(row.locality_status or "unknown")
                safety_hold = (
                    str(row.verification_status or "").casefold() in PROFILE_SAFETY_HOLD_STATUSES
                    or (
                        existing_locality in {"unknown", "mobile_or_mixed", "local_verified"}
                        and values["locality_status"] in ELIGIBLE_LOCALITIES
                    )
                )
                for key, value in values.items():
                    if safety_hold and key in {
                        "locality_status",
                        "locality_basis",
                        "evidence_json",
                        "verification_status",
                        "confidence",
                        "verified_at",
                        "valid_until",
                    }:
                        continue
                    if key == "photo_url" and not value and row.photo_url:
                        continue
                    if key == "photo_rights_status" and value in {None, "", "none"} and row.photo_rights_status not in {None, "", "none"}:
                        continue
                    if key == "photo_rights_evidence_json" and not value and row.photo_rights_evidence_json:
                        continue
                    # A broad seed refresh must never downgrade a verified
                    # operational locality decision back to unknown.
                    if key == "locality_status" and value == "unknown" and row.locality_status != "unknown":
                        continue
                    setattr(row, key, value)
            profile_upserts += 1
        await session.flush()

        for item in payload["appearances"]:
            event_id = int(item["event_id"])
            if await session.get(Event, event_id) is None:
                missing_events += 1
                continue
            project_key = _clean_project_key(str(item.get("project_key") or item["project_title"]))
            role = str(item.get("role") or "performer")
            existing = (
                await session.execute(
                    select(EventArtistAppearance)
                    .where(EventArtistAppearance.event_id == event_id)
                    .where(EventArtistAppearance.artist_id == str(item["artist_id"]))
                    .where(EventArtistAppearance.project_key == project_key)
                    .where(EventArtistAppearance.role == role)
                )
            ).scalars().first()
            values = {
                "project_title": str(item["project_title"]),
                "project_key": project_key,
                "visit_cluster_key": str(item.get("visit_cluster_key") or f"{item['artist_id']}:{project_key}"),
                "status": str(item.get("status") or "confirmed"),
                "identity_confidence": item.get("identity_confidence"),
                "physical_visit_status": str(item.get("physical_visit_status") or "review"),
                "physical_visit_confidence": item.get("physical_visit_confidence"),
                "participant_evidence_json": list(item.get("participant_evidence") or []),
                "locality_evidence_ids_json": [
                    str(e.get("evidence_id"))
                    for profile in payload["profiles"]
                    if profile["artist_id"] == item["artist_id"]
                    for e in profile.get("evidence", [])
                    if e.get("evidence_id")
                ],
                "cancellation_evidence_json": list(item.get("cancellation_evidence") or []),
                "visit_evidence_json": list(item.get("visit_evidence") or []),
                "appearance_input_hash": _hash(item),
                "source_revision": str(item["source_revision"]),
                "eligibility_status": str(item.get("eligibility_status") or "review"),
                "exclusion_reason": item.get("exclusion_reason"),
                "media_event_poster_id": item.get("media_event_poster_id"),
                "media_identity_status": str(item.get("media_identity_status") or "unverified"),
                "media_rights_status": str(item.get("media_rights_status") or "event_source"),
                "updated_at": now,
            }
            if existing is None:
                session.add(
                    EventArtistAppearance(
                        event_id=event_id,
                        artist_id=str(item["artist_id"]),
                        role=role,
                        **values,
                    )
                )
            else:
                for key, value in values.items():
                    if key == "status" and existing.status != "confirmed":
                        continue
                    if key == "physical_visit_status" and existing.physical_visit_status != "confirmed":
                        continue
                    if key == "eligibility_status" and existing.eligibility_status != "eligible":
                        continue
                    if key in {"exclusion_reason", "cancellation_evidence_json"} and (
                        existing.exclusion_reason or existing.cancellation_evidence_json
                    ):
                        continue
                    if key == "media_identity_status" and value != "verified" and existing.media_identity_status == "verified":
                        continue
                    if key == "media_event_poster_id" and value is None and existing.media_event_poster_id is not None:
                        continue
                    if key == "media_rights_status" and value in {None, "", "event_source", "unverified"} and existing.media_rights_status not in {None, "", "event_source", "unverified"}:
                        continue
                    setattr(existing, key, value)
            appearance_upserts += 1
        await session.commit()
    return {
        "profiles": profile_upserts,
        "appearances": appearance_upserts,
        "missing_events": missing_events,
    }


async def ensure_artist_arrivals_promo_campaign(db: Database) -> dict[str, int | str]:
    """Register the two artist-arrival activities without enabling delivery.

    The campaign is intentionally created as a draft and both activities are
    disabled. Operators can inspect the daily shadow issues before explicitly
    opting into automatic publication. Repeated calls are idempotent.
    """

    from promo import (
        ARTIST_ARRIVALS_CAMPAIGN_TITLE,
        PROMO_SURFACE_ARTIST_ARRIVAL_DIGEST,
        PROMO_SURFACE_ARTIST_ARRIVAL_HERO,
        PROMO_TARGET_TYPE_ARTIST_ARRIVALS,
    )

    async with db.get_session() as session:
        campaign = (
            await session.execute(
                select(PromoCampaign).where(PromoCampaign.title == ARTIST_ARRIVALS_CAMPAIGN_TITLE)
            )
        ).scalars().first()
        if campaign is None:
            campaign = PromoCampaign(
                title=ARTIST_ARRIVALS_CAMPAIGN_TITLE,
                status="draft",
                goal_comment=(
                    "Редакционная подсветка подтверждённых приездов артистов из России "
                    "и других стран; местные артисты исключены только из arrival-сигнала."
                ),
                priority=2,
            )
            session.add(campaign)
            await session.flush()

        target = (
            await session.execute(
                select(PromoTarget)
                .where(PromoTarget.campaign_id == campaign.id)
                .where(PromoTarget.target_type == PROMO_TARGET_TYPE_ARTIST_ARRIVALS)
            )
        ).scalars().first()
        if target is None:
            session.add(
                PromoTarget(
                    campaign_id=int(campaign.id),
                    target_type=PROMO_TARGET_TYPE_ARTIST_ARRIVALS,
                    query_text="verified_non_local_physical_visit",
                )
            )

        activity_defaults = {
            PROMO_SURFACE_ARTIST_ARRIVAL_DIGEST: {
                "publication_mode": "shadow",
                "min_unique_artists": 3,
                "preferred_unique_artists": 4,
                "min_unique_projects": 2,
                "require_verified_photos": True,
                "channels": ["telegram", "vk"],
                "repeat_policy": "artist_project_ever",
            },
            PROMO_SURFACE_ARTIST_ARRIVAL_HERO: {
                "publication_mode": "shadow",
                "min_unique_artists": 1,
                "require_verified_photos": False,
                "channels": ["static_site"],
                "cooldown_days": 30,
            },
        }
        for surface, config in activity_defaults.items():
            activity = (
                await session.execute(
                    select(PromoActivity)
                    .where(PromoActivity.campaign_id == campaign.id)
                    .where(PromoActivity.surface == surface)
                )
            ).scalars().first()
            if activity is None:
                session.add(
                    PromoActivity(
                        campaign_id=int(campaign.id),
                        surface=surface,
                        profile_key="artist-arrivals-v1",
                        max_per_publish=8 if surface == PROMO_SURFACE_ARTIST_ARRIVAL_DIGEST else 1,
                        selection_policy="verified_arrival_manifest",
                        config_json=config,
                        enabled=False,
                    )
                )
            else:
                # Add new safe defaults without overwriting operator choices
                # such as an explicitly enabled activity or adjusted threshold.
                activity.config_json = {**config, **dict(activity.config_json or {})}
        await session.commit()
        return {"campaign_id": int(campaign.id), "campaign_status": campaign.status}


async def _published_dedupe_keys(
    db: Database,
    *,
    surface_prefix: str = "artist_arrival_digest",
) -> set[str]:
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(ArtistPublicationLedger).where(
                    ArtistPublicationLedger.surface.like(f"{surface_prefix}%")
                )
            )
        ).scalars().all()
    tg_target = str(
        os.getenv("ARTIST_ARRIVALS_TG_TARGET")
        or os.getenv("TG_EVENT_CHANNEL")
        or os.getenv("TG_EVENT_CHANNEL_ID")
        or "@kldevents"
    ).strip()
    vk_target = str(
        os.getenv("ARTIST_ARRIVALS_VK_GROUP_ID")
        or os.getenv("VK_EVENTS_GROUP_ID")
        or ""
    ).strip().lstrip("-")
    if not tg_target or not vk_target:
        return set()
    delivered: dict[str, set[tuple[str, str]]] = {}
    for row in rows:
        if str(row.publish_status or "").casefold() not in PUBLICATION_SUCCESS_STATUSES:
            continue
        delivered.setdefault(row.dedupe_key, set()).add((row.surface, row.target_key))
    required_targets = {
        (f"{surface_prefix}:telegram", tg_target),
        (f"{surface_prefix}:vk", vk_target),
    }
    return {
        dedupe_key
        for dedupe_key, targets in delivered.items()
        if required_targets.issubset(targets)
    }


def _event_public_url(event: Event) -> str | None:
    for value in (event.telegraph_url, event.ticket_link, event.source_post_url):
        clean = str(value or "").strip()
        if clean:
            return clean
    return None


def event_artist_source_revision(event: Event) -> str:
    """Hash only fields that can invalidate a reviewed physical appearance."""

    return _hash(
        {
            "id": event.id,
            "title": event.title,
            "description": event.description,
            "short_description": event.short_description,
            "search_digest": event.search_digest,
            "date": event.date,
            "end_date": event.end_date,
            "time": event.time,
            "location_name": event.location_name,
            "city": event.city,
            "source_text": event.source_text,
            "source_texts": event.source_texts,
            "source_post_url": event.source_post_url,
            "source_vk_post_url": event.source_vk_post_url,
            "ticket_link": event.ticket_link,
            "lifecycle_status": event.lifecycle_status,
            "identity_status": event.identity_status,
            "silent": event.silent,
        }
    )


async def build_artist_arrival_issue(
    db: Database,
    *,
    now_utc: datetime | None = None,
    horizon_days: int | None = None,
    min_artists: int | None = None,
    min_projects: int | None = None,
    max_items: int | None = None,
    published_dedupe_keys: Iterable[str] | None = None,
    persist: bool = True,
) -> ArtistDigestIssue:
    now_utc = now_utc or datetime.now(timezone.utc)
    today = now_utc.date()
    horizon_days = max(1, int(horizon_days or os.getenv("ARTIST_ARRIVALS_HORIZON_DAYS", "14")))
    min_artists = max(1, int(min_artists or os.getenv("ARTIST_ARRIVALS_MIN_ARTISTS", "3")))
    min_projects = max(1, int(min_projects or os.getenv("ARTIST_ARRIVALS_MIN_PROJECTS", "2")))
    max_items = max(1, min(10, int(max_items or os.getenv("ARTIST_ARRIVALS_MAX_ITEMS", "8"))))
    window_end = today + timedelta(days=horizon_days)
    already_published = set(published_dedupe_keys or await _published_dedupe_keys(db))

    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(EventArtistAppearance, ArtistRegistryEntity, Event)
                .join(ArtistRegistryEntity, ArtistRegistryEntity.artist_id == EventArtistAppearance.artist_id)
                .join(Event, Event.id == EventArtistAppearance.event_id)
            )
        ).all()

    excluded: Counter[str] = Counter()
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for appearance, artist, event in rows:
        event_date = _event_start_date(event.date)
        if event.identity_status != "canonical" or event.silent:
            excluded["event_not_canonical"] += 1
            continue
        if event.lifecycle_status != "active" or appearance.status != "confirmed" or appearance.cancelled_at is not None:
            excluded["cancelled_or_inactive"] += 1
            continue
        if event_date is None or event_date < today or event_date > window_end:
            excluded["outside_window"] += 1
            continue
        if appearance.eligibility_status != "eligible" or appearance.physical_visit_status != "confirmed":
            excluded[appearance.exclusion_reason or "appearance_not_confirmed"] += 1
            continue
        if appearance.source_revision != event_artist_source_revision(event):
            excluded["source_revision_changed"] += 1
            continue
        valid_until = _utc(artist.valid_until)
        if valid_until is not None and valid_until < now_utc:
            excluded["locality_evidence_expired"] += 1
            continue
        if artist.locality_status not in ELIGIBLE_LOCALITIES:
            excluded[
                "local_artist" if artist.locality_status == "local_verified" else "locality_not_verified"
            ] += 1
            continue
        if artist.verification_status != "verified":
            excluded["artist_not_verified"] += 1
            continue
        if not artist.evidence_json or not appearance.participant_evidence_json:
            excluded["evidence_missing"] += 1
            continue

        key = (artist.artist_id, appearance.project_key)
        dedupe_key = f"{artist.artist_id}:{appearance.project_key}"
        item = grouped.setdefault(
            key,
            {
                "item_key": dedupe_key,
                "dedupe_key": dedupe_key,
                "artist_id": artist.artist_id,
                "artist_name": artist.display_name,
                "arrival_kind": "international" if artist.locality_status == "non_local_international_verified" else "russia",
                "locality_status": artist.locality_status,
                "project_title": appearance.project_title,
                "project_key": appearance.project_key,
                "visit_cluster_key": appearance.visit_cluster_key,
                "role": appearance.role,
                "event_ids": [],
                "dates": [],
                "venues": [],
                "municipalities": [],
                "event_url": None,
                "photo_url": (
                    artist.photo_url
                    if artist.photo_rights_status in PHOTO_ALLOWED_STATUSES
                    and appearance.media_identity_status == "verified"
                    and artist.photo_rights_evidence_json
                    else None
                ),
                "photo_rights_status": artist.photo_rights_status,
                "media_identity_status": appearance.media_identity_status,
                "photo_rights_evidence_ids": [
                    _hash(e)[:20] for e in artist.photo_rights_evidence_json
                ],
                "media_ready": bool(
                    artist.photo_url
                    and artist.photo_rights_status in PHOTO_ALLOWED_STATUSES
                    and appearance.media_identity_status == "verified"
                    and artist.photo_rights_evidence_json
                ),
                "source_revisions": [],
                "participant_evidence_ids": [
                    _hash(e)[:20] for e in appearance.participant_evidence_json
                ],
                "locality_evidence_ids": list(appearance.locality_evidence_ids_json),
                "digest_previously_published": dedupe_key in already_published,
            },
        )
        item["event_ids"].append(int(event.id))
        item["dates"].append(event_date.isoformat())
        if (
            artist.photo_url
            and artist.photo_rights_status in PHOTO_ALLOWED_STATUSES
            and appearance.media_identity_status == "verified"
            and artist.photo_rights_evidence_json
        ):
            item["photo_url"] = artist.photo_url
            item["photo_rights_status"] = artist.photo_rights_status
            item["media_identity_status"] = "verified"
            item["media_ready"] = True
        if event.location_name and event.location_name not in item["venues"]:
            item["venues"].append(event.location_name)
        if event.city and event.city not in item["municipalities"]:
            item["municipalities"].append(event.city)
        item["event_url"] = item["event_url"] or _event_public_url(event)
        item["source_revisions"].append(appearance.source_revision)

    items = list(grouped.values())
    for item in items:
        item["event_ids"] = sorted(set(item["event_ids"]))
        item["dates"] = sorted(set(item["dates"]))
        item["source_revisions"] = sorted(set(item["source_revisions"]))
    items.sort(key=lambda x: (x["dates"][0], x["arrival_kind"], x["artist_name"].casefold()))
    digest_items = [item for item in items if not item["digest_previously_published"]][:max_items]
    selected_keys = {item["item_key"] for item in digest_items}
    for item in items:
        item["social_selected"] = item["item_key"] in selected_keys
    unique_artists = len({item["artist_id"] for item in digest_items})
    unique_projects = len({item["project_key"] for item in digest_items})
    meets_threshold = unique_artists >= min_artists and unique_projects >= min_projects
    threshold = {
        "min_unique_artists": min_artists,
        "preferred_unique_artists": max(4, min_artists),
        "min_unique_projects": min_projects,
        "max_social_items": max_items,
        "horizon_days": horizon_days,
    }
    hash_payload = {
        "schema_version": ARTIST_ARRIVAL_SCHEMA_VERSION,
        "build_date": today.isoformat(),
        "window_end": window_end.isoformat(),
        "items": items,
        "threshold": threshold,
    }
    manifest_hash = _hash(hash_payload)
    issue = ArtistDigestIssue(
        manifest_hash=manifest_hash,
        build_date=today.isoformat(),
        window_start=today.isoformat(),
        window_end=window_end.isoformat(),
        status="preview" if meets_threshold else "suppressed_threshold",
        unique_artist_count=unique_artists,
        unique_project_count=unique_projects,
        meets_threshold=meets_threshold,
        threshold_json=threshold,
        items_json=items,
        excluded_counts_json=dict(sorted(excluded.items())),
    )
    if not persist:
        return issue
    async with db.get_session() as session:
        existing = (
            await session.execute(
                select(ArtistDigestIssue).where(ArtistDigestIssue.manifest_hash == manifest_hash)
            )
        ).scalars().first()
        if existing is not None:
            return existing
        session.add(issue)
        await session.commit()
        await session.refresh(issue)
    return issue


def public_artist_arrival_projection(issue: ArtistDigestIssue | None) -> dict[str, Any]:
    if issue is None:
        return {
            "schema_version": ARTIST_ARRIVAL_SCHEMA_VERSION,
            "manifest_hash": None,
            "generated_at": None,
            "expires_at": None,
            "eligible": False,
            "shadow_eligible": False,
            "publication_mode": "shadow",
            "items": [],
        }
    today = datetime.now(timezone.utc).date().isoformat()
    if issue.window_end < today:
        return public_artist_arrival_projection(None)
    safe_items = []
    for item in issue.items_json:
        if item.get("locality_status") not in ELIGIBLE_LOCALITIES:
            continue
        if not any(str(value)[:10] >= today for value in item.get("dates") or []):
            continue
        safe_items.append(
            {
                key: item.get(key)
                for key in (
                    "item_key",
                    "artist_name",
                    "arrival_kind",
                    "project_title",
                    "role",
                    "event_ids",
                    "dates",
                    "venues",
                    "municipalities",
                    "event_url",
                    "photo_url",
                    "media_ready",
                )
            }
        )
    return {
        "schema_version": ARTIST_ARRIVAL_SCHEMA_VERSION,
        "manifest_hash": issue.manifest_hash,
        "generated_at": issue.created_at.isoformat() if issue.created_at else None,
        "expires_at": f"{issue.window_end}T23:59:59+02:00",
        # Activity/campaign state is resolved by the database-aware exporter.
        # This pure helper is safe for shadow/review consumers only.
        "eligible": False,
        "shadow_eligible": bool(safe_items),
        "publication_mode": "shadow",
        "social_threshold_met": bool(issue.meets_threshold),
        "items": safe_items,
    }
