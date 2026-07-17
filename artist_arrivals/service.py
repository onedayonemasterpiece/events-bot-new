from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlsplit

from sqlalchemy import select

from db import Database
from models import (
    ArtistDigestIssue,
    ArtistMediaAsset,
    ArtistMediaProvenance,
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
CURATED_MEDIA_PATH = (
    Path(__file__).with_name("data")
    / "curated_artist_media_candidates_batch_001.json"
)
ELIGIBLE_LOCALITIES = frozenset(
    {"non_local_ru_verified", "non_local_international_verified"}
)
PHOTO_ALLOWED_STATUSES = frozenset(
    {
        "event_artist_verified",
        "press_kit_verified",
        "cc_verified",
        # This is not an automatic "open internet" permission. It is a
        # separately reviewed informational-use decision with the exact
        # source/author and legal basis recorded in provenance.
        "informational_citation_reviewed",
    }
)
PUBLICATION_SUCCESS_STATUSES = frozenset({"published", "scheduled", "tg_published", "vk_scheduled"})
PROFILE_SAFETY_HOLD_STATUSES = frozenset({"review", "manual_hold", "rejected", "needs_review"})
MEDIA_OPERATIONAL_STATUSES = frozenset({"ready", "hold", "rejected", "takedown", "unavailable"})


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


def photo_publication_metadata(
    status: str | None,
    evidence: Iterable[dict[str, Any]] | None,
) -> dict[str, str] | None:
    """Return sanitized public credit data when photo provenance is usable.

    Pinterest/social pages may be recorded as discovery evidence, but a
    platform logo or a generic platform mention is never treated as a rights
    basis. The higher-risk informational-citation lane is deliberately manual:
    the reviewer must record lawful publication, author/source, purpose and a
    concrete review decision before the image can reach a public card.
    """

    clean_status = str(status or "none").strip().casefold()
    if clean_status not in PHOTO_ALLOWED_STATUSES:
        return None
    for raw in evidence or []:
        item = dict(raw or {})
        source_url = str(item.get("source_url") or "").strip()
        parsed = urlsplit(source_url)
        if parsed.scheme != "https" or not parsed.hostname:
            continue
        service = str(item.get("service") or item.get("source_platform") or "").strip()
        account = str(
            item.get("account_handle") or item.get("source_account") or ""
        ).strip().lstrip("@")
        if not service or not account:
            continue
        credit = str(
            item.get("credit_text")
            or item.get("author_or_rightsholder")
            or item.get("source_name")
            or f"{service} · @{account}"
        ).strip()
        if not credit:
            continue
        if clean_status == "informational_citation_reviewed":
            if not (
                item.get("lawfully_published_confirmed") is True
                and str(item.get("review_status") or "").casefold() == "approved"
                and str(item.get("basis") or "").casefold()
                == "gc_rf_1274_informational_citation"
                and str(item.get("purpose") or "").casefold()
                == "artist_arrival_information"
                and str(item.get("reviewed_by") or "").strip()
                and str(item.get("reviewed_at") or "").strip()
            ):
                continue
        return {
            "credit_text": credit[:180],
            "source_url": source_url,
            "source_service": service[:80],
            "source_account": account[:120],
        }
    return None


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


def load_curated_artist_media_candidates(
    path: Path | str = CURATED_MEDIA_PATH,
) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("schema_version") != "kenigevents.artist_media_candidates.v1":
        raise ValueError("unsupported curated artist-media schema")
    if not isinstance(payload.get("candidates"), list):
        raise ValueError("curated artist-media payload is incomplete")
    for item in payload["candidates"]:
        source = dict(item.get("source") or {})
        required = (
            item.get("candidate_key"),
            item.get("artist_id"),
            source.get("service"),
            source.get("account_handle"),
            source.get("source_page_url"),
        )
        if not all(str(value or "").strip() for value in required):
            raise ValueError("artist-media candidate requires service, account and source page")
    return payload


async def ensure_curated_artist_media_candidates(
    db: Database,
    *,
    path: Path | str = CURATED_MEDIA_PATH,
) -> dict[str, int]:
    """Store reviewed discovery links without making remote images public.

    The seed is deliberately candidate-only. An operator/materializer must
    verify identity and rights, create an immutable object, and set a managed
    CDN URL before the selector can expose the asset to a digest or the site.
    """

    payload = load_curated_artist_media_candidates(path)
    now = datetime.now(timezone.utc)
    asset_upserts = provenance_upserts = missing_artists = 0
    async with db.get_session() as session:
        for item in payload["candidates"]:
            artist_id = str(item["artist_id"])
            if await session.get(ArtistRegistryEntity, artist_id) is None:
                missing_artists += 1
                continue
            candidate_key = str(item["candidate_key"])
            asset = (
                await session.execute(
                    select(ArtistMediaAsset).where(
                        ArtistMediaAsset.candidate_key == candidate_key
                    )
                )
            ).scalars().first()
            values = {
                "artist_id": artist_id,
                "media_role": str(item.get("media_role") or "portrait"),
                "lifecycle_status": str(item.get("lifecycle_status") or "candidate"),
                "identity_status": str(item.get("identity_status") or "unverified"),
                "identity_confidence": item.get("identity_confidence"),
                "quality_status": str(item.get("quality_status") or "review"),
                "rights_status": str(item.get("rights_status") or "review"),
                "storage_status": str(item.get("storage_status") or "remote_candidate"),
                "preferred": bool(item.get("preferred")),
                "priority": int(item.get("priority") or 100),
                "updated_at": now,
            }
            if asset is None:
                asset = ArtistMediaAsset(candidate_key=candidate_key, **values)
                session.add(asset)
                await session.flush()
            elif asset.lifecycle_status not in MEDIA_OPERATIONAL_STATUSES:
                for key, value in values.items():
                    setattr(asset, key, value)
            asset_upserts += 1

            source = dict(item["source"])
            observation_key = candidate_key
            provenance = (
                await session.execute(
                    select(ArtistMediaProvenance).where(
                        ArtistMediaProvenance.observation_key == observation_key
                    )
                )
            ).scalars().first()
            provenance_values = {
                "asset_id": int(asset.id),
                "source_kind": "pinterest_curated_discovery",
                "service": str(source["service"]),
                "account_handle": str(source["account_handle"]),
                "account_name": source.get("account_name"),
                "account_url": source.get("account_url"),
                "source_page_url": str(source["source_page_url"]),
                "source_media_url": source.get("source_media_url"),
                "original_source_url": source.get("original_source_url"),
                "credit_text": str(
                    source.get("credit_text")
                    or f"{source['service']} · @{source['account_handle']}"
                ),
                "review_status": "candidate",
                "updated_at": now,
            }
            if provenance is None:
                session.add(
                    ArtistMediaProvenance(
                        observation_key=observation_key,
                        **provenance_values,
                    )
                )
            elif provenance.review_status not in {"approved", "rejected", "takedown"}:
                for key, value in provenance_values.items():
                    setattr(provenance, key, value)
            provenance_upserts += 1
        await session.commit()
    return {
        "assets": asset_upserts,
        "provenance": provenance_upserts,
        "missing_artists": missing_artists,
    }


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


def _artist_media_public_candidate(
    asset: ArtistMediaAsset,
    provenances: Iterable[ArtistMediaProvenance],
    *,
    event_id: int,
    selected_asset_id: int | None,
) -> tuple[int, dict[str, Any]] | None:
    """Return a ranked, fail-closed managed media candidate."""

    if (
        asset.lifecycle_status != "ready"
        or asset.storage_status != "ready"
        or asset.identity_status != "verified"
        or asset.rights_status not in PHOTO_ALLOWED_STATUSES
        or asset.quality_status not in {"approved", "verified"}
        or asset.taken_down_at is not None
    ):
        return None
    cdn_url = str(asset.cdn_url or "").strip()
    parsed = urlsplit(cdn_url)
    allowed_hosts = {
        value.strip().casefold()
        for value in os.getenv(
            "ARTIST_MEDIA_CDN_HOSTS", "static.kenigevents.ru"
        ).split(",")
        if value.strip()
    }
    if parsed.scheme != "https" or str(parsed.hostname or "").casefold() not in allowed_hosts:
        return None

    source_priority = {
        "manual_preferred": 0,
        "press_kit": 10,
        "official_artist": 20,
        "event_announcement": 30,
        "organizer_or_venue": 40,
        "informational_citation": 50,
        "pinterest_curated_discovery": 80,
    }
    usable: list[tuple[int, ArtistMediaProvenance]] = []
    for provenance in provenances:
        if provenance.review_status != "approved":
            continue
        if not all(
            str(value or "").strip()
            for value in (
                provenance.service,
                provenance.account_handle,
                provenance.source_page_url,
                provenance.credit_text,
            )
        ):
            continue
        rank = source_priority.get(provenance.source_kind, 70)
        if provenance.event_id == event_id:
            rank -= 25
        usable.append((rank, provenance))
    if not usable:
        return None
    usable.sort(key=lambda item: (item[0], int(item[1].id or 0)))
    provenance_rank, provenance = usable[0]
    rank = int(asset.priority or 100) + provenance_rank
    if asset.id == selected_asset_id:
        rank -= 1000
    elif asset.preferred:
        rank -= 100
    return rank, {
        "artist_media_asset_id": asset.id,
        "photo_url": cdn_url,
        "photo_rights_status": asset.rights_status,
        "media_identity_status": asset.identity_status,
        "media_ready": True,
        "photo_credit_text": provenance.credit_text,
        "photo_source_url": provenance.source_page_url,
        "photo_source_service": provenance.service,
        "photo_source_account": provenance.account_handle,
    }


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
    raw_horizon = (
        horizon_days
        if horizon_days is not None
        else int(os.getenv("ARTIST_ARRIVALS_HORIZON_DAYS", "0"))
    )
    if raw_horizon < 0:
        raise ValueError("ARTIST_ARRIVALS_HORIZON_DAYS must be >= 0")
    # 0 is the product default: every future appearance already present in the
    # canonical event catalogue. The sparse appearance table, not Event, is
    # scanned, so a six-month announcement does not duplicate the event DB.
    horizon_days = raw_horizon or None
    min_artists = max(1, int(min_artists or os.getenv("ARTIST_ARRIVALS_MIN_ARTISTS", "3")))
    min_projects = max(1, int(min_projects or os.getenv("ARTIST_ARRIVALS_MIN_PROJECTS", "2")))
    max_items = max(1, min(10, int(max_items or os.getenv("ARTIST_ARRIVALS_MAX_ITEMS", "8"))))
    configured_window_end = today + timedelta(days=horizon_days) if horizon_days else None
    already_published = set(published_dedupe_keys or await _published_dedupe_keys(db))

    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(EventArtistAppearance, ArtistRegistryEntity, Event)
                .join(ArtistRegistryEntity, ArtistRegistryEntity.artist_id == EventArtistAppearance.artist_id)
                .join(Event, Event.id == EventArtistAppearance.event_id)
            )
        ).all()
        artist_ids = sorted({str(row[0].artist_id) for row in rows})
        media_assets = (
            await session.execute(
                select(ArtistMediaAsset).where(
                    ArtistMediaAsset.artist_id.in_(artist_ids)
                )
            )
        ).scalars().all() if artist_ids else []
        asset_ids = [int(asset.id) for asset in media_assets if asset.id is not None]
        media_provenance_rows = (
            await session.execute(
                select(ArtistMediaProvenance).where(
                    ArtistMediaProvenance.asset_id.in_(asset_ids)
                )
            )
        ).scalars().all() if asset_ids else []

    assets_by_artist: dict[str, list[ArtistMediaAsset]] = {}
    for asset in media_assets:
        assets_by_artist.setdefault(str(asset.artist_id), []).append(asset)
    provenance_by_asset: dict[int, list[ArtistMediaProvenance]] = {}
    for provenance in media_provenance_rows:
        provenance_by_asset.setdefault(int(provenance.asset_id), []).append(provenance)

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
        if (
            event_date is None
            or event_date < today
            or (configured_window_end is not None and event_date > configured_window_end)
        ):
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
        photo_meta = photo_publication_metadata(
            artist.photo_rights_status,
            artist.photo_rights_evidence_json,
        )
        selected_media: tuple[int, dict[str, Any]] | None = None
        for asset in assets_by_artist.get(str(artist.artist_id), []):
            candidate = _artist_media_public_candidate(
                asset,
                provenance_by_asset.get(int(asset.id or 0), []),
                event_id=int(event.id),
                selected_asset_id=appearance.selected_artist_media_asset_id,
            )
            if candidate is not None and (
                selected_media is None or candidate[0] < selected_media[0]
            ):
                selected_media = candidate
        legacy_media = None
        legacy_photo_parsed = urlsplit(str(artist.photo_url or "").strip())
        legacy_allowed_hosts = {
            value.strip().casefold()
            for value in os.getenv(
                "ARTIST_MEDIA_CDN_HOSTS", "static.kenigevents.ru"
            ).split(",")
            if value.strip()
        }
        if (
            artist.photo_url
            and photo_meta
            and appearance.media_identity_status == "verified"
            and legacy_photo_parsed.scheme == "https"
            and str(legacy_photo_parsed.hostname or "").casefold()
            in legacy_allowed_hosts
        ):
            legacy_media = {
                "artist_media_asset_id": None,
                "photo_url": artist.photo_url,
                "photo_rights_status": artist.photo_rights_status,
                "media_identity_status": "verified",
                "media_ready": True,
                "photo_credit_text": photo_meta["credit_text"],
                "photo_source_url": photo_meta["source_url"],
                "photo_source_service": photo_meta["source_service"],
                "photo_source_account": photo_meta["source_account"],
            }
        active_media = selected_media[1] if selected_media else legacy_media
        active_media_rank = selected_media[0] if selected_media else (10_000 if legacy_media else None)
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
                "photo_url": active_media["photo_url"] if active_media else None,
                "photo_rights_status": (
                    active_media["photo_rights_status"] if active_media else artist.photo_rights_status
                ),
                "media_identity_status": (
                    active_media["media_identity_status"] if active_media else appearance.media_identity_status
                ),
                "photo_rights_evidence_ids": [
                    _hash(e)[:20] for e in artist.photo_rights_evidence_json
                ],
                "photo_credit_text": active_media["photo_credit_text"] if active_media else None,
                "photo_source_url": active_media["photo_source_url"] if active_media else None,
                "photo_source_service": active_media.get("photo_source_service") if active_media else None,
                "photo_source_account": active_media.get("photo_source_account") if active_media else None,
                "artist_media_asset_id": active_media.get("artist_media_asset_id") if active_media else None,
                "media_ready": bool(active_media),
                "_media_selection_rank": active_media_rank,
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
        if active_media and (
            item.get("_media_selection_rank") is None
            or active_media_rank is not None
            and active_media_rank < item["_media_selection_rank"]
        ):
            item.update(active_media)
            item["_media_selection_rank"] = active_media_rank
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
        item.pop("_media_selection_rank", None)
    items.sort(key=lambda x: (x["dates"][0], x["arrival_kind"], x["artist_name"].casefold()))
    digest_items = [item for item in items if not item["digest_previously_published"]][:max_items]
    selected_keys = {item["item_key"] for item in digest_items}
    for item in items:
        item["social_selected"] = item["item_key"] in selected_keys
    unique_artists = len({item["artist_id"] for item in digest_items})
    unique_projects = len({item["project_key"] for item in digest_items})
    meets_threshold = unique_artists >= min_artists and unique_projects >= min_projects
    window_end = configured_window_end or max(
        (date.fromisoformat(value) for item in items for value in item["dates"]),
        default=today,
    )
    threshold = {
        "min_unique_artists": min_artists,
        "preferred_unique_artists": max(4, min_artists),
        "min_unique_projects": min_projects,
        "max_social_items": max_items,
        "horizon_days": horizon_days,
        "horizon_mode": "bounded" if horizon_days else "all_future_catalogue",
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


async def prune_artist_arrival_shadow_issues(
    db: Database,
    *,
    now_utc: datetime | None = None,
    retention_days: int | None = None,
    keep_issue_id: int | None = None,
) -> dict[str, int]:
    """Bound frozen preview growth without deleting publication evidence.

    Only old, unpublished issues with no delivery-ledger rows are removed.
    Published/scheduled carousels and ambiguous ``sending`` reservations remain
    auditable indefinitely, while the daily shadow history is bounded.
    """

    now_utc = now_utc or datetime.now(timezone.utc)
    retention_days = (
        int(retention_days)
        if retention_days is not None
        else int(os.getenv("ARTIST_ARRIVALS_SHADOW_RETENTION_DAYS", "45"))
    )
    if retention_days < 1:
        raise ValueError("ARTIST_ARRIVALS_SHADOW_RETENTION_DAYS must be >= 1")
    cutoff = now_utc - timedelta(days=retention_days)
    deleted = 0
    protected = 0
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(ArtistDigestIssue).where(ArtistDigestIssue.created_at < cutoff)
            )
        ).scalars().all()
        for row in rows:
            if keep_issue_id is not None and row.id == keep_issue_id:
                protected += 1
                continue
            has_delivery = (
                await session.execute(
                    select(ArtistPublicationLedger.id)
                    .where(ArtistPublicationLedger.issue_id == row.id)
                    .limit(1)
                )
            ).scalars().first() is not None
            if row.published_at is not None or row.published_targets_json or has_delivery:
                protected += 1
                continue
            await session.delete(row)
            deleted += 1
        await session.commit()
    return {
        "deleted": deleted,
        "protected": protected,
        "retention_days": retention_days,
    }


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
                    "photo_credit_text",
                    "photo_source_url",
                    "photo_source_service",
                    "photo_source_account",
                    "artist_media_asset_id",
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
