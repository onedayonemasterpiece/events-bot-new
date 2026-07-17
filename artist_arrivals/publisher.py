from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import os
import socket
from urllib.parse import urljoin, urlsplit
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Sequence

from sqlalchemy import select

from db import Database
from models import ArtistDigestIssue, ArtistPublicationLedger, PromoActivity, PromoCampaign
from promo import PROMO_SURFACE_ARTIST_ARRIVAL_DIGEST

from .rendering import (
    RenderedArtistCard,
    build_telegram_input_rich_message,
    build_vk_carousel_message,
    render_artist_arrival_card,
)
from .service import PHOTO_ALLOWED_STATUSES, PUBLICATION_SUCCESS_STATUSES


@dataclass
class ArtistArrivalPublishResult:
    mode: str
    issue_id: int | None
    ready: bool
    blockers: list[str] = field(default_factory=list)
    targets: list[dict[str, Any]] = field(default_factory=list)


def _enabled(value: str | None, *, default: bool = False) -> bool:
    clean = str(value or "").strip().casefold()
    if not clean:
        return default
    return clean in {"1", "true", "yes", "on"}


def _target_key(value: str | int | None) -> str:
    return str(value or "").strip()


def _content_hash(issue: ArtistDigestIssue, surface: str, target: str) -> str:
    raw = f"{issue.manifest_hash}:{surface}:{target}:artist-arrival-card-v1"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _social_items(issue: ArtistDigestIssue) -> list[dict[str, Any]]:
    return [item for item in issue.items_json if item.get("social_selected", True)]


def _auto_gate(
    issue: ArtistDigestIssue,
    items: Sequence[dict[str, Any]],
    *,
    require_verified_photos: bool,
) -> list[str]:
    blockers: list[str] = []
    if not issue.meets_threshold:
        blockers.append("digest_threshold_not_met")
    if not 3 <= len(items) <= 10:
        blockers.append("telegram_slideshow_requires_3_to_10_items")
    if require_verified_photos:
        for item in items:
            if not (
                item.get("photo_url")
                and item.get("media_identity_status") == "verified"
                and item.get("photo_rights_status") in PHOTO_ALLOWED_STATUSES
                and item.get("photo_rights_evidence_ids")
            ):
                blockers.append("verified_artist_photos_missing")
                break
    return blockers


async def _activity_enabled(db: Database) -> tuple[bool, int | None, dict[str, Any]]:
    async with db.get_session() as session:
        row = (
            await session.execute(
                select(PromoActivity, PromoCampaign)
                .join(PromoCampaign, PromoCampaign.id == PromoActivity.campaign_id)
                .where(PromoActivity.surface == PROMO_SURFACE_ARTIST_ARRIVAL_DIGEST)
                .order_by(PromoActivity.id.desc())
                .limit(1)
            )
        ).first()
    if row is None:
        return False, None, {}
    activity, campaign = row
    now = datetime.now(timezone.utc)

    def aware(value):
        if value is None:
            return None
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)

    starts_at = aware(campaign.starts_at)
    ends_at = aware(campaign.ends_at)
    campaign_active = (
        campaign.status == "active"
        and (starts_at is None or starts_at <= now)
        and (ends_at is None or ends_at >= now)
    )
    return bool(activity.enabled and campaign_active), activity.id, dict(activity.config_json or {})


async def _fetch_photo(url: str) -> bytes:
    import httpx

    async def checked(value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("artist photo URL must be a credential-free HTTPS URL")
        host = parsed.hostname.rstrip(".").casefold()
        allowed = {
            item.strip().rstrip(".").casefold()
            for item in os.getenv("ARTIST_ARRIVALS_PHOTO_HOST_ALLOWLIST", "").split(",")
            if item.strip()
        }
        if not allowed or not any(host == item or host.endswith(f".{item}") for item in allowed):
            raise ValueError(f"artist photo host is not allowlisted: {host}")
        addresses = await asyncio.get_running_loop().run_in_executor(
            None, lambda: socket.getaddrinfo(host, parsed.port or 443, type=socket.SOCK_STREAM)
        )
        for address in addresses:
            ip = ipaddress.ip_address(address[4][0])
            if not ip.is_global:
                raise ValueError(f"artist photo host resolves to a non-public address: {host}")
        return value

    max_bytes = 12 * 1024 * 1024
    current = await checked(url)
    async with httpx.AsyncClient(follow_redirects=False, timeout=20) as client:
        for _ in range(4):
            async with client.stream("GET", current) as response:
                if response.is_redirect:
                    location = response.headers.get("location")
                    if not location:
                        raise ValueError("artist photo redirect has no location")
                    current = await checked(urljoin(current, location))
                    continue
                response.raise_for_status()
                content_type = str(response.headers.get("content-type") or "").casefold()
                if content_type and not content_type.startswith("image/"):
                    raise ValueError(f"artist photo is not an image: {content_type}")
                content_length = response.headers.get("content-length")
                if content_length and int(content_length) > max_bytes:
                    raise ValueError("artist photo size is invalid")
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > max_bytes:
                        raise ValueError("artist photo size is invalid")
                    chunks.append(chunk)
                payload = b"".join(chunks)
                break
        else:
            raise ValueError("artist photo has too many redirects")
    if not payload:
        raise ValueError("artist photo size is invalid")
    return payload


async def _render_cards_with_verified_media(
    items: Sequence[dict[str, Any]],
    *,
    photo_fetcher: Callable[[str], Awaitable[bytes]] = _fetch_photo,
) -> list[RenderedArtistCard]:
    cards: list[RenderedArtistCard] = []
    for item in items:
        source_image = None
        if (
            item.get("photo_url")
            and item.get("media_identity_status") == "verified"
            and item.get("photo_rights_status") in PHOTO_ALLOWED_STATUSES
            and item.get("photo_rights_evidence_ids")
        ):
            source_image = await photo_fetcher(str(item["photo_url"]))
        cards.append(render_artist_arrival_card(item, source_image=source_image))
    return cards


async def _delivery_states(
    db: Database,
    *,
    surface: str,
    target: str,
    dedupe_keys: set[str],
) -> dict[str, str]:
    if not dedupe_keys:
        return {}
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(ArtistPublicationLedger)
                .where(ArtistPublicationLedger.surface == surface)
                .where(ArtistPublicationLedger.target_key == target)
                .where(ArtistPublicationLedger.dedupe_key.in_(dedupe_keys))
            )
        ).scalars().all()
    return {row.dedupe_key: str(row.publish_status or "").casefold() for row in rows}


async def _reserve_delivery(
    db: Database,
    *,
    issue: ArtistDigestIssue,
    activity_id: int | None,
    surface: str,
    target: str,
    items: Sequence[dict[str, Any]],
) -> None:
    """Persist a fail-closed reservation before the public network call.

    If the process dies after the send but before success is recorded, the
    retained ``sending`` rows force manual reconciliation instead of an
    automatic duplicate carousel.
    """

    now = datetime.now(timezone.utc)
    content_hash = _content_hash(issue, surface, target)
    async with db.get_session() as session:
        for item in items:
            dedupe_key = str(item["dedupe_key"])
            existing = (
                await session.execute(
                    select(ArtistPublicationLedger)
                    .where(ArtistPublicationLedger.surface == surface)
                    .where(ArtistPublicationLedger.target_key == target)
                    .where(ArtistPublicationLedger.dedupe_key == dedupe_key)
                )
            ).scalars().first()
            if existing is not None:
                raise RuntimeError(f"unreconciled artist-arrival delivery reservation: {surface}:{dedupe_key}")
            session.add(
                ArtistPublicationLedger(
                    issue_id=int(issue.id),
                    activity_id=activity_id,
                    artist_id=str(item["artist_id"]),
                    project_key=str(item["project_key"]),
                    surface=surface,
                    target_key=target,
                    dedupe_key=dedupe_key,
                    content_hash=content_hash,
                    publish_status="sending",
                    attempts=1,
                    updated_at=now,
                )
            )
        await session.commit()


async def _record_delivery(
    db: Database,
    *,
    issue: ArtistDigestIssue,
    activity_id: int | None,
    surface: str,
    target: str,
    status: str,
    target_url: str | None,
    target_message_id: int | None,
    items: Sequence[dict[str, Any]],
) -> None:
    now = datetime.now(timezone.utc)
    content_hash = _content_hash(issue, surface, target)
    async with db.get_session() as session:
        for item in items:
            dedupe_key = str(item["dedupe_key"])
            existing = (
                await session.execute(
                    select(ArtistPublicationLedger)
                    .where(ArtistPublicationLedger.surface == surface)
                    .where(ArtistPublicationLedger.target_key == target)
                    .where(ArtistPublicationLedger.dedupe_key == dedupe_key)
                )
            ).scalars().first()
            values = {
                "issue_id": int(issue.id),
                "activity_id": activity_id,
                "artist_id": str(item["artist_id"]),
                "project_key": str(item["project_key"]),
                "content_hash": content_hash,
                "publish_status": status,
                "target_url": target_url,
                "target_message_id": target_message_id,
                "updated_at": now,
            }
            if existing is None:
                session.add(
                    ArtistPublicationLedger(
                        surface=surface,
                        target_key=target,
                        dedupe_key=dedupe_key,
                        attempts=1,
                        **values,
                    )
                )
            else:
                for key, value in values.items():
                    setattr(existing, key, value)
                if status not in PUBLICATION_SUCCESS_STATUSES:
                    existing.attempts = int(existing.attempts or 0) + 1
        await session.commit()


async def reconcile_artist_arrival_delivery(
    db: Database,
    *,
    surface: str,
    target: str,
    dedupe_keys: Sequence[str],
    outcome: str,
    target_url: str | None = None,
    target_message_id: int | None = None,
) -> int:
    """Resolve an ambiguous network send after an operator inspects the target.

    ``published`` converts every matching ``sending`` reservation to a success;
    ``not_published`` removes the reservations and makes a retry possible.  The
    latter must only be used after the operator has confirmed that the carousel
    is absent from the exact target.
    """

    clean_outcome = str(outcome).strip().casefold()
    if clean_outcome not in {"published", "not_published"}:
        raise ValueError("outcome must be published or not_published")
    keys = {str(value).strip() for value in dedupe_keys if str(value).strip()}
    if not keys:
        raise ValueError("at least one dedupe key is required")
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(ArtistPublicationLedger)
                .where(ArtistPublicationLedger.surface == surface)
                .where(ArtistPublicationLedger.target_key == target)
                .where(ArtistPublicationLedger.dedupe_key.in_(keys))
            )
        ).scalars().all()
        found = {row.dedupe_key for row in rows}
        if found != keys:
            raise RuntimeError("reconciliation set does not exactly match reserved ledger rows")
        if any(str(row.publish_status or "").casefold() != "sending" for row in rows):
            raise RuntimeError("only sending reservations can be reconciled")
        if clean_outcome == "not_published":
            for row in rows:
                await session.delete(row)
        else:
            now = datetime.now(timezone.utc)
            success_status = "tg_published" if surface.endswith(":telegram") else "published"
            for row in rows:
                row.publish_status = success_status
                row.target_url = target_url
                row.target_message_id = target_message_id
                row.updated_at = now
                row.details_json = {**dict(row.details_json or {}), "reconciled": True}
        await session.commit()
    return len(rows)


async def _set_issue_review_state(
    db: Database,
    issue: ArtistDigestIssue,
    *,
    status: str,
    targets: list[dict[str, Any]],
) -> None:
    async with db.get_session() as session:
        current = await session.get(ArtistDigestIssue, issue.id)
        if current is None:
            return
        current.status = status
        current.published_targets_json = targets
        current.updated_at = datetime.now(timezone.utc)
        if status == "published":
            current.published_at = current.updated_at
        await session.commit()


def _telegram_message_url(message: Any) -> str | None:
    chat = getattr(message, "chat", None)
    message_id = getattr(message, "message_id", None)
    username = str(getattr(chat, "username", "") or "").strip().lstrip("@")
    if username and message_id:
        return f"https://t.me/{username}/{message_id}"
    return None


async def publish_artist_arrival_issue(
    db: Database,
    issue: ArtistDigestIssue,
    bot: Any = None,
    *,
    publication_mode: str | None = None,
    telegram_target: str | int | None = None,
    vk_group_id: str | None = None,
    telegram_sender: Callable[..., Awaitable[Any]] | None = None,
    vk_uploader: Callable[..., Awaitable[str | None]] | None = None,
    vk_sender: Callable[..., Awaitable[str | None]] | None = None,
    photo_fetcher: Callable[[str], Awaitable[bytes]] = _fetch_photo,
) -> ArtistArrivalPublishResult:
    """Publish one frozen issue or record why it remains a shadow preview.

    Automatic network delivery requires all three gates: ``publication_mode``
    is ``auto``, ``ARTIST_ARRIVALS_ALLOW_PUBLICATION=1`` and the corresponding
    promo activity is enabled. This deliberate triple gate prevents a schema
    rollout from unexpectedly posting to public channels.
    """

    mode = str(publication_mode or os.getenv("ARTIST_ARRIVALS_PUBLICATION_MODE", "shadow")).strip().casefold()
    activity_enabled, activity_id, config = await _activity_enabled(db)
    items = _social_items(issue)
    # Public social cards always require reviewed person identity and explicit
    # rights provenance. Activity config may not weaken this invariant.
    blockers = _auto_gate(issue, items, require_verified_photos=True)
    if mode != "auto":
        blockers.append("publication_mode_is_shadow")
    if not _enabled(os.getenv("ARTIST_ARRIVALS_ALLOW_PUBLICATION"), default=False):
        blockers.append("public_delivery_switch_is_off")
    if not activity_enabled:
        blockers.append("promo_activity_is_disabled")

    tg_target = telegram_target
    if tg_target is None:
        tg_target = os.getenv("ARTIST_ARRIVALS_TG_TARGET") or os.getenv("TG_EVENT_CHANNEL") or os.getenv("TG_EVENT_CHANNEL_ID")
    vk_target = str(vk_group_id or os.getenv("ARTIST_ARRIVALS_VK_GROUP_ID") or os.getenv("VK_EVENTS_GROUP_ID") or "").strip().lstrip("-")
    if not _target_key(tg_target):
        blockers.append("telegram_target_missing")
    if not vk_target:
        blockers.append("vk_target_missing")

    result = ArtistArrivalPublishResult(
        mode=mode,
        issue_id=issue.id,
        ready=not blockers,
        blockers=list(dict.fromkeys(blockers)),
    )
    if blockers:
        await _set_issue_review_state(
            db,
            issue,
            status="suppressed_threshold" if not issue.meets_threshold else "shadow_ready",
            targets=[{"mode": mode, "blockers": result.blockers}],
        )
        return result

    cards = await _render_cards_with_verified_media(items, photo_fetcher=photo_fetcher)
    expected_keys = {str(item["dedupe_key"]) for item in items}

    tg_surface = f"{PROMO_SURFACE_ARTIST_ARRIVAL_DIGEST}:telegram"
    tg_key = _target_key(tg_target)
    tg_states = await _delivery_states(
        db,
        surface=tg_surface,
        target=tg_key,
        dedupe_keys=expected_keys,
    )
    tg_success = {key for key, status in tg_states.items() if status in PUBLICATION_SUCCESS_STATUSES}
    tg_uncertain = set(tg_states) - tg_success
    if tg_uncertain:
        raise RuntimeError("unreconciled Telegram artist-arrival send; refusing duplicate carousel")
    if tg_success and tg_success != expected_keys:
        raise RuntimeError("partial Telegram artist-arrival ledger; refusing duplicate carousel")
    if tg_success == expected_keys:
        result.targets.append({"surface": "telegram", "status": "already_published", "target": tg_key})
    else:
        rich_message = build_telegram_input_rich_message(items, cards)
        await _reserve_delivery(
            db,
            issue=issue,
            activity_id=activity_id,
            surface=tg_surface,
            target=tg_key,
            items=items,
        )
        if telegram_sender is None:
            if bot is None:
                raise ValueError("Telegram bot is required for auto publication")
            telegram_sender = bot.send_rich_message
        sent = await telegram_sender(chat_id=tg_target, rich_message=rich_message)
        sent_id = getattr(sent, "message_id", None)
        sent_url = _telegram_message_url(sent)
        await _record_delivery(
            db,
            issue=issue,
            activity_id=activity_id,
            surface=tg_surface,
            target=tg_key,
            status="tg_published",
            target_url=sent_url,
            target_message_id=sent_id,
            items=items,
        )
        result.targets.append({"surface": "telegram", "status": "published", "url": sent_url, "message_id": sent_id})

    vk_surface = f"{PROMO_SURFACE_ARTIST_ARRIVAL_DIGEST}:vk"
    vk_states = await _delivery_states(
        db,
        surface=vk_surface,
        target=vk_target,
        dedupe_keys=expected_keys,
    )
    vk_success = {key for key, status in vk_states.items() if status in PUBLICATION_SUCCESS_STATUSES}
    vk_uncertain = set(vk_states) - vk_success
    if vk_uncertain:
        raise RuntimeError("unreconciled VK artist-arrival send; refusing duplicate carousel")
    if vk_success and vk_success != expected_keys:
        raise RuntimeError("partial VK artist-arrival ledger; refusing duplicate carousel")
    if vk_success == expected_keys:
        result.targets.append({"surface": "vk", "status": "already_published", "target": vk_target})
    else:
        if vk_uploader is None or vk_sender is None:
            from main import post_to_vk, upload_vk_photo_bytes

            vk_uploader = vk_uploader or upload_vk_photo_bytes
            vk_sender = vk_sender or post_to_vk
        attachments: list[str] = []
        for card in cards:
            attachment = await vk_uploader(
                vk_target,
                card.jpeg,
                db,
                bot,
                filename=card.filename,
            )
            if not attachment:
                raise RuntimeError(f"VK artist-arrival card upload failed: {card.filename}")
            attachments.append(attachment)
        await _reserve_delivery(
            db,
            issue=issue,
            activity_id=activity_id,
            surface=vk_surface,
            target=vk_target,
            items=items,
        )
        vk_url = await vk_sender(
            vk_target,
            build_vk_carousel_message(items),
            db,
            bot,
            attachments=attachments,
            carousel=True,
        )
        if not vk_url:
            raise RuntimeError("VK artist-arrival carousel publication failed")
        await _record_delivery(
            db,
            issue=issue,
            activity_id=activity_id,
            surface=vk_surface,
            target=vk_target,
            status="published",
            target_url=vk_url,
            target_message_id=None,
            items=items,
        )
        result.targets.append({"surface": "vk", "status": "published", "url": vk_url})

    result.ready = True
    await _set_issue_review_state(db, issue, status="published", targets=result.targets)
    return result
