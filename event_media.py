from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import urllib.request
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any, Iterable, Sequence
from urllib.parse import quote, urlsplit, urlunsplit

from sqlalchemy import func, or_, select, text

from media_dedup import (
    ImageFingerprints,
    compute_global_ssim,
    compute_image_fingerprints,
    hamming_distance_hex,
)
from models import (
    Event,
    EventMediaPairReview,
    EventPoster,
    JobOutbox,
    JobStatus,
    JobTask,
)

logger = logging.getLogger(__name__)

APPROVED = "approved"
PENDING_REVIEW = "pending_review"
DUPLICATE = "duplicate"
REJECTED = "rejected"
UNAVAILABLE = "unavailable"
PUBLIC_EVENT_POSTER_STATUSES = (APPROVED,)
PAIR_POLICY_VERSION = "event-media-pair-v1"
PAIR_PROMPT_VERSION = "event-media-vision-v1"

_REVIEW_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "decision": {"type": "string", "enum": ["duplicate", "distinct", "uncertain"]},
        "duplicate_kind": {
            "type": "string",
            "enum": ["exact", "mirror_reencode", "crop_overlay", "none", "unknown"],
        },
        "confidence": {"type": "number"},
        "semantic_conflict": {"type": "boolean"},
        "canonical_side": {
            "type": "string",
            "enum": ["left", "right", "either", "unknown"],
        },
        "reason_code": {"type": "string"},
    },
    "required": [
        "decision",
        "duplicate_kind",
        "confidence",
        "semantic_conflict",
        "canonical_side",
        "reason_code",
    ],
    "additionalProperties": False,
}


@dataclass(frozen=True, slots=True)
class DownloadedPoster:
    data: bytes
    mime_type: str
    source_url: str


def _env_int(name: str, default: int, *, lo: int = 0, hi: int = 100000) -> int:
    try:
        value = int(str(os.getenv(name, default)).strip())
    except Exception:
        value = default
    return max(lo, min(hi, value))


def _env_float(name: str, default: float, *, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        value = float(str(os.getenv(name, default)).strip())
    except Exception:
        value = default
    return max(lo, min(hi, value))


def event_media_require_cdn() -> bool:
    """Whether public event galleries must contain CDN URLs only."""

    return str(os.getenv("EVENT_MEDIA_REQUIRE_CDN") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def is_event_media_cdn_url(url: str | None) -> bool:
    raw = str(url or "").strip()
    if not raw:
        return False
    try:
        host = (urlsplit(raw).hostname or "").casefold()
        expected = (
            urlsplit(
                os.getenv("EVENT_MEDIA_CDN_BASE_URL")
                or os.getenv("PUBLIC_ASSET_BASE_URL")
                or "https://static.kenigevents.ru"
            ).hostname
            or ""
        ).casefold()
    except Exception:
        return False
    return bool(expected and host == expected)


def _is_managed_url(url: str | None) -> bool:
    raw = str(url or "").strip()
    if not raw:
        return False
    try:
        from yandex_storage import is_managed_storage_url

        return bool(is_managed_storage_url(raw))
    except Exception:
        low = raw.casefold()
        return "/p/dh16/" in low or "/storage/v1/object/" in low


def resolve_poster_display_url(poster: EventPoster) -> str | None:
    """Return exactly one public URL for one logical EventPoster row."""

    managed_candidates = [
        str(value or "").strip()
        for value in (poster.supabase_url, poster.catbox_url)
        if str(value or "").strip() and _is_managed_url(str(value))
    ]
    if managed_candidates:
        from yandex_storage import canonicalize_yandex_public_url

        canonical = canonicalize_yandex_public_url(managed_candidates[0])
        if canonical and (not event_media_require_cdn() or is_event_media_cdn_url(canonical)):
            return canonical
    for value in (poster.supabase_url, poster.catbox_url):
        raw = str(value or "").strip()
        if raw and (not event_media_require_cdn() or is_event_media_cdn_url(raw)):
            return raw
    return None


async def get_event_gallery_rows(session: Any, event_id: int) -> list[EventPoster]:
    rows = (
        await session.execute(
            select(EventPoster)
            .where(
                EventPoster.event_id == int(event_id),
                EventPoster.review_status == APPROVED,
            )
            .order_by(EventPoster.display_order.asc(), EventPoster.id.asc())
        )
    ).scalars().all()
    return [row for row in rows if resolve_poster_display_url(row)]


async def get_event_gallery_urls(
    session: Any,
    event_id: int,
    *,
    legacy_fallback: bool = True,
) -> list[str]:
    rows = await get_event_gallery_rows(session, event_id)
    urls: list[str] = []
    for row in rows:
        url = resolve_poster_display_url(row)
        if url and url not in urls:
            urls.append(url)
    if urls or not legacy_fallback:
        return urls
    # Legacy fallback is only for a genuinely pre-ledger event. If any poster
    # row exists, an empty approved set is an intentional fail-closed result.
    has_ledger_rows = (
        await session.execute(
            select(EventPoster.id).where(EventPoster.event_id == int(event_id)).limit(1)
        )
    ).scalar_one_or_none()
    if has_ledger_rows is not None:
        return []
    event = await session.get(Event, int(event_id))
    for value in list(getattr(event, "photo_urls", None) or []) if event else []:
        url = str(value or "").strip()
        if url and url not in urls:
            urls.append(url)
    return urls


async def sync_event_gallery_projection(session: Any, event_id: int) -> bool:
    """Synchronize the temporary Event.photo_urls cache from approved rows."""

    event = await session.get(Event, int(event_id))
    if event is None:
        return False
    rows = await get_event_gallery_rows(session, int(event_id))
    urls: list[str] = []
    for row in rows:
        url = resolve_poster_display_url(row)
        if url and url not in urls:
            urls.append(url)
    before = [str(value or "").strip() for value in list(event.photo_urls or []) if str(value or "").strip()]
    before_count = int(getattr(event, "photo_count", 0) or 0)
    changed = before != urls or before_count != len(urls)
    if changed:
        event.photo_urls = urls
        event.photo_count = len(urls)
        if getattr(event, "preview_3d_url", None):
            event.preview_3d_url = None
        session.add(event)
    return changed


def _event_context(event: Event) -> dict[str, str]:
    return {
        "title": str(getattr(event, "title", "") or "").strip()[:300],
        "date": str(getattr(event, "date", "") or "").strip()[:40],
        "time": str(getattr(event, "time", "") or "").strip()[:20],
        "location": " · ".join(
            part
            for part in [
                str(getattr(event, "location_name", "") or "").strip(),
                str(getattr(event, "location_address", "") or "").strip(),
            ]
            if part
        )[:400],
    }


def _context_hash(event: Event) -> str:
    payload = json.dumps(_event_context(event), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _pair_input_hash(
    event_id: int,
    left: EventPoster,
    right: EventPoster,
    context_hash: str,
) -> str:
    tokens = []
    for row in (left, right):
        tokens.append(
            str(
                row.pixel_sha256
                or row.raw_sha256
                or row.poster_hash
                or row.phash
                or row.id
            )
        )
    tokens.sort()
    # Review rows own poster foreign keys from one event.  Without event_id two
    # otherwise identical events could collide on the global idempotency key.
    payload = "|".join(
        [str(int(event_id)), *tokens, context_hash, PAIR_PROMPT_VERSION, PAIR_POLICY_VERSION]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def enqueue_event_media_review_job(
    session: Any,
    event_id: int,
    *,
    next_run_at: datetime | None = None,
    force_followup: bool = False,
) -> bool:
    statuses = [JobStatus.pending] if force_followup else [JobStatus.pending, JobStatus.running]
    existing = (
        await session.execute(
            select(JobOutbox.id)
            .where(
                JobOutbox.event_id == int(event_id),
                JobOutbox.task == JobTask.event_media_review,
                JobOutbox.status.in_(statuses),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if existing is not None:
        return False
    session.add(
        JobOutbox(
            event_id=int(event_id),
            task=JobTask.event_media_review,
            status=JobStatus.pending,
            next_run_at=next_run_at or datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            coalesce_key=f"event_media_review:{int(event_id)}",
        )
    )
    return True


async def _collapse_exact_display_url_duplicates(
    session: Any,
    event_id: int,
    rows: Sequence[EventPoster],
) -> list[EventPoster]:
    """Resolve repeated logical URLs before downloads, hashes, or VLM calls."""

    groups: dict[str, list[EventPoster]] = {}
    for row in rows:
        url = resolve_poster_display_url(row)
        if url:
            groups.setdefault(url, []).append(row)
    losers: list[EventPoster] = []
    survivor_ids: set[int] = set()
    now = datetime.now(timezone.utc)
    for grouped in groups.values():
        survivor = _choose_survivor(*grouped[:2]) if len(grouped) == 2 else max(
            grouped, key=_poster_quality_key
        )
        survivor_ids.add(int(survivor.id or 0))
        for row in grouped:
            if row.id == survivor.id:
                continue
            row.review_status = DUPLICATE
            row.duplicate_of_id = int(survivor.id or 0)
            row.review_reason = "exact_display_url_duplicate"
            row.reviewed_at = now
            session.add(row)
            losers.append(row)
    loser_ids = [int(row.id or 0) for row in losers if row.id]
    if loser_ids:
        related = (
            await session.execute(
                select(EventMediaPairReview).where(
                    EventMediaPairReview.event_id == int(event_id),
                    EventMediaPairReview.status.in_(("pending", "deferred")),
                    or_(
                        EventMediaPairReview.left_poster_id.in_(loser_ids),
                        EventMediaPairReview.right_poster_id.in_(loser_ids),
                    ),
                )
            )
        ).scalars().all()
        for review in related:
            review.status = "cancelled"
            review.decision = "duplicate"
            review.duplicate_kind = "exact"
            review.confidence = 1.0
            review.reason_code = "exact_display_url_duplicate"
            review.last_error = None
            review.resolved_at = now
            review.updated_at = now
            session.add(review)
    return [row for row in rows if int(row.id or 0) in survivor_ids]


async def ensure_event_media_reviews(session: Any, event_id: int) -> int:
    """Create cached unordered pair decisions for every unresolved gallery row."""

    event = await session.get(Event, int(event_id))
    if event is None:
        return 0
    rows = (
        await session.execute(
            select(EventPoster)
            .where(
                EventPoster.event_id == int(event_id),
                EventPoster.review_status.in_((APPROVED, PENDING_REVIEW)),
            )
            .order_by(EventPoster.display_order.asc(), EventPoster.id.asc())
        )
    ).scalars().all()
    rows = [row for row in rows if resolve_poster_display_url(row)]
    rows = await _collapse_exact_display_url_duplicates(
        session, int(event_id), rows
    )
    pending_rows = [row for row in rows if row.review_status == PENDING_REVIEW]
    if pending_rows and not any(row.review_status == APPROVED for row in rows):
        seed = pending_rows[0]
        seed.review_status = APPROVED
        seed.review_reason = "first_event_media_seed"
        seed.reviewed_at = datetime.now(timezone.utc)
        session.add(seed)
    if len(rows) == 1 and rows[0].review_status == PENDING_REVIEW:
        rows[0].review_status = APPROVED
        rows[0].review_reason = "single_event_media"
        rows[0].reviewed_at = datetime.now(timezone.utc)
        session.add(rows[0])
        return 0

    context_hash = _context_hash(event)
    # Review one quarantined image against the currently approved gallery at a
    # time. This keeps a legacy 30-image backfill bounded and avoids eagerly
    # materialising O(n²) pending rows; later candidates will compare against a
    # newly approved target in their own turn.
    pending_rows = [row for row in rows if row.review_status == PENDING_REVIEW]
    approved_rows = [row for row in rows if row.review_status == APPROVED]
    if not pending_rows:
        await sync_event_gallery_projection(session, int(event_id))
        return 0
    target = pending_rows[0]
    created = 0
    for approved in approved_rows:
        left, right = approved, target
        left_id, right_id = sorted((int(left.id or 0), int(right.id or 0)))
        if not left_id or not right_id:
            continue
        existing = (
            await session.execute(
                select(EventMediaPairReview.id)
                .where(
                    EventMediaPairReview.event_id == int(event_id),
                    EventMediaPairReview.left_poster_id == left_id,
                    EventMediaPairReview.right_poster_id == right_id,
                    EventMediaPairReview.context_hash == context_hash,
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        if existing is not None:
            continue
        row_by_id = {int(left.id or 0): left, int(right.id or 0): right}
        review = EventMediaPairReview(
            event_id=int(event_id),
            left_poster_id=left_id,
            right_poster_id=right_id,
            context_hash=context_hash,
            pair_input_hash=_pair_input_hash(
                int(event_id), row_by_id[left_id], row_by_id[right_id], context_hash
            ),
        )
        session.add(review)
        created += 1
    if created:
        await enqueue_event_media_review_job(session, int(event_id))
    await sync_event_gallery_projection(session, int(event_id))
    return created


def _quote_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            quote(parts.path, safe="/%:@"),
            quote(parts.query, safe="=&?/:+,%"),
            parts.fragment,
        )
    )


def _download_url(url: str, *, max_bytes: int, timeout: float) -> DownloadedPoster:
    req = urllib.request.Request(
        _quote_url(url),
        headers={"User-Agent": "events-bot-event-media-review/1.0"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        length = response.headers.get("Content-Length")
        if length and int(length) > max_bytes:
            raise ValueError("image_too_large")
        data = response.read(max_bytes + 1)
        if len(data) > max_bytes:
            raise ValueError("image_too_large")
        if not data:
            raise ValueError("empty_image")
        mime = str(response.headers.get("Content-Type") or "application/octet-stream").split(";", 1)[0]
        final_url = str(getattr(response, "url", None) or url)
        return DownloadedPoster(data=data, mime_type=mime, source_url=final_url)


async def materialize_event_media_candidate_to_cdn(
    candidate: Any, *, session: Any | None = None
) -> bool:
    """Materialize one Smart Update candidate in the canonical CDN bucket.

    The original source URL remains provenance in ``catbox_url``.  Public
    projection always uses ``supabase_url``/``supabase_path`` pointing at the
    CDN.  Existing raw Object Storage URLs for the current origin bucket are
    host-canonicalized without downloading or copying the object.
    """

    from media_dedup import build_supabase_poster_object_path, prepare_image_for_supabase
    from yandex_storage import (
        canonicalize_yandex_public_url,
        parse_yandex_storage_url,
        upload_yandex_public_bytes,
    )

    values = [
        str(getattr(candidate, name, None) or "").strip()
        for name in ("supabase_url", "catbox_url")
    ]
    urls = [value for value in values if value]
    for url in urls:
        canonical = canonicalize_yandex_public_url(url)
        parsed = parse_yandex_storage_url(url)
        if canonical and parsed and is_event_media_cdn_url(canonical):
            candidate.supabase_url = canonical
            candidate.supabase_path = parsed[1]
            return True
        if is_event_media_cdn_url(url):
            candidate.supabase_url = url
            if parsed:
                candidate.supabase_path = parsed[1]
            return True

    if not urls:
        return False
    max_bytes = _env_int(
        "EVENT_MEDIA_REVIEW_MAX_IMAGE_BYTES", 8_000_000, lo=100_000, hi=36_700_160
    )
    timeout = float(
        _env_int("EVENT_MEDIA_REVIEW_DOWNLOAD_TIMEOUT_SEC", 25, lo=3, hi=120)
    )
    downloaded: DownloadedPoster | None = None
    last_error: Exception | None = None
    for url in urls:
        try:
            downloaded = await asyncio.to_thread(
                _download_url, url, max_bytes=max_bytes, timeout=timeout
            )
            break
        except Exception as exc:
            last_error = exc
    if downloaded is None:
        logger.warning("event_media.cdn_download_failed url=%s err=%s", urls[0], last_error)
        return False

    quality = _env_int("SUPABASE_POSTERS_WEBP_QUALITY", 82, lo=1, hi=100)
    prepared = await asyncio.to_thread(
        prepare_image_for_supabase,
        downloaded.data,
        dhash_size=16,
        webp_quality=quality,
    )
    if prepared is None:
        logger.warning("event_media.cdn_prepare_failed url=%s", downloaded.source_url)
        return False
    object_path = build_supabase_poster_object_path(
        prepared.dhash_hex,
        prefix=(os.getenv("SUPABASE_POSTERS_PREFIX") or "p").strip() or "p",
        dhash_size=16,
    )
    hosted = await asyncio.to_thread(
        upload_yandex_public_bytes,
        prepared.webp_bytes,
        object_path=object_path,
        content_type="image/webp",
    )
    hosted = canonicalize_yandex_public_url(hosted)
    if not hosted or not is_event_media_cdn_url(hosted):
        logger.warning("event_media.cdn_upload_failed path=%s", object_path)
        return False
    candidate.supabase_url = hosted
    candidate.supabase_path = object_path
    if not str(getattr(candidate, "catbox_url", None) or "").strip():
        candidate.catbox_url = downloaded.source_url
    digest = hashlib.sha256(downloaded.data).hexdigest()
    if hasattr(candidate, "raw_sha256"):
        conflicting_poster_id: int | None = None
        event_id = getattr(candidate, "event_id", None)
        poster_id = getattr(candidate, "id", None)
        if session is not None and event_id is not None:
            conflict_stmt = select(EventPoster.id).where(
                EventPoster.event_id == int(event_id),
                EventPoster.raw_sha256 == digest,
            )
            if poster_id is not None:
                conflict_stmt = conflict_stmt.where(EventPoster.id != int(poster_id))
            conflicting_poster_id = (
                await session.execute(conflict_stmt.limit(1))
            ).scalar_one_or_none()
        if conflicting_poster_id is None:
            candidate.raw_sha256 = digest
        else:
            # Production has a partial unique index on
            # (event_id, raw_sha256).  Keep byte identity on the existing
            # survivor and let the pair-review row preserve/adjudicate the
            # equality instead of aborting the whole CDN retry transaction.
            candidate.raw_sha256 = None
            logger.info(
                "event_media.cdn_raw_sha_conflict event_id=%s poster_id=%s survivor_id=%s",
                event_id,
                poster_id,
                conflicting_poster_id,
            )
    else:
        candidate.sha256 = digest
    candidate.phash = prepared.dhash_hex
    return True


async def materialize_event_posters_to_cdn(session: Any, event_id: int) -> tuple[int, int]:
    """Retry CDN materialization for ledger rows; return (updated, failed)."""

    rows = (
        await session.execute(
            select(EventPoster)
            .where(
                EventPoster.event_id == int(event_id),
                EventPoster.review_status.in_((APPROVED, PENDING_REVIEW)),
            )
            .order_by(EventPoster.display_order.asc(), EventPoster.id.asc())
        )
    ).scalars().all()
    updated = 0
    failed = 0
    for row in rows:
        before = (row.supabase_url, row.supabase_path, row.raw_sha256, row.phash)
        ok = await materialize_event_media_candidate_to_cdn(row, session=session)
        if not ok:
            row.review_status = PENDING_REVIEW
            row.review_reason = "cdn_mirror_pending"
            row.reviewed_at = None
            row.updated_at = datetime.now(timezone.utc)
            session.add(row)
            failed += 1
            continue
        if row.review_reason == "cdn_mirror_pending":
            row.review_reason = "awaiting_automated_pair_review"
        after = (row.supabase_url, row.supabase_path, row.raw_sha256, row.phash)
        if before != after:
            row.updated_at = datetime.now(timezone.utc)
            session.add(row)
            updated += 1
    return updated, failed


async def _download_poster(poster: EventPoster) -> DownloadedPoster:
    urls: list[str] = []
    preferred = resolve_poster_display_url(poster)
    for value in (preferred, poster.supabase_url, poster.catbox_url):
        url = str(value or "").strip()
        if url and url not in urls:
            urls.append(url)
    max_bytes = _env_int("EVENT_MEDIA_REVIEW_MAX_IMAGE_BYTES", 8_000_000, lo=100_000, hi=36_700_160)
    timeout = float(_env_int("EVENT_MEDIA_REVIEW_DOWNLOAD_TIMEOUT_SEC", 25, lo=3, hi=120))
    last_error: Exception | None = None
    for url in urls:
        try:
            return await asyncio.to_thread(_download_url, url, max_bytes=max_bytes, timeout=timeout)
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"event_media_download_failed:{last_error}")


def _apply_fingerprints(poster: EventPoster, fp: ImageFingerprints) -> None:
    poster.raw_sha256 = fp.raw_sha256
    poster.pixel_sha256 = fp.pixel_sha256
    poster.phash = fp.dhash_hex
    poster.perceptual_hash = fp.phash_hex
    poster.width = fp.width
    poster.height = fp.height
    poster.mime_type = fp.mime_type
    poster.updated_at = datetime.now(timezone.utc)


def _poster_quality_key(poster: EventPoster) -> tuple[int, int, int, int]:
    url = resolve_poster_display_url(poster)
    area = int(poster.width or 0) * int(poster.height or 0)
    return (
        int(poster.review_status == APPROVED),
        int(_is_managed_url(url)),
        area,
        -int(poster.id or 0),
    )


def _choose_survivor(left: EventPoster, right: EventPoster) -> EventPoster:
    return max((left, right), key=_poster_quality_key)


async def _claim_feature_budget(session: Any, stage: str, limit: int) -> bool:
    if limit <= 0:
        return False
    day = datetime.now(timezone.utc).date().isoformat()
    await session.execute(
        text(
            """
            INSERT INTO event_media_review_usage(day, stage, calls, updated_at)
            VALUES (:day, :stage, 1, CURRENT_TIMESTAMP)
            ON CONFLICT(day, stage) DO UPDATE SET
                calls=event_media_review_usage.calls + 1,
                updated_at=CURRENT_TIMESTAMP
            WHERE event_media_review_usage.calls < :limit
            """
        ),
        {"day": day, "stage": stage, "limit": int(limit)},
    )
    changed = (await session.execute(text("SELECT changes()"))).scalar_one()
    return bool(int(changed or 0))


def _parse_json_object(raw: str) -> dict[str, Any] | None:
    text_value = str(raw or "").strip()
    if text_value.startswith("```"):
        text_value = re.sub(r"^```(?:json)?\s*", "", text_value, flags=re.I)
        text_value = re.sub(r"\s*```$", "", text_value)
    try:
        value = json.loads(text_value)
    except Exception:
        start = text_value.find("{")
        end = text_value.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            value = json.loads(text_value[start : end + 1])
        except Exception:
            return None
    return value if isinstance(value, dict) else None


def _validated_decision(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    decision = str(value.get("decision") or "").strip()
    duplicate_kind = str(value.get("duplicate_kind") or "unknown").strip()
    canonical_side = str(value.get("canonical_side") or "unknown").strip()
    if decision not in {"duplicate", "distinct", "uncertain"}:
        return None
    if duplicate_kind not in {"exact", "mirror_reencode", "crop_overlay", "none", "unknown"}:
        return None
    if canonical_side not in {"left", "right", "either", "unknown"}:
        return None
    try:
        confidence = max(0.0, min(1.0, float(value.get("confidence"))))
    except Exception:
        return None
    return {
        "decision": decision,
        "duplicate_kind": duplicate_kind,
        "confidence": confidence,
        "semantic_conflict": bool(value.get("semantic_conflict", False)),
        "canonical_side": canonical_side,
        "reason_code": str(value.get("reason_code") or "unspecified")[:120],
    }


def _review_prompt(event: Event, left: EventPoster, right: EventPoster, *, ssim: float | None) -> str:
    evidence = {
        "event": _event_context(event),
        "left": {
            "ocr_title": str(left.ocr_title or "")[:500],
            "ocr_text": str(left.ocr_text or "")[:2500],
            "width": left.width,
            "height": left.height,
            "dhash16": left.phash,
            "phash16": left.perceptual_hash,
        },
        "right": {
            "ocr_title": str(right.ocr_title or "")[:500],
            "ocr_text": str(right.ocr_text or "")[:2500],
            "width": right.width,
            "height": right.height,
            "dhash16": right.phash,
            "phash16": right.perceptual_hash,
        },
        "metrics": {
            "dhash_hamming": hamming_distance_hex(left.phash, right.phash),
            "phash_hamming": hamming_distance_hex(left.perceptual_hash, right.perceptual_hash),
            "global_ssim": None if ssim is None else round(float(ssim), 6),
        },
    }
    return (
        "Ты проверяешь РОВНО ДВА изображения одного события. Реши, являются ли они одной и той же "
        "картинкой/афишей, включая зеркало CDN, повторное JPEG/WebP кодирование, небольшой crop или overlay. "
        "Две разные фотографии одного события и две самостоятельные афиши должны быть distinct. Если похожий "
        "шаблон содержит другую дату, время, название, адрес или цену либо одно изображение вообще не относится "
        "к событию, поставь semantic_conflict=true, distinct и canonical_side у изображения этого события. "
        "Не делай вывод только по hash/SSIM: обязательно сравни визуальную композицию. Верни только JSON по schema.\n"
        + json.dumps(evidence, ensure_ascii=False, sort_keys=True)
    )


async def _call_reviewer(
    *,
    event: Event,
    left: EventPoster,
    right: EventPoster,
    left_media: DownloadedPoster,
    right_media: DownloadedPoster,
    model: str,
    stage: str,
    session: Any,
) -> tuple[dict[str, Any] | None, int]:
    limit_name = "EVENT_MEDIA_REVIEW_DAILY_PRIMARY_CALLS" if stage == "primary" else "EVENT_MEDIA_REVIEW_DAILY_ESCALATION_CALLS"
    default_limit = 100 if stage == "primary" else 25
    if not await _claim_feature_budget(session, stage, _env_int(limit_name, default_limit, lo=0, hi=5000)):
        return None, 0
    await session.commit()

    from google_ai import GoogleAIClient, SecretsProvider
    from main import get_supabase_client

    client = GoogleAIClient(
        supabase_client=get_supabase_client(),
        secrets_provider=SecretsProvider(),
        consumer="event_media_review",
        account_name="event-media-review",
        default_env_var_name=(os.getenv("EVENT_MEDIA_REVIEW_GOOGLE_KEY_ENV") or "GOOGLE_API_KEY4").strip(),
        reserve_overflow_key_envs=[],
    )
    client.allow_reserve_fallback = False
    client.allow_local_limiter_fallback = False
    client.allow_local_limiter_on_reserve_error = False
    client.fallback_models = []
    client.max_retries = 1
    client.provider_timeout_seconds = float(
        _env_int("EVENT_MEDIA_REVIEW_PROVIDER_TIMEOUT_SEC", 45, lo=10, hi=120)
    )
    ssim = await asyncio.to_thread(compute_global_ssim, left_media.data, right_media.data)
    prompt = [
        _review_prompt(event, left, right, ssim=ssim),
        {"inline_data": {"mime_type": left_media.mime_type, "data": left_media.data}},
        {"inline_data": {"mime_type": right_media.mime_type, "data": right_media.data}},
    ]
    raw, _usage = await client.generate_content_async(
        model=model,
        prompt=prompt,
        generation_config={
            "temperature": 0,
            "response_mime_type": "application/json",
            # This is standard JSON Schema.  The current google-genai SDK
            # contract routes it through response_json_schema; response_schema
            # is the narrower OpenAPI Schema protobuf and rejects fields such
            # as additionalProperties before the model is called.
            "response_json_schema": _REVIEW_SCHEMA,
        },
        max_output_tokens=_env_int("EVENT_MEDIA_REVIEW_MAX_OUTPUT_TOKENS", 256, lo=128, hi=512),
    )
    return _validated_decision(_parse_json_object(raw)), 1


async def _reconcile_pending_posters(session: Any, event_id: int) -> int:
    pending = (
        await session.execute(
            select(EventPoster)
            .where(
                EventPoster.event_id == int(event_id),
                EventPoster.review_status == PENDING_REVIEW,
            )
            .order_by(EventPoster.id.asc())
        )
    ).scalars().all()
    changed = 0
    for poster in pending:
        reviews = (
            await session.execute(
                select(EventMediaPairReview).where(
                    EventMediaPairReview.event_id == int(event_id),
                    or_(
                        EventMediaPairReview.left_poster_id == int(poster.id or 0),
                        EventMediaPairReview.right_poster_id == int(poster.id or 0),
                    ),
                )
            )
        ).scalars().all()
        if not reviews:
            continue
        if any(review.decision == "duplicate" and review.canonical_poster_id != poster.id for review in reviews):
            canonical = next(
                review.canonical_poster_id
                for review in reviews
                if review.decision == "duplicate" and review.canonical_poster_id != poster.id
            )
            poster.review_status = DUPLICATE
            poster.duplicate_of_id = canonical
            poster.review_reason = "automated_pair_duplicate"
            poster.reviewed_at = datetime.now(timezone.utc)
            session.add(poster)
            for related in reviews:
                if related.status in {"pending", "deferred"}:
                    related.status = "cancelled"
                    related.reason_code = "candidate_already_resolved_duplicate"
                    related.updated_at = datetime.now(timezone.utc)
                    session.add(related)
            changed += 1
            continue
        if all(review.status == "resolved" and review.decision == "distinct" for review in reviews):
            poster.review_status = APPROVED
            poster.duplicate_of_id = None
            poster.review_reason = "automated_pair_distinct"
            poster.reviewed_at = datetime.now(timezone.utc)
            session.add(poster)
            changed += 1
    return changed


def _next_utc_day() -> datetime:
    now = datetime.now(timezone.utc)
    return datetime.combine(now.date() + timedelta(days=1), time(hour=0, minute=5), tzinfo=timezone.utc)


async def _recover_stale_running_reviews(
    session: Any,
    event_id: int,
    *,
    now: datetime,
) -> int:
    """Return interrupted pair calls to the automatic queue after a restart."""

    stale_after = timedelta(
        seconds=_env_int("EVENT_MEDIA_REVIEW_RUNNING_STALE_SECONDS", 600, lo=60, hi=3600)
    )
    rows = (
        await session.execute(
            select(EventMediaPairReview).where(
                EventMediaPairReview.event_id == int(event_id),
                EventMediaPairReview.status == "running",
                EventMediaPairReview.updated_at <= now - stale_after,
            )
        )
    ).scalars().all()
    for review in rows:
        review.status = "deferred"
        review.decision = "uncertain"
        review.reason_code = "automatic_running_recovered"
        review.last_error = "interrupted_running_review"
        review.next_run_at = now
        review.updated_at = now
        session.add(review)
    return len(rows)


async def review_next_event_media_pair(event_id: int, db: Any, bot: Any = None) -> bool:
    """Process at most one pair; unresolved media never enters the public gallery."""

    del bot
    projection_changed = False
    async with db.get_session() as session:
        event = await session.get(Event, int(event_id))
        if event is None:
            return False
        now = datetime.now(timezone.utc)
        recovered = await _recover_stale_running_reviews(
            session, int(event_id), now=now
        )
        if recovered:
            logger.warning(
                "event_media: recovered stale running reviews event_id=%s count=%s",
                event_id,
                recovered,
            )
        await ensure_event_media_reviews(session, int(event_id))
        review = (
            await session.execute(
                select(EventMediaPairReview)
                .where(
                    EventMediaPairReview.event_id == int(event_id),
                    EventMediaPairReview.status.in_(("pending", "deferred")),
                    EventMediaPairReview.next_run_at <= now,
                )
                .order_by(EventMediaPairReview.created_at.asc(), EventMediaPairReview.id.asc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if review is None:
            projection_changed = await sync_event_gallery_projection(session, int(event_id))
            await session.commit()
            return projection_changed
        left = await session.get(EventPoster, int(review.left_poster_id))
        right = await session.get(EventPoster, int(review.right_poster_id))
        if left is None or right is None:
            review.status = "resolved"
            review.decision = "distinct"
            review.reason_code = "missing_poster_row"
            review.resolved_at = now
            session.add(review)
            await session.commit()
            return False
        review.status = "running"
        review.attempts = int(review.attempts or 0) + 1
        review.updated_at = now
        session.add(review)
        await session.commit()

    provider_calls = 0
    decision: dict[str, Any] | None = None
    left_media: DownloadedPoster | None = None
    right_media: DownloadedPoster | None = None
    error: str | None = None
    try:
        left_media, right_media = await asyncio.gather(_download_poster(left), _download_poster(right))
        left_fp, right_fp = await asyncio.gather(
            asyncio.to_thread(compute_image_fingerprints, left_media.data),
            asyncio.to_thread(compute_image_fingerprints, right_media.data),
        )
        if left_fp is None or right_fp is None:
            raise RuntimeError("image_decode_failed")
        if left_fp.raw_sha256 == right_fp.raw_sha256:
            decision = {
                "decision": "duplicate",
                "duplicate_kind": "exact",
                "confidence": 1.0,
                "semantic_conflict": False,
                "canonical_side": "either",
                "reason_code": "raw_sha256_equal",
            }
        elif left_fp.pixel_sha256 == right_fp.pixel_sha256:
            decision = {
                "decision": "duplicate",
                "duplicate_kind": "exact",
                "confidence": 1.0,
                "semantic_conflict": False,
                "canonical_side": "either",
                "reason_code": "pixel_sha256_equal",
            }
    except Exception as exc:
        error = f"{type(exc).__name__}:{exc}"[:500]

    async with db.get_session() as session:
        review = await session.get(EventMediaPairReview, int(review.id or 0))
        left = await session.get(EventPoster, int(left.id or 0))
        right = await session.get(EventPoster, int(right.id or 0))
        event = await session.get(Event, int(event_id))
        if review is None or left is None or right is None or event is None:
            return False
        if error is None and left_media is not None and right_media is not None:
            left_fp = await asyncio.to_thread(compute_image_fingerprints, left_media.data)
            right_fp = await asyncio.to_thread(compute_image_fingerprints, right_media.data)
            if left_fp and right_fp:
                # Avoid the partial unique raw-SHA index by keeping the duplicate
                # fingerprint on the survivor only; the decision row preserves evidence.
                if left_fp.raw_sha256 == right_fp.raw_sha256:
                    survivor = _choose_survivor(left, right)
                    loser = right if survivor.id == left.id else left
                    _apply_fingerprints(left, left_fp)
                    _apply_fingerprints(right, right_fp)
                    # The partial unique index keeps the byte identity on the
                    # canonical row. The loser still retains pixel/perceptual
                    # evidence and the pair row records raw equality.
                    loser.raw_sha256 = None
                    session.add(survivor)
                    session.add(loser)
                else:
                    _apply_fingerprints(left, left_fp)
                    _apply_fingerprints(right, right_fp)
                    session.add(left)
                    session.add(right)

        if error is None and decision is None and left_media is not None and right_media is not None:
            primary_model = (os.getenv("EVENT_MEDIA_REVIEW_PRIMARY_MODEL") or "gemini-3.1-flash-lite").strip()
            escalation_model = (os.getenv("EVENT_MEDIA_REVIEW_ESCALATION_MODEL") or "models/gemma-4-31b-it").strip()
            try:
                primary, calls = await _call_reviewer(
                    event=event,
                    left=left,
                    right=right,
                    left_media=left_media,
                    right_media=right_media,
                    model=primary_model,
                    stage="primary",
                    session=session,
                )
                provider_calls += calls
                review.primary_model = primary_model
                decision = primary
                if calls == 0:
                    # The primary allowance is a hard feature budget, not a
                    # signal to consume the smaller escalation allowance.
                    error = "media_review_primary_budget_exhausted"
                min_confidence = _env_float("EVENT_MEDIA_REVIEW_PRIMARY_CONFIDENCE", 0.92)
                needs_escalation = (
                    calls > 0
                    and (
                        decision is None
                        or decision["decision"] == "uncertain"
                        or float(decision["confidence"]) < min_confidence
                    )
                )
                if needs_escalation:
                    escalation, calls = await _call_reviewer(
                        event=event,
                        left=left,
                        right=right,
                        left_media=left_media,
                        right_media=right_media,
                        model=escalation_model,
                        stage="escalation",
                        session=session,
                    )
                    provider_calls += calls
                    review.escalation_model = escalation_model
                    if escalation is not None:
                        if decision and decision["decision"] not in {"uncertain", escalation["decision"]}:
                            decision = None
                            error = "primary_escalation_disagreement"
                        else:
                            decision = escalation
                    elif calls == 0:
                        error = "media_review_budget_exhausted"
            except Exception as exc:
                error = f"{type(exc).__name__}:{exc}"[:500]

        review.provider_calls = int(review.provider_calls or 0) + provider_calls
        review.updated_at = datetime.now(timezone.utc)
        max_attempts = _env_int("EVENT_MEDIA_REVIEW_MAX_UNCERTAIN_ATTEMPTS", 3, lo=1, hi=20)
        if decision and decision["decision"] in {"duplicate", "distinct"}:
            survivor = _choose_survivor(left, right)
            review.status = "resolved"
            review.decision = str(decision["decision"])
            review.duplicate_kind = str(decision["duplicate_kind"])
            review.confidence = float(decision["confidence"])
            review.semantic_conflict = bool(decision["semantic_conflict"])
            canonical: EventPoster | None = None
            if decision["decision"] == "duplicate":
                canonical = survivor
            elif bool(decision["semantic_conflict"]):
                if decision["canonical_side"] == "left":
                    canonical = left
                elif decision["canonical_side"] == "right":
                    canonical = right
                else:
                    # Fail closed for an ambiguous relevance conflict: keep the
                    # already-public side and quarantine/reject the candidate.
                    canonical = left if left.review_status == APPROVED else right
                noncanonical = right if canonical.id == left.id else left
                noncanonical.review_status = REJECTED
                noncanonical.duplicate_of_id = None
                noncanonical.review_reason = "automated_semantic_conflict"
                noncanonical.reviewed_at = datetime.now(timezone.utc)
                session.add(noncanonical)
            review.canonical_poster_id = int(canonical.id or 0) if canonical is not None else None
            review.reason_code = str(decision["reason_code"])
            review.response_json = decision
            review.last_error = None
            review.resolved_at = datetime.now(timezone.utc)
        elif int(review.attempts or 0) >= max_attempts:
            review.status = "unresolved"
            review.decision = "uncertain"
            review.reason_code = "automatic_review_exhausted"
            review.last_error = error or "uncertain"
            review.response_json = decision
        else:
            review.status = "deferred"
            review.decision = "uncertain"
            review.reason_code = "automatic_retry_scheduled"
            review.last_error = error or "uncertain"
            review.response_json = decision
            review.next_run_at = _next_utc_day()
        session.add(review)
        await _reconcile_pending_posters(session, int(event_id))
        await ensure_event_media_reviews(session, int(event_id))
        projection_changed = await sync_event_gallery_projection(session, int(event_id))

        remaining = (
            await session.execute(
                select(func.min(EventMediaPairReview.next_run_at)).where(
                    EventMediaPairReview.event_id == int(event_id),
                    EventMediaPairReview.status.in_(("pending", "deferred")),
                )
            )
        ).scalar_one_or_none()
        if remaining is not None:
            if getattr(remaining, "tzinfo", None) is None:
                remaining = remaining.replace(tzinfo=timezone.utc)
            await enqueue_event_media_review_job(
                session,
                int(event_id),
                next_run_at=max(datetime.now(timezone.utc) + timedelta(seconds=2), remaining),
                force_followup=True,
            )
        await session.commit()

    if projection_changed:
        try:
            from main import schedule_event_update_tasks

            async with db.get_session() as session:
                fresh = await session.get(Event, int(event_id))
            if fresh is not None:
                await schedule_event_update_tasks(db, fresh)
        except Exception:
            logger.warning("event_media: failed to schedule projection rebuild event_id=%s", event_id, exc_info=True)
    return projection_changed


async def ingest_event_media_urls(
    session: Any,
    event_id: int,
    urls: Sequence[str],
    *,
    source: str,
) -> tuple[int, bool]:
    """Route legacy/recovery URL writers back through Smart Update's media gate."""

    from smart_event_update import PosterCandidate, _apply_posters

    candidates = [
        PosterCandidate(catbox_url=str(url).strip())
        for url in urls
        if str(url or "").strip()
    ]
    if not candidates:
        return 0, False
    added, _urls, _preview, _pruned, changed = await _apply_posters(
        session,
        int(event_id),
        candidates,
    )
    logger.info(
        "event_media.ingest_urls event_id=%s source=%s candidates=%s added=%s changed=%s",
        event_id,
        source,
        len(candidates),
        added,
        changed,
    )
    return int(added), bool(changed)
