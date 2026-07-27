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
from functools import lru_cache
from typing import Any, Iterable, Sequence
from urllib.parse import quote, urlsplit, urlunsplit

from sqlalchemy import and_, func, or_, select, text, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from media_dedup import (
    ImageFingerprints,
    compute_global_ssim,
    compute_image_fingerprints,
    hamming_distance_hex,
)
from models import (
    Event,
    EventImageGeometry,
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
MEDIA_ROLE_PROMPT_VERSION = "event-media-role-v1"
IMAGE_GEOMETRY_PROMPT_VERSION = "event-image-geometry-v1"
IMAGE_GEOMETRY_MAX_FACE_BOXES = 25
MEDIA_ROLES = {
    "event_identity_poster",
    "event_photo",
    "attendee_information",
    "program_or_schedule",
    "wayfinding",
    "sponsor_or_brand",
    "unknown_document",
    "unknown_visual",
}

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

_MEDIA_ROLE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "media_role": {"type": "string", "enum": sorted(MEDIA_ROLES)},
        "image_text_mode": {
            "type": "string",
            "enum": ["ocr_text", "visual_only", "unknown"],
        },
        "primary_purpose": {"type": "string"},
        "confidence": {"type": "number"},
        "reason_code": {"type": "string"},
        "poster_contract": {
            "type": "object",
            "properties": {
                "primary_event_promotion": {"type": "boolean"},
                "event_identity_grounded": {"type": "boolean"},
                "not_utility_document": {"type": "boolean"},
            },
            "required": [
                "primary_event_promotion",
                "event_identity_grounded",
                "not_utility_document",
            ],
            "additionalProperties": False,
        },
        "focal_point": {
            "anyOf": [
                {"type": "null"},
                {
                    "type": "object",
                    "properties": {"x": {"type": "number"}, "y": {"type": "number"}},
                    "required": ["x", "y"],
                    "additionalProperties": False,
                },
            ]
        },
        "safe_crop": {"type": "boolean"},
        "evidence": {
            "type": "array",
            "items": {"type": "string"},
            "maxItems": 6,
        },
    },
    "required": [
        "media_role",
        "image_text_mode",
        "primary_purpose",
        "confidence",
        "reason_code",
        "poster_contract",
        "focal_point",
        "safe_crop",
        "evidence",
    ],
    "additionalProperties": False,
}

_IMAGE_GEOMETRY_SCHEMA: dict[str, Any] = {
    # Keep to the compact provider Schema subset proven by the Gemma 4 vision
    # smoke. Application validation below owns lengths, ordering and bounds;
    # JSON-Schema-only keywords (minItems/additionalProperties/etc.) cause the
    # hosted Gemma endpoint to reject the whole request with a generic 400.
    "type": "OBJECT",
    "properties": {
        "face_boxes_yxyx": {
            "type": "ARRAY",
            "items": {
                "type": "ARRAY",
                "items": {"type": "INTEGER"},
            },
        },
        "valuable_region_yxyx": {
            "type": "ARRAY",
            "items": {"type": "INTEGER"},
        },
        "valuable_region_confidence": {"type": "NUMBER"},
        "reason_code": {"type": "STRING"},
    },
    "required": [
        "face_boxes_yxyx",
        "valuable_region_yxyx",
        "valuable_region_confidence",
        "reason_code",
    ],
}


@dataclass(frozen=True, slots=True)
class DownloadedPoster:
    data: bytes
    mime_type: str
    source_url: str


@dataclass(frozen=True, slots=True)
class ImageGeometryOutcome:
    status: str
    poster_id: int
    geometry_id: int | None = None
    pixel_sha256: str | None = None
    provider_called: bool = False
    cache_hit: bool = False
    error: str | None = None


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
        return (
            "/p/image/v2/" in low
            or "/p/dh16/" in low
            or "/storage/v1/object/" in low
        )


def _exact_content_digest_from_poster_path(path: str | None) -> str | None:
    match = re.fullmatch(
        r"[^/]+/image/v2/(?P<prefix>[0-9a-f]{2})/(?P<digest>[0-9a-f]{64})\.webp",
        str(path or "").strip().lstrip("/"),
        flags=re.IGNORECASE,
    )
    if match is None:
        return None
    digest = match.group("digest").lower()
    return digest if digest.startswith(match.group("prefix").lower()) else None


def _is_exact_content_poster_path(path: str | None) -> bool:
    return _exact_content_digest_from_poster_path(path) is not None


async def assign_event_poster_raw_sha256(
    candidate: Any,
    digest: str | None,
    *,
    session: Any | None = None,
    event_id: int | None = None,
) -> bool:
    """Assign served-byte identity without violating the per-event unique key.

    ``PosterCandidate`` objects do not carry an event id and exact-v2 URLs can
    return before downloading.  Keep the conflict check in one place so both
    those early returns and persisted ``EventPoster`` rows obey the same
    production invariant.  A losing duplicate deliberately keeps ``NULL``;
    pair review can then adjudicate it without aborting the transaction.
    """

    if not hasattr(candidate, "raw_sha256"):
        return False
    normalized = str(digest or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", normalized):
        candidate.raw_sha256 = None
        return False

    resolved_event_id = event_id
    if resolved_event_id is None:
        resolved_event_id = getattr(candidate, "event_id", None)
    poster_id = getattr(candidate, "id", None)
    conflicting_poster_id: int | None = None
    if session is not None and resolved_event_id is not None:
        conflict_stmt = select(EventPoster.id).where(
            EventPoster.event_id == int(resolved_event_id),
            EventPoster.raw_sha256 == normalized,
        )
        if poster_id is not None:
            conflict_stmt = conflict_stmt.where(EventPoster.id != int(poster_id))
        conflicting_poster_id = (
            await session.execute(conflict_stmt.limit(1))
        ).scalar_one_or_none()

    if conflicting_poster_id is None:
        candidate.raw_sha256 = normalized
        return True

    candidate.raw_sha256 = None
    logger.info(
        "event_media.cdn_raw_sha_conflict event_id=%s poster_id=%s survivor_id=%s",
        resolved_event_id,
        poster_id,
        conflicting_poster_id,
    )
    return False


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


def _poster_visual_identity_snapshot(poster: Any) -> tuple[str, str, str, str, str]:
    """Capture the row identity that must stay stable across provider calls."""

    return (
        str(resolve_poster_display_url(poster) or "").strip(),
        str(getattr(poster, "supabase_url", None) or "").strip(),
        str(getattr(poster, "supabase_path", None) or "").strip(),
        str(getattr(poster, "catbox_url", None) or "").strip(),
        str(getattr(poster, "pixel_sha256", None) or "").strip(),
    )


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


async def _insert_pair_review_if_absent(
    session: Any,
    *,
    event_id: int,
    left_poster_id: int,
    right_poster_id: int,
    context_hash: str,
    pair_input_hash: str,
) -> bool:
    """Insert one review idempotently even if another writer wins the race."""

    statement = (
        sqlite_insert(EventMediaPairReview)
        .values(
            event_id=int(event_id),
            left_poster_id=int(left_poster_id),
            right_poster_id=int(right_poster_id),
            context_hash=context_hash,
            pair_input_hash=pair_input_hash,
        )
        .on_conflict_do_nothing(index_elements=["pair_input_hash"])
    )
    result = await session.execute(statement)
    return bool(result.rowcount)


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
            select(JobOutbox)
            .where(
                JobOutbox.event_id == int(event_id),
                JobOutbox.task == JobTask.event_media_review,
                JobOutbox.status.in_(statuses),
            )
            .limit(1)
        )
    ).scalar_one_or_none()
    if existing is not None:
        requested = next_run_at or datetime.now(timezone.utc)
        if getattr(requested, "tzinfo", None) is None:
            requested = requested.replace(tzinfo=timezone.utc)
        current = existing.next_run_at
        if getattr(current, "tzinfo", None) is None:
            current = current.replace(tzinfo=timezone.utc)
        if existing.status == JobStatus.pending and requested < current:
            existing.next_run_at = requested
            existing.updated_at = datetime.now(timezone.utc)
            session.add(existing)
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
        pair_input_hash = _pair_input_hash(
            int(event_id), row_by_id[left_id], row_by_id[right_id], context_hash
        )
        inserted = await _insert_pair_review_if_absent(
            session,
            event_id=int(event_id),
            left_poster_id=left_id,
            right_poster_id=right_id,
            context_hash=context_hash,
            pair_input_hash=pair_input_hash,
        )
        created += int(inserted)
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

    from media_dedup import (
        build_content_addressed_poster_object_path,
        build_event_thumbnail_object_path,
        prepare_image_for_supabase,
    )
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
            previous_display_url = str(
                getattr(candidate, "supabase_url", None)
                or getattr(candidate, "catbox_url", None)
                or ""
            ).strip()
            if previous_display_url and previous_display_url != canonical:
                invalidate_event_poster_visual_evidence(
                    candidate, reason="display_identity_changed"
                )
            candidate.supabase_url = canonical
            candidate.supabase_path = parsed[1]
            exact_digest = _exact_content_digest_from_poster_path(parsed[1])
            if exact_digest:
                await assign_event_poster_raw_sha256(
                    candidate, exact_digest, session=session
                )
            if _is_exact_content_poster_path(parsed[1]) and (
                not hasattr(candidate, "thumbnail_256_url")
                or (
                    str(getattr(candidate, "thumbnail_256_url", None) or "").strip()
                    and str(getattr(candidate, "thumbnail_512_url", None) or "").strip()
                )
            ):
                return True
        if is_event_media_cdn_url(url):
            candidate.supabase_url = url
            if parsed:
                candidate.supabase_path = parsed[1]
            candidate_path = str(getattr(candidate, "supabase_path", None) or "")
            exact_digest = _exact_content_digest_from_poster_path(candidate_path)
            if exact_digest:
                await assign_event_poster_raw_sha256(
                    candidate, exact_digest, session=session
                )
            if _is_exact_content_poster_path(candidate_path) and (
                not hasattr(candidate, "thumbnail_256_url")
                or (
                    str(getattr(candidate, "thumbnail_256_url", None) or "").strip()
                    and str(getattr(candidate, "thumbnail_512_url", None) or "").strip()
                )
            ):
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
    # dHash is deliberately fuzzy and therefore cannot be an immutable object
    # identity: two different renditions can share it and overwrite the same
    # public URL.  Address canonical WebP objects by their exact encoded bytes.
    prefix = (os.getenv("SUPABASE_POSTERS_PREFIX") or "p").strip().strip("/") or "p"
    encoded_sha256 = str(prepared.encoded_sha256 or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", encoded_sha256):
        logger.warning("event_media.cdn_invalid_encoded_sha url=%s", downloaded.source_url)
        return False
    object_path = build_content_addressed_poster_object_path(
        encoded_sha256,
        prefix=prefix,
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
    previous_display_url = str(
        getattr(candidate, "supabase_url", None)
        or getattr(candidate, "catbox_url", None)
        or ""
    ).strip()
    if previous_display_url and previous_display_url != hosted:
        invalidate_event_poster_visual_evidence(
            candidate, reason="display_identity_changed"
        )
    candidate.supabase_url = hosted
    candidate.supabase_path = object_path
    if hasattr(candidate, "pixel_sha256"):
        canonical_fp = await asyncio.to_thread(
            compute_image_fingerprints, prepared.webp_bytes
        )
        if canonical_fp is None:
            logger.warning("event_media.cdn_fingerprint_failed path=%s", object_path)
            return False
        _apply_pixel_fingerprints(candidate, canonical_fp)
    elif hasattr(candidate, "width"):
        candidate.width = int(prepared.width or 0) or getattr(candidate, "width", None)
        candidate.height = int(prepared.height or 0) or getattr(candidate, "height", None)
        candidate.mime_type = "image/webp"
    for thumb in prepared.thumbnails:
        thumb_path = build_event_thumbnail_object_path(
            prepared.encoded_sha256,
            longest_edge=thumb.longest_edge,
        )
        thumb_url = await asyncio.to_thread(
            upload_yandex_public_bytes,
            thumb.webp_bytes,
            object_path=thumb_path,
            content_type="image/webp",
            cache_control="public, max-age=31536000, immutable",
        )
        thumb_url = canonicalize_yandex_public_url(thumb_url)
        if not thumb_url:
            logger.warning(
                "event_media.thumbnail_upload_failed path=%s edge=%s",
                thumb_path,
                thumb.longest_edge,
            )
            continue
        for attr, value in (
            (f"thumbnail_{thumb.longest_edge}_url", thumb_url),
            (f"thumbnail_{thumb.longest_edge}_path", thumb_path),
            (f"thumbnail_{thumb.longest_edge}_width", thumb.width),
            (f"thumbnail_{thumb.longest_edge}_height", thumb.height),
        ):
            if hasattr(candidate, attr):
                setattr(candidate, attr, value)
    if not str(getattr(candidate, "catbox_url", None) or "").strip():
        candidate.catbox_url = downloaded.source_url
    # raw_sha256 describes the bytes served by supabase_url. poster_hash keeps
    # the original source/candidate identity independently.
    digest = encoded_sha256
    if hasattr(candidate, "raw_sha256"):
        await assign_event_poster_raw_sha256(candidate, digest, session=session)
    else:
        if not str(getattr(candidate, "sha256", None) or "").strip():
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
        before = (
            row.supabase_url,
            row.supabase_path,
            row.raw_sha256,
            row.phash,
            row.thumbnail_256_url,
            row.thumbnail_512_url,
        )
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
        after = (
            row.supabase_url,
            row.supabase_path,
            row.raw_sha256,
            row.phash,
            row.thumbnail_256_url,
            row.thumbnail_512_url,
        )
        if before != after:
            row.updated_at = datetime.now(timezone.utc)
            session.add(row)
            updated += 1
    return updated, failed


async def _download_poster(poster: EventPoster) -> DownloadedPoster:
    preferred = resolve_poster_display_url(poster)
    url = str(preferred or "").strip()
    if not url:
        raise RuntimeError("event_media_download_failed:no_current_display_url")
    max_bytes = _env_int("EVENT_MEDIA_REVIEW_MAX_IMAGE_BYTES", 8_000_000, lo=100_000, hi=36_700_160)
    timeout = float(_env_int("EVENT_MEDIA_REVIEW_DOWNLOAD_TIMEOUT_SEC", 25, lo=3, hi=120))
    try:
        return await asyncio.to_thread(
            _download_url, url, max_bytes=max_bytes, timeout=timeout
        )
    except Exception as exc:
        # Geometry and semantic evidence must describe the public display URL,
        # never an alternate provenance URL with potentially different pixels.
        raise RuntimeError(f"event_media_download_failed:{exc}") from exc


def invalidate_event_poster_visual_evidence(
    poster: Any,
    *,
    reason: str = "display_identity_changed",
) -> None:
    """Fail closed when the bytes behind an EventPoster may have changed."""

    for name in (
        "raw_sha256",
        "pixel_sha256",
        "phash",
        "perceptual_hash",
        "width",
        "height",
        "mime_type",
        "image_geometry_id",
        "thumbnail_256_url",
        "thumbnail_256_path",
        "thumbnail_256_width",
        "thumbnail_256_height",
        "thumbnail_512_url",
        "thumbnail_512_path",
        "thumbnail_512_width",
        "thumbnail_512_height",
        "ocr_text",
        "ocr_title",
        "image_text_mode",
        "media_role",
        "media_role_confidence",
        "media_semantic_evidence_json",
        "media_semantic_model",
        "media_semantic_prompt_version",
        "media_semantic_context_hash",
        "media_semantic_classified_at",
        "focal_x",
        "focal_y",
    ):
        if hasattr(poster, name):
            setattr(poster, name, None)
    if hasattr(poster, "media_semantic_status"):
        poster.media_semantic_status = "pending"
    if hasattr(poster, "media_semantic_reason_code"):
        poster.media_semantic_reason_code = reason
    if hasattr(poster, "safe_crop"):
        poster.safe_crop = False
    if hasattr(poster, "updated_at"):
        poster.updated_at = datetime.now(timezone.utc)


def _apply_fingerprints(poster: EventPoster, fp: ImageFingerprints) -> None:
    _apply_pixel_fingerprints(poster, fp)
    poster.raw_sha256 = fp.raw_sha256


def _apply_pixel_fingerprints(poster: Any, fp: ImageFingerprints) -> None:
    previous_pixel = str(getattr(poster, "pixel_sha256", None) or "").strip()
    if previous_pixel and previous_pixel != str(fp.pixel_sha256):
        invalidate_event_poster_visual_evidence(
            poster, reason="pixel_identity_changed"
        )
    elif (
        getattr(poster, "image_geometry_id", None) is not None
        and previous_pixel != str(fp.pixel_sha256)
    ):
        invalidate_event_poster_visual_evidence(
            poster, reason="pixel_identity_changed"
        )
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


def _validated_media_role(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    role = str(value.get("media_role") or "").strip()
    text_mode = str(value.get("image_text_mode") or "").strip()
    contract = value.get("poster_contract")
    if role not in MEDIA_ROLES or text_mode not in {"ocr_text", "visual_only", "unknown"}:
        return None
    if not isinstance(contract, dict):
        return None
    required_contract = {
        "primary_event_promotion": bool(contract.get("primary_event_promotion")),
        "event_identity_grounded": bool(contract.get("event_identity_grounded")),
        "not_utility_document": bool(contract.get("not_utility_document")),
    }
    try:
        confidence = max(0.0, min(1.0, float(value.get("confidence"))))
    except Exception:
        return None
    # Fail closed: a poster label requires all three LLM predicates and a
    # configured confidence floor. Deterministic code validates consistency;
    # it never promotes another role into a poster.
    if role == "event_identity_poster" and (
        not all(required_contract.values())
        or confidence < _env_float("EVENT_MEDIA_ROLE_POSTER_CONFIDENCE", 0.88)
    ):
        return None
    focal = value.get("focal_point")
    if focal is not None:
        if not isinstance(focal, dict):
            return None
        try:
            focal = {
                "x": max(0.0, min(1.0, float(focal.get("x")))),
                "y": max(0.0, min(1.0, float(focal.get("y")))),
            }
        except Exception:
            return None
    evidence = [str(item)[:240] for item in list(value.get("evidence") or [])[:6]]
    return {
        "media_role": role,
        "image_text_mode": text_mode,
        "primary_purpose": str(value.get("primary_purpose") or "")[:160],
        "confidence": confidence,
        "reason_code": str(value.get("reason_code") or "unspecified")[:120],
        "poster_contract": required_contract,
        "focal_point": focal,
        "safe_crop": bool(value.get("safe_crop", False)),
        "evidence": evidence,
    }


def image_geometry_model() -> str:
    return (os.getenv("EVENT_IMAGE_GEOMETRY_MODEL") or "gemma-4-31b-it").strip()


def _normalize_yxyx_box(value: Any) -> list[float] | None:
    if not isinstance(value, (list, tuple)) or len(value) != 4:
        return None
    try:
        ymin, xmin, ymax, xmax = [float(item) for item in value]
    except Exception:
        return None
    if not all(0.0 <= item <= 1000.0 for item in (ymin, xmin, ymax, xmax)):
        return None
    if ymax <= ymin or xmax <= xmin:
        return None
    return [
        round(ymin / 1000.0, 6),
        round(xmin / 1000.0, 6),
        round(ymax / 1000.0, 6),
        round(xmax / 1000.0, 6),
    ]


def _validated_image_geometry(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    raw_faces = value.get("face_boxes_yxyx")
    if not isinstance(raw_faces, list) or len(raw_faces) > IMAGE_GEOMETRY_MAX_FACE_BOXES:
        return None
    faces: list[list[float]] = []
    for raw in raw_faces:
        box = _normalize_yxyx_box(raw)
        if box is None:
            return None
        faces.append(box)
    valuable = _normalize_yxyx_box(value.get("valuable_region_yxyx"))
    if valuable is None:
        return None
    try:
        confidence = float(value.get("valuable_region_confidence"))
    except Exception:
        return None
    if not 0.0 <= confidence <= 1.0:
        return None
    return {
        "face_boxes_yxyx": faces,
        "valuable_region_yxyx": valuable,
        "valuable_region_confidence": round(confidence, 6),
        "reason_code": str(value.get("reason_code") or "viewer_value_region")[:120],
    }


def _image_geometry_prompt() -> str:
    return (
        "Analyze ONE image for metadata used later by independent crop mechanisms. "
        "Do not choose an aspect ratio and do not construct a final crop. "
        "Coordinates are integers 0..1000, origin top-left. "
        "face_boxes_yxyx: up to 25 largest/clearest visible human faces as tight "
        "[ymin,xmin,ymax,xmax] boxes; return [] when no face is visible. "
        "valuable_region_yxyx: the smallest coherent rectangle containing the part with maximum "
        "viewer value and meaning; prioritize principal people/faces, the main subject and essential "
        "visual identity or key text. Use the full frame only when it is genuinely all essential. "
        "valuable_region_confidence is 0..1. reason_code is a short stable snake_case label. "
        "Return JSON only."
    )


@lru_cache(maxsize=4)
def _get_image_geometry_client(pool_csv: str):
    from google_ai import GoogleAIClient, SecretsProvider
    from main import get_supabase_client

    pool = [item.strip() for item in pool_csv.split(",") if item.strip()]
    default_env = pool[0] if pool else "GOOGLE_API_KEY4"
    client = GoogleAIClient(
        supabase_client=get_supabase_client(),
        secrets_provider=SecretsProvider(),
        consumer="smart_update_image_geometry",
        account_name="smart-update-image-geometry",
        default_env_var_name=default_env,
        reserve_key_envs=pool,
        reserve_overflow_key_envs=[],
    )
    client.allow_reserve_fallback = False
    client.allow_local_limiter_fallback = False
    client.allow_local_limiter_on_reserve_error = False
    client.fallback_models = []
    # One provider attempt per item. The durable outbox/backfill runner owns
    # retry timing so a single image can never turn into an unpaced burst.
    client.max_retries = 1
    client.provider_timeout_seconds = float(
        _env_int("EVENT_IMAGE_GEOMETRY_PROVIDER_TIMEOUT_SEC", 90, lo=10, hi=120)
    )
    return client


async def _call_image_geometry_provider(media: DownloadedPoster):
    pool_csv = (
        os.getenv("EVENT_IMAGE_GEOMETRY_GOOGLE_KEY_ENVS")
        or "GOOGLE_API_KEY4,GOOGLE_API_KEY5"
    ).strip()
    client = _get_image_geometry_client(pool_csv)
    raw, usage = await client.generate_content_async(
        model=image_geometry_model(),
        prompt=[
            {"inline_data": {"mime_type": media.mime_type, "data": media.data}},
            _image_geometry_prompt(),
        ],
        generation_config={
            # Hosted Gemma 4's tested generation envelope. Determinism comes
            # from the compact contract and strict validator, not temperature 0.
            "temperature": 1.0,
            "top_p": 0.95,
            "top_k": 64,
            "response_mime_type": "application/json",
            "response_schema": _IMAGE_GEOMETRY_SCHEMA,
            "thinking_config": {
                "include_thoughts": False,
                "thinking_level": "MINIMAL",
            },
        },
        max_output_tokens=_env_int(
            "EVENT_IMAGE_GEOMETRY_MAX_OUTPUT_TOKENS", 768, lo=192, hi=768
        ),
    )
    return _validated_image_geometry(_parse_json_object(raw)), usage


async def _current_geometry_for_poster(session: Any, poster: EventPoster):
    if not poster.image_geometry_id:
        return None
    geometry = await session.get(EventImageGeometry, int(poster.image_geometry_id))
    if (
        geometry is not None
        and geometry.status == "classified"
        and geometry.model == image_geometry_model()
        and geometry.prompt_version == IMAGE_GEOMETRY_PROMPT_VERSION
        and bool(str(poster.pixel_sha256 or "").strip())
        and poster.pixel_sha256 == geometry.pixel_sha256
    ):
        return geometry
    return None


async def analyze_event_poster_geometry(
    event_id: int,
    poster_id: int,
    db: Any,
) -> ImageGeometryOutcome:
    """Analyze or reuse geometry for one approved image; never choose a crop."""

    async with db.get_session() as session:
        poster = await session.get(EventPoster, int(poster_id))
        if poster is None or int(poster.event_id) != int(event_id):
            return ImageGeometryOutcome("missing", int(poster_id), error="poster_not_found")
        current = await _current_geometry_for_poster(session, poster)
        if current is not None:
            return ImageGeometryOutcome(
                "classified",
                int(poster_id),
                geometry_id=int(current.id or 0),
                pixel_sha256=current.pixel_sha256,
                cache_hit=True,
            )
        try:
            downloaded_identity = _poster_visual_identity_snapshot(poster)
            media = await _download_poster(poster)
        except Exception as exc:
            # A stale/404 public object is an item-level failure, not a reason
            # to abort a resumable external batch or the outbox worker.
            return ImageGeometryOutcome(
                "error",
                int(poster_id),
                error=f"image_download_failed:{type(exc).__name__}:{exc}"[:500],
            )

    fingerprints = await asyncio.to_thread(compute_image_fingerprints, media.data)
    if fingerprints is None:
        return ImageGeometryOutcome(
            "error", int(poster_id), error="image_fingerprint_failed"
        )
    pixel_sha256 = str(fingerprints.pixel_sha256)
    model = image_geometry_model()

    async with db.get_session() as session:
        poster = await session.get(EventPoster, int(poster_id))
        if poster is None or int(poster.event_id) != int(event_id):
            return ImageGeometryOutcome("missing", int(poster_id), error="poster_drifted")
        if _poster_visual_identity_snapshot(poster) != downloaded_identity:
            await enqueue_event_media_review_job(
                session,
                int(event_id),
                next_run_at=datetime.now(timezone.utc) + timedelta(seconds=2),
                force_followup=True,
            )
            await session.commit()
            return ImageGeometryOutcome(
                "pending",
                int(poster_id),
                error="poster_drifted_during_download",
            )
        _apply_pixel_fingerprints(poster, fingerprints)
        provider_identity = _poster_visual_identity_snapshot(poster)
        session.add(poster)
        cached = (
            await session.execute(
                select(EventImageGeometry).where(
                    EventImageGeometry.pixel_sha256 == pixel_sha256,
                    EventImageGeometry.model == model,
                    EventImageGeometry.prompt_version == IMAGE_GEOMETRY_PROMPT_VERSION,
                    EventImageGeometry.status == "classified",
                )
            )
        ).scalar_one_or_none()
        if cached is not None:
            await session.execute(
                update(EventPoster)
                .where(EventPoster.pixel_sha256 == pixel_sha256)
                .values(image_geometry_id=int(cached.id or 0))
            )
            await session.commit()
            return ImageGeometryOutcome(
                "classified",
                int(poster_id),
                geometry_id=int(cached.id or 0),
                pixel_sha256=pixel_sha256,
                cache_hit=True,
            )
        if not await _claim_feature_budget(
            session,
            "image_geometry",
            _env_int("EVENT_IMAGE_GEOMETRY_DAILY_CALLS", 100, lo=0, hi=1500),
        ):
            await session.commit()
            return ImageGeometryOutcome(
                "pending",
                int(poster_id),
                pixel_sha256=pixel_sha256,
                error="daily_budget_exhausted",
            )
        await session.commit()

    decision: dict[str, Any] | None = None
    usage = None
    error: str | None = None
    try:
        decision, usage = await _call_image_geometry_provider(media)
        if decision is None:
            error = "invalid_image_geometry_response"
    except Exception as exc:
        error = f"{type(exc).__name__}:{exc}"[:500]

    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        poster = await session.get(EventPoster, int(poster_id))
        if (
            poster is None
            or int(poster.event_id) != int(event_id)
            or _poster_visual_identity_snapshot(poster) != provider_identity
        ):
            if poster is not None and int(poster.event_id) == int(event_id):
                await enqueue_event_media_review_job(
                    session,
                    int(event_id),
                    next_run_at=now + timedelta(seconds=2),
                    force_followup=True,
                )
                await session.commit()
            return ImageGeometryOutcome(
                "pending",
                int(poster_id),
                pixel_sha256=pixel_sha256,
                provider_called=True,
                error="poster_drifted_during_geometry",
            )
        geometry = (
            await session.execute(
                select(EventImageGeometry).where(
                    EventImageGeometry.pixel_sha256 == pixel_sha256,
                    EventImageGeometry.model == model,
                    EventImageGeometry.prompt_version == IMAGE_GEOMETRY_PROMPT_VERSION,
                )
            )
        ).scalar_one_or_none()
        if geometry is None:
            geometry = EventImageGeometry(
                pixel_sha256=pixel_sha256,
                model=model,
                prompt_version=IMAGE_GEOMETRY_PROMPT_VERSION,
            )
        geometry.status = "classified" if decision is not None else "error"
        geometry.source_width = int(fingerprints.width or 0) or None
        geometry.source_height = int(fingerprints.height or 0) or None
        geometry.face_boxes_yxyx_json = (
            decision["face_boxes_yxyx"] if decision is not None else None
        )
        geometry.valuable_region_yxyx_json = (
            decision["valuable_region_yxyx"] if decision is not None else None
        )
        geometry.valuable_region_confidence = (
            float(decision["valuable_region_confidence"])
            if decision is not None
            else None
        )
        geometry.reason_code = decision["reason_code"] if decision is not None else error
        geometry.prompt_tokens = int(getattr(usage, "input_tokens", 0) or 0)
        geometry.completion_tokens = int(getattr(usage, "output_tokens", 0) or 0)
        geometry.total_tokens = int(getattr(usage, "total_tokens", 0) or 0)
        geometry.analyzed_at = now
        geometry.updated_at = now
        session.add(geometry)
        await session.flush()
        await session.execute(
            update(EventPoster)
            .where(EventPoster.pixel_sha256 == pixel_sha256)
            .values(image_geometry_id=int(geometry.id or 0))
        )
        await session.commit()
        return ImageGeometryOutcome(
            geometry.status,
            int(poster_id),
            geometry_id=int(geometry.id or 0),
            pixel_sha256=pixel_sha256,
            provider_called=True,
            error=error,
        )


async def _next_geometry_candidate_id(session: Any, event_id: int) -> int | None:
    model = image_geometry_model()
    retry_before = datetime.now(timezone.utc) - timedelta(hours=20)
    stmt = (
        select(EventPoster.id)
        .outerjoin(
            EventImageGeometry,
            EventPoster.image_geometry_id == EventImageGeometry.id,
        )
        .where(
            EventPoster.event_id == int(event_id),
            EventPoster.review_status == APPROVED,
            or_(
                EventPoster.image_geometry_id.is_(None),
                EventImageGeometry.id.is_(None),
                EventPoster.pixel_sha256.is_(None),
                EventImageGeometry.pixel_sha256.is_(None),
                EventPoster.pixel_sha256 != EventImageGeometry.pixel_sha256,
                EventImageGeometry.model.is_(None),
                EventImageGeometry.model != model,
                EventImageGeometry.prompt_version.is_(None),
                EventImageGeometry.prompt_version != IMAGE_GEOMETRY_PROMPT_VERSION,
                EventImageGeometry.status.is_(None),
                and_(
                    EventImageGeometry.status != "classified",
                    EventImageGeometry.status != "error",
                ),
                and_(
                    EventImageGeometry.status == "error",
                    EventImageGeometry.updated_at <= retry_before,
                ),
            ),
        )
        .order_by(EventPoster.display_order.asc(), EventPoster.id.asc())
        .limit(1)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def _enqueue_geometry_followup_if_needed(
    session: Any,
    event_id: int,
    *,
    delay_seconds: int | None = None,
) -> bool:
    candidate_id = await _next_geometry_candidate_id(session, int(event_id))
    if candidate_id is None:
        return False
    delay = delay_seconds
    if delay is None:
        delay = _env_int("EVENT_IMAGE_GEOMETRY_FOLLOWUP_SECONDS", 7, lo=5, hi=3600)
    return await enqueue_event_media_review_job(
        session,
        int(event_id),
        next_run_at=datetime.now(timezone.utc) + timedelta(seconds=int(delay)),
        force_followup=True,
    )


def _media_role_prompt(event: Event, poster: EventPoster) -> str:
    evidence = {
        "event": _event_context(event),
        "image_observation": {
            "ocr_title": str(poster.ocr_title or "")[:500],
            "ocr_text": str(poster.ocr_text or "")[:3000],
            "width": poster.width,
            "height": poster.height,
        },
    }
    return (
        "Классифицируй основное назначение ОДНОГО изображения относительно конкретного события. "
        "event_identity_poster допустим только если изображение прежде всего идентифицирует или рекламирует "
        "именно это событие, event identity подтверждена визуально/текстом и изображение не является служебным "
        "документом. Карточки услуг, цен, правил, условий посещения, расписания, программы, карты, парковки, "
        "спонсорские плашки и общая реклама площадки НЕ являются афишей, даже если содержат название/логотип "
        "события. Фотография события/артиста/места — event_photo. При недостатке контекста выбери unknown_document "
        "для текстового документа или unknown_visual для нетекстового визуала. Не делай вывод по порядку, имени "
        "файла, OCR-факту или соотношению сторон. focal_point описывает главный визуальный объект в 0..1; safe_crop "
        "разрешай только если умеренный crop не разрушит лица, текст или event identity. Верни только JSON по schema.\n"
        + json.dumps(evidence, ensure_ascii=False, sort_keys=True)
    )


def _media_role_candidate_condition(context_hash: str, *, now: datetime | None = None):
    eligible_at = now or datetime.now(timezone.utc)
    return or_(
        EventPoster.media_semantic_status.is_(None),
        EventPoster.media_semantic_status == "stale",
        and_(
            EventPoster.media_semantic_status == "pending",
            or_(
                EventPoster.media_semantic_classified_at.is_(None),
                EventPoster.media_semantic_classified_at <= eligible_at,
            ),
        ),
        EventPoster.media_semantic_prompt_version != MEDIA_ROLE_PROMPT_VERSION,
        EventPoster.media_semantic_prompt_version.is_(None),
        EventPoster.media_semantic_context_hash != context_hash,
        EventPoster.media_semantic_context_hash.is_(None),
    )


def _media_role_transient_retry_at(error: str | None) -> datetime | None:
    value = str(error or "").strip().lower()
    if not value:
        return None
    day_markers = (
        "rpd",
        "per day",
        "daily",
        "free_tier_requests",
        "requests per day",
    )
    if any(marker in value for marker in day_markers):
        return _next_utc_day()
    minute_markers = (
        "429",
        "resource_exhausted",
        "rate limit",
        "ratelimit",
        "rpm",
        "tpm",
        "quota",
    )
    if any(marker in value for marker in minute_markers):
        return datetime.now(timezone.utc) + timedelta(
            seconds=_env_int("EVENT_MEDIA_ROLE_RATE_RETRY_SECONDS", 600, lo=60, hi=3600)
        )
    temporary_markers = (
        "timeout",
        "timed out",
        "temporarily unavailable",
        "service unavailable",
        "connectionerror",
        "connection reset",
        "servererror",
        "http 500",
        "http 502",
        "http 503",
        "http 504",
    )
    if any(marker in value for marker in temporary_markers):
        return datetime.now(timezone.utc) + timedelta(
            seconds=_env_int("EVENT_MEDIA_ROLE_TRANSIENT_RETRY_SECONDS", 900, lo=60, hi=7200)
        )
    return None


async def _next_media_role_retry_at(
    session: Any,
    event_id: int,
    context_hash: str,
    *,
    now: datetime,
) -> datetime | None:
    return (
        await session.execute(
            select(func.min(EventPoster.media_semantic_classified_at)).where(
                EventPoster.event_id == int(event_id),
                EventPoster.review_status == APPROVED,
                EventPoster.media_semantic_status == "pending",
                EventPoster.media_semantic_prompt_version == MEDIA_ROLE_PROMPT_VERSION,
                EventPoster.media_semantic_context_hash == context_hash,
                EventPoster.media_semantic_classified_at > now,
            )
        )
    ).scalar_one_or_none()


async def _classify_event_poster_role(event_id: int, poster_id: int, db: Any) -> bool:
    async with db.get_session() as session:
        event = await session.get(Event, int(event_id))
        poster = await session.get(EventPoster, int(poster_id))
        if event is None or poster is None:
            return False
        context_hash = _context_hash(event)
        if (
            poster.media_semantic_status == "classified"
            and poster.media_semantic_prompt_version == MEDIA_ROLE_PROMPT_VERSION
            and poster.media_semantic_context_hash == context_hash
        ):
            return False
        role_visual_identity = _poster_visual_identity_snapshot(poster)
        media = await _download_poster(poster)
        model = (os.getenv("EVENT_MEDIA_ROLE_MODEL") or "gemini-3.1-flash-lite").strip()
        if not await _claim_feature_budget(
            session,
            "semantic_role",
            _env_int("EVENT_MEDIA_ROLE_DAILY_CALLS", 150, lo=0, hi=5000),
        ):
            retry_at = _next_utc_day()
            poster.media_semantic_status = "pending"
            poster.media_semantic_reason_code = "daily_budget_exhausted"
            poster.media_semantic_classified_at = retry_at
            session.add(poster)
            await enqueue_event_media_review_job(
                session,
                int(event_id),
                next_run_at=retry_at,
                force_followup=True,
            )
            await session.commit()
            return False
        await session.commit()

    decision: dict[str, Any] | None = None
    error: str | None = None
    try:
        from google_ai import GoogleAIClient, SecretsProvider
        from main import get_supabase_client

        pool_csv = (
            os.getenv("EVENT_MEDIA_ROLE_GOOGLE_KEY_ENVS")
            or os.getenv("EVENT_MEDIA_ROLE_GOOGLE_KEY_ENV")
            or "GOOGLE_API_KEY4,GOOGLE_API_KEY5"
        ).strip()
        pool = [item.strip() for item in pool_csv.split(",") if item.strip()]
        default_env = pool[0] if pool else "GOOGLE_API_KEY4"
        client = GoogleAIClient(
            supabase_client=get_supabase_client(),
            secrets_provider=SecretsProvider(),
            consumer="event_media_role",
            account_name="event-media-role",
            default_env_var_name=default_env,
            reserve_key_envs=pool,
            reserve_overflow_key_envs=[],
        )
        client.allow_reserve_fallback = False
        client.allow_local_limiter_fallback = False
        client.allow_local_limiter_on_reserve_error = False
        client.fallback_models = []
        client.max_retries = 1
        client.provider_timeout_seconds = float(
            _env_int("EVENT_MEDIA_ROLE_PROVIDER_TIMEOUT_SEC", 45, lo=10, hi=120)
        )
        raw, _usage = await client.generate_content_async(
            model=model,
            prompt=[
                _media_role_prompt(event, poster),
                {"inline_data": {"mime_type": media.mime_type, "data": media.data}},
            ],
            generation_config={
                "temperature": 0,
                "response_mime_type": "application/json",
                "response_json_schema": _MEDIA_ROLE_SCHEMA,
            },
            max_output_tokens=_env_int("EVENT_MEDIA_ROLE_MAX_OUTPUT_TOKENS", 512, lo=256, hi=1024),
        )
        decision = _validated_media_role(_parse_json_object(raw))
        if decision is None:
            error = "invalid_or_low_confidence_media_role"
    except Exception as exc:
        error = f"{type(exc).__name__}:{exc}"[:500]

    async with db.get_session() as session:
        event = await session.get(Event, int(event_id))
        poster = await session.get(EventPoster, int(poster_id))
        if event is None or poster is None:
            return False
        if (
            _poster_visual_identity_snapshot(poster) != role_visual_identity
            or _context_hash(event) != context_hash
        ):
            now = datetime.now(timezone.utc)
            await enqueue_event_media_review_job(
                session,
                int(event_id),
                next_run_at=now + timedelta(seconds=2),
                force_followup=True,
            )
            await session.commit()
            logger.info(
                "event_media.provider_result_discarded event_id=%s poster_id=%s stage=semantic_role reason=identity_drift",
                event_id,
                poster_id,
            )
            return False
        poster.media_semantic_model = model
        poster.media_semantic_prompt_version = MEDIA_ROLE_PROMPT_VERSION
        poster.media_semantic_context_hash = _context_hash(event)
        now = datetime.now(timezone.utc)
        retry_at = _media_role_transient_retry_at(error) if decision is None else None
        poster.media_semantic_classified_at = retry_at or now
        if decision is None:
            poster.media_role = None
            poster.media_role_confidence = None
            poster.media_semantic_status = "pending" if retry_at is not None else "error"
            poster.media_semantic_reason_code = (
                f"transient_provider_error:{error}"[:500]
                if retry_at is not None
                else error or "invalid_response"
            )
            poster.media_semantic_evidence_json = None
            poster.focal_x = None
            poster.focal_y = None
            poster.safe_crop = False
        else:
            poster.image_text_mode = decision["image_text_mode"]
            poster.media_role = decision["media_role"]
            poster.media_role_confidence = float(decision["confidence"])
            poster.media_semantic_status = "classified"
            poster.media_semantic_reason_code = decision["reason_code"]
            poster.media_semantic_evidence_json = decision
            focal = decision.get("focal_point")
            poster.focal_x = focal.get("x") if focal else None
            poster.focal_y = focal.get("y") if focal else None
            poster.safe_crop = bool(decision.get("safe_crop", False))
        poster.updated_at = datetime.now(timezone.utc)
        session.add(poster)
        context_hash = _context_hash(event)
        remaining = (
            await session.execute(
                select(EventPoster.id)
                .where(
                    EventPoster.event_id == int(event_id),
                    EventPoster.review_status == APPROVED,
                    _media_role_candidate_condition(context_hash, now=now),
                )
                .limit(1)
            )
        ).scalar_one_or_none()
        future_retry = await _next_media_role_retry_at(
            session, int(event_id), context_hash, now=now
        )
        next_run_at = now + timedelta(seconds=2) if remaining is not None else future_retry
        if next_run_at is not None:
            if getattr(next_run_at, "tzinfo", None) is None:
                next_run_at = next_run_at.replace(tzinfo=timezone.utc)
            await enqueue_event_media_review_job(
                session,
                int(event_id),
                next_run_at=next_run_at,
                force_followup=True,
            )
        await session.commit()
    return decision is not None


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
            context_hash = _context_hash(event)
            role_candidate_id = (
                await session.execute(
                    select(EventPoster.id)
                    .where(
                        EventPoster.event_id == int(event_id),
                        EventPoster.review_status == APPROVED,
                        _media_role_candidate_condition(context_hash, now=now),
                    )
                    .order_by(EventPoster.display_order.asc(), EventPoster.id.asc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            role_retry_at = await _next_media_role_retry_at(
                session, int(event_id), context_hash, now=now
            )
            pair_retry_at = (
                await session.execute(
                    select(func.min(EventMediaPairReview.next_run_at)).where(
                        EventMediaPairReview.event_id == int(event_id),
                        EventMediaPairReview.status.in_(("pending", "deferred")),
                        EventMediaPairReview.next_run_at > now,
                    )
                )
            ).scalar_one_or_none()
            if pair_retry_at is not None and getattr(pair_retry_at, "tzinfo", None) is None:
                pair_retry_at = pair_retry_at.replace(tzinfo=timezone.utc)
            geometry_candidate_id = await _next_geometry_candidate_id(
                session, int(event_id)
            )
            await session.commit()
            if role_candidate_id is not None:
                classified = await _classify_event_poster_role(
                    int(event_id), int(role_candidate_id), db
                )
                if classified:
                    async with db.get_session() as followup_session:
                        await _enqueue_geometry_followup_if_needed(
                            followup_session, int(event_id)
                        )
                        if pair_retry_at is not None:
                            await enqueue_event_media_review_job(
                                followup_session,
                                int(event_id),
                                next_run_at=pair_retry_at,
                                force_followup=True,
                            )
                        await followup_session.commit()
                    return bool(projection_changed or classified)
                # Semantic role and geometry have independent daily budgets.
                # If role classification cannot make progress, fall through
                # so its still-pending first poster cannot starve geometry.
            if geometry_candidate_id is not None:
                outcome = await analyze_event_poster_geometry(
                    int(event_id), int(geometry_candidate_id), db
                )
                async with db.get_session() as followup_session:
                    next_run_at = None
                    if outcome.status == "pending":
                        next_run_at = _next_utc_day()
                    elif outcome.status == "error":
                        # Match _next_geometry_candidate_id's stale-error
                        # window. Scheduling only at the next UTC boundary can
                        # wake too early, see no candidate, and lose the retry.
                        next_run_at = datetime.now(timezone.utc) + timedelta(
                            hours=20, minutes=1
                        )
                    remaining_id = await _next_geometry_candidate_id(
                        followup_session, int(event_id)
                    )
                    if (
                        remaining_id is not None
                        or outcome.status in {"pending", "error"}
                        or role_retry_at is not None
                        or pair_retry_at is not None
                    ):
                        geometry_followup_needed = (
                            remaining_id is not None
                            or outcome.status in {"pending", "error"}
                        )
                        if geometry_followup_needed:
                            requested_next_run = next_run_at or datetime.now(
                                timezone.utc
                            ) + timedelta(
                                seconds=_env_int(
                                    "EVENT_IMAGE_GEOMETRY_FOLLOWUP_SECONDS",
                                    7,
                                    lo=5,
                                    hi=3600,
                                )
                            )
                        else:
                            requested_next_run = role_retry_at or pair_retry_at
                        for retry_at in (role_retry_at, pair_retry_at):
                            if retry_at is None:
                                continue
                            if getattr(retry_at, "tzinfo", None) is None:
                                retry_at = retry_at.replace(tzinfo=timezone.utc)
                            requested_next_run = (
                                retry_at
                                if requested_next_run is None
                                else min(requested_next_run, retry_at)
                            )
                        assert requested_next_run is not None
                        await enqueue_event_media_review_job(
                            followup_session,
                            int(event_id),
                            next_run_at=requested_next_run,
                            force_followup=True,
                        )
                    await followup_session.commit()
                return bool(projection_changed or outcome.status == "classified")
            future_retry_at = None
            for retry_at in (role_retry_at, pair_retry_at):
                if retry_at is None:
                    continue
                if getattr(retry_at, "tzinfo", None) is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                future_retry_at = (
                    retry_at
                    if future_retry_at is None
                    else min(future_retry_at, retry_at)
                )
            if future_retry_at is not None:
                async with db.get_session() as followup_session:
                    await enqueue_event_media_review_job(
                        followup_session,
                        int(event_id),
                        next_run_at=future_retry_at,
                        force_followup=True,
                    )
                    await followup_session.commit()
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
        # Resolving the final pending pair can promote a new approved poster
        # after the last pair-review job has already been consumed. Arm the
        # independent semantic/geometry enrichment pass for that new poster.
        await _enqueue_geometry_followup_if_needed(session, int(event_id))

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
