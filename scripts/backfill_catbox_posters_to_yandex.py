#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

from sqlalchemy import or_
from sqlmodel import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import Database
from media_dedup import prepare_image_for_supabase
from models import Event, EventPoster, EventSource
from net import http_call
from poster_media import PosterMedia, is_supabase_storage_url, process_media
from smart_event_update import PosterCandidate
from source_parsing.telegram.handlers import (
    _extract_photo_urls_from_public_tg_html,
    _parse_tg_source_url,
)
from supabase_storage import parse_storage_object_url
from vk_auto_queue import fetch_vk_post_text_and_photos
from vk_intake import _download_photo_media, _vk_wall_source_ids_from_url


CATBOX_HOST_RE = re.compile(r"(?:^|//)(?:files\.)?catbox\.moe/", re.IGNORECASE)
TG_HOST_RE = re.compile(r"(?:^|//)t\.me/", re.IGNORECASE)
VK_WALL_RE = re.compile(r"vk\.com/wall-?\d+_\d+", re.IGNORECASE)
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}


@dataclass(slots=True)
class ExistingPosterState:
    id: int
    poster_hash: str | None
    phash: str | None
    catbox_url: str | None
    supabase_url: str | None
    supabase_path: str | None
    ocr_text: str | None
    ocr_title: str | None
    updated_at: datetime | None


@dataclass(slots=True)
class FetchedPoster:
    data: bytes
    raw_sha256: str
    dhash_hex: str | None
    hosted_url: str | None
    hosted_path: str | None
    original_url: str | None
    name: str


@dataclass(slots=True)
class BackfillResult:
    event_id: int
    title: str
    source_kind: str
    source_url: str | None
    status: str
    changed_rows: int = 0
    inserted_rows: int = 0
    deleted_rows: int = 0
    photo_urls_changed: bool = False
    enqueued_telegraph: bool = False
    fetched: int = 0
    matched: int = 0
    unmatched_existing: int = 0
    unmatched_fetched: int = 0
    note: str | None = None


def _get_default_db_path() -> str:
    return (
        os.getenv("DB_PATH")
        or ("/tmp/db.sqlite" if os.path.exists("/tmp/db.sqlite") else "db.sqlite")
    )


def _is_catbox_url(url: str | None) -> bool:
    return bool(CATBOX_HOST_RE.search(str(url or "").strip()))


def _is_tg_url(url: str | None) -> bool:
    return bool(TG_HOST_RE.search(str(url or "").strip()))


def _is_vk_url(url: str | None) -> bool:
    return bool(VK_WALL_RE.search(str(url or "").strip()))


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value or "").strip().replace("Z", "+00:00")
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(raw)
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _safe_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item or "").strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except Exception:
            return [raw]
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item or "").strip()]
        return []
    return []


def _normalize_image_name(url: str, *, prefix: str, index: int) -> str:
    try:
        ext = (Path(urlparse(url).path).suffix or "").lower()
    except Exception:
        ext = ""
    if ext not in IMAGE_EXTENSIONS:
        ext = ".jpg"
    return f"{prefix}_{index}{ext}"


def _candidate_source_urls(event: Event, sources: Iterable[EventSource], *, source_kind: str) -> list[str]:
    urls: list[str] = []

    def _add(url: str | None) -> None:
        raw = str(url or "").strip()
        if not raw or raw in urls:
            return
        if source_kind == "tg" and not _is_tg_url(raw):
            return
        if source_kind == "vk" and not _is_vk_url(raw):
            return
        urls.append(raw)

    if source_kind == "tg":
        _add(event.source_post_url)
    else:
        _add(event.source_vk_post_url)
        _add(event.source_post_url)

    for source in sources:
        _add(getattr(source, "source_url", None))
    return urls


def _event_needs_backfill(event: Event, posters: list[ExistingPosterState]) -> bool:
    if any(_is_catbox_url(url) for url in (event.photo_urls or [])):
        return True
    for row in posters:
        if row.catbox_url and not row.supabase_url:
            return True
    return False


def _row_display_order(rows: list[ExistingPosterState], photo_urls: list[str]) -> list[int]:
    index_by_url = {
        str(url).strip(): idx
        for idx, url in enumerate(photo_urls or [])
        if str(url or "").strip()
    }
    keyed: list[tuple[int, int]] = []
    for idx, row in enumerate(rows):
        best = index_by_url.get(str(row.supabase_url or "").strip(), 10_000)
        best = min(best, index_by_url.get(str(row.catbox_url or "").strip(), 10_000))
        keyed.append((best, idx))
    keyed.sort()
    return [idx for _best, idx in keyed]


def _match_existing_rows(
    *,
    existing_rows: list[ExistingPosterState],
    fetched_rows: list[FetchedPoster],
    photo_urls: list[str],
    allow_positional_match: bool,
) -> tuple[dict[int, int], list[int], list[int], list[str]]:
    matches: dict[int, int] = {}
    notes: list[str] = []
    remaining_existing = set(range(len(existing_rows)))
    remaining_fetched = set(range(len(fetched_rows)))

    by_hash: dict[str, list[int]] = {}
    by_phash: dict[str, list[int]] = {}
    for idx, row in enumerate(existing_rows):
        if row.poster_hash:
            by_hash.setdefault(str(row.poster_hash).strip(), []).append(idx)
        if row.phash:
            by_phash.setdefault(str(row.phash).strip().lower(), []).append(idx)

    for fetch_idx, fetched in enumerate(fetched_rows):
        candidates = [idx for idx in by_hash.get(fetched.raw_sha256, []) if idx in remaining_existing]
        if len(candidates) == 1:
            row_idx = candidates[0]
            matches[row_idx] = fetch_idx
            remaining_existing.discard(row_idx)
            remaining_fetched.discard(fetch_idx)
            notes.append("match:poster_hash")

    for fetch_idx in list(sorted(remaining_fetched)):
        fetched = fetched_rows[fetch_idx]
        if not fetched.dhash_hex:
            continue
        candidates = [idx for idx in by_phash.get(fetched.dhash_hex.lower(), []) if idx in remaining_existing]
        if len(candidates) == 1:
            row_idx = candidates[0]
            matches[row_idx] = fetch_idx
            remaining_existing.discard(row_idx)
            remaining_fetched.discard(fetch_idx)
            notes.append("match:phash")

    if allow_positional_match and remaining_existing and remaining_fetched and len(remaining_existing) == len(remaining_fetched):
        ordered_existing = [idx for idx in _row_display_order(existing_rows, photo_urls) if idx in remaining_existing]
        ordered_fetched = sorted(remaining_fetched)
        for row_idx, fetch_idx in zip(ordered_existing, ordered_fetched):
            matches[row_idx] = fetch_idx
            remaining_existing.discard(row_idx)
            remaining_fetched.discard(fetch_idx)
        notes.append("match:position")

    return matches, sorted(remaining_existing), sorted(remaining_fetched), notes


@contextmanager
def _managed_storage_upload_mode() -> Iterable[None]:
    old = os.getenv("UPLOAD_IMAGES_SUPABASE_MODE")
    os.environ["UPLOAD_IMAGES_SUPABASE_MODE"] = "only"
    try:
        yield
    finally:
        if old is None:
            os.environ.pop("UPLOAD_IMAGES_SUPABASE_MODE", None)
        else:
            os.environ["UPLOAD_IMAGES_SUPABASE_MODE"] = old


def _poster_webp_quality() -> int:
    raw = (os.getenv("SUPABASE_POSTERS_WEBP_QUALITY") or "82").strip()
    try:
        return max(1, min(100, int(raw)))
    except Exception:
        return 82


async def _build_fetched_poster(
    *,
    data: bytes,
    name: str,
    original_url: str | None,
) -> FetchedPoster | None:
    if not data:
        return None
    prepared = await asyncio.to_thread(
        prepare_image_for_supabase,
        data,
        dhash_size=16,
        webp_quality=_poster_webp_quality(),
    )
    return FetchedPoster(
        data=data,
        raw_sha256=hashlib.sha256(data).hexdigest(),
        dhash_hex=(prepared.dhash_hex if prepared else None),
        hosted_url=None,
        hosted_path=None,
        original_url=(str(original_url or "").strip() or None),
        name=name,
    )


async def _upload_fetched_poster(fetched: FetchedPoster) -> FetchedPoster | None:
    with _managed_storage_upload_mode():
        poster_items, _msg = await process_media(
            [(fetched.data, fetched.name)],
            need_catbox=True,
            need_ocr=False,
        )
    if not poster_items:
        return None
    poster: PosterMedia = poster_items[0]
    hosted_url = str(poster.supabase_url or poster.catbox_url or "").strip()
    if not hosted_url or not is_supabase_storage_url(hosted_url):
        return None
    parsed = parse_storage_object_url(hosted_url)
    fetched.hosted_url = hosted_url
    fetched.hosted_path = parsed[1] if parsed else None
    return fetched


async def _download_tg_public_images(source_url: str, *, limit: int) -> list[FetchedPoster]:
    username, message_id = _parse_tg_source_url(source_url)
    if not username or not message_id:
        return []
    page_url = f"https://t.me/s/{username}/{int(message_id)}"
    try:
        resp = await http_call(
            "tg_backfill_page",
            "GET",
            page_url,
            timeout=20,
            retries=2,
            backoff=1.0,
            headers={"User-Agent": os.getenv("HTTP_IMAGE_UA", "Mozilla/5.0")},
        )
    except Exception:
        return []
    if int(getattr(resp, "status_code", 0) or 0) != 200:
        return []
    try:
        html_text = (getattr(resp, "content", b"") or b"").decode("utf-8", errors="ignore")
    except Exception:
        return []
    urls = _extract_photo_urls_from_public_tg_html(
        html_text,
        username=username,
        message_id=int(message_id),
        limit=limit,
    )
    out: list[FetchedPoster] = []
    for idx, url in enumerate(urls[: max(1, int(limit or 1))], start=1):
        try:
            img = await http_call(
                "tg_backfill_img",
                "GET",
                url,
                timeout=25,
                retries=2,
                backoff=1.0,
                headers={"User-Agent": os.getenv("HTTP_IMAGE_UA", "Mozilla/5.0")},
            )
        except Exception:
            continue
        if int(getattr(img, "status_code", 0) or 0) != 200:
            continue
        data = getattr(img, "content", b"") or b""
        if not data or len(data) > 8 * 1024 * 1024:
            continue
        fetched = await _build_fetched_poster(
            data=data,
            name=_normalize_image_name(url, prefix=f"tg_{username}_{int(message_id)}", index=idx),
            original_url=url,
        )
        if fetched:
            out.append(fetched)
    return out


async def _download_vk_images(source_url: str, *, db: Database, limit: int) -> list[FetchedPoster]:
    group_id, post_id = _vk_wall_source_ids_from_url(source_url)
    if not group_id or not post_id:
        return []
    _text, photo_urls, _published_at, _metrics, fetch_status = await fetch_vk_post_text_and_photos(
        group_id,
        post_id,
        db=db,
        bot=None,
        limit=max(1, int(limit or 1)),
    )
    if not fetch_status.ok or not photo_urls:
        return []
    downloaded = await _download_photo_media(photo_urls[: max(1, int(limit or 1))])
    out: list[FetchedPoster] = []
    for idx, item in enumerate(downloaded, start=1):
        data, name = item
        original_url = photo_urls[idx - 1] if idx - 1 < len(photo_urls) else None
        fetched = await _build_fetched_poster(
            data=data,
            name=name or _normalize_image_name(original_url or "", prefix=f"vk_{group_id}_{post_id}", index=idx),
            original_url=original_url,
        )
        if fetched:
            out.append(fetched)
    return out


async def _enqueue_telegraph_rebuild(db: Database, event_id: int) -> None:
    import main as main_mod

    await main_mod.enqueue_job(db, int(event_id), main_mod.JobTask.telegraph_build, depends_on=None)


async def _load_existing_posters(session, event_id: int) -> list[ExistingPosterState]:
    result = await session.execute(
        select(EventPoster).where(EventPoster.event_id == int(event_id)).order_by(EventPoster.id.asc())
    )
    out: list[ExistingPosterState] = []
    for row in result.scalars().all():
        out.append(
            ExistingPosterState(
                id=int(row.id),
                poster_hash=str(row.poster_hash or "").strip() or None,
                phash=str(row.phash or "").strip() or None,
                catbox_url=str(row.catbox_url or "").strip() or None,
                supabase_url=str(row.supabase_url or "").strip() or None,
                supabase_path=str(row.supabase_path or "").strip() or None,
                ocr_text=row.ocr_text,
                ocr_title=row.ocr_title,
                updated_at=_parse_dt(row.updated_at),
            )
        )
    return out


async def _close_http_clients() -> None:
    try:
        main_mod = sys.modules.get("main") or sys.modules.get("__main__")

        if main_mod is not None and hasattr(main_mod, "close_shared_session"):
            await main_mod.close_shared_session()
    except Exception:
        pass
    try:
        import net as net_mod

        session = getattr(net_mod, "_session", None)
        if session is not None and not session.closed:
            await session.close()
        connector = getattr(net_mod, "_connector", None)
        if connector is not None and not connector.closed:
            await connector.close()
        net_mod._session = None
        net_mod._connector = None
    except Exception:
        pass


def _build_updated_photo_urls(
    *,
    event: Event,
    existing_rows: list[ExistingPosterState],
    fetched_rows: list[FetchedPoster],
    matches: dict[int, int],
    unmatched_existing: list[int],
    keep_partial_catbox: bool,
) -> list[str]:
    out: list[str] = []
    managed_by_row: dict[int, str] = {}
    for row_idx, fetch_idx in matches.items():
        managed_by_row[row_idx] = fetched_rows[fetch_idx].hosted_url
    managed_extra = [
        fetched_rows[idx].hosted_url
        for idx in range(len(fetched_rows))
        if idx not in set(matches.values())
    ]

    for url in event.photo_urls or []:
        raw = str(url or "").strip()
        if not raw:
            continue
        replaced = False
        if _is_catbox_url(raw):
            for row_idx, row in enumerate(existing_rows):
                if str(row.catbox_url or "").strip() == raw and row_idx in managed_by_row:
                    managed = managed_by_row[row_idx]
                    if managed and managed not in out:
                        out.append(managed)
                    replaced = True
                    break
            if not replaced and keep_partial_catbox and raw not in out:
                out.append(raw)
            continue
        if raw not in out:
            out.append(raw)

    prefix: list[str] = []
    for managed in list(managed_by_row.values()) + managed_extra:
        if managed and managed not in prefix and managed not in out:
            prefix.append(managed)
    out = prefix + out

    if not out:
        for managed in list(managed_by_row.values()) + managed_extra:
            if managed and managed not in out:
                out.append(managed)
        if keep_partial_catbox:
            for row_idx in unmatched_existing:
                raw = str(existing_rows[row_idx].catbox_url or "").strip()
                if raw and raw not in out:
                    out.append(raw)
    return out


async def _process_event(
    *,
    db: Database,
    event: Event,
    source_kind: str,
    source_url: str | None,
    existing_rows: list[ExistingPosterState],
    fetched_rows: list[FetchedPoster],
    apply: bool,
    allow_partial: bool,
    allow_positional_match: bool,
    enqueue_telegraph: bool,
) -> BackfillResult:
    result = BackfillResult(
        event_id=int(event.id or 0),
        title=str(event.title or ""),
        source_kind=source_kind,
        source_url=source_url,
        status="dry_run",
        fetched=len(fetched_rows),
    )
    catbox_only_rows = [
        row for row in existing_rows if row.catbox_url and not row.supabase_url
    ]
    matches, unmatched_existing, unmatched_fetched, notes = _match_existing_rows(
        existing_rows=catbox_only_rows,
        fetched_rows=fetched_rows,
        photo_urls=list(event.photo_urls or []),
        allow_positional_match=allow_positional_match,
    )
    result.matched = len(matches)
    result.unmatched_existing = len(unmatched_existing)
    result.unmatched_fetched = len(unmatched_fetched)

    full_match = not unmatched_existing
    if not fetched_rows:
        result.status = "fetch_failed"
        result.note = "no_source_images"
        return result
    if catbox_only_rows and not matches and not allow_partial:
        result.status = "skipped"
        result.note = "no_confident_matches"
        return result
    if catbox_only_rows and not full_match and not allow_partial:
        result.status = "partial_skipped"
        result.note = ",".join(notes) or "partial_match"
        return result

    keep_partial_catbox = bool(not full_match)
    if apply:
        uploaded_rows = []
        for fetched in fetched_rows:
            uploaded = await _upload_fetched_poster(fetched)
            if uploaded:
                uploaded_rows.append(uploaded)
        if not uploaded_rows:
            result.status = "upload_failed"
            result.note = "managed_storage_upload_failed"
            return result
        fetched_rows = uploaded_rows
        matches, unmatched_existing, unmatched_fetched, notes = _match_existing_rows(
            existing_rows=catbox_only_rows,
            fetched_rows=fetched_rows,
            photo_urls=list(event.photo_urls or []),
            allow_positional_match=allow_positional_match,
        )
        result.fetched = len(fetched_rows)
        result.matched = len(matches)
        result.unmatched_existing = len(unmatched_existing)
        result.unmatched_fetched = len(unmatched_fetched)
        full_match = not unmatched_existing
        keep_partial_catbox = bool(not full_match)

    updated_photo_urls = _build_updated_photo_urls(
        event=event,
        existing_rows=catbox_only_rows,
        fetched_rows=fetched_rows,
        matches=matches,
        unmatched_existing=unmatched_existing,
        keep_partial_catbox=keep_partial_catbox,
    )
    result.photo_urls_changed = updated_photo_urls != list(event.photo_urls or [])

    if not apply:
        result.status = "would_apply" if (matches or fetched_rows) else "dry_run"
        result.note = ",".join(notes) or ("full_match" if full_match else "partial_match")
        return result

    async with db.get_session() as session:
        event_db = await session.get(Event, int(event.id))
        if event_db is None:
            result.status = "missing"
            result.note = "event_not_found"
            return result

        rows_db = (
            await session.execute(
                select(EventPoster).where(EventPoster.event_id == int(event.id)).order_by(EventPoster.id.asc())
            )
        ).scalars().all()
        catbox_db_rows = [row for row in rows_db if row.catbox_url and not row.supabase_url]

        now = datetime.now(timezone.utc)
        matched_fetch_indices = set(matches.values())
        for row_idx, fetch_idx in matches.items():
            if row_idx >= len(catbox_db_rows):
                continue
            row = catbox_db_rows[row_idx]
            fetched = fetched_rows[fetch_idx]
            row.supabase_url = fetched.hosted_url
            if fetched.hosted_path:
                row.supabase_path = fetched.hosted_path
            if fetched.dhash_hex and not getattr(row, "phash", None):
                row.phash = fetched.dhash_hex
            row.updated_at = now
            session.add(row)
            result.changed_rows += 1

        for fetch_idx, fetched in enumerate(fetched_rows):
            if fetch_idx in matched_fetch_indices:
                continue
            session.add(
                EventPoster(
                    event_id=int(event.id),
                    catbox_url=fetched.original_url if (allow_partial and _is_catbox_url(fetched.original_url)) else None,
                    supabase_url=fetched.hosted_url,
                    supabase_path=fetched.hosted_path,
                    poster_hash=fetched.raw_sha256,
                    phash=fetched.dhash_hex,
                    updated_at=now,
                )
            )
            result.inserted_rows += 1

        if result.photo_urls_changed:
            event_db.photo_urls = updated_photo_urls
            event_db.photo_count = len(updated_photo_urls)
            session.add(event_db)

        await session.commit()

    if enqueue_telegraph and result.event_id:
        await _enqueue_telegraph_rebuild(db, result.event_id)
        result.enqueued_telegraph = True

    result.status = "applied_full" if full_match else "applied_partial"
    result.note = ",".join(notes) or ("full_match" if full_match else "partial_match")
    return result


async def _iter_candidates(
    *,
    db: Database,
    source_kind: str,
    event_id: int | None,
    days: int | None,
    limit: int,
) -> list[tuple[Event, list[EventSource], list[ExistingPosterState], str | None]]:
    cutoff = None
    if days and int(days) > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(days))
    out: list[tuple[Event, list[EventSource], list[ExistingPosterState], str | None]] = []

    async with db.get_session() as session:
        stmt = select(Event).order_by(Event.added_at.desc(), Event.id.desc())
        if event_id:
            stmt = stmt.where(Event.id == int(event_id))
        elif cutoff:
            stmt = stmt.where(Event.added_at >= cutoff)
        events = list((await session.execute(stmt)).scalars().all())

        for event in events:
            source_rows = list(
                (
                    await session.execute(
                        select(EventSource).where(EventSource.event_id == int(event.id)).order_by(EventSource.id.asc())
                    )
                ).scalars().all()
            )
            poster_rows = await _load_existing_posters(session, int(event.id))
            if not _event_needs_backfill(event, poster_rows):
                continue
            source_urls = _candidate_source_urls(event, source_rows, source_kind=source_kind)
            source_url = source_urls[0] if source_urls else None
            if not source_url:
                continue
            out.append((event, source_rows, poster_rows, source_url))
            if limit > 0 and len(out) >= limit:
                break
    return out


def _print_result(res: BackfillResult) -> None:
    parts = [
        f"event_id={res.event_id}",
        f"status={res.status}",
        f"source={res.source_kind}",
        f"fetched={res.fetched}",
        f"matched={res.matched}",
        f"unmatched_existing={res.unmatched_existing}",
        f"unmatched_fetched={res.unmatched_fetched}",
    ]
    if res.changed_rows:
        parts.append(f"changed_rows={res.changed_rows}")
    if res.inserted_rows:
        parts.append(f"inserted_rows={res.inserted_rows}")
    if res.photo_urls_changed:
        parts.append("photo_urls_changed=1")
    if res.enqueued_telegraph:
        parts.append("telegraph_enqueued=1")
    if res.note:
        parts.append(f"note={res.note}")
    title = (res.title or "").strip().replace("\n", " ")[:120]
    if title:
        parts.append(f"title={title}")
    print(" ".join(parts))


async def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill catbox-backed event posters into Yandex Object Storage using "
            "the original Telegram/VK source post."
        )
    )
    parser.add_argument("--db", default=_get_default_db_path(), help="Path to SQLite DB")
    parser.add_argument("--source", choices=["tg", "vk"], required=True, help="Source kind to backfill")
    parser.add_argument("--event-id", type=int, default=0, help="Process only one event_id")
    parser.add_argument("--days", type=int, default=0, help="Only events added in the last N days")
    parser.add_argument("--limit", type=int, default=100, help="Max events to inspect (0 = unlimited)")
    parser.add_argument("--image-limit", type=int, default=5, help="Max images fetched per source post")
    parser.add_argument("--apply", action="store_true", help="Write changes to DB")
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Apply partial matches too; default is fail-closed for events with unmatched catbox poster rows",
    )
    parser.add_argument(
        "--no-positional-match",
        action="store_true",
        help="Disable position-based fallback when source and DB poster counts line up",
    )
    parser.add_argument(
        "--no-enqueue-telegraph",
        action="store_true",
        help="Do not enqueue telegraph_build for changed events",
    )
    args = parser.parse_args()

    db = Database(str(args.db))
    await db.init()
    try:
        candidates = await _iter_candidates(
            db=db,
            source_kind=str(args.source),
            event_id=int(args.event_id or 0) or None,
            days=int(args.days or 0) or None,
            limit=int(args.limit or 0),
        )
        print(
            f"candidates={len(candidates)} source={args.source} apply={int(bool(args.apply))} "
            f"allow_partial={int(bool(args.allow_partial))}"
        )
        stats: dict[str, int] = {}
        for event, _sources, poster_rows, source_url in candidates:
            if args.source == "tg":
                fetched = await _download_tg_public_images(str(source_url), limit=max(1, int(args.image_limit or 1)))
            else:
                fetched = await _download_vk_images(
                    str(source_url),
                    db=db,
                    limit=max(1, int(args.image_limit or 1)),
                )
            result = await _process_event(
                db=db,
                event=event,
                source_kind=str(args.source),
                source_url=source_url,
                existing_rows=poster_rows,
                fetched_rows=fetched,
                apply=bool(args.apply),
                allow_partial=bool(args.allow_partial),
                allow_positional_match=not bool(args.no_positional_match),
                enqueue_telegraph=bool(args.apply and not args.no_enqueue_telegraph),
            )
            stats[result.status] = stats.get(result.status, 0) + 1
            _print_result(result)
        summary = " ".join(f"{key}={stats[key]}" for key in sorted(stats))
        print(f"summary {summary or 'none=0'}")
        return 0
    finally:
        await _close_http_clients()
        await db.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
