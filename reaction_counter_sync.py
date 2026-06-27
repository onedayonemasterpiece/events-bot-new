from __future__ import annotations

import asyncio
import json
import logging
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from db import Database

_TABLE = "personalization_event_reaction_counter"


@dataclass(frozen=True, slots=True)
class SourceReactionCounter:
    """Compact public counter aggregate for one canonical event.

    The values are raw source metrics only: no popularity coefficients, boosts or
    normalization are applied here. `service_likes_count` is intentionally not
    represented because first-party likes are owned by the personalization write
    path and must not be overwritten by source sync.
    """

    event_id: int
    source_likes_count: int
    source_views_count: int
    source_engagement_sources_count: int


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _env_flag(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return bool(default)
    return raw not in {"0", "false", "no", "off"}


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _event_id_list(event_ids: Iterable[int] | None) -> list[int] | None:
    if event_ids is None:
        return None
    out: list[int] = []
    seen: set[int] = set()
    for raw in event_ids:
        try:
            value = int(raw)
        except Exception:
            continue
        if value <= 0 or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _where_event_ids(alias: str, event_ids: list[int] | None) -> tuple[str, list[int]]:
    if not event_ids:
        return "", []
    placeholders = ",".join("?" for _ in event_ids)
    return f" WHERE {alias}.event_id IN ({placeholders})", list(event_ids)


def _safe_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return max(0, int(value))
    try:
        return max(0, int(value))
    except Exception:
        return 0


def _add_metric(
    buckets: dict[int, dict[tuple[Any, ...], tuple[int, int]]],
    *,
    event_id: Any,
    key: tuple[Any, ...],
    likes: Any,
    views: Any,
) -> None:
    eid = int(event_id)
    likes_i = _safe_int(likes)
    views_i = _safe_int(views)
    bucket = buckets.setdefault(eid, {})
    prev = bucket.get(key)
    if prev is None:
        bucket[key] = (likes_i, views_i)
    else:
        bucket[key] = (max(prev[0], likes_i), max(prev[1], views_i))


async def aggregate_source_reaction_counters(
    db: Database,
    event_ids: Iterable[int] | None = None,
) -> list[SourceReactionCounter]:
    """Aggregate raw TG/VK source likes/views per event from Fly SQLite.

    Contract:
    - one distinct source post contributes at most once per event;
    - repeated age buckets use max(raw likes/views), not a sum;
    - no coefficients or popularity boosts are applied;
    - when `event_ids` is provided, zero rows are returned for requested events
      without metrics so Supabase can be reset to zero if needed.
    """

    target_ids = _event_id_list(event_ids)
    buckets: dict[int, dict[tuple[Any, ...], tuple[int, int]]] = {}

    async with db.raw_conn() as conn:
        # Telegram: canonical exact source URL mapping.
        where, params = _where_event_ids("es", target_ids)
        cur = await conn.execute(
            f"""
            SELECT es.event_id, m.source_id, m.message_id,
                   MAX(COALESCE(m.likes, 0)) AS likes,
                   MAX(COALESCE(m.views, 0)) AS views
            FROM event_source es
            JOIN telegram_post_metric m
              ON m.source_url = es.source_url
            {where}
            GROUP BY es.event_id, m.source_id, m.message_id
            """,
            params,
        )
        for row in await cur.fetchall():
            _add_metric(
                buckets,
                event_id=row[0],
                key=("tg", int(row[1]), int(row[2])),
                likes=row[3],
                views=row[4],
            )

        # Telegram: structured chat username + message id fallback for rows where
        # the URL representation changed but the canonical Telegram source is known.
        where, params = _where_event_ids("es", target_ids)
        cur = await conn.execute(
            f"""
            SELECT es.event_id, m.source_id, m.message_id,
                   MAX(COALESCE(m.likes, 0)) AS likes,
                   MAX(COALESCE(m.views, 0)) AS views
            FROM event_source es
            JOIN telegram_source ts
              ON lower(replace(COALESCE(es.source_chat_username, ''), '@', '')) = lower(ts.username)
            JOIN telegram_post_metric m
              ON m.source_id = ts.id
             AND m.message_id = es.source_message_id
            {where}
              {'AND' if where else 'WHERE'} es.source_message_id IS NOT NULL
            GROUP BY es.event_id, m.source_id, m.message_id
            """,
            params,
        )
        for row in await cur.fetchall():
            _add_metric(
                buckets,
                event_id=row[0],
                key=("tg", int(row[1]), int(row[2])),
                likes=row[3],
                views=row[4],
            )

        # VK: canonical exact source URL mapping.
        where, params = _where_event_ids("es", target_ids)
        cur = await conn.execute(
            f"""
            SELECT es.event_id, m.group_id, m.post_id,
                   MAX(COALESCE(m.likes, 0)) AS likes,
                   MAX(COALESCE(m.views, 0)) AS views
            FROM event_source es
            JOIN vk_post_metric m
              ON m.source_url = es.source_url
            {where}
            GROUP BY es.event_id, m.group_id, m.post_id
            """,
            params,
        )
        for row in await cur.fetchall():
            _add_metric(
                buckets,
                event_id=row[0],
                key=("vk", int(row[1]), int(row[2])),
                likes=row[3],
                views=row[4],
            )

        # VK: preferred inbox-import mapping when available. This covers imported
        # VK posts even if event_source URL formatting diverges.
        where, params = _where_event_ids("ie", target_ids)
        try:
            cur = await conn.execute(
                f"""
                SELECT ie.event_id, m.group_id, m.post_id,
                       MAX(COALESCE(m.likes, 0)) AS likes,
                       MAX(COALESCE(m.views, 0)) AS views
                FROM vk_inbox_import_event ie
                JOIN vk_inbox i
                  ON i.id = ie.inbox_id
                JOIN vk_post_metric m
                  ON m.group_id = i.group_id
                 AND m.post_id = i.post_id
                {where}
                GROUP BY ie.event_id, m.group_id, m.post_id
                """,
                params,
            )
            for row in await cur.fetchall():
                _add_metric(
                    buckets,
                    event_id=row[0],
                    key=("vk", int(row[1]), int(row[2])),
                    likes=row[3],
                    views=row[4],
                )
        except Exception:
            logging.debug("VK inbox reaction-counter mapping unavailable", exc_info=True)

    if target_ids is not None:
        for eid in target_ids:
            buckets.setdefault(int(eid), {})

    counters: list[SourceReactionCounter] = []
    for event_id in sorted(buckets):
        metrics = buckets[event_id]
        likes_sum = sum(v[0] for v in metrics.values())
        views_sum = sum(v[1] for v in metrics.values())
        counters.append(
            SourceReactionCounter(
                event_id=int(event_id),
                source_likes_count=int(likes_sum),
                source_views_count=int(views_sum),
                source_engagement_sources_count=int(len(metrics)),
            )
        )
    return counters


def build_source_counter_payload(
    counters: Iterable[SourceReactionCounter],
    *,
    refreshed_at: str | None = None,
) -> list[dict[str, Any]]:
    ts = refreshed_at or _utc_iso()
    out: list[dict[str, Any]] = []
    for counter in counters:
        out.append(
            {
                "event_id": int(counter.event_id),
                "source_likes_count": int(counter.source_likes_count),
                "source_views_count": int(counter.source_views_count),
                "source_engagement_sources_count": int(counter.source_engagement_sources_count),
                "source_refreshed_at": ts,
                "updated_at": ts,
            }
        )
    return out


def _personalization_supabase_rest_config() -> tuple[str, str] | None:
    base_url = (os.getenv("PERSONALIZATION_SUPABASE_URL") or "").strip().rstrip("/")
    key = (
        os.getenv("PERSONALIZATION_SUPABASE_SECRET_KEY")
        or os.getenv("PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY")
        or ""
    ).strip()
    if not base_url or not key:
        return None
    return base_url, key


def _postgrest_upsert_source_counter_payload(
    payload: list[dict[str, Any]],
    *,
    timeout: float,
    chunk_size: int = 500,
) -> int:
    if not payload:
        return 0
    config = _personalization_supabase_rest_config()
    if not config:
        logging.debug("Personalization Supabase source-counter sync skipped: env is not configured")
        return 0
    base_url, key = config
    endpoint = f"{base_url}/rest/v1/{_TABLE}?on_conflict=event_id"
    sent = 0
    for start in range(0, len(payload), max(1, int(chunk_size))):
        chunk = payload[start : start + max(1, int(chunk_size))]
        data = json.dumps(chunk, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        request = urllib.request.Request(
            endpoint,
            data=data,
            method="POST",
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates,return=minimal",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=float(timeout)) as response:
                status = int(getattr(response, "status", 0) or 0)
                if status >= 400:
                    raise RuntimeError(f"Supabase counter upsert failed with HTTP {status}")
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="replace")[:1000]
            except Exception:
                body = ""
            raise RuntimeError(f"Supabase counter upsert failed with HTTP {exc.code}: {body}") from exc
        sent += len(chunk)
    return sent


async def upsert_source_reaction_counters_to_supabase(
    counters: Iterable[SourceReactionCounter],
    *,
    raise_on_error: bool = False,
) -> int:
    if not _env_flag("PERSONALIZATION_REACTION_COUNTER_SYNC_ENABLED", True):
        return 0
    payload = build_source_counter_payload(counters)
    if not payload:
        return 0
    timeout = _env_float("PERSONALIZATION_REACTION_COUNTER_SYNC_TIMEOUT_SECONDS", 10.0)
    try:
        return await asyncio.to_thread(
            _postgrest_upsert_source_counter_payload,
            payload,
            timeout=timeout,
        )
    except Exception:
        if raise_on_error:
            raise
        logging.warning("Personalization Supabase source-counter sync failed", exc_info=True)
        return 0


async def sync_source_reaction_counters(
    db: Database,
    *,
    event_ids: Iterable[int] | None = None,
    raise_on_error: bool = False,
) -> dict[str, int]:
    counters = await aggregate_source_reaction_counters(db, event_ids=event_ids)
    sent = await upsert_source_reaction_counters_to_supabase(counters, raise_on_error=raise_on_error)
    return {
        "events_aggregated": int(len(counters)),
        "events_upserted": int(sent),
        "source_likes_count": int(sum(c.source_likes_count for c in counters)),
        "source_views_count": int(sum(c.source_views_count for c in counters)),
        "source_engagement_sources_count": int(sum(c.source_engagement_sources_count for c in counters)),
    }


async def _resolve_event_ids_for_telegram_metric(
    db: Database,
    *,
    source_id: int,
    message_id: int,
    source_url: str | None,
) -> list[int]:
    ids: set[int] = set()
    async with db.raw_conn() as conn:
        if source_url:
            cur = await conn.execute(
                "SELECT DISTINCT event_id FROM event_source WHERE source_url=?",
                (str(source_url),),
            )
            ids.update(int(r[0]) for r in await cur.fetchall() if r and r[0] is not None)
        cur = await conn.execute(
            "SELECT username FROM telegram_source WHERE id=?",
            (int(source_id),),
        )
        row = await cur.fetchone()
        username = str(row[0]).lstrip("@").lower() if row and row[0] else None
        if username:
            cur = await conn.execute(
                """
                SELECT DISTINCT event_id
                FROM event_source
                WHERE source_message_id=?
                  AND lower(replace(COALESCE(source_chat_username, ''), '@', ''))=?
                """,
                (int(message_id), username),
            )
            ids.update(int(r[0]) for r in await cur.fetchall() if r and r[0] is not None)
    return sorted(ids)


async def _resolve_event_ids_for_vk_metric(
    db: Database,
    *,
    group_id: int,
    post_id: int,
    source_url: str | None,
) -> list[int]:
    ids: set[int] = set()
    async with db.raw_conn() as conn:
        if source_url:
            cur = await conn.execute(
                "SELECT DISTINCT event_id FROM event_source WHERE source_url=?",
                (str(source_url),),
            )
            ids.update(int(r[0]) for r in await cur.fetchall() if r and r[0] is not None)
        try:
            cur = await conn.execute(
                """
                SELECT DISTINCT ie.event_id
                FROM vk_inbox_import_event ie
                JOIN vk_inbox i ON i.id = ie.inbox_id
                WHERE i.group_id=? AND i.post_id=?
                """,
                (int(group_id), int(post_id)),
            )
            ids.update(int(r[0]) for r in await cur.fetchall() if r and r[0] is not None)
        except Exception:
            logging.debug("VK inbox reaction-counter event resolution unavailable", exc_info=True)
    return sorted(ids)


_PENDING_EVENT_IDS: set[int] = set()
_PENDING_DB: Database | None = None
_PENDING_TASK: asyncio.Task[None] | None = None
_PENDING_LOCK: asyncio.Lock | None = None


def _pending_lock() -> asyncio.Lock:
    global _PENDING_LOCK
    if _PENDING_LOCK is None:
        _PENDING_LOCK = asyncio.Lock()
    return _PENDING_LOCK


async def _flush_pending_after_delay(delay: float) -> None:
    global _PENDING_TASK, _PENDING_DB
    if delay > 0:
        await asyncio.sleep(float(delay))
    lock = _pending_lock()
    async with lock:
        event_ids = sorted(_PENDING_EVENT_IDS)
        _PENDING_EVENT_IDS.clear()
        db = _PENDING_DB
        _PENDING_TASK = None
    if db is None or not event_ids:
        return
    await sync_source_reaction_counters(db, event_ids=event_ids, raise_on_error=False)


async def queue_source_reaction_counter_sync(db: Database, event_ids: Iterable[int]) -> None:
    ids = _event_id_list(event_ids) or []
    if not ids or not _env_flag("PERSONALIZATION_REACTION_COUNTER_SYNC_ENABLED", True):
        return
    if _personalization_supabase_rest_config() is None:
        return
    # In tests/one-shot scripts this can be set to 0 for deterministic inline sync.
    delay = max(0.0, _env_float("PERSONALIZATION_REACTION_COUNTER_SYNC_DEBOUNCE_SECONDS", 3.0))
    if _env_flag("PERSONALIZATION_REACTION_COUNTER_SYNC_INLINE", False) or delay <= 0.0:
        await sync_source_reaction_counters(db, event_ids=ids, raise_on_error=False)
        return

    global _PENDING_TASK, _PENDING_DB
    lock = _pending_lock()
    async with lock:
        _PENDING_EVENT_IDS.update(ids)
        _PENDING_DB = db
        if _PENDING_TASK is None or _PENDING_TASK.done():
            _PENDING_TASK = asyncio.create_task(_flush_pending_after_delay(delay))


async def sync_source_reaction_counters_for_telegram_metric(
    db: Database,
    *,
    source_id: int,
    message_id: int,
    source_url: str | None,
) -> None:
    try:
        event_ids = await _resolve_event_ids_for_telegram_metric(
            db,
            source_id=int(source_id),
            message_id=int(message_id),
            source_url=source_url,
        )
        await queue_source_reaction_counter_sync(db, event_ids)
    except Exception:
        logging.warning("Failed to queue Telegram source reaction-counter sync", exc_info=True)


async def sync_source_reaction_counters_for_vk_metric(
    db: Database,
    *,
    group_id: int,
    post_id: int,
    source_url: str | None,
) -> None:
    try:
        event_ids = await _resolve_event_ids_for_vk_metric(
            db,
            group_id=int(group_id),
            post_id=int(post_id),
            source_url=source_url,
        )
        await queue_source_reaction_counter_sync(db, event_ids)
    except Exception:
        logging.warning("Failed to queue VK source reaction-counter sync", exc_info=True)
