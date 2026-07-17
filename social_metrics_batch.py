from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Awaitable, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from db import Database


BUCKETS: tuple[tuple[str, int], ...] = (
    ("1h", 60 * 60),
    ("6h", 6 * 60 * 60),
    ("24h", 24 * 60 * 60),
    ("72h", 72 * 60 * 60),
)
TERMINAL_STATUSES = {"collected", "not_found", "skipped_late"}
OWN_TG_USERNAME = "kldevents"
OWN_TG_AFISHA_USERNAME = "kenigevents"
OWN_VK_TARGET = "klgdevents"


@dataclass(slots=True, frozen=True)
class SocialPostTarget:
    platform: str
    publisher_id: str
    post_id: int
    source_url: str
    post_ts: int | None = None
    legacy_source_id: int | None = None
    owned: bool = False
    publication_kind: str = "external_event_source"

    @property
    def key(self) -> tuple[str, str, int]:
        return (self.platform, self.publisher_id, self.post_id)


@dataclass(slots=True, frozen=True)
class MetricPayload:
    post_id: int
    post_ts: int | None
    views: int | None
    likes: int | None
    comments: int | None
    shares: int | None
    reactions: dict[str, int] | None = None


@dataclass(slots=True, frozen=True)
class SnapshotWrite:
    target: SocialPostTarget
    age_bucket: str
    collected_ts: int
    payload: MetricPayload | None = None
    status: str = "collected"
    error_code: str | None = None


@dataclass(slots=True)
class BatchDiagnostics:
    enabled: bool = True
    targets: int = 0
    due: int = 0
    collected: int = 0
    not_found: int = 0
    errors: int = 0
    skipped_late: int = 0
    chunks: int = 0
    resolver: dict[str, Any] = field(default_factory=dict)
    telegram: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "targets": self.targets,
            "due": self.due,
            "collected": self.collected,
            "not_found": self.not_found,
            "errors": self.errors,
            "skipped_late": self.skipped_late,
            "chunks": self.chunks,
            "resolver": self.resolver,
            "telegram": self.telegram,
        }


VkFetcher = Callable[[int, Sequence[int]], Awaitable[dict[int, MetricPayload]]]
TgFetcher = Callable[[Sequence[SocialPostTarget]], Awaitable[dict[tuple[str, int], MetricPayload]]]


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or ("1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name) or str(default)).strip())
    except Exception:
        return int(default)


def _first_env(*names: str) -> str:
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def _env_seconds_range(name: str, default: tuple[float, float]) -> tuple[float, float]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        left, right = (float(part.strip()) for part in raw.split(",", 1))
    except (TypeError, ValueError):
        logging.warning("invalid %s=%r; using %s,%s", name, raw, *default)
        return default
    return max(0.0, min(left, right)), max(0.0, max(left, right))


async def _human_pause(name: str, default: tuple[float, float]) -> float:
    lower, upper = _env_seconds_range(name, default)
    delay = random.SystemRandom().uniform(lower, upper) if upper > lower else lower
    if delay > 0:
        await asyncio.sleep(delay)
    return delay


def _popular_tg_auth_raw() -> str:
    """Return only the dedicated popularity-reader session.

    E2E, S22, TELEGRAM_SESSION, the old generic SOCIAL_METRICS name and
    publishing/editor sessions are intentionally excluded.
    """
    return _first_env("TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR")


def _published_at_ts(value: Any) -> int | None:
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_vk_url(value: str | None) -> tuple[int, int] | None:
    match = re.search(r"wall(-?\d+)_(\d+)", str(value or ""))
    if not match:
        return None
    return abs(int(match.group(1))), int(match.group(2))


def _parse_tg_url(value: str | None) -> tuple[str, int] | None:
    raw = str(value or "")
    # Private channel links are t.me/c/<internal-chat-id>/<message-id>; "c"
    # is not a username. Callers with a canonical event message id must use
    # their known channel role instead.
    if re.search(r"(?:https?://)?t\.me/c/\d+/\d+", raw, flags=re.IGNORECASE):
        return None
    match = re.search(r"(?:https?://)?t\.me/(?:s/)?([A-Za-z0-9_]+)/([0-9]+)", raw)
    if not match:
        return None
    return match.group(1).lower(), int(match.group(2))


def _private_tg_message_id(value: str | None) -> int | None:
    match = re.search(
        r"(?:https?://)?t\.me/c/\d+/(\d+)(?:[/?#].*)?$",
        str(value or ""),
        flags=re.IGNORECASE,
    )
    return int(match.group(1)) if match else None


def _legacy_age_day(*, post_ts: int | None, collected_ts: int) -> int | None:
    if not isinstance(post_ts, int) or post_ts <= 0:
        return None
    age_day = max(0, int(collected_ts) - int(post_ts)) // 86400
    max_age = max(0, _env_int("POST_POPULARITY_MAX_AGE_DAY", 2))
    return int(age_day) if age_day <= max_age else None


def _metric_count(item: Mapping[str, Any], key: str) -> int | None:
    if key not in item:
        return None
    raw = item.get(key)
    value = raw.get("count") if isinstance(raw, Mapping) else raw
    return int(value) if isinstance(value, int) and value >= 0 else None


def _vk_item_payload(item: Mapping[str, Any]) -> MetricPayload:
    raw_date = item.get("date")
    post_ts = int(raw_date) if isinstance(raw_date, (int, float)) and raw_date > 0 else None
    return MetricPayload(
        post_id=int(item.get("id") or 0),
        post_ts=post_ts,
        views=_metric_count(item, "views"),
        likes=_metric_count(item, "likes"),
        comments=_metric_count(item, "comments"),
        shares=_metric_count(item, "reposts"),
    )


def _due_plan(
    *,
    post_ts: int | None,
    now_ts: int,
    terminal_buckets: set[str],
) -> tuple[str | None, tuple[str, ...]]:
    """Return the newest due bucket plus older buckets that must be marked late.

    A late run never copies one current counter value into several historical points.
    This keeps velocity honest after downtime while making the job self-healing.
    """
    if not isinstance(post_ts, int) or post_ts <= 0:
        return None, ()
    age = max(0, int(now_ts) - int(post_ts))
    due = [name for name, seconds in BUCKETS if age >= seconds and name not in terminal_buckets]
    if not due:
        return None, ()
    return due[-1], tuple(due[:-1])


def _merge_targets(targets: Iterable[SocialPostTarget]) -> list[SocialPostTarget]:
    merged: dict[tuple[str, str, int], SocialPostTarget] = {}
    for target in targets:
        if target.post_id <= 0 or not target.publisher_id:
            continue
        previous = merged.get(target.key)
        if previous is None:
            merged[target.key] = target
            continue
        merged[target.key] = SocialPostTarget(
            platform=target.platform,
            publisher_id=target.publisher_id,
            post_id=target.post_id,
            source_url=target.source_url or previous.source_url,
            post_ts=target.post_ts or previous.post_ts,
            legacy_source_id=target.legacy_source_id or previous.legacy_source_id,
            owned=target.owned or previous.owned,
            publication_kind=(
                target.publication_kind
                if target.publication_kind != "external_event_source"
                else previous.publication_kind
            ),
        )
    return sorted(merged.values(), key=lambda item: item.key)


def _active_event_sql(today: str) -> tuple[str, tuple[str, str]]:
    return (
        """
        COALESCE(NULLIF(TRIM(e.lifecycle_status), ''), 'active')='active'
        AND COALESCE(e.silent, 0)=0
        AND COALESCE(NULLIF(TRIM(e.identity_status), ''), 'canonical')='canonical'
        AND (e.date >= ? OR (e.end_date IS NOT NULL AND e.end_date >= ?))
        """,
        (today, today),
    )


async def _load_terminal_buckets(
    db: Database,
) -> dict[tuple[str, str, int], set[str]]:
    out: dict[tuple[str, str, int], set[str]] = {}
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT platform, publisher_id, post_id, age_bucket
            FROM social_metric_snapshot
            WHERE status IN ('collected','not_found','skipped_late')
            """
        )
        for platform, publisher_id, post_id, age_bucket in await cur.fetchall():
            out.setdefault((str(platform), str(publisher_id), int(post_id)), set()).add(str(age_bucket))
    return out


async def load_social_post_targets(
    db: Database,
    *,
    now_utc: datetime | None = None,
) -> list[SocialPostTarget]:
    now = now_utc or datetime.now(timezone.utc)
    today = now.astimezone(ZoneInfo("Europe/Kaliningrad")).date().isoformat()
    active_sql, active_params = _active_event_sql(today)
    event_channel = (
        os.getenv("SOCIAL_METRICS_TG_EVENT_CHANNEL") or OWN_TG_USERNAME
    ).strip().lstrip("@").lower()
    afisha_channel = (
        os.getenv("SOCIAL_METRICS_TG_AFISHA_CHANNEL") or OWN_TG_AFISHA_USERNAME
    ).strip().lstrip("@").lower()
    targets: list[SocialPostTarget] = []
    async with db.raw_conn() as conn:
        # Own Telegram publications remain discoverable even before the dedicated
        # session is configured. Their post date is bootstrapped by Telegram later.
        cur = await conn.execute(
            f"""
            SELECT e.tg_event_post_url, e.tg_event_post_id, MAX(m.message_ts)
            FROM event e
            LEFT JOIN telegram_source ts ON LOWER(ts.username)=LOWER(?)
            LEFT JOIN telegram_post_metric m
              ON m.source_id=ts.id AND m.message_id=e.tg_event_post_id
            WHERE {active_sql}
              AND e.tg_event_post_id IS NOT NULL
            GROUP BY e.tg_event_post_url, e.tg_event_post_id
            """,
            (event_channel, *active_params),
        )
        for url, message_id, message_ts in await cur.fetchall():
            parsed = _parse_tg_url(str(url or ""))
            username = parsed[0] if parsed else event_channel
            post_id = parsed[1] if parsed else int(message_id or 0)
            if post_id > 0:
                targets.append(
                    SocialPostTarget(
                        platform="telegram",
                        publisher_id=username,
                        post_id=post_id,
                        source_url=str(url or f"https://t.me/{username}/{post_id}"),
                        post_ts=int(message_ts) if isinstance(message_ts, int) else None,
                        owned=True,
                        publication_kind="event_announcement",
                    )
                )

        # The broad @kenigevents channel also contains digests, stories and
        # editorial/service posts. Only Bot-API forwards that already carry an
        # exact promo_exposure.event_id + source_url + target_url contract are
        # eligible here. This deliberately avoids text/hashtag guessing and
        # prevents a digest counter from being attributed to every event in it.
        cur = await conn.execute(
            f"""
            SELECT pe.details_json, pe.published_at,
                   e.tg_event_post_id, e.tg_event_post_url
            FROM promo_exposure pe
            JOIN event e ON e.id=pe.event_id
            WHERE {active_sql}
              AND pe.surface='tg_repost'
              AND pe.publish_status='TG_FORWARDED'
            """,
            active_params,
        )
        for details_json, published_at, event_message_id, event_post_url in await cur.fetchall():
            details = _json_object(details_json)
            source_url = str(details.get("source_url") or "")
            source = _parse_tg_url(source_url)
            target = _parse_tg_url(str(details.get("target_url") or ""))
            if not target or target[0] != afisha_channel:
                continue
            canonical_message_id = int(event_message_id or 0)
            if source:
                source_matches_event = (
                    source[0] == event_channel
                    and canonical_message_id > 0
                    and source[1] == canonical_message_id
                )
            else:
                private_message_id = _private_tg_message_id(source_url)
                canonical_private_id = _private_tg_message_id(str(event_post_url or ""))
                source_matches_event = (
                    canonical_message_id > 0
                    and private_message_id == canonical_message_id
                    and canonical_private_id == canonical_message_id
                )
            if not source_matches_event:
                continue
            targets.append(
                SocialPostTarget(
                    platform="telegram",
                    publisher_id=afisha_channel,
                    post_id=target[1],
                    source_url=f"https://t.me/{afisha_channel}/{target[1]}",
                    post_ts=_published_at_ts(published_at),
                    owned=True,
                    publication_kind="event_forward",
                )
            )

        # Poll winners are another exact event-forward ledger for @kenigevents.
        # poll_message_id and reply_message_id are intentionally not selected.
        cur = await conn.execute(
            f"""
            SELECT r.poll_chat_id, r.forwarded_message_id, r.updated_at
            FROM poll_repost_run r
            JOIN event e ON e.id=r.chosen_event_id
            WHERE {active_sql}
              AND r.status='forwarded'
              AND r.forwarded_message_id IS NOT NULL
            """,
            active_params,
        )
        for poll_chat_id, forwarded_message_id, updated_at in await cur.fetchall():
            username = str(poll_chat_id or "").strip().lstrip("@").lower()
            if username != afisha_channel:
                continue
            post_id = int(forwarded_message_id or 0)
            if post_id <= 0:
                continue
            targets.append(
                SocialPostTarget(
                    platform="telegram",
                    publisher_id=afisha_channel,
                    post_id=post_id,
                    source_url=f"https://t.me/{afisha_channel}/{post_id}",
                    post_ts=_published_at_ts(updated_at),
                    owned=True,
                    publication_kind="event_forward",
                )
            )

        # Resolved own VK publications. post_ts comes from the compatibility table
        # when available; missing dates are bootstrapped by the first API batch.
        cur = await conn.execute(
            f"""
            SELECT p.live_url, p.live_post_id, MAX(m.post_ts)
            FROM event_publication p
            JOIN event e ON e.id=p.event_id
            LEFT JOIN vk_post_metric m
              ON m.group_id=? AND m.post_id=p.live_post_id
            WHERE {active_sql}
              AND p.platform='vk' AND p.target=? AND p.status='published'
              AND p.live_post_id IS NOT NULL
            GROUP BY p.live_post_id
            """,
            (abs(_env_int("VK_EVENTS_GROUP_ID", 231920894)), *active_params, OWN_VK_TARGET),
        )
        group_id = abs(_env_int("VK_EVENTS_GROUP_ID", 231920894))
        for url, post_id, post_ts in await cur.fetchall():
            targets.append(
                SocialPostTarget(
                    platform="vk",
                    publisher_id=str(group_id),
                    post_id=int(post_id),
                    source_url=str(url or f"https://vk.com/wall-{group_id}_{int(post_id)}"),
                    post_ts=int(post_ts) if isinstance(post_ts, int) else None,
                    owned=True,
                    publication_kind="event_announcement",
                )
            )

        # Already-known external source posts. Exact URL joins intentionally keep
        # this MVP bounded to sources the monitoring pipelines have verified.
        cur = await conn.execute(
            f"""
            SELECT DISTINCT 'vk', CAST(m.group_id AS TEXT), m.post_id, m.source_url, m.post_ts, NULL
            FROM vk_post_metric m
            JOIN event_source es ON es.source_url=m.source_url
            JOIN event e ON e.id=es.event_id
            WHERE {active_sql}
            UNION
            SELECT DISTINCT 'telegram', LOWER(ts.username), m.message_id, m.source_url, m.message_ts, m.source_id
            FROM telegram_post_metric m
            JOIN telegram_source ts ON ts.id=m.source_id
            JOIN event_source es ON es.source_url=m.source_url
            JOIN event e ON e.id=es.event_id
            WHERE {active_sql}
            """,
            (*active_params, *active_params),
        )
        for platform, publisher_id, post_id, source_url, post_ts, legacy_source_id in await cur.fetchall():
            if platform == "telegram" and not _env_enabled(
                "SOCIAL_METRICS_TG_INCLUDE_EXTERNAL",
                default=False,
            ):
                # External Telegram posts already receive age_day metrics from
                # the monitoring pipeline. Keep this dedicated human-like
                # session bounded to our two managed channels by default.
                continue
            targets.append(
                SocialPostTarget(
                    platform=str(platform),
                    publisher_id=str(publisher_id),
                    post_id=int(post_id),
                    source_url=str(source_url or ""),
                    post_ts=int(post_ts) if isinstance(post_ts, int) else None,
                    legacy_source_id=int(legacy_source_id) if isinstance(legacy_source_id, int) else None,
                    publication_kind="external_event_source",
                )
            )
    return _merge_targets(targets)


async def _default_vk_fetcher(group_id: int, post_ids: Sequence[int]) -> dict[int, MetricPayload]:
    import poll_to_forward_popularity as pfp

    data = await pfp._vk_call_async(
        "wall.getById",
        {"posts": ",".join(f"-{abs(int(group_id))}_{int(post_id)}" for post_id in post_ids)},
    )
    if isinstance(data, Mapping) and data.get("error"):
        error = data.get("error")
        message = error.get("error_msg") if isinstance(error, Mapping) else str(error)
        raise RuntimeError(str(message or "vk_api_error"))
    response = data.get("response") if isinstance(data, Mapping) else None
    items = response.get("items") if isinstance(response, Mapping) else response
    out: dict[int, MetricPayload] = {}
    for item in items or []:
        if not isinstance(item, Mapping):
            continue
        payload = _vk_item_payload(item)
        if payload.post_id > 0:
            out[payload.post_id] = payload
    return out


async def _vk_raw_posts_rate_limited(
    group_id: int,
    post_ids: Sequence[int],
) -> dict[int, Mapping[str, Any]]:
    import poll_to_forward_popularity as pfp

    out: dict[int, Mapping[str, Any]] = {}
    pause = max(0, _env_int("SOCIAL_METRICS_VK_BATCH_PAUSE_MS", 350)) / 1000.0
    chunks = [list(post_ids[start : start + 100]) for start in range(0, len(post_ids), 100)]
    for index, chunk in enumerate(chunks):
        data = await pfp._vk_call_async(
            "wall.getById",
            {"posts": ",".join(f"-{abs(int(group_id))}_{int(post_id)}" for post_id in chunk)},
        )
        if isinstance(data, Mapping) and data.get("error"):
            raise RuntimeError("vk_resolver_get_by_id_error")
        response = data.get("response") if isinstance(data, Mapping) else None
        items = response.get("items") if isinstance(response, Mapping) else response
        for item in items or []:
            if isinstance(item, Mapping) and int(item.get("id") or 0) > 0:
                out[int(item["id"])] = item
        if pause and index + 1 < len(chunks):
            await asyncio.sleep(pause)
    return out


async def _vk_wall_scan_rate_limited(group_id: int, *, limit: int) -> list[Mapping[str, Any]]:
    import poll_to_forward_popularity as pfp

    out: list[Mapping[str, Any]] = []
    pause = max(0, _env_int("SOCIAL_METRICS_VK_BATCH_PAUSE_MS", 350)) / 1000.0
    offsets = list(range(0, max(0, int(limit)), 100))
    for index, offset in enumerate(offsets):
        data = await pfp._vk_call_async(
            "wall.get",
            {"owner_id": f"-{abs(int(group_id))}", "filter": "owner", "count": 100, "offset": offset},
        )
        if isinstance(data, Mapping) and data.get("error"):
            raise RuntimeError("vk_resolver_wall_scan_error")
        response = data.get("response") if isinstance(data, Mapping) else None
        items = response.get("items") if isinstance(response, Mapping) else []
        chunk = [item for item in (items or []) if isinstance(item, Mapping)]
        if not chunk:
            break
        out.extend(chunk)
        if len(chunk) < 100:
            break
        if pause and index + 1 < len(offsets):
            await asyncio.sleep(pause)
    return out[: max(0, int(limit))]


async def resolve_owned_vk_publications_batch(
    db: Database,
    *,
    now_utc: datetime | None = None,
) -> dict[str, int]:
    """Resolve postponed/stale own VK IDs in one direct batch plus one wall scan."""
    import poll_to_forward_popularity as pfp

    now = now_utc or datetime.now(timezone.utc)
    today = now.astimezone(ZoneInfo("Europe/Kaliningrad")).date().isoformat()
    active_sql, active_params = _active_event_sql(today)
    group_id = abs(_env_int("VK_EVENTS_GROUP_ID", 231920894))
    cooldown = max(1, _env_int("SOCIAL_METRICS_VK_RESOLVE_COOLDOWN_HOURS", 6)) * 3600
    cutoff = int(now.timestamp()) - cooldown
    rows: list[dict[str, Any]] = []
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            f"""
            SELECT e.id, e.title, e.date, e.time, e.location_name, e.source_vk_post_url,
                   p.status, p.resolved_at
            FROM event e
            LEFT JOIN event_publication p
              ON p.event_id=e.id AND p.platform='vk' AND p.target=?
            WHERE {active_sql}
              AND e.source_vk_post_url IS NOT NULL
              AND TRIM(e.source_vk_post_url)!=''
              AND (p.live_post_id IS NULL OR p.status!='published')
            """,
            (OWN_VK_TARGET, *active_params),
        )
        for row in await cur.fetchall():
            resolved_at = str(row[7] or "")
            if str(row[6] or "") == "missing" and resolved_at:
                try:
                    if int(datetime.fromisoformat(resolved_at).timestamp()) > cutoff:
                        continue
                except Exception:
                    pass
            parsed = _parse_vk_url(str(row[5] or ""))
            if parsed and parsed[0] == group_id:
                rows.append(
                    {
                        "id": int(row[0]),
                        "title": str(row[1] or ""),
                        "date": str(row[2] or ""),
                        "time": str(row[3] or ""),
                        "location_name": str(row[4] or ""),
                        "stored_url": str(row[5] or ""),
                        "stored_post_id": int(parsed[1]),
                    }
                )
    if not rows:
        return {"candidates": 0, "published": 0, "missing": 0, "changed_id": 0}

    # Resolution needs raw post text, so it deliberately uses the existing raw
    # batch helper instead of the metrics-only collector fetcher.
    stored_ids = sorted({int(row["stored_post_id"]) for row in rows})
    direct = await _vk_raw_posts_rate_limited(group_id, stored_ids)

    unresolved = []
    matches: list[tuple[dict[str, Any], int | None, str, float, str]] = []
    for row in rows:
        item = direct.get(int(row["stored_post_id"]))
        if item:
            try:
                target_date = date.fromisoformat(str(row["date"])[:10])
            except Exception:
                target_date = now.astimezone(ZoneInfo("Europe/Kaliningrad")).date()
            event = type("EventMatch", (), row)()
            score, confidence = pfp._match_post_score(event, item, target_date)
            if score > 0:
                matches.append((row, int(item.get("id") or 0), "direct", confidence, "published"))
                continue
        unresolved.append(row)

    wall_items: list[Mapping[str, Any]] = []
    if unresolved:
        wall_items = await _vk_wall_scan_rate_limited(
            group_id,
            limit=max(100, _env_int("SOCIAL_METRICS_VK_WALL_SCAN_LIMIT", 1000)),
        )
        for row in unresolved:
            try:
                target_date = date.fromisoformat(str(row["date"])[:10])
            except Exception:
                target_date = now.astimezone(ZoneInfo("Europe/Kaliningrad")).date()
            event = type("EventMatch", (), row)()
            best: tuple[int, float, Mapping[str, Any]] | None = None
            for item in wall_items:
                score, confidence = pfp._match_post_score(event, item, target_date)
                if score <= 0:
                    continue
                item_ts = int(item.get("date") or 0)
                if best is None or (score, item_ts) > (best[0], int(best[2].get("date") or 0)):
                    best = (score, confidence, item)
            if best:
                matches.append((row, int(best[2].get("id") or 0), "wall_scan", best[1], "published"))
            else:
                matches.append((row, None, "unmatched", 0.0, "missing"))

    resolved_at = now.isoformat()
    write_rows = []
    changed = 0
    published = 0
    for row, live_post_id, method, confidence, status in matches:
        live_url = f"https://vk.com/wall-{group_id}_{live_post_id}" if live_post_id else None
        if live_post_id and int(live_post_id) != int(row["stored_post_id"]):
            changed += 1
        if live_post_id:
            published += 1
        write_rows.append(
            (
                int(row["id"]),
                "vk",
                OWN_VK_TARGET,
                row["stored_url"],
                live_url,
                int(row["stored_post_id"]),
                int(live_post_id) if live_post_id else None,
                method,
                float(confidence),
                status,
                resolved_at,
            )
        )
    async with db.raw_conn() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            await conn.executemany(
                """
                INSERT INTO event_publication(
                    event_id, platform, target, stored_url, live_url, stored_post_id,
                    live_post_id, match_method, match_confidence, status, resolved_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(event_id, platform, target) DO UPDATE SET
                    stored_url=excluded.stored_url,
                    live_url=excluded.live_url,
                    stored_post_id=excluded.stored_post_id,
                    live_post_id=excluded.live_post_id,
                    match_method=excluded.match_method,
                    match_confidence=excluded.match_confidence,
                    status=excluded.status,
                    resolved_at=excluded.resolved_at
                """,
                write_rows,
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
    return {
        "candidates": len(rows),
        "published": published,
        "missing": len(rows) - published,
        "changed_id": changed,
        "wall_items": len(wall_items),
    }


def _popular_tg_auth_bundle() -> dict[str, Any]:
    raw = _popular_tg_auth_raw()
    if not raw:
        raise RuntimeError("TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR is missing")
    try:
        padded = raw + "=" * (-len(raw) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"invalid TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR: {exc}") from exc
    if not isinstance(payload, dict) or not str(payload.get("session") or "").strip():
        raise RuntimeError("TELEGRAM_AUTH_BUNDLE_CHECK_POPULAR missing session")
    return payload


async def _default_tg_fetcher(
    targets: Sequence[SocialPostTarget],
) -> dict[tuple[str, int], MetricPayload]:
    from telethon import TelegramClient
    from telethon.sessions import StringSession

    bundle = _popular_tg_auth_bundle()
    api_id = int(_first_env("SOCIAL_METRICS_TG_API_ID", "TG_API_ID") or "0")
    api_hash = _first_env("SOCIAL_METRICS_TG_API_HASH", "TG_API_HASH")
    if not api_id or not api_hash:
        raise RuntimeError("SOCIAL_METRICS_TG_API_ID/HASH or TG_API_ID/HASH are missing")
    device_kwargs = {
        key: bundle[key]
        for key in (
            "device_model",
            "system_version",
            "app_version",
            "lang_code",
            "system_lang_code",
        )
        if bundle.get(key)
    }
    out: dict[tuple[str, int], MetricPayload] = {}
    # A scheduled reader should not reconnect at a perfectly fixed wall-clock
    # instant or hammer several channel history requests back-to-back. The
    # pauses are deliberately bounded and configurable; no read receipts,
    # typing actions or fake interactions are generated.
    await _human_pause("SOCIAL_METRICS_TG_STARTUP_DELAY_SECONDS", (4.0, 12.0))
    client = TelegramClient(
        StringSession(str(bundle["session"])),
        api_id,
        api_hash,
        flood_sleep_threshold=max(1, _env_int("SOCIAL_METRICS_TG_FLOOD_SLEEP_SECONDS", 60)),
        **device_kwargs,
    )
    async with client:
        by_channel: dict[str, list[SocialPostTarget]] = {}
        for target in targets:
            by_channel.setdefault(target.publisher_id, []).append(target)
        batch_size = max(1, min(100, _env_int("SOCIAL_METRICS_TG_BATCH_SIZE", 50)))
        made_request = False
        for channel_index, (username, channel_targets) in enumerate(sorted(by_channel.items())):
            if channel_index:
                await _human_pause("SOCIAL_METRICS_TG_BETWEEN_CHANNELS_SECONDS", (5.0, 15.0))
            entity = await client.get_input_entity(username)
            for start in range(0, len(channel_targets), batch_size):
                if made_request:
                    await _human_pause("SOCIAL_METRICS_TG_BETWEEN_REQUESTS_SECONDS", (2.0, 5.0))
                chunk = channel_targets[start : start + batch_size]
                messages = await client.get_messages(entity, ids=[item.post_id for item in chunk])
                made_request = True
                for message in messages or []:
                    if message is None or not getattr(message, "id", None):
                        continue
                    reactions_container = getattr(message, "reactions", None)
                    reactions: dict[str, int] = {}
                    raw_reactions = getattr(reactions_container, "results", None) or []
                    for result in raw_reactions:
                        count = getattr(result, "count", None)
                        reaction = getattr(result, "reaction", None)
                        label = getattr(reaction, "emoticon", None) or str(reaction or "reaction")
                        if isinstance(count, int) and count >= 0:
                            reactions[str(label)] = reactions.get(str(label), 0) + count
                    message_date = getattr(message, "date", None)
                    post_ts = int(message_date.timestamp()) if isinstance(message_date, datetime) else None
                    replies = getattr(getattr(message, "replies", None), "replies", None)
                    out[(username, int(message.id))] = MetricPayload(
                        post_id=int(message.id),
                        post_ts=post_ts,
                        views=int(message.views) if isinstance(getattr(message, "views", None), int) else None,
                        likes=sum(reactions.values()) if reactions_container is not None else None,
                        comments=int(replies) if isinstance(replies, int) else None,
                        shares=int(message.forwards) if isinstance(getattr(message, "forwards", None), int) else None,
                        reactions=reactions if reactions_container is not None else None,
                    )
    return out


async def _write_snapshot_chunk(db: Database, writes: Sequence[SnapshotWrite]) -> None:
    if not writes:
        return
    async with db.raw_conn() as conn:
        await conn.execute("BEGIN IMMEDIATE")
        try:
            for write in writes:
                payload = write.payload
                await conn.execute(
                    """
                    INSERT INTO social_metric_snapshot(
                        platform, publisher_id, post_id, age_bucket, publication_kind, source_url, post_ts,
                        collected_ts, views, likes, comments, shares, reactions_json,
                        status, error_code
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(platform, publisher_id, post_id, age_bucket) DO UPDATE SET
                        publication_kind=excluded.publication_kind,
                        source_url=COALESCE(excluded.source_url, social_metric_snapshot.source_url),
                        post_ts=COALESCE(excluded.post_ts, social_metric_snapshot.post_ts),
                        collected_ts=excluded.collected_ts,
                        views=CASE WHEN excluded.views IS NULL THEN social_metric_snapshot.views
                                   ELSE MAX(COALESCE(social_metric_snapshot.views, -1), excluded.views) END,
                        likes=CASE WHEN excluded.likes IS NULL THEN social_metric_snapshot.likes
                                   ELSE MAX(COALESCE(social_metric_snapshot.likes, -1), excluded.likes) END,
                        comments=CASE WHEN excluded.comments IS NULL THEN social_metric_snapshot.comments
                                      ELSE MAX(COALESCE(social_metric_snapshot.comments, -1), excluded.comments) END,
                        shares=CASE WHEN excluded.shares IS NULL THEN social_metric_snapshot.shares
                                    ELSE MAX(COALESCE(social_metric_snapshot.shares, -1), excluded.shares) END,
                        reactions_json=COALESCE(excluded.reactions_json, social_metric_snapshot.reactions_json),
                        status=excluded.status,
                        error_code=excluded.error_code
                    """,
                    (
                        write.target.platform,
                        write.target.publisher_id,
                        int(write.target.post_id),
                        write.age_bucket,
                        write.target.publication_kind,
                        write.target.source_url or None,
                        int(payload.post_ts) if payload and isinstance(payload.post_ts, int) else write.target.post_ts,
                        int(write.collected_ts),
                        payload.views if payload else None,
                        payload.likes if payload else None,
                        payload.comments if payload else None,
                        payload.shares if payload else None,
                        json.dumps(payload.reactions, ensure_ascii=False, sort_keys=True)
                        if payload and payload.reactions is not None
                        else None,
                        write.status,
                        write.error_code,
                    ),
                )
                if write.status != "collected" or payload is None:
                    continue
                age_day = _legacy_age_day(
                    post_ts=payload.post_ts,
                    collected_ts=write.collected_ts,
                )
                if age_day is None:
                    continue
                if write.target.platform == "vk":
                    await conn.execute(
                        """
                        INSERT INTO vk_post_metric(
                            group_id, post_id, age_day, source_url, post_ts, collected_ts,
                            views, likes, comments, reposts
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(group_id, post_id, age_day) DO UPDATE SET
                            source_url=COALESCE(excluded.source_url, vk_post_metric.source_url),
                            post_ts=COALESCE(excluded.post_ts, vk_post_metric.post_ts),
                            collected_ts=excluded.collected_ts,
                            views=CASE WHEN excluded.views IS NULL THEN vk_post_metric.views ELSE MAX(COALESCE(vk_post_metric.views,-1),excluded.views) END,
                            likes=CASE WHEN excluded.likes IS NULL THEN vk_post_metric.likes ELSE MAX(COALESCE(vk_post_metric.likes,-1),excluded.likes) END,
                            comments=CASE WHEN excluded.comments IS NULL THEN vk_post_metric.comments ELSE MAX(COALESCE(vk_post_metric.comments,-1),excluded.comments) END,
                            reposts=CASE WHEN excluded.reposts IS NULL THEN vk_post_metric.reposts ELSE MAX(COALESCE(vk_post_metric.reposts,-1),excluded.reposts) END
                        """,
                        (
                            int(write.target.publisher_id),
                            int(write.target.post_id),
                            int(age_day),
                            write.target.source_url or None,
                            payload.post_ts,
                            int(write.collected_ts),
                            payload.views,
                            payload.likes,
                            payload.comments,
                            payload.shares,
                        ),
                    )
                elif write.target.platform == "telegram":
                    source_id = write.target.legacy_source_id
                    if source_id is None:
                        await conn.execute(
                            "INSERT OR IGNORE INTO telegram_source(username, title, enabled) VALUES(?,?,1)",
                            (write.target.publisher_id, write.target.publisher_id),
                        )
                        cur = await conn.execute(
                            "SELECT id FROM telegram_source WHERE LOWER(username)=LOWER(?)",
                            (write.target.publisher_id,),
                        )
                        row = await cur.fetchone()
                        source_id = int(row[0]) if row else None
                    if source_id is not None:
                        await conn.execute(
                            """
                            INSERT INTO telegram_post_metric(
                                source_id, message_id, age_day, source_url, message_ts,
                                collected_ts, views, likes, comments, forwards, reactions_json
                            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                            ON CONFLICT(source_id, message_id, age_day) DO UPDATE SET
                                source_url=COALESCE(excluded.source_url, telegram_post_metric.source_url),
                                message_ts=COALESCE(excluded.message_ts, telegram_post_metric.message_ts),
                                collected_ts=excluded.collected_ts,
                                views=CASE WHEN excluded.views IS NULL THEN telegram_post_metric.views ELSE MAX(COALESCE(telegram_post_metric.views,-1),excluded.views) END,
                                likes=CASE WHEN excluded.likes IS NULL THEN telegram_post_metric.likes ELSE MAX(COALESCE(telegram_post_metric.likes,-1),excluded.likes) END,
                                comments=CASE WHEN excluded.comments IS NULL THEN telegram_post_metric.comments ELSE MAX(COALESCE(telegram_post_metric.comments,-1),excluded.comments) END,
                                forwards=CASE WHEN excluded.forwards IS NULL THEN telegram_post_metric.forwards ELSE MAX(COALESCE(telegram_post_metric.forwards,-1),excluded.forwards) END,
                                reactions_json=COALESCE(excluded.reactions_json, telegram_post_metric.reactions_json)
                            """,
                            (
                                source_id,
                                int(write.target.post_id),
                                int(age_day),
                                write.target.source_url or None,
                                payload.post_ts,
                                int(write.collected_ts),
                                payload.views,
                                payload.likes,
                                payload.comments,
                                payload.shares,
                                json.dumps(payload.reactions, ensure_ascii=False, sort_keys=True)
                                if payload.reactions is not None
                                else None,
                            ),
                        )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


def _writes_for_payload(
    target: SocialPostTarget,
    payload: MetricPayload,
    *,
    now_ts: int,
    terminal: set[str],
) -> list[SnapshotWrite]:
    bucket, skipped = _due_plan(
        post_ts=payload.post_ts or target.post_ts,
        now_ts=now_ts,
        terminal_buckets=terminal,
    )
    writes = [
        SnapshotWrite(target=target, age_bucket=name, collected_ts=now_ts, status="skipped_late")
        for name in skipped
    ]
    if bucket:
        writes.append(
            SnapshotWrite(
                target=target,
                age_bucket=bucket,
                collected_ts=now_ts,
                payload=payload,
                status="collected",
            )
        )
    return writes


async def run_social_metrics_batch(
    db: Database,
    *,
    now_utc: datetime | None = None,
    vk_fetcher: VkFetcher | None = None,
    tg_fetcher: TgFetcher | None = None,
    resolve_vk: bool = True,
) -> dict[str, Any]:
    """Collect all currently due social metrics in platform/publisher batches."""
    if not _env_enabled("ENABLE_SOCIAL_METRICS_BATCH", default=False):
        return BatchDiagnostics(enabled=False).as_dict()
    now = now_utc or datetime.now(timezone.utc)
    now_ts = int(now.timestamp())
    diag = BatchDiagnostics()
    vk_enabled = _env_enabled("SOCIAL_METRICS_VK_ENABLED", default=True)
    tg_enabled = _env_enabled("SOCIAL_METRICS_TG_ENABLED", default=False)
    if vk_enabled and resolve_vk:
        try:
            diag.resolver = await resolve_owned_vk_publications_batch(
                db,
                now_utc=now,
            )
        except Exception as exc:
            logging.warning("social metrics VK resolver failed: %s", exc, exc_info=True)
            diag.resolver = {"error": type(exc).__name__}

    targets = await load_social_post_targets(db, now_utc=now)
    diag.targets = len(targets)
    terminal_map = await _load_terminal_buckets(db)
    planned: dict[tuple[str, str, int], str | None] = {}
    for target in targets:
        terminal = terminal_map.get(target.key, set())
        bucket, _skipped = _due_plan(post_ts=target.post_ts, now_ts=now_ts, terminal_buckets=terminal)
        if bucket or target.post_ts is None:
            planned[target.key] = bucket
    diag.due = len(planned)

    if vk_enabled:
        fetcher = vk_fetcher or _default_vk_fetcher
        groups: dict[int, list[SocialPostTarget]] = {}
        for target in targets:
            if target.platform == "vk" and target.key in planned:
                groups.setdefault(int(target.publisher_id), []).append(target)
        chunk_size = max(1, min(100, _env_int("SOCIAL_METRICS_VK_BATCH_SIZE", 100)))
        pause = max(0, _env_int("SOCIAL_METRICS_VK_BATCH_PAUSE_MS", 350)) / 1000.0
        made_vk_request = False
        for group_id, group_targets in sorted(groups.items()):
            for start in range(0, len(group_targets), chunk_size):
                chunk = group_targets[start : start + chunk_size]
                diag.chunks += 1
                if made_vk_request and pause:
                    await asyncio.sleep(pause)
                made_vk_request = True
                try:
                    payloads = await fetcher(group_id, [target.post_id for target in chunk])
                except Exception as exc:
                    code = type(exc).__name__
                    writes = []
                    for target in chunk:
                        bucket = planned.get(target.key)
                        if bucket:
                            writes.append(
                                SnapshotWrite(
                                    target=target,
                                    age_bucket=bucket,
                                    collected_ts=now_ts,
                                    status="error",
                                    error_code=code,
                                )
                            )
                    await _write_snapshot_chunk(db, writes)
                    diag.errors += len(writes)
                else:
                    writes: list[SnapshotWrite] = []
                    for target in chunk:
                        payload = payloads.get(target.post_id)
                        terminal = terminal_map.get(target.key, set())
                        if payload is None:
                            bucket = planned.get(target.key)
                            if bucket:
                                writes.append(
                                    SnapshotWrite(
                                        target=target,
                                        age_bucket=bucket,
                                        collected_ts=now_ts,
                                        status="not_found",
                                        error_code="post_not_found",
                                    )
                                )
                                diag.not_found += 1
                            continue
                        item_writes = _writes_for_payload(
                            target,
                            payload,
                            now_ts=now_ts,
                            terminal=terminal,
                        )
                        writes.extend(item_writes)
                        diag.skipped_late += sum(1 for item in item_writes if item.status == "skipped_late")
                        diag.collected += sum(1 for item in item_writes if item.status == "collected")
                    await _write_snapshot_chunk(db, writes)

    tg_targets = [
        target
        for target in targets
        if target.platform == "telegram" and target.key in planned
    ]
    if not tg_enabled:
        diag.telegram = {"enabled": False, "targets_ready": len(tg_targets)}
    elif not _popular_tg_auth_raw():
        # Never fall back to TELEGRAM_AUTH_BUNDLE_E2E/S22/TELEGRAM_SESSION.
        diag.telegram = {"enabled": False, "reason": "missing_dedicated_bundle", "targets_ready": len(tg_targets)}
    elif tg_targets:
        fetcher_tg = tg_fetcher or _default_tg_fetcher
        try:
            payloads = await fetcher_tg(tg_targets)
        except Exception as exc:
            code = type(exc).__name__
            writes = [
                SnapshotWrite(
                    target=target,
                    age_bucket=planned[target.key],
                    collected_ts=now_ts,
                    status="error",
                    error_code=code,
                )
                for target in tg_targets
                if planned.get(target.key)
            ]
            await _write_snapshot_chunk(db, writes)
            diag.errors += len(writes)
            diag.telegram = {"enabled": True, "error": code, "targets": len(tg_targets)}
        else:
            writes: list[SnapshotWrite] = []
            for target in tg_targets:
                payload = payloads.get((target.publisher_id, target.post_id))
                if payload is None:
                    bucket = planned.get(target.key)
                    if bucket:
                        writes.append(
                            SnapshotWrite(
                                target=target,
                                age_bucket=bucket,
                                collected_ts=now_ts,
                                status="not_found",
                                error_code="post_not_found",
                            )
                        )
                        diag.not_found += 1
                    continue
                item_writes = _writes_for_payload(
                    target,
                    payload,
                    now_ts=now_ts,
                    terminal=terminal_map.get(target.key, set()),
                )
                writes.extend(item_writes)
                diag.skipped_late += sum(1 for item in item_writes if item.status == "skipped_late")
                diag.collected += sum(1 for item in item_writes if item.status == "collected")
            await _write_snapshot_chunk(db, writes)
            diag.telegram = {"enabled": True, "targets": len(tg_targets), "returned": len(payloads)}
    else:
        diag.telegram = {"enabled": True, "targets": 0}

    result = diag.as_dict()
    logging.info("social_metrics_batch %s", json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result
