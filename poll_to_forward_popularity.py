from __future__ import annotations

import asyncio
import json
import os
import re
import time
import unicodedata
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

from db import Database


MONTHS_RU_GEN = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря",
}

METRIC_KEYS = ("views", "likes", "comments", "reposts")
KLD_TARGET = "klgdevents"


@dataclass(slots=True, frozen=True)
class PopularitySignal:
    source: str
    url: str
    group_id: int
    post_id: int
    weight: float
    score: float
    above: tuple[str, ...]
    metrics: dict[str, int | None] = field(default_factory=dict)
    medians: dict[str, float | None] = field(default_factory=dict)
    method: str = ""
    confidence: float = 0.0

    def to_trace(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "url": self.url,
            "group_id": self.group_id,
            "post_id": self.post_id,
            "weight": self.weight,
            "score": round(float(self.score), 4),
            "above": list(self.above),
            "metrics": self.metrics,
            "medians": self.medians,
            "method": self.method,
            "confidence": self.confidence,
        }


@dataclass(slots=True, frozen=True)
class EventPopularity:
    event_id: int
    score: float
    group_key: str
    best_signal: PopularitySignal
    signals: tuple[PopularitySignal, ...]

    def to_trace(self) -> dict[str, Any]:
        return {
            "score": round(float(self.score), 4),
            "group_key": self.group_key,
            "best_signal": self.best_signal.to_trace(),
            "signals": [signal.to_trace() for signal in self.signals],
        }


@dataclass(slots=True, frozen=True)
class PopularityResult:
    by_event_id: dict[int, EventPopularity]
    diagnostics: dict[str, Any]


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _vk_group_id() -> int:
    raw = (os.getenv("VK_EVENTS_GROUP_ID") or "231920894").strip().lstrip("-")
    try:
        return abs(int(raw))
    except Exception:
        return 231920894


def _kldevents_baseline_min_sample() -> int:
    return max(10, _env_int("POLL_TO_FORWARD_KLDEVENTS_BASELINE_MIN_SAMPLE", 30))


def _vk_token() -> str:
    for name in ("VK_USER_TOKEN", "VK_ACCESS_TOKEN4", "VK_SERVICE_TOKEN", "VK_TOKEN"):
        token = (os.getenv(name) or "").strip()
        if token:
            return token
    return ""


def _normalize_text(value: str | None) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    text = text.replace("ё", "е")
    text = re.sub(r"[^a-zа-я0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalized_tokens(value: str | None) -> tuple[str, ...]:
    return tuple(_normalize_text(value).split())


def _contains_token_sequence(tokens: Sequence[str], needle: Sequence[str]) -> bool:
    if not needle or len(needle) > len(tokens):
        return False
    size = len(needle)
    return any(tuple(tokens[index : index + size]) == tuple(needle) for index in range(len(tokens) - size + 1))


def _title_key(title: str | None) -> str:
    tokens = [
        token
        for token in _normalize_text(title).split()
        if len(token) >= 3 or token.isdigit()
    ]
    return " ".join(tokens[:6])


def _location_tokens(location: str | None) -> tuple[str, ...]:
    tokens = [token for token in _normalize_text(location).split() if len(token) >= 4]
    return tuple(tokens[:3])


def _date_label(value: date) -> str:
    return f"{value.day} {MONTHS_RU_GEN.get(value.month, '')}".strip()


def _parse_wall_url(url: str | None, *, group_id: int | None = None) -> tuple[int, int] | None:
    match = re.search(r"wall(-?\d+)_(\d+)", str(url or ""))
    if not match:
        return None
    try:
        owner_id = int(match.group(1))
        post_id = int(match.group(2))
    except Exception:
        return None
    if group_id is not None and abs(owner_id) != abs(int(group_id)):
        return None
    return owner_id, post_id


def _metric_count(item: Mapping[str, Any], key: str) -> int | None:
    raw = item.get(key)
    if isinstance(raw, Mapping):
        value = raw.get("count")
    else:
        value = raw
    if isinstance(value, int) and value >= 0:
        return int(value)
    return None


def _post_metrics(item: Mapping[str, Any]) -> dict[str, int | None]:
    return {key: _metric_count(item, key) for key in METRIC_KEYS}


def _published_ts(item: Mapping[str, Any]) -> int | None:
    value = item.get("date")
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    return None


def _vk_call(method: str, params: Mapping[str, Any]) -> dict[str, Any]:
    token = _vk_token()
    if not token:
        return {"error": {"error_msg": "missing_vk_token"}}
    payload = dict(params)
    payload["access_token"] = token
    payload["v"] = "5.199"
    url = f"https://api.vk.com/method/{method}?{urllib.parse.urlencode(payload)}"
    with urllib.request.urlopen(url, timeout=25) as response:
        data = json.loads(response.read().decode("utf-8"))
    return data if isinstance(data, dict) else {}


async def _vk_call_async(method: str, params: Mapping[str, Any]) -> dict[str, Any]:
    return await asyncio.to_thread(_vk_call, method, params)


async def _vk_get_by_ids(group_id: int, post_ids: Sequence[int]) -> dict[int, Mapping[str, Any]]:
    ids = [int(post_id) for post_id in post_ids if int(post_id or 0) > 0]
    out: dict[int, Mapping[str, Any]] = {}
    for start in range(0, len(ids), 100):
        chunk = ids[start : start + 100]
        if not chunk:
            continue
        data = await _vk_call_async(
            "wall.getById",
            {"posts": ",".join(f"-{int(group_id)}_{post_id}" for post_id in chunk)},
        )
        response = data.get("response") if isinstance(data, dict) else None
        items = response.get("items") if isinstance(response, Mapping) else response
        for item in items or []:
            if not isinstance(item, Mapping):
                continue
            try:
                post_id = int(item.get("id") or 0)
            except Exception:
                post_id = 0
            if post_id > 0:
                out[post_id] = item
    return out


async def _vk_scan_wall(group_id: int, *, limit: int) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    count = 100
    for offset in range(0, max(0, int(limit)), count):
        data = await _vk_call_async(
            "wall.get",
            {"owner_id": f"-{int(group_id)}", "filter": "owner", "count": count, "offset": offset},
        )
        response = data.get("response") if isinstance(data, dict) else None
        items = response.get("items") if isinstance(response, Mapping) else []
        chunk = [item for item in (items or []) if isinstance(item, Mapping)]
        if not chunk:
            break
        out.extend(chunk)
        if len(chunk) < count:
            break
    return out[: max(0, int(limit))]


def _match_post_score(event: Any, item: Mapping[str, Any], target_date: date) -> tuple[int, float]:
    text = str(item.get("text") or "")
    normalized = _normalize_text(text)
    key = _title_key(getattr(event, "title", None))
    key_tokens = tuple(key.split())
    if not key or not _contains_token_sequence(_normalized_tokens(text), key_tokens):
        return 0, 0.0
    if _normalize_text(_date_label(target_date)) not in normalized:
        return 0, 0.0

    score = 2
    time_value = str(getattr(event, "time", "") or "").strip()
    time_anchor = ""
    match = re.search(r"\d{1,2}:\d{2}", time_value)
    if match:
        time_anchor = match.group(0)
        if time_anchor in text:
            score += 1
    location_tokens = _location_tokens(getattr(event, "location_name", None))
    location_hit = any(token in normalized for token in location_tokens)
    if location_hit:
        score += 1

    # A title+date-only match is too weak for this feature when more anchors exist.
    if (time_anchor or location_tokens) and score < 3:
        return 0, 0.0
    confidence = min(1.0, 0.45 + 0.18 * score)
    return score, confidence


async def _upsert_event_publication(
    db: Database,
    *,
    event_id: int,
    stored_url: str | None,
    live_url: str | None,
    stored_post_id: int | None,
    live_post_id: int | None,
    match_method: str,
    match_confidence: float,
    status: str,
) -> None:
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO event_publication(
                event_id, platform, target, stored_url, live_url, stored_post_id,
                live_post_id, match_method, match_confidence, status, resolved_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
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
            (
                int(event_id),
                "vk",
                KLD_TARGET,
                stored_url,
                live_url,
                int(stored_post_id) if stored_post_id else None,
                int(live_post_id) if live_post_id else None,
                match_method,
                float(match_confidence),
                status,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        await conn.commit()


async def _upsert_vk_metric(
    db: Database,
    *,
    group_id: int,
    post_id: int,
    source_url: str,
    post_ts: int | None,
    metrics: Mapping[str, int | None],
    now_ts: int,
) -> None:
    from source_parsing.post_metrics import compute_age_day, normalize_age_day, upsert_vk_post_metric

    age_day = normalize_age_day(compute_age_day(published_ts=post_ts, collected_ts=now_ts))
    if not isinstance(age_day, int):
        return
    await upsert_vk_post_metric(
        db,
        group_id=int(group_id),
        post_id=int(post_id),
        age_day=int(age_day),
        source_url=source_url,
        post_ts=post_ts,
        views=metrics.get("views") if isinstance(metrics.get("views"), int) else None,
        likes=metrics.get("likes") if isinstance(metrics.get("likes"), int) else None,
        comments=metrics.get("comments") if isinstance(metrics.get("comments"), int) else None,
        reposts=metrics.get("reposts") if isinstance(metrics.get("reposts"), int) else None,
        collected_ts=int(now_ts),
    )


def _median(values: Sequence[int]) -> float | None:
    vals = sorted(int(value) for value in values if isinstance(value, int) and value >= 0)
    if not vals:
        return None
    mid = len(vals) // 2
    if len(vals) % 2:
        return float(vals[mid])
    return float(vals[mid - 1] + vals[mid]) / 2.0


async def _load_vk_baseline(
    db: Database,
    *,
    group_id: int,
    include_comments: bool = True,
    now_ts: int,
) -> tuple[dict[str, float | None], int]:
    horizon_days = max(1, _env_int("POST_POPULARITY_HORIZON_DAYS", 90))
    since_ts = int(now_ts) - horizon_days * 86400
    max_age = max(0, _env_int("POST_POPULARITY_MAX_AGE_DAY", 2))
    async with db.raw_conn() as conn:
        cur0 = await conn.execute("PRAGMA table_info(vk_post_metric)")
        columns = {str(row[1]) for row in await cur0.fetchall()}
        metric_cols = ["views", "likes"]
        if include_comments and "comments" in columns:
            metric_cols.append("comments")
        if include_comments and "reposts" in columns:
            metric_cols.append("reposts")
        select_parts = ", ".join(f"MAX({col}) AS {col}" for col in metric_cols)
        cur = await conn.execute(
            f"""
            SELECT post_id, {select_parts}
            FROM vk_post_metric
            WHERE group_id=?
              AND age_day <= ?
              AND post_ts IS NOT NULL
              AND post_ts >= ?
            GROUP BY post_id
            """,
            (int(group_id), int(max_age), int(since_ts)),
        )
        rows = await cur.fetchall()
    medians: dict[str, float | None] = {}
    for key in METRIC_KEYS:
        if key not in metric_cols:
            medians[key] = None
            continue
        idx = metric_cols.index(key) + 1
        medians[key] = _median([row[idx] for row in rows if row and len(row) > idx])
    return medians, len({int(row[0]) for row in rows if row and row[0] is not None})


def _score_signal(
    *,
    source: str,
    url: str,
    group_id: int,
    post_id: int,
    weight: float,
    metrics: Mapping[str, int | None],
    medians: Mapping[str, float | None],
    method: str,
    confidence: float,
) -> PopularitySignal | None:
    above: list[str] = []
    lifts: list[float] = []
    for key in METRIC_KEYS:
        value = metrics.get(key)
        median = medians.get(key)
        if not isinstance(value, int) or value < 0 or median is None:
            continue
        if median <= 0:
            if value <= 0:
                continue
            lift = 2.0
        elif float(value) <= float(median):
            continue
        else:
            lift = float(value) / float(median)
        above.append(key)
        lifts.append(float(lift))
    if not lifts:
        return None
    score = float(weight) * (max(lifts) + 0.01 * sum(lifts))
    return PopularitySignal(
        source=source,
        url=url,
        group_id=int(group_id),
        post_id=int(post_id),
        weight=float(weight),
        score=float(score),
        above=tuple(above),
        metrics={key: metrics.get(key) for key in METRIC_KEYS},
        medians={key: medians.get(key) for key in METRIC_KEYS},
        method=method,
        confidence=float(confidence),
    )


async def _source_popularity_signals(
    db: Database,
    events: Sequence[Any],
    *,
    now_ts: int,
) -> tuple[dict[int, list[PopularitySignal]], dict[str, Any]]:
    group_cache: dict[int, tuple[dict[str, float | None], int]] = {}
    out: dict[int, list[PopularitySignal]] = {}
    scanned = 0
    with_metrics = 0
    async with db.raw_conn() as conn:
        cur_cols = await conn.execute("PRAGMA table_info(vk_post_metric)")
        columns = {str(row[1]) for row in await cur_cols.fetchall()}
        metric_cols = ["views", "likes"]
        if "comments" in columns:
            metric_cols.append("comments")
        if "reposts" in columns:
            metric_cols.append("reposts")
        metric_select = ", ".join(metric_cols)
        for event in events:
            event_id = int(getattr(event, "id", 0) or 0)
            if event_id <= 0:
                continue
            cur = await conn.execute("SELECT source_url FROM event_source WHERE event_id=?", (event_id,))
            source_urls = [str(row[0] or "") for row in await cur.fetchall()]
            for source_url in source_urls:
                parsed = _parse_wall_url(source_url)
                if not parsed:
                    continue
                group_id = abs(int(parsed[0]))
                post_id = int(parsed[1])
                scanned += 1
                cur2 = await conn.execute(
                    f"""
                    SELECT {metric_select}
                    FROM vk_post_metric
                    WHERE group_id=? AND post_id=?
                    ORDER BY age_day DESC, collected_ts DESC
                    LIMIT 1
                    """,
                    (group_id, post_id),
                )
                row = await cur2.fetchone()
                if not row:
                    continue
                with_metrics += 1
                if group_id not in group_cache:
                    group_cache[group_id] = await _load_vk_baseline(
                        db,
                        group_id=group_id,
                        now_ts=now_ts,
                    )
                medians, sample = group_cache[group_id]
                if sample < max(2, _env_int("POST_POPULARITY_MIN_SAMPLE", 2)):
                    continue
                values = {metric_cols[index]: row[index] for index in range(len(metric_cols))}
                metrics = {
                    key: values.get(key) if isinstance(values.get(key), int) else None
                    for key in METRIC_KEYS
                }
                signal = _score_signal(
                    source="source_vk",
                    url=source_url,
                    group_id=group_id,
                    post_id=post_id,
                    weight=1.0,
                    metrics=metrics,
                    medians=medians,
                    method="event_source",
                    confidence=1.0,
                )
                if signal:
                    out.setdefault(event_id, []).append(signal)
    return out, {"source_urls_scanned": scanned, "source_metric_rows": with_metrics}


async def _kldevents_signals(
    db: Database,
    events: Sequence[Any],
    *,
    target_date: date,
    now_ts: int,
) -> tuple[dict[int, list[PopularitySignal]], dict[str, Any]]:
    group_id = _vk_group_id()
    stored_by_event: dict[int, tuple[str, int] | None] = {}
    stored_ids: list[int] = []
    for event in events:
        event_id = int(getattr(event, "id", 0) or 0)
        parsed = _parse_wall_url(getattr(event, "source_vk_post_url", None), group_id=group_id)
        if event_id > 0 and parsed:
            stored_by_event[event_id] = (str(getattr(event, "source_vk_post_url", "") or ""), int(parsed[1]))
            stored_ids.append(int(parsed[1]))
        elif event_id > 0:
            stored_by_event[event_id] = None

    direct_items = await _vk_get_by_ids(group_id, stored_ids) if stored_ids else {}
    wall_limit = max(100, _env_int("POLL_TO_FORWARD_KLDEVENTS_WALL_SCAN_LIMIT", 1000))
    wall_items = await _vk_scan_wall(group_id, limit=wall_limit)
    wall_index = [(item, _normalize_text(str(item.get("text") or ""))) for item in wall_items]
    medians, baseline_sample = await _load_vk_baseline(db, group_id=group_id, now_ts=now_ts)
    min_sample = _kldevents_baseline_min_sample()
    if baseline_sample < min_sample:
        vals: dict[str, list[int]] = {key: [] for key in METRIC_KEYS}
        for item in wall_items:
            for key, value in _post_metrics(item).items():
                if isinstance(value, int) and value >= 0:
                    vals[key].append(value)
        medians = {key: _median(values) for key, values in vals.items()}
        baseline_sample = min((len(values) for values in vals.values() if values), default=0)
        baseline_source = "wall_scan_bootstrap"
        baseline_confidence = "low"
    else:
        baseline_source = "vk_post_metric"
        baseline_confidence = "normal"

    out: dict[int, list[PopularitySignal]] = {}
    direct_found = 0
    wall_scan_found = 0
    changed_id = 0
    unmatched = 0
    collapsed_posts: dict[int, int] = {}

    for event in events:
        event_id = int(getattr(event, "id", 0) or 0)
        if event_id <= 0:
            continue
        stored = stored_by_event.get(event_id)
        stored_url = stored[0] if stored else None
        stored_post_id = stored[1] if stored else None
        direct_item = direct_items.get(stored_post_id or 0)
        direct_match: tuple[Mapping[str, Any], float] | None = None
        if direct_item:
            direct_score, direct_conf = _match_post_score(event, direct_item, target_date)
            if direct_score > 0:
                direct_match = (direct_item, direct_conf)
            direct_found += 1

        best: tuple[int, float, Mapping[str, Any]] | None = None
        for candidate, _normalized in wall_index:
            score, conf = _match_post_score(event, candidate, target_date)
            if score <= 0:
                continue
            if best is None or (score, _published_ts(candidate) or 0) > (
                best[0],
                _published_ts(best[2]) or 0,
            ):
                best = (score, conf, candidate)
        item: Mapping[str, Any] | None = None
        method = ""
        confidence = 0.0
        if best:
            item = best[2]
            method = "wall_scan"
            confidence = best[1]
            wall_scan_found += 1
        elif direct_match:
            item = direct_match[0]
            method = "direct"
            confidence = direct_match[1]
        if not item:
            unmatched += 1
            await _upsert_event_publication(
                db,
                event_id=event_id,
                stored_url=stored_url,
                live_url=None,
                stored_post_id=stored_post_id,
                live_post_id=None,
                match_method="unmatched",
                match_confidence=0.0,
                status="missing",
            )
            continue
        live_post_id = int(item.get("id") or 0)
        if stored_post_id and live_post_id and stored_post_id != live_post_id:
            changed_id += 1
        live_url = f"https://vk.com/wall-{group_id}_{live_post_id}"
        metrics = _post_metrics(item)
        post_ts = _published_ts(item)
        await _upsert_event_publication(
            db,
            event_id=event_id,
            stored_url=stored_url,
            live_url=live_url,
            stored_post_id=stored_post_id,
            live_post_id=live_post_id,
            match_method=method or "unknown",
            match_confidence=confidence,
            status="published",
        )
        await _upsert_vk_metric(
            db,
            group_id=group_id,
            post_id=live_post_id,
            source_url=live_url,
            post_ts=post_ts,
            metrics=metrics,
            now_ts=now_ts,
        )
        collapsed_posts[live_post_id] = collapsed_posts.get(live_post_id, 0) + 1
        signal = _score_signal(
            source="kldevents_vk",
            url=live_url,
            group_id=group_id,
            post_id=live_post_id,
            weight=max(1.0, _env_float("POLL_TO_FORWARD_KLDEVENTS_WEIGHT", 4.0)),
            metrics=metrics,
            medians=medians,
            method=method or "unknown",
            confidence=confidence,
        )
        if signal:
            out.setdefault(event_id, []).append(signal)

    return out, {
        "kldevents_group_id": group_id,
        "stored_urls": len(stored_ids),
        "direct_found": direct_found,
        "wall_items_scanned": len(wall_items),
        "wall_scan_found": wall_scan_found,
        "changed_id_count": changed_id,
        "unmatched": unmatched,
        "baseline_sample": baseline_sample,
        "baseline_min_sample": min_sample,
        "baseline_source": baseline_source,
        "baseline_confidence": baseline_confidence,
        "collapsed_live_posts": sum(1 for count in collapsed_posts.values() if count > 1),
    }


async def build_event_popularity(
    db: Database,
    events: Sequence[Any],
    *,
    target_date: date,
    now_utc: datetime | None = None,
) -> PopularityResult:
    now_value = now_utc or datetime.now(timezone.utc)
    now_ts = int(now_value.timestamp())
    source_signals, source_diag = await _source_popularity_signals(db, events, now_ts=now_ts)
    kld_signals: dict[int, list[PopularitySignal]] = {}
    kld_diag: dict[str, Any] = {"disabled": True}
    if (os.getenv("POLL_TO_FORWARD_KLDEVENTS_STATS_ENABLED") or "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        try:
            kld_signals, kld_diag = await _kldevents_signals(
                db,
                events,
                target_date=target_date,
                now_ts=now_ts,
            )
        except Exception as exc:
            kld_diag = {"error": str(exc) or type(exc).__name__}

    by_event_id: dict[int, EventPopularity] = {}
    for event in events:
        event_id = int(getattr(event, "id", 0) or 0)
        signals = tuple((source_signals.get(event_id) or []) + (kld_signals.get(event_id) or []))
        if not signals:
            continue
        best = max(signals, key=lambda signal: signal.score)
        by_event_id[event_id] = EventPopularity(
            event_id=event_id,
            score=float(best.score),
            group_key=f"{best.source}:{best.group_id}:{best.post_id}",
            best_signal=best,
            signals=signals,
        )

    return PopularityResult(
        by_event_id=by_event_id,
        diagnostics={
            **source_diag,
            **{f"kldevents_{key}": value for key, value in kld_diag.items()},
            "popular_events": len(by_event_id),
        },
    )


def popularity_trace_for_event(popularity: EventPopularity | None) -> dict[str, Any] | None:
    return popularity.to_trace() if popularity else None
