from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from admin_chat import resolve_superadmin_chat_id
from db import Database

KGD80_FESTIVAL = "80 историй о главном"
REPORT_TZ = ZoneInfo("Europe/Kaliningrad")
FIRST_SENT_SETTING = "kgd80_social_report:first_sent_at"
LAST_SENT_SETTING = "kgd80_social_report:last_sent_date"

# Raffle conversion model. The final raffle currency is the same as
# stamps/visits: one verified visit/stamp is worth VISIT_POINTS. Social activity
# is subordinate: one very strong day (>=1 repost and >=2 comments) is about
# half a visit, so two such days roughly equal one visit.
VISIT_POINTS = 10.0
SOCIAL_DAILY_CAP_POINTS = VISIT_POINTS / 2.0
SOCIAL_BONUS_WEIGHTS: dict[str, float] = {
    "repost": 3.0,
    "comment": 1.0,
    "like": 0.2,
    "view": 0.002,
}
VIEW_POINTS_CAP_PER_POST = 2.0

_WALL_RE = re.compile(r"(?:^|/)wall(?P<owner>-?\d+)_(?P<post>\d+)")


@dataclass(frozen=True)
class Kgd80SocialSummary:
    posts: int
    views: int
    likes: int
    comments: int
    reposts: int
    view_points: float
    like_points: float
    comment_points: float
    repost_points: float
    total_points: float
    first_report: bool


@dataclass(frozen=True)
class ParticipantRafflePoints:
    full_name: str
    stamps: int
    visits: int
    social_days: int
    likes: int
    comments: int
    reposts: int
    attendance_points: float
    raw_social_points: float
    social_points: float
    winner_damping: float
    final_draw_points: float


def parse_vk_wall_ids(url: str | None) -> tuple[int, int] | None:
    value = str(url or "").strip()
    if not value:
        return None
    match = _WALL_RE.search(value)
    if not match:
        return None
    owner_id = int(match.group("owner"))
    post_id = int(match.group("post"))
    # vk_post_metric stores positive community ids in the crawler path.
    return abs(owner_id), post_id


def calculate_social_bonus(
    *,
    views: int = 0,
    likes: int = 0,
    comments: int = 0,
    reposts: int = 0,
    posts: int = 1,
) -> dict[str, float]:
    post_count = max(1, int(posts or 1))
    view_points = min(
        float(max(0, int(views or 0))) * SOCIAL_BONUS_WEIGHTS["view"],
        VIEW_POINTS_CAP_PER_POST * post_count,
    )
    like_points = float(max(0, int(likes or 0))) * SOCIAL_BONUS_WEIGHTS["like"]
    comment_points = float(max(0, int(comments or 0))) * SOCIAL_BONUS_WEIGHTS["comment"]
    repost_points = float(max(0, int(reposts or 0))) * SOCIAL_BONUS_WEIGHTS["repost"]
    total = view_points + like_points + comment_points + repost_points
    return {
        "view": view_points,
        "like": like_points,
        "comment": comment_points,
        "repost": repost_points,
        "total": total,
    }


def calculate_participant_raffle_points(
    *,
    full_name: str,
    stamps: int = 0,
    visits: int = 0,
    social_days: int = 1,
    likes: int = 0,
    comments: int = 0,
    reposts: int = 0,
    won_other_draw: bool = False,
) -> ParticipantRafflePoints:
    stamps = max(0, int(stamps or 0))
    visits = max(0, int(visits or 0))
    social_days = max(1, int(social_days or 1))
    likes = max(0, int(likes or 0))
    comments = max(0, int(comments or 0))
    reposts = max(0, int(reposts or 0))
    attendance_points = float(stamps + visits) * VISIT_POINTS
    raw_social_points = (
        float(reposts) * SOCIAL_BONUS_WEIGHTS["repost"]
        + float(comments) * SOCIAL_BONUS_WEIGHTS["comment"]
        + float(likes) * SOCIAL_BONUS_WEIGHTS["like"]
    )
    social_points = min(raw_social_points, SOCIAL_DAILY_CAP_POINTS * social_days)
    winner_damping = 0.5 if won_other_draw else 1.0
    final_draw_points = attendance_points * winner_damping + social_points
    return ParticipantRafflePoints(
        full_name=full_name,
        stamps=stamps,
        visits=visits,
        social_days=social_days,
        likes=likes,
        comments=comments,
        reposts=reposts,
        attendance_points=attendance_points,
        raw_social_points=raw_social_points,
        social_points=social_points,
        winner_damping=winner_damping,
        final_draw_points=final_draw_points,
    )


def _metric_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


async def _get_setting(db: Database, key: str) -> str | None:
    async with db.raw_conn() as conn:
        cur = await conn.execute("SELECT value FROM setting WHERE key=?", (key,))
        row = await cur.fetchone()
    return str(row[0]) if row and row[0] is not None else None


async def _set_setting(db: Database, key: str, value: str) -> None:
    async with db.raw_conn() as conn:
        await conn.execute(
            """
            INSERT INTO setting(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )
        await conn.commit()


async def collect_kgd80_social_summary(
    db: Database,
    *,
    now: datetime | None = None,
    lookback_hours: int = 24,
) -> Kgd80SocialSummary:
    now_local = (now or datetime.now(REPORT_TZ)).astimezone(REPORT_TZ)
    since = now_local - timedelta(hours=max(1, int(lookback_hours or 24)))
    first_report = not bool(await _get_setting(db, FIRST_SENT_SETTING))

    post_ids: set[tuple[int, int]] = set()
    async with db.raw_conn() as conn:
        cur = await conn.execute(
            """
            SELECT pe.details_json
            FROM promo_exposure pe
            JOIN promo_campaign pc ON pc.id = pe.campaign_id
            LEFT JOIN promo_target pt ON pt.campaign_id = pc.id
            WHERE (
                    pc.title LIKE ?
                 OR pt.festival_name = ?
                 OR pt.query_text = ?
            )
              AND pe.surface IN ('vk_publication', 'vk_repost', 'vk_story_forward')
              AND COALESCE(pe.published_at, pe.created_at) >= ?
            """,
            (
                f"%{KGD80_FESTIVAL}%",
                KGD80_FESTIVAL,
                KGD80_FESTIVAL,
                since.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        rows = await cur.fetchall()

    for (raw_details,) in rows:
        if not raw_details:
            continue
        try:
            details = json.loads(raw_details) if isinstance(raw_details, str) else raw_details
        except Exception:
            continue
        if not isinstance(details, dict):
            continue
        for key in ("target_url", "source_url", "url"):
            ids = parse_vk_wall_ids(details.get(key))
            if ids:
                post_ids.add(ids)

    if not post_ids:
        points = calculate_social_bonus(posts=0)
        return Kgd80SocialSummary(
            posts=0,
            views=0,
            likes=0,
            comments=0,
            reposts=0,
            view_points=points["view"],
            like_points=points["like"],
            comment_points=points["comment"],
            repost_points=points["repost"],
            total_points=points["total"],
            first_report=first_report,
        )

    views = likes = comments = reposts = 0
    async with db.raw_conn() as conn:
        for group_id, post_id in sorted(post_ids):
            cur = await conn.execute(
                """
                SELECT
                    MAX(COALESCE(views, 0)),
                    MAX(COALESCE(likes, 0)),
                    MAX(COALESCE(comments, 0)),
                    MAX(COALESCE(reposts, 0))
                FROM vk_post_metric
                WHERE group_id=? AND post_id=?
                """,
                (int(group_id), int(post_id)),
            )
            row = await cur.fetchone()
            if not row:
                continue
            views += _metric_int(row[0])
            likes += _metric_int(row[1])
            comments += _metric_int(row[2])
            reposts += _metric_int(row[3])

    points = calculate_social_bonus(
        views=views,
        likes=likes,
        comments=comments,
        reposts=reposts,
        posts=len(post_ids),
    )
    return Kgd80SocialSummary(
        posts=len(post_ids),
        views=views,
        likes=likes,
        comments=comments,
        reposts=reposts,
        view_points=points["view"],
        like_points=points["like"],
        comment_points=points["comment"],
        repost_points=points["repost"],
        total_points=points["total"],
        first_report=first_report,
    )


def _fmt_points(value: float) -> str:
    if math.isclose(value, round(value), abs_tol=0.001):
        return str(int(round(value)))
    return f"{value:.1f}"


def format_kgd80_social_report(summary: Kgd80SocialSummary) -> str:
    prefix = "Первичный" if summary.first_report else "Ежедневный"
    lines = [
        f"📊 {prefix} отчёт KGD80 / «{KGD80_FESTIVAL}»",
        "Сообщение создано автоматически.",
        "",
        "Зафиксированная VK-активность за последние 24 часа:",
        f"• постов с метриками: {summary.posts}",
        f"• просмотры: {summary.views} → +{_fmt_points(summary.view_points)} баллов",
        f"• лайки: {summary.likes} → +{_fmt_points(summary.like_points)} баллов",
        f"• комментарии: {summary.comments} → +{_fmt_points(summary.comment_points)} баллов",
        f"• репосты: {summary.reposts} → +{_fmt_points(summary.repost_points)} баллов",
        f"Итого расчётного социального бонуса: +{_fmt_points(summary.total_points)} баллов.",
        "",
        "Модель конвертации: 1 посещение/штамп = 10 баллов; соцактивность за день ограничена 5 баллами.",
        "Соцвеса: репост ×3, комментарий ×1, лайк ×0.2, просмотр ×0.002 только для агрегатного отчёта.",
        "Примечание: бонус социальной активности считается без понижения за победы в других розыгрышах; понижение применяется только к базовой вероятности.",
    ]
    return "\n".join(lines)


async def send_kgd80_social_report(
    db: Database,
    bot,
    *,
    chat_id: int | None = None,
    now: datetime | None = None,
) -> Kgd80SocialSummary | None:
    target_chat_id = int(chat_id) if chat_id else await resolve_superadmin_chat_id(db)
    if not target_chat_id:
        return None
    summary = await collect_kgd80_social_summary(db, now=now)
    await bot.send_message(target_chat_id, format_kgd80_social_report(summary))
    now_local = (now or datetime.now(REPORT_TZ)).astimezone(REPORT_TZ)
    if summary.first_report:
        await _set_setting(db, FIRST_SENT_SETTING, now_local.isoformat())
    await _set_setting(db, LAST_SENT_SETTING, now_local.date().isoformat())
    return summary


async def kgd80_social_report_scheduler(db: Database, bot) -> None:
    await send_kgd80_social_report(db, bot)
