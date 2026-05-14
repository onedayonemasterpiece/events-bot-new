from __future__ import annotations

import html
import re
from datetime import date, datetime, timezone

from aiogram import Bot, types
from sqlalchemy import func, select, text

from db import Database
from models import Event, PromoActivity, PromoCampaign, PromoExposure, PromoTarget
from promo import (
    INITIAL_80_STORIES_FESTIVAL,
    create_event_promo_campaign,
    create_festival_promo_campaign,
    default_campaign_end,
    ensure_initial_80_stories_campaign,
)

_RU_MONTHS = {
    "января": 1,
    "январь": 1,
    "февраля": 2,
    "февраль": 2,
    "марта": 3,
    "март": 3,
    "апреля": 4,
    "апрель": 4,
    "мая": 5,
    "май": 5,
    "июня": 6,
    "июнь": 6,
    "июля": 7,
    "июль": 7,
    "августа": 8,
    "август": 8,
    "сентября": 9,
    "сентябрь": 9,
    "октября": 10,
    "октябрь": 10,
    "ноября": 11,
    "ноябрь": 11,
    "декабря": 12,
    "декабрь": 12,
}


def _parse_until_date(raw: str, *, today: date | None = None) -> tuple[str, date | None]:
    today = today or datetime.now(timezone.utc).date()
    text = " ".join(str(raw or "").split())
    if not text:
        return "", None

    patterns = [
        re.compile(r"\b(?:до|until)\s+(\d{4}-\d{2}-\d{2})\b", re.IGNORECASE),
        re.compile(r"\b(?:до|until)\s+(\d{1,2})\.(\d{1,2})(?:\.(\d{4}))?\b", re.IGNORECASE),
        re.compile(
            r"\bдо\s+(\d{1,2})\s+([а-яё]+)(?:\s+(\d{4}))?\b",
            re.IGNORECASE,
        ),
    ]
    for pattern in patterns:
        match = pattern.search(text)
        if not match:
            continue
        end: date | None = None
        year_was_explicit = True
        try:
            if pattern is patterns[0]:
                end = date.fromisoformat(match.group(1))
            elif pattern is patterns[1]:
                day = int(match.group(1))
                month = int(match.group(2))
                year_was_explicit = bool(match.group(3))
                year = int(match.group(3) or today.year)
                end = date(year, month, day)
            else:
                day = int(match.group(1))
                month = _RU_MONTHS.get(match.group(2).casefold().replace("ё", "е"))
                year_was_explicit = bool(match.group(3))
                year = int(match.group(3) or today.year)
                if month:
                    end = date(year, month, day)
        except ValueError:
            end = None
        if end is None:
            continue
        if end < today and not year_was_explicit:
            end = date(end.year + 1, end.month, end.day)
        cleaned = (text[: match.start()] + text[match.end() :]).strip()
        return " ".join(cleaned.split()), end
    return text, None


def _strip_wrapping_quotes(value: str) -> str:
    text = " ".join(str(value or "").split()).strip()
    return text.strip(" \"'«»“”")


def _natural_add_args(arg_text: str) -> tuple[str, str] | None:
    text = " ".join(str(arg_text or "").split()).strip()
    low = text.casefold()
    for prefix in ("add festival", "festival", "фестиваль"):
        if low.startswith(prefix):
            return "festival", text[len(prefix) :].strip()
    for prefix in ("add event", "event", "событие"):
        if low.startswith(prefix):
            return "event", text[len(prefix) :].strip()
    festival_match = re.search(
        r"(?:продвигай|продвинь|продвижение|добавь в промо)\s+(?:события\s+)?фестивал[ья]\s+(.+)$",
        text,
        flags=re.IGNORECASE,
    )
    if festival_match:
        return "festival", festival_match.group(1).strip()
    event_match = re.search(
        r"(?:продвигай|продвинь|продвижение|добавь в промо)\s+(?:событие\s+)?(.+)$",
        text,
        flags=re.IGNORECASE,
    )
    if event_match:
        return "event", event_match.group(1).strip()
    return None


def _status_label(status: str) -> str:
    return {
        "draft": "черновик",
        "active": "активна",
        "paused": "пауза",
        "archived": "архив",
    }.get(status, status)


def _fmt_dt(value: object) -> str:
    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value or "").strip()
        if not raw:
            return "дата неизвестна"
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return raw
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")


def _viewer_facing_video_status(status: str, profile_key: str | None) -> bool:
    if status == "PUBLISHED_MAIN":
        return True
    profile = str(profile_key or "")
    return status == "PUBLISHED_TEST" and (
        profile == "popular_review" or profile.startswith("popular_review_")
    )


async def _video_publication_groups_for_campaign(
    db: Database,
    campaign: PromoCampaign,
) -> list[dict[str, object]]:
    async with db.get_session() as session:
        rows = (
            await session.execute(
                text(
                    """
                    SELECT
                        vs.id AS session_id,
                        vs.profile_key AS profile_key,
                        vs.status AS status,
                        COALESCE(vs.published_at, vs.finished_at, vs.started_at, vs.created_at) AS published_at,
                        vs.test_chat_id AS test_chat_id,
                        vs.main_chat_id AS main_chat_id,
                        vi.event_id AS event_id,
                        e.title AS title,
                        e.date AS event_date,
                        vi.position AS position
                    FROM videoannounce_item vi
                    JOIN videoannounce_session vs ON vs.id = vi.session_id
                    JOIN event e ON e.id = vi.event_id
                    WHERE vi.promo_campaign_id = :campaign_id
                    ORDER BY published_at DESC, vs.id DESC, vi.position
                    """
                ),
                {"campaign_id": int(campaign.id)},
            )
        ).mappings().all()

    groups: dict[int, dict[str, object]] = {}
    for row in rows:
        status = str(row["status"] or "")
        profile_key = str(row["profile_key"] or "")
        if not _viewer_facing_video_status(status, profile_key):
            continue
        session_id = int(row["session_id"])
        group = groups.setdefault(
            session_id,
            {
                "session_id": session_id,
                "profile_key": profile_key,
                "status": status,
                "published_at": row["published_at"],
                "target_count": len(
                    {
                        int(value)
                        for value in (row["test_chat_id"], row["main_chat_id"])
                        if value is not None
                    }
                ),
                "items": [],
            },
        )
        group_items = group["items"]
        assert isinstance(group_items, list)
        group_items.append(
            {
                "event_id": int(row["event_id"]),
                "title": str(row["title"] or ""),
                "event_date": str(row["event_date"] or ""),
                "position": int(row["position"] or 0),
            }
        )
    return sorted(
        groups.values(),
        key=lambda item: str(item.get("published_at") or ""),
        reverse=True,
    )


async def _future_count_for_campaign(db: Database, campaign: PromoCampaign) -> int:
    async with db.get_session() as session:
        target_res = await session.execute(
            select(PromoTarget).where(PromoTarget.campaign_id == campaign.id)
        )
        targets = target_res.scalars().all()
        total = 0
        today_iso = datetime.now(timezone.utc).date().isoformat()
        for target in targets:
            if target.target_type == "event" and target.event_id:
                res = await session.execute(
                    select(func.count())
                    .select_from(Event)
                    .where(Event.id == target.event_id)
                    .where(Event.date >= today_iso)
                    .where(Event.lifecycle_status == "active")
                    .where(Event.silent.is_(False))
                )
                total += int(res.scalar() or 0)
            elif target.target_type == "festival" and target.festival_name:
                query = (
                    select(func.count())
                    .select_from(Event)
                    .where(Event.festival == target.festival_name)
                    .where(Event.date >= today_iso)
                    .where(Event.lifecycle_status == "active")
                    .where(Event.silent.is_(False))
                )
                if campaign.ends_at is not None:
                    query = query.where(Event.date <= campaign.ends_at.date().isoformat())
                res = await session.execute(query)
                total += int(res.scalar() or 0)
        return total


async def _campaign_lines(
    db: Database,
    *,
    include_archived: bool = False,
    include_details: bool = False,
) -> list[str]:
    async with db.get_session() as session:
        query = select(PromoCampaign).order_by(PromoCampaign.status, PromoCampaign.created_at)
        if not include_archived:
            query = query.where(PromoCampaign.status != "archived")
        res = await session.execute(query)
        campaigns = res.scalars().all()
    if not campaigns:
        return ["Промо-кампаний пока нет."]

    lines: list[str] = []
    for campaign in campaigns:
        future_count = await _future_count_for_campaign(db, campaign)
        async with db.get_session() as session:
            act_res = await session.execute(
                select(PromoActivity).where(PromoActivity.campaign_id == campaign.id)
            )
            activities = act_res.scalars().all()
            exposure_res = await session.execute(
                select(func.count())
                .select_from(PromoExposure)
                .where(PromoExposure.campaign_id == campaign.id)
                .where(PromoExposure.public_target_count > 0)
            )
            recorded_exposures = int(exposure_res.scalar() or 0)
        video_groups = await _video_publication_groups_for_campaign(db, campaign)
        video_show_count = sum(len(group.get("items") or []) for group in video_groups)
        activity_labels = ", ".join(
            filter(
                None,
                [
                    (
                        f"{activity.surface}"
                        + (f":{activity.profile_key}" if activity.profile_key else "")
                        + (f" x{activity.max_per_publish}" if activity.max_per_publish else "")
                    )
                    for activity in activities
                    if activity.enabled
                ],
            )
        )
        ends = campaign.ends_at.date().isoformat() if campaign.ends_at else "без срока"
        block = [
            f"#{campaign.id} <b>{html.escape(campaign.title)}</b>",
            f"Статус: {_status_label(campaign.status)}; до: {ends}",
            (
                f"Будущих событий сейчас: {future_count}; "
                f"видео-публикаций: {len(video_groups)}; промо-показов: {video_show_count}"
            ),
            f"Активности: {html.escape(activity_labels or '—')}",
        ]
        if include_details and video_groups:
            block.append("Публикации:")
            for group in video_groups[:8]:
                items = group.get("items") or []
                positions = ", ".join(str(item["position"]) for item in items)
                item_titles = "; ".join(
                    f"#{item['event_id']} {item['title']} ({item['event_date']})"
                    for item in items[:4]
                )
                if len(items) > 4:
                    item_titles += f"; ещё {len(items) - 4}"
                block.append(
                    (
                        f"• {_fmt_dt(group.get('published_at'))}: "
                        f"{html.escape(str(group.get('profile_key') or 'video'))} "
                        f"session #{group.get('session_id')}, "
                        f"статус {html.escape(str(group.get('status') or ''))}, "
                        f"каналов: {int(group.get('target_count') or 0)}, "
                        f"поз.: {html.escape(positions)} — {html.escape(item_titles)}"
                    )
                )
            if len(video_groups) > 8:
                block.append(f"• … ещё {len(video_groups) - 8}")
        elif include_details and recorded_exposures:
            block.append(f"Записанных exposure rows: {recorded_exposures}")
        lines.append("\n".join(block))
    return lines


async def _set_campaign_status(db: Database, campaign_id: int, status: str) -> bool:
    async with db.get_session() as session:
        campaign = await session.get(PromoCampaign, campaign_id)
        if campaign is None:
            return False
        campaign.status = status
        campaign.updated_at = datetime.now(timezone.utc)
        if status == "archived":
            campaign.archived_at = datetime.now(timezone.utc)
        session.add(campaign)
        await session.commit()
        return True


async def handle_promo_command(message: types.Message, db: Database, bot: Bot) -> None:
    args = (message.text or "").split(maxsplit=1)
    arg_text = args[1].strip() if len(args) > 1 else ""
    lowered = arg_text.casefold()

    if not arg_text or lowered in {"list", "список"}:
        await ensure_initial_80_stories_campaign(db)
        lines = ["<b>Промо-кампании</b>", ""]
        lines.extend(await _campaign_lines(db))
        lines.append("")
        lines.append(
            "Команды: /promo report, /promo seed80, /promo add festival НАЗВАНИЕ, "
            "/promo add event НАЗВАНИЕ, /promo pause ID, /promo start ID, /promo archive ID"
        )
        await bot.send_message(message.chat.id, "\n\n".join(lines), parse_mode="HTML")
        return

    if lowered in {"report", "отчет", "отчёт"}:
        lines = ["<b>Промо-отчёт</b>", ""]
        lines.extend(await _campaign_lines(db, include_archived=True, include_details=True))
        await bot.send_message(message.chat.id, "\n\n".join(lines), parse_mode="HTML")
        return

    if lowered in {"seed80", "80", "80stories"}:
        campaign = await ensure_initial_80_stories_campaign(db)
        if campaign is None:
            await bot.send_message(
                message.chat.id,
                (
                    f"Не создал кампанию: фестиваль {INITIAL_80_STORIES_FESTIVAL!r} "
                    "должен существовать и иметь будущие события."
                ),
            )
            return
        await bot.send_message(message.chat.id, f"Кампания готова: #{campaign.id} {campaign.title}")
        return

    add_args = _natural_add_args(arg_text)
    if add_args is not None:
        kind, raw_query = add_args
        query, end_date = _parse_until_date(raw_query)
        query = _strip_wrapping_quotes(query)
        if kind == "festival":
            result = await create_festival_promo_campaign(
                db,
                festival_name=query,
                ends_at=end_date,
                created_by=int(message.from_user.id) if message.from_user else None,
            )
        else:
            result = await create_event_promo_campaign(
                db,
                query_text=query,
                ends_at=end_date,
                created_by=int(message.from_user.id) if message.from_user else None,
            )
        if result.status == "ambiguous" and result.matches:
            lines = [html.escape(result.message), ""]
            for ev in result.matches:
                lines.append(
                    f"#{ev.id} {html.escape(ev.title)} — {html.escape(str(ev.date))} {html.escape(str(ev.time or ''))}"
                )
            await bot.send_message(message.chat.id, "\n".join(lines), parse_mode="HTML")
            return
        suffix = ""
        if result.campaign and end_date is None:
            suffix = f"\nСрок не указан, поставил ближайшие 3 месяца: до {default_campaign_end().isoformat()}."
        await bot.send_message(message.chat.id, result.message + suffix)
        return

    parts = arg_text.split()
    if len(parts) == 2 and parts[0].casefold() in {"pause", "start", "archive"}:
        try:
            campaign_id = int(parts[1])
        except ValueError:
            await bot.send_message(message.chat.id, "ID кампании должен быть числом.")
            return
        target_status = {
            "pause": "paused",
            "start": "active",
            "archive": "archived",
        }[parts[0].casefold()]
        ok = await _set_campaign_status(db, campaign_id, target_status)
        await bot.send_message(
            message.chat.id,
            f"Готово: #{campaign_id} → {_status_label(target_status)}" if ok else "Кампания не найдена.",
        )
        return

    await bot.send_message(
        message.chat.id,
        (
            "Использование: /promo, /promo report, /promo seed80, "
            "/promo add festival НАЗВАНИЕ [до ДАТА], /promo add event НАЗВАНИЕ [до ДАТА], "
            "/promo pause ID, /promo start ID, /promo archive ID"
        ),
    )
