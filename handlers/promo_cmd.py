from __future__ import annotations

import html
import re
from datetime import date, datetime, timezone

from aiogram import Bot, types
from sqlalchemy import func, or_, select, text

from db import Database
from models import Event, EventSource, PromoActivity, PromoCampaign, PromoExposure, PromoTarget
from promo import (
    INITIAL_80_STORIES_FESTIVAL,
    INITIAL_80_STORIES_PRIORITY,
    PROMO_TARGET_TYPE_TG_CHAT_AUTHOR,
    PROMO_SURFACE_TG_BUTTON_HIGHLIGHT,
    PROMO_SURFACE_TG_EVENT_PUBLISH,
    PROMO_SURFACE_TG_REPOST,
    PROMO_SURFACE_VK_PUBLICATION,
    PROMO_SURFACE_VK_CHANNEL_PUBLISH,
    PROMO_SURFACE_VK_REPOST,
    PROMO_SURFACE_VK_STORY,
    _parse_chat_author_query,
    create_event_promo_campaign,
    create_festival_promo_campaign,
    default_campaign_end,
    ensure_initial_80_stories_campaign,
    normalize_promo_priority,
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


def _promo_keyboard(campaigns: list[PromoCampaign]) -> types.InlineKeyboardMarkup:
    keyboard: list[list[types.InlineKeyboardButton]] = [
        [
            types.InlineKeyboardButton(text="Отчёт", callback_data="vidpromo:report"),
            types.InlineKeyboardButton(text="Seed 80", callback_data="vidpromo:seed80"),
        ]
    ]
    for campaign in campaigns[:8]:
        cid = int(campaign.id or 0)
        if cid <= 0:
            continue
        status = str(campaign.status or "")
        toggle_action = "pause" if status == "active" else "start"
        toggle_label = "Пауза" if status == "active" else "Старт"
        keyboard.append(
            [
                types.InlineKeyboardButton(
                    text=f"#{cid} {toggle_label}",
                    callback_data=f"vidpromo:{toggle_action}:{cid}",
                ),
                types.InlineKeyboardButton(
                    text="Архив",
                    callback_data=f"vidpromo:archive:{cid}",
                ),
            ]
        )
        keyboard.append(
            [
                types.InlineKeyboardButton(
                    text={0: "P0 max", 1: "P1", 2: "P2", 3: "P3 min"}[priority],
                    callback_data=f"vidpromo:priority:{cid}:{priority}",
                )
                for priority in range(0, 4)
            ]
        )
    keyboard.append(
        [types.InlineKeyboardButton(text="Обновить", callback_data="vidpromo:list")]
    )
    return types.InlineKeyboardMarkup(inline_keyboard=keyboard)


def _current_or_future_campaign_filter(now_utc: datetime):
    return or_(PromoCampaign.ends_at.is_(None), PromoCampaign.ends_at >= now_utc)


async def _active_campaigns(db: Database, *, include_archived: bool = False) -> list[PromoCampaign]:
    now_utc = datetime.now(timezone.utc)
    async with db.get_session() as session:
        query = select(PromoCampaign).order_by(
            PromoCampaign.priority,
            PromoCampaign.status,
            PromoCampaign.created_at,
        )
        if not include_archived:
            query = query.where(PromoCampaign.status != "archived").where(
                _current_or_future_campaign_filter(now_utc)
            )
        res = await session.execute(query)
        return list(res.scalars().all())


async def send_promo_menu(
    db: Database,
    bot: Bot,
    chat_id: int,
    *,
    include_report: bool = False,
) -> None:
    await ensure_initial_80_stories_campaign(db)
    campaigns = await _active_campaigns(db, include_archived=include_report)
    lines = ["<b>Промо-кампании</b>", ""]
    lines.extend(
        await _campaign_lines(
            db,
            include_archived=include_report,
            include_details=include_report,
        )
    )
    lines.append("")
    lines.append(
        "Priority: P0 — высший, P3 — низкий. Кнопки P0..P3 только меняют приоритет кампании; "
        "публикации сами не запускают. 80 историй держим на P"
        f"{INITIAL_80_STORIES_PRIORITY}."
    )
    await bot.send_message(
        chat_id,
        "\n\n".join(lines),
        parse_mode="HTML",
        reply_markup=_promo_keyboard(campaigns),
    )


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


def _activity_label(activity: PromoActivity) -> str:
    surface_label = {
        "hero_talk": "Hero-talk · видимость в браузере",
        "video_general": "Видеоанонс",
        "daily_highlight": "Ежедневная подборка",
        "telegraph_month": "Telegraph: месяц",
        "telegraph_weekend": "Telegraph: выходные",
        PROMO_SURFACE_TG_BUTTON_HIGHLIGHT: "TG-кнопка «Подробнее»",
        PROMO_SURFACE_TG_EVENT_PUBLISH: "TG-публикация",
        PROMO_SURFACE_TG_REPOST: "TG-репост",
        PROMO_SURFACE_VK_PUBLICATION: "VK-публикация",
        PROMO_SURFACE_VK_CHANNEL_PUBLISH: "VK-канал",
        PROMO_SURFACE_VK_REPOST: "VK-репост",
        PROMO_SURFACE_VK_STORY: "VK-история",
    }.get(activity.surface, activity.surface)
    parts = [surface_label]
    if activity.profile_key:
        parts.append(str(activity.profile_key))
    if activity.max_per_publish:
        parts.append(f"x{activity.max_per_publish}")
    if activity.daily_cap:
        parts.append(f"не более {activity.daily_cap}/день")
    return " · ".join(parts)


async def _vk_exposures_for_campaign(
    db: Database,
    campaign: PromoCampaign,
) -> list[dict[str, object]]:
    async with db.get_session() as session:
        rows = (
            await session.execute(
                select(PromoExposure, Event.title, Event.date)
                .join(Event, Event.id == PromoExposure.event_id)
                .where(PromoExposure.campaign_id == campaign.id)
                .where(
                    PromoExposure.surface.in_(
                        [
                            PROMO_SURFACE_VK_PUBLICATION,
                            PROMO_SURFACE_VK_CHANNEL_PUBLISH,
                            PROMO_SURFACE_VK_REPOST,
                            PROMO_SURFACE_VK_STORY,
                        ]
                    )
                )
                .order_by(PromoExposure.published_at.desc(), PromoExposure.id.desc())
                .limit(12)
            )
        ).all()
    result: list[dict[str, object]] = []
    for exposure, title, event_date in rows:
        details = exposure.details_json if isinstance(exposure.details_json, dict) else {}
        targets = exposure.public_targets_json if isinstance(exposure.public_targets_json, list) else []
        url = str(details.get("target_url") or "").strip()
        if not url and targets and isinstance(targets[0], dict):
            url = str(targets[0].get("url") or "").strip()
        result.append(
            {
                "surface": exposure.surface,
                "status": exposure.publish_status,
                "event_id": exposure.event_id,
                "title": title or "",
                "event_date": event_date or "",
                "published_at": exposure.published_at,
                "url": url,
                "source_url": str(details.get("source_url") or "").strip(),
            }
        )
    return result


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
            elif target.target_type == PROMO_TARGET_TYPE_TG_CHAT_AUTHOR:
                chat, author = _parse_chat_author_query(target.query_text)
                if not chat or not author:
                    continue
                query = (
                    select(func.count(func.distinct(Event.id)))
                    .select_from(Event)
                    .join(EventSource, EventSource.event_id == Event.id)
                    .where(func.lower(EventSource.source_chat_username) == chat)
                    .where(func.lower(Event.tg_source_author) == author)
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
    now_utc = datetime.now(timezone.utc)
    async with db.get_session() as session:
        query = select(PromoCampaign).order_by(PromoCampaign.status, PromoCampaign.created_at)
        if not include_archived:
            query = query.where(PromoCampaign.status != "archived").where(
                _current_or_future_campaign_filter(now_utc)
            )
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
        vk_exposures = await _vk_exposures_for_campaign(db, campaign)
        video_show_count = sum(len(group.get("items") or []) for group in video_groups)
        activity_labels = ", ".join(
            filter(
                None,
                [
                    _activity_label(activity)
                    for activity in activities
                    if activity.enabled
                ],
            )
        )
        ends = campaign.ends_at.date().isoformat() if campaign.ends_at else "без срока"
        block = [
            f"#{campaign.id} <b>{html.escape(campaign.title)}</b>",
            (
                f"Статус: {_status_label(campaign.status)}; "
                f"priority: {normalize_promo_priority(getattr(campaign, 'priority', None))}; "
                f"до: {ends}"
            ),
            (
                f"Будущих событий сейчас: {future_count}; "
                f"видео-публикаций: {len(video_groups)}; промо-показов: {video_show_count}; "
                f"VK-активностей: {len(vk_exposures)}"
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
        if include_details and vk_exposures:
            block.append("VK:")
            for item in vk_exposures[:8]:
                label = {
                    PROMO_SURFACE_VK_PUBLICATION: "публикация",
                    PROMO_SURFACE_VK_CHANNEL_PUBLISH: "канал",
                    PROMO_SURFACE_VK_REPOST: "репост",
                    PROMO_SURFACE_VK_STORY: "история",
                }.get(str(item["surface"]), str(item["surface"]))
                source = f" ← {item['source_url']}" if item.get("source_url") else ""
                url = str(item.get("url") or "")
                block.append(
                    (
                        f"• {_fmt_dt(item.get('published_at'))}: {label} · "
                        f"ev#{item['event_id']} ({html.escape(str(item.get('event_date') or ''))}) · "
                        f"{html.escape(str(item.get('status') or ''))} · "
                        f"{html.escape(url or 'url pending')}{html.escape(source)}"
                    )
                )
            if len(vk_exposures) > 8:
                block.append(f"• … ещё {len(vk_exposures) - 8}")
        elif include_details and recorded_exposures and not video_groups:
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


async def _set_campaign_priority(db: Database, campaign_id: int, priority: int) -> bool:
    async with db.get_session() as session:
        campaign = await session.get(PromoCampaign, campaign_id)
        if campaign is None:
            return False
        campaign.priority = normalize_promo_priority(priority)
        campaign.updated_at = datetime.now(timezone.utc)
        session.add(campaign)
        await session.commit()
        return True


async def handle_promo_command(message: types.Message, db: Database, bot: Bot) -> None:
    args = (message.text or "").split(maxsplit=1)
    arg_text = args[1].strip() if len(args) > 1 else ""
    lowered = arg_text.casefold()

    if not arg_text or lowered in {"list", "список"}:
        await send_promo_menu(db, bot, message.chat.id)
        return

    if lowered in {"report", "отчет", "отчёт"}:
        await send_promo_menu(db, bot, message.chat.id, include_report=True)
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

    if len(parts) == 3 and parts[0].casefold() in {"priority", "prio", "p"}:
        try:
            campaign_id = int(parts[1])
            priority = normalize_promo_priority(parts[2])
        except ValueError:
            await bot.send_message(message.chat.id, "ID и priority должны быть числами.")
            return
        ok = await _set_campaign_priority(db, campaign_id, priority)
        await bot.send_message(
            message.chat.id,
            f"Готово: #{campaign_id} priority → {priority}" if ok else "Кампания не найдена.",
        )
        return

    await bot.send_message(
        message.chat.id,
        (
            "Использование: /promo, /promo report, /promo seed80, "
            "/promo add festival НАЗВАНИЕ [до ДАТА], /promo add event НАЗВАНИЕ [до ДАТА], "
            "/promo pause ID, /promo start ID, /promo archive ID, /promo priority ID 0..3"
        ),
    )


async def handle_promo_callback(callback: types.CallbackQuery, db: Database, bot: Bot) -> None:
    async with db.get_session() as session:
        from models import User
        from main import has_admin_access

        user = await session.get(User, callback.from_user.id)
        if not has_admin_access(user):
            await callback.answer("Not authorized", show_alert=True)
            return
    data = callback.data or ""
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else "list"
    chat_id = callback.message.chat.id
    if action in {"list", "menu"}:
        await callback.answer("Промо")
        await send_promo_menu(db, bot, chat_id)
        return
    if action == "report":
        await callback.answer("Отчёт")
        await send_promo_menu(db, bot, chat_id, include_report=True)
        return
    if action == "seed80":
        campaign = await ensure_initial_80_stories_campaign(db)
        await callback.answer("Seed 80")
        if campaign is None:
            await bot.send_message(
                chat_id,
                (
                    f"Не создал кампанию: фестиваль {INITIAL_80_STORIES_FESTIVAL!r} "
                    "должен существовать и иметь будущие события."
                ),
            )
        else:
            await bot.send_message(chat_id, f"Кампания готова: #{campaign.id} {campaign.title}")
        return
    if action in {"pause", "start", "archive"} and len(parts) >= 3:
        try:
            campaign_id = int(parts[2])
        except ValueError:
            await callback.answer("Некорректный ID", show_alert=True)
            return
        target_status = {
            "pause": "paused",
            "start": "active",
            "archive": "archived",
        }[action]
        ok = await _set_campaign_status(db, campaign_id, target_status)
        await callback.answer("Готово" if ok else "Не найдено", show_alert=not ok)
        await send_promo_menu(db, bot, chat_id)
        return
    if action == "priority" and len(parts) >= 4:
        try:
            campaign_id = int(parts[2])
            priority = normalize_promo_priority(parts[3])
        except ValueError:
            await callback.answer("Некорректные числа", show_alert=True)
            return
        ok = await _set_campaign_priority(db, campaign_id, priority)
        await callback.answer(f"P{priority}" if ok else "Не найдено", show_alert=not ok)
        await send_promo_menu(db, bot, chat_id)
        return
    await callback.answer("Неизвестное действие", show_alert=True)
