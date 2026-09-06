"""Partner promo FSM handler.

Routes ``ppromo:*`` callbacks invoked from the per-event ``🎬`` button on
``/events``. The user-facing spec is
``docs/backlog/features/promo-campaigns/partner-promo.md``.

Phase A scope: video_general surface with three slot policies. The
``vk_publication`` and ``vk_repost`` surfaces are now live (runner in
``promo.run_promo_vk_activities``); this handler renders them in the campaign
card and per-activity statistics, while the per-event ``+ Активность`` add
flow still focuses on video/repost.
"""

from __future__ import annotations

import html
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from aiogram import Bot, types
from sqlalchemy import func, or_, select

from db import Database
from models import (
    Event,
    Organization,
    PromoActivity,
    PromoCampaign,
    PromoExposure,
    PromoTarget,
    User,
)
from partner_promo import (
    PARTNER_PROMO_INPUT_COUNT,
    PARTNER_PROMO_INPUT_DISCLOSURE,
    PARTNER_PROMO_INPUT_END_DATE,
    PARTNER_PROMO_INPUT_RENAME,
    PartnerPromoInputSession,
    PartnerPromoSession,
    partner_promo_input_sessions,
    partner_promo_sessions,
)
from promo import (
    PARTNER_PROMO_SLOT_POLICIES,
    PARTNER_PROMO_VIDEO_PROFILES,
    PROMO_DEFAULT_PRIORITY,
    PROMO_POLICY_FIRST_SLOT,
    PROMO_POLICY_FIRST_TWO_SLOTS,
    PROMO_POLICY_GUARANTEED_ANY_POSITION,
    PROMO_SURFACE_VIDEO_GENERAL,
    PROMO_SURFACE_VK_PUBLICATION,
    PROMO_SURFACE_VK_CHANNEL_PUBLISH,
    PROMO_SURFACE_VK_REPOST,
    PROMO_SURFACE_VK_STORY,
    PROMO_VK_DEFAULT_WINDOW_HOURS,
    PartnerPromoSpec,
    build_partner_campaign_title,
    clamp_campaign_end_to_event,
    create_partner_event_promo_campaign,
    _initial_80_vk_channel_publish_activity,
    normalize_promo_priority,
)

logger = logging.getLogger(__name__)


PROFILE_ORDER = ("popular_review", "default", "konb")
SLOT_ORDER = (
    PROMO_POLICY_GUARANTEED_ANY_POSITION,
    PROMO_POLICY_FIRST_TWO_SLOTS,
    PROMO_POLICY_FIRST_SLOT,
)
COUNT_PRESETS = (1, 2, 3, 5, 7, 10)
DEFAULT_DURATIONS_DAYS = (7, 14, 30)


def _current_or_future_campaign_filter(now_utc: datetime):
    return or_(PromoCampaign.ends_at.is_(None), PromoCampaign.ends_at >= now_utc)


def _status_label(status: str) -> str:
    return {
        "draft": "черновик",
        "active": "активна",
        "paused": "пауза",
        "archived": "архив",
    }.get(status, status)


def _event_last_date(event: Event) -> date | None:
    raw = (getattr(event, "end_date", None) or getattr(event, "date", "") or "").split("..", 1)[0].strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


async def _load_event_and_user(
    db: Database, *, event_id: int, user_id: int
) -> tuple[Optional[Event], Optional[User]]:
    async with db.get_session() as session:
        event = await session.get(Event, int(event_id))
        user = await session.get(User, int(user_id))
        return event, user


def _is_authorized(user: User | None, event: Event | None) -> bool:
    if user is None or user.blocked or event is None:
        return False
    if user.is_superadmin:
        return True
    if user.is_partner and int(event.creator_id or 0) == int(user.user_id or 0):
        return True
    return False


async def _load_partner_organization(
    db: Database, user: User
) -> Optional[Organization]:
    name = (user.organization or "").strip()
    if not name:
        return None
    async with db.get_session() as session:
        return await session.get(Organization, name)


async def _list_campaigns_covering_event(
    db: Database, *, user: User, event_id: int
) -> tuple[list[PromoCampaign], list[PromoCampaign]]:
    """Return (event_targeted, festival_covering) campaigns for this event.

    The first list is campaigns whose ``PromoTarget`` directly points to this
    event. The second list is festival-targeted campaigns whose
    ``festival_name`` matches ``event.festival`` — those campaigns cover this
    event implicitly even though there is no direct event-id link.

    For non-superadmin users both lists are filtered by ``created_by``: a
    partner sees only their own campaigns, regardless of target type.
    """

    now_utc = datetime.now(timezone.utc)
    async with db.get_session() as session:
        event = await session.get(Event, int(event_id))
        festival_name = (getattr(event, "festival", None) or "").strip() if event else ""

        event_stmt = (
            select(PromoCampaign)
            .join(PromoTarget, PromoTarget.campaign_id == PromoCampaign.id)
            .where(PromoTarget.target_type == "event")
            .where(PromoTarget.event_id == int(event_id))
            .where(PromoCampaign.status != "archived")
            .where(_current_or_future_campaign_filter(now_utc))
            .order_by(PromoCampaign.created_at.desc())
        )
        if not user.is_superadmin:
            event_stmt = event_stmt.where(PromoCampaign.created_by == int(user.user_id))
        event_campaigns = list((await session.execute(event_stmt)).scalars().all())

        festival_campaigns: list[PromoCampaign] = []
        if festival_name:
            fest_stmt = (
                select(PromoCampaign)
                .join(PromoTarget, PromoTarget.campaign_id == PromoCampaign.id)
                .where(PromoTarget.target_type == "festival")
                .where(PromoTarget.festival_name == festival_name)
                .where(PromoCampaign.status != "archived")
                .where(_current_or_future_campaign_filter(now_utc))
                .order_by(PromoCampaign.created_at.desc())
            )
            if not user.is_superadmin:
                fest_stmt = fest_stmt.where(
                    PromoCampaign.created_by == int(user.user_id)
                )
            festival_campaigns = list(
                (await session.execute(fest_stmt)).scalars().all()
            )

        # Dedup: a campaign cannot be in both buckets (event vs festival
        # target), but be defensive in case of mixed-target campaigns.
        event_ids = {int(c.id or 0) for c in event_campaigns}
        festival_campaigns = [
            c for c in festival_campaigns if int(c.id or 0) not in event_ids
        ]
        return event_campaigns, festival_campaigns


def _surface_buttons(
    *, event_id: int, organization: Organization | None, vk_repost_available: bool
) -> list[list[types.InlineKeyboardButton]]:
    rows: list[list[types.InlineKeyboardButton]] = []
    for profile in PROFILE_ORDER:
        if profile == "konb" and (
            organization is None or (organization.video_profile_key or "") != "konb"
        ):
            continue
        label = PARTNER_PROMO_VIDEO_PROFILES[profile]
        rows.append(
            [
                types.InlineKeyboardButton(
                    text=f"🎬 {label}",
                    callback_data=f"ppromo:surface:{event_id}:video_general:{profile}",
                )
            ]
        )
    if vk_repost_available:
        rows.append(
            [
                types.InlineKeyboardButton(
                    text="📨 Репост в партнёрский паблик",
                    callback_data=f"ppromo:surface:{event_id}:vk_repost:none",
                )
            ]
        )
    rows.append(
        [
            types.InlineKeyboardButton(
                text="🌐 Сайт",
                callback_data=f"ppromo:site:{event_id}",
            )
        ]
    )
    rows.append(_nav_row(event_id, step=1))
    return rows


def _nav_row(event_id: int, *, step: int) -> list[types.InlineKeyboardButton]:
    row: list[types.InlineKeyboardButton] = []
    if step > 1:
        row.append(
            types.InlineKeyboardButton(
                text="◀ Назад", callback_data=f"ppromo:back:{event_id}:{step}"
            )
        )
    row.append(
        types.InlineKeyboardButton(
            text="✕ Отмена", callback_data=f"ppromo:cancel:{event_id}"
        )
    )
    return row


def _event_card_text(event: Event) -> str:
    title = html.escape(event.title or "")
    date_str = html.escape(str(event.date or "")[:10])
    return f"<b>{title}</b>\n<i>{date_str}</i> · #{event.id}"


def _campaign_list_text(
    event: Event,
    event_campaigns: list[PromoCampaign],
    festival_campaigns: list[PromoCampaign],
) -> str:
    lines = [_event_card_text(event), ""]
    if not event_campaigns and not festival_campaigns:
        lines.append("Кампаний по этому событию пока нет.")
        return "\n".join(lines)
    if event_campaigns:
        lines.append("<b>Кампании по событию:</b>")
        for c in event_campaigns:
            lines.append(
                f"#{c.id} <b>{html.escape(c.title)}</b> — {_status_label(c.status)}"
            )
    if festival_campaigns:
        if event_campaigns:
            lines.append("")
        festival_name = (event.festival or "").strip()
        header = "<b>Покрывающие фестивальные кампании"
        if festival_name:
            header += f" ({html.escape(festival_name)})"
        header += ":</b>"
        lines.append(header)
        for c in festival_campaigns:
            lines.append(
                f"#{c.id} <b>{html.escape(c.title)}</b> — {_status_label(c.status)}"
            )
    return "\n".join(lines)


def _campaigns_keyboard(
    event_id: int,
    event_campaigns: list[PromoCampaign],
    festival_campaigns: list[PromoCampaign],
) -> types.InlineKeyboardMarkup:
    rows: list[list[types.InlineKeyboardButton]] = []
    for c in event_campaigns[:5]:
        rows.append(
            [
                types.InlineKeyboardButton(
                    text=f"📊 #{c.id} {c.title[:35]}",
                    callback_data=f"ppromo:view:{c.id}",
                )
            ]
        )
    for c in festival_campaigns[:3]:
        rows.append(
            [
                types.InlineKeyboardButton(
                    text=f"🎪 #{c.id} {c.title[:33]}",
                    callback_data=f"ppromo:view:{c.id}",
                )
            ]
        )
    rows.append(
        [
            types.InlineKeyboardButton(
                text="➕ Новая промо-кампания",
                callback_data=f"ppromo:new:{event_id}",
            )
        ]
    )
    rows.append(
        [
            types.InlineKeyboardButton(
                text="✕ Закрыть", callback_data=f"ppromo:cancel:{event_id}"
            )
        ]
    )
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


async def _send_or_edit(
    bot: Bot, callback: types.CallbackQuery, text: str, markup: types.InlineKeyboardMarkup
) -> None:
    chat_id: int | None = None
    if callback.message is not None:
        chat_id = callback.message.chat.id
        try:
            await callback.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
            return
        except Exception:
            logger.debug("partner_promo: edit_text failed, sending new", exc_info=True)
    if chat_id is None:
        target = getattr(callback, "_reply_target", None)
        if target is not None:
            chat_id = target.chat.id
    if chat_id is None and callback.from_user is not None:
        chat_id = callback.from_user.id
    if chat_id is None:
        return
    await bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")


async def render_step0_campaign_list(
    callback: types.CallbackQuery, db: Database, bot: Bot, *, event: Event, user: User
) -> None:
    event_campaigns, festival_campaigns = await _list_campaigns_covering_event(
        db, user=user, event_id=int(event.id)
    )
    text = _campaign_list_text(event, event_campaigns, festival_campaigns)
    markup = _campaigns_keyboard(
        int(event.id), event_campaigns, festival_campaigns
    )
    await _send_or_edit(bot, callback, text, markup)


async def render_step1_surface(
    callback: types.CallbackQuery,
    db: Database,
    bot: Bot,
    *,
    event: Event,
    user: User,
) -> None:
    organization = await _load_partner_organization(db, user)
    vk_repost_available = bool(
        organization
        and organization.vk_source_group_ids
        and (event.source_vk_post_url or "")
    )
    rows = _surface_buttons(
        event_id=int(event.id),
        organization=organization,
        vk_repost_available=vk_repost_available,
    )
    text = (
        _event_card_text(event)
        + "\n\nШаг 1/6. Выберите тип размещения:"
    )
    await _send_or_edit(
        bot, callback, text, types.InlineKeyboardMarkup(inline_keyboard=rows)
    )


def _slot_rows(event_id: int) -> list[list[types.InlineKeyboardButton]]:
    rows: list[list[types.InlineKeyboardButton]] = []
    for policy in SLOT_ORDER:
        label = PARTNER_PROMO_SLOT_POLICIES[policy]
        rows.append(
            [
                types.InlineKeyboardButton(
                    text=label,
                    callback_data=f"ppromo:slot:{event_id}:{policy}",
                )
            ]
        )
    rows.append(_nav_row(event_id, step=2))
    return rows


async def render_step2_slot(
    callback: types.CallbackQuery,
    bot: Bot,
    *,
    event: Event,
    session: PartnerPromoSession,
) -> None:
    profile_label = PARTNER_PROMO_VIDEO_PROFILES.get(
        session.profile_key or "", session.profile_key or ""
    )
    text = (
        _event_card_text(event)
        + f"\n\nШаг 2/6. {html.escape(profile_label)}"
        + "\nВыберите расположение в видео:\n\n"
        + "<i>Слот может не гарантироваться сегодня, если он уже занят\n"
        + "более приоритетной кампанией — попытка перенесётся на следующий выпуск.</i>"
    )
    markup = types.InlineKeyboardMarkup(inline_keyboard=_slot_rows(int(event.id)))
    await _send_or_edit(bot, callback, text, markup)


def _count_rows(event_id: int) -> list[list[types.InlineKeyboardButton]]:
    rows: list[list[types.InlineKeyboardButton]] = []
    line: list[types.InlineKeyboardButton] = []
    for n in COUNT_PRESETS:
        line.append(
            types.InlineKeyboardButton(
                text=str(n), callback_data=f"ppromo:count:{event_id}:{n}"
            )
        )
        if len(line) == 3:
            rows.append(line)
            line = []
    if line:
        rows.append(line)
    rows.append(
        [
            types.InlineKeyboardButton(
                text="Ввести число",
                callback_data=f"ppromo:count_input:{event_id}",
            )
        ]
    )
    rows.append(_nav_row(event_id, step=3))
    return rows


async def render_step3_count(
    callback: types.CallbackQuery, bot: Bot, *, event: Event
) -> None:
    text = _event_card_text(event) + "\n\nШаг 3/6. Сколько показов?"
    markup = types.InlineKeyboardMarkup(inline_keyboard=_count_rows(int(event.id)))
    await _send_or_edit(bot, callback, text, markup)


def _end_date_rows(event_id: int, event_last: date | None) -> list[list[types.InlineKeyboardButton]]:
    rows: list[list[types.InlineKeyboardButton]] = []
    for days in DEFAULT_DURATIONS_DAYS:
        rows.append(
            [
                types.InlineKeyboardButton(
                    text=f"+{days} дней",
                    callback_data=f"ppromo:end:{event_id}:d{days}",
                )
            ]
        )
    if event_last is not None:
        rows.append(
            [
                types.InlineKeyboardButton(
                    text=f"До даты события ({event_last.isoformat()})",
                    callback_data=f"ppromo:end:{event_id}:event",
                )
            ]
        )
    rows.append(
        [
            types.InlineKeyboardButton(
                text="Ввести дату",
                callback_data=f"ppromo:end_input:{event_id}",
            )
        ]
    )
    rows.append(_nav_row(event_id, step=4))
    return rows


async def render_step4_end(
    callback: types.CallbackQuery, bot: Bot, *, event: Event
) -> None:
    text = _event_card_text(event) + "\n\nШаг 4/6. Дата окончания кампании:"
    rows = _end_date_rows(int(event.id), _event_last_date(event))
    markup = types.InlineKeyboardMarkup(inline_keyboard=rows)
    await _send_or_edit(bot, callback, text, markup)


def _mode_rows(event_id: int) -> list[list[types.InlineKeyboardButton]]:
    return [
        [
            types.InlineKeyboardButton(
                text="Партнёрский / коммерческий",
                callback_data=f"ppromo:mode:{event_id}:partner",
            )
        ],
        [
            types.InlineKeyboardButton(
                text="Редакционный (бесплатный)",
                callback_data=f"ppromo:mode:{event_id}:editorial",
            )
        ],
        _nav_row(event_id, step=5),
    ]


async def render_step5_mode(
    callback: types.CallbackQuery, bot: Bot, *, event: Event
) -> None:
    text = (
        _event_card_text(event)
        + "\n\nШаг 5/6. Режим кампании:\n\n"
        + "<i>Партнёрский режим публикует подпись «Партнёрский материал».\n"
        + "Редакционный — только маркер ✨ без подписи.</i>"
    )
    markup = types.InlineKeyboardMarkup(inline_keyboard=_mode_rows(int(event.id)))
    await _send_or_edit(bot, callback, text, markup)


def _confirm_rows(
    event_id: int, *, session: PartnerPromoSession | None = None
) -> list[list[types.InlineKeyboardButton]]:
    is_add_activity = session is not None and session.add_to_campaign_id is not None
    primary_label = "✅ Добавить активность" if is_add_activity else "✅ Запустить"
    rows: list[list[types.InlineKeyboardButton]] = [
        [
            types.InlineKeyboardButton(
                text=primary_label,
                callback_data=f"ppromo:confirm:{event_id}",
            )
        ],
    ]
    # Rename is only meaningful when creating a new campaign — in add-activity
    # mode the campaign already has a fixed title.
    secondary_row: list[types.InlineKeyboardButton] = []
    if not is_add_activity:
        secondary_row.append(
            types.InlineKeyboardButton(
                text="✏ Переименовать",
                callback_data=f"ppromo:rename_input:{event_id}",
            )
        )
    secondary_row.append(
        types.InlineKeyboardButton(
            text="✕ Отмена", callback_data=f"ppromo:cancel:{event_id}"
        )
    )
    rows.append(secondary_row)
    # In add-activity mode the previous step is step 3 (count) — Back goes to
    # the count picker, skipping end-date/mode which aren't asked.
    back_step = 4 if is_add_activity else 6
    rows.append(
        [
            types.InlineKeyboardButton(
                text="◀ Назад",
                callback_data=f"ppromo:back:{event_id}:{back_step}",
            )
        ]
    )
    return rows


def _summary_text(
    event: Event,
    sess: PartnerPromoSession,
    *,
    suggested_title: str,
    mode_label: str,
    campaign: PromoCampaign | None = None,
) -> str:
    profile = (
        PARTNER_PROMO_VIDEO_PROFILES.get(sess.profile_key or "", sess.profile_key or "—")
        if sess.surface == PROMO_SURFACE_VIDEO_GENERAL
        else "Репост ВК"
    )
    slot = (
        PARTNER_PROMO_SLOT_POLICIES.get(sess.slot_policy or "", sess.slot_policy or "—")
        if sess.surface == PROMO_SURFACE_VIDEO_GENERAL
        else "—"
    )
    if sess.add_to_campaign_id is not None and campaign is not None:
        # Add-activity mode: campaign already exists; show its parameters as
        # the inherited context and only highlight the new placement.
        end = (
            campaign.ends_at.date().isoformat() if campaign.ends_at else "—"
        )
        disclosure = campaign.sponsorship_disclosure or "редакционный (без подписи)"
        lines = [
            "<b>Добавление активности</b>",
            "",
            f"К кампании: #{campaign.id} <i>{html.escape(campaign.title)}</i>",
            f"Событие: #{event.id} {html.escape(event.title or '')}",
            f"Период (наследуется): до {end}",
            f"Режим (наследуется): {disclosure}",
            "",
            f"Новая активность: {html.escape(profile)}" + (
                f", {html.escape(slot)}" if sess.surface == PROMO_SURFACE_VIDEO_GENERAL else ""
            ),
            f"Количество показов: {sess.count}",
        ]
        return "\n".join(lines)

    end = sess.ends_at.isoformat() if sess.ends_at else "—"
    disclosure = sess.sponsorship_disclosure or "—"
    title = sess.title_override or suggested_title
    lines = [
        "<b>Подтвердите кампанию</b>",
        "",
        f"Название: <code>{html.escape(title)}</code>",
        f"Событие: #{event.id} {html.escape(event.title or '')}",
        f"Размещение: {html.escape(profile)}" + (
            f", {html.escape(slot)}" if sess.surface == PROMO_SURFACE_VIDEO_GENERAL else ""
        ),
        f"Количество показов: {sess.count}",
        f"Период: до {end}",
        f"Режим: {mode_label}",
    ]
    if not sess.is_editorial:
        lines.append(f"Раскрытие: «{html.escape(disclosure)}»")
    return "\n".join(lines)


async def render_step6_confirm(
    callback: types.CallbackQuery,
    db: Database,
    bot: Bot,
    *,
    event: Event,
    user: User,
    session: PartnerPromoSession,
) -> None:
    organization = await _load_partner_organization(db, user)
    suggested = build_partner_campaign_title(
        organization_name=organization.name if organization else (user.organization or None),
        partner_username=user.username,
        event_title=event.title or "",
        created_date=datetime.now(timezone.utc).date(),
        is_superadmin=bool(user.is_superadmin),
    )
    mode_label = "редакционный" if session.is_editorial else "партнёрский"
    campaign: PromoCampaign | None = None
    if session.add_to_campaign_id is not None:
        async with db.get_session() as ses:
            campaign = await ses.get(PromoCampaign, int(session.add_to_campaign_id))
    text = _summary_text(
        event, session, suggested_title=suggested, mode_label=mode_label, campaign=campaign
    )
    markup = types.InlineKeyboardMarkup(
        inline_keyboard=_confirm_rows(int(event.id), session=session)
    )
    await _send_or_edit(bot, callback, text, markup)


def _parse_date_input(raw: str) -> date | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        pass
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%d.%m"):
        try:
            parsed = datetime.strptime(raw, fmt).date()
            if fmt == "%d.%m":
                parsed = parsed.replace(year=datetime.now(timezone.utc).year)
            return parsed
        except ValueError:
            continue
    return None


async def _begin_flow(
    callback: types.CallbackQuery, db: Database, bot: Bot, *, event_id: int
) -> None:
    event, user = await _load_event_and_user(
        db, event_id=event_id, user_id=callback.from_user.id
    )
    if not _is_authorized(user, event):
        await callback.answer("Not authorized", show_alert=True)
        return
    await callback.answer()
    await render_step0_campaign_list(callback, db, bot, event=event, user=user)


async def _open_new_form(
    callback: types.CallbackQuery, db: Database, bot: Bot, *, event_id: int
) -> None:
    event, user = await _load_event_and_user(
        db, event_id=event_id, user_id=callback.from_user.id
    )
    if not _is_authorized(user, event):
        await callback.answer("Not authorized", show_alert=True)
        return
    partner_promo_sessions[int(callback.from_user.id)] = PartnerPromoSession(
        event_id=int(event_id), step=1
    )
    await callback.answer()
    await render_step1_surface(callback, db, bot, event=event, user=user)


async def _open_add_activity_form(
    callback: types.CallbackQuery, db: Database, bot: Bot, *, campaign_id: int
) -> None:
    """Start the abbreviated FSM that appends an activity to a campaign.

    Period, mode and disclosure are taken from the campaign — the user only
    picks surface/slot/count, then confirms.
    """

    user_id = int(callback.from_user.id)
    user = await _load_user(db, user_id)
    if not _is_role_authorized_for_menu(user):
        await callback.answer("Not authorized", show_alert=True)
        return
    campaign = await _load_campaign_with_visibility(
        db, user=user, campaign_id=campaign_id
    )
    if campaign is None:
        await callback.answer("Кампания недоступна", show_alert=True)
        return
    if campaign.status == "archived":
        await callback.answer(
            "Кампания в архиве. Сначала восстановите её.", show_alert=True
        )
        return
    # Find the campaign's event target so we can reuse the existing
    # event-bound rendering and FSM helpers.
    async with db.get_session() as session:
        target = (
            await session.execute(
                select(PromoTarget)
                .where(PromoTarget.campaign_id == campaign.id)
                .where(PromoTarget.target_type == "event")
            )
        ).scalars().first()
    if target is None or not target.event_id:
        await callback.answer(
            "У кампании нет связанного события — добавление активности доступно "
            "только для event-targeted кампаний.",
            show_alert=True,
        )
        return
    event_id = int(target.event_id)
    event, _ = await _load_event_and_user(db, event_id=event_id, user_id=user_id)
    if event is None:
        await callback.answer("Событие не найдено.", show_alert=True)
        return
    partner_promo_sessions[user_id] = PartnerPromoSession(
        event_id=event_id,
        step=1,
        add_to_campaign_id=int(campaign.id),
    )
    await callback.answer()
    await render_step1_surface(callback, db, bot, event=event, user=user)


async def _handle_surface(
    callback: types.CallbackQuery,
    db: Database,
    bot: Bot,
    *,
    event_id: int,
    surface: str,
    profile: str,
) -> None:
    event, user = await _load_event_and_user(
        db, event_id=event_id, user_id=callback.from_user.id
    )
    if not _is_authorized(user, event):
        await callback.answer("Not authorized", show_alert=True)
        return
    if surface == PROMO_SURFACE_VK_REPOST:
        await callback.answer(
            "Репост в партнёрский паблик появится в следующем релизе.",
            show_alert=True,
        )
        return
    if surface != PROMO_SURFACE_VIDEO_GENERAL:
        await callback.answer("Неизвестная поверхность", show_alert=True)
        return
    if profile not in PARTNER_PROMO_VIDEO_PROFILES:
        await callback.answer("Неизвестный профиль", show_alert=True)
        return
    sess = partner_promo_sessions.get(int(callback.from_user.id))
    if sess is None or sess.event_id != int(event_id):
        sess = PartnerPromoSession(event_id=int(event_id))
        partner_promo_sessions[int(callback.from_user.id)] = sess
    sess.surface = surface
    sess.profile_key = profile
    sess.step = 2
    await callback.answer()
    await render_step2_slot(callback, bot, event=event, session=sess)


async def _handle_slot(
    callback: types.CallbackQuery,
    db: Database,
    bot: Bot,
    *,
    event_id: int,
    policy: str,
) -> None:
    if policy not in PARTNER_PROMO_SLOT_POLICIES:
        await callback.answer("Неизвестная политика", show_alert=True)
        return
    sess = partner_promo_sessions.get(int(callback.from_user.id))
    if sess is None or sess.event_id != int(event_id):
        await callback.answer("Сессия истекла, начните заново.", show_alert=True)
        return
    event, user = await _load_event_and_user(
        db, event_id=event_id, user_id=callback.from_user.id
    )
    if not _is_authorized(user, event):
        await callback.answer("Not authorized", show_alert=True)
        return
    sess.slot_policy = policy
    sess.step = 3
    await callback.answer()
    await render_step3_count(callback, bot, event=event)


async def _handle_count(
    callback: types.CallbackQuery,
    db: Database,
    bot: Bot,
    *,
    event_id: int,
    n: int,
) -> None:
    if n <= 0 or n > 365:
        await callback.answer("Допускаются числа от 1 до 365.", show_alert=True)
        return
    sess = partner_promo_sessions.get(int(callback.from_user.id))
    if sess is None or sess.event_id != int(event_id):
        await callback.answer("Сессия истекла.", show_alert=True)
        return
    event, user = await _load_event_and_user(
        db, event_id=event_id, user_id=callback.from_user.id
    )
    if not _is_authorized(user, event):
        await callback.answer("Not authorized", show_alert=True)
        return
    sess.count = int(n)
    if sess.add_to_campaign_id is not None:
        # Add-activity mode skips end-date + mode and jumps to confirm —
        # those values are inherited from the existing campaign.
        sess.step = 6
        await callback.answer()
        await render_step6_confirm(
            callback, db, bot, event=event, user=user, session=sess
        )
        return
    sess.step = 4
    await callback.answer()
    await render_step4_end(callback, bot, event=event)


async def _handle_end_choice(
    callback: types.CallbackQuery,
    db: Database,
    bot: Bot,
    *,
    event_id: int,
    choice: str,
) -> None:
    sess = partner_promo_sessions.get(int(callback.from_user.id))
    if sess is None or sess.event_id != int(event_id):
        await callback.answer("Сессия истекла.", show_alert=True)
        return
    event, user = await _load_event_and_user(
        db, event_id=event_id, user_id=callback.from_user.id
    )
    if not _is_authorized(user, event):
        await callback.answer("Not authorized", show_alert=True)
        return
    today = datetime.now(timezone.utc).date()
    ends_at: date | None
    if choice.startswith("d"):
        try:
            days = int(choice[1:])
        except ValueError:
            days = 0
        if days <= 0:
            await callback.answer("Некорректный период.", show_alert=True)
            return
        ends_at = today + timedelta(days=days)
    elif choice == "event":
        ends_at = _event_last_date(event)
        if ends_at is None:
            await callback.answer("У события нет даты.", show_alert=True)
            return
    else:
        await callback.answer("Неизвестный выбор.", show_alert=True)
        return
    clamped = clamp_campaign_end_to_event(ends_at, event)
    if clamped < today:
        await callback.answer(
            "После клампа на дату события дата окончания в прошлом.",
            show_alert=True,
        )
        return
    sess.ends_at = clamped
    sess.step = 5
    await callback.answer()
    await render_step5_mode(callback, bot, event=event)


async def _handle_mode(
    callback: types.CallbackQuery,
    db: Database,
    bot: Bot,
    *,
    event_id: int,
    mode: str,
) -> None:
    if mode not in {"partner", "editorial"}:
        await callback.answer("Неизвестный режим.", show_alert=True)
        return
    sess = partner_promo_sessions.get(int(callback.from_user.id))
    if sess is None or sess.event_id != int(event_id):
        await callback.answer("Сессия истекла.", show_alert=True)
        return
    event, user = await _load_event_and_user(
        db, event_id=event_id, user_id=callback.from_user.id
    )
    if not _is_authorized(user, event):
        await callback.answer("Not authorized", show_alert=True)
        return
    organization = await _load_partner_organization(db, user)
    if mode == "editorial":
        sess.is_editorial = True
        sess.sponsorship_disclosure = None
    else:
        sess.is_editorial = False
        if not sess.sponsorship_disclosure:
            sess.sponsorship_disclosure = (
                (organization.sponsorship_default if organization else None)
                or "Партнёрский материал"
            )
    sess.step = 6
    await callback.answer()
    await render_step6_confirm(callback, db, bot, event=event, user=user, session=sess)


async def _handle_confirm(
    callback: types.CallbackQuery, db: Database, bot: Bot, *, event_id: int
) -> None:
    sess = partner_promo_sessions.get(int(callback.from_user.id))
    if sess is None or sess.event_id != int(event_id):
        await callback.answer("Сессия истекла.", show_alert=True)
        return
    event, user = await _load_event_and_user(
        db, event_id=event_id, user_id=callback.from_user.id
    )
    if not _is_authorized(user, event):
        await callback.answer("Not authorized", show_alert=True)
        return
    is_add_activity = sess.add_to_campaign_id is not None
    minimum_filled = all(
        [
            sess.surface,
            sess.profile_key if sess.surface == PROMO_SURFACE_VIDEO_GENERAL else True,
            sess.slot_policy if sess.surface == PROMO_SURFACE_VIDEO_GENERAL else True,
            sess.count,
        ]
    )
    if not is_add_activity:
        minimum_filled = minimum_filled and bool(sess.ends_at)
    if not minimum_filled:
        await callback.answer("Не все шаги заполнены.", show_alert=True)
        return
    organization = await _load_partner_organization(db, user)
    if is_add_activity:
        from promo import PartnerActivitySpec, add_partner_activity_to_campaign

        result = await add_partner_activity_to_campaign(
            db,
            PartnerActivitySpec(
                campaign_id=int(sess.add_to_campaign_id),
                surface=str(sess.surface),
                profile_key=sess.profile_key,
                slot_policy=str(sess.slot_policy or PROMO_POLICY_GUARANTEED_ANY_POSITION),
                count=int(sess.count or 0),
            ),
            actor_user_id=int(user.user_id),
        )
    else:
        spec = PartnerPromoSpec(
            event_id=int(event.id),
            creator_user_id=int(user.user_id),
            organization_name=(organization.name if organization else (user.organization or None)),
            surface=str(sess.surface),
            profile_key=sess.profile_key,
            slot_policy=str(sess.slot_policy or PROMO_POLICY_GUARANTEED_ANY_POSITION),
            count=int(sess.count or 0),
            ends_at=sess.ends_at,
            is_editorial=bool(sess.is_editorial),
            sponsorship_disclosure=sess.sponsorship_disclosure,
            title_override=sess.title_override,
            priority=normalize_promo_priority(
                1 if user.is_superadmin else PROMO_DEFAULT_PRIORITY
            ),
        )
        result = await create_partner_event_promo_campaign(db, spec)
    partner_promo_sessions.pop(int(callback.from_user.id), None)
    if result.status != "created" or result.campaign is None:
        await callback.answer(result.message[:200], show_alert=True)
        return
    if is_add_activity:
        text = (
            f"✅ Активность добавлена к #{result.campaign.id}.\n"
            f"{html.escape(result.message)}"
        )
    else:
        text = (
            f"✅ Кампания #{result.campaign.id} активна.\n"
            f"{html.escape(result.message)}"
        )
    rows = [
        [
            types.InlineKeyboardButton(
                text="📊 Открыть карточку",
                callback_data=f"ppromo:view:{result.campaign.id}",
            )
        ],
        [
            types.InlineKeyboardButton(
                text="✕ Закрыть",
                callback_data=f"ppromo:cancel:{event_id}",
            )
        ],
    ]
    await _send_or_edit(
        bot,
        callback,
        text,
        types.InlineKeyboardMarkup(inline_keyboard=rows),
    )
    await callback.answer("Готово")


async def _handle_back(
    callback: types.CallbackQuery, db: Database, bot: Bot, *, event_id: int, current_step: int
) -> None:
    sess = partner_promo_sessions.get(int(callback.from_user.id))
    if sess is None or sess.event_id != int(event_id):
        await callback.answer("Сессия истекла.", show_alert=True)
        return
    event, user = await _load_event_and_user(
        db, event_id=event_id, user_id=callback.from_user.id
    )
    if not _is_authorized(user, event):
        await callback.answer("Not authorized", show_alert=True)
        return
    target = max(1, current_step - 1)
    sess.step = target
    await callback.answer()
    if target == 1:
        await render_step1_surface(callback, db, bot, event=event, user=user)
    elif target == 2:
        await render_step2_slot(callback, bot, event=event, session=sess)
    elif target == 3:
        await render_step3_count(callback, bot, event=event)
    elif target == 4:
        await render_step4_end(callback, bot, event=event)
    elif target == 5:
        await render_step5_mode(callback, bot, event=event)


async def _handle_cancel(
    callback: types.CallbackQuery, db: Database, bot: Bot, *, event_id: int
) -> None:
    partner_promo_sessions.pop(int(callback.from_user.id), None)
    partner_promo_input_sessions.pop(int(callback.from_user.id), None)
    await callback.answer("Отменено")
    try:
        if callback.message is not None:
            await callback.message.delete()
    except Exception:
        logger.debug("partner_promo: cancel delete failed", exc_info=True)


async def _handle_site_alert(callback: types.CallbackQuery) -> None:
    await callback.answer(
        "Размещение на сайте появится в следующих релизах.",
        show_alert=True,
    )


async def _handle_input_request(
    callback: types.CallbackQuery, *, event_id: int, field: str
) -> None:
    sess = partner_promo_sessions.get(int(callback.from_user.id))
    if sess is None or sess.event_id != int(event_id):
        await callback.answer("Сессия истекла.", show_alert=True)
        return
    partner_promo_input_sessions[int(callback.from_user.id)] = PartnerPromoInputSession(
        event_id=int(event_id), field=field
    )
    prompts = {
        PARTNER_PROMO_INPUT_COUNT: "Пришлите количество показов сообщением (целое число > 0).",
        PARTNER_PROMO_INPUT_END_DATE: "Пришлите дату окончания (YYYY-MM-DD или ДД.ММ.ГГГГ).",
        PARTNER_PROMO_INPUT_DISCLOSURE: "Пришлите текст раскрытия (или . чтобы оставить «Партнёрский материал»).",
        PARTNER_PROMO_INPUT_RENAME: "Пришлите новое название кампании.",
    }
    await callback.answer(prompts.get(field, "Пришлите значение"), show_alert=True)


async def handle_partner_promo_callback(
    callback: types.CallbackQuery, db: Database, bot: Bot
) -> bool:
    """Return True if the callback was consumed by this handler."""

    data = callback.data or ""
    if not data.startswith("ppromo:"):
        return False
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    try:
        if action == "start" and len(parts) >= 3:
            await _begin_flow(callback, db, bot, event_id=int(parts[2]))
        elif action == "new" and len(parts) >= 3:
            await _open_new_form(callback, db, bot, event_id=int(parts[2]))
        elif action == "surface" and len(parts) >= 5:
            await _handle_surface(
                callback,
                db,
                bot,
                event_id=int(parts[2]),
                surface=parts[3],
                profile=parts[4],
            )
        elif action == "slot" and len(parts) >= 4:
            await _handle_slot(
                callback, db, bot, event_id=int(parts[2]), policy=parts[3]
            )
        elif action == "count" and len(parts) >= 4:
            await _handle_count(
                callback, db, bot, event_id=int(parts[2]), n=int(parts[3])
            )
        elif action == "count_input" and len(parts) >= 3:
            await _handle_input_request(
                callback, event_id=int(parts[2]), field=PARTNER_PROMO_INPUT_COUNT
            )
        elif action == "end" and len(parts) >= 4:
            await _handle_end_choice(
                callback, db, bot, event_id=int(parts[2]), choice=parts[3]
            )
        elif action == "end_input" and len(parts) >= 3:
            await _handle_input_request(
                callback, event_id=int(parts[2]), field=PARTNER_PROMO_INPUT_END_DATE
            )
        elif action == "mode" and len(parts) >= 4:
            await _handle_mode(
                callback, db, bot, event_id=int(parts[2]), mode=parts[3]
            )
        elif action == "confirm" and len(parts) >= 3:
            await _handle_confirm(callback, db, bot, event_id=int(parts[2]))
        elif action == "back" and len(parts) >= 4:
            await _handle_back(
                callback,
                db,
                bot,
                event_id=int(parts[2]),
                current_step=int(parts[3]),
            )
        elif action == "cancel" and len(parts) >= 3:
            await _handle_cancel(callback, db, bot, event_id=int(parts[2]))
        elif action == "site" and len(parts) >= 3:
            await _handle_site_alert(callback)
        elif action == "rename_input" and len(parts) >= 3:
            await _handle_input_request(
                callback, event_id=int(parts[2]), field=PARTNER_PROMO_INPUT_RENAME
            )
        else:
            await callback.answer("Неизвестное действие", show_alert=True)
    except Exception:
        logger.exception("partner_promo: callback handling failed data=%r", data)
        try:
            await callback.answer("Ошибка обработки", show_alert=True)
        except Exception:
            pass
    return True


async def handle_partner_promo_reply(
    message: types.Message, db: Database, bot: Bot
) -> bool:
    """Consume free-text replies for count/date/disclosure/rename inputs."""

    user_id = int(message.from_user.id) if message.from_user else 0
    pending = partner_promo_input_sessions.get(user_id)
    if pending is None:
        return False
    sess = partner_promo_sessions.get(user_id)
    if sess is None or sess.event_id != pending.event_id:
        partner_promo_input_sessions.pop(user_id, None)
        return False
    raw = (message.text or "").strip()
    if not raw:
        await bot.send_message(message.chat.id, "Пустой ответ. Попробуйте ещё раз.")
        return True
    if pending.field == PARTNER_PROMO_INPUT_COUNT:
        try:
            n = int(raw)
        except ValueError:
            await bot.send_message(message.chat.id, "Нужно число.")
            return True
        if n <= 0 or n > 365:
            await bot.send_message(message.chat.id, "Допускаются числа 1..365.")
            return True
        sess.count = n
        # In add-activity mode jump straight to confirm (period/mode are
        # inherited from the campaign).
        sess.step = 6 if sess.add_to_campaign_id is not None else 4
    elif pending.field == PARTNER_PROMO_INPUT_END_DATE:
        parsed = _parse_date_input(raw)
        if parsed is None:
            await bot.send_message(message.chat.id, "Дата не распознана.")
            return True
        event, _ = await _load_event_and_user(
            db, event_id=sess.event_id, user_id=user_id
        )
        if event is None:
            await bot.send_message(message.chat.id, "Событие не найдено.")
            partner_promo_input_sessions.pop(user_id, None)
            return True
        today = datetime.now(timezone.utc).date()
        clamped = clamp_campaign_end_to_event(parsed, event)
        if clamped < today:
            await bot.send_message(
                message.chat.id,
                "После клампа на дату события дата окончания в прошлом. Введите другую дату.",
            )
            return True
        sess.ends_at = clamped
        sess.step = 5
    elif pending.field == PARTNER_PROMO_INPUT_DISCLOSURE:
        sess.sponsorship_disclosure = (
            raw if raw != "." else (sess.sponsorship_disclosure or "Партнёрский материал")
        )
    elif pending.field == PARTNER_PROMO_INPUT_RENAME:
        sess.title_override = raw[:120]
    else:
        partner_promo_input_sessions.pop(user_id, None)
        return False

    partner_promo_input_sessions.pop(user_id, None)
    event, user = await _load_event_and_user(
        db, event_id=sess.event_id, user_id=user_id
    )
    if event is None or user is None:
        await bot.send_message(message.chat.id, "Событие не найдено.")
        return True

    fake_callback = _DummyCallback(message=message, from_user=message.from_user)
    if sess.step == 4:
        await render_step4_end(fake_callback, bot, event=event)
    elif sess.step == 5:
        await render_step5_mode(fake_callback, bot, event=event)
    else:
        await render_step6_confirm(
            fake_callback, db, bot, event=event, user=user, session=sess
        )
    return True


class _DummyCallback:
    """Minimal stand-in so render_step* helpers can be reused after a reply.

    ``_send_or_edit`` checks ``callback.message``; with this stub it falls
    through to ``bot.send_message`` which is the correct behaviour for a
    fresh reply.
    """

    def __init__(self, *, message: types.Message, from_user: types.User | None) -> None:
        self.message = None  # force send_message path
        self.from_user = from_user
        self._reply_target = message

    async def answer(self, *args, **kwargs) -> None:  # noqa: D401
        return None


# ---------------------------------------------------------------------------
# Promo management menu (button-rich UI shared by admin and partner)
# ---------------------------------------------------------------------------

from sqlalchemy import func
from models import PromoActivity, PromoExposure


_SURFACE_LABELS: dict[str, str] = {
    "hero_talk": "Hero-talk",
    "video_general": "🎬 Видеоанонс",
    "video_slot": "🎬 Видеоанонс (слот)",
    "daily_highlight": "📅 Ежедневная подборка",
    "telegraph_month": "📰 Telegraph: месяц",
    "telegraph_weekend": "📰 Telegraph: выходные",
    "vk_publication": "📢 VK-публикация",
    "vk_channel_publish": "📣 VK-канал",
    "vk_repost": "📨 VK-репост",
    "vk_story": "VK-история",
    "placeholder": "—",
}

_PROFILE_LABELS: dict[str, str] = {
    "popular_review": "Популярное",
    "default": "Завтра",
    "konb": "КОНБ",
    "cherryflash_libsvtav1": "CherryFlash (тех.)",
}

_SLOT_POLICY_LABELS: dict[str, str] = {
    "guaranteed_any_position": "любая позиция",
    "first_two_slots": "слот 1–2",
    "first_slot": "только слот 1",
    "diverse_shuffle": "органическая ротация",
    "least_recent": "ротация по давности",
    "fixed_event": "фиксированное событие",
}


def _humanize_activity(activity: PromoActivity) -> str:
    """Render a PromoActivity row in partner-friendly Russian.

    Technical keys (``video_general``, ``popular_review``, ``first_two_slots``)
    are not shown — the operator reading the card might not be on the
    engineering team. Falls back to the raw key when no translation exists.
    """

    surface = _SURFACE_LABELS.get(activity.surface, activity.surface or "—")
    bits: list[str] = [surface]
    cfg = activity.config_json if isinstance(activity.config_json, dict) else {}
    if activity.surface == "hero_talk":
        labels = {"home_hero": "верх страницы", "page_end": "конец страницы"}
        for placement, label in labels.items():
            bits.append(f"{label}: {'вкл' if cfg.get('placements', {}).get(placement) else 'выкл'}")
        bits.append("единица: квалифицированная видимость")
        return " · ".join(bits)
    if activity.surface == PROMO_SURFACE_VK_PUBLICATION:
        # VK-публикация: слотовая политика не применима; показываем целевой паблик.
        group = str(cfg.get("target_group") or activity.profile_key or "").strip()
        if group:
            bits.append(f"vk.com/{group}")
        window = int(cfg.get("window_hours") or PROMO_VK_DEFAULT_WINDOW_HOURS)
        bits.append(f"минимум {int(activity.daily_cap or activity.max_per_publish or 1)}/{window}ч")
    elif activity.surface == PROMO_SURFACE_VK_CHANNEL_PUBLISH:
        channel = str(cfg.get("target_channel") or activity.profile_key or "").strip()
        if channel:
            bits.append(channel)
        peer_hint = str(cfg.get("peer_id_env") or cfg.get("peer_ids_env") or "").strip()
        if peer_hint:
            bits.append(peer_hint)
        window = int(cfg.get("window_hours") or PROMO_VK_DEFAULT_WINDOW_HOURS)
        bits.append(f"сообщений {int(activity.daily_cap or activity.max_per_publish or 1)}/{window}ч")
    elif activity.surface == PROMO_SURFACE_VK_REPOST:
        # VK-репост: показываем направление source → target.
        source = str(cfg.get("source_group") or "").strip()
        target = str(cfg.get("target_group") or "").strip()
        if source and target:
            bits.append(f"vk.com/{source} → vk.com/{target}")
        elif activity.profile_key:
            bits.append(str(activity.profile_key))
    elif activity.surface == PROMO_SURFACE_VK_STORY:
        source = str(cfg.get("source_group") or "").strip()
        target = str(cfg.get("target_group") or activity.profile_key or "").strip()
        if source and target:
            bits.append(f"vk.com/{source} → story vk.com/{target}")
        elif target:
            bits.append(f"story vk.com/{target}")
        window = int(cfg.get("window_hours") or PROMO_VK_DEFAULT_WINDOW_HOURS)
        bits.append(f"историй {int(activity.daily_cap or activity.max_per_publish or 1)}/{window}ч")
    else:
        if activity.profile_key:
            bits.append(_PROFILE_LABELS.get(activity.profile_key, activity.profile_key))
        if activity.surface == "video_general":
            policy_label = _SLOT_POLICY_LABELS.get(
                activity.selection_policy or "", activity.selection_policy or ""
            )
            if policy_label:
                bits.append(policy_label)
    if activity.daily_cap and activity.surface not in (
        PROMO_SURFACE_VK_PUBLICATION,
        PROMO_SURFACE_VK_CHANNEL_PUBLISH,
        PROMO_SURFACE_VK_REPOST,
        PROMO_SURFACE_VK_STORY,
    ):
        bits.append(f"не более {int(activity.daily_cap)} в день")
    if activity.target_exposure_goal:
        bits.append(f"всего показов: {int(activity.target_exposure_goal)}")
    if not activity.enabled:
        bits.append("выключена")
    return " · ".join(bits)


def _is_role_authorized_for_menu(user: User | None) -> bool:
    if user is None or user.blocked:
        return False
    return bool(user.is_superadmin or user.is_partner)


async def _load_user(db: Database, user_id: int) -> User | None:
    async with db.get_session() as session:
        return await session.get(User, int(user_id))


async def _list_campaigns_for_role(
    db: Database, *, user: User, include_archived: bool
) -> list[PromoCampaign]:
    now_utc = datetime.now(timezone.utc)
    async with db.get_session() as session:
        stmt = select(PromoCampaign).order_by(
            PromoCampaign.status, PromoCampaign.created_at.desc()
        )
        if not include_archived:
            stmt = stmt.where(PromoCampaign.status != "archived").where(
                _current_or_future_campaign_filter(now_utc)
            )
        if not user.is_superadmin:
            stmt = stmt.where(PromoCampaign.created_by == int(user.user_id))
        res = await session.execute(stmt)
        return list(res.scalars().all())


def _truncate(text: str, limit: int) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def _menu_text(user: User, campaigns: list[PromoCampaign], *, archived: bool) -> str:
    role = "суперадмин" if user.is_superadmin else "партнёр"
    header = f"<b>Промо-кампании</b> ({role})"
    if archived:
        header += " — архив"
    if not campaigns:
        body = "Кампаний нет."
    else:
        lines = []
        for c in campaigns[:15]:
            ends = c.ends_at.date().isoformat() if c.ends_at else "—"
            goal = (
                f" · {c.total_exposure_goal}" if c.total_exposure_goal is not None else ""
            )
            lines.append(
                f"#{c.id} {html.escape(_truncate(c.title, 50))} · "
                f"{_status_label(c.status)} · до {ends}{goal}"
            )
        body = "\n".join(lines)
        if len(campaigns) > 15:
            body += f"\n… ещё {len(campaigns) - 15}"
    return header + "\n\n" + body


def _menu_keyboard(
    campaigns: list[PromoCampaign], *, archived: bool, is_superadmin: bool
) -> types.InlineKeyboardMarkup:
    rows: list[list[types.InlineKeyboardButton]] = []
    for c in campaigns[:10]:
        label = f"#{c.id} {_truncate(c.title, 36)}"
        rows.append(
            [
                types.InlineKeyboardButton(
                    text=label, callback_data=f"ppromo:view:{c.id}"
                )
            ]
        )
    nav: list[types.InlineKeyboardButton] = []
    if archived:
        nav.append(
            types.InlineKeyboardButton(
                text="▣ К активным", callback_data="ppromo:menu:active"
            )
        )
    else:
        nav.append(
            types.InlineKeyboardButton(
                text="📦 Архив", callback_data="ppromo:menu:archived"
            )
        )
    if is_superadmin:
        nav.append(
            types.InlineKeyboardButton(
                text="🌟 Seed 80", callback_data="ppromo:menu:seed80"
            )
        )
    nav.append(
        types.InlineKeyboardButton(text="✕ Закрыть", callback_data="ppromo:close")
    )
    rows.append(nav)
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_menu(
    bot: Bot,
    callback: types.CallbackQuery | None,
    chat_id: int,
    *,
    db: Database,
    user: User,
    archived: bool,
) -> None:
    campaigns = await _list_campaigns_for_role(
        db, user=user, include_archived=archived
    )
    if archived:
        campaigns = [c for c in campaigns if c.status == "archived"]
    text = _menu_text(user, campaigns, archived=archived)
    markup = _menu_keyboard(
        campaigns, archived=archived, is_superadmin=bool(user.is_superadmin)
    )
    if callback is not None and callback.message is not None:
        try:
            await callback.message.edit_text(
                text, reply_markup=markup, parse_mode="HTML"
            )
            return
        except Exception:
            logger.debug("partner_promo: menu edit_text failed", exc_info=True)
    await bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")


async def _load_campaign_with_visibility(
    db: Database, *, user: User, campaign_id: int
) -> PromoCampaign | None:
    async with db.get_session() as session:
        campaign = await session.get(PromoCampaign, int(campaign_id))
    if campaign is None:
        return None
    if user.is_superadmin:
        return campaign
    if int(campaign.created_by or 0) == int(user.user_id):
        return campaign
    return None


async def _campaign_card_text(db: Database, campaign: PromoCampaign) -> str:
    async with db.get_session() as session:
        target_res = await session.execute(
            select(PromoTarget).where(PromoTarget.campaign_id == campaign.id)
        )
        targets = list(target_res.scalars().all())
        act_res = await session.execute(
            select(PromoActivity).where(PromoActivity.campaign_id == campaign.id)
        )
        activities = list(act_res.scalars().all())
        exposure_res = await session.execute(
            select(func.count())
            .select_from(PromoExposure)
            .where(PromoExposure.campaign_id == campaign.id)
            .where(PromoExposure.public_target_count > 0)
        )
        recorded = int(exposure_res.scalar() or 0)

    target_lines: list[str] = []
    for t in targets:
        if t.target_type == "event" and t.event_id:
            target_lines.append(f"Событие #{t.event_id}: {html.escape(t.query_text or '')}")
        elif t.target_type == "festival":
            target_lines.append(f"Фестиваль: {html.escape(t.festival_name or '')}")
        elif t.target_type == "tg_chat_author":
            chat, _, author = str(t.query_text or "").partition(":")
            chat = chat.strip().lstrip("@")
            author = author.strip().lstrip("@")
            target_lines.append(
                f"Чат: t.me/{html.escape(chat)} · автор @{html.escape(author)}"
            )

    act_lines: list[str] = [
        "• " + _humanize_activity(a) for a in activities
    ]

    ends = campaign.ends_at.date().isoformat() if campaign.ends_at else "—"
    disclosure = campaign.sponsorship_disclosure or "редакционно (без подписи)"
    progress = (
        f"{recorded}/{campaign.total_exposure_goal}"
        if campaign.total_exposure_goal is not None
        else f"{recorded}"
    )
    lines = [
        f"<b>#{campaign.id} {html.escape(campaign.title)}</b>",
        f"Статус: {_status_label(campaign.status)} · приоритет {campaign.priority}",
        f"Период: до {ends}",
        f"Цель показов: {progress}",
        f"Раскрытие: {html.escape(disclosure)}",
        "",
        "Цели:",
        *(target_lines or ["—"]),
        "",
        "Активности:",
        *(act_lines or ["—"]),
    ]
    return "\n".join(lines)


def _campaign_card_keyboard(
    campaign: PromoCampaign,
    *,
    is_superadmin: bool,
    activities: list[PromoActivity] | None = None,
    is_initial_80: bool = False,
) -> types.InlineKeyboardMarkup:
    cid = int(campaign.id or 0)
    rows: list[list[types.InlineKeyboardButton]] = []
    status = campaign.status
    primary: list[types.InlineKeyboardButton] = []
    if status == "active":
        primary.append(
            types.InlineKeyboardButton(
                text="⏸ Пауза", callback_data=f"ppromo:pause:{cid}"
            )
        )
    elif status == "paused":
        primary.append(
            types.InlineKeyboardButton(
                text="▶ Запустить", callback_data=f"ppromo:resume:{cid}"
            )
        )
    elif status == "draft":
        primary.append(
            types.InlineKeyboardButton(
                text="▶ Запустить", callback_data=f"ppromo:resume:{cid}"
            )
        )
    if status != "archived":
        primary.append(
            types.InlineKeyboardButton(
                text="📦 Архив", callback_data=f"ppromo:archive:{cid}"
            )
        )
    else:
        primary.append(
            types.InlineKeyboardButton(
                text="🔄 Восстановить", callback_data=f"ppromo:resume:{cid}"
            )
        )
    rows.append(primary)
    rows.append(
        [
            types.InlineKeyboardButton(
                text="📊 Статистика", callback_data=f"ppromo:stats:{cid}"
            ),
            types.InlineKeyboardButton(
                text="✏ Переименовать", callback_data=f"ppromo:cren:{cid}"
            ),
        ]
    )
    if status != "archived":
        rows.append(
            [
                types.InlineKeyboardButton(
                    text="➕ Активность",
                    callback_data=f"ppromo:addact:{cid}",
                )
            ]
        )
        activity_rows: list[types.InlineKeyboardButton] = []
        channel_seen = False
        for activity in activities or []:
            if activity.id is None:
                continue
            if activity.surface != PROMO_SURFACE_VK_CHANNEL_PUBLISH:
                continue
            channel_seen = True
            activity_rows.append(
                types.InlineKeyboardButton(
                    text=("➖ VK-канал" if activity.enabled else "➕ VK-канал"),
                    callback_data=f"ppromo:acttoggle:{cid}:{int(activity.id)}",
                )
            )
        if is_initial_80 and not channel_seen:
            activity_rows.append(
                types.InlineKeyboardButton(
                    text="➕ VK-канал",
                    callback_data=f"ppromo:add80vkchan:{cid}",
                )
            )
        for idx in range(0, len(activity_rows), 2):
            rows.append(activity_rows[idx : idx + 2])
    if is_superadmin:
        rows.append(
            [
                types.InlineKeyboardButton(
                    text=f"P{p}",
                    callback_data=f"ppromo:prio:{cid}:{p}",
                )
                for p in range(0, 4)
            ]
        )
    rows.append(
        [
            types.InlineKeyboardButton(
                text="◀ К списку", callback_data="ppromo:menu:active"
            )
        ]
    )
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_campaign_card(
    bot: Bot,
    callback: types.CallbackQuery | None,
    chat_id: int,
    *,
    db: Database,
    user: User,
    campaign: PromoCampaign,
) -> None:
    text = await _campaign_card_text(db, campaign)
    async with db.get_session() as session:
        activities = list(
            (
                await session.execute(
                    select(PromoActivity).where(PromoActivity.campaign_id == campaign.id)
                )
            ).scalars().all()
        )
        target = (
            await session.execute(
                select(PromoTarget).where(PromoTarget.campaign_id == campaign.id)
            )
        ).scalars().first()
    is_initial_80 = (
        str(campaign.title or "") == "80 историй о главном / summer visibility"
        or (
            target is not None
            and target.target_type == "festival"
            and str(target.festival_name or "") == "80 историй о главном"
        )
    )
    markup = _campaign_card_keyboard(
        campaign,
        is_superadmin=bool(user.is_superadmin),
        activities=activities,
        is_initial_80=is_initial_80,
    )
    if callback is not None and callback.message is not None:
        try:
            await callback.message.edit_text(
                text, reply_markup=markup, parse_mode="HTML"
            )
            return
        except Exception:
            logger.debug("partner_promo: card edit_text failed", exc_info=True)
    await bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")


async def _change_campaign_status(
    db: Database,
    *,
    user: User,
    campaign_id: int,
    new_status: str,
) -> tuple[bool, str]:
    campaign = await _load_campaign_with_visibility(db, user=user, campaign_id=campaign_id)
    if campaign is None:
        return False, "Кампания недоступна."
    async with db.get_session() as session:
        c = await session.get(PromoCampaign, int(campaign_id))
        if c is None:
            return False, "Кампания не найдена."
        if not user.is_superadmin and int(c.created_by or 0) != int(user.user_id):
            return False, "Not authorized"
        c.status = new_status
        c.updated_at = datetime.now(timezone.utc)
        if new_status == "archived":
            c.archived_at = datetime.now(timezone.utc)
        session.add(c)
        await session.commit()
    return True, f"Готово: {_status_label(new_status)}"


async def _change_campaign_priority(
    db: Database,
    *,
    user: User,
    campaign_id: int,
    priority: int,
) -> tuple[bool, str]:
    if not user.is_superadmin:
        return False, "Приоритет меняет только суперадмин."
    async with db.get_session() as session:
        c = await session.get(PromoCampaign, int(campaign_id))
        if c is None:
            return False, "Кампания не найдена."
        c.priority = normalize_promo_priority(priority)
        c.updated_at = datetime.now(timezone.utc)
        session.add(c)
        await session.commit()
    return True, f"P{priority}"


# Statuses that count as a real public action. ``VK_SCHEDULED`` is included
# because promo VK posts land in the community postponed queue, not as an
# immediate ``wall.post`` — excluding it would under-count VK activities.
_STATS_PUBLISHED_STATUSES = (
    "PUBLISHED_MAIN",
    "PUBLISHED_TEST",
    "VK_SCHEDULED",
    "VK_CHANNEL_SENT",
)
_STATUS_RU = {
    "PUBLISHED_MAIN": "опубликовано",
    "PUBLISHED_TEST": "тест",
    "VK_SCHEDULED": "в отложке",
    "VK_CHANNEL_SENT": "отправлено в VK-канал",
}


def _exposure_url(exposure: PromoExposure) -> str:
    details = exposure.details_json if isinstance(exposure.details_json, dict) else {}
    url = str(details.get("target_url") or "").strip()
    if not url:
        targets = (
            exposure.public_targets_json
            if isinstance(exposure.public_targets_json, list)
            else []
        )
        if targets and isinstance(targets[0], dict):
            url = str(targets[0].get("url") or "").strip()
    return url


def _link_html(url: str) -> str:
    if not url:
        return "ссылка готовится"
    short = url.replace("https://", "").replace("http://", "")
    return f'<a href="{html.escape(url, quote=True)}">{html.escape(short)}</a>'


def _stats_when(exposure: PromoExposure) -> str:
    if exposure.published_at is None:
        return "—"
    return exposure.published_at.astimezone(timezone.utc).strftime("%d.%m %H:%M")


def _stats_exposure_line(exposure: PromoExposure, title: str, *, is_vk: bool) -> str:
    when = _stats_when(exposure)
    status_ru = _STATUS_RU.get(exposure.publish_status, exposure.publish_status or "")
    title_s = html.escape((title or "")[:40])
    ev = int(exposure.event_id or 0)
    if is_vk:
        url = _exposure_url(exposure)
        details = exposure.details_json if isinstance(exposure.details_json, dict) else {}
        source_url = str(details.get("source_url") or "").strip()
        tail = f" ← {_link_html(source_url)}" if source_url else ""
        return (
            f"  • {when} · ev#{ev} «{title_s}» · "
            f"{html.escape(status_ru)} · {_link_html(url)}{tail}"
        )
    return (
        f"  • {when} · ev#{ev} «{title_s}» · "
        f"поз. {int(exposure.position or 0)} · {html.escape(status_ru)}"
    )


async def _campaign_stats_text(db: Database, campaign: PromoCampaign) -> str:
    async with db.get_session() as session:
        activities = list(
            (
                await session.execute(
                    select(PromoActivity)
                    .where(PromoActivity.campaign_id == campaign.id)
                    .order_by(PromoActivity.id)
                )
            ).scalars().all()
        )
        totals_res = await session.execute(
            select(PromoExposure.activity_id, func.count(PromoExposure.id))
            .where(PromoExposure.campaign_id == campaign.id)
            .where(PromoExposure.publish_status.in_(_STATS_PUBLISHED_STATUSES))
            .group_by(PromoExposure.activity_id)
        )
        totals = {aid: int(cnt or 0) for aid, cnt in totals_res.all()}
        recent_rows = list(
            (
                await session.execute(
                    select(PromoExposure, Event.title)
                    .join(Event, Event.id == PromoExposure.event_id)
                    .where(PromoExposure.campaign_id == campaign.id)
                    .order_by(
                        PromoExposure.published_at.desc(), PromoExposure.id.desc()
                    )
                    .limit(200)
                )
            ).all()
        )

    by_activity: dict[Optional[int], list[tuple[PromoExposure, str]]] = {}
    for exposure, title in recent_rows:
        by_activity.setdefault(exposure.activity_id, []).append((exposure, title or ""))

    now_utc = datetime.now(timezone.utc)
    current_ids = {int(a.id) for a in activities if a.id is not None}
    lines = [f"<b>📊 Статистика #{campaign.id}</b>", ""]

    if not activities:
        lines.append("Активностей пока нет.")
        return "\n".join(lines)

    for activity in activities:
        aid = int(activity.id) if activity.id is not None else None
        total = totals.get(aid, 0)
        bucket = by_activity.get(aid, [])
        is_vk = activity.surface in (
            PROMO_SURFACE_VK_PUBLICATION,
            PROMO_SURFACE_VK_CHANNEL_PUBLISH,
            PROMO_SURFACE_VK_REPOST,
            PROMO_SURFACE_VK_STORY,
        )
        lines.append(f"<b>{html.escape(_humanize_activity(activity))}</b>")
        if is_vk:
            cfg = activity.config_json if isinstance(activity.config_json, dict) else {}
            window = int(cfg.get("window_hours") or PROMO_VK_DEFAULT_WINDOW_HOURS)
            since = now_utc - timedelta(hours=window)
            in_window = sum(
                1
                for exp, _t in bucket
                if exp.publish_status in _STATS_PUBLISHED_STATUSES
                and exp.published_at is not None
                and exp.published_at.astimezone(timezone.utc) >= since
            )
            target_n = int(activity.daily_cap or activity.max_per_publish or 1)
            lines.append(
                f"  промо-действий за {window}ч: {in_window} / цель {target_n}; "
                f"всего: {total}"
            )
        else:
            lines.append(f"  всего показов: {total}")
        shown = 0
        for exposure, title in bucket:
            if shown >= 5:
                break
            shown += 1
            lines.append(_stats_exposure_line(exposure, title, is_vk=is_vk))
        if shown == 0:
            lines.append("  — показов пока нет")
        lines.append("")

    # Exposures not linked to a current activity (legacy / unlinked rows).
    leftover_total = sum(cnt for aid, cnt in totals.items() if aid not in current_ids)
    leftover_recent = [
        (exp, title)
        for aid, items in by_activity.items()
        if aid not in current_ids
        for exp, title in items
    ]
    if leftover_total or leftover_recent:
        lines.append("<b>Прочее (без привязки к активности)</b>")
        lines.append(f"  всего показов: {leftover_total}")
        leftover_recent.sort(
            key=lambda it: it[0].published_at or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        for exposure, title in leftover_recent[:5]:
            is_vk = exposure.surface in (
                PROMO_SURFACE_VK_PUBLICATION,
                PROMO_SURFACE_VK_CHANNEL_PUBLISH,
                PROMO_SURFACE_VK_REPOST,
                PROMO_SURFACE_VK_STORY,
            )
            lines.append(_stats_exposure_line(exposure, title, is_vk=is_vk))

    return "\n".join(lines).rstrip()


async def _render_campaign_stats(
    bot: Bot,
    callback: types.CallbackQuery | None,
    chat_id: int,
    *,
    db: Database,
    campaign: PromoCampaign,
) -> None:
    text = await _campaign_stats_text(db, campaign)
    rows = [
        [
            types.InlineKeyboardButton(
                text="◀ Назад к карточке",
                callback_data=f"ppromo:view:{campaign.id}",
            )
        ]
    ]
    markup = types.InlineKeyboardMarkup(inline_keyboard=rows)
    if callback is not None and callback.message is not None:
        try:
            await callback.message.edit_text(
                text, reply_markup=markup, parse_mode="HTML"
            )
            return
        except Exception:
            logger.debug("partner_promo: stats edit_text failed", exc_info=True)
    await bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")


# ---- public entry points ----------------------------------------------------


async def handle_promo_menu_command(
    message: types.Message, db: Database, bot: Bot
) -> None:
    """Entry point for ``/promo`` (no-args). Shared by admin and partner."""

    user = await _load_user(db, int(message.from_user.id))
    if not _is_role_authorized_for_menu(user):
        await bot.send_message(message.chat.id, "Not authorized")
        return
    await _render_menu(
        bot, None, message.chat.id, db=db, user=user, archived=False
    )


async def _handle_menu_action(
    callback: types.CallbackQuery, db: Database, bot: Bot, *, action: str
) -> None:
    user = await _load_user(db, int(callback.from_user.id))
    if not _is_role_authorized_for_menu(user):
        await callback.answer("Not authorized", show_alert=True)
        return
    if action == "active":
        await callback.answer()
        await _render_menu(
            bot, callback, callback.message.chat.id, db=db, user=user, archived=False
        )
    elif action == "archived":
        await callback.answer()
        await _render_menu(
            bot, callback, callback.message.chat.id, db=db, user=user, archived=True
        )
    elif action == "seed80":
        if not user.is_superadmin:
            await callback.answer("Только суперадмин", show_alert=True)
            return
        from promo import ensure_initial_80_stories_campaign  # local import

        campaign = await ensure_initial_80_stories_campaign(db)
        if campaign is None:
            await callback.answer(
                "Не создал: фестиваль 80 историй без будущих событий.",
                show_alert=True,
            )
        else:
            await callback.answer(f"Готово: #{campaign.id}")
        await _render_menu(
            bot, callback, callback.message.chat.id, db=db, user=user, archived=False
        )
    else:
        await callback.answer("Неизвестное действие", show_alert=True)


async def _handle_view_campaign(
    callback: types.CallbackQuery, db: Database, bot: Bot, *, campaign_id: int
) -> None:
    user = await _load_user(db, int(callback.from_user.id))
    if not _is_role_authorized_for_menu(user):
        await callback.answer("Not authorized", show_alert=True)
        return
    campaign = await _load_campaign_with_visibility(
        db, user=user, campaign_id=campaign_id
    )
    if campaign is None:
        await callback.answer("Кампания недоступна", show_alert=True)
        return
    await callback.answer()
    await _render_campaign_card(
        bot, callback, callback.message.chat.id, db=db, user=user, campaign=campaign
    )


async def _handle_campaign_status_change(
    callback: types.CallbackQuery,
    db: Database,
    bot: Bot,
    *,
    campaign_id: int,
    new_status: str,
) -> None:
    user = await _load_user(db, int(callback.from_user.id))
    if not _is_role_authorized_for_menu(user):
        await callback.answer("Not authorized", show_alert=True)
        return
    ok, msg = await _change_campaign_status(
        db, user=user, campaign_id=campaign_id, new_status=new_status
    )
    if not ok:
        await callback.answer(msg, show_alert=True)
        return
    campaign = await _load_campaign_with_visibility(
        db, user=user, campaign_id=campaign_id
    )
    if campaign is None:
        await callback.answer(msg)
        await _render_menu(
            bot, callback, callback.message.chat.id, db=db, user=user, archived=False
        )
        return
    await callback.answer(msg)
    await _render_campaign_card(
        bot, callback, callback.message.chat.id, db=db, user=user, campaign=campaign
    )


async def _handle_campaign_priority(
    callback: types.CallbackQuery,
    db: Database,
    bot: Bot,
    *,
    campaign_id: int,
    priority: int,
) -> None:
    user = await _load_user(db, int(callback.from_user.id))
    if not _is_role_authorized_for_menu(user):
        await callback.answer("Not authorized", show_alert=True)
        return
    ok, msg = await _change_campaign_priority(
        db, user=user, campaign_id=campaign_id, priority=priority
    )
    if not ok:
        await callback.answer(msg, show_alert=True)
        return
    campaign = await _load_campaign_with_visibility(
        db, user=user, campaign_id=campaign_id
    )
    if campaign is None:
        await callback.answer(msg)
        return
    await callback.answer(msg)
    await _render_campaign_card(
        bot, callback, callback.message.chat.id, db=db, user=user, campaign=campaign
    )


async def _handle_activity_toggle(
    callback: types.CallbackQuery,
    db: Database,
    bot: Bot,
    *,
    campaign_id: int,
    activity_id: int,
) -> None:
    user = await _load_user(db, int(callback.from_user.id))
    if not _is_role_authorized_for_menu(user):
        await callback.answer("Not authorized", show_alert=True)
        return
    campaign = await _load_campaign_with_visibility(db, user=user, campaign_id=campaign_id)
    if campaign is None:
        await callback.answer("Кампания недоступна", show_alert=True)
        return
    async with db.get_session() as session:
        activity = await session.get(PromoActivity, int(activity_id))
        if activity is None or int(activity.campaign_id) != int(campaign_id):
            await callback.answer("Активность не найдена", show_alert=True)
            return
        new_enabled = not bool(activity.enabled)
        activity.enabled = new_enabled
        campaign_obj = await session.get(PromoCampaign, int(campaign_id))
        if campaign_obj is not None:
            campaign_obj.updated_at = datetime.now(timezone.utc)
            session.add(campaign_obj)
        session.add(activity)
        await session.commit()
    fresh = await _load_campaign_with_visibility(db, user=user, campaign_id=campaign_id)
    await callback.answer("Активность включена" if new_enabled else "Активность выключена")
    if fresh is not None:
        await _render_campaign_card(bot, callback, callback.message.chat.id, db=db, user=user, campaign=fresh)


async def _handle_add_initial_80_vk_channel(
    callback: types.CallbackQuery,
    db: Database,
    bot: Bot,
    *,
    campaign_id: int,
) -> None:
    user = await _load_user(db, int(callback.from_user.id))
    if not _is_role_authorized_for_menu(user):
        await callback.answer("Not authorized", show_alert=True)
        return
    campaign = await _load_campaign_with_visibility(db, user=user, campaign_id=campaign_id)
    if campaign is None:
        await callback.answer("Кампания недоступна", show_alert=True)
        return
    async with db.get_session() as session:
        target = (
            await session.execute(
                select(PromoTarget).where(PromoTarget.campaign_id == campaign_id)
            )
        ).scalars().first()
        is_initial_80 = (
            str(campaign.title or "") == "80 историй о главном / summer visibility"
            or (
                target is not None
                and target.target_type == "festival"
                and str(target.festival_name or "") == "80 историй о главном"
            )
        )
        if not is_initial_80:
            await callback.answer("VK-канал сейчас доступен только кампании 80 историй.", show_alert=True)
            return
        existing = (
            await session.execute(
                select(PromoActivity)
                .where(PromoActivity.campaign_id == campaign_id)
                .where(PromoActivity.surface == PROMO_SURFACE_VK_CHANNEL_PUBLISH)
            )
        ).scalars().first()
        if existing is None:
            session.add(_initial_80_vk_channel_publish_activity(int(campaign_id)))
        else:
            existing.enabled = True
            session.add(existing)
        campaign_obj = await session.get(PromoCampaign, int(campaign_id))
        if campaign_obj is not None:
            campaign_obj.updated_at = datetime.now(timezone.utc)
            session.add(campaign_obj)
        await session.commit()
    fresh = await _load_campaign_with_visibility(db, user=user, campaign_id=campaign_id)
    await callback.answer("VK-канал добавлен")
    if fresh is not None:
        await _render_campaign_card(bot, callback, callback.message.chat.id, db=db, user=user, campaign=fresh)


async def _handle_campaign_stats(
    callback: types.CallbackQuery, db: Database, bot: Bot, *, campaign_id: int
) -> None:
    user = await _load_user(db, int(callback.from_user.id))
    if not _is_role_authorized_for_menu(user):
        await callback.answer("Not authorized", show_alert=True)
        return
    campaign = await _load_campaign_with_visibility(
        db, user=user, campaign_id=campaign_id
    )
    if campaign is None:
        await callback.answer("Кампания недоступна", show_alert=True)
        return
    await callback.answer()
    await _render_campaign_stats(
        bot, callback, callback.message.chat.id, db=db, campaign=campaign
    )


async def _handle_campaign_rename_request(
    callback: types.CallbackQuery, db: Database, *, campaign_id: int
) -> None:
    user = await _load_user(db, int(callback.from_user.id))
    if not _is_role_authorized_for_menu(user):
        await callback.answer("Not authorized", show_alert=True)
        return
    campaign = await _load_campaign_with_visibility(
        db, user=user, campaign_id=campaign_id
    )
    if campaign is None:
        await callback.answer("Кампания недоступна", show_alert=True)
        return
    partner_promo_input_sessions[int(callback.from_user.id)] = PartnerPromoInputSession(
        event_id=0,
        field=PARTNER_PROMO_INPUT_RENAME,
        campaign_id=int(campaign_id),
    )
    await callback.answer(
        "Пришлите новое название кампании сообщением.",
        show_alert=True,
    )


async def _handle_rename_reply(
    message: types.Message,
    bot: Bot,
    db: Database,
    *,
    user_id: int,
    pending: PartnerPromoInputSession,
) -> bool:
    new_title = (message.text or "").strip()
    if not new_title:
        await bot.send_message(message.chat.id, "Пустое название.")
        return True
    new_title = new_title[:120]
    user = await _load_user(db, user_id)
    if not _is_role_authorized_for_menu(user):
        await bot.send_message(message.chat.id, "Not authorized")
        return True
    campaign_id = int(pending.campaign_id or 0)
    if campaign_id <= 0:
        return False
    campaign = await _load_campaign_with_visibility(
        db, user=user, campaign_id=campaign_id
    )
    if campaign is None:
        await bot.send_message(message.chat.id, "Кампания недоступна.")
        return True
    async with db.get_session() as session:
        c = await session.get(PromoCampaign, campaign_id)
        if c is None:
            await bot.send_message(message.chat.id, "Кампания не найдена.")
            return True
        c.title = new_title
        c.updated_at = datetime.now(timezone.utc)
        session.add(c)
        await session.commit()
    partner_promo_input_sessions.pop(user_id, None)
    await bot.send_message(message.chat.id, f"Готово: #{campaign_id} → {new_title}")
    return True


# Extend the existing callback dispatcher with management actions.
_ORIGINAL_HANDLE_PARTNER_PROMO_CALLBACK = handle_partner_promo_callback


async def handle_partner_promo_callback(  # noqa: F811 — extend dispatch
    callback: types.CallbackQuery, db: Database, bot: Bot
) -> bool:
    data = callback.data or ""
    if not data.startswith("ppromo:"):
        return False
    parts = data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    try:
        if action == "menu":
            sub = parts[2] if len(parts) >= 3 else "active"
            await _handle_menu_action(callback, db, bot, action=sub)
            return True
        if action == "close":
            await callback.answer("Закрыто")
            try:
                if callback.message is not None:
                    await callback.message.delete()
            except Exception:
                logger.debug("partner_promo: close delete failed", exc_info=True)
            return True
        if action == "view" and len(parts) >= 3:
            await _handle_view_campaign(
                callback, db, bot, campaign_id=int(parts[2])
            )
            return True
        if action in {"pause", "resume", "archive"} and len(parts) >= 3:
            new_status = {
                "pause": "paused",
                "resume": "active",
                "archive": "archived",
            }[action]
            await _handle_campaign_status_change(
                callback, db, bot, campaign_id=int(parts[2]), new_status=new_status
            )
            return True
        if action == "prio" and len(parts) >= 4:
            await _handle_campaign_priority(
                callback,
                db,
                bot,
                campaign_id=int(parts[2]),
                priority=int(parts[3]),
            )
            return True
        if action == "stats" and len(parts) >= 3:
            await _handle_campaign_stats(
                callback, db, bot, campaign_id=int(parts[2])
            )
            return True
        if action == "cren" and len(parts) >= 3:
            await _handle_campaign_rename_request(
                callback, db, campaign_id=int(parts[2])
            )
            return True
        if action == "addact" and len(parts) >= 3:
            await _open_add_activity_form(
                callback, db, bot, campaign_id=int(parts[2])
            )
            return True
        if action == "acttoggle" and len(parts) >= 4:
            await _handle_activity_toggle(
                callback,
                db,
                bot,
                campaign_id=int(parts[2]),
                activity_id=int(parts[3]),
            )
            return True
        if action == "add80vkchan" and len(parts) >= 3:
            await _handle_add_initial_80_vk_channel(
                callback, db, bot, campaign_id=int(parts[2])
            )
            return True
    except Exception:
        logger.exception("partner_promo: management callback failed data=%r", data)
        try:
            await callback.answer("Ошибка обработки", show_alert=True)
        except Exception:
            pass
        return True
    return await _ORIGINAL_HANDLE_PARTNER_PROMO_CALLBACK(callback, db, bot)


# Extend the reply dispatcher so PARTNER_PROMO_INPUT_RENAME without an active
# FSM session is routed to the rename-by-campaign-id path.
_ORIGINAL_HANDLE_PARTNER_PROMO_REPLY = handle_partner_promo_reply


async def handle_partner_promo_reply(  # noqa: F811 — extend dispatch
    message: types.Message, db: Database, bot: Bot
) -> bool:
    user_id = int(message.from_user.id) if message.from_user else 0
    pending = partner_promo_input_sessions.get(user_id)
    if pending is None:
        return False
    if (
        pending.field == PARTNER_PROMO_INPUT_RENAME
        and pending.campaign_id is not None
        and pending.event_id == 0
    ):
        handled = await _handle_rename_reply(
            message, bot, db, user_id=user_id, pending=pending
        )
        return handled
    return await _ORIGINAL_HANDLE_PARTNER_PROMO_REPLY(message, db, bot)
