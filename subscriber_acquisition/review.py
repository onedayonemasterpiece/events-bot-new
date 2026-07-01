from __future__ import annotations

import html
from datetime import datetime, timezone
from typing import Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from sqlalchemy import select

from models import AcqOpportunity, AcqReviewFeedback, AcqSurface
from .config import AcqConfig, load_config
from .safety import ensure_review_chat

_ACTION_TO_STATUS = {"approve": "approved", "reject": "rejected", "keep": "keep"}
_ACTION_TO_LABEL = {"approve": "✅ Да", "reject": "❌ Нет", "keep": "🕒 Потом"}


def build_review_keyboard(opp: AcqOpportunity) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="✅ Да", callback_data=f"acq:approve:{opp.id}"),
            InlineKeyboardButton(text="❌ Нет + причина", callback_data=f"acq:reject:{opp.id}"),
            InlineKeyboardButton(text="🕒 Потом", callback_data=f"acq:keep:{opp.id}"),
        ],
        [
            InlineKeyboardButton(text="💬 Оставить причину", callback_data=f"acq:comment:{opp.id}"),
        ],
    ]
    link_row = []
    if opp.context_url:
        link_row.append(InlineKeyboardButton(text="🔗 Контекст", url=opp.context_url))
    target_url = opp.link_target_url or opp.fallback_link_target_url
    if target_url:
        link_row.append(InlineKeyboardButton(text="🎯 Куда", url=target_url))
    if link_row:
        rows.append(link_row[:2])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def format_review_card(opp: AcqOpportunity, surface: AcqSurface | None = None) -> str:
    where = f"{opp.platform.upper()}"
    if surface:
        where += f" / {surface.title or surface.handle or surface.url}"
    events_count = len(opp.event_ids_json or [])
    target_kind = opp.link_target_kind or "none"
    target_label = opp.link_target_label or target_kind
    snippet = " ".join((opp.context_text_snippet or "").split())[:260]
    return (
        f"🧭 <b>Кандидат #{opp.id}</b>\n\n"
        f"<b>Где:</b> {html.escape(where)}\n"
        f"<b>Запрос:</b> «{html.escape(snippet)}»\n"
        f"<b>Тема:</b> {html.escape(opp.topic_cluster or opp.matched_intent or '—')}\n"
        f"<b>События:</b> {events_count}\n"
        f"<b>Куда вести:</b> {html.escape(target_label)} ({html.escape(target_kind)})\n"
        f"<b>Охват:</b> ~{int(opp.reach_low or 0)}\n"
        f"<b>Риск:</b> spam={html.escape(opp.spam_risk)} / safety={html.escape(opp.safety_risk)}\n\n"
        f"<b>Почему:</b>\n{html.escape(snippet or opp.link_target_reason or '—')}"
        f"\n\n<i>Если жмёте «Нет», причину можно оставить ответом на эту карточку.</i>"
    )


async def publish_review_cards(db, bot: Any, opportunities: list[AcqOpportunity], *, config: AcqConfig | None = None) -> int:
    cfg = config or load_config()
    if not opportunities or not cfg.review_chat_id or cfg.review_group_max_cards_per_run <= 0:
        return 0
    ensure_review_chat(cfg.review_chat_id, review_chat_id=cfg.review_chat_id)
    max_cards = min(int(cfg.review_group_max_cards_per_run or 0), 20)
    posted = 0
    async with db.get_session() as session:
        for opp in opportunities[:max_cards]:
            surface = await session.get(AcqSurface, opp.surface_id) if opp.surface_id else None
            sent = await bot.send_message(
                cfg.review_chat_id,
                format_review_card(opp, surface),
                parse_mode="HTML",
                reply_markup=build_review_keyboard(opp),
                message_thread_id=cfg.review_thread_id,
                disable_web_page_preview=True,
            )
            if sent is not None:
                db_opp = await session.get(AcqOpportunity, opp.id)
                sent_chat_id = int(getattr(getattr(sent, "chat", None), "id", cfg.review_chat_id))
                sent_message_id = int(getattr(sent, "message_id", 0) or 0)
                ensure_review_chat(sent_chat_id, review_chat_id=cfg.review_chat_id)
                if db_opp is not None:
                    db_opp.review_message_chat_id = sent_chat_id
                    db_opp.review_message_id = sent_message_id
                    db_opp.last_shown_at = datetime.now(timezone.utc)
                    session.add(db_opp)
                    session.add(AcqReviewFeedback(
                        opportunity_id=db_opp.id,
                        surface_id=db_opp.surface_id,
                        action="shown",
                        review_message_chat_id=sent_chat_id,
                        review_message_id=sent_message_id,
                    ))
                posted += 1
        await session.commit()
    return posted


async def publish_surface_cards(db, bot: Any, surfaces: list[AcqSurface], *, config: AcqConfig | None = None) -> int:
    cfg = config or load_config()
    if not surfaces or not cfg.review_chat_id or cfg.review_group_max_cards_per_run <= 0:
        return 0
    ensure_review_chat(cfg.review_chat_id, review_chat_id=cfg.review_chat_id)
    frontier = [
        surface for surface in surfaces
        if str(surface.source or "").strip().lower() in {"discovered", "linked_discussion"}
        and str(surface.status or "").strip().lower() == "candidate"
    ]
    max_cards = min(int(cfg.review_group_max_cards_per_run or 0), 10)
    posted = 0
    async with db.get_session() as session:
        for surface in frontier[:max_cards]:
            sent = await bot.send_message(
                cfg.review_chat_id,
                format_surface_card(surface),
                parse_mode="HTML",
                reply_markup=build_surface_keyboard(surface),
                message_thread_id=cfg.review_thread_id,
                disable_web_page_preview=True,
            )
            if sent is not None:
                sent_chat_id = int(getattr(getattr(sent, "chat", None), "id", cfg.review_chat_id))
                sent_message_id = int(getattr(sent, "message_id", 0) or 0)
                ensure_review_chat(sent_chat_id, review_chat_id=cfg.review_chat_id)
                session.add(AcqReviewFeedback(
                    surface_id=surface.id,
                    action="surface_shown",
                    review_message_chat_id=sent_chat_id,
                    review_message_id=sent_message_id,
                ))
                posted += 1
        await session.commit()
    return posted


async def record_feedback(db, *, opportunity_id: int | None, surface_id: int | None = None, reviewer_id: int | None = None, action: str, note: str | None = None, review_message_chat_id: int | None = None, review_message_id: int | None = None) -> AcqReviewFeedback:
    if action not in {"approve", "reject", "keep", "comment", "shown"}:
        raise ValueError("invalid acquisition feedback action")
    async with db.get_session() as session:
        opp = await session.get(AcqOpportunity, opportunity_id) if opportunity_id else None
        if opp is not None and action in _ACTION_TO_STATUS:
            opp.status = _ACTION_TO_STATUS[action]
            opp.reviewed_by = reviewer_id
            opp.reviewed_at = datetime.now(timezone.utc)
            opp.review_note = note or opp.review_note
            session.add(opp)
            if opp.surface_id:
                surface = await session.get(AcqSurface, opp.surface_id)
                if surface is not None:
                    if action == "approve":
                        surface.approved_score = float(surface.approved_score or 0) + 1
                    elif action == "reject":
                        surface.rejected_score = float(surface.rejected_score or 0) + 1
                    surface.reviewed_by = reviewer_id
                    surface.reviewed_at = datetime.now(timezone.utc)
                    session.add(surface)
        fb = AcqReviewFeedback(
            opportunity_id=opportunity_id,
            surface_id=surface_id or (opp.surface_id if opp else None),
            reviewer_id=reviewer_id,
            action=action,
            note=note,
            review_message_chat_id=review_message_chat_id,
            review_message_id=review_message_id,
        )
        session.add(fb)
        await session.commit()
        await session.refresh(fb)
        return fb


async def find_opportunity_by_review_message(db, *, chat_id: int, message_id: int) -> AcqOpportunity | None:
    async with db.get_session() as session:
        return (await session.execute(
            select(AcqOpportunity).where(
                AcqOpportunity.review_message_chat_id == int(chat_id),
                AcqOpportunity.review_message_id == int(message_id),
            )
        )).scalar_one_or_none()


_SURFACE_ACTION_TO_STATUS = {"approve": "approved", "reject": "rejected", "pause": "paused", "candidate": "candidate"}


def build_surface_keyboard(surface: AcqSurface) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Да", callback_data=f"acqsurf:approve:{surface.id}"),
        InlineKeyboardButton(text="❌ Нет + причина", callback_data=f"acqsurf:reject:{surface.id}"),
        InlineKeyboardButton(text="🕒 Потом", callback_data=f"acqsurf:pause:{surface.id}"),
    ], [
        InlineKeyboardButton(text="💬 Оставить причину", callback_data=f"acqsurf:comment:{surface.id}"),
        InlineKeyboardButton(text="🔗 Открыть", url=surface.url),
    ]])


def format_surface_card(surface: AcqSurface) -> str:
    reach = surface.reach_json or {}
    risk = surface.risk_json or {}
    return (
        f"🧭 <b>Surface #{surface.id}</b>\n"
        f"<b>Где:</b> {html.escape(surface.platform)}/{html.escape(surface.surface_type)}\n"
        f"<b>Название:</b> {html.escape(surface.title or surface.handle or surface.url)}\n"
        f"<b>Статус:</b> {html.escape(surface.status)}\n"
        f"<b>Охват:</b> {html.escape(str(reach.get('low') or reach.get('members') or reach.get('confidence') or 'unknown'))}\n"
        f"<b>Риск:</b> {html.escape(str(risk.get('level') or risk.get('spam_risk') or 'unknown'))}\n"
        f"<b>Источник:</b> {html.escape(surface.source or '—')}"
    )


async def record_surface_feedback(db, *, surface_id: int, reviewer_id: int | None = None, action: str, note: str | None = None) -> AcqReviewFeedback:
    if action not in _SURFACE_ACTION_TO_STATUS:
        raise ValueError("invalid acquisition surface feedback action")
    async with db.get_session() as session:
        surface = await session.get(AcqSurface, surface_id)
        if surface is None:
            raise ValueError("surface not found")
        surface.status = _SURFACE_ACTION_TO_STATUS[action]
        surface.reviewed_by = reviewer_id
        surface.reviewed_at = datetime.now(timezone.utc)
        surface.review_note = note or surface.review_note
        if action == "approve":
            surface.approved_score = float(surface.approved_score or 0) + 1
        elif action == "reject":
            surface.rejected_score = float(surface.rejected_score or 0) + 1
        session.add(surface)
        fb = AcqReviewFeedback(surface_id=surface_id, reviewer_id=reviewer_id, action=f"surface_{action}", note=note)
        session.add(fb)
        await session.commit()
        await session.refresh(fb)
        return fb
