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
            InlineKeyboardButton(text="❌ Нет", callback_data=f"acq:reject:{opp.id}"),
            InlineKeyboardButton(text="🕒 Потом", callback_data=f"acq:keep:{opp.id}"),
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
    )


async def publish_review_cards(db, bot: Any, opportunities: list[AcqOpportunity], *, config: AcqConfig | None = None) -> int:
    cfg = config or load_config()
    if not opportunities or not cfg.review_chat_id or cfg.review_group_max_cards_per_run <= 0:
        return 0
    ensure_review_chat(cfg.review_chat_id, review_chat_id=cfg.review_chat_id)
    posted = 0
    async with db.get_session() as session:
        for opp in opportunities[: cfg.review_group_max_cards_per_run]:
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
                if db_opp is not None:
                    db_opp.review_message_chat_id = int(getattr(getattr(sent, "chat", None), "id", cfg.review_chat_id))
                    db_opp.review_message_id = int(getattr(sent, "message_id", 0) or 0)
                    db_opp.last_shown_at = datetime.now(timezone.utc)
                    session.add(db_opp)
                posted += 1
        await session.commit()
    return posted


async def record_feedback(db, *, opportunity_id: int | None, surface_id: int | None = None, reviewer_id: int | None = None, action: str, note: str | None = None, review_message_chat_id: int | None = None, review_message_id: int | None = None) -> AcqReviewFeedback:
    if action not in {"approve", "reject", "keep", "comment"}:
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
