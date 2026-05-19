from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import select

from db import Database
from handlers.partner_promo_cmd import (
    _campaign_card_keyboard,
    _campaign_card_text,
    _campaign_stats_text,
    _list_campaigns_for_role,
    _menu_keyboard,
    _menu_text,
)
from models import (
    Event,
    PromoActivity,
    PromoCampaign,
    PromoExposure,
    PromoTarget,
    User,
)
from promo import (
    PROMO_POLICY_FIRST_SLOT,
    PROMO_POLICY_FIRST_TWO_SLOTS,
    PROMO_SURFACE_VIDEO_GENERAL,
    PartnerPromoSpec,
    create_partner_event_promo_campaign,
)


def _event(title: str, day: str, creator_id: int = 100) -> Event:
    return Event(
        title=title,
        description="Description",
        short_description="Short",
        search_digest="Digest",
        source_text="source",
        date=day,
        time="19:00",
        location_name="Venue",
        city="Калининград",
        photo_urls=["https://example.com/poster.jpg"],
        photo_count=1,
        creator_id=creator_id,
    )


def _partner(user_id: int) -> User:
    return User(user_id=user_id, username=f"u{user_id}", is_partner=True, organization="КОНБ")


def _admin(user_id: int = 1) -> User:
    return User(user_id=user_id, username="root", is_superadmin=True)


async def _seed_campaign(
    db: Database,
    *,
    event_title: str,
    creator: int,
    event_date: str = "2026-06-10",
    end_date: date | None = None,
    is_editorial: bool = False,
    slot_policy: str = PROMO_POLICY_FIRST_TWO_SLOTS,
) -> PromoCampaign:
    async with db.get_session() as session:
        session.add(_event(event_title, event_date, creator_id=creator))
        await session.commit()
        ev_id = (
            await session.execute(select(Event.id).where(Event.title == event_title))
        ).scalar_one()

    spec = PartnerPromoSpec(
        event_id=int(ev_id),
        creator_user_id=creator,
        organization_name="КОНБ",
        surface=PROMO_SURFACE_VIDEO_GENERAL,
        profile_key="konb",
        slot_policy=slot_policy,
        count=2,
        ends_at=end_date or date(2026, 6, 1),
        is_editorial=is_editorial,
        sponsorship_disclosure=None if is_editorial else "Партнёрский материал",
    )
    result = await create_partner_event_promo_campaign(
        db, spec, now_utc=datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc)
    )
    assert result.status == "created"
    return result.campaign


@pytest.mark.asyncio
async def test_list_campaigns_partner_sees_only_own(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(_partner(100))
        session.add(_partner(200))
        await session.commit()

    own = await _seed_campaign(db, event_title="Лекция своя", creator=100)
    other = await _seed_campaign(db, event_title="Лекция чужая", creator=200)

    async with db.get_session() as session:
        partner_one = await session.get(User, 100)

    campaigns = await _list_campaigns_for_role(
        db, user=partner_one, include_archived=False
    )
    titles = [c.title for c in campaigns]
    assert own.title in titles
    assert other.title not in titles


@pytest.mark.asyncio
async def test_list_campaigns_admin_sees_all(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(_admin(1))
        session.add(_partner(100))
        session.add(_partner(200))
        await session.commit()

    await _seed_campaign(db, event_title="Своё", creator=100)
    await _seed_campaign(db, event_title="Чужое", creator=200)

    async with db.get_session() as session:
        admin = await session.get(User, 1)

    campaigns = await _list_campaigns_for_role(
        db, user=admin, include_archived=False
    )
    assert len(campaigns) >= 2


@pytest.mark.asyncio
async def test_menu_text_and_keyboard_include_campaign(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(_partner(100))
        await session.commit()

    campaign = await _seed_campaign(db, event_title="Лекция", creator=100)

    async with db.get_session() as session:
        partner = await session.get(User, 100)

    text = _menu_text(partner, [campaign], archived=False)
    assert "Промо-кампании" in text
    assert f"#{campaign.id}" in text

    markup = _menu_keyboard([campaign], archived=False, is_superadmin=False)
    btn_texts = [b.text for row in markup.inline_keyboard for b in row]
    assert any(f"#{campaign.id}" in t for t in btn_texts)
    # Partner does not see Seed 80 button
    assert all("Seed 80" not in t for t in btn_texts)


@pytest.mark.asyncio
async def test_menu_keyboard_admin_has_seed80(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(_admin(1))
        await session.commit()
        admin = await session.get(User, 1)

    markup = _menu_keyboard([], archived=False, is_superadmin=True)
    btn_texts = [b.text for row in markup.inline_keyboard for b in row]
    assert any("Seed 80" in t for t in btn_texts)


@pytest.mark.asyncio
async def test_campaign_card_text_has_target_and_activity(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(_partner(100))
        await session.commit()

    campaign = await _seed_campaign(db, event_title="Карточка", creator=100)
    text = await _campaign_card_text(db, campaign)
    assert f"#{campaign.id}" in text
    assert "Цели:" in text
    assert "Активности:" in text
    assert "video_general" in text
    assert "konb" in text


@pytest.mark.asyncio
async def test_campaign_card_keyboard_active_has_pause(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(_partner(100))
        await session.commit()

    campaign = await _seed_campaign(db, event_title="Active", creator=100)
    kb = _campaign_card_keyboard(campaign, is_superadmin=False)
    labels = [b.text for row in kb.inline_keyboard for b in row]
    assert any("Пауза" in l for l in labels)
    assert any("Архив" in l for l in labels)
    assert any("Статистика" in l for l in labels)
    assert any("Переименовать" in l for l in labels)
    # No priority row for partner
    assert all(not l.startswith("P") for l in labels)


@pytest.mark.asyncio
async def test_campaign_card_keyboard_paused_has_resume(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(_partner(100))
        await session.commit()

    campaign = await _seed_campaign(db, event_title="Paused", creator=100)
    async with db.get_session() as session:
        c = await session.get(PromoCampaign, int(campaign.id))
        c.status = "paused"
        session.add(c)
        await session.commit()
        await session.refresh(c)
        campaign = c

    kb = _campaign_card_keyboard(campaign, is_superadmin=False)
    labels = [b.text for row in kb.inline_keyboard for b in row]
    assert any("Запустить" in l for l in labels)


@pytest.mark.asyncio
async def test_campaign_card_keyboard_admin_has_priority_row(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(_admin(1))
        session.add(_partner(100))
        await session.commit()

    campaign = await _seed_campaign(db, event_title="AdminPrio", creator=100)
    kb = _campaign_card_keyboard(campaign, is_superadmin=True)
    labels = [b.text for row in kb.inline_keyboard for b in row]
    assert any(l in {"P0", "P1", "P2", "P3"} for l in labels)


@pytest.mark.asyncio
async def test_campaign_stats_text_when_no_exposures(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        session.add(_partner(100))
        await session.commit()

    campaign = await _seed_campaign(db, event_title="Stats", creator=100)
    text = await _campaign_stats_text(db, campaign)
    assert "Публичных показов пока нет" in text
