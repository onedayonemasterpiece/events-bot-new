from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import select

from db import Database
from models import (
    Event,
    Organization,
    PromoActivity,
    PromoCampaign,
    PromoTarget,
    User,
)
from promo import (
    PARTNER_PROMO_SLOT_POLICIES,
    PARTNER_PROMO_VIDEO_PROFILES,
    PROMO_POLICY_FIRST_SLOT,
    PROMO_POLICY_FIRST_TWO_SLOTS,
    PROMO_POLICY_GUARANTEED_ANY_POSITION,
    PROMO_SURFACE_VIDEO_GENERAL,
    PROMO_SURFACE_VK_REPOST,
    PartnerActivitySpec,
    PartnerPromoSpec,
    add_partner_activity_to_campaign,
    build_partner_campaign_title,
    clamp_campaign_end_to_event,
    create_partner_event_promo_campaign,
)


@pytest.fixture(autouse=True)
def _freeze_promo_menu_clock(monkeypatch):
    # Visibility must be evaluated at the same date as these May/June fixtures.
    # Do not weaken the production filter that hides expired active campaigns.
    class FixtureDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 5, 18, 8, tzinfo=timezone.utc)
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)
    monkeypatch.setattr("handlers.partner_promo_cmd.datetime", FixtureDatetime)


def _event(title: str, day: str, *, creator_id: int = 100, end_date: str | None = None) -> Event:
    return Event(
        title=title,
        description="Description",
        short_description="Short",
        search_digest="Digest",
        source_text="source",
        date=day,
        end_date=end_date,
        time="19:00",
        location_name="Venue",
        city="Калининград",
        photo_urls=["https://example.com/poster.jpg"],
        photo_count=1,
        creator_id=creator_id,
    )


def _partner(user_id: int, *, username: str = "partner", org: str = "КОНБ") -> User:
    return User(
        user_id=user_id,
        username=username,
        is_partner=True,
        organization=org,
    )


def test_clamp_campaign_end_to_event_picks_min() -> None:
    ev = _event("Концерт", "2026-06-10", end_date=None)
    clamped = clamp_campaign_end_to_event(date(2026, 8, 1), ev)
    assert clamped == date(2026, 6, 10)


def test_clamp_campaign_end_to_event_uses_end_date_for_multiday() -> None:
    ev = _event("Фестиваль", "2026-06-10", end_date="2026-06-15")
    clamped = clamp_campaign_end_to_event(date(2026, 7, 1), ev)
    assert clamped == date(2026, 6, 15)


def test_clamp_campaign_end_keeps_earlier_requested_end() -> None:
    ev = _event("Концерт", "2026-06-10")
    clamped = clamp_campaign_end_to_event(date(2026, 6, 1), ev)
    assert clamped == date(2026, 6, 1)


def test_build_partner_campaign_title_partner_prefix() -> None:
    title = build_partner_campaign_title(
        organization_name="КОНБ",
        partner_username="ek_tikun",
        event_title="Лекция о Канте",
        created_date=date(2026, 5, 18),
        is_superadmin=False,
    )
    assert title.startswith("КОНБ · ")
    assert "Лекция" in title
    assert title.endswith("2026-05-18")


def test_build_partner_campaign_title_admin_prefix() -> None:
    title = build_partner_campaign_title(
        organization_name=None,
        partner_username="admin",
        event_title="Концерт",
        created_date=date(2026, 5, 18),
        is_superadmin=True,
    )
    assert title.startswith("editorial · ")


def test_policy_and_profile_catalogs_match_spec() -> None:
    assert set(PARTNER_PROMO_SLOT_POLICIES) == {
        PROMO_POLICY_GUARANTEED_ANY_POSITION,
        PROMO_POLICY_FIRST_TWO_SLOTS,
        PROMO_POLICY_FIRST_SLOT,
    }
    assert set(PARTNER_PROMO_VIDEO_PROFILES) == {"popular_review", "default", "konb"}


@pytest.mark.asyncio
async def test_create_partner_event_promo_basic_flow(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(_partner(100, username="ek_tikun"))
        session.add(_event("Лекция о Канте", "2026-06-01", creator_id=100))
        await session.commit()
        ev_id = (
            await session.execute(select(Event.id).where(Event.title == "Лекция о Канте"))
        ).scalar_one()

    spec = PartnerPromoSpec(
        event_id=int(ev_id),
        creator_user_id=100,
        organization_name="КОНБ",
        surface=PROMO_SURFACE_VIDEO_GENERAL,
        profile_key="konb",
        slot_policy=PROMO_POLICY_FIRST_TWO_SLOTS,
        count=3,
        ends_at=date(2026, 5, 25),
        is_editorial=False,
        sponsorship_disclosure="Партнёрский материал",
        priority=2,
    )
    result = await create_partner_event_promo_campaign(db, spec, now_utc=now_utc)
    assert result.status == "created"
    assert result.campaign is not None

    async with db.get_session() as session:
        campaign = await session.get(PromoCampaign, int(result.campaign.id))
        assert campaign is not None
        assert campaign.status == "active"
        assert campaign.total_exposure_goal == 3
        assert campaign.sponsorship_disclosure == "Партнёрский материал"
        assert campaign.priority == 2

        target = (
            await session.execute(
                select(PromoTarget).where(PromoTarget.campaign_id == campaign.id)
            )
        ).scalar_one()
        assert target.target_type == "event"
        assert int(target.event_id) == int(ev_id)

        activity = (
            await session.execute(
                select(PromoActivity).where(PromoActivity.campaign_id == campaign.id, PromoActivity.surface == PROMO_SURFACE_VIDEO_GENERAL)
            )
        ).scalar_one()
        assert activity.surface == PROMO_SURFACE_VIDEO_GENERAL
        assert activity.profile_key == "konb"
        assert activity.selection_policy == PROMO_POLICY_FIRST_TWO_SLOTS
        assert activity.slot is None
        assert activity.target_exposure_goal == 3


@pytest.mark.asyncio
async def test_create_partner_promo_clamps_end_to_event(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(_partner(200))
        session.add(_event("Концерт", "2026-05-25", creator_id=200))
        await session.commit()
        ev_id = (
            await session.execute(select(Event.id).where(Event.title == "Концерт"))
        ).scalar_one()

    spec = PartnerPromoSpec(
        event_id=int(ev_id),
        creator_user_id=200,
        organization_name="КОНБ",
        surface=PROMO_SURFACE_VIDEO_GENERAL,
        profile_key="popular_review",
        slot_policy=PROMO_POLICY_GUARANTEED_ANY_POSITION,
        count=2,
        ends_at=date(2026, 9, 1),  # way past event date
        is_editorial=False,
        sponsorship_disclosure=None,
    )
    result = await create_partner_event_promo_campaign(db, spec, now_utc=now_utc)
    assert result.status == "created"
    assert result.campaign is not None
    assert result.campaign.ends_at is not None
    assert result.campaign.ends_at.date() == date(2026, 5, 25)


@pytest.mark.asyncio
async def test_create_partner_promo_first_slot_sets_slot_1(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(_partner(300))
        session.add(_event("Спектакль", "2026-06-01", creator_id=300))
        await session.commit()
        ev_id = (
            await session.execute(select(Event.id).where(Event.title == "Спектакль"))
        ).scalar_one()

    spec = PartnerPromoSpec(
        event_id=int(ev_id),
        creator_user_id=300,
        organization_name=None,
        surface=PROMO_SURFACE_VIDEO_GENERAL,
        profile_key="popular_review",
        slot_policy=PROMO_POLICY_FIRST_SLOT,
        count=1,
        ends_at=date(2026, 5, 30),
        is_editorial=False,
        sponsorship_disclosure="Партнёрский материал",
    )
    result = await create_partner_event_promo_campaign(db, spec, now_utc=now_utc)
    assert result.status == "created"

    async with db.get_session() as session:
        activity = (
            await session.execute(
                select(PromoActivity).where(PromoActivity.campaign_id == result.campaign.id, PromoActivity.surface == PROMO_SURFACE_VIDEO_GENERAL)
            )
        ).scalar_one()
        assert activity.slot == 1
        assert activity.selection_policy == PROMO_POLICY_FIRST_SLOT


@pytest.mark.asyncio
async def test_create_partner_promo_editorial_mode_skips_disclosure(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(_partner(400))
        session.add(_event("Открытие", "2026-06-01", creator_id=400))
        await session.commit()
        ev_id = (
            await session.execute(select(Event.id).where(Event.title == "Открытие"))
        ).scalar_one()

    spec = PartnerPromoSpec(
        event_id=int(ev_id),
        creator_user_id=400,
        organization_name="КОНБ",
        surface=PROMO_SURFACE_VIDEO_GENERAL,
        profile_key="default",
        slot_policy=PROMO_POLICY_GUARANTEED_ANY_POSITION,
        count=1,
        ends_at=date(2026, 5, 30),
        is_editorial=True,
        sponsorship_disclosure="ignored when editorial",
    )
    result = await create_partner_event_promo_campaign(db, spec, now_utc=now_utc)
    assert result.status == "created"
    assert result.campaign.sponsorship_disclosure is None


@pytest.mark.asyncio
async def test_create_partner_promo_rejects_past_event(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(_partner(500))
        session.add(_event("Прошедшее", "2026-04-01", creator_id=500))
        await session.commit()
        ev_id = (
            await session.execute(select(Event.id).where(Event.title == "Прошедшее"))
        ).scalar_one()

    spec = PartnerPromoSpec(
        event_id=int(ev_id),
        creator_user_id=500,
        organization_name=None,
        surface=PROMO_SURFACE_VIDEO_GENERAL,
        profile_key="popular_review",
        slot_policy=PROMO_POLICY_GUARANTEED_ANY_POSITION,
        count=1,
        ends_at=date(2026, 5, 25),
        is_editorial=False,
        sponsorship_disclosure=None,
    )
    result = await create_partner_event_promo_campaign(db, spec, now_utc=now_utc)
    assert result.status == "not_eligible"


@pytest.mark.asyncio
async def test_organization_schema_round_trips(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    # The init seed inserts the КОНБ row; we round-trip a different name
    # here to verify model fields work without colliding with the seed.
    async with db.get_session() as session:
        session.add(
            Organization(
                name="Тест-Орг",
                vk_source_group_ids=[111, 222],
                video_profile_key="custom",
                sponsorship_default="Материал ТО",
            )
        )
        await session.commit()

    async with db.get_session() as session:
        org = await session.get(Organization, "Тест-Орг")
        assert org is not None
        assert org.video_profile_key == "custom"
        assert org.vk_source_group_ids == [111, 222]
        assert org.sponsorship_default == "Материал ТО"

        konb = await session.get(Organization, "Научная библиотека")
        assert konb is not None
        assert konb.video_profile_key == "konb"
        assert konb.vk_source_group_ids == [30777579]


@pytest.mark.asyncio
async def test_add_partner_activity_appends_to_existing_campaign(tmp_path) -> None:
    """Add-activity FSM lands a fresh PromoActivity on the existing campaign."""

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 19, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(_partner(600))
        session.add(_event("Лекция", "2026-06-15", creator_id=600))
        await session.commit()
        ev_id = (
            await session.execute(select(Event.id).where(Event.title == "Лекция"))
        ).scalar_one()

    spec = PartnerPromoSpec(
        event_id=int(ev_id),
        creator_user_id=600,
        organization_name="КОНБ",
        surface=PROMO_SURFACE_VIDEO_GENERAL,
        profile_key="popular_review",
        slot_policy=PROMO_POLICY_FIRST_TWO_SLOTS,
        count=3,
        ends_at=date(2026, 6, 1),
        is_editorial=False,
        sponsorship_disclosure="Партнёрский материал",
    )
    initial = await create_partner_event_promo_campaign(db, spec, now_utc=now_utc)
    assert initial.status == "created"
    assert initial.campaign is not None
    campaign_id = int(initial.campaign.id)

    # Add a second activity — a different profile + slot policy.
    add_result = await add_partner_activity_to_campaign(
        db,
        PartnerActivitySpec(
            campaign_id=campaign_id,
            surface=PROMO_SURFACE_VIDEO_GENERAL,
            profile_key="default",
            slot_policy=PROMO_POLICY_FIRST_SLOT,
            count=1,
        ),
        actor_user_id=600,
        now_utc=now_utc,
    )
    assert add_result.status == "created"
    assert add_result.campaign is not None
    assert int(add_result.campaign.id) == campaign_id

    async with db.get_session() as session:
        activities = list(
            (
                await session.execute(
                    select(PromoActivity).where(PromoActivity.campaign_id == campaign_id)
                )
            ).scalars().all()
        )
    assert len(activities) == 3
    assert sum(a.surface == "tg_button_highlight" for a in activities) == 1
    surfaces = [(a.surface, a.profile_key, a.selection_policy, a.target_exposure_goal) for a in activities]
    assert (PROMO_SURFACE_VIDEO_GENERAL, "popular_review", PROMO_POLICY_FIRST_TWO_SLOTS, 3) in surfaces
    assert (PROMO_SURFACE_VIDEO_GENERAL, "default", PROMO_POLICY_FIRST_SLOT, 1) in surfaces
    # first_slot activity stores slot=1; first_two_slots stores slot=None.
    slots = {(a.profile_key, a.slot) for a in activities}
    assert ("default", 1) in slots
    assert ("popular_review", None) in slots


@pytest.mark.asyncio
async def test_add_partner_activity_rejects_archived_campaign(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 19, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(_partner(700))
        session.add(_event("Архив", "2026-06-15", creator_id=700))
        await session.commit()
        ev_id = (
            await session.execute(select(Event.id).where(Event.title == "Архив"))
        ).scalar_one()

    spec = PartnerPromoSpec(
        event_id=int(ev_id),
        creator_user_id=700,
        organization_name=None,
        surface=PROMO_SURFACE_VIDEO_GENERAL,
        profile_key="popular_review",
        slot_policy=PROMO_POLICY_GUARANTEED_ANY_POSITION,
        count=1,
        ends_at=date(2026, 6, 1),
        is_editorial=False,
        sponsorship_disclosure=None,
    )
    initial = await create_partner_event_promo_campaign(db, spec, now_utc=now_utc)
    assert initial.status == "created"
    campaign_id = int(initial.campaign.id)

    # Archive the campaign
    async with db.get_session() as session:
        c = await session.get(PromoCampaign, campaign_id)
        c.status = "archived"
        session.add(c)
        await session.commit()

    add_result = await add_partner_activity_to_campaign(
        db,
        PartnerActivitySpec(
            campaign_id=campaign_id,
            surface=PROMO_SURFACE_VIDEO_GENERAL,
            profile_key="default",
            slot_policy=PROMO_POLICY_GUARANTEED_ANY_POSITION,
            count=1,
        ),
        actor_user_id=700,
        now_utc=now_utc,
    )
    assert add_result.status == "invalid"
    assert "архив" in add_result.message


@pytest.mark.asyncio
async def test_add_partner_activity_supports_vk_repost(tmp_path) -> None:
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 19, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(_partner(800))
        session.add(_event("ВК", "2026-06-15", creator_id=800))
        await session.commit()
        ev_id = (
            await session.execute(select(Event.id).where(Event.title == "ВК"))
        ).scalar_one()

    spec = PartnerPromoSpec(
        event_id=int(ev_id),
        creator_user_id=800,
        organization_name="КОНБ",
        surface=PROMO_SURFACE_VIDEO_GENERAL,
        profile_key="popular_review",
        slot_policy=PROMO_POLICY_GUARANTEED_ANY_POSITION,
        count=2,
        ends_at=date(2026, 6, 1),
        is_editorial=False,
        sponsorship_disclosure="Партнёрский материал",
    )
    initial = await create_partner_event_promo_campaign(db, spec, now_utc=now_utc)
    assert initial.status == "created"
    campaign_id = int(initial.campaign.id)

    add_result = await add_partner_activity_to_campaign(
        db,
        PartnerActivitySpec(
            campaign_id=campaign_id,
            surface=PROMO_SURFACE_VK_REPOST,
            profile_key=None,
            slot_policy=PROMO_POLICY_GUARANTEED_ANY_POSITION,  # ignored for vk_repost
            count=4,
        ),
        actor_user_id=800,
        now_utc=now_utc,
    )
    assert add_result.status == "created"

    async with db.get_session() as session:
        vk_act = (
            await session.execute(
                select(PromoActivity)
                .where(PromoActivity.campaign_id == campaign_id)
                .where(PromoActivity.surface == PROMO_SURFACE_VK_REPOST)
            )
        ).scalar_one()
    assert vk_act.target_exposure_goal == 4
    assert vk_act.profile_key is None


@pytest.mark.asyncio
async def test_list_campaigns_covering_event_includes_festival(tmp_path) -> None:
    """Event belonging to a festival shows the covering festival campaign."""

    from handlers.partner_promo_cmd import _list_campaigns_covering_event
    from promo import create_festival_promo_campaign

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 26, 8, 0, tzinfo=timezone.utc)

    # superadmin user — sees everything
    async with db.get_session() as session:
        session.add(User(user_id=1, username="root", is_superadmin=True))
        from models import Festival
        session.add(Festival(name="80 историй о главном"))
        ev = _event("Зоопарк", "2026-06-04")
        ev.festival = "80 историй о главном"
        session.add(ev)
        await session.commit()
        ev_id = (
            await session.execute(select(Event.id).where(Event.title == "Зоопарк"))
        ).scalar_one()
        admin = await session.get(User, 1)

    fest_result = await create_festival_promo_campaign(
        db,
        festival_name="80 историй о главном",
        ends_at=date(2026, 7, 18),
        now_utc=now_utc,
    )
    assert fest_result.status == "created", fest_result.message
    fest_id = int(fest_result.campaign.id)

    event_camps, fest_camps = await _list_campaigns_covering_event(
        db, user=admin, event_id=int(ev_id)
    )
    assert event_camps == []
    assert len(fest_camps) == 1
    assert int(fest_camps[0].id) == fest_id


@pytest.mark.asyncio
async def test_partner_campaign_menu_hides_ended_active_campaigns_by_default(tmp_path) -> None:
    from handlers.partner_promo_cmd import _list_campaigns_for_role

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()

    async with db.get_session() as session:
        admin = User(user_id=1, username="root", is_superadmin=True)
        session.add(admin)
        session.add_all(
            [
                PromoCampaign(
                    title="Past partner promo",
                    status="active",
                    starts_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
                    ends_at=datetime(2020, 1, 2, tzinfo=timezone.utc),
                ),
                PromoCampaign(
                    title="Future partner promo",
                    status="active",
                    starts_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
                    ends_at=datetime(2099, 1, 2, tzinfo=timezone.utc),
                ),
            ]
        )
        await session.commit()
        admin = await session.get(User, 1)

    current = await _list_campaigns_for_role(db, user=admin, include_archived=False)
    report = await _list_campaigns_for_role(db, user=admin, include_archived=True)

    assert [c.title for c in current] == ["Future partner promo"]
    assert {c.title for c in report} == {"Past partner promo", "Future partner promo"}


@pytest.mark.asyncio
async def test_list_campaigns_covering_event_partner_hides_admin_festival(tmp_path) -> None:
    """A partner does not see a festival campaign created by another user."""

    from handlers.partner_promo_cmd import _list_campaigns_covering_event
    from promo import create_festival_promo_campaign

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 26, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(_partner(900, username="lib"))
        from models import Festival
        session.add(Festival(name="80 историй о главном"))
        ev = _event("Зоопарк partner", "2026-06-04", creator_id=900)
        ev.festival = "80 историй о главном"
        session.add(ev)
        await session.commit()
        ev_id = (
            await session.execute(
                select(Event.id).where(Event.title == "Зоопарк partner")
            )
        ).scalar_one()
        partner = await session.get(User, 900)

    # Admin-created seed-style festival campaign (created_by=None / different uid)
    fest_result = await create_festival_promo_campaign(
        db,
        festival_name="80 историй о главном",
        ends_at=date(2026, 7, 18),
        now_utc=now_utc,
        created_by=None,
    )
    assert fest_result.status == "created"

    event_camps, fest_camps = await _list_campaigns_covering_event(
        db, user=partner, event_id=int(ev_id)
    )
    assert event_camps == []
    # Partner does NOT see admin's festival campaign
    assert fest_camps == []


@pytest.mark.asyncio
async def test_list_campaigns_covering_event_combines_event_and_festival(tmp_path) -> None:
    """Both event-targeted and festival-covering campaigns surface side by side."""

    from handlers.partner_promo_cmd import _list_campaigns_covering_event
    from promo import create_festival_promo_campaign

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime(2026, 5, 26, 8, 0, tzinfo=timezone.utc)

    async with db.get_session() as session:
        session.add(User(user_id=1, username="root", is_superadmin=True))
        from models import Festival
        session.add(Festival(name="80 историй о главном"))
        ev = _event("Двойник", "2026-06-04")
        ev.festival = "80 историй о главном"
        session.add(ev)
        await session.commit()
        ev_id = (
            await session.execute(select(Event.id).where(Event.title == "Двойник"))
        ).scalar_one()
        admin = await session.get(User, 1)

    fest_res = await create_festival_promo_campaign(
        db,
        festival_name="80 историй о главном",
        ends_at=date(2026, 7, 18),
        now_utc=now_utc,
    )
    assert fest_res.status == "created"

    # Event-targeted campaign on top of the festival one
    ev_spec = PartnerPromoSpec(
        event_id=int(ev_id),
        creator_user_id=1,
        organization_name=None,
        surface=PROMO_SURFACE_VIDEO_GENERAL,
        profile_key="popular_review",
        slot_policy=PROMO_POLICY_FIRST_TWO_SLOTS,
        count=2,
        ends_at=date(2026, 6, 4),
        is_editorial=False,
        sponsorship_disclosure="Партнёрский материал",
    )
    ev_res = await create_partner_event_promo_campaign(db, ev_spec, now_utc=now_utc)
    assert ev_res.status == "created"

    event_camps, fest_camps = await _list_campaigns_covering_event(
        db, user=admin, event_id=int(ev_id)
    )
    assert len(event_camps) == 1
    assert int(event_camps[0].id) == int(ev_res.campaign.id)
    assert len(fest_camps) == 1
    assert int(fest_camps[0].id) == int(fest_res.campaign.id)


@pytest.mark.asyncio
async def test_campaign_stats_text_per_vk_activity_with_links(tmp_path) -> None:
    """Stats screen breaks down per activity, counts VK_SCHEDULED, shows links."""
    from datetime import timedelta

    from models import PromoExposure
    from promo import PROMO_SURFACE_VK_PUBLICATION
    from handlers.partner_promo_cmd import _campaign_stats_text, _humanize_activity

    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now_utc = datetime.now(timezone.utc)

    async with db.get_session() as session:
        session.add(_event("Событие фестиваля", "2026-06-20", creator_id=1))
        await session.commit()
        ev_id = int(
            (await session.execute(select(Event.id))).scalars().first()
        )
        campaign = PromoCampaign(title="80 историй", status="active", starts_at=now_utc)
        session.add(campaign)
        await session.commit()
        await session.refresh(campaign)
        cid = int(campaign.id)
        pub = PromoActivity(
            campaign_id=cid,
            surface=PROMO_SURFACE_VK_PUBLICATION,
            profile_key="klgdevents",
            max_per_publish=2,
            daily_cap=2,
            enabled=True,
            config_json={"target_group": "klgdevents", "window_hours": 24},
        )
        rep = PromoActivity(
            campaign_id=cid,
            surface=PROMO_SURFACE_VK_REPOST,
            profile_key="klgdevents->kenigeventsofficial",
            max_per_publish=1,
            daily_cap=1,
            enabled=True,
            config_json={"source_group": "klgdevents", "target_group": "kenigeventsofficial"},
        )
        session.add_all([pub, rep])
        await session.commit()
        await session.refresh(pub)
        await session.refresh(rep)
        pub_id, rep_id = int(pub.id), int(rep.id)
        # vk_publication recorded as VK_SCHEDULED (postponed) — must still count.
        session.add(
            PromoExposure(
                campaign_id=cid,
                activity_id=pub_id,
                event_id=ev_id,
                surface=PROMO_SURFACE_VK_PUBLICATION,
                placement_kind="rolling_window_deficit",
                publish_status="VK_SCHEDULED",
                public_target_count=1,
                public_targets_json=[{"type": "vk_wall", "url": "https://vk.com/wall-1_123"}],
                published_at=now_utc - timedelta(hours=2),
                details_json={"target_url": "https://vk.com/wall-1_123"},
            )
        )
        session.add(
            PromoExposure(
                campaign_id=cid,
                activity_id=rep_id,
                event_id=ev_id,
                surface=PROMO_SURFACE_VK_REPOST,
                placement_kind="repost",
                publish_status="VK_SCHEDULED",
                public_target_count=1,
                public_targets_json=[{"type": "vk_wall", "url": "https://vk.com/wall-2_55"}],
                published_at=now_utc - timedelta(hours=1),
                details_json={
                    "target_url": "https://vk.com/wall-2_55",
                    "source_url": "https://vk.com/wall-1_123",
                },
            )
        )
        await session.commit()
        camp_obj = await session.get(PromoCampaign, cid)

    text = await _campaign_stats_text(db, camp_obj)
    # Per-activity sections present with humanized labels.
    assert "📢 VK-публикация" in text
    assert "📨 VK-репост" in text
    # VK_SCHEDULED counted (the under-count fix): window + total.
    assert "промо-действий за 24ч: 1 / цель 2" in text
    # Concrete clickable links shown, with repost source link.
    assert "vk.com/wall-1_123" in text
    assert "vk.com/wall-2_55" in text
    assert "←" in text

    # Humanized card labels for the new surfaces.
    pub_label = _humanize_activity(pub)
    assert "📢 VK-публикация" in pub_label
    assert "vk.com/klgdevents" in pub_label
    assert "минимум 2/24ч" in pub_label
    rep_label = _humanize_activity(rep)
    assert "vk.com/klgdevents → vk.com/kenigeventsofficial" in rep_label
