"""Hero is an activity of existing campaigns, never a second lifecycle."""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from db import Database
from models import Event, PromoActivity, PromoCampaign, PromoTarget
from promo import (PartnerActivitySpec, add_partner_activity_to_campaign,
                   hero_activity_eligibility, validate_hero_activity_config)

NOW = datetime(2026, 9, 6, 10, tzinfo=timezone.utc)
CONFIG = {"placements": {"home_hero": True, "page_end": False}, "content_ref": "pack_123"}


@pytest.mark.parametrize("patch", [
    {"campaign_status": "active"}, {"budget": 4}, {"placements": {"home_hero": "false"}},
    {"placements": {"unknown": True}}, {"session_cap": True}, {"session_cap": 0},
    {"content_ref": "https://private/asset"},
])
def test_hero_config_rejects_lifecycle_and_untyped_inputs(patch):
    with pytest.raises(ValueError):
        validate_hero_activity_config({**CONFIG, **patch})


async def fixture_campaign(tmp_path, *, status="active", goal=None):
    db = Database(str(tmp_path / "hero.sqlite"))
    await db.init()
    async with db.get_session() as session:
        campaign = PromoCampaign(title="Existing", status=status, priority=1,
                                 starts_at=NOW - timedelta(days=1), ends_at=NOW + timedelta(days=20),
                                 total_exposure_goal=goal, created_by=71)
        session.add(campaign)
        await session.commit()
        await session.refresh(campaign)
        session.add(PromoTarget(campaign_id=campaign.id, target_type="festival", festival_name="Program"))
        old = PromoActivity(campaign_id=campaign.id, surface="video_general", enabled=True,
                            config_json={"foreign": "keep"}, target_exposure_goal=5)
        session.add(old)
        await session.commit()
        await session.refresh(old)
        return db, campaign.id, old.id


async def add_hero(db, campaign_id):
    return await add_partner_activity_to_campaign(db, PartnerActivitySpec(
        campaign_id=campaign_id, surface="hero_talk", profile_key=None,
        slot_policy="qualified_visibility", count=None, config=CONFIG), actor_user_id=71, now_utc=NOW)


@pytest.mark.asyncio
async def test_existing_campaign_preserved_and_pause_wins(tmp_path):
    """MCP-HT-01/MCP-HT-02 service slice; not production acceptance."""
    db, cid, old_id = await fixture_campaign(tmp_path, status="paused")
    assert (await add_hero(db, cid)).status == "created"
    async with db.get_session() as session:
        rows = list((await session.execute(select(PromoActivity).where(PromoActivity.campaign_id == cid))).scalars())
        assert len(rows) == 2
        old = await session.get(PromoActivity, old_id)
        assert old.enabled and old.config_json == {"foreign": "keep"} and old.target_exposure_goal == 5
        campaign = await session.get(PromoCampaign, cid)
        assert (campaign.status, campaign.created_by, campaign.priority) == ("paused", 71, 1)
        hero = next(row for row in rows if row.surface == "hero_talk")
        assert hero.target_exposure_goal is None
    result = await hero_activity_eligibility(db, activity_id=hero.id, placement="home_hero", now_utc=NOW)
    assert result["reason"] == "campaign_paused" and not result["eligible"]
    assert result["campaign_id"] == cid and result["content_ref"] == "pack_123"


@pytest.mark.asyncio
async def test_dynamic_program_and_independent_placement_activity_gates(tmp_path):
    """HT-AF-06 dynamic selection; no seeded campaign or content writes."""
    db, cid, old_id = await fixture_campaign(tmp_path)
    await add_hero(db, cid)
    async with db.get_session() as session:
        hero = (await session.execute(select(PromoActivity).where(PromoActivity.surface == "hero_talk"))).scalar_one()
    async def check(placement="home_hero"):
        return await hero_activity_eligibility(db, activity_id=hero.id, placement=placement, now_utc=NOW)
    assert (await check())["reason"] == "target_no_eligible_events"
    async with db.get_session() as session:
        event = Event(title="Approved program event", description="Source", source_text="Source", date="2026-09-12", time="19:00",
                      location_name="Venue", festival="Program")
        session.add(event)
        await session.commit()
        await session.refresh(event)
    assert (await check())["event_ids"] == [event.id]
    assert (await check("page_end"))["reason"] == "placement_off"
    async with db.get_session() as session:
        current = await session.get(PromoActivity, hero.id)
        current.enabled = False
        await session.commit()
    assert (await check())["reason"] == "activity_off"
    async with db.get_session() as session:
        assert (await session.get(PromoActivity, old_id)).enabled
        current = await session.get(PromoActivity, hero.id)
        current.enabled = True
        event_row = await session.get(Event, event.id)
        event_row.lifecycle_status = "cancelled"
        await session.commit()
    assert (await check())["reason"] == "target_no_eligible_events"


@pytest.mark.asyncio
async def test_browser_units_do_not_silently_bypass_existing_cap(tmp_path):
    """HT-AF-18: unsupported publication cap explicitly denied, never recounted."""
    db, cid, _ = await fixture_campaign(tmp_path, goal=10)
    result = await add_hero(db, cid)
    assert result.status == "invalid" and "PUBLICATION_CAP_UNSUPPORTED" in result.message
    async with db.get_session() as session:
        assert not list((await session.execute(select(PromoActivity).where(PromoActivity.surface == "hero_talk"))).scalars())
