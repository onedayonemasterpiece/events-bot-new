"""Existing promo service composes atomically; no MCP or production mutations."""
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import func, select

from db import Database
from models import PromoActivity, PromoCampaign, PromoTarget
from promo import (PartnerPromoSpec, PartnerActivitySpec, create_partner_event_promo_campaign,
                   add_partner_activity_to_campaign, PROMO_SURFACE_VIDEO_GENERAL,
                   PROMO_SURFACE_VK_REPOST, PROMO_POLICY_FIRST_SLOT)
from test_partner_promo import _event, _partner

NOW = datetime(2026, 5, 18, 8, tzinfo=timezone.utc)


async def setup(tmp_path):
    db = Database(str(tmp_path / 'promo.sqlite'))
    await db.init()
    async with db.get_session() as session:
        event = _event('Atomic promo fixture', '2026-06-01')
        session.add(event)
        session.add(_partner(100))
        await session.commit()
        spec = PartnerPromoSpec(event_id=event.id, creator_user_id=100,
            organization_name='Fixture', surface=PROMO_SURFACE_VIDEO_GENERAL,
            profile_key='default', slot_policy=PROMO_POLICY_FIRST_SLOT, count=3,
            ends_at=date(2026, 5, 25), is_editorial=False, sponsorship_disclosure='Partner')
    return db, spec


async def counts(db):
    async with db.get_session() as session:
        return tuple([await session.scalar(select(func.count()).select_from(model))
                      for model in (PromoCampaign, PromoTarget, PromoActivity)])


@pytest.mark.asyncio
async def test_failure_after_campaign_flush_leaves_no_partial_active_campaign(tmp_path, monkeypatch):
    import promo
    db, spec = await setup(tmp_path)
    def broken_default(*args, **kwargs):
        raise RuntimeError('failure before activity persistence')
    monkeypatch.setattr(promo, '_default_tg_button_highlight_activity', broken_default)
    try:
        with pytest.raises(RuntimeError, match='failure before'):
            await create_partner_event_promo_campaign(db, spec, now_utc=NOW)
        assert await counts(db) == (0, 0, 0)
    finally:
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('commit', [False, True])
async def test_caller_transaction_owns_campaign_target_and_both_activities(tmp_path, commit):
    db, spec = await setup(tmp_path)
    try:
        async with db.get_session() as session:
            result = await create_partner_event_promo_campaign(db, spec, now_utc=NOW, session=session)
            assert result.status == 'created' and result.campaign.id
            assert await counts(db) == (0, 0, 0)  # Flushed is not committed.
            if commit:
                await session.commit()
            else:
                await session.rollback()
        assert await counts(db) == ((1, 1, 2) if commit else (0, 0, 0))
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_caller_can_rollback_activity_without_changing_campaign_or_existing_activity(tmp_path):
    db, spec = await setup(tmp_path)
    try:
        created = await create_partner_event_promo_campaign(db, spec, now_utc=NOW)
        assert await counts(db) == (1, 1, 2)
        async with db.get_session() as session:
            campaign = await session.get(PromoCampaign, created.campaign.id)
            campaign.status = 'paused'
            await session.commit()
        async with db.get_session() as session:
            result = await add_partner_activity_to_campaign(db,
                PartnerActivitySpec(campaign_id=created.campaign.id, surface=PROMO_SURFACE_VK_REPOST,
                                    profile_key=None, slot_policy='diverse_shuffle', count=2),
                actor_user_id=100, now_utc=NOW, session=session)
            assert result.status == 'created' and result.campaign.status == 'paused'
            await session.rollback()
        assert await counts(db) == (1, 1, 2)
        async with db.get_session() as session:
            assert (await session.get(PromoCampaign, created.campaign.id)).status == 'paused'
    finally:
        await db.close()
