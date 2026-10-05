from datetime import date, datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import select
from db import Database
from models import Event, User, PromoCampaign, PromoTarget, PromoActivity
from promo import (PartnerPromoSpec, create_partner_event_promo_campaign,
                   PROMO_SURFACE_VIDEO_GENERAL, PROMO_POLICY_FIRST_SLOT)

NOW = datetime(2026,9,6,10,tzinfo=timezone.utc)


@pytest_asyncio.fixture
async def database(tmp_path, monkeypatch):
    monkeypatch.setenv('DB_INIT_SKIP_VK_SOURCES_SEED', '1')
    db=Database(str(tmp_path/'promo.sqlite'))
    await db.init()
    async with db.get_session() as session:
        event=Event(title='Real event',description='D',date='2026-10-01',time='19:00',location_name='Hall',source_text='S')
        session.add(event)
        await session.commit()
        await session.refresh(event)
        event_id=event.id
    yield db,event_id
    await db.close()


def spec(event_id,creator=None):
    return PartnerPromoSpec(event_id=event_id,creator_user_id=creator,organization_name='Display organization',
        surface=PROMO_SURFACE_VIDEO_GENERAL,profile_key='profile',slot_policy=PROMO_POLICY_FIRST_SLOT,
        count=3,ends_at=date(2026,11,1),is_editorial=False,sponsorship_disclosure='Disclosure')


@pytest.mark.asyncio
async def test_oauth_null_creator_never_looks_up_user(database,monkeypatch):
    db,event_id=database
    async with db.get_session() as session:
        original=session.get
        async def guarded(model,*args,**kwargs):
            assert model is not User
            return await original(model,*args,**kwargs)
        monkeypatch.setattr(session,'get',guarded)
        result=await create_partner_event_promo_campaign(db,spec(event_id),now_utc=NOW,session=session)
        assert result.status=='created'
        assert result.campaign.created_by is None
        assert result.campaign.title.startswith('Display organization ·')
        assert result.campaign.sponsorship_disclosure=='Disclosure'
        assert result.campaign.total_exposure_goal==3
        assert result.campaign.ends_at.date()==date(2026,10,1)
        await session.commit()
    async with db.get_session() as session:
        assert not (await session.execute(select(User))).scalars().all()
        assert len((await session.execute(select(PromoCampaign))).scalars().all())==1
        assert len((await session.execute(select(PromoTarget))).scalars().all())==1
        assert len((await session.execute(select(PromoActivity))).scalars().all())==2


@pytest.mark.asyncio
async def test_oauth_caller_rollback_atomic(database):
    db,event_id=database
    async with db.get_session() as session:
        result=await create_partner_event_promo_campaign(db,spec(event_id),now_utc=NOW,session=session)
        assert result.campaign.id
        await session.rollback()
    async with db.get_session() as session:
        for model in (PromoCampaign,PromoTarget,PromoActivity,User):
            assert not (await session.execute(select(model))).scalars().all()


@pytest.mark.asyncio
@pytest.mark.parametrize('superadmin,prefix',[(False,'Display organization'),(True,'editorial')])
async def test_legacy_real_telegram_creator_unchanged(database,superadmin,prefix):
    db,event_id=database
    async with db.get_session() as session:
        session.add(User(user_id=123,username='real-user',is_partner=True,is_superadmin=superadmin))
        await session.commit()
    result=await create_partner_event_promo_campaign(db,spec(event_id,123),now_utc=NOW)
    assert result.status=='created'
    assert result.campaign.created_by==123
    assert result.campaign.title.startswith(prefix+' ·')
    assert result.campaign.sponsorship_disclosure=='Disclosure'


@pytest.mark.asyncio
async def test_null_creator_does_not_bypass_eligibility(database):
    db,event_id=database
    async with db.get_session() as session:
        event=await session.get(Event,event_id)
        event.silent=True
        await session.commit()
    result=await create_partner_event_promo_campaign(db,spec(event_id),now_utc=NOW)
    assert result.status=='not_eligible'
    assert result.campaign is None
