import json
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from models import PromoCampaign, PromoActivity, PromoTarget
from db import Database
from promo import run_promo_vk_activities
from guide_excursions.max_delivery import get_delivery, request_key


@pytest.mark.asyncio
@pytest.mark.parametrize("afisha_cap", [1, None])
@pytest.mark.parametrize("lost_reply", [False, True])
@pytest.mark.parametrize("late_start", [False, True])
async def test_digest_campaign_schedule_cadence_pause_and_restart(tmp_path, monkeypatch, lost_reply, late_start, afisha_cap):
    import promo
    import guide_excursions.visual_digest as visual
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime(2026, 11, 3, 8, 30, tzinfo=timezone.utc)
    calls = []
    tick = now

    async def run(at):
        nonlocal tick
        tick = at
        return await run_promo_vk_activities(db, now_utc=at)

    async def group(ref):
        return int(ref)

    async def live(*args, post_id, **kwargs):
        return post_id

    async def at(url):
        return now

    async def publish(*args, **kwargs):
        calls.append(dict(kwargs, sent_at=tick))
        if lost_reply:
            raise TimeoutError("Reply lost after VK accepted the repost")
        return f"https://vk.com/wall-{kwargs['target_group_id']}_{len(calls)}"

    async def api(method, params, *args, **kwargs):
        assert method == "wall.get"
        items = []
        for idx, call in enumerate(calls, 1):
            if -call["target_group_id"] == params["owner_id"]:
                owner, post = promo._vk_owner_post_from_url(call["source_url"])
                items.append({"id": idx, "date": int(call["sent_at"].timestamp()), "copy_history": [{"owner_id": owner, "id": post}]})
        return {"response": {"items": items}}
    monkeypatch.setitem(sys.modules, "main", SimpleNamespace(_vk_api=api, _vk_user_token=lambda: "test"))
    monkeypatch.setattr(promo, "_resolve_vk_group_id", group)
    monkeypatch.setattr(promo, "_vk_post_datetime", at)
    monkeypatch.setattr(promo, "_publish_vk_repost", publish)
    monkeypatch.setattr(visual, "_resolve_live_vk_wall_post_id", live)
    async with db.get_session() as session:
        c = PromoCampaign(title="Excursion digest", status="active", starts_at=now-timedelta(days=1))
        session.add(c)
        await session.flush()
        session.add(PromoTarget(campaign_id=c.id, target_type="guide_digest", query_text="visual_schedule"))
        activities = [PromoActivity(campaign_id=c.id, surface="vk_repost", daily_cap=afisha_cap if dest == 200 else 1,
                     config_json={"source_group": 100, "target_group": dest, "every_days": days})
                     for dest, days in [(200, 1), (300, 3)]]
        session.add_all(activities)
        await session.commit()
        cid = c.id
        aids = [a.id for a in activities]
    async def edition(issue, at, family="visual_schedule"):
        async with db.raw_conn() as conn:
            await conn.execute("CREATE TABLE IF NOT EXISTS guide_digest_issue (id INTEGER PRIMARY KEY, family TEXT, status TEXT, published_at TEXT, published_targets_json TEXT)")
            await conn.execute("INSERT INTO guide_digest_issue(id,family,status,published_at,published_targets_json) VALUES (?, ?, 'published', ?, ?)",
                (issue, family, at.isoformat(), json.dumps({"vk:source": {"group_id": 100, "post_urls": [f"https://vk.com/wall-100_{issue}"]}})))
            await conn.commit()
    try:
        await edition(1, now)
        initial = await run(now + timedelta(minutes=1))
        assert [r.status for r in initial] == ["scheduled", "scheduled"]
        assert calls == []
        key = request_key(1, "promo_vk_repost", f"campaign:{cid}:activity:{aids[0]}:group:200")
        frozen = await get_delivery(db, key)
        delay = (datetime.fromisoformat(frozen["payload"]["due_at"]) - now).total_seconds()/60
        assert 30 <= delay <= 175
        await run(now + timedelta(minutes=2))
        assert (await get_delivery(db, key))["payload"] == frozen["payload"]
        other_key = request_key(1, "promo_vk_repost", f"campaign:{cid}:activity:{aids[1]}:group:300")
        other = await get_delivery(db, other_key)
        dues = [datetime.fromisoformat(r["payload"]["due_at"]) for r in [frozen, other]]
        assert abs(dues[0] - dues[1]) >= timedelta(minutes=35)
        first_tick = now + timedelta(hours=3) if late_start else min(dues)
        second_tick = first_tick + timedelta(minutes=35) if late_start else max(dues)
        await run(first_tick)
        assert len(calls) == 1, "Overdue destinations must not be published together"
        await run(first_tick + timedelta(minutes=5))
        assert len(calls) == 1
        await run(second_tick)
        assert len(calls) == 2
        await run(now + timedelta(hours=4))
        assert len(calls) == 2
        assert (calls[1]["sent_at"] - calls[0]["sent_at"]).total_seconds() >= 1800
        assert (await get_delivery(db, key))["state"] == "published"
        # No fake Event or event-level exposure is needed for a digest.
        async with db.raw_conn() as conn:
            assert (await (await conn.execute("SELECT count(*) FROM promo_exposure")).fetchone())[0] == 0
        now += timedelta(days=1)
        await edition(2, now)
        await run(now + timedelta(hours=3))
        assert len(calls) == 3 and calls[-1]["target_group_id"] == 200
        await run(now + timedelta(hours=4))
        now += timedelta(days=2)
        await edition(3, now)
        await run(now + timedelta(hours=3))
        assert len(calls) == 4
        await run(now + timedelta(hours=3, minutes=5))
        assert len(calls) == 4
        await run(now + timedelta(hours=3, minutes=35))
        assert len(calls) == 5
        await run(now + timedelta(hours=4))
        assert len(calls) == 5
        if afisha_cap is None:
            async with db.get_session() as session:
                session.add(PromoTarget(campaign_id=cid, target_type="guide_digest", query_text="new_occurrences"))
                await session.commit()
        await edition(33, now, "new_occurrences" if afisha_cap is None else "visual_schedule")
        capped = await run(now + timedelta(hours=5))
        if afisha_cap is None:
            assert len(calls) == 6 and calls[-1]["target_group_id"] == 200
        else:
            assert all(r.reason in {"dedup_window", "daily_cap_reached"} for r in capped)
            assert len(calls) == 5
        async with db.get_session() as session:
            campaign = await session.get(PromoCampaign, cid)
            campaign.status = "paused"
            await session.commit()
        now += timedelta(days=3)
        await edition(34, now)
        assert await run(now + timedelta(hours=3)) == []
        assert len(calls) == (6 if afisha_cap is None else 5)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_late_vk_receipt_and_corrected_canonical_source_share_the_normal_edition_claim(tmp_path, monkeypatch):
    import promo
    import promo_guide_digest as runner
    import guide_excursions.visual_digest as visual
    from unittest.mock import AsyncMock
    from guide_excursions.max_delivery import prepare_delivery

    db = Database(str(tmp_path / "late.sqlite"))
    await db.init()
    now = datetime(2026,11,17,8,tzinfo=timezone.utc)
    campaign = PromoCampaign(id=14,title="Afisha",status="active")
    activity = PromoActivity(id=48,campaign_id=14,surface="vk_repost",config_json={"source_group":100,"target_group":200})
    target = PromoTarget(campaign_id=14,target_type="guide_digest",query_text="visual_schedule")
    sent = AsyncMock(return_value="https://vk.com/wall-200_22")
    monkeypatch.setattr(promo,"_publish_vk_repost",sent)
    monkeypatch.setattr(visual,"_resolve_live_vk_wall_post_id",AsyncMock(side_effect=lambda *a,post_id,**kw:post_id))
    actual = {411: now-timedelta(minutes=10), 413: now, 414: now+timedelta(hours=2)}
    monkeypatch.setattr(promo,"_vk_post_datetime",AsyncMock(side_effect=lambda url: actual[int(url.rsplit("_",1)[-1])]))
    monkeypatch.setattr(runner.random.SystemRandom,"choice",lambda self,slots:slots[0])
    def canonical(post):return json.dumps({"vk":{"group_id":100,"post_urls":[f"https://vk.com/wall-100_{post}"]}})
    async def tick(at):
        return await runner.run_digest_repost(db,None,campaign=campaign,activity=activity,target=target,now_utc=at)
    try:
        async with db.raw_conn() as conn:
            await conn.execute("CREATE TABLE IF NOT EXISTS guide_digest_issue(id INTEGER PRIMARY KEY,family,status,published_at,published_targets_json)")
            await conn.executemany("INSERT INTO guide_digest_issue(id,family,status,published_at,published_targets_json) VALUES (?,'visual_schedule','published',?,?)",
                                   [(350,"2026-11-16 08:00",canonical(411)),(351,now.isoformat(),canonical(413))])
            await conn.commit()
        assert (await tick(now)).source_url.endswith("_413")
        vk = await prepare_delivery(db,issue_id=350,provider="vk",target_id="100",payload={})
        async with db.raw_conn() as conn:
            await conn.execute("UPDATE guide_visual_delivery SET state='published',updated_at=? WHERE request_key=?",(now.isoformat(),vk["request_key"]))
            await conn.commit()
        assert (await tick(now)).source_url.endswith("_411")
        await tick(now+timedelta(minutes=20))
        key = request_key(351,"promo_vk_repost","campaign:14:activity:48:group:200")
        # Source changes while its own destination is still only prepared.
        async with db.raw_conn() as conn:
            await conn.execute("UPDATE guide_digest_issue SET published_targets_json=? WHERE id=351",(canonical(414),))
            await conn.commit()
        assert (await tick(now+timedelta(hours=2))).status == "scheduled"
        updated = await get_delivery(db,key)
        assert updated["payload"]["source_url"].endswith("_414")
        assert datetime.fromisoformat(updated["payload"]["due_at"]) == now+timedelta(hours=2,minutes=30)
        await tick(now+timedelta(hours=3))
        await tick(now+timedelta(hours=3,minutes=5))
        assert [a.kwargs["source_url"] for a in sent.await_args_list] == ["https://vk.com/wall-100_411","https://vk.com/wall-100_414"]
    finally:
        await db.close()
