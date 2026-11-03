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
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_digest_campaign_schedule_cadence_pause_and_restart(tmp_path, monkeypatch, lost_reply):
    import promo
    import guide_excursions.visual_digest as visual
    db = Database(str(tmp_path / "db.sqlite"))
    await db.init()
    now = datetime(2026, 11, 3, 8, 30, tzinfo=timezone.utc)
    calls = []

    async def group(ref):
        return int(ref)

    async def live(*args, post_id, **kwargs):
        return post_id

    async def at(url):
        return now

    async def publish(*args, **kwargs):
        calls.append(kwargs)
        if lost_reply:
            raise TimeoutError("Reply lost after VK accepted the repost")
        return f"https://vk.com/wall-{kwargs['target_group_id']}_{len(calls)}"

    async def api(method, params, *args, **kwargs):
        assert method == "wall.get"
        items = []
        for idx, call in enumerate(calls, 1):
            if -call["target_group_id"] == params["owner_id"]:
                owner, post = promo._vk_owner_post_from_url(call["source_url"])
                items.append({"id": idx, "copy_history": [{"owner_id": owner, "id": post}]})
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
        activities = [PromoActivity(campaign_id=c.id, surface="vk_repost", daily_cap=1,
                     config_json={"source_group": 100, "target_group": dest, "every_days": days})
                     for dest, days in [(200, 1), (300, 3)]]
        session.add_all(activities)
        await session.commit()
        cid = c.id
        aids = [a.id for a in activities]
    async def edition(issue, at):
        async with db.raw_conn() as conn:
            await conn.execute("CREATE TABLE IF NOT EXISTS guide_digest_issue (id INTEGER PRIMARY KEY, family TEXT, status TEXT, published_at TEXT, published_targets_json TEXT)")
            await conn.execute("INSERT INTO guide_digest_issue(id,family,status,published_at,published_targets_json) VALUES (?, 'visual_schedule', 'published', ?, ?)",
                (issue, at.isoformat(), json.dumps({"vk:source": {"group_id": 100, "post_urls": [f"https://vk.com/wall-100_{issue}"]}})))
            await conn.commit()
    try:
        await edition(1, now)
        initial = await run_promo_vk_activities(db, now_utc=now + timedelta(minutes=1))
        assert [r.status for r in initial] == ["scheduled", "scheduled"]
        assert calls == []
        key = request_key(1, "promo_vk_repost", f"campaign:{cid}:activity:{aids[0]}:group:200")
        frozen = await get_delivery(db, key)
        delay = (datetime.fromisoformat(frozen["payload"]["due_at"]) - now).total_seconds()/60
        assert 30 <= delay <= 175
        await run_promo_vk_activities(db, now_utc=now + timedelta(minutes=2))
        assert (await get_delivery(db, key))["payload"] == frozen["payload"]
        await run_promo_vk_activities(db, now_utc=now + timedelta(hours=3))
        assert len(calls) == 2
        await run_promo_vk_activities(db, now_utc=now + timedelta(hours=4))
        assert len(calls) == 2
        assert (await get_delivery(db, key))["state"] == "published"
        # No fake Event or event-level exposure is needed for a digest.
        async with db.raw_conn() as conn:
            assert (await (await conn.execute("SELECT count(*) FROM promo_exposure")).fetchone())[0] == 0
        now += timedelta(days=1)
        await edition(2, now)
        await run_promo_vk_activities(db, now_utc=now + timedelta(hours=3))
        assert len(calls) == 3 and calls[-1]["target_group_id"] == 200
        await run_promo_vk_activities(db, now_utc=now + timedelta(hours=4))
        now += timedelta(days=2)
        await edition(3, now)
        await run_promo_vk_activities(db, now_utc=now + timedelta(hours=3))
        assert len(calls) == 5
        await run_promo_vk_activities(db, now_utc=now + timedelta(hours=4))
        assert len(calls) == 5
        await edition(33, now)
        capped = await run_promo_vk_activities(db, now_utc=now + timedelta(hours=5))
        assert all(r.reason in {"dedup_window", "daily_cap_reached"} for r in capped)
        assert len(calls) == 5
        async with db.get_session() as session:
            campaign = await session.get(PromoCampaign, cid)
            campaign.status = "paused"
            await session.commit()
        now += timedelta(days=3)
        await edition(34, now)
        assert await run_promo_vk_activities(db, now_utc=now + timedelta(hours=3)) == []
        assert len(calls) == 5
    finally:
        await db.close()
