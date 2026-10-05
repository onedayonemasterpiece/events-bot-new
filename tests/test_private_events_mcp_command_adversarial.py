"""Policy changes, campaign state CAS, collision and media boundary probes."""

from datetime import date, timedelta
import pytest
from sqlalchemy import select
from models import Event, PromoExposure, PromoCampaign
from test_private_events_mcp_application_commands import setup, request
from private_events_mcp.tool_catalog import ToolExecutionError


@pytest.mark.asyncio
async def test_collision_and_range_null_end_date(config, tmp_path):
    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    async with db.get_session() as session:
        e = await session.get(Event, eid)
        e.end_date = (date.today() + timedelta(days=21)).isoformat()
        session.add(e)
        other = Event(
            title=e.title,
            source_text="Other source",
            description="Other",
            date=e.date,
            time="20:00",
            location_name=e.location_name,
            city=e.city,
        )
        session.add(other)
        await session.commit()
        day = e.date
    p = await events.prepare(
        owner, "reschedule", request(eid, changes={"time": "20:00"}), "collision-op"
    )
    with pytest.raises(ToolExecutionError) as exc:
        await events.commit(owner, p["preparation_ref"], p["action_digest"])
    assert exc.value.error_code == "EVENT_IDENTITY_CONFLICT"
    with pytest.raises(ToolExecutionError):
        await events.prepare(
            owner, "reschedule", request(eid, changes={"date": day}), "range-implicit"
        )
    p = await events.prepare(
        owner,
        "reschedule",
        request(eid, changes={"date": day, "end_date": None}),
        "range-explicit",
    )
    await events.commit(owner, p["preparation_ref"], p["action_digest"])
    async with db.get_session() as session:
        assert (await session.get(Event, eid)).end_date is None


@pytest.mark.asyncio
async def test_policy_change_before_review_and_idempotency_conflict(config, tmp_path):
    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    p = await events.prepare(a, "postpone", request(eid), "policy-change")
    with pytest.raises(ToolExecutionError) as exc:
        await events.prepare(
            a, "postpone", request(eid, organizer_comment="different"), "policy-change"
        )
    assert exc.value.error_code == "IDEMPOTENCY_CONFLICT"
    store.change(
        a.subject.split(":")[1], action="portfolio", expected_revision=1, event_ids=[]
    )
    with pytest.raises(ToolExecutionError):
        await events.commit(a, p["preparation_ref"], p["action_digest"])
    async with db.get_session() as session:
        assert (await session.get(Event, eid)).lifecycle_status == "active"


@pytest.mark.asyncio
async def test_promo_archive_replay_limits_and_event_revision(config, tmp_path):
    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    cap = await promo.capabilities(a, eid)
    req = dict(
        event_id=eid,
        event_revision=cap["event_revision"],
        surface="vk_repost",
        profile_key=None,
        slot_policy=None,
        count=2,
        ends_at=(date.today() + timedelta(days=10)).isoformat(),
        is_editorial=True,
        sponsorship_disclosure=None,
        title_override=None,
    )
    with pytest.raises(ToolExecutionError):
        await promo.prepare(a, "promo_create", {**req, "count": 4}, "limit-exceeded")
    p = await promo.prepare(a, "promo_create", req, "promo-create")
    await promo.operation(a, p["preparation_ref"], p["action_digest"])
    made = await promo.operation(
        owner, p["preparation_ref"], p["action_digest"], "approve"
    )
    cid = made["result"]["campaign_id"]
    async with db.get_session() as session:
        session.add(
            PromoExposure(
                campaign_id=cid,
                event_id=eid,
                surface="vk_repost",
                placement_kind="partner",
                publish_status="published",
                public_target_count=1,
            )
        )
        await session.commit()
    detail = await promo.campaign_get(a, cid)
    pending = await promo.prepare(
        a,
        "promo_activity_add",
        dict(
            campaign_id=cid,
            campaign_revision=detail["campaign_revision"],
            surface="vk_repost",
            profile_key=None,
            slot_policy=None,
            count=1,
        ),
        "add-activity",
    )
    change = await events.prepare(
        owner, "reschedule", request(eid, changes={"time": "21:00"}), "change-target"
    )
    await events.commit(owner, change["preparation_ref"], change["action_digest"])
    with pytest.raises(ToolExecutionError) as exc:
        await promo.operation(a, pending["preparation_ref"], pending["action_digest"])
    assert exc.value.error_code == "STALE_EVENT_REVISION"
    detail = await promo.campaign_get(a, cid)
    state = await promo.prepare(
        owner,
        "promo_archive",
        dict(campaign_id=cid, campaign_revision=detail["campaign_revision"]),
        "archive-one",
    )
    await promo.operation(owner, state["preparation_ref"], state["action_digest"])
    await promo.operation(a, p["preparation_ref"], p["action_digest"])
    assert (await promo.campaign_get(a, cid))["campaign"]["status"] == "archived"
    with pytest.raises(ToolExecutionError):
        await promo.prepare(
            owner,
            "promo_resume",
            dict(
                campaign_id=cid,
                campaign_revision=(await promo.campaign_get(a, cid))[
                    "campaign_revision"
                ],
            ),
            "restore-forbidden",
        )
    change = await events.prepare(owner, "cancel", request(eid), "cancel-history")
    await events.commit(owner, change["preparation_ref"], change["action_digest"])
    async with db.get_session() as session:
        assert (await session.execute(select(PromoExposure))).scalars().all()
        assert (await session.get(PromoCampaign, cid)).total_exposure_goal == 2


@pytest.mark.asyncio
async def test_public_notice_gate_and_superseded_recovery(config, tmp_path):
    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    seen = []

    async def gate(value, proposed, mode):
        seen.append((value, proposed["lifecycle_status"]))
        raise ToolExecutionError("PUBLIC_TEXT_FACT_CONFLICT")

    events.text_gate = gate
    with pytest.raises(ToolExecutionError):
        await events.prepare(
            owner,
            "cancel",
            request(eid, public_notice="Still active"),
            "notice-conflict",
        )
    assert seen == [("Still active", "cancelled")]

    async def broken(*args):
        raise RuntimeError("scheduler unavailable")

    events.reconcile = broken
    p = await events.prepare(
        owner, "reschedule", request(eid, changes={"time": "20:00"}), "old-reconcile"
    )
    await events.commit(owner, p["preparation_ref"], p["action_digest"])
    newer = await events.prepare(
        owner, "reschedule", request(eid, changes={"time": "21:00"}), "new-reconcile"
    )
    await events.commit(owner, newer["preparation_ref"], newer["action_digest"])
    scheduled = []

    async def record(event, operation):
        scheduled.append(operation["operation_ref"])

    events.reconcile = record
    await events.recover()
    assert scheduled == [newer["operation_ref"]]
    assert (await events.get(owner, p["operation_ref"]))["result"][
        "publication_status"
    ] == "superseded"


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["add", "replace"])
async def test_media_uses_verified_staged_bytes_and_existing_gate(
    config, tmp_path, monkeypatch, action
):
    import main
    import event_media
    from types import SimpleNamespace
    from models import EventPoster
    from private_events_mcp.event_assets import EventAssetService
    from private_events_mcp.crypto import AccessIdentity
    from private_events_mcp.tool_catalog import ToolCallContext
    from test_private_events_mcp_media_store import make_store
    from test_private_events_mcp_event_assets import file, allowed

    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    storage, fetcher = make_store(tmp_path / "media")
    events.assets = EventAssetService(
        ingestor=storage, binding_key="k" * 32, authorize=allowed, clock=lambda: 1000
    )
    ctx = ToolCallContext(
        AccessIdentity(
            owner.subject,
            owner.client_id,
            owner.scopes,
            owner.audience,
            "token",
            owner.expires_at,
        ),
        owner.audience,
    )
    staged = await events.assets.stage(file(), ctx)

    async def process(raw, **kwargs):
        assert raw[0][0].startswith(b"\x89PNG") and kwargs["need_ocr"] is False
        return [
            SimpleNamespace(
                digest=staged["content_digest"].split(":")[1],
                catbox_url="https://example.test/new.png",
                supabase_url=None,
            )
        ], []

    async def materialize(poster):
        return True

    monkeypatch.setattr(main, "process_media", process)
    monkeypatch.setattr(
        event_media, "materialize_event_media_candidate_to_cdn", materialize
    )
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "0")
    async with db.get_session() as session:
        old = EventPoster(
            event_id=eid,
            poster_hash="f" * 64,
            catbox_url="https://example.test/old.png",
            review_status="approved",
        )
        session.add(old)
        await session.commit()
        old_id = old.id
    p = await events.prepare(
        owner,
        "edit",
        request(
            eid,
            media={
                "action": action,
                "images": [
                    {
                        "asset_ref": staged["asset_ref"],
                        "content_digest": staged["content_digest"],
                    }
                ],
            },
        ),
        "media-" + action,
    )
    await events.commit(owner, p["preparation_ref"], p["action_digest"])
    async with db.get_session() as session:
        posters = list((await session.execute(select(EventPoster))).scalars())
        assert len(posters) == 2
        assert (await session.get(EventPoster, old_id)).review_status == (
            "rejected" if action == "replace" else "approved"
        )
    assert len(fetcher.calls) == 1
