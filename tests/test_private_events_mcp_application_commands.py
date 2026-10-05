"""Real canonical transactions, review and replay without MCP transport."""

from dataclasses import replace
from datetime import date, timedelta
import time
import pytest
from sqlalchemy import text
from db import Database
from models import Event, PromoCampaign
from private_events_mcp.actors import ActorContext, CommandPolicy
from private_events_mcp.oauth import SUBJECT
from private_events_mcp.partner_access import (
    PartnerAccessStore,
    PARTNER_SCOPES,
    PARTNER_ACTIONS,
)
from private_events_mcp.event_commands import EventCommandService
from private_events_mcp.promo_commands import PromoCommandService
from private_events_mcp.tool_catalog import ToolExecutionError


async def setup(config, tmp_path):
    c = replace(
        config,
        database_path=str(tmp_path / "db.sqlite"),
        event_create_enabled=True,
        event_operations_enabled=True,
        partner_enabled=True,
        partner_event_create_enabled=True,
        owner_promo_enabled=True,
        partner_promo_enabled=True,
    )
    db = Database(c.database_path)
    await db.init()
    async with db.get_session() as session:
        e = Event(
            title="Concert",
            description="Live music",
            source_text="Organizer",
            date=(date.today() + timedelta(days=20)).isoformat(),
            time="19:00",
            location_name="Hall",
            city="Kaliningrad",
        )
        session.add(e)
        await session.commit()
        eid = e.id
    store = PartnerAccessStore(
        c.database_path, resource=c.partner_resource, signing_key=c.signing_key
    )
    partners = []
    for name in ("a", "b"):
        p = store.create(
            tenant_id=name,
            organization_id=name,
            display_name=name,
            policy={
                "scopes": list(PARTNER_SCOPES),
                "actions": list(PARTNER_ACTIONS),
                "auto_approve": ["event_edit"],
            },
            redirect_uris=["https://example.com/callback"],
            expires_at=int(time.time()) + 86400,
            event_ids=[eid] if name == "a" else [],
        )
        grant = store.get(p["principal_id"])
        partners.append(
            ActorContext(
                grant.subject,
                grant.client_id,
                c.partner_resource,
                grant.scopes,
                int(time.time()) + 3600,
            )
        )
    owner = ActorContext(
        SUBJECT,
        c.oauth_client_id,
        c.resource,
        frozenset(
            {
                "events:read",
                "events:write",
                "operations:read",
                "promo:read",
                "promo:write",
                "partners:manage",
            }
        ),
        int(time.time()) + 3600,
    )
    policy = CommandPolicy(lambda: c, store)

    async def gate(value, proposed, mode):
        return value

    async def reconcile(event, operation):
        pass

    return (
        c,
        db,
        store,
        eid,
        owner,
        *partners,
        EventCommandService(db, policy, text_gate=gate, reconcile=reconcile),
        PromoCommandService(db, policy),
    )


def request(eid, **values):
    return {
        "event_id": eid,
        "source": {"type": "organizer", "external_id": "test-source"},
        "organizer_comment": "Organizer confirmed",
        **values,
    }


@pytest.mark.asyncio
async def test_partner_lifecycle_review_stale_replay_and_tenant(config, tmp_path):
    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    p = await events.prepare(
        a, "reschedule", request(eid, changes={"time": "20:00"}), "reschedule-one"
    )
    r = await events.commit(a, p["preparation_ref"], p["action_digest"])
    assert r["status"] == "review_required"
    async with db.get_session() as session:
        assert (await session.get(Event, eid)).time == "19:00"
    with pytest.raises(ToolExecutionError):
        await events.prepare(b, "cancel", request(eid), "foreign-event")
    r = await events.commit(
        owner, p["preparation_ref"], p["action_digest"], decision="approve"
    )
    assert r["status"] == "accepted"
    async with db.get_session() as session:
        assert (await session.get(Event, eid)).time == "20:00"
    cancel = await events.prepare(owner, "cancel", request(eid), "cancel-after")
    await events.commit(owner, cancel["preparation_ref"], cancel["action_digest"])
    await events.commit(a, p["preparation_ref"], p["action_digest"])
    async with db.get_session() as session:
        assert (await session.get(Event, eid)).lifecycle_status == "cancelled"


@pytest.mark.asyncio
async def test_revoke_after_prepare_and_review_blocks(config, tmp_path):
    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    p = await events.prepare(a, "cancel", request(eid), "revoke-cancel")
    await events.commit(a, p["preparation_ref"], p["action_digest"])
    principal = a.subject.split(":")[1]
    store.change(principal, action="revoke", expected_revision=1)
    with pytest.raises(ToolExecutionError):
        await events.commit(
            owner, p["preparation_ref"], p["action_digest"], decision="approve"
        )
    async with db.get_session() as session:
        assert (await session.get(Event, eid)).lifecycle_status == "active"


@pytest.mark.asyncio
async def test_edit_revision_and_reconciliation_recovery(config, tmp_path):
    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    p = await events.prepare(
        a,
        "edit",
        request(eid, changes={"description": "New approved text"}),
        "edit-text-one",
    )

    async def broken(*args):
        raise RuntimeError("scheduler unavailable")

    events.reconcile = broken
    r = await events.commit(a, p["preparation_ref"], p["action_digest"])
    assert r["status"] == "reconciliation_pending"
    async with db.get_session() as session:
        assert (await session.get(Event, eid)).description == "New approved text"

    async def good(*args):
        pass

    events.reconcile = good
    assert await events.recover() == 1
    assert (await events.get(a, p["operation_ref"]))["status"] == "accepted"
    with pytest.raises(ToolExecutionError):
        await events.prepare(
            a, "edit", request(eid, changes={"date": "2030-01-01"}), "invalid-edit"
        )


@pytest.mark.asyncio
async def test_promo_review_limits_isolation_pause_replay_archive(config, tmp_path):
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
    p = await promo.prepare(a, "promo_create", req, "promo-create-one")
    assert (await promo.operation(a, p["preparation_ref"], p["action_digest"]))[
        "status"
    ] == "review_required"
    async with db.get_session() as session:
        assert (
            await session.execute(text("SELECT count(*) FROM promo_campaign"))
        ).scalar() == 0
    r = await promo.operation(
        owner, p["preparation_ref"], p["action_digest"], "approve"
    )
    cid = r["result"]["campaign_id"]
    assert not (await promo.campaigns_list(b))["campaigns"]
    with pytest.raises(ToolExecutionError):
        await promo.campaign_get(b, cid)
    detail = await promo.campaign_get(a, cid)
    pause = await promo.prepare(
        owner,
        "promo_pause",
        dict(campaign_id=cid, campaign_revision=detail["campaign_revision"]),
        "promo-pause-one",
    )
    await promo.operation(owner, pause["preparation_ref"], pause["action_digest"])
    await promo.operation(a, p["preparation_ref"], p["action_digest"])
    assert (await promo.campaign_get(a, cid))["campaign"]["status"] == "paused"
    async with db.get_session() as session:
        assert (await session.get(PromoCampaign, cid)).created_by is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,changes",
    [
        ("edit", {"description": "One"}),
        ("reschedule", {"time": "21:00"}),
        ("cancel", {}),
        ("postpone", {}),
    ],
)
async def test_two_preparations_conflict_and_no_partial_mutation(
    config, tmp_path, kind, changes
):
    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    first = await events.prepare(
        owner,
        kind,
        request(eid, **({"changes": changes} if changes else {})),
        "first-change",
    )
    other = await events.prepare(
        owner, "reschedule", request(eid, changes={"time": "22:00"}), "second-change"
    )
    await events.commit(owner, other["preparation_ref"], other["action_digest"])
    with pytest.raises(ToolExecutionError) as exc:
        await events.commit(owner, first["preparation_ref"], first["action_digest"])
    assert exc.value.error_code == "STALE_EVENT_REVISION"
    async with db.get_session() as session:
        e = await session.get(Event, eid)
        assert (
            e.time == "22:00"
            and e.lifecycle_status == "active"
            and e.description == "Live music"
        )


@pytest.mark.asyncio
async def test_postpone_then_reschedule_and_frozen_provenance(config, tmp_path):
    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    p = await events.prepare(owner, "postpone", request(eid), "postpone-one")
    await events.commit(owner, p["preparation_ref"], p["action_digest"])
    async with db.get_session() as session:
        e = await session.get(Event, eid)
        assert e.lifecycle_status == "postponed"
        original = e.date
    p = await events.prepare(
        owner,
        "reschedule",
        request(
            eid,
            changes={"date": original, "time": "21:00", "location_name": "New hall"},
        ),
        "known-schedule",
    )
    await events.commit(owner, p["preparation_ref"], p["action_digest"])
    async with db.get_session() as session:
        e = await session.get(Event, eid)
        assert (e.lifecycle_status, e.time, e.location_name) == (
            "active",
            "21:00",
            "New hall",
        )
        row = (
            await session.execute(
                text(
                    "SELECT before_json,after_json,organizer_comment FROM event_change_log WHERE operation_ref=:ref"
                ),
                {"ref": p["operation_ref"]},
            )
        ).first()
        assert (
            "postponed" in row[0]
            and "New hall" in row[1]
            and row[2] == "Organizer confirmed"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("decision", ["conflict", "unresolved", None])
async def test_public_text_gate_fails_closed(monkeypatch, decision):
    import smart_event_update
    from private_events_mcp.event_commands import canonical_text_gate

    async def verdict(*args, **kwargs):
        return {"verdict": decision, "fields": ["time"]} if decision else None

    monkeypatch.setattr(smart_event_update, "_ask_gemma_json", verdict)
    with pytest.raises(ToolExecutionError):
        await canonical_text_gate(
            "Starts at 20:00", {"title": "Concert", "time": "19:00"}, "replace_exact"
        )


@pytest.mark.asyncio
async def test_public_text_omission_and_locked_facts_passed_to_gateway(monkeypatch):
    import smart_event_update
    from private_events_mcp.event_commands import canonical_text_gate

    async def verdict(prompt, *args, **kwargs):
        assert "locked_facts" in prompt and "Only acoustic" in prompt
        return {"verdict": "consistent", "fields": []}

    monkeypatch.setattr(smart_event_update, "_ask_gemma_json", verdict)
    assert (
        await canonical_text_gate(
            "Live music",
            {"title": "Concert", "locked_facts": ["Only acoustic"]},
            "preserve_original",
        )
        == "Live music"
    )


@pytest.mark.parametrize(
    "hours,expected",
    [
        (23, "not_planned"),
        (24, "not_planned"),
        (25, "planned"),
        (None, "notice_review_required"),
    ],
)
def test_notice_uses_authoritative_age_only(hours, expected):
    from datetime import datetime, timezone
    from private_events_mcp.event_commands import notice_verdict

    now = datetime(2030, 1, 2, tzinfo=timezone.utc)
    stamp = (now - timedelta(hours=hours)).isoformat() if hours is not None else None
    assert notice_verdict(stamp, published=True, now=now) == expected
    assert notice_verdict(stamp, published=False, now=now) == "not_planned"


@pytest.mark.asyncio
async def test_job_revision_binding_and_truthful_publication_status(config, tmp_path):
    from private_events_mcp.publication_status import (
        PublicationReadService,
        revision_binding,
        publication_job_is_current,
    )
    from models import JobOutbox, JobTask
    import main

    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    async with db.get_session() as session:
        from static_site_release import event_public_revision

        ev = await session.get(Event, eid)
        rev = event_public_revision(ev)
    token = revision_binding.set((rev, "evt_op_" + "x" * 24))
    try:
        await main.enqueue_job(db, eid, JobTask.telegraph_build)
    finally:
        revision_binding.reset(token)
    service = PublicationReadService(db, events.policy)
    result = await service.read(a, eid)
    page = next(r for r in result["surfaces"] if r["surface"] == "telegraph")
    assert page["transport"] == "queued" and page["revision"] == "update_queued"
    async with db.get_session() as session:
        job = await session.get(JobOutbox, 1)
        assert job.target_event_revision == rev
        e = await session.get(Event, eid)
        e.description = "Changed"
        session.add(e)
        await session.commit()
    assert not await publication_job_is_current(db, job)
    with pytest.raises(ToolExecutionError):
        await service.read(b, eid)


@pytest.mark.asyncio
@pytest.mark.parametrize("uncertain", [False, True])
async def test_lifecycle_worker_receipt_and_no_blind_notice_replay(
    config, tmp_path, monkeypatch, uncertain
):
    import main
    from models import JobOutbox, JobTask, JobStatus
    from static_site_release import event_public_revision
    from private_events_mcp.lifecycle_publication import run_lifecycle_publication

    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    p = await events.prepare(owner, "cancel", request(eid), "notice-cancellation")
    await events.commit(owner, p["preparation_ref"], p["action_digest"])
    async with db.get_session() as session:
        event = await session.get(Event, eid)
        job = JobOutbox(
            event_id=eid,
            task=JobTask.tg_event_publish,
            status=JobStatus.running,
            target_event_revision=event_public_revision(event),
            event_operation_ref=p["operation_ref"],
            payload={
                "publication_kind": "lifecycle_notice",
                "operation_ref": p["operation_ref"],
            },
            coalesce_key="notice:" + p["operation_ref"] + ":telegram",
        )
        session.add(job)
        await session.commit()
        jid = job.id
    calls = []

    async def publish(view, message, *args, **kwargs):
        calls.append(message)
        assert "ОТМЕНЕНО" in message and "Organizer confirmed" not in message
        assert view.tg_event_post_id is None
        if uncertain:
            raise TimeoutError("ambiguous transport result")
        return "https://t.me/events_test/123", 123, "text", "digest"

    monkeypatch.setattr(main, "publish_tg_event_announcement", publish)
    if uncertain:
        with pytest.raises(TimeoutError):
            await run_lifecycle_publication(db, job, object())
        with pytest.raises(RuntimeError):
            await run_lifecycle_publication(db, job, object())
        async with db.get_session() as session:
            current = await session.get(JobOutbox, jid)
            assert (
                current.status == JobStatus.paused
                and current.terminal_reason == "outcome_unknown"
            )
    else:
        assert await run_lifecycle_publication(db, job, object())
        assert await run_lifecycle_publication(db, job, object())
        async with db.get_session() as session:
            receipt = (
                await session.execute(
                    text(
                        "SELECT first_published_at,applied_event_revision FROM event_publication WHERE target LIKE 'notice:%'"
                    )
                )
            ).first()
            assert receipt[0] and receipt[1] == job.target_event_revision
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_media_remove_reorder_use_existing_gallery_projection(
    config, tmp_path, monkeypatch
):
    from models import EventPoster
    from sqlalchemy import select

    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    monkeypatch.setenv("EVENT_MEDIA_REQUIRE_CDN", "0")
    async with db.get_session() as session:
        for index in range(2):
            session.add(
                EventPoster(
                    event_id=eid,
                    poster_hash=str(index) * 64,
                    catbox_url=f"https://example.test/{index}.jpg",
                    review_status="approved",
                    display_order=index,
                )
            )
        await session.commit()
        ids = list(
            (
                await session.execute(
                    select(EventPoster.id)
                    .where(EventPoster.event_id == eid)
                    .order_by(EventPoster.id)
                )
            ).scalars()
        )
    p = await events.prepare(
        owner,
        "edit",
        request(eid, media={"action": "reorder", "poster_ids": list(reversed(ids))}),
        "reorder-images",
    )
    await events.commit(owner, p["preparation_ref"], p["action_digest"])
    async with db.get_session() as session:
        assert (await session.get(EventPoster, ids[1])).display_order == 0
    p = await events.prepare(
        owner,
        "edit",
        request(eid, media={"action": "remove", "poster_ids": [ids[0]]}),
        "remove-image",
    )
    await events.commit(owner, p["preparation_ref"], p["action_digest"])
    async with db.get_session() as session:
        assert (await session.get(EventPoster, ids[0])).review_status == "rejected"
        assert (await session.get(EventPoster, ids[1])).review_status == "approved"
        assert (
            "https://example.test/0.jpg"
            not in (await session.get(Event, eid)).photo_urls
        )
