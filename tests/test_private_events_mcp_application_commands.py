"""Real canonical transactions, review and replay without MCP transport."""

from dataclasses import replace
from datetime import date, timedelta
import time
import pytest
from sqlalchemy import select, text
from db import Database
from models import Event, Organization, PromoActivity, PromoCampaign
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
            photo_urls=["https://example.test/poster.jpg"],
            photo_count=1,
            source_vk_post_url="https://vk.com/wall-101_202",
        )
        session.add_all(
            [
                e,
                Organization(
                    name="a",
                    vk_source_group_ids=[101],
                ),
                Organization(
                    name="b",
                    vk_source_group_ids=[202],
                ),
            ]
        )
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
    assert cap["supported_surfaces"] == ["video_general", "vk_repost"]
    assert cap["available_surfaces"] == ["video_general", "vk_repost"]
    assert cap["surface_verdicts"]["video_general"] == {
        "available": True,
        "reason": None,
    }
    assert cap["surface_verdicts"]["vk_repost"] == {
        "available": True,
        "reason": None,
    }
    assert "konb" not in cap["video_profiles"]
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
    async with db.get_session() as session:
        activity = (
            await session.execute(
                select(PromoActivity).where(
                    PromoActivity.campaign_id == cid,
                    PromoActivity.surface == "vk_repost",
                )
            )
        ).scalars().one()
        assert activity.config_json["source_group"] == "101"
        assert activity.config_json["target_group"] == "kenigeventsofficial"
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
async def test_promo_update_review_clamp_limits_and_owner_priority(config, tmp_path):
    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    cap = await promo.capabilities(a, eid)
    create = dict(
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
    prepared = await promo.prepare(a, "promo_create", create, "promo-update-create")
    await promo.operation(a, prepared["preparation_ref"], prepared["action_digest"])
    accepted = await promo.operation(
        owner, prepared["preparation_ref"], prepared["action_digest"], "approve"
    )
    cid = accepted["result"]["campaign_id"]
    before = await promo.campaign_get(a, cid)
    before_revision = before["campaign_revision"]

    update = await promo.prepare(
        a,
        "promo_update",
        {
            "campaign_id": cid,
            "campaign_revision": before_revision,
            "title": "  Partner renamed  ",
            "ends_at": (date.today() + timedelta(days=90)).isoformat(),
            "total_exposure_goal": 3,
            "daily_exposure_cap": 1,
        },
        "promo-update-partner",
    )
    assert update["proposal"]["title"] == "Partner renamed"
    assert (await promo.operation(
        a, update["preparation_ref"], update["action_digest"]
    ))["status"] == "review_required"
    untouched = await promo.campaign_get(a, cid)
    assert untouched["campaign"]["title"] == before["campaign"]["title"]
    assert untouched["campaign_revision"] == before_revision

    approved = await promo.operation(
        owner, update["preparation_ref"], update["action_digest"], "approve"
    )
    assert approved["status"] == "accepted"
    assert approved["result"]["updated_fields"] == [
        "daily_exposure_cap",
        "ends_at",
        "title",
        "total_exposure_goal",
    ]
    async with db.get_session() as session:
        event = await session.get(Event, eid)
        expected_end = (event.end_date or event.date).split("..", 1)[0]
    assert approved["result"]["effective_ends_at"] == expected_end

    after = await promo.campaign_get(a, cid)
    assert after["campaign"]["title"] == "Partner renamed"
    assert after["campaign"]["total_exposure_goal"] == 3
    assert after["campaign"]["daily_exposure_cap"] == 1
    assert after["campaign_revision"] == approved["result"]["campaign_revision"]
    assert after["campaign_revision"] != before_revision

    for suffix, patch in (
        ("priority", {"priority": 0}),
        ("over-limit", {"total_exposure_goal": 4}),
        ("empty", {}),
        ("null", {"title": None}),
    ):
        with pytest.raises(ToolExecutionError):
            await promo.prepare(
                a,
                "promo_update",
                {
                    "campaign_id": cid,
                    "campaign_revision": after["campaign_revision"],
                    **patch,
                },
                "promo-update-denied-" + suffix,
            )

    owner_update = await promo.prepare(
        owner,
        "promo_update",
        {
            "campaign_id": cid,
            "campaign_revision": after["campaign_revision"],
            "priority": 0,
        },
        "promo-update-owner-priority",
    )
    owner_result = await promo.operation(
        owner, owner_update["preparation_ref"], owner_update["action_digest"]
    )
    assert owner_result["status"] == "accepted"
    async with db.get_session() as session:
        assert (await session.get(PromoCampaign, cid)).priority == 0


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

@pytest.mark.asyncio
async def test_partner_promo_readback_exposes_caps_and_safe_non_delivery_reasons(
    config, tmp_path
):
    from datetime import datetime, timezone
    from models import PromoExposure

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
    prepared = await promo.prepare(a, "promo_create", req, "promo-readback-create")
    await promo.operation(
        a, prepared["preparation_ref"], prepared["action_digest"]
    )
    accepted = await promo.operation(
        owner, prepared["preparation_ref"], prepared["action_digest"], "approve"
    )
    cid = accepted["result"]["campaign_id"]
    initial = await promo.campaign_get(a, cid)
    activity_id = initial["activities"][0]["id"]
    now = datetime.now(timezone.utc)
    async with db.get_session() as session:
        for status, details, public_count in (
            ("PUBLISHED_MAIN", {}, 1),
            ("PUBLISHED_MAIN", {}, 1),
            (
                "FAILED_NO_MEDIA",
                {
                    "reason": "source_unavailable",
                    "provider_payload": "must-not-leak",
                },
                0,
            ),
        ):
            session.add(
                PromoExposure(
                    campaign_id=cid,
                    activity_id=activity_id,
                    event_id=eid,
                    surface="vk_repost",
                    placement_kind="rolling_window_repost",
                    publish_status=status,
                    public_target_count=public_count,
                    published_at=now,
                    details_json=details,
                )
            )
        await session.commit()

    before_outcome = await promo.campaign_get(a, cid)
    from promo import PromoVkActionResult, record_promo_activity_outcomes

    assert await record_promo_activity_outcomes(
        db,
        [
            PromoVkActionResult(
                campaign_id=cid,
                activity_id=activity_id,
                surface="vk_repost",
                event_id=eid,
                status="failed",
                reason="opaque-provider-error-detail",
            )
        ],
        now_utc=now,
    ) == 1
    detail = await promo.campaign_get(a, cid)
    assert detail["campaign_revision"] == before_outcome["campaign_revision"]
    assert "campaign_total_goal_reached" in detail["non_delivery_reasons"]
    assert "activity_goal_reached" in detail["non_delivery_reasons"]
    assert "source_unavailable" in detail["non_delivery_reasons"]
    assert "provider_error" in detail["non_delivery_reasons"]
    activity = detail["activities"][0]
    assert activity["last_outcome_status"] == "failed"
    assert activity["last_outcome_reason"] == "provider_error"
    assert activity["last_outcome_event_id"] == eid
    assert activity["last_attempt_at"]
    failed = next(
        row
        for row in detail["recorded_exposures"]["rows"]
        if row["recorded_publish_status"] == "FAILED_NO_MEDIA"
    )
    assert failed["delivery_reason"] == "source_unavailable"
    rendered = str(detail)
    assert "provider_payload" not in rendered
    assert "opaque-provider-error-detail" not in rendered
    assert detail["delivery_state"] == "recorded_publication"
    assert detail["delivery_evidence"] == (
        "durable_recorded_rows_current_activity_outcome_and_server_state"
    )
    async with db.get_session() as session:
        outcome = (
            await session.execute(
                text(
                    "SELECT status,reason_code,event_id FROM promo_activity_outcome "
                    "WHERE activity_id=:id"
                ),
                {"id": activity_id},
            )
        ).one()
        assert tuple(outcome) == ("failed", "provider_error", eid)

@pytest.mark.asyncio
async def test_promo_activity_outcome_is_safe_durable_and_revision_neutral(
    config, tmp_path
):
    from datetime import datetime, timezone
    from promo import PromoVkActionResult, record_promo_activity_outcomes

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
    prepared = await promo.prepare(a, "promo_create", req, "promo-outcome-create")
    assert (await promo.operation(
        a, prepared["preparation_ref"], prepared["action_digest"]
    ))["status"] == "review_required"
    accepted = await promo.operation(
        owner, prepared["preparation_ref"], prepared["action_digest"], "approve"
    )
    cid = accepted["result"]["campaign_id"]
    before = await promo.campaign_get(a, cid)
    revision = before["campaign_revision"]
    activity_id = before["activities"][0]["id"]

    await record_promo_activity_outcomes(
        db,
        [
            PromoVkActionResult(
                campaign_id=cid,
                activity_id=activity_id,
                surface="vk_repost",
                event_id=eid,
                status="failed",
                reason="opaque-provider-error-detail",
            )
        ],
        now_utc=datetime.now(timezone.utc),
    )

    after = await promo.campaign_get(a, cid)
    assert after["campaign_revision"] == revision
    assert after["delivery_state"] == "non_delivery_recorded"
    assert "provider_error" in after["non_delivery_reasons"]
    activity = after["activities"][0]
    assert activity["last_outcome_status"] == "failed"
    assert activity["last_outcome_reason"] == "provider_error"
    assert activity["last_outcome_event_id"] == eid
    assert "opaque-provider-error-detail" not in str(after)

    async with db.get_session() as session:
        row = (
            await session.execute(
                text(
                    "SELECT status,reason_code,event_id FROM promo_activity_outcome "
                    "WHERE activity_id=:id"
                ),
                {"id": activity_id},
            )
        ).one()
        assert tuple(row) == ("failed", "provider_error", eid)
        assert (
            await session.execute(
                text("SELECT count(*) FROM promo_activity_outcome")
            )
        ).scalar() == 1
        assert (
            await session.execute(text("SELECT count(*) FROM promo_exposure"))
        ).scalar() == 0

    await db.init()
    repeated = await promo.campaign_get(a, cid)
    assert repeated["campaign_revision"] == revision
    assert repeated["activities"][0]["last_outcome_reason"] == "provider_error"

    with pytest.raises(ToolExecutionError):
        await promo.campaign_get(b, cid)


@pytest.mark.asyncio
async def test_static_site_readback_separates_candidate_build_from_root_deploy(
    config, tmp_path
):
    from datetime import datetime, timezone
    from models import JobOutbox, JobTask, JobStatus
    from private_events_mcp.publication_status import PublicationReadService
    from static_site_release import event_public_revision

    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    async with db.get_session() as session:
        event = await session.get(Event, eid)
        revision = event_public_revision(event)
        job = JobOutbox(
            event_id=0,
            task=JobTask.static_site_build,
            status=JobStatus.done,
            payload={
                "event_revisions": {str(eid): revision},
                "snapshot": {
                    "manifest": {"event_revisions": {str(eid): revision}}
                },
                "build_receipt": {
                    "build_id": "production-partner-test",
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "publication": {"status": "published"},
                    "root_promotion": {"status": "disabled"},
                },
            },
        )
        session.add(job)
        await session.commit()
        jid = job.id

    service = PublicationReadService(db, events.policy)
    current = await service.read(a, eid)
    site = next(row for row in current["surfaces"] if row["surface"] == "static_site")
    assert site["transport"] == "built"
    assert site["candidate_state"] == "published"
    assert site["deployment_state"] == "not_deployed"
    assert site["revision"] == "up_to_date"
    assert site["public_url"] is None

    async with db.get_session() as session:
        job = await session.get(JobOutbox, jid)
        payload = dict(job.payload)
        receipt = dict(payload["build_receipt"])
        receipt["root_promotion"] = {
            "status": "promoted",
            "current": {"build_id": "production-partner-test"},
        }
        payload["build_receipt"] = receipt
        job.payload = payload
        session.add(job)
        await session.commit()

    deployed = await service.read(a, eid)
    site = next(row for row in deployed["surfaces"] if row["surface"] == "static_site")
    assert site["transport"] == "published"
    assert site["deployment_state"] == "current"
    assert site["applied_revision"] == revision

    async with db.get_session() as session:
        event = await session.get(Event, eid)
        event.description = "newer canonical revision"
        session.add(event)
        await session.commit()
    stale = await service.read(a, eid)
    site = next(row for row in stale["surfaces"] if row["surface"] == "static_site")
    assert site["revision"] == "stale"
    assert site["deployment_state"] == "stale"

@pytest.mark.asyncio
async def test_video_promo_public_exposure_updates_partner_activity_outcome(
    config, tmp_path
):
    from datetime import datetime, timezone
    from models import (
        VideoAnnounceSession,
        VideoAnnounceSessionStatus,
        VideoAnnounceItem,
        VideoAnnounceItemStatus,
    )
    from promo import record_video_promo_exposures

    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    cap = await promo.capabilities(a, eid)
    req = dict(
        event_id=eid,
        event_revision=cap["event_revision"],
        surface="video_general",
        profile_key="popular_review",
        slot_policy="guaranteed_any_position",
        count=1,
        ends_at=(date.today() + timedelta(days=10)).isoformat(),
        is_editorial=True,
        sponsorship_disclosure=None,
        title_override=None,
    )
    prepared = await promo.prepare(a, "promo_create", req, "promo-video-readback")
    await promo.operation(a, prepared["preparation_ref"], prepared["action_digest"])
    accepted = await promo.operation(
        owner, prepared["preparation_ref"], prepared["action_digest"], "approve"
    )
    cid = accepted["result"]["campaign_id"]
    detail = await promo.campaign_get(a, cid)
    activity_id = detail["activities"][0]["id"]

    async with db.get_session() as session:
        video = VideoAnnounceSession(
            status=VideoAnnounceSessionStatus.PUBLISHED_MAIN,
            profile_key="popular_review",
        )
        session.add(video)
        await session.commit()
        await session.refresh(video)
        session.add(
            VideoAnnounceItem(
                session_id=int(video.id),
                event_id=eid,
                status=VideoAnnounceItemStatus.READY,
                position=1,
                promo_campaign_id=cid,
                promo_activity_id=activity_id,
                promo_placement_kind="general_boost",
            )
        )
        await session.commit()
        session_id = int(video.id)

    published_at = datetime.now(timezone.utc)
    assert await record_video_promo_exposures(
        db,
        session_id=session_id,
        publish_status="PUBLISHED_MAIN",
        published_at=published_at,
        public_target_count=1,
        public_targets=[{"kind": "test-private-target"}],
    ) == 1

    detail = await promo.campaign_get(a, cid)
    activity = detail["activities"][0]
    assert activity["last_outcome_status"] == "published"
    assert activity["last_outcome_reason"] is None
    assert activity["last_outcome_event_id"] == eid
    assert any(
        row["recorded_publish_status"] == "PUBLISHED_MAIN"
        and row["surface"] == "video"
        for row in detail["recorded_exposures"]["rows"]
    )

@pytest.mark.asyncio
async def test_partner_promo_capabilities_fail_closed_on_surface_and_profile(
    config, tmp_path
):
    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    async with db.get_session() as session:
        org = await session.get(Organization, "a")
        org.vk_source_group_ids = [999]
        session.add(org)
        await session.commit()

    cap = await promo.capabilities(a, eid)
    assert cap["surface_verdicts"]["video_general"]["available"] is True
    assert cap["surface_verdicts"]["vk_repost"] == {
        "available": False,
        "reason": "vk_source_not_allowed",
    }
    assert cap["available_surfaces"] == ["video_general"]

    vk_request = dict(
        event_id=eid,
        event_revision=cap["event_revision"],
        surface="vk_repost",
        profile_key=None,
        slot_policy=None,
        count=1,
        ends_at=(date.today() + timedelta(days=10)).isoformat(),
        is_editorial=True,
        sponsorship_disclosure=None,
        title_override=None,
    )
    with pytest.raises(ToolExecutionError) as exc:
        await promo.prepare(a, "promo_create", vk_request, "promo-vk-denied-source")
    assert exc.value.error_code == "PROMO_SURFACE_UNAVAILABLE"

    konb_request = {
        **vk_request,
        "surface": "video_general",
        "profile_key": "konb",
        "slot_policy": "guaranteed_any_position",
    }
    with pytest.raises(ToolExecutionError) as exc:
        await promo.prepare(a, "promo_create", konb_request, "promo-konb-denied")
    assert exc.value.error_code == "PROMO_PROFILE_DENIED"

    async with db.get_session() as session:
        org = await session.get(Organization, "a")
        org.vk_source_group_ids = [101]
        org.video_profile_key = "konb"
        session.add(org)
        await session.commit()

    cap = await promo.capabilities(a, eid)
    assert cap["surface_verdicts"]["vk_repost"]["available"] is True
    assert set(cap["available_surfaces"]) == {"video_general", "vk_repost"}
    assert "konb" in cap["video_profiles"]

@pytest.mark.asyncio
async def test_partner_vk_repost_campaign_runs_worker_and_reads_back(
    config, tmp_path, monkeypatch
):
    from datetime import datetime, timedelta, timezone
    from promo import run_promo_vk_activities

    c, db, store, eid, owner, a, b, events, promo = await setup(config, tmp_path)
    cap = await promo.capabilities(a, eid)
    req = dict(
        event_id=eid,
        event_revision=cap["event_revision"],
        surface="vk_repost",
        profile_key=None,
        slot_policy=None,
        count=1,
        ends_at=(date.today() + timedelta(days=10)).isoformat(),
        is_editorial=True,
        sponsorship_disclosure=None,
        title_override=None,
    )
    prepared = await promo.prepare(a, "promo_create", req, "promo-vk-worker-create")
    assert (await promo.operation(
        a, prepared["preparation_ref"], prepared["action_digest"]
    ))["status"] == "review_required"
    accepted = await promo.operation(
        owner, prepared["preparation_ref"], prepared["action_digest"], "approve"
    )
    cid = accepted["result"]["campaign_id"]

    now_utc = datetime.now(timezone.utc).replace(
        hour=14, minute=0, second=0, microsecond=0
    )
    async with db.get_session() as session:
        event = await session.get(Event, eid)

    async def resolve_group(value):
        raw = str(value)
        if raw == "101":
            return 101
        if raw == "kenigeventsofficial":
            return 999
        return None

    async def recent_posts(events_arg, *, group_id, since_utc, until_utc, db):
        assert group_id == 101
        assert any(int(ev.id) == eid for ev in events_arg)
        return [
            (
                event,
                "https://vk.com/wall-101_202",
                now_utc - timedelta(hours=1),
            )
        ]

    async def caption(_event):
        return "Partner promo test caption"

    publish_calls = []

    async def publish(
        db_arg,
        bot_arg,
        *,
        source_url,
        target_group_id,
        message,
    ):
        publish_calls.append(
            {
                "source_url": source_url,
                "target_group_id": target_group_id,
                "message": message,
            }
        )
        return "https://vk.com/wall-999_303"

    monkeypatch.setattr("promo._resolve_vk_group_id", resolve_group)
    monkeypatch.setattr("promo._recent_event_vk_posts", recent_posts)
    monkeypatch.setattr("promo._build_promo_vk_repost_caption", caption)
    monkeypatch.setattr("promo._publish_vk_repost", publish)

    results = await run_promo_vk_activities(db, None, now_utc=now_utc)
    mine = [
        item
        for item in results
        if item.campaign_id == cid and item.surface == "vk_repost"
    ]
    assert len(mine) == 1
    assert mine[0].status == "published"
    assert mine[0].event_id == eid
    assert publish_calls == [
        {
            "source_url": "https://vk.com/wall-101_202",
            "target_group_id": 999,
            "message": "Partner promo test caption",
        }
    ]

    detail = await promo.campaign_get(a, cid)
    activity = detail["activities"][0]
    assert activity["last_outcome_status"] == "published"
    assert activity["last_outcome_reason"] is None
    assert activity["last_outcome_event_id"] == eid
    exposure = next(
        row
        for row in detail["recorded_exposures"]["rows"]
        if row["surface"] == "vk_repost"
    )
    assert exposure["recorded_publish_status"] == "PUBLISHED_MAIN"
    assert exposure["recorded_public_target_count"] == 1
    assert detail["delivery_state"] == "recorded_publication"
