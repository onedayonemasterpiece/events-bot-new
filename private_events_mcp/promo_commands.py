"""Shared owner/partner promo commands over existing campaign services and ledger."""

from datetime import date, datetime, timezone, timedelta
import json
import secrets
import time
from sqlalchemy import text, select
from models import Event, PromoCampaign, PromoTarget
from promo import (
    PartnerPromoSpec,
    PartnerActivitySpec,
    create_partner_event_promo_campaign,
    add_partner_activity_to_campaign,
    PARTNER_PROMO_VIDEO_PROFILES,
    PARTNER_PROMO_SLOT_POLICIES,
    _event_is_promo_eligible,
)
from static_site_release import event_public_revision
from .actors import ActorContext
from .event_commands import canonical, digest, fail, one
from .promo_operations import PromoOperationStore, service_slot_policy

# Only the surfaces accepted by the existing partner campaign service. Global
# owner placements do not acquire partner authority merely by being installed.
PARTNER_SURFACES = ("video_general", "vk_repost")


class PromoCommandService:
    def __init__(self, database, policy):
        self.database, self.policy = database, policy
        self.snapshots = PromoOperationStore(database, authorize=lambda *args: False)

    async def _check(self, session, actor, action, request, policy_revision=None):
        event_id = request.get("event_id")
        grant = await self.policy.check_session(
            session, actor, action, event_id=event_id, revision=policy_revision
        )
        if request.get("campaign_id"):
            if grant:
                # Campaign belongs to the durable organization, with all targets
                # still in this principal's current explicit portfolio.
                owner = await one(
                    session,
                    "SELECT tenant_id,organization_id FROM mcp_partner_campaign WHERE campaign_id=:id",
                    id=request["campaign_id"],
                )
                if (
                    not owner
                    or owner["tenant_id"] != grant.tenant_id
                    or owner["organization_id"] != grant.organization_id
                ):
                    fail("NOT_FOUND")
                targets = (
                    (
                        await session.execute(
                            select(PromoTarget).where(
                                PromoTarget.campaign_id == request["campaign_id"]
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                if not targets or any(t.target_type != "event" for t in targets):
                    fail("NOT_FOUND")
                for target in targets:
                    await self.policy.check_session(
                        session,
                        actor,
                        action,
                        event_id=target.event_id,
                        revision=policy_revision,
                    )
            if await session.get(PromoCampaign, request["campaign_id"]) is None:
                fail("NOT_FOUND")
        return grant

    async def capabilities(self, actor, event_id):
        async with self.database.get_session() as session:
            grant = await self._check(
                session, actor, "promo_read", {"event_id": event_id}
            )
            event = await session.get(Event, event_id)
            if event is None:
                fail("NOT_FOUND")
            return {
                "event_id": event_id,
                "event_revision": event_public_revision(event),
                "supported_surfaces": list(PARTNER_SURFACES),
                "video_profiles": dict(PARTNER_PROMO_VIDEO_PROFILES),
                "slot_policies": dict(PARTNER_PROMO_SLOT_POLICIES),
                "limits": dict(grant.limits) if grant else None,
                "lifecycle_status": event.lifecycle_status,
                "publication_state": "not_observed",
            }

    async def campaigns_list(self, actor, after_id=0, limit=20, status=None):
        async with self.database.get_session() as session:
            grant = await self._check(session, actor, "promo_read", {})
            params = {"after": after_id, "limit": limit + 1}
            where = "c.id>:after"
            if grant:
                where += " AND EXISTS(SELECT 1 FROM mcp_partner_campaign p WHERE p.campaign_id=c.id AND p.tenant_id=:tenant AND p.organization_id=:org) AND NOT EXISTS(SELECT 1 FROM promo_target t WHERE t.campaign_id=c.id AND (t.target_type!='event' OR NOT EXISTS(SELECT 1 FROM mcp_partner_event e WHERE e.event_id=t.event_id AND e.principal_id=:principal AND e.tenant_id=:tenant AND e.organization_id=:org)))"
                params.update(
                    tenant=grant.tenant_id,
                    org=grant.organization_id,
                    principal=grant.principal_id,
                )
            if status:
                where += " AND c.status=:status"
                params["status"] = status
            rows = (
                (
                    await session.execute(
                        text(
                            "SELECT c.* FROM promo_campaign c WHERE "
                            + where
                            + " ORDER BY c.id LIMIT :limit"
                        ),
                        params,
                    )
                )
                .mappings()
                .all()
            )
            return {
                "campaigns": [
                    self.snapshots._campaign_summary(row) for row in rows[:limit]
                ],
                "has_more": len(rows) > limit,
                "next_after_id": rows[limit - 1]["id"] if len(rows) > limit else None,
            }

    async def campaign_get(self, actor, campaign_id):
        async with self.database.get_session() as session:
            await self._check(
                session, actor, "promo_read", {"campaign_id": campaign_id}
            )
            (
                campaign,
                targets,
                activities,
                rev,
            ) = await self.snapshots._campaign_snapshot(session, campaign_id)
            rows = (
                (
                    await session.execute(
                        text(
                            "SELECT event_id,surface,publish_status,published_at,public_target_count FROM promo_exposure WHERE campaign_id=:id ORDER BY id DESC LIMIT 17"
                        ),
                        {"id": campaign_id},
                    )
                )
                .mappings()
                .all()
            )
            reasons = []
            model = await session.get(PromoCampaign, campaign_id)
            for target in targets:
                event = (
                    await session.get(Event, target["event_id"])
                    if target["event_id"]
                    else None
                )
                if event is None or not _event_is_promo_eligible(
                    event, today=datetime.now(timezone.utc).date(), campaign=model
                ):
                    reasons.append("target_ineligible")
            if campaign["status"] != "active":
                reasons.append("campaign_" + campaign["status"])
            return {
                "campaign": self.snapshots._campaign_summary(campaign),
                "campaign_revision": rev,
                "targets": [
                    {"event_id": r["event_id"], "target_type": r["target_type"]}
                    for r in targets[:16]
                ],
                "activities": [
                    {
                        k: r[k]
                        for k in (
                            "id",
                            "surface",
                            "profile_key",
                            "enabled",
                            "target_exposure_goal",
                            "daily_cap",
                        )
                    }
                    for r in activities[:16]
                ],
                "recorded_exposures": {
                    "rows": [dict(r) for r in rows[:16]],
                    "has_more": len(rows) > 16,
                    "accounting": "recorded_publication_units_not_browser_impressions",
                },
                "ineligibility_reasons": sorted(set(reasons)),
                "publication_state": "not_observed",
            }

    async def _validate(self, session, actor, kind, request, grant, *, replay=False):
        if kind == "promo_create":
            event = await session.get(Event, request["event_id"])
            if event is None:
                fail("NOT_FOUND")
            if not replay and event_public_revision(event) != request["event_revision"]:
                fail("STALE_EVENT_REVISION")
            events = [event]
            activities = []
        else:
            (
                campaign,
                targets,
                activities,
                rev,
            ) = await self.snapshots._campaign_snapshot(session, request["campaign_id"])
            if not replay and rev != request["campaign_revision"]:
                fail("PROMO_CAMPAIGN_REVISION_CONFLICT")
            if campaign["status"] == "archived" and not replay:
                fail("PROMO_CAMPAIGN_ARCHIVED")
            events = [
                await session.get(Event, t["event_id"])
                for t in targets
                if t["target_type"] == "event"
            ]
        if replay:
            return
        if kind in {"promo_create", "promo_activity_add", "promo_resume"}:
            if not events or any(
                e is None
                or e.lifecycle_status != "active"
                or e.silent
                or (e.end_date or e.date) < date.today().isoformat()
                for e in events
            ):
                fail("PROMO_EVENT_INELIGIBLE")
        if grant:
            limits = grant.limits
            if kind in {"promo_create", "promo_resume"}:
                count = (
                    await session.execute(
                        text(
                            "SELECT count(*) FROM promo_campaign c JOIN mcp_partner_campaign p ON p.campaign_id=c.id WHERE p.tenant_id=:tenant AND p.organization_id=:org AND c.status='active'"
                        ),
                        {"tenant": grant.tenant_id, "org": grant.organization_id},
                    )
                ).scalar()
                if count >= limits["active_campaigns"]:
                    fail("PROMO_LIMIT_EXCEEDED")
            if kind in {"promo_create", "promo_activity_add"}:
                if request["count"] > limits["campaign_exposures"]:
                    fail("PROMO_LIMIT_EXCEEDED")
                if request["surface"] not in PARTNER_SURFACES:
                    fail("PROMO_SURFACE_DENIED")
                if kind == "promo_create":
                    if date.fromisoformat(
                        request["ends_at"]
                    ) > date.today() + timedelta(days=limits["campaign_days"]):
                        fail("PROMO_LIMIT_EXCEEDED")
                elif len(activities) >= limits["activities"]:
                    fail("PROMO_LIMIT_EXCEEDED")

    async def _target_revisions(self, session, request):
        ids = (
            [request["event_id"]]
            if request.get("event_id")
            else list(
                (
                    await session.execute(
                        select(PromoTarget.event_id).where(
                            PromoTarget.campaign_id == request["campaign_id"],
                            PromoTarget.target_type == "event",
                        )
                    )
                ).scalars()
            )
        )
        result = {}
        for eid in ids:
            event = await session.get(Event, eid)
            if (
                event is None
                or event.identity_status != "canonical"
                or event.merged_into_event_id is not None
            ):
                fail("PROMO_EVENT_INELIGIBLE")
            result[str(eid)] = event_public_revision(event)
        return result

    async def prepare(self, actor, kind, request, idempotency_key):
        from .event_create import _idempotency_key

        if kind not in {
            "promo_create",
            "promo_activity_add",
            "promo_pause",
            "promo_resume",
            "promo_archive",
        }:
            fail("INVALID_ARGUMENTS")
        from .promo_command_tools import CreateRequest, ActivityRequest, StateRequest
        from pydantic import ValidationError

        try:
            if kind == "promo_create":
                request = CreateRequest.model_validate(request).model_dump()
            elif kind == "promo_activity_add":
                request = ActivityRequest.model_validate(request).model_dump()
            else:
                request = StateRequest.model_validate(
                    {**request, "action": kind.removeprefix("promo_")}
                ).model_dump(exclude={"action"})
        except ValidationError:
            fail("INVALID_ARGUMENTS")
        if kind in {"promo_create", "promo_activity_add"}:
            if (
                request["surface"] == "vk_repost"
                and (
                    request["profile_key"] is not None
                    or request["slot_policy"] is not None
                )
            ) or (
                request["surface"] == "video_general"
                and (
                    request["profile_key"] not in PARTNER_PROMO_VIDEO_PROFILES
                    or request["slot_policy"] not in PARTNER_PROMO_SLOT_POLICIES
                )
            ):
                fail("INVALID_ARGUMENTS")
        key = digest(_idempotency_key(idempotency_key))
        async with self.database.get_session() as session:
            await session.execute(text("BEGIN IMMEDIATE"))
            grant = await self._check(session, actor, kind, request)
            old = await one(
                session,
                "SELECT * FROM event_change_log WHERE operation_kind=:kind AND actor_subject=:subject AND actor_client_id=:client_id AND actor_audience=:audience AND idempotency_hash=:key",
                kind=kind,
                **actor.binding(),
                key=key,
            )
            if old:
                if json.loads(old["request_json"])["request"] != request:
                    fail("IDEMPOTENCY_CONFLICT")
                return self._view(old)
            await self._validate(session, actor, kind, request, grant)
            frozen = {
                "event_revisions": await self._target_revisions(session, request),
                "actor": actor.binding(),
                "kind": kind,
                "request": request,
                "policy_revision": grant.policy_revision if grant else None,
                "review_required": bool(grant and kind not in grant.auto_approve),
                "expires_at": int(time.time()) + 600,
            }
            ref = "evt_op_" + secrets.token_urlsafe(24)
            await session.execute(
                text(
                    "INSERT INTO event_change_log(operation_ref,operation_kind,actor_subject,actor_client_id,actor_audience,idempotency_hash,action_digest,source_type,source_url,request_json,status,event_id,base_event_revision) VALUES(:ref,:kind,:subject,:client_id,:audience,:key,:digest,'promo_command','',:request,'prepared',:event,:revision)"
                ),
                dict(
                    ref=ref,
                    kind=kind,
                    **actor.binding(),
                    key=key,
                    digest=digest(frozen),
                    request=canonical(frozen),
                    event=request.get("event_id"),
                    revision=request.get("event_revision"),
                ),
            )
            row = await one(
                session,
                "SELECT * FROM event_change_log WHERE operation_ref=:ref",
                ref=ref,
            )
            await session.commit()
            return self._view(row)

    def _view(self, row):
        frozen = json.loads(row["request_json"])
        return {
            "operation_ref": row["operation_ref"],
            "preparation_ref": row["operation_ref"],
            "action_digest": row["action_digest"],
            "status": row["status"],
            "policy_verdict": "owner_review_required"
            if frozen["review_required"]
            else "auto_approved",
            "proposal": frozen["request"],
            "expires_at": frozen["expires_at"],
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
        }

    async def operation(
        self, actor, ref, action_digest=None, decision=None, review=False
    ):
        async with self.database.get_session() as session:
            await session.execute(text("BEGIN IMMEDIATE"))
            row = await one(
                session,
                "SELECT * FROM event_change_log WHERE operation_ref=:ref AND source_type='promo_command'",
                ref=ref,
            )
            if not row:
                fail("NOT_FOUND")
            frozen = json.loads(row["request_json"])
            request = frozen["request"]
            kind = frozen["kind"]
            actual = actor
            if digest(frozen) != row["action_digest"]:
                fail("ACTION_DIGEST_CONFLICT")
            if review or decision:
                await self.policy.check_session(
                    session, actor, "promo_read", review=True
                )
                actual = ActorContext(
                    **frozen["actor"],
                    scopes=frozenset({"partner:promo:request", "partner:promo:read"}),
                    expires_at=2**63 - 1,
                )
            elif actor.binding() != frozen["actor"]:
                fail("NOT_FOUND")
            grant = await self._check(
                session,
                actual,
                kind if action_digest else "promo_read",
                request,
                frozen["policy_revision"],
            )
            if action_digest is None:
                return self._view(row)
            if action_digest != row["action_digest"]:
                fail("ACTION_DIGEST_CONFLICT")
            if row["status"] in {"accepted", "rejected"}:
                return self._view(row)
            if row["status"] not in {"prepared", "review_required"}:
                fail("OPERATION_STATE_CONFLICT")
            if decision and (
                decision not in {"approve", "reject"}
                or row["status"] != "review_required"
            ):
                fail("REVIEW_REQUIRED")
            if row["status"] == "prepared" and time.time() >= frozen["expires_at"]:
                fail("PREPARATION_EXPIRED")
            if (
                await self._target_revisions(session, request)
                != frozen["event_revisions"]
            ):
                fail("STALE_EVENT_REVISION")
            await self._validate(session, actual, kind, request, grant)
            status = (
                "rejected"
                if decision == "reject"
                else "review_required"
                if frozen["review_required"] and not decision
                else "accepted"
            )
            result = None
            if status == "accepted":
                if kind == "promo_create":
                    spec = PartnerPromoSpec(
                        event_id=request["event_id"],
                        creator_user_id=None,
                        organization_name=grant.display_name if grant else None,
                        **{
                            key: request.get(key)
                            for key in (
                                "surface",
                                "profile_key",
                                "count",
                                "is_editorial",
                                "sponsorship_disclosure",
                                "title_override",
                            )
                        },
                        slot_policy=service_slot_policy(request),
                        ends_at=date.fromisoformat(request["ends_at"]),
                    )
                    made = await create_partner_event_promo_campaign(
                        self.database, spec, session=session
                    )
                    if made.campaign is None:
                        fail("PROMO_EVENT_INELIGIBLE")
                    campaign_id = made.campaign.id
                    if grant:
                        made.campaign.daily_exposure_cap = grant.limits[
                            "daily_exposures"
                        ]
                        session.add(made.campaign)
                        await session.execute(
                            text(
                                "INSERT INTO mcp_partner_campaign VALUES(:id,:principal,:tenant,:org)"
                            ),
                            dict(
                                id=campaign_id,
                                principal=grant.principal_id,
                                tenant=grant.tenant_id,
                                org=grant.organization_id,
                            ),
                        )
                elif kind == "promo_activity_add":
                    spec = PartnerActivitySpec(
                        **{
                            k: request[k]
                            for k in ("campaign_id", "surface", "profile_key", "count")
                        },
                        slot_policy=service_slot_policy(request),
                    )
                    made = await add_partner_activity_to_campaign(
                        self.database, spec, actor_user_id=None, session=session
                    )
                    if made.campaign is None:
                        fail("PROMO_EVENT_INELIGIBLE")
                    campaign_id = request["campaign_id"]
                else:
                    campaign_id = request["campaign_id"]
                    campaign = await session.get(PromoCampaign, campaign_id)
                    campaign.status = {
                        "promo_pause": "paused",
                        "promo_resume": "active",
                        "promo_archive": "archived",
                    }[kind]
                    campaign.updated_at = datetime.now(timezone.utc)
                    session.add(campaign)
                result = {
                    "campaign_id": campaign_id,
                    "publication_state": "not_observed",
                    "reviewed_by": actor.subject if decision else None,
                }
            await session.execute(
                text(
                    "UPDATE event_change_log SET status=:status,result_json=:result,updated_at=CURRENT_TIMESTAMP WHERE operation_ref=:ref"
                ),
                dict(
                    status=status, result=canonical(result) if result else None, ref=ref
                ),
            )
            updated = await one(
                session,
                "SELECT * FROM event_change_log WHERE operation_ref=:ref",
                ref=ref,
            )
            await session.commit()
            return self._view(updated)
