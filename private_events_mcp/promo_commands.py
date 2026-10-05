"""Shared owner/partner promo commands over existing campaign services and ledger."""

from datetime import date, datetime, timezone, timedelta
import json
import secrets
import time
from sqlalchemy import text, select, func
from models import (
    Event,
    Organization,
    PromoActivity,
    PromoCampaign,
    PromoTarget,
    PromoExposure,
)
from promo import (
    PartnerPromoSpec,
    PartnerActivitySpec,
    create_partner_event_promo_campaign,
    add_partner_activity_to_campaign,
    PUBLIC_PROMO_EXPOSURE_STATUSES,
    PROMO_SAFE_ACTIVITY_OUTCOME_STATUSES,
    PARTNER_PROMO_VIDEO_PROFILES,
    PARTNER_PROMO_SLOT_POLICIES,
    _event_is_promo_eligible,
    _event_has_stored_poster,
    _promo_day_bounds,
    _vk_owner_post_from_url,
    partner_safe_promo_reason,
    partner_vk_repost_config,
    clamp_campaign_end_to_event,
    normalize_promo_priority,
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

    async def _placement_context(self, session, event, grant):
        organization = (
            await session.get(Organization, grant.organization_id)
            if grant is not None
            else None
        )
        profiles = dict(PARTNER_PROMO_VIDEO_PROFILES)
        if grant is not None and (
            organization is None
            or str(organization.video_profile_key or "").strip() != "konb"
        ):
            profiles.pop("konb", None)

        video_available = _event_has_stored_poster(event)
        source_packet = _vk_owner_post_from_url(
            str(getattr(event, "source_vk_post_url", None) or "")
        )
        source_group_id = abs(int(source_packet[0])) if source_packet else None
        vk_available = source_group_id is not None
        vk_reason = None if vk_available else "source_unavailable"
        if grant is not None and source_group_id is not None:
            allowed: set[int] = set()
            if organization is not None:
                for value in organization.vk_source_group_ids or []:
                    try:
                        allowed.add(abs(int(value)))
                    except (TypeError, ValueError):
                        continue
            if source_group_id not in allowed:
                vk_available = False
                vk_reason = "vk_source_not_allowed"

        verdicts = {
            "video_general": {
                "available": video_available,
                "reason": None if video_available else "event_posters_missing",
            },
            "vk_repost": {
                "available": vk_available,
                "reason": vk_reason,
            },
        }
        return verdicts, profiles, source_group_id

    async def _configure_server_activity(
        self, session, *, campaign_id, surface, event, grant
    ):
        if surface != "vk_repost":
            return
        verdicts, _profiles, source_group_id = await self._placement_context(
            session, event, grant
        )
        if not verdicts["vk_repost"]["available"] or source_group_id is None:
            fail("PROMO_SURFACE_UNAVAILABLE")
        activity = (
            await session.execute(
                select(PromoActivity)
                .where(
                    PromoActivity.campaign_id == campaign_id,
                    PromoActivity.surface == "vk_repost",
                )
                .order_by(PromoActivity.id.desc())
                .limit(1)
            )
        ).scalars().first()
        if activity is None:
            fail("PROMO_OPERATION_CONFLICT")
        activity.config_json = partner_vk_repost_config(source_group_id)
        session.add(activity)
        await session.flush()

    async def capabilities(self, actor, event_id):
        async with self.database.get_session() as session:
            grant = await self._check(
                session, actor, "promo_read", {"event_id": event_id}
            )
            event = await session.get(Event, event_id)
            if event is None:
                fail("NOT_FOUND")
            verdicts, profiles, _source_group_id = await self._placement_context(
                session, event, grant
            )
            return {
                "event_id": event_id,
                "event_revision": event_public_revision(event),
                "supported_surfaces": list(PARTNER_SURFACES),
                "available_surfaces": [
                    surface
                    for surface in PARTNER_SURFACES
                    if verdicts[surface]["available"]
                ],
                "surface_verdicts": verdicts,
                "video_profiles": profiles,
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
            campaign, targets, activities, rev = await self.snapshots._campaign_snapshot(
                session, campaign_id
            )
            recorded = list(
                (
                    await session.execute(
                        select(PromoExposure)
                        .where(PromoExposure.campaign_id == campaign_id)
                        .order_by(
                            PromoExposure.published_at.desc(),
                            PromoExposure.id.desc(),
                        )
                        .limit(17)
                    )
                )
                .scalars()
                .all()
            )
            model = await session.get(PromoCampaign, campaign_id)
            if model is None:
                fail("NOT_FOUND")

            now = datetime.now(timezone.utc)
            reasons: list[str] = []
            starts_at = model.starts_at
            ends_at = model.ends_at
            if starts_at and starts_at.tzinfo is None:
                starts_at = starts_at.replace(tzinfo=timezone.utc)
            if ends_at and ends_at.tzinfo is None:
                ends_at = ends_at.replace(tzinfo=timezone.utc)
            if starts_at and starts_at > now:
                reasons.append("campaign_not_started")
            if ends_at and ends_at < now:
                reasons.append("campaign_window_closed")
            if campaign["status"] != "active":
                reasons.append("campaign_" + campaign["status"])

            for target in targets:
                event = (
                    await session.get(Event, target["event_id"])
                    if target["event_id"]
                    else None
                )
                if event is None or not _event_is_promo_eligible(
                    event, today=now.date(), campaign=model
                ):
                    reasons.append("target_ineligible")

            day_start, day_end = _promo_day_bounds(now)
            public_statuses = tuple(PUBLIC_PROMO_EXPOSURE_STATUSES)
            all_counts = dict(
                (
                    await session.execute(
                        select(PromoExposure.activity_id, func.count(PromoExposure.id))
                        .where(PromoExposure.campaign_id == campaign_id)
                        .where(PromoExposure.publish_status.in_(public_statuses))
                        .group_by(PromoExposure.activity_id)
                    )
                ).all()
            )
            day_counts = dict(
                (
                    await session.execute(
                        select(PromoExposure.activity_id, func.count(PromoExposure.id))
                        .where(PromoExposure.campaign_id == campaign_id)
                        .where(PromoExposure.publish_status.in_(public_statuses))
                        .where(PromoExposure.published_at >= day_start)
                        .where(PromoExposure.published_at < day_end)
                        .group_by(PromoExposure.activity_id)
                    )
                ).all()
            )
            outcome_rows = (
                (
                    await session.execute(
                        text(
                            "SELECT activity_id,last_attempt_at,status,reason_code,event_id "
                            "FROM promo_activity_outcome WHERE campaign_id=:id"
                        ),
                        {"id": campaign_id},
                    )
                )
                .mappings()
                .all()
            )
            outcomes = {}
            outcome_reasons: list[str] = []
            for row in outcome_rows:
                status = str(row["status"] or "")
                if status not in PROMO_SAFE_ACTIVITY_OUTCOME_STATUSES:
                    status = "unknown"
                reason = partner_safe_promo_reason(row["reason_code"])
                normalized = {
                    "last_attempt_at": row["last_attempt_at"],
                    "status": status,
                    "reason_code": reason,
                    "event_id": row["event_id"],
                }
                outcomes[int(row["activity_id"])] = normalized
                if status in {"failed", "skipped", "unknown"}:
                    outcome_reasons.append(reason or "provider_error")

            total_count = sum(int(value or 0) for value in all_counts.values())
            daily_count = sum(int(value or 0) for value in day_counts.values())
            if (
                model.total_exposure_goal is not None
                and total_count >= max(0, int(model.total_exposure_goal))
            ):
                reasons.append("campaign_total_goal_reached")
            if (
                model.daily_exposure_cap is not None
                and daily_count >= max(0, int(model.daily_exposure_cap))
            ):
                reasons.append("campaign_daily_cap_reached")
            for activity in activities:
                activity_id = int(activity["id"])
                if not bool(activity["enabled"]):
                    reasons.append("activity_disabled")
                if (
                    activity["target_exposure_goal"] is not None
                    and int(all_counts.get(activity_id, 0) or 0)
                    >= max(0, int(activity["target_exposure_goal"]))
                ):
                    reasons.append("activity_goal_reached")
                if (
                    activity["daily_cap"] is not None
                    and int(day_counts.get(activity_id, 0) or 0)
                    >= max(0, int(activity["daily_cap"]))
                ):
                    reasons.append("activity_daily_cap_reached")

            exposure_rows = []
            recorded_failure_reasons: list[str] = []
            for exposure in recorded[:16]:
                details = (
                    exposure.details_json
                    if isinstance(exposure.details_json, dict)
                    else {}
                )
                raw_reason = details.get("reason")
                reason = (
                    partner_safe_promo_reason(raw_reason)
                    if isinstance(raw_reason, str)
                    else None
                )
                if exposure.publish_status not in PUBLIC_PROMO_EXPOSURE_STATUSES:
                    reason = reason or "recorded_non_public_outcome"
                    recorded_failure_reasons.append(reason)
                else:
                    reason = None
                exposure_rows.append(
                    {
                        "exposure_id": exposure.id,
                        "event_id": exposure.event_id,
                        "activity_id": exposure.activity_id,
                        "surface": exposure.surface,
                        "placement_kind": exposure.placement_kind,
                        "recorded_publish_status": exposure.publish_status,
                        "recorded_public_target_count": exposure.public_target_count,
                        "recorded_published_at": exposure.published_at,
                        "delivery_reason": reason,
                    }
                )

            activity_rows = []
            for row in activities[:16]:
                item = {
                    key: row[key]
                    for key in (
                        "id",
                        "surface",
                        "profile_key",
                        "enabled",
                        "target_exposure_goal",
                        "daily_cap",
                    )
                }
                outcome = outcomes.get(int(row["id"]))
                item.update(
                    {
                        "last_attempt_at": outcome["last_attempt_at"] if outcome else None,
                        "last_outcome_status": outcome["status"] if outcome else None,
                        "last_outcome_reason": (
                            outcome["reason_code"] if outcome else None
                        ),
                        "last_outcome_event_id": outcome["event_id"] if outcome else None,
                    }
                )
                activity_rows.append(item)

            non_delivery_reasons = sorted(
                set([*reasons, *recorded_failure_reasons, *outcome_reasons])
            )
            public_recorded = any(
                exposure.publish_status in PUBLIC_PROMO_EXPOSURE_STATUSES
                for exposure in recorded
            )
            delivery_state = (
                "recorded_publication"
                if public_recorded
                else "non_delivery_recorded"
                if non_delivery_reasons
                else "not_observed"
            )

            return {
                "campaign": self.snapshots._campaign_summary(campaign),
                "campaign_revision": rev,
                "targets": [
                    {"event_id": r["event_id"], "target_type": r["target_type"]}
                    for r in targets[:16]
                ],
                "activities": activity_rows,
                "recorded_exposures": {
                    "rows": exposure_rows,
                    "has_more": len(recorded) > 16,
                    "accounting": "recorded_publication_units_not_browser_impressions",
                },
                "ineligibility_reasons": sorted(set(reasons)),
                "non_delivery_reasons": non_delivery_reasons,
                "delivery_state": delivery_state,
                "delivery_evidence": (
                    "durable_recorded_rows_current_activity_outcome_and_server_state"
                ),
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
        if kind == "promo_update":
            mutable = {
                key
                for key in (
                    "title",
                    "ends_at",
                    "total_exposure_goal",
                    "daily_exposure_cap",
                    "priority",
                )
                if key in request
            }
            if not mutable or any(request[key] is None for key in mutable):
                fail("INVALID_ARGUMENTS")
            if "title" in request and not str(request["title"]).strip():
                fail("INVALID_ARGUMENTS")
            if grant and "priority" in request:
                fail("PROMO_PARAMETER_DENIED")
            if "ends_at" in request:
                effective_end = date.fromisoformat(request["ends_at"])
                for event in events:
                    if event is not None:
                        effective_end = clamp_campaign_end_to_event(effective_end, event)
                if effective_end < date.today():
                    fail("PROMO_EVENT_INELIGIBLE")
                if grant and effective_end > date.today() + timedelta(
                    days=grant.limits["campaign_days"]
                ):
                    fail("PROMO_LIMIT_EXCEEDED")
            if grant:
                if (
                    "total_exposure_goal" in request
                    and request["total_exposure_goal"] > grant.limits["campaign_exposures"]
                ):
                    fail("PROMO_LIMIT_EXCEEDED")
                if (
                    "daily_exposure_cap" in request
                    and request["daily_exposure_cap"] > grant.limits["daily_exposures"]
                ):
                    fail("PROMO_LIMIT_EXCEEDED")
        if kind in {"promo_create", "promo_activity_add", "promo_resume"}:
            if not events or any(
                e is None
                or e.lifecycle_status != "active"
                or e.silent
                or (e.end_date or e.date) < date.today().isoformat()
                for e in events
            ):
                fail("PROMO_EVENT_INELIGIBLE")
        if kind in {"promo_create", "promo_activity_add"}:
            surface = request["surface"]
            for event in events:
                verdicts, profiles, _source_group_id = await self._placement_context(
                    session, event, grant
                )
                if not verdicts[surface]["available"]:
                    fail("PROMO_SURFACE_UNAVAILABLE")
                if (
                    surface == "video_general"
                    and request.get("profile_key") not in profiles
                ):
                    fail("PROMO_PROFILE_DENIED")
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
            "promo_update",
        }:
            fail("INVALID_ARGUMENTS")
        from .promo_command_tools import (
            CreateRequest,
            ActivityRequest,
            StateRequest,
            UpdateRequest,
        )
        from pydantic import ValidationError

        try:
            if kind == "promo_create":
                request = CreateRequest.model_validate(request).model_dump()
            elif kind == "promo_activity_add":
                request = ActivityRequest.model_validate(request).model_dump()
            elif kind == "promo_update":
                request = UpdateRequest.model_validate(request).model_dump(
                    exclude_unset=True
                )
                if request.get("title") is not None:
                    request["title"] = request["title"].strip()
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
                    target_event = await session.get(Event, request["event_id"])
                    if target_event is None:
                        fail("PROMO_EVENT_INELIGIBLE")
                    await self._configure_server_activity(
                        session,
                        campaign_id=campaign_id,
                        surface=request["surface"],
                        event=target_event,
                        grant=grant,
                    )
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
                    target_id = (
                        await session.execute(
                            select(PromoTarget.event_id)
                            .where(
                                PromoTarget.campaign_id == campaign_id,
                                PromoTarget.target_type == "event",
                            )
                            .order_by(PromoTarget.id.asc())
                            .limit(1)
                        )
                    ).scalar()
                    target_event = (
                        await session.get(Event, target_id) if target_id else None
                    )
                    if target_event is None:
                        fail("PROMO_EVENT_INELIGIBLE")
                    await self._configure_server_activity(
                        session,
                        campaign_id=campaign_id,
                        surface=request["surface"],
                        event=target_event,
                        grant=grant,
                    )
                elif kind == "promo_update":
                    campaign_id = request["campaign_id"]
                    campaign = await session.get(PromoCampaign, campaign_id)
                    if campaign is None:
                        fail("NOT_FOUND")
                    updated_fields = []
                    effective_ends_at = None
                    if "title" in request:
                        campaign.title = request["title"]
                        updated_fields.append("title")
                    if "ends_at" in request:
                        effective_end = date.fromisoformat(request["ends_at"])
                        target_ids = list(
                            (
                                await session.execute(
                                    select(PromoTarget.event_id).where(
                                        PromoTarget.campaign_id == campaign_id,
                                        PromoTarget.target_type == "event",
                                    )
                                )
                            ).scalars()
                        )
                        for target_id in target_ids:
                            event = await session.get(Event, target_id)
                            if event is not None:
                                effective_end = clamp_campaign_end_to_event(
                                    effective_end, event
                                )
                        campaign.ends_at = datetime(
                            effective_end.year,
                            effective_end.month,
                            effective_end.day,
                            23,
                            59,
                            59,
                            tzinfo=timezone.utc,
                        )
                        effective_ends_at = effective_end.isoformat()
                        updated_fields.append("ends_at")
                    if "total_exposure_goal" in request:
                        campaign.total_exposure_goal = request["total_exposure_goal"]
                        updated_fields.append("total_exposure_goal")
                    if "daily_exposure_cap" in request:
                        campaign.daily_exposure_cap = request["daily_exposure_cap"]
                        updated_fields.append("daily_exposure_cap")
                    if "priority" in request:
                        campaign.priority = normalize_promo_priority(request["priority"])
                        updated_fields.append("priority")
                    campaign.updated_at = datetime.now(timezone.utc)
                    session.add(campaign)
                    await session.flush()
                    _, _, _, updated_revision = await self.snapshots._campaign_snapshot(
                        session, campaign_id
                    )
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
                if kind == "promo_update":
                    result.update(
                        {
                            "updated_fields": sorted(updated_fields),
                            "campaign_revision": updated_revision,
                            "effective_ends_at": effective_ends_at,
                        }
                    )
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
