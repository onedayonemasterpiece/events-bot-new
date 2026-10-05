"""Closed MCP schemas for the shared promo commands."""

from typing import Annotated, Literal
from pydantic import Field
from .actors import ActorContext
from .promo_tools import (
    StrictInput,
    CampaignRequest,
    ActivityRequest,
    CommitInput,
    OperationInput,
    CampaignInput,
    CampaignsInput,
    _parse,
    _profile,
)
from .event_command_tools import Decision
from .tool_catalog import ToolSpec


class CapabilityInput(StrictInput):
    event_id: Annotated[int, Field(ge=1, le=2**63 - 1)]


class CreateRequest(CampaignRequest):
    accepted_event_operation_ref: str | None = None


class StateRequest(CampaignInput):
    campaign_revision: Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
    action: Literal["pause", "resume", "archive"]


class UpdateRequest(CampaignInput):
    campaign_revision: Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
    title: Annotated[str, Field(min_length=1, max_length=200)] | None = None
    ends_at: Annotated[str, Field(pattern=r"^\d{4}-\d{2}-\d{2}$")] | None = None
    total_exposure_goal: Annotated[int, Field(ge=1, le=10000)] | None = None
    daily_exposure_cap: Annotated[int, Field(ge=1, le=10000)] | None = None
    priority: Annotated[int, Field(ge=0, le=3)] | None = None


class CreateInput(StrictInput):
    request: CreateRequest
    idempotency_key: Annotated[str, Field(min_length=8, max_length=160)]


class ActivityInput(CreateInput):
    request: ActivityRequest


class StateInput(CreateInput):
    request: StateRequest


class UpdateInput(CreateInput):
    request: UpdateRequest


def build_promo_command_tools(service, *, partner=False, state_only=False):
    specs = []

    def add(name, model, handler, description, write=False, review=False):
        scope = (
            "partners:manage"
            if review
            else ("partner:promo:request" if write else "partner:promo:read")
            if partner
            else ("promo:write" if write else "promo:read")
        )
        specs.append(
            ToolSpec(
                name=name,
                title=name.replace("_", " "),
                description=description,
                input_schema=model.model_json_schema(),
                output_schema={"type": "object"},
                scopes=frozenset({scope}),
                handler=handler,
                read_only=not write,
                cacheable=False,
                publicly_discoverable=False,
                timeout_seconds=15,
            )
        )

    for name, model, kind in [
        ("promo_campaign_create", CreateInput, "promo_create"),
        ("promo_activity_add", ActivityInput, "promo_activity_add"),
        ("promo_campaign_update", UpdateInput, "promo_update"),
        ("promo_campaign_state", StateInput, None),
    ]:
        if state_only and kind not in {None, "promo_update"}:
            continue

        async def prepare(args, context, model=model, kind=kind):
            parsed = _parse(model, args)
            if kind in {"promo_create", "promo_activity_add"}:
                _profile(parsed.request)
            request = parsed.request.model_dump(
                exclude_unset=(kind == "promo_update")
            )
            actual_kind = kind or "promo_" + request.pop("action")
            return await service.prepare(
                ActorContext.from_mcp(context),
                actual_kind,
                request,
                parsed.idempotency_key,
            )

        async def commit(args, context):
            parsed = _parse(CommitInput, args)
            return await service.operation(
                ActorContext.from_mcp(context),
                parsed.preparation_ref,
                parsed.action_digest,
            )

        add(
            name + "_prepare",
            model,
            prepare,
            "Freeze a separate promo proposal for your own event/campaign. Read promo_capabilities or promo_campaign_get for current revision first. No campaign/activity changes until commit and required owner review.",
            True,
        )
        add(
            name + "_commit",
            CommitInput,
            commit,
            "Commit the exact prepared promo operation. review_required awaits owner approval. accepted records a campaign change, not delivery. Read promo_operation_get then promo_campaign_get; never blindly repeat outcome_unknown.",
            True,
        )
    if not state_only:

        async def capabilities(args, context):
            p = _parse(CapabilityInput, args)
            return await service.capabilities(
                ActorContext.from_mcp(context), p.event_id
            )

        async def read(args, context):
            p = _parse(CampaignInput, args)
            return await service.campaign_get(
                ActorContext.from_mcp(context), p.campaign_id
            )

        async def listing(args, context):
            p = _parse(CampaignsInput, args)
            return await service.campaigns_list(
                ActorContext.from_mcp(context), **p.model_dump()
            )

        async def operation(args, context):
            p = _parse(OperationInput, args)
            return await service.operation(
                ActorContext.from_mcp(context), p.operation_ref
            )

        add(
            "promo_capabilities",
            CapabilityInput,
            capabilities,
            "Read current own event revision, supported partner-safe surfaces and limits before preparing promo.",
        )
        add(
            "promo_campaign_get",
            CampaignInput,
            read,
            "Read own current campaign, revision, activities, limits, eligibility and recent recorded exposures. Recorded exposure is not a browser impression or live delivery verification.",
        )
        add(
            "promo_campaigns_list",
            CampaignsInput,
            listing,
            "List only currently authorized campaigns using bounded keyset pagination.",
        )
        add(
            "promo_operation_get",
            OperationInput,
            operation,
            "Read your durable frozen promo proposal and result; historical replay never resumes a paused or archived campaign.",
        )
    if not partner:

        async def decide(args, context):
            p = _parse(Decision, args)
            return await service.operation(
                ActorContext.from_mcp(context),
                p.preparation_ref,
                p.action_digest,
                p.decision,
            )

        async def review(args, context):
            p = _parse(OperationInput, args)
            return await service.operation(
                ActorContext.from_mcp(context), p.operation_ref, review=True
            )

        add(
            "promo_operation_decide",
            Decision,
            decide,
            "Approve or reject frozen partner promo after reviewing promo_operation_review_get. Current grant, event, campaign revision and limits are checked inside the canonical transaction.",
            True,
            True,
        )
        add(
            "promo_operation_review_get",
            OperationInput,
            review,
            "Read frozen partner promo request for owner approval; this does not create or execute a campaign.",
            False,
            True,
        )
    return tuple(specs)
