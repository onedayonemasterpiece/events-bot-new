"""Default-off owner promo projection over the existing campaign services."""
from __future__ import annotations

import time
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .oauth import SUBJECT
from .promo_operations import PromoActor, PromoOperationError, PromoOperationStore
from .tool_catalog import ToolExecutionError, ToolSpec


class StrictInput(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)


OperationRef = Annotated[str, Field(pattern=r'^evt_op_[A-Za-z0-9_-]{20,120}$')]
Digest = Annotated[str, Field(pattern=r'^[a-f0-9]{64}$')]


class CampaignRequest(StrictInput):
    accepted_event_operation_ref: OperationRef
    event_id: Annotated[int, Field(ge=1, le=2**63-1)]
    event_revision: Digest
    surface: Literal['video_general', 'vk_repost']
    profile_key: Literal['popular_review', 'default', 'konb'] | None
    slot_policy: Literal['guaranteed_any_position', 'first_two_slots', 'first_slot'] | None
    count: Annotated[int, Field(ge=1, le=10000)]
    ends_at: Annotated[str, Field(pattern=r'^\d{4}-\d{2}-\d{2}$')]
    is_editorial: bool
    sponsorship_disclosure: Annotated[str, Field(min_length=1, max_length=500)] | None
    title_override: Annotated[str, Field(min_length=1, max_length=200)] | None


class PrepareInput(StrictInput):
    request: CampaignRequest
    idempotency_key: Annotated[str, Field(min_length=8, max_length=160,
                                        pattern=r'^[A-Za-z0-9._~:@/-]+$')]


class ActivityRequest(StrictInput):
    campaign_id: Annotated[int, Field(ge=1, le=2**63-1)]
    campaign_revision: Digest
    surface: Literal['video_general', 'vk_repost']
    profile_key: Literal['popular_review', 'default', 'konb'] | None
    slot_policy: Literal['guaranteed_any_position', 'first_two_slots', 'first_slot'] | None
    count: Annotated[int, Field(ge=1, le=10000)]


class ActivityPrepareInput(StrictInput):
    request: ActivityRequest
    idempotency_key: Annotated[str, Field(min_length=8, max_length=160,
                                        pattern=r'^[A-Za-z0-9._~:@/-]+$')]


class CommitInput(StrictInput):
    preparation_ref: OperationRef
    action_digest: Digest


class OperationInput(StrictInput):
    operation_ref: OperationRef


class CapabilitiesInput(StrictInput):
    accepted_event_operation_ref: OperationRef
    event_id: Annotated[int, Field(ge=1, le=2**63-1)]


class CampaignInput(StrictInput):
    campaign_id: Annotated[int, Field(ge=1, le=2**63-1)]


class CampaignsInput(StrictInput):
    after_id: Annotated[int, Field(ge=0, le=2**63-1)] = 0
    limit: Annotated[int, Field(ge=1, le=50)] = 20
    status: Literal['draft', 'active', 'paused', 'archived'] | None = None


def _parse(model, args):
    try:
        return model.model_validate(args)
    except ValidationError:
        raise ToolExecutionError('PROMO_INVALID_ARGUMENTS') from None


def _profile(request):
    if ((request.surface == 'video_general' and (request.profile_key is None or request.slot_policy is None))
            or (request.surface == 'vk_repost' and (request.profile_key is not None or request.slot_policy is not None))):
        raise ToolExecutionError('PROMO_INVALID_ARGUMENTS')


class OwnerPromoTools:
    def __init__(self, database, *, config_getter):
        self.database = database
        self.config_getter = config_getter

    def _current(self, context, scope):
        c, identity = self.config_getter(), context.identity
        return bool(c.enabled and c.owner_promo_enabled and c.event_create_enabled
                    and identity.subject == SUBJECT and identity.expires_at > int(time.time())
                    and identity.client_id and identity.client_id in {c.oauth_client_id, c.opencode_oauth_client_id}
                    and identity.audience == context.resource == c.resource and scope in identity.scopes)

    def _store(self, context, scope):
        if not self._current(context, scope):
            raise ToolExecutionError('PROMO_OWNER_SCOPE_REQUIRED')
        actor = PromoActor(context.identity.subject, context.identity.client_id, context.identity.audience)

        async def authorize(session, actual_actor, action, request):
            return actual_actor == actor and self._current(context, scope)

        return PromoOperationStore(self.database, authorize=authorize), actor

    async def _run(self, call):
        try:
            return await call
        except PromoOperationError as exc:
            code = str(exc).upper()
            if not code.startswith('PROMO_'):
                code = 'PROMO_' + code
            if len(code) > 63 or not code.replace('_', '').isalnum():
                code = 'PROMO_OPERATION_REJECTED'
            raise ToolExecutionError(code) from None

    async def prepare(self, args, context):
        parsed = _parse(PrepareInput, args)
        _profile(parsed.request)
        store, actor = self._store(context, 'promo:write')
        return await self._run(store.prepare(parsed.request.model_dump(), actor=actor,
                                             idempotency_key=parsed.idempotency_key))

    async def prepare_activity(self, args, context):
        parsed = _parse(ActivityPrepareInput, args)
        _profile(parsed.request)
        store, actor = self._store(context, 'promo:write')
        return await self._run(store.prepare_activity(parsed.request.model_dump(), actor=actor,
                                                      idempotency_key=parsed.idempotency_key))

    async def commit_activity(self, args, context):
        parsed = _parse(CommitInput, args)
        store, actor = self._store(context, 'promo:write')
        return await self._run(store.commit_activity(parsed.preparation_ref,
                                                     action_digest=parsed.action_digest, actor=actor))

    async def capabilities(self, args, context):
        parsed = _parse(CapabilitiesInput, args)
        store, actor = self._store(context, 'promo:read')
        return await self._run(store.capabilities(parsed.accepted_event_operation_ref,
                                                 parsed.event_id, actor=actor))

    async def campaign_get(self, args, context):
        parsed = _parse(CampaignInput, args)
        store, actor = self._store(context, 'promo:read')
        return await self._run(store.campaign_get(parsed.campaign_id, actor=actor))

    async def campaigns_list(self, args, context):
        parsed = _parse(CampaignsInput, args)
        store, actor = self._store(context, 'promo:read')
        return await self._run(store.campaigns_list(actor=actor, **parsed.model_dump()))

    async def commit(self, args, context):
        parsed = _parse(CommitInput, args)
        store, actor = self._store(context, 'promo:write')
        return await self._run(store.commit(parsed.preparation_ref, action_digest=parsed.action_digest, actor=actor))

    async def operation_get(self, args, context):
        parsed = _parse(OperationInput, args)
        store, actor = self._store(context, 'promo:read')
        return await self._run(store.operation_get(parsed.operation_ref, actor=actor))

    def tools(self):
        specs = (
            ('promo_activity_add_prepare', ActivityPrepareInput, 'promo:write', self.prepare_activity, True,
             'Prepare one activity in an existing campaign; keep its status, window, caps and other activities'),
            ('promo_activity_add_commit', CommitInput, 'promo:write', self.commit_activity, True,
             'Add the exact prepared activity without resuming a campaign; enabled campaigns may execute later'),
            ('promo_campaigns_list', CampaignsInput, 'promo:read', self.campaigns_list, False,
             'Read bounded current shared campaigns with keyset pagination; no delivery success implied'),
            ('promo_campaign_get', CampaignInput, 'promo:read', self.campaign_get, False,
             'Read current campaign status, targets and activities separately from historical operation receipts'),
            ('promo_capabilities', CapabilitiesInput, 'promo:read', self.capabilities, False,
             'Read your accepted Event current revision and supported promo inputs; commit rechecks eligibility'),
            ('promo_campaign_create_prepare', PrepareInput, 'promo:write', self.prepare, True,
             'Prepare a separate active campaign for your accepted Event; no campaign is created yet'),
            ('promo_campaign_create_commit', CommitInput, 'promo:write', self.commit, True,
             'Create the exact prepared active campaign; existing schedulers may publish afterwards'),
            ('promo_operation_get', OperationInput, 'promo:read', self.operation_get, False,
             'Read your exact promo operation; campaign creation is not publication evidence'),
        )
        return tuple(ToolSpec(name=name, title=title, description=title,
            input_schema=model.model_json_schema(), output_schema={'type': 'object'},
            scopes=frozenset({scope}), handler=handler, read_only=not write,
            destructive=False, idempotent=True, open_world=write, cacheable=False,
            publicly_discoverable=False, timeout_seconds=5.0 if write else None)
            for name, model, scope, handler, write, title in specs)
