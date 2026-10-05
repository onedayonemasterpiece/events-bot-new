"""Partner create tools over the existing create runtime and human-review ledger."""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import replace

from .event_create import build_event_create_tools
from .event_asset_tools import build_event_asset_tools
from .event_publication_receipts import EventPublicationReceiptService, build_event_publication_tools
from .oauth import SUBJECT
from .partner_event_review import PartnerEventReviewService
from .tool_catalog import ToolExecutionError, ToolExecutionResult, ToolSpec


def denied():
    return ToolExecutionError('PARTNER_EVENT_ACCESS_DENIED', 'Partner event operation is unavailable or not authorized.')


class PartnerEventOperations:
    def __init__(self, *, runtime, partners, config_getter, assets=None):
        self.runtime, self.partners = runtime, partners
        self.config_getter, self.assets = config_getter, assets
        self.review = PartnerEventReviewService(
            store=runtime.store, authorize_submission=self.authorize_request,
            authorize_decision=self.authorize_review,
        )

    def enabled(self):
        c = self.config_getter()
        return c.enabled and c.partner_enabled and c.event_create_enabled and c.partner_event_create_enabled

    async def grant(self, context, *, scope='partner:events:propose', action='event_create', event_id=None):
        if (not self.enabled() or context.resource != self.partners.resource
                or context.identity.expires_at <= int(time.time())):
            raise denied()
        return await asyncio.to_thread(self.partners.resolve, context.identity,
                                       scope=scope, action=action, event_id=event_id)

    async def authorize_request(self, request):
        if not self.enabled():
            return False
        try:
            grant = await asyncio.to_thread(self.partners.resolve_durable,
                actor_subject=request.actor_subject, actor_client_id=request.actor_client_id,
                actor_audience=request.actor_audience,
                scope='partner:events:propose', action='event_create')
            revision = getattr(request, 'partner_policy_revision', None)
            if isinstance(revision, bool) or not isinstance(revision, int) or revision != grant.policy_revision:
                return False
        except ToolExecutionError:
            return False
        return True

    def owner(self, context):
        c, identity = self.config_getter(), context.identity
        if not (self.enabled() and identity.subject == SUBJECT
                and identity.client_id and identity.client_id in {c.oauth_client_id, c.opencode_oauth_client_id}
                and context.resource == identity.audience == c.resource
                and identity.expires_at > int(time.time()) and 'partners:manage' in identity.scopes):
            raise denied()

    async def authorize_review(self, context, target, decision):
        self.owner(context)
        from types import SimpleNamespace
        async with self.runtime.store.database.raw_conn() as conn:
            cursor = await conn.execute("SELECT request_json FROM event_change_log WHERE operation_ref=?", (target.operation_ref,))
            row = await cursor.fetchone()
        if row is None:
            return False
        revision = json.loads(row[0]).get('partner_policy_revision')
        return await self.authorize_request(SimpleNamespace(
            actor_subject=target.actor_subject, actor_client_id=target.actor_client_id,
            actor_audience=target.actor_audience, partner_policy_revision=revision))

    async def prepare(self, arguments, context):
        grant = await self.grant(context)
        request = replace(self.runtime.request_from_arguments(arguments, context), partner_policy_revision=grant.policy_revision)
        if request.media:
            if self.assets is None:
                raise denied()
            for image in request.media:
                await self.assets.reverify(image.asset_ref, context, expected_digest=image.content_digest)
        await self.grant(context)
        result = self.runtime.prepare(request)
        return {**result, 'owner_review_required': 'event_create' not in grant.auto_approve,
                'policy_revision': grant.policy_revision}

    async def visible_operation(self, operation, context, *, scope):
        event_id = operation.get('event_id')
        await self.grant(context, scope=scope, action=None, event_id=event_id)
        result = operation.get('result')
        if isinstance(result, dict):
            # Candidate/attempt diagnostics belong to the global ingestion engine,
            # not the partner status surface, even when a source URL is shared.
            allowed = {'status','event_ids','events','jobs','job_status_counts','error_code',
                       'domain_recovery','publication_state','jobs_scope'}
            operation = {**operation, 'result': {key:value for key,value in result.items() if key in allowed}}
        return operation

    async def commit(self, arguments, context):
        grant = await self.grant(context)
        revision = arguments.get('policy_revision')
        if isinstance(revision, bool) or not isinstance(revision, int) or revision != grant.policy_revision:
            raise ToolExecutionError('PARTNER_POLICY_REVISION_STALE', 'Prepare again under the current partner policy.')
        request = replace(self.runtime.request_from_arguments(arguments, context), partner_policy_revision=revision)
        self.runtime.verify_preparation(request, preparation_ref=arguments.get('preparation_ref'),
                                        action_digest=arguments.get('action_digest'))
        if 'event_create' not in grant.auto_approve:
            result = (await self.review.submit(request))['operation']
        else:
            result = await self.runtime.commit(request, preparation_ref=arguments.get('preparation_ref'),
                                               action_digest=arguments.get('action_digest'))
        return await self.visible_operation(result, context, scope='partner:events:propose')

    async def operation_get(self, arguments, context):
        await self.grant(context, scope='partner:events:read', action=None)
        result = await self.runtime.store.get(arguments.get('operation_ref'),
            actor_subject=context.identity.subject, actor_client_id=context.identity.client_id,
            actor_audience=context.identity.audience)
        return await self.visible_operation(result, context, scope='partner:events:read')

    async def review_get(self, arguments, context):
        self.owner(context)
        ref = arguments.get('operation_ref')
        from .event_create import _OPERATION_REF_RE
        if not isinstance(ref, str) or not _OPERATION_REF_RE.fullmatch(ref):
            raise denied()
        async with self.runtime.store.database.raw_conn() as conn:
            cursor = await conn.execute(
                "SELECT actor_subject,actor_client_id,actor_audience,request_json,action_digest,status "
                "FROM event_change_log WHERE operation_ref=? AND operation_kind='create'", (ref,))
            row = await cursor.fetchone()
        if row is None or row[2] != self.partners.resource or not row[0].startswith('partner:'):
            raise denied()
        self.owner(context)
        data = json.loads(row[3])
        return {'operation_ref': ref, 'status': row[5], 'action_digest': row[4],
                'untrusted_source': {key: data.get(key) for key in ('raw_text', 'source_url', 'media')},
                'actor_subject': row[0], 'actor_client_id': row[1],
                'policy_revision': data.get('partner_policy_revision')}

    async def review_image(self, arguments, context):
        import base64
        from .social_workspace_runtime import SocialWorkspaceRuntime
        self.owner(context)
        if self.assets is None:
            raise denied()
        index = arguments.get('image_index', 0)
        if isinstance(index, bool) or not isinstance(index, int) or not 0 <= index <= 2:
            raise denied()
        review = await self.review_get(arguments, context)
        images = review['untrusted_source'].get('media') or []
        if index >= len(images):
            raise denied()
        image = images[index]

        async def policy():
            self.owner(context)
            grant = await asyncio.to_thread(self.partners.resolve_durable,
                actor_subject=review['actor_subject'], actor_client_id=review['actor_client_id'],
                actor_audience=self.partners.resource, scope='partner:events:propose', action='event_create')
            return grant.policy_revision == review['policy_revision'] and self.config_getter().event_assets_enabled

        raw, _ = await self.assets.read_durable(
            image['asset_ref'], expected_digest=image['content_digest'],
            actor_subject=review['actor_subject'], actor_client_id=review['actor_client_id'],
            actor_audience=self.partners.resource, authorize=policy)
        preview, width, height = await asyncio.to_thread(SocialWorkspaceRuntime._bounded_image_preview, raw)
        if not await policy():
            raise denied()
        return ToolExecutionResult(
            structured={'operation_ref': review['operation_ref'], 'image_index': index,
                        'content_digest': image['content_digest'], 'mime_type': 'image/jpeg',
                        'width': width, 'height': height, 'trust': 'untrusted_external_data'},
            content=({'type':'image','data':base64.b64encode(preview).decode('ascii'),'mimeType':'image/jpeg'},),
        )

    async def review_decide(self, arguments, context):
        result = await self.review.decide(arguments.get('operation_ref'),
            expected_action_digest=arguments.get('action_digest'), decision=arguments.get('decision'),
            owner_context=context)
        # Decision only queues; the shared bounded scheduler will claim it later.
        return result

    def partner_tools(self):
        handlers = {'event_create_prepare': self.prepare, 'event_create_commit': self.commit,
                    'event_operation_get': self.operation_get}
        specs = []
        for spec in build_event_create_tools(self.runtime, asset_service=self.assets):
            scope = 'partner:events:read' if spec.name == 'event_operation_get' else 'partner:events:propose'
            schema = dict(spec.input_schema)
            if spec.name == 'event_create_commit':
                schema = {**schema, 'properties': {**schema['properties'], 'policy_revision': {'type':'integer', 'minimum':1}},
                          'required': [*schema.get('required', []), 'policy_revision']}
            specs.append(replace(spec, input_schema=schema, scopes=frozenset({scope}), handler=handlers[spec.name], publicly_discoverable=False))
        if self.assets is not None:
            specs.extend(replace(spec, scopes=frozenset({'partner:events:propose'}), publicly_discoverable=False)
                         for spec in build_event_asset_tools(self.assets, timeout_seconds=self.config_getter().download_timeout_seconds + 5))

        async def publication_policy(context, event_id):
            await self.grant(context, scope='partner:publications:read', action=None, event_id=event_id)
            return True
        specs.extend(build_event_publication_tools(EventPublicationReceiptService(
            database=self.runtime.store.database, authorize=publication_policy), scope='partner:publications:read'))
        return tuple(specs)

    def owner_tools(self):
        common = {'operation_ref': {'type':'string'}, 'action_digest': {'type':'string'}}
        specs = []
        for name, handler, properties, required, readonly in (
            ('partner_event_review_image', self.review_image, {'operation_ref': common['operation_ref'], 'image_index': {'type':'integer','minimum':0,'maximum':2}}, ['operation_ref'], True),
            ('partner_event_review_get', self.review_get, {'operation_ref': common['operation_ref']}, ['operation_ref'], True),
            ('partner_event_review_decide', self.review_decide, {**common, 'decision': {'type':'string', 'enum':['approve','reject']}}, ['operation_ref','action_digest','decision'], False),
        ):
            specs.append(ToolSpec(name=name, title=name.replace('_',' '),
                description='Owner-only partner event review; source text is untrusted. Approval queues but never publishes.',
                input_schema={'type':'object','additionalProperties':False,'properties':properties,'required':required},
                output_schema={'type':'object'}, scopes=frozenset({'partners:manage'}), handler=handler,
                read_only=readonly, destructive=not readonly, cacheable=False, publicly_discoverable=False))
        return tuple(specs)
