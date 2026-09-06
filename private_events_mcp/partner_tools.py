"""Owner administration and strictly portfolio-scoped partner read projection."""
from __future__ import annotations

import asyncio
from typing import Any, Mapping

from .partner_access import PartnerAccessStore, _integer, _error
from .tool_catalog import ToolCallContext, ToolSpec


def _schema(properties, required=()):
    return {'type': 'object', 'additionalProperties': False, 'properties': properties, 'required': list(required)}


def _spec(name, title, scope, properties, handler, *, required=(), read_only=True, idempotent=True):
    return ToolSpec(name=name, title=title, description=title, scopes=frozenset({scope}),
                    input_schema=_schema(properties, required), output_schema={'type': 'object'},
                    handler=handler, read_only=read_only, destructive=not read_only,
                    idempotent=idempotent, open_world=False, publicly_discoverable=False,
                    cacheable=False)


def build_partner_admin_tools(store: PartnerAccessStore, config):
    def owner(context):
        identity = context.identity
        if identity.audience != config.resource or identity.client_id not in {
            config.oauth_client_id, config.opencode_oauth_client_id,
        } or 'partners:manage' not in identity.scopes or identity.subject.startswith('partner:'):
            raise _error('OWNER_REQUIRED')

    async def create(args, context):
        owner(context)
        result = await asyncio.to_thread(store.create, **args)
        result.update(mcp_url=config.partner_resource, authorization_endpoint=config.authorization_endpoint,
                      token_endpoint=config.token_endpoint,
                      credential_delivery='Show login_secret once; deliver privately. Never place it in the native app as client_secret.')
        return result

    async def read(args, context):
        owner(context)
        if args.get('principal_id'):
            grant = await asyncio.to_thread(store.get, args['principal_id'])
            with store._connect() as conn:
                ids = [row[0] for row in conn.execute('SELECT event_id FROM mcp_partner_event WHERE principal_id=? ORDER BY event_id', (grant.principal_id,))]
            return dict(grant.public(), event_ids=ids)
        return {'partners': await asyncio.to_thread(store.list, before=args.get('before'), limit=args.get('limit', 25))}

    async def change(args, context):
        owner(context)
        return await asyncio.to_thread(store.change, **args)

    return (
        _spec('partner_create', 'Create a Telegram-independent partner and return a private login code once', 'partners:manage', {
            'tenant_id': {'type':'string'}, 'organization_id': {'type':'string'}, 'display_name': {'type':'string'},
            'policy': {'type':'object'}, 'redirect_uris': {'type':'array','items':{'type':'string'}},
            'event_ids': {'type':'array','items':{'type':'integer'}}, 'expires_at': {'type':'integer'},
        }, create, required=('tenant_id','organization_id','display_name','policy','redirect_uris','expires_at'), read_only=False, idempotent=False),
        _spec('partner_get', 'Read a partner or a bounded partner directory; credentials are never returned', 'partners:manage', {
            'principal_id': {'type':'string'}, 'before':{'type':'string'},'limit':{'type':'integer','minimum':1,'maximum':50},
        }, read),
        _spec('partner_access_change', 'Suspend/resume/revoke/rotate or replace current rights and portfolio with revision checking', 'partners:manage', {
            'principal_id':{'type':'string'}, 'action':{'type':'string','enum':['suspend','resume','revoke','rotate','policy','portfolio']},
            'expected_revision':{'type':'integer'}, 'policy':{'type':'object'},
            'event_ids':{'type':'array','items':{'type':'integer'}},'expires_at':{'type':'integer'},
        }, change, required=('principal_id','action','expected_revision'), read_only=False),
    )


def build_partner_read_tools(store: PartnerAccessStore):
    async def workspace(args, context):
        grant = await asyncio.to_thread(store.resolve, context.identity, scope='partner:events:read')
        return dict(grant.public(), telegram_required=False, capabilities={
            'events_read': True,
            # Do not advertise not-yet-wired mutations or placeholder placements.
            'event_operations': False, 'promo_operations': False,
        })

    def read_events(args, identity):
        event_id = args.get('event_id')
        if event_id is not None:
            event_id = _integer(event_id, 1, 2**63-1, 'event_id')
        grant = store.resolve(identity, scope='partner:events:read', event_id=event_id)
        limit = _integer(args.get('limit', 20), 1, 50, 'limit')
        before = args.get('before_event_id')
        if before is not None:
            before = _integer(before, 1, 2**63-1, 'before_event_id')
        query = args.get('query', '')
        if not isinstance(query, str) or len(query) > 200:
            raise _error('INVALID_ARGUMENTS')
        # Tenant/organization checks occur in SQL before search, limits and readback.
        where = 'p.principal_id=? AND p.tenant_id=? AND p.organization_id=?'
        params: list[Any] = [grant.principal_id, grant.tenant_id, grant.organization_id]
        if event_id is not None:
            where += ' AND e.id=?'; params.append(event_id)
        if before is not None:
            where += ' AND e.id<?'; params.append(before)
        if query:
            where += ' AND instr(lower(e.title),lower(?))>0'; params.append(query)
        with store._connect() as conn:
            rows = conn.execute('SELECT e.id,e.title,e.description,e.date,e.time,e.end_date,e.location_name,e.city,e.lifecycle_status '
                'FROM event e JOIN mcp_partner_event p ON p.event_id=e.id WHERE '+where+' ORDER BY e.id DESC LIMIT ?', (*params,limit)).fetchall()
        return {'events':[dict(row) for row in rows], 'next_before_event_id':rows[-1]['id'] if len(rows)==limit else None}

    async def events(args, context):
        return await asyncio.to_thread(read_events, args, context.identity)

    return (
        _spec('partner_workspace_get', 'Read the current partner identity and current capabilities', 'partner:events:read', {}, workspace),
        _spec('partner_events_list', 'Search/read only explicitly assigned events; direct foreign IDs are denied', 'partner:events:read', {
            'event_id':{'type':'integer'}, 'query':{'type':'string','maxLength':200},
            'before_event_id':{'type':'integer'}, 'limit':{'type':'integer','minimum':1,'maximum':50},
        }, events),
    )
