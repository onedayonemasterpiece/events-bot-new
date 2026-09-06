"""Default-off owner-only durable Hero draft tools. No generation or publication."""
from __future__ import annotations

import time
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from hero_talk.compiler import HeroCompileError
from hero_talk.store import HeroActor, HeroProgramStore, HeroStoreError
from .oauth import SUBJECT
from .tool_catalog import ToolExecutionError, ToolSpec

Id=Annotated[str,Field(pattern=r'^[A-Za-z0-9][A-Za-z0-9_.:-]{0,119}$')]
Text=Annotated[str,Field(min_length=1,max_length=1000)]


class StrictModel(BaseModel):
    model_config=ConfigDict(extra='forbid',strict=True)


class FragmentStyle(StrictModel):
    accent: bool | None=None
    breakAfter: bool | None=None


class LiteralFragment(FragmentStyle):
    text: Text


class FactFragment(FragmentStyle):
    fact_token: Id


class LinkFragment(FragmentStyle):
    link_token: Id
    label: Annotated[str,Field(min_length=1,max_length=200)]


class Node(StrictModel):
    node_id: Id
    fragments: Annotated[list[LiteralFragment | FactFragment | LinkFragment],Field(min_length=1,max_length=24)]
    next_node_id: Id | None=None
    media_ref: Id | None=None
    media_policy: Literal['required','optional_text_fallback'] | None=None


class Chain(StrictModel):
    chain_id: Id
    nodes: Annotated[list[Node],Field(min_length=1,max_length=3)]


class Dependency(StrictModel):
    ref: Id
    revision: Annotated[str,Field(min_length=1,max_length=200)]


class OwnerDraft(StrictModel):
    program_id: Id
    revision: Annotated[int,Field(ge=1,le=2**63-1)]
    # No delegated campaign edit or generated-text provenance in this first tool slice.
    origin: Literal['editorial_program']
    author_mode: Literal['verbatim']
    placements: Annotated[list[Literal['home_hero','page_end']],Field(min_length=1,max_length=2)]
    topic_anchor: Id
    chains: Annotated[list[Chain],Field(min_length=1,max_length=8)]
    safe_until: Annotated[str,Field(min_length=20,max_length=40)]
    dependencies: Annotated[list[Dependency],Field(max_length=64)]


class PrepareInput(StrictModel):
    action: Literal['upsert_draft']
    program: OwnerDraft
    expected_revision: Annotated[int,Field(ge=0,le=2**63-2)]
    idempotency_key: Annotated[str,Field(min_length=8,max_length=160,pattern=r'^[A-Za-z0-9._~:@/-]+$')]


class CommitInput(StrictModel):
    preparation_ref: Annotated[str,Field(pattern=r'^hero_op_[A-Za-z0-9_-]{20,120}$')]
    action_digest: Annotated[str,Field(pattern=r'^[a-f0-9]{64}$')]


class GetInput(StrictModel):
    program_id: Id


class OperationInput(StrictModel):
    operation_ref: Annotated[str,Field(pattern=r'^hero_op_[A-Za-z0-9_-]{20,120}$')]


def _parse(model,args):
    try:
        return model.model_validate(args)
    except ValidationError:
        # Pydantic errors include input values; never expose private draft text.
        raise ToolExecutionError('HERO_INVALID_ARGUMENTS') from None


def _safe_error(exc):
    code='HERO_'+str(exc).upper()
    if len(code)>63 or not code.replace('_','').isalnum():
        code='HERO_DRAFT_REJECTED'
    return ToolExecutionError(code)


class HeroDraftOperations:
    def __init__(self,database,*,config_getter):
        self.database=database
        self.config_getter=config_getter

    def _current(self,context,scope):
        config=self.config_getter(); identity=context.identity
        return bool(config.enabled and config.hero_drafts_enabled
                    and identity.subject==SUBJECT and identity.expires_at>int(time.time())
                    and identity.client_id and identity.client_id in {config.oauth_client_id,config.opencode_oauth_client_id}
                    and identity.audience==context.resource==config.resource and scope in identity.scopes)

    def _store(self,context,scope):
        if not self._current(context,scope):
            raise ToolExecutionError('HERO_OWNER_SCOPE_REQUIRED')
        actor=HeroActor(context.identity.subject,context.identity.client_id,context.identity.audience)
        async def authorize(conn,actual_actor,action,current):
            return bool(actual_actor==actor and self._current(context,scope))
        return HeroProgramStore(self.database,authorize=authorize),actor

    async def prepare(self,args,context):
        parsed=_parse(PrepareInput,args)
        store,actor=self._store(context,'hero:write')
        program=parsed.program.model_dump(exclude_none=True)
        # No normalization/coercion of owner text. These structural checks do not
        # claim semantic, media, public route or rendered acceptance.
        from hero_talk.compiler import _date
        try:
            _date(program['safe_until'])
            if len(set(program['placements']))!=len(program['placements']):
                raise HeroCompileError('placement_invalid')
            result=await store.prepare_draft(program,expected_revision=parsed.expected_revision,
                                             actor=actor,idempotency_key=parsed.idempotency_key)
        except (HeroCompileError,HeroStoreError) as exc:
            raise _safe_error(exc) from None
        return {**result,'preparation_ref':result['operation_ref'],
                'effect':'store_private_draft_only','publication_enabled':False,
                'pending_gates':['semantic_review','canonical_dependencies','media_rights','rendered_preview','public_readback','live_permit']}

    async def commit(self,args,context):
        parsed=_parse(CommitInput,args)
        store,actor=self._store(context,'hero:write')
        try:
            return await store.commit_draft(parsed.preparation_ref,action_digest=parsed.action_digest,actor=actor)
        except (HeroCompileError,HeroStoreError) as exc:
            raise _safe_error(exc) from None

    async def get(self,args,context):
        parsed=_parse(GetInput,args);store,actor=self._store(context,'hero:read')
        try:
            return await store.get(parsed.program_id,actor=actor)
        except (HeroCompileError,HeroStoreError) as exc:
            raise _safe_error(exc) from None

    async def operation_get(self,args,context):
        parsed=_parse(OperationInput,args);store,actor=self._store(context,'hero:read')
        try:
            return await store.operation_get(parsed.operation_ref,actor=actor)
        except (HeroCompileError,HeroStoreError) as exc:
            raise _safe_error(exc) from None

    def tools(self):
        result=[]
        for name,model,scope,handler,write,title in (
            ('hero_talk_prepare',PrepareInput,'hero:write',self.prepare,True,'Prepare one private verbatim editorial draft; no publication'),
            ('hero_talk_commit',CommitInput,'hero:write',self.commit,True,'Store the exact prepared Hero draft; never activate'),
            ('hero_talk_get',GetInput,'hero:read',self.get,False,'Read exact Hero program draft and desired/active revisions'),
            ('hero_talk_operation_get',OperationInput,'hero:read',self.operation_get,False,'Read the current actor-bound Hero draft operation'),
        ):
            result.append(ToolSpec(name=name,title=title,description=title,input_schema=model.model_json_schema(),
                output_schema={'type':'object'},scopes=frozenset({scope}),handler=handler,
                read_only=not write,destructive=False,idempotent=True,open_world=False,
                cacheable=False,publicly_discoverable=False,timeout_seconds=5.0 if write else None))
        return tuple(result)
