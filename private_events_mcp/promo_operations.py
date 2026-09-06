"""Owner OAuth promo preparation and atomic commit on the canonical ledger.

Authorization is mandatory on the exact transaction session, including historical
replay. Capabilities use a read snapshot; operation mutations use BEGIN IMMEDIATE. Host callbacks are local/read-only and must not commit or call providers.
Preparation does not create a campaign. Commit creates an ACTIVE campaign through
the existing service; its ordinary scheduler may execute it later.
"""
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import re
import secrets
import time
from types import SimpleNamespace

from sqlalchemy import text
from static_site_release import event_public_revision
from promo import PartnerPromoSpec, create_partner_event_promo_campaign, PARTNER_PROMO_VIDEO_PROFILES, PARTNER_PROMO_SLOT_POLICIES
from .oauth import SUBJECT

FIELDS = frozenset({'accepted_event_operation_ref','event_id','event_revision','surface',
    'profile_key','slot_policy','count','ends_at','is_editorial','sponsorship_disclosure','title_override'})
REF = re.compile(r'evt_op_[A-Za-z0-9_-]{20,120}')
SHA = re.compile(r'[a-f0-9]{64}')
KIND = 'promo_campaign_create'


class PromoOperationError(ValueError):
    pass


def fail(code):
    raise PromoOperationError(code)


def canonical(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


@dataclass(frozen=True)
class PromoActor:
    subject: str
    client_id: str
    audience: str

    def validate(self):
        if self.subject != SUBJECT:
            fail('PROMO_ACCESS_DENIED')
        for value in asdict(self).values():
            if not isinstance(value,str) or not value.strip() or len(value)>2048 or any(ord(c)<32 or ord(c)==127 for c in value):
                fail('PROMO_ACCESS_DENIED')


def validate_request(request):
    if not isinstance(request,dict) or set(request)!=FIELDS:
        fail('PROMO_INVALID_REQUEST')
    if type(request['event_id']) is not int or not 1<=request['event_id']<=2**63-1:
        fail('PROMO_INVALID_REQUEST')
    if type(request['count']) is not int or not 1<=request['count']<=10000 or type(request['is_editorial']) is not bool:
        fail('PROMO_INVALID_REQUEST')
    for field, pattern in [('accepted_event_operation_ref',REF),('event_revision',SHA)]:
        if not isinstance(request[field],str) or pattern.fullmatch(request[field]) is None:
            fail('PROMO_INVALID_REQUEST')
    if request['surface'] not in ('video_general','vk_repost'):
        fail('PROMO_INVALID_REQUEST')
    if not isinstance(request['slot_policy'],str) or request['slot_policy'] not in PARTNER_PROMO_SLOT_POLICIES:
        fail('PROMO_INVALID_REQUEST')
    profile=request['profile_key']
    if ((request['surface']=='video_general' and (not isinstance(profile,str) or profile not in PARTNER_PROMO_VIDEO_PROFILES))
            or (request['surface']=='vk_repost' and profile is not None)):
        fail('PROMO_INVALID_REQUEST')
    for field,limit,nullable in [('profile_key',120,True),('slot_policy',120,False),
        ('sponsorship_disclosure',1000,True),('title_override',300,True)]:
        value=request[field]
        if value is None and nullable:
            continue
        if not isinstance(value,str) or not value.strip() or len(value)>limit or any(ord(c)<32 or ord(c)==127 for c in value):
            fail('PROMO_INVALID_REQUEST')
    try:
        if not isinstance(request['ends_at'],str) or date.fromisoformat(request['ends_at']).isoformat()!=request['ends_at']:
            fail('PROMO_INVALID_REQUEST')
    except ValueError:
        fail('PROMO_INVALID_REQUEST')
    return dict(request)


def revision(row):
    values=dict(row)
    for field in ('linked_event_ids','photo_urls','topics'):
        if isinstance(values.get(field),str):
            values[field]=json.loads(values[field])
    for field in ('silent','time_is_default','is_free','pushkin_card'):
        if values.get(field) is not None:
            values[field]=bool(values[field])
    return event_public_revision(SimpleNamespace(**values))


class PromoOperationStore:
    def __init__(self,database,authorize,clock=time.time):
        if not callable(authorize):
            fail('PROMO_AUTHORIZATION_REQUIRED')
        self.database,self.authorize,self.clock=database,authorize,clock

    async def _authorize(self,session,actor,action,request):
        actor.validate()
        # Do not expose a mutable reference to the frozen envelope to callbacks.
        if await self.authorize(session,actor,action,dict(request)) is not True:
            fail('PROMO_ACCESS_DENIED')
        if not session.in_transaction():
            fail('PROMO_TRANSACTION_LOST')

    async def _accepted_target(self,session,actor,request):
        result=await session.execute(text('SELECT * FROM event_change_log WHERE operation_ref=:ref'),
                                     {'ref':request['accepted_event_operation_ref']})
        op=result.mappings().first()
        if (op is None or op['operation_kind']!='create' or op['status']!='accepted'
                or type(op['event_id']) is not int or op['event_id']!=request['event_id']
                or any(op['actor_'+key]!=value for key,value in asdict(actor).items())):
            fail('PROMO_EVENT_BINDING_DENIED')
        try:
            accepted=json.loads(op['result_json'])
            if not isinstance(accepted,dict) or accepted.get('status')!='accepted':
                fail('PROMO_EVENT_BINDING_DENIED')
            ids=accepted['event_ids']
        except (ValueError,TypeError,KeyError):
            fail('PROMO_EVENT_BINDING_DENIED')
        if not isinstance(ids,list) or len(ids)!=1 or type(ids[0]) is not int or ids[0]!=request['event_id']:
            fail('PROMO_EVENT_BINDING_DENIED')
        result=await session.execute(text('SELECT * FROM event WHERE id=:id'),{'id':request['event_id']})
        event=result.mappings().first()
        if event is None or event['identity_status']!='canonical' or event['merged_into_event_id'] is not None:
            fail('PROMO_EVENT_BINDING_DENIED')
        return event

    async def _binding(self,session,actor,request):
        event=await self._accepted_target(session,actor,request)
        if revision(event)!=request['event_revision']:
            fail('PROMO_EVENT_REVISION_CONFLICT')

    async def capabilities(self,accepted_event_operation_ref,event_id,*,actor):
        """Read current exact accepted target tokens, not an inventory or eligibility permit."""
        actor.validate()
        if (not isinstance(accepted_event_operation_ref,str)
                or REF.fullmatch(accepted_event_operation_ref) is None
                or type(event_id) is not int or not 1<=event_id<=2**63-1):
            fail('PROMO_INVALID_REQUEST')
        request={'accepted_event_operation_ref':accepted_event_operation_ref,'event_id':event_id}
        async with self.database.get_session() as session:
            try:
                await session.execute(text('BEGIN'))
                await self._authorize(session,actor,'capabilities',request)
                event=await self._accepted_target(session,actor,request)
                response={**request,'event_revision':revision(event),
                    'lifecycle_status':event['lifecycle_status'],'silent':bool(event['silent']),
                    'supported_surfaces':['video_general','vk_repost'],
                    'video_profiles':dict(PARTNER_PROMO_VIDEO_PROFILES),
                    'slot_policies':dict(PARTNER_PROMO_SLOT_POLICIES),
                    'business_validation':'commit_recheck_required'}
                await session.rollback()  # Read snapshot only; no operation reservation.
                return response
            except BaseException:
                await session.rollback()
                raise

    def _stored(self,row,actor):
        if (row is None or row['operation_kind']!=KIND
                or any(row['actor_'+key]!=value for key,value in asdict(actor).items())):
            fail('PROMO_OPERATION_NOT_FOUND')
        try:
            envelope=json.loads(row['request_json'])
            request=validate_request(envelope['request'])
            if (set(envelope)!={'schema','actor','request','expires_at'} or envelope['schema']!='promo-preparation-v1'
                    or envelope['actor']!=asdict(actor) or type(envelope['expires_at']) is not int
                    or digest(envelope)!=row['action_digest'] or row['event_id']!=request['event_id']
                    or row['base_event_revision']!=request['event_revision']):
                fail('PROMO_OPERATION_CONFLICT')
        except (ValueError,TypeError,KeyError):
            fail('PROMO_OPERATION_CONFLICT')
        return envelope,request

    def _prepared(self,row,envelope):
        return {'operation_ref':row['operation_ref'],'preparation_ref':row['operation_ref'],
                'action_digest':row['action_digest'],'expires_at':envelope['expires_at'],
                'status':('expired' if row['status']=='prepared' and self.clock()>=envelope['expires_at'] else row['status']),
                'planned_campaign_status':'active',
                'business_validation':'commit_recheck_required'}

    def _receipt(self,row):
        try:
            receipt=json.loads(row['result_json'])
            expected={'schema':'promo-operation-result-v1','status':'accepted',
                'operation_ref':row['operation_ref'],'action_digest':row['action_digest'],
                'event_id':row['event_id'],'campaign_id':receipt['campaign_id'],
                'campaign_status_at_commit':'active','publication_state':'not_observed'}
            if receipt!=expected or type(receipt['campaign_id']) is not int or receipt['campaign_id']<1:
                fail('PROMO_OPERATION_CONFLICT')
        except (TypeError,ValueError,KeyError):
            fail('PROMO_OPERATION_CONFLICT')
        return receipt

    async def prepare(self,request,*,actor,idempotency_key):
        actor.validate()
        request=validate_request(request)
        if not isinstance(idempotency_key,str) or re.fullmatch(r'[A-Za-z0-9._~:@/-]{8,160}',idempotency_key) is None:
            fail('PROMO_INVALID_IDEMPOTENCY_KEY')
        key=hashlib.sha256(idempotency_key.encode()).hexdigest()
        async with self.database.get_session() as session:
            try:
                await session.execute(text('BEGIN IMMEDIATE'))
                await self._authorize(session,actor,'prepare',request)
                result=await session.execute(text('SELECT * FROM event_change_log WHERE operation_kind=:kind AND actor_subject=:subject AND actor_client_id=:client_id AND actor_audience=:audience AND idempotency_hash=:key'),
                    {**asdict(actor),'kind':KIND,'key':key})
                row=result.mappings().first()
                if row:
                    envelope,stored=self._stored(row,actor)
                    if stored!=request:
                        fail('PROMO_IDEMPOTENCY_CONFLICT')
                    if row['status'] not in ('prepared','accepted'):
                        fail('PROMO_OPERATION_CONFLICT')
                    if row['status']=='prepared' and self.clock()<envelope['expires_at']:
                        await self._binding(session,actor,request)
                    response=self._prepared(row,envelope)
                else:
                    await self._binding(session,actor,request)
                    envelope={'schema':'promo-preparation-v1','actor':asdict(actor),'request':request,'expires_at':int(self.clock())+600}
                    ref='evt_op_'+secrets.token_urlsafe(24)
                    values={**asdict(actor),'ref':ref,'kind':KIND,'key':key,'digest':digest(envelope),
                        'request':canonical(envelope),'event':request['event_id'],'revision':request['event_revision']}
                    await session.execute(text("INSERT INTO event_change_log(operation_ref,operation_kind,actor_subject,actor_client_id,actor_audience,idempotency_hash,action_digest,source_type,source_url,request_json,status,event_id,base_event_revision) VALUES(:ref,:kind,:subject,:client_id,:audience,:key,:digest,'owner_oauth','',:request,'prepared',:event,:revision)"),values)
                    response=self._prepared({'operation_ref':ref,'action_digest':values['digest'],'status':'prepared'},envelope)
                await session.commit()
                return response
            except BaseException:
                await session.rollback()
                raise

    async def commit(self,preparation_ref,*,action_digest,actor):
        if not isinstance(action_digest,str) or SHA.fullmatch(action_digest) is None:
            fail('PROMO_ACTION_DIGEST_CONFLICT')
        return await self._read_or_commit(preparation_ref,actor,action_digest)

    async def operation_get(self,operation_ref,*,actor):
        return await self._read_or_commit(operation_ref,actor,None)

    async def _read_or_commit(self,ref,actor,expected_digest):
        actor.validate()
        if not isinstance(ref,str) or REF.fullmatch(ref) is None:
            fail('PROMO_OPERATION_NOT_FOUND')
        async with self.database.get_session() as session:
            try:
                await session.execute(text('BEGIN IMMEDIATE'))
                row=(await session.execute(text('SELECT * FROM event_change_log WHERE operation_ref=:ref'),{'ref':ref})).mappings().first()
                envelope,request=self._stored(row,actor)
                await self._authorize(session,actor,'commit' if expected_digest is not None else 'operation_get',request)
                if expected_digest is not None and expected_digest!=row['action_digest']:
                    fail('PROMO_ACTION_DIGEST_CONFLICT')
                if row['status']=='accepted':
                    response=self._receipt(row)
                elif row['status']!='prepared':
                    fail('PROMO_OPERATION_CONFLICT')
                elif expected_digest is None:
                    response=self._prepared(row,envelope)
                else:
                    if self.clock()>=envelope['expires_at']:
                        fail('PROMO_PREPARATION_EXPIRED')
                    await self._binding(session,actor,request)
                    if any(row[field] for field in ('before_json','after_json','changed_fields_json',
                            'result_event_revision','domain_receipt_json','result_json')):
                        fail('PROMO_OPERATION_CONFLICT')
                    spec=PartnerPromoSpec(event_id=request['event_id'],creator_user_id=None,organization_name=None,
                        **{key:request[key] for key in ('surface','profile_key','slot_policy','count','is_editorial','sponsorship_disclosure','title_override')},
                        ends_at=date.fromisoformat(request['ends_at']))
                    result=await create_partner_event_promo_campaign(self.database,spec,
                        now_utc=datetime.fromtimestamp(self.clock(),timezone.utc),session=session)
                    if result.campaign is None or result.status!='created':
                        fail('PROMO_BUSINESS_VALIDATION_FAILED')
                    response={'schema':'promo-operation-result-v1','status':'accepted','operation_ref':ref,
                        'action_digest':row['action_digest'],'event_id':request['event_id'],
                        'campaign_id':int(result.campaign.id),'campaign_status_at_commit':'active','publication_state':'not_observed'}
                    changed=await session.execute(text("UPDATE event_change_log SET status='accepted',result_json=:result,updated_at=CURRENT_TIMESTAMP,completed_at=CURRENT_TIMESTAMP WHERE operation_ref=:ref AND status='prepared' AND action_digest=:digest"),
                        {'result':canonical(response),'ref':ref,'digest':row['action_digest']})
                    if changed.rowcount!=1:
                        fail('PROMO_OPERATION_CONFLICT')
                await session.commit()
                return response
            except BaseException:
                await session.rollback()
                raise
