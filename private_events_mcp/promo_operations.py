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
from promo import PartnerActivitySpec, add_partner_activity_to_campaign, PartnerPromoSpec, create_partner_event_promo_campaign, PARTNER_PROMO_VIDEO_PROFILES, PARTNER_PROMO_SLOT_POLICIES, PROMO_POLICY_GUARANTEED_ANY_POSITION, PROMO_POLICY_DIVERSE_SHUFFLE
from .oauth import SUBJECT

FIELDS = frozenset({'accepted_event_operation_ref','event_id','event_revision','surface',
    'profile_key','slot_policy','count','ends_at','is_editorial','sponsorship_disclosure','title_override'})
REF = re.compile(r'evt_op_[A-Za-z0-9_-]{20,120}')
SHA = re.compile(r'[a-f0-9]{64}')
KIND = 'promo_campaign_create'
ACTIVITY_KIND = 'promo_activity_add'
ACTIVITY_FIELDS = frozenset({'campaign_id','campaign_revision','surface','profile_key','slot_policy','count'})


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


def validate_slot_policy(request):
    policy=request['slot_policy']
    if ((request['surface']=='video_general' and (not isinstance(policy,str) or policy not in PARTNER_PROMO_SLOT_POLICIES))
            or (request['surface']=='vk_repost' and policy is not None)):
        fail('PROMO_INVALID_REQUEST')


def effective_selection_policy(request):
    return PROMO_POLICY_DIVERSE_SHUFFLE if request['surface']=='vk_repost' else request['slot_policy']


def service_slot_policy(request):
    # Legacy helper requires a placeholder; VK does not expose slot selection.
    return PROMO_POLICY_GUARANTEED_ANY_POSITION if request['surface']=='vk_repost' else request['slot_policy']


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
    validate_slot_policy(request)
    profile=request['profile_key']
    if ((request['surface']=='video_general' and (not isinstance(profile,str) or profile not in PARTNER_PROMO_VIDEO_PROFILES))
            or (request['surface']=='vk_repost' and profile is not None)):
        fail('PROMO_INVALID_REQUEST')
    for field,limit,nullable in [('profile_key',120,True),('slot_policy',120,True),
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

    @staticmethod
    def _campaign_summary(row):
        # Human-facing fields only, never goal_comment/creator/private configuration.
        output={'campaign_id':row['id'],
                'cap_accounting':'legacy_publication_units_not_browser_visibility'}
        for key in ('title','status','starts_at','ends_at','total_exposure_goal',
                    'daily_exposure_cap','priority','sponsorship_disclosure'):
            value=row[key]
            if isinstance(value,str):
                value=''.join(c for c in value if ord(c)>=32 and ord(c)!=127)[:1000 if key=='sponsorship_disclosure' else 300]
            output[key]=value
        return output

    async def campaigns_list(self,*,actor,after_id=0,limit=20,status=None):
        actor.validate()
        if (type(after_id) is not int or not 0<=after_id<=2**63-1
                or type(limit) is not int or not 1<=limit<=50
                or (status is not None and status not in ('draft','active','paused','archived'))):
            fail('PROMO_INVALID_REQUEST')
        request={'after_id':after_id,'limit':limit,'status':status}
        async with self.database.get_session() as session:
            try:
                await session.execute(text('BEGIN'))
                await self._authorize(session,actor,'campaigns_list',request)
                query='SELECT * FROM promo_campaign WHERE id>:after_id'
                if status is not None:
                    query+=' AND status=:status'
                rows=(await session.execute(text(query+' ORDER BY id ASC LIMIT :take'),
                    {**request,'take':limit+1})).mappings().all()
                more=len(rows)>limit
                visible=rows[:limit]
                response={'campaigns':[self._campaign_summary(row) for row in visible],
                    'has_more':more,'next_after_id':visible[-1]['id'] if more else None,
                    'publication_state':'not_observed','delivery_stats':'unavailable'}
                await session.rollback()
                return response
            except BaseException:
                await session.rollback()
                raise

    async def _campaign_snapshot(self,session,campaign_id):
        campaign=(await session.execute(text('SELECT * FROM promo_campaign WHERE id=:id'),
            {'id':campaign_id})).mappings().first()
        if campaign is None:
            fail('PROMO_CAMPAIGN_NOT_FOUND')
        snapshots={}
        for table in ('promo_target','promo_activity'):
            snapshots[table]=(await session.execute(text('SELECT * FROM '+table+
                ' WHERE campaign_id=:id ORDER BY id ASC LIMIT 257'),{'id':campaign_id})).mappings().all()
        targets=snapshots['promo_target']
        activities=snapshots['promo_activity']
        complete=len(targets)<=256 and len(activities)<=256
        # Hash every field in the bounded complete raw business snapshot,
        # including hidden config; never return its content or hash a prefix.
        campaign_revision=digest({'schema':'promo-campaign-revision-v1',
            'campaign':dict(campaign),'targets':[dict(row) for row in targets],
            'activities':[dict(row) for row in activities]}) if complete else None
        return campaign,targets,activities,campaign_revision

    async def campaign_get(self,campaign_id,*,actor):
        actor.validate()
        if type(campaign_id) is not int or not 1<=campaign_id<=2**63-1:
            fail('PROMO_INVALID_REQUEST')
        async with self.database.get_session() as session:
            try:
                await session.execute(text('BEGIN'))
                await self._authorize(session,actor,'campaign_get',{'campaign_id':campaign_id})
                campaign,targets,activities,campaign_revision=await self._campaign_snapshot(session,campaign_id)
                complete=campaign_revision is not None
                public_targets=[{'target_id':row['id'],'target_type':str(row['target_type'])[:80],
                    'event_id':row['event_id'] if row['target_type']=='event' else None} for row in targets[:16]]
                public_activities=[]
                for row in activities[:16]:
                    item={'activity_id':row['id'],'config_state':'unavailable'}
                    for key in ('surface','profile_key','slot','max_per_publish','target_exposure_goal',
                                'daily_cap','selection_policy','enabled'):
                        value=row[key]
                        item[key]=str(value)[:120] if isinstance(value,str) else value
                    item['enabled']=bool(item['enabled'])
                    public_activities.append(item)
                # These are bounded ledger observations, not live provider verification
                # or browser visibility counts. An empty page makes no delivery claim.
                recorded=(await session.execute(text(
                    'SELECT id AS exposure_id,event_id,activity_id,surface,placement_kind,'
                    'publish_status AS recorded_publish_status,'
                    'public_target_count AS recorded_public_target_count,'
                    'published_at AS recorded_published_at FROM promo_exposure '
                    'WHERE campaign_id=:id ORDER BY published_at DESC,id DESC LIMIT 17'),
                    {'id':campaign_id})).mappings().all()
                recorded_page={'source':'promo_exposure','scope':'recent_recorded_rows_only',
                    'rows':[dict(row) for row in recorded[:16]],'has_more':len(recorded)>16}
                response={'campaign':self._campaign_summary(campaign),
                    'recorded_exposures':recorded_page,
                    'campaign_revision':campaign_revision,
                    'revision_unavailable_reason':None if complete else 'snapshot_too_large',
                    'targets':public_targets,'targets_count':len(targets),
                    'targets_count_is_lower_bound':len(targets)>256,'targets_truncated':len(targets)>16,
                    'activities':public_activities,'activities_count':len(activities),
                    'activities_count_is_lower_bound':len(activities)>256,'activities_truncated':len(activities)>16,
                    'publication_state':'not_observed','delivery_stats':'unavailable'}
                await session.rollback()
                return response
            except BaseException:
                await session.rollback()
                raise

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
                    'slot_policy_by_surface':{'video_general':list(PARTNER_PROMO_SLOT_POLICIES),'vk_repost':[]},
                    'vk_selection_policy':PROMO_POLICY_DIVERSE_SHUFFLE,
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
                'business_validation':'commit_recheck_required',
                'effective_selection_policy':effective_selection_policy(envelope['request'])}

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
        return await self._read_or_commit(operation_ref,actor,None,expected_kind=None)

    async def _read_or_commit(self,ref,actor,expected_digest,expected_kind=KIND):
        actor.validate()
        if not isinstance(ref,str) or REF.fullmatch(ref) is None:
            fail('PROMO_OPERATION_NOT_FOUND')
        async with self.database.get_session() as session:
            try:
                await session.execute(text('BEGIN IMMEDIATE'))
                row=(await session.execute(text('SELECT * FROM event_change_log WHERE operation_ref=:ref'),{'ref':ref})).mappings().first()
                if row is not None and row['operation_kind']==ACTIVITY_KIND and expected_kind in (None,ACTIVITY_KIND):
                    response=await self._activity_read_or_apply(session,row,actor,expected_digest)
                    await session.commit()
                    return response
                if expected_kind==ACTIVITY_KIND:
                    fail('PROMO_OPERATION_KIND_CONFLICT')
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
                        **{key:request[key] for key in ('surface','profile_key','count','is_editorial','sponsorship_disclosure','title_override')},
                        slot_policy=service_slot_policy(request),
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

    @staticmethod
    def _activity_request(request):
        if not isinstance(request,dict) or set(request)!=ACTIVITY_FIELDS:
            fail('PROMO_INVALID_REQUEST')
        if (type(request['campaign_id']) is not int or not 1<=request['campaign_id']<=2**63-1
                or not isinstance(request['campaign_revision'],str) or SHA.fullmatch(request['campaign_revision']) is None
                or type(request['count']) is not int or not 1<=request['count']<=10000
                or request['surface'] not in ('video_general','vk_repost')):
            fail('PROMO_INVALID_REQUEST')
        validate_slot_policy(request)
        profile=request['profile_key']
        if ((request['surface']=='video_general' and (not isinstance(profile,str) or profile not in PARTNER_PROMO_VIDEO_PROFILES))
                or (request['surface']=='vk_repost' and profile is not None)):
            fail('PROMO_INVALID_REQUEST')
        return dict(request)

    def _activity_stored(self,row,actor):
        if (row is None or row['operation_kind']!=ACTIVITY_KIND
                or any(row['actor_'+key]!=value for key,value in asdict(actor).items())):
            fail('PROMO_OPERATION_NOT_FOUND')
        try:
            envelope=json.loads(row['request_json'])
            request=self._activity_request(envelope['request'])
            if (set(envelope)!={'schema','actor','request','expires_at'} or envelope['schema']!='promo-activity-preparation-v1'
                    or envelope['actor']!=asdict(actor) or type(envelope['expires_at']) is not int
                    or digest(envelope)!=row['action_digest'] or row['event_id'] is not None
                    or row['base_event_revision'] is not None):
                fail('PROMO_OPERATION_CONFLICT')
        except (ValueError,TypeError,KeyError):
            fail('PROMO_OPERATION_CONFLICT')
        return envelope,request

    def _activity_prepared(self,row,envelope):
        return {'operation_ref':row['operation_ref'],'preparation_ref':row['operation_ref'],
            'action_digest':row['action_digest'],'expires_at':envelope['expires_at'],
            'status':'expired' if row['status']=='prepared' and self.clock()>=envelope['expires_at'] else row['status'],
            'planned_campaign_status':'unchanged','planned_activity_enabled':True,
            'effective_selection_policy':effective_selection_policy(envelope['request'])}

    async def _activity_snapshot(self,session,request):
        campaign,targets,activities,current=await self._campaign_snapshot(session,request['campaign_id'])
        if current is None:
            fail('PROMO_CAMPAIGN_SNAPSHOT_TOO_LARGE')
        if campaign['status']=='archived':
            fail('PROMO_CAMPAIGN_ARCHIVED')
        if campaign['status'] not in ('draft','active','paused'):
            fail('PROMO_CAMPAIGN_STATUS_INVALID')
        if len(activities)>=256:
            fail('PROMO_CAMPAIGN_SNAPSHOT_TOO_LARGE')
        if current!=request['campaign_revision']:
            fail('PROMO_CAMPAIGN_REVISION_CONFLICT')
        return campaign,targets,activities

    async def prepare_activity(self,request,*,actor,idempotency_key):
        actor.validate()
        request=self._activity_request(request)
        if not isinstance(idempotency_key,str) or re.fullmatch(r'[A-Za-z0-9._~:@/-]{8,160}',idempotency_key) is None:
            fail('PROMO_INVALID_IDEMPOTENCY_KEY')
        key=hashlib.sha256(idempotency_key.encode()).hexdigest()
        async with self.database.get_session() as session:
            try:
                await session.execute(text('BEGIN IMMEDIATE'))
                await self._authorize(session,actor,'prepare_activity',request)
                row=(await session.execute(text('SELECT * FROM event_change_log WHERE operation_kind=:kind AND actor_subject=:subject AND actor_client_id=:client_id AND actor_audience=:audience AND idempotency_hash=:key'),
                    {**asdict(actor),'kind':ACTIVITY_KIND,'key':key})).mappings().first()
                if row:
                    envelope,stored=self._activity_stored(row,actor)
                    if stored!=request:
                        fail('PROMO_IDEMPOTENCY_CONFLICT')
                    if row['status'] not in ('prepared','accepted'):
                        fail('PROMO_OPERATION_CONFLICT')
                    if row['status']=='prepared' and self.clock()<envelope['expires_at']:
                        await self._activity_snapshot(session,request)
                    response=self._activity_prepared(row,envelope)
                else:
                    await self._activity_snapshot(session,request)
                    envelope={'schema':'promo-activity-preparation-v1','actor':asdict(actor),'request':request,'expires_at':int(self.clock())+600}
                    ref='evt_op_'+secrets.token_urlsafe(24)
                    values={**asdict(actor),'ref':ref,'kind':ACTIVITY_KIND,'key':key,'digest':digest(envelope),'request':canonical(envelope)}
                    await session.execute(text("INSERT INTO event_change_log(operation_ref,operation_kind,actor_subject,actor_client_id,actor_audience,idempotency_hash,action_digest,source_type,source_url,request_json,status) VALUES(:ref,:kind,:subject,:client_id,:audience,:key,:digest,'owner_oauth','',:request,'prepared')"),values)
                    response=self._activity_prepared({'operation_ref':ref,'action_digest':values['digest'],'status':'prepared'},envelope)
                await session.commit()
                return response
            except BaseException:
                await session.rollback()
                raise

    async def commit_activity(self,preparation_ref,*,action_digest,actor):
        if not isinstance(action_digest,str) or SHA.fullmatch(action_digest) is None:
            fail('PROMO_ACTION_DIGEST_CONFLICT')
        return await self._read_or_commit(preparation_ref,actor,action_digest,expected_kind=ACTIVITY_KIND)

    async def _activity_read_or_apply(self,session,row,actor,expected_digest):
        envelope,request=self._activity_stored(row,actor)
        await self._authorize(session,actor,'commit_activity' if expected_digest is not None else 'operation_get',request)
        if expected_digest is not None and expected_digest!=row['action_digest']:
            fail('PROMO_ACTION_DIGEST_CONFLICT')
        if row['status']=='accepted':
            try:
                receipt=json.loads(row['result_json'])
                expected={'schema':'promo-activity-result-v1','status':'accepted','operation_ref':row['operation_ref'],
                    'action_digest':row['action_digest'],'campaign_id':request['campaign_id'],'activity_id':receipt['activity_id'],
                    'campaign_status_at_commit':receipt['campaign_status_at_commit'],
                    'activity_enabled_at_commit':True,'publication_state':'not_observed'}
                if (receipt!=expected or receipt.get('activity_enabled_at_commit') is not True or type(receipt['activity_id']) is not int or receipt['activity_id']<1
                        or receipt['campaign_status_at_commit'] not in ('draft','active','paused')):
                    fail('PROMO_OPERATION_CONFLICT')
                return receipt
            except (ValueError,TypeError,KeyError):
                fail('PROMO_OPERATION_CONFLICT')
        if row['status']!='prepared':
            fail('PROMO_OPERATION_CONFLICT')
        if expected_digest is None:
            return self._activity_prepared(row,envelope)
        if self.clock()>=envelope['expires_at']:
            fail('PROMO_PREPARATION_EXPIRED')
        campaign,targets,activities=await self._activity_snapshot(session,request)
        if any(row[field] for field in ('before_json','after_json','changed_fields_json','result_event_revision','domain_receipt_json','result_json')):
            fail('PROMO_OPERATION_CONFLICT')
        spec=PartnerActivitySpec(**{key:request[key] for key in ('campaign_id','surface','profile_key','count')},
            slot_policy=service_slot_policy(request))
        result=await add_partner_activity_to_campaign(self.database,spec,actor_user_id=None,
            now_utc=datetime.fromtimestamp(self.clock(),timezone.utc),session=session)
        if result.campaign is None or result.status!='created':
            fail('PROMO_BUSINESS_VALIDATION_FAILED')
        added=(await session.execute(text('SELECT id FROM promo_activity WHERE campaign_id=:id AND id>:previous ORDER BY id LIMIT 2'),
            {'id':request['campaign_id'],'previous':max((item['id'] for item in activities),default=0)})).scalars().all()
        if len(added)!=1:
            fail('PROMO_ACTIVITY_RESULT_CONFLICT')
        response={'schema':'promo-activity-result-v1','status':'accepted','operation_ref':row['operation_ref'],
            'action_digest':row['action_digest'],'campaign_id':request['campaign_id'],'activity_id':int(added[0]),
            'campaign_status_at_commit':campaign['status'],'activity_enabled_at_commit':True,'publication_state':'not_observed'}
        changed=await session.execute(text("UPDATE event_change_log SET status='accepted',result_json=:result,updated_at=CURRENT_TIMESTAMP,completed_at=CURRENT_TIMESTAMP WHERE operation_ref=:ref AND operation_kind=:kind AND status='prepared' AND action_digest=:digest"),
            {'result':canonical(response),'ref':row['operation_ref'],'kind':ACTIVITY_KIND,'digest':row['action_digest']})
        if changed.rowcount!=1:
            fail('PROMO_OPERATION_CONFLICT')
        return response
