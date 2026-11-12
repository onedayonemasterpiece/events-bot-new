import json
import hashlib
from datetime import date

import pytest
from sqlmodel import select
import main
from db import Database
from models import Event, Setting
import vk_calendar_comments as comments


def event(**kw):
    values=dict(id=1,title='Лекция',date='2026-11-14',time='18:00',location_name='Музей',description='Программа',source_text='Исходник',source_vk_post_url='https://vk.com/wall-231920894_10')
    values.update(kw)
    return Event(**values)


@pytest.mark.asyncio
async def test_calendar_preserves_time_and_explicit_all_day_span():
    timed=event()
    exhibition=event(id=2,time='',date='2026-11-14',end_date='2026-11-16')
    raw=await comments.calendar_bytes(None,[timed,exhibition])
    assert raw==await comments.calendar_bytes(None,[timed,exhibition])
    parts=main.Calendar.from_ical(raw).walk('VEVENT')
    assert parts[0].decoded('DTSTART').hour==18
    assert parts[1].decoded('DTSTART')==date(2026,11,14)
    assert parts[1].decoded('DTEND')==date(2026,11,17)
    assert str(parts[0]['UID'])!=str(parts[1]['UID'])


@pytest.mark.asyncio
async def test_bad_date_is_not_invented():
    with pytest.raises(ValueError,match='exact event date'):
        await comments.calendar_bytes(None,[event(date='')])


@pytest.mark.asyncio
@pytest.mark.parametrize('title,filename', [('Лекция','Лекция.ics'), ('🎨 👩🏽‍🎨 🇷🇺 Лекция ❤️','Лекция.ics'), ('1️⃣ C++ — 2026','1 C++ — 2026.ics'), ('🎶','Событие.ics')])
@pytest.mark.parametrize('adopt',[False,True])
async def test_delivery_reuses_document_after_comment_failure_and_updates_without_duplicate(tmp_path,monkeypatch,adopt,title,filename):
    db=Database(str(tmp_path/'db.sqlite'));await db.init()
    ev=event(title=title)
    async with db.get_session() as session:
        session.add(ev);await session.commit()
    monkeypatch.setattr(main,'VK_AFISHA_GROUP_ID','231920894')
    monkeypatch.setattr(main,'_vk_user_token',lambda:'fixture')
    bot=object()
    monkeypatch.setattr(main,'get_bot',lambda:bot)
    calls=[];fail=[not adopt];live_comments={90:29} if adopt else {};documents=[29];guids={}
    async def api(method,params,*context,**kwargs):
        assert context==(db,bot)
        calls.append((method,params))
        if method=='wall.getById':value={'items':[{'id':10,'post_type':'post','date':1794441600}]}
        elif method=='wall.getComments':value={'count':len(live_comments),'items':[{'id':cid,'from_id':-231920894,'attachments':[{'type':'doc','doc':{'ext':'ics','owner_id':20,'id':did}}]} for cid,did in live_comments.items()]}
        elif method=='docs.getWallUploadServer':
            assert 'group_id' not in params  # Actual group-storage access was denied.
            value={'upload_url':'https://upload.example/file'}
        elif method=='docs.save':
            assert params['title']==filename
            documents[0]+=1
            value={'doc':{'id':documents[0],'owner_id':20,'ext':'ics'}}
        elif method=='wall.createComment':
            cid=guids.setdefault(params['guid'],90+len(guids)+int(adopt))
            live_comments[cid]=int(params['attachments'].split('_')[1])
            if fail[0]:fail[0]=False;raise RuntimeError('Lost comment response')
            value={'comment_id':cid}
        elif method=='wall.deleteComment':
            assert params['comment_id'] in live_comments
            live_comments.pop(params['comment_id']);value=1
        else:raise AssertionError(method)
        return {'response':value}
    monkeypatch.setattr(main,'_vk_api',api)
    class Response:
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        def raise_for_status(self):pass
        async def json(self,**kw):return {'file':'uploaded-file'}
    class HTTP:
        def post(self,*args,**kw):return Response()
    monkeypatch.setattr(main,'get_http_session',lambda:HTTP())
    if not adopt:
        with pytest.raises(RuntimeError,match='Lost comment response'):
            await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
    assert await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url) is adopt
    assert sum(m=='docs.save' for m,_ in calls)==1
    creates=[p for m,p in calls if m=='wall.createComment']
    assert len(creates)==1
    assert len(live_comments)==1
    assert not await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
    before=len(calls)
    assert not await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
    assert len(calls)==before
    async with db.get_session() as session:
        row=await session.get(Event,1);row.time='19:00';session.add(row);await session.commit()
    assert await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
    assert [m for m,_ in calls[-2:]]==['wall.deleteComment','wall.createComment']
    assert calls[-1][1]['guid']!=creates[0]['guid']
    assert len(live_comments)==1
    async with db.get_session() as session:
        state=json.loads((await session.execute(select(Setting))).scalars().one().value)
    assert state['text'] in comments.COMMENT_TEXTS
    state['text']='Добавьте событие в календарь, чтобы не забыть 📅 Файл прикреплён.'
    old_hash=hashlib.sha256(filename.encode()+b'\0'+await comments.calendar_bytes(db,[event(title=title,time='19:00',added_at=ev.added_at)])).hexdigest()
    state.update(hash=old_hash,document_hash=old_hash)
    async with db.get_session() as session:
        row=(await session.execute(select(Setting))).scalars().one()
        row.value=json.dumps(state);session.add(row);await session.commit()
    before_saves=sum(m=='docs.save' for m,_ in calls)
    before_calls=len(calls)
    assert not await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
    assert len(calls)==before_calls
    assert sum(m=='docs.save' for m,_ in calls)==before_saves
    assert not any(m=='wall.editComment' for m,_ in calls)
    monkeypatch.setattr(comments,'COMMENT_TEXTS',('Другой шаблон',))
    before=len(calls)
    assert not await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
    assert len(calls)==before
    await db.close()


@pytest.mark.asyncio
async def test_external_wall_is_never_modified(monkeypatch):
    monkeypatch.setattr(main,'VK_AFISHA_GROUP_ID','231920894')
    assert not await comments.ensure_calendar_comment(None,event(),'https://vk.com/wall-999_10')


@pytest.mark.asyncio
@pytest.mark.parametrize('state',['postponed','silent','cancelled'])
async def test_nonpublic_or_hidden_event_has_no_document_upload(tmp_path,monkeypatch,state):
    db=Database(str(tmp_path/'db.sqlite'));await db.init()
    ev=event(silent=state=='silent',lifecycle_status='cancelled' if state=='cancelled' else 'active')
    async with db.get_session() as session:
        session.add(ev);await session.commit()
    monkeypatch.setattr(main,'VK_AFISHA_GROUP_ID','231920894')
    monkeypatch.setattr(main,'_vk_user_token',lambda:'fixture')
    async def api(method,params,*context,**kwargs):
        assert state=='postponed' and method=='wall.getById'
        return {'response':{'items':[{'id':10,'post_type':'postpone'}]}}
    monkeypatch.setattr(main,'_vk_api',api)
    assert not await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
    async with db.get_session() as session:
        assert not (await session.execute(select(Setting))).scalars().all()
    await db.close()


@pytest.mark.asyncio
async def test_publication_defers_calendar_delivery_to_the_periodic_scan(tmp_path,monkeypatch):
    from unittest.mock import AsyncMock
    db=Database(str(tmp_path/'db.sqlite'));await db.init()
    ev=event(source_vk_post_url=None)
    async with db.get_session() as session:
        session.add(ev);await session.commit()
    monkeypatch.setattr(main,'VK_EVENTS_GROUP_ID','231920894')
    monkeypatch.setattr(main,'VK_AFISHA_GROUP_ID','231920894')
    monkeypatch.setattr(main,'_recover_managed_vk_live_url',AsyncMock(return_value=False))
    monkeypatch.setattr(main,'_managed_vk_post_state',AsyncMock(return_value=(True,True)))
    publish=AsyncMock(return_value='https://vk.com/wall-231920894_10')
    monkeypatch.setattr(main,'sync_vk_source_post',publish)
    delivery=AsyncMock(side_effect=[RuntimeError('Calendar network failure'),True])
    monkeypatch.setattr(comments,'ensure_calendar_comment',delivery)
    await main.job_sync_vk_source_post(1,db,None)
    await main.job_sync_vk_source_post(1,db,None)
    publish.assert_awaited_once()
    delivery.assert_not_awaited()
    await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('captcha',[False,True])
async def test_live_scan_uses_exact_postponed_binding_for_shared_session_post(tmp_path,monkeypatch,captcha):
    from unittest.mock import AsyncMock
    db=Database(str(tmp_path/'db.sqlite'));await db.init()
    ev=event(date='2099-11-14')
    async with db.get_session() as session:
        session.add(ev);await session.commit()
    monkeypatch.setattr(main,'VK_EVENTS_GROUP_ID','231920894')
    monkeypatch.setattr(main,'VK_SERVICE_TOKEN','read-fixture')
    api=AsyncMock(return_value={'response':{'items':[{'id':20,'postponed_id':10,'text':'Общее расписание двух сеансов'}]}})
    monkeypatch.setattr(main,'_vk_api',api)
    async def recover(db,ev,**kw):
        assert kw['candidate_item']['postponed_id']==10
        ev.source_vk_post_url='https://vk.com/wall-231920894_20'
        return True
    monkeypatch.setattr(main,'_recover_managed_vk_live_url',recover)
    delivery=AsyncMock(side_effect=main.VKAPIError(14,'Captcha needed') if captcha else None,return_value=True)
    pause=AsyncMock()
    monkeypatch.setattr(main,'vk_captcha_pause_outbox',pause)
    monkeypatch.setattr(comments,'ensure_calendar_comment',delivery)
    assert await main._reconcile_recent_managed_vk_live_urls(db,bot=None)==1
    assert delivery.await_args.args[-1]=='https://vk.com/wall-231920894_20'
    if captcha:pause.assert_awaited_once_with(db)
    else:pause.assert_not_awaited()
    await db.close()


@pytest.mark.asyncio
async def test_calendar_scan_stops_after_first_captcha_for_shared_post(tmp_path,monkeypatch):
    from unittest.mock import AsyncMock
    db=Database(str(tmp_path/'db.sqlite'));await db.init()
    try:
        async with db.get_session() as session:
            session.add(event(date='2099-11-14'))
            session.add(event(id=2,date='2099-11-14',time='20:00'))
            await session.commit()
        monkeypatch.setattr(main,'VK_EVENTS_GROUP_ID','231920894')
        monkeypatch.setattr(main,'VK_SERVICE_TOKEN','read-fixture')
        monkeypatch.setattr(main,'_vk_api',AsyncMock(return_value={'response':{'items':[{'id':10,'post_type':'post'}]}}))
        delivery=AsyncMock(side_effect=main.VKAPIError(14,'Captcha needed'))
        pause=AsyncMock()
        monkeypatch.setattr(comments,'ensure_calendar_comment',delivery)
        monkeypatch.setattr(main,'vk_captcha_pause_outbox',pause)
        assert await main._reconcile_recent_managed_vk_live_urls(db,bot=None)==0
        assert delivery.await_count==1
        pause.assert_awaited_once_with(db)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_known_captcha_repeated_calendar_scan_does_not_renew_pause(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone
    from unittest.mock import AsyncMock
    from models import JobOutbox, JobStatus, JobTask

    db = Database(str(tmp_path / 'captcha-cadence.sqlite'))
    await db.init()
    original_marker = 'captcha_wait:original'
    original_at = datetime.now(timezone.utc) - timedelta(minutes=9)
    real_api = main._vk_api
    pauses = AsyncMock(wraps=main.vk_captcha_pause_outbox)
    user_cached_calls = []
    service_reads = []
    try:
        async with db.get_session() as session:
            session.add(event(date='2099-11-14'))
            await session.flush()
            session.add(JobOutbox(event_id=1, task=JobTask.vk_sync, status=JobStatus.paused,
                                  last_error=original_marker, updated_at=original_at,
                                  next_run_at=original_at + timedelta(days=3650)))
            await session.commit()
        monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '231920894')
        monkeypatch.setattr(main, 'VK_AFISHA_GROUP_ID', '231920894')
        monkeypatch.setattr(main, 'VK_SERVICE_TOKEN', 'read-fixture')
        monkeypatch.setattr(main, '_vk_user_token', lambda: 'user-fixture')
        monkeypatch.setattr(main, '_vk_captcha_needed', True)
        monkeypatch.setattr(main, '_vk_captcha_resume', None)
        monkeypatch.setattr(main, 'get_bot', lambda: None)
        monkeypatch.setattr(comments, 'calendar_bytes', AsyncMock(return_value=b'calendar'))
        monkeypatch.setattr(main, 'vk_captcha_pause_outbox', pauses)

        async def api(method, params, *args, **kwargs):
            if kwargs.get('token_kind') == 'service':
                service_reads.append(method)
                return {'response': {'items': [{'id': 10, 'post_type': 'post'}]}}
            user_cached_calls.append(method)
            # The real transport returns cached code14 before any HTTP call.
            return await real_api(method, params, *args, **kwargs)

        monkeypatch.setattr(main, '_vk_api', api)
        for _ in range(2):
            async with db.get_session() as session:
                session.add(JobOutbox(event_id=1, task=JobTask.vk_sync,
                                      status=JobStatus.pending))
                await session.commit()
            assert await main._reconcile_recent_managed_vk_live_urls(db, bot=None) == 0

        async with db.get_session() as session:
            rows = (await session.execute(select(JobOutbox).order_by(JobOutbox.id))).scalars().all()
            actual = [(r.status.value, r.last_error, str(r.updated_at)) for r in rows]
        print({'pause_calls': pauses.await_count, 'cached_user_methods': user_cached_calls,
               'actual_jobs': actual})
        pauses.assert_not_awaited()
        assert service_reads == ['wall.get', 'wall.get']
        assert [row.status for row in rows] == [JobStatus.paused, JobStatus.pending, JobStatus.pending]
        assert rows[0].last_error == original_marker
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_live_scan_reuses_exact_provider_item_before_calendar_cutoff(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock

    db = Database(str(tmp_path / 'calendar-provider-item.sqlite'))
    await db.init()
    try:
        async with db.get_session() as session:
            session.add(event(date='2099-11-14'))
            await session.commit()
        item = {'id': 10, 'owner_id': -231920894, 'post_type': 'post', 'date': 1}
        calls = []
        monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '231920894')
        monkeypatch.setattr(main, 'VK_AFISHA_GROUP_ID', '231920894')
        monkeypatch.setattr(main, 'VK_SERVICE_TOKEN', 'service-fixture')
        monkeypatch.setattr(main, '_vk_user_token', lambda: 'user-fixture')
        monkeypatch.setattr(main, '_vk_captcha_needed', False)
        monkeypatch.setattr(comments, 'calendar_bytes', AsyncMock(return_value=b'calendar'))

        async def api(method, params, *args, **kwargs):
            calls.append((method, kwargs.get('token_kind')))
            return {'response': {'items': [item]}}

        monkeypatch.setattr(main, '_vk_api', api)
        assert await main._reconcile_recent_managed_vk_live_urls(db, bot=None) == 0
        assert calls == [('wall.get', 'service')]
        for mismatch in ({**item, 'id': 11}, {**item, 'owner_id': -999}):
            calls.clear()
            assert not await comments.ensure_calendar_comment(
                db, event(date='2099-11-14'), 'https://vk.com/wall-231920894_10',
                candidate_item=mismatch,
            )
            assert calls == [('wall.getById', 'user')]
        # The actual old publication timestamp still excludes calendar delivery.
        async with db.get_session() as session:
            assert (await session.execute(select(Setting))).scalars().all() == []
    finally:
        await db.close()

@pytest.mark.asyncio
@pytest.mark.parametrize('age,post_type', [(0,'post'),(180,'post'),(-3600,'post'),(1800,'postpone')])
async def test_calendar_waits_after_actual_publication(tmp_path,monkeypatch,age,post_type):
    from datetime import datetime,timezone
    from unittest.mock import AsyncMock
    fixed=datetime(2026,11,12,14,0,tzinfo=timezone.utc)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return fixed
    db=Database(str(tmp_path/'delayed.sqlite'));await db.init()
    try:
        async with db.get_session() as session:
            session.add(event());await session.commit()
        monkeypatch.setattr(comments,'datetime',Clock)
        monkeypatch.setattr(comments,'calendar_bytes',AsyncMock(return_value=b'calendar'))
        monkeypatch.setattr(main,'VK_AFISHA_GROUP_ID','231920894')
        monkeypatch.setattr(main,'_vk_user_token',lambda:'fixture')
        api=AsyncMock()
        monkeypatch.setattr(main,'_vk_api',api)
        item=dict(id=10,owner_id=-231920894,date=int(fixed.timestamp())-age,post_type=post_type)
        assert not await comments.ensure_calendar_comment(db,event(),event().source_vk_post_url,candidate_item=item)
        api.assert_not_awaited()
    finally:await db.close()

@pytest.mark.asyncio
async def test_calendar_scan_delivers_one_changed_comment_and_skips_unchanged(tmp_path,monkeypatch):
    from unittest.mock import AsyncMock
    db=Database(str(tmp_path/'one-comment.sqlite'));await db.init()
    try:
        async with db.get_session() as session:
            for eid in range(1,4):
                session.add(event(id=eid,date='2099-11-14',source_vk_post_url=f'https://vk.com/wall-231920894_{eid}'))
            await session.commit()
        monkeypatch.setattr(main,'VK_EVENTS_GROUP_ID','231920894')
        monkeypatch.setattr(main,'VK_SERVICE_TOKEN','fixture')
        monkeypatch.setattr(main,'_vk_captcha_needed',False)
        items=[dict(id=eid,owner_id=-231920894,date=1794480000,post_type='post') for eid in range(1,4)]
        monkeypatch.setattr(main,'_vk_api',AsyncMock(return_value={'response':{'items':items}}))
        delivery=AsyncMock(side_effect=[False,True,True])
        monkeypatch.setattr(comments,'ensure_calendar_comment',delivery)
        await main._reconcile_recent_managed_vk_live_urls(db,bot=None)
        assert delivery.await_count==2
    finally:await db.close()
