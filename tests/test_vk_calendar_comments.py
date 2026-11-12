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
@pytest.mark.parametrize('adopt',[False,True])
async def test_delivery_reuses_document_after_comment_failure_and_updates_without_duplicate(tmp_path,monkeypatch,adopt):
    db=Database(str(tmp_path/'db.sqlite'));await db.init()
    ev=event()
    async with db.get_session() as session:
        session.add(ev);await session.commit()
    monkeypatch.setattr(main,'VK_AFISHA_GROUP_ID','231920894')
    monkeypatch.setattr(main,'_vk_user_token',lambda:'fixture')
    calls=[];fail=[not adopt]
    async def api(method,params,**kwargs):
        calls.append((method,params))
        if method=='wall.getById':value={'items':[{'id':10,'post_type':'post','date':1794441600}]}
        elif method=='wall.getComments':value={'count':int(adopt),'items':[{'id':90,'from_id':-231920894,'attachments':[{'type':'doc','doc':{'ext':'ics'}}]}] if adopt else []}
        elif method=='docs.getWallUploadServer':
            assert 'group_id' not in params  # Actual group-storage access was denied.
            value={'upload_url':'https://upload.example/file'}
        elif method=='docs.save':
            assert params['title']=='Лекция.ics'
            value={'doc':{'id':30,'owner_id':20,'ext':'ics'}}
        elif method=='wall.createComment':
            if fail[0]:fail[0]=False;raise RuntimeError('Lost comment response')
            value={'comment_id':90}
        elif method=='wall.editComment':value=1
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
    assert await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
    assert sum(m=='docs.save' for m,_ in calls)==1
    creates=[p for m,p in calls if m=='wall.createComment']
    if adopt:assert not creates
    else:assert len(creates)==2 and creates[0]['guid']==creates[1]['guid']
    assert await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
    before=len(calls)
    assert await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
    assert len(calls)==before
    async with db.get_session() as session:
        row=await session.get(Event,1);row.time='19:00';session.add(row);await session.commit()
    assert await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
    assert calls[-1][0]=='wall.editComment'
    async with db.get_session() as session:
        state=json.loads((await session.execute(select(Setting))).scalars().one().value)
    assert state['text'] in comments.COMMENT_TEXTS
    state['text']='Добавьте событие в календарь, чтобы не забыть 📅 Файл прикреплён.'
    old_hash=hashlib.sha256(await comments.calendar_bytes(db,[event(time='19:00')])).hexdigest()
    state.update(hash=old_hash,document_hash=old_hash)
    async with db.get_session() as session:
        row=(await session.execute(select(Setting))).scalars().one()
        row.value=json.dumps(state);session.add(row);await session.commit()
    before_saves=sum(m=='docs.save' for m,_ in calls)
    assert await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
    assert sum(m=='docs.save' for m,_ in calls)==before_saves+1
    assert calls[-1][0]=='wall.editComment' and calls[-1][1]['comment_id']==90
    assert 'два раза' in calls[-1][1]['message']
    assert sum(m=='wall.createComment' for m,_ in calls)==len(creates)
    before=len(calls)
    assert await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
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
    async def api(method,params,**kwargs):
        assert state=='postponed' and method=='wall.getById'
        return {'response':{'items':[{'id':10,'post_type':'postpone'}]}}
    monkeypatch.setattr(main,'_vk_api',api)
    assert not await comments.ensure_calendar_comment(db,ev,ev.source_vk_post_url)
    async with db.get_session() as session:
        assert not (await session.execute(select(Setting))).scalars().all()
    await db.close()


@pytest.mark.asyncio
async def test_failed_calendar_delivery_retries_without_another_wall_post(tmp_path,monkeypatch):
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
    with pytest.raises(RuntimeError,match='Calendar network failure'):
        await main.job_sync_vk_source_post(1,db,None)
    await main.job_sync_vk_source_post(1,db,None)
    publish.assert_awaited_once()
    assert delivery.await_count==2
    await db.close()


@pytest.mark.asyncio
async def test_live_scan_uses_exact_postponed_binding_for_shared_session_post(tmp_path,monkeypatch):
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
    delivery=AsyncMock(return_value=True)
    monkeypatch.setattr(comments,'ensure_calendar_comment',delivery)
    assert await main._reconcile_recent_managed_vk_live_urls(db,bot=None)==1
    assert delivery.await_args.args[-1]=='https://vk.com/wall-231920894_20'
    await db.close()
