"""Actual reaper + local initialized SQLite, external handler never dispatched."""
from datetime import datetime, timedelta, timezone

import pytest
import main
from sqlalchemy import text
from models import Event, JobOutbox, JobTask, JobStatus


@pytest.mark.asyncio
@pytest.mark.parametrize('ledger_status,age,heartbeat,expected', [
    ('created', 660, False, JobStatus.error),
    ('created', 590, False, JobStatus.running),
    ('alive', 18000, True, JobStatus.running),
])
async def test_prepush_created_rearms_only_after_existing_grace(tmp_path, monkeypatch, ledger_status, age, heartbeat, expected):
    monkeypatch.setenv('STATIC_SITE_PRE_HANDOFF_STALE_SECONDS','600')
    db=main.Database(str(tmp_path/'db.sqlite'));await db.init()
    now=datetime.now(timezone.utc)
    calls=[]
    async def handler(*args):calls.append(args);raise AssertionError('no external handler expected in this reaper tick')
    monkeypatch.setitem(main.JOB_HANDLERS,'static_site_build',handler)
    try:
        async with db.get_session() as session:
            event=Event(title='Offline prepush owner',date='2099-01-01',time='19:00',description='facts',source_text='facts',location_name='Venue')
            session.add(event);await session.commit();await session.refresh(event)
            owner=JobOutbox(event_id=event.id,task=JobTask.static_site_build,status=JobStatus.running,attempts=0,
                coalesce_key='static_site_build:prod',payload={'remote_handoff':{'run_id':'offline-prepush'}},
                updated_at=now-timedelta(seconds=age),next_run_at=now-timedelta(seconds=age))
            session.add(owner);await session.commit();await session.refresh(owner);owner_id=owner.id
        main.claim_static_site_build(db.path,job_id=owner_id,run_id='offline-prepush',input_fingerprint='a'*64,effective_date=now.date().isoformat(),request_watermark=None)
        async with db.get_session() as session:
            await session.execute(text('''INSERT INTO kaggle_run_ledger(run_id,kind,kernel_ref,dataset_ref,status,token_hash,progress_json,created_at,updated_at,last_heartbeat_at)
              VALUES('offline-prepush','static_site_builder','owner/fixed','owner/exact-input',:status,'offline-only','{}',:created,:updated,:heartbeat)'''),
              {'status':ledger_status,'created':(now-timedelta(seconds=age)).isoformat(),'updated':now.isoformat() if heartbeat else (now-timedelta(seconds=age)).isoformat(),'heartbeat':now.isoformat() if heartbeat else None})
            await session.commit()
        await main._run_due_jobs_once(db,None,allowed_tasks={JobTask.static_site_build},max_jobs=1)
        async with db.get_session() as session:
            owner=await session.get(JobOutbox,owner_id)
            assert owner.status==expected, (owner.status,owner.last_error)
            if expected==JobStatus.error:
                assert owner.last_error=='stale'
                assert owner.attempts==1
                assert owner.next_run_at.replace(tzinfo=timezone.utc)>now
            state=(await session.execute(text("SELECT active_run_id FROM static_site_build_state WHERE release_channel='secret_preview'"))).scalar_one()
            assert state=='offline-prepush'  # Recovery must retain the exact owner, not permit an unchecked new push.
        assert calls==[]
    finally:await db.close()
