"""SQLite routing integration using actual producers, worker and snapshots.
No production, external network, providers, Kaggle or VK writes.
Static handler is an explicit external-consumer stub: this verifies queue and
snapshot transitions, not semantic quality, remote adoption or VK rendering.
"""
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import main
import event_vector_sync as vectors

ALLOWED = {main.JobTask.event_vector_sync, main.JobTask.static_site_build}


@pytest.mark.asyncio
async def test_initialized_sqlite_normal_routing(tmp_path, monkeypatch):
    for key, value in {
        'ENABLE_EVENT_VECTOR_SYNC': '1',
        'ENABLE_STATIC_SITE_KAGGLE_BUILDER': '1',
        'STATIC_SITE_BUILD_PROFILE': 'related-only',
        'STATIC_SITE_RELATED_MODE': 'pgvector',
        'STATIC_SITE_GEMMA_RELATED_VERIFY': '1',
        'EVENT_UPDATE_SYNC': '',
        'DISABLE_ICS_JOBS': '1',
        'DISABLE_PAGE_JOBS': '',
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv('EVENT_MEDIA_REQUIRE_CDN', raising=False)
    monkeypatch.setattr(main, '_vk_captcha_needed', False)
    monkeypatch.setattr(main, 'VK_EVENTS_GROUP_ID', '231920894')
    receipt_path = tmp_path / 'vector.json'
    monkeypatch.setenv('EVENT_VECTOR_SYNC_RECEIPT_PATH', str(receipt_path))
    monkeypatch.setenv('STATIC_SITE_VECTOR_RECEIPT_PATH', str(receipt_path))

    db = main.Database(str(tmp_path / 'initialized.sqlite'))
    await db.init()
    picks, submissions = [], []
    first_failure = True
    advance_during_v1 = True
    ids = {}
    frozen_revisions = {}

    async def row(task):
        async with db.get_session() as session:
            records = (await session.execute(main.select(main.JobOutbox).where(
                main.JobOutbox.task == task).order_by(main.JobOutbox.id))).scalars().all()
            return records

    async def advance_vector_due():
        # Fixture clock advance only, local SQLite. This is not production resume.
        with sqlite3.connect(db.path) as con:
            con.execute("update joboutbox set next_run_at=? where task='event_vector_sync' and status in ('pending','error')",
                        ((datetime.now(timezone.utc) - timedelta(seconds=2)).replace(tzinfo=None).isoformat(' '),))

    async def schedule(event):
        return await main.schedule_event_update_tasks(db, event,
            skip_vk_sync=True, defer_external_projections=True)

    async def fake_process(cmd, *, stage, run_id):
        nonlocal first_failure, advance_during_v1
        if stage == 'export':
            Path(cmd[cmd.index('--output-dir')+1], 'preview-events.json').write_text('{"events": []}')
            return []
        if first_failure:
            first_failure = False
            raise RuntimeError('offline provider failure')
        # Actual vector function made its backup before reaching this boundary.
        if advance_during_v1:
            advance_during_v1 = False
            async with db.get_session() as session:
                original = await session.get(main.Event, ids['a'])
                original.title = 'A revision a2'
                session.add(original)
                newer = main.Event(title='New twelve', description='facts', source_text='source',
                    date='2099-01-01', time='19:00', location_name='Venue', photo_urls=['https://offline.invalid/twelve.jpg'])
                session.add(newer)
                await session.commit()
                await session.refresh(newer)
                ids['twelve'] = int(newer.id)
                await session.refresh(original)
                await schedule(original)
                await schedule(newer)
            # V2 is deliberately overdue before S1 is enqueued.
            await advance_vector_due()
        Path(cmd[cmd.index('--report-json')+1]).write_text(json.dumps({
            'events': 3, 'complete': True, 'coverage': {'status': 'complete'},
            'catalog_revision': 'a'*64, 'corpus_revision': 'b'*64,
            'search_document_revision': 'b'*64, 'search_v3_hash': 'b'*64,
            'related_v1_hash': 'c'*64,
        }))
        return []
    monkeypatch.setattr(vectors, '_run_process', fake_process)
    async def vector_handler(eid, db_obj, bot):
        picks.append('vector')
        return await vectors.job_event_vector_sync(eid, db_obj, bot)

    async def static_transport_consumer(eid, db_obj, bot):
        """Explicit stub for remote static consumer; never posts or calls Kaggle."""
        picks.append('static')
        receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else {}
        if receipt.get('complete'):
            projection = receipt['projection_snapshot']['sqlite_path']
            with sqlite3.connect(projection) as con:
                observed = dict(con.execute('select id,title from event'))
            submissions.append(observed)
        else:
            submissions.append('legacy-R0')
        return True

    monkeypatch.setitem(main.JOB_HANDLERS, main.JobTask.event_vector_sync.value, vector_handler)
    monkeypatch.setitem(main.JOB_HANDLERS, main.JobTask.static_site_build.value, static_transport_consumer)
    try:
        async with db.get_session() as session:
            a = main.Event(title='A revision a1', description='facts', source_text='source',
                date='2099-01-01', time='19:00', location_name='Venue')
            eleven = main.Event(title='New eleven', description='facts', source_text='source',
                date='2099-01-01', time='19:00', location_name='Venue', photo_urls=['https://offline.invalid/eleven.jpg'])
            session.add_all([a, eleven]); await session.commit()
            await session.refresh(a); await session.refresh(eleven)
            ids.update(a=int(a.id), eleven=int(eleven.id))
            frozen_revisions[str(a.id)] = main.event_public_revision(a)
            await schedule(a); await schedule(eleven)
        assert not await row(main.JobTask.static_site_build), 'automatic stale S0 was created'
        await advance_vector_due()
        assert await main._run_due_jobs_once(db, None, allowed_tasks=ALLOWED, max_jobs=1) == 1
        failed = (await row(main.JobTask.event_vector_sync))[-1]
        assert failed.status == main.JobStatus.error and failed.attempts == 1
        assert main._ensure_utc(failed.next_run_at) > datetime.now(timezone.utc)
        assert not submissions and not await row(main.JobTask.static_site_build)

        # Automatic helper calls during vector backoff must not create stale S0.
        for trigger in ('vk_publication_live', 'startup_catchup', 'calendar_rollover'):
            await main.enqueue_static_site_build_request(db, reason='offline automatic',
                event_ids=[ids['eleven']], trigger=trigger)
        assert not await row(main.JobTask.static_site_build)

        # One finite legacy request is permitted. It completes and does not
        # produce a new automatic copy of itself.
        await main.enqueue_job(db, ids['eleven'], main.JobTask.static_site_build,
            payload={'trigger': 'smart_update'}, coalesce_key='static_site_build:prod',
            next_run_at=datetime.now(timezone.utc)-timedelta(seconds=1))
        assert await main._run_due_jobs_once(db, None, allowed_tasks=ALLOWED, max_jobs=1) == 1
        assert picks[-1] == 'static' and submissions == ['legacy-R0']
        # The real initial publication guard must not mistake finite legacy
        # progress for readiness of the newly imported event. It exits before
        # any external publishing path.
        with pytest.raises(main.StaticSiteSingleFlightDeferred, match='vk_initial_waiting_related'):
            await main.job_sync_vk_source_post(ids['eleven'], db, None)
        await advance_vector_due()
        assert await main._run_due_jobs_once(db, None, allowed_tasks=ALLOWED, max_jobs=1) == 1
        assert picks[-1] == 'vector'
        receipt = json.loads(receipt_path.read_text())
        assert receipt['complete'] and str(ids['twelve']) not in receipt['event_revisions']
        assert receipt['event_revisions'][str(ids['a'])] == frozen_revisions[str(ids['a'])]

        # V2 was due earlier, but ready S1 must run first.
        assert await main._run_due_jobs_once(db, None, allowed_tasks=ALLOWED, max_jobs=1) == 1
        assert picks[-1] == 'static'
        observed = submissions[-1]
        assert observed[ids['a']] == 'A revision a1'
        assert ids['eleven'] in observed and ids['twelve'] not in observed
        assert await main._run_due_jobs_once(db, None, allowed_tasks=ALLOWED, max_jobs=1) == 1
        assert picks[-1] == 'vector'
        assert await main._run_due_jobs_once(db, None, allowed_tasks=ALLOWED, max_jobs=1) == 1
        observed = submissions[-1]
        assert picks[-1] == 'static'
        assert observed[ids['a']] == 'A revision a2' and ids['twelve'] in observed

        # Manual cold and HTML remain documented, direct static requests.
        for profile in ('related-only', 'production-candidate'):
            monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', profile)
            await main.enqueue_static_site_build_request(db, reason='explicit cold fixture',
                event_ids=[ids['eleven']], semantic_cache_mode='cold', force_rebuild=True)
            pending = [r for r in await row(main.JobTask.static_site_build) if r.status == main.JobStatus.pending]
            assert len(pending) == 1
            assert pending[0].payload['semantic_cache_mode'] == 'cold'
            assert pending[0].payload['force_rebuild'] is True
        print('initialized SQLite picks:', picks)
    finally:
        await db.close()
