"""Actual adopted-result owner and SQLite routing; remote output is a fixture."""
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import main
import event_vector_sync as vectors
from scripts.run_static_site_builder_kaggle import related_publication_anchor_ids


@pytest.mark.asyncio
async def test_quiet_initial_batch_continues_through_vector_without_new_import(tmp_path, monkeypatch):
    for key, value in {
        'ENABLE_EVENT_VECTOR_SYNC': '1',
        'ENABLE_STATIC_SITE_KAGGLE_BUILDER': '1',
        'STATIC_SITE_BUILD_PROFILE': 'related-only',
        'STATIC_SITE_GEMMA_RELATED_VERIFY': '1',
        'VK_EVENTS_GROUP_ID': '231920894',
        'EVENT_VECTOR_SYNC_DEBOUNCE_SECONDS': '90',
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(main, '_vk_captcha_needed', False)
    receipt = tmp_path / 'vector.json'
    monkeypatch.setenv('EVENT_VECTOR_SYNC_RECEIPT_PATH', str(receipt))
    monkeypatch.setenv('STATIC_SITE_VECTOR_RECEIPT_PATH', str(receipt))
    db = main.Database(str(tmp_path / 'initialized.sqlite'))
    await db.init()
    picks, ready, ids = [], set(), []
    output = tmp_path / 'remote-output'
    output.mkdir()

    async def external_vector_process(cmd, *, stage, run_id):
        if stage == 'export':
            Path(cmd[cmd.index('--output-dir') + 1], 'preview-events.json').write_text('{"events": []}')
        else:
            Path(cmd[cmd.index('--report-json') + 1]).write_text(json.dumps({
                'events': 25, 'complete': True, 'coverage': {'status': 'complete'},
                'catalog_revision': 'a'*64, 'corpus_revision': 'b'*64,
                'search_document_revision': 'b'*64, 'search_v3_hash': 'b'*64,
                'related_v1_hash': 'c'*64,
            }))
        return []

    monkeypatch.setattr(vectors, '_run_process', external_vector_process)

    async def vector_handler(event_id, database, bot):
        picks.append('vector')
        return await vectors.job_event_vector_sync(event_id, database, bot)

    async def adopted_static_handler(event_id, database, bot):
        picks.append('static')
        remaining = set(related_publication_anchor_ids(db.path)) - ready
        ready.update(sorted(remaining)[:20])
        graph = {
            'cache': {'state': 'incremental'},
            'gemma_verification': {'changed_event_ids': sorted(remaining),
                                   'verified_event_ids': sorted(ready)},
            'related': {str(i): {'strict_verified': i in ready, 'pure_related': []} for i in ids},
        }
        # These are already-adopted remote results, not semantic/provider evidence.
        (output / 'event_related.json').write_text(json.dumps(graph))
        (output / 'static_site_build_result.json').write_text('{"profile":"related-only"}')
        Path(db.path).with_name('static_site_event_related.json').write_text(json.dumps(graph))
        async with db.get_session() as session:
            job = (await session.execute(main.select(main.JobOutbox).where(
                main.JobOutbox.task == main.JobTask.static_site_build))).scalars().one()
            job_id = job.id
        run_id = f'offline-adoption-{len(picks)}'
        fingerprint = str(len(picks) % 10) * 64
        claim = main.claim_static_site_build(db.path, job_id=job_id, run_id=run_id,
            input_fingerprint=fingerprint, effective_date='2026-11-15', request_watermark=None)
        assert claim.action == 'claimed'
        result = await main._finish_static_site_candidate(
            db=db, job_id=job_id, output_dir=output, build_id=run_id, run_id=run_id,
            repo_sha='a'*40, snapshot_metadata=None, candidate_token='offline-only',
            input_fingerprint=fingerprint, clock=SimpleNamespace(effective_date='2026-11-15'),
            claim_token=claim.claim_token,
        )
        with sqlite3.connect(db.path) as connection:
            assert connection.execute('SELECT active_claim_token FROM static_site_build_state').fetchone()[0] is None
        return result

    monkeypatch.setitem(main.JOB_HANDLERS, main.JobTask.event_vector_sync.value, vector_handler)
    monkeypatch.setitem(main.JOB_HANDLERS, main.JobTask.static_site_build.value, adopted_static_handler)
    try:
        async with db.get_session() as session:
            events = [main.Event(title=f'Initial {i}', description='facts', source_text='facts',
                date='2099-01-01', time='19:00', location_name='Venue',
                photo_urls=['https://offline.invalid/poster.jpg']) for i in range(25)]
            session.add_all(events)
            await session.commit()
            for event in events:
                await session.refresh(event)
                ids.append(event.id)
        for event_id in ids:
            await main.enqueue_job(db, event_id, main.JobTask.vk_sync,
                next_run_at=datetime.now(timezone.utc)+timedelta(hours=1))
        await main.enqueue_static_site_build_request(db, reason='offline adopted batch', event_ids=ids)
        allowed = {main.JobTask.static_site_build, main.JobTask.event_vector_sync}
        assert await main._run_due_jobs_once(db, None, allowed_tasks=allowed, max_jobs=1) == 1
        assert len(ready) == 20
        with sqlite3.connect(db.path) as connection:
            row = connection.execute("SELECT status,next_run_at FROM joboutbox WHERE task='event_vector_sync'").fetchone()
            assert row and row[0] == 'pending', 'five initial posts otherwise wait for another import or periodic 180m'
            assert main._ensure_utc(datetime.fromisoformat(row[1])) <= datetime.now(timezone.utc) + timedelta(seconds=91)
            connection.execute("UPDATE joboutbox SET next_run_at=? WHERE task='event_vector_sync'",
                               ((datetime.now(timezone.utc)-timedelta(seconds=1)).replace(tzinfo=None).isoformat(' '),))
        assert await main._run_due_jobs_once(db, None, allowed_tasks=allowed, max_jobs=1) == 1
        assert await main._run_due_jobs_once(db, None, allowed_tasks=allowed, max_jobs=1) == 1
        assert picks == ['static', 'vector', 'static']
        assert len(ready) == 25
        assert await main._run_due_jobs_once(db, None, allowed_tasks=allowed, max_jobs=1) == 0
    finally:
        await db.engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize('cache_state,verified', [('incremental', []), ('miss_rebuilt', [1])])
async def test_no_progress_or_legacy_cold_does_not_reset_provider_backoff(tmp_path, monkeypatch, cache_state, verified):
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'related-only')
    db = main.Database(str(tmp_path / 'backoff.sqlite'))
    await db.init()
    deferred = datetime.now(timezone.utc) + timedelta(hours=1)
    try:
        async with db.get_session() as session:
            event = main.Event(title='Pending', description='facts', source_text='facts',
                date='2099-01-01', time='19:00', location_name='Venue',
                photo_urls=['https://offline.invalid/poster.jpg'])
            session.add(event)
            await session.commit()
            await session.refresh(event)
        await main.enqueue_job(db, event.id, main.JobTask.event_vector_sync,
            coalesce_key='event_vector_sync:prod', next_run_at=deferred)
        await main.enqueue_static_site_build_request(db, reason='offline adopted error batch', event_ids=[event.id])
        with sqlite3.connect(db.path) as connection:
            connection.execute("UPDATE joboutbox SET status='error', attempts=2, last_error='provider_backoff' WHERE task='event_vector_sync'")
            before = connection.execute("SELECT status,attempts,next_run_at,last_error FROM joboutbox WHERE task='event_vector_sync'").fetchone()
            job_id = connection.execute("SELECT id FROM joboutbox WHERE task='static_site_build'").fetchone()[0]
        fingerprint = 'f'*64
        claim = main.claim_static_site_build(db.path, job_id=job_id, run_id='offline-no-progress',
            input_fingerprint=fingerprint, effective_date='2026-11-15', request_watermark=None)
        output = tmp_path / 'output'
        output.mkdir()
        (output / 'static_site_build_result.json').write_text('{"profile":"related-only"}')
        (output / 'event_related.json').write_text(json.dumps({
            'cache': {'state': cache_state},
            'gemma_verification': {'changed_event_ids': [1, 2], 'verified_event_ids': verified},
        }))
        await main._finish_static_site_candidate(db=db, job_id=job_id, output_dir=output,
            build_id='offline-no-progress', run_id='offline-no-progress', repo_sha='a'*40,
            snapshot_metadata=None, candidate_token='offline', input_fingerprint=fingerprint,
            clock=SimpleNamespace(effective_date='2026-11-15'), claim_token=claim.claim_token)
        with sqlite3.connect(db.path) as connection:
            after = connection.execute("SELECT status,attempts,next_run_at,last_error FROM joboutbox WHERE task='event_vector_sync'").fetchone()
        assert after == before
    finally:
        await db.engine.dispose()
