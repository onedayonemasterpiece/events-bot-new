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
        newly_ready = sorted(remaining)[:20]
        ready.update(newly_ready)
        graph = {
            'cache': {'state': 'incremental'},
            'gemma_verification': {'changed_event_ids': sorted(remaining),
                                   'verified_event_ids': sorted(ready),
                                   'attempted_anchors': len(newly_ready), 'errors': []},
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
            'gemma_verification': {'changed_event_ids': [1, 2], 'verified_event_ids': verified,
                'attempted_anchors': 1, 'errors': ([] if verified else [{'anchor_event_id': 1}])},
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


@pytest.mark.asyncio
async def test_mixed_capped_batch_advances_without_retrieving_warm_reverse_anchors(tmp_path, monkeypatch):
    import importlib.util
    spec = importlib.util.spec_from_file_location('continuation_exporter',
        Path(__file__).resolve().parents[1] / 'site/scripts/export-production-preview-data.py')
    exporter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(exporter)
    for name, value in {'STATIC_SITE_BUILD_PROFILE': 'related-only',
                        'GOOGLE_API_KEY4': 'offline-only',
                        'STATIC_SITE_GEMMA_RELATED_PASSES': '1',
                        'STATIC_SITE_GEMMA_RELATED_MAX_ATTEMPTS': '1',
                        'STATIC_SITE_GEMMA_RELATED_RETRY_BACKOFF_SEC': '0'}.items():
        monkeypatch.setenv(name, value)
    events = [{'id': i, 'title': f'Event {i}'} for i in (1, 2, 3, 4)]
    calls, retrieval = [], []

    def audit(**kwargs):
        event_id = kwargs['anchor']['id']
        calls.append(event_id)
        if event_id == 3:
            raise RuntimeError('permanent provider failure')
        return {peer['id']: {'llm_semantic_score': .8, 'confidence': .9,
                            'reject': False, 'similarity_class': 'highly_similar'}
                for peer in kwargs['candidates']}

    def retrieve(_events, *, anchor_ids=None, **_kwargs):
        retrieval.append(set(anchor_ids))
        return {str(i): [{'event_id': 3 if i in (1, 4) else 1,
                         'vector_similarity': .8, 'related_score': .8,
                         'slot_type': 'pure_related'}] for i in anchor_ids}

    monkeypatch.setattr(exporter, 'call_gemma_related_audit', audit)
    monkeypatch.setattr(exporter, 'build_pgvector_related_chain', retrieve)
    cache = tmp_path / 'related-cache.json'

    def calculate(selected):
        return exporter.build_related(events, current_date='2026-11-15', related_mode='pgvector',
            cache_path=cache, gemma_verify=True, gemma_max_anchors=2, anchor_ids=set(selected))

    calculate({1})  # Warm A references the subsequently failing B.
    db = main.Database(str(tmp_path / 'mixed.sqlite'))
    await db.init()
    output = tmp_path / 'mixed-output'
    output.mkdir()
    try:
        async with db.get_session() as session:
            session.add_all([main.Event(id=i, title=f'Event {i}', description='facts',
                source_text='facts', date='2099-01-01', time='19:00', location_name='Venue',
                photo_urls=['https://offline.invalid/poster.jpg']) for i in (1, 2, 3, 4)])
            await session.commit()
        await main.enqueue_static_site_build_request(db, reason='offline mixed batch', event_ids=[2, 3, 4])
        with sqlite3.connect(db.path) as connection:
            job_id = connection.execute("SELECT id FROM joboutbox WHERE task='static_site_build'").fetchone()[0]

        async def adopt(payload, number):
            (output / 'static_site_build_result.json').write_text('{"profile":"related-only"}')
            (output / 'event_related.json').write_text(json.dumps(payload))
            run_id = f'mixed-{number}'
            fingerprint = str(number)*64
            claim = main.claim_static_site_build(db.path, job_id=job_id, run_id=run_id,
                input_fingerprint=fingerprint, effective_date='2026-11-15', request_watermark=None)
            await main._finish_static_site_candidate(db=db, job_id=job_id, output_dir=output,
                build_id=run_id, run_id=run_id, repo_sha='a'*40, snapshot_metadata=None,
                candidate_token='offline', input_fingerprint=fingerprint,
                clock=SimpleNamespace(effective_date='2026-11-15'), claim_token=claim.claim_token)

        mixed = calculate({2, 3, 4})
        meta = mixed['gemma_verification']
        assert meta['attempted_anchors'] == 2 and meta['errors'][0]['anchor_event_id'] == 3
        assert 4 in meta['verified_event_ids'] and 2 not in meta['verified_event_ids']
        await adopt(mixed, 1)
        with sqlite3.connect(db.path) as connection:
            assert connection.execute("SELECT status FROM joboutbox WHERE task='event_vector_sync'").fetchone() == ('pending',)
        remainder = calculate({2, 3, 4})
        assert 2 in remainder['gemma_verification']['verified_event_ids']
        await adopt(remainder, 2)

        retrieval.clear()
        repeated = calculate({2, 3})
        meta = repeated['gemma_verification']
        assert retrieval == [{3}], 'warm reverse anchors are not new publication work'
        assert meta['cache_hits'] == 0 and meta['attempted_anchors'] == 1
        assert {item['anchor_event_id'] for item in meta['attempts'] if item['status'] == 'ok'} == set()
        assert meta['changed_event_ids'] == [3]
        assert {1, 2, 4}.issubset(meta['verified_event_ids'])
        with sqlite3.connect(db.path) as connection:
            connection.execute("UPDATE joboutbox SET status='error', attempts=2, last_error='provider_backoff', next_run_at=? WHERE task='event_vector_sync'",
                ((datetime.now(timezone.utc)+timedelta(hours=1)).replace(tzinfo=None).isoformat(' '),))
            before = connection.execute("SELECT status,attempts,next_run_at,last_error FROM joboutbox WHERE task='event_vector_sync'").fetchone()
        await adopt(repeated, 3)
        with sqlite3.connect(db.path) as connection:
            after = connection.execute("SELECT status,attempts,next_run_at,last_error FROM joboutbox WHERE task='event_vector_sync'").fetchone()
        assert after == before, 'warm A/C must not restart permanently failing B'
    finally:
        await db.engine.dispose()


@pytest.mark.parametrize('arrivals', [False, True])
def test_capped_scope_rotates_after_actual_attempts_to_reach_healthy_older_anchors(tmp_path, monkeypatch, arrivals):
    import importlib.util
    spec = importlib.util.spec_from_file_location('rotation_exporter',
        Path(__file__).resolve().parents[1] / 'site/scripts/export-production-preview-data.py')
    exporter = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(exporter)
    monkeypatch.setenv('GOOGLE_API_KEY4', 'offline-only')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_PASSES', '1')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_MAX_ATTEMPTS', '1')
    monkeypatch.setenv('STATIC_SITE_GEMMA_RELATED_RETRY_BACKOFF_SEC', '0')
    events = [{'id': i, 'title': f'Event {i}'} for i in range(1, 46)]
    healthy = set(range(1, 6))
    attempted = []

    def audit(**kwargs):
        event_id = kwargs['anchor']['id']
        attempted.append(event_id)
        if event_id not in healthy:
            raise RuntimeError('permanent provider failure')
        return {peer['id']: {'llm_semantic_score': .8, 'confidence': .9,
                            'reject': False, 'similarity_class': 'highly_similar'}
                for peer in kwargs['candidates']}

    def retrieve(_events, *, anchor_ids=None, **_kwargs):
        return {str(i): [{'event_id': 1 if i != 1 else 2, 'vector_similarity': .8,
                         'related_score': .8, 'slot_type': 'pure_related'}] for i in anchor_ids}

    monkeypatch.setattr(exporter, 'call_gemma_related_audit', audit)
    monkeypatch.setattr(exporter, 'build_pgvector_related_chain', retrieve)
    cache = tmp_path / 'rotation-cache.json'
    batches = []
    for number in range(3):
        if number == 1 and arrivals:
            events.extend({'id': i, 'title': f'New {i}'} for i in (46, 47))
            healthy.update((46, 47))
        before = len(attempted)
        result = exporter.build_related(events, current_date='2026-11-15', related_mode='pgvector',
            cache_path=cache, gemma_verify=True, gemma_max_anchors=20,
            anchor_ids={event['id'] for event in events})
        batches.append(attempted[before:])
    assert batches[0] == list(range(45, 25, -1))
    assert batches[1] == list(range(25, 5, -1))
    assert healthy <= set(result['gemma_verification']['verified_event_ids'])
    # Full HTML/cold callers omit anchor_ids and retain their prior ID order.
    before = len(attempted)
    exporter.maybe_apply_gemma_audit(events, retrieve(events, anchor_ids={event['id'] for event in events}),
        enabled=True, model='models/gemma-4-26b-a4b-it', key_env='GOOGLE_API_KEY4',
        cache={'gemma_verification': {'attempts': [{'anchor_event_id': 26}]}}, max_anchors=20)
    assert attempted[before:] == sorted((event['id'] for event in events), reverse=True)[:20]


@pytest.mark.asyncio
async def test_full_candidate_capped_pending_vk_continues_without_new_effects(tmp_path, monkeypatch):
    """Actual full exporter/audit/cache, checked-candidate completion and coalesced outbox."""
    import importlib.util
    root = Path(__file__).resolve().parents[1]
    def load(name, path):
        spec = importlib.util.spec_from_file_location(name, root / path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    exporter = load('full_capped_exporter', 'site/scripts/export-production-preview-data.py')
    kernel = load('full_capped_kernel', 'kaggle/StaticSiteBuilder/static_site_builder.py')
    for key, value in {
        'STATIC_SITE_BUILD_PROFILE': 'production-candidate',
        'ENABLE_STATIC_SITE_KAGGLE_BUILDER': '1',
        'STATIC_SITE_GEMMA_RELATED_VERIFY': '1',
        'VK_EVENTS_GROUP_ID': '231920894',
        'GOOGLE_API_KEY4': 'offline-only',
        'STATIC_SITE_GEMMA_RELATED_PASSES': '1',
        'STATIC_SITE_GEMMA_RELATED_MAX_ATTEMPTS': '1',
    }.items():
        monkeypatch.setenv(key, value)
    for key in ('ENABLE_STATIC_SITE_SECRET_PUBLISH', 'ENABLE_STATIC_SITE_ROOT_PROMOTION'):
        monkeypatch.setenv(key, '0')
    db = main.Database(str(tmp_path / 'full.sqlite'))
    await db.init()
    events = [{'id': i, 'title': f'Programme {i}', 'date': '2099-01-01'} for i in range(1, 5)]
    calls, retrieval, batches = [], [], []
    output = tmp_path / 'output'
    output.mkdir()
    cache = tmp_path / 'event_related_chain_cache.json'

    def retrieve(catalog, *, anchor_ids=None, graph_meta_out=None, **kwargs):
        assert anchor_ids is None, 'full candidate must retrieve the full graph'
        ids = {e['id'] for e in catalog}
        retrieval.append(ids)
        graph_meta_out.update(event_count=len(ids), policy='pgvector_selective_reciprocity_v1')
        return {str(i): [{'event_id': min(ids - {i}), 'vector_similarity': .8,
                 'related_score': .8, 'slot_type': 'pure_related'}] for i in ids}

    def audit(**kwargs):
        calls.append(kwargs['anchor']['id'])
        return {e['id']: {'llm_semantic_score': .8, 'confidence': .9,
                           'reject': False, 'similarity_class': 'highly_similar'}
                for e in kwargs['candidates']}

    monkeypatch.setattr(exporter, 'build_pgvector_related_chain', retrieve)
    monkeypatch.setattr(exporter, 'call_gemma_related_audit', audit)
    monkeypatch.setattr(kernel, 'WORKING', tmp_path)
    monkeypatch.setattr(kernel, 'SITE_DIR', root / 'site')
    monkeypatch.setattr(kernel, 'find_input_file',
                        lambda name: Path(db.path) if name == 'full.sqlite' else None)
    monkeypatch.setattr(kernel, 'validate_snapshot_input', lambda *a: {})
    monkeypatch.setattr(kernel, 'validate_semantic_cache_contract', lambda _: {'mode': 'warm'})
    monkeypatch.setattr(kernel, 'validate_build_clock',
                        lambda _: {'effective_date': '2026-11-28', 'current_datetime': '2026-11-28T12:00:00+02:00'})
    for name in ('ensure_python_deps_for_gemma', 'load_encrypted_secrets_to_env',
                 'load_kaggle_secret_to_env', 'status_event'):
        monkeypatch.setattr(kernel, name, lambda *a, **k: None)

    def run_export(cmd, **kwargs):
        assert '--related-anchor-ids' not in cmd
        options = {}
        if '--related-audit-priority-ids' in cmd:
            options['audit_priority_ids'] = {
                int(i) for i in cmd[cmd.index('--related-audit-priority-ids')+1].split(',') if i}
        graph = exporter.build_related(events, current_date='2026-11-28', related_mode='pgvector',
            cache_path=cache, gemma_verify=True, gemma_max_anchors=2, **options)
        assert set(graph['related']) == {'1', '2', '3', '4'}
        batches.append(graph)
        (output / 'event_related.json').write_text(json.dumps(graph))

    monkeypatch.setattr(kernel, 'run', run_export)
    monkeypatch.setattr(main, 'validate_production_candidate_result',
                        lambda *a, **k: ({'event_count': 4, 'profile': 'production-candidate', 'ok': True}, None))

    def export():
        kernel.export_preview_data_if_configured({
            'profile': 'production-candidate', 'catalog_mode': 'full', 'export_in_kaggle': True,
            'sqlite_db_filename': 'full.sqlite', 'related_mode': 'pgvector',
            'gemma_related_verify': True, 'gemma_related_max_anchors': 2,
            'related_anchor_ids': related_publication_anchor_ids(db.path),
        })

    async def adopted_handler(event_id, database, bot):
        async with database.get_session() as session:
            job = (await session.execute(main.select(main.JobOutbox).where(
                main.JobOutbox.task == main.JobTask.static_site_build,
                main.JobOutbox.status == main.JobStatus.running))).scalars().one()
            job_id = job.id
        fingerprint, _ = main.compute_static_site_input_fingerprint(
            db.path, effective_date='2026-11-28', repo_sha='a'*40,
            build_config={'profile': 'production-candidate'}, related_cache_path=cache)
        run_id = f'full-capped-{len(batches)}'
        claim = main.claim_static_site_build(db.path, job_id=job_id, run_id=run_id,
            input_fingerprint=fingerprint, effective_date='2026-11-28', request_watermark=None)
        assert claim.action == 'claimed'
        export()
        (output / 'static_site_build_result.json').write_text(
            '{"profile":"production-candidate","ok":true}')
        return await main._finish_static_site_candidate(
            db=db, job_id=job_id, output_dir=output, build_id=run_id, run_id=run_id,
            repo_sha='a'*40, snapshot_metadata=SimpleNamespace(snapshot_id='snap', sha256='b'*64),
            candidate_token='offline-only', input_fingerprint=fingerprint,
            clock=SimpleNamespace(effective_date='2026-11-28'), claim_token=claim.claim_token)

    monkeypatch.setitem(main.JOB_HANDLERS, main.JobTask.static_site_build.value, adopted_handler)
    try:
        async with db.get_session() as session:
            session.add_all([main.Event(id=e['id'], title=e['title'], description='facts',
                source_text='facts', date=e['date'], time='19:00', location_name='Venue',
                photo_urls=['https://offline.invalid/poster.jpg']) for e in events])
            await session.commit()
        for i in (1, 2, 3):
            await main.enqueue_job(db, i, main.JobTask.vk_sync,
                next_run_at=datetime.now(timezone.utc)+timedelta(hours=1))
        assert set(related_publication_anchor_ids(db.path)) == {1, 2, 3}
        await main.enqueue_static_site_build_request(db, reason='offline full candidate', event_ids=[1, 2, 3])
        allowed = {main.JobTask.static_site_build}
        assert await main._run_due_jobs_once(db, None, allowed_tasks=allowed, max_jobs=1) == 1
        with sqlite3.connect(db.path) as connection:
            rows = connection.execute("SELECT id FROM joboutbox WHERE task='static_site_build' AND status='pending'").fetchall()
            assert (calls, len(rows), batches[0]['related']['2']['strict_verified']) == ([3, 2], 1, True)
            connection.execute("UPDATE joboutbox SET next_run_at=? WHERE id=?",
                ((datetime.now(timezone.utc)-timedelta(seconds=1)).replace(tzinfo=None).isoformat(' '), rows[0][0]))
        assert await main._run_due_jobs_once(db, None, allowed_tasks=allowed, max_jobs=1) == 1
        assert calls == [3, 2, 1, 4]
        assert all(batches[-1]['related'][str(i)]['strict_verified'] for i in (1, 2, 3))
        assert batches[-1]['strict_verified_related']
        assert await main._run_due_jobs_once(db, None, allowed_tasks=allowed, max_jobs=1) == 0
        with sqlite3.connect(db.path) as connection:
            assert connection.execute("SELECT COUNT(*) FROM joboutbox WHERE task='static_site_build' AND status='pending'").fetchone()[0] == 0
        export()
        assert calls == [3, 2, 1, 4], 'warm/no-change must not call Gemma'
        assert retrieval == [{1, 2, 3, 4}]
    finally:
        await db.engine.dispose()
