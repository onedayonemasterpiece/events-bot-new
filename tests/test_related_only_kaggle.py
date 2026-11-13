import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_related_uses_completed_vector_snapshot_while_ingest_advances(tmp_path, monkeypatch):
    import event_vector_sync as vectors
    import main
    from static_site_release import related_projection_input, validate_vector_barrier

    db = main.Database(str(tmp_path / 'live.sqlite'))
    await db.init()
    monkeypatch.setenv('ENABLE_EVENT_VECTOR_SYNC', '1')
    monkeypatch.setenv('ENABLE_STATIC_SITE_KAGGLE_BUILDER', '1')
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'related-only')
    receipt_path = tmp_path / 'vector-receipt.json'
    monkeypatch.setenv('EVENT_VECTOR_SYNC_RECEIPT_PATH', str(receipt_path))
    try:
        async with db.get_session() as session:
            event = main.Event(title='Projected title', description='Facts', date='2099-01-01',
                               time='19:00', location_name='Дом искусств', city='Калининград', source_text='source')
            session.add(event)
            await session.commit()
            await session.refresh(event)
            event_id = int(event.id)
            projected_revision = main.event_public_revision(event)

        async def project(cmd, *, stage, run_id):
            if stage == 'export':
                Path(cmd[cmd.index('--output-dir') + 1], 'preview-events.json').write_text('{"events": []}')
            else:
                # A real Smart Update can commit while provider embedding runs.
                with sqlite3.connect(db.path) as c:
                    c.execute('update event set title=? where id=?', ('New live title', event_id))
                Path(cmd[cmd.index('--report-json') + 1]).write_text(json.dumps({
                    'events': 1, 'complete': True, 'coverage': {'status': 'complete'},
                    'catalog_revision': 'a' * 64, 'corpus_revision': 'b' * 64,
                    'search_document_revision': 'b' * 64, 'search_v3_hash': 'b' * 64,
                    'related_v1_hash': 'c' * 64,
                }))
            return []

        monkeypatch.setattr(vectors, '_run_process', project)
        await vectors.run_event_vector_sync(db, trigger='scheduled')
        payload = {'event_revisions': {str(event_id): 'new-live-revision', '99999': 'new-event'},
                   'vector_barrier': {'required': True}}
        source, bound = related_projection_input(payload, receipt_path)
        assert bound['event_revisions'] == {str(event_id): projected_revision}
        assert validate_vector_barrier(bound, receipt_path)['status'] == 'complete'
        with sqlite3.connect(source) as c:
            assert c.execute('select title from event where id=?', (event_id,)).fetchone()[0] == 'Projected title'
        with sqlite3.connect(db.path) as c:
            assert c.execute('select title from event where id=?', (event_id,)).fetchone()[0] == 'New live title'
            queued = json.loads(c.execute("select payload from joboutbox where task='static_site_build'").fetchone()[0])
            assert 'vector_projection_ready' in queued['reasons']
    finally:
        await db.close()


def test_related_only_returns_importable_json_without_site_build(tmp_path, monkeypatch):
    from scripts import run_static_site_builder_kaggle as runner

    source = Path(__file__).parents[1] / 'kaggle/StaticSiteBuilder/static_site_builder.py'
    spec = importlib.util.spec_from_file_location('related_only_kernel', source)
    kernel = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(kernel)
    site = tmp_path / 'site'
    data = site / 'src/data'
    data.mkdir(parents=True)
    related = {'related': {'42': {'similar': [43]}}, 'generated_at': '2026-11-12'}
    (data / 'preview-related.json').write_text(json.dumps(related))
    (tmp_path / 'event_related_chain_cache.json').write_text('{}')
    clock = {'effective_date': '2026-11-12'}
    config = {'profile': 'related-only', 'build_id': 'production-related-test',
              'run_id': 'test-run', 'repo_sha': 'a' * 40, 'input_fingerprint': 'b' * 64}
    monkeypatch.setattr(kernel, 'WORKING', tmp_path)
    monkeypatch.setattr(kernel, 'SITE_DIR', site)
    monkeypatch.setattr(kernel, 'RESULT_PATH', tmp_path / 'static_site_build_result.json')
    monkeypatch.setattr(kernel, 'read_config', lambda: config)
    monkeypatch.setattr(kernel, 'validate_build_clock', lambda _: clock)
    monkeypatch.setattr(kernel, 'validate_semantic_cache_contract', lambda _: {'mode': 'warm', 'inputs_staged': []})
    monkeypatch.setattr(kernel, 'ensure_site_source', lambda _: {})
    for name in ('init_status', 'cleanup_transient_workspace'):
        monkeypatch.setattr(kernel, name, lambda: None)
    for name in ('ensure_python_deps_for_bge', 'export_preview_data_if_configured'):
        monkeypatch.setattr(kernel, name, lambda _: None)
    monkeypatch.setattr(kernel, 'status_event', lambda *a, **kw: None)
    monkeypatch.setattr(kernel, 'finish_status', lambda **kw: None)
    def unexpected(*a, **kw):
        pytest.fail('related-only invoked a site/service-share subprocess')
    monkeypatch.setattr(kernel, 'run', unexpected)
    monkeypatch.setattr(kernel, 'render_daily_service_share', unexpected)
    assert kernel.main() == 0

    args = SimpleNamespace(**config, build_clock=clock, image_source_contract={
        'manifest_sha256': 'c' * 64, 'source_tree_sha256': 'd' * 64},
        related_cache=str(tmp_path / 'durable/cache.json'))
    monkeypatch.setattr(runner, 'validate_static_site_source_identity', lambda *a, **kw: None)
    result = runner.validate_downloaded_result(tmp_path, args)
    assert result['event_count'] == 1
    assert json.loads((tmp_path / 'durable/static_site_event_related.json').read_text()) == related
    (tmp_path / 'event_related.json').write_text('{}')
    with pytest.raises(RuntimeError, match='artifact mismatch'):
        runner.validate_downloaded_result(tmp_path, args)


@pytest.mark.asyncio
async def test_related_completion_releases_claim_without_site_success(tmp_path, monkeypatch):
    import main
    from static_site_release import claim_static_site_build

    db_path = tmp_path / 'state.sqlite'
    claim = claim_static_site_build(str(db_path), job_id=12, run_id='related-test',
                                   input_fingerprint='b' * 64, effective_date='2026-11-12',
                                   request_watermark=None)
    (tmp_path / 'static_site_build_result.json').write_text(json.dumps({'profile': 'related-only'}))
    async def patch(*a, **kw):
        pass
    monkeypatch.setattr(main, '_patch_static_site_request_payload', patch)
    import vk_event_relations
    monkeypatch.setattr(vk_event_relations, 'enqueue_refresh', patch)
    assert await main._finish_static_site_candidate(
        db=SimpleNamespace(path=str(db_path)), job_id=12, output_dir=tmp_path,
        build_id='production-related-test', run_id='related-test', repo_sha='a' * 40,
        snapshot_metadata=None, candidate_token='', input_fingerprint='b' * 64,
        clock=SimpleNamespace(effective_date='2026-11-12'), claim_token=claim.claim_token,
    ) is True
    with sqlite3.connect(db_path) as c:
        assert c.execute('select active_job_id,last_success_at from static_site_build_state').fetchone() == (None, None)


@pytest.mark.asyncio
async def test_import_stream_does_not_postpone_ready_related_compute(tmp_path, monkeypatch):
    import main
    from datetime import datetime, timedelta, timezone
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'related-only')
    db = main.Database(str(tmp_path / 'queue.sqlite'))
    await db.init()
    try:
        async with db.get_session() as session:
            event = main.Event(title='Event', description='', source_text='', date='2099-01-01', time='19:00', location_name='Venue')
            session.add(event)
            await session.commit()
            await session.refresh(event)
        due = datetime.now(timezone.utc) + timedelta(seconds=30)
        for offset in [0, 20, 40]:
            await main.enqueue_job(db, event.id, main.JobTask.event_vector_sync,
                coalesce_key='event_vector_sync:prod', next_run_at=due + timedelta(seconds=offset))
        with sqlite3.connect(db.path) as c:
            actual = datetime.fromisoformat(c.execute("select next_run_at from joboutbox where task='event_vector_sync'").fetchone()[0]).replace(tzinfo=timezone.utc)
        assert actual == due
        await main.enqueue_static_site_build_request(db, reason='vector_projection_ready', event_ids=[event.id])
        with sqlite3.connect(db.path) as c:
            before = c.execute("select next_run_at from joboutbox where task='static_site_build'").fetchone()[0]
        await main.enqueue_static_site_build_request(db, reason='next_import', event_ids=[event.id], delay_seconds=900, trigger='smart_update')
        with sqlite3.connect(db.path) as c:
            assert c.execute("select next_run_at from joboutbox where task='static_site_build'").fetchone()[0] == before
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_related_job_does_not_require_website_authorized_search(tmp_path, monkeypatch):
    import main
    from static_site_release import create_immutable_projection_snapshot
    db = main.Database(str(tmp_path / 'live.sqlite'))
    await db.init()
    monkeypatch.setenv('ENABLE_STATIC_SITE_KAGGLE_BUILDER', '1')
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'related-only')
    monkeypatch.setenv('STATIC_SITE_RELATED_MODE', 'pgvector')
    monkeypatch.setenv('STATIC_SITE_SECRET_CANDIDATE_REQUIRE_AUTHORIZED_SEARCH', '1')
    monkeypatch.setenv('STATIC_SITE_SNAPSHOT_DIR', str(tmp_path / 'handoff'))
    async def noop(*args, **kwargs):
        return None
    async def previous(**kwargs):
        return False, None
    for name in ('_prune_static_site_terminal_outputs', '_prune_static_site_terminal_snapshots_for_capacity',
                 '_prune_static_site_abandoned_scratch_for_capacity', '_reconcile_current_static_site_candidate_status',
                 '_patch_static_site_request_payload'):
        monkeypatch.setattr(main, name, noop)
    monkeypatch.setattr(main, '_recover_previous_static_site_attempt', previous)
    monkeypatch.setattr(main, '_static_site_storage_preflight', lambda: None)
    monkeypatch.setattr(main, '_resolve_static_site_repo_sha', lambda: 'a' * 40)
    monkeypatch.setattr(main, '_resolve_static_site_image_source_identity', lambda *args: None)
    def website_gate(**kwargs):
        pytest.fail('related-only attempted the deferred website Search qualification')
    monkeypatch.setattr(main, 'bind_snapshot_search_receipt', website_gate)
    def stop_at_claim(*args, **kwargs):
        raise RuntimeError('reached_shared_kaggle_claim')
    monkeypatch.setattr(main, 'claim_static_site_build', stop_at_claim)
    try:
        projected, manifest, _ = create_immutable_projection_snapshot(db.path, tmp_path / 'vector', request_payload={})
        receipt = tmp_path / 'receipt.json'
        receipt.write_text(json.dumps({'schema_version': 'event_vector_sync_receipt_v2', 'status': 'complete',
            'complete': True, 'catalog_revision': 'a' * 64, 'corpus_revision': 'b' * 64,
            'search_document_revision': 'b' * 64, 'search_v3_hash': 'b' * 64, 'related_v1_hash': 'c' * 64,
            'coverage': {'status': 'complete'}, 'event_revisions': {},
            'projection_snapshot': {'sqlite_path': str(projected), 'manifest_path': str(manifest)}}))
        monkeypatch.setenv('STATIC_SITE_VECTOR_RECEIPT_PATH', str(receipt))
        async def request(*args):
            return 12, {'event_revisions': {}, 'vector_barrier': {'required': True}}
        monkeypatch.setattr(main, '_static_site_running_request', request)
        with pytest.raises(RuntimeError, match='reached_shared_kaggle_claim'):
            await main.job_static_site_build_kaggle(1, db, None)
    finally:
        await db.close()
