import io
import json
import zipfile
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from db import Database
from kaggle_status import create_kaggle_run_config, make_kaggle_run_event_handler, reconcile_kaggle_run_failure_from_host
from guide_excursions import kaggle_service, result_delivery, service
import kaggle_registry


def package(run_id='push', member=None, run_error=None):
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w') as bundle:
        bundle.writestr('guide_excursions_results.json', json.dumps({
            'schema_version': 1, 'run_id': run_id, 'sources': [], 'partial': bool(run_error), 'run_error': run_error,
        }))
        bundle.writestr(member or 'guide_media/photo.jpg', b'photo')
    return output.getvalue()


@pytest.mark.asyncio
async def test_push_survives_dead_waiter_restart_late_failure_and_duplicate(tmp_path, monkeypatch):
    monkeypatch.setattr(kaggle_service, 'RESULTS_STORE_ROOT', tmp_path / 'results')
    monkeypatch.setattr(kaggle_registry, '_REGISTRY_PATH', tmp_path / 'jobs.json')
    monkeypatch.setattr('scheduling.request_guide_result_import', lambda: None)
    db = Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    config = await create_kaggle_run_config(db, run_id='guide_monitor:push', session_id=None,
        kind='guide_monitoring', notebook='GuideExcursionsMonitor', kernel_ref='account/guide',
        callback_url='https://example.test/internal/kaggle/run-event')
    await reconcile_kaggle_run_failure_from_host(db, run_id='guide_monitor:push', message='waiter died')
    await kaggle_registry.register_job('guide_monitoring', 'account/guide', meta={'run_id': 'newer'})
    app = web.Application()
    app.router.add_post('/run-event', make_kaggle_run_event_handler(db))
    client = TestClient(TestServer(app))
    await client.start_server()
    headers = {'Content-Type': 'application/zip', 'X-Kaggle-Run-ID': config['run_id'],
               'Authorization': 'Bearer ' + config['token']}
    try:
        for _ in range(2):
            response = await client.post('/run-event', data=package(run_error='quota'), headers=headers)
            assert response.status == 200, await response.text()
            assert (await response.json())['saved'] is True
        assert (tmp_path / 'results/guide-excursions-push-push/guide_media/photo.jpg').read_bytes() == b'photo'
        assert len(await result_delivery.pending_result_jobs(db)) == 1
        await db.close()
        db = Database(str(tmp_path / 'db.sqlite'))
        await db.init()
        async def no_admin(_db): return None
        monkeypatch.setattr(service, 'resolve_superadmin_chat_id', no_admin)
        jobs = await result_delivery.pending_result_jobs(db)
        # An unrelated current kernel job must never be fetched or removed by the late delivery.
        async def only_delivered(_type): return []
        monkeypatch.setattr(service, 'list_jobs', only_delivered)
        assert await service.resume_guide_monitor_jobs(db, None) == 1
        assert await result_delivery.pending_result_jobs(db) == []
        assert not (Path(jobs[0]['meta']['results_path']).parent / '.delivery_pending').exists()
        current = await kaggle_registry.list_jobs('guide_monitoring')
        assert current[0]['meta']['run_id'] == 'newer'
        result = await service.run_guide_import_from_results(db, results_path=jobs[0]['meta']['results_path'],
            bot=None, chat_id=None, run_id='push', trigger='duplicate')
        assert result.import_completed and result.errors == ['quota']
        # Lost ACK / delivery after ordinary retention must not pin a new copy forever.
        import shutil
        shutil.rmtree(Path(jobs[0]['meta']['results_path']).parent)
        response = await client.post('/run-event', data=package(run_error='quota'), headers=headers)
        assert response.status == 200
        assert not (Path(jobs[0]['meta']['results_path']).parent / '.delivery_pending').exists()
        async with db.raw_conn() as conn:
            assert (await (await conn.execute("SELECT COUNT(*) FROM ops_run WHERE kind='guide_monitoring'")).fetchone())[0] == 1
    finally:
        await client.close()
        await db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('bad_token,bad_member,bad_run', [(True,False,False),(False,True,False),(False,False,True)])
async def test_rejects_unbound_or_unsafe_packages(tmp_path, monkeypatch, bad_token, bad_member, bad_run):
    monkeypatch.setattr(kaggle_service, 'RESULTS_STORE_ROOT', tmp_path / 'results')
    db = Database(str(tmp_path / 'db.sqlite')); await db.init()
    config = await create_kaggle_run_config(db, run_id='guide_monitor:push', session_id=None,
        kind='guide_monitoring', notebook='GuideExcursionsMonitor', callback_url='https://example.test/run-event')
    app = web.Application(); app.router.add_post('/run-event', make_kaggle_run_event_handler(db))
    client = TestClient(TestServer(app)); await client.start_server()
    try:
        response = await client.post('/run-event', data=package('other' if bad_run else 'push', '../escape' if bad_member else None),
            headers={'Content-Type':'application/zip','X-Kaggle-Run-ID':config['run_id'],
                     'Authorization':'Bearer ' + ('wrong' if bad_token else config['token'])})
        assert response.status == (403 if bad_token else 400)
        assert await result_delivery.pending_result_jobs(db) == []
        assert not (tmp_path / 'escape').exists()
    finally:
        await client.close(); await db.close()


def test_kaggle_keeps_all_outputs_when_receiver_is_unavailable(tmp_path, monkeypatch):
    scope, Namespace = runner_functions()
    result = tmp_path / 'guide_excursions_results.json'
    result.write_text('{"run_id":"push","sources":[]}')
    scope.update(STATUS_CLIENT=Namespace(enabled=True,config={
        'callback_url':'https://example.test/run-event','run_id':'guide_monitor:push','token':'test-only'}),
        WORK_DIR=tmp_path, RESULT_PATH=result, MEDIA_OUTPUT_DIR=tmp_path / 'guide_media')
    attempts=[]; sleeps=[]
    def unavailable(*args, **kwargs):
        attempts.append(1)
        raise ConnectionError('receiver restarting')
    monkeypatch.setattr(scope['urllib'].request, 'urlopen', unavailable)
    monkeypatch.setattr(scope['time'], 'sleep', sleeps.append)
    scope['deliver_guide_results']()
    assert len(attempts) == 3 and sleeps == [10,20]
    assert result.exists() and not (tmp_path / 'guide_results_delivery.zip').exists()


def runner_functions():
    import ast
    from types import SimpleNamespace
    source = Path('kaggle/GuideExcursionsMonitor/guide_excursions_monitor.py').read_text()
    wanted = {'deliver_guide_results', 'run_with_delivery', '_write_results'}
    tree = ast.parse(source)
    nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in wanted]
    import asyncio, urllib.request, urllib.error, time
    from datetime import datetime, timezone
    scope = dict(asyncio=asyncio, urllib=urllib, time=time, json=json, zipfile=zipfile,
                 datetime=datetime, timezone=timezone)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<real-guide-runner>', 'exec'), scope)
    return scope, SimpleNamespace


@pytest.mark.asyncio
async def test_real_kaggle_sender_pushes_partial_json_and_binary_media(tmp_path, monkeypatch):
    import os
    scope, Namespace = runner_functions()
    monkeypatch.setattr(kaggle_service, 'RESULTS_STORE_ROOT', tmp_path / 'received')
    monkeypatch.setattr('scheduling.request_guide_result_import', lambda: None)
    db = Database(str(tmp_path / 'db.sqlite')); await db.init()
    config = await create_kaggle_run_config(db, run_id='guide_monitor:push', session_id=None,
        kind='guide_monitoring', notebook='GuideExcursionsMonitor', callback_url='https://unused.test/')
    app = web.Application(); app.router.add_post('/run-event', make_kaggle_run_event_handler(db))
    client = TestClient(TestServer(app)); await client.start_server()
    work = tmp_path / 'kaggle'; work.mkdir(); media = work / 'guide_media'; media.mkdir()
    photo = os.urandom(1100000)  # Exercise streamed upload beyond aiohttp's default 1 MiB JSON limit.
    (media / 'photo.jpg').write_bytes(photo)
    result = work / 'guide_excursions_results.json'
    result.write_text(json.dumps({'schema_version':1,'run_id':'push','sources':[], 'partial':True}))
    config['callback_url'] = str(client.make_url('/run-event'))
    scope.update(STATUS_CLIENT=Namespace(enabled=True,config=config), WORK_DIR=work,
                 RESULT_PATH=result, MEDIA_OUTPUT_DIR=media, _status_event=lambda *a, **kw: None)
    async def fails_after_checkpoint():
        raise RuntimeError('quota during execution')
    scope['main'] = fails_after_checkpoint
    try:
        with pytest.raises(RuntimeError, match='quota during execution'):
            await scope['run_with_delivery']()
        jobs = await result_delivery.pending_result_jobs(db)
        payload = json.loads(Path(jobs[0]['meta']['results_path']).read_text())
        assert payload['run_error'] == 'RuntimeError: quota during execution'
        assert (Path(jobs[0]['meta']['results_path']).parent / 'guide_media/photo.jpg').read_bytes() == photo
        assert not (work / 'guide_results_delivery.zip').exists()
        assert result.exists()  # Provider output remains a fallback even after ACK.
        kaggle_service._prune_results_store()
        assert Path(jobs[0]['meta']['results_path']).exists()
    finally:
        await client.close(); await db.close()


@pytest.mark.asyncio
async def test_existing_os_lock_defers_import_until_descriptor_release(tmp_path, monkeypatch):
    import fcntl
    db = Database(str(tmp_path / 'db.sqlite')); await db.init()
    monkeypatch.setattr(kaggle_registry, '_REGISTRY_PATH', tmp_path / 'jobs.json')
    try:
        with open(str(Path(db.path).resolve()) + '.guide-import.lock', 'a') as other_process:
            fcntl.flock(other_process, fcntl.LOCK_EX | fcntl.LOCK_NB)
            assert await service.resume_guide_monitor_jobs(db, None) == 0
            with pytest.raises(BlockingIOError):
                await service.run_guide_import_from_results(db, results_path='unused', bot=None,
                    chat_id=None, run_id='same', trigger='callback')
        # Descriptor release unlocks it; no lease/timer reset.
        assert await service.resume_guide_monitor_jobs(db, None) == 0
    finally:
        await db.close()
