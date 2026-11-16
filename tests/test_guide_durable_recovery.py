import ast
import json
import logging
import os
import sys
from types import SimpleNamespace
from pathlib import Path

import pytest

from db import Database
from guide_excursions import service


@pytest.mark.parametrize('broad,guide', [('0','1'), ('0','0'), ('1','1')])
def test_guide_recovery_is_registered_when_broad_sweep_is_disabled(monkeypatch, broad, guide):
    import scheduling
    async def broad_sweep(*args, **kwargs):
        raise AssertionError('Broad recovery must not be invoked by guide-only registration')
    monkeypatch.setitem(sys.modules, 'kaggle_recovery', SimpleNamespace(kaggle_recovery_scheduler=broad_sweep))
    monkeypatch.setenv('ENABLE_KAGGLE_RECOVERY', broad)
    monkeypatch.setenv('ENABLE_GUIDE_EXCURSIONS_SCHEDULED', guide)
    tree = ast.parse(Path('scheduling.py').read_text())
    startup = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'startup')
    index = next(i for i, n in enumerate(startup.body) if isinstance(n, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id == 'enable_kaggle_recovery' for t in n.targets))
    block = []
    for node in startup.body[index:]:
        block.append(node)
        if isinstance(node, ast.If):
            break
    jobs = []
    namespace = dict(os=os, logging=logging, is_prod=True, db=object(), bot=object(),
                     _env_enabled=scheduling._env_enabled,
                     _guide_monitoring_schedule_settings=scheduling._guide_monitoring_schedule_settings,
                     _notify_admin_skip=None,
                     _register_job=lambda *args, **kwargs: jobs.append((args, kwargs)),
                     _job_wrapper=lambda _name, fn, **kwargs: fn)
    exec(compile(ast.Module(body=block, type_ignores=[]), 'scheduling.py', 'exec'), namespace)
    assert len(jobs) == int(broad == '1' or guide == '1')
    if jobs:
        from kaggle_recovery import kaggle_recovery_scheduler
        assert jobs[0][0][1] is (kaggle_recovery_scheduler if broad == '1' else service.resume_guide_monitor_jobs)
        assert jobs[0][1]['max_instances'] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('import_ok', [False, True])
async def test_recovery_survives_reused_pid_and_keeps_failed_import(tmp_path, monkeypatch, import_ok):
    db = Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    jobs = [{'kernel_ref': 'account/guide', 'meta': {'run_id': 'durable', 'pid': os.getpid()}}]
    path = tmp_path / 'result.json'
    path.write_text(json.dumps({'run_id': 'durable', 'sources': []}))
    imported = []
    removed = []
    class Client:
        def get_kernel_status(self, _ref):
            return {'status': 'COMPLETE'}
    async def download(*args):
        return path
    async def perform(_db, **kwargs):
        imported.append(kwargs)
        return service.GuideMonitorResult('durable', 1, 'recovery_import', 'full',
            {'sources_scanned': int(import_ok)}, [] if import_ok else ['disk full'],
            import_completed=import_ok)
    async def listing(*args):
        return jobs
    async def update(_kind, _ref, *, meta_updates):
        jobs[0]['meta'].update(meta_updates)
    async def remove(*args):
        removed.append(args)
    monkeypatch.setattr(service, 'KaggleClient', Client)
    monkeypatch.setattr(service, 'list_jobs', listing)
    monkeypatch.setattr(service, 'download_guide_results', download)
    monkeypatch.setattr(service, 'run_guide_import_from_results', perform)
    monkeypatch.setattr(service, 'remove_job', remove)
    monkeypatch.setattr(service, 'update_job_meta', update)
    try:
        result = await service.resume_guide_monitor_jobs(db, None, chat_id=123)
        assert len(imported) == 1
        assert jobs[0]['meta']['results_path'] == str(path)
        assert result == int(import_ok)
        assert bool(removed) is import_ok
        if not import_ok:
            import_ok = True
            assert await service.resume_guide_monitor_jobs(db, None, chat_id=123) == 1
            assert len(imported) == 2 and len(removed) == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_accepted_import_receipt_prevents_second_materialization(tmp_path, monkeypatch):
    db = Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    calls = []
    async def import_once(_db, **kwargs):
        calls.append(kwargs)
        return {'sources_scanned': 1}, [], {'run_id': 'exact', 'sources': [{'username': 'guide'}]}
    monkeypatch.setattr(service, '_import_results_file', import_once)
    try:
        kwargs = dict(results_path=str(tmp_path / 'saved.json'), bot=None, chat_id=None,
                      run_id='exact', trigger='recovery_import')
        first = await service.run_guide_import_from_results(db, **kwargs)
        second = await service.run_guide_import_from_results(db, **kwargs)
        assert first.import_completed and second.import_completed
        assert first.ops_run_id == second.ops_run_id
        assert len(calls) == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_completed_import_releases_exact_session_lease(tmp_path, monkeypatch):
    db = Database(str(tmp_path / 'db.sqlite'))
    await db.init()
    async def imported(*args, **kwargs):
        return {'sources_scanned': 1}, [], {'run_id': 'finished'}
    async def retention(*args, **kwargs):
        return {}
    monkeypatch.setattr(service, '_import_results_file_inner', imported)
    monkeypatch.setattr(service, '_run_guide_media_retention_guard', retention)
    try:
        async with db.raw_conn() as conn:
            await conn.execute("INSERT INTO kaggle_run_ledger(run_id,token_hash,status) VALUES ('guide_monitor:finished','fixture','alive')")
            await conn.execute("INSERT INTO kaggle_resource_lease(resource_key,run_id,expires_at) VALUES ('telegram_session:s22','guide_monitor:finished','2099-01-01')")
            await conn.commit()
        await service._import_results_file(db, results_path='fixture.json')
        async with db.raw_conn() as conn:
            assert (await (await conn.execute("SELECT status FROM kaggle_resource_lease")).fetchone())[0] == 'released'
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_recovery_does_not_compete_with_existing_local_run(monkeypatch):
    calls = []
    async def listing(*args):
        calls.append(1)
        return []
    monkeypatch.setattr(service, 'list_jobs', listing)
    async with service._RUN_LOCK:
        assert await service.resume_guide_monitor_jobs(object(), None, chat_id=123) == 0
    assert calls == []
