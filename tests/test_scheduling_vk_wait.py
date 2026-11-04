"""Exercise the registered normal VK job against the real shared heavy gate."""
import asyncio
import sys
from types import SimpleNamespace

import pytest
import heavy_ops
import scheduling


@pytest.mark.asyncio
@pytest.mark.parametrize('job_id', ['vk_auto_import_0', 'guide_visual_digest'])
async def test_registered_vk_slot_waits_for_active_parser_event(monkeypatch, job_id):
    class Scheduler:
        def __init__(self, **kwargs):
            self.jobs = {}
        def configure(self, **kwargs): pass
        def add_listener(self, *args): pass
        def start(self): pass
        def shutdown(self, **kwargs): pass
        def get_job(self, job_id): return self.jobs.get(job_id)
        def add_job(self, func, trigger, id, args=None, **kwargs):
            job = SimpleNamespace(id=id, func=func, args=args, next_run_time=None)
            self.jobs[id] = job
            return job

    called = []
    async def vk(*args, **kwargs):
        called.append('vk')
    monkeypatch.setitem(sys.modules, 'vk_auto_queue', SimpleNamespace(vk_auto_import_scheduler=vk))
    monkeypatch.setattr(scheduling, '_run_scheduled_guide_visual_digest', vk)
    monkeypatch.setattr(scheduling, 'AsyncIOScheduler', Scheduler)
    monkeypatch.setattr(scheduling, '_scheduler', None)
    semaphore = asyncio.Semaphore(1)
    monkeypatch.setattr(heavy_ops, '_resolve_semaphore', lambda: semaphore)
    monkeypatch.setenv('DEV_MODE', '1')
    monkeypatch.setenv('ENABLE_VK_AUTO_IMPORT', '1')
    monkeypatch.setenv('ENABLE_GUIDE_VISUAL_DIGEST_SCHEDULED', '1')
    monkeypatch.setenv('SCHED_HEAVY_GUARD_MODE', 'skip')
    monkeypatch.setenv('SCHED_HEAVY_TRY_TIMEOUT_SEC', '0.001')
    monkeypatch.delenv('SCHED_SERIALIZE_HEAVY_JOBS', raising=False)
    monkeypatch.delenv('ENABLE_NIGHTLY_PAGE_SYNC', raising=False)
    callbacks = {name: lambda *a, **k: None for name in [
        'vk_scheduler','vk_poll_scheduler','vk_crawl_cron','cleanup_scheduler',
        'nightly_page_sync','rebuild_fest_nav_if_changed']}
    task = None
    started, release = asyncio.Event(), asyncio.Event()
    async def parser_event():
        async with heavy_ops.heavy_operation(kind='source_parsing', trigger='scheduled'):
            started.set()
            await release.wait()
    holder = asyncio.create_task(parser_event())
    try:
        scheduler = scheduling.startup(None, None, **callbacks)
        job = scheduler.get_job(job_id)
        await asyncio.wait_for(started.wait(), timeout=2)
        task = asyncio.create_task(job.func(*job.args))
        await asyncio.sleep(0.03)
        assert not task.done(), 'A scheduled VK slot must wait, not terminalize as skipped'
        assert called == []
        release.set()
        await asyncio.wait_for(task, timeout=2)
        assert called == ['vk']
    finally:
        release.set()
        await holder
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        scheduling.cleanup()
