"""Exact remote failure releases its real ledger lease before adoption cleanup."""
import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import pytest
import scripts.run_static_site_builder_kaggle as runner
from db import Database
from kaggle_status import create_kaggle_run_config

@pytest.mark.parametrize('remote,matched,expected', [(status, True, 'failed') for status in ('ERROR', 'FAILED', 'CANCELLED', 'CANCELED')] + [('RUNNING', True, 'alive'), ('UNKNOWN', True, 'alive'), ('ERROR', False, 'alive')])
def test_authoritative_terminal_releases_exact_lease(tmp_path, monkeypatch, remote, matched, expected):
    monkeypatch.setenv('KAGGLE_STATUS_CALLBACK_URL', 'https://example.test/internal/kaggle/run-event')
    path = str(tmp_path / 'db.sqlite')
    async def seed():
        db = Database(path)
        await db.init()
        await create_kaggle_run_config(db, run_id='old-run', session_id=None, kind='static_site_builder', notebook='StaticSiteBuilder')
        now = datetime.now(timezone.utc)
        async with db.raw_conn() as conn:
            await conn.execute("UPDATE kaggle_run_ledger SET status='alive',phase='export' WHERE run_id='old-run'")
            for resource, run in [('static_site:builder', 'old-run'), ('unrelated', 'another-run')]:
                await conn.execute("INSERT INTO kaggle_resource_lease(resource_key,run_id,holder_kind,status,acquired_at,expires_at,updated_at) VALUES (?,?,'kaggle','active',?,?,?)", (resource,run,now.isoformat(),(now+timedelta(hours=3)).isoformat(),now.isoformat()))
            await conn.commit()
        await db.close()
    asyncio.run(seed())
    class Client:
        def kernel_has_dataset_sources(self, *args):
            return matched, {'dataset_sources': ['owner/exact' if matched else 'owner/other']}
        def get_kernel_status(self, *args):
            return {'status': remote}
    code = runner.adopt_existing_kernel_output(SimpleNamespace(expected_dataset_ref='owner/exact',status_db=path,run_id='old-run'), Client(), 'owner/kernel')
    import sqlite3
    with sqlite3.connect(path) as conn:
        actual = conn.execute("SELECT status FROM kaggle_run_ledger WHERE run_id='old-run'").fetchone()[0]
        lease = conn.execute("SELECT status FROM kaggle_resource_lease WHERE resource_key='static_site:builder'").fetchone()[0]
        unrelated = conn.execute("SELECT status FROM kaggle_resource_lease WHERE resource_key='unrelated'").fetchone()[0]
    assert code == (75 if remote == 'RUNNING' and matched else 76)
    assert unrelated == 'active'
    assert actual == expected
    assert lease == ('released' if expected == 'failed' else 'active')


def test_reconciliation_error_escapes_before_unavailable_cleanup(tmp_path, monkeypatch):
    import kaggle_status
    import db
    closed = []
    original_close = db.Database.close

    async def close(self):
        closed.append(self.path)
        await original_close(self)

    async def fail(*args, **kwargs):
        raise RuntimeError('ledger unavailable')

    monkeypatch.setattr(db.Database, 'close', close)
    monkeypatch.setattr(kaggle_status, 'reconcile_kaggle_run_failure_from_host', fail)
    client = SimpleNamespace(
        kernel_has_dataset_sources=lambda *_args: (True, {}),
        get_kernel_status=lambda *_args: {'status': 'ERROR'},
    )
    path = str(tmp_path / 'status.sqlite')
    with pytest.raises(RuntimeError, match='ledger unavailable'):
        runner.adopt_existing_kernel_output(SimpleNamespace(
            expected_dataset_ref='owner/exact', status_db=path, run_id='old-run'),
            client, 'owner/kernel')
    assert closed == [path]
