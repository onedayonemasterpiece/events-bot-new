"""Portable recovery seam: exact unavailable owner reconciles before claim clear."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
import main
import kaggle_status
from static_site_release import StaticSiteSingleFlightDeferred

@pytest.mark.asyncio
@pytest.mark.parametrize('outcome', ['unavailable', 'reconcile_error', 'live'])
async def test_static_unavailable_reconciles_before_releasing_claim(monkeypatch, outcome):
    run_id = 'static-site:production-original:run'
    fingerprint = 'b' * 64
    handoff = dict(build_id='production-original', run_id=run_id, repo_sha='a' * 40,
        candidate_token='c' * 43, snapshot_path='/offline/original.sqlite',
        manifest_path='/offline/original.manifest.json', input_fingerprint=fingerprint,
        current_datetime='2026-11-15T08:32:00+02:00', profile='related-only')
    claim = SimpleNamespace(run_id=run_id, input_fingerprint=fingerprint,
        dataset_ref='owner/original-input', effective_date='2026-11-15',
        claim_token='original', remote_status='created', remote_terminal_at=None)
    monkeypatch.setenv('STATIC_SITE_BUILD_PROFILE', 'related-only')
    monkeypatch.setattr(main, 'recoverable_static_site_build', lambda *a, **kw: claim)
    monkeypatch.setattr(main, 'validate_snapshot', lambda *a: SimpleNamespace(
        snapshot_id='original', sha256='d' * 64, size_bytes=1))
    commands, order = [], []
    async def launch(*args, **kwargs):
        commands.append(args)
        async def communicate(): return b'', None
        return SimpleNamespace(returncode=75 if outcome == 'live' else 76, communicate=communicate)
    monkeypatch.setattr(main.asyncio, 'create_subprocess_exec', launch)
    async def reconcile(db, *, run_id, message):
        assert run_id == claim.run_id
        order.append('reconcile')
        if outcome == 'reconcile_error': raise RuntimeError('offline reconciliation failed')
        return {'status': 'failed_reconciled', 'released_resources': 1}
    monkeypatch.setattr(kaggle_status, 'reconcile_kaggle_run_failure_from_host', reconcile)
    def finish(*args, **kwargs):
        assert kwargs['run_id'] == claim.run_id and kwargs['success'] is False
        order.append('finish_claim')
    monkeypatch.setattr(main, 'finish_static_site_build_claim', finish)
    async def patch(*args): order.append('clear_handoff')
    monkeypatch.setattr(main, '_patch_static_site_request_payload', patch)
    monkeypatch.setattr(main, 'delete_immutable_snapshot', lambda *args: 0)
    monkeypatch.setattr(main, '_delete_terminal_static_site_output', AsyncMock())
    recovery = main._recover_previous_static_site_attempt(
        db=SimpleNamespace(path='/offline/db.sqlite'), job_id=12,
        request_payload={'remote_handoff': handoff}, limit=5000,
        current_repo_sha='e' * 40, current_source_identity=None)
    if outcome == 'live':
        with pytest.raises(StaticSiteSingleFlightDeferred, match='remote_recovery_live'): await recovery
        assert order == []
    elif outcome == 'reconcile_error':
        with pytest.raises(RuntimeError, match='offline reconciliation failed'): await recovery
        assert order == ['reconcile']  # Claim and handoff survive the failed reconciliation.
    else:
        assert await recovery == (False, None)
        assert order == ['reconcile', 'finish_claim', 'clear_handoff']
    assert len(commands) == 1 and '--adopt-existing' in commands[0]
    assert commands[0][commands[0].index('--expected-dataset-ref') + 1] == claim.dataset_ref
