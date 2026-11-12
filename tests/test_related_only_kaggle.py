import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest


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
    assert await main._finish_static_site_candidate(
        db=SimpleNamespace(path=str(db_path)), job_id=12, output_dir=tmp_path,
        build_id='production-related-test', run_id='related-test', repo_sha='a' * 40,
        snapshot_metadata=None, candidate_token='', input_fingerprint='b' * 64,
        clock=SimpleNamespace(effective_date='2026-11-12'), claim_token=claim.claim_token,
    ) is True
    with sqlite3.connect(db_path) as c:
        assert c.execute('select active_job_id,last_success_at from static_site_build_state').fetchone() == (None, None)
