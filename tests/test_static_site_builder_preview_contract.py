from __future__ import annotations

import importlib.util
import inspect
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BUILDER_PATH = ROOT / "kaggle" / "StaticSiteBuilder" / "static_site_builder.py"


def load_builder():
    spec = importlib.util.spec_from_file_location(
        "static_site_builder_preview_contract_test",
        BUILDER_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_production_preview_contract_gate_is_ephemeral_and_isolated(
    tmp_path: Path, monkeypatch
) -> None:
    builder = load_builder()
    site = tmp_path / "site"
    site.mkdir()
    monkeypatch.setattr(builder, "SITE_DIR", site)
    monkeypatch.setattr(builder, "status_event", lambda *_args, **_kwargs: None)
    calls: list[tuple[list[str], dict[str, str]]] = []

    def fake_run(command, *, cwd, env, **_kwargs):
        calls.append((list(command), dict(env)))
        if command == ["npm", "run", "build:preview"]:
            (site / "dist" / env["PREVIEW_BUILD_ID"]).mkdir(parents=True)

    monkeypatch.setattr(builder, "run", fake_run)
    original_env = {
        "PREVIEW_BUILD_ID": "production-build",
        "PUBLIC_PREVIEW_BUILD_ID": "production-build",
        "SITE_BASE_PATH": "/production-build",
    }
    result = builder.run_production_preview_contract_gate(
        original_env,
        build_id="production-build-20260727",
    )

    assert [command for command, _env in calls] == [
        ["npm", "run", "build:preview"],
        ["npm", "run", "check:preview"],
    ]
    gate_ids = {env["PREVIEW_BUILD_ID"] for _command, env in calls}
    assert len(gate_ids) == 1
    gate_id = gate_ids.pop()
    assert gate_id == "preview-gate-production-build-20260727"
    assert all(env["PUBLIC_PREVIEW_BUILD_ID"] == gate_id for _command, env in calls)
    assert all(env["SITE_BASE_PATH"] == f"/{gate_id}" for _command, env in calls)
    assert original_env["PREVIEW_BUILD_ID"] == "production-build"
    assert result == {
        "status": "ok",
        "build_id": gate_id,
        "archived": False,
        "published": False,
    }


def test_production_candidate_orders_preview_gate_before_root_build() -> None:
    builder = load_builder()
    source = inspect.getsource(builder.main)
    preview_call = source.index("preview_contract = run_production_preview_contract_gate")
    production_build = source.index("run(['npm', 'run', 'build:production']")
    root_archive = source.index("root_archive = WORKING")
    assert preview_call < production_build < root_archive
    assert "'preview_contract': preview_contract" in source
    assert "production build did not clear ephemeral preview contract output" in source


def test_astro_env_uses_the_validated_frozen_clock_not_kernel_wall_time():
    builder = load_builder()
    clock = {
        'time_zone': 'Europe/Kaliningrad',
        'effective_date': '2026-09-04',
        'current_datetime': '2026-09-04T23:59:00+02:00',
    }
    config = {'build_clock': clock, 'current_date': clock['effective_date'],
              'current_datetime': clock['current_datetime'], 'profile': 'preview'}
    validated = builder.validate_build_clock(config)
    env = {'STATIC_SITE_CURRENT_DATE': '2099-01-01', 'STATIC_SITE_CURRENT_DATETIME': '2099-01-01T00:00:00Z'}
    builder.apply_build_clock_env(env, validated)
    assert env['STATIC_SITE_CURRENT_DATE'] == '2026-09-04'
    assert env['STATIC_SITE_CURRENT_DATETIME'] == '2026-09-04T23:59:00+02:00'
    assert 'apply_build_clock_env(env, build_clock)' in inspect.getsource(builder.main)


def test_real_preview_clock_mismatch_is_rejected_before_archiving():
    builder = load_builder()
    clock = {'effective_date': '2026-09-04', 'current_datetime': '2026-09-04T23:59:00+02:00'}
    builder.validate_preview_clock({'currentDate': clock['effective_date'], 'referenceIso': clock['current_datetime']}, clock, 'real')
    for manifest in ({'currentDate': '2026-09-05', 'referenceIso': clock['current_datetime']},
                     {'currentDate': clock['effective_date'], 'referenceIso': '2026-09-04T23:59:01+02:00'}):
        try:
            builder.validate_preview_clock(manifest, clock, 'real')
        except RuntimeError as exc:
            assert 'frozen build clock' in str(exc)
        else:
            raise AssertionError('clock drift accepted')
    builder.validate_preview_clock({'currentDate': '2027-06-04', 'referenceIso': 'golden-owned'}, clock, 'golden')
    assert 'validate_preview_clock(preview_manifest, build_clock, preview_data_mode)' in inspect.getsource(builder.main)
