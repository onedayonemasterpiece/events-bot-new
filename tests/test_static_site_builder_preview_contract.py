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
