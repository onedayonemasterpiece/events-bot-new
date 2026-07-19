from __future__ import annotations

import sys
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest


def _arg_after(cmd: list[str], name: str) -> str:
    return cmd[cmd.index(name) + 1]


def test_snapshot_retention_context_fails_closed_for_malformed_active_handoff(tmp_path: Path) -> None:
    import main

    database = tmp_path / "retention.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE static_site_build_state(release_channel TEXT PRIMARY KEY, active_job_id INTEGER)"
        )
        connection.execute(
            "CREATE TABLE joboutbox(id INTEGER PRIMARY KEY, task TEXT, payload TEXT)"
        )
        connection.execute(
            "INSERT INTO static_site_build_state VALUES('secret_preview', 7)"
        )
        connection.execute(
            "INSERT INTO joboutbox VALUES(7, 'static_site_build', '{malformed')"
        )
    assert main._static_site_snapshot_retention_context(str(database)) == (False, [])

    payload = json.dumps({
        "snapshot": {
            "sqlite_path":"/data/static_site_snapshots/active.sqlite",
            "manifest_path":"/data/static_site_snapshots/active.manifest.json",
        }
    })
    with sqlite3.connect(database) as connection:
        connection.execute("UPDATE joboutbox SET payload=? WHERE id=7", (payload,))
    assert main._static_site_snapshot_retention_context(str(database)) == (
        True,
        [
            "/data/static_site_snapshots/active.sqlite",
            "/data/static_site_snapshots/active.manifest.json",
        ],
    )


@pytest.mark.asyncio
async def test_static_site_preflight_reconciles_exact_current_candidate_before_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import kaggle_status
    import main

    calls: list[tuple[object, str, str]] = []
    candidate = SimpleNamespace(run_id="static-site:published-run")
    monkeypatch.setattr(main, "resolve_current_secret_candidate", lambda _path: candidate)

    async def fake_reconcile(db, *, run_id, message):  # noqa: ANN001
        calls.append((db, run_id, message))
        return {"status": "already_terminal", "released_resource_count": 1}

    monkeypatch.setattr(kaggle_status, "reconcile_kaggle_run_terminal_from_host", fake_reconcile)
    db = SimpleNamespace(path="/tmp/does-not-need-to-exist.sqlite")

    result = await main._reconcile_current_static_site_candidate_status(db)

    assert result == {"status": "already_terminal", "released_resource_count": 1}
    assert calls == [(db, candidate.run_id, "static-site preflight reconciled current published candidate")]


def test_static_site_build_kaggle_command_includes_pgvector_handoff(monkeypatch: pytest.MonkeyPatch) -> None:
    import main

    monkeypatch.setenv("STATIC_SITE_RELATED_MODE", "pgvector")
    monkeypatch.setenv("STATIC_SITE_SYNC_PGVECTOR_VECTORS", "1")
    monkeypatch.setenv("STATIC_SITE_PGVECTOR_EMBEDDING_MODEL", "gemini-embedding-2")
    monkeypatch.setenv("STATIC_SITE_PGVECTOR_EMBEDDING_KEY_ENV", "GOOGLE_API_KEY4")
    monkeypatch.setenv("STATIC_SITE_PGVECTOR_MAX_PROVIDER_CALLS", "123")
    monkeypatch.setenv("STATIC_SITE_GEMMA_RELATED_VERIFY", "1")
    monkeypatch.setenv("STATIC_SITE_GEMMA_RELATED_MAX_ANCHORS", "15")
    monkeypatch.setenv("STATIC_SITE_ASSET_BASE_URL", "https://static.kenigevents.ru")
    monkeypatch.setenv("STATIC_SITE_ASTRO_ASSET_BASE_URL", "https://static.kenigevents.ru/{buildId}")
    monkeypatch.setenv("STATIC_SITE_ICS_BASE_URL", "https://static.kenigevents.ru/ics")
    monkeypatch.setenv("PERSONALIZATION_SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY", "sb_publishable_test")

    cmd = main._static_site_build_kaggle_command(
        db_path="/data/db.sqlite",
        build_id="preview-test-pgvector",
        limit=70,
        current_date="2026-06-29",
        script_path="/repo/scripts/run_static_site_builder_kaggle.py",
        status_callback_url="https://events-bot.example/internal/kaggle/run-event",
    )

    assert cmd[0] == sys.executable
    assert _arg_after(cmd, "--db") == "/data/db.sqlite"
    assert _arg_after(cmd, "--status-db") == "/data/db.sqlite"
    assert _arg_after(cmd, "--status-callback-url") == "https://events-bot.example/internal/kaggle/run-event"
    assert _arg_after(cmd, "--related-mode") == "pgvector"
    assert "--sync-pgvector-vectors" in cmd
    assert _arg_after(cmd, "--pgvector-embedding-model") == "gemini-embedding-2"
    assert _arg_after(cmd, "--pgvector-embedding-key-env") == "GOOGLE_API_KEY4"
    assert _arg_after(cmd, "--pgvector-max-provider-calls") == "123"
    assert "--gemma-related-verify" in cmd
    assert _arg_after(cmd, "--gemma-related-max-anchors") == "15"
    assert _arg_after(cmd, "--asset-base-url") == "https://static.kenigevents.ru"
    assert _arg_after(cmd, "--astro-asset-base-url") == "https://static.kenigevents.ru/{buildId}"
    assert _arg_after(cmd, "--ics-base-url") == "https://static.kenigevents.ru/ics"
    assert _arg_after(cmd, "--public-personalization-supabase-url") == "https://example.supabase.co"
    assert _arg_after(cmd, "--public-personalization-supabase-publishable-key") == "sb_publishable_test"
    assert _arg_after(cmd, "--public-yandex-auth-provider") == "custom:yandex"
    assert "--export-in-kaggle" in cmd
    assert "--download-output" in cmd


def test_static_site_build_kaggle_command_rejects_unknown_related_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    import main

    monkeypatch.setenv("STATIC_SITE_RELATED_MODE", "not-a-mode")

    with pytest.raises(ValueError):
        main._static_site_build_kaggle_command(
            db_path="/data/db.sqlite",
            build_id="preview-test",
            limit=1,
            current_date="2026-06-29",
            script_path="/repo/scripts/run_static_site_builder_kaggle.py",
            status_callback_url="https://events-bot.example/internal/kaggle/run-event",
        )


def test_add_build_08_command_separates_snapshot_from_live_status_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import main

    monkeypatch.delenv("STATIC_SITE_KAGGLE_TIMEOUT_MINUTES", raising=False)
    cmd = main._static_site_build_kaggle_command(
        db_path="/data/static_site_snapshots/request.sqlite",
        status_db_path="/data/db.sqlite",
        build_id="preview-secret-test",
        limit=5000,
        current_date="2026-07-17",
        script_path="/repo/scripts/run_static_site_builder_kaggle.py",
        status_callback_url="https://events-bot.example/internal/kaggle/run-event",
    )
    assert _arg_after(cmd, "--db") == "/data/static_site_snapshots/request.sqlite"
    assert _arg_after(cmd, "--status-db") == "/data/db.sqlite"
    assert _arg_after(cmd, "--timeout-minutes") == "90"
    assert main.JOB_MAX_RUNTIME[main.JobTask.static_site_build] == 5400


def test_add_build_08_production_candidate_binds_snapshot_repo_run_and_secret() -> None:
    import main

    cmd = main._static_site_build_kaggle_command(
        db_path="/data/static_site_snapshots/request.sqlite",
        status_db_path="/data/db.sqlite",
        snapshot_manifest_path="/data/static_site_snapshots/request.manifest.json",
        build_id="production-secret-test",
        repo_sha="a" * 40,
        run_id="static-site:production-secret-test:12345678",
        candidate_token="-" + "A" * 42,
        profile="production-candidate",
        limit=5000,
        current_date="2026-07-17",
        current_datetime="2026-07-17T00:00:00+02:00",
        input_fingerprint="f" * 64,
        script_path="/repo/scripts/run_static_site_builder_kaggle.py",
        status_callback_url="https://events-bot.example/internal/kaggle/run-event",
    )
    assert _arg_after(cmd, "--profile") == "production-candidate"
    assert _arg_after(cmd, "--catalog-mode") == "full"
    assert _arg_after(cmd, "--snapshot-manifest").endswith("request.manifest.json")
    assert _arg_after(cmd, "--repo-sha") == "a" * 40
    assert _arg_after(cmd, "--run-id").startswith("static-site:")
    assert "--candidate-token=" + "-" + "A" * 42 in cmd
    assert _arg_after(cmd, "--current-datetime") == "2026-07-17T00:00:00+02:00"
    assert _arg_after(cmd, "--input-fingerprint") == "f" * 64


def test_add_build_08_recovery_command_never_pushes_and_binds_dataset() -> None:
    import main

    cmd = main._static_site_build_kaggle_command(
        db_path="/data/static_site_snapshots/request.sqlite",
        status_db_path="/data/db.sqlite",
        snapshot_manifest_path="/data/static_site_snapshots/request.manifest.json",
        build_id="production-secret-test",
        repo_sha="a" * 40,
        run_id="static-site:production-secret-test:12345678",
        candidate_token="A" * 43,
        profile="production-candidate",
        limit=5000,
        current_date="2026-07-18",
        current_datetime="2026-07-18T12:00:00+02:00",
        input_fingerprint="f" * 64,
        script_path="/repo/scripts/run_static_site_builder_kaggle.py",
        status_callback_url="https://events-bot.example/internal/kaggle/run-event",
        adopt_existing=True,
        expected_dataset_ref="owner/static-site-builder-input-exact",
    )
    assert "--adopt-existing" in cmd
    assert _arg_after(cmd, "--expected-dataset-ref") == "owner/static-site-builder-input-exact"


def test_runner_adopts_exact_complete_output_without_push(tmp_path: Path) -> None:
    from scripts.run_static_site_builder_kaggle import adopt_existing_kernel_output

    class Client:
        pushes = 0
        def kernel_has_dataset_sources(self, _kernel, expected):
            return expected == ["owner/exact-input"], {"dataset_sources": ["owner/exact-input"]}
        def get_kernel_status(self, _kernel):
            return {"status": "COMPLETE"}
        def download_kernel_output(self, _kernel, *, path, force=True):
            output = Path(path)
            (output / "static_site_build_result.json").write_text(
                json.dumps({"ok": True, "build_id": "preview-adopt"}), encoding="utf-8"
            )
            return ["static_site_build_result.json"]

    args = SimpleNamespace(
        expected_dataset_ref="owner/exact-input",
        build_id="preview-adopt",
        profile="preview",
        keep_secret_datasets=True,
    )
    import scripts.run_static_site_builder_kaggle as runner
    old_root = runner.ARTIFACT_ROOT
    runner.ARTIFACT_ROOT = tmp_path
    try:
        assert adopt_existing_kernel_output(args, Client(), "owner/kernel") == 0
    finally:
        runner.ARTIFACT_ROOT = old_root
    assert Client.pushes == 0


def test_runner_closes_status_database_after_config_creation() -> None:
    import asyncio
    from scripts.run_static_site_builder_kaggle import _create_status_config_and_close

    class Database:
        closed = False

        async def close(self):
            self.closed = True

    async def create_config(db, **kwargs):
        assert not db.closed
        return {"run_id": kwargs["run_id"]}

    db = Database()
    result = asyncio.run(
        _create_status_config_and_close(db, create_config, run_id="static-site:test")
    )
    assert result == {"run_id": "static-site:test"}
    assert db.closed is True


def test_static_site_kernel_loads_status_helper_from_mounted_dataset(tmp_path: Path) -> None:
    import importlib.util

    kernel_path = (
        Path(__file__).resolve().parents[1]
        / "kaggle"
        / "StaticSiteBuilder"
        / "static_site_builder.py"
    )
    spec = importlib.util.spec_from_file_location("static_site_builder_status_test", kernel_path)
    assert spec and spec.loader
    kernel = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(kernel)

    mounted = tmp_path / "status-static-site-builder" / "v1"
    mounted.mkdir(parents=True)
    (mounted / "kaggle_status_client.py").write_text(
        "def load_status_client(**kwargs):\n    return ('mounted', kwargs)\n",
        encoding="utf-8",
    )

    loader = kernel._load_status_loader([tmp_path])

    assert loader is not None
    assert loader(output_dir="/tmp/output") == ("mounted", {"output_dir": "/tmp/output"})


def test_add_build_11_astro_asset_template_resolves_to_exact_build() -> None:
    from scripts.run_static_site_builder_kaggle import resolve_build_template

    assert resolve_build_template(
        "https://static.kenigevents.ru/{buildId}", "production-tested-1"
    ) == "https://static.kenigevents.ru/production-tested-1"
    with pytest.raises(ValueError, match="unresolved build template"):
        resolve_build_template("https://static.kenigevents.ru/{unknown}", "production-tested-1")
