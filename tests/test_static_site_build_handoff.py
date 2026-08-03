from __future__ import annotations

import asyncio
import sys
import json
import importlib.util
import sqlite3
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest


def test_kaggle_site_source_bundle_includes_repo_level_transport_contract(
    tmp_path: Path,
) -> None:
    from scripts import run_static_site_builder_kaggle as runner

    site_dir = tmp_path / "site"
    site_dir.mkdir()
    (site_dir / "package.json").write_text("{}\n", encoding="utf-8")
    archive = tmp_path / "site_source.tarball"

    runner.tar_site_source(site_dir, archive)

    with tarfile.open(archive, "r:gz") as bundle:
        names = set(bundle.getnames())
        contract = bundle.extractfile(
            "docs/testing/transport-fault-profiles.v1.yml"
        )
        assert "site/package.json" in names
        assert contract is not None
        assert b"static_site_transport_fault_profiles.v1" in contract.read()


def _arg_after(cmd: list[str], name: str) -> str:
    return cmd[cmd.index(name) + 1]


def test_publication_workspace_uses_ephemeral_scratch_and_cleans_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import main

    scratch = tmp_path / "publication-scratch"
    scratch.mkdir()
    monkeypatch.setenv("STATIC_SITE_PUBLICATION_SCRATCH_DIR", str(scratch))
    workspace = main._static_site_publication_workspace()
    path = Path(workspace.name)

    assert path.parent == scratch.resolve()
    assert path.is_dir()
    (path / "expanded-candidate.bin").write_bytes(b"candidate")

    workspace.cleanup()
    assert not path.exists()


@pytest.mark.asyncio
async def test_vector_barrier_refreshes_intermediate_smart_update_revision(tmp_path: Path) -> None:
    import main

    db = main.Database(str(tmp_path / "events.sqlite"))
    await db.init()
    try:
        async with db.get_session() as session:
            event = main.Event(
                title="Current canonical title",
                description="Current canonical description",
                date="2026-08-08",
                time="19:00",
                city="Калининград",
                location_name="Дом искусств",
                source_text="source",
                photo_urls=["https://static.kenigevents.ru/p/current.webp"],
            )
            session.add(event)
            await session.commit()
            await session.refresh(event)
            event_id = int(event.id)
            current_revision = main.event_public_revision(event)

        payload = main.make_static_site_request_payload(
            reason="smart_update",
            event_ids=[event_id, 999999],
            event_revisions={event_id: "historical-intermediate", 999999: "deleted"},
            require_vector_barrier=True,
        )
        old_watermark = payload["target_watermark"]
        refreshed, changed_ids = await main._refresh_static_site_vector_barrier_payload(
            db, payload
        )

        assert changed_ids == [event_id, 999999]
        assert refreshed["event_revisions"] == {str(event_id): current_revision}
        assert refreshed["target_watermark"] != old_watermark
        assert refreshed["target_watermark"] == main.static_site_request_watermark(refreshed)

        unchanged, changed_ids = await main._refresh_static_site_vector_barrier_payload(
            db, refreshed
        )
        assert changed_ids == []
        assert unchanged == refreshed
    finally:
        await db.close()


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


def test_output_retention_context_preserves_only_exact_active_handoff(tmp_path: Path) -> None:
    import main

    database = tmp_path / "retention.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE static_site_build_state(release_channel TEXT PRIMARY KEY, active_job_id INTEGER)"
        )
        connection.execute(
            "CREATE TABLE joboutbox(id INTEGER PRIMARY KEY, task TEXT, payload TEXT)"
        )
        connection.execute("INSERT INTO static_site_build_state VALUES('secret_preview', 7)")
        connection.execute(
            "INSERT INTO joboutbox VALUES(7, 'static_site_build', ?)",
            (json.dumps({"remote_handoff": {"build_id": "production-exact-123"}}),),
        )

    assert main._static_site_output_retention_context(str(database)) == (
        True,
        ["production-exact-123"],
    )
    with sqlite3.connect(database) as connection:
        connection.execute(
            "UPDATE joboutbox SET payload=? WHERE id=7",
            (json.dumps({"remote_handoff": {"build_id": "../../operator"}}),),
        )
    assert main._static_site_output_retention_context(str(database)) == (False, [])


def test_shared_static_site_artifact_root_is_configurable(tmp_path: Path, monkeypatch) -> None:
    from static_site_release import static_site_artifact_root, static_site_scratch_root

    root = tmp_path / "persistent-static"
    monkeypatch.setenv("STATIC_SITE_ARTIFACT_ROOT", str(root))
    monkeypatch.delenv("STATIC_SITE_SCRATCH_DIR", raising=False)

    assert static_site_artifact_root("/ignored/repo") == root.resolve()
    assert static_site_scratch_root(static_site_artifact_root()) == root.resolve() / ".tmp"


def test_static_site_preflight_rejects_critical_root_scratch(monkeypatch) -> None:
    import main

    monkeypatch.setattr(
        main,
        "runtime_scratch_health",
        lambda: {
            "status": "critical",
            "tempfile_status": "error",
            "tempfile_error": "OSError",
        },
    )

    with pytest.raises(main.StaticSiteRetryableError, match="root_scratch_preflight"):
        main._static_site_storage_preflight()


@pytest.mark.asyncio
async def test_static_site_receipt_payload_retries_sqlite_writer_collision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio
    import main
    from db import Database
    from models import JobOutbox, JobStatus, JobTask

    monkeypatch.setenv("DB_TIMEOUT_SEC", "0.1")
    database = tmp_path / "receipt-retry.sqlite"
    db = Database(str(database))
    await db.init()
    async with db.get_session() as session:
        session.add(
            JobOutbox(
                event_id=1,
                task=JobTask.static_site_build,
                status=JobStatus.running,
                payload={"before": True},
                coalesce_key="static_site_build:prod",
                next_run_at=main.datetime.now(main.timezone.utc),
                updated_at=main.datetime.now(main.timezone.utc),
            )
        )
        await session.commit()
        job_id = int(
            (
                await session.execute(
                    main.select(JobOutbox.id).order_by(JobOutbox.id.desc()).limit(1)
                )
            ).scalar_one()
        )

    blocker = sqlite3.connect(database, timeout=0)
    blocker.execute("BEGIN IMMEDIATE")
    blocker.execute("UPDATE joboutbox SET updated_at=updated_at WHERE id=?", (job_id,))
    task = asyncio.create_task(
        main._patch_static_site_request_payload(db, job_id, {"receipt": "exact"})
    )
    await asyncio.sleep(0.2)
    blocker.rollback()
    blocker.close()
    await task

    with sqlite3.connect(database) as connection:
        payload = json.loads(
            connection.execute("SELECT payload FROM joboutbox WHERE id=?", (job_id,)).fetchone()[0]
        )
    assert payload == {"before": True, "receipt": "exact"}
    await db.close()


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
    monkeypatch.setenv("PERSONALIZATION_SUPABASE_RELAY_URL", "https://relay.example.test")
    monkeypatch.setenv("STATIC_SITE_SECRET_CANDIDATE_ARTIFACT_RESEARCH", "1")
    monkeypatch.setenv("STATIC_SITE_SECRET_CANDIDATE_REQUIRE_AUTHORIZED_SEARCH", "1")

    cmd = main._static_site_build_kaggle_command(
        db_path="/data/db.sqlite",
        build_id="preview-test-pgvector",
        limit=70,
        current_date="2026-06-29",
        script_path="/repo/scripts/run_static_site_builder_kaggle.py",
        status_callback_url="https://events-bot.example/internal/kaggle/run-event",
        related_corpus_revision="b" * 64,
    )

    assert cmd[0] == sys.executable
    assert _arg_after(cmd, "--db") == "/data/db.sqlite"
    assert _arg_after(cmd, "--status-db") == "/data/db.sqlite"
    assert _arg_after(cmd, "--status-callback-url") == "https://events-bot.example/internal/kaggle/run-event"
    assert _arg_after(cmd, "--related-mode") == "pgvector"
    assert _arg_after(cmd, "--related-corpus-revision") == "b" * 64
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
    assert _arg_after(cmd, "--public-personalization-supabase-relay-url") == "https://relay.example.test"
    assert _arg_after(cmd, "--public-yandex-auth-provider") == "custom:yandex"
    assert "--secret-candidate-artifact-research" in cmd
    assert "--secret-candidate-require-authorized-search" in cmd
    assert "--export-in-kaggle" in cmd
    assert "--download-output" in cmd


def test_kaggle_runtime_payload_forwards_interest_club_release_gates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import run_static_site_builder_kaggle as runner

    monkeypatch.setenv("GOOGLE_API_KEY4", "google-test")
    monkeypatch.setenv("PERSONALIZATION_SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("PERSONALIZATION_SUPABASE_SECRET_KEY", "secret-test")
    monkeypatch.setenv(
        "GOOGLE_AI_LIMITER_SUPABASE_URL", "https://limiter.supabase.co"
    )
    monkeypatch.setenv(
        "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY", "limiter-secret-test"
    )
    monkeypatch.setenv("ENABLE_INTEREST_CLUB_STATIC_PROJECTION", "1")
    monkeypatch.setenv("PUBLIC_INTEREST_CLUBS_ENABLED", "1")
    args = SimpleNamespace(
        gemma_related_verify=False,
        related_mode="pgvector",
        sync_pgvector_vectors=True,
        gemma_related_key_env="GOOGLE_API_KEY4",
        pgvector_embedding_key_env="GOOGLE_API_KEY4",
    )

    payload = runner.build_runtime_secret_payload(args)

    assert payload["ENABLE_INTEREST_CLUB_STATIC_PROJECTION"] == "1"
    assert payload["PUBLIC_INTEREST_CLUBS_ENABLED"] == "1"
    assert payload["GOOGLE_AI_LIMITER_SUPABASE_URL"] == "https://limiter.supabase.co"


def test_kaggle_runner_and_builder_forward_related_corpus_revision(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from scripts import run_static_site_builder_kaggle as runner

    revision = "c" * 64
    kernel_src = tmp_path / "kernel"
    kernel_src.mkdir()
    staging = tmp_path / "staging"
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    staged_site = tmp_path / "staged-site"
    staged_site.mkdir()
    monkeypatch.setattr(runner, "KERNEL_SRC", kernel_src)
    monkeypatch.setattr(runner, "copy_tree", lambda *_args: None)
    monkeypatch.setattr(runner, "prepare_site_source", lambda *_args: staged_site)
    monkeypatch.setattr(
        runner,
        "tar_site_source",
        lambda _source, target: Path(target).write_bytes(b"site"),
    )
    monkeypatch.setattr(runner, "create_input_dataset", lambda *_args: None)
    monkeypatch.setattr(runner, "wait_dataset_ready", lambda *_args, **_kwargs: None)
    args = SimpleNamespace(
        build_id="preview-revision",
        run_id="run-revision",
        profile="preview",
        catalog_mode="slice",
        repo_sha="",
        candidate_token="",
        snapshot_contract=None,
        current_date="2026-07-20",
        current_datetime="",
        build_clock=None,
        input_fingerprint="",
        focus_date_from="",
        focus_date_to="",
        limit=50,
        public_site_origin="https://kenigevents.ru",
        asset_base_url="",
        astro_asset_base_url="",
        ics_base_url="",
        public_personalization_supabase_url="",
        public_personalization_supabase_publishable_key="",
        public_personalization_supabase_relay_url="https://relay.example.test",
        public_yandex_auth_provider="custom:yandex",
        secret_candidate_artifact_research=True,
        secret_candidate_require_authorized_search=True,
        export_in_kaggle=False,
        db="",
        snapshot_manifest="",
        related_cache="",
        related_mode="pgvector",
        related_corpus_revision=revision,
        sync_pgvector_vectors=False,
        pgvector_embedding_model="gemini-embedding-2",
        pgvector_embedding_key_env="GOOGLE_API_KEY4",
        pgvector_max_provider_calls=1000,
        gemma_related_verify=False,
        gemma_related_model="models/gemma-4-26b-a4b-it",
        gemma_related_key_env="GOOGLE_API_KEY4",
        gemma_related_max_anchors=0,
    )

    runner.stage_kernel_and_dataset(args, staging, dataset, object(), "owner")

    config = json.loads((dataset / "build_config.json").read_text(encoding="utf-8"))
    assert config["related_corpus_revision"] == revision
    assert config["secret_candidate_artifact_research"] is True
    assert config["secret_candidate_require_authorized_search"] is True
    assert config["public_personalization_supabase_relay_url"] == "https://relay.example.test"

    builder_path = (
        Path(__file__).resolve().parents[1]
        / "kaggle"
        / "StaticSiteBuilder"
        / "static_site_builder.py"
    )
    spec = importlib.util.spec_from_file_location("static_site_builder_revision_test", builder_path)
    assert spec and spec.loader
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)
    candidate_env: dict[str, str] = {}
    builder.apply_secret_candidate_research_env(candidate_env, config | {"profile": "production-candidate"})
    assert candidate_env == {
        "PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH": "tail",
        "SECRET_CANDIDATE_REQUIRE_AUTHORIZED_SEARCH": "1",
    }
    preview_env: dict[str, str] = {}
    builder.apply_secret_candidate_research_env(preview_env, config | {"profile": "preview"})
    assert preview_env == {}
    working = tmp_path / "working"
    site_dir = tmp_path / "site"
    (site_dir / "scripts").mkdir(parents=True)
    (site_dir / "scripts" / "export-production-preview-data.py").write_text("", encoding="utf-8")
    input_db = tmp_path / "events.sqlite"
    input_db.write_bytes(b"sqlite")
    working.mkdir()
    monkeypatch.setattr(builder, "WORKING", working)
    monkeypatch.setattr(builder, "SITE_DIR", site_dir)
    monkeypatch.setattr(
        builder,
        "find_input_file",
        lambda name: input_db if name == "events.sqlite" else None,
    )
    monkeypatch.setattr(builder, "validate_snapshot_input", lambda *_args: {})
    monkeypatch.setattr(
        builder,
        "validate_build_clock",
        lambda _config: {
            "effective_date": "2026-07-20",
            "current_datetime": "2026-07-20T12:00:00+02:00",
        },
    )
    monkeypatch.setattr(builder, "ensure_python_deps_for_gemma", lambda *_args: None)
    monkeypatch.setattr(builder, "load_encrypted_secrets_to_env", lambda: None)
    commands: list[list[str]] = []
    monkeypatch.setattr(
        builder,
        "run",
        lambda command, **_kwargs: commands.append(command),
    )
    builder.export_preview_data_if_configured(
        {
            "export_in_kaggle": True,
            "sqlite_db_filename": "events.sqlite",
            "related_mode": "pgvector",
            "related_corpus_revision": revision,
            "current_date": "2026-07-20",
            "catalog_mode": "slice",
            "limit": 50,
        }
    )
    assert _arg_after(commands[-1], "--related-corpus-revision") == revision


def test_fly_requires_vector_receipt_and_keeps_strict_gemma_verifier_off() -> None:
    import tomllib

    config = tomllib.loads((Path(__file__).resolve().parents[1] / "fly.toml").read_text())
    env = config["env"]
    assert env["STATIC_SITE_REQUIRE_VECTOR_BARRIER"] == "1"
    assert env["EVENT_VECTOR_SYNC_RECEIPT_PATH"] == "/data/event_vector_sync_receipt.json"
    assert env["STATIC_SITE_VECTOR_RECEIPT_PATH"] == "/data/event_vector_sync_receipt.json"
    assert env["ENABLE_EVENT_VECTOR_SYNC"] == "1"
    assert env["STATIC_SITE_SYNC_PGVECTOR_VECTORS"] == "0"
    assert env["STATIC_SITE_GEMMA_RELATED_VERIFY"] == "0"
    assert env["STATIC_SITE_GEMMA_RELATED_MAX_ANCHORS"] == "0"


def test_fly_release_bakes_exact_main_revision_into_image() -> None:
    root = Path(__file__).resolve().parents[1]
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    deploy_script = (root / "scripts" / "deploy_fly_main.sh").read_text(
        encoding="utf-8"
    )

    assert "ARG STATIC_SITE_IMAGE_REPO_SHA" in dockerfile
    assert "> /app/.static-site-repo-sha" in dockerfile
    assert 'MAIN_SHA="$(git rev-parse origin/main)"' in deploy_script
    assert 'if [[ "$HEAD_SHA" != "$MAIN_SHA" ]]' in deploy_script
    assert '--build-arg "STATIC_SITE_IMAGE_REPO_SHA=$HEAD_SHA"' in deploy_script


def test_kaggle_builder_bridges_decrypted_public_club_flag_into_astro(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    builder_path = (
        Path(__file__).resolve().parents[1]
        / "kaggle"
        / "StaticSiteBuilder"
        / "static_site_builder.py"
    )
    spec = importlib.util.spec_from_file_location(
        "static_site_builder_club_env_test", builder_path
    )
    assert spec and spec.loader
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)

    astro_env = {"PUBLIC_SITE_ORIGIN": "https://kenigevents.ru"}
    monkeypatch.setenv("PUBLIC_INTEREST_CLUBS_ENABLED", "1")
    builder.apply_public_interest_clubs_env(astro_env)

    assert astro_env["PUBLIC_INTEREST_CLUBS_ENABLED"] == "1"


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

    monkeypatch.setenv("STATIC_SITE_RELATED_MODE", "pgvector")
    with pytest.raises(ValueError, match="related corpus revision"):
        main._static_site_build_kaggle_command(
            db_path="/data/db.sqlite",
            build_id="preview-test",
            limit=1,
            current_date="2026-06-29",
            script_path="/repo/scripts/run_static_site_builder_kaggle.py",
            status_callback_url="https://events-bot.example/internal/kaggle/run-event",
            related_corpus_revision="not-a-hash",
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


def test_runner_output_directory_rejects_traversal_and_symlinks(tmp_path: Path) -> None:
    from scripts.run_static_site_builder_kaggle import prepare_output_directory

    with pytest.raises(ValueError, match="build id"):
        prepare_output_directory(tmp_path, "preview-../../operator")
    outside = tmp_path / "operator"
    outside.mkdir()
    link = tmp_path / "output-preview-linked"
    link.symlink_to(outside, target_is_directory=True)
    with pytest.raises(RuntimeError, match="symlink"):
        prepare_output_directory(tmp_path, "preview-linked")
    assert outside.is_dir()


def test_runner_storage_preflight_checks_root_and_durable_scratch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import scripts.run_static_site_builder_kaggle as runner

    scratch = tmp_path / "static-scratch"
    probes: list[Path] = []
    monkeypatch.setattr(runner, "SCRATCH_ROOT", scratch)
    monkeypatch.setattr(
        runner,
        "runtime_scratch_health",
        lambda: {"status": "ok", "tempfile_status": "ok"},
    )
    monkeypatch.setattr(
        runner,
        "writable_disk_health",
        lambda path, **_kwargs: probes.append(Path(path))
        or {"status": "ok", "tempfile_status": "ok"},
    )

    runner.require_static_site_storage_ready()

    assert scratch.is_dir()
    assert probes == [scratch]


def test_runner_prunes_only_abandoned_owned_scratch(tmp_path: Path) -> None:
    from scripts.run_static_site_builder_kaggle import prune_abandoned_static_site_scratch

    abandoned = tmp_path / "static-site-kaggle-dead123"
    abandoned.mkdir()
    (abandoned / "snapshot.sqlite").write_bytes(b"stale")
    unrelated = tmp_path / "operator-notes"
    unrelated.mkdir()
    (unrelated / "keep.txt").write_text("keep", encoding="utf-8")
    outside = tmp_path / "outside"
    outside.mkdir()
    link = tmp_path / "static-site-kaggle-linked"
    link.symlink_to(outside, target_is_directory=True)

    report = prune_abandoned_static_site_scratch(tmp_path)

    assert report == {
        "removed_directories": ["static-site-kaggle-dead123"],
        "removed_bytes": 5,
    }
    assert not abandoned.exists()
    assert unrelated.is_dir()
    assert link.is_symlink()
    assert outside.is_dir()


def test_host_capacity_cleanup_reaches_runner_scratch_before_preflight(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import main

    artifact_root = tmp_path / "static-builder"
    abandoned = artifact_root / "tmp" / "static-site-kaggle-dead123"
    abandoned.mkdir(parents=True)
    (abandoned / "events.sqlite").write_bytes(b"stale")
    monkeypatch.setenv("STATIC_SITE_ARTIFACT_ROOT", str(artifact_root))
    monkeypatch.setenv("STATIC_SITE_SCRATCH_DIR", str(artifact_root / "tmp"))

    report = asyncio.run(main._prune_static_site_abandoned_scratch_for_capacity())

    assert report == {
        "status": "ok",
        "removed_directories": ["static-site-kaggle-dead123"],
        "removed_bytes": 5,
    }
    assert not abandoned.exists()


def test_capacity_cleanup_removes_unreferenced_terminal_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import asyncio
    import sqlite3
    import main

    database = tmp_path / "db.sqlite"
    sqlite3.connect(database).close()
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    snapshot = snapshots / "snapshot-20260731T010203-deadbeef00.sqlite"
    manifest = snapshots / "snapshot-20260731T010203-deadbeef00.manifest.json"
    snapshot.write_bytes(b"snapshot")
    manifest.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("STATIC_SITE_SNAPSHOT_DIR", str(snapshots))

    report = asyncio.run(
        main._prune_static_site_terminal_snapshots_for_capacity(
            SimpleNamespace(path=str(database))
        )
    )

    assert report["removed_snapshot_ids"] == [snapshot.stem]
    assert report["retained_terminal_count"] == 0
    assert not snapshot.exists()
    assert not manifest.exists()


def test_static_site_storage_capacity_defers_without_consuming_attempt_budget() -> None:
    import main

    source = Path(main.__file__).read_text(encoding="utf-8")
    assert 'StaticSiteSingleFlightDeferred(f"static_site_capacity_deferred:{exc}")' in source


def test_static_site_remote_recovery_precedes_new_build_capacity_gate() -> None:
    import main

    source = Path(main.__file__).read_text(encoding="utf-8")
    recovery = source.index("recovered, recovered_result = await _recover_previous_static_site_attempt(")
    recovered_return = source.index("return recovered_result if recovered_result is not None else True", recovery)
    capacity = source.index("await asyncio.to_thread(_static_site_storage_preflight)", recovery)

    assert recovery < recovered_return < capacity


def test_runner_adoption_cleans_duplicate_before_capacity_gate() -> None:
    source = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_static_site_builder_kaggle.py"
    ).read_text(encoding="utf-8")
    adopt = source.index("def adopt_existing_kernel_output(")
    prepare = source.index("out_dir = prepare_output_directory", adopt)
    adoption_capacity = source.index("require_static_site_storage_ready()", prepare)
    download = source.index("download_kernel_output", adoption_capacity)
    dispatch = source.index("if args.adopt_existing:")
    new_build_capacity = source.index("require_static_site_storage_ready()", dispatch)

    assert prepare < adoption_capacity < download
    assert dispatch < new_build_capacity


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


def test_static_site_kernel_releases_resource_before_terminal_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib.util

    kernel_path = (
        Path(__file__).resolve().parents[1]
        / "kaggle"
        / "StaticSiteBuilder"
        / "static_site_builder.py"
    )
    spec = importlib.util.spec_from_file_location("static_site_builder_finish_order_test", kernel_path)
    assert spec and spec.loader
    kernel = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(kernel)
    calls: list[str] = []

    class StatusClient:
        def release_resource(self, key: str) -> None:
            calls.append(f"release:{key}")

        def stop_alive(self) -> None:
            calls.append("stop_alive")

    kernel.STATUS_CLIENT = StatusClient()
    kernel.ACQUIRED_RESOURCES[:] = ["static_site:builder"]
    monkeypatch.setattr(
        kernel,
        "status_event",
        lambda event, **_kwargs: calls.append(event),
    )

    kernel.finish_status(ok=False, message="failed")

    assert calls == ["release:static_site:builder", "report_written", "stop_alive"]
    assert kernel.ACQUIRED_RESOURCES == []


def test_static_site_kernel_terminally_reports_status_bootstrap_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import importlib.util

    kernel_path = (
        Path(__file__).resolve().parents[1]
        / "kaggle"
        / "StaticSiteBuilder"
        / "static_site_builder.py"
    )
    spec = importlib.util.spec_from_file_location("static_site_builder_bootstrap_failure_test", kernel_path)
    assert spec and spec.loader
    kernel = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(kernel)
    calls: list[tuple[bool, str | None]] = []
    monkeypatch.setattr(kernel, "WORKING", tmp_path)
    monkeypatch.setattr(kernel, "RESULT_PATH", tmp_path / "static_site_build_result.json")
    monkeypatch.setattr(
        kernel,
        "init_status",
        lambda: (_ for _ in ()).throw(RuntimeError("resource busy")),
    )
    monkeypatch.setattr(kernel, "cleanup_transient_workspace", lambda: None)
    monkeypatch.setattr(
        kernel,
        "finish_status",
        lambda *, ok, message=None: calls.append((ok, message)),
    )

    with pytest.raises(RuntimeError, match="resource busy"):
        kernel.main()

    assert calls == [(False, "RuntimeError: resource busy")]
    failure = json.loads((tmp_path / "static_site_build_result.json").read_text(encoding="utf-8"))
    assert failure["ok"] is False
    assert failure["error"] == "resource busy"


def test_static_site_kernel_browser_command_deadline_is_forwarded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import importlib.util

    kernel_path = (
        Path(__file__).resolve().parents[1]
        / "kaggle"
        / "StaticSiteBuilder"
        / "static_site_builder.py"
    )
    spec = importlib.util.spec_from_file_location("static_site_builder_timeout_test", kernel_path)
    assert spec and spec.loader
    kernel = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(kernel)
    captured: dict[str, object] = {}

    def fake_run(command, **kwargs):
        captured.update({"command": command, **kwargs})

    monkeypatch.setattr(kernel.subprocess, "run", fake_run)
    kernel.run(["npm", "run", "check:browser-release"], tmp_path, timeout_seconds=300)

    assert captured["timeout"] == 300
    assert captured["check"] is True


def test_static_site_kernel_installs_chromium_with_linux_dependencies() -> None:
    import importlib.util

    kernel_path = (
        Path(__file__).resolve().parents[1]
        / "kaggle"
        / "StaticSiteBuilder"
        / "static_site_builder.py"
    )
    spec = importlib.util.spec_from_file_location("static_site_builder_playwright_deps_test", kernel_path)
    assert spec and spec.loader
    kernel = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(kernel)

    assert kernel.PLAYWRIGHT_CHROMIUM_INSTALL_COMMAND == (
        "npx",
        "playwright",
        "install",
        "--with-deps",
        "--only-shell",
        "chromium",
    )


def test_static_site_kernel_retains_loaded_media_and_keyboard_browser_evidence() -> None:
    kernel_path = (
        Path(__file__).resolve().parents[1]
        / "kaggle"
        / "StaticSiteBuilder"
        / "static_site_builder.py"
    )
    source = kernel_path.read_text(encoding="utf-8")

    assert "--artifact-dir" in source
    assert "browser-release-report.json" in source
    assert "browser_evidence" in source
    assert "browser-evidence.tar.gz" in source
    runner_source = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_static_site_builder_kaggle.py"
    ).read_text(encoding="utf-8")
    assert "{'production_root', 'secret_candidate', 'browser_evidence'}" in runner_source


def test_add_build_11_astro_asset_template_resolves_to_exact_build() -> None:
    from scripts.run_static_site_builder_kaggle import resolve_build_template

    assert resolve_build_template(
        "https://static.kenigevents.ru/{buildId}", "production-tested-1"
    ) == "https://static.kenigevents.ru/production-tested-1"
    with pytest.raises(ValueError, match="unresolved build template"):
        resolve_build_template("https://static.kenigevents.ru/{unknown}", "production-tested-1")


def test_production_candidate_requires_collection_semantics_independent_of_related_and_unusual(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import main

    monkeypatch.setenv("STATIC_SITE_RELATED_MODE", "pgvector")
    monkeypatch.setenv("STATIC_SITE_UNUSUAL_ENABLED", "0")
    cmd = main._static_site_build_kaggle_command(
        db_path="/data/snapshot.sqlite",
        status_db_path="/data/db.sqlite",
        build_id="production-collection-contract",
        limit=100,
        current_date="2026-08-01",
        current_datetime="2026-08-01T12:00:00+02:00",
        input_fingerprint="a" * 64,
        script_path="/repo/scripts/run_static_site_builder_kaggle.py",
        status_callback_url="https://example.test/internal/kaggle/run-event",
        snapshot_manifest_path="/data/snapshot.manifest.json",
        repo_sha="b" * 40,
        run_id="static-site:test",
        candidate_token="c" * 43,
        profile="production-candidate",
        related_corpus_revision="d" * 64,
    )

    assert _arg_after(cmd, "--related-mode") == "pgvector"
    assert "--unusual-enabled" not in cmd
    assert "--collection-semantic-compute" in cmd
    assert _arg_after(cmd, "--collection-batch").endswith("collection-batch-v1.json")
    assert _arg_after(cmd, "--collection-batch-last-good").endswith(
        "collection-batch-last-good.json"
    )
