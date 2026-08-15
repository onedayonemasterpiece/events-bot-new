from __future__ import annotations

import importlib.util
import hashlib
import json
import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_kernel():
    path = (
        Path(__file__).resolve().parents[1]
        / "kaggle"
        / "StaticSiteBuilder"
        / "static_site_builder.py"
    )
    spec = importlib.util.spec_from_file_location(
        "static_projection_kernel_test", path
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_kernel_reads_mounted_projection_without_working_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kernel = _load_kernel()
    working = tmp_path / "working"
    site = tmp_path / "site"
    projection = tmp_path / "input" / "static-projection-deadbeef.sqlite"
    projection.parent.mkdir()
    projection.write_bytes(b"projection")
    (site / "scripts").mkdir(parents=True)
    (site / "scripts" / "export-production-preview-data.py").write_text(
        "", encoding="utf-8"
    )
    working.mkdir()
    commands: list[list[str]] = []
    validated: list[Path] = []
    monkeypatch.setattr(kernel, "WORKING", working)
    monkeypatch.setattr(kernel, "SITE_DIR", site)
    monkeypatch.setattr(
        kernel,
        "find_input_file",
        lambda name: projection
        if name == "static-projection-deadbeef.sqlite"
        else None,
    )
    monkeypatch.setattr(
        kernel,
        "validate_snapshot_input",
        lambda path, _config: validated.append(path) or {},
    )
    monkeypatch.setattr(
        kernel,
        "validate_build_clock",
        lambda _config: {
            "effective_date": "2026-08-15",
            "current_datetime": "2026-08-15T12:00:00+02:00",
        },
    )
    monkeypatch.setattr(
        kernel,
        "run",
        lambda command, **_kwargs: commands.append(command),
    )

    kernel.export_preview_data_if_configured(
        {
            "export_in_kaggle": True,
            "sqlite_db_filename": projection.name,
            "profile": "preview",
            "catalog_mode": "slice",
            "limit": 5,
            "related_mode": "sparse",
            "build_clock": {},
        }
    )

    assert validated == [projection.resolve()]
    assert commands
    db_arg = commands[-1][commands[-1].index("--db") + 1]
    assert db_arg == str(projection.resolve())
    assert not list(working.glob("*.sqlite*"))


def test_kernel_validates_exact_projection_column_manifest(tmp_path: Path) -> None:
    kernel = _load_kernel()
    projection = tmp_path / "projection.sqlite"
    with sqlite3.connect(projection) as con:
        con.execute("create table event(id integer, title text)")
        con.execute("insert into event values(1,'Event')")
        con.commit()
    digest = hashlib.sha256(projection.read_bytes()).hexdigest()
    config = {
        "profile": "production-candidate",
        "snapshot": {
            "snapshot_id": "snapshot",
            "sha256": digest,
            "size": projection.stat().st_size,
            "projection_schema_version": "static_site_projection_sqlite_v1",
            "table_row_counts": {"event": 1},
            "table_columns": {"event": ["id", "title"]},
        },
    }
    assert kernel.validate_snapshot_input(projection, config)["snapshot_id"] == "snapshot"
    config["snapshot"]["table_columns"]["event"].append("source_text")
    with pytest.raises(RuntimeError, match="column mismatch"):
        kernel.validate_snapshot_input(projection, config)


def test_kernel_cleanup_refuses_to_publish_legacy_sqlite_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kernel = _load_kernel()
    working = tmp_path / "working"
    working.mkdir()
    nested = working / "nested" / "deep"
    nested.mkdir(parents=True)
    leaked = nested / "events.sqlite"
    leaked_wal = nested / "events.sqlite-wal"
    leaked_shm = nested / "events.sqlite-shm"
    for path in (leaked, leaked_wal, leaked_shm):
        path.write_bytes(b"private")
    monkeypatch.setattr(kernel, "WORKING", working)
    monkeypatch.setattr(kernel, "EXTRACT_ROOT", working / "source")

    kernel.cleanup_transient_workspace()

    assert not any(path.exists() for path in (leaked, leaked_wal, leaked_shm))


def test_final_output_validation_recursively_rejects_sqlite_artifacts(
    tmp_path: Path,
) -> None:
    import scripts.run_static_site_builder_kaggle as runner

    output = tmp_path / "output"
    nested = output / "archive" / "private"
    nested.mkdir(parents=True)
    leaked = nested / "projection.sqlite-shm"
    leaked.write_bytes(b"private")

    with pytest.raises(RuntimeError, match="forbidden SQLite artifacts"):
        runner.assert_no_sqlite_artifacts(output)


def _runner_args(db: Path, manifest: Path):
    return SimpleNamespace(
        build_id="production-projection-test",
        run_id="run-projection-test",
        profile="production-candidate",
        catalog_mode="full",
        repo_sha="a" * 40,
        candidate_token="c" * 43,
        snapshot_contract={
            "snapshot_id": "snapshot-projection-test",
            "sha256": "b" * 64,
            "size": db.stat().st_size,
            "quick_check": "ok",
            "projection_schema_version": "static_site_projection_sqlite_v1",
            "table_row_counts": {"event": 1},
            "table_columns": {"event": ["id"]},
        },
        current_date="2026-08-15",
        current_datetime="2026-08-15T12:00:00+02:00",
        build_clock={
            "time_zone": "Europe/Kaliningrad",
            "effective_date": "2026-08-15",
            "current_datetime": "2026-08-15T12:00:00+02:00",
        },
        input_fingerprint="d" * 64,
        focus_date_from="",
        focus_date_to="",
        limit=5000,
        public_site_origin="https://kenigevents.ru",
        asset_base_url="",
        astro_asset_base_url="",
        ics_base_url="",
        public_personalization_supabase_url="",
        public_personalization_supabase_publishable_key="",
        public_personalization_supabase_relay_url="",
        public_yandex_auth_provider="custom:yandex",
        public_authorized_search_transport="json",
        secret_candidate_artifact_research=False,
        secret_candidate_require_authorized_search=False,
        search_corpus_receipt="",
        export_in_kaggle=True,
        db=str(db),
        snapshot_manifest=str(manifest),
        related_cache="",
        related_mode="sparse",
        related_corpus_revision="",
        sync_pgvector_vectors=False,
        pgvector_embedding_model="gemini-embedding-2",
        pgvector_embedding_key_env="GOOGLE_API_KEY4",
        pgvector_max_provider_calls=1000,
        gemma_related_verify=False,
        gemma_related_model="models/gemma-4-26b-a4b-it",
        gemma_related_key_env="GOOGLE_API_KEY4",
        gemma_related_max_anchors=0,
        semantic_cache_mode="warm",
        collection_semantic_compute=True,
        collection_product_source_scope="static-site-builder-export",
        collection_product_evidence_trust_scope="all",
        unusual_enabled=False,
        unusual_migration=False,
    )


def test_runner_stages_content_addressed_projection_without_events_sqlite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import scripts.run_static_site_builder_kaggle as runner

    db = tmp_path / "snapshot.sqlite"
    db.write_bytes(b"immutable-projection")
    manifest = tmp_path / "snapshot.manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    dataset = tmp_path / "dataset"
    staging = tmp_path / "kernel"
    dataset.mkdir()
    staging.mkdir()
    observed: dict[str, object] = {}

    def prepare(_args, work_dir):
        site = work_dir / "site"
        site.mkdir()
        (site / "package.json").write_text("{}", encoding="utf-8")
        return site

    monkeypatch.setattr(runner, "resolve_repo_sha", lambda value: value)
    monkeypatch.setattr(
        runner,
        "resolve_image_source_contract",
        lambda _args: {
            "manifest_sha256": "e" * 64,
            "source_tree_sha256": "f" * 64,
        },
    )
    monkeypatch.setattr(runner, "prepare_site_source", prepare)
    monkeypatch.setattr(runner, "payload_source_tree_digest", lambda _site: "1" * 64)
    monkeypatch.setattr(
        runner,
        "tar_site_source",
        lambda _site, target: Path(target).write_bytes(b"site"),
    )
    monkeypatch.setattr(
        runner,
        "create_input_dataset",
        lambda _client, folder, ref: observed.update(
            {"ref": ref, "files": sorted(path.name for path in folder.iterdir())}
        ),
    )
    monkeypatch.setattr(
        runner,
        "wait_dataset_ready",
        lambda _client, ref, *, expected_files: observed.update(
            {"ready_ref": ref, "expected": sorted(expected_files)}
        ),
    )

    _build_id, dataset_ref = runner.stage_kernel_and_dataset(
        _runner_args(db, manifest), staging, dataset, object(), "owner"
    )

    projection_files = list(dataset.glob("static-projection-*.sqlite"))
    assert len(projection_files) == 1
    assert projection_files[0].stat().st_ino == db.stat().st_ino
    assert "events.sqlite" not in observed["files"]
    assert dataset_ref == observed["ready_ref"] == observed["ref"]
    assert dataset_ref.startswith("owner/static-site-builder-input-")
    assert projection_files[0].name in observed["expected"]


def test_adoption_uses_hash_bound_contract_when_local_scratch_is_gone(
    tmp_path: Path,
) -> None:
    import scripts.run_static_site_builder_kaggle as runner

    args = SimpleNamespace(
        profile="production-candidate",
        db=str(tmp_path / "gone.sqlite"),
        snapshot_manifest=str(tmp_path / "gone.manifest.json"),
        adopt_existing=True,
        expected_snapshot_id="snapshot-projection",
        expected_snapshot_sha256="a" * 64,
        expected_snapshot_size=12345,
    )

    assert runner.load_snapshot_contract(args) == {
        "snapshot_id": "snapshot-projection",
        "sha256": "a" * 64,
        "size": 12345,
        "quick_check": "ok",
    }


def test_output_scratch_defaults_outside_persistent_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from static_site_release import static_site_output_root

    monkeypatch.delenv("STATIC_SITE_OUTPUT_SCRATCH_DIR", raising=False)
    monkeypatch.setenv("RUNTIME_SCRATCH_PATH", str(tmp_path))
    output = static_site_output_root()
    assert output.parent == tmp_path
    assert not str(output).startswith("/data/")


def test_durable_cache_persistence_is_size_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import scripts.run_static_site_builder_kaggle as runner

    cache = tmp_path / "cache.bin"
    cache.write_bytes(b"x" * 2048)
    monkeypatch.setenv("STATIC_SITE_DURABLE_CACHE_MAX_BYTES", "1024")
    with pytest.raises(RuntimeError, match="durable cache exceeds bound"):
        runner.require_bounded_durable_cache(cache)
