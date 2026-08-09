from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


SEMANTIC_INPUTS = {
    "static_event_bge_vectors.npz": "bge_vector_cache",
    "static_event_bge_vectors.receipt.json": "bge_vector_receipt",
    "unusual_events_cache.json": "unusual_cache",
    "unusual_events_last_good.json": "unusual_last_good",
    "collection-batch-last-good.json": "collection_batch_last_good",
}


def _runner_args(tmp_path: Path, mode: str) -> SimpleNamespace:
    tmp_path.mkdir(parents=True, exist_ok=True)
    values: dict[str, object] = {
        "build_id": f"production-semantic-{mode}",
        "run_id": f"static-site:semantic-{mode}",
        "profile": "production-candidate",
        "catalog_mode": "full",
        "repo_sha": "a" * 40,
        "candidate_token": "c" * 43,
        "snapshot_contract": {
            "snapshot_id": f"snapshot-{mode}",
            "sha256": "b" * 64,
            "size": 1,
            "quick_check": "ok",
        },
        "current_date": "2026-08-09",
        "current_datetime": "2026-08-09T12:00:00+02:00",
        "build_clock": {
            "effective_date": "2026-08-09",
            "current_datetime": "2026-08-09T12:00:00+02:00",
            "time_zone": "Europe/Kaliningrad",
        },
        "input_fingerprint": "d" * 64,
        "focus_date_from": "",
        "focus_date_to": "",
        "limit": 5000,
        "public_site_origin": "https://kenigevents.ru",
        "asset_base_url": "",
        "astro_asset_base_url": "",
        "ics_base_url": "",
        "public_personalization_supabase_url": "",
        "public_personalization_supabase_publishable_key": "",
        "public_personalization_supabase_relay_url": "",
        "public_yandex_auth_provider": "custom:yandex",
        "public_authorized_search_transport": "json",
        "secret_candidate_artifact_research": False,
        "secret_candidate_require_authorized_search": False,
        "search_corpus_receipt": "",
        "export_in_kaggle": True,
        "db": str(tmp_path / "snapshot.sqlite"),
        "snapshot_manifest": "",
        "related_cache": str(tmp_path / "event_related_chain_cache.json"),
        "related_mode": "bge",
        "related_corpus_revision": "e" * 64,
        "related_response_max_bytes": 256 * 1024,
        "related_total_response_max_bytes": 16 * 1024 * 1024,
        "sync_pgvector_vectors": False,
        "pgvector_embedding_model": "gemini-embedding-2",
        "pgvector_embedding_key_env": "GOOGLE_API_KEY4",
        "pgvector_max_provider_calls": 1000,
        "gemma_related_verify": False,
        "gemma_related_model": "models/gemma-4-26b-a4b-it",
        "gemma_related_key_env": "GOOGLE_API_KEY4",
        "gemma_related_max_anchors": 0,
        "bge_model_revision": "f" * 40,
        "bge_batch_size": 8,
        "unusual_enabled": True,
        "unusual_migration": True,
        "collection_semantic_compute": True,
        "collection_product_source_scope": "static-site-builder-export",
        "collection_product_evidence_trust_scope": "all",
        "semantic_cache_mode": mode,
    }
    Path(values["db"]).write_bytes(b"x")
    Path(values["related_cache"]).write_text("related stays staged\n", encoding="utf-8")
    for filename, attribute in SEMANTIC_INPUTS.items():
        source = tmp_path / f"durable-{filename}"
        source.write_text(f"durable {filename}\n", encoding="utf-8")
        values[attribute] = str(source)
    values["collection_batch"] = str(tmp_path / "collection-batch-v1.json")
    values["collection_product_snapshot"] = str(
        tmp_path / "static-collection-product-snapshot-v1.json"
    )
    return SimpleNamespace(**values)


@pytest.mark.parametrize("mode", ["warm", "cold"])
def test_runner_stages_only_warm_semantic_inputs_and_records_exact_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    from scripts import run_static_site_builder_kaggle as runner

    kernel_source = tmp_path / "kernel-source"
    kernel_source.mkdir()
    monkeypatch.setattr(runner, "KERNEL_SRC", kernel_source)
    monkeypatch.setattr(
        runner,
        "resolve_image_source_contract",
        lambda _args: {
            "manifest_sha256": "1" * 64,
            "source_tree_sha256": "2" * 64,
        },
    )

    def prepare_site(_args: object, work_dir: Path) -> Path:
        site = work_dir / "site"
        site.mkdir()
        (site / "package.json").write_text("{}\n", encoding="utf-8")
        return site

    monkeypatch.setattr(runner, "prepare_site_source", prepare_site)
    monkeypatch.setattr(runner, "create_input_dataset", lambda *_args: None)
    monkeypatch.setattr(runner, "wait_dataset_ready", lambda *_args, **_kwargs: None)

    dataset = tmp_path / f"dataset-{mode}"
    staging = tmp_path / f"staging-{mode}"
    dataset.mkdir()
    staging.mkdir()
    args = _runner_args(tmp_path / mode, mode)

    runner.stage_kernel_and_dataset(args, staging, dataset, object(), "owner")

    config = json.loads((dataset / "build_config.json").read_text(encoding="utf-8"))
    assert config["semantic_cache_mode"] == mode
    assert (dataset / "event_related_chain_cache.json").is_file()
    expected = sorted(SEMANTIC_INPUTS) if mode == "warm" else []
    assert config["semantic_cache_inputs_staged"] == expected
    assert sorted(name for name in SEMANTIC_INPUTS if (dataset / name).is_file()) == expected
    for filename, attribute in SEMANTIC_INPUTS.items():
        assert Path(getattr(args, attribute)).read_text(encoding="utf-8") == (
            f"durable {filename}\n"
        )


@pytest.mark.asyncio
async def test_operator_cold_request_is_durable_and_automatic_cold_is_rejected(
    tmp_path: Path,
) -> None:
    import main
    from models import JobOutbox, JobTask
    from sqlalchemy import select

    db = main.Database(str(tmp_path / "requests.sqlite"))
    await db.init()
    try:
        action = await main.enqueue_static_site_build_request(
            db,
            reason="semantic cold canary",
            correlation_id="cold-canary",
            trigger="operator_request",
            semantic_cache_mode="cold",
        )
        assert action in {"new", "requeued", "merged-rearmed"}
        async with db.get_session() as session:
            row = (
                await session.execute(
                    select(JobOutbox).where(JobOutbox.task == JobTask.static_site_build)
                )
            ).scalar_one()
        assert row.payload["semantic_cache_mode"] == "cold"
        assert row.payload["target_watermark"] == main.static_site_request_watermark(
            row.payload
        )
        with pytest.raises(ValueError, match="explicit operator"):
            await main.enqueue_static_site_build_request(
                db,
                reason="not an operator",
                trigger="smart_update",
                semantic_cache_mode="cold",
            )
    finally:
        await db.close()


def test_mode_changes_request_watermark_and_input_fingerprint(tmp_path: Path) -> None:
    import main
    from static_site_release import compute_static_site_input_fingerprint

    common = {
        "reason": "same request",
        "event_ids": [42],
        "correlation_id": "same-correlation",
        "effect_at": "2026-08-09T10:00:00+00:00",
        "trigger": "operator_request",
    }
    warm = main.make_static_site_request_payload(
        **common, semantic_cache_mode="warm"
    )
    cold = main.make_static_site_request_payload(
        **common, semantic_cache_mode="cold"
    )
    assert warm["target_watermark"] != cold["target_watermark"]

    database = tmp_path / "fingerprint.sqlite"
    with sqlite3.connect(database):
        pass
    warm_fingerprint, _ = compute_static_site_input_fingerprint(
        database,
        effective_date="2026-08-09",
        repo_sha="a" * 40,
        build_config={"semantic_cache_mode": "warm"},
    )
    cold_fingerprint, _ = compute_static_site_input_fingerprint(
        database,
        effective_date="2026-08-09",
        repo_sha="a" * 40,
        build_config={"semantic_cache_mode": "cold"},
    )
    assert warm_fingerprint != cold_fingerprint


def test_request_cli_accepts_explicit_semantic_cache_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scripts import request_static_site_build

    monkeypatch.setattr(
        sys,
        "argv",
        ["request_static_site_build.py", "--reason", "cold canary", "--semantic-cache-mode", "cold"],
    )
    assert request_static_site_build.parse_args().semantic_cache_mode == "cold"


def test_runner_validates_semantic_cache_mode_in_downloaded_result(
    tmp_path: Path,
) -> None:
    from scripts import run_static_site_builder_kaggle as runner

    output = tmp_path / "output"
    output.mkdir()
    result_path = output / "static_site_build_result.json"
    result_path.write_text(
        json.dumps(
            {
                "ok": True,
                "build_id": "preview-semantic-cold",
                "semantic_cache_mode": "cold",
                "semantic_cache_inputs_staged": [],
            }
        ),
        encoding="utf-8",
    )
    args = SimpleNamespace(
        build_id="preview-semantic-cold",
        profile="preview",
        semantic_cache_mode="cold",
    )
    assert runner.validate_downloaded_result(output, args)["semantic_cache_mode"] == "cold"
    args.semantic_cache_mode = "warm"
    with pytest.raises(RuntimeError, match="semantic cache mode mismatch"):
        runner.validate_downloaded_result(output, args)


@pytest.mark.asyncio
async def test_remote_recovery_forwards_handoff_semantic_cache_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import main

    fingerprint = "a" * 64
    claim = SimpleNamespace(
        run_id="static-site:cold-recovery",
        input_fingerprint=fingerprint,
        dataset_ref="owner/exact-cold-input",
        effective_date="2026-08-09",
        remote_status="running",
    )
    monkeypatch.setattr(main, "recoverable_static_site_build", lambda *_a, **_k: claim)
    monkeypatch.setattr(main, "validate_snapshot", lambda *_a, **_k: SimpleNamespace())
    captured: dict[str, object] = {}

    def command(**kwargs: object) -> list[str]:
        captured.update(kwargs)
        return ["runner"]

    monkeypatch.setattr(main, "_static_site_build_kaggle_command", command)

    class Process:
        returncode = 75

        async def communicate(self) -> tuple[bytes, None]:
            return b"remote still running\n", None

    async def subprocess(*_args: object, **_kwargs: object) -> Process:
        return Process()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", subprocess)
    handoff = {
        "build_id": "production-cold-recovery",
        "run_id": claim.run_id,
        "repo_sha": "b" * 40,
        "candidate_token": "c" * 43,
        "snapshot_path": "/data/cold.sqlite",
        "manifest_path": "/data/cold.manifest.json",
        "input_fingerprint": fingerprint,
        "current_datetime": "2026-08-09T12:00:00+02:00",
        "semantic_cache_mode": "cold",
    }
    with pytest.raises(main.StaticSiteSingleFlightDeferred, match="recovery_live"):
        await main._recover_previous_static_site_attempt(
            db=SimpleNamespace(path="/data/db.sqlite"),
            job_id=7,
            request_payload={"semantic_cache_mode": "cold", "remote_handoff": handoff},
            limit=5000,
            current_repo_sha="b" * 40,
            current_source_identity=None,
        )
    assert captured["semantic_cache_mode"] == "cold"
    assert captured["adopt_existing"] is True
